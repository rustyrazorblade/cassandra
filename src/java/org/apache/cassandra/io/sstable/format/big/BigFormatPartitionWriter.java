/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.SortedTablePartitionWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriter;

/**
 * Column index builder used by {@link org.apache.cassandra.io.sstable.format.big.BigTableWriter}.
 * For index entries that exceed {@link org.apache.cassandra.config.Config#column_index_cache_size},
 * this uses the serialization logic as in {@link RowIndexEntry}.
 */
public class BigFormatPartitionWriter extends SortedTablePartitionWriter
{
    @VisibleForTesting
    public static final int DEFAULT_GRANULARITY = 64 * 1024;

    /**
     * Ceiling on how far ahead {@link #presizeIndexOffsets(long)} will size the offsets buffer, in elements. 1M
     * elements is a 4MiB buffer, enough for a 64GiB partition at the default granularity; anything beyond that is
     * covered by the (doubling) growth path instead of being reserved up front on the strength of an estimate.
     */
    private static final int MAX_PRESIZED_INDEX_OFFSETS = 1 << 20;

    // used, if the row-index-entry reaches config switchIndexInfoToBufferThreshold
    private DataOutputBuffer rowIndexEntryBuffer;
    // used to track the total serialized size of indexSamples (unused for buffer)
    private int indexSamplesSerializedSize;
    // used, until the row-index-entry reaches switchIndexInfoToBufferThreshold (from config column_index_cache_size, or default 64k)
    private final List<IndexInfo> indexSamples = new ArrayList<>();

    private DataOutputBuffer reusableBuffer;

    private int columnIndexCount;
    // offsets of the serialized IndexInfo objects; retained across partitions for the lifetime of this writer
    private final PooledIntArray indexOffsets = new PooledIntArray();

    private final ISerializer<IndexInfo> idxSerializer;

    /** Beyond this limit we switch from storing IndexInfo in the list to directly serializing them into a buffer */
    private final int switchIndexInfoToBufferThreshold;
    /** If a partition grows beyond this size we store inter-partition index data in IndexInfo */
    private final int indexBlockThreshold;

    BigFormatPartitionWriter(SerializationHeader header,
                             SequentialWriter writer,
                             Version version,
                             ISerializer<IndexInfo> indexInfoSerializer)
    {
        this(header, writer, version, indexInfoSerializer, DatabaseDescriptor.getColumnIndexCacheSize(), DatabaseDescriptor.getColumnIndexSize(DEFAULT_GRANULARITY));
    }

    BigFormatPartitionWriter(SerializationHeader header,
                             SequentialWriter writer,
                             Version version,
                             ISerializer<IndexInfo> indexInfoSerializer,
                             int cacheSizeThreshold,
                             int indexSize)
    {
        super(header, writer, version);
        this.idxSerializer = indexInfoSerializer;
        this.switchIndexInfoToBufferThreshold = cacheSizeThreshold;
        this.indexBlockThreshold = indexSize;
    }

    public void reset()
    {
        super.reset();
        this.columnIndexCount = 0;
        this.indexSamplesSerializedSize = 0;
        this.indexSamples.clear();

        if (this.rowIndexEntryBuffer != null)
            this.reusableBuffer = this.rowIndexEntryBuffer;
        this.rowIndexEntryBuffer = null;
    }

    public int getColumnIndexCount()
    {
        return columnIndexCount;
    }

    public ByteBuffer buffer()
    {
        return rowIndexEntryBuffer != null ? rowIndexEntryBuffer.buffer() : null;
    }

    public List<IndexInfo> indexSamples()
    {
        if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0) <= switchIndexInfoToBufferThreshold)
        {
            return indexSamples;
        }

        return null;
    }

    /**
     * @return an on-heap copy of this partition's index block offsets, or {@code null} if it has no index blocks, as
     *         for a partition carrying only a partition-level deletion. Building the copy is only worthwhile when
     *         {@link #indexSamples()} is non-null, since that is the only case {@link RowIndexEntry#create} reads it.
     */
    public int[] offsets()
    {
        return columnIndexCount > 0
               ? indexOffsets.toArray(columnIndexCount)
               : null;
    }

    /**
     * Sizes the offsets buffer for a partition of the given estimated size before its first index block is added, so
     * that writing a large partition does not repeatedly grow and copy the buffer. Purely an optimisation - an
     * estimate that undershoots is corrected by {@link PooledIntArray#ensureCapacity(int)} as blocks are added.
     *
     * <p>A {@code column_index_size} of 0 is a legal configuration meaning "index every row", so the block count
     * cannot be derived from the partition size; such a writer is left to grow its buffer geometrically instead.
     *
     * @param estimatedPartitionSizeBytes estimated size of the partition about to be written, or a non-positive value
     *                                    if no estimate is available
     */
    void presizeIndexOffsets(long estimatedPartitionSizeBytes)
    {
        if (estimatedPartitionSizeBytes <= 0 || indexBlockThreshold <= 0)
            return;

        // one block beyond the estimate, plus the trailing partial block that finish() always adds
        long estimatedBlocks = estimatedPartitionSizeBytes / indexBlockThreshold + 2;
        indexOffsets.ensureCapacity((int) Math.min(estimatedBlocks, MAX_PRESIZED_INDEX_OFFSETS));
    }

    private void addIndexBlock() throws IOException
    {
        IndexInfo cIndexInfo = new IndexInfo(firstClustering,
                                             lastClustering,
                                             indexBlockStartOffset,
                                             currentOffsetInPartition() - indexBlockStartOffset,
                                             !openMarker.isLive() ? openMarker : null);

        // indexOffsets is used for both shallow (ShallowIndexedEntry) and non-shallow IndexedEntry.
        // For shallow ones, we need it to serialize the offsts in finish().
        // For non-shallow ones, the offsts are passed into IndexedEntry, so we don't have to
        // calculate the offsets again.

        // indexOffsets contains the offsets of the serialized IndexInfo objects.
        // I.e. indexOffsets[0] is always 0 so we don't have to deal with a special handling
        // for index #0 and always subtracting 1 for the index (which could be error-prone).
        indexOffsets.ensureCapacity(columnIndexCount + 1);
        //the 0th element is always 0
        indexOffsets.set(columnIndexCount,
                         columnIndexCount == 0
                         ? 0
                         : (rowIndexEntryBuffer != null
                            ? Ints.checkedCast(rowIndexEntryBuffer.position())
                            : indexSamplesSerializedSize));
        columnIndexCount++;

        // First, we collect the IndexInfo objects until we reach Config.column_index_cache_size in an ArrayList.
        // When column_index_cache_size is reached, we switch to byte-buffer mode.
        if (rowIndexEntryBuffer == null)
        {
            indexSamplesSerializedSize += idxSerializer.serializedSize(cIndexInfo);
            if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.INT_SIZE > switchIndexInfoToBufferThreshold)
            {
                rowIndexEntryBuffer = reuseOrAllocateBuffer();
                // serialize pre-existing samples
                for (IndexInfo indexSample : indexSamples)
                {
                    /** {@link IndexInfo.Serializer#serialize} */
                    idxSerializer.serialize(indexSample, rowIndexEntryBuffer);
                }
                // release pre-existing samples
                indexSamples.clear();
            }
            else
            {
                indexSamples.add(cIndexInfo);
            }
        }
        // don't put an else here since buffer may be allocated in preceding if block
        if (rowIndexEntryBuffer != null)
        {
            /** {@link IndexInfo.Serializer#serialize} */
            idxSerializer.serialize(cIndexInfo, rowIndexEntryBuffer);
        }

        firstClustering = null;
    }

    private DataOutputBuffer reuseOrAllocateBuffer()
    {
        // Check whether a reusable DataOutputBuffer already exists for this
        // ColumnIndex instance and return it.
        if (reusableBuffer != null)
        {
            DataOutputBuffer buffer = reusableBuffer;
            buffer.clear();
            return buffer;
        }
        // don't use the standard RECYCLER as that only recycles up to 1MB and requires proper cleanup
        return new DataOutputBuffer(switchIndexInfoToBufferThreshold * 2);
    }

    @Override
    public void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        super.addUnfiltered(unfiltered);

        // if we hit the column index size that we have to index after, go ahead and index it.
        long sizeSinceLastIndexBlock = currentOffsetInPartition() - indexBlockStartOffset;
        if (sizeSinceLastIndexBlock >= this.indexBlockThreshold)
            addIndexBlock();
    }

    @Override
    public long finish() throws IOException
    {
        long endPosition = super.finish();

        // It's possible we add no rows, just a top level deletion
        if (written == 0)
            return endPosition;

        // the last column may have fallen on an index boundary already.  if not, index it explicitly.
        if (firstClustering != null)
            addIndexBlock();

        // If we serialize the IndexInfo objects directly in the code above into 'buffer',
        // we have to write the offsts to these here. The offsets have already been collected
        // in indexOffsets[]. buffer is != null, if it exceeds Config.column_index_cache_size.
        // In the other case, when buffer==null, the offsets are serialized in RowIndexEntry.IndexedEntry.serialize().
        if (rowIndexEntryBuffer != null)
        {
            for (int i = 0; i < columnIndexCount; i++)
                rowIndexEntryBuffer.writeInt(indexOffsets.get(i));
        }

        // we should always have at least one computed index block, but we only write it out if there is more than that.
        assert columnIndexCount > 0 && getHeaderLength() >= 0;

        return endPosition;
    }

    public int indexInfoSerializedSize()
    {
        return rowIndexEntryBuffer != null
               ? rowIndexEntryBuffer.buffer().limit()
               : indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0);
    }

    @Override
    public void releaseBuffers()
    {
        indexOffsets.release();
    }

    @Override
    public void close()
    {
        releaseBuffers();
    }
}