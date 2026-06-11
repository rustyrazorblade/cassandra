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

package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.ClusteringDescriptorPrefixView;
import org.apache.cassandra.io.sstable.CursorIndexWriter;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.bti.RowIndexReader.IndexInfo;

/**
 * BTI-format index production for the cursor writer: a row-index trie per partition
 * (separators between block boundary clusterings, {@link RowIndexWriter}) and partition
 * index entries ({@link TrieIndexEntry} appended through {@link BtiTableWriter.IndexWriter}),
 * matching {@link BtiFormatPartitionWriter} block-for-block — including the rule that
 * single-block partitions skip the row trie entirely (entry position -1).
 *
 * Boundary clusterings are captured at row time into two reusable descriptors (the
 * descriptors passed to rowWritten are transient) and exposed to the trie through reusable
 * {@link ClusteringDescriptorPrefixView}s. The open-marker snapshot per block allocates only
 * when a range tombstone is actually open at the block start.
 */
public class BtiCursorIndexWriter extends CursorIndexWriter
{
    private static final int DEFAULT_GRANULARITY = 16 * 1024; // BtiFormatPartitionWriter.DEFAULT_GRANULARITY

    private final BtiTableWriter.IndexWriter indexWriter;
    private final org.apache.cassandra.dht.IPartitioner partitioner;
    private final RowIndexWriter rowTrie;
    private final int rowIndexBlockSize;

    private final ClusteringDescriptor firstClustering;
    private final ClusteringDescriptor lastClustering;
    private final AbstractType<?>[] clusteringTypes;
    private boolean blockOpen; // a first clustering has been captured for the current block
    private int rowIndexBlockCount;
    private DeletionTime blockStartOpenMarker = DeletionTime.LIVE;

    public BtiCursorIndexWriter(BtiTableWriter writer,
                                ClusteringComparator comparator,
                                AbstractType<?>[] clusteringTypes)
    {
        this.indexWriter = (BtiTableWriter.IndexWriter) writer.indexWriter;
        this.partitioner = writer.metadata().partitioner;
        this.rowTrie = new RowIndexWriter(comparator, indexWriter.rowIndexWriter, writer.descriptor.version);
        this.rowIndexBlockSize = DatabaseDescriptor.getColumnIndexSize(DEFAULT_GRANULARITY);
        this.firstClustering = new ClusteringDescriptor(clusteringTypes);
        this.lastClustering = new ClusteringDescriptor(clusteringTypes);
        this.clusteringTypes = clusteringTypes;
    }

    @Override
    protected void reset()
    {
        rowTrie.reset();
        rowIndexBlockCount = 0;
        blockOpen = false;
        blockStartOpenMarker = DeletionTime.LIVE;
    }

    @Override
    public void rowWritten(UnfilteredDescriptor descriptor, long rowStart, long rowEnd,
                           DeletionTime openMarker) throws IOException
    {
        if (!blockOpen)
        {
            firstClustering.copy(descriptor);
            blockOpen = true;
        }
        lastClustering.copy(descriptor);

        /** {@link BtiFormatPartitionWriter#addUnfiltered} */
        if (currentOffsetInPartition(rowEnd) - indexBlockStartOffset >= rowIndexBlockSize)
            addIndexBlock(rowEnd, openMarker);
    }

    /** {@link BtiFormatPartitionWriter#addIndexBlock()} */
    private void addIndexBlock(long endOfRowPosition, DeletionTime openMarkerAtEnd) throws IOException
    {
        IndexInfo info = new IndexInfo(indexBlockStartOffset, blockStartOpenMarker);
        // snapshots: RowIndexWriter retains the prefixes lazily across add() calls (prevMax)
        rowTrie.add(ClusteringDescriptorPrefixView.snapshotOf(firstClustering, clusteringTypes),
                    ClusteringDescriptorPrefixView.snapshotOf(lastClustering, clusteringTypes), info);
        blockOpen = false;
        ++rowIndexBlockCount;
        notePosition(endOfRowPosition);
        // the marker open at the end of this block opens the next one; snapshot because the
        // trie retains IndexInfo until complete() and the writer's instance is mutable
        blockStartOpenMarker = openMarkerAtEnd.isLive() ? DeletionTime.LIVE
                                                        : DeletionTime.build(openMarkerAtEnd.markedForDeleteAt(),
                                                                             openMarkerAtEnd.localDeletionTime());
    }

    @Override
    public void endPartition(DecoratedKey key, byte[] keyBytes, int keyLength, int headerLength,
                             DeletionTime partitionDeletionTime, long partitionEnd) throws IOException
    {
        /** {@link BtiFormatPartitionWriter#finish()} + {@link BtiTableWriter#createRowIndexEntry} */
        // the last rows may not have fallen on an index boundary; index the final block explicitly
        if (rowIndexBlockCount > 0 && blockOpen)
            addIndexBlock(partitionEnd, DeletionTime.LIVE);

        // the iterator's complete() payload is the partition length BEFORE the end-of-partition
        // marker (SortedTablePartitionWriter.finish captures it pre-write); partitionEnd here is
        // post-marker, hence the -1
        long trieRoot = rowIndexBlockCount > 1 ? rowTrie.complete(partitionEnd - 1 - partitionStart) : -1;
        TrieIndexEntry entry = TrieIndexEntry.create(partitionStart, trieRoot,
                                                     partitionDeletionTime, rowIndexBlockCount);
        // snapshot: PartitionIndexBuilder retains the key for its next-entry comparison and
        // separator computation; the caller's key AND ITS TOKEN are reusable (mutated in
        // place per partition, see ReusableDecoratedKey.recalculateToken), so both the bytes
        // and the token must be fresh
        java.nio.ByteBuffer keyCopy = org.apache.cassandra.utils.ByteBufferUtil.clone(key.getKey());
        indexWriter.append(new org.apache.cassandra.db.BufferDecoratedKey(partitioner.getToken(keyCopy), keyCopy), entry);
    }
}
