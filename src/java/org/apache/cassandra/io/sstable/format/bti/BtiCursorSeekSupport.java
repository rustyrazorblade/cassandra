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

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.bti.RowIndexReader.IndexInfo;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Bridge exposing the BTI row index to the cursor read path ({@code CursorReads}) without
 * widening the visibility of the bti package's
 * internals: {@link TrieIndexEntry} and {@link BtiTableReader#getExactPosition} are
 * package-private, so the partition lookup and row-index floor query live here.
 *
 * Read-only over shipped BTI machinery: this class only calls the exact primitives the BTI
 * iterator path itself uses ({@code getExactPosition} for the partition entry — the same call
 * {@link BtiTableReader#rowIterator} makes, with identical listener/metrics notifications — and
 * {@link RowIndexReader#separatorFloor} for the block floor, mirroring
 * {@code SSTableIterator.ForwardIndexedReader.setForSlice}).
 */
public final class BtiCursorSeekSupport
{
    private BtiCursorSeekSupport()
    {
    }

    /**
     * Result of the exact-key partition lookup: where the partition starts in Data.db, and — when
     * the partition is wide enough to have been row-indexed — the handle needed to query its row
     * index trie.
     */
    public static final class PartitionEntry
    {
        public final long dataPosition;
        private final long indexTrieRoot; // -1 when the partition has no row index

        private PartitionEntry(long dataPosition, long indexTrieRoot)
        {
            this.dataPosition = dataPosition;
            this.indexTrieRoot = indexTrieRoot;
        }

        public boolean isIndexed()
        {
            return indexTrieRoot != -1;
        }
    }

    /**
     * A position within a partition's row cluster to start reading from: the absolute Data.db
     * position of an unfiltered's flags byte, plus the range-tombstone deletion open at that
     * point per the row index ({@code null} when no range tombstone is open there) — the exact
     * payload {@code ForwardIndexedReader.setForSlice} seeds {@code openMarker} with.
     */
    public static final class SeekPoint
    {
        public final long dataPosition;
        public final DeletionTime openMarker;

        private SeekPoint(long dataPosition, DeletionTime openMarker)
        {
            this.dataPosition = dataPosition;
            this.openMarker = openMarker;
        }
    }

    /**
     * Exact-key partition lookup, with the same {@link SSTableReadsListener} notifications (and
     * bloom-filter / partition-index metrics updates) as the BTI iterator path's own
     * {@code rowIterator(key, ...)} entry point.
     *
     * @return the partition's entry, or {@code null} if this sstable does not contain the key
     */
    public static PartitionEntry exactPartitionEntry(BtiTableReader sstable, DecoratedKey key, SSTableReadsListener listener)
    {
        TrieIndexEntry entry = sstable.getExactPosition(key, listener, true);
        return entry == null ? null : new PartitionEntry(entry.position, entry.indexTrieRoot);
    }

    /**
     * Floor query on an indexed partition's row index: the latest index block that could contain
     * the first unfiltered at-or-after {@code sliceStart}. Mirrors
     * {@code ForwardIndexedReader.setForSlice}'s
     * {@code indexReader.separatorFloor(comparator.asByteComparable(slice.start()))} exactly,
     * including the base-position rebasing of the block's data offset.
     *
     * @param entry a lookup result with {@link PartitionEntry#isIndexed()} true
     */
    public static SeekPoint floorBlock(BtiTableReader sstable,
                                       PartitionEntry entry,
                                       ClusteringComparator comparator,
                                       ClusteringBound<?> sliceStart)
    {
        assert entry.isIndexed();
        try (RowIndexReader index = new RowIndexReader(sstable.rowIndexFile(), entry.indexTrieRoot, sstable.descriptor.version))
        {
            IndexInfo info = index.separatorFloor(comparator.asByteComparable(sliceStart));
            assert info != null; // same invariant ForwardIndexedReader asserts
            return new SeekPoint(entry.dataPosition + info.offset, info.openDeletion);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    /**
     * Opens a reverse walk over an indexed partition's row index blocks, driving a reverse read the
     * way {@code SSTableReversedIterator.ReverseIndexedReader.setForSlice} does: a
     * {@link RowIndexReverseIterator} positioned at {@code sliceEnd} yields the covering blocks from
     * the slice end backward, so a reverse read with a small limit touches only the tail blocks.
     *
     * @param entry a lookup result with {@link PartitionEntry#isIndexed()} true
     */
    public static ReverseBlockCursor reverseBlockCursor(BtiTableReader sstable,
                                                        PartitionEntry entry,
                                                        ClusteringComparator comparator,
                                                        ClusteringBound<?> sliceEnd)
    {
        assert entry.isIndexed();
        RowIndexReverseIterator index = new RowIndexReverseIterator(sstable.rowIndexFile(),
                                                                    entry.indexTrieRoot,
                                                                    ByteComparable.EMPTY,
                                                                    comparator.asByteComparable(sliceEnd),
                                                                    sstable.descriptor.version);
        return new ReverseBlockCursor(sstable, entry.dataPosition, index);
    }

    /**
     * A cursor over an indexed partition's row index blocks in REVERSE order (slice end toward slice
     * start).  Each {@link #nextBlock()} returns the next block's {@link SeekPoint}: the absolute
     * Data.db position of the block's first unfiltered and the range-tombstone deletion open at the
     * block start, mirroring {@code ReverseIndexedReader.gotoBlock}.  {@code null} when no earlier
     * block remains.
     */
    public static final class ReverseBlockCursor implements AutoCloseable
    {
        private final BtiTableReader sstable;
        private final long basePosition;
        private final RowIndexReverseIterator index;

        private ReverseBlockCursor(BtiTableReader sstable, long basePosition, RowIndexReverseIterator index)
        {
            this.sstable = sstable;
            this.basePosition = basePosition;
            this.index = index;
        }

        public SeekPoint nextBlock()
        {
            try
            {
                IndexInfo info = index.nextIndexInfo();
                if (info == null)
                    return null;
                return new SeekPoint(basePosition + info.offset, info.openDeletion);
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }

        @Override
        public void close()
        {
            index.close();
        }
    }
}
