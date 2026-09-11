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

/**
 * Bridge exposing the BTI row index to the cursor read path ({@code CursorReads}, Phase 2 /
 * milestone M1 of CASSANDRA-20428) without widening the visibility of the bti package's
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
}
