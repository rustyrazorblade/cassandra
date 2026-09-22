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

package org.apache.cassandra.db.cursorreads;

import java.util.List;
import java.util.function.Supplier;

import com.google.common.base.Strings;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

/**
 * Differential scenarios for a REVERSE-order ({@code ORDER BY ... DESC} within a partition)
 * single-partition read served on the cursor path (CASSANDRA-20428, gap #5).  Before this gap was
 * closed {@code CursorReads.isReadSupported} declined {@code filter.isReversed()}, so a reverse read
 * served on the legacy iterator path, breaking the no-fallback rule.
 *
 * <p>A reverse read serves in descending clustering order.  A single sstable leg walks the partition
 * block by block from the slice end backward (the cursor analog of BTI
 * {@code SSTableReversedIterator}); a multi-leg read reconciles the per-leg reverse streams in a
 * descending merge.  Per block the reverse producer forward-collects the block's unfiltereds, emits
 * them in reverse, and synthesizes the open/close range-tombstone bound markers at the block edges.
 *
 * <p>Every scenario asserts the result is byte-identical to the iterator path through the base harness
 * ({@link CursorReadDifferentialTester}).  The base harness's silent-fallback guard proves the cursor
 * path actually engaged.
 *
 * <p>The base class runs BIG; {@link BtiReverseCursorReadDifferentialTest} pins BTI, the priority
 * format for this work and the one whose row index drives lazy backward block traversal.
 */
public class ReverseCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void pinSelectedFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(formatName());
    }

    @After
    public void restoreSelectedFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /** The sstable format this class pins.  The BTI subclass overrides it. */
    protected String formatName()
    {
        return "big";
    }

    // ---------------------------------------------------------------- command builders

    private static DecoratedKey key(ColumnFamilyStore cfs, long pk)
    {
        return cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(pk));
    }

    private static Slice inclSlice(TableMetadata metadata, long startIncl, long endIncl)
    {
        ClusteringBound<?> start = ClusteringBound.create(metadata.comparator, true, true, startIncl);
        ClusteringBound<?> end = ClusteringBound.create(metadata.comparator, false, true, endIncl);
        return Slice.make(start, end);
    }

    /** A reverse (DESC) read.  With no ranges it is a full-partition reverse read (Slices.ALL). */
    private static Supplier<SinglePartitionReadCommand> reverseRead(ColumnFamilyStore cfs, long pk,
                                                                    long now, int limit, long[]... ranges)
    {
        return () -> {
            TableMetadata metadata = cfs.metadata();
            Slices slices;
            if (ranges.length == 0)
            {
                slices = Slices.ALL;
            }
            else
            {
                Slices.Builder builder = new Slices.Builder(metadata.comparator);
                for (long[] range : ranges)
                    builder.add(inclSlice(metadata, range[0], range[1]));
                slices = builder.build();
            }
            DataLimits limits = limit < 0 ? DataLimits.NONE : DataLimits.cqlLimits(limit);
            return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(),
                                                     limits, key(cfs, pk),
                                                     new ClusteringIndexSliceFilter(slices, true)); // reversed
        };
    }

    // ---------------------------------------------------------------- single-leg

    /** Full-partition reverse read against one sstable leg. */
    @Test
    public void reverseFullPartitionSingleLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1));
    }

    /** Reverse read of a single bounded slice against one sstable leg. */
    @Test
    public void reverseSliceSingleLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1, new long[]{ 8, 27 }));
    }

    /** Reverse read of a slice that spans a range tombstone: the reverse producer must synthesize the
     *  open/close bound markers in reverse order. */
    @Test
    public void reverseSliceWithRangeTombstoneSingleLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 10L, 25L);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1, new long[]{ 5, 30 }));
    }

    /** Reverse read across a whole-partition deletion plus surviving newer rows. */
    @Test
    public void reverseCrossingPartitionDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        execute("DELETE FROM %s WHERE pk = ?", 1L);
        for (long ck = 5; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck + 100);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1));
    }

    /** Reverse multi-slice read against one sstable leg. */
    @Test
    public void reverseMultiSliceSingleLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1,
                                                         new long[]{ 3, 9 }, new long[]{ 20, 30 }));
    }

    // ---------------------------------------------------------------- limit touches only the tail

    /**
     * A reverse read with a small LIMIT must touch only the TAIL blocks of the partition, not scan the
     * whole partition and reverse.  Asserted with the {@code unfilteredsMaterialized} counter: a
     * limited reverse read must materialize far fewer unfiltereds than a full reverse read of the same
     * partition.  A whole-partition-then-reverse implementation would materialize the whole partition.
     */
    @Test
    public void reverseLimitTouchesOnlyTail() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 400; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();

        long now = FBUtilities.nowInSeconds();

        // byte-identity for the limited reverse read
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, 5));

        // now measure the materialization footprint of the limited reverse read in isolation
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long before = CursorReads.unfilteredsMaterialized();
            responseBytes(reverseRead(cfs, 1L, now, 5).get());
            long limitedDelta = CursorReads.unfilteredsMaterialized() - before;

            assertTrue("a reverse LIMIT 5 read materialized " + limitedDelta + " unfiltereds; it must " +
                       "touch only the tail blocks, not the whole 400-row partition",
                       limitedDelta > 0 && limitedDelta < 100);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    // ---------------------------------------------------------------- multi-leg

    /** Reverse read across two overlapping sstable legs, with a cross-leg range tombstone. */
    @Test
    public void reverseMultiLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "first-" + ck);
        flush();
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 9L, 18L);
        for (long ck = 0; ck < 30; ck += 2)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "second-" + ck, 1L, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1, new long[]{ 3, 25 }));
    }

    /** Reverse read merging a memtable leg with an sstable leg. */
    @Test
    public void reverseMemtablePlusSstable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "sstable-" + ck);
        flush();
        for (long ck = 0; ck < 30; ck += 3)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "memtable-" + ck, 1L, ck);

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1));
    }

    // ---------------------------------------------------------------- wide, row-indexed partition

    /** Builds a single sstable leg holding one partition wide enough to be row-indexed (past
     *  column_index_size, default 64KiB), so the reverse walk seeks real index blocks.  Each row
     *  carries a padded text value, so a few thousand rows span many blocks. */
    private ColumnFamilyStore buildBlockIndexedPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, pad text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String pad = Strings.repeat("x", 200);
        for (long ck = 0; ck < 4000; ck++)
            execute("INSERT INTO %s (pk, ck, v1, pad) VALUES (?, ?, ?, ?)", 1L, ck, ck, pad);
        flush();
        assertTrue("scenario needs a single sstable leg", cfs.getLiveSSTables().size() == 1);
        return cfs;
    }

    /**
     * A full reverse read over a wide, row-indexed partition.  The tail block the reverse walk seeks
     * first can start on the partition-end marker; the cursor must treat that as an empty block and
     * step to the earlier block, exactly as {@code SSTableReversedIterator} does when
     * {@code deserializer.hasNext()} is false.  Regression for CASSANDRA-20428: before the fix the
     * reverse block seek threw {@code IllegalStateException} on the end-of-partition marker.
     */
    @Test
    public void reverseFullPartitionBlockIndexed() throws Throwable
    {
        ColumnFamilyStore cfs = buildBlockIndexedPartition();
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1));
    }

    /** A bounded reverse slice over the same wide, row-indexed partition. */
    @Test
    public void reverseSliceBlockIndexed() throws Throwable
    {
        ColumnFamilyStore cfs = buildBlockIndexedPartition();
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1, new long[]{ 500, 3500 }));
    }

    /** A limited reverse read over the same wide, row-indexed partition: only the tail blocks. */
    @Test
    public void reverseLimitBlockIndexed() throws Throwable
    {
        ColumnFamilyStore cfs = buildBlockIndexedPartition();
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, 5));
    }

    /** Builds a wide, row-indexed partition (as {@link #buildBlockIndexedPartition}) that ALSO carries
     *  a range tombstone spanning many blocks, so the tombstone stays OPEN across several block
     *  boundaries.  The reverse walk must carry the block's open deletion (row-index
     *  {@code IndexInfo.openDeletion}) from block to block as it descends. */
    private ColumnFamilyStore buildBlockIndexedPartitionWithSpanningRangeTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, pad text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String pad = Strings.repeat("x", 200);
        for (long ck = 0; ck < 4000; ck++)
            execute("INSERT INTO %s (pk, ck, v1, pad) VALUES (?, ?, ?, ?)", 1L, ck, ck, pad);
        // a range tombstone that opens in an early block and closes in a much later block, so every
        // block between them is entered with an open deletion.
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 800L, 3200L);
        flush();
        assertTrue("scenario needs a single sstable leg", cfs.getLiveSSTables().size() == 1);
        return cfs;
    }

    /** Full reverse read over a wide partition whose range tombstone stays open across many blocks. */
    @Test
    public void reverseFullPartitionBlockIndexedSpanningRangeTombstone() throws Throwable
    {
        ColumnFamilyStore cfs = buildBlockIndexedPartitionWithSpanningRangeTombstone();
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1));
    }

    /** Bounded reverse slice that descends from a block above the range tombstone, through the
     *  blocks the tombstone spans, and into a block below it. */
    @Test
    public void reverseSliceBlockIndexedSpanningRangeTombstone() throws Throwable
    {
        ColumnFamilyStore cfs = buildBlockIndexedPartitionWithSpanningRangeTombstone();
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1, new long[]{ 500, 3500 }));
    }

    /** Bounded reverse slice fully INSIDE the spanning range tombstone: every block the slice touches
     *  is entered with the tombstone already open, and the slice edges must close/open it. */
    @Test
    public void reverseSliceInsideSpanningRangeTombstone() throws Throwable
    {
        ColumnFamilyStore cfs = buildBlockIndexedPartitionWithSpanningRangeTombstone();
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, reverseRead(cfs, 1L, now, -1, new long[]{ 1200, 2800 }));
    }

    // ---------------------------------------------------------------- memtable-only

    /** A memtable-only reverse read (no sstable). */
    @Test
    public void reverseMemtableOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        // no flush: served from the memtable only

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = reverseRead(cfs, 1L, now, -1);

        DatabaseDescriptor.setCursorReadsEnabled(false);
        List<String> oracleRecords = canonicalRecords(cmd.get());
        byte[] oracleBytes = responseBytes(cmd.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("memtable-only reverse read must pass the cursor read gate",
                       CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));
            List<String> cursorRecords = canonicalRecords(cmd.get());
            byte[] cursorBytes = responseBytes(cmd.get());
            compareRecords(oracleRecords, cursorRecords);
            assertResponseBytesEqual(oracleBytes, cursorBytes);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }
}
