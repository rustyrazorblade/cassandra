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

import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * M1 (Phase 2, CASSANDRA-20428) differential scenarios for bounded materialization: narrow
 * slices of wide multi-index-block partitions, slice bounds cutting range tombstones open at the
 * seek point, partition-boundary slices, and seeks past all rows.
 *
 * THIS class runs under the default (BIG) format, where the cursor path has no row-index seek:
 * it proves the scenario corpus is served correctly by the Phase 1 eager walk.
 * {@link BtiSeekSliceCursorReadDifferentialTest} re-runs the corpus under BTI — where the seek
 * engages — and enables the seek-effectiveness guard via {@link #seekCapableFormat()}.
 *
 * The guard exists because the differential comparison alone CANNOT catch a broken seek: a
 * cursor path that materializes the whole partition and slices afterwards produces byte-identical
 * results by design. So on a seek-capable format every scenario additionally asserts, from
 * {@code CursorReads}' production counters, that (a) the expected number of row-index seeks
 * actually happened ({@link CursorReads#sstableLegRowIndexSeeks}) and (b) the number of
 * materialized unfiltereds stayed bounded by index-block granularity instead of partition size
 * ({@link CursorReads#unfilteredsMaterialized}) — the same never-trust-a-silent-path pattern as
 * the harness's {@code sstableLegsServed()} fallback guard.
 *
 * All wide partitions here are written with 1KiB index blocks ({@code column_index_size}), so
 * {@link #WIDE_ROWS} rows span dozens of row-index blocks and a block-granular seek is clearly
 * distinguishable from a partition scan.
 */
public class SeekSliceCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    protected static final int WIDE_ROWS = 2000;

    /**
     * Materialization cap per cursor run for the narrow-slice scenarios below: generous room for
     * the slice's own rows plus one 1KiB index block of in-block pre-slice rows (~30 rows of the
     * shapes used here) plus markers — while remaining ~20x below {@link #WIDE_ROWS}, so an
     * accidental whole-partition walk fails loudly.
     */
    protected static final long NARROW_SLICE_MATERIALIZATION_CAP = 100;

    private int originalColumnIndexSizeKiB;

    @Before
    public void shrinkIndexBlocks()
    {
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
    }

    @After
    public void restoreIndexBlocks()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    /** Overridden to true by the BTI subclass: scenarios then enforce the seek/bounded-materialization guard. */
    protected boolean seekCapableFormat()
    {
        return false;
    }

    /**
     * Differential assertion (identical records + identical intra-node ReadResponse bytes, see
     * the base harness) plus the seek-effectiveness guard on a seek-capable format. The harness
     * executes the cursor path exactly twice per call (canonical records + response bytes),
     * hence the doubling below.
     *
     * @param expectedSeeksPerRun sstable legs expected to actually issue a row-index seek in one
     *                            cursor execution (0 for scenarios that must NOT seek, e.g. a
     *                            slice starting in the partition's first index block)
     * @param maxMaterializedPerRun cap on unfiltereds materialized in one cursor execution, or -1
     *                            for scenarios with no meaningful bound
     */
    protected void assertSeekBoundedCursorReadMatchesIterator(ColumnFamilyStore cfs,
                                                              Supplier<SinglePartitionReadCommand> command,
                                                              int expectedSeeksPerRun,
                                                              long maxMaterializedPerRun)
    {
        long seeksBefore = CursorReads.sstableLegRowIndexSeeks();
        long materializedBefore = CursorReads.unfilteredsMaterialized();

        assertCursorReadMatchesIterator(cfs, command);

        long seeks = CursorReads.sstableLegRowIndexSeeks() - seeksBefore;
        long materialized = CursorReads.unfilteredsMaterialized() - materializedBefore;
        if (!seekCapableFormat())
        {
            assertEquals("row-index seek issued on a format the cursor path does not seek on", 0, seeks);
            return;
        }
        assertEquals("row-index seeks across the harness's two cursor runs", 2L * expectedSeeksPerRun, seeks);
        if (maxMaterializedPerRun >= 0)
            assertTrue("cursor path materialized " + materialized + " unfiltereds over two runs, bound 2 x "
                       + maxMaterializedPerRun + " — the seek/end-stop did not actually bound the walk",
                       materialized <= 2 * maxMaterializedPerRun);
    }

    private void createWideTable()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
    }

    private void insertWideRows(long rows) throws Throwable
    {
        for (long ck = 0; ck < rows; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "value-" + ck);
    }

    @Test
    public void narrowMidSliceOnWideIndexedPartition() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertWideRows(WIDE_ROWS);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1000L).toIncl(1010L).build(),
            1, NARROW_SLICE_MATERIALIZATION_CAP);
        // exclusive-bound variant, landing between rows
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromExcl(1499L).toExcl(1503L).build(),
            1, NARROW_SLICE_MATERIALIZATION_CAP);
    }

    @Test
    public void sliceStartCutsRangeTombstoneOpenBlocksEarlier() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertWideRows(WIDE_ROWS);
        // A range tombstone whose OPEN marker sits many index blocks before the seek point: the
        // only way the seeked read can know the deletion is open at the slice start is the row
        // index's per-block openDeletion payload (RowIndexReader.IndexInfo). The overlapping
        // second delete adds boundary markers to the on-disk stream.
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 400L, 1600L);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 1500L, 1700L);
        flush();

        long now = FBUtilities.nowInSeconds();
        // seek lands deep inside the first open range: artificial open marker at the slice start
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1000L).toIncl(1010L).build(),
            1, NARROW_SLICE_MATERIALIZATION_CAP);
        // slice covering the overlap's boundary markers, both bounds inside open ranges
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1450L).toIncl(1650L).build(),
            1, 320);
        // slice ending exactly where a range tombstone closes
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1550L).toExcl(1700L).build(),
            1, 320);
    }

    @Test
    public void sliceAtPartitionStartDoesNotSeek() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertWideRows(WIDE_ROWS);
        flush();

        long now = FBUtilities.nowInSeconds();
        // The floor block for a slice starting at the first row is the block already being read
        // after the partition header — a forward seek would be a no-op, so none must be issued
        // (mirrors ForwardIndexedReader's position > filePointer check). The end-stop still
        // bounds materialization.
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(0L).toIncl(10L).build(),
            0, NARROW_SLICE_MATERIALIZATION_CAP);
    }

    @Test
    public void sliceAtPartitionEnd() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertWideRows(WIDE_ROWS);
        flush();

        long now = FBUtilities.nowInSeconds();
        // no end bound (slice end = TOP): the walk runs to the partition end, but the seek skips
        // everything before the last blocks
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(WIDE_ROWS - 10L).build(),
            1, NARROW_SLICE_MATERIALIZATION_CAP);
    }

    @Test
    public void seekPastAllRows() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertWideRows(WIDE_ROWS);
        // A lone sentinel row far beyond the dense range. Without it, a slice past every row is
        // eliminated at the COMMAND level (SinglePartitionReadCommand's intersects() check against
        // the sstable's min/max clustering skips the leg before any cursor or iterator runs), so
        // the scenario would prove nothing about the seek — a first run of this suite failed its
        // own served-legs guard exactly that way. The sentinel keeps the slice inside the
        // sstable's clustering range while every DENSE row stays below the slice start.
        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, 1_000_000L, 0L, "sentinel");
        flush();

        long now = FBUtilities.nowInSeconds();
        // slice start beyond every dense row: the floor is a tail index block; its rows are
        // materialized (bounded), drained as pre-slice data, the sentinel sits at-or-past the
        // slice end, and the result is empty
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(WIDE_ROWS + 3000L).toIncl(WIDE_ROWS + 4000L).build(),
            1, NARROW_SLICE_MATERIALIZATION_CAP);
    }

    @Test
    public void endBoundOnlySliceStopsWithoutSeeking() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertWideRows(WIDE_ROWS);
        flush();

        long now = FBUtilities.nowInSeconds();
        // start = BOTTOM: nothing to seek to, but the end-stop must still cut the walk short —
        // this isolates the slice-end stop from the row-index seek
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).toIncl(15L).build(),
            0, NARROW_SLICE_MATERIALIZATION_CAP);
    }

    @Test
    public void staticRowSurvivesSeek() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 text static, v1 bigint, v2 text, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("UPDATE %s SET s1 = ? WHERE pk = ?", "static-value", 1L);
        for (long ck = 0; ck < WIDE_ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "value-" + ck);
        // sentinel far beyond the dense range — see seekPastAllRows: without it the static-only
        // slice below is downgraded to a Slices.NONE statics read at the command level (the slice
        // does not intersect the sstable's clustering range) and no seek can happen at all
        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, 1_000_000L, 0L, "sentinel");
        flush();

        long now = FBUtilities.nowInSeconds();
        // the static row lives at the partition start, BEFORE the seek target — it must be
        // materialized (it is read before the seek is issued) even though every regular row
        // ahead of the slice is skipped
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1200L).toIncl(1210L).build(),
            1, NARROW_SLICE_MATERIALIZATION_CAP);
        // static-only result: the slice is past every dense row, so the seek jumps to a tail
        // block, the pre-slice skip drains it, the sentinel sits past the slice end, and only
        // the static row comes back
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(WIDE_ROWS + 500L).toIncl(WIDE_ROWS + 600L).build(),
            1, NARROW_SLICE_MATERIALIZATION_CAP);
    }

    /**
     * M2.2: a multi-leg read routes through the cursor-level MERGE core AND each indexed BTI leg
     * seeks independently to its row-index floor block for the slice start ({@code seekForMerge}),
     * so the scenario asserts (a) the differential result stays byte-identical, (b) BOTH legs
     * actually seeked (2 per run on a seek-capable format; the M2.1 interim version of this test
     * asserted 0 — the merged path used the eager per-leg walk then), (c) the MERGED
     * materialization stayed block-bounded instead of partition-sized, and (d) the merge core
     * actually served the read. {@code MergeSeekSliceCursorReadDifferentialTest} carries the
     * dedicated M2.2 corpus (cross-leg open-RT seeding, mixed formats, the wrong-seed mutation
     * test); this scenario keeps the original M1 suite honest about merged reads.
     */
    @Test
    public void multiSSTableLegsEachSeek() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertWideRows(WIDE_ROWS);
        flush();
        // second wide, overlapping sstable: newer values for every row
        for (long ck = 0; ck < WIDE_ROWS; ck++)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "second-" + ck, 1L, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        long mergesBefore = CursorReads.cursorMergesServed();
        // 2 seeks per cursor run (one per leg); the MERGED materialization (winners from the two
        // legs' floor blocks through the slice end) must stay block-bounded — an unseeked merged
        // walk would materialize ~WIDE_ROWS merged unfiltereds and fail the cap
        assertSeekBoundedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(700L).toIncl(710L).build(),
            2, NARROW_SLICE_MATERIALIZATION_CAP);
        assertEquals("multi-leg read must have been served by the cursor-level merge core " +
                     "(two cursor runs per harness call)",
                     2L, CursorReads.cursorMergesServed() - mergesBefore);
    }
}
