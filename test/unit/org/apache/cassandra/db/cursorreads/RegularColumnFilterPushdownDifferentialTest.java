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

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * M3.2c (CASSANDRA-20428): differential + scan-metrics-parity scenarios for REGULAR-column
 * (value-window) filter pushdown — rows whose regular-column expression fails at winner
 * resolution inside the cursor merge's cell walk are ABANDONED at production (never emitted),
 * while every scan metric, threshold, warning text and abort the iterator path's
 * below-the-filter {@code MetricRecording} would have recorded for them is preserved through the
 * M3.2b {@code ScanStatsAccumulator} apparatus.
 *
 * What is genuinely new relative to M3.2b's clustering pushdown — and what these scenarios pin
 * down specifically:
 * <ul>
 *   <li><b>Late resolution</b>: the verdict lands mid-cell-walk (or at row end, when the filter
 *       column never surfaces a winner), after some of the row's cells were already merged and
 *       emitted — the abandoned row's already-done work must still convert into the exact
 *       dropped-row accounting a pre-cell rejection would have produced
 *       ({@link #lateResolvingFilterColumnAfterEarlierCellWork},
 *       {@link #filterColumnAbsentFromSomeRows}).</li>
 *   <li><b>Winner-value evaluation surfaces</b>: sstable winners evaluate over a reusable window
 *       on the staged scratch bytes, memtable winners over {@code existingCell().buffer()}
 *       directly, and single-contributor memtable rows through the REAL
 *       {@code Expression.isSatisfiedBy} on the escape-hatch row object
 *       ({@link #memtableWonFilterCells}, {@link #memtableOnlyRowsThroughEscapeHatch}).</li>
 *   <li><b>Tie-fed verdicts</b>: an exact-timestamp value tie on the FILTER column must feed the
 *       filter the same winning value on both paths, regardless of which leg holds it
 *       ({@link #exactTimestampValueTieOnFilterColumn}).</li>
 *   <li><b>AND semantics with clustering expressions</b>: either family failing abandons the row,
 *       and clustering short-circuiting first must not skip the accounting the iterator path
 *       still performs ({@link #combinedClusteringAndRegularFilters}).</li>
 * </ul>
 */
public class RegularColumnFilterPushdownDifferentialTest extends CursorReadDifferentialTester
{
    private static final int ROWS = 64;
    /** every 4th ck of the main range is row-tombstoned in leg 1 */
    private static final int ROW_TOMBSTONED = ROWS / 4;                 // 16
    /** every ck % 4 == 1 of the main range gets a v1 cell tombstone in leg 1 (row stays live) */
    private static final int CELL_TOMBSTONED = ROWS / 4;                // 16
    /** cks 100..107: rows with no PK liveness whose only cell is deleted in leg 1 (dead rows) */
    private static final int DEAD_ROWS = 8;
    private static final int TOTAL_TOMBSTONES = ROW_TOMBSTONED + CELL_TOMBSTONED + DEAD_ROWS; // 40
    private static final int TOTAL_LIVE = ROWS - ROW_TOMBSTONED;        // 48

    @After
    public void resetAccountingHooks()
    {
        CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING = false;
    }

    // ---------------------------------------------------------------- byte identity + engagement

    @Test
    public void regularFilterKeepsOneRowDropsRest() throws Throwable
    {
        Workload w = loadMixedWorkload();
        // v1 = 20 holds only for ck = 2 (the overwritten live rows carry v1 = ck * 10); every
        // other row — live-mismatching, cell-tombstoned (dead v1 winner), row-tombstoned (v1
        // winner shadowed) and fully-dead alike — is abandoned at production
        long materializedBefore = CursorReads.unfilteredsMaterialized();
        assertRegularPushdownDropsRows(w.cfs, w.strictFiltered(f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L))));
        // the allocation property the byte comparison cannot see: abandoned rows never reach the
        // materialized output — only the one kept row lands, per cursor execution of the
        // differential run. (Abandoned rows may still do bounded partial work first — that is
        // the plan's priced cost of late resolution, measured by the allocation gate, and it
        // never inflates this counter because abandoned rows are discarded before emission.)
        assertEquals("abandoned rows must not reach the materialized output",
                     2, CursorReads.unfilteredsMaterialized() - materializedBefore);
        assertScanMetricsParity(w.cfs, w.strictFiltered(f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L))), true);
    }

    @Test
    public void regularFilterMixedKeepAndDrop() throws Throwable
    {
        Workload w = loadMixedWorkload();
        // v1 < 320 keeps the surviving overwritten/cell-carrying rows of the first half of the
        // main range; the drop set mixes live mismatches, dead v1 winners and shadowed v1 winners
        Consumer<RowFilter> filter = f -> f.add(w.col("v1"), Operator.LT, ByteBufferUtil.bytes(320L));
        assertRegularPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    @Test
    public void slicedReadWithRegularFilter() throws Throwable
    {
        // sub-slice read: BIG legs walk from the partition head, so rows are merged — and
        // abandoned — BOTH before the slice start (those reach the metrics stage on neither
        // path and must contribute nothing, the onEmittedSurface=false arm of abandonment) and
        // inside the emitted surface; the slicer's artificial-bound simulation must stay exact
        // around abandoned rows
        Workload w = loadMixedWorkload();
        // v1 = 220 keeps exactly ck = 22 (in-slice, live-overwritten); ck 0..7 are merged before
        // the slice start and abandoned off-surface
        Consumer<RowFilter> filter = f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(220L));
        assertRegularPushdownDropsRows(w.cfs, w.slicedStrictFiltered(8L, 47L, filter));
        assertScanMetricsParity(w.cfs, w.slicedStrictFiltered(8L, 47L, filter), true);
    }

    // ---------------------------------------------------------------- memtable-won evaluation

    @Test
    public void memtableWonFilterCells() throws Throwable
    {
        // the second source stays UNFLUSHED: the winning v1 cells of the overwritten rows come
        // from the object-backed memtable leg, so the filter verdict evaluates
        // existingCell().buffer() directly (no staging, no windowing) — and the M2.3 cell escape
        // hatch must keep firing for kept rows
        Workload w = loadMixedWorkload(false);
        Consumer<RowFilter> filter = f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L));
        long cellsReusedBefore = CursorReads.memtableCellsReused();
        assertRegularPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        assertTrue("kept rows' memtable-won cells must still emit through the cell escape hatch",
                   CursorReads.memtableCellsReused() > cellsReusedBefore);
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    @Test
    public void memtableOnlyRowsThroughEscapeHatch() throws Throwable
    {
        // rows ck 200..207 exist ONLY in the memtable leg: their single-contributor groups take
        // the M2.3 whole-row escape hatch, so the regular-column verdict runs the REAL
        // Expression.isSatisfiedBy on the live row object — kept hatch rows must still be emitted
        // via row reuse, failing ones must be dropped with full accounting
        Workload w = loadMixedWorkload(false);
        for (long ck = 200; ck < 208; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck % 2 == 0 ? 20L : 21L);
        Consumer<RowFilter> filter = f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L));
        long rowsReusedBefore = CursorReads.memtableRowsReused();
        assertRegularPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        assertTrue("kept memtable-only rows must still be emitted through the whole-row escape hatch",
                   CursorReads.memtableRowsReused() > rowsReusedBefore);
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    // ---------------------------------------------------------------- tie-fed verdicts

    @Test
    public void exactTimestampValueTieOnFilterColumn() throws Throwable
    {
        // both legs write v1 for the same rows at the SAME timestamp with different values — a
        // full-metadata tie (CellResolution.COMPARE) resolved by value comparison, with the
        // greater-value leg alternating per row so neither leg order nor flush order can fake
        // stability. The filter verdict must consume the TIE WINNER's bytes on both paths:
        // EQ greater-value keeps every row, EQ loser-value keeps none.
        Workload w = loadTieWorkload();
        Consumer<RowFilter> keepAll = f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(200L));
        long droppedBefore = CursorReads.rowsDroppedByRegularColumnFilter();
        assertCursorReadMatchesIterator(w.cfs, w.strictFiltered(keepAll));
        assertEquals("EQ tie-winner value must keep every row — a drop here means the verdict " +
                     "consumed a tie LOSER's bytes",
                     droppedBefore, CursorReads.rowsDroppedByRegularColumnFilter());
        assertScanMetricsParity(w.cfs, w.strictFiltered(keepAll), false);

        Consumer<RowFilter> dropAll = f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(100L));
        assertRegularPushdownDropsRows(w.cfs, w.strictFiltered(dropAll));
        assertScanMetricsParity(w.cfs, w.strictFiltered(dropAll), true);
    }

    // ---------------------------------------------------------------- late resolution

    @Test
    public void lateResolvingFilterColumnAfterEarlierCellWork() throws Throwable
    {
        // THE genuinely new wrinkle of M3.2c: the filter column (z1) sorts LAST in the cell
        // order, so the a1/a2 cells of every row are merged — and for started rows, emitted into
        // the row builder — before the z1 winner resolves and fails. The abandoned rows' earlier
        // cell work is bounded wasted allocation; their accounting must still classify EXACTLY
        // like a pre-cell rejection (the probe replays the already-emitted content), which the
        // metrics parity here proves against the iterator path's real MetricRecording
        Workload w = loadLateResolutionWorkload();
        Consumer<RowFilter> filter = f -> f.add(w.col("z1"), Operator.EQ, ByteBufferUtil.bytes(7L));
        assertRegularPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    @Test
    public void filterColumnAbsentFromSomeRows() throws Throwable
    {
        // rows ck 32..39 never wrote z1 at all: their groups merge COMPLETELY (every cell
        // emitted) before the absence is knowable, and only the row-end AND-semantics check can
        // abandon them — getValue returns null for the absent cell, so the top filter drops them
        // identically. z1 = 7 exists (ck = 7), so kept and late-absence-dropped rows coexist.
        Workload w = loadLateResolutionWorkload();
        Consumer<RowFilter> filter = f -> f.add(w.col("z1"), Operator.GTE, ByteBufferUtil.bytes(0L));
        // GTE 0 passes every row that HAS z1 — the entire drop set is the absence rows
        assertRegularPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    // ---------------------------------------------------------------- combined with clustering

    @Test
    public void combinedClusteringAndRegularFilters() throws Throwable
    {
        // AND semantics across both row-level families in ONE query: rows ck >= 32 fail the
        // CLUSTERING expression first (rejected at group formation — the regular expression is
        // never evaluated for them, exactly like the top filter's short-circuit ordering), while
        // rows ck < 32 pass clustering and most then fail the REGULAR expression at winner
        // resolution; ck = 2 alone passes both. Accounting must be exact for BOTH drop shapes.
        Workload w = loadMixedWorkload();
        Consumer<RowFilter> filter = f -> {
            f.add(w.col("ck"), Operator.LT, ByteBufferUtil.bytes(32L));
            f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L));
        };
        assertRegularPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    @Test
    public void combinedFiltersRegularFailsFirstOrdering() throws Throwable
    {
        // the reverse elimination ordering: the CLUSTERING expression passes EVERY row (ck < 1000)
        // so the whole drop set is decided by the regular expression — proving the clustering
        // pass-through does not short-circuit the regular verdict or its accounting
        Workload w = loadMixedWorkload();
        Consumer<RowFilter> filter = f -> {
            f.add(w.col("ck"), Operator.LT, ByteBufferUtil.bytes(1000L));
            f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L));
        };
        assertRegularPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    // ---------------------------------------------------------------- thresholds via dropped rows

    @Test
    public void warnThresholdCrossedOnlyByRegularDroppedRowTombstones() throws Throwable
    {
        Workload w = loadMixedWorkload();
        int originalWarn = DatabaseDescriptor.getTombstoneWarnThreshold();
        DatabaseDescriptor.setTombstoneWarnThreshold(10);
        try
        {
            // the kept row (ck = 2, v1 = 20) carries ZERO tombstones: the warning — and its
            // exact-count text — exists purely because of the regular-abandoned rows' tombstones
            Supplier<SinglePartitionReadCommand> cmd =
                w.strictFiltered(f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L)));
            ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(w.cfs, cmd, true);
            assertEquals("warn threshold must have tripped exactly once", 1, iterator.tombstoneWarnings);
            assertTrue("warning text (with exact live/tombstone counts) must be captured",
                       !iterator.clientWarnings.isEmpty());
            assertTrue("warning text must embed the full scanned counts, abandoned rows included: "
                       + iterator.clientWarnings,
                       iterator.clientWarnings.get(0).contains(TOTAL_TOMBSTONES + " tombstone cells"));
        }
        finally
        {
            DatabaseDescriptor.setTombstoneWarnThreshold(originalWarn);
        }
    }

    @Test
    public void abortThresholdCrossedOnlyByRegularDroppedRowTombstones() throws Throwable
    {
        Workload w = loadMixedWorkload();
        int originalFail = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneFailureThreshold(10);
        try
        {
            // production-time abort twin under regular abandonment: the crossing happens entirely
            // inside abandoned rows, so the cursor path must throw at merge time with the SAME
            // message (count and last-scanned clustering) and the same metric side effects
            Supplier<SinglePartitionReadCommand> cmd =
                w.strictFiltered(f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L)));
            ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(w.cfs, cmd, true);
            assertTrue("read must have aborted with TombstoneOverwhelmingException", iterator.aborted());
            assertEquals("abort must have been metered exactly once", 1, iterator.tombstoneFailures);
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalFail);
        }
    }

    // ---------------------------------------------------------------- M3.2d: limit composition

    @Test
    public void limitedQueryEngagesRegularPushdownWhenFilterIsFullyPushable() throws Throwable
    {
        Workload w = loadMixedWorkload();
        // M3.2d replaces M3.2b/c's limits().isUnlimited() gate with cooperation: a fully pushable
        // filter under a CQL_LIMIT now engages row-level pushdown too. v1 = 20 matches exactly
        // ONE row in the whole 64-row partition (ck = 2, per loadMixedWorkload's documented v1
        // landscape) — far short of LIMIT 10's requirement, so the top counter is NEVER satisfied
        // and the merge must scan the ENTIRE partition (mergesStoppedByLimit must NOT advance,
        // exactly like the iterator path's counter, which also never stops early when the real
        // population can't fill the limit). What DOES change from the pre-M3.2d behavior is that
        // row-level pushdown is no longer categorically disengaged just because a limit is
        // present: every one of the ~63 non-matching rows is now dropped/abandoned at production
        // instead of riding along to the top filter unfiltered. The scenario where the limit DOES
        // get satisfied (partway through a mix of dropped and kept rows) lives in
        // LimitedFilterPushdownDifferentialTest, along with the exact-landing-on-a-dropped-row
        // edge case.
        Consumer<RowFilter> filter = f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L));
        long limitStoppedBefore = CursorReads.mergesStoppedByLimit();
        assertRegularPushdownDropsRows(w.cfs, w.limitFiltered(10, filter));
        assertEquals("the limit can never be satisfied by a single-match filter over a 64-row " +
                     "partition with LIMIT 10, so the bound must not have stopped the merge early",
                     limitStoppedBefore, CursorReads.mergesStoppedByLimit());
        assertScanMetricsParity(w.cfs, w.limitFiltered(10, filter), true);
    }

    @Test
    public void multiCellContainsStillFallsBack() throws Throwable
    {
        // M3.2a's gate exclusions must survive M3.2c untouched: SIMPLE kind on a COMPLEX column
        // (multi-cell set CONTAINS needs a materialized ComplexColumnData) disengages the whole
        // pushdown — the query is still cursor-served, filtered entirely up top
        Workload w = loadComplexColumnWorkload();
        Consumer<RowFilter> filter = f -> f.add(w.col("tags"), Operator.CONTAINS, ByteBufferUtil.bytes("common"));
        long engagedBefore = CursorReads.filterPushdownEngaged();
        assertServedWithoutRowDrops(w.cfs, w.strictFiltered(filter));
        assertEquals("pushdown must not engage on a multi-cell CONTAINS filter",
                     engagedBefore, CursorReads.filterPushdownEngaged());
    }

    @Test
    public void complexMapElementStillFallsBack() throws Throwable
    {
        // MAP_ELEMENT kind (by-path complex cell lookup) — not Kind.SIMPLE, unpushable, unchanged
        Workload w = loadComplexColumnWorkload();
        Consumer<RowFilter> filter = f -> f.addMapEquality(w.col("m"), ByteBufferUtil.bytes("stable"),
                                                           Operator.EQ, ByteBufferUtil.bytes("x"));
        long engagedBefore = CursorReads.filterPushdownEngaged();
        assertServedWithoutRowDrops(w.cfs, w.strictFiltered(filter));
        assertEquals("pushdown must not engage on a complex MAP_ELEMENT filter",
                     engagedBefore, CursorReads.filterPushdownEngaged());
    }

    // ---------------------------------------------------------------- negative control

    @Test
    public void parityHarnessDetectsBrokenAbandonedRowAccounting() throws Throwable
    {
        // the M3.2b negative control re-armed against the ABANDONMENT path: the skew hook
        // silently skips the dropped rows' tombstone accounting, and the parity harness must
        // fail on it — otherwise every regular-pushdown parity scenario above is vacuous
        Workload w = loadMixedWorkload();
        Supplier<SinglePartitionReadCommand> cmd =
            w.strictFiltered(f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(20L)));

        DatabaseDescriptor.setCursorReadsEnabled(false);
        ScanMetricsCapture.Snapshot iterator = ScanMetricsCapture.capture(w.cfs, () -> consume(cmd.get()));

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING = true;
            long droppedBefore = CursorReads.rowsDroppedByRegularColumnFilter();
            ScanMetricsCapture.Snapshot cursor = ScanMetricsCapture.capture(w.cfs, () -> consume(cmd.get()));
            assertTrue("skew hook proved nothing: no rows were abandoned at production",
                       CursorReads.rowsDroppedByRegularColumnFilter() > droppedBefore);
            try
            {
                ScanMetricsCapture.assertParity("negative control: silently skipped abandoned-row tombstones",
                                                iterator, cursor);
            }
            catch (AssertionError expected)
            {
                return;
            }
            fail("ScanMetricsCapture accepted a cursor run whose abandoned-row tombstone accounting " +
                 "was silently skipped — the M3.2c parity scenarios are vacuous");
        }
        finally
        {
            CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING = false;
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    // ---------------------------------------------------------------- guards

    /** Byte identity + the full engagement guard set for a regular-column filter that DROPS rows:
     *  context attached, regular-column abandonments happened, the partition-level short-circuit
     *  did not fire. */
    private void assertRegularPushdownDropsRows(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long engagedBefore = CursorReads.filterPushdownEngaged();
        long droppedBefore = CursorReads.rowsDroppedByFilter();
        long regularDroppedBefore = CursorReads.rowsDroppedByRegularColumnFilter();
        long partitionsSkippedBefore = CursorReads.partitionsSkippedByFilter();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertTrue("filter pushdown did not engage for a pushable regular-column filter (silent non-engagement)",
                   CursorReads.filterPushdownEngaged() > engagedBefore);
        assertTrue("regular-column pushdown abandoned no rows — a silently non-evaluating verdict is " +
                   "byte-identical (the top filter still drops the rows) and fails exactly this",
                   CursorReads.rowsDroppedByRegularColumnFilter() > regularDroppedBefore);
        assertTrue("regular abandonments must also count as filter-dropped rows",
                   CursorReads.rowsDroppedByFilter() > droppedBefore);
        assertEquals("partition-level short-circuit fired for a partition the filter keeps",
                     partitionsSkippedBefore, CursorReads.partitionsSkippedByFilter());
    }

    /** Byte identity + the non-engagement guard: the query is cursor-served but NO row is
     *  dropped at production (neither clustering nor regular). */
    private void assertServedWithoutRowDrops(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long droppedBefore = CursorReads.rowsDroppedByFilter();
        long regularDroppedBefore = CursorReads.rowsDroppedByRegularColumnFilter();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertEquals("row-level pushdown dropped rows for a shape that must not engage",
                     droppedBefore, CursorReads.rowsDroppedByFilter());
        assertEquals("regular-column pushdown abandoned rows for a shape that must not engage",
                     regularDroppedBefore, CursorReads.rowsDroppedByRegularColumnFilter());
    }

    /**
     * Scan-metrics parity between the paths (with the standard silent-fallback guards), returning
     * the iterator-path snapshot for extra assertions.
     *
     * @param expectDroppedRows whether the cursor capture must have abandoned rows at production
     *                          (the accounting guard) or must NOT have (the non-engagement guard)
     */
    private ScanMetricsCapture.Snapshot assertScanMetricsParity(ColumnFamilyStore cfs,
                                                                Supplier<SinglePartitionReadCommand> cmd,
                                                                boolean expectDroppedRows)
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        ScanMetricsCapture.Snapshot iterator = ScanMetricsCapture.capture(cfs, () -> consume(cmd.get()));

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = cmd.get();
            assertTrue("scenario is not supported by the cursor read gate; this parity run would " +
                       "silently compare iterator vs iterator",
                       CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));
            long servedBefore = CursorReads.sstableLegsServed();
            long regularDroppedBefore = CursorReads.rowsDroppedByRegularColumnFilter();
            ScanMetricsCapture.Snapshot cursor = ScanMetricsCapture.capture(cfs, () -> consume(cmd.get()));
            assertTrue("cursor path did not actually serve any sstable leg (silent fallback?)",
                       CursorReads.sstableLegsServed() - servedBefore > 0);
            if (expectDroppedRows)
                assertTrue("cursor capture abandoned no rows at production; parity proved nothing",
                           CursorReads.rowsDroppedByRegularColumnFilter() > regularDroppedBefore);
            else
                assertEquals("cursor capture abandoned rows for a shape that must not engage",
                             regularDroppedBefore, CursorReads.rowsDroppedByRegularColumnFilter());

            ScanMetricsCapture.assertParity("iterator vs cursor, regular-column filter pushdown", iterator, cursor);
            return iterator;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** Full executeLocally consumption — metric recording happens partly at iteration, partly at
     *  close, and the production-time abort twin fires at iterator creation. */
    private void consume(SinglePartitionReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
        }
    }

    // ---------------------------------------------------------------- workload

    protected final class Workload
    {
        final ColumnFamilyStore cfs;
        final long nowInSec;

        Workload(ColumnFamilyStore cfs, long nowInSec)
        {
            this.cfs = cfs;
            this.nowInSec = nowInSec;
        }

        ColumnMetadata col(String name)
        {
            ColumnMetadata column = cfs.metadata().getColumn(ByteBufferUtil.bytes(name));
            assertTrue("no such column: " + name, column != null);
            return column;
        }

        /** A pk=0 read carrying a STRICT, reconciliation-free filter and no limit (the shape
         *  row-level pushdown engages for). */
        Supplier<SinglePartitionReadCommand> strictFiltered(Consumer<RowFilter> expressions)
        {
            return build(DataLimits.NONE, expressions);
        }

        /** The same strict filter under a CQL LIMIT — since M3.2d, row-level pushdown ENGAGES for
         *  this shape when the filter is fully pushable (cooperating with the limit bound instead
         *  of requiring its absence); see {@code limitedQueryEngagesRegularPushdownWhenFilterIsFullyPushable}. */
        Supplier<SinglePartitionReadCommand> limitFiltered(int limit, Consumer<RowFilter> expressions)
        {
            return build(DataLimits.cqlLimits(limit), null, null, expressions);
        }

        /** A strict filtered read over a sub-slice [from, to] of the partition. */
        Supplier<SinglePartitionReadCommand> slicedStrictFiltered(long from, long to, Consumer<RowFilter> expressions)
        {
            return build(DataLimits.NONE, from, to, expressions);
        }

        private Supplier<SinglePartitionReadCommand> build(DataLimits limits, Consumer<RowFilter> expressions)
        {
            return build(limits, null, null, expressions);
        }

        private Supplier<SinglePartitionReadCommand> build(DataLimits limits, Long from, Long to,
                                                           Consumer<RowFilter> expressions)
        {
            return () -> {
                SinglePartitionReadCommand base;
                if (from != null)
                    base = (SinglePartitionReadCommand) Util.cmd(cfs, 0L).fromIncl(from).toIncl(to)
                                                            .withNowInSeconds(nowInSec).build();
                else
                    base = (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(nowInSec).build();
                RowFilter rowFilter = RowFilter.create(false);
                expressions.accept(rowFilter);
                return SinglePartitionReadCommand.create(cfs.metadata(), base.nowInSec(),
                                                         ColumnFilter.all(cfs.metadata()), rowFilter,
                                                         limits, base.partitionKey(),
                                                         base.clusteringIndexFilter());
            };
        }
    }

    private Workload loadMixedWorkload() throws Throwable
    {
        return loadMixedWorkload(true);
    }

    /**
     * The M3.2b mixed workload, verbatim (same known scan-metric inventory:
     * {@value #TOTAL_LIVE} live rows, {@value #TOTAL_TOMBSTONES} tombstones), now filtered on the
     * REGULAR column v1 instead of the clustering key. The v1 landscape after the merge:
     * <ul>
     *   <li>overwritten live rows (ck % 4 in {2, 3}): v1 = ck * 10 — the only EQ-20 match is ck = 2;</li>
     *   <li>cell-tombstoned rows (ck % 4 == 1): the v1 winner is a TOMBSTONE (getValue null — dropped);</li>
     *   <li>row-tombstoned rows (ck % 4 == 0): the v1 winner is SHADOWED (no cell — dropped);</li>
     *   <li>dead rows (ck 100..107): no PK liveness, dead v1 winner (dropped).</li>
     * </ul>
     *
     * @param flushSecond when false the leg-1 writes stay in the memtable (the M2.3 adapter joins
     *                    the merge; overwritten rows' winning v1 cells are memtable-won)
     */
    private Workload loadMixedWorkload(boolean flushSecond) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck * 10, "r0-" + ck);
        for (long ck = 100; ck < 100 + DEAD_ROWS; ck++)
            execute("UPDATE %s SET v1 = ? WHERE pk = 0 AND ck = ?", ck, ck);
        flush();

        for (long ck = 0; ck < ROWS; ck++)
        {
            if (ck % 4 == 0)
                execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", ck);
            else if (ck % 4 == 1)
                execute("DELETE v1 FROM %s WHERE pk = 0 AND ck = ?", ck);
            else
                execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck * 10);
        }
        for (long ck = 100; ck < 100 + DEAD_ROWS; ck++)
            execute("DELETE v1 FROM %s WHERE pk = 0 AND ck = ?", ck);
        if (flushSecond)
        {
            flush();
            assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());
        }
        else
        {
            assertEquals("expected exactly 1 sstable under the memtable", 1, cfs.getLiveSSTables().size());
            assertTrue("leg-1 writes must still live in the memtable",
                       cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize() > 0);
        }

        // workload-shape sanity against the documented inventory
        assertEquals("live-row inventory drifted", TOTAL_LIVE, execute("SELECT ck FROM %s WHERE pk = 0").size());
        assertEquals("EQ-20 keep-set drifted", 1,
                     execute("SELECT ck FROM %s WHERE pk = 0 AND v1 = 20 ALLOW FILTERING").size());

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }

    /**
     * Exact-timestamp value ties on the filter column: both legs write v1 for ck 0..7 at
     * TIMESTAMP 1000 with values {100, 200}, the greater value alternating between legs per row —
     * so the tie winner is always 200 but comes from leg 0 for even cks and leg 1 for odd ones.
     */
    private Workload loadTieWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?) USING TIMESTAMP 1000",
                    ck, ck % 2 == 0 ? 200L : 100L);
        flush();
        for (long ck = 0; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?) USING TIMESTAMP 1000",
                    ck, ck % 2 == 0 ? 100L : 200L);
        flush();
        assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());

        // tie-resolution sanity: the greater value must win every row on the reference path
        assertEquals("tie winners drifted", 8,
                     execute("SELECT ck FROM %s WHERE pk = 0 AND v1 = 200 ALLOW FILTERING").size());

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }

    /**
     * The late-resolution workload: the filter column z1 sorts LAST (a1 < a2 < z1 in column
     * order), so every row's a1/a2 cell groups are merged before the z1 verdict can resolve.
     * Rows ck 0..31 carry all three columns (z1 = ck, overwritten across both legs so multiple
     * contributors reach the winner resolution); rows ck 32..39 never write z1 at all — their
     * failure is only knowable at row end, after the entire row has merged.
     */
    private Workload loadLateResolutionWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, a1 bigint, a2 bigint, z1 bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 32; ck++)
            execute("INSERT INTO %s (pk, ck, a1, a2, z1) VALUES (0, ?, ?, ?, ?)", ck, ck, ck * 2, ck);
        for (long ck = 32; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, a1, a2) VALUES (0, ?, ?, ?)", ck, ck, ck * 2);
        flush();
        for (long ck = 0; ck < 32; ck++)
            execute("INSERT INTO %s (pk, ck, a1, z1) VALUES (0, ?, ?, ?)", ck, ck + 1, ck);
        for (long ck = 32; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, a1) VALUES (0, ?, ?)", ck, ck + 1);
        flush();
        assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());

        // shape sanity: exactly one EQ-7 match, and the absence rows really lack z1
        assertEquals("EQ-7 keep-set drifted", 1,
                     execute("SELECT ck FROM %s WHERE pk = 0 AND z1 = 7 ALLOW FILTERING").size());
        assertEquals("absence-row inventory drifted", 32,
                     execute("SELECT ck FROM %s WHERE pk = 0 AND z1 >= 0 ALLOW FILTERING").size());

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }

    /** Complex columns (multi-cell set + map) for the mandatory-fallback shapes, two overlapping
     *  sources so the merged path would engage if the gate ever (wrongly) let these through. */
    private Workload loadComplexColumnWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, tags set<text>, m map<text, text>, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 16; ck++)
            execute("INSERT INTO %s (pk, ck, v1, tags, m) VALUES (0, ?, ?, {'common', 'r0'}, {'stable': 'x'})",
                    ck, ck * 10);
        flush();
        for (long ck = 0; ck < 16; ck++)
            execute("UPDATE %s SET tags = tags + {'r1'}, m['extra'] = 'y' WHERE pk = 0 AND ck = ?", ck);
        flush();
        assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }
}
