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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;

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
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * M3.2b (CASSANDRA-20428): differential + scan-metrics-parity scenarios for CLUSTERING-column
 * filter pushdown — row groups whose clustering fails a gate-approved expression are dropped at
 * production inside the cursor merge (never materialized), while every scan metric, threshold,
 * warning text and abort the iterator path's below-the-filter {@code MetricRecording} would have
 * recorded for them is preserved through the {@code ScanStatsAccumulator} apparatus.
 *
 * Scenario families:
 * <ul>
 *   <li><b>Byte identity + engagement guards</b>: clustering filters that keep some rows and drop
 *       others must produce byte-identical results with {@code rowsDroppedByFilter} advancing (a
 *       silently non-evaluating probe is byte-identical — the top filter still drops the rows —
 *       and fails exactly this) and near-zero materialization of the dropped rows.</li>
 *   <li><b>Scan-metrics parity</b> ({@link ScanMetricsCapture}): histograms/totalRowsRead, the
 *       warn threshold crossed ONLY via dropped-row tombstones (exact-count warning text), the
 *       abort threshold crossed ONLY via dropped-row tombstones (identical
 *       {@code TombstoneOverwhelmingException} message and metric deltas — the production-time
 *       abort twin), and gcable tombstones inside dropped rows under both
 *       {@code only_purge_repaired_tombstones} settings (the gcable-purge twin).</li>
 *   <li><b>LIMIT composition (M3.2d)</b>: a LIMITed query with a fully pushable filter now ENGAGES
 *       row-level pushdown too, cooperating with the production LIMIT bound instead of requiring
 *       its absence — {@link #limitedQueryEngagesClusteringPushdownWhenFilterIsFullyPushable}
 *       proves the merge stops at the limit's SATISFACTION point without ever over-scanning into
 *       filter-droppable territory it does not need to reach. The full composition matrix (limit
 *       satisfied only after passing dropped rows, LIMIT landing exactly on a dropped row,
 *       tombstone-threshold-crossing dropped rows under a LIMIT, unpushable filters keeping the
 *       limit bound disengaged) lives in the dedicated
 *       {@code LimitedFilterPushdownDifferentialTest}. (Regular-column expressions originally rode
 *       along without dropping here; since M3.2c they drop at production too — see
 *       {@code RegularColumnFilterPushdownDifferentialTest} for their dedicated coverage.)</li>
 *   <li><b>Negative control</b>: {@code TEST_SKEW_DROPPED_ROW_ACCOUNTING} silently skips the
 *       dropped rows' tombstone accounting; the parity harness MUST fail on it, or every parity
 *       scenario above is vacuous.</li>
 *   <li><b>Samplers</b>: the per-partition top-K samplers (not captured by
 *       {@code ScanMetricsCapture}) fold the dropped rows in — verified by targeted comparison of
 *       {@code finishSampling} outputs between paths.</li>
 * </ul>
 */
public class ClusteringFilterPushdownDifferentialTest extends CursorReadDifferentialTester
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
    public void clusteringFilterKeepsOneRowDropsRest() throws Throwable
    {
        Workload w = loadMixedWorkload();
        // ck = 2: one live, tombstone-free row kept; all 79 other rows (live, cell-tombstoned,
        // PK-deleted and dead alike) dropped at production
        long materializedBefore = CursorReads.unfilteredsMaterialized();
        assertClusteringPushdownDropsRows(w.cfs, w.strictFiltered(f -> f.add(w.col("ck"), Operator.EQ, ByteBufferUtil.bytes(2L))));
        // the allocation property that IS the pushdown's payoff, invisible to the byte
        // comparison: the two cursor executions of the differential run materialize ONLY the one
        // kept row each — none of the 79 dropped rows
        assertEquals("dropped rows must not be materialized",
                     2, CursorReads.unfilteredsMaterialized() - materializedBefore);
        assertScanMetricsParity(w.cfs, w.strictFiltered(f -> f.add(w.col("ck"), Operator.EQ, ByteBufferUtil.bytes(2L))), true);
    }

    @Test
    public void clusteringFilterMixedKeepAndDrop() throws Throwable
    {
        Workload w = loadMixedWorkload();
        // ck < 32 keeps the first half of the main range; dropped rows (32..63, 100..107) are a
        // mix of live rows, cell-tombstoned live rows, PK-deletion-only rows and fully-dead rows
        Consumer<RowFilter> filter = f -> f.add(w.col("ck"), Operator.LT, ByteBufferUtil.bytes(32L));
        assertClusteringPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    @Test
    public void clusteringFilterWithMemtableLeg() throws Throwable
    {
        // the second source stays UNFLUSHED: dropped groups walk the object-backed memtable
        // adapter's metadata surface through the same accounting path as byte-backed legs
        Workload w = loadMixedWorkload(false);
        Consumer<RowFilter> filter = f -> f.add(w.col("ck"), Operator.LT, ByteBufferUtil.bytes(32L));
        assertClusteringPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    @Test
    public void slicedReadWithClusteringFilterAndRangeTombstones() throws Throwable
    {
        // sub-slice read + range tombstones + clustering filter: dropped rows exist BOTH before
        // the slice start (BIG walks from the partition head — those reach the metrics stage on
        // neither path and must contribute nothing) and inside the emitted surface, interleaved
        // with real range-tombstone markers and the slicer's artificial slice-bound markers
        Workload w = loadRangeTombstoneWorkload();
        Consumer<RowFilter> filter = f -> f.add(w.col("ck"), Operator.LT, ByteBufferUtil.bytes(24L));
        assertClusteringPushdownDropsRows(w.cfs, w.slicedStrictFiltered(8L, 47L, filter));
        assertScanMetricsParity(w.cfs, w.slicedStrictFiltered(8L, 47L, filter), true);
    }

    @Test
    public void abortAmidRangeTombstonesAndDroppedRows() throws Throwable
    {
        // the production-time abort twin's counting ORDER: markers (real and artificial,
        // purge-twinned) interleave with dropped rows in stream order, so the crossing element —
        // embedded verbatim in the exception message — must match the iterator path's exactly
        Workload w = loadRangeTombstoneWorkload();
        int originalFail = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneFailureThreshold(6);
        try
        {
            Consumer<RowFilter> filter = f -> f.add(w.col("ck"), Operator.LT, ByteBufferUtil.bytes(24L));
            ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(w.cfs, w.slicedStrictFiltered(8L, 47L, filter), true);
            assertTrue("read must have aborted with TombstoneOverwhelmingException", iterator.aborted());
            assertEquals("abort must have been metered exactly once", 1, iterator.tombstoneFailures);
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalFail);
        }
    }

    // ---------------------------------------------------------------- thresholds via dropped rows

    @Test
    public void warnThresholdCrossedOnlyByDroppedRowTombstones() throws Throwable
    {
        Workload w = loadMixedWorkload();
        int originalWarn = DatabaseDescriptor.getTombstoneWarnThreshold();
        DatabaseDescriptor.setTombstoneWarnThreshold(10);
        try
        {
            // the kept row (ck = 2) carries ZERO tombstones: the warning — and its exact-count
            // text — exists purely because of the dropped rows' contributions
            Supplier<SinglePartitionReadCommand> cmd =
                w.strictFiltered(f -> f.add(w.col("ck"), Operator.EQ, ByteBufferUtil.bytes(2L)));
            ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(w.cfs, cmd, true);
            assertEquals("warn threshold must have tripped exactly once", 1, iterator.tombstoneWarnings);
            assertTrue("warning text (with exact live/tombstone counts) must be captured",
                       !iterator.clientWarnings.isEmpty());
            assertTrue("warning text must embed the full scanned counts, dropped rows included: "
                       + iterator.clientWarnings,
                       iterator.clientWarnings.get(0).contains(TOTAL_TOMBSTONES + " tombstone cells"));
        }
        finally
        {
            DatabaseDescriptor.setTombstoneWarnThreshold(originalWarn);
        }
    }

    @Test
    public void abortThresholdCrossedOnlyByDroppedRowTombstones() throws Throwable
    {
        Workload w = loadMixedWorkload();
        int originalFail = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneFailureThreshold(10);
        try
        {
            // production-time abort twin: the crossing happens entirely inside dropped rows, so
            // the cursor path must throw at merge time with the SAME message (count and
            // last-scanned clustering) and the same metric side effects the iterator path's
            // pull-side abort produces
            Supplier<SinglePartitionReadCommand> cmd =
                w.strictFiltered(f -> f.add(w.col("ck"), Operator.EQ, ByteBufferUtil.bytes(2L)));
            ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(w.cfs, cmd, true);
            assertTrue("read must have aborted with TombstoneOverwhelmingException", iterator.aborted());
            assertEquals("abort must have been metered exactly once", 1, iterator.tombstoneFailures);
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalFail);
        }
    }

    // ---------------------------------------------------------------- gcable-purge twin

    @Test
    public void gcableTombstonesInDroppedRowsArePurgedFromCounts() throws Throwable
    {
        // gc_grace_seconds = 0 and a query pinned an hour in the future: every tombstone in the
        // dropped rows is gcable, withoutPurgeableTombstones drops them BELOW the metrics stage
        // on the iterator path, and the accounting's purge twin must reach the identical counts
        Workload w = loadGcableWorkload("");
        Consumer<RowFilter> filter = f -> f.add(w.col("ck"), Operator.LT, ByteBufferUtil.bytes(32L));
        assertClusteringPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
        // a single histogram update of 0 quantizes to max=1 (EstimatedHistogram bucket ceiling —
        // the same quantization ScanMetricsCapture documents), so "everything purged" is max <= 1
        assertTrue("workload drift: every tombstone must have purged below the metrics stage, saw "
                   + iterator.describe(),
                   iterator.tombstonesMax <= 1);
    }

    @Test
    public void gcableTombstonesInDroppedRowsUnderOnlyPurgeRepaired() throws Throwable
    {
        // same shape, but only_purge_repaired_tombstones = true and nothing is repaired: the
        // purge is blocked by the oldestUnrepairedTombstone bound, so the same tombstones now DO
        // count — proving the accounting twins the full purge predicate, not just gcBefore
        Workload w = loadGcableWorkload(" AND compaction = {'class': 'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones': 'true'}");
        Consumer<RowFilter> filter = f -> f.add(w.col("ck"), Operator.LT, ByteBufferUtil.bytes(32L));
        assertClusteringPushdownDropsRows(w.cfs, w.strictFiltered(filter));
        ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
        assertTrue("workload drift: unrepaired tombstones must NOT purge under only_purge_repaired_tombstones, saw "
                   + iterator.describe(),
                   iterator.tombstonesMax > 1);
    }

    // ---------------------------------------------------------------- deliberate non-engagement

    @Test
    public void regularColumnFilterNowDropsAtProduction() throws Throwable
    {
        // M3.2b asserted this shape rode along WITHOUT dropping ("M3.2c's scope, correctly not
        // touched"); M3.2c landed that scope, so the same strict regular-column expression now
        // drops rows at production — full byte-identity, drop-guard and metrics-parity coverage
        // lives in RegularColumnFilterPushdownDifferentialTest, this keeps the original scenario
        // shape pinned in the M3.2b suite
        Workload w = loadMixedWorkload();
        Consumer<RowFilter> filter = f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(70L));
        long engagedBefore = CursorReads.filterPushdownEngaged();
        long regularDroppedBefore = CursorReads.rowsDroppedByRegularColumnFilter();
        assertCursorReadMatchesIterator(w.cfs, w.strictFiltered(filter));
        assertTrue("pushdown context must still attach for a pushable regular-column filter",
                   CursorReads.filterPushdownEngaged() > engagedBefore);
        assertTrue("M3.2c: a strict regular-column filter must now drop rows at production",
                   CursorReads.rowsDroppedByRegularColumnFilter() > regularDroppedBefore);
        assertScanMetricsParity(w.cfs, w.strictFiltered(filter), true);
    }

    @Test
    public void limitedQueryEngagesClusteringPushdownWhenFilterIsFullyPushable() throws Throwable
    {
        Workload w = loadMixedWorkload();
        // M3.2d: this filter (ck < 32) is fully pushable, so limitBoundFor no longer declines
        // just because rowFilter() is non-empty, and clustering pushdown's own
        // limits().isUnlimited() gate is replaced by cooperation with the limit bound — BOTH
        // engage together for the same query. LIMIT 10 is satisfied well inside the ck < 32
        // keep-range (roughly ck ~= 13, since every 4th row is row-tombstoned/dead and doesn't
        // count toward the limit) — comfortably short of ck = 32, where clustering pushdown would
        // otherwise have rows to drop. The merge must therefore stop (mergesStoppedByLimit
        // advances) WITHOUT ever reaching — let alone evaluating or dropping — a single ck >= 32
        // row: rowsDroppedByFilter must stay untouched here, proving the bounded merge does not
        // over-scan past where the top counter would have stopped. The dedicated scenario where
        // the limit's satisfaction point falls AFTER filter-dropped rows (so both counters must
        // advance together) lives in LimitedFilterPushdownDifferentialTest, which also covers the
        // exact-landing-on-a-dropped-row edge case this class does not need to reproduce.
        Consumer<RowFilter> filter = f -> f.add(w.col("ck"), Operator.LT, ByteBufferUtil.bytes(32L));
        long limitStoppedBefore = CursorReads.mergesStoppedByLimit();
        long droppedBefore = CursorReads.rowsDroppedByFilter();
        long engagedBefore = CursorReads.filterPushdownEngaged();
        assertCursorReadMatchesIterator(w.cfs, w.limitFiltered(10, filter));
        assertTrue("filter pushdown must engage for a pushable filter even under a LIMIT (M3.2d)",
                   CursorReads.filterPushdownEngaged() > engagedBefore);
        assertTrue("the production limit bound must engage for a filtered query whose filter is " +
                   "fully pushable (M3.2d lifts limitBoundFor's non-empty-rowFilter decline)",
                   CursorReads.mergesStoppedByLimit() > limitStoppedBefore);
        assertEquals("the bounded merge must not scan past its own limit satisfaction point into " +
                     "the filter-droppable ck >= 32 region",
                     droppedBefore, CursorReads.rowsDroppedByFilter());
        assertScanMetricsParity(w.cfs, w.limitFiltered(10, filter), false);
    }

    // ---------------------------------------------------------------- negative control

    @Test
    public void parityHarnessDetectsBrokenDroppedRowAccounting() throws Throwable
    {
        Workload w = loadMixedWorkload();
        Supplier<SinglePartitionReadCommand> cmd =
            w.strictFiltered(f -> f.add(w.col("ck"), Operator.EQ, ByteBufferUtil.bytes(2L)));

        DatabaseDescriptor.setCursorReadsEnabled(false);
        ScanMetricsCapture.Snapshot iterator = ScanMetricsCapture.capture(w.cfs, () -> consume(cmd.get()));

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING = true;
            long droppedBefore = CursorReads.rowsDroppedByFilter();
            ScanMetricsCapture.Snapshot cursor = ScanMetricsCapture.capture(w.cfs, () -> consume(cmd.get()));
            assertTrue("skew hook proved nothing: no rows were dropped at production",
                       CursorReads.rowsDroppedByFilter() > droppedBefore);
            try
            {
                ScanMetricsCapture.assertParity("negative control: silently skipped dropped-row tombstones",
                                                iterator, cursor);
            }
            catch (AssertionError expected)
            {
                return;
            }
            fail("ScanMetricsCapture accepted a cursor run whose dropped-row tombstone accounting " +
                 "was silently skipped — the M3.2b parity scenarios are vacuous");
        }
        finally
        {
            CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING = false;
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    // ---------------------------------------------------------------- samplers

    @Test
    public void samplersFoldDroppedRowsIn() throws Throwable
    {
        Workload w = loadMixedWorkload();
        Supplier<SinglePartitionReadCommand> cmd =
            w.strictFiltered(f -> f.add(w.col("ck"), Operator.EQ, ByteBufferUtil.bytes(2L)));

        DatabaseDescriptor.setCursorReadsEnabled(false);
        long[] iteratorSamples = sampledCounts(w.cfs, cmd);
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long droppedBefore = CursorReads.rowsDroppedByFilter();
            long[] cursorSamples = sampledCounts(w.cfs, cmd);
            assertTrue("cursor run did not drop rows at production; sampler parity proved nothing",
                       CursorReads.rowsDroppedByFilter() > droppedBefore);
            assertEquals("topReadPartitionRowCount sample diverged", iteratorSamples[0], cursorSamples[0]);
            assertEquals("topReadPartitionTombstoneCount sample diverged", iteratorSamples[1], cursorSamples[1]);
            // the sampled totals must be the FULL partition inventory, dropped rows included
            assertEquals("sampled live rows must include dropped rows", TOTAL_LIVE, iteratorSamples[0]);
            assertEquals("sampled tombstones must include dropped rows", TOTAL_TOMBSTONES, iteratorSamples[1]);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** Runs one read under active samplers and returns {live-row sample count, tombstone sample
     *  count} for the queried partition (0 when absent). */
    private long[] sampledCounts(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd) throws Exception
    {
        cfs.metric.topReadPartitionRowCount.beginSampling(10, 60_000);
        cfs.metric.topReadPartitionTombstoneCount.beginSampling(10, 60_000);
        consume(cmd.get());
        waitForSamplerExecutor();
        long rows = sampleCountFor(cfs.metric.topReadPartitionRowCount.finishSampling(10));
        long tombstones = sampleCountFor(cfs.metric.topReadPartitionTombstoneCount.finishSampling(10));
        return new long[]{ rows, tombstones };
    }

    private static long sampleCountFor(List<Sampler.Sample<ByteBuffer>> samples)
    {
        long total = 0;
        for (Sampler.Sample<ByteBuffer> sample : samples)
            total += sample.count;
        return total;
    }

    private static void waitForSamplerExecutor()
    {
        int waited = 0;
        while (Sampler.samplerExecutor.getPendingTaskCount() > 0)
        {
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            if (++waited > 100)
                throw new AssertionError("sampler executor not drained within timeout");
        }
    }

    // ---------------------------------------------------------------- guards

    /** Byte identity + the full engagement guard set for a clustering filter that DROPS rows:
     *  context attached, row drops happened, the partition-level short-circuit did not fire. */
    private void assertClusteringPushdownDropsRows(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long engagedBefore = CursorReads.filterPushdownEngaged();
        long droppedBefore = CursorReads.rowsDroppedByFilter();
        long partitionsSkippedBefore = CursorReads.partitionsSkippedByFilter();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertTrue("filter pushdown did not engage for a pushable clustering filter (silent non-engagement)",
                   CursorReads.filterPushdownEngaged() > engagedBefore);
        assertTrue("clustering pushdown dropped no rows — a silently non-evaluating probe is " +
                   "byte-identical (the top filter still drops the rows) and fails exactly this",
                   CursorReads.rowsDroppedByFilter() > droppedBefore);
        assertEquals("partition-level short-circuit fired for a partition the filter keeps",
                     partitionsSkippedBefore, CursorReads.partitionsSkippedByFilter());
    }

    /** Byte identity + the non-engagement guard: the query is cursor-served but NO row is
     *  dropped at production. */
    private void assertServedWithoutRowDrops(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long droppedBefore = CursorReads.rowsDroppedByFilter();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertEquals("clustering pushdown dropped rows for a shape that must not engage",
                     droppedBefore, CursorReads.rowsDroppedByFilter());
    }

    /**
     * Scan-metrics parity between the paths (with the standard silent-fallback guards), returning
     * the iterator-path snapshot for extra assertions.
     *
     * @param expectDroppedRows whether the cursor capture must have dropped rows at production
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
            long droppedBefore = CursorReads.rowsDroppedByFilter();
            ScanMetricsCapture.Snapshot cursor = ScanMetricsCapture.capture(cfs, () -> consume(cmd.get()));
            assertTrue("cursor path did not actually serve any sstable leg (silent fallback?)",
                       CursorReads.sstableLegsServed() - servedBefore > 0);
            if (expectDroppedRows)
                assertTrue("cursor capture dropped no rows at production; parity proved nothing",
                           CursorReads.rowsDroppedByFilter() > droppedBefore);
            else
                assertEquals("cursor capture dropped rows for a shape that must not engage",
                             droppedBefore, CursorReads.rowsDroppedByFilter());

            ScanMetricsCapture.assertParity("iterator vs cursor, clustering filter pushdown", iterator, cursor);
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
         *  clustering pushdown engages for). */
        Supplier<SinglePartitionReadCommand> strictFiltered(Consumer<RowFilter> expressions)
        {
            return build(DataLimits.NONE, expressions);
        }

        /** The same strict filter under a CQL LIMIT — since M3.2d, clustering pushdown ENGAGES for
         *  this shape when the filter is fully pushable (cooperating with the limit bound instead
         *  of requiring its absence); see {@code limitedQueryEngagesClusteringPushdownWhenFilterIsFullyPushable}. */
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
     * One pk=0 partition over two overlapping sources with a known scan-metric inventory
     * ({@code MetricRecording}'s classification, nothing purgeable under the default gc_grace):
     * <ul>
     *   <li>main range ck 0..{@value #ROWS}: leg 0 writes every row (v1, v2); leg 1 row-tombstones
     *       every 4th (PK-deletion-only rows: 1 tombstone each, 0 live), cell-tombstones v1 on
     *       ck % 4 == 1 (1 tombstone + 1 live each), overwrites v1 on the rest (live);</li>
     *   <li>ck 100..107: created by UPDATE in leg 0 (no PK liveness), their only cell deleted in
     *       leg 1 — DEAD rows carrying one dead cell each (1 tombstone, 0 live).</li>
     * </ul>
     * Totals: {@value #TOTAL_LIVE} live rows, {@value #TOTAL_TOMBSTONES} tombstones.
     *
     * @param flushSecond when false the leg-1 writes stay in the memtable (the M2.3 adapter joins
     *                    the merge and the accounting walk)
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

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }

    /**
     * The range-tombstone variant: the leg-0 rows of the main range plus leg-1 range deletions
     * over [20, 30) and [40, 44), row tombstones on ck % 8 == 1, and v1 overwrites elsewhere —
     * so a sliced, clustering-filtered read sees real RT markers interleaved with dropped rows,
     * plus artificial slice-bound markers when a range deletion covers a slice bound.
     */
    private Workload loadRangeTombstoneWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck * 10, "r0-" + ck);
        flush();
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 20 AND ck < 30");
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 40 AND ck < 44");
        for (long ck = 0; ck < ROWS; ck++)
        {
            if (ck % 8 == 1)
                execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", ck);
            else if (ck % 8 == 3)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck * 10);
        }
        flush();
        assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }

    /**
     * The gcable variant: gc_grace_seconds = 0 (plus any extra table options), the same
     * row/cell-tombstone shapes, and the query pinned ONE HOUR in the future so every tombstone
     * is provably past its grace period at read time.
     */
    private Workload loadGcableWorkload(String extraTableOptions) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 0" + extraTableOptions);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck * 10, "r0-" + ck);
        flush();
        for (long ck = 0; ck < ROWS; ck++)
        {
            if (ck % 4 == 0)
                execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", ck);
            else if (ck % 4 == 1)
                execute("DELETE v1 FROM %s WHERE pk = 0 AND ck = ?", ck);
        }
        flush();
        assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());

        return new Workload(cfs, FBUtilities.nowInSeconds() + 3600);
    }
}
