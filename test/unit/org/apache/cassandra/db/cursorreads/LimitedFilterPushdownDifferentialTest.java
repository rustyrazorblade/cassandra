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
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * M3.2d (CASSANDRA-20428): differential + scan-metrics-parity scenarios for the COMBINED
 * filter+limit production bound — the actual new performance win of M3.2, since before this
 * increment a filtered+limited query got NO production bound at all (the limit disengaged because
 * of the filter, the filter's row-level pushdown disengaged because of the limit — two separate
 * conservative gates that, combined, blocked exactly the {@code SELECT ... WHERE <filter> LIMIT n}
 * shape that matters most for real workloads).
 *
 * This class is deliberately held to a HIGHER rigor bar than M3.2a/b/c's own suites: every
 * scenario below is chosen to distinguish CORRECT composition (a row must survive the filter AND
 * the slice-position check before it counts toward the limit) from a plausible-looking but wrong
 * composition (counting rows toward the limit regardless of filter verdict, which would under-
 * produce — the merge stopping early having satisfied the limit with rows the top filter would
 * still have dropped, silently returning FEWER real results than the query asked for). The
 * differential harness ({@link #assertCursorReadMatchesIterator}) is the decisive judge for every
 * scenario: it compares against the REAL iterator path's real {@code RowFilter.filter} then
 * {@code DataLimits.filter} chain, so a wrong composition shows up as a byte divergence, not a
 * hand-derived row count this class would have to get right on its own.
 *
 * <ul>
 *   <li><b>Basic engagement across LIMIT shapes</b> ({@link #pushableFilterEngagesAcrossLimitShapes}):
 *       LIMIT 1, LIMIT 16, and a LIMIT larger than the total match count (an "effectively
 *       unlimited" shape that is still a real, finite {@code CQL_LIMIT} — the production bound
 *       must still ATTEMPT to engage via {@code limitBoundFor}'s kind check, but
 *       {@code mergesStoppedByLimit} must NOT advance since the counter is never actually
 *       satisfied, exactly mirroring the top counter's own behavior).</li>
 *   <li><b>The critical edge case</b> ({@link #limitLandsExactlyPastFilterDroppedRows}): the LIMIT's
 *       positional landing point (counting ALL produced rows, filter-dropped or not) falls on a
 *       row the filter drops — a broken implementation that counted every produced row toward the
 *       limit (rather than only filter-surviving ones) would under-produce here. The merge must
 *       keep pulling past the dropped row(s) until the limit's real requirement of SURVIVING rows
 *       is met.</li>
 *   <li><b>LIMIT landing mid-open-range-tombstone</b>
 *       ({@link #limitLandsInsideResurrectedRowUnderOpenRangeTombstone}): the limit's satisfying
 *       row is a write that RESURRECTS a clustering position nominally inside a broad range
 *       tombstone's span (a later-timestamped overwrite that supersedes the RT) — proving the
 *       composed bound handles a live, filter-matching, limit-satisfying row correctly even while
 *       the merge's cross-leg open-marker tracking still considers that span nominally deleted.</li>
 *   <li><b>Paging + filter</b> ({@link #pagedFilteredReadResumesAcrossDroppedRows}): resume
 *       correctness across page boundaries with filter-dropped rows interspersed, via the real
 *       {@code SinglePartitionPager} machinery and per-page serialized {@link
 *       org.apache.cassandra.service.pager.PagingState} equality.</li>
 *   <li><b>Unpushable filters keep the limit bound disengaged</b>
 *       ({@link #unpushableFilterShapesKeepLimitBoundDisengaged}): multi-cell CONTAINS, complex
 *       MAP_ELEMENT and a needsReconciliation filter, each combined with a LIMIT — proving
 *       {@code limitBoundFor} correctly declines when {@code filterPushdownFor} would (the "only
 *       lift when filterPushdownFor also engages" condition), not merely that the query still
 *       runs.</li>
 *   <li><b>Three-way composition</b> (filter + tombstone threshold + LIMIT,
 *       {@link #abortStillFiresWhenThresholdCrossedBeforeLimitIsSatisfied} and
 *       {@link #abortMustNotFireWhenLimitIsSatisfiedBeforeThresholdCrossing}): the sharpest test of
 *       "the merge must not over-scan past its own limit satisfaction point" — a tombstone
 *       threshold that would ONLY be crossed by rows AFTER the limit is satisfied must NOT trigger
 *       the production-time abort, while one crossed BEFORE an (unsatisfied, large) limit still
 *       must.</li>
 *   <li><b>Partition-level short-circuit composes with the limit bound</b>
 *       ({@link #partitionLevelFilterDropComposesWithLimit}): genuinely new territory this
 *       increment unlocks — a partition-level filter drop under a LIMIT never even reaches the
 *       row/limit machinery ({@code countPartition} is never called), so {@code mergesStoppedByLimit}
 *       must stay untouched while {@code partitionsSkippedByFilter} advances.</li>
 * </ul>
 */
public class LimitedFilterPushdownDifferentialTest extends CursorReadDifferentialTester
{
    @After
    public void resetAccountingHooks()
    {
        CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING = false;
    }

    // ---------------------------------------------------------------- basic engagement

    @Test
    public void pushableFilterEngagesAcrossLimitShapes() throws Throwable
    {
        // v = 1 for ck % 5 == 2 (ck = 2, 7, 12, ..., 97): 20 matches spread across 100 rows, every
        // match preceded by 1-4 filter-dropped rows — every LIMIT below requires walking past drops
        Workload w = loadPeriodicWorkload();
        Consumer<RowFilter> filter = f -> f.add(w.col("v"), Operator.EQ, ByteBufferUtil.bytes(1L));

        // LIMIT 1: satisfied at ck = 2, after dropping ck = 0, 1
        assertComposedEngagement(w.cfs, w.limitFiltered(1, filter), true, true);

        // LIMIT 16: satisfied at the 16th match (ck = 2 + 5*15 = 77), after dropping ~61 rows
        assertComposedEngagement(w.cfs, w.limitFiltered(16, filter), true, true);

        // LIMIT 1000: only 20 total matches exist, so the counter is NEVER satisfied — the merge
        // must scan the WHOLE partition (mergesStoppedByLimit must NOT advance, exactly like the
        // top counter's own isDoneForPartition() never tripping), while row-level pushdown still
        // drops every one of the ~80 non-matching rows along the way (it is no longer categorically
        // disengaged just because a limit is present)
        assertComposedEngagement(w.cfs, w.limitFiltered(1000, filter), false, true);
    }

    // ---------------------------------------------------------------- the critical edge case

    @Test
    public void limitLandsExactlyPastFilterDroppedRows() throws Throwable
    {
        // v = 1 ONLY for ck = 5 (every other row of a 20-row partition is filter-dropped). LIMIT 1
        // lands, positionally, on ck = 0 if (incorrectly) every PRODUCED row counted toward the
        // limit regardless of filter verdict — ck = 0 is filter-dropped, so a broken "count all
        // produced rows" composition would stop there and under-produce (0 rows instead of 1). The
        // correct composition (count only filter-SURVIVING rows) must keep pulling through ck =
        // 0..4 — all dropped — to reach ck = 5, the query's one true result.
        Workload w = loadSingleMatchWorkload(20, 5);
        Consumer<RowFilter> filter = f -> f.add(w.col("v"), Operator.EQ, ByteBufferUtil.bytes(1L));
        Supplier<SinglePartitionReadCommand> cmd = w.limitFiltered(1, filter);

        // pin down the expected shape directly against the real CQL evaluator before trusting the
        // differential comparison to it — the workload itself must actually have this drop-then-
        // match shape, not accidentally match at ck = 0
        UntypedResultSet matches = execute("SELECT ck FROM %s WHERE pk = 0 AND v = 1 ALLOW FILTERING");
        assertEquals("workload drift: exactly one row must match v = 1", 1, matches.size());
        assertEquals("workload drift: the match must not be the first row (or this isn't the edge case)",
                     5L, matches.one().getLong("ck"));

        assertComposedEngagement(w.cfs, cmd, true, true);
    }

    // ---------------------------------------------------------------- mid-open-range-tombstone

    @Test
    public void limitLandsInsideResurrectedRowUnderOpenRangeTombstone() throws Throwable
    {
        Workload w = loadRangeTombstoneWorkload();
        Consumer<RowFilter> filter = f -> f.add(w.col("v"), Operator.EQ, ByteBufferUtil.bytes(1L));

        // workload sanity: 11 total matches (6 in the prefix, the resurrected ck=50 inside the
        // nominal RT span, 4 in the suffix) — the 7th match must be the resurrected row
        UntypedResultSet matches = execute("SELECT ck FROM %s WHERE pk = 0 AND v = 1 ALLOW FILTERING");
        assertEquals("workload drift: expected 11 total matches", 11, matches.size());
        UntypedResultSet resurrected = execute("SELECT v FROM %s WHERE pk = 0 AND ck = 50");
        assertFalse("workload drift: ck = 50 must have been resurrected (not still range-deleted)",
                    resurrected.isEmpty());
        assertEquals(1L, resurrected.one().getLong("v"));

        // LIMIT 7: 6 matches in the ck < 30 prefix, the 7th is ck = 50 — nominally inside the
        // [30, 80) range-tombstone span, surviving only because its write timestamp supersedes the
        // RT. The merge's cross-leg open-marker tracking must still consider that span "open" when
        // it reaches ck = 50, yet correctly treat the resurrected row as live and count it.
        assertComposedEngagement(w.cfs, w.limitFiltered(7, filter), true, true);
    }

    // ---------------------------------------------------------------- paging + filter

    @Test
    public void pagedFilteredReadResumesAcrossDroppedRows() throws Throwable
    {
        Workload w = loadPeriodicWorkload();
        // page size 7 over 100 rows with matches every 5th row starting at ck=2 (20 matches total,
        // ~15 pages): every page boundary falls at an arbitrary position relative to the drop/match
        // pattern, so resume correctness cannot rely on boundaries lining up with matches
        Supplier<SinglePartitionReadCommand> cmd =
            w.pagedFiltered(f -> f.add(w.col("v"), Operator.EQ, ByteBufferUtil.bytes(1L)));
        long engagedBefore = CursorReads.filterPushdownEngaged();
        long droppedBefore = CursorReads.rowsDroppedByFilter();
        long stoppedBefore = CursorReads.mergesStoppedByLimit();
        assertPagedReadMatchesIterator(w.cfs, cmd, 7, 3);
        assertTrue("filter pushdown did not engage across the paged sequence",
                   CursorReads.filterPushdownEngaged() > engagedBefore);
        assertTrue("row-level pushdown dropped no rows across the paged sequence",
                   CursorReads.rowsDroppedByFilter() > droppedBefore);
        assertTrue("the per-page production bound did not engage across the paged sequence",
                   CursorReads.mergesStoppedByLimit() > stoppedBefore);
    }

    // ---------------------------------------------------------------- unpushable filters

    @Test
    public void unpushableFilterShapesKeepLimitBoundDisengaged() throws Throwable
    {
        Workload w = loadComplexColumnWorkload();

        // multi-cell CONTAINS on a SET column: SIMPLE kind but a COMPLEX column, unpushable
        assertLimitBoundStaysDisengaged(w.cfs,
            w.limitFiltered(10, f -> f.add(w.col("tags"), Operator.CONTAINS, ByteBufferUtil.bytes("common"))));

        // MAP_ELEMENT: not Kind.SIMPLE, unpushable
        assertLimitBoundStaysDisengaged(w.cfs,
            w.limitFiltered(10, f -> f.addMapEquality(w.col("m"), ByteBufferUtil.bytes("stable"),
                                                      Operator.EQ, ByteBufferUtil.bytes("x"))));

        // needsReconciliation: purge-before-evaluate semantics differ, gated out wholesale —
        // Util.cmd's own filterOn builds exactly this shape (RowFilter.create(true)), so this is
        // also every ordinary CQL-path filtered+limited query until a future increment revisits it
        long stoppedBefore = CursorReads.mergesStoppedByLimit();
        long engagedBefore = CursorReads.filterPushdownEngaged();
        Supplier<SinglePartitionReadCommand> needsReconciliation =
            () -> (SinglePartitionReadCommand) Util.cmd(w.cfs, 0L).withNowInSeconds(w.nowInSec)
                                                    .withLimit(10).filterOn("v1", Operator.EQ, 40L).build();
        assertTrue("workload drift: this filter shape must need reconciliation",
                   needsReconciliation.get().rowFilter().needsReconciliation());
        assertCursorReadMatchesIterator(w.cfs, needsReconciliation);
        assertEquals("filter pushdown must not engage on a needsReconciliation filter",
                     engagedBefore, CursorReads.filterPushdownEngaged());
        assertEquals("the limit bound must stay disengaged when the filter is not fully pushable "
                     + "(the 'only lift when filterPushdownFor also engages' condition)",
                     stoppedBefore, CursorReads.mergesStoppedByLimit());
    }

    // ---------------------------------------------------------------- three-way composition

    @Test
    public void abortStillFiresWhenThresholdCrossedBeforeLimitIsSatisfied() throws Throwable
    {
        Workload w = loadTombstoneWorkload();
        int originalFail = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneFailureThreshold(10);
        try
        {
            // only ONE row matches v = 1 in the whole partition (ck = 0) — LIMIT 1000 can never be
            // satisfied, so the merge must scan the entire partition, crossing the 10-tombstone
            // threshold among the cell-tombstoned ck = 1..50 rows exactly like the iterator path
            Consumer<RowFilter> filter = f -> f.add(w.col("v"), Operator.EQ, ByteBufferUtil.bytes(1L));
            Supplier<SinglePartitionReadCommand> cmd = w.limitFiltered(1000, filter);
            ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(w.cfs, cmd);
            assertTrue("read must have aborted with TombstoneOverwhelmingException despite the "
                       + "(unsatisfied) LIMIT being present", iterator.aborted());
            assertEquals("abort must have been metered exactly once", 1, iterator.tombstoneFailures);
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalFail);
        }
    }

    @Test
    public void abortMustNotFireWhenLimitIsSatisfiedBeforeThresholdCrossing() throws Throwable
    {
        Workload w = loadTombstoneWorkload();
        int originalFail = DatabaseDescriptor.getTombstoneFailureThreshold();
        // threshold of 1 is FAR below the ~50 tombstones scattered through ck = 1..50 — if the
        // merge ever scanned that region, it would abort many times over. LIMIT 1 is satisfied
        // IMMEDIATELY by ck = 0 (the workload's only match, and the very first row), so the merge
        // must stop before ever reaching ck = 1 — the tombstone region must never be scanned at all
        DatabaseDescriptor.setTombstoneFailureThreshold(1);
        try
        {
            Consumer<RowFilter> filter = f -> f.add(w.col("v"), Operator.EQ, ByteBufferUtil.bytes(1L));
            Supplier<SinglePartitionReadCommand> cmd = w.limitFiltered(1, filter);
            ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(w.cfs, cmd);
            assertFalse("read must NOT have aborted: the tombstone-crossing region lies entirely "
                        + "AFTER the LIMIT's satisfaction point, so a correct composition never "
                        + "scans it — an over-scanning bug would abort here", iterator.aborted());
            assertEquals(0, iterator.tombstoneFailures);
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalFail);
        }
    }

    // ---------------------------------------------------------------- partition-level + limit

    @Test
    public void partitionLevelFilterDropComposesWithLimit() throws Throwable
    {
        // a STATIC-column filter that fails: the M3.2a partition-level short-circuit fires before
        // any row/limit machinery ever runs — countPartition (and therefore the limit bound) must
        // never even be reached for this partition, exactly like the iterator path, where
        // RowFilter.filter's own partition-level check closes the partition before the DataLimits
        // counter (which sits ABOVE the filter) ever sees it
        Workload w = loadStaticWorkload();
        Consumer<RowFilter> filter = f -> f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(999L));
        long skippedBefore = CursorReads.partitionsSkippedByFilter();
        long stoppedBefore = CursorReads.mergesStoppedByLimit();
        long engagedBefore = CursorReads.filterPushdownEngaged();
        assertCursorReadMatchesIterator(w.cfs, w.limitFiltered(10, filter));
        assertTrue("filter pushdown context did not attach for a limited+filtered query",
                   CursorReads.filterPushdownEngaged() > engagedBefore);
        assertTrue("partition-level short-circuit did not fire for a partition the static filter drops",
                   CursorReads.partitionsSkippedByFilter() > skippedBefore);
        assertEquals("a partition dropped at the partition level must never reach the row-level "
                     + "limit machinery (countPartition is never called for it)",
                     stoppedBefore, CursorReads.mergesStoppedByLimit());
    }

    // ---------------------------------------------------------------- guards

    /**
     * Byte-identity plus the two composed engagement guards.
     *
     * @param expectLimitStop   whether the production LIMIT bound must have stopped the merge
     *                          ({@code mergesStoppedByLimit} advances) — false for a limit that can
     *                          never be satisfied by the real match count
     * @param expectFilterDrops whether row-level filter pushdown must have dropped at least one row
     */
    private void assertComposedEngagement(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd,
                                          boolean expectLimitStop, boolean expectFilterDrops)
    {
        long engagedBefore = CursorReads.filterPushdownEngaged();
        long droppedBefore = CursorReads.rowsDroppedByFilter();
        long stoppedBefore = CursorReads.mergesStoppedByLimit();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertTrue("filter pushdown did not engage for a pushable filter under a LIMIT (M3.2d)",
                   CursorReads.filterPushdownEngaged() > engagedBefore);
        if (expectLimitStop)
            assertTrue("the composed production limit bound did not stop the merge",
                       CursorReads.mergesStoppedByLimit() > stoppedBefore);
        else
            assertEquals("the limit bound must not report stopping a merge it never actually bounded",
                         stoppedBefore, CursorReads.mergesStoppedByLimit());
        if (expectFilterDrops)
            assertTrue("row-level pushdown dropped no rows — composition proved nothing",
                       CursorReads.rowsDroppedByFilter() > droppedBefore);
        else
            assertEquals("row-level pushdown dropped rows for a shape expected to keep everything",
                         droppedBefore, CursorReads.rowsDroppedByFilter());
    }

    private void assertLimitBoundStaysDisengaged(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long stoppedBefore = CursorReads.mergesStoppedByLimit();
        long engagedBefore = CursorReads.filterPushdownEngaged();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertEquals("filter pushdown must not engage on an unpushable filter shape",
                     engagedBefore, CursorReads.filterPushdownEngaged());
        assertEquals("the limit bound must stay disengaged when filterPushdownFor declines",
                     stoppedBefore, CursorReads.mergesStoppedByLimit());
    }

    private ScanMetricsCapture.Snapshot assertScanMetricsParity(ColumnFamilyStore cfs,
                                                                 Supplier<SinglePartitionReadCommand> cmd)
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        ScanMetricsCapture.Snapshot iterator = ScanMetricsCapture.capture(cfs, () -> consume(cmd.get()));

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = cmd.get();
            assertTrue("scenario is not supported by the cursor read gate; this parity run would "
                       + "silently compare iterator vs iterator",
                       CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));
            long servedBefore = CursorReads.sstableLegsServed();
            ScanMetricsCapture.Snapshot cursor = ScanMetricsCapture.capture(cfs, () -> consume(cmd.get()));
            assertTrue("cursor path did not actually serve any sstable leg (silent fallback?)",
                       CursorReads.sstableLegsServed() - servedBefore > 0);
            ScanMetricsCapture.assertParity("iterator vs cursor, combined filter+limit bound", iterator, cursor);
            return iterator;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private void consume(SinglePartitionReadCommand command)
    {
        try (org.apache.cassandra.db.ReadExecutionController controller = command.executionController();
             org.apache.cassandra.db.partitions.UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (org.apache.cassandra.db.rows.UnfilteredRowIterator partition = partitions.next())
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

        /** A pk=0 read carrying a STRICT, reconciliation-free pushable filter under a CQL LIMIT —
         *  the exact shape M3.2d's combined bound engages for. */
        Supplier<SinglePartitionReadCommand> limitFiltered(int limit, Consumer<RowFilter> expressions)
        {
            return build(DataLimits.cqlLimits(limit), expressions);
        }

        /** The same strict filter, unpaged at the base-command level — resumed page-by-page via
         *  {@link #assertPagedReadMatchesIterator}, which drives its own per-page CQL_PAGING_LIMIT. */
        Supplier<SinglePartitionReadCommand> pagedFiltered(Consumer<RowFilter> expressions)
        {
            return build(DataLimits.NONE, expressions);
        }

        private Supplier<SinglePartitionReadCommand> build(DataLimits limits, Consumer<RowFilter> expressions)
        {
            return () -> {
                SinglePartitionReadCommand base =
                    (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(nowInSec).build();
                RowFilter rowFilter = RowFilter.create(false);
                expressions.accept(rowFilter);
                return SinglePartitionReadCommand.create(cfs.metadata(), base.nowInSec(),
                                                         ColumnFilter.all(cfs.metadata()), rowFilter,
                                                         limits, base.partitionKey(),
                                                         base.clusteringIndexFilter());
            };
        }
    }

    /**
     * ROWS rows, v = 1 exactly where {@code (ck - offset) % 5 == 0}, else v = 0 — a periodic
     * keep/drop pattern with matches spread evenly, every one preceded by 1-4 drops. Two
     * overlapping sstables (v gets overwritten in round 2, same winning values, so the merge is
     * real and multi-leg without changing the match set).
     */
    private Workload loadPeriodicWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) "
                    + "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 100; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, (ck - 2) % 5 == 0 ? 1L : 0L);
        flush();
        // round 2: rewrite every row with the SAME value (overlapping leg, same match set) so the
        // merge is genuinely multi-source without perturbing which cks match
        for (long ck = 0; ck < 100; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, (ck - 2) % 5 == 0 ? 1L : 0L);
        flush();
        assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());

        assertEquals("periodic match-set drifted", 20,
                     execute("SELECT ck FROM %s WHERE pk = 0 AND v = 1 ALLOW FILTERING").size());
        return new Workload(cfs, FBUtilities.nowInSeconds());
    }

    /** {@code rows} rows, v = 1 at exactly {@code matchCk} (>= 1), else v = 0 — the minimal
     *  drop-then-match shape for the exact-landing-on-a-dropped-row edge case. Two overlapping
     *  sstables (same values rewritten in round 2): the filter/limit bound only ever attaches for
     *  a MERGED (>= 2 leg) read — {@code completeSingleLeg} single-leg reads get no bound at all,
     *  by every prior increment's own scope choice — so a single-flush workload here would
     *  silently exercise a code path this test isn't even trying to cover. */
    private Workload loadSingleMatchWorkload(int rows, long matchCk) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) "
                    + "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < rows; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, ck == matchCk ? 1L : 0L);
        flush();
        for (long ck = 0; ck < rows; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, ck == matchCk ? 1L : 0L);
        flush();
        assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());
        return new Workload(cfs, FBUtilities.nowInSeconds());
    }

    /**
     * The mid-open-range-tombstone workload: a periodic prefix (ck 0..29, matches at ck % 5 == 2:
     * 2, 7, 12, 17, 22, 27 — 6 matches), a broad range tombstone over [30, 80) written AFTER the
     * prefix, a single row at ck = 50 RESURRECTED with a later timestamp (still inside the RT's
     * nominal span but superseding it), and a periodic suffix (ck 80..99, matches at
     * 82, 87, 92, 97 — 4 matches). 11 total matches; the 7th is the resurrected ck = 50.
     */
    private Workload loadRangeTombstoneWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) "
                    + "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, (ck - 2) % 5 == 0 ? 1L : 0L);
        for (long ck = 80; ck < 100; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, (ck - 2) % 5 == 0 ? 1L : 0L);
        flush();

        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 30 AND ck < 80");
        flush();

        // resurrect ck = 50 with a strictly later write than the range tombstone
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 50, 1)");
        flush();
        assertEquals("expected exactly 3 overlapping sstables", 3, cfs.getLiveSSTables().size());

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }

    /**
     * 51-row workload for the three-way (filter + tombstone threshold + LIMIT) scenarios: ck = 0
     * matches v = 1 (the query's ONLY match, and the very first row in clustering order); ck =
     * 1..50 all have v cell-tombstoned (v != 1, so they are ALSO filter-dropped, and each
     * contributes exactly one tombstone to the dropped-row accounting).
     */
    private Workload loadTombstoneWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) "
                    + "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, 1)");
        for (long ck = 1; ck <= 50; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, 0)", ck);
        flush();
        for (long ck = 1; ck <= 50; ck++)
            execute("DELETE v FROM %s WHERE pk = 0 AND ck = ?", ck);
        flush();
        assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());

        assertEquals("tombstone-workload match-set drifted", 1,
                     execute("SELECT ck FROM %s WHERE pk = 0 AND v = 1 ALLOW FILTERING").size());
        return new Workload(cfs, FBUtilities.nowInSeconds());
    }

    /** Complex columns (multi-cell set + map) for the unpushable-shape scenarios. */
    private Workload loadComplexColumnWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, tags set<text>, m map<text, text>, "
                    + "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
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

    /** A static-column filter workload for the partition-level short-circuit + limit scenario. */
    private Workload loadStaticWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s bigint static, v bigint, "
                    + "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, s) VALUES (0, 42)");
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, ck);
        flush();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, ck);
        flush();
        assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }
}
