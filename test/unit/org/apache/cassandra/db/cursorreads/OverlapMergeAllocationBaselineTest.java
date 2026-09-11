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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * M2.0 (CASSANDRA-20428, Phase 3 first increment): BASELINE measurement of the per-source
 * materialization cost that M2's cursor-level merge (M2.1+) is meant to eliminate. This is a
 * RECORDING/DIAGNOSTIC test, not a pass/fail performance gate — there is nothing to compare
 * against until the merge core exists. Its assertions are workload-correctness and
 * silent-fallback guards only (the numbers must be REAL, not vacuous); the measured numbers are
 * logged and journaled as the "before" side of M2.4's before/after comparison, and M2.1 extends
 * this scaffolding into a real gate (cursor:iterator ratio strictly < 1.0 on the S=5
 * shadow-heavy scenario, bound calibrated from the numbers this test records).
 *
 * WHAT IS BEING MEASURED. Today, when S sstables overlap on the same rows, each of the S legs is
 * fully, independently materialized by {@link CursorReads#sstableRowIterator} into real
 * {@code Row}/{@code Cell} objects before the OBJECT-level merge ({@code UnfilteredRowIterators
 * .merge}) discards the shadowed/superseded S-1 copies. That is the "compaction backlog" /
 * "read-heavy-before-compaction-catches-up" shape the Phase 0 JFR profile flagged (journal
 * FINDING #3: source amplification is a major real-world factor). A cursor-level merge
 * reconciles at the descriptor/byte level and materializes ONLY the merge winners, so the
 * S-1 shadowed copies' materialization is entirely avoidable. Two numbers are captured per
 * scenario:
 * <ol>
 *   <li><b>Total thread-allocated bytes per workload pass</b>, iterator path vs cursor path,
 *       via the established {@code com.sun.management.ThreadMXBean} methodology
 *       ({@link CursorReadAllocationGateTest}: warmup both paths to JIT steady state, then
 *       min-of-N measured passes; exact {@link CursorReads#sstableLegsServed} accounting proves
 *       every expected leg really ran through the cursor path and that the iterator measurement
 *       never consulted it).</li>
 *   <li><b>The shadowed-waste fraction</b>: what part of the materialized inventory (rows,
 *       range-tombstone markers, cells, cell-value bytes) is discarded by the object merge
 *       because a later source superseded it. Sigma(per-leg materialized) is enumerated with
 *       test-only instrumentation — direct calls to the public
 *       {@link CursorReads#sstableRowIterator} per (command x live sstable), exactly the calls
 *       {@code queryMemtableAndDiskInternal} makes for these shapes — and the merge survivors
 *       are counted from {@code executeLocally}'s merged output. Waste = 1 - merged/Sigma(legs).
 *       The per-leg enumeration is cross-checked against the production
 *       {@link CursorReads#unfilteredsMaterialized} counter observed during a real merged read
 *       of the same commands, so the test-side accounting provably matches what production
 *       materializes (no production instrumentation added; M2.0 is a zero-production-change
 *       increment). This fraction is the direct predictor of M2.1's payoff — more precise than
 *       total allocation, which also carries per-leg constants and merge/orchestration costs
 *       common to both paths.</li>
 * </ol>
 *
 * WORKLOAD (per the M2 design's S in {1, 2, 5}, shadow-heavy and tie-heavy variants; each
 * scenario gets its own table):
 * <ul>
 *   <li><b>Shadow-heavy</b>: S flush rounds; every round rewrites ALL columns of the SAME
 *       {@value #PARTITIONS} x {@value #ROWS_PER_PARTITION} (pk, ck) grid with naturally
 *       increasing timestamps, so rounds 0..S-2 are fully superseded by round S-1. The final
 *       round also issues explicit deletes (row tombstone at ck=3, cell tombstone on v2 at
 *       ck=5, range tombstone [40, 48) — per partition), covering the shadowed-by-delete case
 *       alongside shadowed-by-overwrite. Expected waste ~ (S-1)/S.</li>
 *   <li><b>Tie-heavy</b>: S rounds writing the same grid {@code USING TIMESTAMP} with the SAME
 *       fixed timestamp but different values per round, so every cell merge is an
 *       exact-timestamp tie resolved by the value comparison (greater value wins — the last
 *       round's values are constructed to win). Same S-1 discarded copies, but through the
 *       tie-break arm of reconciliation rather than the timestamp arm.</li>
 * </ul>
 * Everything is flushed (the memtable is asserted empty) so the merge is sstable-legs-only and
 * the waste accounting is exact; there are no partition-level deletions, so the
 * {@code mostRecentPartitionTombstone} elimination loop cannot drop legs and every read
 * genuinely merges S sources. Reads are full-partition (both paths materialize the whole
 * partition per leg, so the measured delta is the merge shape itself, not the separate M1 seek
 * story — the BTI mid-slice variant of {@link CursorReadAllocationGateTest} covers that).
 *
 * SINCE M2.1 (the cursor-level merge core) this class is no longer baseline-only: multi-leg
 * reads route through {@code CursorReadMerger}, so (a) the shadowed-waste cross-check flips from
 * "materialized == Sigma(per-leg)" to "materialized == merged output" — the anti-fallback
 * effectiveness guard — with exact merge-served counter accounting, and (b) the S=5 scenarios
 * carry the design's first real allocation-WIN gate (cursor:iterator strictly below 1.0; bound
 * documented at the assertion). The per-leg Sigma enumeration below still measures the avoidable
 * waste via direct single-leg {@link CursorReads#sstableRowIterator} calls, which keep Phase 1
 * per-leg behavior by design.
 */
public class OverlapMergeAllocationBaselineTest extends CursorReadDifferentialTester
{
    private static final int PARTITIONS = 8;
    private static final int ROWS_PER_PARTITION = 128;
    private static final int WARMUP_PASSES = 20;
    private static final int MEASURED_PASSES = 8;
    private static final long TIE_TIMESTAMP = 1000;

    /** Blackhole so pass consumption cannot be dead-code-eliminated. */
    private static volatile long sink;

    private enum Variant { SHADOW, TIE }

    @Test
    public void shadowHeavyS1() throws Throwable
    {
        runScenario(1, Variant.SHADOW);
    }

    @Test
    public void shadowHeavyS2() throws Throwable
    {
        runScenario(2, Variant.SHADOW);
    }

    @Test
    public void shadowHeavyS5() throws Throwable
    {
        runScenario(5, Variant.SHADOW);
    }

    @Test
    public void tieHeavyS2() throws Throwable
    {
        runScenario(2, Variant.TIE);
    }

    @Test
    public void tieHeavyS5() throws Throwable
    {
        runScenario(5, Variant.TIE);
    }

    // ---------------------------------------------------------------- scenario driver

    private void runScenario(int sources, Variant variant) throws Throwable
    {
        com.sun.management.ThreadMXBean bean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", bean != null);

        ColumnFamilyStore cfs = loadWorkload(sources, variant);
        List<Supplier<SinglePartitionReadCommand>> commands = fullPartitionCommands(cfs);
        String label = String.format("%s S=%d", variant == Variant.SHADOW ? "shadow-heavy" : "tie-heavy", sources);

        long[] best = measureBothPaths(bean, cfs, commands, sources);
        long iteratorBest = best[0];
        long cursorBest = best[1];
        double ratio = (double) cursorBest / iteratorBest;

        Counts merged = new Counts();
        Counts legs = new Counts();
        measureShadowedWaste(cfs, commands, sources, merged, legs);

        long wastedRows = legs.rows - merged.rows;
        long wastedCells = legs.cells - merged.cells;
        long wastedValueBytes = legs.valueBytes - merged.valueBytes;
        logger.info("overlap-merge measurement [{}] (M2.0 recorded the per-leg baseline; since M2.1 " +
                    "multi-leg reads are served by the cursor-level merge, single-leg by the Phase 1 path):\n" +
                    "  allocation/pass: iterator={}B cursor={}B ratio={}\n" +
                    "  per-leg materialized (sum over {} legs x {} reads): rows={} markers={} cells={} valueBytes={}\n" +
                    "  merge survivors (emitted):                          rows={} markers={} cells={} valueBytes={}\n" +
                    "  DISCARDED by object merge (M2.1's target):          rows={} ({}) cells={} ({}) valueBytes={} ({})",
                    label, iteratorBest, cursorBest, String.format("%.4f", ratio),
                    sources, commands.size(),
                    legs.rows, legs.markers, legs.cells, legs.valueBytes,
                    merged.rows, merged.markers, merged.cells, merged.valueBytes,
                    wastedRows, pct(wastedRows, legs.rows),
                    wastedCells, pct(wastedCells, legs.cells),
                    wastedValueBytes, pct(wastedValueBytes, legs.valueBytes));

        // baseline-recording sanity: the numbers must be real, and the workload must actually be
        // overlap/shadow-shaped, or the recorded baseline is meaningless
        assertTrue("no allocation measured", iteratorBest > 0 && cursorBest > 0);
        assertTrue("per-leg inventory must be at least the merged inventory",
                   legs.rows >= merged.rows && legs.cells >= merged.cells);
        if (sources > 1)
        {
            assertTrue("multi-source scenario produced no shadowed rows — workload is not overlap-heavy",
                       wastedRows > 0);
            assertTrue("multi-source scenario produced no shadowed cells — workload is not overlap-heavy",
                       wastedCells > 0);
        }
        if (sources == 5)
            assertTrue(String.format("S=5 scenario should discard the majority of materialized cells " +
                                     "(expected ~4/5), but waste was only %s — workload is not shadow-heavy",
                                     pct(wastedCells, legs.cells)),
                       wastedCells * 2 > legs.cells);

        // M2.1 ALLOCATION GATE (the assertion this scaffolding was built to carry, per the M2
        // design §5): with the cursor-level merge serving multi-leg reads, the cursor path must
        // allocate strictly LESS than the iterator path on the S=5 overlap workloads — the
        // per-leg shells/values of merge losers are no longer materialized. Bounds calibrated
        // empirically against measured post-M2.1 steady-state ratios (JDK21; the M2.0 "before"
        // ratio was ~1.06-1.08 on every overlap scenario, journal FINDING #11):
        //   shadow-heavy S=5: measured 0.2256 (BIG) / 0.2287 (BTI)  -> bound 0.5 (>2x headroom)
        //   tie-heavy   S=5: measured 0.3856 (BIG) / 0.3880 (BTI)  -> bound 0.7 (tie groups
        //                    materialize BOTH sides of every value-compare, so the win is smaller
        //                    by construction until compaction-style scratch comparison is worth it)
        // Either bound trips far before parity — a regression back toward per-leg
        // materialization (~1.0+) fails loudly, and even a halving of the win is caught.
        if (sources == 5)
        {
            double bound = variant == Variant.SHADOW ? 0.5 : 0.7;
            assertTrue(String.format("S=5 %s: cursor-level merge shows no allocation win: " +
                                     "iterator=%,dB cursor=%,dB ratio=%.4f (bound %.2f) — is the merge " +
                                     "materializing per-leg again?",
                                     label, iteratorBest, cursorBest, ratio, bound),
                       ratio < bound);
        }
    }

    // ---------------------------------------------------------------- allocation measurement
    // (the CursorReadAllocationGateTest methodology: warmup both paths, min-of-N, exact leg
    // accounting in both directions)

    private long[] measureBothPaths(com.sun.management.ThreadMXBean bean,
                                    ColumnFamilyStore cfs,
                                    List<Supplier<SinglePartitionReadCommand>> commands,
                                    int sources) throws Throwable
    {
        assertGateOpenForAll(cfs, commands);
        assertEquals("expected exactly S overlapping sstables", sources, cfs.getLiveSSTables().size());
        long expectedLegsPerPass = (long) commands.size() * sources;

        try
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
            for (int i = 0; i < WARMUP_PASSES; i++)
                runPass(commands);

            DatabaseDescriptor.setCursorReadsEnabled(true);
            long servedBeforeWarmup = CursorReads.sstableLegsServed();
            for (int i = 0; i < WARMUP_PASSES; i++)
                runPass(commands);
            assertEquals("cursor warmup did not serve the expected sstable legs (silent fallback?)",
                         WARMUP_PASSES * expectedLegsPerPass,
                         CursorReads.sstableLegsServed() - servedBeforeWarmup);

            DatabaseDescriptor.setCursorReadsEnabled(false);
            long servedBeforeIterator = CursorReads.sstableLegsServed();
            long missedBeforeIterator = CursorReads.sstableLegsWithoutPartition();
            long iteratorBest = measureBest(bean, commands);
            assertEquals("iterator-path measurement unexpectedly ran the cursor path",
                         servedBeforeIterator, CursorReads.sstableLegsServed());
            assertEquals("iterator-path measurement unexpectedly consulted the cursor path",
                         missedBeforeIterator, CursorReads.sstableLegsWithoutPartition());

            DatabaseDescriptor.setCursorReadsEnabled(true);
            long servedBeforeCursor = CursorReads.sstableLegsServed();
            long cursorBest = measureBest(bean, commands);
            assertEquals("cursor measurement did not serve the expected sstable legs (silent fallback?)",
                         MEASURED_PASSES * expectedLegsPerPass,
                         CursorReads.sstableLegsServed() - servedBeforeCursor);

            return new long[]{ iteratorBest, cursorBest };
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private long measureBest(com.sun.management.ThreadMXBean bean, List<Supplier<SinglePartitionReadCommand>> commands) throws Throwable
    {
        long tid = Thread.currentThread().getId();
        long best = Long.MAX_VALUE;
        for (int i = 0; i < MEASURED_PASSES; i++)
        {
            long before = bean.getThreadAllocatedBytes(tid);
            runPass(commands);
            best = Math.min(best, bean.getThreadAllocatedBytes(tid) - before);
        }
        return best;
    }

    private void assertGateOpenForAll(ColumnFamilyStore cfs, List<Supplier<SinglePartitionReadCommand>> commands)
    {
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            for (Supplier<SinglePartitionReadCommand> supplier : commands)
            {
                SinglePartitionReadCommand probe = supplier.get();
                assertTrue("command shape is not supported by the cursor read gate — this measurement " +
                           "would silently compare iterator vs iterator: " + probe,
                           CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));
            }
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private void runPass(List<Supplier<SinglePartitionReadCommand>> commands) throws Throwable
    {
        long consumed = 0;
        for (Supplier<SinglePartitionReadCommand> supplier : commands)
        {
            SinglePartitionReadCommand command = supplier.get();
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller))
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        if (!partition.staticRow().isEmpty())
                            consumed++;
                        while (partition.hasNext())
                            consumed += partition.next().clustering().size();
                    }
                }
            }
        }
        sink += consumed;
    }

    // ---------------------------------------------------------------- shadowed-waste accounting

    /** Materialized-inventory tally: rows and RT markers (static row excluded, matching
     *  {@link CursorReads#unfilteredsMaterialized}), cells (simple + complex) and cell value bytes. */
    private static final class Counts
    {
        long rows;
        long markers;
        long cells;
        long valueBytes;
    }

    /**
     * Counts the merge survivors from one real cursor-path pass over {@code executeLocally} (the
     * production merge), then enumerates Sigma(per-leg materialized) by invoking
     * {@link CursorReads#sstableRowIterator} directly per (command x live sstable) — the same
     * public entry point, key, slices and column filter {@code queryMemtableAndDiskInternal} uses
     * for these shapes. Cross-checks the enumeration against the production
     * {@link CursorReads#unfilteredsMaterialized} delta observed during the merged pass, proving
     * the test-side per-leg accounting equals what production actually materialized.
     */
    private void measureShadowedWaste(ColumnFamilyStore cfs,
                                      List<Supplier<SinglePartitionReadCommand>> commands,
                                      int sources,
                                      Counts merged,
                                      Counts legs) throws Throwable
    {
        // merge survivors, from a real merged read (cursor path enabled end to end)
        DatabaseDescriptor.setCursorReadsEnabled(true);
        long materializedBefore;
        long materializedDuringMergedPass;
        try
        {
            materializedBefore = CursorReads.unfilteredsMaterialized();
            long servedBefore = CursorReads.sstableLegsServed();
            long mergesBefore = CursorReads.cursorMergesServed();
            long mergedLegsBefore = CursorReads.sstableLegsCursorMerged();
            for (Supplier<SinglePartitionReadCommand> supplier : commands)
            {
                SinglePartitionReadCommand command = supplier.get();
                try (ReadExecutionController controller = command.executionController();
                     UnfilteredPartitionIterator partitions = command.executeLocally(controller))
                {
                    while (partitions.hasNext())
                    {
                        try (UnfilteredRowIterator partition = partitions.next())
                        {
                            accumulate(partition, merged);
                        }
                    }
                }
            }
            materializedDuringMergedPass = CursorReads.unfilteredsMaterialized() - materializedBefore;
            assertEquals("merged waste pass did not serve the expected legs (silent fallback?)",
                         (long) commands.size() * sources,
                         CursorReads.sstableLegsServed() - servedBefore);
            // M2.1: multi-leg reads must actually route through the cursor-level merge core (one
            // merge per read, every leg cursor-merged); single-leg reads must NOT
            assertEquals("cursor-level merges served during the merged pass",
                         sources > 1 ? commands.size() : 0L,
                         CursorReads.cursorMergesServed() - mergesBefore);
            assertEquals("sstable legs cursor-merged during the merged pass",
                         sources > 1 ? (long) commands.size() * sources : 0L,
                         CursorReads.sstableLegsCursorMerged() - mergedLegsBefore);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }

        // Sigma(per-leg materialized): direct per-leg cursor materialization, test-side
        for (Supplier<SinglePartitionReadCommand> supplier : commands)
        {
            SinglePartitionReadCommand command = supplier.get();
            Slices slices = ((ClusteringIndexSliceFilter) command.clusteringIndexFilter()).getSlices(cfs.metadata());
            for (SSTableReader sstable : liveSSTablesFor(cfs, command))
            {
                try (UnfilteredRowIterator leg = CursorReads.sstableRowIterator(sstable, cfs.metadata(),
                                                                                command.partitionKey(), slices,
                                                                                command.columnFilter(),
                                                                                SSTableReadsListener.NOOP_LISTENER))
                {
                    accumulate(leg, legs);
                }
            }
        }

        // M2.0 originally cross-checked materializedDuringMergedPass against Sigma(per-leg): every
        // leg was independently materialized. Since M2.1 the counter measures the MERGE's
        // effectiveness instead: for multi-leg reads it must equal the MERGED output (winners
        // only), far below Sigma(per-leg) on overlap workloads — a silent fallback to per-leg
        // materialization would still produce byte-identical results and fail exactly this.
        // Single-leg reads keep the Phase 1 path, where materialized == per-leg by construction.
        if (sources == 1)
            assertEquals("single-leg pass must keep Phase 1 materialization accounting exactly",
                         materializedDuringMergedPass, legs.rows + legs.markers);
        else
            assertEquals("merged-mode materialization must equal the MERGED output, not " +
                         "Sigma(per-leg)=" + (legs.rows + legs.markers) + " — did the merge silently " +
                         "fall back to per-leg materialization?",
                         materializedDuringMergedPass, merged.rows + merged.markers);
    }

    private static void accumulate(UnfilteredRowIterator partition, Counts counts)
    {
        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            if (!unfiltered.isRow())
            {
                counts.markers++;
                continue;
            }
            counts.rows++;
            for (ColumnData cd : (Row) unfiltered)
            {
                if (cd.column().isComplex())
                {
                    for (Cell<?> cell : (ComplexColumnData) cd)
                    {
                        counts.cells++;
                        counts.valueBytes += cell.buffer().remaining();
                    }
                }
                else
                {
                    counts.cells++;
                    counts.valueBytes += ((Cell<?>) cd).buffer().remaining();
                }
            }
        }
    }

    private static String pct(long part, long whole)
    {
        return whole == 0 ? "n/a" : String.format("%.1f%%", 100.0 * part / whole);
    }

    // ---------------------------------------------------------------- workload

    private List<Supplier<SinglePartitionReadCommand>> fullPartitionCommands(ColumnFamilyStore cfs)
    {
        long now = FBUtilities.nowInSeconds();
        List<Supplier<SinglePartitionReadCommand>> commands = new ArrayList<>();
        for (long pk = 0; pk < PARTITIONS; pk++)
        {
            long key = pk;
            commands.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build());
        }
        return commands;
    }

    private ColumnFamilyStore loadWorkload(int sources, Variant variant) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, " +
                    "v1 bigint, v2 text, v3 int, v4 text, v5 double, v6 bigint, v7 text, v8 int, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < sources; round++)
        {
            for (long pk = 0; pk < PARTITIONS; pk++)
            {
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                {
                    if (variant == Variant.SHADOW)
                        // natural timestamps: each round fully supersedes the previous one
                        execute("INSERT INTO %s (pk, ck, v1, v2, v3, v4, v5, v6, v7, v8) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                pk, ck, pk * 1000 + ck * 10 + round,
                                "shadow-" + round + "-" + pk + "-" + ck,
                                (int) ck + round, "text-" + round + "-" + ck, round + 0.5,
                                pk * ck + round, "t" + round + "-" + ck, round);
                    else
                        // identical timestamp every round, values constructed so the LAST round
                        // wins every value-compare tie-break (both text and numeric columns)
                        execute("INSERT INTO %s (pk, ck, v1, v2, v3, v4, v5, v6, v7, v8) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP " + TIE_TIMESTAMP,
                                pk, ck, pk * 1000 + ck * 10 + round,
                                "tie-" + round + "-" + pk + "-" + ck,
                                (int) ck + round, "text-" + round + "-" + ck, round + 0.5,
                                pk * ck + round, "t" + round + "-" + ck, round);
                }
            }
            // shadow variant: explicit deletes in the final round, written after that round's
            // inserts so they carry later timestamps — shadow-by-delete alongside
            // shadow-by-overwrite. (Not in the tie variant: a natural-timestamp delete would
            // supersede every tie and turn it into the shadow shape.)
            if (variant == Variant.SHADOW && round == sources - 1)
            {
                for (long pk = 0; pk < PARTITIONS; pk++)
                {
                    execute("DELETE FROM %s WHERE pk = ? AND ck = 3", pk);                  // row tombstone
                    execute("DELETE v2 FROM %s WHERE pk = ? AND ck = 5", pk);               // cell tombstone
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= 40 AND ck < 48", pk);    // range tombstone
                }
            }
            flush();
        }

        assertEquals("expected exactly S overlapping sstables", sources, cfs.getLiveSSTables().size());
        assertEquals("memtable must be empty so the merge is sstable-legs-only and waste accounting exact",
                     0, cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize());
        sanityCheckWorkload(sources, variant);
        return cfs;
    }

    /** The workload must actually read merged data of the intended shape, or the recorded
     *  baseline measures nothing (same discipline as the allocation gate's workload check). */
    private void sanityCheckWorkload(int sources, Variant variant) throws Throwable
    {
        if (variant == Variant.SHADOW)
        {
            // per partition: 128 - 1 (row tombstone at ck=3) - 8 (range tombstone [40,48)) live rows
            UntypedResultSet full = execute("SELECT * FROM %s WHERE pk = ?", 0L);
            assertEquals(ROWS_PER_PARTITION - 1 - 8, full.size());
            // the surviving values are the FINAL round's (everything earlier is shadowed)
            UntypedResultSet winner = execute("SELECT v2 FROM %s WHERE pk = ? AND ck = ?", 1L, 7L);
            assertEquals("shadow-" + (sources - 1) + "-1-7", winner.one().getString("v2"));
            // cell tombstone deleted v2 but the row survives
            UntypedResultSet cell = execute("SELECT v1, v2 FROM %s WHERE pk = ? AND ck = 5", 2L);
            assertEquals(1, cell.size());
            assertTrue("v2 should be cell-tombstoned", !cell.one().has("v2"));
        }
        else
        {
            // no deletes: full grid, every cell an exact-timestamp tie won by the last round
            UntypedResultSet full = execute("SELECT * FROM %s WHERE pk = ?", 0L);
            assertEquals(ROWS_PER_PARTITION, full.size());
            UntypedResultSet winner = execute("SELECT v2 FROM %s WHERE pk = ? AND ck = ?", 1L, 7L);
            assertEquals("tie-" + (sources - 1) + "-1-7", winner.one().getString("v2"));
        }
    }

    private static com.sun.management.ThreadMXBean threadMXBean()
    {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof com.sun.management.ThreadMXBean))
            return null;
        com.sun.management.ThreadMXBean sunBean = (com.sun.management.ThreadMXBean) bean;
        if (!sunBean.isThreadAllocatedMemorySupported())
            return null;
        if (!sunBean.isThreadAllocatedMemoryEnabled())
            sunBean.setThreadAllocatedMemoryEnabled(true);
        return sunBean;
    }
}
