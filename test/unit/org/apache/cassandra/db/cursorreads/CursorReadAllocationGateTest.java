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
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Allocation gate for the Phase 1 cursor-served read slice (CASSANDRA-20428).
 *
 * Unlike {@code CursorCompactionAllocationGateTest}, which asserts an ABSOLUTE ceiling because
 * cursor compaction's property is garbage-freedom, Phase 1 of the cursor read path deliberately
 * claims NO allocation win: seam (i) materializes the same {@code Row}/{@code Cell}/{@code BTreeRow}
 * inventory as the iterator path at the {@code UnfilteredRowIterator} boundary. The property to
 * gate is therefore COMPARATIVE parity: the same workload, run through both paths, must not
 * allocate meaningfully more on the cursor path than on the iterator path. The differential
 * harness cannot catch an allocation regression — the output bytes are identical whether or not
 * the path allocates — hence this separate gate (same rationale as the compaction gate).
 *
 * Method (mirrors the compaction gate's established methodology): thread-allocated bytes via
 * {@code com.sun.management.ThreadMXBean} around whole workload passes, warmup passes first to
 * reach JIT steady state on BOTH paths, then min-of-N measured passes per path to suppress
 * transient noise. JFR is used only by the always-passing diagnostic test for offline attribution
 * when the gate fails, never for the gate assertion itself.
 *
 * Workload: the Phase 0 profile workload ({@code ReadPathAllocationProfileTest}) restricted to
 * query shapes inside the Phase 1 gate — single-partition, ASC, full-partition or single-slice,
 * memtable + {@value #FLUSH_ROUNDS} overlapping sstables, sparse rows, row/cell/range tombstones,
 * one wide partition. Shapes outside the gate (names filters/point reads, range scans, reversed)
 * would silently run iterator-vs-iterator and are excluded; the silent-fallback guard below
 * proves the cursor path really served every expected sstable leg during the cursor measurement
 * (exact {@link CursorReads#sstableLegsServed()} accounting, the same discipline as the
 * differential harness) and was never consulted during the iterator measurement.
 *
 * MARGIN ({@link #PARITY_MARGIN}) — initial value from static reasoning, to be tightened against
 * measured numbers (compaction-gate style: document the measured baseline once known):
 * <ul>
 *   <li>Per-row/cell/value inventory is identical by construction on both paths (the shim builds
 *       real {@code BTreeRow}s/{@code ArrayCell}s/value {@code byte[]}s exactly like
 *       {@code UnfilteredSerializer} deserialization does), so the expected ratio is ~1.0 plus
 *       per-sstable-leg constants.</li>
 *   <li>Cursor-side per-leg constants the iterator path does not pay: one
 *       {@code SSTableCursorReader} open per leg (descriptors, cell cursors,
 *       {@code DeserializationHelper}) plus the materializer's 4KB transfer buffer, value
 *       scratch and the eager materialized-unfiltereds list — roughly 5-10KB per leg, vs the
 *       iterator's own per-leg machinery of a few KB. The workload keeps ≥100 rows per narrow
 *       partition so per-leg inventory (~tens of KB) amortizes these constants to an expected
 *       ~5-10% of the baseline.</li>
 *   <li>15% margin therefore expects to pass with headroom while still tripping on a real
 *       regression: one extra small (~32B) object per materialized row across this workload
 *       costs ~+5-8% and a per-cell leak considerably more, both within reach of the gate;
 *       a new per-leg buffer of tens of KB trips it immediately.</li>
 * </ul>
 *
 * KNOWN, DOCUMENTED divergence kept OUT of the parity gate: Phase 1 has no intra-partition seek
 * ({@code PartitionMaterializer} materializes the WHOLE partition eagerly;
 * {@code SlicedMaterializedIterator} applies the slice afterwards), so a mid-partition slice of a
 * wide partition allocates ~(partitionRows/sliceRows)x the iterator path, which skips/seeks
 * (4KiB column index in the test config) instead of materializing. That is Phase 2 (M1) work,
 * not a Phase 1 regression; it is gated separately by
 * {@link #widePartitionMidSliceEagerMaterializationStaysBounded} with a loose, documented ceiling
 * that only catches GROSS regressions (e.g. super-linear behavior). When Phase 2 seek lands, that
 * ceiling must be tightened to the parity margin. The parity gate's slice shapes are near-full
 * (excluding a single row) so slice-handling code is exercised while the known eager
 * materialization divergence stays at one row per read.
 *
 * This test is the standing instrument for M2/M3: those milestones must show the cursor:iterator
 * ratio DECREASING below 1.0 as the merge (seam ii) and late materialization (seams iii/iv)
 * capture the real win; report the logged before/after numbers in the journal at each milestone.
 */
public class CursorReadAllocationGateTest extends CursorReadDifferentialTester
{
    private static final int FLUSH_ROUNDS = 5;
    private static final int NARROW_PARTITIONS = 12;
    private static final int ROWS_PER_NARROW = 128;
    private static final long WIDE_PK = 1_000_000L;
    private static final int WIDE_ROWS = 1000;
    private static final int WARMUP_PASSES = 20;
    private static final int MEASURED_PASSES = 8;

    /** See class javadoc for the derivation. Initial value from static reasoning; tighten once
     *  the measured steady-state ratio is documented here. */
    private static final double PARITY_MARGIN = 0.15;

    /**
     * Loose tripwire for the KNOWN Phase 1 eager-materialization gap on mid-partition slices:
     * each mid slice covers 1/5 of the wide partition, so the cursor path materializes ~5x the
     * iterator path's row inventory by design; shared merge/orchestration/memtable costs pull the
     * observed ratio below that. 6.0 only catches gross regressions (e.g. materializing the
     * partition once per slice, or super-linear behavior). MUST be tightened to
     * {@code 1 + PARITY_MARGIN} when Phase 2 (BTI row-index seek, M1) lands.
     */
    private static final double MID_SLICE_KNOWN_GAP_CEILING = 6.0;

    /** Blackhole so pass consumption cannot be dead-code-eliminated. */
    private static volatile long sink;

    @Test
    public void cursorPathAllocationParityOnPhase1Slice() throws Throwable
    {
        com.sun.management.ThreadMXBean bean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", bean != null);

        ColumnFamilyStore cfs = loadWorkload();
        List<Supplier<SinglePartitionReadCommand>> commands = parityCommands(cfs);

        long[] best = measureBothPaths(bean, cfs, commands);
        long iteratorBest = best[0];
        long cursorBest = best[1];

        double ratio = (double) cursorBest / iteratorBest;
        logger.info("cursor read allocation parity gate: iterator={}B cursor={}B ratio={} margin={}",
                    iteratorBest, cursorBest, String.format("%.4f", ratio), PARITY_MARGIN);
        assertTrue(String.format("cursor read path allocates more than the iterator path beyond the " +
                                 "parity margin: iterator=%,dB cursor=%,dB ratio=%.4f exceeds %.2f. " +
                                 "Phase 1 (seam i) claims allocation PARITY — a per-row/cell/leg " +
                                 "allocation has been introduced on the cursor read path. Run " +
                                 "recordReadAllocationProfiles and diff the JFR attributions.",
                                 iteratorBest, cursorBest, ratio, 1 + PARITY_MARGIN),
                   cursorBest <= (long) (iteratorBest * (1 + PARITY_MARGIN)));
    }

    @Test
    public void widePartitionMidSliceEagerMaterializationStaysBounded() throws Throwable
    {
        com.sun.management.ThreadMXBean bean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", bean != null);

        ColumnFamilyStore cfs = loadWorkload();
        List<Supplier<SinglePartitionReadCommand>> commands = midSliceCommands(cfs);

        long[] best = measureBothPaths(bean, cfs, commands);
        long iteratorBest = best[0];
        long cursorBest = best[1];

        double ratio = (double) cursorBest / iteratorBest;
        logger.info("cursor read mid-slice KNOWN-GAP tripwire (Phase 1 eager materialization, no seek): " +
                    "iterator={}B cursor={}B ratio={} ceiling={} — Phase 2 seek must bring this to parity",
                    iteratorBest, cursorBest, String.format("%.4f", ratio), MID_SLICE_KNOWN_GAP_CEILING);
        assertTrue(String.format("mid-partition slice cursor allocation exceeds even the documented " +
                                 "Phase 1 eager-materialization gap: iterator=%,dB cursor=%,dB " +
                                 "ratio=%.4f exceeds ceiling %.2f (expected ~<5x from whole-partition " +
                                 "materialization of 1/5-partition slices)",
                                 iteratorBest, cursorBest, ratio, MID_SLICE_KNOWN_GAP_CEILING),
                   cursorBest <= (long) (iteratorBest * MID_SLICE_KNOWN_GAP_CEILING));
    }

    /**
     * Diagnostic, not a gate: warms both paths, then dumps a JFR allocation profile (with stacks)
     * of measured passes for each path — /tmp/cursor-read-alloc-iterator.jfr and
     * /tmp/cursor-read-alloc-cursor.jfr — for offline attribution when the parity gate fails.
     * Always passes.
     */
    @Test
    public void recordReadAllocationProfiles() throws Throwable
    {
        com.sun.management.ThreadMXBean bean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", bean != null);

        ColumnFamilyStore cfs = loadWorkload();
        List<Supplier<SinglePartitionReadCommand>> commands = parityCommands(cfs);
        assertGateOpenForAll(cfs, commands);
        try
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
            for (int i = 0; i < WARMUP_PASSES; i++)
                runPass(commands);
            DatabaseDescriptor.setCursorReadsEnabled(true);
            for (int i = 0; i < WARMUP_PASSES; i++)
                runPass(commands);

            DatabaseDescriptor.setCursorReadsEnabled(false);
            recordProfile(java.nio.file.Path.of("/tmp/cursor-read-alloc-iterator.jfr"), commands);
            DatabaseDescriptor.setCursorReadsEnabled(true);
            recordProfile(java.nio.file.Path.of("/tmp/cursor-read-alloc-cursor.jfr"), commands);
            logger.info("read allocation profiles dumped to /tmp/cursor-read-alloc-{iterator,cursor}.jfr");
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private void recordProfile(java.nio.file.Path dest, List<Supplier<SinglePartitionReadCommand>> commands) throws Throwable
    {
        try (jdk.jfr.Recording recording = new jdk.jfr.Recording())
        {
            recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
            recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
            recording.start();
            for (int i = 0; i < 30; i++)
                runPass(commands);
            recording.stop();
            recording.dump(dest);
        }
    }

    // ---------------------------------------------------------------- measurement

    /**
     * Warmups then min-of-N measurement of the SAME workload through both paths, with the
     * silent-fallback guard applied to both directions:
     * <ul>
     *   <li>before anything, every command shape must pass {@link CursorReads#isReadSupported}
     *       (otherwise the "cursor" measurement would silently compare iterator vs iterator);</li>
     *   <li>the cursor warmup and measurement must serve EXACTLY the expected number of sstable
     *       legs ({@code commands x sstables x passes} — every key exists in every sstable and
     *       every slice intersects every sstable's clustering range);</li>
     *   <li>the iterator measurement must not advance the cursor counters at all.</li>
     * </ul>
     *
     * @return {iteratorBestBytes, cursorBestBytes}
     */
    private long[] measureBothPaths(com.sun.management.ThreadMXBean bean,
                                    ColumnFamilyStore cfs,
                                    List<Supplier<SinglePartitionReadCommand>> commands) throws Throwable
    {
        assertGateOpenForAll(cfs, commands);
        int sstables = cfs.getLiveSSTables().size();
        assertEquals(FLUSH_ROUNDS, sstables);
        long expectedLegsPerPass = (long) commands.size() * sstables;

        try
        {
            // warm up the iterator path, then the cursor path, so both branches of the
            // production call sites reach JIT steady state before either is measured
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

    /** Min thread-allocated bytes for one workload pass over {@link #MEASURED_PASSES} passes. */
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

    /** One workload pass; measured on both paths identically. No allocation beyond the reads
     *  themselves (fresh command objects per pass are part of read orchestration on both paths). */
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

    // ---------------------------------------------------------------- workload

    /** Parity shapes: full-partition reads (both paths materialize every row) plus near-full
     *  slices whose known eager-materialization divergence is a single row per read. */
    private List<Supplier<SinglePartitionReadCommand>> parityCommands(ColumnFamilyStore cfs)
    {
        long now = FBUtilities.nowInSeconds();
        List<Supplier<SinglePartitionReadCommand>> commands = new ArrayList<>();
        for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
        {
            long key = pk;
            commands.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build());
        }
        // near-full slices: exercise SlicedMaterializedIterator's bound handling; the iterator
        // path skips exactly one row that the cursor path still materializes (see class javadoc)
        commands.add(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1L).build());
        commands.add(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 2L).withNowInSeconds(now).toIncl((long) (ROWS_PER_NARROW - 2)).build());
        // full wide partition: 1000 rows across all sstable stripes + memtable overlay,
        // range tombstone band and row tombstones included
        commands.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, WIDE_PK).withNowInSeconds(now).build());
        return commands;
    }

    /** Mid-partition 1/5 slices of the wide partition: the KNOWN Phase 1 eager-materialization
     *  gap shape (cursor materializes all 1000 rows per leg, iterator index-seeks the slice). */
    private List<Supplier<SinglePartitionReadCommand>> midSliceCommands(ColumnFamilyStore cfs)
    {
        long now = FBUtilities.nowInSeconds();
        List<Supplier<SinglePartitionReadCommand>> commands = new ArrayList<>();
        commands.add(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, WIDE_PK).withNowInSeconds(now).fromIncl(200L).toExcl(400L).build());
        commands.add(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, WIDE_PK).withNowInSeconds(now).fromIncl(450L).toExcl(650L).build());
        commands.add(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, WIDE_PK).withNowInSeconds(now).fromIncl(700L).toExcl(900L).build());
        return commands;
    }

    /**
     * The Phase 0 profile workload restricted to gate-supported shapes: {@value #FLUSH_ROUNDS}
     * fully-overlapping sstables (every round rewrites every key) plus a live memtable overlay,
     * sparse rows (odd ck: 2 of 8 regular columns), row/cell/range tombstones in both narrow and
     * wide partitions. Narrow partitions carry {@value #ROWS_PER_NARROW} rows so per-leg
     * inventory amortizes the cursor's per-leg open constants (see margin derivation).
     */
    private ColumnFamilyStore loadWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, " +
                    "v1 bigint, v2 text, v3 int, v4 text, v5 double, v6 bigint, v7 text, v8 int, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < FLUSH_ROUNDS; round++)
        {
            for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
            {
                for (long ck = 0; ck < ROWS_PER_NARROW; ck++)
                {
                    if (ck % 2 == 1)
                        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)",
                                pk, ck, pk * 100 + ck + round, "sparse-" + round + "-" + ck);
                    else
                        execute("INSERT INTO %s (pk, ck, v1, v2, v3, v4, v5, v6, v7, v8) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                pk, ck, pk + round, "text-value-" + round + "-" + pk + "-" + ck,
                                (int) ck, "second-text-column-" + round, round + 0.5,
                                pk * ck, "third-text-" + ck, round);
                }
            }
            // wide partition: interleaved clustering stripes so every sstable contributes to
            // any slice (ck % FLUSH_ROUNDS == round)
            for (long ck = round; ck < WIDE_ROWS; ck += FLUSH_ROUNDS)
                execute("INSERT INTO %s (pk, ck, v1, v2, v3, v4, v5, v6, v7, v8) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        WIDE_PK, ck, ck, "wide-value-" + ck, (int) ck, "wide-text-" + round,
                        ck * 1.5, ck * 7, "w" + ck, round);

            // tombstones in the LAST round, written after that round's inserts so they carry
            // later timestamps and are not shadowed
            if (round == FLUSH_ROUNDS - 1)
            {
                for (long pk = 0; pk < NARROW_PARTITIONS; pk += 4)
                    execute("DELETE FROM %s WHERE pk = ? AND ck = 2", pk);                     // row tombstones
                for (long pk = 0; pk < NARROW_PARTITIONS; pk += 3)
                    execute("DELETE v2 FROM %s WHERE pk = ? AND ck = 4", pk);                 // cell tombstones
                for (long pk = 0; pk < NARROW_PARTITIONS; pk += 5)
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= 40 AND ck < 48", pk);      // narrow range tombstones
                execute("DELETE FROM %s WHERE pk = ? AND ck >= 100 AND ck < 140", WIDE_PK);   // wide range tombstone
                for (long ck = 500; ck < 520; ck += 2)
                    execute("DELETE FROM %s WHERE pk = ? AND ck = ?", WIDE_PK, ck);           // wide row tombstones
            }
            flush();
        }

        // final overlay stays in the memtable: reads genuinely merge memtable + FLUSH_ROUNDS
        // sstables (memtable legs stay on the object path on BOTH runs — they cancel)
        for (long pk = 0; pk < NARROW_PARTITIONS; pk += 3)
        {
            execute("INSERT INTO %s (pk, ck, v1, v3) VALUES (?, ?, ?, ?)", pk, 1L, pk, 42);
            execute("INSERT INTO %s (pk, ck, v1, v3) VALUES (?, ?, ?, ?)", pk, 5L, pk, 43);
        }
        for (long ck = 7; ck < WIDE_ROWS; ck += 10)
        {
            if (ck >= 100 && ck < 140)
                continue; // don't resurrect the range-tombstoned band
            execute("INSERT INTO %s (pk, ck, v2, v5) VALUES (?, ?, ?, ?)", WIDE_PK, ck, "memtable-" + ck, 9.9);
        }

        assertEquals("expected exactly FLUSH_ROUNDS overlapping sstables",
                     FLUSH_ROUNDS, cfs.getLiveSSTables().size());
        assertTrue("expected live data in the memtable",
                   cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize() > 0);
        sanityCheckWorkload();
        return cfs;
    }

    /** The workload must actually read merged data of the expected shape (a wrong workload
     *  makes the gate measure nothing). Same discipline as the Phase 0 profile harness. */
    private void sanityCheckWorkload() throws Throwable
    {
        // pk 0: row tombstone at ck=2 (pk%4==0) and range tombstone [40,48) (pk%5==0)
        UntypedResultSet full0 = execute("SELECT * FROM %s WHERE pk = ?", 0L);
        assertEquals(ROWS_PER_NARROW - 1 - 8, full0.size());
        // pk 1: untombstoned
        UntypedResultSet full1 = execute("SELECT * FROM %s WHERE pk = ?", 1L);
        assertEquals(ROWS_PER_NARROW, full1.size());
        // pk 3: cell tombstone on v2 at ck=4 (pk%3==0), row survives
        UntypedResultSet cell = execute("SELECT v2 FROM %s WHERE pk = ? AND ck = 4", 3L);
        assertEquals(1, cell.size());
        assertFalse("v2 should be cell-tombstoned", cell.one().has("v2"));
        // wide partition: 1000 - 40 (range tombstone band) - 10 (row tombstones) = 950
        UntypedResultSet wide = execute("SELECT * FROM %s WHERE pk = ?", WIDE_PK);
        assertEquals(950, wide.size());
        // memtable overlay visible in the merge
        UntypedResultSet mem = execute("SELECT v2 FROM %s WHERE pk = ? AND ck = ?", WIDE_PK, 7L);
        assertEquals("memtable-7", mem.one().getString("v2"));
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
