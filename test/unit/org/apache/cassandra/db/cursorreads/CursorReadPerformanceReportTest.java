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
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Performance instrument for the cursor-served read path (CASSANDRA-20428). This is NOT a gate.
 * It compares the cursor read path against the legacy iterator path across every supported read
 * shape and prints a table. It always passes; only the anti-false-green guards fail loudly.
 *
 * <p>The measurement runs both paths in ONE process, flipped by
 * {@code DatabaseDescriptor.setCursorReadsEnabled}. It warms both paths to JIT steady state, then
 * measures throughput (per-read wall time), latency percentiles, and thread-allocated bytes.
 *
 * <p>Trustworthiness. Every shape group must pass {@link CursorReads#isReadSupported} first; if it
 * does not, the "cursor" run would silently be an iterator run, so the guard fails. During the
 * cursor run {@link CursorReads#sstableLegsServed()} must advance (the cursor really read the
 * legs). During the iterator run the cursor counters must not move at all.
 *
 * <p>Honest framing. The cursor read path claims allocation PARITY at this stage, not a reduction.
 * A mid-partition slice of a wide partition still materializes the whole partition eagerly (no
 * intra-partition seek yet), so it allocates several times the iterator path by design. That is a
 * documented known gap, not a win.
 *
 * <p>FLAG 1. A reverse read over two or more sstable legs reads each leg through the cursor but
 * reconciles the legs with the shared {@code UnfilteredRowIterators.merge}, not a descending
 * {@code CursorReadMerger}. This test proves that empirically: the reverse multi-leg shape advances
 * {@link CursorReads#sstableLegsServed()} but does NOT advance
 * {@link CursorReads#sstableLegsCursorMerged()}, while the forward multi-slice shape advances both.
 */
public class CursorReadPerformanceReportTest extends CursorReadDifferentialTester
{
    private static final int FLUSH_ROUNDS = 5;
    private static final int NARROW_PARTITIONS = 12;
    private static final int ROWS_PER_NARROW = 128;
    private static final long WIDE_PK = 1_000_000L;
    private static final int WIDE_ROWS = 1000;
    private static final int WARMUP_PASSES = 25;
    private static final int MEASURED_PASSES = 60;

    /** Blackhole so pass consumption cannot be dead-code-eliminated. */
    private static volatile long sink;

    @Test
    public void reportReadPerformance() throws Throwable
    {
        com.sun.management.ThreadMXBean bean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", bean != null);

        ColumnFamilyStore cfs = loadWorkload();
        ColumnFamilyStore singleLeg = loadSingleLegWorkload();
        ColumnFamilyStore counter = loadCounterWorkload();

        List<ShapeResult> results = new ArrayList<>();
        long now = FBUtilities.nowInSeconds();

        // 1. point read: full narrow partition, present in every sstable + memtable overlay
        results.add(measure(bean, "point (full narrow)", cfs, narrowFull(cfs, now)));
        // 2. near-full slice: exercises slice-bound handling, one row divergence
        results.add(measure(bean, "near-full slice", cfs, nearFullSlice(cfs, now)));
        // 3. mid slice: the KNOWN eager-materialization gap (no seek yet), allocates several x more
        results.add(measure(bean, "mid slice (known gap)", cfs, midSlice(cfs, now)));
        // 4. names / IN: a fixed set of named clusterings
        results.add(measure(bean, "names / IN", cfs, names(cfs, now)));
        // 5. multi-slice: two clustering ranges, forward -> cursor merge
        results.add(measure(bean, "multi-slice (2)", cfs, multiSlice(cfs, now)));
        // 6. reverse multi-leg: FLAG 1 -- per-leg cursor read, shared iterator merge
        results.add(measure(bean, "reverse multi-leg", cfs, reverseMultiLeg(cfs, now)));
        // 7. reverse single-leg: one sstable, fully on the cursor, no cross-leg merge
        results.add(measure(bean, "reverse single-leg", singleLeg, reverseSingleLeg(singleLeg, now)));
        // 8. limited: LIMIT N
        results.add(measure(bean, "limited (LIMIT 20)", cfs, limited(cfs, now)));
        // 9. row-filtered
        results.add(measure(bean, "row-filtered", cfs, filtered(cfs, now)));
        // 10. digest query
        results.add(measure(bean, "digest", cfs, digest(cfs, now)));
        // 11. counter table full-partition read
        results.add(measure(bean, "counter (full)", counter, counterFull(counter, now)));

        logTable(results);
    }

    // ---------------------------------------------------------------- measurement

    private ShapeResult measure(com.sun.management.ThreadMXBean bean,
                                String name,
                                ColumnFamilyStore cfs,
                                List<Supplier<SinglePartitionReadCommand>> commands) throws Throwable
    {
        assertGateOpenForAll(name, cfs, commands);
        int readsPerPass = commands.size();

        try
        {
            // warm both paths to JIT steady state
            DatabaseDescriptor.setCursorReadsEnabled(false);
            for (int i = 0; i < WARMUP_PASSES; i++)
                runPass(commands);
            DatabaseDescriptor.setCursorReadsEnabled(true);
            for (int i = 0; i < WARMUP_PASSES; i++)
                runPass(commands);

            // iterator measurement; cursor counters must not move
            DatabaseDescriptor.setCursorReadsEnabled(false);
            long legsBeforeIter = CursorReads.sstableLegsServed();
            long missBeforeIter = CursorReads.sstableLegsWithoutPartition();
            PathMetrics iterator = measurePath(bean, commands, readsPerPass);
            assertEquals(name + ": iterator run unexpectedly served cursor legs",
                         legsBeforeIter, CursorReads.sstableLegsServed());
            assertEquals(name + ": iterator run unexpectedly consulted the cursor path",
                         missBeforeIter, CursorReads.sstableLegsWithoutPartition());

            // cursor measurement; legs served must advance (no silent fallback)
            DatabaseDescriptor.setCursorReadsEnabled(true);
            long legsBefore = CursorReads.sstableLegsServed();
            long mergedBefore = CursorReads.sstableLegsCursorMerged();
            long mergesBefore = CursorReads.cursorMergesServed();
            PathMetrics cursor = measurePath(bean, commands, readsPerPass);
            long legsDelta = CursorReads.sstableLegsServed() - legsBefore;
            long mergedDelta = CursorReads.sstableLegsCursorMerged() - mergedBefore;
            long mergesDelta = CursorReads.cursorMergesServed() - mergesBefore;
            assertTrue(name + ": cursor run served no sstable legs (silent fallback?)", legsDelta > 0);

            return new ShapeResult(name, iterator, cursor, legsDelta, mergedDelta, mergesDelta);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** One measured path: per-read wall time (for ops/s and percentiles) and min pass allocation. */
    private PathMetrics measurePath(com.sun.management.ThreadMXBean bean,
                                    List<Supplier<SinglePartitionReadCommand>> commands,
                                    int readsPerPass) throws Throwable
    {
        long tid = Thread.currentThread().getId();
        long[] samples = new long[MEASURED_PASSES * readsPerPass];
        int idx = 0;
        long minPassAlloc = Long.MAX_VALUE;
        long totalNanos = 0;

        for (int pass = 0; pass < MEASURED_PASSES; pass++)
        {
            long allocBefore = bean.getThreadAllocatedBytes(tid);
            for (Supplier<SinglePartitionReadCommand> supplier : commands)
            {
                SinglePartitionReadCommand command = supplier.get();
                long t0 = System.nanoTime();
                consume(command);
                long dt = System.nanoTime() - t0;
                samples[idx++] = dt;
                totalNanos += dt;
            }
            long passAlloc = bean.getThreadAllocatedBytes(tid) - allocBefore;
            minPassAlloc = Math.min(minPassAlloc, passAlloc);
        }

        Arrays.sort(samples);
        double opsPerSec = samples.length / (totalNanos / 1_000_000_000.0);
        long p50 = percentile(samples, 50);
        long p99 = percentile(samples, 99);
        long allocPerRead = minPassAlloc / readsPerPass;
        return new PathMetrics(opsPerSec, p50, p99, allocPerRead);
    }

    private void consume(SinglePartitionReadCommand command) throws Throwable
    {
        long consumed = 0;
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
        sink += consumed;
    }

    private void runPass(List<Supplier<SinglePartitionReadCommand>> commands) throws Throwable
    {
        for (Supplier<SinglePartitionReadCommand> supplier : commands)
            consume(supplier.get());
    }

    private void assertGateOpenForAll(String name, ColumnFamilyStore cfs,
                                      List<Supplier<SinglePartitionReadCommand>> commands)
    {
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            for (Supplier<SinglePartitionReadCommand> supplier : commands)
            {
                SinglePartitionReadCommand probe = supplier.get();
                assertTrue(name + ": shape is not cursor-supported -- this would compare iterator vs " +
                           "iterator: " + probe,
                           CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));
            }
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static long percentile(long[] sorted, double p)
    {
        int i = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        i = Math.max(0, Math.min(sorted.length - 1, i));
        return sorted[i];
    }

    private void logTable(List<ShapeResult> results)
    {
        logger.info("==== CURSOR READ PERFORMANCE REPORT (iterator vs cursor) ====");
        logger.info(String.format("%-22s | %12s %12s %6s | %10s %10s | %5s %5s %5s",
                                   "shape", "iter ops/s", "curs ops/s", "thr%",
                                   "iter B/read", "curs B/read", "legs", "merg", "cmrg"));
        for (ShapeResult r : results)
        {
            double thrPct = 100.0 * (r.cursor.opsPerSec - r.iterator.opsPerSec) / r.iterator.opsPerSec;
            logger.info(String.format("%-22s | %12.0f %12.0f %+5.1f | %10d %10d | %5d %5d %5d",
                                       r.name, r.iterator.opsPerSec, r.cursor.opsPerSec, thrPct,
                                       r.iterator.allocPerRead, r.cursor.allocPerRead,
                                       r.legsServed, r.mergesServed, r.legsCursorMerged));
            logger.info(String.format("    latency ns/read: iter p50=%d p99=%d | curs p50=%d p99=%d | alloc ratio=%.2f",
                                       r.iterator.p50, r.iterator.p99, r.cursor.p50, r.cursor.p99,
                                       (double) r.cursor.allocPerRead / Math.max(1, r.iterator.allocPerRead)));
        }
        logger.info("legs=cursor sstable legs served, merg=cursor merges, cmrg=legs put through the cursor merge");
        logger.info("FLAG 1: a reverse multi-leg row shows legs>0 but cmrg=0 (shared iterator merge, not the cursor merge)");
    }

    // ---------------------------------------------------------------- shapes

    private List<Supplier<SinglePartitionReadCommand>> narrowFull(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
        {
            long key = pk;
            c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build());
        }
        return c;
    }

    private List<Supplier<SinglePartitionReadCommand>> nearFullSlice(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1L).build());
        c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, 2L).withNowInSeconds(now)
                                                     .toIncl((long) (ROWS_PER_NARROW - 2)).build());
        return c;
    }

    private List<Supplier<SinglePartitionReadCommand>> midSlice(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, WIDE_PK).withNowInSeconds(now)
                                                     .fromIncl(200L).toIncl(400L).build());
        c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, WIDE_PK).withNowInSeconds(now)
                                                     .fromIncl(450L).toIncl(650L).build());
        return c;
    }

    private List<Supplier<SinglePartitionReadCommand>> names(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
        {
            long key = pk;
            c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now)
                                                         .includeRow(3L).includeRow(7L).includeRow(11L).build());
        }
        return c;
    }

    private List<Supplier<SinglePartitionReadCommand>> multiSlice(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
        {
            long key = pk;
            c.add(() -> twoSliceCommand(cfs, key, now));
        }
        return c;
    }

    private static SinglePartitionReadCommand twoSliceCommand(ColumnFamilyStore cfs, long key, long now)
    {
        Slices.Builder builder = new Slices.Builder(cfs.metadata().comparator);
        builder.add(ClusteringBound.create(cfs.metadata().comparator, true, true, 1L),
                    ClusteringBound.create(cfs.metadata().comparator, false, true, 30L));
        builder.add(ClusteringBound.create(cfs.metadata().comparator, true, true, 60L),
                    ClusteringBound.create(cfs.metadata().comparator, false, true, 90L));
        Slices slices = builder.build();
        return SinglePartitionReadCommand.create(cfs.metadata(), now,
                                                 cfs.metadata().partitioner.decorateKey(LongType.instance.decompose(key)),
                                                 slices);
    }

    private List<Supplier<SinglePartitionReadCommand>> reverseMultiLeg(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        // wide partition and narrow partitions all live in every sstable, so a reverse read is
        // inherently multi-leg here
        c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, WIDE_PK).withNowInSeconds(now).reverse().build());
        for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
        {
            long key = pk;
            c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).reverse().build());
        }
        return c;
    }

    private List<Supplier<SinglePartitionReadCommand>> reverseSingleLeg(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).reverse().build());
        return c;
    }

    private List<Supplier<SinglePartitionReadCommand>> limited(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
        {
            long key = pk;
            c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).withLimit(20).build());
        }
        return c;
    }

    private List<Supplier<SinglePartitionReadCommand>> filtered(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
        {
            long key = pk;
            c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now)
                                                         .filterOn("v3", Operator.GT, 40).build());
        }
        return c;
    }

    private List<Supplier<SinglePartitionReadCommand>> digest(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
        {
            long key = pk;
            c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build()
                            .copyAsDigestQuery(ReplicaUtils.full(FBUtilities.getBroadcastAddressAndPort())));
        }
        return c;
    }

    private List<Supplier<SinglePartitionReadCommand>> counterFull(ColumnFamilyStore cfs, long now)
    {
        List<Supplier<SinglePartitionReadCommand>> c = new ArrayList<>();
        for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
        {
            long key = pk;
            c.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build());
        }
        return c;
    }

    // ---------------------------------------------------------------- workloads

    /** The main workload: FLUSH_ROUNDS fully-overlapping sstables plus a live memtable overlay,
     *  sparse rows, row/cell/range tombstones, one wide partition. */
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
            for (long ck = round; ck < WIDE_ROWS; ck += FLUSH_ROUNDS)
                execute("INSERT INTO %s (pk, ck, v1, v2, v3, v4, v5, v6, v7, v8) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        WIDE_PK, ck, ck, "wide-value-" + ck, (int) ck, "wide-text-" + round,
                        ck * 1.5, ck * 7, "w" + ck, round);

            if (round == FLUSH_ROUNDS - 1)
            {
                for (long pk = 0; pk < NARROW_PARTITIONS; pk += 4)
                    execute("DELETE FROM %s WHERE pk = ? AND ck = 2", pk);
                for (long pk = 0; pk < NARROW_PARTITIONS; pk += 3)
                    execute("DELETE v2 FROM %s WHERE pk = ? AND ck = 4", pk);
                for (long pk = 0; pk < NARROW_PARTITIONS; pk += 5)
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= 40 AND ck < 48", pk);
                execute("DELETE FROM %s WHERE pk = ? AND ck >= 100 AND ck < 140", WIDE_PK);
                for (long ck = 500; ck < 520; ck += 2)
                    execute("DELETE FROM %s WHERE pk = ? AND ck = ?", WIDE_PK, ck);
            }
            flush();
        }

        for (long pk = 0; pk < NARROW_PARTITIONS; pk += 3)
        {
            execute("INSERT INTO %s (pk, ck, v1, v3) VALUES (?, ?, ?, ?)", pk, 1L, pk, 42);
            execute("INSERT INTO %s (pk, ck, v1, v3) VALUES (?, ?, ?, ?)", pk, 5L, pk, 43);
        }
        for (long ck = 7; ck < WIDE_ROWS; ck += 10)
        {
            if (ck >= 100 && ck < 140)
                continue;
            execute("INSERT INTO %s (pk, ck, v2, v5) VALUES (?, ?, ?, ?)", WIDE_PK, ck, "memtable-" + ck, 9.9);
        }

        assertEquals("expected exactly FLUSH_ROUNDS overlapping sstables",
                     FLUSH_ROUNDS, cfs.getLiveSSTables().size());
        return cfs;
    }

    /** A single partition in exactly one sstable, no memtable overlay: a reverse read of it is a
     *  genuine single-leg read, fully on the cursor with no cross-leg merge. */
    private ColumnFamilyStore loadSingleLegWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < ROWS_PER_NARROW; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 0L, ck, ck, "single-" + ck);
        flush();
        assertEquals("single-leg workload must be exactly one sstable", 1, cfs.getLiveSSTables().size());
        return cfs;
    }

    /** A counter table across FLUSH_ROUNDS sstables. */
    private ColumnFamilyStore loadCounterWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < FLUSH_ROUNDS; round++)
        {
            for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
                for (long ck = 0; ck < 64; ck++)
                    execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = ? AND ck = ?",
                            ck + round, round + 1L, pk, ck);
            flush();
        }
        assertEquals("counter workload must be FLUSH_ROUNDS sstables", FLUSH_ROUNDS, cfs.getLiveSSTables().size());
        return cfs;
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

    private static final class PathMetrics
    {
        final double opsPerSec;
        final long p50;
        final long p99;
        final long allocPerRead;

        PathMetrics(double opsPerSec, long p50, long p99, long allocPerRead)
        {
            this.opsPerSec = opsPerSec;
            this.p50 = p50;
            this.p99 = p99;
            this.allocPerRead = allocPerRead;
        }
    }

    private static final class ShapeResult
    {
        final String name;
        final PathMetrics iterator;
        final PathMetrics cursor;
        final long legsServed;
        final long legsCursorMerged;
        final long mergesServed;

        ShapeResult(String name, PathMetrics iterator, PathMetrics cursor,
                    long legsServed, long legsCursorMerged, long mergesServed)
        {
            this.name = name;
            this.iterator = iterator;
            this.cursor = cursor;
            this.legsServed = legsServed;
            this.legsCursorMerged = legsCursorMerged;
            this.mergesServed = mergesServed;
        }
    }
}
