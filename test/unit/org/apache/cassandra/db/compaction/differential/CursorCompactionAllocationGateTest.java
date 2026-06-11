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

package org.apache.cassandra.db.compaction.differential;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Regression gate for cursor compaction's GARBAGE-FREE property: steady-state heap
 * allocation must not scale with the number of rows/cells compacted.
 *
 * Method: measure thread-allocated bytes (ThreadMXBean) around CompactionTask.execute for a
 * SMALL table and a 10x BIG table (same row shape, warmed by prior iterations, min over
 * several measured iterations to suppress transient noise), and assert the difference stays
 * under a fixed ceiling.
 *
 * The ceiling is NOT zero. JFR decomposition of the measured baseline delta (~450KB at these
 * sizes) shows it is entirely outside cursor-owned code:
 * Ref$Debug stack captures (-Dcassandra.debugrefcount=true in the ant test JVM, build.xml —
 * production default is false) scaling with buffer-chunk Ref churn, chunk-cache machinery
 * scaling with data volume, and per-key metadata (bloom/summary). The cursor reader/writer/
 * compactor hot loops contribute ZERO scaling allocation post finding #8
 * (ClusteringPrefix.Kind.values() cached as ALL_KINDS).
 * Ceiling 512KB = measured 450KB + margin; run-to-run variance of the min-of-3 measurement
 * is ~hundreds of bytes, so this trips at a regression of ~+60KB ≈ +6 bytes per row.
 * JMH gc.alloc.rate.norm remains the precision instrument; this is the always-on tripwire.
 *
 * The differential harness cannot catch allocation regressions — output bytes are identical
 * whether or not the path allocates — hence this separate gate.
 */
public class CursorCompactionAllocationGateTest extends DifferentialCompactionTester
{
    private static final int SMALL_ROWS_PER_PARTITION = 100;
    private static final int SMALL_PARTITIONS = 6;
    private static final int SCALE = 10;
    private static final int WARMUP_ITERATIONS = 4;
    private static final int MEASURED_ITERATIONS = 3;
    private static final long CEILING_BYTES = 512 * 1024;

    /** Format variants override: BTI adds inherent per-partition index work (trie nodes,
     *  key snapshot, builder internals — paid by the iterator path too). */
    protected long ceilingBytes()
    {
        return CEILING_BYTES;
    }

    @Test
    public void allocationDoesNotScaleWithRows() throws Exception
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          threadMXBean != null);

        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            long smallAlloc = measureSteadyStateAllocation(threadMXBean, SMALL_PARTITIONS, true);
            long bigAlloc = measureSteadyStateAllocation(threadMXBean, SMALL_PARTITIONS * SCALE, true);
            long delta = bigAlloc - smallAlloc;

            // iterator-path numbers measured purely for context in the log: the iterator
            // allocates per row/cell BY DESIGN and is not gated
            long smallIter = measureSteadyStateAllocation(threadMXBean, SMALL_PARTITIONS, false);
            long bigIter = measureSteadyStateAllocation(threadMXBean, SMALL_PARTITIONS * SCALE, false);

            logger.info("cursor compaction allocation: small={}B big={}B delta={}B ceiling={}B " +
                        "(iterator path for context: small={}B big={}B delta={}B)",
                        smallAlloc, bigAlloc, delta, ceilingBytes(),
                        smallIter, bigIter, bigIter - smallIter);
            assertTrue(String.format("cursor compaction allocation scales with data: " +
                                     "%,dB (small) -> %,dB (big), delta %,dB exceeds ceiling %,dB. " +
                                     "A per-row/cell allocation has been introduced on the cursor hot path.",
                                     smallAlloc, bigAlloc, delta, CEILING_BYTES),
                       delta <= ceilingBytes());
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
        }
    }

    private long measureSteadyStateAllocation(com.sun.management.ThreadMXBean threadMXBean, int partitions, boolean cursor) throws Exception
    {
        return measureSteadyStateAllocation(threadMXBean, partitions, cursor, 2, "val", WARMUP_ITERATIONS, MEASURED_ITERATIONS);
    }

    private long measureSteadyStateAllocation(com.sun.management.ThreadMXBean threadMXBean, int partitions, boolean cursor,
                                              int rounds, String valuePadding, int warmup, int measured) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < rounds; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                    execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, valuePadding + ck);
            flush();
        }

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        // guard against vacuous measurement: the gate is meaningless if the cursor run
        // silently fell back to the iterator pipeline
        if (cursor)
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

        lastInputBytes = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            lastInputBytes += sstable.onDiskLength();

        long best = Long.MAX_VALUE;
        for (int i = 0; i < warmup + measured; i++)
        {
            long allocated = compactOnceMeasured(threadMXBean, cfs, gcBefore);
            if (i >= warmup)
                best = Math.min(best, allocated);
        }
        return best;
    }

    /** Total on-disk input bytes of the most recent measureSteadyStateAllocation call. */
    private long lastInputBytes;

    /**
     * Measurement at realistic file sizes (4 input sstables of ~10MB each, uncompressed):
     * 192 partitions x 100 rows x ~520B rows per file vs a 10x-smaller small run. Logs the
     * numbers for calibration; the scaling assertion uses a provisional ceiling of 4x the
     * small-file gate's, pending calibration from logged runs.
     */
    @Test
    public void allocationAtLargeFileSizes() throws Exception
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue(threadMXBean != null);

        String padding = "v".repeat(500);
        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        try
        {
            // 4 rounds = 4 input files; big: 192 partitions * 100 rows * ~520B = ~10MB/file
            long smallAlloc = measureSteadyStateAllocation(threadMXBean, 19, true, 4, padding, 2, 2);
            long smallBytes = lastInputBytes;
            long bigAlloc = measureSteadyStateAllocation(threadMXBean, 192, true, 4, padding, 2, 2);
            long bigBytes = lastInputBytes;
            long delta = bigAlloc - smallAlloc;
            long extraBytes = bigBytes - smallBytes;
            double perInputByte = (double) delta / extraBytes;
            long smallIter = measureSteadyStateAllocation(threadMXBean, 19, false, 4, padding, 2, 2);
            long bigIter = measureSteadyStateAllocation(threadMXBean, 192, false, 4, padding, 2, 2);

            logger.info("LARGE-FILE cursor compaction allocation (4 files, ~10MB each big): " +
                        "cursor small={}B big={}B delta={}B over {}B extra input = {}B/B; " +
                        "iterator small={}B big={}B delta={}B",
                        smallAlloc, bigAlloc, delta, extraBytes, String.format("%.3f", perInputByte),
                        smallIter, bigIter, bigIter - smallIter);
            // The residual scales with data VOLUME, not row count: JFR decomposition at this
            // scale = 62% Ref$Debug stack captures (test-env only,
            // -Dcassandra.debugrefcount=true), chunk-cache machinery, per-compaction
            // constants — ZERO cursor-owned sites. Measured ~0.27 B allocated per extra
            // input byte in the test env; ceiling 0.5 B/B trips on any real per-element
            // regression while absorbing volume-proportional test-env noise.
            assertTrue(String.format("cursor allocation per input byte too high: %.3f B/B (delta %,dB over %,dB)",
                                     perInputByte, delta, extraBytes),
                       perInputByte <= 0.5);
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
        }
    }

    /** Compacts all live sstables on the cursor path, measuring ONLY execute(); restores inputs. */
    private long compactOnceMeasured(com.sun.management.ThreadMXBean threadMXBean, ColumnFamilyStore cfs, long gcBefore) throws Exception
    {
        Set<SSTableReader> inputs = new HashSet<>(cfs.getLiveSSTables());
        Set<Descriptor> liveBeforeDescs = new HashSet<>();
        List<Descriptor> inputDescriptors = new ArrayList<>();
        for (SSTableReader in : inputs)
        {
            liveBeforeDescs.add(in.descriptor);
            inputDescriptors.add(in.descriptor);
        }

        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting", txn);
        CompactionTask task = new CompactionTask(cfs, txn, gcBefore, true /* keepOriginals */);

        long tid = Thread.currentThread().getId();
        long before = threadMXBean.getThreadAllocatedBytes(tid);
        task.execute(ActiveCompactionsTracker.NOOP);
        long allocated = threadMXBean.getThreadAllocatedBytes(tid) - before;

        List<SSTableReader> retainedInputClones = new ArrayList<>();
        List<SSTableReader> outputs = identifyOutputs(cfs, liveBeforeDescs, liveBeforeDescs, retainedInputClones);
        restoreAfterCompaction(cfs, outputs, retainedInputClones, inputDescriptors, inputs.size());
        return allocated;
    }

    /**
     * Sparse rows: every other row omits a column, so the row carries a column-subset
     * encoding instead of the all-columns flag. Exercises the per-row subset path in
     * SSTableCursorReader (UnfilteredDescriptor.loadRow -> Columns.deserializeSubset ->
     * CellCursor.init identity-cache miss), which the full-row scenario cannot see.
     */
    @Test
    public void allocationDoesNotScaleWithSparseRows() throws Exception
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue(threadMXBean != null);

        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            long smallAlloc = measureSparse(threadMXBean, SMALL_PARTITIONS);
            long bigAlloc = measureSparse(threadMXBean, SMALL_PARTITIONS * SCALE);
            long delta = bigAlloc - smallAlloc;
            logger.info("sparse-row cursor compaction allocation: small={}B big={}B delta={}B ceiling={}B",
                        smallAlloc, bigAlloc, delta, ceilingBytes());
            assertTrue(String.format("sparse-row cursor compaction allocation scales with data: " +
                                     "%,dB -> %,dB, delta %,dB exceeds ceiling %,dB",
                                     smallAlloc, bigAlloc, delta, CEILING_BYTES),
                       delta <= ceilingBytes());
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
        }
    }

    private long measureSparse(com.sun.management.ThreadMXBean threadMXBean, int partitions) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                {
                    if (ck % 2 == 0)
                        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck);
                    else
                        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "val" + ck);
                }
            flush();
        }
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);
        long best = Long.MAX_VALUE;
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++)
        {
            long allocated = compactOnceMeasured(threadMXBean, cfs, gcBefore);
            if (i >= WARMUP_ITERATIONS)
                best = Math.min(best, allocated);
        }
        return best;
    }

    /**
     * Garbage-free property for RANGE-TOMBSTONE-dense workloads (task-15 M6): the marker
     * read/merge/write path runs on the ReusableDeletionTime pool and open-marker tracking,
     * which the row-centric gates barely touch. Each partition carries 300 bounded range
     * tombstones per round, the second round's bounds shifted by one so every marker pair
     * overlaps across sstables and forces real deletion reconciliation.
     *
     * Asserted per INPUT BYTE, not as a fixed delta: at marker-dense (sub-MB) scales the
     * test-env residual (Ref$Debug stack captures, chunk-cache machinery) exceeds any fixed
     * ceiling — the same calibration lesson as the complex-column gate. JFR attribution of
     * the first run (recordRangeTombstoneAllocationProfile): ZERO cursor-owned sites; the
     * profile is Ref$Debug + buffer-pool + per-compaction constants. Markers are ~25-35B on
     * disk, so a leak of one small object per marker costs >1.5 B/B and trips the 1.0 B/B
     * ceiling with wide margin.
     */
    @Test
    public void allocationDoesNotScaleWithRangeTombstones() throws Exception
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue(threadMXBean != null);

        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            long smallAlloc = measureRangeTombstones(threadMXBean, 12);
            long smallBytes = lastInputBytes;
            long bigAlloc = measureRangeTombstones(threadMXBean, 96);
            long bigBytes = lastInputBytes;
            long delta = bigAlloc - smallAlloc;
            long extraBytes = bigBytes - smallBytes;
            double perInputByte = (double) delta / extraBytes;
            logger.info("RT-dense cursor compaction allocation: small={}B big={}B delta={}B " +
                        "over {}B extra input = {} B/B (ceiling {} B/B)",
                        smallAlloc, bigAlloc, delta, extraBytes,
                        String.format("%.3f", perInputByte), rtPerInputByteCeiling());
            assertTrue(String.format("RT-dense cursor compaction allocation scales with markers: " +
                                     "%,dB -> %,dB, delta %,dB over %,dB extra input = %.3f B/B exceeds " +
                                     "ceiling %.2f B/B. A per-marker allocation has been introduced on " +
                                     "the cursor hot path.",
                                     smallAlloc, bigAlloc, delta, extraBytes, perInputByte,
                                     rtPerInputByteCeiling()),
                       perInputByte <= rtPerInputByteCeiling());
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
        }
    }

    /** Calibrated from measured 0.684 B/B (BIG) — all test-env residual by JFR attribution
     *  (Ref$Debug, buffer pool; zero cursor frames). Format variants override: BTI pays an
     *  inherent per-partition index cost (trie nodes, key snapshots — the iterator pays it
     *  too) that dominates on tiny marker-dense partitions. */
    protected double rtPerInputByteCeiling()
    {
        return 1.0;
    }

    private long measureRangeTombstones(com.sun.management.ThreadMXBean threadMXBean, int partitions) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
            {
                // a few surviving rows well outside the tombstoned ck range
                for (long r = 0; r < 5; r++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 100_000L + r, "v" + r);
                // 300 bounded RTs; round 1 shifts bounds by 1 so markers overlap across rounds
                for (long t = 0; t < 300; t++)
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?",
                            pk, t * 4 + round, t * 4 + round + 2);
            }
            flush();
        }
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);
        lastInputBytes = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            lastInputBytes += sstable.onDiskLength();
        long best = Long.MAX_VALUE;
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++)
        {
            long allocated = compactOnceMeasured(threadMXBean, cfs, gcBefore);
            if (i >= WARMUP_ITERATIONS)
                best = Math.min(best, allocated);
        }
        return best;
    }

    /**
     * Garbage-free property for COMPLEX (multi-cell) columns through the full cursor
     * pipeline: read + per-(column,path) merge + marker/assembly write. Every row carries a
     * map and a set (plus a complex deletion from the collection-literal INSERT), and the
     * two rounds force real path-level merging. Same ceiling as the other gates.
     */
    @Test
    public void allocationDoesNotScaleWithComplexColumns() throws Exception
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue(threadMXBean != null);

        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            long smallAlloc = measureComplex(threadMXBean, 19);
            long smallBytes = lastInputBytes;
            long bigAlloc = measureComplex(threadMXBean, 192);
            long bigBytes = lastInputBytes;
            long delta = bigAlloc - smallAlloc;
            long extraBytes = bigBytes - smallBytes;
            double perInputByte = (double) delta / extraBytes;
            logger.info("complex-column cursor compaction allocation: small={}B big={}B delta={}B " +
                        "over {}B extra input = {} B/B",
                        smallAlloc, bigAlloc, delta, extraBytes, String.format("%.3f", perInputByte));
            // Measured at the same multi-MB scale as allocationAtLargeFileSizes so the
            // calibrated 0.5 B/B ceiling applies (at sub-MB inputs the volume-proportional
            // test-env residual exceeds 1 B/B for ANY scenario, simple included). JFR over 30
            // warmed complex compactions (recordComplexAllocationProfile) attributes ZERO
            // scaling allocation to cursor frames: the profile is the per-compaction
            // histogram-spool constants in maybeSwitchWriter, which cancel in the delta.
            assertTrue(String.format("complex-column cursor allocation per input byte too high: " +
                                     "%.3f B/B (delta %,dB over %,dB extra input)",
                                     perInputByte, delta, extraBytes),
                       perInputByte <= 0.5);
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
        }
    }

    private long measureComplex(com.sun.management.ThreadMXBean threadMXBean, int partitions) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String padding = "x".repeat(180); // multi-MB inputs so the 0.5 B/B residual calibration applies
        for (int round = 0; round < 4; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                {
                    execute("INSERT INTO %s (pk, ck, m, s, v) VALUES (?, ?, ?, ?, ?)",
                            pk, ck, map("k" + ck, ck, "r" + round, (long) round), set((int) ck, round), padding + ck);
                    if (ck % 4 == 0)
                        execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "extra", ck, pk, ck);
                }
            flush();
        }
        lastInputBytes = 0;
        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : cfs.getLiveSSTables())
            lastInputBytes += sstable.onDiskLength();
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 4; i++)
        {
            long allocated = compactOnceMeasured(threadMXBean, cfs, gcBefore);
            if (i >= 2)
                best = Math.min(best, allocated);
        }
        return best;
    }

    /**
     * Diagnostic, not a gate: records JFR allocation events (with stacks) over many warmed
     * cursor compactions of the big table and dumps to /tmp/cursor-alloc.jfr for offline
     * attribution of the scaling allocation (jfr print + aggregation). Always passes.
     */
    @Test
    public void recordAllocationProfile() throws Exception
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue(threadMXBean != null);

        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'enabled': 'false'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            int partitions = SMALL_PARTITIONS * SCALE;
            for (int round = 0; round < 2; round++)
            {
                for (long pk = 0; pk < partitions; pk++)
                    for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "val" + ck);
                flush();
            }
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

            for (int i = 0; i < WARMUP_ITERATIONS; i++)
                compactOnceMeasured(threadMXBean, cfs, gcBefore);

            try (jdk.jfr.Recording recording = new jdk.jfr.Recording())
            {
                recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
                recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
                recording.start();
                for (int i = 0; i < 30; i++)
                    compactOnceMeasured(threadMXBean, cfs, gcBefore);
                recording.stop();
                recording.dump(java.nio.file.Path.of("/tmp/cursor-alloc.jfr"));
            }
            logger.info("allocation profile dumped to /tmp/cursor-alloc.jfr");
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
        }
    }

    /** Diagnostic, not a gate: JFR allocation profile over warmed cursor compactions of the
     *  big RANGE-TOMBSTONE-dense table; dumps /tmp/cursor-alloc-rt.jfr for attribution. */
    @Test
    public void recordRangeTombstoneAllocationProfile() throws Exception
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue(threadMXBean != null);

        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            int partitions = SMALL_PARTITIONS * SCALE;
            for (int round = 0; round < 2; round++)
            {
                for (long pk = 0; pk < partitions; pk++)
                {
                    for (long r = 0; r < 5; r++)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 100_000L + r, "v" + r);
                    for (long t = 0; t < 150; t++)
                        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?",
                                pk, t * 4 + round, t * 4 + round + 2);
                }
                flush();
            }
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

            for (int i = 0; i < WARMUP_ITERATIONS; i++)
                compactOnceMeasured(threadMXBean, cfs, gcBefore);

            try (jdk.jfr.Recording recording = new jdk.jfr.Recording())
            {
                recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
                recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
                recording.start();
                for (int i = 0; i < 30; i++)
                    compactOnceMeasured(threadMXBean, cfs, gcBefore);
                recording.stop();
                recording.dump(java.nio.file.Path.of("/tmp/cursor-alloc-rt.jfr"));
            }
            logger.info("allocation profile dumped to /tmp/cursor-alloc-rt.jfr");
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
        }
    }

    /** Diagnostic, not a gate: JFR allocation profile over warmed cursor compactions of the
     *  big COMPLEX table; dumps /tmp/cursor-alloc-complex.jfr for offline attribution. */
    @Test
    public void recordComplexAllocationProfile() throws Exception
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue(threadMXBean != null);

        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, v text, " +
                        "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            int partitions = SMALL_PARTITIONS * SCALE;
            for (int round = 0; round < 2; round++)
            {
                for (long pk = 0; pk < partitions; pk++)
                    for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                    {
                        execute("INSERT INTO %s (pk, ck, m, s, v) VALUES (?, ?, ?, ?, ?)",
                                pk, ck, map("k" + ck, ck, "r" + round, (long) round), set((int) ck, round), "v" + ck);
                        if (ck % 4 == 0)
                            execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "extra", ck, pk, ck);
                    }
                flush();
            }
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

            for (int i = 0; i < WARMUP_ITERATIONS; i++)
                compactOnceMeasured(threadMXBean, cfs, gcBefore);

            try (jdk.jfr.Recording recording = new jdk.jfr.Recording())
            {
                recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
                recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
                recording.start();
                for (int i = 0; i < 30; i++)
                    compactOnceMeasured(threadMXBean, cfs, gcBefore);
                recording.stop();
                recording.dump(java.nio.file.Path.of("/tmp/cursor-alloc-complex.jfr"));
            }
            logger.info("allocation profile dumped to /tmp/cursor-alloc-complex.jfr");
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
        }
    }

    /** Diagnostic: JFR profile at large file sizes; dumps /tmp/cursor-alloc-large.jfr. */
    @Test
    public void recordLargeFileAllocationProfile() throws Exception
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue(threadMXBean != null);
        String padding = "v".repeat(500);
        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'enabled': 'false'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int round = 0; round < 4; round++)
            {
                for (long pk = 0; pk < 192; pk++)
                    for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, padding + ck);
                flush();
            }
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);
            for (int i = 0; i < 2; i++)
                compactOnceMeasured(threadMXBean, cfs, gcBefore);
            try (jdk.jfr.Recording recording = new jdk.jfr.Recording())
            {
                recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
                recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
                recording.start();
                for (int i = 0; i < 8; i++)
                    compactOnceMeasured(threadMXBean, cfs, gcBefore);
                recording.stop();
                recording.dump(java.nio.file.Path.of("/tmp/cursor-alloc-large.jfr"));
            }
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
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
