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

package org.apache.cassandra.db.memtable.differential;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assume;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.memtable.Flushing;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.MemtableCursorFlusher;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.utils.ThreadStats;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertTrue;

/**
 * Measures thread-allocated bytes (JFR-free, {@link ThreadStats}) around a
 * real {@link Flushing.FlushRunnable#call} for a fixed, already-built memtable, comparing the
 * iterator path against the cursor path — the actual performance claim behind CASSANDRA-21554,
 * not just its correctness.
 * <p>
 * Unlike compaction (whose {@code CompactionTask} the analogous
 * {@code CursorCompactionAllocationGateTest} invokes directly on the calling thread), flush
 * normally runs on a background per-disk flush executor
 * ({@code ColumnFamilyStore.perDiskflushExecutors}) — invisible to {@code ThreadStats} unless
 * measured on whichever thread actually does the writing. Rather than fighting that executor,
 * this drives the same {@code Flushing.flushRunnables(cfs, memtable, txn)} the real flush path
 * builds, but calls {@code FlushRunnable.call()} directly and synchronously on the test's own
 * thread — the same one {@code ThreadStats.getCurrentThreadAllocatedBytes()} then measures. Each
 * measurement uses its own offline {@link LifecycleTransaction}, aborted afterward (never
 * committed, never opened) rather than calling {@code cfs.replaceFlushed} — the memtable is
 * read, not consumed, so the same fixed memtable is flushed repeatedly across warmup/measured
 * iterations without needing to rebuild it each time.
 */
public class MemtableFlushAllocationGateTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(MemtableFlushAllocationGateTest.class);
    private static final int WARMUP_ITERATIONS = 20;
    private static final int MEASURED_ITERATIONS = 10;

    @Test
    public void wideRowsAllocateLessViaCursor() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        for (int k = 0; k < 200; k++)
            for (int c = 0; c < 50; c++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, "value-" + k + "-" + c);
        // Observed ~42.5% at authoring time; the floor below is deliberately well under that so
        // ordinary JIT/heap noise doesn't flake the test, while still catching a real regression
        // back toward "no better than the iterator path".
        measureAndAssert("wide rows: 200 partitions x 50 rows", 25.0);
    }

    @Test
    public void largeCollectionsAllocateLessViaCursor() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<int, text>)");
        for (int k = 0; k < 50; k++)
        {
            Map<Integer, String> m = new LinkedHashMap<>();
            for (int i = 0; i < 500; i++)
                m.put(i, "value-" + i);
            execute("INSERT INTO %s (k, m) VALUES (?, ?)", k, m);
        }
        // This scenario's win is genuinely small (~2.2% at authoring time: most of its allocation
        // is the collection's own cell values, which both paths copy) - the floor only checks the
        // cursor path isn't strictly worse here, not that it wins big.
        measureAndAssert("large collections: 50 partitions x 500-entry map", 0.0);
    }

    @Test
    public void rangeTombstonesAllocateLessViaCursor() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        for (int k = 0; k < 100; k++)
        {
            for (int c = 0; c < 100; c++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, "value-" + c);
            for (int c = 0; c < 100; c += 4)
                execute("DELETE FROM %s WHERE k = ? AND c >= ? AND c < ?", k, c, c + 2);
        }
        // Observed ~48.6% at authoring time; see wideRowsAllocateLessViaCursor for why the floor
        // sits well below that.
        measureAndAssert("range tombstones: 100 partitions x (100 rows, 25 range deletes)", 30.0);
    }

    /**
     * @param minReductionPct the minimum {@code cursor} allocation reduction versus
     *                        {@code iterator}, as a percentage, this scenario must clear. A real
     *                        bound rather than just {@code cursor <= iterator} - the latter would
     *                        pass even if the cursor path's advantage regressed away to nothing,
     *                        as long as it didn't turn strictly negative.
     */
    private void measureAndAssert(String label, double minReductionPct) throws Throwable
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", ThreadStats.isThreadAllocatedMemorySupported());

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = retireCurrentMemtable(cfs);
        assertTrue("scenario's table/memtable doesn't satisfy MemtableCursorFlusher.isSupported " +
                   "- this benchmark would silently measure nothing meaningful for the cursor path",
                   MemtableCursorFlusher.isSupported(cfs.metadata(), memtable));

        try
        {
            long iteratorBest = measureBest(cfs, memtable, false);
            long cursorBest = measureBest(cfs, memtable, true);
            double reductionPct = 100.0 * (iteratorBest - cursorBest) / iteratorBest;

            logger.info("Flush allocation [{}]: iterator={} bytes, cursor={} bytes, reduction={}%",
                       label, iteratorBest, cursorBest, String.format("%.1f", reductionPct));

            assertTrue(String.format("cursor flush allocation reduction for [%s] was only %.1f%%, below the required %.1f%% floor: iterator=%d cursor=%d",
                                     label, reductionPct, minReductionPct, iteratorBest, cursorBest),
                      reductionPct >= minReductionPct);
        }
        finally
        {
            DatabaseDescriptor.setCursorFlushEnabled(false);
        }
    }

    /**
     * {@code Flushing.flushRunnables} requires the memtable's commit-log upper bound to already
     * be finalized ({@code AbstractMemtableWithCommitlog.getFinalCommitLogUpperBound} asserts
     * it) - normally set as a side effect of {@code ColumnFamilyStore.Flush}'s constructor when
     * a real (background-executor-driven) flush retires a memtable. This replicates exactly
     * that constructor's sequence (ColumnFamilyStore.java, the {@code Flush} inner class) - swap
     * in a fresh replacement memtable, tell the old one to stop accepting writes past this
     * point via a write-order barrier, and record the commit log position as of that barrier -
     * without going through the real Flush/executor machinery, since we want the retired
     * memtable held for direct, repeated, measured flushing afterward rather than consumed by a
     * real background flush.
     */
    private Memtable retireCurrentMemtable(ColumnFamilyStore cfs)
    {
        AtomicReference<CommitLogPosition> commitLogUpperBound = new AtomicReference<>();
        Memtable newMemtable = cfs.createMemtable(commitLogUpperBound);
        Memtable oldMemtable = cfs.getTracker().switchMemtable(false, newMemtable);
        OpOrder.Barrier writeBarrier = Keyspace.writeOrder.newBarrier();
        oldMemtable.switchOut(writeBarrier, commitLogUpperBound);

        CommitLogPosition lastReplayPosition;
        while (true)
        {
            lastReplayPosition = new Memtable.LastCommitLogPosition(CommitLog.instance.getCurrentPosition());
            CommitLogPosition currentLast = commitLogUpperBound.get();
            if ((currentLast == null || currentLast.compareTo(lastReplayPosition) <= 0)
                && commitLogUpperBound.compareAndSet(currentLast, lastReplayPosition))
                break;
        }

        writeBarrier.issue();
        writeBarrier.markBlocking();
        writeBarrier.await();
        return oldMemtable;
    }

    private long measureBest(ColumnFamilyStore cfs, Memtable memtable, boolean cursor) throws Throwable
    {
        DatabaseDescriptor.setCursorFlushEnabled(cursor);
        long best = Long.MAX_VALUE;
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++)
        {
            long allocated = measureOnce(cfs, memtable);
            if (i >= WARMUP_ITERATIONS)
                best = Math.min(best, allocated);
        }
        return best;
    }

    /**
     * One flush of {@code memtable}, measured on the calling thread and then unwound without
     * ever being committed or opened — see the class javadoc for why this is safe to repeat
     * against the same memtable across many iterations.
     */
    private long measureOnce(ColumnFamilyStore cfs, Memtable memtable) throws Throwable
    {
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
        {
            List<Flushing.FlushRunnable> runnables = Flushing.flushRunnables(cfs, memtable, txn);
            List<SSTableMultiWriter> writers = new ArrayList<>(runnables.size());

            long before = ThreadStats.getCurrentThreadAllocatedBytes();
            Throwable fail = null;
            try
            {
                for (Flushing.FlushRunnable runnable : runnables)
                    writers.add(runnable.call());
            }
            catch (Throwable t)
            {
                fail = t;
            }
            long allocated = ThreadStats.getCurrentThreadAllocatedBytes() - before;

            for (SSTableMultiWriter writer : writers)
                fail = writer.abort(fail);
            fail = txn.abort(fail);
            // Flushing.flushRunnables refuses a memtable already carrying an "ongoing flush
            // transaction" (Preconditions.checkState in Flushing.flushRunnables) - it's just an
            // AtomicReference (AbstractMemtable.setFlushTransaction is a plain getAndSet), so
            // clearing it back to null after unwinding this never-committed attempt is what
            // makes repeating the measurement against the same memtable valid for the next
            // iteration, rather than a real flush's one-shot use.
            memtable.setFlushTransaction(null);
            if (fail != null)
                Throwables.maybeFail(fail);

            return allocated;
        }
    }
}
