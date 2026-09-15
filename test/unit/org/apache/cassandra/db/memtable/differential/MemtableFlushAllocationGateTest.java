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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
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
    // The raw measurement is contaminated. A ~1MB additive allocation lands in ThreadStats's window on some
    // JVM runs and, when it does, it hits *every* measured iteration of *both* paths by the same amount (a
    // write-path buffer that the pool serves off-heap on a good run and falls back to on-heap on a bad one).
    // So a per-path minimum over many samples does not clear it: on a bad run the minimum is contaminated too.
    //
    // The invariant is the *difference* iterator-minus-cursor. Because the ~1MB lands on both paths equally
    // in one measured iteration, measuring the two paths paired in the same iteration cancels it exactly:
    // (iterator+c) - (cursor+c) = iterator - cursor, whatever c is that iteration. So the gate asserts on the
    // median paired byte difference, not on a percentage (whose denominator the contamination inflates). The
    // median is also robust to an occasional per-path outlier. A real cursor regression raises cursor
    // allocation on every iteration, shrinks the difference on every iteration, and cannot hide from it.
    private static final int WARMUP_ITERATIONS = 40;
    private static final int MEASURED_ITERATIONS = 40;

    // The cursor flush path requires a heap-based memtable allocator (MemtableCursorFlusher.isSupported);
    // ant test-latest otherwise leaks offheap_objects in from config. Pin it, so this test owns its
    // environment. Each scenario's DDL pins the skiplist memtable inline, for the same reason.
    private Config.MemtableAllocationType originalAllocationType;

    @Before
    public void pinHeapAllocation()
    {
        originalAllocationType = DatabaseDescriptor.getMemtableAllocationType();
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = Config.MemtableAllocationType.heap_buffers;
    }

    @After
    public void restoreAllocation()
    {
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = originalAllocationType;
    }

    @Test
    public void wideRowsAllocateLessViaCursor() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c)) WITH memtable = 'skiplist'");
        for (int k = 0; k < 200; k++)
            for (int c = 0; c < 50; c++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, "value-" + k + "-" + c);
        // Observed ~59KiB saved per flush (iterator ~170KiB, cursor ~111KiB, ~35%) at authoring time; the
        // floor below is deliberately well under that so ordinary JIT/heap noise doesn't flake the test,
        // while still catching a real regression back toward "no better than the iterator path".
        measureAndAssert("wide rows: 200 partitions x 50 rows", 30_000);
    }

    @Test
    public void largeCollectionsAllocateLessViaCursor() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<int, text>) WITH memtable = 'skiplist'");
        for (int k = 0; k < 50; k++)
        {
            Map<Integer, String> m = new LinkedHashMap<>();
            for (int i = 0; i < 500; i++)
                m.put(i, "value-" + i);
            execute("INSERT INTO %s (k, m) VALUES (?, ?)", k, m);
        }
        // This scenario's win is genuinely small (~16KiB, ~21% at authoring time: most of its allocation
        // is the collection's own cell values, which both paths copy) - the floor only checks the cursor
        // path isn't strictly worse here (saves >= 0 bytes), not that it wins big.
        measureAndAssert("large collections: 50 partitions x 500-entry map", 0);
    }

    @Test
    public void rangeTombstonesAllocateLessViaCursor() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c)) WITH memtable = 'skiplist'");
        for (int k = 0; k < 100; k++)
        {
            for (int c = 0; c < 100; c++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, "value-" + c);
            for (int c = 0; c < 100; c += 4)
                execute("DELETE FROM %s WHERE k = ? AND c >= ? AND c < ?", k, c, c + 2);
        }
        // Observed ~353KiB saved per flush (iterator ~486KiB, cursor ~133KiB, ~73%) at authoring time; see
        // wideRowsAllocateLessViaCursor for why the floor sits well below that.
        measureAndAssert("range tombstones: 100 partitions x (100 rows, 25 range deletes)", 200_000);
    }

    /**
     * @param minBytesSaved the minimum median per-flush allocation, in bytes, the {@code cursor} path must
     *                      save versus the {@code iterator} path. A real bound rather than just
     *                      {@code cursor <= iterator} - the latter would pass even if the cursor path's
     *                      advantage regressed away to nothing, as long as it didn't turn strictly negative.
     */
    private void measureAndAssert(String label, long minBytesSaved) throws Throwable
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", ThreadStats.isThreadAllocatedMemorySupported());

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = retireCurrentMemtable(cfs);
        assertTrue("scenario's table/memtable doesn't satisfy MemtableCursorFlusher.isSupported " +
                   "- this benchmark would silently measure nothing meaningful for the cursor path",
                   MemtableCursorFlusher.isSupported(cfs.metadata(), memtable));

        try
        {
            long medianBytesSaved = measureMedianBytesSaved(cfs, memtable);

            logger.info("Flush allocation [{}]: cursor saves a median of {} bytes per flush versus the iterator path",
                       label, medianBytesSaved);

            assertTrue(String.format("cursor flush saved only %d bytes for [%s], below the required %d-byte floor",
                                     medianBytesSaved, label, minBytesSaved),
                      medianBytesSaved >= minBytesSaved);
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

    /**
     * Returns the median, over the measured iterations, of the per-iteration allocation the cursor path
     * saves versus the iterator path. Both paths are measured in the same iteration so the shared ~1MB
     * contamination (see the field comment) cancels in the difference; the median then rejects an
     * occasional per-path outlier.
     */
    private long measureMedianBytesSaved(ColumnFamilyStore cfs, Memtable memtable) throws Throwable
    {
        long[] saved = new long[MEASURED_ITERATIONS];
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++)
        {
            long iteratorAllocated = measureOnce(cfs, memtable, false);
            long cursorAllocated = measureOnce(cfs, memtable, true);
            if (i >= WARMUP_ITERATIONS)
                saved[i - WARMUP_ITERATIONS] = iteratorAllocated - cursorAllocated;
        }
        Arrays.sort(saved);
        return saved[saved.length / 2];
    }

    /**
     * One flush of {@code memtable} on the {@code cursor} or iterator path, measured on the calling thread
     * and then unwound without ever being committed or opened — see the class javadoc for why this is safe
     * to repeat against the same memtable across many iterations.
     */
    private long measureOnce(ColumnFamilyStore cfs, Memtable memtable, boolean cursor) throws Throwable
    {
        DatabaseDescriptor.setCursorFlushEnabled(cursor);
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
