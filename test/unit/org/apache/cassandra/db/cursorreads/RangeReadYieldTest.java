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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeScan;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.cursorreads.RangeScanTesting.assertLinesEqual;
import static org.apache.cassandra.db.cursorreads.RangeScanTesting.responseLines;
import static org.apache.cassandra.db.cursorreads.RangeScanTesting.runToEnd;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * What a {@link CursorRangeScan} holds while a segment runs, and what it gives back at a yield: the
 * table's read order group (a read barrier waits for the segment and passes once it yields), the
 * memory of a flushed memtable (released only after the yield), and the files of compacted sstables
 * (deleted only after the yield).  After each, the scan goes on and reads exactly what the iterator
 * path reads.
 */
public class RangeReadYieldTest extends RangeReadOracle
{
    private static final int PARTITIONS = 50;
    private static final int ROWS = 20;
    private static final long WAIT_SECONDS = 60;

    @Test
    public void readBarrierWaitsForTheSegmentAndPassesAfterTheYield() throws Throwable
    {
        ColumnFamilyStore cfs = load(1, true);
        long now = FBUtilities.nowInSeconds();
        List<String> expected = iteratorLines(now);

        BlockingSink sink = new BlockingSink(cfs, 1);
        CursorRangeScan scan = CursorRangeScan.open(command(now));
        Thread reader = runFirstSegment(scan, sink);
        assertTrue("the scan reached its first partition", sink.entered.await(WAIT_SECONDS, TimeUnit.SECONDS));

        OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
        barrier.issue();
        CountDownLatch passed = new CountDownLatch(1);
        Thread waiter = new Thread(() -> { barrier.await(); passed.countDown(); }, "read-barrier-waiter");
        waiter.start();
        assertFalse("the read barrier passed while the segment runs", passed.await(500, TimeUnit.MILLISECONDS));

        sink.release.countDown();
        reader.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        assertEquals(CursorRangeScan.ScanEnd.YIELDED, sink.firstEnd.get());
        assertTrue("the read barrier passed after the yield", passed.await(WAIT_SECONDS, TimeUnit.SECONDS));

        runToEnd(scan, sink);
        assertLinesEqual("the scan around a read barrier", expected, sink.lines);
    }

    @Test
    public void flushedMemtableIsFreedOnlyAfterTheYield() throws Throwable
    {
        ColumnFamilyStore cfs = load(0, true);
        long now = FBUtilities.nowInSeconds();
        List<String> expected = iteratorLines(now);
        long before = poolUsed();
        assertTrue("the memtable holds the data: " + before, before > 0);

        BlockingSink sink = new BlockingSink(cfs, 1);
        CursorRangeScan scan = CursorRangeScan.open(command(now));
        Thread reader = runFirstSegment(scan, sink);
        assertTrue(sink.entered.await(WAIT_SECONDS, TimeUnit.SECONDS));

        // the flush completes, but the memtable's memory waits for the segment's read order group
        flush();
        assertTrue("the memtable's memory was freed while the segment reads it: " + poolUsed() + " of " + before,
                   holdsFor(500, () -> poolUsed() >= before));

        sink.release.countDown();
        reader.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        assertEquals(CursorRangeScan.ScanEnd.YIELDED, sink.firstEnd.get());
        assertTrue("the memtable's memory is freed after the yield: " + poolUsed() + " of " + before,
                   becomes(() -> poolUsed() < before));

        runToEnd(scan, sink);
        assertLinesEqual("the scan around a flush", expected, sink.lines);
    }

    @Test
    public void compactedSSTablesAreDeletedOnlyAfterTheYield() throws Throwable
    {
        ColumnFamilyStore cfs = load(4, false);
        long now = FBUtilities.nowInSeconds();
        List<String> expected = iteratorLines(now);
        List<File> files = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            files.add(sstable.descriptor.fileFor(Components.DATA));

        BlockingSink sink = new BlockingSink(cfs, 1);
        CursorRangeScan scan = CursorRangeScan.open(command(now));
        Thread reader = runFirstSegment(scan, sink);
        assertTrue(sink.entered.await(WAIT_SECONDS, TimeUnit.SECONDS));

        cfs.forceMajorCompaction();
        assertEquals("the compaction made one sstable", 1, cfs.getLiveSSTables().size());
        assertTrue("a compacted sstable's data file was deleted while the segment reads it",
                   holdsFor(500, () -> allExist(files)));

        sink.release.countDown();
        reader.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        assertEquals(CursorRangeScan.ScanEnd.YIELDED, sink.firstEnd.get());
        assertTrue("the compacted sstables' data files are deleted after the yield", becomes(() -> noneExist(files)));

        runToEnd(scan, sink);
        assertLinesEqual("the scan around a compaction", expected, sink.lines);
    }

    // ---------------------------------------------------------------- helpers

    /** A sink that yields after each partition and, in the first segment, waits at its {@code blockAt}th partition. */
    private static final class BlockingSink extends RangeScanTesting.RecordingSink
    {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicReference<CursorRangeScan.ScanEnd> firstEnd = new AtomicReference<>();
        private final int blockAt;
        private int started;

        BlockingSink(ColumnFamilyStore cfs, int blockAt)
        {
            super(cfs.metadata(), () -> true);
            this.blockAt = blockAt;
        }

        @Override
        public void partitionStart(DecoratedKey key, DeletionTime partitionDeletion, Row staticRow)
        {
            super.partitionStart(key, partitionDeletion, staticRow);
            if (++started == blockAt)
            {
                entered.countDown();
                try
                {
                    if (!release.await(WAIT_SECONDS, TimeUnit.SECONDS))
                        throw new AssertionError("the test did not release the sink");
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
        }
    }

    private static Thread runFirstSegment(CursorRangeScan scan, BlockingSink sink)
    {
        Thread reader = new Thread(() -> sink.firstEnd.set(scan.run(sink)), "range-scan-segment");
        reader.start();
        return reader;
    }

    /** Partitions of rows, in {@code sstables} sstables (or none), and newer rows left in the memtable when {@code overlay}. */
    private ColumnFamilyStore load(int sstables, boolean overlay) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        int rounds = Math.max(1, sstables);
        for (int s = 0; s < rounds; s++)
        {
            for (int k = 0; k < PARTITIONS; k++)
                for (int c = 0; c < ROWS; c++)
                    if ((k + c) % rounds == s)
                        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?", k, c, "v" + k + '-' + c + "-".repeat(100), 1000L + s);
            if (sstables > 0)
                flush();
        }
        if (overlay)
        {
            for (int k = 0; k < PARTITIONS; k += 3)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2000", k, 1, "memtable");
        }
        return cfs;
    }

    private PartitionRangeReadCommand command(long now)
    {
        return rangeCommand("SELECT * FROM %s", now);
    }

    private List<String> iteratorLines(long now)
    {
        setRangePath(false);
        return responseLines(command(now));
    }

    private static long poolUsed()
    {
        return AbstractAllocatorMemtable.MEMORY_POOL.onHeap.used() + AbstractAllocatorMemtable.MEMORY_POOL.offHeap.used();
    }

    private static boolean allExist(List<File> files)
    {
        for (File file : files)
            if (!file.exists())
                return false;
        return true;
    }

    private static boolean noneExist(List<File> files)
    {
        for (File file : files)
            if (file.exists())
                return false;
        return true;
    }

    /** Whether {@code condition} holds at every check for {@code millis}. */
    private static boolean holdsFor(long millis, BooleanSupplier condition)
    {
        try
        {
            Awaitility.await().during(millis, TimeUnit.MILLISECONDS).atMost(millis + TimeUnit.SECONDS.toMillis(WAIT_SECONDS), TimeUnit.MILLISECONDS)
                      .pollInterval(10, TimeUnit.MILLISECONDS).until(condition::getAsBoolean);
            return true;
        }
        catch (ConditionTimeoutException e)
        {
            return false;
        }
    }

    /** Whether {@code condition} comes to hold within the wait. */
    private static boolean becomes(BooleanSupplier condition)
    {
        try
        {
            Awaitility.await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(condition::getAsBoolean);
            return true;
        }
        catch (ConditionTimeoutException e)
        {
            return false;
        }
    }
}
