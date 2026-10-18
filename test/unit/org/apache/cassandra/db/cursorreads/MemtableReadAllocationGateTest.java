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
import java.util.function.Supplier;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Allocation gate for the replica response of reads that take data from a memtable: the memtable
 * leg reads cells from the memtable's own storage and clones none, so the bytes allocated per row
 * are no higher than for the same rows read from one sstable.  The merge of a memtable with an
 * sstable allocates per row no more than the merge of two sstables of the same shape.
 *
 * <p>Thread-allocated bytes around whole reads, after warm-up passes, the least of several
 * measured passes.  Runs under the configured allocation type
 * ({@code OffheapObjectsMemtableReadAllocationGateTest} forces {@code offheap_objects}), for the
 * trie and skip list memtables.
 */
public class MemtableReadAllocationGateTest extends CursorReadDifferentialTester
{
    private static final Logger logger = LoggerFactory.getLogger(MemtableReadAllocationGateTest.class);
    private static final int ROWS = 2000;
    private static final int WARMUP_PASSES = 30;
    private static final int MEASURED_PASSES = 10;

    @Test
    public void trieMemtable() throws Throwable
    {
        run("trie");
    }

    @Test
    public void skipListMemtable() throws Throwable
    {
        run("skiplist");
    }

    @Test
    public void filteredTrieMemtable() throws Throwable
    {
        runFiltered("trie");
    }

    @Test
    public void filteredSkipListMemtable() throws Throwable
    {
        runFiltered("skiplist");
    }

    /**
     * A row the row filter rejects, on a partition only the memtable holds, is dropped before the
     * merge reads its cells.  The filter is on {@code v2}, and {@code v1} sorts before it, so a row
     * that streamed its cells would write {@code v1} before {@code v2} rejects it.  One row
     * matches.  The replica response is the iterator path's, and allocates per row no more than
     * the same read of one sstable.
     */
    private void runFiltered(String memtable) throws Throwable
    {
        com.sun.management.ThreadMXBean bean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", bean != null);

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, m map<int, int>, PRIMARY KEY (pk, ck)) " +
                    "WITH memtable = '" + memtable + "'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // pk 2: one sstable.  pk 1: the memtable only.
        write(2, 0);
        flush();
        write(1, 0);

        long now = FBUtilities.nowInSeconds();
        for (long pk : new long[]{ 1, 2 })
        {
            byte[] iterator = replicaResponseBytes(filteredRead(cfs, now, pk).get());
            DatabaseDescriptor.setCursorReadsEnabled(true);
            try
            {
                assertArrayEquals("replica response of pk " + pk, iterator, replicaResponseBytes(filteredRead(cfs, now, pk).get()));
            }
            finally
            {
                DatabaseDescriptor.setCursorReadsEnabled(false);
            }
        }
        long before = CursorReads.memtableRowsFilteredBeforeCells();
        long memtableOnly = bytesPerRow(bean, cfs, filteredRead(cfs, now, 1L), true);
        assertEquals("memtable rows the filter dropped before their cells were read",
                     (long) (WARMUP_PASSES + MEASURED_PASSES) * (ROWS - 1), CursorReads.memtableRowsFilteredBeforeCells() - before);
        long sstableOnly = bytesPerRow(bean, cfs, filteredRead(cfs, now, 2L), false);
        logger.info("filtered replica response bytes/row ({}, {}): memtable only {}, one sstable {}",
                    memtable, AbstractAllocatorMemtable.MEMORY_POOL.getClass().getSimpleName(), memtableOnly, sstableOnly);
        assertTrue("filtered memtable-only read allocates " + memtableOnly + " B/row, one sstable " + sstableOnly,
                   memtableOnly <= sstableOnly);
    }

    /** The read of {@code pk} filtered to the one row {@link #write} gives {@code ck = 3}. */
    private static Supplier<SinglePartitionReadCommand> filteredRead(ColumnFamilyStore cfs, long now, long pk)
    {
        return () -> (SinglePartitionReadCommand) Util.cmd(cfs, pk)
                                                      .filterOn("v2", Operator.EQ, "value-3-0")
                                                      .withNowInSeconds(now).build();
    }

    private void run(String memtable) throws Throwable
    {
        com.sun.management.ThreadMXBean bean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", bean != null);

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, m map<int, int>, PRIMARY KEY (pk, ck)) " +
                    "WITH memtable = '" + memtable + "'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // pk 2: one sstable.  pk 4: two sstables, every row in both.  pk 3: one sstable, every row
        // again in the memtable.  pk 1: the memtable only.
        write(2, 0);
        write(4, 0);
        write(3, 0);
        flush();
        write(4, 1);
        flush();
        write(3, 1);
        write(1, 0);

        long now = FBUtilities.nowInSeconds();
        long memtableOnly = bytesPerRow(bean, cfs, read(cfs, now, 1L), true);
        long sstableOnly = bytesPerRow(bean, cfs, read(cfs, now, 2L), false);
        long memtableAndSSTable = bytesPerRow(bean, cfs, read(cfs, now, 3L), true);
        long twoSSTables = bytesPerRow(bean, cfs, read(cfs, now, 4L), false);
        logger.info("replica response bytes/row ({}, {}): memtable only {}, one sstable {}, memtable and sstable {}, two sstables {}",
                    memtable, AbstractAllocatorMemtable.MEMORY_POOL.getClass().getSimpleName(),
                    memtableOnly, sstableOnly, memtableAndSSTable, twoSSTables);
        assertTrue("memtable-only read allocates " + memtableOnly + " B/row, one sstable " + sstableOnly,
                   memtableOnly <= sstableOnly);
        assertTrue("memtable and sstable merge allocates " + memtableAndSSTable + " B/row, two sstables " + twoSSTables,
                   memtableAndSSTable <= twoSSTables);
    }

    private void write(long pk, int version)
    {
        for (long ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?)", pk, ck, ck + version, "value-" + ck + '-' + version,
                    map((int) ck, version, (int) ck + 1, version));
    }

    private static Supplier<SinglePartitionReadCommand> read(ColumnFamilyStore cfs, long now, long pk)
    {
        return () -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).build();
    }

    /** The least bytes per row the replica response allocates over the measured passes. */
    private long bytesPerRow(com.sun.management.ThreadMXBean bean, ColumnFamilyStore cfs,
                             Supplier<SinglePartitionReadCommand> read, boolean readsMemtable)
    {
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            for (int i = 0; i < WARMUP_PASSES; i++)
                replicaResponseBytes(read.get());
            long rowsReused = CursorReads.memtableRowsReused();
            long cellsReused = CursorReads.memtableCellsReused();
            long rowsCloned = CursorReads.memtableRowsCloned();
            long rowsBuilt = CursorReads.mergeSinkRowsBuilt();
            long streamed = CursorReads.memtableLegsStreamed();
            long served = CursorReads.transcodeResponsesServed();
            long best = Long.MAX_VALUE;
            long thread = Thread.currentThread().getId();
            for (int i = 0; i < MEASURED_PASSES; i++)
            {
                SinglePartitionReadCommand command = read.get();
                long before = bean.getThreadAllocatedBytes(thread);
                replicaResponseBytes(command);
                best = Math.min(best, bean.getThreadAllocatedBytes(thread) - before);
            }
            assertEquals("replica responses streamed", MEASURED_PASSES, CursorReads.transcodeResponsesServed() - served);
            assertEquals("memtable rows reused", 0, CursorReads.memtableRowsReused() - rowsReused);
            assertEquals("memtable cells reused", 0, CursorReads.memtableCellsReused() - cellsReused);
            assertEquals("memtable rows cloned", 0, CursorReads.memtableRowsCloned() - rowsCloned);
            assertEquals("rows built", 0, CursorReads.mergeSinkRowsBuilt() - rowsBuilt);
            assertEquals("memtable legs read from the memtable's own storage",
                         readsMemtable ? MEASURED_PASSES : 0, CursorReads.memtableLegsStreamed() - streamed);
            return best / ROWS;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static com.sun.management.ThreadMXBean threadMXBean()
    {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof com.sun.management.ThreadMXBean))
            return null;
        com.sun.management.ThreadMXBean sun = (com.sun.management.ThreadMXBean) bean;
        if (!sun.isThreadAllocatedMemorySupported())
            return null;
        if (!sun.isThreadAllocatedMemoryEnabled())
            sun.setThreadAllocatedMemoryEnabled(true);
        return sun;
    }
}
