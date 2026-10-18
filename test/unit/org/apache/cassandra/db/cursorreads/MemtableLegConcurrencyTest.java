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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Cursor reads of memtable data, under {@code offheap_objects}, while writers overwrite the rows,
 * a flusher switches and frees memtables, and readers read the replica response and
 * {@code executeLocally}.  The memtable leg reads the memtable's own memory, so a read that
 * outlived the memory it reads, or a write it saw half-done, would return a value that does not
 * belong to its row.  Every value written holds its clustering, and every value read is checked
 * against the row it is in.
 *
 * <p>At the end every read order group is closed (a new read barrier completes at once) and the
 * memtable pool gives back the memory of the flushed memtables.
 *
 * <p>The run time comes from {@code -Dcassandra.test.memtable_leg_concurrency_seconds=N}
 * (default {@value #DEFAULT_SECONDS}).
 */
public class MemtableLegConcurrencyTest extends CursorReadDifferentialTester
{
    private static final int DEFAULT_SECONDS = 8;
    private static final int SECONDS = Integer.getInteger("cassandra.test.memtable_leg_concurrency_seconds", DEFAULT_SECONDS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int ROWS = 200;
    private static final int WRITERS = 2;
    private static final int READERS = 3;
    private static final String FILLER = "x".repeat(200);

    // Shadows CQLTester.setUpClass so the allocation type is set before the memtable pool exists.
    @BeforeClass
    public static void setUpClass()
    {
        daemonInitialization();
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            Config conf = (Config) confField.get(null);
            conf.memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        prePrepareServer();
        prepareServer();
    }

    @Before
    public void assertNativePool()
    {
        assertTrue("the offheap_objects config did not take effect", AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool);
    }

    @After
    public void cursorReadsOff()
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
    }

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

    private void run(String memtable) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, m map<int, text>, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v, m) VALUES (1, ?, ?, ?)", ck, value(ck, 0), map(1, value(ck, 0)));
        flush();

        MemtablePool pool = AbstractAllocatorMemtable.MEMORY_POOL;
        long offHeapBefore = pool.offHeap.used();
        DatabaseDescriptor.setCursorReadsEnabled(true);

        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicLong reads = new AtomicLong();
        AtomicLong rowsChecked = new AtomicLong();
        AtomicLong flushes = new AtomicLong();
        long streamedBefore = CursorReads.memtableLegsStreamed();
        List<Thread> threads = new ArrayList<>();
        CountDownLatch done = new CountDownLatch(WRITERS + READERS + 1);
        for (int w = 0; w < WRITERS; w++)
        {
            int writer = w;
            threads.add(thread("writer-" + w, done, failure, () -> {
                Random r = new Random(writer);
                long version = 1;
                while (!stop.get())
                {
                    int ck = r.nextInt(ROWS);
                    switch (r.nextInt(10))
                    {
                        case 0:
                            execute("DELETE FROM %s WHERE pk = 1 AND ck = ?", ck);
                            break;
                        case 1:
                            execute("DELETE FROM %s WHERE pk = 1 AND ck >= ? AND ck < ?", ck, ck + 3);
                            break;
                        case 2:
                            execute("UPDATE %s SET m[?] = ? WHERE pk = 1 AND ck = ?", r.nextInt(4), value(ck, version++), ck);
                            break;
                        default:
                            execute("INSERT INTO %s (pk, ck, v, m) VALUES (1, ?, ?, ?)", ck, value(ck, version), map(1, value(ck, version)));
                            version++;
                            break;
                    }
                }
            }));
        }
        threads.add(thread("flusher", done, failure, () -> {
            while (!stop.get())
            {
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                flushes.incrementAndGet();
                Thread.yield();
            }
        }));
        for (int rd = 0; rd < READERS; rd++)
        {
            int reader = rd;
            threads.add(thread("reader-" + rd, done, failure, () -> {
                Random r = new Random(100 + reader);
                while (!stop.get())
                {
                    long now = FBUtilities.nowInSeconds();
                    int from = r.nextInt(ROWS);
                    SinglePartitionReadCommand command = r.nextBoolean()
                                                         ? (SinglePartitionReadCommand) Util.cmd(cfs, 1).withNowInSeconds(now).build()
                                                         : (SinglePartitionReadCommand) Util.cmd(cfs, 1).withNowInSeconds(now).fromIncl(from).toIncl(from + 20).build();
                    if (r.nextBoolean())
                    {
                        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
                        try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
                        {
                            rowsChecked.addAndGet(check(cfs, partitions));
                        }
                    }
                    else
                    {
                        try (ReadExecutionController controller = command.executionController();
                             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
                        {
                            rowsChecked.addAndGet(check(cfs, partitions));
                        }
                    }
                    reads.incrementAndGet();
                }
            }));
        }
        for (Thread t : threads)
            t.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SECONDS);
        while (System.nanoTime() < deadline && failure.get() == null)
            done.await(100, TimeUnit.MILLISECONDS);
        stop.set(true);
        assertTrue("the threads did not stop", done.await(2, TimeUnit.MINUTES));
        for (Thread t : threads)
            t.join();
        assertNull("a thread failed: " + failure.get(), failure.get());
        assertTrue("no flush ran", flushes.get() > 0);
        assertTrue("no read ran", reads.get() > 0);
        assertTrue("no row was checked", rowsChecked.get() > 0);
        assertTrue("no memtable leg was read from the memtable's own memory",
                   CursorReads.memtableLegsStreamed() > streamedBefore);

        // every read order group is closed: a new barrier completes
        OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
        barrier.issue();
        Thread waiter = new Thread(barrier::await, "read-barrier");
        waiter.setDaemon(true);
        waiter.start();
        waiter.join(TimeUnit.MINUTES.toMillis(1));
        assertTrue("a read order group is still open", !waiter.isAlive());
        // the flushed memtables give their memory back, once the read barrier after each flush passes
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        await().atMost(1, TimeUnit.MINUTES)
               .untilAsserted(() -> assertTrue("memtable memory not given back: " + (pool.offHeap.used() - offHeapBefore) + " bytes",
                                               pool.offHeap.used() - offHeapBefore < 1 << 20));
    }

    /** Checks every row of {@code partitions}: each value read belongs to its row. */
    private static long check(ColumnFamilyStore cfs, UnfilteredPartitionIterator partitions)
    {
        ColumnMetadata v = cfs.metadata().getColumn(ByteBufferUtil.bytes("v"));
        ColumnMetadata m = cfs.metadata().getColumn(ByteBufferUtil.bytes("m"));
        long rows = 0;
        while (partitions.hasNext())
        {
            try (UnfilteredRowIterator partition = partitions.next())
            {
                assertEquals(1, ByteBufferUtil.toInt(partition.partitionKey().getKey()));
                while (partition.hasNext())
                {
                    Unfiltered unfiltered = partition.next();
                    if (!unfiltered.isRow())
                        continue;
                    Row row = (Row) unfiltered;
                    int ck = ByteBufferUtil.toInt(row.clustering().bufferAt(0));
                    assertTrue("clustering out of range: " + ck, ck >= 0 && ck < ROWS + 3);
                    Cell<?> cell = row.getCell(v);
                    if (cell != null && !cell.isTombstone())
                        checkValue(ck, cell.buffer());
                    ComplexColumnData map = row.getComplexColumnData(m);
                    if (map != null)
                    {
                        for (Cell<?> element : map)
                        {
                            if (!element.isTombstone())
                                checkValue(ck, element.buffer());
                        }
                    }
                    rows++;
                }
            }
        }
        return rows;
    }

    private static void checkValue(int ck, ByteBuffer value)
    {
        String text = UTF8Type.instance.compose(value);
        String prefix = "ck" + ck + '-';
        assertTrue("a value of row " + ck + " does not belong to it: " + text, text.startsWith(prefix) && text.endsWith(FILLER));
    }

    private static String value(int ck, long version)
    {
        return "ck" + ck + '-' + version + '-' + FILLER;
    }

    private interface Body
    {
        void run() throws Throwable;
    }

    private static Thread thread(String name, CountDownLatch done, AtomicReference<Throwable> failure, Body body)
    {
        Thread thread = new Thread(() -> {
            try
            {
                body.run();
            }
            catch (Throwable t)
            {
                failure.compareAndSet(null, t);
            }
            finally
            {
                done.countDown();
            }
        }, name);
        thread.setDaemon(true);
        return thread;
    }
}
