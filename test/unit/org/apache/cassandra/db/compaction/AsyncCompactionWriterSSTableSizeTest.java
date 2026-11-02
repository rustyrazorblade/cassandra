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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Size-based SSTable switching reads the writer's on-disk size estimate on the compaction thread,
 * while the async writer may still hold chunks it has not written. These tests hold the writer
 * thread back with Byteman on the second chunk until the producer waits for it, which makes the lag
 * certain rather than load-dependent.
 */
@RunWith(BMUnitRunner.class)
@BMRules(rules = {
    @BMRule(name = "Hold chunk writes after the first for the table under test",
            targetClass = "org.apache.cassandra.io.compress.AsyncChunkPipeline",
            targetMethod = "processChunk",
            targetLocation = "AT ENTRY",
            condition = "$0.completed > 0",
            action = "org.apache.cassandra.db.compaction.AsyncCompactionWriterSSTableSizeTest.holdWrite($0.owner.getPath())"),
    @BMRule(name = "Release held chunk writes once the producer waits for them",
            targetClass = "org.apache.cassandra.io.compress.AsyncChunkPipeline",
            targetMethod = "awaitProgress",
            targetLocation = "AT ENTRY",
            action = "org.apache.cassandra.db.compaction.AsyncCompactionWriterSSTableSizeTest.releaseWrites($0.owner.getPath())")
})
public class AsyncCompactionWriterSSTableSizeTest extends CQLTester
{
    private static final int CHUNK_LENGTH = 16 * 1024;
    private static final int VALUE_SIZE = 4096;
    private static final int ROWS = 200;
    private static final long MAX_SSTABLE_SIZE = 64L * 1024;

    private static volatile String heldPath;
    private static volatile CountDownLatch release = new CountDownLatch(0);
    private static final AtomicInteger holds = new AtomicInteger();
    private static volatile boolean holdTimedOut;

    private static boolean savedEnabled;

    @BeforeClass
    public static void enableAsyncWriter()
    {
        savedEnabled = DatabaseDescriptor.asyncCompactionWriterEnabled();
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(true);
    }

    @AfterClass
    public static void restoreAsyncWriter()
    {
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(savedEnabled);
    }

    @After
    public void releaseHeldWrites()
    {
        heldPath = null;
        release.countDown();
    }

    public static void holdWrite(String path)
    {
        String held = heldPath;
        if (held != null && path.contains(held))
        {
            holds.incrementAndGet();
            if (!Uninterruptibles.awaitUninterruptibly(release, 1, TimeUnit.MINUTES))
                holdTimedOut = true;
        }
    }

    /** Releases only once a write is held, so a wait that comes first does not end the test early. */
    public static void releaseWrites(String path)
    {
        String held = heldPath;
        if (held != null && path.contains(held) && holds.get() > 0)
        {
            heldPath = null;
            release.countDown();
        }
    }

    /**
     * The whole output fits in the writer's buffer, so with the writer held the compaction can
     * finish with only one chunk written. The switch must still happen at the cap.
     */
    @Test
    public void maxSSTableSizeWriterSwitchesAtCapWhileWritesArePending() throws Throwable
    {
        assertSwitchesAtCap((cfs, directories, txn, inputs) -> new MaxSSTableSizeWriter(cfs, directories, txn, inputs, MAX_SSTABLE_SIZE, 0));
    }

    @Test
    public void majorLeveledCompactionWriterSwitchesAtCapWhileWritesArePending() throws Throwable
    {
        assertSwitchesAtCap((cfs, directories, txn, inputs) -> new MajorLeveledCompactionWriter(cfs, directories, txn, inputs, MAX_SSTABLE_SIZE));
    }

    private interface WriterFactory
    {
        CompactionAwareWriter create(ColumnFamilyStore cfs, Directories directories, ILifecycleTransaction txn, Set<SSTableReader> inputs);
    }

    private void assertSwitchesAtCap(WriterFactory factory) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v blob) WITH compression = " +
                    "{'class': 'LZ4Compressor', 'chunk_length_in_kb': " + CHUNK_LENGTH / 1024 + '}');
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertIncompressibleRows();
        flush();

        Set<SSTableReader> originals = new HashSet<>(cfs.getLiveSSTables());
        LifecycleTransaction txn = cfs.getTracker().tryModify(originals, OperationType.COMPACTION);
        CompactionTask task = new CompactionTask(cfs, txn, FBUtilities.nowInSeconds())
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return factory.create(cfs, directories, transaction, nonExpiredSSTables);
            }
        };

        holdWritesFor(cfs);
        task.execute(CompactionManager.instance.active);

        assertTrue("the writer thread was never held, so nothing was pending", holds.get() >= 1);
        assertFalse("a held write waited out its timeout; nothing released it", holdTimedOut);

        Set<SSTableReader> result = cfs.getLiveSSTables();
        assertTrue("expected the size cap to split the output, got " + result.size() + " sstable(s)", result.size() >= 2);

        // The switch is checked once per partition and the estimate leaves out the chunk still being
        // filled, so an sstable may pass the cap by one partition and one chunk. Allow one more chunk
        // for per-chunk overhead.
        long limit = MAX_SSTABLE_SIZE + VALUE_SIZE + 2 * CHUNK_LENGTH;
        for (SSTableReader sstable : result)
            assertTrue("sstable of " + sstable.onDiskLength() + " bytes is over the " + limit + " byte limit",
                       sstable.onDiskLength() <= limit);
        assertRowCount(execute("SELECT * FROM %s"), ROWS);
    }

    private void holdWritesFor(ColumnFamilyStore cfs)
    {
        holds.set(0);
        holdTimedOut = false;
        release = new CountDownLatch(1);
        heldPath = cfs.getKeyspaceName() + '/' + cfs.name + '-';
    }

    private void insertIncompressibleRows() throws Throwable
    {
        Random random = new Random(42);
        byte[] value = new byte[VALUE_SIZE];
        for (int i = 0; i < ROWS; i++)
        {
            random.nextBytes(value);
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, ByteBuffer.wrap(value.clone()));
        }
    }
}
