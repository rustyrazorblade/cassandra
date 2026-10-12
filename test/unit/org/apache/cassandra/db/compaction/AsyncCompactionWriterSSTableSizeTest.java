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

import com.google.common.util.concurrent.Uninterruptibles;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

/**
 * Size-based SSTable switching reads the writer's on-disk size estimate on the compaction thread,
 * while the async writer may still hold chunks it has not written. These tests hold the writer
 * thread back with Byteman so only the first chunk of each file is written until the producer first
 * waits for the writer, which makes the lag certain rather than load-dependent.
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
            targetMethod = "drain",
            targetLocation = "AT ENTRY",
            action = "org.apache.cassandra.db.compaction.AsyncCompactionWriterSSTableSizeTest.releaseWrites($0.owner.getPath())"),
    @BMRule(name = "Record the flush size estimate before the flush waits for the writer",
            targetClass = "org.apache.cassandra.db.compaction.unified.ShardedMultiWriter",
            targetMethod = "prepareToCommit",
            targetLocation = "AT ENTRY",
            action = "org.apache.cassandra.db.compaction.AsyncCompactionWriterSSTableSizeTest.recordFlushEstimate($0.getTableId(), $0.getOnDiskBytesWritten())")
})
public class AsyncCompactionWriterSSTableSizeTest extends CQLTester
{
    private static final int CHUNK_LENGTH = 16 * 1024;
    private static final int VALUE_SIZE = 4096;
    private static final int ROWS = 200;

    private static volatile String heldPath;
    private static volatile CountDownLatch release = new CountDownLatch(0);
    private static volatile TableId recordedTable;
    private static volatile long recordedEstimate = -1;

    @BeforeClass
    public static void requireAsyncWriter()
    {
        assertTrue("these tests need async_compaction_writer_enabled", DatabaseDescriptor.asyncCompactionWriterEnabled());
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
            Uninterruptibles.awaitUninterruptibly(release, 1, TimeUnit.MINUTES);
    }

    public static void releaseWrites(String path)
    {
        String held = heldPath;
        if (held != null && path.contains(held))
        {
            heldPath = null;
            release.countDown();
        }
    }

    public static void recordFlushEstimate(TableId table, long estimate)
    {
        if (table.equals(recordedTable))
            recordedEstimate = estimate;
    }

    /**
     * The whole output fits in the writer's buffer, so with the writer held the compaction can
     * finish with only one chunk written. The switch must still happen at the cap.
     */
    @Test
    public void maxSSTableSizeWriterSwitchesAtCapWhileWritesArePending() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v blob) WITH compression = " +
                    "{'class': 'LZ4Compressor', 'chunk_length_in_kb': " + CHUNK_LENGTH / 1024 + '}');
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertIncompressibleRows();
        flush();

        long maxSSTableSize = 64L * 1024;
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
                return new MaxSSTableSizeWriter(cfs, directories, transaction, nonExpiredSSTables, maxSSTableSize, 0);
            }
        };

        holdWritesFor(cfs);
        task.execute(CompactionManager.instance.active);

        Set<SSTableReader> result = cfs.getLiveSSTables();
        assertTrue("expected the size cap to split the output, got " + result.size() + " sstable(s)", result.size() >= 2);

        // The switch is checked once per partition and the estimate leaves out the chunk still being
        // filled, so an sstable may pass the cap by one partition and one chunk. Allow one more chunk
        // for per-chunk overhead.
        long limit = maxSSTableSize + VALUE_SIZE + 2 * CHUNK_LENGTH;
        for (SSTableReader sstable : result)
            assertTrue("sstable of " + sstable.onDiskLength() + " bytes is over the " + limit + " byte limit",
                       sstable.onDiskLength() <= limit);
        assertRowCount(execute("SELECT * FROM %s"), ROWS);
    }

    /**
     * UCS flushes through ShardedMultiWriter, whose on-disk size is the sum of its writers' estimates.
     * Read before the flush first waits for the writer, it must already count the pending chunks.
     */
    @Test
    public void shardedMultiWriterCountsPendingWrites() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v blob) WITH compaction = " +
                    "{'class': 'UnifiedCompactionStrategy', 'base_shard_count': 1} AND compression = " +
                    "{'class': 'LZ4Compressor', 'chunk_length_in_kb': " + CHUNK_LENGTH / 1024 + '}');
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertIncompressibleRows();

        recordedTable = cfs.metadata().id;
        recordedEstimate = -1;
        holdWritesFor(cfs);
        flush();

        long onDisk = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            onDisk += sstable.onDiskLength();
        // The data is incompressible, so the pending chunks count at about their on-disk size. Allow
        // for the chunk still being filled, which the estimate leaves out, and for per-chunk overhead.
        assertTrue("estimate " + recordedEstimate + " before the flush waited for its writer must count the " +
                   "pending chunks; the flushed data is " + onDisk + " bytes",
                   recordedEstimate >= onDisk - 2 * CHUNK_LENGTH);
    }

    private void holdWritesFor(ColumnFamilyStore cfs)
    {
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
