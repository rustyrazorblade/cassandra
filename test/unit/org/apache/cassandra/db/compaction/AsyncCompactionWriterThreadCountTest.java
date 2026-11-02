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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.unified.ShardedCompactionWriter;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A compaction that writes many outputs must hold a writer thread only for the output it is
 * writing. Each test samples the live writer threads for its table at every partition and checks
 * the count never passes the number of open outputs, which is one here, and is 0 once the
 * compaction is done.
 */
public class AsyncCompactionWriterThreadCountTest extends CQLTester
{
    private static final int CHUNK_KB = 4;
    private static final int VALUE_SIZE = 512;
    private static final int ROWS = 2000;
    private static final long MAX_SSTABLE_BYTES = 64 * 1024;
    private static final int MIN_OUTPUTS = 8;

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

    @Test
    public void maxSSTableSizeWriterHoldsOneWriterThread() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        WriterThreads threads = new WriterThreads(cfs);
        compact(cfs, (c, d, txn, inputs) -> new MaxSSTableSizeWriter(c, d, txn, inputs, MAX_SSTABLE_BYTES, 0)
        {
            @Override
            protected boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key)
            {
                threads.sample();
                return super.shouldSwitchWriterInCurrentLocation(key);
            }
        });
        assertBounded(cfs, threads);
    }

    @Test
    public void majorLeveledCompactionWriterHoldsOneWriterThread() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        WriterThreads threads = new WriterThreads(cfs);
        compact(cfs, (c, d, txn, inputs) -> new MajorLeveledCompactionWriter(c, d, txn, inputs, MAX_SSTABLE_BYTES)
        {
            @Override
            protected boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key)
            {
                threads.sample();
                return super.shouldSwitchWriterInCurrentLocation(key);
            }
        });
        assertBounded(cfs, threads);
    }

    /** One UCS task that writes every shard, as a compaction does with parallelize_output_shards off. */
    @Test
    public void shardedCompactionWriterHoldsOneWriterThread() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        WriterThreads threads = new WriterThreads(cfs);
        compact(cfs, (c, d, txn, inputs) -> new ShardedCompactionWriter(c, d, txn, inputs, false, true,
                                                                        ShardManager.create(c).boundaries(32))
        {
            @Override
            protected boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key)
            {
                threads.sample();
                return super.shouldSwitchWriterInCurrentLocation(key);
            }
        });
        assertBounded(cfs, threads);
    }

    private interface WriterFactory
    {
        CompactionAwareWriter create(ColumnFamilyStore cfs, Directories directories, ILifecycleTransaction txn, Set<SSTableReader> inputs);
    }

    private static void compact(ColumnFamilyStore cfs, WriterFactory factory)
    {
        Set<SSTableReader> inputs = new HashSet<>(cfs.getLiveSSTables());
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        new CompactionTask(cfs, txn, FBUtilities.nowInSeconds())
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return factory.create(cfs, directories, transaction, nonExpiredSSTables);
            }
        }.execute(ActiveCompactionsTracker.NOOP);
    }

    private void assertBounded(ColumnFamilyStore cfs, WriterThreads threads) throws Throwable
    {
        int outputs = cfs.getLiveSSTables().size();
        assertTrue("expected at least " + MIN_OUTPUTS + " outputs, got " + outputs, outputs >= MIN_OUTPUTS);
        assertTrue("no writer thread was ever seen, so the test checked nothing", threads.max.get() >= 1);
        assertEquals("live writer threads passed the one open output across " + outputs + " outputs",
                     1, threads.max.get());
        assertEquals("writer threads left after the compaction", 0, threads.live());
        assertRowCount(execute("SELECT * FROM %s"), ROWS);
    }

    private ColumnFamilyStore populate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v blob) WITH compression = " +
                    "{'class': 'LZ4Compressor', 'chunk_length_in_kb': " + CHUNK_KB + '}');
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Random random = new Random(42);
        for (int i = 0; i < ROWS; i++)
        {
            byte[] value = new byte[VALUE_SIZE];
            random.nextBytes(value);
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, ByteBuffer.wrap(value));
            if (i == ROWS / 2)
                flush();
        }
        flush();
        return cfs;
    }

    /** Live writer threads for one table. The thread name starts with the table directory name. */
    private static final class WriterThreads
    {
        private final String prefix;
        final AtomicInteger max = new AtomicInteger();

        WriterThreads(ColumnFamilyStore cfs)
        {
            this.prefix = "CompactionWriter-" + cfs.getDirectories().getDirectoryForNewSSTables().name() + '/';
        }

        void sample()
        {
            max.accumulateAndGet(live(), Math::max);
        }

        int live()
        {
            int count = 0;
            for (Thread t : Thread.getAllStackTraces().keySet())
                if (t.isAlive() && t.getName().startsWith(prefix))
                    count++;
            return count;
        }
    }
}
