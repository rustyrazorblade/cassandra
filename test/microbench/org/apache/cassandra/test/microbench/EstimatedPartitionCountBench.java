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

package org.apache.cassandra.test.microbench;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * The EstimatedPartitionCount gauge, read on every metrics scrape. Its sstable half is memoised, so
 * {@link #memoised()} is the scrape cost and {@link #recomputed()} is the cost of a miss: one Statistics.db
 * load per sstable, then a merge of their cardinality estimators.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class EstimatedPartitionCountBench extends CQLTester
{
    @Param({ "1", "10", "20", "50", "100" })
    int sstableCount;

    @Param({ "1000" })
    int partitionsPerSSTable;

    ColumnFamilyStore cfs;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        // setUpClass() prepares the server; calling prepareServer() again re-registers the node and fails.
        CQLTester.setUpClass();
        DatabaseDescriptor.setAutoSnapshot(false);

        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = " +
                                         "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (k bigint PRIMARY KEY, v bigint) " +
                                             "with compression = {'enabled': false}");
        execute("use " + keyspace + ';');

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        String insert = "INSERT INTO " + table + " (k, v) VALUES (?, ?)";
        for (int sstable = 0; sstable < sstableCount; sstable++)
        {
            long base = (long) sstable * partitionsPerSSTable;
            for (int i = 0; i < partitionsPerSSTable; i++)
                execute(insert, base + i, (long) i);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        if (cfs.getLiveSSTables().size() != sstableCount)
            throw new AssertionError("Expected " + sstableCount + " sstables, got " + cfs.getLiveSSTables().size());
    }

    @TearDown(Level.Trial)
    public void teardown()
    {
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    @Benchmark
    public long memoised()
    {
        return cfs.metric.estimatedPartitionCount.getValue();
    }

    @Benchmark
    public long recomputed()
    {
        try (ColumnFamilyStore.RefViewFragment fragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            return SSTableReader.getApproximateKeyCount(fragment.sstables);
        }
    }
}
