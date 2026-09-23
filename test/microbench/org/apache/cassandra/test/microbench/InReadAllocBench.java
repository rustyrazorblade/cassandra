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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Measurement-only bench for task #36: does the cursor read path allocate an extra per-partition
 * scratch (the ValueTransfer byte[4096]) on a plain multi-partition IN read, the same signature the
 * legacy-2i EQ read showed?  No secondary index here.  A {@code SELECT ... WHERE pk IN (...)} builds
 * a SinglePartitionReadCommand group, one command per partition key, each routed through
 * queryMemtableAndDisk exactly like the 2i fan-out.  If the cursor arm allocates ~4 KB more per
 * added partition (scaling with partition count), the regression is a general many-command read-path
 * cost, not 2i-only.
 *
 * Few rows per partition (mirrors the ~10 rows/partition of the profiled 2i EQ read), so fixed
 * per-command setup dominates.  BTI, UCS, trie memtable, one sstable.
 *
 * <pre>
 *   ant microbench -Dbenchmark.name=InReadAllocBench \
 *       -Djmh.args="-p isCursor=true,false -p partitions=1,10,50 -prof gc"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(1)
@State(Scope.Benchmark)
public class InReadAllocBench extends CQLTester
{
    /** Rows per partition; small on purpose so fixed per-command setup dominates, matching the 2i EQ shape. */
    private static final int ROWS_PER_PARTITION = 10;

    @Param({ "1", "10", "50" })
    protected int partitions = 10;

    @Param({ "true", "false" })
    protected boolean isCursor = true;

    private ColumnFamilyStore cfs;
    private boolean cursorReadsWas;
    private String selectedFormatWas;
    private String selectQuery;
    private Object[] inKeys;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        beforeTest(); // JMH does not run JUnit @Before, so create the default keyspaces explicitly
        cursorReadsWas = DatabaseDescriptor.cursorReadsEnabled();
        selectedFormatWas = DatabaseDescriptor.getSelectedSSTableFormat().name();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        DatabaseDescriptor.setCursorReadsEnabled(isCursor);

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH compaction = {'class':'UnifiedCompactionStrategy'} " +
                    "AND memtable = 'trie' " +
                    "AND gc_grace_seconds = 864000");
        cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String table = formatQuery("%s");
        for (int pk = 0; pk < partitions; pk++)
        {
            for (int ck = 0; ck < ROWS_PER_PARTITION; ck++)
                executeFormattedQuery("INSERT INTO " + table + " (pk, ck, v) VALUES (?, ?, ?)",
                                      (long) pk, (long) ck, (long) (pk * 1000 + ck));
        }
        flush(); // one sstable: each per-partition command opens exactly one leg

        // Build the IN query with one placeholder per partition key.
        StringBuilder in = new StringBuilder("SELECT pk, ck FROM %s WHERE pk IN (");
        inKeys = new Object[partitions];
        for (int pk = 0; pk < partitions; pk++)
        {
            in.append(pk == 0 ? "?" : ", ?");
            inKeys[pk] = (long) pk;
        }
        in.append(')');
        selectQuery = formatQuery(in.toString());

        long matched = 0;
        for (UntypedResultSet.Row r : executeFormattedQuery(selectQuery, inKeys))
        {
            r.getLong("pk");
            matched++;
        }
        long expected = (long) partitions * ROWS_PER_PARTITION;
        if (matched != expected)
            throw new IllegalStateException("InReadAllocBench expected " + expected + " rows, got " + matched);
        System.out.println("InReadAllocBench: isCursor=" + isCursor + " partitions=" + partitions
                           + " rows/op=" + matched + " sstables=" + cfs.getLiveSSTables().size());
    }

    @Benchmark
    public void inRead(Blackhole bh) throws Throwable
    {
        for (UntypedResultSet.Row r : executeFormattedQuery(selectQuery, inKeys))
        {
            bh.consume(r.getLong("pk"));
            bh.consume(r.getLong("ck"));
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws Throwable
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursorReadsWas);
        DatabaseDescriptor.setSelectedSSTableFormat(selectedFormatWas);
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }
}
