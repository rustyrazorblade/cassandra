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

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.DiskErrorsHandlerService;

/**
 * Measures the throughput and per-op allocation of {@link CommitLog#add} on the write path.
 *
 * Run with the JMH gc profiler ({@code -prof gc}) to read the heap bytes allocated per op from
 * {@code gc.alloc.rate.norm}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CommitLogAddBench
{
    private static final String KEYSPACE = "commitlog_add_bench";
    private static final String TABLE = "standard1";

    @Param("100")
    public int payloadSize = 100;

    @Param("50000")
    public int poolSize = 50000;

    private Mutation[] pool;
    private int index;

    @Setup(Level.Trial)
    public void setup() throws Exception
    {
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        ServerTestUtils.daemonInitialization();
        DiskErrorsHandlerService.configure();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE, 0, AsciiType.instance, BytesType.instance));
        CommitLog.instance.start();

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        ByteBuffer payload = randomPayload(payloadSize);

        // Build the mutations up front so the measured op holds nothing but CommitLog.add. Each one carries its
        // cached serialization by the time it reaches the commit log, as a mutation does on the real write path.
        // The benchmark cycles through the pool.
        pool = new Mutation[poolSize];
        for (int i = 0; i < pool.length; i++)
            pool[i] = mutation(cfs, payload, i);
    }

    @TearDown(Level.Trial)
    public void teardown() throws InterruptedException, ExecutionException
    {
        CommitLog.instance.shutdownBlocking();
    }

    @Benchmark
    public CommitLogPosition add() throws Exception
    {
        Mutation mutation = pool[index];
        index = index + 1 == pool.length ? 0 : index + 1;
        return CommitLog.instance.add(mutation);
    }

    private static Mutation mutation(ColumnFamilyStore cfs, ByteBuffer payload, int i)
    {
        return new RowUpdateBuilder(cfs.metadata(), 0, "key" + i).clustering("bytes")
                                                                 .add("val", payload)
                                                                 .build();
    }

    private static ByteBuffer randomPayload(int size)
    {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }
}
