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

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import com.sun.management.ThreadMXBean;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.DiskErrorsHandlerService;

/**
 * Drives {@link CommitLog#add} in a tight loop so a profiler can be attached to the write path on its own.
 *
 * Prints READY once the warmup is done and DONE once the measured phase is over, so a harness can start and
 * stop the profiler around the measured phase only. Reports the heap bytes the benchmark thread allocated and
 * the throughput it reached.
 *
 * Usage: CommitLogAddBench &lt;payload bytes&gt; &lt;warmup iterations&gt; &lt;measured iterations&gt; [pool size]
 */
public class CommitLogAddBench
{
    private static final String KEYSPACE = "commitlog_add_bench";
    private static final String TABLE = "standard1";

    public static void main(String[] args) throws Exception
    {
        try
        {
            run(args);
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.out);
            System.out.flush();
            Runtime.getRuntime().halt(1);
        }
    }

    private static void run(String[] args) throws Exception
    {
        int payloadSize = Integer.parseInt(args[0]);
        int warmupIterations = Integer.parseInt(args[1]);
        int measuredIterations = Integer.parseInt(args[2]);
        int poolSize = args.length > 3 ? Integer.parseInt(args[3]) : 50_000;

        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        org.apache.cassandra.ServerTestUtils.daemonInitialization();
        DiskErrorsHandlerService.configure();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE, 0, AsciiType.instance, BytesType.instance));
        CommitLog.instance.start();

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        ByteBuffer payload = randomPayload(payloadSize);

        // A fresh mutation per iteration, as on the write path. Reusing one would cache its serialization after
        // the first call and measure something no production write does.
        for (int i = 0; i < warmupIterations; i++)
            CommitLog.instance.add(mutation(cfs, payload, i));

        // Built up front so the measured phase holds nothing but CommitLog.add. Building them inside the loop
        // would bury the write path under RowUpdateBuilder's own allocation. The measured loop cycles through
        // the pool, which keeps the run long enough to profile without holding every mutation in memory. Each
        // one carries its cached serialization by then, as a mutation does by the time it reaches the commit
        // log on the real write path.
        Mutation[] pool = new Mutation[Math.min(measuredIterations, poolSize)];
        for (int i = 0; i < pool.length; i++)
            pool[i] = mutation(cfs, payload, warmupIterations + i);

        System.out.println("READY");
        System.out.flush();

        ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        long threadId = Thread.currentThread().getId();
        long bytesBefore = threads.getThreadAllocatedBytes(threadId);
        long startNanos = System.nanoTime();

        for (int i = 0; i < measuredIterations; i++)
            CommitLog.instance.add(pool[i % pool.length]);

        long elapsedNanos = System.nanoTime() - startNanos;
        long allocated = threads.getThreadAllocatedBytes(threadId) - bytesBefore;

        // Hold the process open so the harness can dump and stop the profiler before the JVM exits. Sleeping
        // allocates nothing, so the tail of the profile stays empty.
        System.out.println("DONE");
        System.out.flush();
        if (Boolean.getBoolean("bench.hold"))
            Thread.sleep(10_000);

        System.out.printf("payload_bytes=%d%n", payloadSize);
        System.out.printf("iterations=%d%n", measuredIterations);
        System.out.printf("pool_size=%d%n", pool.length);
        System.out.printf("elapsed_ms=%d%n", elapsedNanos / 1_000_000);
        System.out.printf("ops_per_sec=%.1f%n", measuredIterations / (elapsedNanos / 1e9));
        System.out.printf("heap_bytes_allocated=%d%n", allocated);
        System.out.printf("heap_bytes_per_op=%.1f%n", (double) allocated / measuredIterations);
        System.out.flush();

        System.exit(0);
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
