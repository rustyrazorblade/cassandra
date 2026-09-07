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

package org.apache.cassandra.schema;


import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import one.profiler.AsyncProfiler;
import one.profiler.Counter;
import org.apache.cassandra.cql3.CQLTester;

/**
 * Times CREATE TABLE on a single node so the statement can be profiled. Not an assertion test;
 * it exists to give async-profiler a long enough run of the DDL path to sample.
 */
public class CreateTableLatencyTest extends CQLTester
{
    private static final int WARMUP = Integer.getInteger("createtable.warmup", 20);
    private static final int COUNT = Integer.getInteger("createtable.count", 300);
    private static final String PROFILE = System.getProperty("createtable.profile");
    private static final String OPTS = System.getProperty("createtable.profileopts", "event=cpu,interval=1ms");
    private static final int FROM = Integer.getInteger("createtable.from", 0);
    private static final int TO = Integer.getInteger("createtable.to", Integer.MAX_VALUE);

    @Test
    public void createTables()
    {
        for (int i = 0; i < WARMUP; i++)
            create("warm_" + i);

        AsyncProfiler profiler = null;

        long[] nanos = new long[COUNT];
        long start = System.nanoTime();
        for (int i = 0; i < COUNT; i++)
        {
            if (i == FROM)
                profiler = startProfiler();

            long t0 = System.nanoTime();
            create("bench_" + i);
            nanos[i] = System.nanoTime() - t0;

            if (i == TO)
            {
                stopProfiler(profiler);
                profiler = null;
            }
        }
        long total = System.nanoTime() - start;

        stopProfiler(profiler);
        report(nanos, total);
    }

    private AsyncProfiler startProfiler()
    {
        if (PROFILE == null)
            return null;

        AsyncProfiler profiler = AsyncProfiler.getInstance();
        try
        {
            profiler.execute("start," + OPTS);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return profiler;
    }

    private void stopProfiler(AsyncProfiler profiler)
    {
        if (profiler == null)
            return;

        try
        {
            Files.write(Paths.get(PROFILE + ".collapsed"), profiler.dumpCollapsed(Counter.SAMPLES).getBytes());
            profiler.execute("stop,file=" + PROFILE + ".html");
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void create(String name)
    {
        schemaChange("CREATE TABLE " + KEYSPACE + '.' + name + " (k int PRIMARY KEY, v text)");
    }

    private void report(long[] nanos, long total)
    {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        double sum = 0;
        for (long n : nanos)
            sum += n;

        StringBuilder sb = new StringBuilder();
        sb.append("\n=== CREATE TABLE latency, n=").append(nanos.length).append(" ===\n");
        sb.append(String.format("mean   %8.2f ms%n", sum / nanos.length / 1e6));
        sb.append(String.format("p50    %8.2f ms%n", ms(sorted[(int) (sorted.length * 0.50)])));
        sb.append(String.format("p90    %8.2f ms%n", ms(sorted[(int) (sorted.length * 0.90)])));
        sb.append(String.format("p99    %8.2f ms%n", ms(sorted[(int) (sorted.length * 0.99)])));
        sb.append(String.format("max    %8.2f ms%n", ms(sorted[sorted.length - 1])));
        sb.append(String.format("first  %8.2f ms%n", ms(nanos[0])));
        sb.append(String.format("last   %8.2f ms%n", ms(nanos[nanos.length - 1])));
        sb.append(String.format("wall   %8.2f s%n", TimeUnit.NANOSECONDS.toMillis(total) / 1000.0));
        sb.append("--- mean per block of ").append(Math.max(1, nanos.length / 10)).append(" ---\n");
        int block = Math.max(1, nanos.length / 10);
        for (int from = 0; from < nanos.length; from += block)
        {
            int to = Math.min(from + block, nanos.length);
            double blockSum = 0;
            for (int i = from; i < to; i++)
                blockSum += nanos[i];
            sb.append(String.format("tables %5d..%5d  %8.2f ms%n", from, to - 1, blockSum / (to - from) / 1e6));
        }
        System.out.println(sb);
        System.out.flush();
    }

    private static double ms(long n)
    {
        return n / 1e6;
    }
}
