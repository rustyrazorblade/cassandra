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

import com.google.common.util.concurrent.RateLimiter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.io.util.ThroughputLimiter;

/**
 * The cost of one charge to the compaction limiter, against the Guava limiter it replaces.  "unlimited" is the rate
 * that compaction_throughput: 0 sets.  "limited" is a rate high enough that no charge waits.  The "mixed" group
 * charges with six compaction threads and two flush threads at once, flush charging 1 MiB with acquirePriority.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class ThroughputLimiterBench
{
    private static final int CHUNK = 16 << 10;

    @Param({ "unlimited", "limited" })
    public String rate;

    private ThroughputLimiter limiter;
    private RateLimiter guava;

    @Setup
    public void setUp()
    {
        double bytesPerSecond = rate.equals("unlimited") ? Double.MAX_VALUE : 1e15;
        limiter = new ThroughputLimiter(bytesPerSecond);
        guava = RateLimiter.create(bytesPerSecond);
    }

    @Benchmark
    @Threads(1)
    public void throughputLimiter()
    {
        limiter.acquire(CHUNK);
    }

    @Benchmark
    @Threads(8)
    public void throughputLimiter8Threads()
    {
        limiter.acquire(CHUNK);
    }

    @Benchmark
    @Threads(1)
    public void guavaRateLimiter()
    {
        guava.acquire(CHUNK);
    }

    @Benchmark
    @Threads(8)
    public void guavaRateLimiter8Threads()
    {
        guava.acquire(CHUNK);
    }

    @Benchmark
    @Threads(1)
    public void throughputLimiterPriority()
    {
        limiter.acquirePriority(CHUNK);
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(6)
    public void mixedAcquire()
    {
        limiter.acquire(CHUNK);
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(2)
    public void mixedAcquirePriority()
    {
        limiter.acquirePriority(1 << 20);
    }
}
