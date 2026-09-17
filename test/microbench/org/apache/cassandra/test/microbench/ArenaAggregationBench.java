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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.marshal.Int32Type;

/**
 * Benchmark comparing arena-based sort/dedup vs on-heap implementation.
 *
 * Operations tested:
 * - ORDER BY non-clustering column + LIMIT over large single partition
 * - DISTINCT on arbitrary columns over large single partition
 *
 * Both implementations use type-aware comparison (AbstractType.compare).
 * The arena version uses Panama FFM with off-heap buffers.
 * The on-heap version uses nested byte-array lists plus Collections.sort and HashSet.
 *
 * Run with: ant jmh-microbench -Djmh.benchmarks=ArenaAggregationBench -Djmh.args="-prof gc"
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@State(Scope.Benchmark)
public class ArenaAggregationBench
{
    @Param({"100000", "1000000"})
    int rowCount;

    @Param({"orderBy", "distinct"})
    String operation;

    private List<List<byte[]>> rows;
    private int limit = 100;

    @Setup(Level.Trial)
    public void setup()
    {
        Random rand = new Random(42);
        rows = new ArrayList<>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            List<byte[]> row = new ArrayList<>(3);
            // Column 0: partition key (always 1)
            row.add(Int32Type.instance.decompose(1).array());
            // Column 1: clustering key (sequential)
            row.add(Int32Type.instance.decompose(i).array());
            // Column 2: random value for sorting/dedup
            row.add(Int32Type.instance.decompose(rand.nextInt(rowCount / 10)).array());
            rows.add(row);
        }
    }

    /**
     * Arena-based sort + LIMIT (off-heap).
     * Placeholder: would use ArenaAggregationOperator.
     * For now, measures the on-heap baseline to ensure the benchmark compiles.
     */
    @Benchmark
    public List<List<byte[]>> arenaSortLimit()
    {
        // Placeholder: actual arena impl would go here
        // For now, use on-heap as baseline
        return heapSortLimit();
    }

    /**
     * On-heap sort + LIMIT.
     * Materializes nested byte-array lists, sorts, takes first N.
     */
    @Benchmark
    public List<List<byte[]>> heapSortLimit()
    {
        List<List<byte[]>> copy = new ArrayList<>(rows);

        // Sort by column 2 (the random value)
        copy.sort((a, b) -> Int32Type.instance.compare(
            ByteBuffer.wrap(a.get(2)),
            ByteBuffer.wrap(b.get(2))));

        // Take first N
        return copy.subList(0, Math.min(limit, copy.size()));
    }

    /**
     * Arena-based DISTINCT (off-heap hash table).
     * Placeholder: would use ArenaGroupTable.
     */
    @Benchmark
    public List<List<byte[]>> arenaDistinct()
    {
        // Placeholder: actual arena impl would go here
        return heapDistinct();
    }

    /**
     * On-heap DISTINCT (HashSet).
     * Uses row hash + type-aware equality.
     */
    @Benchmark
    public List<List<byte[]>> heapDistinct()
    {
        Set<ByteBuffer> seen = new HashSet<>();
        List<List<byte[]>> result = new ArrayList<>();

        for (List<byte[]> row : rows)
        {
            // Dedup on column 2
            ByteBuffer key = ByteBuffer.wrap(row.get(2));
            if (seen.add(key))
            {
                result.add(row);
            }
        }

        return result;
    }
}
