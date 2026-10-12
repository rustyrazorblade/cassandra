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

package org.apache.cassandra.db.compaction.differential;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The random schemas and workloads of {@link RandomDifferentialCompactionTest}, anticompacted with
 * random full, transient and unrepaired ranges on both paths.  The ranges and the choice to purge
 * come from the example's seed, so an example replays alone when its seed is the run seed.  Every
 * other example runs 60 days in the future, past the workload's TTLs and gc grace, so that it
 * purges.  The selected sstable format is the one the test target selects: BIG for {@code test},
 * BTI for {@code test-latest}.
 */
public class RandomAntiCompactionDifferentialTest extends RandomDifferentialCompactionTest
{
    private static final long PURGE_OFFSET_SECONDS = TimeUnit.DAYS.toSeconds(60);

    /** The example the comparisons below were counted for, and how many it has had. */
    private long countedExample;
    private int comparisonsInExample;

    @Override
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs, Set<SSTableReader> inputs,
                                                         TaskFactory taskFactory, long gcBefore) throws Exception
    {
        boolean purge = purges(exampleSeed);
        logger.info("random anticompaction example seed={} purges={}", exampleSeed, purge);
        if (!purge)
            return assertAntiCompactionMatchesIterator(cfs, inputs, taskFactory, gcBefore);
        long purgeNow = FBUtilities.nowInSeconds() + PURGE_OFFSET_SECONDS;
        return assertAntiCompactionMatchesIterator(cfs, inputs, taskWithFixedNow(purgeNow), cfs.getDefaultGcBefore(purgeNow));
    }

    /**
     * Each example after the first gets its seed from the one before by a linear congruential step
     * with an odd multiplier and an odd addend, so the lowest bit flips from one example to the next:
     * every other example purges, and the choice depends only on the example's seed.
     */
    static boolean purges(long exampleSeed)
    {
        return (exampleSeed & 1) == 1;
    }

    @Override
    protected long anticompactionRangesSeed(int comparison)
    {
        if (comparisonsInExample == 0 || countedExample != exampleSeed)
        {
            countedExample = exampleSeed;
            comparisonsInExample = 0;
        }
        return exampleSeed * 31 + comparisonsInExample++;
    }

    @Override
    protected long replaySeed()
    {
        return exampleSeed;
    }
}
