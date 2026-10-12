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

package org.apache.cassandra.db.commitlog;

import org.junit.internal.AssumptionViolatedException;

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * Runs a seeded test a fixed number of times, chaining seeds so every example is individually
 * reproducible from the one printed on failure. Copied from RandomDifferentialCompactionTest, which took
 * it from RandomSchemaTest; the commit log properties reproduce by seed for the same reason those do,
 * because shrinking a failure that owns a server fixture means re-running the fixture hundreds of times.
 */
final class CommitLogSeedRunner
{
    private static final long multiplier = 0x5DEECE66DL;
    private static final long addend = 0xBL;
    private static final long mask = (1L << 48) - 1;

    private long seed = CassandraRelevantProperties.TEST_COMMITLOG_SEED.getLong(System.currentTimeMillis());
    private final int examples;

    CommitLogSeedRunner(int examples)
    {
        this.examples = examples;
    }

    /** Dead code on purpose: plug a failing seed in here to reproduce. */
    @SuppressWarnings("unused")
    CommitLogSeedRunner withFixedSeed(long seed)
    {
        this.seed = seed;
        return this;
    }

    interface SeededTest
    {
        void run(long seed) throws Throwable;
    }

    void run(SeededTest test) throws Throwable
    {
        for (int i = 0; i < examples; i++)
        {
            if (i > 0)
                seed = (seed * multiplier + addend) & mask;
            try
            {
                test.run(seed);
            }
            catch (AssumptionViolatedException a)
            {
                // an Assume skip has to stay a skip: JUnit decides skip-vs-fail on the type thrown, not
                // on its cause, so wrapping this below would turn a soak red
                throw a;
            }
            catch (Throwable t)
            {
                // keep the cause's detail in the message: junit XML only preserves the top-level message
                throw new AssertionError("Failure for seed " + seed + " (example " + i + "): " + t.getMessage(), t);
            }
        }
    }
}
