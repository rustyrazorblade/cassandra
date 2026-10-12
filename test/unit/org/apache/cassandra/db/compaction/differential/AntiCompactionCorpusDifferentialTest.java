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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Every shape of the {@link DifferentialSchemas#minimalCorpus() minimal corpus}, on BIG and BTI,
 * anticompacted on both paths with random full, transient and unrepaired ranges.  The purge
 * shapes purge on both paths.  The run seed ({@link DifferentialRunSeed}) is logged, and
 * {@code -Dcassandra.test.differential.seed} set to it replays every shape.
 */
@RunWith(Parameterized.class)
public class AntiCompactionCorpusDifferentialTest extends DifferentialCompactionTester
{
    /** Range sets anticompacted for each shape. */
    private static final int RANGE_SETS = 3;

    @Parameterized.Parameter(0)
    public String format;

    @Parameterized.Parameter(1)
    public DifferentialSchema schema;

    @Parameterized.Parameters(name = "{0}-{1}")
    public static List<Object[]> parameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (String format : new String[]{ "big", "bti" })
            for (DifferentialSchema schema : DifferentialSchemas.minimalCorpus())
                params.add(new Object[]{ format, schema });
        return params;
    }

    @Before
    public void selectFormat()
    {
        selectSSTableFormat(format);
    }

    @After
    public void restoreFormat()
    {
        restoreSelectedFormat();
    }

    @Test
    public void cursorMatchesIteratorForShape() throws Exception
    {
        ColumnFamilyStore cfs = writeAndGuardShape(schema, 1);
        // the purge shapes use a one second TTL and gc_grace 0, so all of it expires within a second
        if (schema.expectsPurge())
            AntiCompactionDifferentialTest.awaitClockPast(FBUtilities.nowInSeconds() + 1);

        long seed = DifferentialRunSeed.SEED ^ (format + schema.name()).hashCode();
        logger.info("anticompaction corpus seed={} (run seed {}) format={} shape={}", seed, DifferentialRunSeed.SEED, format, schema.name());
        Random random = new Random(seed);
        for (int i = 0; i < RANGE_SETS; i++)
        {
            RangesAtEndpoint ranges = AntiCompactionRanges.randomRanges(random, AntiCompactionRanges.liveTokens(cfs), 1 + random.nextInt(64));
            try
            {
                assertCursorMatchesIteratorForAntiCompaction(cfs, ranges);
            }
            catch (Throwable t)
            {
                throw new AssertionError("failure; " + DifferentialRunSeed.replayAdvice(DifferentialRunSeed.SEED) + "; format " + format +
                                         ", shape " + schema.name() + ", range set " + i + ", ranges " + ranges + ": " + t, t);
            }
        }
    }
}
