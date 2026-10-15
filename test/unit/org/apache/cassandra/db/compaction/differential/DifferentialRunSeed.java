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

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * The one run seed of the seeded differential tests and burns: {@code -Dcassandra.test.differential.seed},
 * or the clock when it is not set.  It is read once, so every test of a run that derives its
 * seeds from it replays with that one value.  Each user logs the seed it derives.
 */
public final class DifferentialRunSeed
{
    public static final long SEED = CassandraRelevantProperties.TEST_DIFFERENTIAL_SEED.getLong(System.currentTimeMillis());

    private DifferentialRunSeed()
    {
    }

    /** How to replay a seed: the property, passed through test-isolated.sh, or through -Dtest.jvm.args with plain ant. */
    public static String replayAdvice(long seed)
    {
        String property = "-D" + CassandraRelevantProperties.TEST_DIFFERENTIAL_SEED.getKey() + '=' + seed;
        return "replay with " + property + " through test-isolated.sh, or -Dtest.jvm.args=" + property + " with plain ant";
    }
}
