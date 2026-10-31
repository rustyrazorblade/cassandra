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
package org.apache.cassandra.distributed.test;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With memtable_flush_throttle_enabled, a flush charges the compaction limiter, but the flushes of a drain do not, and
 * drain lifts compaction_throughput, so a flush or compaction already waiting on it does not hold the drain up.
 */
public class FlushThrottleDrainTest extends TestBaseImpl
{
    @Test
    public void drainIsNotThrottled() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("memtable_flush_throttle_enabled", true)
                                                                       .set("compaction_throughput", "16MiB/s"))
                                           .start()))
        {
            IInvokableInstance node = cluster.get(1);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v text) WITH compression = {'enabled': false}"));
            insert(cluster, 0);
            node.flush(KEYSPACE);
            long flushCharged = node.callOnInstance(() -> CompactionManager.instance.getRateLimiter().priorityBytesCharged());
            assertThat(flushCharged).as("a flush with the option on charges the limiter").isPositive();
            assertThat(node.callOnInstance(() -> CompactionManager.instance.getRateLimiter().isLimited())).isTrue();

            insert(cluster, 1_000);
            node.nodetoolResult("drain").asserts().success();
            assertThat(node.callOnInstance(() -> CompactionManager.instance.getRateLimiter().priorityBytesCharged()))
                .as("the drain flush does not charge the limiter")
                .isEqualTo(flushCharged);
            assertThat(node.callOnInstance(() -> CompactionManager.instance.getRateLimiter().isLimited()))
                .as("drain lifts the limit")
                .isFalse();
        }
    }

    private static void insert(Cluster cluster, int from)
    {
        for (int pk = from; pk < from + 1_000; pk++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"), ConsistencyLevel.ONE, pk, "value " + pk);
    }
}
