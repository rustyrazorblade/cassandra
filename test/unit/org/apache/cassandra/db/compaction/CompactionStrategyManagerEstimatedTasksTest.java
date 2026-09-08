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

package org.apache.cassandra.db.compaction;

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link CompactionStrategyManager#getEstimatedRemainingTasks()} is read by every metrics scrape, so it walks
 * the strategy holders directly instead of building a Guava concat/transform chain. This pins the total it
 * produces to the sum over the strategies the manager exposes.
 */
public class CompactionStrategyManagerEstimatedTasksTest extends CQLTester
{
    private static int sumOverStrategies(CompactionStrategyManager csm)
    {
        int tasks = 0;
        for (List<AbstractCompactionStrategy> group : csm.getStrategies())
            for (AbstractCompactionStrategy strategy : group)
                tasks += strategy.getEstimatedRemainingTasks();
        return tasks;
    }

    @Test
    public void matchesSumOverStrategies()
    {
        // LeveledCompactionStrategy computes its estimate on demand from the manifest. SizeTiered caches it
        // as a side effect of the background compaction loop, which is disabled here, so it would report 0.
        // With STCS-in-L0 enabled, the manifest estimates one task per max_threshold sstables in L0.
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = " +
                    "{'class':'LeveledCompactionStrategy', 'max_threshold':'4'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        CompactionStrategyManager csm = cfs.getCompactionStrategyManager();
        assertEquals(0, csm.getEstimatedRemainingTasks());
        assertEquals(sumOverStrategies(csm), csm.getEstimatedRemainingTasks());

        // Eight sstables in L0 against a max_threshold of 4 gives a deterministic estimate of two tasks.
        for (int sstable = 0; sstable < 8; sstable++)
        {
            for (int i = 0; i < 50; i++)
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", sstable * 50 + i, i);
            flush();
        }

        int expected = sumOverStrategies(csm);
        assertTrue("expected pending compactions across 8 L0 sstables, got " + expected, expected > 0);
        assertEquals(expected, csm.getEstimatedRemainingTasks());

        // Repeated reads are stable; nothing about the traversal mutates strategy state.
        assertEquals(expected, csm.getEstimatedRemainingTasks());
    }
}
