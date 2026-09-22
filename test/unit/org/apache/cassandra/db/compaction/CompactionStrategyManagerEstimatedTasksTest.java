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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;

import static org.junit.Assert.assertEquals;

/**
 * {@link CompactionStrategyManager#getEstimatedRemainingTasks()} is read by every metrics scrape, so it walks
 * the strategy holders directly instead of building a Guava concat/transform chain. This pins the total it
 * produces to the sum over every holder the manager owns.
 */
public class CompactionStrategyManagerEstimatedTasksTest extends CQLTester
{
    // Fill an empty column family store with eight L0 sstables, 50 rows each.
    private void writeEightL0SStables(int keyBase)
    {
        for (int sstable = 0; sstable < 8; sstable++)
        {
            for (int i = 0; i < 50; i++)
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", keyBase + sstable * 50 + i, i);
            flush();
        }
    }

    @Test
    public void matchesConcreteEstimate()
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

        // Eight L0 sstables against a max_threshold of 4 give a deterministic estimate of two tasks.
        writeEightL0SStables(0);
        assertEquals(2, csm.getEstimatedRemainingTasks());

        // Repeated reads are stable; nothing about the traversal mutates strategy state.
        assertEquals(2, csm.getEstimatedRemainingTasks());
    }

    @Test
    public void sumsAcrossHolders() throws Exception
    {
        // The manager owns four holders (repaired, unrepaired, pending and transient repairs). The metric
        // must sum over every holder, not just the unrepaired one. This exercises two non-empty holders so
        // a bug that dropped the repaired holder from the sum would fail here.
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = " +
                    "{'class':'LeveledCompactionStrategy', 'max_threshold':'4'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        CompactionStrategyManager csm = cfs.getCompactionStrategyManager();

        // Eight L0 sstables, then mark them repaired so they move to the repaired holder: two tasks there.
        writeEightL0SStables(0);
        List<SSTableReader> repaired = new ArrayList<>(cfs.getLiveSSTables());
        csm.mutateRepaired(repaired, System.currentTimeMillis(), ActiveRepairService.NO_PENDING_REPAIR, false);
        assertEquals(2, csm.getEstimatedRemainingTasks());

        // Eight more L0 sstables stay unrepaired: two tasks in the unrepaired holder. The total is the sum.
        writeEightL0SStables(1000);
        assertEquals(4, csm.getEstimatedRemainingTasks());
    }
}
