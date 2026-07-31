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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.repair.ValidationCompactionController;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regression coverage for the partial-range (validation) {@link CursorCompactor} constructor
 * unregistering from {@link CompactionManager#active} when it throws after
 * {@link ActiveCompactionsTracker#beginCompaction} has already registered it. Without that cleanup a
 * failed setup (e.g. an I/O error opening the partial-range cursors) leaves a phantom
 * {@code nodetool compactionstats} entry that never clears, since {@code finishCompaction} otherwise
 * only runs from {@code close()} - which never runs on a constructor that threw.
 */
public class CursorCompactorRegistrationLeakTest extends CQLTester
{
    private static boolean isRegistered(TimeUUID compactionId)
    {
        for (CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
            if (compactionId.equals(holder.getCompactionInfo().getTaskId()))
                return true;
        return false;
    }

    @Test
    public void constructorFailureAfterBeginCompactionUnregisters() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "v" + ck);
        flush();

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        long nowInSec = FBUtilities.nowInSeconds();
        long gcBefore = cfs.getDefaultGcBefore(nowInSec);
        TimeUUID compactionId = nextTimeUUID();

        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            // An empty bounds list makes StatefulCursor.positionAt throw (asserts non-empty; even
            // with assertions off it fails on the empty iterator) - inside
            // convertSSTablesToPartialRangeCursors, which runs AFTER the constructor's
            // beginCompaction registration. This is exactly the ctor-throws-after-register path.
            Map<SSTableReader, List<PartitionPositionBounds>> emptyBounds =
                Collections.singletonMap(sstable, Collections.emptyList());

            assertFalse("precondition: id must not already be registered", isRegistered(compactionId));

            boolean threw = false;
            try
            {
                new CursorCompactor(OperationType.VALIDATION, emptyBounds, controller, nowInSec, compactionId,
                                    CompactionManager.instance.active);
            }
            catch (Throwable expected)
            {
                threw = true;
            }

            assertTrue("the constructor must throw for this scenario, or the test is vacuous", threw);
            assertFalse("a CursorCompactor whose constructor threw after beginCompaction must be unregistered " +
                        "from CompactionManager.instance.active (else it lingers in nodetool compactionstats)",
                        isRegistered(compactionId));
        }
    }
}
