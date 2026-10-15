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

package org.apache.cassandra.db.repair;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.repair.LocalValidation;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;

/**
 * cursor_repair_enabled alone decides whether repair validation and anticompaction take the
 * cursor path; cursor_compaction_enabled does not change it either way.
 */
public class CursorRepairEnabledTest extends CQLTester
{
    private boolean originalCompaction;
    private boolean originalRepair;

    @Before
    public void saveFlags()
    {
        originalCompaction = DatabaseDescriptor.cursorCompactionEnabled();
        originalRepair = DatabaseDescriptor.cursorRepairEnabled();
    }

    @After
    public void restoreFlags()
    {
        DatabaseDescriptor.setCursorCompactionEnabled(originalCompaction);
        DatabaseDescriptor.setCursorRepairEnabled(originalRepair);
    }

    @Test
    public void repairOnCompactionOffRunsTheCursorPath() throws Throwable
    {
        assertPaths(false, true);
    }

    @Test
    public void repairOffCompactionOnRunsTheLegacyPath() throws Throwable
    {
        assertPaths(true, false);
    }

    private void assertPaths(boolean cursorCompaction, boolean cursorRepair) throws Throwable
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursorCompaction);
        DatabaseDescriptor.setCursorRepairEnabled(cursorRepair);
        String where = "cursor_compaction_enabled=" + cursorCompaction + " cursor_repair_enabled=" + cursorRepair + ": ";
        createTable("CREATE TABLE %s (k bigint, c int, v text, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int flush = 0; flush < 2; flush++)
        {
            for (long k = 0; k < 200; k++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, flush, "v" + k);
            flush();
        }

        // a full validation of the whole ring
        List<Range<Token>> ring = List.of(new Range<>(Murmur3Partitioner.MINIMUM, Murmur3Partitioner.MINIMUM));
        TimeUUID parent = TimeUUID.Generator.nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(parent, FBUtilities.getBroadcastAddressAndPort(), List.of(cfs), ring,
                                                                   false, ActiveRepairService.UNREPAIRED_SSTABLE, true, PreviewKind.NONE);
        long validations = CassandraTableRepairManager.cursorValidationsRun.sum();
        long validationFallbacks = CassandraTableRepairManager.cursorValidationFallbacks.sum();
        try
        {
            LocalValidation.validate(cfs, parent, TimeUUID.Generator.nextTimeUUID(), ring, false, PreviewKind.NONE, FBUtilities.nowInSeconds(), false);
        }
        finally
        {
            ActiveRepairService.instance().removeParentRepairSession(parent);
        }
        assertEquals(where + "cursor validations", cursorRepair ? 1 : 0, CassandraTableRepairManager.cursorValidationsRun.sum() - validations);
        assertEquals(where + "cursor validation fallbacks", 0, CassandraTableRepairManager.cursorValidationFallbacks.sum() - validationFallbacks);

        // anticompaction of all sstables with the first half of the ring repaired
        RangesAtEndpoint half = RangesAtEndpoint.of(Replica.fullReplica(FBUtilities.getBroadcastAddressAndPort(),
                                                                        new Range<>(new Murmur3Partitioner.LongToken(Long.MIN_VALUE),
                                                                                    new Murmur3Partitioner.LongToken(0))));
        long anticompactions = CompactionManager.cursorAnticompactionsRun.sum();
        long anticompactionFallbacks = CompactionManager.cursorAnticompactionFallbacks.sum();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.ANTICOMPACTION))
        {
            CompactionManager.instance.antiCompactGroup(cfs, half, txn, TimeUUID.Generator.nextTimeUUID(), () -> false);
        }
        assertEquals(where + "cursor anticompactions", cursorRepair ? 1 : 0, CompactionManager.cursorAnticompactionsRun.sum() - anticompactions);
        assertEquals(where + "cursor anticompaction fallbacks", 0, CompactionManager.cursorAnticompactionFallbacks.sum() - anticompactionFallbacks);
    }
}
