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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.LegacySSTableTest;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Anticompaction of the sstables of every legacy version, with cursor_repair_enabled on.  The
 * cursor path rejects each version that is not the latest of its format.  The fixtures use
 * ByteOrderedPartitioner, which the cursor path also rejects, so every version falls back to the
 * legacy path and is counted.  The rows read back the same, and the outputs are pending for the
 * session or unrepaired as their ranges say.
 */
public class LegacySSTableAntiCompactionTest
{
    private static final String[] TABLES = { "simple", "simple_counter", "clust", "clust_counter" };

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        LegacySSTableTest.defineSchema();
    }

    @After
    public void tearDown() throws Exception
    {
        DatabaseDescriptor.setCursorRepairEnabled(true);
        for (String version : LegacySSTableTest.legacyVersions)
            LegacySSTableTest.truncateLegacyTables(version);
    }

    @Test
    public void anticompactEveryLegacyVersion() throws Exception
    {
        DatabaseDescriptor.setCursorRepairEnabled(true);
        for (String version : LegacySSTableTest.legacyVersions)
        {
            LegacySSTableTest.loadLegacyTables(version);
            for (String suffix : TABLES)
                assertAntiCompaction(version, String.format("legacy_%s_%s", version, suffix));
        }
    }

    private static void assertAntiCompaction(String version, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        Set<SSTableReader> inputs = new HashSet<>(cfs.getLiveSSTables());
        assertFalse(table + " has no sstable", inputs.isEmpty());
        String where = table + " (version " + version + "): ";
        boolean latest = true;
        for (SSTableReader sstable : inputs)
            latest &= sstable.descriptor.version.isLatestVersion();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(inputs))
        {
            String reason = CursorCompactor.unsupportedScannersReason(cfs.metadata(), scanners);
            if (latest)
                assertNull(where + "the cursor path rejects the latest version: " + reason, reason);
            else
                assertTrue(where + "the cursor path does not reject an old version: " + reason, reason != null && reason.contains("is not the latest"));
        }
        List<String> rowsBefore = rows(table);

        // the first half of the partitions are in the full range
        List<Token> tokens = new ArrayList<>();
        for (SSTableReader sstable : inputs)
            tokens.add(sstable.getFirst().getToken());
        tokens.sort(Token::compareTo);
        Token split = tokens.get(tokens.size() / 2);
        RangesAtEndpoint ranges = RangesAtEndpoint.of(Replica.fullReplica(FBUtilities.getBroadcastAddressAndPort(),
                                                                          new Range<>(cfs.getPartitioner().getMinimumToken(), split)));
        long runsBefore = CompactionManager.cursorAnticompactionsRun.sum();
        long fallbacksBefore = CompactionManager.cursorAnticompactionFallbacks.sum();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.ANTICOMPACTION))
        {
            assertNotNull(txn);
            CompactionManager.instance.antiCompactGroup(cfs, ranges, txn, nextTimeUUID(), () -> false);
        }
        assertEquals(where + "the cursor path ran", 0, CompactionManager.cursorAnticompactionsRun.sum() - runsBefore);
        assertEquals(where + "the fallback was not counted", 1, CompactionManager.cursorAnticompactionFallbacks.sum() - fallbacksBefore);
        assertEquals(where + "the rows changed", rowsBefore, rows(table));
        for (SSTableReader out : cfs.getLiveSSTables())
        {
            assertFalse(where + "an input is still live: " + out, inputs.contains(out));
            boolean full = out.getLast().getToken().compareTo(split) <= 0;
            boolean unrepaired = out.getFirst().getToken().compareTo(split) > 0;
            assertTrue(where + "an output spans the end of the full range: " + out, full || unrepaired);
            assertEquals(where + "an output is in the wrong repair state: " + out, full, out.isPendingRepair());
        }
        org.apache.cassandra.db.compaction.AntiCompactionAssertions.assertEveryFileBelongsToALiveSSTable(cfs);
    }

    private static List<String> rows(String table)
    {
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : QueryProcessor.executeInternal("SELECT * FROM legacy_tables." + table))
        {
            StringBuilder values = new StringBuilder();
            for (ColumnSpecification column : row.getColumns())
                values.append(column.name).append('=').append(row.has(column.name.toString()) ? ByteBufferUtil.bytesToHex(row.getBytes(column.name.toString())) : "null").append(' ');
            rows.add(values.toString());
        }
        assertTrue(table + " has no rows", !rows.isEmpty());
        rows.sort(String::compareTo);
        return rows;
    }
}
