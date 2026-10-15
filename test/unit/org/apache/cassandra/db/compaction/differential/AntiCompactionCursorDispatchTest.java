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
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.liveTokens;
import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.range;
import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.ranges;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Anticompaction takes the cursor path for exactly the tables that regular cursor compaction
 * supports, and only when {@code cursor_repair_enabled} is on.  The dispatch counter
 * {@link CompactionManager#cursorAnticompactionsRun} shows which path ran.
 */
@RunWith(Parameterized.class)
public class AntiCompactionCursorDispatchTest extends DifferentialCompactionTester
{
    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static List<Object[]> formats()
    {
        return List.of(new Object[]{ "big" }, new Object[]{ "bti" });
    }

    private boolean originalEnabled;

    @Before
    public void selectFormat()
    {
        selectSSTableFormat(format);
        originalEnabled = DatabaseDescriptor.cursorRepairEnabled();
    }

    @After
    public void restore()
    {
        restoreSelectedFormat();
        DatabaseDescriptor.setCursorRepairEnabled(originalEnabled);
    }

    @Test
    public void simpleTable() throws Throwable
    {
        assertDispatch("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))",
                       "INSERT INTO %s (pk, ck, v) VALUES (?, ?, 'x')", true);
    }

    @Test
    public void staticColumnsAndCollections() throws Throwable
    {
        assertDispatch("CREATE TABLE %s (pk bigint, ck bigint, s text static, m map<text, int>, l list<text>, PRIMARY KEY (pk, ck))",
                       "INSERT INTO %s (pk, ck, s, m, l) VALUES (?, ?, 's', {'a': 1}, ['x'])", true);
    }

    @Test
    public void multiCellUdt() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertDispatch("CREATE TABLE %s (pk bigint, ck bigint, u " + udt + ", PRIMARY KEY (pk, ck))",
                       "INSERT INTO %s (pk, ck, u) VALUES (?, ?, {a: 1, b: 'x'})", true);
    }

    @Test
    public void counters() throws Throwable
    {
        assertDispatch("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))",
                       "UPDATE %s SET c = c + 1 WHERE pk = ? AND ck = ?", true);
    }

    /** A legacy secondary index, named explicitly so the default index type of the target does not matter. */
    @Test
    public void legacyIndexUsesLegacyPath() throws Throwable
    {
        assertIndexedTableUsesLegacyPath("legacy_local_table");
    }

    @Test
    public void saiIndexUsesLegacyPath() throws Throwable
    {
        assertIndexedTableUsesLegacyPath("sai");
    }

    /** An indexed table falls back, and the legacy result is right: every row stays and the index still finds them. */
    private void assertIndexedTableUsesLegacyPath(String indexType) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s (v) USING '" + indexType + "'");
        ColumnFamilyStore cfs = writeTwoSSTables("INSERT INTO %s (pk, ck, v) VALUES (?, ?, 'x')");
        assertDispatchMatchesSupport(false);
        assertEquals("rows were lost", 40, execute("SELECT * FROM %s").size());
        assertEquals("the index lost rows", 40, execute("SELECT * FROM %s WHERE v = 'x'").size());
        int pending = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (sstable.isPendingRepair())
                pending++;
        assertEquals("the full and transient outputs are pending", 2, pending);
    }

    @Test
    public void droppedCollectionUsesLegacyPath() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, PRIMARY KEY (pk, ck))");
        writeTwoSSTables("INSERT INTO %s (pk, ck, m, v) VALUES (?, ?, {'a': 'b'}, 'x')");
        execute("ALTER TABLE %s DROP m");
        assertDispatchMatchesSupport(false);
    }

    /** A dropped counter column still in a header takes the cursor path, as regular cursor compaction does. */
    @Test
    public void droppedCounterUsesCursorPath() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, d counter, PRIMARY KEY (pk, ck))");
        writeTwoSSTables("UPDATE %s SET c = c + 1, d = d + 1 WHERE pk = ? AND ck = ?");
        execute("ALTER TABLE %s DROP c");
        assertDispatchMatchesSupport(true);
    }

    /** With the setting off, a supported table takes the legacy path. */
    @Test
    public void settingOffUsesLegacyPath() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = writeTwoSSTables("INSERT INTO %s (pk, ck, v) VALUES (?, ?, 'x')");
        assertTrue(isSupportedNow(cfs));
        long fallbacksBefore = CompactionManager.cursorAnticompactionFallbacks.sum();
        assertEquals("cursor runs with the setting off", 0, antiCompactOnce(cfs, false));
        assertEquals("the setting off is not a fallback", 0, CompactionManager.cursorAnticompactionFallbacks.sum() - fallbacksBefore);
    }

    /** The setting does not depend on cursor_compaction_enabled. */
    @Test
    public void settingWorksWithCursorCompactionOff() throws Throwable
    {
        boolean originalCompaction = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(false);
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = writeTwoSSTables("INSERT INTO %s (pk, ck, v) VALUES (?, ?, 'x')");
            assertEquals(1, antiCompactOnce(cfs, true));
        }
        finally
        {
            DatabaseDescriptor.setCursorCompactionEnabled(originalCompaction);
        }
    }

    /** The production entry point, performAnticompaction, reaches the cursor path. */
    @Test
    public void performAnticompactionUsesCursorPath() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = writeTwoSSTables("INSERT INTO %s (pk, ck, v) VALUES (?, ?, 'x')");
        List<Long> tokens = liveTokens(cfs);
        RangesAtEndpoint ranges = ranges(List.of(range(tokens.get(2), tokens.get(10))), List.of(range(tokens.get(10), tokens.get(14))));

        DatabaseDescriptor.setCursorRepairEnabled(true);
        TimeUUID session = TimeUUID.Generator.nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(session, FBUtilities.getBroadcastAddressAndPort(), List.of(cfs),
                                                                   ImmutableSet.copyOf(ranges.ranges()), true,
                                                                   ActiveRepairService.UNREPAIRED_SSTABLE, true, PreviewKind.NONE);
        try
        {
            long before = CompactionManager.cursorAnticompactionsRun.sum();
            Set<SSTableReader> sstables = cfs.getLiveSSTables();
            LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
            assertNotNull(txn);
            CompactionManager.instance.performAnticompaction(cfs, ranges, Refs.ref(sstables), txn, session, () -> false);
            assertEquals("the cursor path did not run", 1, CompactionManager.cursorAnticompactionsRun.sum() - before);
            assertEquals("full, transient and unrepaired outputs", 3, cfs.getLiveSSTables().size());
        }
        finally
        {
            ActiveRepairService.instance().removeParentRepairSession(session);
        }
    }

    private void assertDispatch(String ddl, String write, boolean expectSupported) throws Throwable
    {
        createTable(ddl);
        writeTwoSSTables(write);
        assertDispatchMatchesSupport(expectSupported);
    }

    /** Anticompaction runs the cursor path if and only if regular cursor compaction supports the table. */
    private void assertDispatchMatchesSupport(boolean expectSupported) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        boolean supported = isSupportedNow(cfs);
        assertEquals("regular cursor compaction support for this table", expectSupported, supported);
        long fallbacksBefore = CompactionManager.cursorAnticompactionFallbacks.sum();
        assertEquals("cursor anticompaction runs if and only if cursor compaction supports the table",
                     supported ? 1 : 0, antiCompactOnce(cfs, true));
        assertEquals("a group that the cursor path does not support is counted as a fallback",
                     supported ? 0 : 1, CompactionManager.cursorAnticompactionFallbacks.sum() - fallbacksBefore);
    }

    /** @return how many times the cursor path ran */
    private long antiCompactOnce(ColumnFamilyStore cfs, boolean enabled)
    {
        DatabaseDescriptor.setCursorRepairEnabled(enabled);
        List<Long> tokens = liveTokens(cfs);
        RangesAtEndpoint ranges = ranges(List.of(range(tokens.get(2), tokens.get(10))), List.of(range(tokens.get(10), tokens.get(14))));
        long before = CompactionManager.cursorAnticompactionsRun.sum();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.ANTICOMPACTION))
        {
            assertNotNull(txn);
            int outputs = CompactionManager.instance.antiCompactGroup(cfs, ranges, txn, TimeUUID.Generator.nextTimeUUID(), () -> false);
            assertEquals("full, transient and unrepaired outputs", 3, outputs);
        }
        return CompactionManager.cursorAnticompactionsRun.sum() - before;
    }

    /** Twenty partitions in two overlapping sstables; {@code write} binds pk and ck. */
    private ColumnFamilyStore writeTwoSSTables(String write) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 20; pk++)
                execute(write, pk, (long) round);
            flush();
        }
        assertEquals(2, cfs.getLiveSSTables().size());
        return cfs;
    }

    /** The regular cursor compaction gate, over the scanners and controller antiCompactGroup builds. */
    private static boolean isSupportedNow(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        try (CompactionController controller = new CompactionController(cfs, inputs, CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));
             AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs)))
        {
            return CursorCompactor.isSupported(scanners, controller);
        }
    }
}
