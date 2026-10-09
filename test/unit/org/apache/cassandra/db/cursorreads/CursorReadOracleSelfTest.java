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

package org.apache.cassandra.db.cursorreads;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Proves every {@link CursorReadOracle} surface can fail.  Each {@code CursorReads.TEST_*} flag must
 * trip the surface it corrupts, the test-side paging-state hook must trip S3, a leg read by the
 * iterator path after the support gate accepted the read must trip S6, and every flag must be reset
 * after each test.  The class fails if a surface was never tripped.
 */
public class CursorReadOracleSelfTest extends CursorReadOracle
{
    /** Every {@code CursorReads.TEST_*} flag and the surface it must trip.  A new flag must be added here. */
    private static final Map<String, Surface> FLAG_SURFACES = Map.ofEntries(
        Map.entry("TEST_CORRUPT_CELL_TIMESTAMPS", Surface.S1),
        Map.entry("TEST_CORRUPT_MERGE_DECISIONS", Surface.S1),
        Map.entry("TEST_DROP_MERGE_SEEK_OPEN_MARKER", Surface.S1),
        Map.entry("TEST_SKEW_MERGE_SEEK_OPEN_MARKER", Surface.S1),
        Map.entry("TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS", Surface.S1),
        Map.entry("TEST_FORCE_MEMTABLE_ROW_REUSE", Surface.S1),
        Map.entry("TEST_SKEW_DROPPED_ROW_ACCOUNTING", Surface.S4),
        Map.entry("TEST_TRANSCODE_SKEW_TIMESTAMP", Surface.S2),
        Map.entry("TEST_TRANSCODE_WRONG_FLAGS", Surface.S2),
        Map.entry("TEST_CORRUPT_STREAMED_CELL_VALUE", Surface.S2));

    private static final EnumSet<Surface> tripped = EnumSet.noneOf(Surface.class);
    private static final TreeSet<String> flagsTripped = new TreeSet<>();

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;

    @Before
    public void pinBti()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    @AfterClass
    public static void everySurfaceAndFlagTripped()
    {
        assertEquals("every surface must be shown to fail", EnumSet.allOf(Surface.class), tripped);
        assertEquals("every TEST_* flag must be shown to trip its surface", new TreeSet<>(FLAG_SURFACES.keySet()), flagsTripped);
    }

    private interface OracleCall
    {
        void run();
    }

    /** {@code call} passes honestly, fails with {@code expected} under {@code flag}, and passes again. */
    private void expectFlagTrips(String flag, Surface expected, OracleCall call) throws Exception
    {
        assertEquals("self test maps " + flag + " to the wrong surface", FLAG_SURFACES.get(flag), expected);
        call.run();
        Field field = CursorReads.class.getField(flag);
        field.setBoolean(null, true);
        try
        {
            expectTrip(expected, call, flag);
        }
        finally
        {
            field.setBoolean(null, false);
        }
        flagsTripped.add(flag);
        call.run();
    }

    private static void expectTrip(Surface expected, OracleCall call, String what)
    {
        try
        {
            call.run();
        }
        catch (Divergence d)
        {
            assertTrue(what + " tripped " + d.surfaces + ", expected " + expected + ": " + d.getMessage(),
                       d.surfaces.contains(expected));
            tripped.add(expected);
            return;
        }
        fail("the oracle did not detect " + what + " on surface " + expected);
    }

    // ---------------------------------------------------------------- flags

    @Test
    public void corruptCellTimestampsTripsS1() throws Exception
    {
        ColumnFamilyStore cfs = twoOverlappingSSTables();
        expectFlagTrips("TEST_CORRUPT_CELL_TIMESTAMPS", Surface.S1,
                        () -> assertExecuteLocallyMatches(ReadCase.of("cell timestamps", 2), cfs, fullRead(cfs, 1L)));
    }

    @Test
    public void corruptMergeDecisionsTripsS1() throws Exception
    {
        ColumnFamilyStore cfs = twoOverlappingSSTables();
        expectFlagTrips("TEST_CORRUPT_MERGE_DECISIONS", Surface.S1,
                        () -> assertExecuteLocallyMatches(ReadCase.of("merge decisions", 3), cfs, fullRead(cfs, 1L)));
    }

    @Test
    public void mergeSeekOpenMarkerFlagsTripS1() throws Exception
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // leg 1 seeks into a range tombstone whose open marker sits blocks before the slice start
        for (long ck = 0; ck < 2000; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1L, ck, ck, "value-" + ck);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 100L, 1900L);
        for (long ck = 100; ck < 1600; ck++)
            execute("UPDATE %s USING TIMESTAMP 3000 SET v1 = ? WHERE pk = ? AND ck = ?", ck, 1L, ck);
        flush();
        // leg 2 holds rows under that tombstone, shadowed only through the seek's open marker
        for (long ck = 1000; ck < 1006; ck++)
            execute("UPDATE %s USING TIMESTAMP 1500 SET v2 = ? WHERE pk = ? AND ck = ?", "shadowed-" + ck, 1L, ck);
        execute("UPDATE %s USING TIMESTAMP 2500 SET v2 = ? WHERE pk = ? AND ck = ?", "post-delete", 1L, 1003L);
        flush();
        LongFunction<SinglePartitionReadCommand> read =
            now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1000L).toIncl(1010L).build();
        expectFlagTrips("TEST_DROP_MERGE_SEEK_OPEN_MARKER", Surface.S1,
                        () -> assertExecuteLocallyMatches(ReadCase.of("dropped seek open marker", 4), cfs, read));
        expectFlagTrips("TEST_SKEW_MERGE_SEEK_OPEN_MARKER", Surface.S1,
                        () -> assertExecuteLocallyMatches(ReadCase.of("skewed seek open marker", 5), cfs, read));
    }

    @Test
    public void memtableLegFlagsTripS1() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, 'zzz-sstable') USING TIMESTAMP 1000", ck);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 2");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (2, 100, 'survivor') USING TIMESTAMP 3000");
        flush();
        // pk 1: timestamp ties the memtable must lose by value; pk 2: memtable rows under a newer partition deletion
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, 'aaa-memtable') USING TIMESTAMP 1000", ck);
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (2, ?, ?) USING TIMESTAMP 1000", ck, "shadowed" + ck);
        expectFlagTrips("TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS", Surface.S1,
                        () -> assertExecuteLocallyMatches(ReadCase.of("memtable timestamps", 6), cfs, fullRead(cfs, 1L)));
        expectFlagTrips("TEST_FORCE_MEMTABLE_ROW_REUSE", Surface.S1,
                        () -> assertExecuteLocallyMatches(ReadCase.of("memtable row reuse", 7), cfs, fullRead(cfs, 2L)));
    }

    @Test
    public void skewedDroppedRowAccountingTripsS4() throws Exception
    {
        ColumnFamilyStore cfs = filteredTombstoneWorkload();
        expectFlagTrips("TEST_SKEW_DROPPED_ROW_ACCOUNTING", Surface.S4,
                        () -> assertExecuteLocallyMatches(ReadCase.of("dropped row accounting", 8), cfs, clusteringFiltered(cfs, 2L)));
    }

    @Test
    public void transcodeFlagsTripS2() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "value-" + ck);
        flush();
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 3L);
        for (long ck = 10; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "value-" + ck);
        flush();
        for (String flag : new String[]{ "TEST_TRANSCODE_SKEW_TIMESTAMP", "TEST_TRANSCODE_WRONG_FLAGS", "TEST_CORRUPT_STREAMED_CELL_VALUE" })
            expectFlagTrips(flag, Surface.S2,
                            () -> assertReplicaResponsesMatch(ReadCase.of(flag, 9), cfs, fullRead(cfs, 1L)));
    }

    // ---------------------------------------------------------------- surfaces with no production flag

    @Test
    public void corruptPagingStateTripsS3()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        OracleCall call = () -> assertCqlMatches(ReadCase.of("paging state", 10), cfs, "SELECT * FROM %s WHERE pk = 1", 3);
        call.run();
        TEST_CORRUPT_CURSOR_PAGING_STATE = true;
        try
        {
            expectTrip(Surface.S3, call, "a corrupted paging state");
        }
        finally
        {
            TEST_CORRUPT_CURSOR_PAGING_STATE = false;
        }
        call.run();
    }

    /** A tombstone failure only one path hits: the S5 failure comparison must catch it. */
    @Test
    public void oneSidedTombstoneFailureTripsS5()
    {
        ColumnFamilyStore cfs = filteredTombstoneWorkload();
        int originalFail = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneFailureThreshold(10);
        try
        {
            OracleCall call = () -> assertExecuteLocallyMatches(ReadCase.of("one-sided failure", 11), cfs, clusteringFiltered(cfs, 2L));
            call.run();
            CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING = true;
            try
            {
                expectTrip(Surface.S5, call, "a tombstone failure on the iterator path only");
            }
            finally
            {
                CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING = false;
            }
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalFail);
        }
    }

    /** A leg read by the iterator path after the gate accepted the read (a row transformer) must trip S6. */
    @Test
    public void legReadByIteratorPathTripsS6()
    {
        ColumnFamilyStore cfs = twoOverlappingSSTables();
        ReadCase c = ReadCase.of("row transformer", 12);
        expectTrip(Surface.S6, () -> compareCustom(c, cfs, EnumSet.of(Surface.S1), into -> {
            SinglePartitionReadCommand cmd = fullRead(cfs, 1L).apply(c.nowInSec);
            ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, cmd.partitionKey()));
            try (ReadExecutionController controller = cmd.executionController();
                 UnfilteredRowIterator partition = cmd.queryMemtableAndDisk(cfs, view, source -> new Transformation<BaseRowIterator<?>>() {}, controller))
            {
                canonicalRecordsInto(new SingletonUnfilteredPartitionIterator(partition), into.records);
            }
        }), "sstable legs read by the iterator path under an accepted gate");
        assertTrue("the row transformer read must count fallen-back legs", CursorReads.sstableLegsFellBackToIterator() > 0);
    }

    /** A read the gate rejects, run as if it were served, must trip S6; and the reverse. */
    @Test
    public void wrongGateExpectationTripsS6()
    {
        ColumnFamilyStore cfs = twoOverlappingSSTables();
        expectTrip(Surface.S6, () -> assertExecuteLocallyMatches(ReadCase.of("served read marked rejected", 13)
                                                                         .rejectedBecause(UnsupportedReason.DROPPED_COLLECTION_OR_COUNTER_IN_HEADER),
                                                                 cfs, fullRead(cfs, 1L)),
                   "a served read declared as rejected");

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore indexed = getCurrentColumnFamilyStore();
        indexed.disableAutoCompaction();
        createIndex("CREATE INDEX ON %s (v1) USING 'legacy_local_table'");
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        expectTrip(Surface.S6, () -> assertExecuteLocallyMatches(ReadCase.of("rejected read expected served", 14), indexed, fullRead(indexed, 1L)),
                   "a read the gate rejects (secondary index) expected to be served");
    }

    @Test
    public void everyTestFlagIsMappedAndReset() throws Exception
    {
        TreeSet<String> flags = new TreeSet<>();
        for (Field field : CursorReads.class.getFields())
        {
            if (field.getName().startsWith("TEST_") && field.getType() == boolean.class && Modifier.isStatic(field.getModifiers()))
            {
                flags.add(field.getName());
                field.setBoolean(null, true);
            }
        }
        assertEquals("a CursorReads.TEST_* flag has no self test", new TreeSet<>(FLAG_SURFACES.keySet()), flags);
        TEST_CORRUPT_CURSOR_PAGING_STATE = true;

        resetCursorReadState();
        resetOracleHooks();

        for (String flag : flags)
            assertFalse(flag + " is not reset after a test", CursorReads.class.getField(flag).getBoolean(null));
        assertFalse(TEST_CORRUPT_CURSOR_PAGING_STATE);
    }

    // ---------------------------------------------------------------- fixtures

    private static LongFunction<SinglePartitionReadCommand> fullRead(ColumnFamilyStore cfs, long pk)
    {
        return now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).build();
    }

    private ColumnFamilyStore twoOverlappingSSTables()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?)", ck, ck, "old" + ck);
        flush();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?)", ck, ck + 100, "new" + ck);
        flush();
        return cfs;
    }

    /** Two sstables over one partition; the second deletes every fourth row and a cell of the next. */
    private ColumnFamilyStore filteredTombstoneWorkload()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 64; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck * 10, "r0-" + ck);
        flush();
        for (long ck = 0; ck < 64; ck++)
        {
            if (ck % 4 == 0)
                execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", ck);
            else if (ck % 4 == 1)
                execute("DELETE v1 FROM %s WHERE pk = 0 AND ck = ?", ck);
            else
                execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck * 10);
        }
        flush();
        return cfs;
    }

    /** A strict row filter on the clustering column, which the cursor merge pushes down and so drops rows at production. */
    private static LongFunction<SinglePartitionReadCommand> clusteringFiltered(ColumnFamilyStore cfs, long ck)
    {
        return now -> {
            SinglePartitionReadCommand base = (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build();
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(cfs.metadata().getColumn(ByteBufferUtil.bytes("ck")), Operator.EQ, ByteBufferUtil.bytes(ck));
            return SinglePartitionReadCommand.create(cfs.metadata(), base.nowInSec(), ColumnFilter.all(cfs.metadata()), rowFilter,
                                                     DataLimits.NONE, base.partitionKey(), base.clusteringIndexFilter());
        };
    }
}
