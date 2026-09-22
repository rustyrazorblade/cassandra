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

import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The NOT-supported side of the cursor read gate: shapes outside the gate must be rejected up
 * front, never touch the cursor, and return results identical to the flag-off iterator path.
 *
 * Also contains the "mutation test": corrupt the cursor materializer and prove the differential
 * harness actually fails — the guard against a harness that would pass a broken cursor path.
 *
 * The latest-sstable-version gate arm is not exercised here (fabricating a pre-latest-version
 * sstable needs mock readers, not this CQLTester harness); {@link CursorReadVersionGateTest}
 * covers that arm directly.
 */
public class CursorReadGateTest extends CursorReadDifferentialTester
{
    private ColumnFamilyStore prepareSimpleTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v" + ck);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 4L, 6L);
        flush();
        for (long ck = 0; ck < 10; ck += 2)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "second-" + ck, 1L, ck);
        flush();
        return cfs;
    }

    /**
     * A reverse (ORDER BY ... DESC) read is now served on the cursor path (CASSANDRA-20428, gap #5).
     * The gate accepts it, and executeLocally opens an sstable leg for it. The byte-identical result
     * is proven in {@code ReverseCursorReadDifferentialTest}.
     */
    @Test
    public void reversedQueryIsGateSupported() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).reverse().build();

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("reverse read must now pass the cursor read gate",
                       CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));

            long servedBefore = CursorReads.sstableLegsServed();
            canonicalRecords(cmd.get());
            assertTrue("executeLocally must serve the reverse read on the cursor path",
                       CursorReads.sstableLegsServed() > servedBefore);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /**
     * A names read is now served on the cursor path (CASSANDRA-20428, gap #9). The gate accepts it,
     * and executeLocally opens an sstable leg for it. The byte-identical result is proven in
     * {@code NamesFilterCursorReadDifferentialTest}.
     */
    @Test
    public void namesFilterIsGateSupported() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).includeRow(3L).includeRow(7L).build();

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("names read must now pass the cursor read gate",
                       CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));

            long servedBefore = CursorReads.sstableLegsServed();
            canonicalRecords(cmd.get());
            assertTrue("executeLocally must serve the names read on the cursor path",
                       CursorReads.sstableLegsServed() > servedBefore);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /**
     * A multi-slice read is now served on the cursor path (CASSANDRA-20428, gap #8). The gate accepts
     * it, and executeLocally opens an sstable leg for it. The byte-identical result is proven in
     * {@code MultiSliceCursorReadDifferentialTest}.
     */
    @Test
    public void multiSliceIsGateSupported() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = () -> multiSliceCommand(cfs, now);

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("multi-slice read must now pass the cursor read gate",
                       CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));

            long servedBefore = CursorReads.sstableLegsServed();
            canonicalRecords(cmd.get());
            assertTrue("executeLocally must serve the multi-slice read on the cursor path",
                       CursorReads.sstableLegsServed() > servedBefore);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static SinglePartitionReadCommand multiSliceCommand(ColumnFamilyStore cfs, long now)
    {
        Slices.Builder builder = new Slices.Builder(cfs.metadata().comparator);
        builder.add(org.apache.cassandra.db.ClusteringBound.create(cfs.metadata().comparator, true, true, 1L),
                    org.apache.cassandra.db.ClusteringBound.create(cfs.metadata().comparator, false, true, 3L));
        builder.add(org.apache.cassandra.db.ClusteringBound.create(cfs.metadata().comparator, true, true, 6L),
                    org.apache.cassandra.db.ClusteringBound.create(cfs.metadata().comparator, false, true, 8L));
        Slices slices = builder.build();
        assertEquals("scenario must actually be multi-slice", 2, slices.size());
        return SinglePartitionReadCommand.create(cfs.metadata(), now,
                                                 cfs.metadata().partitioner.decorateKey(LongType.instance.decompose(1L)),
                                                 slices);
    }

    @Test
    public void indexedTableFallsBack() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s (v1)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertFallsBackUnchanged(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).build());
    }

    /**
     * A counter table now PASSES the gate (CASSANDRA-20428). The counter read routes through the
     * cursor merge/materialize path, which folds every surviving shard set byte-for-byte and stays
     * off the single-winner transcode fast path. The full counter-fold differential corpus lives in
     * {@code CounterCursorReadDifferentialTest}; this only guards the gate decision itself.
     */
    @Test
    public void counterTableIsGateSupported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 5; ck++)
            execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", ck + 1, 1L, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).build();

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("counter table must now pass the cursor read gate (CASSANDRA-20428)",
                       CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }

        assertCursorReadMatchesIterator(cfs, cmd);
    }

    /**
     * A memtable-only read (no sstable) now PASSES the gate (CASSANDRA-20428 gap #2 dropped the
     * empty-sstable floor). On the {@code executeLocally} path there is no sstable leg to cursor-serve
     * -- a memtable has no cursor representation -- so the memtable iterator takes the object path and
     * no sstable leg is counted, yet the result must stay byte-identical to the flag-off oracle. The
     * memtable-only read's actual cursor engagement is on the transcode path and is proven in
     * {@code TranscodeRoutingDifferentialTest.memtableOnlyReadEngagesTranscodePath}.
     */
    @Test
    public void memtableOnlyReadIsGateSupported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        // no flush: the read is served from the memtable only

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).build();

        DatabaseDescriptor.setCursorReadsEnabled(false);
        java.util.List<String> oracleRecords = canonicalRecords(cmd.get());
        byte[] oracleBytes = responseBytes(cmd.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("memtable-only read must now pass the cursor read gate",
                       CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));

            long servedBefore = CursorReads.sstableLegsServed();
            java.util.List<String> cursorRecords = canonicalRecords(cmd.get());
            byte[] cursorBytes = responseBytes(cmd.get());
            assertEquals("executeLocally must not open an sstable leg for a memtable-only read",
                         servedBefore, CursorReads.sstableLegsServed());

            compareRecords(oracleRecords, cursorRecords);
            assertResponseBytesEqual(oracleBytes, cursorBytes);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    @Test
    public void rowCacheTableFallsBack() throws Throwable
    {
        // the gate consults ColumnFamilyStore.isRowCacheEnabled(), which requires BOTH the schema
        // opt-in and a non-zero global row cache capacity
        CacheService.instance.setRowCacheCapacityInMB(1);
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck)) " +
                        "WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
            flush();
            assertTrue("scenario precondition: row cache must be enabled for this table", cfs.isRowCacheEnabled());

            long now = FBUtilities.nowInSeconds();
            SinglePartitionReadCommand probe = (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).build();
            DatabaseDescriptor.setCursorReadsEnabled(true);
            try
            {
                assertFalse("row-cache-enabled table must be gated out",
                            CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));
                // NOTE: no record comparison here — a row-cache read legitimately serves from the
                // cache on repeat execution, which is iterator-path behavior on both sides and
                // orthogonal to what this gate test verifies.
                long servedBefore = CursorReads.sstableLegsServed();
                canonicalRecords((SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).build());
                assertEquals("cursor path ran for a row-cache table", servedBefore, CursorReads.sstableLegsServed());
            }
            finally
            {
                DatabaseDescriptor.setCursorReadsEnabled(false);
            }
        }
        finally
        {
            CacheService.instance.setRowCacheCapacityInMB(0);
        }
    }

    /**
     * The mutation test: corrupt the cursor materializer (cell timestamps skewed by +1) and prove
     * the harness FAILS. A harness that passes a corrupted cursor path is worthless.
     */
    @Test
    public void deliberateCorruptionIsDetected() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).build();

        // sanity: the scenario passes when the cursor path is honest
        assertCursorReadMatchesIterator(cfs, cmd);

        CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS = true;
        try
        {
            assertCursorReadMatchesIterator(cfs, cmd);
            fail("differential harness FAILED TO DETECT a corrupted cursor read path");
        }
        catch (AssertionError expected)
        {
            if (expected.getMessage() != null && expected.getMessage().contains("FAILED TO DETECT"))
                throw expected;
            assertTrue("harness failed for an unexpected reason: " + expected.getMessage(),
                       expected.getMessage() != null
                       && (expected.getMessage().contains("LOGICAL divergence")
                           || expected.getMessage().contains("BYTE divergence")));
        }
        finally
        {
            CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS = false;
        }

        // and it must pass again once the corruption is removed
        assertCursorReadMatchesIterator(cfs, cmd);
    }
}
