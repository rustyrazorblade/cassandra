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

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/**
 * Differential scenarios inside the Phase 1 cursor read gate's supported surface: every test runs
 * the same single-partition read through the iterator path and the cursor path and asserts
 * identical canonical records and identical intra-node ReadResponse bytes (see the base class).
 */
public class BasicCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private Supplier<SinglePartitionReadCommand> fullPartition(ColumnFamilyStore cfs, long now, Object... key)
    {
        return () -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build();
    }

    @Test
    public void singleSSTableFullPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck * 10, "v" + ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
    }

    @Test
    public void prefixSliceReads() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck += 2) // even ck only: slice bounds also probed BETWEEN rows
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v" + ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        // lower bound only (inclusive, on and between rows)
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(6L).build());
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromExcl(7L).build());
        // upper bound only
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).toIncl(12L).build());
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).toExcl(12L).build());
        // both bounds
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(4L).toIncl(14L).build());
        // empty slice (bounds between the same pair of rows)
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(3L).toExcl(4L).build());
    }

    @Test
    public void sparseRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, a text, b bigint, c int, d text, e double, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 16; ck++)
        {
            switch ((int) (ck % 4))
            {
                case 0: execute("INSERT INTO %s (pk, ck, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                1L, ck, "a" + ck, ck, (int) ck, "d" + ck, ck * 1.5); break;
                case 1: execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1L, ck, "only-a" + ck); break;
                case 2: execute("UPDATE %s SET c = ?, e = ? WHERE pk = ? AND ck = ?", (int) ck, ck * 0.5, 1L, ck); break;
                case 3: execute("INSERT INTO %s (pk, ck, b, d) VALUES (?, ?, ?, ?)", 1L, ck, ck * 7, "d" + ck); break;
            }
        }
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(2L).toIncl(13L).build());
    }

    @Test
    public void tombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v" + ck);
        // row tombstones
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 3L);
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 21L);
        // cell tombstones
        execute("UPDATE %s SET v2 = null WHERE pk = ? AND ck = ?", 1L, 5L);
        execute("DELETE v1 FROM %s WHERE pk = ? AND ck = ?", 1L, 7L);
        // range tombstone ck in [10, 20)
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 10L, 20L);
        // overlapping range tombstone with a later timestamp -> boundary markers on disk
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 15L, 25L);
        flush();

        long now = FBUtilities.nowInSeconds();
        // full partition: markers pass through as written
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        // slice starting INSIDE an open range tombstone: exercises the artificial open-marker at
        // the slice start (ForwardReader.handlePreSliceData mirror)
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(17L).toIncl(30L).build());
        // slice ending INSIDE an open range tombstone: artificial close-marker at the slice end
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(0L).toIncl(12L).build());
        // slice fully inside the deleted range
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(16L).toExcl(19L).build());
    }

    @Test
    public void widePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 2000; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "value-" + ck);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 900L, 950L);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(880L).toExcl(1100L).build());
    }

    @Test
    public void multiSSTableMerge() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, v3 int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // 5 overlapping sstables rewriting the same rows; later rounds add tombstones
        for (int round = 0; round < 5; round++)
        {
            for (long pk = 0; pk < 3; pk++)
                for (long ck = 0; ck < 12; ck++)
                {
                    if (ck % 2 == 0)
                        execute("INSERT INTO %s (pk, ck, v1, v2, v3) VALUES (?, ?, ?, ?, ?)",
                                pk, ck, ck * 100 + round, "r" + round + "-" + ck, round);
                    else
                        execute("UPDATE %s SET v1 = ? WHERE pk = ? AND ck = ?", ck * 1000 + round, pk, ck);
                }
            if (round == 3)
            {
                execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 4L);
                execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 8L, 10L);
                execute("DELETE FROM %s WHERE pk = ?", 2L); // partition deletion, then round 4 re-inserts
            }
            flush();
        }
        assertEquals(5, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 0L));
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 2L)); // partition-deletion + newer data
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(3L).toIncl(9L).build());
    }

    @Test
    public void memtableAndSSTableMerge() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "sstable" + ck);
        flush();
        for (long ck = 0; ck < 20; ck += 3)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck + 5000, "flushed2-" + ck);
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 6L);
        flush();
        // memtable overlay: newer values, a memtable-only row and a memtable row tombstone.
        // The memtable leg stays on the object path (deliberate Phase 1 shortcut); the sstable legs
        // are cursor-served, and the merge across them must be identical.
        for (long ck = 0; ck < 20; ck += 4)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "memtable-" + ck, 1L, ck);
        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, 100L, 100L, "memtable-only");
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 9L);

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(4L).toIncl(12L).build());
    }

    @Test
    public void collectionsAndUdt() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, l list<text>, " +
                    "u " + udt + ", v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 8; ck++)
        {
            execute("INSERT INTO %s (pk, ck, m, s, l, u, v) VALUES (?, ?, ?, ?, ?, {a: ?, b: ?}, ?)",
                    1L, ck, map("k" + ck, ck, "x", 42L), set((int) ck, 7), list("a" + ck, "b"), (int) ck, "f" + ck, "v" + ck);
            execute("UPDATE %s SET m[?] = ?, u.b = ? WHERE pk = ? AND ck = ?", "extra" + ck, ck * 10, "upd" + ck, 1L, ck);
        }
        // full-collection overwrite -> complex deletion + fresh cells
        execute("UPDATE %s SET m = ? WHERE pk = ? AND ck = ?", map("only", 1L), 1L, 2L);
        // deletion-only complex column (delete the collection, keep the row)
        execute("DELETE s FROM %s WHERE pk = ? AND ck = ?", 1L, 4L);
        // a row whose ONLY content is a deletion-only complex column
        execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", 1L, 50L);
        flush();
        // second sstable: element-level overwrites merged on read
        for (long ck = 0; ck < 8; ck += 2)
            execute("UPDATE %s SET m[?] = ?, s = s + ? WHERE pk = ? AND ck = ?", "k" + ck, ck + 900, set(99), 1L, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(2L).toIncl(5L).build());
    }

    @Test
    public void staticColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 text static, s2 bigint static, v text, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("UPDATE %s SET s1 = ?, s2 = ? WHERE pk = ?", "static-1", 11L, 1L);
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "v" + ck);
        flush();
        execute("UPDATE %s SET s1 = ? WHERE pk = ?", "static-updated", 1L); // second sstable, static merge
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 10L, "second");
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        // static-only selection: no regular columns fetched, rows survive on liveness alone
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).columns("s1").build());
        // slice + statics
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(2L).toIncl(4L).build());
    }

    @Test
    public void partitionDeletionShadowingInSameSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 text static, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // older static + rows, then a partition deletion, then newer rows — ALL in one sstable:
        // the single-source case where no merge shadowing runs and raw emission must match
        execute("UPDATE %s SET s1 = ? WHERE pk = ?", "old-static", 1L);
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "old" + ck);
        execute("DELETE FROM %s WHERE pk = ?", 1L);
        for (long ck = 2; ck < 4; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "new" + ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
    }

    @Test
    public void columnSubsetSelection() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, v3 int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2, v3) VALUES (?, ?, ?, ?, ?)", 1L, ck, ck, "v" + ck, (int) ck);
        // rows where the queried column is older than the row liveness (exercises the
        // fetched-but-not-queried valueless-cell machinery on the other columns)
        for (long ck = 0; ck < 10; ck += 3)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1L, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).columns("v1").build());
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).columns("v1", "v3").fromIncl(2L).toIncl(8L).build());
    }

    @Test
    public void absentPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 64; pk++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, 0L, pk);
        flush();

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = fullPartition(cfs, now, 100000L);
        // the view only contains sstables whose token range covers the queried key; if the absent
        // key happens to fall outside every sstable's range the gate is (correctly) closed and this
        // scenario proves nothing — skip instead of passing vacuously
        Assume.assumeFalse("absent key not covered by any sstable's key range",
                           liveSSTablesFor(cfs, cmd.get()).isEmpty());
        assertCursorReadMatchesIterator(cfs, cmd, false);
    }
}
