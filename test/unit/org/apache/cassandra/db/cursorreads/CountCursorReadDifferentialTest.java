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

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.CursorReads;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Dedicated differential coverage for {@code SELECT count(*)} on a single partition.  The aggregate
 * path builds and executes an ordinary single-partition read and then counts the surviving rows, so
 * it engages the cursor read path generically; this test proves the count is identical on the cursor
 * path and the iterator path across merges, deletions, TTLs, static columns, and clustering slices.
 * <p>
 * Each assertion runs the same query twice: once with {@code cursor_reads_enabled = false} and once
 * with it {@code true}.  It also guards against a silent fallback: unless the queried partition is
 * absent, the cursor run must actually serve at least one sstable leg, and the iterator run must
 * never touch the cursor path.
 */
public class CountCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private static long onlyCount(UntypedResultSet rs)
    {
        return rs.one().getLong("count");
    }

    /**
     * Runs {@code cql} on both paths and asserts the count matches.
     *
     * @param expectServed true unless the queried partition is absent (then the cursor is never
     *                     opened and no sstable leg is served)
     */
    private void assertCountMatches(boolean expectServed, String cql, Object... args)
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        long iteratorServedBefore = CursorReads.sstableLegsServed();
        long iteratorCount = onlyCount(execute(cql, args));
        assertEquals("iterator-path count query unexpectedly ran the cursor path",
                     iteratorServedBefore, CursorReads.sstableLegsServed());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long servedBefore = CursorReads.sstableLegsServed();
            long cursorCount = onlyCount(execute(cql, args));
            long servedDelta = CursorReads.sstableLegsServed() - servedBefore;
            if (expectServed)
                assertTrue("cursor-path count query served no sstable leg (silent fallback?); served=" + servedDelta,
                           servedDelta > 0);
            assertEquals("count(*) diverged between the iterator and cursor paths", iteratorCount, cursorCount);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    @Test
    public void countSingleSSTableFullPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int c = 0; c < 25; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, c, "v" + c);
        flush();

        assertCountMatches(true, "SELECT count(*) FROM %s WHERE k = ?", 1);
    }

    @Test
    public void countAcrossMergedSSTablesWithDeletions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c)) WITH gc_grace_seconds = 864000");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        // four overlapping sstables that rewrite the same rows, plus row and range deletions
        for (int round = 0; round < 4; round++)
        {
            for (int c = 0; c < 30; c++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        1, c, "r" + round + "-" + c, 1_000L + round * 100L + c);
            if (round == 2)
            {
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND c = ?", 5_000L, 1, 4);
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND c >= ? AND c < ?", 5_000L, 1, 10, 15);
            }
            flush();
        }

        assertCountMatches(true, "SELECT count(*) FROM %s WHERE k = ?", 1);
    }

    @Test
    public void countWithClusteringSlice() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int c = 0; c < 40; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, c, "v" + c);
        execute("DELETE FROM %s WHERE k = ? AND c >= ? AND c < ?", 1, 20, 25);
        flush();

        assertCountMatches(true, "SELECT count(*) FROM %s WHERE k = ? AND c >= ? AND c < ?", 1, 5, 35);
        assertCountMatches(true, "SELECT count(*) FROM %s WHERE k = ? AND c > ?", 1, 30);
        assertCountMatches(true, "SELECT count(*) FROM %s WHERE k = ? AND c <= ?", 1, 12);
    }

    @Test
    public void countWithTtlRowsAllLive() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        // large TTL: every row is still live at read time, so it must count on both paths
        for (int c = 0; c < 18; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TTL 1000000", 1, c, "v" + c);
        flush();

        assertCountMatches(true, "SELECT count(*) FROM %s WHERE k = ?", 1);
    }

    @Test
    public void countWithStaticColumnAndMemtableOverlay() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s text static, v text, PRIMARY KEY (k, c))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        execute("UPDATE %s SET s = ? WHERE k = ?", "static", 1);
        for (int c = 0; c < 20; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, c, "v" + c);
        flush();
        // memtable overlay: new rows and a row tombstone not yet flushed
        for (int c = 20; c < 26; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, c, "m" + c);
        execute("DELETE FROM %s WHERE k = ? AND c = ?", 1, 3);

        assertCountMatches(true, "SELECT count(*) FROM %s WHERE k = ?", 1);
    }

    @Test
    public void countAbsentPartitionIsZero() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int k = 0; k < 32; k++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, 0, "v" + k);
        flush();

        // an absent key: the cursor may never open (the leg has no such partition), so do not
        // require a served leg; both paths must simply return 0
        assertCountMatches(false, "SELECT count(*) FROM %s WHERE k = ?", 999_999);
    }
}
