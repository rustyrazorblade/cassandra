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

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Dropped-column scenarios: the iterator path discards cells (and complex deletions) of a
 * dropped column at deserialization when their timestamp is at or before the drop
 * (UnfilteredSerializer.readSimpleColumn/readComplexColumn via DeserializationHelper.isDropped /
 * isDroppedComplexDeletion). The cursor path must filter identically, or dropped data survives
 * cursor compaction and is resurrected by ALTER TABLE ... ADD of the same column.
 *
 * Timestamp determinism: ClientState timestamps are strictly increasing per node, so a write
 * executed before ALTER TABLE DROP always carries a timestamp strictly below droppedTime;
 * writes that must survive the drop use an explicit far-future USING TIMESTAMP.
 */
public class DroppedColumnDifferentialCompactionTest extends DifferentialCompactionTester
{
    /** Far in the future relative to any wall-clock droppedTime (year ~2100, microseconds). */
    private static final long FUTURE_TS = 4102444800_000000L;

    @Test
    public void droppedRegularColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 5; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "dropped-" + ck);
        flush();

        alterTable("ALTER TABLE %s DROP v2");

        // second generation written after the drop, overlapping the first
        for (long pk = 0; pk < 5; pk++)
            for (long ck = 5; ck < 15; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck + 100);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * Cells written with a timestamp ABOVE droppedTime survive compaction on both paths
     * (isDropped is timestamp-gated, not column-gated) — pins the comparison direction.
     */
    @Test
    public void cellsNewerThanDropRetained() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?) USING TIMESTAMP " + FUTURE_TS,
                    ck, ck, "survives-" + ck);
        // and some cells that do not survive
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v2) VALUES (1, ?, ?)", ck, "dropped-" + ck);
        flush();

        alterTable("ALTER TABLE %s DROP v2");

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (2, ?, ?)", ck, ck);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /** DROP then ADD: pre-drop cells are filtered, post-re-add cells survive — the resurrection shape. */
    @Test
    public void droppedColumnReAdded() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck, "old-" + ck);
        flush();

        alterTable("ALTER TABLE %s DROP v2");
        alterTable("ALTER TABLE %s ADD v2 text");

        for (long ck = 5; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck, v2) VALUES (0, ?, ?)", ck, "new-" + ck);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /** Complex (multi-cell) column drop: per-cell filtering plus the dropped complex deletion. */
    @Test
    public void droppedComplexColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, m) VALUES (0, ?, ?, {'a': 1, 'b': 2, 'c': 3})", ck, ck);
        flush();

        // complex deletions (DELETE m sets one) and overwrites in a second generation
        for (long ck = 0; ck < 5; ck++)
            execute("DELETE m FROM %s WHERE pk = 0 AND ck = ?", ck);
        for (long ck = 5; ck < 10; ck++)
            execute("UPDATE %s SET m['d'] = 4 WHERE pk = 0 AND ck = ?", ck);
        flush();

        alterTable("ALTER TABLE %s DROP m");

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck + 100);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * A complex deletion with markedForDeleteAt ABOVE droppedTime survives the drop while every
     * cell of the column is filtered: the column must still surface as deletion-only on both
     * paths (pins the cursor reader's pause-at-surviving-deletion behavior).
     *
     * Single-sstable compaction on purpose: merging a second source for the same rows hits a
     * pre-existing UPSTREAM iterator crash (NPE in Row.Merger.ColumnDataReducer.getReduced,
     * Row.java:904, complexBuilder null) when a dropped column's surviving complex deletion
     * meets another row version — the reference path itself cannot compact that shape, so the
     * differential harness cannot pin it.
     */
    @Test
    public void droppedComplexColumnSurvivingDeletion() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text, bigint>, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // old cells (filtered by the drop) and a far-future complex deletion (survives it),
        // in one sstable: the column becomes deletion-only after filtering
        for (long ck = 0; ck < 10; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v1, m) VALUES (0, ?, ?, {'a': 1, 'b': 2})", ck, ck);
            execute("DELETE m FROM %s USING TIMESTAMP " + FUTURE_TS + " WHERE pk = 0 AND ck = ?", ck);
        }
        flush();

        alterTable("ALTER TABLE %s DROP m");

        assertCursorMatchesIterator(cfs);
    }

    @Test
    public void droppedStaticColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s text static, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 5; pk++)
        {
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, "static-" + pk);
            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck);
        }
        flush();

        alterTable("ALTER TABLE %s DROP s");

        for (long pk = 0; pk < 5; pk++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, 99L, 99L);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * Dropped counter column: counter contexts merge VALUES across sources (not pick-a-winner),
     * so dropped-column filtering must happen per source before the context merge — filtering
     * the merged result would fold dropped shards into the output.
     */
    @Test
    public void droppedCounterColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s SET c1 = c1 + 1, c2 = c2 + 10 WHERE pk = 0 AND ck = ?", ck);
        flush();
        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s SET c1 = c1 + 2, c2 = c2 + 20 WHERE pk = 0 AND ck = ?", ck);
        flush();

        alterTable("ALTER TABLE %s DROP c2");

        for (long ck = 5; ck < 15; ck++)
            execute("UPDATE %s SET c1 = c1 + 3 WHERE pk = 0 AND ck = ?", ck);
        flush();

        assertCursorMatchesIterator(cfs);
    }
}
