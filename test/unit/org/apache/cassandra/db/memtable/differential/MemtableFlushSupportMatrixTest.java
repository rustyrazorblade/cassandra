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

package org.apache.cassandra.db.memtable.differential;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.partitions.MemtableCursorFlusher;

import static org.junit.Assert.assertFalse;

/**
 * Per-type, per-size differential matrix for the memtable cursor flush path: every case flushes
 * data through both paths and compares the output.  Covers complex columns (collections, UDTs)
 * and counters, which {@link BasicFlushDifferentialTest} does not.
 */
public class MemtableFlushSupportMatrixTest extends MemtableFlushDifferentialTester
{
    private static final long BASE_TS = 2_000_000_000_000L;

    private static AtomicLong ts()
    {
        return new AtomicLong(BASE_TS);
    }

    @Test
    public void everyPrimitiveType() throws Exception
    {
        assertFlushMatches("CREATE TABLE %s (" +
                           "k int PRIMARY KEY, " +
                           "c_ascii ascii, c_bigint bigint, c_blob blob, c_boolean boolean, " +
                           "c_date date, c_decimal decimal, c_double double, c_float float, " +
                           "c_inet inet, c_int int, c_smallint smallint, c_text text, " +
                           "c_time time, c_timestamp timestamp, c_timeuuid timeuuid, " +
                           "c_tinyint tinyint, c_uuid uuid, c_varchar varchar, c_varint varint)",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               execute("INSERT INTO " + t + " (k, c_ascii, c_bigint, c_blob, c_boolean, c_date, " +
                                       "c_decimal, c_double, c_float, c_inet, c_int, c_smallint, c_text, c_time, " +
                                       "c_timestamp, c_timeuuid, c_tinyint, c_uuid, c_varchar, c_varint) " +
                                       "VALUES (1, 'abc', 123456789, 0xCAFEBABE, true, '2024-01-01', " +
                                       "12345.6789, 3.14159, 2.71, '127.0.0.1', 42, 7, 'hello world', " +
                                       "'12:34:56.789000000', '2024-06-01T00:00:00Z', 0bff01c0-9334-11f1-9b94-1d23e8e5d6d1, 3, " +
                                       "12345678-1234-1234-1234-123456789abc, 'varchar-value', 999999999999999999999) " +
                                       "USING TIMESTAMP " + ts.getAndIncrement());
                               // second row, leaving the nullable columns unset
                               execute("INSERT INTO " + t + " (k, c_int) VALUES (2, 7) USING TIMESTAMP " + ts.getAndIncrement());
                           });
    }

    private void mapSizes(boolean frozen) throws Exception
    {
        String type = frozen ? "frozen<map<int, text>>" : "map<int, text>";
        assertFlushMatches("CREATE TABLE %s (k int PRIMARY KEY, m " + type + ")",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               for (int size : new int[]{ 0, 1, 100, 10000 })
                               {
                                   Map<Integer, String> m = new LinkedHashMap<>();
                                   for (int i = 0; i < size; i++)
                                       m.put(i, "value-" + i);
                                   execute("INSERT INTO " + t + " (k, m) VALUES (?, ?) USING TIMESTAMP " + ts.getAndIncrement(),
                                          size, m);
                               }
                           });
    }

    @Test
    public void nonFrozenMapSizes() throws Exception
    {
        mapSizes(false);
    }

    @Test
    public void frozenMapSizes() throws Exception
    {
        mapSizes(true);
    }

    private void listSizes(boolean frozen) throws Exception
    {
        // A non-frozen list keys its elements by a client-generated TimeUUID cell path, which the
        // two tables can't reproduce byte-for-byte, so it compares logically only.  A frozen list
        // is one opaque value and goes through the byte-for-byte comparison.
        String type = frozen ? "frozen<list<text>>" : "list<text>";
        BiConsumer<String, String> populate = (ks, tbl) -> {
            String t = ks + "." + tbl;
            AtomicLong ts = ts();
            for (int size : new int[]{ 0, 1, 100, 10000 })
            {
                List<String> l = new java.util.ArrayList<>();
                for (int i = 0; i < size; i++)
                    l.add("value-" + i);
                execute("INSERT INTO " + t + " (k, l) VALUES (?, ?) USING TIMESTAMP " + ts.getAndIncrement(),
                       size, l);
            }
        };
        String tableCql = "CREATE TABLE %s (k int PRIMARY KEY, l " + type + ")";
        if (frozen)
            assertFlushMatches(tableCql, populate);
        else
            assertFlushMatchesLogically(tableCql, populate, false);
    }

    @Test
    public void nonFrozenListSizes() throws Exception
    {
        listSizes(false);
    }

    @Test
    public void frozenListSizes() throws Exception
    {
        listSizes(true);
    }

    private void setSizes(boolean frozen) throws Exception
    {
        String type = frozen ? "frozen<set<int>>" : "set<int>";
        assertFlushMatches("CREATE TABLE %s (k int PRIMARY KEY, s " + type + ")",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               for (int size : new int[]{ 0, 1, 100, 10000 })
                               {
                                   Set<Integer> s = new LinkedHashSet<>();
                                   for (int i = 0; i < size; i++)
                                       s.add(i);
                                   execute("INSERT INTO " + t + " (k, s) VALUES (?, ?) USING TIMESTAMP " + ts.getAndIncrement(),
                                          size, s);
                               }
                           });
    }

    @Test
    public void nonFrozenSetSizes() throws Exception
    {
        setSizes(false);
    }

    @Test
    public void frozenSetSizes() throws Exception
    {
        setSizes(true);
    }

    @Test
    public void multiCellCollectionDeletion() throws Exception
    {
        // A collection delete with no cells, alongside a normal insert, exercises the
        // deletion-only complex-column path.  Logical-only: a DELETE's local_delete_time is
        // wall-clock seconds, not controlled by USING TIMESTAMP.
        assertFlushMatchesLogically("CREATE TABLE %s (k int PRIMARY KEY, m map<int, text>)",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               Map<Integer, String> m = new LinkedHashMap<>();
                               m.put(1, "a");
                               m.put(2, "b");
                               execute("INSERT INTO " + t + " (k, m) VALUES (1, ?) USING TIMESTAMP " + ts.getAndIncrement(), m);
                               execute("DELETE m FROM " + t + " USING TIMESTAMP " + ts.getAndIncrement() + " WHERE k = 1");
                               execute("INSERT INTO " + t + " (k, m) VALUES (2, ?) USING TIMESTAMP " + ts.getAndIncrement(), m);
                               execute("DELETE m[?] FROM " + t + " USING TIMESTAMP " + ts.getAndIncrement() + " WHERE k = 2", 1);
                           }, false);
    }

    @Test
    public void udtSimpleAndNested() throws Exception
    {
        String innerType = createType("CREATE TYPE %s (a int, b text)");
        String outerType = createType("CREATE TYPE %s (inner frozen<" + innerType + ">, tag text)");
        // ul is a non-frozen list, which forces the logical-only comparison; see listSizes().
        assertFlushMatchesLogically("CREATE TABLE %s (k int PRIMARY KEY, u frozen<" + outerType + ">, ul list<frozen<" + innerType + ">>)",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               execute("INSERT INTO " + t + " (k, u, ul) VALUES (1, {inner: {a: 1, b: 'x'}, tag: 'outer'}, " +
                                       "[{a: 1, b: 'p'}, {a: 2, b: 'q'}]) USING TIMESTAMP " + ts.getAndIncrement());
                           }, false);
    }

    /**
     * A non-frozen UDT is a multi-cell complex column, keyed by field position rather than a cell
     * path.  Logical-only: the field-level DELETE below carries a wall-clock local_delete_time
     * that USING TIMESTAMP does not control.
     */
    @Test
    public void nonFrozenUdt() throws Exception
    {
        String type = createType("CREATE TYPE %s (a int, b text, c bigint)");
        assertFlushMatchesLogically("CREATE TABLE %s (k int PRIMARY KEY, u " + type + ")",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               execute("INSERT INTO " + t + " (k, u) VALUES (1, {a: 1, b: 'x', c: 100}) USING TIMESTAMP " + ts.getAndIncrement());
                               // partial value: a non-frozen UDT can set only some fields, leaving the rest absent.
                               execute("INSERT INTO " + t + " (k, u) VALUES (2, {a: 2}) USING TIMESTAMP " + ts.getAndIncrement());
                               // overwrite one field within an existing UDT value.
                               execute("UPDATE " + t + " USING TIMESTAMP " + ts.getAndIncrement() + " SET u.b = 'updated' WHERE k = 1");
                               // tombstone one field, leaving its siblings live.
                               execute("DELETE u.c FROM " + t + " USING TIMESTAMP " + ts.getAndIncrement() + " WHERE k = 1");
                           }, false);
    }

    /**
     * {@code SSTableCursorWriter} tracks each row's complex columns in marker arrays that start at
     * 8 entries and grow by 8.  20 complex columns in one row crosses that growth twice, which no
     * other scenario in this suite does.
     */
    @Test
    public void manyComplexColumnsInOneRow() throws Exception
    {
        int columnCount = 20;
        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (k int PRIMARY KEY");
        for (int i = 0; i < columnCount; i++)
            ddl.append(", m").append(i).append(" map<int, text>");
        ddl.append(")");

        assertFlushMatchesLogically(ddl.toString(),
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               StringBuilder insertCols = new StringBuilder("k");
                               StringBuilder insertVals = new StringBuilder("?");
                               List<Object> bindings = new ArrayList<>();
                               bindings.add(1);
                               for (int i = 0; i < columnCount; i++)
                               {
                                   insertCols.append(", m").append(i);
                                   insertVals.append(", ?");
                                   Map<Integer, String> m = new LinkedHashMap<>();
                                   for (int j = 0; j < 5; j++)
                                       m.put(j, "v" + i + "-" + j);
                                   bindings.add(m);
                               }
                               execute("INSERT INTO " + t + " (" + insertCols + ") VALUES (" + insertVals + ") USING TIMESTAMP " + ts.getAndIncrement(),
                                      bindings.toArray());
                           }, false);
    }

    @Test
    public void tuple() throws Exception
    {
        assertFlushMatches("CREATE TABLE %s (k int PRIMARY KEY, t frozen<tuple<int, text, bigint>>)",
                           (ks, tbl) -> {
                               String tt = ks + "." + tbl;
                               AtomicLong ts = ts();
                               execute("INSERT INTO " + tt + " (k, t) VALUES (1, (42, 'hi', 9999999999)) USING TIMESTAMP " + ts.getAndIncrement());
                           });
    }

    @Test
    public void vector() throws Exception
    {
        assertFlushMatches("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 4>)",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               execute("INSERT INTO " + t + " (k, v) VALUES (1, [1.0, 2.5, -3.25, 0.0]) USING TIMESTAMP " + ts.getAndIncrement());
                           });
    }

    @Test
    public void duration() throws Exception
    {
        assertFlushMatches("CREATE TABLE %s (k int PRIMARY KEY, d duration)",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               execute("INSERT INTO " + t + " (k, d) VALUES (1, 89h4m48s) USING TIMESTAMP " + ts.getAndIncrement());
                           });
    }

    @Test
    public void counterRegular() throws Exception
    {
        assertFlushMatchesLogically("CREATE TABLE %s (k int PRIMARY KEY, c counter)",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               // counters have no USING TIMESTAMP; each UPDATE merges into the
                               // memtable's counter context, which the flush path passes through
                               // unmodified.
                               for (int i = 0; i < 5; i++)
                                   execute("UPDATE " + t + " SET c = c + ? WHERE k = 1", (long) (i + 1));
                               execute("UPDATE " + t + " SET c = c - ? WHERE k = 2", 3L);
                           }, true);
    }

    @Test
    public void counterStatic() throws Exception
    {
        assertFlushMatchesLogically("CREATE TABLE %s (k int, c1 int, cnt counter static, PRIMARY KEY (k, c1))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               for (int i = 0; i < 3; i++)
                                   execute("UPDATE " + t + " SET cnt = cnt + ? WHERE k = 1", (long) (i + 1));
                           }, true);
    }

    /**
     * Asserts {@code isSupported} declines {@code cfs}'s current memtable, then flushes it with the
     * cluster-level flag on, checking that a declined table takes the iterator fallback and still
     * flushes correctly.
     */
    private void assertDeclinedThenFlush(ColumnFamilyStore cfs, String declineMessage) throws Throwable
    {
        assertFalse(declineMessage, MemtableCursorFlusher.isSupported(cfs.metadata(), cfs.getCurrentMemtable()));

        DatabaseDescriptor.setCursorFlushEnabled(true);
        try
        {
            flush();
        }
        finally
        {
            DatabaseDescriptor.setCursorFlushEnabled(false);
        }
    }

    @Test
    public void indexedTableFallsBackToIteratorFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE INDEX ON %s (v)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 1, "hello");
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 2, "world");

        assertDeclinedThenFlush(cfs, "indexed table must be declined by isSupported");

        assertRows(execute("SELECT k, v FROM %s WHERE v = ?", "hello"), row(1, "hello"));
        assertRows(execute("SELECT k, v FROM %s WHERE v = ?", "world"), row(2, "world"));
    }

    /**
     * TrieMemtable is out of scope for the cursor flush path.  Checks that it declines cleanly and
     * still flushes correctly through the iterator fallback.
     */
    @Test
    public void trieMemtableFallsBackToIteratorFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH memtable = 'trie'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 1, "hello");
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 2, "world");

        assertDeclinedThenFlush(cfs, "TrieMemtable must be declined by isSupported");

        assertRows(execute("SELECT k, v FROM %s WHERE k = ?", 1), row(1, "hello"));
        assertRows(execute("SELECT k, v FROM %s WHERE k = ?", 2), row(2, "world"));
    }

    @Test
    public void offHeapAllocatorFallsBackToIteratorFlush() throws Throwable
    {
        Config.MemtableAllocationType original = DatabaseDescriptor.getMemtableAllocationType();
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

            execute("INSERT INTO %s (k, v) VALUES (?, ?)", 1, "hello");
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", 2, "world");

            assertDeclinedThenFlush(cfs, "an off-heap allocator must be declined by isSupported");

            assertRows(execute("SELECT k, v FROM %s WHERE k = ?", 1), row(1, "hello"));
            assertRows(execute("SELECT k, v FROM %s WHERE k = ?", 2), row(2, "world"));
        }
        finally
        {
            DatabaseDescriptor.getRawConfig().memtable_allocation_type = original;
        }
    }

    /**
     * A vint widens from 1 byte to 2 at 128 and to 3 at 16384.  20,000 rows crosses both
     * boundaries in the vint-encoded row-index and distance fields, which 500 rows does not.
     */
    @Test
    public void veryWidePartitionCrossesVintLengthBoundaries() throws Exception
    {
        int rows = 20_000;
        assertFlushMatches("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               AtomicLong ts = ts();
                               for (int c = 0; c < rows; c++)
                                   execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + ts.getAndIncrement(),
                                          1, c, "value-" + c);
                           });
    }
}
