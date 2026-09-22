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

import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Basic end-to-end checks of the memtable cursor flush path, via
 * {@link MemtableFlushDifferentialTester}, with small scenarios so a failure points at the basic
 * mechanism.
 * <p>
 * Every mutation uses an explicit {@code USING TIMESTAMP} from a fixed base.  The two tables are
 * populated sequentially, so an auto-assigned wall-clock timestamp would differ between them and
 * show up as a false divergence.
 */
public class BasicFlushDifferentialTest extends MemtableFlushDifferentialTester
{
    private static final long BASE_TS = 1_000_000_000_000L;

    @Test
    public void simpleInserts() throws Exception
    {
        assertFlushMatches("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               for (int k = 0; k < 5; k++)
                                   for (int c = 0; c < 5; c++)
                                       execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + (ts++),
                                              k, c, "value-" + k + "-" + c);
                           });
    }

    @Test
    public void rowAndRangeDeletions() throws Exception
    {
        // Logical-only: a DELETE's local_delete_time is wall-clock seconds, not set by USING
        // TIMESTAMP, so it can differ between the two sequential populate() calls.
        assertFlushMatchesLogically("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               for (int c = 0; c < 20; c++)
                                   execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + (ts++),
                                          1, c, "value-" + c);
                               execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ? AND c = ?", 1, 5);
                               execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ? AND c >= ? AND c < ?", 1, 8, 12);
                               execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ?", 2);
                           }, false);
    }

    @Test
    public void ttlAndOverwrites() throws Exception
    {
        // Logical-only: a TTL row's expires_at is wall-clock based, the same risk as deletions.
        assertFlushMatchesLogically("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TTL 12345 AND TIMESTAMP " + (ts++), 1, 1, "a");
                               execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + (ts++), 1, 1, "b");
                               execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TTL 12345 AND TIMESTAMP " + (ts++), 1, 2, "c");
                           }, false);
    }

    @Test
    public void staticColumn() throws Exception
    {
        assertFlushMatches("CREATE TABLE %s (k int, c int, s text STATIC, v text, PRIMARY KEY (k, c))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               execute("INSERT INTO " + t + " (k, s) VALUES (?, ?) USING TIMESTAMP " + (ts++), 1, "static-value");
                               for (int c = 0; c < 5; c++)
                                   execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + (ts++),
                                          1, c, "value-" + c);
                               execute("INSERT INTO " + t + " (k, s) VALUES (?, ?) USING TIMESTAMP " + (ts++), 2, "only-static");
                           });
    }

    /**
     * Covers {@code writeStaticRow}'s branches that {@code staticColumn} does not: a column-level
     * static-cell tombstone, and a whole-partition deletion that wipes a written static value.
     */
    @Test
    public void staticRowEdgeCases() throws Exception
    {
        assertFlushMatchesLogically("CREATE TABLE %s (k int, c int, s text STATIC, v text, PRIMARY KEY (k, c))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               // Static value tombstoned at the column level; leaves an empty static row.
                               execute("INSERT INTO " + t + " (k, s) VALUES (?, ?) USING TIMESTAMP " + (ts++), 1, "static-value");
                               execute("DELETE s FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ?", 1);

                               // Static value set, then the whole partition deleted.
                               execute("INSERT INTO " + t + " (k, s) VALUES (?, ?) USING TIMESTAMP " + (ts++), 2, "will-be-wiped");
                               execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ?", 2);

                               // Static value alongside regular rows, the common case.
                               execute("INSERT INTO " + t + " (k, s) VALUES (?, ?) USING TIMESTAMP " + (ts++), 3, "kept");
                               execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + (ts++), 3, 1, "row");
                           }, false);
    }

    @Test
    public void widePartition() throws Exception
    {
        assertFlushMatches("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               for (int c = 0; c < 500; c++)
                                   execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + (ts++),
                                          1, c, "value-" + c);
                           });
    }

    @Test
    public void shardedSkipListMemtable() throws Exception
    {
        // Runs against ShardedSkipListMemtable, the other memtable class isSupported() accepts;
        // "skiplist_sharded" is a named memtable config in test/conf/cassandra.yaml.
        assertFlushMatches("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c)) WITH memtable = 'skiplist_sharded'",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               for (int k = 0; k < 20; k++)
                                   for (int c = 0; c < 20; c++)
                                       execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + (ts++),
                                              k, c, "value-" + k + "-" + c);
                           });
    }

    /**
     * Covers individual bits of
     * {@link org.apache.cassandra.db.rows.Cell.Serializer#encodeFlags}: an empty non-null value
     * ({@code HAS_EMPTY_VALUE_MASK}), a single deleted cell with live siblings
     * ({@code IS_DELETED_MASK}), and an {@code UPDATE}-built row whose cells cannot take
     * {@code USE_ROW_TIMESTAMP_MASK}.
     */
    @Test
    public void cellFlagEdgeCases() throws Exception
    {
        assertFlushMatches("CREATE TABLE %s (k int, c int, v text, w text, PRIMARY KEY (k, c))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               // HAS_EMPTY_VALUE_MASK: an explicit empty string, not a null/absent cell.
                               execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + (ts++), 1, 1, "");

                               // IS_DELETED_MASK on one cell only: v tombstoned, w left live in the same row.
                               execute("INSERT INTO " + t + " (k, c, v, w) VALUES (?, ?, ?, ?) USING TIMESTAMP " + (ts++), 1, 2, "x", "y");
                               execute("DELETE v FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ? AND c = ?", 1, 2);

                               // UPDATE builds a row with no primary-key liveness, so its cells can never
                               // take USE_ROW_TIMESTAMP_MASK.
                               execute("UPDATE " + t + " USING TIMESTAMP " + (ts++) + " SET v = ? WHERE k = ? AND c = ?", "updated", 1, 3);
                           });
    }

    /**
     * A 3-column composite clustering key with prefix range deletes at 1-column and 2-column
     * depth, the case the marker interleaving handles against a live memtable.
     */
    @Test
    public void multiColumnClusteringWithPrefixRangeDeletes() throws Exception
    {
        assertFlushMatchesLogically("CREATE TABLE %s (k int, c1 int, c2 int, c3 int, v text, PRIMARY KEY (k, c1, c2, c3))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               for (int c1 = 0; c1 < 4; c1++)
                                   for (int c2 = 0; c2 < 4; c2++)
                                       for (int c3 = 0; c3 < 4; c3++)
                                           execute("INSERT INTO " + t + " (k, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP " + (ts++),
                                                  1, c1, c2, c3, "value-" + c1 + "-" + c2 + "-" + c3);
                               // 1-column prefix: every (c2, c3) under c1 = 1.
                               execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ? AND c1 = ?", 1, 1);
                               // 2-column prefix: every c3 under (c1, c2) = (2, 1).
                               execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ? AND c1 = ? AND c2 = ?", 1, 2, 1);
                               // fully-specified clustering: a single row.
                               execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ? AND c1 = ? AND c2 = ? AND c3 = ?", 1, 3, 1, 1);
                           }, false);
    }

    /** {@code CLUSTERING ORDER BY ... DESC} reverses the comparator both flush paths sort by. */
    @Test
    public void clusteringOrderDesc() throws Exception
    {
        assertFlushMatchesLogically("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               for (int c = 0; c < 20; c++)
                                   execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + (ts++),
                                          1, c, "value-" + c);
                               execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ? AND c >= ? AND c < ?", 1, 8, 12);
                           }, false);
    }

    /**
     * Variable-length clustering columns (text and blob), including an empty-string clustering
     * value.
     */
    @Test
    public void textAndBlobClustering() throws Exception
    {
        assertFlushMatches("CREATE TABLE %s (k int, c1 text, c2 blob, v text, PRIMARY KEY (k, c1, c2))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               execute("INSERT INTO " + t + " (k, c1, c2, v) VALUES (?, ?, ?, ?) USING TIMESTAMP " + (ts++), 1, "", ByteBufferUtil.bytes(""), "empty-clustering");
                               execute("INSERT INTO " + t + " (k, c1, c2, v) VALUES (?, ?, ?, ?) USING TIMESTAMP " + (ts++), 1, "alpha", ByteBufferUtil.bytes("beta"), "short");
                               execute("INSERT INTO " + t + " (k, c1, c2, v) VALUES (?, ?, ?, ?) USING TIMESTAMP " + (ts++), 1, "z".repeat(200), ByteBufferUtil.bytes("y".repeat(200)), "long");
                           });
    }
}
