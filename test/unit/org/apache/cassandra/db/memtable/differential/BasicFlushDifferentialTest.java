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
 * First, minimal exercise of the whole memtable cursor flush path end to end (writer wiring +
 * row/cell walk + range-tombstone interleaving) via {@link MemtableFlushDifferentialTester},
 * before the full per-type/per-size matrix. Deliberately small scenarios so a failure here
 * localizes to "the basic mechanism is broken" rather than needing the matrix's breadth to
 * even get a first green run.
 * <p>
 * Every mutation uses an explicit {@code USING TIMESTAMP}, starting fresh from a fixed base for
 * each of the two populate() calls {@link MemtableFlushDifferentialTester#assertFlushMatches}
 * makes (one per table): CQL auto-assigns the wall-clock write timestamp otherwise, and since
 * the two tables are populated sequentially (a live memtable can only be flushed once, so
 * there's no way to populate them simultaneously), that would make every row's timestamp
 * genuinely differ between the two captures — a harness artifact, not a real divergence.
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
        // logical-only: a DELETE's local_delete_time is server-wall-clock-seconds at processing
        // time (not controlled by USING TIMESTAMP) and can genuinely straddle a second boundary
        // between the two sequential populate() calls - see
        // MemtableFlushDifferentialTester.assertFlushMatchesLogically. Observed flaking on a
        // rerun of this exact scenario; not hypothetical.
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
        // logical-only: a TTL row's expires_at/local_delete_time is nowInSec() + ttl at insert
        // time (not controlled by USING TIMESTAMP) - same server-wall-clock-second risk as
        // deletions, see MemtableFlushDifferentialTester.assertFlushMatchesLogically.
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
     * {@code staticColumn} above only ever writes a static value and leaves it alone -
     * {@code writeStaticRow}'s branches (empty-static-row fast path vs. filtering an actual
     * static row against the partition deletion) need a column-level static-cell tombstone and a
     * whole-partition deletion that must wipe an already-written static value to all get hit.
     */
    @Test
    public void staticRowEdgeCases() throws Exception
    {
        assertFlushMatchesLogically("CREATE TABLE %s (k int, c int, s text STATIC, v text, PRIMARY KEY (k, c))",
                           (ks, tbl) -> {
                               String t = ks + "." + tbl;
                               long ts = BASE_TS;
                               // static value set, then tombstoned at the column level (not a row/partition
                               // deletion) - leaves an empty-but-not-absent static row.
                               execute("INSERT INTO " + t + " (k, s) VALUES (?, ?) USING TIMESTAMP " + (ts++), 1, "static-value");
                               execute("DELETE s FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ?", 1);

                               // static value set, then the whole partition deleted - writeStaticRow must
                               // filter the static row down to empty against that partition deletion, not
                               // write the already-tombstoned value.
                               execute("INSERT INTO " + t + " (k, s) VALUES (?, ?) USING TIMESTAMP " + (ts++), 2, "will-be-wiped");
                               execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = ?", 2);

                               // static value alongside regular rows in the same partition (the common case,
                               // included here as a baseline alongside the two edge cases above).
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
        // isSupported() accepts ShardedSkipListMemtable as well as the default SkipListMemtable
        // - every other scenario in this suite runs against the default, so this is the one
        // exercising that second accepted memtable class end to end. "skiplist_sharded" is a
        // named memtable configuration defined in test/conf/cassandra.yaml.
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
     * Targets {@link org.apache.cassandra.db.rows.Cell.Serializer#encodeFlags}'s individual bits
     * directly, each in a way {@link #simpleInserts} and friends don't naturally exercise:
     * {@code HAS_EMPTY_VALUE_MASK} (an empty, non-null value - distinct from no cell at all),
     * {@code IS_DELETED_MASK} on a single cell without a row/range/partition deletion (a
     * standalone-column {@code DELETE}, leaving sibling columns live), and
     * {@code USE_ROW_TIMESTAMP_MASK} deliberately NOT set (an {@code UPDATE}-built row has no
     * shared primary-key liveness for its cells to match, unlike an {@code INSERT}-built one).
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

                               // UPDATE builds a row with no primary-key liveness info, so its cells can
                               // never take USE_ROW_TIMESTAMP_MASK - unlike an INSERT-built row's cells,
                               // which typically share the row's own liveness timestamp.
                               execute("UPDATE " + t + " USING TIMESTAMP " + (ts++) + " SET v = ? WHERE k = ? AND c = ?", "updated", 1, 3);
                           });
    }

    /**
     * Every scenario above uses a single {@code int} clustering column - this exercises a
     * 3-column composite clustering key together with prefix range deletes at both the
     * 1-column and 2-column prefix depth (deleting "every c2 under this c1", not a single
     * fully-specified clustering value), the case {@code RangeTombstoneListCursor} and
     * {@code MemtableCursorFlusher}'s marker interleaving exist to handle correctly against a
     * live memtable.
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
     * Variable-length clustering columns (text/blob), unlike every {@code int} clustering column
     * used elsewhere in this suite - including an empty-string clustering value, the degenerate
     * case of "variable-length" clustering.
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
