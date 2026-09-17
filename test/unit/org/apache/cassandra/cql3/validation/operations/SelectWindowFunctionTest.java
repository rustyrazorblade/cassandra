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
package org.apache.cassandra.cql3.validation.operations;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_WINDOW_FUNCTION_ENABLED;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the ROW_NUMBER window function:
 * {@code SELECT <cols>, ROW_NUMBER() OVER (ORDER BY <non-clustering col> [ASC|DESC]) FROM t
 * WHERE <full single-column-PK equality> [LIMIT n]}.
 *
 * <p>The feature is behind the {@code cassandra.cql.window_function.enabled} flag.  The rank is the
 * post-sort position of the row inside the single buffered partition, so it follows the window
 * ORDER BY, not the storage (clustering) order.  The rank is a {@code bigint}.  Research POC.
 *
 * <p>These tests set ONLY the window flag.  They never set the arena aggregation flag; a window
 * query must engage the arena regardless of that flag.  If a test passed only because the arena
 * flag was on, it would not prove the window path stands on its own.
 */
public class SelectWindowFunctionTest extends CQLTester
{
    @Before
    public void enableWindow()
    {
        CQL_WINDOW_FUNCTION_ENABLED.setBoolean(true);
    }

    @After
    public void resetWindow()
    {
        CQL_WINDOW_FUNCTION_ENABLED.reset();
    }

    /**
     * Seeds one partition (pk = 1) whose rows are inserted so the storage order (by clustering ck)
     * is NOT the ORDER BY v order.  ck vs v: (1,30), (2,10), (3,20).  Sorted by v ascending the
     * order is ck 2, 3, 1; by v descending it is ck 1, 3, 2.  A correct rank never matches ck.
     */
    private void seedPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 30)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 10)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 20)");
    }

    // --- Correctness -----------------------------------------------------------------------------

    /**
     * ASC: the rank follows the sorted v order 10, 20, 30 and is 1, 2, 3, not the storage order.
     */
    @Test
    public void testHappyPathAsc() throws Throwable
    {
        seedPartition();

        assertRows(execute("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1"),
                   row(2, 10, 1L),
                   row(3, 20, 2L),
                   row(1, 30, 3L));
    }

    /**
     * The default direction is ASC when neither ASC nor DESC is given.
     */
    @Test
    public void testHappyPathDefaultDirection() throws Throwable
    {
        seedPartition();

        assertRows(execute("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v) FROM %s WHERE pk = 1"),
                   row(2, 10, 1L),
                   row(3, 20, 2L),
                   row(1, 30, 3L));
    }

    /**
     * DESC: the rank follows the sorted v order 30, 20, 10 and is 1, 2, 3.
     */
    @Test
    public void testHappyPathDesc() throws Throwable
    {
        seedPartition();

        assertRows(execute("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v DESC) FROM %s WHERE pk = 1"),
                   row(1, 30, 1L),
                   row(3, 20, 2L),
                   row(2, 10, 3L));
    }

    /**
     * LIMIT trims AFTER the sort and rank.  The first two rows by v ascending keep ranks 1 and 2.
     */
    @Test
    public void testLimitAfterRank() throws Throwable
    {
        seedPartition();

        assertRows(execute("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1 LIMIT 2"),
                   row(2, 10, 1L),
                   row(3, 20, 2L));
    }

    /**
     * The ORDER BY column need not be in the SELECT list.  It is added to the buffer only to drive
     * the sort (CASSANDRA-4911); the client sees just the requested columns plus the rank.
     */
    @Test
    public void testOrderByColumnNotProjected() throws Throwable
    {
        seedPartition();

        assertRows(execute("SELECT ck, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1"),
                   row(2, 1L),
                   row(3, 2L),
                   row(1, 3L));
    }

    /**
     * An alias on ROW_NUMBER names the trailing rank column.
     */
    @Test
    public void testRankAlias() throws Throwable
    {
        seedPartition();

        assertRows(execute("SELECT ck, ROW_NUMBER() OVER (ORDER BY v ASC) AS rn FROM %s WHERE pk = 1"),
                   row(2, 1L),
                   row(3, 2L),
                   row(1, 3L));
    }

    /**
     * A matching top-level ORDER BY (same column, same direction) is accepted; the two agree on the
     * single sort key.
     */
    @Test
    public void testMatchingTopLevelOrderBy() throws Throwable
    {
        seedPartition();

        assertRows(execute("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v DESC) FROM %s WHERE pk = 1 ORDER BY v DESC"),
                   row(1, 30, 1L),
                   row(3, 20, 2L),
                   row(2, 10, 3L));
    }

    /**
     * An absent partition returns the window result shape with no rows, not an error.
     */
    @Test
    public void testEmptyPartition() throws Throwable
    {
        seedPartition();

        assertEmpty(execute("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 999"));
    }

    // --- Flag gate -------------------------------------------------------------------------------

    /**
     * With the flag off the query fails at prepare with the message that names the flag.
     */
    @Test
    public void testFlagOffFailsAtPrepare() throws Throwable
    {
        CQL_WINDOW_FUNCTION_ENABLED.setBoolean(false);
        seedPartition();

        assertInvalidMessage("Window functions are not enabled",
                             "SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1");
    }

    // --- Rejections (each with its own message) --------------------------------------------------

    /**
     * No WHERE is a key range: there is no single partition to rank over.
     */
    @Test
    public void testRejectNoWhere() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("ROW_NUMBER requires a single partition",
                             "SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s");
    }

    /**
     * IN on the partition key spans more than one partition.
     */
    @Test
    public void testRejectInOnPartitionKey() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("ROW_NUMBER does not support IN on the partition key",
                             "SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk IN (1, 2)");
    }

    /**
     * ORDER BY a clustering column is not the arena's non-clustering sort shape.
     */
    @Test
    public void testRejectOrderByClusteringColumn() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("must order by a non-clustering, non-key column",
                             "SELECT ck, v, ROW_NUMBER() OVER (ORDER BY ck ASC) FROM %s WHERE pk = 1");
    }

    /**
     * ORDER BY the partition key is likewise rejected.
     */
    @Test
    public void testRejectOrderByPartitionKey() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("must order by a non-clustering, non-key column",
                             "SELECT ck, v, ROW_NUMBER() OVER (ORDER BY pk ASC) FROM %s WHERE pk = 1");
    }

    /**
     * A top-level ORDER BY that disagrees with the window ORDER BY direction is rejected.
     */
    @Test
    public void testRejectMismatchedTopLevelOrderBy() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("must match the window ORDER BY exactly",
                             "SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1 ORDER BY v DESC");
    }

    /**
     * DISTINCT changes the row set the rank runs over.
     */
    @Test
    public void testRejectDistinct() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("ROW_NUMBER does not support DISTINCT",
                             "SELECT DISTINCT pk, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1");
    }

    /**
     * GROUP BY changes the row set the rank runs over.
     */
    @Test
    public void testRejectGroupBy() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("ROW_NUMBER does not support GROUP BY",
                             "SELECT ck, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1 GROUP BY ck");
    }

    /**
     * PER PARTITION LIMIT changes the buffered row set.
     */
    @Test
    public void testRejectPerPartitionLimit() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("ROW_NUMBER does not support PER PARTITION LIMIT",
                             "SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1 PER PARTITION LIMIT 2");
    }

    /**
     * An aggregate alongside ROW_NUMBER would never run on the arena's non-aggregate branch.
     */
    @Test
    public void testRejectAggregate() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("ROW_NUMBER does not support aggregate functions",
                             "SELECT COUNT(v), ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1");
    }

    /**
     * Only one window function is supported.
     */
    @Test
    public void testRejectTwoWindowFunctions() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("Only one window function is supported",
                             "SELECT ROW_NUMBER() OVER (ORDER BY v ASC), ROW_NUMBER() OVER (ORDER BY v DESC) FROM %s WHERE pk = 1");
    }

    /**
     * ROW_NUMBER must be the last item in the SELECT clause.
     */
    @Test
    public void testRejectRowNumberNotLast() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("must be the last item in the SELECT clause",
                             "SELECT ROW_NUMBER() OVER (ORDER BY v ASC), ck FROM %s WHERE pk = 1");
    }

    /**
     * SELECT JSON changes the shape the arena buffers.
     */
    @Test
    public void testRejectJson() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("ROW_NUMBER does not support SELECT JSON",
                             "SELECT JSON ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1");
    }

    /**
     * A query that pins the full partition key but still uses a secondary index on another column
     * is rejected with the index-specific message, not the key-range message.
     */
    @Test
    public void testRejectSecondaryIndex() throws Throwable
    {
        seedPartition();
        createIndex("CREATE INDEX ON %s (v)");

        assertInvalidMessage("ROW_NUMBER does not support secondary index queries",
                             "SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1 AND v = 10");
    }

    // --- Identifier compatibility ----------------------------------------------------------------

    /**
     * OVER and ROW_NUMBER are unreserved keywords, so a column may still be named over or row_number.
     */
    @Test
    public void testUnreservedKeywordColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, over int, row_number int)");
        execute("INSERT INTO %s (pk, over, row_number) VALUES (1, 7, 9)");

        assertRows(execute("SELECT over, row_number FROM %s WHERE pk = 1"),
                   row(7, 9));
    }

    // --- Distributed (coordinator) path ----------------------------------------------------------

    /**
     * The distributed path routes through executeNet, a real coordinator read, not executeInternal.
     * The rank must still follow the window ORDER BY.
     */
    @Test
    public void testHappyPathDistributed() throws Throwable
    {
        requireNetwork();
        seedPartition();

        com.datastax.driver.core.ResultSet rs =
            executeNet("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1");

        List<Long> ranks = new ArrayList<>();
        List<Integer> vs = new ArrayList<>();
        for (com.datastax.driver.core.Row r : rs)
        {
            vs.add(r.getInt("v"));
            ranks.add(r.getLong("row_number"));
        }

        assertEquals(java.util.Arrays.asList(10, 20, 30), vs);
        assertEquals(java.util.Arrays.asList(1L, 2L, 3L), ranks);
    }

    /**
     * A window query must return every row in one page even when the client fetch size is smaller
     * than the partition, so the rank stays a continuous 1..N.  If the window term were dropped from
     * the single-page dispatch, the coordinator would page and the rank would restart per page.
     */
    @Test
    public void testRanksContinuousAcrossSmallFetchSize() throws Throwable
    {
        requireNetwork();

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        // Ten rows, inserted so storage order (by ck) is the reverse of the v order.
        int n = 10;
        for (int i = 0; i < n; i++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", i, (n - i) * 10);

        // Fetch size 3 is far smaller than the partition; the window path must still return one page.
        com.datastax.driver.core.ResultSet rs =
            executeNetWithPaging("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1", 3);

        List<Long> ranks = new ArrayList<>();
        List<Integer> vs = new ArrayList<>();
        for (com.datastax.driver.core.Row r : rs)
        {
            vs.add(r.getInt("v"));
            ranks.add(r.getLong("row_number"));
        }

        List<Long> expectedRanks = new ArrayList<>();
        List<Integer> expectedVs = new ArrayList<>();
        for (int i = 0; i < n; i++)
        {
            expectedRanks.add((long) (i + 1));
            expectedVs.add((i + 1) * 10);
        }
        assertEquals(expectedVs, vs);
        assertEquals(expectedRanks, ranks);
    }

    /**
     * Ties on the sort column still get distinct, contiguous ranks 1..N with no gaps or duplicates.
     * The order within a tie group is arena-defined (the sort is not stable), so this asserts the
     * rank multiset, not the row order.
     */
    @Test
    public void testTiesGetContiguousRanks() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 10)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 10)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 20)");

        List<Long> ranks = new ArrayList<>();
        for (Object[] r : getRows(execute("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1")))
            ranks.add((Long) r[2]);

        java.util.Collections.sort(ranks);
        assertEquals(java.util.Arrays.asList(1L, 2L, 3L), ranks);
    }

    /**
     * A NULL in the sort column still gets a rank; the ranks cover 1..n over every row.  Cassandra
     * orders nulls first on ASC, so the null row ranks 1.
     */
    @Test
    public void testNullSortColumnGetsRank() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 20)");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 2)"); // v is null
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 10)");

        assertRows(execute("SELECT ck, v, ROW_NUMBER() OVER (ORDER BY v ASC) FROM %s WHERE pk = 1"),
                   row(2, null, 1L),
                   row(3, 10, 2L),
                   row(1, 20, 3L));
    }

    /**
     * ROW_NUMBER inside another expression is not a top-level SELECT item, so it is rejected at
     * prepare with a clear message rather than surfacing as an internal server error.
     */
    @Test
    public void testRejectNestedWindow() throws Throwable
    {
        seedPartition();

        assertInvalidMessage("ROW_NUMBER() cannot be used inside another expression",
                             "SELECT ck, ROW_NUMBER() OVER (ORDER BY v ASC) + 1 FROM %s WHERE pk = 1");
    }

    /**
     * A top-level ANN/topK ORDER BY alongside ROW_NUMBER is rejected with the ANN-specific message.
     */
    @Test
    public void testRejectAnnOrdering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val int, embedding vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(embedding) USING 'StorageAttachedIndex'");

        assertInvalidMessage("ROW_NUMBER does not support ANN/topK ordering",
                             "SELECT pk, ROW_NUMBER() OVER (ORDER BY val ASC) FROM %s ORDER BY embedding ANN OF [1.0, 2.0] LIMIT 3");
    }

    /**
     * A window ORDER BY over a type the arena cannot encode (a collection) is rejected.
     */
    @Test
    public void testRejectNonEncodableSortColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, fv frozen<list<int>>, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, fv) VALUES (1, 1, [1, 2])");

        assertInvalidMessage("must order by a column the arena can sort",
                             "SELECT ck, fv, ROW_NUMBER() OVER (ORDER BY fv ASC) FROM %s WHERE pk = 1");
    }
}
