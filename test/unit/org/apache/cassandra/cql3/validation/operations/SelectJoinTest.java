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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_JOIN_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_JOIN_MAX_ROWS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_SUBQUERY_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_WINDOW_FUNCTION_ENABLED;

/**
 * Tests for the broadcast hash JOIN:
 * {@code SELECT <probe cols> FROM t1 JOIN t2 ON t1.a = t2.b [WHERE ...] [LIMIT n]}.
 *
 * <p>The single supported shape is an INNER join of exactly two tables on one single-column
 * equi-predicate.  t1 is the probe (streamed) side; t2 is the build side, fully materialized and
 * hashed at the coordinator.  The output is the probe columns the user selected, followed by every
 * build column.  The feature is behind {@code cassandra.cql.join.enabled}.  Research POC.</p>
 *
 * <p>The SELECT list references only probe (t1) columns; the build columns are added automatically as
 * {@code SELECT *} of t2.  A probe key that the user does not select still drives the match but is not
 * shipped (CASSANDRA-4911).</p>
 */
public class SelectJoinTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        // Dynamic data masking must be on so a column can be MASKED (masked-join-key rejection test).
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        // Paging and column-metadata tests drive the native protocol through the java driver.
        requireNetwork();
    }

    @Before
    public void enableJoin()
    {
        CQL_JOIN_ENABLED.setBoolean(true);
    }

    @After
    public void resetJoin()
    {
        CQL_JOIN_ENABLED.reset();
    }

    /**
     * Creates a probe table t1 (id, val) and a build table t2 (bid, label) and seeds them so that
     * id 1 and 2 match, id 3 has no build row, and bid 4 has no probe row.
     */
    private String[] seedBasic() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, label text)");

        execute(qualify(t1, "INSERT INTO %s (id, val) VALUES (1, 'a')"));
        execute(qualify(t1, "INSERT INTO %s (id, val) VALUES (2, 'b')"));
        execute(qualify(t1, "INSERT INTO %s (id, val) VALUES (3, 'c')"));

        execute(qualify(t2, "INSERT INTO %s (bid, label) VALUES (1, 'x')"));
        execute(qualify(t2, "INSERT INTO %s (bid, label) VALUES (2, 'y')"));
        execute(qualify(t2, "INSERT INTO %s (bid, label) VALUES (4, 'z')"));

        return new String[]{ t1, t2 };
    }

    private String qualify(String table, String query)
    {
        return String.format(query, keyspace() + '.' + table);
    }

    private String fqn(String table)
    {
        return keyspace() + '.' + table;
    }

    // --- Correctness -----------------------------------------------------------------------------

    /**
     * The user selects both join keys.  Output is (id, val, bid, label) for the matched rows only.
     */
    @Test
    public void testHappyPath() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";

        assertRowsIgnoringOrder(execute(q),
                                row(1, "a", 1, "x"),
                                row(2, "b", 2, "y"));
    }

    /**
     * The probe key is not in the SELECT list.  It still drives the match but is not shipped, so the
     * output is (val, bid, label).  This exercises the synthetic (CASSANDRA-4911) probe-key path.
     */
    @Test
    public void testProbeKeyNotSelected() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";

        assertRowsIgnoringOrder(execute(q),
                                row("a", 1, "x"),
                                row("b", 2, "y"));
    }

    /**
     * The ON clause reads the same in either order; a swapped side resolves the same probe/build keys.
     */
    @Test
    public void testOnClauseTableOrderSwapped() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[1] + ".bid = " + t[0] + ".id";

        assertRowsIgnoringOrder(execute(q),
                                row(1, "a", 1, "x"),
                                row(2, "b", 2, "y"));
    }

    /**
     * No build row matches any probe row, so the INNER join returns nothing.
     */
    @Test
    public void testNoMatches() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, label text)");
        execute(qualify(t1, "INSERT INTO %s (id, val) VALUES (1, 'a')"));
        execute(qualify(t2, "INSERT INTO %s (bid, label) VALUES (9, 'z')"));

        String q = "SELECT id, val FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid";
        assertEmpty(execute(q));
    }

    /**
     * A WHERE clause filters the probe side before the join.  Only the surviving probe rows match.
     */
    @Test
    public void testWhereFiltersProbeSide() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid WHERE id = 1";

        assertRowsIgnoringOrder(execute(q),
                                row(1, "a", 1, "x"));
    }

    /**
     * One probe row matches many build rows when the build key is not unique.  The join emits one
     * output row per matching build row.
     */
    @Test
    public void testOneToManyBuildSide() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        String t2 = createTable("CREATE TABLE %s (bid int, sub int, label text, PRIMARY KEY (bid, sub))");
        execute(qualify(t1, "INSERT INTO %s (id, val) VALUES (1, 'a')"));
        execute(qualify(t2, "INSERT INTO %s (bid, sub, label) VALUES (1, 10, 'x')"));
        execute(qualify(t2, "INSERT INTO %s (bid, sub, label) VALUES (1, 20, 'y')"));

        String q = "SELECT id, val FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid";
        assertRowsIgnoringOrder(execute(q),
                                row(1, "a", 1, 10, "x"),
                                row(1, "a", 1, 20, "y"));
    }

    /**
     * LIMIT bounds the joined output, not the probe read.  With two possible joined rows, LIMIT 1
     * returns exactly one.
     */
    @Test
    public void testLimitAppliesToJoinedOutput() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid LIMIT 1";

        assertRowCount(execute(q), 1);
    }

    // --- Rejections --------------------------------------------------------------------------------

    /**
     * With the flag off, a JOIN is rejected at prepare and points at the flag.
     */
    @Test
    public void testRejectedWhenFlagOff() throws Throwable
    {
        String[] t = seedBasic();
        CQL_JOIN_ENABLED.setBoolean(false);
        String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";
        assertInvalidMessage("cassandra.cql.join.enabled", q);
    }

    /**
     * A join of a table with itself is rejected; the shape requires two different tables.
     */
    @Test
    public void testRejectSelfJoin() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        String q = "SELECT id FROM " + fqn(t1) + " JOIN " + fqn(t1) + " ON " + t1 + ".id = " + t1 + ".id";
        assertInvalidMessage("two different tables", q);
    }

    /**
     * The ON clause must reference the two joined tables, one on each side.
     */
    @Test
    public void testRejectOnClauseWrongTables() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[0] + ".id";
        assertInvalidMessage("must reference the two joined tables", q);
    }

    /**
     * The two ON columns must have the same type; int against text is rejected.
     */
    @Test
    public void testRejectTypeMismatch() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        String t2 = createTable("CREATE TABLE %s (bid text PRIMARY KEY, label text)");
        String q = "SELECT id FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid";
        assertInvalidMessage("same type", q);
    }

    /**
     * A double join key is rejected: equal values can have different byte encodings, so a byte match
     * would be wrong.
     */
    @Test
    public void testRejectNonByteCanonicalType() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id double PRIMARY KEY, val text)");
        String t2 = createTable("CREATE TABLE %s (bid double PRIMARY KEY, label text)");
        String q = "SELECT id FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid";
        assertInvalidMessage("not supported", q);
    }

    /**
     * An undefined column in the ON clause is rejected.
     */
    @Test
    public void testRejectUndefinedOnColumn() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".nope = " + t[1] + ".bid";
        assertInvalidMessage("Undefined column", q);
    }

    /**
     * A duplicate output column name is rejected; the driver keys a row by column name.
     */
    @Test
    public void testRejectDuplicateColumnName() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, label text)");
        String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, label text)");
        String q = "SELECT id, label FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid";
        assertInvalidMessage("duplicate column name", q);
    }

    /**
     * ORDER BY with a join is not supported.  The ORDER BY here is valid on its own (a clustering
     * column under a restricted partition key), so the rejection comes from the join path, not from
     * the standard ordering validation.
     */
    @Test
    public void testRejectOrderBy() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int, ck int, val text, PRIMARY KEY (id, ck))");
        String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, label text)");
        String q = "SELECT id, val FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid WHERE id = 1 ORDER BY ck";
        assertInvalidMessage("JOIN does not support ORDER BY", q);
    }

    /**
     * An aggregate function with a join is not supported.
     */
    @Test
    public void testRejectAggregate() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT count(id) FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";
        assertInvalidMessage("aggregate", q);
    }

    /**
     * SELECT DISTINCT with a join is not supported.
     */
    @Test
    public void testRejectDistinct() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT DISTINCT id FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";
        assertInvalidMessage("DISTINCT", q);
    }

    /**
     * SELECT JSON with a join is not supported.
     */
    @Test
    public void testRejectJson() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT JSON id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";
        assertInvalidMessage("JSON", q);
    }

    /**
     * GROUP BY with a join is not supported.
     */
    @Test
    public void testRejectGroupBy() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid GROUP BY id";
        assertInvalidMessage("JOIN does not support GROUP BY", q);
    }

    /**
     * PER PARTITION LIMIT with a join is not supported.
     */
    @Test
    public void testRejectPerPartitionLimit() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid PER PARTITION LIMIT 1";
        assertInvalidMessage("JOIN does not support PER PARTITION LIMIT", q);
    }

    /**
     * A window function alongside a join is not supported.  The window flag is on, so the window builds
     * and the rejection comes from the join path, not from the window feature gate.
     */
    @Test
    public void testRejectWindowFunction() throws Throwable
    {
        CQL_WINDOW_FUNCTION_ENABLED.setBoolean(true);
        try
        {
            String[] t = seedBasic();
            String q = "SELECT id, ROW_NUMBER() OVER (ORDER BY val) FROM " + fqn(t[0]) +
                       " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid WHERE id = 1";
            assertInvalidMessage("JOIN does not support window functions", q);
        }
        finally
        {
            CQL_WINDOW_FUNCTION_ENABLED.reset();
        }
    }

    /**
     * F3: a subquery in the same statement as a join is not supported.
     */
    @Test
    public void testRejectSubquery() throws Throwable
    {
        CQL_SUBQUERY_ENABLED.setBoolean(true);
        try
        {
            String[] t = seedBasic();
            String inner = createTable("CREATE TABLE %s (k int PRIMARY KEY)");
            String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid" +
                       " WHERE id IN (SELECT k FROM " + fqn(inner) + ")";
            assertInvalidMessage("JOIN does not support a subquery", q);
        }
        finally
        {
            CQL_SUBQUERY_ENABLED.reset();
        }
    }

    /**
     * LOW-1: a function in the SELECT clause is not supported (it builds a SelectionWithProcessing whose
     * row layout would break the synthetic-key slice).
     */
    @Test
    public void testRejectFunctionInSelect() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT ttl(val), id FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";
        assertInvalidMessage("JOIN does not support a function in the SELECT clause", q);
    }

    /**
     * F2: a DESC (reversed) double clustering column as the join key is rejected; unwrapping the reversed
     * type still finds the non-byte-canonical double underneath.
     */
    @Test
    public void testRejectReversedDoubleKey() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int, ck double, val text, PRIMARY KEY (id, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        String t2 = createTable("CREATE TABLE %s (bid int, bk double, label text, PRIMARY KEY (bid, bk)) WITH CLUSTERING ORDER BY (bk DESC)");
        String q = "SELECT id FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".ck = " + t2 + ".bk";
        assertInvalidMessage("not supported", q);
    }

    /**
     * F2: a frozen collection with a double element is rejected; the check recurses into subtypes.
     */
    @Test
    public void testRejectFrozenListOfDoubleKey() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id frozen<list<double>> PRIMARY KEY, val text)");
        String t2 = createTable("CREATE TABLE %s (bid frozen<list<double>> PRIMARY KEY, label text)");
        String q = "SELECT val FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid";
        assertInvalidMessage("not supported", q);
    }

    /**
     * T10: plain float and plain decimal join keys are rejected, like double.
     */
    @Test
    public void testRejectFloatAndDecimalKeys() throws Throwable
    {
        String f1 = createTable("CREATE TABLE %s (id float PRIMARY KEY, val text)");
        String f2 = createTable("CREATE TABLE %s (bid float PRIMARY KEY, label text)");
        assertInvalidMessage("not supported",
                             "SELECT id FROM " + fqn(f1) + " JOIN " + fqn(f2) + " ON " + f1 + ".id = " + f2 + ".bid");

        String d1 = createTable("CREATE TABLE %s (id decimal PRIMARY KEY, val text)");
        String d2 = createTable("CREATE TABLE %s (bid decimal PRIMARY KEY, label text)");
        assertInvalidMessage("not supported",
                             "SELECT id FROM " + fqn(d1) + " JOIN " + fqn(d2) + " ON " + d1 + ".id = " + d2 + ".bid");
    }

    /**
     * F1: a masked join key is rejected at prepare; the two sides would hash different bytes and drop
     * every match silently.
     */
    @Test
    public void testRejectMaskedJoinKey() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, label text)");
        execute(qualify(t1, "ALTER TABLE %s ALTER id MASKED WITH DEFAULT"));

        String q = "SELECT val FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid";
        assertInvalidMessage("masked join column", q);
    }

    // --- Parse rejections (T6) ---------------------------------------------------------------------

    /**
     * T6: the grammar knows only the single bare INNER JOIN shape.  LEFT, RIGHT, and OUTER joins do not
     * parse.
     */
    @Test
    public void testRejectOuterJoinSyntax() throws Throwable
    {
        String[] t = seedBasic();
        String on = " ON " + t[0] + ".id = " + t[1] + ".bid";
        assertInvalidSyntax("SELECT id FROM " + fqn(t[0]) + " LEFT JOIN " + fqn(t[1]) + on);
        assertInvalidSyntax("SELECT id FROM " + fqn(t[0]) + " RIGHT JOIN " + fqn(t[1]) + on);
        assertInvalidSyntax("SELECT id FROM " + fqn(t[0]) + " OUTER JOIN " + fqn(t[1]) + on);
    }

    /**
     * T6: a third table in the JOIN does not parse; the shape is exactly two tables.
     */
    @Test
    public void testRejectThreeTableJoinSyntax() throws Throwable
    {
        String[] t = seedBasic();
        String t3 = createTable("CREATE TABLE %s (cid int PRIMARY KEY, note text)");
        String q = "SELECT id FROM " + fqn(t[0]) +
                   " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid" +
                   " JOIN " + fqn(t3) + " ON " + t[0] + ".id = " + t3 + ".cid";
        assertInvalidSyntax(q);
    }

    /**
     * T6: a compound ON (with AND) and an inequality ON do not parse; the shape is one single-column
     * equi-predicate.
     */
    @Test
    public void testRejectCompoundAndInequalityOnSyntax() throws Throwable
    {
        String[] t = seedBasic();
        assertInvalidSyntax("SELECT id FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) +
                            " ON " + t[0] + ".id = " + t[1] + ".bid AND " + t[0] + ".id = " + t[1] + ".bid");
        assertInvalidSyntax("SELECT id FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) +
                            " ON " + t[0] + ".id < " + t[1] + ".bid");
    }

    // --- Caps (F4/F5/F6) ---------------------------------------------------------------------------

    /**
     * F4: the build side fails loudly when it exceeds the row cap, naming the threshold.
     */
    @Test
    public void testBuildSideCapFires() throws Throwable
    {
        CQL_JOIN_MAX_ROWS.setInt(2);
        try
        {
            String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
            String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, label text)");
            execute(qualify(t1, "INSERT INTO %s (id, val) VALUES (1, 'a')"));
            for (int i = 0; i < 5; i++)
                execute(qualify(t2, "INSERT INTO %s (bid, label) VALUES (" + i + ", 'x')"));

            String q = "SELECT id, val FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid";
            assertInvalidMessage("build side", q);
        }
        finally
        {
            CQL_JOIN_MAX_ROWS.reset();
        }
    }

    /**
     * F5: the probe side fails loudly when it exceeds the row cap.  The build side stays small so the
     * build cap does not fire first.
     */
    @Test
    public void testProbeSideCapFires() throws Throwable
    {
        CQL_JOIN_MAX_ROWS.setInt(2);
        try
        {
            String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
            String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, label text)");
            for (int i = 0; i < 5; i++)
                execute(qualify(t1, "INSERT INTO %s (id, val) VALUES (" + i + ", 'a')"));
            execute(qualify(t2, "INSERT INTO %s (bid, label) VALUES (0, 'x')"));

            String q = "SELECT id, val FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".id = " + t2 + ".bid";
            assertInvalidMessage("probe side", q);
        }
        finally
        {
            CQL_JOIN_MAX_ROWS.reset();
        }
    }

    /**
     * F6: the joined output fails loudly when the cross product exceeds the row cap, even though each
     * side alone stays under it.
     */
    @Test
    public void testJoinedOutputCapFires() throws Throwable
    {
        CQL_JOIN_MAX_ROWS.setInt(3);
        try
        {
            // 2 probe rows and 2 build rows on the same key produce 4 output rows, over the cap of 3.
            String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, ref int, val text)");
            String t2 = createTable("CREATE TABLE %s (bid int, sub int, label text, PRIMARY KEY (bid, sub))");
            execute(qualify(t1, "INSERT INTO %s (id, ref, val) VALUES (1, 5, 'a')"));
            execute(qualify(t1, "INSERT INTO %s (id, ref, val) VALUES (2, 5, 'b')"));
            execute(qualify(t2, "INSERT INTO %s (bid, sub, label) VALUES (5, 10, 'x')"));
            execute(qualify(t2, "INSERT INTO %s (bid, sub, label) VALUES (5, 20, 'y')"));

            String q = "SELECT id, val FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".ref = " + t2 + ".bid";
            assertInvalidMessage("output rows", q);
        }
        finally
        {
            CQL_JOIN_MAX_ROWS.reset();
        }
    }

    // --- Extra correctness (T1-T4, T8, T9) ---------------------------------------------------------

    /**
     * T1: LIMIT counts matched output rows, not probe rows read.  Many non-matching probe rows sort
     * before the matches in one partition; with LIMIT below the match count, the probe read is not
     * capped (it is forced to NO_LIMIT), so exactly LIMIT matched rows come back.
     */
    @Test
    public void testLimitCountsMatchedRowsOnly() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (pk int, ck int, ref int, PRIMARY KEY (pk, ck))");
        String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, label text)");
        // ck 0..7 do not match (ref 999); ck 8,9,10 match (ref 1,2,3).  Non-matches sort first.
        for (int ck = 0; ck <= 7; ck++)
            execute(qualify(t1, "INSERT INTO %s (pk, ck, ref) VALUES (1, " + ck + ", 999)"));
        execute(qualify(t1, "INSERT INTO %s (pk, ck, ref) VALUES (1, 8, 1)"));
        execute(qualify(t1, "INSERT INTO %s (pk, ck, ref) VALUES (1, 9, 2)"));
        execute(qualify(t1, "INSERT INTO %s (pk, ck, ref) VALUES (1, 10, 3)"));
        execute(qualify(t2, "INSERT INTO %s (bid, label) VALUES (1, 'x')"));
        execute(qualify(t2, "INSERT INTO %s (bid, label) VALUES (2, 'y')"));
        execute(qualify(t2, "INSERT INTO %s (bid, label) VALUES (3, 'z')"));

        String q = "SELECT ck FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".ref = " + t2 + ".bid LIMIT 2";
        assertRowCount(execute(q), 2);
    }

    /**
     * T2: many probe rows share a key and many build rows share it; the join emits the full cross
     * product.
     */
    @Test
    public void testManyToMany() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, ref int, val text)");
        String t2 = createTable("CREATE TABLE %s (bid int, sub int, label text, PRIMARY KEY (bid, sub))");
        execute(qualify(t1, "INSERT INTO %s (id, ref, val) VALUES (1, 5, 'a')"));
        execute(qualify(t1, "INSERT INTO %s (id, ref, val) VALUES (2, 5, 'b')"));
        execute(qualify(t2, "INSERT INTO %s (bid, sub, label) VALUES (5, 10, 'x')"));
        execute(qualify(t2, "INSERT INTO %s (bid, sub, label) VALUES (5, 20, 'y')"));

        String q = "SELECT id, val FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".ref = " + t2 + ".bid";
        assertRowsIgnoringOrder(execute(q),
                                row(1, "a", 5, 10, "x"),
                                row(1, "a", 5, 20, "y"),
                                row(2, "b", 5, 10, "x"),
                                row(2, "b", 5, 20, "y"));
    }

    /**
     * T3: a null join key drops the row on both sides (regular nullable columns, not primary keys).
     */
    @Test
    public void testNullJoinKeysDroppedBothSides() throws Throwable
    {
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, ref int)");
        String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, bref int, label text)");
        execute(qualify(t1, "INSERT INTO %s (id, ref) VALUES (1, null)")); // null probe key: dropped
        execute(qualify(t1, "INSERT INTO %s (id, ref) VALUES (2, 5)"));    // matches
        execute(qualify(t2, "INSERT INTO %s (bid, bref, label) VALUES (10, null, 'x')")); // null build key: dropped
        execute(qualify(t2, "INSERT INTO %s (bid, bref, label) VALUES (20, 5, 'y')"));    // matches

        String q = "SELECT id FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".ref = " + t2 + ".bref";
        assertRowsIgnoringOrder(execute(q),
                                row(2, 20, 5, "y"));
    }

    /**
     * T4: paging returns the identical complete result at fetch sizes below, equal to, and above the
     * result size.
     */
    @Test
    public void testPagingReturnsSameResult() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";

        Set<List<Object>> expected = new HashSet<>();
        for (Row r : executeNetWithPaging(getDefaultVersion(), q, 100).all())
            expected.add(java.util.Arrays.asList(r.getInt(0), r.getString(1), r.getInt(2), r.getString(3)));
        assertEquals(2, expected.size());

        for (int pageSize : new int[]{ 1, 2, 100 })
        {
            Set<List<Object>> got = new HashSet<>();
            for (Row r : executeNetWithPaging(getDefaultVersion(), q, pageSize).all())
                got.add(java.util.Arrays.asList(r.getInt(0), r.getString(1), r.getInt(2), r.getString(3)));
            assertEquals("page size " + pageSize, expected, got);
        }
    }

    /**
     * T8: the combined result metadata carries the correct keyspace and table per column: the probe
     * columns name t1, the build columns name t2.
     */
    @Test
    public void testCombinedMetadataKeyspaceAndTable() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT id, val FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";

        ColumnDefinitions defs = executeNet(getDefaultVersion(), q).getColumnDefinitions();
        assertEquals(4, defs.size());
        // Probe columns id, val name t1.
        assertEquals(keyspace(), defs.getKeyspace(0));
        assertEquals(t[0], defs.getTable(0));
        assertEquals("id", defs.getName(0));
        assertEquals(t[0], defs.getTable(1));
        assertEquals("val", defs.getName(1));
        // Build columns bid, label name t2.
        assertEquals(keyspace(), defs.getKeyspace(2));
        assertEquals(t[1], defs.getTable(2));
        assertEquals("bid", defs.getName(2));
        assertEquals(t[1], defs.getTable(3));
        assertEquals("label", defs.getName(3));
    }

    /**
     * T9: a SELECT * probe ships all t1 columns, then the join appends all t2 columns.
     */
    @Test
    public void testSelectStarProbe() throws Throwable
    {
        String[] t = seedBasic();
        String q = "SELECT * FROM " + fqn(t[0]) + " JOIN " + fqn(t[1]) + " ON " + t[0] + ".id = " + t[1] + ".bid";

        ColumnDefinitions defs = executeNet(getDefaultVersion(), q).getColumnDefinitions();
        assertEquals(4, defs.size());
        assertEquals("id", defs.getName(0));
        assertEquals("val", defs.getName(1));
        assertEquals("bid", defs.getName(2));
        assertEquals("label", defs.getName(3));

        assertRowsIgnoringOrder(execute(q),
                                row(1, "a", 1, "x"),
                                row(2, "b", 2, "y"));
    }
}
