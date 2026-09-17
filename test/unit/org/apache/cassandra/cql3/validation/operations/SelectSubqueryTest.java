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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_SUBQUERY_ENABLED;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the uncorrelated IN-subquery on the partition key:
 * {@code SELECT ... FROM t1 WHERE pk IN (SELECT one_col FROM t2 [WHERE ...])}.
 * The feature is behind the {@code cassandra.cql.subquery.enabled} flag.  Research POC.
 */
public class SelectSubqueryTest extends CQLTester
{
    @Before
    public void enableSubqueries()
    {
        CQL_SUBQUERY_ENABLED.setBoolean(true);
    }

    @After
    public void resetSubqueries()
    {
        CQL_SUBQUERY_ENABLED.reset();
    }

    /**
     * Creates an inner table and populates it with the given single-column values.  Returns the
     * fully-qualified inner table name so the outer query can reference it.
     */
    private String createInner(String type, Object... values) throws Throwable
    {
        String inner = qTable("CREATE TABLE %s (k " + type + " PRIMARY KEY)");
        for (Object v : values)
            execute("INSERT INTO " + inner + " (k) VALUES (?)", v);
        return inner;
    }

    /** Creates a table and returns its keyspace-qualified name, so a raw query can reference it. */
    private String qTable(String query)
    {
        return keyspace() + '.' + createTable(query);
    }

    // --- Correctness -------------------------------------------------------------------------

    @Test
    public void testHappyPath() throws Throwable
    {
        String inner = createInner("int", 1, 3);

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (pk, v) VALUES (1, 'a')");
        execute("INSERT INTO %s (pk, v) VALUES (2, 'b')");
        execute("INSERT INTO %s (pk, v) VALUES (3, 'c')");

        assertRowsIgnoringOrder(execute("SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + ")"),
                                row(1, "a"),
                                row(3, "c"));
    }

    @Test
    public void testHappyPathDistributed() throws Throwable
    {
        // The distributed (executeNet) path resolves the inner through query.execute(), a real
        // coordinator read, not executeInternal().  pk=2 is a decoy: it exists in the outer table
        // but not in the inner set {1, 3}, so it must be filtered out.
        requireNetwork();

        String inner = createInner("int", 1, 3);

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (pk, v) VALUES (1, 'a')");
        execute("INSERT INTO %s (pk, v) VALUES (2, 'b')");
        execute("INSERT INTO %s (pk, v) VALUES (3, 'c')");

        com.datastax.driver.core.ResultSet rs =
            executeNet("SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + ")");

        java.util.Set<Integer> pks = new java.util.HashSet<>();
        for (com.datastax.driver.core.Row r : rs)
            pks.add(r.getInt("pk"));

        assertEquals(new java.util.HashSet<>(java.util.Arrays.asList(1, 3)), pks);
    }

    @Test
    public void testHappyPathWithInnerWhere() throws Throwable
    {
        String inner = qTable("CREATE TABLE %s (k int PRIMARY KEY, keep boolean)");
        execute("INSERT INTO " + inner + " (k, keep) VALUES (1, true)");
        execute("INSERT INTO " + inner + " (k, keep) VALUES (2, false)");
        execute("INSERT INTO " + inner + " (k, keep) VALUES (3, true)");

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (pk, v) VALUES (1, 'a')");
        execute("INSERT INTO %s (pk, v) VALUES (2, 'b')");
        execute("INSERT INTO %s (pk, v) VALUES (3, 'c')");

        assertRowsIgnoringOrder(execute("SELECT pk, v FROM %s WHERE pk IN " +
                                        "(SELECT k FROM " + inner + " WHERE keep = true ALLOW FILTERING)"),
                                row(1, "a"),
                                row(3, "c"));
    }

    @Test
    public void testEmptyInnerYieldsNoRows() throws Throwable
    {
        String inner = createInner("int"); // no rows

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (pk, v) VALUES (1, 'a')");

        assertEmpty(execute("SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + ")"));
    }

    // Deduplication is proven antagonistically in SelectSubqueryGuardrailTest: raw duplicate inner
    // rows exceed the fail threshold, but the distinct keys stay under it, so the query succeeds.

    @Test
    public void testLargeInnerUnpaged() throws Throwable
    {
        String inner = qTable("CREATE TABLE %s (k int PRIMARY KEY)");
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        for (int i = 0; i < 500; i++)
        {
            execute("INSERT INTO " + inner + " (k) VALUES (?)", i);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, i);
        }

        // No client page size is set on execute(); the inner is fully materialized regardless.
        Object[][] expected = new Object[500][];
        for (int i = 0; i < 500; i++)
            expected[i] = row(i, i);

        assertRowsIgnoringOrder(execute("SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + ")"),
                                expected);
    }

    @Test
    public void testPagedOuterReturnsCompleteResults() throws Throwable
    {
        requireNetwork();

        String inner = qTable("CREATE TABLE %s (k int PRIMARY KEY)");
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        for (int i = 0; i < 50; i++)
        {
            execute("INSERT INTO " + inner + " (k) VALUES (?)", i);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, i);
        }

        // A small client page size must not truncate the outer result.
        com.datastax.driver.core.ResultSet rs =
            executeNetWithPaging("SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + ")", 7);

        int count = 0;
        for (com.datastax.driver.core.Row ignored : rs)
            count++;
        assertEquals(50, count);
    }

    // --- Flag gating -------------------------------------------------------------------------

    @Test
    public void testFlagOffRejects() throws Throwable
    {
        CQL_SUBQUERY_ENABLED.reset();

        String inner = createInner("int", 1);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        assertInvalidMessage("IN-subqueries are not enabled",
                             "SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + ")");
    }

    @Test
    public void testPlainInStillWorksWithFlagOff() throws Throwable
    {
        CQL_SUBQUERY_ENABLED.reset();

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (pk, v) VALUES (1, 'a')");
        execute("INSERT INTO %s (pk, v) VALUES (2, 'b')");

        // A plain IN value list must behave exactly as before when the flag is off.
        assertRowsIgnoringOrder(execute("SELECT pk, v FROM %s WHERE pk IN (1, 2)"),
                                row(1, "a"),
                                row(2, "b"));
    }

    // --- Outer-column rejections (H5) --------------------------------------------------------

    @Test
    public void testNonPartitionKeyColumnRejected() throws Throwable
    {
        String inner = createInner("int", 1);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");

        assertInvalidMessage("non-partition-key column",
                             "SELECT pk, v FROM %s WHERE v IN (SELECT k FROM " + inner + ") ALLOW FILTERING");
    }

    @Test
    public void testClusteringColumnRejected() throws Throwable
    {
        String inner = createInner("int", 1);
        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY (pk, c))");

        assertInvalidMessage("clustering column",
                             "SELECT pk, v FROM %s WHERE c IN (SELECT k FROM " + inner + ") ALLOW FILTERING");
    }

    @Test
    public void testCompositePartitionKeyComponentRejected() throws Throwable
    {
        String inner = createInner("int", 1);
        createTable("CREATE TABLE %s (pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2)))");

        assertInvalidMessage("composite",
                             "SELECT pk1, v FROM %s WHERE pk1 IN (SELECT k FROM " + inner + ") ALLOW FILTERING");
    }

    // --- Inner-shape rejections (H2, H3, TYPE, projection) ----------------------------------

    @Test
    public void testMultiColumnProjectionRejected() throws Throwable
    {
        String inner = qTable("CREATE TABLE %s (k int PRIMARY KEY, extra int)");
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        assertInvalidMessage("project exactly one column",
                             "SELECT pk, v FROM %s WHERE pk IN (SELECT k, extra FROM " + inner + ")");
    }

    @Test
    public void testTypeMismatchRejected() throws Throwable
    {
        String inner = createInner("text", "x");
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        assertInvalidMessage("Type mismatch in IN-subquery",
                             "SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + ")");
    }

    @Test
    public void testAggregatingInnerRejected() throws Throwable
    {
        String inner = createInner("int", 1, 2);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        assertInvalidMessage("aggregate functions",
                             "SELECT pk, v FROM %s WHERE pk IN (SELECT count(k) FROM " + inner + ")");
    }

    @Test
    public void testGroupByInnerRejected() throws Throwable
    {
        String inner = createInner("int", 1, 2);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        assertInvalidMessage("GROUP BY",
                             "SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + " GROUP BY k)");
    }

    @Test
    public void testDistinctInnerRejected() throws Throwable
    {
        String inner = createInner("int", 1, 2);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        assertInvalidMessage("DISTINCT",
                             "SELECT pk, v FROM %s WHERE pk IN (SELECT DISTINCT k FROM " + inner + ")");
    }

    @Test
    public void testNestedSubqueryRejected() throws Throwable
    {
        String innermost = createInner("int", 1);
        String middle = qTable("CREATE TABLE %s (k int PRIMARY KEY)");
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        assertInvalidMessage("Nested IN-subqueries are not supported",
                             "SELECT pk, v FROM %s WHERE pk IN " +
                             "(SELECT k FROM " + middle + " WHERE k IN (SELECT k FROM " + innermost + "))");
    }

    @Test
    public void testInnerBindMarkerRejected() throws Throwable
    {
        String inner = qTable("CREATE TABLE %s (k int PRIMARY KEY, flag int)");
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        assertInvalidMessage("bind markers",
                             "SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + " WHERE flag = ? ALLOW FILTERING)",
                             1);
    }

    // --- M1: NOT IN is not wired into the grammar for subqueries -----------------------------

    @Test
    public void testNotInSubqueryIsSyntaxError() throws Throwable
    {
        String inner = createInner("int", 1);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        assertInvalidThrow(SyntaxException.class,
                           "SELECT pk, v FROM %s WHERE pk NOT IN (SELECT k FROM " + inner + ")");
    }

    // H1 (oversized inner fails the guardrail) needs an ordinary-user ClientState, because guardrails
    // are excluded for internal and superuser queries.  See SelectSubqueryGuardrailTest.

    // --- SEMANTICS: null inner values are skipped --------------------------------------------

    @Test
    public void testNullInnerValuesSkipped() throws Throwable
    {
        // The inner projects a regular column that is null for some rows.  Null must not become an
        // IN entry.
        String inner = qTable("CREATE TABLE %s (k int PRIMARY KEY, proj int)");
        execute("INSERT INTO " + inner + " (k, proj) VALUES (1, 100)");
        execute("INSERT INTO " + inner + " (k) VALUES (2)");            // proj is null
        execute("INSERT INTO " + inner + " (k, proj) VALUES (3, 300)");

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (pk, v) VALUES (100, 'a')");
        execute("INSERT INTO %s (pk, v) VALUES (300, 'c')");

        assertRowsIgnoringOrder(execute("SELECT pk, v FROM %s WHERE pk IN " +
                                        "(SELECT proj FROM " + inner + ")"),
                                row(100, "a"),
                                row(300, "c"));
    }
}
