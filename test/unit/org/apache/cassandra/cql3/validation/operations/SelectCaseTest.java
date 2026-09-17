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

import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_CASE_EXPRESSION_ENABLED;

/**
 * Tests for CASE expressions in the SELECT projection list. The feature is behind the
 * {@code cassandra.cql.case_expression.enabled} flag.
 */
public class SelectCaseTest extends CQLTester
{
    @Before
    public void enableCase()
    {
        CQL_CASE_EXPRESSION_ENABLED.setBoolean(true);
    }

    @After
    public void resetCase()
    {
        CQL_CASE_EXPRESSION_ENABLED.reset();
    }

    // --- Correctness: simple and searched forms ---------------------------------------------

    @Test
    public void testSimpleForm() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        execute("INSERT INTO %s (k, v) VALUES (2, 2)");
        execute("INSERT INTO %s (k, v) VALUES (3, 9)");

        assertRowsIgnoringOrder(execute("SELECT k, CASE v WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END FROM %s"),
                                row(1, "one"),
                                row(2, "two"),
                                row(3, "many"));
    }

    @Test
    public void testSearchedForm() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 5)");
        execute("INSERT INTO %s (k, v) VALUES (2, 50)");

        assertRowsIgnoringOrder(execute("SELECT k, CASE WHEN v > 10 THEN 'big' ELSE 'small' END FROM %s"),
                                row(1, "small"),
                                row(2, "big"));
    }

    @Test
    public void testElseAbsentYieldsNull() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        execute("INSERT INTO %s (k, v) VALUES (2, 7)");

        assertRowsIgnoringOrder(execute("SELECT k, CASE v WHEN 1 THEN 'one' END FROM %s"),
                                row(1, "one"),
                                row(2, (String) null));
    }

    @Test
    public void testNullOperandDoesNotMatch() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, null)");

        // A null operand is not equal to any value; the ELSE branch is used.
        assertRows(execute("SELECT CASE v WHEN 1 THEN 'one' ELSE 'other' END FROM %s WHERE k = 1"),
                   row("other"));
        // No ELSE and no match -> null.
        assertRows(execute("SELECT CASE v WHEN 1 THEN 'one' END FROM %s WHERE k = 1"),
                   row((String) null));
    }

    @Test
    public void testAllSixComparisonOperators() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 5)");

        assertRows(execute("SELECT CASE WHEN v = 5 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));
        assertRows(execute("SELECT CASE WHEN v != 6 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));
        assertRows(execute("SELECT CASE WHEN v < 6 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));
        assertRows(execute("SELECT CASE WHEN v <= 5 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));
        assertRows(execute("SELECT CASE WHEN v > 4 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));
        assertRows(execute("SELECT CASE WHEN v >= 5 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));
    }

    @Test
    public void testOperatorBoundaries() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 5)");

        // At the boundary value the strict operators take the ELSE branch; the inclusive ones do not.
        assertRows(execute("SELECT CASE WHEN v < 5 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("n"));
        assertRows(execute("SELECT CASE WHEN v <= 5 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));
        assertRows(execute("SELECT CASE WHEN v > 5 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("n"));
        assertRows(execute("SELECT CASE WHEN v >= 5 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));

        // Off by one below the value distinguishes the strict operators the other way.
        assertRows(execute("SELECT CASE WHEN v < 6 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));
        assertRows(execute("SELECT CASE WHEN v > 4 THEN 'y' ELSE 'n' END FROM %s WHERE k = 1"), row("y"));
    }

    @Test
    public void testBindMarkers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 5)");

        // Marker as the WHEN value in a simple form; the operand column anchors its type.
        assertRows(execute("SELECT CASE v WHEN ? THEN 'hit' ELSE 'miss' END FROM %s WHERE k = 1", 5),
                   row("hit"));
        assertRows(execute("SELECT CASE v WHEN ? THEN 'hit' ELSE 'miss' END FROM %s WHERE k = 1", 6),
                   row("miss"));

        // Marker as a THEN result; the ELSE literal anchors the result type.
        assertRows(execute("SELECT CASE WHEN v = 5 THEN ? ELSE 'x' END FROM %s WHERE k = 1", "y"),
                   row("y"));
    }

    @Test
    public void testUntypedBindMarkerComparisonRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        // Neither comparison operand carries a type, so the comparison type cannot be inferred.
        assertInvalidMessage("Cannot infer the type of a CASE WHEN comparison",
                             "SELECT CASE WHEN ? > ? THEN 1 ELSE 2 END FROM %s");
    }

    @Test
    public void testNestedCase() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 15)");
        execute("INSERT INTO %s (k, v) VALUES (2, 3)");
        execute("INSERT INTO %s (k, v) VALUES (3, -1)");

        String query = "SELECT k, CASE WHEN v > 0 THEN CASE WHEN v > 10 THEN 'big' ELSE 'small' END ELSE 'neg' END FROM %s";
        assertRowsIgnoringOrder(execute(query),
                                row(1, "big"),
                                row(2, "small"),
                                row(3, "neg"));
    }

    @Test
    public void testCaseOverArithmetic() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 4)");
        execute("INSERT INTO %s (k, v) VALUES (2, 6)");

        assertRowsIgnoringOrder(execute("SELECT k, CASE WHEN v + 1 > 6 THEN 'hi' ELSE 'lo' END FROM %s"),
                                row(1, "lo"),
                                row(2, "hi"));
    }

    @Test
    public void testCaseOverScalarFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 7)");

        // blobasint(intasblob(v)) round-trips to v, so the condition is always true.
        assertRows(execute("SELECT CASE WHEN blobasint(intasblob(v)) = v THEN 'ok' ELSE 'no' END FROM %s WHERE k = 1"),
                   row("ok"));
    }

    @Test
    public void testCaseAsAggregateArgument() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 3)");
        execute("INSERT INTO %s (k, v) VALUES (2, 8)");
        execute("INSERT INTO %s (k, v) VALUES (3, 20)");

        // max over (v when v>5 else 0) = 20
        assertRows(execute("SELECT max(CASE WHEN v > 5 THEN v ELSE 0 END) FROM %s"),
                   row(20));
    }

    // --- Regression guards (C2) --------------------------------------------------------------

    @Test
    public void testDecimalEqualityUsesCompareForCQL() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, amount decimal)");
        // Stored with a different scale than the literal in the query.
        execute("INSERT INTO %s (k, amount) VALUES (1, 1.00)");

        // 1.00 and 1.0 are numerically equal for decimal; raw byte equality would miss this.
        assertRows(execute("SELECT CASE amount WHEN 1.0 THEN 'a' ELSE 'b' END FROM %s WHERE k = 1"),
                   row("a"));
    }

    @Test
    public void testReversedClusteringOperandComparesCorrectly() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 5, 100)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 3, 200)");

        // c is a reversed clustering column; comparison must ignore the reversed ordering.
        assertRows(execute("SELECT c, CASE c WHEN 5 THEN 'five' ELSE 'other' END FROM %s WHERE k = 1"),
                   row(5, "five"),
                   row(3, "other"));
    }

    @Test
    public void testReversedClusteringOrderingOperatorStripsReversedType() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 5, 100)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 3, 200)");

        // A searched form with an ORDERING operator on a DESC column. compareForCQL must strip the
        // ReversedType so the result follows natural value order: 5 > 4 -> 'hi', 3 > 4 is false -> 'lo'.
        assertRows(execute("SELECT c, CASE WHEN c > 4 THEN 'hi' ELSE 'lo' END FROM %s WHERE k = 1"),
                   row(5, "hi"),
                   row(3, "lo"));
    }

    // --- Fetch fan-out (C3) ------------------------------------------------------------------

    @Test
    public void testFetchFanOut() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, d int)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 1, 100, 200)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 9, 300, 400)");

        // b, c and d appear only inside the CASE; they must still be fetched.
        assertRowsIgnoringOrder(execute("SELECT a, CASE WHEN b = 1 THEN c ELSE d END FROM %s"),
                                row(1, 100),
                                row(2, 400));
    }

    // --- Type-mismatch rejection (H1) --------------------------------------------------------

    @Test
    public void testMismatchedResultTypesRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidMessage("must have the same type",
                             "SELECT CASE WHEN v > 0 THEN 1 ELSE 'x' END FROM %s");
    }

    @Test
    public void testUntypedAnchorRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidMessage("Cannot infer the result type",
                             "SELECT CASE WHEN v > 0 THEN null ELSE null END FROM %s");
    }

    @Test
    public void testMismatchedColumnComparisonRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int, b bigint)");
        // Two columns of different fixed widths. Neither coerces, so the comparison must be rejected
        // rather than silently reading the wrong number of bytes.
        assertInvalidMessage("Cannot compare CASE WHEN operands of types",
                             "SELECT CASE WHEN i > b THEN 'a' ELSE 'b' END FROM %s");
    }

    @Test
    public void testUntypedComparisonRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        // Both comparison operands are untyped, so the comparison type cannot be inferred.
        // This is distinct from the result-type inference failure covered above.
        assertInvalidMessage("Cannot infer the type of a CASE WHEN comparison",
                             "SELECT CASE WHEN null = null THEN 1 ELSE 2 END FROM %s");
    }

    // --- GROUP BY rejection (H2) -------------------------------------------------------------

    @Test
    public void testCaseInGroupByRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidMessage("CASE expressions are not supported in the GROUP BY clause",
                             "SELECT k, count(v) FROM %s GROUP BY CASE k WHEN 1 THEN 1 ELSE 2 END");
    }

    // --- Loud failure and compatibility ------------------------------------------------------

    @Test
    public void testDisabledByDefault() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        CQL_CASE_EXPRESSION_ENABLED.setBoolean(false);
        assertInvalidMessage("CASE expressions are not enabled",
                             "SELECT CASE v WHEN 1 THEN 'one' ELSE 'x' END FROM %s");
    }

    @Test
    public void testGrammarNonsenseFormsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        // Operand together with a comparison in a WHEN clause.
        assertInvalidMessage("cannot use a comparison",
                             "SELECT CASE v WHEN v = 1 THEN 'a' ELSE 'b' END FROM %s");
        // Searched CASE with a bare non-comparison selector in a WHEN clause.
        assertInvalidMessage("requires a comparison",
                             "SELECT CASE WHEN v THEN 'a' ELSE 'b' END FROM %s");
    }

    @Test
    public void testUnionAllIsSyntaxError() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        // The parse fails at the UNION token, before the second table name is read.
        assertInvalidThrow(SyntaxException.class,
                           "SELECT v FROM %s UNION ALL SELECT v FROM other");
    }

    // --- Backward-compat identifiers (M1) ----------------------------------------------------

    @Test
    public void testKeywordsUsableAsIdentifiersFlagOn() throws Throwable
    {
        runIdentifierCompat();
    }

    @Test
    public void testKeywordsUsableAsIdentifiersFlagOff() throws Throwable
    {
        CQL_CASE_EXPRESSION_ENABLED.setBoolean(false);
        runIdentifierCompat();
    }

    private void runIdentifierCompat() throws Throwable
    {
        // case, when and else are unreserved, so they remain valid UNQUOTED column identifiers.
        // Quoting would pass even for reserved words, so the unquoted forms are what prove compatibility.
        createTable("CREATE TABLE %s (k int PRIMARY KEY, case int, when int, else int)");
        execute("INSERT INTO %s (k, case, when, else) VALUES (1, 10, 20, 30)");

        assertRows(execute("SELECT case, when, else FROM %s WHERE k = 1"),
                   row(10, 20, 30));

        // usable as an unquoted alias
        assertRows(execute("SELECT k AS case FROM %s WHERE k = 1"),
                   row(1));

        // usable as an unquoted UDT field name accessed with dot syntax
        String udt = createType("CREATE TYPE %s (case int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, u frozen<" + udt + ">)");
        execute("INSERT INTO %s (k, u) VALUES (1, {case: 42})");
        assertRows(execute("SELECT u.case FROM %s WHERE k = 1"),
                   row(42));
    }
}
