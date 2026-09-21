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
package org.apache.cassandra.cql3;

import org.junit.Test;

import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Guards the bare fragment parser entry points against trailing, unconsumed input.  The bare term and
 * comparatorType rules stop at the first complete construct.  Without an end-of-input check they accept
 * a dangling operator; for example, the term "1 +" returns 1 and drops the "+".  ANTLR 3 LL(*) rejected
 * this.  The parser must now consume the whole input and raise a {@link SyntaxException} on trailing
 * tokens.  Full query parsing already enforces this through the grammar rule that matches EOF.
 */
public class FragmentParserEofTest
{
    private static Term.Raw parseTerm(String cql)
    {
        return CQLFragmentParser.parseAny(p -> p.term().raw, cql, "CQL term");
    }

    private static CQL3Type.Raw parseType(String cql)
    {
        return CQLFragmentParser.parseAny(p -> p.comparatorType().t, cql, "CQL type");
    }

    @Test
    public void termWithTrailingOperatorFails()
    {
        try
        {
            parseTerm("1 +");
            fail("expected SyntaxException for a term with a trailing operator");
        }
        catch (SyntaxException e)
        {
            // expected: the trailing '+' must not be silently dropped
        }
    }

    @Test
    public void validTermParses()
    {
        assertNotNull(parseTerm("1 + 2"));
    }

    @Test
    public void typeWithTrailingOperatorFails()
    {
        try
        {
            parseType("int +");
            fail("expected SyntaxException for a type with trailing input");
        }
        catch (SyntaxException e)
        {
            // expected: the trailing '+' must not be silently dropped
        }
    }

    @Test
    public void validTypesParse()
    {
        assertNotNull(parseType("int"));
        assertNotNull(parseType("list<text>"));
    }

    @Test
    public void fullQueryStillParses()
    {
        CQLStatement.Raw stmt = CQLFragmentParser.parseAny(p -> p.query().stmnt,
                                                           "SELECT * FROM ks.tbl WHERE k = 1",
                                                           "statement");
        assertNotNull(stmt);
    }

    /**
     * Empty, whitespace-only, and comment-only input carries no token for the fragment rule.  The EOF
     * check and the grammar must reject each with a clean {@link SyntaxException}, not a surprise
     * exception type (for example a NullPointerException on an absent parse result).
     */
    @Test
    public void emptyAndCommentOnlyInputIsRejected()
    {
        for (String blank : new String[]{ "", "   ", "-- comment", "/* */" })
        {
            assertTermRejected(blank);
            assertTypeRejected(blank);
        }
    }

    private static void assertTermRejected(String cql)
    {
        try
        {
            parseTerm(cql);
            fail("expected a SyntaxException for term input [" + cql + ']');
        }
        catch (SyntaxException e)
        {
            // expected
        }
    }

    private static void assertTypeRejected(String cql)
    {
        try
        {
            parseType(cql);
            fail("expected a SyntaxException for type input [" + cql + ']');
        }
        catch (SyntaxException e)
        {
            // expected
        }
    }

    /**
     * The query path already matches a trailing EOF through the grammar.  Trailing garbage and a second
     * statement must both be rejected, guarding the reworked end-of-input logic on the query path.
     */
    @Test
    public void queryWithTrailingInputFails()
    {
        for (String cql : new String[]{ "SELECT * FROM t GARBAGE", "SELECT * FROM t; SELECT * FROM t" })
        {
            try
            {
                CQLFragmentParser.parseAny(p -> p.query().stmnt, cql, "statement");
                fail("expected a SyntaxException for [" + cql + ']');
            }
            catch (SyntaxException e)
            {
                // expected: the trailing input must not be silently dropped
            }
        }
    }

    /**
     * The other bare-fragment callers that gained the strict EOF check must still accept their normal
     * input.  WhereClause.parse and the CREATE TABLE fragment are the two in-tree examples.
     */
    @Test
    public void otherFragmentCallersStillParseValidInput()
    {
        assertNotNull(WhereClause.parse("k = 1 AND v > 2"));
        assertNotNull(CQLFragmentParser.parseAny(p -> p.createTableStatement().stmt,
                                                 "CREATE TABLE ks.t (id int PRIMARY KEY, v text)",
                                                 "CREATE TABLE"));
    }

    /**
     * A statement fragment such as createTableStatement does not consume a trailing ';'.  The query rule
     * matches "(';')* EOF", but a bare statement fragment does not.  CreateTableStatement.parse, and
     * AccordKeyspace at class init, pass CQL that ends in ';'.  The EOF check must tolerate the trailing
     * terminator, or every server-based test fails at schema init.  A dangling operator on a bare term
     * must still fail; see termWithTrailingOperatorFails.
     */
    @Test
    public void statementFragmentWithTrailingSemicolonParses()
    {
        // The exact shape that AccordKeyspace parses at class init: a CREATE TABLE that ends in ';'.
        assertNotNull(CQLFragmentParser.parseAny(p -> p.createTableStatement().stmt,
                                                 "CREATE TABLE ks.t (id int PRIMARY KEY, v text);",
                                                 "CREATE TABLE"));
        // More than one terminator matches "(';')*".
        assertNotNull(CQLFragmentParser.parseAny(p -> p.createTableStatement().stmt,
                                                 "CREATE TABLE ks.t (id int PRIMARY KEY, v text);;",
                                                 "CREATE TABLE"));
    }
}
