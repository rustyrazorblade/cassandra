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

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.Test;

import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.Types;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Guards the opt-in fail-fast parse mode.  The default LL fallback recovers and reports the first
 * error with ANTLR 3 wording; the fail-fast fallback aborts on the first error with no recovery.
 * The CQL type fragment ({@code comparatorType}, via {@link CQLTypeParser}) uses fail-fast because a
 * malformed type string has no useful multi-error recovery.  The full query path keeps the default
 * recover-and-report behavior, so its error wording does not change.
 */
public class FragmentFailFastTest
{
    // A malformed CQL type: the closing '>' is missing.  It fails both parse passes.
    private static final String MALFORMED_TYPE = "map<int";

    /**
     * The fail-fast LL pass installs no ErrorCollector, so BailErrorStrategy aborts the parse with a
     * ParseCancellationException.  The default pass, in contrast, throws a SyntaxException from the
     * ErrorCollector.  A ParseCancellationException here proves the bail strategy replaced recovery.
     */
    @Test
    public void failFastLlPassBailsWithoutRecovery()
    {
        try
        {
            CQLFragmentParser.parseAnyUnhandled(p -> p.comparatorType().t, MALFORMED_TYPE, true);
            fail("expected the fail-fast LL pass to bail");
        }
        catch (ParseCancellationException e)
        {
            // expected: BailErrorStrategy aborted on the first error, no recovery ran
        }
    }

    /**
     * The default LL pass recovers and reports through the ErrorCollector, so the same malformed input
     * surfaces a SyntaxException rather than a ParseCancellationException.
     */
    @Test
    public void defaultLlPassReportsSyntaxException()
    {
        try
        {
            CQLFragmentParser.parseAnyUnhandled(p -> p.comparatorType().t, MALFORMED_TYPE, false);
            fail("expected the default LL pass to report a SyntaxException");
        }
        catch (SyntaxException e)
        {
            // expected: the ErrorCollector path reports the first error
        }
    }

    /**
     * The public fail-fast entry point turns the bail into a clear SyntaxException for the caller.
     */
    @Test
    public void parseAnyFailFastGivesCleanSyntaxException()
    {
        try
        {
            CQLFragmentParser.parseAnyFailFast(p -> p.comparatorType().t, MALFORMED_TYPE, "CQL type");
            fail("expected a SyntaxException for a malformed type");
        }
        catch (SyntaxException e)
        {
            assertTrue("expected an \"Invalid or malformed CQL type\" message, got: " + e.getMessage(),
                       e.getMessage().contains("Invalid or malformed CQL type"));
        }
    }

    /**
     * A valid type still parses under fail-fast: it takes the SLL fast path, so the LL fallback and its
     * bail strategy never run.
     */
    @Test
    public void validTypeParsesUnderFailFast()
    {
        assertNotNull(CQLFragmentParser.parseAnyFailFast(p -> p.comparatorType().t, "list<text>", "CQL type"));
    }

    /**
     * CQLTypeParser uses the fail-fast path, so a malformed type is rejected with a SyntaxException and
     * a valid type still parses.
     */
    @Test
    public void cqlTypeParserRejectsMalformedType()
    {
        try
        {
            CQLTypeParser.parse("ks", MALFORMED_TYPE, Types.none());
            fail("expected a SyntaxException for a malformed type");
        }
        catch (SyntaxException e)
        {
            // expected
        }

        assertNotNull(CQLTypeParser.parse("ks", "list<text>", Types.none()));
    }

    /**
     * The default full query path is unchanged.  A malformed query still reports the ANTLR 3 wording
     * through the recover-and-report path, not a fail-fast bail.
     */
    @Test
    public void defaultQueryPathStillReports()
    {
        try
        {
            CQLFragmentParser.parseAny(p -> p.query().stmnt, "SELECT FROM t", "query");
            fail("expected a SyntaxException for a malformed query");
        }
        catch (SyntaxException e)
        {
            assertTrue("expected the ANTLR 3 wording, got: " + e.getMessage(),
                       e.getMessage().contains("no viable alternative at input 'FROM'"));
        }
    }
}
