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

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Guards the CQL parse-time budget.  The budget caps the wall-clock cost of ANTLR 4 ALL(*) prediction,
 * so an adversarial input fails fast instead of using unbounded CPU.  A legitimate query is unaffected.
 * The budget is set with the {@code cassandra.cql.parse_time_budget_ms} property; 0 or less disables it.
 */
public class ParseTimeBudgetTest
{
    @After
    public void restoreBudget()
    {
        CassandraRelevantProperties.CQL_PARSE_TIME_BUDGET_MS.reset();
    }

    private static Term.Raw parseTerm(String cql)
    {
        return CQLFragmentParser.parseAny(p -> p.term().raw, cql, "CQL term");
    }

    // A long chain of additions: valid, but large enough that its prediction work far exceeds a 1 ms
    // budget on any machine.
    private static String bigAdditionChain(int operators)
    {
        StringBuilder sb = new StringBuilder("1");
        for (int i = 0; i < operators; i++)
            sb.append(" + 1");
        return sb.toString();
    }

    /**
     * A long chain of additions forces one prediction per operator.  With a 1 ms budget the parse
     * trips the cap well before it consumes the whole input; it does not run to the end, so the test
     * stays fast.  The huge input only guarantees there is more prediction work left after the deadline
     * passes, on any machine.  This is deterministic: it uses no sleep and no fixed timing assumption.
     */
    @Test
    public void adversarialInputTripsBudget()
    {
        CassandraRelevantProperties.CQL_PARSE_TIME_BUDGET_MS.setLong(1);

        try
        {
            parseTerm(bigAdditionChain(200_000));
            fail("expected the parse-time budget to trip");
        }
        catch (SyntaxException e)
        {
            // The message names the configured cap, e.g. "... of 1 ms".
            assertTrue("expected a parse-time budget message, got: " + e.getMessage(),
                       e.getMessage().contains("parse-time budget of 1 ms"));
        }
    }

    /**
     * Proves the shared deadline bounds BOTH passes.  A leading ')' is an immediate parse error, so the
     * SLL fast pass bails at the first token, far under the 1 ms budget; it cannot itself trip the cap.
     * The LL fallback then recovers past the ')' and does full-context prediction over the huge tail,
     * which trips the shared deadline.  So a budget trip on this input can only come from the LL pass.
     */
    @Test
    public void llPassHonorsSharedDeadline()
    {
        CassandraRelevantProperties.CQL_PARSE_TIME_BUDGET_MS.setLong(1);

        try
        {
            parseTerm(") " + bigAdditionChain(200_000));
            fail("expected the parse-time budget to trip during the LL pass");
        }
        catch (SyntaxException e)
        {
            assertTrue("expected a parse-time budget message, got: " + e.getMessage(),
                       e.getMessage().contains("parse-time budget"));
        }
    }

    /**
     * Proves the deadline also guards the lexer, not just parser prediction.  A huge run of line
     * comments before the first real token is pure tokenizing cost; the parser predicts nothing until
     * that token arrives.  With a 1 ms budget the lexer deadline trips while skipping the comments.
     */
    @Test
    public void lexerCostTripsBudget()
    {
        CassandraRelevantProperties.CQL_PARSE_TIME_BUDGET_MS.setLong(1);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 500_000; i++)
            sb.append("-- c\n");
        sb.append('1');

        try
        {
            parseTerm(sb.toString());
            fail("expected the parse-time budget to trip while lexing");
        }
        catch (SyntaxException e)
        {
            assertTrue("expected a parse-time budget message, got: " + e.getMessage(),
                       e.getMessage().contains("parse-time budget"));
        }
    }

    /**
     * A normal query parses fine under the generous default budget.
     */
    @Test
    public void normalTermParsesUnderDefaultBudget()
    {
        assertNotNull(parseTerm("1 + 2"));
    }

    /**
     * A budget of 0 disables the cap, so the same adversarial input parses to completion.
     */
    @Test
    public void zeroBudgetDisablesCap()
    {
        CassandraRelevantProperties.CQL_PARSE_TIME_BUDGET_MS.setLong(0);
        assertNotNull(parseTerm(bigAdditionChain(2_000)));
    }

    /**
     * A negative budget also disables the cap, so the same adversarial input parses to completion.
     */
    @Test
    public void negativeBudgetDisablesCap()
    {
        CassandraRelevantProperties.CQL_PARSE_TIME_BUDGET_MS.setLong(-1);
        assertNotNull(parseTerm(bigAdditionChain(2_000)));
    }
}
