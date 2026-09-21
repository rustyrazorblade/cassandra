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

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.WritableToken;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Guards the keyword reclassifier in the lexer.  The lexer no longer has a rule per keyword; it
 * lexes an IDENT and {@code nextToken()} rewrites the token type with {@code setType}.  That call
 * only works when the token factory produces a {@link WritableToken}.  The default
 * {@code CommonTokenFactory} does, but a later change to the factory could silently turn
 * reclassification into a no-op, making every keyword parse as a bare identifier.  Assert the
 * invariant, plus the two behaviors the reclassifier must preserve: case-insensitive keyword
 * matching and leaving a non-keyword identifier as IDENT.
 */
public class LexerKeywordClassifierTest
{
    private static List<Token> lex(String cql)
    {
        CqlLexer lexer = new CqlLexer(CharStreams.fromString(cql));
        lexer.removeErrorListeners();
        List<Token> out = new ArrayList<>();
        for (Token t = lexer.nextToken(); t.getType() != Token.EOF; t = lexer.nextToken())
            out.add(t);
        return out;
    }

    @Test
    public void tokenFactoryProducesWritableTokens()
    {
        // If a token is not a WritableToken, setType() cannot run and reclassification no-ops.
        for (Token t : lex("select from where"))
            assertTrue("token is not writable, reclassification would no-op: " + t,
                       t instanceof WritableToken);
    }

    @Test
    public void keywordsAreReclassifiedCaseInsensitively()
    {
        for (String s : new String[]{ "select", "SELECT", "Select", "sElEcT" })
        {
            List<Token> toks = lex(s);
            assertEquals("expected a single token for [" + s + "]", 1, toks.size());
            assertEquals("expected K_SELECT for [" + s + "]",
                         CqlParser.K_SELECT, toks.get(0).getType());
        }
    }

    @Test
    public void nonKeywordStaysIdent()
    {
        List<Token> toks = lex("notakeyword_123");
        assertEquals(1, toks.size());
        assertEquals(CqlParser.IDENT, toks.get(0).getType());
    }

    /**
     * Guards the FLOAT lexer rule against the three-dot regression.  The FLOAT rule consumes a
     * decimal point after an INTEGER, but it must not steal the first dot of a following RANGE
     * ({@code ..}).  The predicate allows the dot only when the next character is not a dot, or
     * when a third dot follows.  A third dot means the first dot belongs to the float, so
     * {@code 0...3.} splits as FLOAT '0.', RANGE '..', FLOAT '3.'.
     */
    @Test
    public void floatDoesNotStealRangeDot()
    {
        List<Token> toks = lex("0...3.");
        assertEquals("expected FLOAT '0.', RANGE '..', FLOAT '3.'", 3, toks.size());

        assertEquals(CqlParser.FLOAT, toks.get(0).getType());
        assertEquals("0.", toks.get(0).getText());

        assertEquals(CqlParser.RANGE, toks.get(1).getType());
        assertEquals("..", toks.get(1).getText());

        assertEquals(CqlParser.FLOAT, toks.get(2).getType());
        assertEquals("3.", toks.get(2).getText());
    }

    @Test
    public void bareTrailingDotIsFloat()
    {
        List<Token> toks = lex("3.");
        assertEquals(1, toks.size());
        assertEquals(CqlParser.FLOAT, toks.get(0).getType());
        assertEquals("3.", toks.get(0).getText());
    }

    @Test
    public void integerRangeIntegerStaysSplit()
    {
        // Two dots between integers is a RANGE, never a float; the integers keep their type.
        List<Token> toks = lex("1..3");
        assertEquals(3, toks.size());

        assertEquals(CqlParser.INTEGER, toks.get(0).getType());
        assertEquals("1", toks.get(0).getText());

        assertEquals(CqlParser.RANGE, toks.get(1).getType());
        assertEquals("..", toks.get(1).getText());

        assertEquals(CqlParser.INTEGER, toks.get(2).getType());
        assertEquals("3", toks.get(2).getText());
    }
}
