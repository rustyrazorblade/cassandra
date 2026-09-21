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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLFragmentParser.CQLParserFunction;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Differential guard for the ANTLR 3 -> 4 migration.
 *
 * <p>The golden fixtures under {@code differential/} were recorded from the ANTLR 3 parser (the
 * pre-migration baseline in the {@code jdk25-upgrade} worktree).  This test parses the same CQL
 * corpus with the ANTLR 4 parser and asserts the built domain objects serialize to the same string.
 * A mismatch means the migration changed the parser's output; that is a regression, not an expected
 * difference.</p>
 *
 * <p>{@link CqlParseTreeDump} produces the canonical string.  The same source file recorded the
 * goldens and runs here, so the comparison isolates the parser.</p>
 */
public class CqlDifferentialTest
{
    private static final Path DIR = Paths.get("test/unit/org/apache/cassandra/cql3/differential");

    @Test
    public void corpusMatchesBaseline() throws Exception
    {
        Path corpus = DIR.resolve("corpus.tsv");
        assertTrue("missing corpus: " + corpus.toAbsolutePath(), Files.exists(corpus));

        List<String> failures = new ArrayList<>();
        int checked = 0;

        for (String line : Files.readAllLines(corpus, StandardCharsets.UTF_8))
        {
            if (line.isBlank())
                continue;

            String[] parts = line.split("\t", 3);
            String name = parts[0];
            String entry = parts[1];
            String cql = parts[2];

            Object parsed = CQLFragmentParser.parseAny(functionFor(entry), cql, name);
            String actual = CqlParseTreeDump.dump(parsed);

            Path goldenPath = DIR.resolve(name + ".golden");
            if (!Files.exists(goldenPath))
            {
                failures.add(name + ": no golden fixture at " + goldenPath.toAbsolutePath());
                continue;
            }

            String golden = new String(Files.readAllBytes(goldenPath), StandardCharsets.UTF_8);
            if (!golden.equals(actual))
                failures.add(name + ":\n  expected: " + golden + "\n  actual:   " + actual);

            checked++;
        }

        assertTrue("corpus is empty", checked > 0);
        if (!failures.isEmpty())
            fail("ANTLR 4 parser output differs from the ANTLR 3 baseline for "
                 + failures.size() + " corpus entries:\n" + String.join("\n", failures));
    }

    /**
     * Error-path differential.  A malformed input must throw {@link SyntaxException} reporting the
     * FIRST error, then stop, with no token deletion or insertion.  This is the ANTLR 3 recovery
     * behavior that {@link CqlErrorStrategy} restores under ANTLR 4.  The expected wording is
     * recorded from the ANTLR 3 baseline (the {@code jdk25-upgrade} worktree).
     *
     * <p>For an error at end of input, the ANTLR 3 EOF token reported the sentinel position
     * {@code line 0:-1}; ANTLR 4 reports the true end-of-input position (for example
     * {@code line 1:13}).  Those cases assert the error clause, not the leading position: the
     * position change is an intentional, more accurate divergence (see the branch notes).  The
     * error clause itself, and the report-first-error-then-stop behavior, match the baseline.</p>
     */
    @Test
    public void errorPathMatchesBaseline()
    {
        // Not at end of input: the full message matches the ANTLR 3 baseline exactly, snippet included.
        assertSyntaxError("SELECT FROM t", "query",
                          "no viable alternative at input 'FROM' (SELECT [FROM]...)");

        // At end of input: assert the error clause; the leading position differs by design (see above).
        assertSyntaxError("SELECT * FROM", "query", "no viable alternative at input '<EOF>'");
        assertSyntaxError("UPDATE t SET", "query", "no viable alternative at input '<EOF>'");
        assertSyntaxError("INSERT INTO t (a) VALUES", "query", "mismatched input '<EOF>' expecting '('");
    }

    private static void assertSyntaxError(String cql, String rule, String expectedClause)
    {
        try
        {
            CQLFragmentParser.parseAny(functionFor(rule), cql, "err");
            fail("expected a SyntaxException for malformed input: " + cql);
        }
        catch (SyntaxException e)
        {
            assertTrue("error for [" + cql + "] should contain \"" + expectedClause
                       + "\" but was: " + e.getMessage(),
                       e.getMessage().contains(expectedClause));
        }
    }

    /**
     * Intentional v4-only acceptance.  The migrated {@code COMMENT} lexer rule makes the trailing
     * line terminator optional, so a single-line comment at end of input (no trailing newline) is
     * now accepted.  The ANTLR 3 lexer rejected it ("mismatched character '&lt;EOF&gt;'").  This is
     * a deliberate divergence recorded in the branch notes; it has no ANTLR 3 golden.
     */
    @Test
    public void commentAtEndOfInputWithoutNewlineIsAccepted()
    {
        assertNotNull(CQLFragmentParser.parseAny(p -> p.query().stmnt,
                                                 "SELECT * FROM t WHERE k = 1 -- trailing comment",
                                                 "comment_eof_dash"));
        assertNotNull(CQLFragmentParser.parseAny(p -> p.query().stmnt,
                                                 "SELECT * FROM t WHERE k = 1 // trailing comment",
                                                 "comment_eof_slash"));
    }

    private static CQLParserFunction<?> functionFor(String entry)
    {
        switch (entry)
        {
            // ANTLR 4 rules return a *Context.  Unwrap to the same domain object the ANTLR 3 rules
            // returned, so the dump matches the baseline and never walks ANTLR parse-tree internals.
            case "query":          return p -> p.query().stmnt;
            case "term":           return p -> p.term().raw;
            case "comparatorType": return p -> p.comparatorType().t;
            default: throw new IllegalArgumentException("unknown corpus entry rule: " + entry);
        }
    }
}
