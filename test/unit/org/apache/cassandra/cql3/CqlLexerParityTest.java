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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Locks the token stream the CQL lexer produces so a grammar restructuring cannot change the
 * parser-visible tokens.  For every string in {@link #CORPUS} it tokenizes with {@link CqlLexer}
 * and records, per token, the symbolic type name ({@code CqlParser.VOCABULARY.getSymbolicName})
 * and the token text.  The recorded stream is compared against a committed golden file.
 *
 * <p>The golden was generated from the unmodified (pre-change) lexer.  The {@link Test} path only
 * READS and COMPARES: a missing golden file is a hard failure, never a silent write-and-pass.  A
 * change that alters any token type or text breaks this test.</p>
 *
 * <p>To (re)generate the golden, run the {@link #main} generator against the pre-change lexer on
 * the compiled classpath (JDK 21):</p>
 * <pre>
 * java -cp "build/test/classes:build/classes/main:conf:test/conf:$(echo build/test/lib/jars/*.jar lib/*.jar | tr ' ' ':')" \
 *      org.apache.cassandra.cql3.CqlLexerParityTest
 * </pre>
 * <p>It writes {@code test/data/cql-lexer-parity/golden.txt}.  The committed golden must be the
 * output of the pre-change lexer over the full corpus, so the post-change lexer reproducing it
 * byte-for-byte is the parity proof.</p>
 */
public class CqlLexerParityTest
{
    private static final Path GOLDEN =
        Paths.get("test", "data", "cql-lexer-parity", "golden.txt");

    /**
     * (a) broad valid statements exercising every literal kind, then (b) every edge token the
     * grammar restructuring can affect.  Each entry is tokenized in isolation.
     */
    private static final String[] CORPUS = new String[]
    {
        // --- (a) broad valid statements ---
        "SELECT id, name FROM ks.users WHERE id = 123",
        "SELECT id, name FROM ks.users WHERE id IN (1, 2, 3, 4, 5) ORDER BY name ASC LIMIT 100",
        "INSERT INTO ks.users (id, name, email) VALUES (?, ?, ?)",
        "INSERT INTO ks.t (a, b, c, d) VALUES (1, 'x', true, 1.5)",
        "UPDATE ks.users SET name = 'alice', email = 'a@example.com' WHERE id = 42",
        "UPDATE ks.t SET v = 1.5e-2 WHERE k = 0xDEADBEEF",
        "DELETE FROM ks.users WHERE id = 42",
        "CREATE TABLE ks.users (id int PRIMARY KEY, name text, email text, created timestamp)",
        "CREATE TABLE ks.t (id uuid PRIMARY KEY, d duration, f float, m map<text, int>, l list<int>)",
        "BEGIN BATCH INSERT INTO ks.users (id, name) VALUES (1, 'a'); "
            + "UPDATE ks.users SET name = 'b' WHERE id = 2; APPLY BATCH",
        "SELECT * FROM t WHERE d = 1y2mo3d AND ts = 123e4567-e89b-12d3-a456-426614174000",
        "SELECT \"quoted col\", \"a\"\"b\" FROM t WHERE flag = false",
        "INSERT INTO t (k, v) VALUES (1, $$raw '' text$$)",
        "SELECT CAST(v AS int) FROM t WHERE k = -1 AND j = -1.5",
        "SELECT id FROM t WHERE token(k) > -nan AND m = infinity",

        // --- (b) edge tokens ---
        // Floats / range / dot
        "1..2",
        "1.2",
        "1.",
        ".5",
        "0...3.",
        "1e3",
        "1E+3",
        "1.5e-2",
        "1.e3",
        "1.5.5",
        // Durations, digit-leading
        "1y2mo3d",
        "1y",
        "2mo",
        "500ms",
        "10us",
        "1ns",
        "-1y",
        "1µs",
        // Durations, ISO 8601 'P'
        "P1Y",
        "P1Y2M3D",
        "P1W",
        "PT1H",
        "PT1H30M",
        "P1YT1H",
        "-P1Y",
        "P2023-01-02T03:04:05",
        // Negative and positive NaN / Infinity
        "-nan",
        "-NaN",
        "-infinity",
        "-INFINITY",
        "nan",
        "infinity",
        // UUIDs
        "123e4567-e89b-12d3-a456-426614174000",
        "deadbeef-dead-beef-dead-beefdeadbeef",
        "123e4567-e89b-12d3-a456-42661417400",
        // Hexnumbers
        "0x",
        "0xDEADBEEF",
        "0Xabc",
        "0xZZ",
        // Booleans
        "true",
        "TRUE",
        "False",
        "\"true\"",
        // Quoted names
        "\"\"",
        "\"a\"",
        "\"a\"\"b\"",
        "\"\"\"\"",
        // Malformed quoted names.  The QUOTED rule's inner quantifier changed from + to *,
        // so these exercise its error-recovery path (the lexer runs with removeErrorListeners).
        "\"abc",       // unterminated: opening quote, no closing quote
        "\"\"\"",      // odd count of double-quotes (three)
        "\"",          // a lone double-quote
        // String literals
        "''",
        "'a''b'",
        "$$x$$",
        "$$ '' $$",
        // Hex-looking identifiers
        "abc",
        "a1",
        "f00d",
        "e10",
    };

    @Test
    public void tokenStreamMatchesGolden() throws IOException
    {
        if (!Files.exists(GOLDEN))
            fail("Golden file is missing: " + GOLDEN.toAbsolutePath()
                 + ".  Regenerate it by running CqlLexerParityTest.main against the pre-change "
                 + "lexer (see the class comment for the exact classpath command).  The test path "
                 + "never writes the golden.");

        String actual = render();
        String expected = new String(Files.readAllBytes(GOLDEN), StandardCharsets.UTF_8);
        assertEquals("CQL lexer token stream diverged from the committed golden", expected, actual);
    }

    /**
     * Explicit regeneration entry point.  Writes the golden from the lexer on the current
     * classpath.  This is the ONLY code path that writes the golden; the {@link Test} above only
     * reads and compares.  See the class comment for the classpath command to invoke this.
     */
    public static void main(String[] args) throws IOException
    {
        String actual = render();
        Files.createDirectories(GOLDEN.getParent());
        Files.write(GOLDEN, actual.getBytes(StandardCharsets.UTF_8));
        System.out.println("Wrote CQL lexer parity golden to " + GOLDEN.toAbsolutePath());
    }

    private static String render()
    {
        StringBuilder sb = new StringBuilder();
        for (String cql : CORPUS)
        {
            sb.append("# CQL: ").append(escape(cql)).append('\n');
            for (Token t : lex(cql))
            {
                String name = CqlParser.VOCABULARY.getSymbolicName(t.getType());
                if (name == null)
                    name = CqlParser.VOCABULARY.getDisplayName(t.getType());
                sb.append(name).append('\t').append(escape(t.getText())).append('\n');
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static List<Token> lex(String cql)
    {
        CqlLexer lexer = new CqlLexer(CharStreams.fromString(cql));
        lexer.removeErrorListeners();
        List<Token> out = new ArrayList<>();
        for (Token t = lexer.nextToken(); t.getType() != Token.EOF; t = lexer.nextToken())
            out.add(t);
        return out;
    }

    private static String escape(String s)
    {
        if (s == null)
            return "<null>";
        StringBuilder b = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++)
        {
            char c = s.charAt(i);
            switch (c)
            {
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:   b.append(c);
            }
        }
        return b.toString();
    }
}
