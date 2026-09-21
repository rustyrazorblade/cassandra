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

import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.tree.AstBuilder;
import org.apache.cassandra.cql3.tree.AstLowering;
import org.apache.cassandra.cql3.tree.SelectAst;
import org.apache.cassandra.cql3.tree.Statement;
import org.apache.cassandra.cql3.tree.UnsupportedAstException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Build smoke and equivalence-by-construction check for the AST parser path (phase 3).
 *
 * <p>For each SELECT corpus entry, asserts that AstBuilder.build runs to completion and that
 * the direct parse and the AST-lowered parse produce byte-identical domain objects under
 * {@link CqlParseTreeDump}. The equivalence holds by construction: AstLowering returns the
 * carried RawStatement unchanged, so the dump equivalence verifies the build succeeds and the
 * equivalence-by-construction wiring is intact. Bind marker ordinal correctness is asserted
 * in AstShapeTest, not here.
 *
 * <p>Non-SELECT entries are skipped (INSERT, UPDATE, DDL are out of scope for the POC).
 * NOTE: CqlParseTreeDump masks lambda/functional fields and Terms.Raw.getText fallback
 * (design-critique finding 3), so equivalence is only as strong as the dump can observe.
 */
public class AstEquivalenceDifferentialTest
{
    private static final Path CORPUS = Paths.get("test/unit/org/apache/cassandra/cql3/differential/corpus.tsv");

    @Test
    public void astEquivalenceForSelectCorpus() throws Exception
    {
        assertTrue("missing corpus: " + CORPUS.toAbsolutePath(), Files.exists(CORPUS));

        List<String> failures = new ArrayList<>();
        List<String> skippedNames = new ArrayList<>();
        int checked = 0;
        int skipped = 0;

        for (String line : Files.readAllLines(CORPUS, StandardCharsets.UTF_8))
        {
            if (line.isBlank())
                continue;

            String[] parts = line.split("\t", 3);
            String name = parts[0];
            String entry = parts[1];
            String cql = parts[2];

            if (!entry.equals("query") || !cql.trim().toUpperCase().startsWith("SELECT"))
            {
                skipped++;
                continue;
            }

            try
            {
                CQLStatement.Raw directParsed = CQLFragmentParser.parseAnyUnhandled(p -> p.query().stmnt, cql);
                String directDump = CqlParseTreeDump.dump(directParsed);

                CqlParser.QueryContext ctx = CQLFragmentParser.parseAnyUnhandled(p -> p.query(), cql);
                Statement astStmt = AstBuilder.build(ctx);
                if (!(astStmt instanceof SelectAst))
                {
                    failures.add(name + ": AstBuilder did not produce SelectAst for: " + cql);
                    continue;
                }

                SelectStatement.RawStatement lowered = AstLowering.lowerSelectAst((SelectAst) astStmt);
                String loweredDump = CqlParseTreeDump.dump(lowered);

                if (!directDump.equals(loweredDump))
                    failures.add(name + ":\n  direct: " + directDump + "\n  lowered: " + loweredDump);

                checked++;
            }
            catch (UnsupportedAstException e)
            {
                skipped++;
                skippedNames.add(name);
            }
            catch (Exception e)
            {
                failures.add(name + ": exception: " + e.getMessage());
            }
        }

        System.out.println("Corpus equivalence check: " + checked + " checked, " + skipped + " skipped");
        if (!skippedNames.isEmpty())
            System.out.println("Skipped (unsupported constructs): " + String.join(", ", skippedNames));

        assertTrue("no SELECT corpus entries checked", checked > 0);
        if (!failures.isEmpty())
            fail("AST path differs from direct parse for " + failures.size() + " corpus entries:\n"
                 + String.join("\n", failures));
    }
}
