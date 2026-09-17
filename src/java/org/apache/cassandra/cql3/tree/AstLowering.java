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
package org.apache.cassandra.cql3.tree;

import org.apache.cassandra.cql3.statements.SelectStatement;

/**
 * POC lowering: returns parser-built RawStatement carried in SelectAst.
 * Full bidirectional AST->domain construction deferred to CEP.
 *
 * For unmodified trees, equivalence dump(direct) == dump(lower(buildAst(ctx)))
 * is exact by construction since we return the same RawStatement the parser built.
 */
public final class AstLowering
{
    private AstLowering() {}

    public static SelectStatement.RawStatement lowerSelectAst(SelectAst ast)
    {
        // POC: return the carried parser-built RawStatement for unmodified trees
        if (ast.carriedRawStatement != null)
            return ast.carriedRawStatement;

        throw new UnsupportedAstException("lowerSelectAst requires carried RawStatement for POC");
    }
}
