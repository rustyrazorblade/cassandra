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
package org.apache.cassandra.test.microbench;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.tree.AstBuilder;
import org.apache.cassandra.cql3.tree.AstLowering;
import org.apache.cassandra.cql3.tree.SelectAst;
import org.apache.cassandra.cql3.tree.Statement;

/**
 * Benchmark comparing direct parse vs AST parse throughput. Measures the overhead
 * of the optimizer insertion point. The number that matters is the ratio: AST time / direct time.
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Benchmark)
public class AstParseBench
{
    @Param({
        "SELECT * FROM tbl",
        "SELECT a, b FROM ks.tbl WHERE x = 1 AND y = 2 ORDER BY a DESC LIMIT 10",
        "SELECT count(*), sum(v) FROM tbl WHERE id = 1 GROUP BY id",
        "SELECT * FROM tbl WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"
    })
    public String query;

    @Benchmark
    public CQLStatement.Raw directParse() throws Exception
    {
        return CQLFragmentParser.parseAnyUnhandled(p -> p.query().stmnt, query);
    }

    @Benchmark
    public CQLStatement.Raw astParse() throws Exception
    {
        CqlParser.QueryContext ctx = CQLFragmentParser.parseAnyUnhandled(p -> p.query(), query);
        Statement ast = AstBuilder.build(ctx);
        if (ast instanceof SelectAst)
            return AstLowering.lowerSelectAst((SelectAst) ast);
        return ctx.stmnt;
    }
}
