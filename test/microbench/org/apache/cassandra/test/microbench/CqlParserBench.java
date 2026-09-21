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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.cql3.CQLFragmentParser;

/**
 * Parse-throughput benchmark for the CQL grammar.  It measures the cost of turning a CQL string into
 * its parsed domain object through the shared {@link CQLFragmentParser} entry point, which is present
 * with the same signature on both the ANTLR 3 baseline ({@code jdk25-upgrade}) and this ANTLR 4
 * branch.  The identical source runs on both trees, so a run on each measures whether ANTLR 4's
 * ALL(*) prediction regressed the parse hot path versus ANTLR 3's LL(*).
 *
 * <p>The parser lambda {@code p -> p.query()} compiles on both branches (the return type differs, the
 * source does not) and performs the full parse plus domain-object construction.  Only CQL that both
 * grammars parse identically is used.</p>
 *
 * <p>Run per branch: {@code ant microbench -Dbenchmark.name=CqlParserBench -Djmh.args="-prof gc"}.
 * The GC profiler reports bytes/op alongside the throughput.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(1)
@State(Scope.Benchmark)
public class CqlParserBench
{
    private static final String POINT_SELECT =
        "SELECT id, name FROM ks.users WHERE id = 123";

    private static final String SELECT_IN_ORDER_LIMIT =
        "SELECT id, name FROM ks.users WHERE id IN (1, 2, 3, 4, 5) ORDER BY name ASC LIMIT 100";

    private static final String INSERT_BIND =
        "INSERT INTO ks.users (id, name, email) VALUES (?, ?, ?)";

    private static final String UPDATE =
        "UPDATE ks.users SET name = 'alice', email = 'a@example.com' WHERE id = 42";

    private static final String DELETE =
        "DELETE FROM ks.users WHERE id = 42";

    private static final String CREATE_TABLE =
        "CREATE TABLE ks.users (id int PRIMARY KEY, name text, email text, created timestamp)";

    private static final String BATCH =
        "BEGIN BATCH "
        + "INSERT INTO ks.users (id, name) VALUES (1, 'a'); "
        + "UPDATE ks.users SET name = 'b' WHERE id = 2; "
        + "APPLY BATCH";

    private static Object parse(String cql) throws Exception
    {
        // p.query() returns CQLStatement.Raw on the ANTLR 3 branch and a QueryContext on the ANTLR 4
        // branch; both do the full parse plus domain-object build.  The source is identical.
        return CQLFragmentParser.parseAnyUnhandled(p -> p.query(), cql);
    }

    @Benchmark
    public Object pointSelect() throws Exception
    {
        return parse(POINT_SELECT);
    }

    @Benchmark
    public Object selectInOrderLimit() throws Exception
    {
        return parse(SELECT_IN_ORDER_LIMIT);
    }

    @Benchmark
    public Object insertWithBindMarkers() throws Exception
    {
        return parse(INSERT_BIND);
    }

    @Benchmark
    public Object update() throws Exception
    {
        return parse(UPDATE);
    }

    @Benchmark
    public Object delete() throws Exception
    {
        return parse(DELETE);
    }

    @Benchmark
    public Object createTable() throws Exception
    {
        return parse(CREATE_TABLE);
    }

    @Benchmark
    public Object batchTwoStatements() throws Exception
    {
        return parse(BATCH);
    }

    @Benchmark
    public void mixed(org.openjdk.jmh.infra.Blackhole bh) throws Exception
    {
        bh.consume(parse(POINT_SELECT));
        bh.consume(parse(SELECT_IN_ORDER_LIMIT));
        bh.consume(parse(INSERT_BIND));
        bh.consume(parse(UPDATE));
        bh.consume(parse(DELETE));
        bh.consume(parse(CREATE_TABLE));
        bh.consume(parse(BATCH));
    }
}
