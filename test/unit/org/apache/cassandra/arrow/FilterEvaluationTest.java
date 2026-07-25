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

package org.apache.cassandra.arrow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JsonUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Post-merge filter evaluation ({@link FilterExpression}/{@link FilterCompiler}), driven through
 * {@link CassandraTableScanner} exactly as a real Flight request would (JSON -&gt; {@link
 * FilterCompiler#compile} -&gt; {@link ArrowRowAssembler}), across BOTH scan producers (cursor via
 * {@link CassandraTableScanner#scan}, iterator fallback via
 * {@link CassandraTableScanner#scanViaIteratorForTesting}) - see {@code ARROW-FLIGHT.md}.
 */
public class FilterEvaluationTest extends CQLTester
{
    private static final long TARGET_BATCH_BYTES = 16L * 1024 * 1024;

    @Test
    public void comparisonOperatorsMatchExpectedRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, amount int, PRIMARY KEY (pk, ck))");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (pk, ck, amount) VALUES (1, ?, ?)", i, i * 10); // 0,10,20,30,40

        assertFilteredCks("{\"cmp\": {\"column\": \"amount\", \"op\": \"EQ\", \"value\": 20}}", Set.of(2));
        assertFilteredCks("{\"cmp\": {\"column\": \"amount\", \"op\": \"NE\", \"value\": 20}}", Set.of(0, 1, 3, 4));
        assertFilteredCks("{\"cmp\": {\"column\": \"amount\", \"op\": \"LT\", \"value\": 20}}", Set.of(0, 1));
        assertFilteredCks("{\"cmp\": {\"column\": \"amount\", \"op\": \"LE\", \"value\": 20}}", Set.of(0, 1, 2));
        assertFilteredCks("{\"cmp\": {\"column\": \"amount\", \"op\": \"GT\", \"value\": 20}}", Set.of(3, 4));
        assertFilteredCks("{\"cmp\": {\"column\": \"amount\", \"op\": \"GE\", \"value\": 20}}", Set.of(2, 3, 4));
    }

    @Test
    public void andOrNotCompose() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, amount int, PRIMARY KEY (pk, ck))");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (pk, ck, amount) VALUES (1, ?, ?)", i, i * 10);

        assertFilteredCks("{\"and\": [{\"cmp\": {\"column\": \"amount\", \"op\": \"GT\", \"value\": 0}}, " +
                           "{\"cmp\": {\"column\": \"amount\", \"op\": \"LT\", \"value\": 40}}]}", Set.of(1, 2, 3));
        assertFilteredCks("{\"or\": [{\"cmp\": {\"column\": \"amount\", \"op\": \"EQ\", \"value\": 0}}, " +
                           "{\"cmp\": {\"column\": \"amount\", \"op\": \"EQ\", \"value\": 40}}]}", Set.of(0, 4));
        assertFilteredCks("{\"not\": {\"cmp\": {\"column\": \"amount\", \"op\": \"EQ\", \"value\": 20}}}", Set.of(0, 1, 3, 4));
    }

    @Test
    public void isNullAndIsNotNull() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 2)"); // v never set -> null

        assertFilteredCks("{\"isNull\": {\"column\": \"v\"}}", Set.of(2));
        assertFilteredCks("{\"isNotNull\": {\"column\": \"v\"}}", Set.of(1));
    }

    @Test
    public void inMatchesAnyListedValue() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, tag text, PRIMARY KEY (pk, ck))");
        String[] tags = { "a", "b", "c", "d" };
        for (int i = 0; i < tags.length; i++)
            execute("INSERT INTO %s (pk, ck, tag) VALUES (1, ?, ?)", i, tags[i]);

        assertFilteredCks("{\"in\": {\"column\": \"tag\", \"values\": [\"a\", \"c\"]}}", Set.of(0, 2));
    }

    @Test
    public void filterOnStaticColumnAppliesToReplicatedValue() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, region text static, amount int, PRIMARY KEY (pk, ck))");
        execute("UPDATE %s SET region = 'east' WHERE pk = 1");
        execute("UPDATE %s SET region = 'west' WHERE pk = 2");
        execute("INSERT INTO %s (pk, ck, amount) VALUES (1, 1, 100)");
        execute("INSERT INTO %s (pk, ck, amount) VALUES (1, 2, 200)");
        execute("INSERT INTO %s (pk, ck, amount) VALUES (2, 1, 300)");

        JsonNode filterJson = readFilter("{\"cmp\": {\"column\": \"region\", \"op\": \"EQ\", \"value\": \"east\"}}");
        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectRows(List.of("pk", "ck", "region", "amount"), filterJson, viaIterator);
            assertThat(rows).as("producer=%s", viaIterator ? "iterator" : "cursor").hasSize(2);
            for (Map<String, Object> row : rows)
            {
                assertThat(row.get("pk")).isEqualTo(1);
                assertThat(row.get("region")).isEqualTo("east");
            }
        }
    }

    @Test
    public void unknownColumnIsRejectedAtCompileTime() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        TableMetadata table = getCurrentColumnFamilyStore().metadata();
        JsonNode filterJson = readFilter("{\"cmp\": {\"column\": \"nope\", \"op\": \"EQ\", \"value\": \"x\"}}");
        assertThatThrownBy(() -> FilterCompiler.compile(filterJson, table))
            .hasMessageContaining("no such column");
    }

    // ================= helpers =================

    private void assertFilteredCks(String filterJson, Set<Integer> expectedCks) throws Exception
    {
        JsonNode filter = readFilter(filterJson);
        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectRows(List.of("pk", "ck", "amount"), filter, viaIterator);
            Set<Integer> actualCks = new HashSet<>();
            for (Map<String, Object> row : rows)
                actualCks.add((Integer) row.get("ck"));
            assertThat(actualCks).as("producer=%s filter=%s", viaIterator ? "iterator" : "cursor", filterJson)
                                  .isEqualTo(expectedCks);
        }
    }

    private static JsonNode readFilter(String json) throws Exception
    {
        return JsonUtils.JSON_OBJECT_MAPPER.readTree(json);
    }

    private List<Map<String, Object>> collectRows(List<String> columns, JsonNode filterJson, boolean viaIterator) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        FilterExpression filter = FilterCompiler.compile(filterJson, cfs.metadata());
        List<Map<String, Object>> rows = new ArrayList<>();
        Consumer<VectorSchemaRoot> onBatch = root -> {
            try
            {
                for (int i = 0; i < root.getRowCount(); i++)
                {
                    Map<String, Object> row = new HashMap<>();
                    for (String column : columns)
                        row.put(column, CassandraTableScannerTestSupport.valueOf(root, column, i));
                    rows.add(row);
                }
            }
            finally
            {
                root.close();
            }
        };

        try (BufferAllocator allocator = new RootAllocator())
        {
            if (viaIterator)
                CassandraTableScanner.scanViaIteratorForTesting(cfs, allocator, TARGET_BATCH_BYTES, onBatch, null, filter, null);
            else
                CassandraTableScanner.scan(cfs, allocator, TARGET_BATCH_BYTES, onBatch, null, filter, null);
        }
        return rows;
    }
}
