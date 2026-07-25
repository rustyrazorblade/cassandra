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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.Test;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Server-side {@code GROUP BY}/aggregation ({@link RowAggregator}/{@link CompiledAggregation}),
 * driven end-to-end from ticket JSON (via {@link FlightTicket#parse}) through
 * {@link CassandraTableScanner}, across BOTH scan producers - see {@code ARROW-FLIGHT.md}.
 */
public class RowAggregationTest extends CQLTester
{
    private static final long TARGET_BATCH_BYTES = 16L * 1024 * 1024;

    @Test
    public void countStarWithNoGroupBy() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, amount int, PRIMARY KEY (pk, ck))");
        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (pk, ck, amount) VALUES (1, ?, ?)", i, i * 10);

        String aggregationJson = "{\"groupBy\": [], \"aggregates\": [{\"function\": \"COUNT\", \"column\": null, \"outputName\": \"n\"}]}";
        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectAggregatedRows(aggregationJson, viaIterator);
            assertThat(rows).as("producer=%s", viaIterator ? "iterator" : "cursor").hasSize(1);
            assertThat(rows.get(0).get("n")).isEqualTo(4L);
        }
    }

    @Test
    public void globalAggregateOverZeroRowsStillEmitsOneRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, amount int, PRIMARY KEY (pk, ck))");
        // no rows inserted at all

        String aggregationJson = "{\"groupBy\": [], \"aggregates\": [" +
                                  "{\"function\": \"COUNT\", \"column\": null, \"outputName\": \"n\"}, " +
                                  "{\"function\": \"SUM\", \"column\": \"amount\", \"outputName\": \"total\"}]}";
        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectAggregatedRows(aggregationJson, viaIterator);
            assertThat(rows).as("producer=%s", viaIterator ? "iterator" : "cursor").hasSize(1);
            assertThat(rows.get(0).get("n")).isEqualTo(0L);
            assertThat(rows.get(0).get("total")).isNull();
        }
    }

    @Test
    public void groupByNonEmptyOverZeroMatchingRowsEmitsNoRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, region text static, amount int, PRIMARY KEY (pk, ck))");
        // no rows inserted

        String aggregationJson = "{\"groupBy\": [\"region\"], \"aggregates\": [{\"function\": \"COUNT\", \"column\": null, \"outputName\": \"n\"}]}";
        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectAggregatedRows(aggregationJson, viaIterator);
            assertThat(rows).as("producer=%s", viaIterator ? "iterator" : "cursor").isEmpty();
        }
    }

    @Test
    public void sumMinMaxAvgGroupedByStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, region text static, amount int, PRIMARY KEY (pk, ck))");
        execute("UPDATE %s SET region = 'east' WHERE pk = 1");
        execute("INSERT INTO %s (pk, ck, amount) VALUES (1, 1, 10)");
        execute("INSERT INTO %s (pk, ck, amount) VALUES (1, 2, 20)");
        execute("UPDATE %s SET region = 'east' WHERE pk = 2");
        execute("INSERT INTO %s (pk, ck, amount) VALUES (2, 1, 5)");
        execute("UPDATE %s SET region = 'west' WHERE pk = 3");
        execute("INSERT INTO %s (pk, ck, amount) VALUES (3, 1, 100)");

        String aggregationJson = "{\"groupBy\": [\"region\"], \"aggregates\": [" +
                                  "{\"function\": \"COUNT\", \"column\": null, \"outputName\": \"n\"}, " +
                                  "{\"function\": \"SUM\", \"column\": \"amount\", \"outputName\": \"total\"}, " +
                                  "{\"function\": \"MIN\", \"column\": \"amount\", \"outputName\": \"lo\"}, " +
                                  "{\"function\": \"MAX\", \"column\": \"amount\", \"outputName\": \"hi\"}, " +
                                  "{\"function\": \"AVG\", \"column\": \"amount\", \"outputName\": \"avg\"}]}";

        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectAggregatedRows(aggregationJson, viaIterator);
            assertThat(rows).as("producer=%s", viaIterator ? "iterator" : "cursor").hasSize(2);

            Map<String, Object> east = rows.stream().filter(r -> "east".equals(r.get("region"))).findFirst().orElseThrow();
            assertThat(east.get("n")).isEqualTo(3L);
            assertThat((BigDecimal) east.get("total")).isEqualByComparingTo(new BigDecimal(35));
            assertThat(east.get("lo")).isEqualTo(5);
            assertThat(east.get("hi")).isEqualTo(20);
            assertThat((Double) east.get("avg")).isEqualTo(35.0 / 3, org.assertj.core.data.Offset.offset(1e-9));

            Map<String, Object> west = rows.stream().filter(r -> "west".equals(r.get("region"))).findFirst().orElseThrow();
            assertThat(west.get("n")).isEqualTo(1L);
            assertThat((BigDecimal) west.get("total")).isEqualByComparingTo(new BigDecimal(100));
            assertThat(west.get("lo")).isEqualTo(100);
            assertThat(west.get("hi")).isEqualTo(100);
            assertThat((Double) west.get("avg")).isEqualTo(100.0);
        }
    }

    // ================= helpers =================

    private List<Map<String, Object>> collectAggregatedRows(String aggregationJson, boolean viaIterator) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String ticketJson = "{\"keyspace\": \"" + KEYSPACE + "\", \"table\": \"" + currentTable() + "\", \"aggregation\": " + aggregationJson + "}";
        FlightTicket ticket = FlightTicket.parse(ticketJson.getBytes(StandardCharsets.UTF_8));
        CompiledAggregation aggregation = CompiledAggregation.compile(ticket.aggregation, cfs.metadata());

        List<Map<String, Object>> rows = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        aggregation.outputSchema.getFields().forEach(f -> columnNames.add(f.getName()));

        Consumer<VectorSchemaRoot> onBatch = root -> {
            try
            {
                for (int i = 0; i < root.getRowCount(); i++)
                {
                    Map<String, Object> row = new HashMap<>();
                    for (String column : columnNames)
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
                CassandraTableScanner.scanViaIteratorForTesting(cfs, allocator, TARGET_BATCH_BYTES, onBatch, null, null, aggregation);
            else
                CassandraTableScanner.scan(cfs, allocator, TARGET_BATCH_BYTES, onBatch, null, null, aggregation);
        }
        return rows;
    }
}
