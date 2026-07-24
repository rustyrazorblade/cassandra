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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.Test;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct (no gRPC/Flight) regression coverage for {@link CassandraTableScanner} / {@link ArrowRowAssembler}
 * bugs found in the adversarial review of the Arrow Flight PoC - see {@code ARROW-FLIGHT.md}.
 * <p>
 * Every test drives both scan producers against the same data: {@link CassandraTableScanner#scan}
 * (which picks the cursor-merge producer for an ordinary table like these) and
 * {@link CassandraTableScanner#scanViaIteratorForTesting} (the test-only seam that forces the
 * iterator-fallback producer regardless of {@code isCursorReadSupported}). Both must agree with each
 * other, and with what a real CQL {@code SELECT *} would return.
 */
public class CassandraTableScannerTest extends CQLTester
{
    private static final long TARGET_BATCH_BYTES = 16L * 1024 * 1024;

    @Test
    public void staticOnlyPartitionEmitsExactlyOneRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s text static, v text, PRIMARY KEY (pk, ck))");
        // Only a static column is ever set for pk=1; no clustering value is ever written, so this
        // partition has no regular row at all - matching the bug's reproduction (task #1).
        execute("UPDATE %s SET s = 'x' WHERE pk = 1");

        List<String> columns = List.of("pk", "ck", "s", "v");
        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectRows(columns, viaIterator);
            assertThat(rows).as("producer=%s", viaIterator ? "iterator" : "cursor").hasSize(1);
            Map<String, Object> row = rows.get(0);
            assertThat(row.get("pk")).isEqualTo(1);
            assertThat(row.get("ck")).isNull();
            assertThat(row.get("s")).isEqualTo("x");
            assertThat(row.get("v")).isNull();
        }
    }

    @Test
    public void partitionWithNoDataEmitsNoRows() throws Throwable
    {
        // Sanity check alongside the static-only-partition fix: a partition that never had ANY
        // data (not even a static value) must still contribute zero rows, not a spurious one.
        createTable("CREATE TABLE %s (pk int, ck int, s text static, v text, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'row-1')");
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 1");

        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectRows(List.of("pk", "ck", "s", "v"), viaIterator);
            assertThat(rows).as("producer=%s", viaIterator ? "iterator" : "cursor").isEmpty();
        }
    }

    @Test
    public void twoComplexColumnsInSameRowBothSurvive() throws Throwable
    {
        // Regression for task #2: with 2+ complex (multi-cell) columns on the same row via the
        // cursor path, only the LAST one used to be finalized correctly (endComplexColumn was only
        // ever reached at end-of-row); every earlier complex column's ListVector/MapVector never
        // got its endValue()/setNull() call.
        createTable("CREATE TABLE %s (pk int, ck int, a list<int>, b map<text, int>, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (1, 1, [1, 2, 3], {'x': 10, 'y': 20})");

        List<Map<String, Object>> rows = collectRows(List.of("pk", "ck", "a", "b"), false);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("a")).isEqualTo(List.of(1, 2, 3));
        assertThat(rows.get(0).get("b")).isEqualTo(Map.of("x", 10, "y", 20));
    }

    @Test
    public void setColumnRoundTripsThroughBothProducers() throws Throwable
    {
        // Regression for task #4: Cassandra always writes a Set element's cell VALUE empty (the
        // element lives in the cell path instead - see cql3/terms/Sets.java), so a Set cell never
        // triggers writeCellValue on the cursor path; it must still be dispatched.
        createTable("CREATE TABLE %s (pk int, ck int, tags set<text>, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, tags) VALUES (1, 1, {'a', 'b', 'c'})");

        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectRows(List.of("pk", "ck", "tags"), viaIterator);
            assertThat(rows).as("producer=%s", viaIterator ? "iterator" : "cursor").hasSize(1);
            @SuppressWarnings("unchecked")
            List<Object> tags = (List<Object>) rows.get(0).get("tags");
            assertThat(tags).containsExactlyInAnyOrder("a", "b", "c");
        }
    }

    @Test
    public void emptyStringScalarRoundTripsThroughBothProducers() throws Throwable
    {
        // Regression for task #5: the on-disk wire format cannot distinguish "no value" from "a
        // genuinely empty value" (both have HAS_EMPTY_VALUE_MASK set); a live cell with an empty
        // value must read back as "", not null.
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, '')");

        for (boolean viaIterator : new boolean[]{ false, true })
        {
            List<Map<String, Object>> rows = collectRows(List.of("pk", "ck", "v"), viaIterator);
            assertThat(rows).as("producer=%s", viaIterator ? "iterator" : "cursor").hasSize(1);
            assertThat(rows.get(0).get("v")).isEqualTo("");
        }
    }

    @Test
    public void emptyValuedListElementRoundTrips() throws Throwable
    {
        // Same root cause as the Set/empty-scalar cases (task #4/#5), but for a List, whose path
        // (not value) carries the cell identity, and whose VALUE is the (possibly empty) element.
        createTable("CREATE TABLE %s (pk int, ck int, l list<text>, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, l) VALUES (1, 1, ['a', '', 'c'])");

        List<Map<String, Object>> rows = collectRows(List.of("pk", "ck", "l"), false);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("l")).isEqualTo(List.of("a", "", "c"));
    }

    @Test
    public void deletedRowIsNotEmittedByIteratorProducer() throws Throwable
    {
        // Regression for task #6: scanViaIterator used to treat every Unfiltered#isRow() as an
        // output row without checking liveness, so a fully row-deleted clustering key (still a
        // real Row object, just with no live data) would come back as a "ghost" all-null row that
        // real CQL would never return.
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 1");

        List<Map<String, Object>> viaIterator = collectRows(List.of("pk", "ck", "v"), true);
        assertThat(viaIterator).isEmpty();

        // Parity check: the pre-existing cursor-merge producer already got this right.
        List<Map<String, Object>> viaCursor = collectRows(List.of("pk", "ck", "v"), false);
        assertThat(viaCursor).isEmpty();
    }

    @Test
    public void ttlExpiredRowIsNotEmittedByIteratorProducer() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x') USING TTL 1");

        // No fake-clock hook exists for cell/row TTL; a short real sleep is the only way to make
        // this actually expire before the scan runs (same technique as ArrowFlightEndToEndTest).
        Thread.sleep(2000);

        List<Map<String, Object>> viaIterator = collectRows(List.of("pk", "ck", "v"), true);
        assertThat(viaIterator).isEmpty();

        List<Map<String, Object>> viaCursor = collectRows(List.of("pk", "ck", "v"), false);
        assertThat(viaCursor).isEmpty();
    }

    // ================= helpers =================

    private List<Map<String, Object>> collectRows(List<String> columns, boolean viaIterator) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<Map<String, Object>> rows = new ArrayList<>();
        Consumer<VectorSchemaRoot> onBatch = root -> {
            try
            {
                for (int i = 0; i < root.getRowCount(); i++)
                {
                    Map<String, Object> row = new HashMap<>();
                    for (String column : columns)
                        row.put(column, valueOf(root, column, i));
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
                CassandraTableScanner.scanViaIteratorForTesting(cfs, allocator, TARGET_BATCH_BYTES, onBatch);
            else
                CassandraTableScanner.scan(cfs, allocator, TARGET_BATCH_BYTES, onBatch);
        }
        return rows;
    }

    private static Object valueOf(VectorSchemaRoot root, String column, int index)
    {
        org.apache.arrow.vector.ValueVector vector = root.getVector(column);
        if (vector.isNull(index))
            return null;
        return normalize(vector.getObject(index));
    }

    /**
     * Recursively converts Arrow's {@code Text} wrapper (used for UTF-8 vectors) to plain
     * {@link String}, and Arrow {@code MapVector}'s "list of {key, value} structs" object
     * representation to a plain {@link java.util.Map}, for easier assertion.
     */
    private static Object normalize(Object value)
    {
        if (value instanceof org.apache.arrow.vector.util.Text)
            return value.toString();
        if (value instanceof List)
        {
            List<?> list = (List<?>) value;
            if (!list.isEmpty() && list.stream().allMatch(CassandraTableScannerTest::isMapEntry))
            {
                Map<Object, Object> result = new java.util.LinkedHashMap<>();
                for (Object element : list)
                {
                    Map<?, ?> entry = (Map<?, ?>) element;
                    result.put(normalize(entry.get("key")), normalize(entry.get("value")));
                }
                return result;
            }
            List<Object> result = new ArrayList<>();
            for (Object element : list)
                result.add(normalize(element));
            return result;
        }
        return value;
    }

    private static boolean isMapEntry(Object o)
    {
        return o instanceof Map && ((Map<?, ?>) o).size() == 2
               && ((Map<?, ?>) o).containsKey("key") && ((Map<?, ?>) o).containsKey("value");
    }
}
