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
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential coverage for token-range-bounded scans (see {@code ARROW-FLIGHT.md} /
 * {@code StatefulCursor#positionAt}/{@code #setEndBound}): a bounded scan's output must match a full
 * unbounded scan filtered client-side down to the same {@code (start, end]} token subrange - for
 * BOTH the cursor-merge producer ({@link CassandraTableScanner#scan}) and the iterator-fallback
 * producer ({@link CassandraTableScanner#scanViaIteratorForTesting}).
 */
public class TokenRangeScanTest extends CQLTester
{
    private static final long TARGET_BATCH_BYTES = 16L * 1024 * 1024;

    @Test
    public void boundedScanMatchesClientSideFilteredFullScan() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        int partitionCount = 20;
        for (int pk = 0; pk < partitionCount; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, pk * 100);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        List<Token> sortedTokens = new ArrayList<>();
        for (int pk = 0; pk < partitionCount; pk++)
            sortedTokens.add(tokenOf(cfs, pk));
        sortedTokens.sort(Token::compareTo);

        // (start, end] covering roughly the middle third of the ring, by token order (not pk order)
        Token start = sortedTokens.get(6);
        Token end = sortedTokens.get(13);
        Range<Token> tokenRange = new Range<>(start, end);

        List<Map<String, Object>> fullScan = collectRows(cfs, null, false);
        List<Map<String, Object>> expected = fullScan.stream()
                                                       .filter(row -> inRange(tokenOf(cfs, (Integer) row.get("pk")), start, end))
                                                       .collect(Collectors.toList());
        assertThat(expected).isNotEmpty();
        assertThat(expected.size()).isLessThan(fullScan.size());

        List<Map<String, Object>> viaCursor = collectRows(cfs, tokenRange, false);
        List<Map<String, Object>> viaIterator = collectRows(cfs, tokenRange, true);

        assertThat(toPkSet(viaCursor)).as("cursor producer").isEqualTo(toPkSet(expected));
        assertThat(toPkSet(viaIterator)).as("iterator producer").isEqualTo(toPkSet(expected));
    }

    @Test
    public void nullTokenRangeReproducesWholeRangeScan() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        for (int pk = 0; pk < 10; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, pk);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<Map<String, Object>> viaCursor = collectRows(cfs, null, false);
        List<Map<String, Object>> viaIterator = collectRows(cfs, null, true);
        assertThat(toPkSet(viaCursor)).hasSize(10).isEqualTo(toPkSet(viaIterator));
    }

    // ================= helpers =================

    private static boolean inRange(Token token, Token start, Token end)
    {
        return token.compareTo(start) > 0 && token.compareTo(end) <= 0;
    }

    private static Token tokenOf(ColumnFamilyStore cfs, int pk)
    {
        DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(pk));
        return key.getToken();
    }

    private static java.util.Set<Integer> toPkSet(List<Map<String, Object>> rows)
    {
        return rows.stream().map(r -> (Integer) r.get("pk")).collect(java.util.stream.Collectors.toSet());
    }

    private List<Map<String, Object>> collectRows(ColumnFamilyStore cfs, Range<Token> tokenRange, boolean viaIterator) throws Exception
    {
        List<Map<String, Object>> rows = new ArrayList<>();
        Consumer<VectorSchemaRoot> onBatch = root -> {
            try
            {
                for (int i = 0; i < root.getRowCount(); i++)
                {
                    Map<String, Object> row = new HashMap<>();
                    row.put("pk", CassandraTableScannerTestSupport.valueOf(root, "pk", i));
                    row.put("v", CassandraTableScannerTestSupport.valueOf(root, "v", i));
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
                CassandraTableScanner.scanViaIteratorForTesting(cfs, allocator, TARGET_BATCH_BYTES, onBatch, tokenRange, null, null);
            else
                CassandraTableScanner.scan(cfs, allocator, TARGET_BATCH_BYTES, onBatch, tokenRange, null, null);
        }
        return rows;
    }
}
