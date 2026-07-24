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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test (task #13): a real embedded table, a real {@link ArrowFlightService}, and a real
 * {@link FlightClient} talking to it over gRPC/Arrow Flight - proving the whole pipeline (type
 * mapping, row assembly, scan orchestration, and the Flight service) produces byte-for-byte correct
 * data, including memtable freshness, null-vs-tombstone/TTL semantics, and static column replication.
 */
public class ArrowFlightEndToEndTest extends CQLTester
{
    @Test
    public void fullTableScanOverFlightMatchesInsertedData() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, ck int, s text static, v text, n int, l list<int>, PRIMARY KEY (pk, ck))");

        // Partition 1: a static column value (replicated onto every row of this partition), a
        // fully-populated row, a row with an explicitly tombstoned cell, and a row with a
        // TTL-expired cell (but a live row marker, so the row itself survives).
        execute("INSERT INTO %s (pk, ck, s, v, n, l) VALUES (1, 1, 'static-1', 'row-1-1', 10, [1, 2, 3])");
        execute("INSERT INTO %s (pk, ck, v, n) VALUES (1, 2, 'row-1-2', 20)");
        execute("DELETE n FROM %s WHERE pk = 1 AND ck = 2");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 'row-1-3')");
        execute("UPDATE %s USING TTL 1 SET n = 30 WHERE pk = 1 AND ck = 3");
        flush();

        // Partition 2: written AFTER the flush above and never explicitly flushed - proves the
        // scan's flush-then-scan strategy picks up current memtable contents, not just what was
        // already on disk. No static value was ever set for this partition.
        execute("INSERT INTO %s (pk, ck, v, n) VALUES (2, 1, 'row-2-1', 40)");

        // No fake-clock hook exists for cell TTL/localDeletionTime; a short real sleep is the only
        // way to make the UPDATE above actually TTL-expire before the scan runs.
        Thread.sleep(2000);

        DatabaseDescriptor.getRawConfig().arrow_flight_port = 0; // ephemeral port for this test
        ArrowFlightService service = new ArrowFlightService();
        service.start();
        try (BufferAllocator clientAllocator = new RootAllocator();
             FlightClient client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", service.getPort())).build())
        {
            FlightInfo info = client.getInfo(FlightDescriptor.path(KEYSPACE, currentTable()));
            assertThat(info.getEndpoints()).hasSize(1);

            Ticket ticket = info.getEndpoints().get(0).getTicket();
            Map<Integer, Map<Integer, Map<String, Object>>> rows = new HashMap<>();
            int totalRows = 0;
            try (FlightStream stream = client.getStream(ticket))
            {
                while (stream.next())
                {
                    VectorSchemaRoot root = stream.getRoot();
                    totalRows += root.getRowCount();
                    for (int i = 0; i < root.getRowCount(); i++)
                    {
                        int pk = (Integer) root.getVector("pk").getObject(i);
                        int ck = (Integer) root.getVector("ck").getObject(i);
                        Map<String, Object> row = new HashMap<>();
                        for (String column : List.of("s", "v", "n", "l"))
                            row.put(column, valueOf(root, column, i));
                        rows.computeIfAbsent(pk, k -> new HashMap<>()).put(ck, row);
                    }
                }
            }

            assertThat(totalRows).isEqualTo(4);
            assertThat(rows.keySet()).containsExactlyInAnyOrder(1, 2);
            assertThat(rows.get(1).keySet()).containsExactlyInAnyOrder(1, 2, 3);

            Map<String, Object> row11 = rows.get(1).get(1);
            assertThat(row11.get("s")).isEqualTo("static-1");
            assertThat(row11.get("v")).isEqualTo("row-1-1");
            assertThat(row11.get("n")).isEqualTo(10);
            assertThat(row11.get("l")).isEqualTo(List.of(1, 2, 3));

            // tombstoned cell reads as null; static column still replicated
            Map<String, Object> row12 = rows.get(1).get(2);
            assertThat(row12.get("s")).isEqualTo("static-1");
            assertThat(row12.get("v")).isEqualTo("row-1-2");
            assertThat(row12.get("n")).isNull();
            assertThat(row12.get("l")).isNull();

            // TTL-expired cell reads as null, but the row itself (and its other live columns) survive
            Map<String, Object> row13 = rows.get(1).get(3);
            assertThat(row13.get("s")).isEqualTo("static-1");
            assertThat(row13.get("v")).isEqualTo("row-1-3");
            assertThat(row13.get("n")).isNull();

            // memtable-only row (never flushed before the scan) is present, proving freshness;
            // no static value was ever set for this partition
            Map<String, Object> row21 = rows.get(2).get(1);
            assertThat(row21.get("s")).isNull();
            assertThat(row21.get("v")).isEqualTo("row-2-1");
            assertThat(row21.get("n")).isEqualTo(40);
        }
        finally
        {
            service.stop();
        }
    }

    private static Object valueOf(VectorSchemaRoot root, String column, int index)
    {
        org.apache.arrow.vector.ValueVector vector = root.getVector(column);
        if (vector.isNull(index))
            return null;
        Object value = vector.getObject(index);
        return value instanceof org.apache.arrow.vector.util.Text ? value.toString() : value;
    }
}
