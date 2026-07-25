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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code getFlightInfo} command-based schema discovery ({@link FlightDescriptor#command}, carrying a
 * full {@link FlightTicket} JSON payload) - both with and without an {@code aggregation} spec - and
 * plain-path discovery ({@link FlightDescriptor#path}), which must keep working unchanged for
 * schema discovery with no aggregation. See {@code ARROW-FLIGHT.md}.
 */
public class FlightTicketSchemaDiscoveryTest extends CQLTester
{
    @Test
    public void commandWithoutAggregationMatchesPlainPathSchema() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, amount int, region text)");

        withServer((client, keyspace, table) -> {
            FlightInfo pathInfo = client.getInfo(FlightDescriptor.path(keyspace, table));

            byte[] ticketJson = ("{\"keyspace\": \"" + keyspace + "\", \"table\": \"" + table + "\"}").getBytes(StandardCharsets.UTF_8);
            FlightInfo commandInfo = client.getInfo(FlightDescriptor.command(ticketJson));

            assertThat(fieldNames(commandInfo.getSchema())).isEqualTo(fieldNames(pathInfo.getSchema()));
            assertThat(commandInfo.getSchema()).isEqualTo(pathInfo.getSchema());
        });
    }

    @Test
    public void commandWithAggregationReturnsAggregatedSchema() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, region text static, amount int, PRIMARY KEY (pk, ck))");

        withServer((client, keyspace, table) -> {
            String ticketJson = "{\"keyspace\": \"" + keyspace + "\", \"table\": \"" + table + "\", " +
                                 "\"aggregation\": {\"groupBy\": [\"region\"], \"aggregates\": [" +
                                 "{\"function\": \"COUNT\", \"column\": null, \"outputName\": \"n\"}, " +
                                 "{\"function\": \"SUM\", \"column\": \"amount\", \"outputName\": \"total\"}]}}";
            FlightInfo info = client.getInfo(FlightDescriptor.command(ticketJson.getBytes(StandardCharsets.UTF_8)));

            assertThat(fieldNames(info.getSchema())).containsExactly("region", "n", "total");
            assertThat(info.getSchema().findField("region").getType()).isEqualTo(new ArrowType.Utf8());
            assertThat(info.getSchema().findField("n").getType()).isEqualTo(new ArrowType.Int(64, true));
            assertThat(info.getSchema().findField("total").getType())
                .isEqualTo(new ArrowType.Decimal(CassandraArrowTypeMapping.DECIMAL_PRECISION, 0, CassandraArrowTypeMapping.DECIMAL_BIT_WIDTH));

            // the endpoint's ticket must itself be usable for getStream, driving the full pipeline
            // (schema discovery -> stream) exactly as a real client would
            Ticket ticket = info.getEndpoints().get(0).getTicket();
            try (FlightStream stream = client.getStream(ticket))
            {
                int rows = 0;
                while (stream.next())
                    rows += stream.getRoot().getRowCount();
                // no data inserted - global GROUP BY on zero rows still emits zero groups since
                // groupBy is non-empty here (see RowAggregationTest#groupByNonEmptyOverZeroMatchingRowsEmitsNoRows)
                assertThat(rows).isEqualTo(0);
            }
        });
    }

    @Test
    public void malformedCommandTicketIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        withServer((client, keyspace, table) -> {
            byte[] badJson = "{ not json".getBytes(StandardCharsets.UTF_8);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> client.getInfo(FlightDescriptor.command(badJson)))
                .isInstanceOf(org.apache.arrow.flight.FlightRuntimeException.class);
        });
    }

    // ================= helpers =================

    private interface FlightAction
    {
        void run(FlightClient client, String keyspace, String table) throws Exception;
    }

    private void withServer(FlightAction action) throws Exception
    {
        Location location = Location.forGrpcInsecure("127.0.0.1", 0);
        try (BufferAllocator serverAllocator = new RootAllocator();
             BufferAllocator clientAllocator = new RootAllocator())
        {
            CassandraFlightProducer producer = new CassandraFlightProducer(serverAllocator, 16L * 1024 * 1024, location);
            try (FlightServer server = FlightServer.builder(serverAllocator, location, producer).build().start())
            {
                try (FlightClient client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", server.getPort())).build())
                {
                    action.run(client, KEYSPACE, currentTable());
                }
            }
        }
    }

    private static List<String> fieldNames(Schema schema)
    {
        return schema.getFields().stream().map(Field::getName).collect(java.util.stream.Collectors.toList());
    }
}
