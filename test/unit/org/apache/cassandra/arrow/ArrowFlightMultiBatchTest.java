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

import java.util.HashSet;
import java.util.Set;

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

import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for task #3 (see {@code ARROW-FLIGHT.md}): a real Flight server/client pair,
 * forced to produce MULTIPLE batches for one stream (via a deliberately tiny per-batch byte
 * target, rather than a huge row count), asserts that every batch's rows - not just the first
 * one's - are visible to a client using the standard
 * {@code while (stream.next()) { root = stream.getRoot(); ... }} idiom.
 * <p>
 * Before the fix, {@code CassandraFlightProducer#getStream} called {@code listener.start(root)}
 * once per batch; Arrow Flight's client-side {@code FlightStream} only binds its public
 * {@code getRoot()} to the FIRST Schema message it ever receives, so every batch after the first
 * was silently invisible to exactly this idiom (used by {@link ArrowFlightEndToEndTest} too, whose
 * tiny dataset never produced more than one batch and so never caught this).
 */
public class ArrowFlightMultiBatchTest extends CQLTester
{
    @Test
    public void allBatchesAreDeliveredNotJustTheFirst() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, payload text)");

        // A ~1KB value per row, well over targetBatchBytes/rowCount, forces many small batches
        // rather than relying on an enormous row count to cross a fixed 16MB default target.
        String payload = "x".repeat(1024);
        int rowCount = 500;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO %s (pk, payload) VALUES (?, ?)", i, payload);

        Location location = Location.forGrpcInsecure("127.0.0.1", 0);
        // Deliberately tiny: with ~1KB rows this guarantees many batches over 500 rows, directly
        // exercising CassandraFlightProducer#getStream's per-batch putNext() handling without
        // depending on ArrowFlightService's fixed production batch-size constant.
        long tinyTargetBatchBytes = 4L * 1024;

        try (BufferAllocator serverAllocator = new RootAllocator();
             BufferAllocator clientAllocator = new RootAllocator())
        {
            CassandraFlightProducer producer = new CassandraFlightProducer(serverAllocator, tinyTargetBatchBytes, location);
            try (FlightServer server = FlightServer.builder(serverAllocator, location, producer).build().start())
            {
                try (FlightClient client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", server.getPort())).build())
                {
                    FlightInfo info = client.getInfo(FlightDescriptor.path(KEYSPACE, currentTable()));
                    Ticket ticket = info.getEndpoints().get(0).getTicket();

                    int totalRows = 0;
                    int batchCount = 0;
                    Set<Integer> seenKeys = new HashSet<>();
                    try (FlightStream stream = client.getStream(ticket))
                    {
                        while (stream.next())
                        {
                            VectorSchemaRoot root = stream.getRoot();
                            batchCount++;
                            totalRows += root.getRowCount();
                            for (int i = 0; i < root.getRowCount(); i++)
                                seenKeys.add((Integer) root.getVector("pk").getObject(i));
                        }
                    }

                    // The core regression assertion: EVERY row must be visible, not just the
                    // first batch's - this is what silently failed before the fix.
                    assertThat(totalRows).isEqualTo(rowCount);
                    assertThat(seenKeys).hasSize(rowCount);
                    for (int i = 0; i < rowCount; i++)
                        assertThat(seenKeys).contains(i);

                    // Confirm the test actually forced multiple batches - otherwise this wouldn't
                    // be exercising the bug at all.
                    assertThat(batchCount).isGreaterThan(1);
                }
            }
        }
    }
}
