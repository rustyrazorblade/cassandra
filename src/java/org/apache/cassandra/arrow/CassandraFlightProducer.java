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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Arrow Flight {@link FlightProducer} exposing every user table as a full-table-scan flight.
 * <p>
 * <b>PoC scope - read loudly documented here, see {@code ARROW-FLIGHT.md} for the full design:</b>
 * <ul>
 *   <li><b>No authentication or authorization whatsoever.</b> Anyone who can open a TCP connection
 *       to this port can read every row of every user table. This is strictly a development/PoC
 *       posture; do not expose this port on an untrusted network. Production would add Flight
 *       handshake + bearer middleware backed by {@code IAuthenticator}/{@code IAuthorizer}.</li>
 *   <li><b>No filter pushdown.</b> {@link #getStream} always streams the whole table; predicates
 *       must be applied client-side.</li>
 *   <li><b>No token-range splitting.</b> {@link #getFlightInfo} always returns exactly one
 *       {@link FlightEndpoint} covering the table's entire local primary range.</li>
 *   <li><b>No point-read API.</b> Full-table scan only.</li>
 * </ul>
 * A flight's {@link FlightDescriptor} path is {@code [keyspace, table]}; its {@link Ticket} is the
 * UTF-8 bytes of {@code "keyspace.table"}.
 */
public class CassandraFlightProducer implements FlightProducer
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraFlightProducer.class);

    private final BufferAllocator allocator;
    private final long targetBatchBytes;
    private final Location location;

    public CassandraFlightProducer(BufferAllocator allocator, long targetBatchBytes, Location location)
    {
        this.allocator = allocator;
        this.targetBatchBytes = targetBatchBytes;
        this.location = location;
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener)
    {
        try
        {
            for (KeyspaceMetadata keyspace : org.apache.cassandra.schema.Schema.instance.getUserKeyspaces())
                for (TableMetadata table : keyspace.tables)
                    listener.onNext(flightInfo(table));
            listener.onCompleted();
        }
        catch (Exception e)
        {
            listener.onError(CallStatus.INTERNAL.withCause(e).toRuntimeException());
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor)
    {
        return flightInfo(resolveTable(descriptor.getPath()));
    }

    private FlightInfo flightInfo(TableMetadata table)
    {
        Schema schema = CassandraArrowTypeMapping.toArrowSchema(table);
        FlightDescriptor descriptor = FlightDescriptor.path(table.keyspace, table.name);
        Ticket ticket = new Ticket(ticketBytes(table));
        FlightEndpoint endpoint = new FlightEndpoint(ticket, location);
        return FlightInfo.builder(schema, descriptor, List.of(endpoint)).build();
    }

    // Splitting on the first two dots (see getStream) could in principle misparse a keyspace name
    // containing a literal '.', but keyspace/table identifiers are validated against
    // SchemaConstants#PATTERN_WORD_CHARS (\w+, matched in full) at creation time, which cannot
    // contain '.' - so this is unreachable in practice, not a live bug.
    private static byte[] ticketBytes(TableMetadata table)
    {
        return (table.keyspace + '.' + table.name).getBytes(StandardCharsets.UTF_8);
    }

    private static TableMetadata resolveTable(List<String> path)
    {
        if (path.size() != 2)
            throw CallStatus.INVALID_ARGUMENT.withDescription("expected a [keyspace, table] path, got " + path).toRuntimeException();
        return resolveTable(path.get(0), path.get(1));
    }

    private static TableMetadata resolveTable(String keyspace, String table)
    {
        TableMetadata metadata = org.apache.cassandra.schema.Schema.instance.getTableMetadata(keyspace, table);
        if (metadata == null)
            throw CallStatus.NOT_FOUND.withDescription("no such table: " + keyspace + '.' + table).toRuntimeException();
        return metadata;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener)
    {
        String[] parts = new String(ticket.getBytes(), StandardCharsets.UTF_8).split("\\.", 2);
        if (parts.length != 2)
        {
            listener.error(CallStatus.INVALID_ARGUMENT.withDescription("malformed ticket").toRuntimeException());
            return;
        }
        try
        {
            TableMetadata table = resolveTable(parts[0], parts[1]);
            ColumnFamilyStore cfs = Keyspace.open(table.keyspace).getColumnFamilyStore(table.name);
            Schema schema = CassandraArrowTypeMapping.toArrowSchema(table);

            // Arrow Flight's client-side FlightStream/VectorLoader only binds its public getRoot()
            // to the FIRST Schema message it receives; calling listener.start(root) more than once
            // per stream re-sends a Schema message but never updates that binding, so every batch
            // after the first would be silently invisible to the standard `while (stream.next())
            // { root = stream.getRoot(); ... }` client idiom. start() is therefore called exactly
            // ONCE here, on a single, stable, long-lived root - each of ArrowRowAssembler's
            // per-batch roots (a fresh one every time, deliberately, so this class never mutates
            // buffers a consumer might still be sending asynchronously) is LOADED into this stable
            // root via VectorUnloader/VectorLoader before every putNext(), rather than rebinding.
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator))
            {
                listener.start(root);
                VectorLoader loader = new VectorLoader(root);

                CassandraTableScanner.scan(cfs, allocator, targetBatchBytes, batch -> {
                    try
                    {
                        awaitReady(listener);
                        if (listener.isCancelled())
                            throw new CancellationSignal();

                        VectorUnloader unloader = new VectorUnloader(batch);
                        try (ArrowRecordBatch recordBatch = unloader.getRecordBatch())
                        {
                            loader.load(recordBatch);
                        }
                        listener.putNext();
                    }
                    finally
                    {
                        // Guaranteed on every exit path (success, cancellation, or any other
                        // exception) so a cancelled/failed stream never leaks this batch's buffers
                        // against the shared, service-lifetime allocator.
                        batch.close();
                    }
                });
            }
            listener.completed();
        }
        catch (CancellationSignal cancelled)
        {
            listener.completed();
        }
        catch (Exception e)
        {
            logger.warn("Arrow Flight scan of ticket {} failed", new String(ticket.getBytes(), StandardCharsets.UTF_8), e);
            listener.error(CallStatus.INTERNAL.withCause(e).toRuntimeException());
        }
    }

    /**
     * The cursor/iterator scan loop is synchronous and pull-driven (see {@code ARROW-FLIGHT.md}
     * task #12): rather than reimplementing it as async, this simply parks the scanning thread
     * until the gRPC stream reports it can accept more data (or the client cancels).
     */
    private static void awaitReady(ServerStreamListener listener)
    {
        while (!listener.isReady() && !listener.isCancelled())
            LockSupport.parkNanos(1_000_000L);
    }

    private static final class CancellationSignal extends RuntimeException
    {
        CancellationSignal()
        {
            super(null, null, false, false);
        }
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream)
    {
        throw CallStatus.UNIMPLEMENTED.withDescription("Arrow Flight PoC is read-only").toRuntimeException();
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener)
    {
        listener.onError(CallStatus.UNIMPLEMENTED.withDescription("no actions are implemented").toRuntimeException());
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener)
    {
        listener.onCompleted();
    }
}
