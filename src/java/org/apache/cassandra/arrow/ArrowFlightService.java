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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Embedded Arrow Flight service, a sibling to {@link org.apache.cassandra.service.NativeTransportService}
 * (see {@link org.apache.cassandra.service.CassandraDaemon#initializeClientTransports()}).
 * <p>
 * <b>This is a development/PoC-only service: the Flight endpoint it opens has NO authentication or
 * authorization of any kind.</b> Anyone who can reach this port can read every row of every user
 * table. Do not enable {@code start_arrow_flight} on a node reachable from an untrusted network.
 * See {@code ARROW-FLIGHT.md} for the production design (Flight handshake + bearer middleware
 * backed by the database's own {@code IAuthenticator}/{@code IAuthorizer}).
 * <p>
 * Gating is intentionally minimal for this PoC: a boolean ({@code start_arrow_flight}) and a port
 * ({@code arrow_flight_port}) in {@code cassandra.yaml}, checked once at daemon startup - no
 * {@code nodetool enablearrowflight}/{@code disablearrowflight} runtime toggle.
 */
public class ArrowFlightService
{
    private static final Logger logger = LoggerFactory.getLogger(ArrowFlightService.class);

    /** Per-batch target size passed to every {@link ArrowRowAssembler} this service creates. */
    private static final long TARGET_BATCH_BYTES = 16L * 1024 * 1024;
    /** Caps the Arrow allocator so an unbounded analytical scan cannot exhaust node memory. */
    private static final long ALLOCATOR_LIMIT_BYTES = 512L * 1024 * 1024;

    private BufferAllocator allocator;
    private FlightServer server;

    public synchronized void start()
    {
        if (server != null)
            return;

        int port = DatabaseDescriptor.getArrowFlightPort();
        String address = DatabaseDescriptor.getRpcAddress().getHostAddress();
        Location location = Location.forGrpcInsecure(address, port);

        allocator = new RootAllocator(ALLOCATOR_LIMIT_BYTES);
        CassandraFlightProducer producer = new CassandraFlightProducer(allocator, TARGET_BATCH_BYTES, location);
        try
        {
            // Both build() and start() can fail (build() e.g. on a bad Location/port bind setup
            // that surfaces before start() is even reached) - either must release the allocator,
            // or a subsequent start() call creates a second, orphaned one.
            server = FlightServer.builder(allocator, location, producer).build().start();
        }
        catch (Exception e)
        {
            allocator.close();
            allocator = null;
            throw new RuntimeException("Failed to start Arrow Flight service on " + location, e);
        }

        logger.warn("Arrow Flight service started on {}. THIS INTERFACE HAS NO AUTHENTICATION OR " +
                    "AUTHORIZATION - anyone who can reach this port can read every row of every " +
                    "user table. This is a development/PoC-only service; do not enable it on a node " +
                    "reachable from an untrusted network. See ARROW-FLIGHT.md.", location);
    }

    // Accepted PoC limitation: unlike native transport's stop(boolean force) (which distinguishes
    // a graceful drain from an immediate forced close), this always does the equivalent of a plain
    // close with no in-flight-scan draining option - acceptable for a PoC with no runtime
    // enable/disable toggle in front of it; add a force parameter if/when this gets one.
    public synchronized void stop()
    {
        if (server == null)
            return;
        try
        {
            server.close();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            server = null;
            // Arrow's allocator throws IllegalStateException if any buffer is still outstanding
            // when closed (e.g. a leaked batch from a bug elsewhere, or a scan thread that hasn't
            // fully wound down yet). That must not propagate out of here: this is called from
            // CassandraDaemon.destroyClientTransports() -> CassandraDaemon.stop(), and an uncaught
            // exception here would abort the rest of daemon shutdown (StorageService.setRpcReady
            // (false) and JMX server shutdown, both of which run AFTER this in that call chain) -
            // log it and let shutdown continue instead.
            try
            {
                allocator.close();
            }
            catch (IllegalStateException e)
            {
                logger.warn("Arrow Flight allocator close reported outstanding buffers " +
                            "(a leaked batch or a still-running scan); continuing shutdown anyway", e);
            }
            allocator = null;
        }
    }

    public synchronized boolean isRunning()
    {
        return server != null;
    }

    public synchronized int getPort()
    {
        if (server == null)
            throw new IllegalStateException("Arrow Flight service is not running");
        return server.getPort();
    }
}
