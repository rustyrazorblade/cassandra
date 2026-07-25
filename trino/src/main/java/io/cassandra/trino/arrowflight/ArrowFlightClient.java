package io.cassandra.trino.arrowflight;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;

import io.trino.spi.connector.SchemaTableName;

import io.cassandra.trino.arrowflight.ticket.ArrowFlightTicket;

/**
 * Thin wrapper over the Arrow Flight Java client for talking to the Cassandra Arrow Flight
 * service (see {@code org.apache.cassandra.arrow.CassandraFlightProducer} and {@code
 * ArrowFlightService} in the Cassandra source tree, and {@code ArrowFlightEndToEndTest} for
 * the client-side usage pattern this mirrors).
 *
 * <p>Every call opens and closes its own short-lived {@link FlightClient} (matching the PoC's
 * no-connection-pooling scope); a production connector would pool these per host.
 */
public final class ArrowFlightClient
{
    private final BufferAllocator allocator;

    public ArrowFlightClient(BufferAllocator allocator)
    {
        this.allocator = allocator;
    }

    /**
     * Every {@code keyspace.table} the service exposes, via {@code ListFlights}. The server
     * enumerates every user keyspace's tables (see {@code CassandraFlightProducer#listFlights}).
     * Unaffected by the ticket-JSON protocol below - {@code ListFlights} descriptors are still
     * plain {@code [keyspace, table]} paths.
     */
    public List<SchemaTableName> listTables(String host, int port)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        FlightClient client = connect(host, port);
        try
        {
            for (FlightInfo info : client.listFlights(org.apache.arrow.flight.Criteria.ALL))
            {
                List<String> path = info.getDescriptor().getPath();
                if (path.size() == 2)
                    tables.add(new SchemaTableName(path.get(0), path.get(1)));
            }
        }
        finally
        {
            closeQuietly(client);
        }
        return tables;
    }

    /**
     * Resolves a table's {@link FlightInfo} - its Arrow schema plus its per-token-range
     * {@link org.apache.arrow.flight.FlightEndpoint}s - via {@code GetFlightInfo}, using the same
     * ticket JSON shape as {@code DoGet} carried in the descriptor's {@code command} bytes (see
     * {@code ARROW-FLIGHT.md}: {@code tokenRange}/{@code filter} are accepted but ignored for
     * schema-resolution purposes there; only {@code aggregation} changes the returned schema).
     */
    public FlightInfo getFlightInfo(String host, int port, ArrowFlightTicket ticket)
    {
        FlightClient client = connect(host, port);
        try
        {
            return client.getInfo(FlightDescriptor.command(ticket.toJsonBytes()));
        }
        finally
        {
            closeQuietly(client);
        }
    }

    private static void closeQuietly(FlightClient client)
    {
        try
        {
            client.close();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        catch (Exception ignored)
        {
            // best-effort
        }
    }

    /** Opens a {@code DoGet} stream for the given ticket; the caller must close the returned handle. */
    public StreamHandle openStream(String host, int port, Ticket ticket)
    {
        FlightClient client = connect(host, port);
        try
        {
            FlightStream stream = client.getStream(ticket);
            return new StreamHandle(client, stream);
        }
        catch (RuntimeException e)
        {
            try
            {
                client.close();
            }
            catch (Exception suppressed)
            {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private FlightClient connect(String host, int port)
    {
        return FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build();
    }

    /** An open Flight stream paired with the client that owns it; {@link #close} releases both. */
    public static final class StreamHandle implements AutoCloseable
    {
        private final FlightClient client;
        private final FlightStream stream;

        StreamHandle(FlightClient client, FlightStream stream)
        {
            this.client = client;
            this.stream = stream;
        }

        public FlightStream stream()
        {
            return stream;
        }

        @Override
        public void close()
        {
            try
            {
                stream.close();
            }
            catch (Exception ignored)
            {
                // best-effort
            }
            try
            {
                client.close();
            }
            catch (Exception ignored)
            {
                // best-effort
            }
        }
    }
}
