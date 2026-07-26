package io.cassandra.trino.arrowflight;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;

import io.trino.spi.HostAddress;
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

    /** Retry budget against a single replica for a retryable (UNAVAILABLE) rejection - see {@link #openFirstAvailable}. */
    private static final int MAX_ATTEMPTS_PER_REPLICA = 5;
    private static final long INITIAL_BACKOFF_MILLIS = 200;
    private static final long MAX_BACKOFF_MILLIS = 2000;

    /**
     * Tries each replica in order, opening a {@code DoGet} stream against the first one that
     * accepts it - see {@code ArrowFlightSplit}'s javadoc for the replica-selection simplification
     * this implements. Shared by {@link ArrowFlightPageSource} (one subrange per split) and
     * {@link ArrowFlightAggregatingPageSource} (every pushed-down aggregation's subrange, fetched
     * and merged within one split).
     * <p>
     * A gRPC streaming call does not block for the server's response at {@code getStream()} time -
     * an error the server throws before ever sending a batch (e.g. an admission-control rejection,
     * see {@code CassandraFlightProducer}'s scan concurrency limit) only surfaces once the client
     * actually reads from the stream. So retry validation here eagerly calls {@code stream.next()}
     * once per attempt: a retryable ({@code UNAVAILABLE}) failure there is retried with backoff
     * against the same replica before moving on, since on a small cluster there may be no other
     * replica to fail over to at all. Any other status (a real error, not transient backpressure)
     * is not retried. The eagerly-consumed first {@code next()} result is preserved on the returned
     * {@link StreamHandle} (see {@link StreamHandle#hasPreloadedFirst()}) so the caller's own first
     * read doesn't silently skip a row batch. Retrying past this point (once real data has already
     * been returned to a caller) is deliberately out of scope - only the pre-first-batch case is
     * safe to retry from scratch here.
     */
    public StreamHandle openFirstAvailable(List<HostAddress> replicas, Ticket ticket)
    {
        if (replicas.isEmpty())
            throw new IllegalStateException("No candidate replicas for subrange");

        RuntimeException lastFailure = null;
        for (HostAddress replica : replicas)
        {
            long backoffMillis = INITIAL_BACKOFF_MILLIS;
            for (int attempt = 0; attempt < MAX_ATTEMPTS_PER_REPLICA; attempt++)
            {
                StreamHandle handle = null;
                try
                {
                    handle = openStream(replica.getHostText(), replica.getPort(), ticket);
                    boolean hasFirstBatch = handle.stream().next();
                    return handle.withPreloadedFirst(hasFirstBatch);
                }
                catch (FlightRuntimeException e)
                {
                    if (handle != null)
                        handle.close();
                    lastFailure = e;
                    boolean retryable = e.status().code() == FlightStatusCode.UNAVAILABLE;
                    if (!retryable || attempt == MAX_ATTEMPTS_PER_REPLICA - 1)
                        break; // not transient backpressure, or out of retries against this replica
                    sleepBackoff(backoffMillis);
                    backoffMillis = Math.min(backoffMillis * 2, MAX_BACKOFF_MILLIS);
                }
                catch (IllegalStateException e)
                {
                    if (handle != null)
                        handle.close();
                    lastFailure = e;
                    break;
                }
            }
        }
        throw lastFailure;
    }

    private static void sleepBackoff(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted during scan-admission retry backoff", e);
        }
    }

    /** An open Flight stream paired with the client that owns it; {@link #close} releases both. */
    public static final class StreamHandle implements AutoCloseable
    {
        private final FlightClient client;
        private final FlightStream stream;
        private boolean firstBatchPreloaded;
        private boolean preloadedHasNext;

        StreamHandle(FlightClient client, FlightStream stream)
        {
            this.client = client;
            this.stream = stream;
        }

        /** Records that {@code stream.next()} was already called once, as part of retry validation - see {@link #openFirstAvailable}. */
        StreamHandle withPreloadedFirst(boolean hasNext)
        {
            this.firstBatchPreloaded = true;
            this.preloadedHasNext = hasNext;
            return this;
        }

        /**
         * True if this handle's very first {@code next()} call has already happened (as part of
         * {@link #openFirstAvailable}'s retry validation) - the caller's own first read must use
         * {@link #consumePreloadedHasNext()} instead of calling {@code stream().next()} again, or
         * it will silently skip that first batch.
         */
        public boolean hasPreloadedFirst()
        {
            return firstBatchPreloaded;
        }

        /** One-shot: returns the preloaded {@code next()} result and clears the flag. */
        public boolean consumePreloadedHasNext()
        {
            if (!firstBatchPreloaded)
                throw new IllegalStateException("no preloaded first batch pending on this handle");
            firstBatchPreloaded = false;
            return preloadedHasNext;
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
