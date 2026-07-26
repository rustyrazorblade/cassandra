package io.cassandra.trino.arrowflight;

import java.util.List;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VectorSchemaRoot;

import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

import io.cassandra.trino.arrowflight.ticket.ArrowFlightTicket;

/**
 * Streams the split's Flight {@code DoGet} for its resolved token range (plus any pushed-down
 * filter/aggregation from the table handle), converting each Arrow batch
 * ({@link VectorSchemaRoot}) into a Trino {@link Page} via {@link ArrowPageBuilder}.
 *
 * <p>Calls {@code DoGet} directly against the ticket bytes - no {@code GetFlightInfo} round trip
 * per split (that only happens once, at schema-resolution time - see
 * {@code ArrowFlightMetadata#arrowSchema}). Tries the split's candidate replicas in order,
 * opening the stream against the first one that accepts the connection/request (see
 * {@code ArrowFlightSplit}'s javadoc for the replica-selection simplification this implements).
 */
public class ArrowFlightPageSource implements ConnectorPageSource
{
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final ArrowFlightClient.StreamHandle streamHandle;
    private boolean finished;
    private long completedPositions;

    public ArrowFlightPageSource(
        ArrowFlightClient client,
        ArrowFlightTicket ticket,
        List<HostAddress> replicas,
        List<ArrowFlightColumnHandle> columns)
    {
        this.columnNames = columns.stream().map(ArrowFlightColumnHandle::name).toList();
        this.columnTypes = columns.stream().map(ArrowFlightColumnHandle::type).toList();

        if (replicas.isEmpty())
            throw new IllegalStateException(
                "Split for " + ticket.keyspace() + "." + ticket.table() + " has no candidate replicas");

        Ticket flightTicket = new Ticket(ticket.toJsonBytes());
        this.streamHandle = client.openFirstAvailable(replicas, flightTicket);
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished)
            return null;
        try
        {
            FlightStream stream = streamHandle.stream();
            boolean hasNext = streamHandle.hasPreloadedFirst() ? streamHandle.consumePreloadedHasNext() : stream.next();
            if (!hasNext)
            {
                finished = true;
                return null;
            }
            Page page = ArrowPageBuilder.toPage(stream.getRoot(), columnNames, columnTypes);
            completedPositions += page.getPositionCount();
            return SourcePage.create(page);
        }
        catch (RuntimeException e)
        {
            close();
            throw e;
        }
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public java.util.OptionalLong getCompletedPositions()
    {
        return java.util.OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        finished = true;
        streamHandle.close();
    }
}
