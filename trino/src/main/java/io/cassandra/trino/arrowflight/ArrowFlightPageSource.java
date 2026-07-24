package io.cassandra.trino.arrowflight;

import java.util.List;

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VectorSchemaRoot;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

/**
 * Streams the split's single Flight {@code DoGet}, converting each Arrow batch
 * ({@link VectorSchemaRoot}) into a Trino {@link Page} via {@link ArrowPageBuilder}.
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
        ArrowFlightSplit split,
        List<ArrowFlightColumnHandle> columns)
    {
        this.columnNames = columns.stream().map(ArrowFlightColumnHandle::name).toList();
        this.columnTypes = columns.stream().map(ArrowFlightColumnHandle::type).toList();

        FlightInfo info = client.getFlightInfo(split.host(), split.port(), split.keyspace(), split.table());
        if (info.getEndpoints().isEmpty())
            throw new IllegalStateException(
                "Cassandra Arrow Flight service returned no endpoint for " + split.keyspace() + "." + split.table());
        Ticket ticket = info.getEndpoints().get(0).getTicket();
        this.streamHandle = client.openStream(split.host(), split.port(), ticket);
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished)
            return null;
        try
        {
            FlightStream stream = streamHandle.stream();
            if (!stream.next())
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
