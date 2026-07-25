package io.cassandra.trino.arrowflight;

import java.util.List;
import java.util.Optional;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import io.cassandra.trino.arrowflight.ticket.ArrowFlightTicket;

/**
 * Builds the per-split Flight ticket - {@code keyspace}/{@code table} plus the split's own
 * {@code tokenRange} and the table handle's pushed-down {@code filter}/{@code aggregation} (see
 * {@code ArrowFlightMetadata#applyFilter}/{@code #applyAggregation}) - and streams it via
 * {@link ArrowFlightPageSource}. Every requested (projected) column is read; Trino applies any
 * part of the query this connector didn't push down.
 */
public class ArrowFlightPageSourceProvider implements ConnectorPageSourceProvider
{
    private final ArrowFlightClient client;

    public ArrowFlightPageSourceProvider(ArrowFlightClient client)
    {
        this.client = client;
    }

    @Override
    public ConnectorPageSource createPageSource(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorSplit split,
        ConnectorTableHandle table,
        Optional<ConnectorTableCredentials> credentials,
        List<ColumnHandle> columns,
        DynamicFilter dynamicFilter)
    {
        ArrowFlightSplit arrowSplit = (ArrowFlightSplit) split;
        ArrowFlightTableHandle tableHandle = (ArrowFlightTableHandle) table;

        ArrowFlightTicket ticket = ArrowFlightTicket.of(arrowSplit.keyspace(), arrowSplit.table())
                                                     .withTokenRange(arrowSplit.tokenRange());
        if (tableHandle.filter().isPresent())
            ticket = ticket.withFilter(tableHandle.filter().get());
        if (tableHandle.aggregation().isPresent())
            ticket = ticket.withAggregation(tableHandle.aggregation().get());

        List<ArrowFlightColumnHandle> projected = columns.stream()
                                                          .map(ArrowFlightColumnHandle.class::cast)
                                                          .toList();
        return new ArrowFlightPageSource(client, ticket, arrowSplit.replicas(), projected);
    }
}
