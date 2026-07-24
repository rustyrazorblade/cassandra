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

/**
 * PoC scope: no filter/projection pushdown (matches the server, see
 * {@code CassandraFlightProducer#getStream}) - every requested column is read and every row
 * of the table is streamed; Trino applies any {@code WHERE}/{@code LIMIT} client-side, which
 * is correct (if not as fast as server-side pushdown would be).
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
        List<ArrowFlightColumnHandle> projected = columns.stream()
                                                          .map(ArrowFlightColumnHandle.class::cast)
                                                          .toList();
        return new ArrowFlightPageSource(client, (ArrowFlightSplit) split, projected);
    }
}
