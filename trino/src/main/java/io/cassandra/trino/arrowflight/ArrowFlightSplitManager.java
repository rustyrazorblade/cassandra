package io.cassandra.trino.arrowflight;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

/**
 * PoC scope (matches the Cassandra-side server, see {@code CassandraFlightProducer} and
 * {@code trino/README.md}): exactly one split per table, since {@code getFlightInfo} always
 * returns exactly one {@link org.apache.arrow.flight.FlightEndpoint} covering the whole table -
 * no token-range splitting, no parallelism yet.
 */
public class ArrowFlightSplitManager implements ConnectorSplitManager
{
    private final ArrowFlightConfig config;

    public ArrowFlightSplitManager(ArrowFlightConfig config)
    {
        this.config = config;
    }

    @Override
    public ConnectorSplitSource getSplits(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorTableHandle table,
        DynamicFilter dynamicFilter,
        Constraint constraint)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        ArrowFlightSplit split = new ArrowFlightSplit(handle.keyspace(), handle.table(), config.host(), config.port());
        return new FixedSplitSource(split);
    }
}
