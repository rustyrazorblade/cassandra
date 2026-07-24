package io.cassandra.trino.arrowflight;

import java.util.Map;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

public class ArrowFlightConnectorFactory implements ConnectorFactory
{
    /** The {@code connector.name} value in the catalog properties file. */
    public static final String NAME = "cassandra_arrow_flight";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return new ArrowFlightConnector(ArrowFlightConfig.fromMap(config));
    }
}
