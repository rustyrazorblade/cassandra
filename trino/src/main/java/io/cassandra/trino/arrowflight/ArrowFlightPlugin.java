package io.cassandra.trino.arrowflight;

import java.util.List;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

/** Trino plugin entry point, registered via {@code META-INF/services/io.trino.spi.Plugin}. */
public class ArrowFlightPlugin implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return List.of(new ArrowFlightConnectorFactory());
    }
}
