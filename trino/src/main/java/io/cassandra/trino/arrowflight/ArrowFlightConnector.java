package io.cassandra.trino.arrowflight;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import io.cassandra.trino.arrowflight.topology.ArrowFlightTopologyService;

public class ArrowFlightConnector implements Connector
{
    private final ArrowFlightConfig config;
    private final BufferAllocator allocator;
    private final ArrowFlightClient client;
    private final ArrowFlightTopologyService topology;

    public ArrowFlightConnector(ArrowFlightConfig config)
    {
        this.config = config;
        this.allocator = new RootAllocator();
        this.client = new ArrowFlightClient(allocator);
        this.topology = new ArrowFlightTopologyService(config);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return ArrowFlightTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return new ArrowFlightMetadata(config, client);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new ArrowFlightSplitManager(topology);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return new ArrowFlightPageSourceProvider(client);
    }

    @Override
    public void shutdown()
    {
        topology.close();
        allocator.close();
    }
}
