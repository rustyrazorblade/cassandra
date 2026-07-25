package io.cassandra.trino.arrowflight;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    // Shared across every aggregated split's page source, so fetching each subrange (token range
    // x replica) that a pushed-down aggregation collapses into one split (see
    // ArrowFlightSplitManager) can run concurrently instead of serially - see
    // ArrowFlightAggregatingPageSource's javadoc. Cached: aggregated queries are the only caller,
    // and their subrange-count-driven concurrency is bursty rather than steady, so threads are
    // reclaimed between queries instead of held idle.
    private final ExecutorService aggregationExecutor;

    public ArrowFlightConnector(ArrowFlightConfig config)
    {
        this.config = config;
        this.allocator = new RootAllocator();
        this.client = new ArrowFlightClient(allocator);
        this.topology = new ArrowFlightTopologyService(config);
        this.aggregationExecutor = Executors.newCachedThreadPool();
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
        return new ArrowFlightPageSourceProvider(client, aggregationExecutor);
    }

    @Override
    public void shutdown()
    {
        topology.close();
        allocator.close();
        aggregationExecutor.shutdown();
    }
}
