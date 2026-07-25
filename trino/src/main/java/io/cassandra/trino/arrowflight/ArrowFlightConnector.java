package io.cassandra.trino.arrowflight;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    // ArrowFlightAggregatingPageSource's javadoc. Fixed-size and bounded, not cached/unbounded: a
    // real cluster can have thousands of subranges (nodes x vnodes) for a single aggregation
    // query, and an unbounded pool would open that many simultaneous Flight connections at once -
    // a thread/connection storm against the cluster. This bound caps concurrent in-flight fetches
    // per query (and across every concurrently-running aggregation query sharing this connector
    // instance); excess subrange fetches simply queue for a free thread instead of all firing at
    // once.
    private static final int AGGREGATION_FETCH_CONCURRENCY = 64;
    private final ExecutorService aggregationExecutor;

    public ArrowFlightConnector(ArrowFlightConfig config)
    {
        this.config = config;
        this.allocator = new RootAllocator();
        this.client = new ArrowFlightClient(allocator);
        this.topology = new ArrowFlightTopologyService(config);
        this.aggregationExecutor = Executors.newFixedThreadPool(AGGREGATION_FETCH_CONCURRENCY);
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
        aggregationExecutor.shutdown();
        try
        {
            // Bounded wait for in-flight subrange fetches before tearing down the allocator/client
            // they read from - otherwise a still-running merge could race against allocator.close().
            if (!aggregationExecutor.awaitTermination(10, TimeUnit.SECONDS))
                aggregationExecutor.shutdownNow();
        }
        catch (InterruptedException e)
        {
            aggregationExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        topology.close();
        allocator.close();
    }
}
