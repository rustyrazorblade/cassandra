package io.cassandra.trino.arrowflight.topology;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cassandra.trino.arrowflight.ArrowFlightConfig;

/**
 * Cluster-aware split planning: discovers ring/topology via cassandra-sidecar and computes a
 * token-range split plan per keyspace using {@code cassandra-analytics-common}'s ring-math
 * library (see {@link SplitPlanner}). One instance is created per connector and lives for the
 * connector's lifetime (see {@code ArrowFlightConnector}); {@link #close()} releases the
 * underlying sidecar client and its Vert.x event loop.
 */
public final class ArrowFlightTopologyService implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFlightTopologyService.class);

    private final ArrowFlightConfig config;
    private final Vertx vertx;
    private final SidecarClient sidecarClient;

    public ArrowFlightTopologyService(ArrowFlightConfig config)
    {
        this.config = config;
        SidecarClients.Built built = SidecarClients.build(config.sidecarContactPoints());
        this.vertx = built.vertx();
        this.sidecarClient = built.sidecarClient();
    }

    /** Computes the current split plan for {@code keyspace} by discovering its ring afresh. */
    public CompletableFuture<List<SplitPlan>> splitPlan(String keyspace)
    {
        CompletableFuture<NodeSettings> nodeSettingsFuture = sidecarClient.nodeSettings();
        CompletableFuture<RingResponse> ringFuture = sidecarClient.ring(keyspace);
        CompletableFuture<SchemaResponse> schemaFuture = sidecarClient.schema(keyspace);

        return CompletableFuture.allOf(nodeSettingsFuture, ringFuture, schemaFuture)
                                 .thenApply(ignored -> SplitPlanner.plan(
                                     nodeSettingsFuture.join(),
                                     ringFuture.join(),
                                     schemaFuture.join(),
                                     keyspace,
                                     config.splitsPerNode(),
                                     config.port()));
    }

    @Override
    public void close()
    {
        try
        {
            sidecarClient.close();
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to close sidecar client", e);
        }
        // VertxHttpClient#close only closes its WebClient, not the Vertx instance that owns the
        // event loop - close it explicitly. Best-effort/fire-and-forget, matching this codebase's
        // other close() handling (see ArrowFlightClient#closeQuietly): daemon threads mean a
        // failure here cannot leak past JVM shutdown.
        vertx.close();
    }
}
