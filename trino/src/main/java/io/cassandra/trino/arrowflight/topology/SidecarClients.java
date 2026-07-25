package io.cassandra.trino.arrowflight.topology;

import java.util.List;
import java.util.stream.Collectors;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.apache.cassandra.sidecar.client.HttpClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClientConfigImpl;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.SimpleSidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.VertxHttpClient;
import org.apache.cassandra.sidecar.client.VertxRequestExecutor;
import org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;

import io.cassandra.trino.arrowflight.ArrowFlightConfig;

/**
 * Builds a {@link SidecarClient}. {@code org.apache.cassandra:sidecar-client} publishes only the
 * transport-agnostic {@code HttpClient} interface - the actual Vert.x-based implementation
 * ({@link VertxHttpClient}/{@link VertxRequestExecutor}) ships in the separate
 * {@code org.apache.cassandra:sidecar-vertx-client} artifact. This wiring mirrors
 * {@code org.apache.cassandra.clients.Sidecar#from}/{@code #buildClient} in
 * {@code cassandra-analytics-sidecar-client} (read from that artifact's published sources jar,
 * since neither {@code sidecar-client} nor {@code sidecar-vertx-client} publish one) - unshaded
 * package names here since we depend on the plain artifacts, not analytics' shaded copy.
 */
final class SidecarClients
{
    private SidecarClients()
    {
    }

    /** A built client plus the {@link Vertx} instance backing it - both must be closed together. */
    record Built(Vertx vertx, SidecarClient sidecarClient)
    {
    }

    static Built build(List<ArrowFlightConfig.SidecarContactPoint> contactPoints)
    {
        List<SidecarInstance> instances = contactPoints.stream()
                                                        .map(cp -> (SidecarInstance) new SidecarInstanceImpl(cp.host(), cp.port()))
                                                        .collect(Collectors.toList());
        SidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(instances);

        // Daemon threads so a leaked/forgotten close() doesn't keep the JVM alive - matches
        // ArrowFlightClient/ArrowFlightConnector's other "best-effort" resource handling.
        Vertx vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(true));

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder<>()
            .ssl(false)
            .userAgent("cassandra-arrow-flight-trino-connector")
            .build();
        SidecarClientConfig sidecarClientConfig = SidecarClientConfigImpl.builder().build();
        RetryPolicy retryPolicy = new ExponentialBackoffRetryPolicy(
            sidecarClientConfig.maxRetries(),
            sidecarClientConfig.retryDelayMillis(),
            sidecarClientConfig.maxRetryDelayMillis());

        VertxHttpClient httpClient = new VertxHttpClient(vertx, httpClientConfig);
        VertxRequestExecutor requestExecutor = new VertxRequestExecutor(httpClient);
        SidecarClient sidecarClient = new SidecarClient(instancesProvider, requestExecutor, sidecarClientConfig, retryPolicy);

        return new Built(vertx, sidecarClient);
    }
}
