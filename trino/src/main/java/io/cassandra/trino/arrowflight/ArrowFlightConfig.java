package io.cassandra.trino.arrowflight;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Connector configuration read from the catalog properties file, e.g.
 * {@code trino/catalog/arrow_flight.properties}:
 *
 * <pre>
 * connector.name=cassandra_arrow_flight
 * arrow-flight.host=127.0.0.1
 * arrow-flight.port=9143
 * sidecar.contact-points=127.0.0.1:9043
 * arrow-flight.splits-per-node=4
 * </pre>
 *
 * <p>{@code arrow-flight.host}/{@code arrow-flight.port} remain a single bootstrap contact point
 * used only for schema discovery (the Arrow schema is uniform cluster-wide, so any live node will
 * do). Actual scan routing is cluster-aware: {@code sidecar.contact-points} seeds ring/topology
 * discovery via cassandra-sidecar (see {@code io.cassandra.trino.arrowflight.topology}), and each
 * computed split targets its owning replica(s) directly rather than the bootstrap host. Sidecar
 * has no notion of this connector's custom {@code arrow_flight_port} (not a stock
 * Cassandra/sidecar concept), so it's assumed uniform across the cluster and taken from
 * {@code arrow-flight.port} rather than discovered per-node.
 */
public record ArrowFlightConfig(
    String host,
    int port,
    List<SidecarContactPoint> sidecarContactPoints,
    int splitsPerNode)
{
    /** Matches the Cassandra-side default ({@code arrow_flight_port} in cassandra.yaml). */
    public static final int DEFAULT_PORT = 9143;

    /** cassandra-sidecar's stock default REST port. */
    public static final int DEFAULT_SIDECAR_PORT = 9043;

    /**
     * Target splits per node. A fixed small multiplier rather than a fixed total, so parallelism
     * scales with cluster size automatically; see {@code io.cassandra.trino.arrowflight.topology}
     * for how this feeds {@code TokenPartitioner}.
     */
    public static final int DEFAULT_SPLITS_PER_NODE = 4;

    /** One {@code host:port} sidecar contact point, e.g. {@code 127.0.0.1:9043}. */
    public record SidecarContactPoint(String host, int port)
    {
        static SidecarContactPoint parse(String hostPort)
        {
            int colon = hostPort.lastIndexOf(':');
            if (colon < 0)
                return new SidecarContactPoint(hostPort, DEFAULT_SIDECAR_PORT);
            return new SidecarContactPoint(
                hostPort.substring(0, colon),
                Integer.parseInt(hostPort.substring(colon + 1)));
        }
    }

    public static ArrowFlightConfig fromMap(Map<String, String> config)
    {
        String host = require(config, "arrow-flight.host");
        int port = config.containsKey("arrow-flight.port")
                   ? Integer.parseInt(config.get("arrow-flight.port"))
                   : DEFAULT_PORT;
        List<SidecarContactPoint> sidecarContactPoints = parseContactPoints(require(config, "sidecar.contact-points"));
        int splitsPerNode = config.containsKey("arrow-flight.splits-per-node")
                            ? Integer.parseInt(config.get("arrow-flight.splits-per-node"))
                            : DEFAULT_SPLITS_PER_NODE;
        return new ArrowFlightConfig(host, port, sidecarContactPoints, splitsPerNode);
    }

    private static List<SidecarContactPoint> parseContactPoints(String csv)
    {
        List<SidecarContactPoint> contactPoints = new ArrayList<>();
        for (String hostPort : csv.split(","))
        {
            String trimmed = hostPort.trim();
            if (!trimmed.isEmpty())
                contactPoints.add(SidecarContactPoint.parse(trimmed));
        }
        if (contactPoints.isEmpty())
            throw new IllegalArgumentException("sidecar.contact-points must contain at least one host:port");
        return contactPoints;
    }

    private static String require(Map<String, String> config, String key)
    {
        String value = config.get(key);
        if (value == null || value.isBlank())
            throw new IllegalArgumentException("Missing required config property: " + key);
        return value;
    }
}
