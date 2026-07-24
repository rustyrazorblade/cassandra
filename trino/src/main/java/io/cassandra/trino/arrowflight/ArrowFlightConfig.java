package io.cassandra.trino.arrowflight;

import java.util.Map;

/**
 * Connector configuration read from the catalog properties file, e.g.
 * {@code trino/catalog/arrow_flight.properties}:
 *
 * <pre>
 * connector.name=cassandra_arrow_flight
 * arrow-flight.host=127.0.0.1
 * arrow-flight.port=9143
 * </pre>
 */
public record ArrowFlightConfig(String host, int port)
{
    /** Matches the Cassandra-side default ({@code arrow_flight_port} in cassandra.yaml). */
    public static final int DEFAULT_PORT = 9143;

    public static ArrowFlightConfig fromMap(Map<String, String> config)
    {
        String host = require(config, "arrow-flight.host");
        int port = config.containsKey("arrow-flight.port")
                   ? Integer.parseInt(config.get("arrow-flight.port"))
                   : DEFAULT_PORT;
        return new ArrowFlightConfig(host, port);
    }

    private static String require(Map<String, String> config, String key)
    {
        String value = config.get(key);
        if (value == null || value.isBlank())
            throw new IllegalArgumentException("Missing required config property: " + key);
        return value;
    }
}
