package io.cassandra.trino.arrowflight;

import java.util.List;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

/**
 * One unit of work: the whole table, read from a single Flight endpoint.
 *
 * <p><b>PoC scope</b> (matches the Cassandra-side server, see {@code CassandraFlightProducer}
 * and {@code trino/README.md}): the server's {@code getFlightInfo} always returns exactly one
 * {@link org.apache.arrow.flight.FlightEndpoint} covering the table's entire local range, with
 * no token-range splitting. So this connector's split manager emits exactly one split per
 * table - no parallelism yet. When the server gains real token-range splitting, this class is
 * the seam to extend into one split per endpoint.
 */
public record ArrowFlightSplit(String keyspace, String table, String host, int port) implements ConnectorSplit
{
    @Override
    public List<HostAddress> getAddresses()
    {
        return List.of(HostAddress.fromParts(host, port));
    }
}
