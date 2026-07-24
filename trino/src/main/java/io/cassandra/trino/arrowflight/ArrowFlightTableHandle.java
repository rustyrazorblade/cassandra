package io.cassandra.trino.arrowflight;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

/**
 * Identifies a Cassandra table to scan: its keyspace and table name, which is also exactly
 * the {@code [keyspace, table]} {@code FlightDescriptor} path the Cassandra Arrow Flight
 * service expects (see {@code CassandraFlightProducer}).
 */
public record ArrowFlightTableHandle(String keyspace, String table) implements ConnectorTableHandle
{
    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(keyspace, table);
    }
}
