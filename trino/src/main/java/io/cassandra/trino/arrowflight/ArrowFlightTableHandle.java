package io.cassandra.trino.arrowflight;

import java.util.Optional;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import io.cassandra.trino.arrowflight.ticket.AggregationSpec;
import io.cassandra.trino.arrowflight.ticket.FilterExpression;

/**
 * Identifies a Cassandra table to scan: its keyspace and table name (also exactly the
 * {@code [keyspace, table]} {@code FlightDescriptor} path the Cassandra Arrow Flight service
 * expects - see {@code CassandraFlightProducer}), plus any predicate/aggregation pushdown state
 * accumulated by {@link ArrowFlightMetadata#applyFilter}/{@code #applyAggregation}. Both are
 * embedded directly in the Flight ticket per split (see {@code ArrowFlightPageSourceProvider}) -
 * this handle is the only place that state is carried between planning and scan time.
 */
public record ArrowFlightTableHandle(
    String keyspace,
    String table,
    Optional<FilterExpression> filter,
    Optional<AggregationSpec> aggregation) implements ConnectorTableHandle
{
    /** A bare table handle with no pushed-down filter/aggregation. */
    public static ArrowFlightTableHandle of(String keyspace, String table)
    {
        return new ArrowFlightTableHandle(keyspace, table, Optional.empty(), Optional.empty());
    }

    public ArrowFlightTableHandle withFilter(FilterExpression newFilter)
    {
        return new ArrowFlightTableHandle(keyspace, table, Optional.of(newFilter), aggregation);
    }

    public ArrowFlightTableHandle withAggregation(AggregationSpec newAggregation)
    {
        return new ArrowFlightTableHandle(keyspace, table, filter, Optional.of(newAggregation));
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(keyspace, table);
    }
}
