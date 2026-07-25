package io.cassandra.trino.arrowflight;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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
 *
 * <p>{@code mergePlanJson} is present exactly when {@code aggregation} is: it tells
 * {@code ArrowFlightAggregatingPageSource} how to combine every sub-range's partial aggregate
 * value(s) into the single final row Trino expects per group (see {@link AggregationMergePlan}) -
 * required because {@link ArrowFlightSplitManager} collapses every sub-range into one split
 * whenever an aggregation is pushed down (Trino's {@code applyAggregation} SPI never merges
 * partial per-split aggregates itself).
 *
 * <p>{@code filter}/{@code mergePlan} are stored as pre-serialized JSON strings, not as
 * {@link FilterExpression}/{@code List<AggregationMergePlan>} objects directly, even though
 * Trino serializes this whole record via Jackson to ship {@code ConnectorTableHandle}s across its
 * internal coordinator/worker codec (even in a single-node deployment): both those types are
 * sealed interfaces, and Trino's {@code ObjectMapperProvider} disables Jackson's
 * annotation-driven mechanisms (relying only on Java records' built-in structural introspection),
 * so a {@code @JsonTypeInfo}-annotated sealed interface field silently loses its type
 * discriminator on the wire - confirmed by decompiling {@code io.airlift.json.BaseJacksonProvider}
 * (see {@code FilterExpression}'s javadoc) and by a live query failing with "missing type id
 * property" despite the annotations being present in the deployed bytecode. {@code keyspace}/
 * {@code table}/{@code aggregation} (a plain, non-polymorphic {@link AggregationSpec}) are
 * unaffected and round-trip via ordinary record introspection with no special handling needed.
 */
public record ArrowFlightTableHandle(
    String keyspace,
    String table,
    Optional<String> filterJson,
    Optional<AggregationSpec> aggregation,
    Optional<String> mergePlanJson) implements ConnectorTableHandle
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> JSON_OBJECT = new TypeReference<>() {};
    private static final TypeReference<List<Map<String, Object>>> JSON_OBJECT_LIST = new TypeReference<>() {};

    /** A bare table handle with no pushed-down filter/aggregation. */
    public static ArrowFlightTableHandle of(String keyspace, String table)
    {
        return new ArrowFlightTableHandle(keyspace, table, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public ArrowFlightTableHandle withFilter(FilterExpression newFilter)
    {
        return new ArrowFlightTableHandle(keyspace, table, Optional.of(writeJson(newFilter.toJson())), aggregation, mergePlanJson);
    }

    public ArrowFlightTableHandle withAggregation(AggregationSpec newAggregation, List<AggregationMergePlan> newMergePlan)
    {
        List<Map<String, Object>> mergePlanJsonList = newMergePlan.stream().map(AggregationMergePlan::toJson).toList();
        return new ArrowFlightTableHandle(keyspace, table, filterJson, Optional.of(newAggregation), Optional.of(writeJson(mergePlanJsonList)));
    }

    /** The pushed-down filter, if any - lazily reconstructed from {@link #filterJson()}. */
    public Optional<FilterExpression> filter()
    {
        return filterJson.map(json -> FilterExpression.fromJson(readJsonObject(json)));
    }

    /** The pushed-down aggregation's per-output-column merge plan, if any - see {@link AggregationMergePlan}. */
    public Optional<List<AggregationMergePlan>> mergePlan()
    {
        return mergePlanJson.map(json -> readJsonObjectList(json).stream().map(AggregationMergePlan::fromJson).toList());
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(keyspace, table);
    }

    private static String writeJson(Object value)
    {
        try
        {
            return MAPPER.writeValueAsString(value);
        }
        catch (com.fasterxml.jackson.core.JsonProcessingException e)
        {
            throw new IllegalStateException("Failed to serialize " + value, e);
        }
    }

    private static Map<String, Object> readJsonObject(String json)
    {
        try
        {
            return MAPPER.readValue(json, JSON_OBJECT);
        }
        catch (com.fasterxml.jackson.core.JsonProcessingException e)
        {
            throw new IllegalStateException("Failed to parse: " + json, e);
        }
    }

    private static List<Map<String, Object>> readJsonObjectList(String json)
    {
        try
        {
            return MAPPER.readValue(json, JSON_OBJECT_LIST);
        }
        catch (com.fasterxml.jackson.core.JsonProcessingException e)
        {
            throw new IllegalStateException("Failed to parse: " + json, e);
        }
    }
}
