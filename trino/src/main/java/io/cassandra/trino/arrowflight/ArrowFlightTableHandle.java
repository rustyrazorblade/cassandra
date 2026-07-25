package io.cassandra.trino.arrowflight;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

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
 * <p>{@code enforcedConstraint} is the raw {@code TupleDomain} already pushed down (as opposed to
 * {@code filterJson}, its translated wire form) - {@code ArrowFlightMetadata#applyFilter} needs it
 * to detect when Trino re-invokes {@code applyFilter} with a constraint that adds nothing beyond
 * what is already enforced (which the SPI contract explicitly allows/expects the framework to do,
 * e.g. across successive optimizer passes) and return {@code Optional.empty()} rather than
 * re-translating and re-wrapping the same filter around itself on every call - see that method's
 * javadoc for the bug this fixes. Deliberately {@code @JsonIgnore}d rather than made to survive
 * the coordinator-to-worker Jackson round-trip: unlike {@code filterJson}/{@code mergePlanJson},
 * {@code TupleDomain<ColumnHandle>} needs Trino's own type-aware codec (a {@code TypeManager} to
 * resolve column types, connector-specific {@code ColumnHandle} resolution, etc.) to deserialize -
 * machinery only Trino's real runtime provides, not a plain {@link ObjectMapper} - and this field
 * is read only during coordinator-side planning ({@code applyFilter} is never called again once a
 * handle has crossed to a worker), so a worker-side handle correctly never needs a real value
 * here. Absent from incoming JSON, it deserializes to {@code null}; every reader must treat that
 * the same as {@link TupleDomain#all()} (see {@code ArrowFlightMetadata#applyFilter}).
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
    @JsonIgnore TupleDomain<ColumnHandle> enforcedConstraint,
    Optional<String> filterJson,
    Optional<AggregationSpec> aggregation,
    Optional<String> mergePlanJson) implements ConnectorTableHandle
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> JSON_OBJECT = new TypeReference<>() {};
    private static final TypeReference<List<Map<String, Object>>> JSON_OBJECT_LIST = new TypeReference<>() {};

    /**
     * Overrides the generated accessor to normalize the {@code @JsonIgnore}'d field's
     * deserialize-with-no-JSON-value result ({@code null}, since the canonical constructor still
     * receives a positional argument for it) to {@link TupleDomain#all()} - every reader (in
     * practice only {@code ArrowFlightMetadata#applyFilter}, coordinator-side) sees a real,
     * usable value regardless of whether this handle was freshly built or deserialized.
     */
    @Override
    public TupleDomain<ColumnHandle> enforcedConstraint()
    {
        return enforcedConstraint == null ? TupleDomain.all() : enforcedConstraint;
    }

    /**
     * Deliberately excludes {@code enforcedConstraint} - it is not meaningful wire identity (see
     * its {@code @JsonIgnore} javadoc above): two handles that differ only in that transient,
     * coordinator-planning-only field are the same handle for every purpose Trino's engine uses
     * {@code equals}/{@code hashCode} for (e.g. plan/split dedup). Without this override, the
     * generated record {@code equals} would compare raw field values directly, and a freshly-built
     * handle (real {@link TupleDomain}) would spuriously differ from its own round-tripped copy
     * (deserializes to {@code null} before the accessor above normalizes it).
     */
    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof ArrowFlightTableHandle that))
            return false;
        return keyspace.equals(that.keyspace) && table.equals(that.table) && filterJson.equals(that.filterJson)
               && aggregation.equals(that.aggregation) && mergePlanJson.equals(that.mergePlanJson);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, table, filterJson, aggregation, mergePlanJson);
    }

    /** A bare table handle with no pushed-down filter/aggregation. */
    public static ArrowFlightTableHandle of(String keyspace, String table)
    {
        return new ArrowFlightTableHandle(keyspace, table, TupleDomain.all(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * @param newEnforcedConstraint the FULL accumulated constraint now enforced (not just the
     *                              incremental addition) - see {@link #enforcedConstraint()}.
     * @param newFilter             the filter tree translated from {@code newEnforcedConstraint} in
     *                              its entirety, replacing (not wrapping) any previous filter - see
     *                              {@code ArrowFlightMetadata#applyFilter}'s javadoc.
     */
    public ArrowFlightTableHandle withFilter(TupleDomain<ColumnHandle> newEnforcedConstraint, FilterExpression newFilter)
    {
        return new ArrowFlightTableHandle(keyspace, table, newEnforcedConstraint, Optional.of(writeJson(newFilter.toJson())), aggregation, mergePlanJson);
    }

    public ArrowFlightTableHandle withAggregation(AggregationSpec newAggregation, List<AggregationMergePlan> newMergePlan)
    {
        List<Map<String, Object>> mergePlanJsonList = newMergePlan.stream().map(AggregationMergePlan::toJson).toList();
        return new ArrowFlightTableHandle(keyspace, table, enforcedConstraint, filterJson, Optional.of(newAggregation), Optional.of(writeJson(mergePlanJsonList)));
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
