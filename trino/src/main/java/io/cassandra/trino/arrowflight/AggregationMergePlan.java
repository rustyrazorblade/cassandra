package io.cassandra.trino.arrowflight;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes how to combine each pushed-down aggregate's per-sub-range partial wire value(s) into
 * one final value, once {@link ArrowFlightTableHandle#aggregation()} forces every sub-range
 * (token range x replica) into a single Trino split (see {@link ArrowFlightSplitManager}) instead
 * of one split per range. Trino's {@code applyAggregation} SPI contract requires the connector to
 * hand back the fully-final aggregated result - Trino never merges partial per-split aggregates
 * itself - so with N sub-ranges each independently computing (say) its own local COUNT/SUM/GROUP
 * BY, this plan tells {@link ArrowFlightAggregatingPageSource} how to combine those N partial
 * rows per group into one correct final row.
 *
 * <p><b>No Jackson polymorphism annotations here</b> - see {@code FilterExpression}'s javadoc for
 * why: Trino's internal coordinator/worker JSON codec disables Jackson's annotation-driven
 * mechanisms (relying only on Java records' built-in structural introspection), so a
 * {@code @JsonTypeInfo}-annotated sealed interface embedded in a {@code ConnectorTableHandle}
 * silently loses its type discriminator on the wire. {@link ArrowFlightTableHandle} instead stores
 * a list of these pre-serialized as a JSON string (via {@link #toJson()} + {@link #fromJson(Map)}).
 */
public sealed interface AggregationMergePlan
{
    /** The final Trino column name this derivation produces (matches an {@code Assignment}). */
    String outputName();

    /** How to combine N sub-ranges' values for one wire column into a single merged value. */
    enum MergeOp
    {
        SUM, MIN, MAX
    }

    /**
     * The numeric domain a decomposed {@code AVG}'s {@code SUM} component is carried in on the
     * wire - matches whichever accumulator kind Cassandra's {@code RowAggregator} picked for the
     * underlying column (integral columns sum as a wire {@code BIGINT}, floating columns as a
     * wire {@code DOUBLE} - the same domain Trino's own {@code sum()} would infer for that column
     * type), so the merging page source knows which native representation to read/accumulate in
     * without needing to serialize a full {@code Type} object into the table handle.
     */
    enum NumericDomain
    {
        INTEGRAL, FLOATING
    }

    /** {@code COUNT}/{@code SUM}/{@code MIN}/{@code MAX}: one wire column, merged then passed through as-is. */
    record Direct(String outputName, String wireColumn, MergeOp mergeOp) implements AggregationMergePlan
    {
    }

    /**
     * {@code AVG(col)}: decomposed at pushdown time (see {@code AggregationPushdown}) into two
     * wire aggregates - {@code SUM(col)} and {@code COUNT(col)} - so cross-sub-range merging can
     * correctly weight each sub-range's contribution (merging bare per-sub-range averages without
     * their underlying counts would silently bias the result toward smaller sub-ranges). The
     * final value is {@code mergedSum / mergedCount}, computed once after every sub-range's
     * contribution has been merged.
     */
    record Average(String outputName, String sumWireColumn, String countWireColumn, NumericDomain sumDomain) implements AggregationMergePlan
    {
    }

    static List<String> wireColumns(AggregationMergePlan column)
    {
        return switch (column)
        {
            case Direct direct -> List.of(direct.wireColumn());
            case Average average -> List.of(average.sumWireColumn(), average.countWireColumn());
        };
    }

    /** This derivation's JSON representation - see {@link #fromJson(Map)} for the reverse. */
    default Map<String, Object> toJson()
    {
        Map<String, Object> json = new LinkedHashMap<>();
        switch (this)
        {
            case Direct direct ->
            {
                json.put("kind", "direct");
                json.put("outputName", direct.outputName());
                json.put("wireColumn", direct.wireColumn());
                json.put("mergeOp", direct.mergeOp().name());
            }
            case Average average ->
            {
                json.put("kind", "average");
                json.put("outputName", average.outputName());
                json.put("sumWireColumn", average.sumWireColumn());
                json.put("countWireColumn", average.countWireColumn());
                json.put("sumDomain", average.sumDomain().name());
            }
        }
        return json;
    }

    /** Reconstructs one derivation from the {@code Map} shape {@link #toJson()} produces. */
    static AggregationMergePlan fromJson(Map<String, Object> json)
    {
        String kind = (String) json.get("kind");
        return switch (kind)
        {
            case "direct" -> new Direct((String) json.get("outputName"), (String) json.get("wireColumn"), MergeOp.valueOf((String) json.get("mergeOp")));
            case "average" -> new Average(
                (String) json.get("outputName"),
                (String) json.get("sumWireColumn"),
                (String) json.get("countWireColumn"),
                NumericDomain.valueOf((String) json.get("sumDomain")));
            default -> throw new IllegalArgumentException("Unrecognized merge plan kind: " + kind);
        };
    }
}
