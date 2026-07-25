package io.cassandra.trino.arrowflight;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.Test;

import io.cassandra.trino.arrowflight.ticket.AggregationSpec;
import io.cassandra.trino.arrowflight.ticket.FilterExpression;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Trino serializes {@code ConnectorTableHandle} (and thus {@link ArrowFlightTableHandle}) via
 * Jackson to ship {@code TaskUpdateRequest}s from coordinator to worker - even in a single-node
 * deployment, since the coordinator always round-trips a query's table handles through this
 * codec when dispatching tasks. {@link ArrowFlightMetadata#applyFilter}/{@code #applyAggregation}
 * unit tests exercise the *translation* logic (Trino predicate/aggregate -> our ticket types) in
 * isolation and never caught this: a live end-to-end run against a real Trino coordinator found
 * that any query with a pushed-down WHERE clause failed at task-dispatch time with "Cannot
 * construct instance of FilterExpression (no Creators...)" - {@link FilterExpression} is a sealed
 * interface, which Jackson cannot deserialize back into a concrete subtype without explicit
 * {@code @JsonTypeInfo}/{@code @JsonSubTypes}. This test exercises that exact codepath directly
 * (a plain {@link ObjectMapper}, not a hand-rolled {@code toJson()} helper) so a regression here
 * fails a fast unit test instead of only surfacing against a live cluster.
 */
class ArrowFlightTableHandleSerializationTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

    private static ArrowFlightTableHandle roundTrip(ArrowFlightTableHandle handle) throws Exception
    {
        String json = MAPPER.writeValueAsString(handle);
        return MAPPER.readValue(json, ArrowFlightTableHandle.class);
    }

    @Test
    void bareHandleRoundTrips() throws Exception
    {
        ArrowFlightTableHandle handle = ArrowFlightTableHandle.of("ks", "tbl");
        assertThat(roundTrip(handle)).isEqualTo(handle);
    }

    @Test
    void simpleCmpFilterRoundTrips() throws Exception
    {
        ArrowFlightTableHandle handle = ArrowFlightTableHandle.of("ks", "tbl")
            .withFilter(new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100));
        assertThat(roundTrip(handle)).isEqualTo(handle);
    }

    @Test
    void nestedAndOrNotFilterRoundTrips() throws Exception
    {
        FilterExpression filter = new FilterExpression.And(List.of(
            new FilterExpression.Or(List.of(
                new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100),
                new FilterExpression.IsNull("deleted_at"))),
            new FilterExpression.Not(new FilterExpression.In("region", List.of("us-east", "us-west"))),
            new FilterExpression.IsNotNull("region")));
        ArrowFlightTableHandle handle = ArrowFlightTableHandle.of("ks", "tbl").withFilter(filter);
        assertThat(roundTrip(handle)).isEqualTo(handle);
    }

    @Test
    void aggregationRoundTrips() throws Exception
    {
        AggregationSpec aggregation = new AggregationSpec(
            List.of("region"),
            List.of(new AggregationSpec.Aggregate(AggregationSpec.Function.SUM, Optional.of("amount"), "total")));
        List<AggregationMergePlan> mergePlan = List.of(new AggregationMergePlan.Direct("total", "total", AggregationMergePlan.MergeOp.SUM));
        ArrowFlightTableHandle handle = ArrowFlightTableHandle.of("ks", "tbl").withAggregation(aggregation, mergePlan);
        assertThat(roundTrip(handle)).isEqualTo(handle);
    }

    @Test
    void averageMergePlanRoundTrips() throws Exception
    {
        AggregationSpec aggregation = new AggregationSpec(
            List.of("region"),
            List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Function.SUM, Optional.of("amount"), "agg_0$sum"),
                new AggregationSpec.Aggregate(AggregationSpec.Function.COUNT, Optional.of("amount"), "agg_0$count")));
        List<AggregationMergePlan> mergePlan = List.of(
            new AggregationMergePlan.Average("agg_0", "agg_0$sum", "agg_0$count", AggregationMergePlan.NumericDomain.INTEGRAL));
        ArrowFlightTableHandle handle = ArrowFlightTableHandle.of("ks", "tbl").withAggregation(aggregation, mergePlan);
        assertThat(roundTrip(handle)).isEqualTo(handle);
    }

    @Test
    void filterAndAggregationTogetherRoundTrip() throws Exception
    {
        FilterExpression filter = new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100);
        AggregationSpec aggregation = new AggregationSpec(
            List.of(),
            List.of(new AggregationSpec.Aggregate(AggregationSpec.Function.COUNT, Optional.empty(), "cnt")));
        List<AggregationMergePlan> mergePlan = List.of(new AggregationMergePlan.Direct("cnt", "cnt", AggregationMergePlan.MergeOp.SUM));
        ArrowFlightTableHandle handle = ArrowFlightTableHandle.of("ks", "tbl")
            .withFilter(filter)
            .withAggregation(aggregation, mergePlan);
        assertThat(roundTrip(handle)).isEqualTo(handle);
    }
}
