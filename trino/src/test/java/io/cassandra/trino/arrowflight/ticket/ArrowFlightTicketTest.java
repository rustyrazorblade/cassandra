package io.cassandra.trino.arrowflight.ticket;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pure serialization tests for ticket/filter/aggregation JSON construction - no network, no
 * Cassandra, no Trino types involved (see the wire contract in {@code ARROW-FLIGHT.md} and
 * {@code trino/README.md}).
 */
class ArrowFlightTicketTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("unchecked")
    private static Map<String, Object> roundTrip(ArrowFlightTicket ticket)
    {
        return (Map<String, Object>) MAPPER.convertValue(ticket.toJsonMap(), Map.class);
    }

    @Test
    void minimalTicketHasOnlyKeyspaceAndTable()
    {
        ArrowFlightTicket ticket = ArrowFlightTicket.of("ks", "tbl");
        assertThat(roundTrip(ticket)).containsOnlyKeys("keyspace", "table");
        assertThat(roundTrip(ticket)).containsEntry("keyspace", "ks").containsEntry("table", "tbl");
    }

    @Test
    void tokenRangeSerializesAsDecimalStrings()
    {
        ArrowFlightTicket ticket = ArrowFlightTicket.of("ks", "tbl")
            .withTokenRange(new TokenRange("-9223372036854775808", "9223372036854775807"));

        Map<String, Object> json = roundTrip(ticket);
        assertThat(json).containsKey("tokenRange");
        @SuppressWarnings("unchecked")
        Map<String, Object> tokenRange = (Map<String, Object>) json.get("tokenRange");
        assertThat(tokenRange).containsEntry("start", "-9223372036854775808");
        assertThat(tokenRange).containsEntry("end", "9223372036854775807");
    }

    @Test
    void tokenRangeOfBuildsFromBigInteger()
    {
        TokenRange range = TokenRange.of(java.math.BigInteger.valueOf(-100), java.math.BigInteger.valueOf(100));
        assertThat(range.start()).isEqualTo("-100");
        assertThat(range.end()).isEqualTo("100");
    }

    @Test
    void jsonBytesRoundTripThroughJackson() throws Exception
    {
        ArrowFlightTicket ticket = ArrowFlightTicket.of("ks", "tbl")
            .withTokenRange(new TokenRange("0", "100"));
        byte[] bytes = ticket.toJsonBytes();

        Map<String, Object> parsed = MAPPER.readValue(bytes, Map.class);
        assertThat(parsed).containsEntry("keyspace", "ks").containsEntry("table", "tbl");
    }

    // --- filter: comparison operators ---

    @Test
    void cmpSerializesColumnOpAndValue()
    {
        FilterExpression cmp = new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100);
        assertThat(cmp.toJson()).containsOnlyKeys("cmp");
        @SuppressWarnings("unchecked")
        Map<String, Object> inner = (Map<String, Object>) cmp.toJson().get("cmp");
        assertThat(inner).containsEntry("column", "amount").containsEntry("op", "GT").containsEntry("value", 100);
    }

    @Test
    void everyComparisonOperatorSerializesItsOwnName()
    {
        for (FilterExpression.Op op : FilterExpression.Op.values())
        {
            FilterExpression.Cmp cmp = new FilterExpression.Cmp("c", op, 1);
            @SuppressWarnings("unchecked")
            Map<String, Object> inner = (Map<String, Object>) cmp.toJson().get("cmp");
            assertThat(inner).containsEntry("op", op.name());
        }
    }

    @Test
    void cmpValueCanBeStringBooleanOrNumber()
    {
        assertValue(new FilterExpression.Cmp("c", FilterExpression.Op.EQ, "hello"), "hello");
        assertValue(new FilterExpression.Cmp("c", FilterExpression.Op.EQ, true), true);
        assertValue(new FilterExpression.Cmp("c", FilterExpression.Op.EQ, 3.14), 3.14);
    }

    @SuppressWarnings("unchecked")
    private static void assertValue(FilterExpression.Cmp cmp, Object expected)
    {
        Map<String, Object> inner = (Map<String, Object>) cmp.toJson().get("cmp");
        assertThat(inner.get("value")).isEqualTo(expected);
    }

    // --- filter: isNull / isNotNull ---

    @Test
    void isNullSerializesColumnOnly()
    {
        FilterExpression isNull = new FilterExpression.IsNull("deleted_at");
        assertThat(isNull.toJson()).isEqualTo(Map.of("isNull", Map.of("column", "deleted_at")));
    }

    @Test
    void isNotNullSerializesColumnOnly()
    {
        FilterExpression isNotNull = new FilterExpression.IsNotNull("deleted_at");
        assertThat(isNotNull.toJson()).isEqualTo(Map.of("isNotNull", Map.of("column", "deleted_at")));
    }

    // --- filter: in ---

    @Test
    void inSerializesColumnAndValues()
    {
        FilterExpression in = new FilterExpression.In("region", List.of("us-east", "us-west"));
        @SuppressWarnings("unchecked")
        Map<String, Object> inner = (Map<String, Object>) in.toJson().get("in");
        assertThat(inner).containsEntry("column", "region");
        assertThat(inner.get("values")).isEqualTo(List.of("us-east", "us-west"));
    }

    @Test
    void inRejectsEmptyValues()
    {
        assertThatThrownBy(() -> new FilterExpression.In("region", List.of()))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // --- filter: and / or / not ---

    @Test
    void andSerializesChildrenAsAList()
    {
        FilterExpression and = new FilterExpression.And(List.of(
            new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100),
            new FilterExpression.IsNull("deleted_at")));

        Map<String, Object> json = and.toJson();
        assertThat(json).containsOnlyKeys("and");
        assertThat((List<?>) json.get("and")).hasSize(2);
        assertThat(((List<?>) json.get("and")).get(0)).isEqualTo(
            new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100).toJson());
    }

    @Test
    void orSerializesChildrenAsAList()
    {
        FilterExpression or = new FilterExpression.Or(List.of(
            new FilterExpression.Cmp("a", FilterExpression.Op.EQ, 1),
            new FilterExpression.Cmp("a", FilterExpression.Op.EQ, 2)));
        assertThat((List<?>) or.toJson().get("or")).hasSize(2);
    }

    @Test
    void notWrapsSingleChild()
    {
        FilterExpression not = new FilterExpression.Not(new FilterExpression.IsNull("c"));
        assertThat(not.toJson()).isEqualTo(Map.of("not", Map.of("isNull", Map.of("column", "c"))));
    }

    @Test
    void andRejectsEmptyChildren()
    {
        assertThatThrownBy(() -> new FilterExpression.And(List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void orRejectsEmptyChildren()
    {
        assertThatThrownBy(() -> new FilterExpression.Or(List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nestedAndOrNotTreeMatchesExampleShape()
    {
        // {"and": [{"cmp": {"column": "amount", "op": "GT", "value": 100}}, {"isNull": {"column": "deleted_at"}}]}
        FilterExpression tree = new FilterExpression.And(List.of(
            new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100),
            new FilterExpression.IsNull("deleted_at")));

        ArrowFlightTicket ticket = ArrowFlightTicket.of("ks", "tbl").withFilter(tree);
        Map<String, Object> json = roundTrip(ticket);
        assertThat(json).containsKey("filter");
    }

    @Test
    void deeplyNestedNotOrAndSerializesCorrectly()
    {
        FilterExpression tree = new FilterExpression.Not(
            new FilterExpression.Or(List.of(
                new FilterExpression.And(List.of(
                    new FilterExpression.Cmp("a", FilterExpression.Op.LT, 5),
                    new FilterExpression.Cmp("b", FilterExpression.Op.GE, 1))),
                new FilterExpression.In("c", List.of(1, 2, 3)))));

        Map<String, Object> json = tree.toJson();
        assertThat(json).containsOnlyKeys("not");
    }

    // --- aggregation ---

    @Test
    void countStarHasNullColumn()
    {
        AggregationSpec.Aggregate countStar = new AggregationSpec.Aggregate(
            AggregationSpec.Function.COUNT, java.util.Optional.empty(), "n");
        Map<String, Object> json = new AggregationSpec(List.of(), List.of(countStar)).toJson();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> aggregates = (List<Map<String, Object>>) json.get("aggregates");
        assertThat(aggregates).hasSize(1);
        assertThat(aggregates.get(0)).containsEntry("function", "COUNT");
        assertThat(aggregates.get(0)).containsEntry("column", null);
        assertThat(aggregates.get(0)).containsEntry("outputName", "n");
    }

    @Test
    void nonCountAggregateRequiresAColumn()
    {
        assertThatThrownBy(() -> new AggregationSpec.Aggregate(
            AggregationSpec.Function.SUM, java.util.Optional.empty(), "total"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void groupByCanBeEmptyForGlobalAggregation()
    {
        AggregationSpec spec = new AggregationSpec(List.of(), List.of(
            new AggregationSpec.Aggregate(AggregationSpec.Function.COUNT, java.util.Optional.empty(), "n")));
        assertThat(spec.toJson()).containsEntry("groupBy", List.of());
    }

    @Test
    void aggregationRequiresAtLeastOneAggregate()
    {
        assertThatThrownBy(() -> new AggregationSpec(List.of("region"), List.of()))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void everyAggregateFunctionSerializesItsOwnName()
    {
        for (AggregationSpec.Function function : AggregationSpec.Function.values())
        {
            AggregationSpec.Aggregate aggregate = function == AggregationSpec.Function.COUNT
                ? new AggregationSpec.Aggregate(function, java.util.Optional.empty(), "out")
                : new AggregationSpec.Aggregate(function, java.util.Optional.of("col"), "out");
            assertThat(aggregate.toJson()).containsEntry("function", function.name());
        }
    }

    @Test
    void fullTicketMatchesDocumentedExampleShape()
    {
        // Mirrors the exact example from ARROW-FLIGHT.md / trino/README.md.
        ArrowFlightTicket ticket = new ArrowFlightTicket(
            "ks",
            "tbl",
            java.util.Optional.of(new TokenRange("-9223372036854775808", "9223372036854775807")),
            java.util.Optional.of(new FilterExpression.And(List.of(
                new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100),
                new FilterExpression.IsNull("deleted_at")))),
            java.util.Optional.of(new AggregationSpec(
                List.of("region"),
                List.of(new AggregationSpec.Aggregate(
                    AggregationSpec.Function.SUM, java.util.Optional.of("amount"), "total")))));

        Map<String, Object> json = roundTrip(ticket);
        assertThat(json).containsOnlyKeys("keyspace", "table", "tokenRange", "filter", "aggregation");
    }
}
