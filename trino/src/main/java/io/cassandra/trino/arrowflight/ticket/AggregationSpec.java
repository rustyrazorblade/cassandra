package io.cassandra.trino.arrowflight.ticket;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The {@code aggregation} clause of a Flight ticket/descriptor:
 * {@code {"groupBy": ["col", ...], "aggregates": [{"function": "SUM", "column": "amount",
 * "outputName": "total"}]}} (see {@code ARROW-FLIGHT.md} and {@code trino/README.md}).
 * {@code groupBy} may be empty (global aggregation); {@code COUNT} with a {@code null} column
 * means {@code COUNT(*)}.
 */
public record AggregationSpec(List<String> groupBy, List<Aggregate> aggregates)
{
    public AggregationSpec
    {
        if (aggregates.isEmpty())
            throw new IllegalArgumentException("aggregation requires at least one aggregate");
        groupBy = List.copyOf(groupBy);
        aggregates = List.copyOf(aggregates);
    }

    public enum Function
    {
        COUNT, SUM, MIN, MAX, AVG
    }

    /**
     * One aggregate function. {@code column} is empty exactly for {@code COUNT(*)} - every other
     * function (and {@code COUNT(col)}) always carries a column.
     */
    public record Aggregate(Function function, Optional<String> column, String outputName)
    {
        public Aggregate
        {
            if (function != Function.COUNT && column.isEmpty())
                throw new IllegalArgumentException(function + " requires a column");
        }

        Map<String, Object> toJson()
        {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("function", function.name());
            json.put("column", column.orElse(null));
            json.put("outputName", outputName);
            return json;
        }
    }

    Map<String, Object> toJson()
    {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("groupBy", groupBy);
        List<Object> aggregatesJson = new ArrayList<>(aggregates.size());
        for (Aggregate aggregate : aggregates)
            aggregatesJson.add(aggregate.toJson());
        json.put("aggregates", aggregatesJson);
        return json;
    }
}
