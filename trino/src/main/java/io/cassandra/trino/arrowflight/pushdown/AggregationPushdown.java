package io.cassandra.trino.arrowflight.pushdown;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;

import io.cassandra.trino.arrowflight.ArrowFlightColumnHandle;
import io.cassandra.trino.arrowflight.ticket.AggregationSpec;

/**
 * Translates Trino's aggregation-pushdown representation ({@code List<AggregateFunction>} +
 * grouping sets, as passed to {@code ConnectorMetadata#applyAggregation}) into the Flight
 * ticket {@code aggregation} clause (see {@code ARROW-FLIGHT.md} and {@link AggregationSpec}).
 *
 * <p>Per the {@code applyAggregation} SPI contract there is no partial pushdown: either every
 * aggregate in the input list translates and the whole aggregation (plus its grouping) is pushed
 * down, or {@link Optional#empty()} is returned and Trino computes the entire aggregation itself
 * client-side. Supported shapes (v1 scope):
 * <ul>
 *   <li>Exactly one grouping set (plain {@code GROUP BY} or global aggregation - {@code [[]]});
 *       {@code GROUPING SETS}/{@code CUBE}/{@code ROLLUP} (multiple grouping sets) are not
 *       supported and fall back to Trino.</li>
 *   <li>{@code COUNT(*)}, {@code COUNT(col)}, {@code SUM(col)}, {@code MIN(col)}, {@code MAX(col)},
 *       {@code AVG(col)} - a single bare column argument (a {@link Variable}), no expressions.</li>
 *   <li>No {@code DISTINCT}, no {@code FILTER (WHERE ...)}, no {@code ORDER BY} within the
 *       aggregate (Cassandra/this protocol has no equivalent for any of those).</li>
 * </ul>
 */
public final class AggregationPushdown
{
    private AggregationPushdown()
    {
    }

    /**
     * @param spec         the {@code aggregation} clause to send to the server
     * @param projections  one {@link Variable} per input aggregate, in the same order - to be
     *                     returned as {@code AggregationApplicationResult#getProjections()}
     * @param assignments  the new synthetic output columns those variables reference - to be
     *                     returned as {@code AggregationApplicationResult#getAssignments()}
     */
    public record Result(AggregationSpec spec, List<ConnectorExpression> projections, List<Assignment> assignments)
    {
    }

    public static Optional<Result> translate(List<AggregateFunction> aggregates, List<List<ColumnHandle>> groupingSets)
    {
        if (groupingSets.size() != 1)
            return Optional.empty();

        List<String> groupBy = new ArrayList<>(groupingSets.get(0).size());
        for (ColumnHandle handle : groupingSets.get(0))
            groupBy.add(((ArrowFlightColumnHandle) handle).name());

        List<AggregationSpec.Aggregate> specAggregates = new ArrayList<>(aggregates.size());
        List<ConnectorExpression> projections = new ArrayList<>(aggregates.size());
        List<Assignment> assignments = new ArrayList<>(aggregates.size());

        for (int i = 0; i < aggregates.size(); i++)
        {
            Optional<AggregationSpec.Aggregate> translated = translateFunction(aggregates.get(i));
            if (translated.isEmpty())
                return Optional.empty();

            String outputName = "agg_" + i;
            var outputType = aggregates.get(i).getOutputType();
            specAggregates.add(new AggregationSpec.Aggregate(translated.get().function(), translated.get().column(), outputName));
            projections.add(new Variable(outputName, outputType));
            assignments.add(new Assignment(outputName, new ArrowFlightColumnHandle(outputName, outputType), outputType));
        }

        return Optional.of(new Result(new AggregationSpec(groupBy, specAggregates), projections, assignments));
    }

    /** Translates one {@link AggregateFunction}, ignoring its (Trino-chosen) output name/type. */
    private static Optional<AggregationSpec.Aggregate> translateFunction(AggregateFunction function)
    {
        if (function.isDistinct() || function.getFilter().isPresent() || !function.getSortItems().isEmpty())
            return Optional.empty();

        Optional<AggregationSpec.Function> mapped = mapFunctionName(function.getFunctionName());
        if (mapped.isEmpty())
            return Optional.empty();

        List<ConnectorExpression> arguments = function.getArguments();
        if (mapped.get() == AggregationSpec.Function.COUNT && arguments.isEmpty())
            return Optional.of(new AggregationSpec.Aggregate(AggregationSpec.Function.COUNT, Optional.empty(), "unused"));

        if (arguments.size() != 1 || !(arguments.get(0) instanceof Variable variable))
            return Optional.empty();

        return Optional.of(new AggregationSpec.Aggregate(mapped.get(), Optional.of(variable.getName()), "unused"));
    }

    private static Optional<AggregationSpec.Function> mapFunctionName(String functionName)
    {
        return switch (functionName.toLowerCase(Locale.ROOT))
        {
            case "count" -> Optional.of(AggregationSpec.Function.COUNT);
            case "sum" -> Optional.of(AggregationSpec.Function.SUM);
            case "min" -> Optional.of(AggregationSpec.Function.MIN);
            case "max" -> Optional.of(AggregationSpec.Function.MAX);
            case "avg" -> Optional.of(AggregationSpec.Function.AVG);
            default -> Optional.empty();
        };
    }
}
