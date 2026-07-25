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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;

import io.cassandra.trino.arrowflight.AggregationMergePlan;
import io.cassandra.trino.arrowflight.AggregationMergePlan.MergeOp;
import io.cassandra.trino.arrowflight.AggregationMergePlan.NumericDomain;
import io.cassandra.trino.arrowflight.ArrowFlightColumnHandle;
import io.cassandra.trino.arrowflight.ticket.AggregationSpec;

/**
 * Translates Trino's aggregation-pushdown representation ({@code List<AggregateFunction>} +
 * grouping sets, as passed to {@code ConnectorMetadata#applyAggregation}) into the Flight
 * ticket {@code aggregation} clause (see {@code ARROW-FLIGHT.md} and {@link AggregationSpec}),
 * plus an {@link AggregationMergePlan} per requested aggregate describing how
 * {@code ArrowFlightAggregatingPageSource} must combine each sub-range's partial wire value(s)
 * into the single final value Trino expects (see that plan's javadoc for why this is needed).
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
 *       {@code AVG(col)} - a single bare column argument (a {@link Variable}), no expressions.
 *       {@code AVG(col)} is decomposed into wire-level {@code SUM(col)}/{@code COUNT(col)}
 *       aggregates rather than pushed down as a single wire {@code AVG} - see
 *       {@link AggregationMergePlan.Average}.</li>
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
     * @param mergePlan    one {@link AggregationMergePlan} per input aggregate, in the same order -
     *                     embedded in the table handle for {@code ArrowFlightAggregatingPageSource}
     */
    public record Result(AggregationSpec spec, List<ConnectorExpression> projections, List<Assignment> assignments, List<AggregationMergePlan> mergePlan)
    {
    }

    public static Optional<Result> translate(List<AggregateFunction> aggregates, List<List<ColumnHandle>> groupingSets)
    {
        if (groupingSets.size() != 1)
            return Optional.empty();

        List<String> groupBy = new ArrayList<>(groupingSets.get(0).size());
        for (ColumnHandle handle : groupingSets.get(0))
            groupBy.add(((ArrowFlightColumnHandle) handle).name());

        List<AggregationSpec.Aggregate> specAggregates = new ArrayList<>();
        List<ConnectorExpression> projections = new ArrayList<>(aggregates.size());
        List<Assignment> assignments = new ArrayList<>(aggregates.size());
        List<AggregationMergePlan> mergePlan = new ArrayList<>(aggregates.size());

        for (int i = 0; i < aggregates.size(); i++)
        {
            AggregateFunction function = aggregates.get(i);
            String outputName = "agg_" + i;
            Type outputType = function.getOutputType();

            Optional<Translated> translated = translateFunction(function, outputName);
            if (translated.isEmpty())
                return Optional.empty();

            specAggregates.addAll(translated.get().wireAggregates());
            mergePlan.add(translated.get().derivation());
            projections.add(new Variable(outputName, outputType));
            assignments.add(new Assignment(outputName, new ArrowFlightColumnHandle(outputName, outputType), outputType));
        }

        return Optional.of(new Result(new AggregationSpec(groupBy, specAggregates), projections, assignments, mergePlan));
    }

    private record Translated(List<AggregationSpec.Aggregate> wireAggregates, AggregationMergePlan derivation)
    {
    }

    /** Translates one {@link AggregateFunction}, using {@code outputName} for both the wire and merge-plan output name. */
    private static Optional<Translated> translateFunction(AggregateFunction function, String outputName)
    {
        if (function.isDistinct() || function.getFilter().isPresent() || !function.getSortItems().isEmpty())
            return Optional.empty();

        Optional<AggregationSpec.Function> mapped = mapFunctionName(function.getFunctionName());
        if (mapped.isEmpty())
            return Optional.empty();

        List<ConnectorExpression> arguments = function.getArguments();

        if (mapped.get() == AggregationSpec.Function.COUNT && arguments.isEmpty())
        {
            AggregationSpec.Aggregate wire = new AggregationSpec.Aggregate(AggregationSpec.Function.COUNT, Optional.empty(), outputName);
            return Optional.of(new Translated(List.of(wire), new AggregationMergePlan.Direct(outputName, outputName, MergeOp.SUM)));
        }

        if (arguments.size() != 1 || !(arguments.get(0) instanceof Variable variable))
            return Optional.empty();

        if (mapped.get() == AggregationSpec.Function.AVG)
        {
            NumericDomain domain = numericDomainOf(variable.getType());
            if (domain == null)
                return Optional.empty();
            String sumName = outputName + "$sum";
            String countName = outputName + "$count";
            List<AggregationSpec.Aggregate> wire = List.of(
                new AggregationSpec.Aggregate(AggregationSpec.Function.SUM, Optional.of(variable.getName()), sumName),
                new AggregationSpec.Aggregate(AggregationSpec.Function.COUNT, Optional.of(variable.getName()), countName));
            return Optional.of(new Translated(wire, new AggregationMergePlan.Average(outputName, sumName, countName, domain)));
        }

        MergeOp mergeOp = switch (mapped.get())
        {
            case SUM, COUNT -> MergeOp.SUM;
            case MIN -> MergeOp.MIN;
            case MAX -> MergeOp.MAX;
            case AVG -> throw new IllegalStateException("AVG is handled above, never reaches the generic branch");
        };
        AggregationSpec.Aggregate wire = new AggregationSpec.Aggregate(mapped.get(), Optional.of(variable.getName()), outputName);
        return Optional.of(new Translated(List.of(wire), new AggregationMergePlan.Direct(outputName, outputName, mergeOp)));
    }

    /**
     * Classifies a column's numeric domain for a decomposed {@code AVG}'s {@code SUM} component -
     * matches the domain Trino's own {@code sum()} would infer for the same input type (integral
     * inputs sum as {@code BIGINT}, floating inputs as {@code DOUBLE}). Decimal/varint Cassandra
     * columns always reach Trino as {@code VARCHAR} (see {@code ArrowPageBuilder} class javadoc),
     * so Trino would never let a user call {@code avg()} on one in the first place - this method
     * is never asked to classify that case.
     */
    private static NumericDomain numericDomainOf(Type type)
    {
        if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType)
            return NumericDomain.INTEGRAL;
        if (type instanceof RealType || type instanceof DoubleType)
            return NumericDomain.FLOATING;
        return null;
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
