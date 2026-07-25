package io.cassandra.trino.arrowflight.pushdown;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;

import io.cassandra.trino.arrowflight.AggregationMergePlan;
import io.cassandra.trino.arrowflight.ArrowFlightColumnHandle;
import io.cassandra.trino.arrowflight.ticket.AggregationSpec;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Translates real Trino {@link AggregateFunction}/grouping-set shapes into
 * {@link AggregationSpec} - no network, no live server.
 */
class AggregationPushdownTest
{
    private static final ArrowFlightColumnHandle AMOUNT = new ArrowFlightColumnHandle("amount", IntegerType.INTEGER);
    private static final ArrowFlightColumnHandle REGION = new ArrowFlightColumnHandle("region", VarcharType.VARCHAR);

    private static AggregateFunction simpleAggregate(String name, io.trino.spi.type.Type outputType, ConnectorExpression... arguments)
    {
        return new AggregateFunction(name, outputType, List.of(arguments), List.of(), false, Optional.empty());
    }

    @Test
    void countStarWithNoGroupByTranslatesToGlobalCountStar()
    {
        AggregateFunction countStar = simpleAggregate("count", BigintType.BIGINT);

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(countStar), List.of(List.of()));

        assertThat(result).isPresent();
        AggregationSpec spec = result.get().spec();
        assertThat(spec.groupBy()).isEmpty();
        assertThat(spec.aggregates()).hasSize(1);
        assertThat(spec.aggregates().get(0).function()).isEqualTo(AggregationSpec.Function.COUNT);
        assertThat(spec.aggregates().get(0).column()).isEmpty();
    }

    @Test
    void countOfColumnCarriesTheColumnName()
    {
        AggregateFunction countCol = simpleAggregate("count", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(countCol), List.of(List.of()));

        assertThat(result).isPresent();
        assertThat(result.get().spec().aggregates().get(0).column()).contains("amount");
    }

    @Test
    void sumMinMaxAvgAllTranslate()
    {
        List<AggregateFunction> aggregates = List.of(
            simpleAggregate("sum", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER)),
            simpleAggregate("min", IntegerType.INTEGER, new Variable("amount", IntegerType.INTEGER)),
            simpleAggregate("max", IntegerType.INTEGER, new Variable("amount", IntegerType.INTEGER)),
            simpleAggregate("avg", DoubleType.DOUBLE, new Variable("amount", IntegerType.INTEGER)));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(aggregates, List.of(List.of()));

        assertThat(result).isPresent();
        // AVG is decomposed into wire SUM+COUNT (never a wire AVG) so cross-subrange merging can
        // correctly weight each subrange's contribution - see AggregationMergePlan.Average.
        List<AggregationSpec.Function> functions = result.get().spec().aggregates().stream()
                                                           .map(AggregationSpec.Aggregate::function)
                                                           .toList();
        assertThat(functions).containsExactly(
            AggregationSpec.Function.SUM, AggregationSpec.Function.MIN,
            AggregationSpec.Function.MAX, AggregationSpec.Function.SUM, AggregationSpec.Function.COUNT);
    }

    @Test
    void avgDecomposesIntoSumAndCountWireAggregatesWithAverageMergePlan()
    {
        AggregateFunction avg = simpleAggregate("avg", DoubleType.DOUBLE, new Variable("amount", IntegerType.INTEGER));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(avg), List.of(List.of()));

        assertThat(result).isPresent();
        assertThat(result.get().spec().aggregates()).hasSize(2);
        assertThat(result.get().spec().aggregates().get(0).function()).isEqualTo(AggregationSpec.Function.SUM);
        assertThat(result.get().spec().aggregates().get(0).column()).contains("amount");
        assertThat(result.get().spec().aggregates().get(1).function()).isEqualTo(AggregationSpec.Function.COUNT);
        assertThat(result.get().spec().aggregates().get(1).column()).contains("amount");

        assertThat(result.get().mergePlan()).hasSize(1);
        AggregationMergePlan derivation = result.get().mergePlan().get(0);
        assertThat(derivation).isInstanceOf(AggregationMergePlan.Average.class);
        AggregationMergePlan.Average average = (AggregationMergePlan.Average) derivation;
        assertThat(average.outputName()).isEqualTo("agg_0");
        assertThat(average.sumWireColumn()).isEqualTo(result.get().spec().aggregates().get(0).outputName());
        assertThat(average.countWireColumn()).isEqualTo(result.get().spec().aggregates().get(1).outputName());
        assertThat(average.sumDomain()).isEqualTo(AggregationMergePlan.NumericDomain.INTEGRAL);
    }

    @Test
    void avgOverFloatingColumnUsesFloatingDomain()
    {
        AggregateFunction avg = simpleAggregate("avg", DoubleType.DOUBLE, new Variable("amount", DoubleType.DOUBLE));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(avg), List.of(List.of()));

        assertThat(result).isPresent();
        AggregationMergePlan.Average average = (AggregationMergePlan.Average) result.get().mergePlan().get(0);
        assertThat(average.sumDomain()).isEqualTo(AggregationMergePlan.NumericDomain.FLOATING);
    }

    @Test
    void countSumMinMaxProduceDirectMergePlansWithSumMergeOpForCount()
    {
        List<AggregateFunction> aggregates = List.of(
            simpleAggregate("count", BigintType.BIGINT),
            simpleAggregate("sum", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER)),
            simpleAggregate("min", IntegerType.INTEGER, new Variable("amount", IntegerType.INTEGER)),
            simpleAggregate("max", IntegerType.INTEGER, new Variable("amount", IntegerType.INTEGER)));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(aggregates, List.of(List.of()));

        assertThat(result).isPresent();
        List<AggregationMergePlan> mergePlan = result.get().mergePlan();
        assertThat(mergePlan).hasSize(4);
        assertThat(mergePlan).allMatch(AggregationMergePlan.Direct.class::isInstance);
        assertThat(mergePlan.stream().map(m -> ((AggregationMergePlan.Direct) m).mergeOp()).toList())
            .containsExactly(
                AggregationMergePlan.MergeOp.SUM, AggregationMergePlan.MergeOp.SUM,
                AggregationMergePlan.MergeOp.MIN, AggregationMergePlan.MergeOp.MAX);
    }

    @Test
    void groupByColumnsArePropagatedByName()
    {
        AggregateFunction sum = simpleAggregate("sum", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(
            List.of(sum), List.of(List.of(REGION)));

        assertThat(result).isPresent();
        assertThat(result.get().spec().groupBy()).containsExactly("region");
    }

    @Test
    void projectionsMatchInputOrderAndAssignmentsCarryOutputType()
    {
        AggregateFunction sum = simpleAggregate("sum", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER));
        AggregateFunction count = simpleAggregate("count", BigintType.BIGINT);

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(
            List.of(sum, count), List.of(List.of()));

        assertThat(result).isPresent();
        assertThat(result.get().projections()).hasSize(2);
        assertThat(result.get().assignments()).hasSize(2);
        assertThat(((Variable) result.get().projections().get(0)).getName())
            .isEqualTo(result.get().assignments().get(0).getVariable());
        assertThat(result.get().assignments().get(0).getType()).isEqualTo(BigintType.BIGINT);
    }

    @Test
    void multipleGroupingSetsAreNotSupported()
    {
        AggregateFunction sum = simpleAggregate("sum", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(
            List.of(sum), List.of(List.of(REGION), List.of()));

        assertThat(result).isEmpty();
    }

    @Test
    void distinctAggregateIsNotSupported()
    {
        AggregateFunction distinctSum = new AggregateFunction(
            "sum", BigintType.BIGINT, List.of(new Variable("amount", IntegerType.INTEGER)), List.of(), true, Optional.empty());

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(distinctSum), List.of(List.of()));

        assertThat(result).isEmpty();
    }

    @Test
    void filteredAggregateIsNotSupported()
    {
        AggregateFunction filtered = new AggregateFunction(
            "sum", BigintType.BIGINT, List.of(new Variable("amount", IntegerType.INTEGER)), List.of(), false,
            Optional.of(new Constant(true, io.trino.spi.type.BooleanType.BOOLEAN)));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(filtered), List.of(List.of()));

        assertThat(result).isEmpty();
    }

    @Test
    void nonVariableArgumentIsNotSupported()
    {
        AggregateFunction sumOfConstant = simpleAggregate("sum", BigintType.BIGINT, new Constant(1L, BigintType.BIGINT));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(sumOfConstant), List.of(List.of()));

        assertThat(result).isEmpty();
    }

    @Test
    void unknownFunctionNameIsNotSupported()
    {
        AggregateFunction approxDistinct = simpleAggregate("approx_distinct", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(approxDistinct), List.of(List.of()));

        assertThat(result).isEmpty();
    }

    @Test
    void wholeAggregationFallsBackWhenAnySingleAggregateIsUnsupported()
    {
        AggregateFunction sum = simpleAggregate("sum", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER));
        AggregateFunction unsupported = simpleAggregate("approx_distinct", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(sum, unsupported), List.of(List.of()));

        assertThat(result).isEmpty();
    }

    @Test
    void emptyGroupingSetsListIsRejected()
    {
        AggregateFunction sum = simpleAggregate("sum", BigintType.BIGINT, new Variable("amount", IntegerType.INTEGER));

        Optional<AggregationPushdown.Result> result = AggregationPushdown.translate(List.of(sum), List.<List<ColumnHandle>>of());

        assertThat(result).isEmpty();
    }
}
