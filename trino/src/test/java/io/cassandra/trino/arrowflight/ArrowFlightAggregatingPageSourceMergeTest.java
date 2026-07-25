package io.cassandra.trino.arrowflight;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.airlift.slice.Slices;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;

import io.cassandra.trino.arrowflight.AggregationMergePlan.MergeOp;
import io.cassandra.trino.arrowflight.ArrowFlightAggregatingPageSource.WireSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Direct unit coverage for {@link ArrowFlightAggregatingPageSource}'s merge arithmetic
 * ({@code combine}/{@code mergeGroupMaps}/{@code averageOf}) using synthetic data, without a live
 * Flight server - complements the live end-to-end validation described in the commit that
 * introduced cross-subrange merging (COUNT(*) returning one unmerged partial result per
 * split/subrange instead of a single total).
 */
class ArrowFlightAggregatingPageSourceMergeTest
{
    @Test
    void combineSumOverLongDomain()
    {
        assertThat(ArrowFlightAggregatingPageSource.combine(MergeOp.SUM, 3L, 4L, BigintType.BIGINT)).isEqualTo(7L);
    }

    @Test
    void combineMinMaxOverDoubleDomain()
    {
        assertThat(ArrowFlightAggregatingPageSource.combine(MergeOp.MIN, 3.5, 1.5, DoubleType.DOUBLE)).isEqualTo(1.5);
        assertThat(ArrowFlightAggregatingPageSource.combine(MergeOp.MAX, 3.5, 1.5, DoubleType.DOUBLE)).isEqualTo(3.5);
    }

    @Test
    void combineMinMaxOverSliceDomain()
    {
        Object a = Slices.utf8Slice("banana");
        Object b = Slices.utf8Slice("apple");
        assertThat(ArrowFlightAggregatingPageSource.combine(MergeOp.MIN, a, b, VarcharType.VARCHAR)).isEqualTo(b);
        assertThat(ArrowFlightAggregatingPageSource.combine(MergeOp.MAX, a, b, VarcharType.VARCHAR)).isEqualTo(a);
    }

    @Test
    void combineSumOverSliceDomainIsRejected()
    {
        assertThatThrownBy(() -> ArrowFlightAggregatingPageSource.combine(
            MergeOp.SUM, Slices.utf8Slice("a"), Slices.utf8Slice("b"), VarcharType.VARCHAR))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void combineTreatsNullAsNoContribution()
    {
        // Neither "unvisited slot" nor "a genuinely null wire value" should poison a later merge -
        // both collapse to the other side's value, matching SQL SUM/MIN/MAX/COUNT null-skipping.
        assertThat(ArrowFlightAggregatingPageSource.combine(MergeOp.SUM, null, 5L, BigintType.BIGINT)).isEqualTo(5L);
        assertThat(ArrowFlightAggregatingPageSource.combine(MergeOp.SUM, 5L, null, BigintType.BIGINT)).isEqualTo(5L);
        assertThat(ArrowFlightAggregatingPageSource.combine(MergeOp.SUM, null, null, BigintType.BIGINT)).isNull();
    }

    @Test
    void averageOfDividesSumByCount()
    {
        assertThat(ArrowFlightAggregatingPageSource.averageOf(10L, 4L)).isEqualTo(2.5);
        assertThat(ArrowFlightAggregatingPageSource.averageOf(10.0, 4L)).isEqualTo(2.5);
    }

    @Test
    void averageOfIsNullWhenCountIsZeroOrEitherSideIsNull()
    {
        assertThat(ArrowFlightAggregatingPageSource.averageOf(10L, 0L)).isNull();
        assertThat(ArrowFlightAggregatingPageSource.averageOf(null, 4L)).isNull();
        assertThat(ArrowFlightAggregatingPageSource.averageOf(10L, null)).isNull();
    }

    @Test
    void mergeGroupMapsCombinesMatchingGroupsAndKeepsDisjointOnes()
    {
        // One group-by column (index 0), two aggregate columns: SUM (index 0) and MAX (index 1).
        WireSchema schema = new WireSchema(
            List.of("region", "sum_amount", "max_amount"),
            List.of(VarcharType.VARCHAR, BigintType.BIGINT, BigintType.BIGINT),
            Arrays.asList(null, MergeOp.SUM, MergeOp.MAX),
            1);

        Map<List<Object>, Object[]> target = new LinkedHashMap<>();
        target.put(List.of("us"), new Object[] {100L, 50L});
        target.put(List.of("eu"), new Object[] {10L, 5L});

        Map<List<Object>, Object[]> source = new LinkedHashMap<>();
        source.put(List.of("us"), new Object[] {200L, 80L});
        source.put(List.of("apac"), new Object[] {1L, 1L});

        ArrowFlightAggregatingPageSource.mergeGroupMaps(schema, target, source);

        assertThat(target).containsOnlyKeys(List.of("us"), List.of("eu"), List.of("apac"));
        assertThat(target.get(List.of("us"))).containsExactly(300L, 80L);
        assertThat(target.get(List.of("eu"))).containsExactly(10L, 5L);
        assertThat(target.get(List.of("apac"))).containsExactly(1L, 1L);
    }

    @Test
    void mergeGroupMapsHandlesEmptyGroupByAsSingleGlobalGroup()
    {
        WireSchema schema = new WireSchema(List.of("cnt"), List.of(BigintType.BIGINT), List.of(MergeOp.SUM), 0);

        Map<List<Object>, Object[]> target = new LinkedHashMap<>();
        target.put(List.of(), new Object[] {40L});
        Map<List<Object>, Object[]> source = new LinkedHashMap<>();
        source.put(List.of(), new Object[] {60L});

        ArrowFlightAggregatingPageSource.mergeGroupMaps(schema, target, source);

        assertThat(target).hasSize(1);
        assertThat(target.get(List.of())).containsExactly(100L);
    }
}
