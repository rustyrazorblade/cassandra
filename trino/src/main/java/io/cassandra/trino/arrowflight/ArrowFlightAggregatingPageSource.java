package io.cassandra.trino.arrowflight;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;

import io.cassandra.trino.arrowflight.AggregationMergePlan.Average;
import io.cassandra.trino.arrowflight.AggregationMergePlan.Direct;
import io.cassandra.trino.arrowflight.AggregationMergePlan.MergeOp;
import io.cassandra.trino.arrowflight.ticket.AggregationSpec;
import io.cassandra.trino.arrowflight.ticket.ArrowFlightTicket;

/**
 * The page source used whenever {@link ArrowFlightTableHandle#aggregation()} is present: fetches
 * every subrange in the split (see {@link ArrowFlightSplitManager} - every subrange is bundled
 * into a single split exactly when an aggregation is pushed down) and merges their partial
 * per-group results into the single final row-per-group Trino expects, per the plan in {@link
 * ArrowFlightTableHandle#mergePlan()} (see {@link AggregationMergePlan} for why this merge is
 * necessary at all: Trino's {@code applyAggregation} SPI never does it itself).
 *
 * <p>Unlike the streaming {@link ArrowFlightPageSource}, this fetches and merges everything
 * eagerly in the constructor (matching {@code ArrowFlightPageSource}'s own precedent of opening
 * its Flight stream(s) eagerly in its constructor) - reasonable given a pushed-down aggregation's
 * result set is, by definition, reduced/grouped and expected to be small relative to the scanned
 * data. Every subrange's fetch is a blocking Flight {@code DoGet} round trip; collapsing every
 * subrange into this one split (see {@link ArrowFlightSplitManager}) would otherwise serialize
 * all of that network I/O onto a single thread, so the fetches are fanned out onto the supplied
 * {@code executor} and joined before merging - this is the only place aggregated queries lose the
 * inter-split parallelism plain scans/filters keep, and even that loss is now confined to the
 * merge step, not the network fetch itself.
 *
 * <p><b>Known scope gap</b>: a global aggregation (no {@code GROUP BY}) over a table with zero
 * matching rows in <em>every</em> subrange produces zero output rows here, whereas standard SQL
 * semantics for a global aggregate (e.g. {@code SELECT COUNT(*) FROM empty}) return exactly one
 * row ({@code count = 0}). This matches the pre-existing single-split behavior's own edge case
 * (whether Cassandra's {@code RowAggregator} emits a zero-row for an empty scan is unchanged by
 * this class) and is not specially handled here.
 */
public class ArrowFlightAggregatingPageSource implements ConnectorPageSource
{
    private static final int MAX_ROWS_PER_PAGE = 4096;

    private final List<Type> outputTypes;
    private final Queue<Page> pages;
    private long completedPositions;
    private boolean closed;

    public ArrowFlightAggregatingPageSource(
        ArrowFlightClient client,
        ExecutorService executor,
        ArrowFlightSplit split,
        ArrowFlightTableHandle tableHandle,
        List<ArrowFlightColumnHandle> columns)
    {
        AggregationSpec aggregation = tableHandle.aggregation()
            .orElseThrow(() -> new IllegalStateException("ArrowFlightAggregatingPageSource requires a pushed-down aggregation"));
        List<AggregationMergePlan> mergePlan = tableHandle.mergePlan()
            .orElseThrow(() -> new IllegalStateException("ArrowFlightAggregatingPageSource requires a merge plan"));

        Map<String, Type> outputTypeByName = new LinkedHashMap<>();
        for (ArrowFlightColumnHandle column : columns)
            outputTypeByName.put(column.name(), column.type());

        List<String> groupByNames = aggregation.groupBy();
        List<Type> groupByTypes = groupByNames.stream().map(outputTypeByName::get).toList();

        WireSchema wireSchema = buildWireSchema(groupByNames, groupByTypes, mergePlan, outputTypeByName);

        // Dispatch every subrange's fetch concurrently; each produces its own independent group
        // map (no shared mutable state, no locking needed), then fold them together sequentially
        // as each completes - the folding itself is pure in-memory work, cheap relative to the
        // network fetch it's waiting on.
        List<CompletableFuture<Map<List<Object>, Object[]>>> futures = split.subRanges().stream()
            .map(subRange -> CompletableFuture.supplyAsync(
                () -> fetchSubRange(client, split, tableHandle, subRange, wireSchema), executor))
            .toList();

        Map<List<Object>, Object[]> mergedByGroup = new LinkedHashMap<>();
        try
        {
            for (CompletableFuture<Map<List<Object>, Object[]>> future : futures)
                mergeGroupMaps(wireSchema, mergedByGroup, future.join());
        }
        catch (CompletionException e)
        {
            // Best-effort: a future already running its blocking DoGet won't observe this
            // interrupt, but any future still queued on the bounded executor (see
            // ArrowFlightConnector.AGGREGATION_FETCH_CONCURRENCY) is cancelled before it starts,
            // instead of doing wasted work against the cluster after we've already given up.
            futures.forEach(future -> future.cancel(true));
            if (e.getCause() instanceof RuntimeException runtimeException)
                throw runtimeException;
            throw e;
        }

        this.outputTypes = columns.stream().map(ArrowFlightColumnHandle::type).toList();
        List<String> outputNames = columns.stream().map(ArrowFlightColumnHandle::name).toList();
        this.pages = buildOutputPages(outputNames, outputTypes, groupByNames, mergePlan, wireSchema, mergedByGroup);
    }

    /**
     * The wire fetch schema: groupBy columns (as-is) followed by every wire aggregate column,
     * flattened. Package-private (not {@code private}) so the merge arithmetic below - {@link
     * #combine}/{@link #mergeGroupMaps} - can be exercised directly with synthetic data in tests,
     * without needing a live Flight server.
     */
    record WireSchema(List<String> columnNames, List<Type> columnTypes, List<MergeOp> aggMergeOps, int groupByCount)
    {
    }

    private static WireSchema buildWireSchema(
        List<String> groupByNames, List<Type> groupByTypes, List<AggregationMergePlan> mergePlan, Map<String, Type> outputTypeByName)
    {
        List<String> names = new ArrayList<>(groupByNames);
        List<Type> types = new ArrayList<>(groupByTypes);
        List<MergeOp> mergeOps = new ArrayList<>();
        for (int i = 0; i < groupByNames.size(); i++)
            mergeOps.add(null);

        for (AggregationMergePlan column : mergePlan)
        {
            if (column instanceof Direct direct)
            {
                names.add(direct.wireColumn());
                types.add(outputTypeByName.get(direct.outputName()));
                mergeOps.add(direct.mergeOp());
            }
            else if (column instanceof Average average)
            {
                names.add(average.sumWireColumn());
                types.add(average.sumDomain() == AggregationMergePlan.NumericDomain.INTEGRAL ? BigintType.BIGINT : DoubleType.DOUBLE);
                mergeOps.add(MergeOp.SUM);
                names.add(average.countWireColumn());
                types.add(BigintType.BIGINT);
                mergeOps.add(MergeOp.SUM);
            }
        }
        return new WireSchema(names, types, mergeOps, groupByNames.size());
    }

    /** Fetches one subrange and returns its own independent group map - safe to build concurrently with other subranges. */
    private static Map<List<Object>, Object[]> fetchSubRange(
        ArrowFlightClient client,
        ArrowFlightSplit split,
        ArrowFlightTableHandle tableHandle,
        ArrowFlightSplit.SubRange subRange,
        WireSchema wireSchema)
    {
        ArrowFlightTicket ticket = ArrowFlightTicket.of(split.keyspace(), split.table())
                                                     .withTokenRange(subRange.tokenRange());
        if (tableHandle.filter().isPresent())
            ticket = ticket.withFilter(tableHandle.filter().get());
        ticket = ticket.withAggregation(tableHandle.aggregation().get());

        Map<List<Object>, Object[]> bySubRangeGroup = new LinkedHashMap<>();
        try (ArrowFlightClient.StreamHandle streamHandle = client.openFirstAvailable(subRange.replicas(), new Ticket(ticket.toJsonBytes())))
        {
            FlightStream stream = streamHandle.stream();
            boolean hasNext = streamHandle.hasPreloadedFirst() ? streamHandle.consumePreloadedHasNext() : stream.next();
            while (hasNext)
            {
                Page page = ArrowPageBuilder.toPage(stream.getRoot(), wireSchema.columnNames(), wireSchema.columnTypes());
                mergePage(page, wireSchema, bySubRangeGroup);
                hasNext = stream.next();
            }
        }
        return bySubRangeGroup;
    }

    private static void mergePage(Page page, WireSchema wireSchema, Map<List<Object>, Object[]> mergedByGroup)
    {
        int groupByCount = wireSchema.groupByCount();
        int aggColumnCount = wireSchema.columnNames().size() - groupByCount;
        for (int position = 0; position < page.getPositionCount(); position++)
        {
            List<Object> groupKey = new ArrayList<>(groupByCount);
            for (int i = 0; i < groupByCount; i++)
                groupKey.add(readValue(wireSchema.columnTypes().get(i), page.getBlock(i), position));

            Object[] merged = mergedByGroup.computeIfAbsent(groupKey, k -> new Object[aggColumnCount]);
            for (int i = 0; i < aggColumnCount; i++)
            {
                Type type = wireSchema.columnTypes().get(groupByCount + i);
                Object value = readValue(type, page.getBlock(groupByCount + i), position);
                merged[i] = combine(wireSchema.aggMergeOps().get(groupByCount + i), merged[i], value, type);
            }
        }
    }

    /** Folds one subrange's group map into the running total, applying each aggregate column's merge op. */
    static void mergeGroupMaps(WireSchema wireSchema, Map<List<Object>, Object[]> target, Map<List<Object>, Object[]> source)
    {
        int groupByCount = wireSchema.groupByCount();
        int aggColumnCount = wireSchema.columnNames().size() - groupByCount;
        for (Map.Entry<List<Object>, Object[]> entry : source.entrySet())
        {
            Object[] existing = target.get(entry.getKey());
            if (existing == null)
            {
                target.put(entry.getKey(), entry.getValue());
                continue;
            }
            Object[] incoming = entry.getValue();
            for (int i = 0; i < aggColumnCount; i++)
            {
                Type type = wireSchema.columnTypes().get(groupByCount + i);
                existing[i] = combine(wireSchema.aggMergeOps().get(groupByCount + i), existing[i], incoming[i], type);
            }
        }
    }

    static Object combine(MergeOp op, Object a, Object b, Type type)
    {
        if (a == null)
            return b;
        if (b == null)
            return a;
        Class<?> javaType = type.getJavaType();
        if (javaType == long.class)
        {
            long x = (Long) a;
            long y = (Long) b;
            return switch (op)
            {
                case SUM -> x + y;
                case MIN -> Math.min(x, y);
                case MAX -> Math.max(x, y);
            };
        }
        if (javaType == double.class)
        {
            double x = (Double) a;
            double y = (Double) b;
            return switch (op)
            {
                case SUM -> x + y;
                case MIN -> Math.min(x, y);
                case MAX -> Math.max(x, y);
            };
        }
        if (javaType == Slice.class)
        {
            Slice x = (Slice) a;
            Slice y = (Slice) b;
            return switch (op)
            {
                case MIN -> x.compareTo(y) <= 0 ? x : y;
                case MAX -> x.compareTo(y) >= 0 ? x : y;
                case SUM -> throw new IllegalArgumentException("SUM is not supported over VARCHAR/VARBINARY");
            };
        }
        throw new IllegalArgumentException("Unsupported merge domain for type " + type + " (javaType=" + javaType + ")");
    }

    private static Object readValue(Type type, Block block, int position)
    {
        if (block.isNull(position))
            return null;
        Class<?> javaType = type.getJavaType();
        if (javaType == long.class)
            return type.getLong(block, position);
        if (javaType == double.class)
            return type.getDouble(block, position);
        if (javaType == boolean.class)
            return type.getBoolean(block, position);
        if (javaType == Slice.class)
            return type.getSlice(block, position);
        return type.getObject(block, position);
    }

    private static void writeValue(Type type, Object value, BlockBuilder builder)
    {
        if (value == null)
        {
            builder.appendNull();
            return;
        }
        Class<?> javaType = type.getJavaType();
        if (javaType == long.class)
            type.writeLong(builder, (Long) value);
        else if (javaType == double.class)
            type.writeDouble(builder, (Double) value);
        else if (javaType == boolean.class)
            type.writeBoolean(builder, (Boolean) value);
        else if (javaType == Slice.class)
            type.writeSlice(builder, (Slice) value);
        else
            type.writeObject(builder, value);
    }

    private static Queue<Page> buildOutputPages(
        List<String> outputNames,
        List<Type> outputTypes,
        List<String> groupByNames,
        List<AggregationMergePlan> mergePlan,
        WireSchema wireSchema,
        Map<List<Object>, Object[]> mergedByGroup)
    {
        Map<String, Integer> groupByIndex = new LinkedHashMap<>();
        for (int i = 0; i < groupByNames.size(); i++)
            groupByIndex.put(groupByNames.get(i), i);

        Map<String, AggregationMergePlan> derivationByOutputName = new LinkedHashMap<>();
        for (AggregationMergePlan column : mergePlan)
            derivationByOutputName.put(column.outputName(), column);

        // Wire aggregate column name -> its flattened index within a group's merged Object[].
        Map<String, Integer> wireAggIndex = new LinkedHashMap<>();
        for (int i = wireSchema.groupByCount(); i < wireSchema.columnNames().size(); i++)
            wireAggIndex.put(wireSchema.columnNames().get(i), i - wireSchema.groupByCount());

        Queue<Page> pages = new ArrayDeque<>();
        List<List<Object>> groupKeys = new ArrayList<>(mergedByGroup.keySet());
        for (int start = 0; start < groupKeys.size(); start += MAX_ROWS_PER_PAGE)
        {
            int end = Math.min(start + MAX_ROWS_PER_PAGE, groupKeys.size());
            int rowCount = end - start;
            BlockBuilder[] builders = new BlockBuilder[outputTypes.size()];
            for (int c = 0; c < outputTypes.size(); c++)
                builders[c] = outputTypes.get(c).createBlockBuilder(null, rowCount);

            for (int row = start; row < end; row++)
            {
                List<Object> groupKey = groupKeys.get(row);
                Object[] merged = mergedByGroup.get(groupKey);
                for (int c = 0; c < outputNames.size(); c++)
                {
                    String name = outputNames.get(c);
                    Integer groupByPos = groupByIndex.get(name);
                    if (groupByPos != null)
                    {
                        writeValue(outputTypes.get(c), groupKey.get(groupByPos), builders[c]);
                        continue;
                    }
                    AggregationMergePlan derivation = derivationByOutputName.get(name);
                    if (derivation instanceof Direct direct)
                    {
                        writeValue(outputTypes.get(c), merged[wireAggIndex.get(direct.wireColumn())], builders[c]);
                    }
                    else if (derivation instanceof Average average)
                    {
                        Object sum = merged[wireAggIndex.get(average.sumWireColumn())];
                        Object count = merged[wireAggIndex.get(average.countWireColumn())];
                        writeValue(outputTypes.get(c), averageOf(sum, count), builders[c]);
                    }
                    else
                    {
                        throw new IllegalStateException("No group-by or aggregate derivation found for output column '" + name + "'");
                    }
                }
            }

            Block[] blocks = new Block[outputTypes.size()];
            for (int c = 0; c < outputTypes.size(); c++)
                blocks[c] = builders[c].build();
            pages.add(new Page(rowCount, blocks));
        }
        return pages;
    }

    /** {@code AVG = SUM / COUNT}; null if there was no non-null contribution or the count is zero. */
    static Object averageOf(Object sum, Object count)
    {
        if (sum == null || count == null)
            return null;
        long countValue = (Long) count;
        if (countValue == 0)
            return null;
        double sumValue = sum instanceof Long longSum ? (double) longSum : (Double) sum;
        return sumValue / countValue;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        Page page = pages.poll();
        if (page == null)
        {
            closed = true;
            return null;
        }
        completedPositions += page.getPositionCount();
        return SourcePage.create(page);
    }

    @Override
    public boolean isFinished()
    {
        return closed && pages.isEmpty();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public java.util.OptionalLong getCompletedPositions()
    {
        return java.util.OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closed = true;
        pages.clear();
    }
}
