package io.cassandra.trino.arrowflight;

import java.util.List;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import io.cassandra.trino.arrowflight.topology.ArrowFlightTopologyService;
import io.cassandra.trino.arrowflight.topology.SplitPlan;

/**
 * Cluster-aware split planning: one split per token subrange computed by
 * {@link ArrowFlightTopologyService} (ring discovery via cassandra-sidecar + split math via
 * cassandra-analytics-common's {@code TokenPartitioner} - see that class's javadoc), each
 * targeting its owning replica(s) directly.
 *
 * <p>Exception: when the table handle carries a pushed-down aggregation, every subrange is
 * instead bundled into a single split. Trino's {@code applyAggregation} SPI expects the connector
 * to return the fully-final aggregated result and never merges partial per-split aggregates
 * itself, so one split per subrange would each independently compute (and Trino would then
 * stream out) its own local partial COUNT/SUM/GROUP BY - see {@link AggregationMergePlan} and
 * {@code ArrowFlightAggregatingPageSource}, which does the actual cross-subrange merge within
 * this single split. This trades inter-split parallelism for correctness on aggregated queries
 * only; plain scans/filters are unaffected.
 *
 * <p>Split planning does one synchronous (blocking) round trip to sidecar per {@code getSplits}
 * call - acceptable here since this runs once per query at planning time, not per row/batch.
 */
public class ArrowFlightSplitManager implements ConnectorSplitManager
{
    private final ArrowFlightTopologyService topology;

    public ArrowFlightSplitManager(ArrowFlightTopologyService topology)
    {
        this.topology = topology;
    }

    @Override
    public ConnectorSplitSource getSplits(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorTableHandle table,
        DynamicFilter dynamicFilter,
        Constraint constraint)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        List<SplitPlan> plans = topology.splitPlan(handle.keyspace()).join();

        if (handle.aggregation().isPresent())
        {
            List<ArrowFlightSplit.SubRange> subRanges = plans.stream()
                                                              .map(plan -> new ArrowFlightSplit.SubRange(plan.tokenRange(), plan.replicas()))
                                                              .toList();
            return new FixedSplitSource(List.of(new ArrowFlightSplit(handle.keyspace(), handle.table(), subRanges)));
        }

        List<ArrowFlightSplit> splits = plans.stream()
                                              .map(plan -> new ArrowFlightSplit(handle.keyspace(), handle.table(), plan.tokenRange(), plan.replicas()))
                                              .toList();
        return new FixedSplitSource(splits);
    }
}
