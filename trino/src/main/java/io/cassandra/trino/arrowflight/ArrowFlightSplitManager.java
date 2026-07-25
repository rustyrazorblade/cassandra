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

        List<ArrowFlightSplit> splits = plans.stream()
                                              .map(plan -> new ArrowFlightSplit(handle.keyspace(), handle.table(), plan.tokenRange(), plan.replicas()))
                                              .toList();
        return new FixedSplitSource(splits);
    }
}
