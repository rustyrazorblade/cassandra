package io.cassandra.trino.arrowflight;

import java.util.List;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import io.cassandra.trino.arrowflight.ticket.TokenRange;

/**
 * One unit of work: a single token subrange of {@code keyspace.table}, plus the ordered list of
 * candidate replica Arrow Flight addresses computed for it by ring/topology discovery (see
 * {@code io.cassandra.trino.arrowflight.topology.ArrowFlightTopologyService}).
 *
 * <p>Unlike the Spark analytics connector's bare-int {@code InputPartition} (which relies on
 * shared broadcast state across executors to resolve a partition ID back into a token range and
 * replica set - not available in Trino, since splits serialize independently to separate
 * workers), this split carries the resolved token range and replica addresses directly.
 *
 * <p><b>Replica-selection simplification</b>: see {@code SplitPlan}'s javadoc - try
 * {@link #replicas()} in order, first one that responds to {@code DoGet} wins (see
 * {@code ArrowFlightPageSource}). No {@code cassandra-analytics} {@code AvailabilityHint}/
 * consistency-level machinery is reproduced; Trino's SPI has no consistency-level concept.
 */
public record ArrowFlightSplit(String keyspace, String table, TokenRange tokenRange, List<HostAddress> replicas)
    implements ConnectorSplit
{
    public ArrowFlightSplit
    {
        replicas = List.copyOf(replicas);
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return replicas;
    }
}
