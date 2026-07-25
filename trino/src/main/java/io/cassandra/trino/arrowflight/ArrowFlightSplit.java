package io.cassandra.trino.arrowflight;

import java.util.List;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import io.cassandra.trino.arrowflight.ticket.TokenRange;

/**
 * One unit of work: one or more token subranges of {@code keyspace.table}, each with the ordered
 * list of candidate replica Arrow Flight addresses computed for it by ring/topology discovery
 * (see {@code io.cassandra.trino.arrowflight.topology.ArrowFlightTopologyService}).
 *
 * <p>Normally a split carries exactly one subrange (plain scans/filters keep full inter-split
 * parallelism - see {@link ArrowFlightSplitManager}). When an aggregation is pushed down, every
 * subrange is instead bundled into a single split: Trino's {@code applyAggregation} SPI expects
 * the connector to return the fully-final result and never merges partial per-split aggregates
 * itself, so {@code ArrowFlightAggregatingPageSource} must fetch and merge every subrange's
 * partial result within one page source (see {@link AggregationMergePlan}).
 *
 * <p>Unlike the Spark analytics connector's bare-int {@code InputPartition} (which relies on
 * shared broadcast state across executors to resolve a partition ID back into a token range and
 * replica set - not available in Trino, since splits serialize independently to separate
 * workers), this split carries the resolved token ranges and replica addresses directly.
 *
 * <p><b>Replica-selection simplification</b>: see {@code SplitPlan}'s javadoc - try each
 * subrange's replicas in order, first one that responds to {@code DoGet} wins (see
 * {@code ArrowFlightPageSource}). No {@code cassandra-analytics} {@code AvailabilityHint}/
 * consistency-level machinery is reproduced; Trino's SPI has no consistency-level concept.
 */
public record ArrowFlightSplit(String keyspace, String table, List<SubRange> subRanges)
    implements ConnectorSplit
{
    public record SubRange(TokenRange tokenRange, List<HostAddress> replicas)
    {
        public SubRange
        {
            replicas = List.copyOf(replicas);
        }
    }

    public ArrowFlightSplit
    {
        subRanges = List.copyOf(subRanges);
        if (subRanges.isEmpty())
            throw new IllegalArgumentException("A split requires at least one subrange");
    }

    /** Convenience constructor for the common one-subrange-per-split (non-aggregated) case. */
    public ArrowFlightSplit(String keyspace, String table, TokenRange tokenRange, List<HostAddress> replicas)
    {
        this(keyspace, table, List.of(new SubRange(tokenRange, replicas)));
    }

    /** The single subrange - only valid when this split carries exactly one (the non-aggregated case). */
    public TokenRange tokenRange()
    {
        if (subRanges.size() != 1)
            throw new IllegalStateException("tokenRange() requires a single-subrange split, this one has " + subRanges.size());
        return subRanges.get(0).tokenRange();
    }

    /** The single subrange's replicas - only valid when this split carries exactly one (the non-aggregated case). */
    public List<HostAddress> replicas()
    {
        if (subRanges.size() != 1)
            throw new IllegalStateException("replicas() requires a single-subrange split, this one has " + subRanges.size());
        return subRanges.get(0).replicas();
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return subRanges.stream().flatMap(subRange -> subRange.replicas().stream()).distinct().toList();
    }
}
