package io.cassandra.trino.arrowflight.topology;

import java.util.List;

import io.trino.spi.HostAddress;

import io.cassandra.trino.arrowflight.ticket.TokenRange;

/**
 * One computed token-range split: the {@code (start, end]} subrange itself, plus an ordered list
 * of candidate replica Arrow Flight addresses (primary first, then fallbacks) able to serve it.
 *
 * <p><b>Replica-selection simplification</b>: {@code cassandra-analytics}'s
 * {@code PartitionedDataLayer}/{@code AvailabilityHint}/consistency-level machinery is not
 * reproduced here. Trino's SPI has no consistency-level concept, so this connector's contract is
 * simpler and documented up front: try {@link #replicas()} in order, first one that responds to
 * {@code DoGet} wins (see {@code ArrowFlightPageSource}). This is a v1 simplification, not a
 * faithful CL emulation - it is closer to CQL driver behavior at {@code ONE} with a fixed replica
 * preference order than to any stronger consistency guarantee.
 */
public record SplitPlan(TokenRange tokenRange, List<HostAddress> replicas)
{
    public SplitPlan
    {
        replicas = List.copyOf(replicas);
    }
}
