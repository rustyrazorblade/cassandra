package io.cassandra.trino.arrowflight.topology;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Range;

import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;

import io.trino.spi.HostAddress;

import io.cassandra.trino.arrowflight.ticket.TokenRange;

/**
 * Pure ring-topology &rarr; split-plan computation: given a sidecar {@link RingResponse} +
 * {@link NodeSettings} + {@link SchemaResponse}, builds a {@code cassandra-analytics-common}
 * {@link CassandraRing}/{@link TokenPartitioner} and converts its output into
 * {@link SplitPlan}s. Deliberately has no dependency on {@link org.apache.cassandra.sidecar.client.SidecarClient}
 * itself (only on the plain response DTOs), so it is unit-testable against synthetic data with no
 * live sidecar - see {@code SplitPlannerTest}.
 */
public final class SplitPlanner
{
    private SplitPlanner()
    {
    }

    /**
     * @param nodeSettings   any live node's settings (used only for the cluster-wide partitioner)
     * @param ring           the keyspace's ring - one entry per vnode/token per replica
     * @param schema         the keyspace's schema DDL text (replication factor is parsed out of it)
     * @param keyspace       the keyspace the ring/schema were fetched for
     * @param splitsPerNode  target split count per node (see {@link
     *                       io.cassandra.trino.arrowflight.ArrowFlightConfig#splitsPerNode()})
     * @param arrowFlightPort the cluster-wide Arrow Flight port (uniform; not sidecar-visible - see
     *                        {@link io.cassandra.trino.arrowflight.ArrowFlightConfig})
     */
    public static List<SplitPlan> plan(
        NodeSettings nodeSettings,
        RingResponse ring,
        SchemaResponse schema,
        String keyspace,
        int splitsPerNode,
        int arrowFlightPort)
    {
        Partitioner partitioner = Partitioner.from(nodeSettings.partitioner());
        ReplicationFactor replicationFactor = ReplicationFactorParser.parse(keyspace, schema.schema());

        List<CassandraInstance> instances = toInstances(ring);
        if (instances.isEmpty())
            throw new IllegalStateException("Sidecar ring response for keyspace '" + keyspace + "' contains no instances");

        CassandraRing cassandraRing = new CassandraRing(partitioner, keyspace, replicationFactor, instances);

        int numNodes = countDistinctNodes(instances);
        int defaultParallelism = Math.max(1, splitsPerNode) * numNodes;
        TokenPartitioner tokenPartitioner = new TokenPartitioner(cassandraRing, defaultParallelism, 1);

        List<SplitPlan> plans = new ArrayList<>(tokenPartitioner.subRanges().size());
        for (Range<BigInteger> subRange : tokenPartitioner.subRanges())
        {
            // Every sub-range TokenPartitioner produces is guaranteed to map to exactly one
            // replica set (it never spans two different owners) - so any point in the range
            // (here, the inclusive upper endpoint - ranges are open-closed, see RangeUtils)
            // resolves the whole range's replica set.
            Collection<CassandraInstance> replicas = cassandraRing.getReplicas(subRange.upperEndpoint());
            List<HostAddress> addresses = replicas.stream()
                                                   .map(instance -> HostAddress.fromParts(instance.nodeName(), arrowFlightPort))
                                                   .collect(Collectors.toList());
            plans.add(new SplitPlan(TokenRange.of(subRange.lowerEndpoint(), subRange.upperEndpoint()), addresses));
        }
        return plans;
    }

    private static List<CassandraInstance> toInstances(RingResponse ring)
    {
        List<CassandraInstance> instances = new ArrayList<>();
        // RingResponse is a PriorityQueue<RingEntry>; a plain iterator() does not drain it and
        // does not guarantee sorted order, but CassandraRing sorts instances by token itself, so
        // the order here doesn't matter.
        for (RingEntry entry : ring)
            instances.add(new CassandraInstance(entry.token(), entry.address(), entry.datacenter()));
        return instances;
    }

    private static int countDistinctNodes(List<CassandraInstance> instances)
    {
        Set<String> nodes = new LinkedHashSet<>();
        for (CassandraInstance instance : instances)
            nodes.add(instance.nodeName());
        return nodes.size();
    }
}
