package io.cassandra.trino.arrowflight.topology;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;
import org.junit.jupiter.api.Test;

import io.trino.spi.HostAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Ring-topology -&gt; split-plan computation, exercised entirely against synthetic sidecar
 * responses and {@code cassandra-analytics-common} classes directly - no live sidecar, no
 * network. Covers: split coverage (no gaps/overlaps versus the full ring), replica-set
 * resolution per split, SimpleStrategy vs NetworkTopologyStrategy replication parsing, and target
 * split-count scaling with cluster size.
 */
class SplitPlannerTest
{
    private static RingEntry ringEntry(String datacenter, String address, String token)
    {
        return new RingEntry(datacenter, address, 7000, "rack1", "Up", "Normal", "1 KiB", "?", token, address, UUID.randomUUID().toString());
    }

    private static NodeSettings nodeSettings(String partitioner) throws Exception
    {
        return NodeSettings.builder()
                            .releaseVersion("5.0")
                            .partitioner(partitioner)
                            .datacenter("datacenter1")
                            .rpcAddress(InetAddress.getByName("127.0.0.1"))
                            .rpcPort(9042)
                            .tokens(Set.of())
                            .hostId(UUID.randomUUID())
                            .build();
    }

    private static SchemaResponse simpleStrategySchema(String keyspace, int rf)
    {
        return new SchemaResponse(keyspace, "CREATE KEYSPACE " + keyspace
                                             + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '" + rf + "'}"
                                             + "  AND durable_writes = true;");
    }

    private static RingResponse ringOf(RingEntry... entries)
    {
        RingResponse ring = new RingResponse();
        ring.addAll(List.of(entries));
        return ring;
    }

    // Four single-token nodes evenly spaced across the Murmur3 range.
    private static RingResponse fourNodeRing()
    {
        return ringOf(
            ringEntry("datacenter1", "10.0.0.1", "-9223372036854775808"),
            ringEntry("datacenter1", "10.0.0.2", "-4611686018427387904"),
            ringEntry("datacenter1", "10.0.0.3", "0"),
            ringEntry("datacenter1", "10.0.0.4", "4611686018427387904"));
    }

    @Test
    void singleNodeProducesSplitsCoveringTheWholeRing() throws Exception
    {
        RingResponse ring = ringOf(ringEntry("datacenter1", "10.0.0.1", "0"));
        NodeSettings settings = nodeSettings("org.apache.cassandra.dht.Murmur3Partitioner");
        SchemaResponse schema = simpleStrategySchema("ks", 1);

        List<SplitPlan> plans = SplitPlanner.plan(settings, ring, schema, "ks", 4, 9143);

        assertThat(plans).isNotEmpty();
        for (SplitPlan plan : plans)
        {
            assertThat(plan.replicas()).containsExactly(HostAddress.fromParts("10.0.0.1", 9143));
        }
        assertNoGapsOrOverlaps(plans);
    }

    @Test
    void fourNodeRingWithRfOneRoutesEachSplitToExactlyOneReplica() throws Exception
    {
        NodeSettings settings = nodeSettings("org.apache.cassandra.dht.Murmur3Partitioner");
        SchemaResponse schema = simpleStrategySchema("ks", 1);

        List<SplitPlan> plans = SplitPlanner.plan(settings, fourNodeRing(), schema, "ks", 4, 9143);

        assertThat(plans).isNotEmpty();
        for (SplitPlan plan : plans)
        {
            assertThat(plan.replicas()).hasSize(1);
        }
        assertNoGapsOrOverlaps(plans);

        // Every node should own at least one split.
        Set<HostAddress> owners = new HashSet<>();
        plans.forEach(plan -> owners.addAll(plan.replicas()));
        assertThat(owners).hasSize(4);
    }

    @Test
    void fourNodeRingWithRfTwoGivesEachSplitTwoOrderedReplicas() throws Exception
    {
        NodeSettings settings = nodeSettings("org.apache.cassandra.dht.Murmur3Partitioner");
        SchemaResponse schema = simpleStrategySchema("ks", 2);

        List<SplitPlan> plans = SplitPlanner.plan(settings, fourNodeRing(), schema, "ks", 4, 9143);

        assertThat(plans).isNotEmpty();
        for (SplitPlan plan : plans)
        {
            assertThat(plan.replicas()).hasSize(2);
            assertThat(plan.replicas()).doesNotHaveDuplicates();
        }
        assertNoGapsOrOverlaps(plans);
    }

    @Test
    void networkTopologyStrategyIsParsedAndHonoured() throws Exception
    {
        NodeSettings settings = nodeSettings("org.apache.cassandra.dht.Murmur3Partitioner");
        SchemaResponse schema = new SchemaResponse("ks",
            "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '1'}"
            + "  AND durable_writes = true;");

        List<SplitPlan> plans = SplitPlanner.plan(settings, fourNodeRing(), schema, "ks", 4, 9143);

        assertThat(plans).isNotEmpty();
        for (SplitPlan plan : plans)
        {
            assertThat(plan.replicas()).hasSize(1);
        }
        assertNoGapsOrOverlaps(plans);
    }

    @Test
    void higherSplitsPerNodeProducesMoreSplits() throws Exception
    {
        NodeSettings settings = nodeSettings("org.apache.cassandra.dht.Murmur3Partitioner");
        SchemaResponse schema = simpleStrategySchema("ks", 1);

        List<SplitPlan> few = SplitPlanner.plan(settings, fourNodeRing(), schema, "ks", 1, 9143);
        List<SplitPlan> many = SplitPlanner.plan(settings, fourNodeRing(), schema, "ks", 8, 9143);

        assertThat(many.size()).isGreaterThan(few.size());
    }

    @Test
    void arrowFlightPortIsAppliedUniformlyNotDiscoveredPerNode() throws Exception
    {
        NodeSettings settings = nodeSettings("org.apache.cassandra.dht.Murmur3Partitioner");
        SchemaResponse schema = simpleStrategySchema("ks", 1);
        RingResponse ring = ringOf(ringEntry("datacenter1", "10.0.0.1", "0"));

        List<SplitPlan> plans = SplitPlanner.plan(settings, ring, schema, "ks", 1, 55555);

        assertThat(plans).allSatisfy(plan ->
            assertThat(plan.replicas()).containsExactly(HostAddress.fromParts("10.0.0.1", 55555)));
    }

    @Test
    void emptyRingFailsFast() throws Exception
    {
        NodeSettings settings = nodeSettings("org.apache.cassandra.dht.Murmur3Partitioner");
        SchemaResponse schema = simpleStrategySchema("ks", 1);

        assertThatThrownBy(() -> SplitPlanner.plan(settings, new RingResponse(), schema, "ks", 4, 9143))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void malformedSchemaFailsFastWithAClearError() throws Exception
    {
        NodeSettings settings = nodeSettings("org.apache.cassandra.dht.Murmur3Partitioner");
        SchemaResponse schema = new SchemaResponse("ks", "CREATE KEYSPACE ks WITH durable_writes = true;");
        RingResponse ring = ringOf(ringEntry("datacenter1", "10.0.0.1", "0"));

        assertThatThrownBy(() -> SplitPlanner.plan(settings, ring, schema, "ks", 4, 9143))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("ks");
    }

    /** No gaps and no overlaps across the full [minToken, maxToken] Murmur3 space. */
    private static void assertNoGapsOrOverlaps(List<SplitPlan> plans)
    {
        List<BigInteger[]> ranges = plans.stream()
                                          .map(plan -> new BigInteger[] {
                                              new BigInteger(plan.tokenRange().start()),
                                              new BigInteger(plan.tokenRange().end())
                                          })
                                          .sorted((a, b) -> a[0].compareTo(b[0]))
                                          .toList();

        assertThat(ranges.get(0)[0]).isEqualTo(BigInteger.valueOf(2).pow(63).negate());
        assertThat(ranges.get(ranges.size() - 1)[1]).isEqualTo(BigInteger.valueOf(2).pow(63).subtract(BigInteger.ONE));

        for (int i = 1; i < ranges.size(); i++)
        {
            // Open-closed (start, end] ranges are contiguous when previous.end == current.start.
            assertThat(ranges.get(i)[0]).isEqualTo(ranges.get(i - 1)[1]);
        }
    }
}
