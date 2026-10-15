/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.repair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.ANTICOMPACTIONS;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.ANTICOMPACTION_FALLBACKS;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.BYTES_STREAMED_IN;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.VALIDATIONS;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.VALIDATION_FALLBACKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A cluster in which nodes 1 and 2 validate and anticompact on the cursor path and nodes 3 and 4
 * on the legacy path, on BIG and BTI.  On consistent data, full repair, incremental repair and
 * previews must find the Merkle trees of the two paths equal: every repair succeeds, the previews
 * report the data in sync, and no node streams a byte.  Last, with node 4 down, {@code repair
 * --force} from node 1 succeeds on the live nodes, each on its own path, leaves no sstable pending
 * and marks nothing repaired, and the live replicas agree.
 */
public class CursorRepairMixedClusterTest extends TestBaseImpl
{
    private static final int NODES = 4;
    private static final int KEYS = 400;

    @Test
    public void big() throws Exception
    {
        run("big");
    }

    @Test
    public void bti() throws Exception
    {
        run("bti");
    }

    private static void run(String format) throws Exception
    {
        try (Cluster cluster = Cluster.build(NODES)
                                      .withNodeProvisionStrategy(INodeProvisionStrategy.Strategy.OneNetworkInterface)
                                      .withDynamicPortAllocation(true)
                                      .withVNodes()
                                      .withTokenCount(4)
                                      .withConfig(config -> CursorIncrementalRepairTestBase.productionStorage(config).with(NETWORK, GOSSIP)
                                                                  .set("hinted_handoff_enabled", false)
                                                                  .set("cursor_compaction_enabled", config.num() <= 2)
                                                                  .set("cursor_repair_enabled", config.num() <= 2)
                                                                  .set("sstable", Map.of("selected_format", format))
                                                                  .set("storage_compatibility_mode", "NONE"))
                                      .start())
        {
            CursorIncrementalRepairTestBase.assertProductionStorage(cluster);
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE ks.t (k int, c int, v text, s text static, PRIMARY KEY (k, c))");
            for (int node = 1; node <= NODES; node++)
                cluster.get(node).nodetoolResult("disableautocompaction", "ks", "t").asserts().success();
            write(cluster, "first");

            long[][] before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
            cluster.get(1).nodetoolResult("repair", "-full", "ks", "t").asserts().success();
            assertPaths(cluster, before, "full repair", false);

            before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
            for (int node = 1; node <= NODES; node++)
                cluster.get(node).nodetoolResult("repair", "ks", "t").asserts().success();
            assertPaths(cluster, before, "incremental repair", true);

            before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
            cluster.get(1).nodetoolResult("repair", "--validate", "ks", "t").asserts().success().notificationContains("Repaired data is in sync");
            assertPaths(cluster, before, "preview of the repaired data", false);

            write(cluster, "second");
            before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
            cluster.get(1).nodetoolResult("repair", "--preview", "ks", "t").asserts().success().notificationContains("Previewed data was in sync");
            assertPaths(cluster, before, "preview of the unrepaired data", false);

            before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
            cluster.get(1).nodetoolResult("repair", "--preview", "--full", "ks", "t").asserts().success().notificationContains("Previewed data was in sync");
            assertPaths(cluster, before, "preview of all data", false);

            before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
            cluster.get(1).nodetoolResult("repair", "-seq", "-full", "ks", "t").asserts().success();
            assertPaths(cluster, before, "sequential full repair", false);

            // the incremental repair of the second writes, limited to the one data center
            String dc = cluster.get(1).config().localDatacenter();
            before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
            for (int node = 1; node <= NODES; node++)
                cluster.get(node).nodetoolResult("repair", "-dc", dc, "ks", "t").asserts().success();
            assertPaths(cluster, before, "incremental repair of data center " + dc, true);
            cluster.get(1).nodetoolResult("repair", "--validate", "ks", "t").asserts().success().notificationContains("Repaired data is in sync");

            // with node 4 down, --force repairs on the live replicas and marks nothing repaired
            write(cluster, "third");
            // the last sessions' sstables must have left the pending set
            CursorIncrementalRepairTestBase.awaitNoPendingSSTables(cluster, "ks", "t");
            Map<Integer, Set<Long>> repairedAtBefore = repairedAt(cluster, 1, 2, 3);
            cluster.get(4).shutdown().get();
            before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
            cluster.get(1).nodetoolResult("repair", "--force", "ks", "t").asserts().success();
            assertPaths(cluster, before, "forced repair with node 4 down", true);
            CursorIncrementalRepairTestBase.awaitNoPendingSSTables(cluster, "ks", "t");
            // compaction may merge repaired sstables, but no sstable gets a new repaired time
            Map<Integer, Set<Long>> repairedAtAfter = repairedAt(cluster, 1, 2, 3);
            for (int node = 1; node <= 3; node++)
                assertTrue("node " + node + ": the forced repair marked sstables repaired: before " + repairedAtBefore.get(node) + ", after " + repairedAtAfter.get(node),
                           repairedAtBefore.get(node).containsAll(repairedAtAfter.get(node)));
            assertLiveReplicasAgree(cluster, 1, 2, 3);
        }
    }

    /** On each node, the repairedAt times of its repaired sstables. */
    private static Map<Integer, Set<Long>> repairedAt(Cluster cluster, int... nodes)
    {
        Map<Integer, Set<Long>> times = new HashMap<>();
        for (int node : nodes)
            times.put(node, cluster.get(node).callOnInstance(() -> {
                Set<Long> found = new HashSet<>();
                for (SSTableReader sstable : Keyspace.open("ks").getColumnFamilyStore("t").getLiveSSTables())
                    if (sstable.isRepaired())
                        found.add(sstable.getRepairedAt());
                return found;
            }));
        return times;
    }

    /** For every key, the live nodes among its replicas hold the same rows, and hold some. */
    private static void assertLiveReplicasAgree(Cluster cluster, int... nodes)
    {
        Map<String, Integer> byAddress = new HashMap<>();
        Map<Integer, Map<Integer, List<String>>> rowsByNode = new HashMap<>();
        for (int node : nodes)
        {
            byAddress.put(cluster.get(node).callOnInstance(() -> org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort()), node);
            Map<Integer, List<String>> rows = new HashMap<>();
            for (Object[] row : cluster.get(node).executeInternal("SELECT k, c, v, s FROM ks.t"))
                rows.computeIfAbsent((Integer) row[0], k -> new ArrayList<>()).add(row[1] + "=" + row[2] + "/" + row[3]);
            rowsByNode.put(node, rows);
        }
        Map<Integer, List<String>> replicas = cluster.get(nodes[0]).callOnInstance(() -> {
            Map<Integer, List<String>> found = new HashMap<>();
            for (int k = 0; k < KEYS; k++)
            {
                List<String> endpoints = new ArrayList<>();
                for (InetAddressAndPort endpoint : StorageService.instance.getNaturalReplicasForToken("ks", "t", String.valueOf(k)).endpoints())
                    endpoints.add(endpoint.getHostAddressAndPort());
                found.put(k, endpoints);
            }
            return found;
        });
        for (Map.Entry<Integer, List<String>> key : replicas.entrySet())
        {
            List<String> agreed = null;
            for (String endpoint : key.getValue())
            {
                Integer node = byAddress.get(endpoint);
                if (node == null)
                    continue;
                List<String> mine = rowsByNode.get(node).getOrDefault(key.getKey(), List.of());
                assertTrue("node " + node + " does not hold key " + key.getKey(), !mine.isEmpty());
                if (agreed == null)
                    agreed = mine;
                assertEquals("the live replicas of key " + key.getKey() + " disagree (node " + node + ")", agreed, mine);
            }
        }
    }

    /** Writes the same rows to every replica, with deletions and a static column, and flushes. */
    private static void write(Cluster cluster, String value)
    {
        for (int k = 0; k < KEYS; k++)
        {
            cluster.coordinator(1).execute("UPDATE ks.t SET s = ? WHERE k = ?", ConsistencyLevel.ALL, value, k);
            for (int c = 0; c < 4; c++)
                cluster.coordinator(1).execute("INSERT INTO ks.t (k, c, v) VALUES (?, ?, ?)", ConsistencyLevel.ALL, k, c, value);
            if (k % 9 == 0)
                cluster.coordinator(1).execute("DELETE FROM ks.t WHERE k = ? AND c >= ? AND c < ?", ConsistencyLevel.ALL, k, 1, 3);
        }
        for (int node = 1; node <= NODES; node++)
            cluster.get(node).flush("ks");
    }

    /**
     * Nodes 1 and 2 validated, and anticompacted if {@code anticompacts}, on the cursor path, and
     * nodes 3 and 4 did not; nothing fell back; no node streamed a byte.  Nodes that are down are
     * skipped.
     */
    private static void assertPaths(Cluster cluster, long[][] before, String what, boolean anticompacts)
    {
        long[][] after = CursorIncrementalRepairTestBase.cursorCounts(cluster);
        for (int node = 1; node <= NODES; node++)
        {
            // a node that is down has no counts
            if (after[node - 1] == null)
                continue;
            long[] delta = new long[after[node - 1].length];
            for (int i = 0; i < delta.length; i++)
                delta[i] = after[node - 1][i] - before[node - 1][i];
            String where = what + ", node " + node + ": ";
            boolean cursor = node <= 2;
            assertEquals(where + "cursor validation", cursor, delta[VALIDATIONS] > 0);
            assertEquals(where + "fell back in validation", 0, delta[VALIDATION_FALLBACKS]);
            if (anticompacts)
                assertEquals(where + "cursor anticompaction", cursor, delta[ANTICOMPACTIONS] > 0);
            else
                assertEquals(where + "anticompacted", 0, delta[ANTICOMPACTIONS]);
            assertEquals(where + "fell back in anticompaction", 0, delta[ANTICOMPACTION_FALLBACKS]);
            assertEquals(where + "streamed bytes on consistent data", 0, delta[BYTES_STREAMED_IN]);
        }
    }
}
