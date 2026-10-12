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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.ANTICOMPACTIONS;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.ANTICOMPACTION_FALLBACKS;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.PARTITIONS_WRITTEN;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.VALIDATIONS;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.VALIDATION_FALLBACKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Repair options on two data centers, nodes 1 and 2 in dc1 with two replicas and nodes 3 and 4
 * in dc2 with one replica, on BIG and BTI, with cursor_repair_enabled on:
 * {@code -dc} and {@code -hosts} over one range from node 1; from every node in turn, the whole
 * cluster, {@code -dcpar}, sequential repair (which validates snapshots) and full repair; and from
 * node 1, previews of the unrepaired data and of all data.  Each step first writes a row that
 * only node 1 gets.  After each step, every node that took part validated on the cursor path, an
 * incremental repair anticompacted on it, and nothing fell back.  A preview reports the row
 * that only node 1 has.  After a repair of one range, its replicas that took part agree on it;
 * after a full repair, all replicas agree; after an incremental repair of the whole ring, every
 * sstable is repaired, all replicas agree, and a preview of the repaired data is in sync.  Then
 * incremental repair from every node of an Accord table, on the cursor path with the replicas
 * agreeing.  Last,
 * {@code nodetool repair} with no keyspace, from every node, repairs every table of several
 * keyspaces.  A second cluster, with one token for each node, repairs a keyspace with a
 * transient replica in dc1 the same way.
 */
public class CursorRepairMultiDataCenterTest extends TestBaseImpl
{
    private static final int NODES = 4;
    private static final int KEYS = 300;

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

    @Test
    public void transientReplicaBig() throws Exception
    {
        runTransient("big");
    }

    @Test
    public void transientReplicaBti() throws Exception
    {
        runTransient("bti");
    }

    /**
     * A keyspace with a transient replica in dc1, on a cluster with one token for each node, as
     * transient replication allows no more: incremental repair from every node, on the cursor
     * path, with the full replicas agreeing.
     */
    private static void runTransient(String format) throws Exception
    {
        try (Cluster cluster = Cluster.build()
                                      .withDC("dc1", 2)
                                      .withDC("dc2", 2)
                                      .withNodeProvisionStrategy(INodeProvisionStrategy.Strategy.OneNetworkInterface)
                                      .withDynamicPortAllocation(true)
                                      .withoutVNodes()
                                      .withConfig(config -> CursorIncrementalRepairTestBase.productionStorage(config).with(NETWORK, GOSSIP)
                                                                  .set("hinted_handoff_enabled", false)
                                                                  .set("transient_replication_enabled", true)
                                                                  .set("cursor_compaction_enabled", true)
                                                                  .set("cursor_repair_enabled", true)
                                                                  .set("sstable", Map.of("selected_format", format)))
                                      .start())
        {
            CursorIncrementalRepairTestBase.assertProductionStorage(cluster);
            cluster.schemaChange("CREATE KEYSPACE ks_transient WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '2/1', 'dc2': 1}");
            cluster.schemaChange("CREATE TABLE ks_transient.t1 (k int, c int, v text, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
            assertIncrementalRepairFromEveryNode(cluster, "ks_transient", "t1", new int[]{ 1, 2, 3, 4 });
        }
    }

    private static void run(String format) throws Exception
    {
        try (Cluster cluster = Cluster.build()
                                      .withDC("dc1", 2)
                                      .withDC("dc2", 2)
                                      .withNodeProvisionStrategy(INodeProvisionStrategy.Strategy.OneNetworkInterface)
                                      .withDynamicPortAllocation(true)
                                      .withVNodes()
                                      .withTokenCount(4)
                                      .withConfig(config -> CursorIncrementalRepairTestBase.productionStorage(config).with(NETWORK, GOSSIP)
                                                                  .set("hinted_handoff_enabled", false)
                                                                  .set("cursor_compaction_enabled", true)
                                                                  .set("cursor_repair_enabled", true)
                                                                  .set("sstable", Map.of("selected_format", format)))
                                      .start())
        {
            CursorIncrementalRepairTestBase.assertProductionStorage(cluster);
            createKeyspace(cluster, "ks");
            int[] all = { 1, 2, 3, 4 };
            Map<String[], Set<Integer>> replicasOfNode1 = replicasOfNode1(cluster, "ks");
            // -dc dc1 and -hosts over one range of node 1, as the dc1 nodes hold every row, so a
            // repair of all their ranges has nothing to split; -hosts with a dc2 node that shares
            // the range
            Map.Entry<String[], Set<Integer>> shared = replicasOfNode1.entrySet().stream()
                                                                      .filter(e -> Long.parseLong(e.getKey()[0]) < Long.parseLong(e.getKey()[1]))
                                                                      .findFirst().get();
            String[] sharedRange = shared.getKey();
            int partner = shared.getValue().stream().filter(n -> n > 2).findFirst().get();
            // each option, the nodes it runs from, the nodes that take part, and whether it is an
            // incremental repair.  Which nodes of an incremental repair anticompact depends on the
            // data each holds outside the repaired ranges, so only some must.
            Object[][] options = {
                { new String[]{ "-dc", "dc1", "-st", sharedRange[0], "-et", sharedRange[1] }, new int[]{ 1 }, new int[]{ 1, 2 }, true },
                { new String[]{ "-hosts", address(cluster, 1) + ',' + address(cluster, partner), "-st", sharedRange[0], "-et", sharedRange[1] },
                  new int[]{ 1 }, new int[]{ 1, partner }, true },
                { new String[]{}, all, all, true },
                { new String[]{ "-dcpar" }, all, all, true },
                { new String[]{ "-seq", "-full" }, all, all, false },
                { new String[]{ "-full" }, all, all, false },
                // from node 1 only: it replicates every key, so its preview sees the row that only it got
                { new String[]{ "--preview" }, new int[]{ 1 }, all, false },
                { new String[]{ "--preview", "--full" }, new int[]{ 1 }, all, false },
            };
            for (int i = 0; i < options.length; i++)
            {
                String[] option = (String[]) options[i][0];
                int[] from = (int[]) options[i][1];
                int[] taking = (int[]) options[i][2];
                boolean incremental = (Boolean) options[i][3];
                write(cluster, "ks", "t1", i);
                long[][] before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
                List<String> command = new ArrayList<>(List.of("repair"));
                command.addAll(Arrays.asList(option));
                command.addAll(List.of("ks", "t1"));
                boolean isPreview = option.length > 0 && option[0].equals("--preview");
                for (int node : from)
                {
                    org.apache.cassandra.distributed.api.NodeToolResult result = cluster.get(node).nodetoolResult(command.toArray(new String[0]));
                    result.asserts().success();
                    // the row that only node 1 got in this step is not repaired yet
                    if (isPreview)
                        result.asserts().notificationContains("Preview complete");
                    // the next session must not meet this one's pending sstables
                    CursorIncrementalRepairTestBase.awaitNoPendingSSTables(cluster, "ks", "t1");
                }
                assertCursorPaths(cluster, before, Arrays.toString(option), taking, incremental);
                String what = Arrays.toString(option);
                if (option.length > 2 && option[option.length - 4].equals("-st"))
                {
                    // a repair of one range: its replicas that took part agree on it
                    assertReplicasAgree(cluster, "ks", "t1", taking, Long.parseLong(sharedRange[0]), Long.parseLong(sharedRange[1]), what);
                }
                else if (incremental)
                {
                    // the whole ring from every node: all repaired, the replicas agree, and so does the repaired data
                    assertAllRepaired(cluster, "ks", "t1");
                    assertReplicasAgree(cluster, "ks", "t1", all, Long.MIN_VALUE, Long.MIN_VALUE, what);
                    cluster.get(1).nodetoolResult("repair", "--validate", "ks", "t1").asserts().success().notificationContains("Repaired data is in sync");
                }
                else if (!isPreview)
                {
                    // a full repair syncs the data but leaves it unrepaired
                    assertReplicasAgree(cluster, "ks", "t1", all, Long.MIN_VALUE, Long.MIN_VALUE, what);
                }
            }

            // an Accord table: incremental repair from every node, on the cursor path, with the replicas agreeing
            cluster.schemaChange("CREATE KEYSPACE ks_accord WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1}");
            cluster.schemaChange("CREATE TABLE ks_accord.t1 (k int, c int, v text, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'");
            assertIncrementalRepairFromEveryNode(cluster, "ks_accord", "t1", all);

            // nodetool repair with no keyspace: every table of every keyspace
            for (int ks = 2; ks <= 4; ks++)
                createKeyspace(cluster, "ks" + ks);
            for (int ks = 2; ks <= 4; ks++)
                for (String table : new String[]{ "t1", "t2" })
                    write(cluster, "ks" + ks, table, 0);
            long[][] before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
            for (int node = 1; node <= NODES; node++)
            {
                cluster.get(node).nodetoolResult("repair").asserts().success();
                for (int ks = 2; ks <= 4; ks++)
                    for (String table : new String[]{ "t1", "t2" })
                        CursorIncrementalRepairTestBase.awaitNoPendingSSTables(cluster, "ks" + ks, table);
            }
            assertCursorPaths(cluster, before, "repair of every keyspace", all, true);
            for (int ks = 2; ks <= 4; ks++)
                for (String table : new String[]{ "t1", "t2" })
                    assertAllRepaired(cluster, "ks" + ks, table);
        }
    }

    /**
     * Writes a round, then incremental repair of the table from every node in turn: every node
     * validates on the cursor path, some anticompact on it, nothing falls back, every sstable ends
     * repaired, and the full replicas agree.
     */
    private static void assertIncrementalRepairFromEveryNode(Cluster cluster, String keyspace, String table, int[] all)
    {
        write(cluster, keyspace, table, 0);
        long[][] before = CursorIncrementalRepairTestBase.cursorCounts(cluster);
        for (int node = 1; node <= NODES; node++)
        {
            cluster.get(node).nodetoolResult("repair", keyspace, table).asserts().success();
            CursorIncrementalRepairTestBase.awaitNoPendingSSTables(cluster, keyspace, table);
        }
        assertCursorPaths(cluster, before, keyspace + '.' + table, all, true);
        assertAllRepaired(cluster, keyspace, table);
        assertReplicasAgree(cluster, keyspace, table, all, Long.MIN_VALUE, Long.MIN_VALUE, keyspace + '.' + table);
    }

    /** A keyspace with two replicas in dc1 and one in dc2, and two tables. */
    private static void createKeyspace(Cluster cluster, String keyspace)
    {
        cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1}");
        for (int t = 1; t <= 2; t++)
        {
            cluster.schemaChange("CREATE TABLE " + keyspace + ".t" + t + " (k int, c int, v text, PRIMARY KEY (k, c))");
            for (int node = 1; node <= NODES; node++)
                cluster.get(node).nodetoolResult("disableautocompaction", keyspace, "t" + t).asserts().success();
        }
    }

    /** For each range of node 1, as its start and end tokens, the nodes that replicate it. */
    private static Map<String[], Set<Integer>> replicasOfNode1(Cluster cluster, String keyspace)
    {
        Map<String, Integer> nodes = new java.util.HashMap<>();
        for (int node = 1; node <= NODES; node++)
            nodes.put(address(cluster, node), node);
        List<List<String>> replicas = cluster.get(1).callOnInstance(() -> {
            List<List<String>> sets = new ArrayList<>();
            org.apache.cassandra.locator.EndpointsByRange ring = StorageService.instance.getRangeToAddressMap(keyspace);
            for (org.apache.cassandra.dht.Range<org.apache.cassandra.dht.Token> range : ring.keySet())
            {
                List<String> endpoints = new ArrayList<>();
                endpoints.add(range.left.toString());
                endpoints.add(range.right.toString());
                for (org.apache.cassandra.locator.InetAddressAndPort endpoint : ring.get(range).endpoints())
                    endpoints.add(endpoint.getHostAddressAndPort());
                if (endpoints.contains(FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort()))
                    sets.add(endpoints);
            }
            return sets;
        });
        Map<String[], Set<Integer>> sets = new java.util.LinkedHashMap<>();
        for (List<String> range : replicas)
        {
            Set<Integer> set = new TreeSet<>();
            range.subList(2, range.size()).forEach(e -> set.add(nodes.get(e)));
            sets.put(new String[]{ range.get(0), range.get(1) }, set);
        }
        return sets;
    }

    private static String address(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort());
    }

    /** Writes to every replica, then a row that only node 1 gets, so repair has something to fix; flushes. */
    private static void write(Cluster cluster, String keyspace, String table, int round)
    {
        for (int k = 0; k < KEYS; k++)
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k, c, v) VALUES (?, ?, ?)", ConsistencyLevel.ALL, k, round % 3, "round" + round);
        cluster.get(1).executeInternal("INSERT INTO " + keyspace + '.' + table + " (k, c, v) VALUES (?, ?, ?)", round, 9, "only on node 1");
        for (int node = 1; node <= NODES; node++)
            cluster.get(node).flush(keyspace);
    }

    /**
     * Every node in {@code taking} validated on the cursor path, and no other node did; for an
     * incremental repair, some node anticompacted on it, and every node that did wrote
     * partitions; for another repair, no node anticompacted; no node fell back.
     */
    private static void assertCursorPaths(Cluster cluster, long[][] before, String what, int[] taking, boolean incremental)
    {
        long[][] after = CursorIncrementalRepairTestBase.cursorCounts(cluster);
        long anticompactions = 0;
        for (int node = 1; node <= NODES; node++)
        {
            int n = node;
            boolean takes = Arrays.stream(taking).anyMatch(t -> t == n);
            String where = what + ", node " + node + ": ";
            long validations = after[node - 1][VALIDATIONS] - before[node - 1][VALIDATIONS];
            assertEquals(where + "cursor validation", takes, validations > 0);
            assertEquals(where + "fell back in validation", 0, after[node - 1][VALIDATION_FALLBACKS] - before[node - 1][VALIDATION_FALLBACKS]);
            assertEquals(where + "fell back in anticompaction", 0, after[node - 1][ANTICOMPACTION_FALLBACKS] - before[node - 1][ANTICOMPACTION_FALLBACKS]);
            if (after[node - 1][ANTICOMPACTIONS] - before[node - 1][ANTICOMPACTIONS] > 0)
                assertTrue(where + "ran cursor anticompaction but the cursor wrote no partition", after[node - 1][PARTITIONS_WRITTEN] - before[node - 1][PARTITIONS_WRITTEN] > 0);
            if (!incremental)
                assertEquals(where + "anticompacted in a repair that does not", 0, after[node - 1][ANTICOMPACTIONS] - before[node - 1][ANTICOMPACTIONS]);
            anticompactions += after[node - 1][ANTICOMPACTIONS] - before[node - 1][ANTICOMPACTIONS];
        }
        if (incremental)
            assertTrue(what + ": no node ran cursor anticompaction", anticompactions > 0);
    }

    /**
     * For every key whose token is in (left, right] (the whole ring when left equals right), the
     * nodes of {@code nodes} that replicate it hold the same rows, and every one of them holds it.
     */
    private static void assertReplicasAgree(Cluster cluster, String keyspace, String table, int[] nodes, long left, long right, String what)
    {
        Map<String, Integer> byAddress = new java.util.HashMap<>();
        for (int node = 1; node <= NODES; node++)
            byAddress.put(address(cluster, node), node);
        // for each key in the range, its replicas, from node 1
        Map<Integer, List<String>> replicas = cluster.get(1).callOnInstance(() -> {
            Map<Integer, List<String>> found = new java.util.HashMap<>();
            org.apache.cassandra.dht.Range<org.apache.cassandra.dht.Token> range =
                new org.apache.cassandra.dht.Range<>(new org.apache.cassandra.dht.Murmur3Partitioner.LongToken(left), new org.apache.cassandra.dht.Murmur3Partitioner.LongToken(right));
            for (int k = 0; k < KEYS; k++)
            {
                org.apache.cassandra.dht.Token token = org.apache.cassandra.config.DatabaseDescriptor.getPartitioner().getToken(org.apache.cassandra.db.marshal.Int32Type.instance.decompose(k));
                if (left != right && !range.contains(token))
                    continue;
                List<String> endpoints = new ArrayList<>();
                // a transient replica drops the data once it is repaired, so only full replicas must hold it
                for (org.apache.cassandra.locator.InetAddressAndPort endpoint : StorageService.instance.getNaturalReplicasForToken(keyspace, table, String.valueOf(k)).filter(org.apache.cassandra.locator.Replica::isFull).endpoints())
                    endpoints.add(endpoint.getHostAddressAndPort());
                found.put(k, endpoints);
            }
            return found;
        });
        assertTrue(what + ": no key in the range", !replicas.isEmpty());
        Map<Integer, Map<Integer, List<String>>> rowsByNode = new java.util.HashMap<>();
        for (int node : nodes)
        {
            Map<Integer, List<String>> rows = new java.util.HashMap<>();
            for (Object[] row : cluster.get(node).executeInternal("SELECT k, c, v FROM " + keyspace + '.' + table))
                rows.computeIfAbsent((Integer) row[0], k -> new ArrayList<>()).add(row[1] + "=" + row[2]);
            rowsByNode.put(node, rows);
        }
        for (Map.Entry<Integer, List<String>> key : replicas.entrySet())
        {
            List<String> agreed = null;
            int holders = 0;
            for (String endpoint : key.getValue())
            {
                int node = byAddress.get(endpoint);
                if (!rowsByNode.containsKey(node))
                    continue;
                List<String> mine = rowsByNode.get(node).getOrDefault(key.getKey(), List.of());
                assertTrue(what + ": node " + node + " does not hold key " + key.getKey(), !mine.isEmpty());
                if (agreed == null)
                    agreed = mine;
                assertEquals(what + ": the replicas of key " + key.getKey() + " disagree (node " + node + ")", agreed, mine);
                holders++;
            }
            assertTrue(what + ": no node that took part replicates key " + key.getKey(), holders > 0 || nodes.length < NODES);
        }
    }

    /** On every node, once the sessions finish, every sstable of the table is repaired. */
    private static void assertAllRepaired(Cluster cluster, String keyspace, String table)
    {
        CursorIncrementalRepairTestBase.awaitNoPendingSSTables(cluster, keyspace, table);
        for (int node = 1; node <= NODES; node++)
        {
            int n = node;
            cluster.get(node).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                assertTrue("node " + n + ": " + keyspace + '.' + table + " has no sstable", !cfs.getLiveSSTables().isEmpty());
                for (SSTableReader sstable : cfs.getLiveSSTables())
                    assertTrue("node " + n + ": an sstable of " + keyspace + '.' + table + " is not repaired: " + sstable, sstable.isRepaired());
            });
        }
    }
}
