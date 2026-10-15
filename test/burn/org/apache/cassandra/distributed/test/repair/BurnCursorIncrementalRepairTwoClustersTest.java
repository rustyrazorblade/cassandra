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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.differential.DifferentialRunSeed;
import org.apache.cassandra.db.repair.CassandraTableRepairManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.LocalValidation;
import org.apache.cassandra.repair.consistent.LocalSessionInfo;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertNoSSTableLeak;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.ANTICOMPACTIONS;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.ANTICOMPACTION_FALLBACKS;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.PARTITIONS_WRITTEN;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.VALIDATIONS;
import static org.apache.cassandra.distributed.test.repair.CursorIncrementalRepairTestBase.VALIDATION_FALLBACKS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Two clusters, one with cursor_repair_enabled on (cursor validation and cursor anticompaction)
 * and one with it off, get the same seeded workload with fixed timestamps and the same incremental repairs, round after
 * round, with autocompaction on and a writer that loads another table all the time.  The
 * workload has row deletions, range deletions and cells with a long TTL.  The rounds take turns:
 * a repair from one node, {@code -pr} from every node, a subrange, and a session that fails on one
 * node.  After each round, every node must hold the same rows on both clusters and the same number
 * of live rows in repaired and in unrepaired sstables, and on the cursor cluster each node's
 * Merkle trees must be the same on the cursor path and on the legacy path.  The trees are not
 * compared across clusters: a deletion's local time is when the node wrote it.
 *
 * <p>A long test, not in the normal run.  {@code -Dcassandra.test.incremental_repair_burn_cycles}
 * sets the rounds (default 20), {@code -Dcassandra.test.incremental_repair_two_cluster_burn_rows} the
 * rows the rounds write as new partitions (default 100,000), and
 * {@code -Dcassandra.test.differential.seed} set to the logged run seed replays the workload.
 */
public class BurnCursorIncrementalRepairTwoClustersTest extends TestBaseImpl
{
    private static final int NODES = 3;
    /** Rows of each partition the burn writes. */
    private static final int CLUSTERINGS = 5;
    /** Writes of each round to random rows already written: deletions, range deletions, TTLs and overwrites. */
    private static final int MIXED_WRITES = 500;
    /** New partitions each round writes, so the rounds together write at least the asked rows. */
    private int keysPerRound;
    private static final String TABLE = "t";
    private static final Logger logger = LoggerFactory.getLogger(BurnCursorIncrementalRepairTwoClustersTest.class);

    @Test
    public void bti() throws Exception
    {
        run("bti");
    }

    @Test
    public void big() throws Exception
    {
        run("big");
    }

    private void run(String format) throws Exception
    {
        int rounds = CassandraRelevantProperties.TEST_INCREMENTAL_REPAIR_BURN_CYCLES.getInt();
        int rows = CassandraRelevantProperties.TEST_INCREMENTAL_REPAIR_TWO_CLUSTER_BURN_ROWS.getInt();
        assertTrue("burn rounds and rows must be positive", rounds > 0 && rows > 0);
        keysPerRound = Math.max(1, (rows + rounds * CLUSTERINGS - 1) / (rounds * CLUSTERINGS));
        long seed = DifferentialRunSeed.SEED ^ format.hashCode();
        logger.info("two cluster burn: format={} rounds={} rows={} ({} new rows each round) seed={} (run seed {})",
                    format, rounds, rows, keysPerRound * CLUSTERINGS, seed, DifferentialRunSeed.SEED);
        try (Cluster cursor = start(format, true); Cluster legacy = start(format, false))
        {
            Random cursorRandom = new Random(seed);
            Random legacyRandom = new Random(seed);
            List<Thread> loads = new ArrayList<>();
            for (Cluster cluster : new Cluster[]{ cursor, legacy })
            {
                cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
                cluster.schemaChange("CREATE TABLE ks." + TABLE + " (k int, c int, v text, PRIMARY KEY (k, c))");
                cluster.schemaChange("CREATE TABLE ks.load (k int, c int, v text, PRIMARY KEY (k, c))");
                loads.add(startLoad(cluster));
            }
            try
            {
                for (int round = 0; round < rounds; round++)
                {
                    try
                    {
                        long[][] cursorBefore = CursorIncrementalRepairTestBase.cursorCounts(cursor);
                        write(cursor, cursorRandom, round);
                        write(legacy, legacyRandom, round);
                        for (Cluster cluster : new Cluster[]{ cursor, legacy })
                            repairRound(cluster, round);
                        for (Cluster cluster : new Cluster[]{ cursor, legacy })
                            CursorIncrementalRepairTestBase.awaitNoPendingSSTables(cluster, "ks", TABLE);
                        // a subrange repair validates only on the subrange's replicas
                        assertPathsRan(cursor, cursorBefore, round % 4 != 2);
                        for (int node = 1; node <= NODES; node++)
                        {
                            assertEquals("node " + node + ": rows differ", rows(cursor, node), rows(legacy, node));
                            assertArrayEquals("node " + node + ": the live rows of the repaired and of the unrepaired sstables differ",
                                              repairedAndUnrepairedRows(cursor, node), repairedAndUnrepairedRows(legacy, node));
                            // compaction stays held from the first validation to the end of the second
                            assertEquals("node " + node + ": the Merkle tree root hashes of the two paths differ",
                                         merkleRoots(cursor, node, false), merkleRoots(cursor, node, true));
                            cursor.get(node).runOnInstance(() -> Keyspace.open("ks").getColumnFamilyStore(TABLE).enableAutoCompaction());
                        }
                        for (Cluster cluster : new Cluster[]{ cursor, legacy })
                            assertNoSSTableLeak(cluster, "ks", TABLE);
                        logger.info("two cluster burn: round {} matched", round);
                    }
                    catch (Throwable t)
                    {
                        throw new AssertionError("failure for seed " + seed + "; " + DifferentialRunSeed.replayAdvice(DifferentialRunSeed.SEED) +
                                                 "; format " + format + ", round " + round + ": " + t, t);
                    }
                }
            }
            finally
            {
                stopLoad = true;
                for (Thread load : loads)
                    load.join(TimeUnit.MINUTES.toMillis(1));
            }
        }
    }

    private static volatile boolean stopLoad;

    /** Writes to the load table of the cluster all the time, and flushes it every few seconds. */
    private static Thread startLoad(Cluster cluster)
    {
        stopLoad = false;
        Thread load = new Thread(() -> {
            long lastFlush = System.nanoTime();
            for (int i = 0; !stopLoad; i++)
            {
                cluster.coordinator(1 + i % NODES).execute("INSERT INTO ks.load (k, c, v) VALUES (?, ?, ?)", ConsistencyLevel.ONE, i % 5000, i % 7, "load " + i);
                if (System.nanoTime() - lastFlush > TimeUnit.SECONDS.toNanos(3))
                {
                    for (int node = 1; node <= NODES; node++)
                        cluster.get(node).flush("ks");
                    lastFlush = System.nanoTime();
                }
            }
        }, "load writer");
        load.setDaemon(true);
        load.start();
        return load;
    }

    /**
     * The round's repair, the same on both clusters: from one node, {@code -pr} from every node, a
     * subrange of one node's first range, or a session that fails on node 3 because it never gets
     * the finalize message.
     */
    private static void repairRound(Cluster cluster, int round)
    {
        int coordinator = 1 + round % NODES;
        switch (round % 4)
        {
            case 0:
                cluster.get(coordinator).nodetoolResult("repair", "ks", TABLE).asserts().success();
                break;
            case 1:
                for (int node = 1; node <= NODES; node++)
                    cluster.get(node).nodetoolResult("repair", "-pr", "ks", TABLE).asserts().success();
                break;
            case 2:
                String[] range = cluster.get(coordinator).callOnInstance(() -> {
                    Range<Token> owned = StorageService.instance.getLocalReplicas("ks").ranges().iterator().next();
                    return new String[]{ owned.left.toString(), owned.right.toString() };
                });
                cluster.get(coordinator).nodetoolResult("repair", "-st", range[0], "-et", range[1], "ks", TABLE).asserts().success();
                break;
            default:
                int failing = coordinator == 3 ? 1 : 3;
                // the finalize message stays dropped until the session has failed there, so a
                // late message cannot finalize it on one cluster and not on the other
                AtomicInteger finalizeDropped = CursorIncrementalRepairTestBase.dropFinalizeCommit(cluster, failing);
                try
                {
                    cluster.get(coordinator).nodetoolResult("repair", "ks", TABLE);
                    String session = null;
                    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
                    while (session == null && System.nanoTime() < deadline)
                    {
                        session = cluster.get(failing).callOnInstance(() -> {
                            for (Map<String, String> info : ActiveRepairService.instance().consistent.local.sessionInfo(false, java.util.Collections.emptySet()))
                                if ("FINALIZE_PROMISED".equals(info.get(LocalSessionInfo.STATE)))
                                    return info.get(LocalSessionInfo.SESSION_ID);
                            return null;
                        });
                        if (session == null)
                            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    }
                    assertTrue("the session never reached FINALIZE_PROMISED on node " + failing, session != null);
                    String id = session;
                    CursorIncrementalRepairTestBase.awaitDropped(finalizeDropped, failing);
                    // only the failing node never got the commit, so both clusters end the same way
                    CursorIncrementalRepairTestBase.awaitFinalizedExcept(cluster, id, failing);
                    cluster.get(failing).runOnInstance(() -> ActiveRepairService.instance().failSession(id, true));
                }
                finally
                {
                    cluster.filters().reset();
                }
                for (int node = 1; node <= NODES; node++)
                    cluster.get(node).runOnInstance(() -> ActiveRepairService.instance().consistent.local.cleanup());
        }
    }

    private static Cluster start(String format, boolean cursor) throws Exception
    {
        Cluster cluster = Cluster.build(NODES)
                      .withNodeProvisionStrategy(INodeProvisionStrategy.Strategy.OneNetworkInterface)
                      .withDynamicPortAllocation(true)
                      .withVNodes()
                      .withTokenCount(4)
                      .withConfig(config -> CursorIncrementalRepairTestBase.productionStorage(config).with(NETWORK, GOSSIP)
                                                  .set("hinted_handoff_enabled", false)
                                                  .set("cursor_compaction_enabled", cursor)
                                                  .set("cursor_repair_enabled", cursor)
                                                  .set("sstable", Map.of("selected_format", format))
                                                  .set("storage_compatibility_mode", "NONE"))
                      .start();
        CursorIncrementalRepairTestBase.assertProductionStorage(cluster);
        return cluster;
    }

    /**
     * Writes with fixed timestamps: this round's new partitions to every replica, then mixed writes
     * to random rows of all partitions written so far (some with a long TTL, some row and range
     * deletions), and some rows to one node only; then flushes.
     */
    private void write(Cluster cluster, Random random, int round)
    {
        long timestamp = 1000L + round;
        int keys = (round + 1) * keysPerRound;
        for (int k = round * keysPerRound; k < keys; k++)
        {
            StringBuilder batch = new StringBuilder("BEGIN UNLOGGED BATCH ");
            for (int c = 0; c < CLUSTERINGS; c++)
                batch.append("INSERT INTO ks." + TABLE + " (k, c, v) VALUES (" + k + ", " + c + ", 'new" + round + "') USING TIMESTAMP " + timestamp + "; ");
            cluster.coordinator(1).execute(batch.append("APPLY BATCH").toString(), ConsistencyLevel.ALL);
        }
        for (int i = 0; i < MIXED_WRITES; i++)
        {
            int k = random.nextInt(keys);
            int kind = random.nextInt(10);
            if (kind == 0)
                cluster.coordinator(1).execute("DELETE FROM ks." + TABLE + " USING TIMESTAMP ? WHERE k = ? AND c = ?",
                                               ConsistencyLevel.ALL, timestamp, k, random.nextInt(CLUSTERINGS));
            else if (kind == 1)
                cluster.coordinator(1).execute("DELETE FROM ks." + TABLE + " USING TIMESTAMP ? WHERE k = ? AND c >= ? AND c < ?",
                                               ConsistencyLevel.ALL, timestamp, k, 1, 3);
            else if (kind == 2)
                cluster.coordinator(1).execute("INSERT INTO ks." + TABLE + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL 864000",
                                               ConsistencyLevel.ALL, k, random.nextInt(CLUSTERINGS), "ttl" + round, timestamp);
            else
                cluster.coordinator(1).execute("INSERT INTO ks." + TABLE + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                               ConsistencyLevel.ALL, k, random.nextInt(CLUSTERINGS), "round" + round, timestamp);
        }
        int only = 1 + random.nextInt(NODES);
        for (int i = 0; i < 50; i++)
        {
            int k = random.nextInt(keys);
            cluster.get(only).executeInternal("INSERT INTO ks." + TABLE + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              k, 9, "only on node " + only, timestamp);
        }
        for (int node = 1; node <= NODES; node++)
            cluster.get(node).flush("ks");
    }

    private static List<String> rows(Cluster cluster, int node)
    {
        List<String> rows = new ArrayList<>();
        for (Object[] row : cluster.get(node).executeInternal("SELECT k, c, v, writetime(v) FROM ks." + TABLE))
            rows.add(Arrays.toString(row));
        rows.sort(String::compareTo);
        return rows;
    }

    /**
     * The live rows of the repaired sstables merged together, and of the unrepaired ones, each as
     * a count and a hash.  Background compaction merges sstables of one repair state only, and
     * at different times on the two clusters, so the sstables themselves differ, but these do not.
     */
    private static long[] repairedAndUnrepairedRows(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore(TABLE);
            long[] counts = new long[4];
            // background compaction would replace the sstables while they are read
            CursorIncrementalRepairTestBase.withNoCompactionRunning(cfs, () -> {
                List<SSTableReader> repaired = new ArrayList<>();
                List<SSTableReader> unrepaired = new ArrayList<>();
                for (SSTableReader sstable : cfs.getLiveSSTables())
                    (sstable.isRepaired() ? repaired : unrepaired).add(sstable);
                long[] repairedRows = liveRows(repaired);
                long[] unrepairedRows = liveRows(unrepaired);
                counts[0] = repairedRows[0];
                counts[1] = repairedRows[1];
                counts[2] = unrepairedRows[0];
                counts[3] = unrepairedRows[1];
            });
            return counts;
        });
    }

    /** The count and a hash of the live rows of the sstables merged together. */
    private static long[] liveRows(List<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
            return new long[]{ 0, 0 };
        List<org.apache.cassandra.io.sstable.ISSTableScanner> scanners = new ArrayList<>();
        try
        {
            for (SSTableReader sstable : sstables)
                scanners.add(sstable.getScanner());
            List<String> rows = new ArrayList<>();
            long nowInSec = org.apache.cassandra.utils.FBUtilities.nowInSeconds();
            try (org.apache.cassandra.db.partitions.UnfilteredPartitionIterator merged =
                 org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.merge(scanners, org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.MergeListener.NOOP);
                 org.apache.cassandra.db.partitions.PartitionIterator live = org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.filter(merged, nowInSec))
            {
                while (live.hasNext())
                {
                    try (org.apache.cassandra.db.rows.RowIterator partition = live.next())
                    {
                        while (partition.hasNext())
                        {
                            org.apache.cassandra.db.rows.Row row = partition.next();
                            rows.add(partition.partitionKey() + "/" + row.clustering().toString(partition.metadata()) + "/" + row.toString(partition.metadata(), false));
                        }
                    }
                }
            }
            rows.sort(String::compareTo);
            return new long[]{ rows.size(), rows.hashCode() };
        }
        finally
        {
            scanners.forEach(org.apache.cassandra.io.sstable.ISSTableScanner::close);
        }
    }

    /**
     * The Merkle tree root hash of each of the node's ranges, from a full validation on the path
     * asked for, which must not fall back.  It holds background compaction of the table; the
     * caller lets it go again.
     */
    private static List<String> merkleRoots(Cluster cluster, int node, boolean cursorPath)
    {
        return cluster.get(node).callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore(TABLE);
            CursorIncrementalRepairTestBase.awaitNoCompaction(cfs);
            boolean original = DatabaseDescriptor.cursorRepairEnabled();
            DatabaseDescriptor.setCursorRepairEnabled(cursorPath);
            long runsBefore = CassandraTableRepairManager.cursorValidationsRun.sum();
            long fallbacksBefore = CassandraTableRepairManager.cursorValidationFallbacks.sum();
            List<Range<Token>> ranges = new ArrayList<>(StorageService.instance.getLocalReplicas("ks").ranges());
            TimeUUID parent = TimeUUID.Generator.nextTimeUUID();
            ActiveRepairService.instance().registerParentRepairSession(parent, FBUtilities.getBroadcastAddressAndPort(), List.of(cfs), ranges,
                                                                       false, ActiveRepairService.UNREPAIRED_SSTABLE, true, PreviewKind.NONE);
            try
            {
                // a fixed nowInSec: the workload has nothing that expires
                List<String> roots = new ArrayList<>();
                for (byte[] root : LocalValidation.validate(cfs, parent, TimeUUID.Generator.nextTimeUUID(), ranges, false, PreviewKind.NONE, 0).rootHashes)
                    roots.add(root == null ? "empty" : org.apache.cassandra.utils.Hex.bytesToHex(root));
                assertEquals("the validation took the other path", cursorPath ? 1 : 0, CassandraTableRepairManager.cursorValidationsRun.sum() - runsBefore);
                assertEquals("the cursor validation fell back", 0, CassandraTableRepairManager.cursorValidationFallbacks.sum() - fallbacksBefore);
                return roots;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                ActiveRepairService.instance().removeParentRepairSession(parent);
                DatabaseDescriptor.setCursorRepairEnabled(original);
            }
        });
    }

    /**
     * On the cursor cluster, the round's validations ran on the cursor path, on every node if
     * {@code everyNodeValidates}, with no fallback; some anticompaction ran there, and every node
     * that ran one wrote partitions.
     */
    private static void assertPathsRan(Cluster cursor, long[][] before, boolean everyNodeValidates)
    {
        long[][] after = CursorIncrementalRepairTestBase.cursorCounts(cursor);
        long anticompactions = 0;
        long validations = 0;
        for (int node = 1; node <= NODES; node++)
        {
            validations += after[node - 1][VALIDATIONS] - before[node - 1][VALIDATIONS];
            if (everyNodeValidates)
                assertTrue("node " + node + " ran no cursor validation", after[node - 1][VALIDATIONS] - before[node - 1][VALIDATIONS] > 0);
            assertEquals("node " + node + " fell back in validation", 0, after[node - 1][VALIDATION_FALLBACKS] - before[node - 1][VALIDATION_FALLBACKS]);
            assertEquals("node " + node + " fell back in anticompaction", 0, after[node - 1][ANTICOMPACTION_FALLBACKS] - before[node - 1][ANTICOMPACTION_FALLBACKS]);
            long ran = after[node - 1][ANTICOMPACTIONS] - before[node - 1][ANTICOMPACTIONS];
            if (ran > 0)
                assertTrue("node " + node + " ran cursor anticompaction but the cursor wrote no partition", after[node - 1][PARTITIONS_WRITTEN] - before[node - 1][PARTITIONS_WRITTEN] > 0);
            anticompactions += ran;
        }
        assertTrue("no node ran cursor validation", validations > 0);
        assertTrue("no node ran cursor anticompaction", anticompactions > 0);
    }
}
