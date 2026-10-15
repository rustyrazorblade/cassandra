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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.repair.CassandraTableRepairManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.consistent.LocalSessionInfo;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertNoSSTableLeak;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Incremental repair on a real cluster, with cursor_repair_enabled on (cursor validation and
 * cursor anticompaction): 4 nodes, RF 3, num_tokens 4 unless a subclass sets it.  With RF below
 * the node count, every node holds data outside the ranges it repairs, so every repair runs real
 * anticompaction.  Each scenario checks, on every
 * node, which path the validations and the anticompactions took and that a supported table never
 * falls back; and, after each repair, that the replicas agree, that no sstable is left pending,
 * and that the files on disk match the live sstables.  A subclass picks the sstable format, and
 * whether the scenarios run under load: autocompaction stays on for the repaired tables, and a
 * writer adds rows, with flushes, to another table and to the table of the running scenario, at
 * keys from {@link #LOAD_KEYS} up, outside the keys the scenario writes.  Every cluster uses UCS,
 * the trie memtable and memtables off heap ({@link #productionStorage}).
 */
public abstract class CursorIncrementalRepairTestBase extends TestBaseImpl
{
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CursorIncrementalRepairTestBase.class);
    private static final int NODES = 4;
    private static final int KEYS = 400;
    private static final AtomicInteger keyspaces = new AtomicInteger();

    protected static Cluster cluster;
    /** Whether the scenarios run under load; see the class comment. */
    private static boolean underLoad;
    private static Thread loadWriter;
    private static volatile boolean stopLoad;
    /** The first key the load writes to a scenario's table; the scenarios write keys below it. */
    static final int LOAD_KEYS = 100_000;
    /** The table of the running scenario that the load also writes to, as keyspace.table; null for none. */
    private static volatile String loadTable;
    /**
     * The load writer holds it to read for each write, and a scenario holds it to write while a
     * node is down: a write or flush on a node that is starting up breaks its startup.
     */
    private static final java.util.concurrent.locks.ReentrantReadWriteLock nodeRestart = new java.util.concurrent.locks.ReentrantReadWriteLock();
    /** The last round written by {@link #writeWithInconsistency} to each table, by keyspace.table. */
    private static final Map<String, Integer> lastRound = new java.util.concurrent.ConcurrentHashMap<>();

    protected static void startCluster(String format) throws Exception
    {
        startCluster(format, false);
    }

    protected static void startCluster(String format, boolean load) throws Exception
    {
        startCluster(format, load, 4);
    }

    /** As above, with {@code tokens} tokens on each node. */
    protected static void startCluster(String format, boolean load, int tokens) throws Exception
    {
        underLoad = load;
        // one address with a free port for each node: other test runs share this machine, and it
        // may have no loopback aliases
        cluster = Cluster.build(NODES)
                         .withInstanceInitializer(PauseCursorAntiCompaction::install)
                         .withNodeProvisionStrategy(INodeProvisionStrategy.Strategy.OneNetworkInterface)
                         .withDynamicPortAllocation(true)
                         .withVNodes()
                         .withTokenCount(tokens)
                         .withConfig(config -> productionStorage(config).with(NETWORK, GOSSIP)
                                                     .set("hinted_handoff_enabled", false)
                                                     .set("cursor_compaction_enabled", true)
                                                     .set("cursor_repair_enabled", true)
                                                     .set("sstable", Map.of("selected_format", format))
                                                     .set("storage_compatibility_mode", "NONE"))
                         .start();
        assertProductionStorage(cluster);
        if (load)
            startLoad();
    }

    @AfterClass
    public static void closeCluster() throws InterruptedException
    {
        stopLoad = true;
        if (loadWriter != null)
            loadWriter.join(TimeUnit.MINUTES.toMillis(1));
        if (cluster != null)
            cluster.close();
    }

    /** Writes rows to a table of its own and to the scenario's table, at consistency ONE, and flushes every node every twenty seconds. */
    private static void startLoad()
    {
        cluster.schemaChange("CREATE KEYSPACE load_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        cluster.schemaChange("CREATE TABLE load_ks.load (k int, c int, v text, PRIMARY KEY (k, c))");
        stopLoad = false;
        loadWriter = new Thread(() -> {
            long lastFlush = System.nanoTime();
            for (int i = 0; !stopLoad; i++)
            {
                nodeRestart.readLock().lock();
                try
                {
                    String table = loadTable;
                    if (table != null && i % 2 == 0)
                        cluster.coordinator(1 + i % NODES).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, ?, ?)", ConsistencyLevel.ONE, LOAD_KEYS + i % 5000, i % 7, "load " + i);
                    else
                        cluster.coordinator(1 + i % NODES).execute("INSERT INTO load_ks.load (k, c, v) VALUES (?, ?, ?)", ConsistencyLevel.ONE, i % 5000, i % 7, "load " + i);
                    // a steady load, about 100 writes a second, that leaves the scenarios time to run;
                    // each flush and compaction logs much at debug, and the test output is kept in the heap
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                    if (System.nanoTime() - lastFlush > TimeUnit.SECONDS.toNanos(20))
                    {
                        flushAll("load_ks");
                        if (table != null)
                            flushAll(table.substring(0, table.indexOf('.')));
                        lastFlush = System.nanoTime();
                    }
                }
                catch (Throwable t)
                {
                    // a node may be down in a scenario; the load goes on
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }
                finally
                {
                    nodeRestart.readLock().unlock();
                }
            }
        }, "load writer");
        loadWriter.setDaemon(true);
        loadWriter.start();
    }

    /**
     * Holds cursor anticompaction on node {@link #NODE} in the middle of its writes, when a partition
     * to pause before is set: the call that would write that partition waits until it is released,
     * and then fails.  The instance has its
     * own copy of this class, so the test calls it on the instance.
     */
    public static class PauseCursorAntiCompaction
    {
        static final int NODE = 2;
        /** The partition to pause before, counted from 1; 0 when none is set. */
        static volatile int pauseAt;
        static final AtomicInteger partitions = new AtomicInteger();
        static volatile java.util.concurrent.CountDownLatch held = new java.util.concurrent.CountDownLatch(1);
        static volatile java.util.concurrent.CountDownLatch released = new java.util.concurrent.CountDownLatch(1);

        public static void install(ClassLoader cl, int node)
        {
            if (node != NODE)
                return;
            new ByteBuddy().rebase(CursorCompactor.class)
                           .method(named("writeNextPartition").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(PauseCursorAntiCompaction.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        static void pauseBefore(int partition)
        {
            partitions.set(0);
            held = new java.util.concurrent.CountDownLatch(1);
            released = new java.util.concurrent.CountDownLatch(1);
            pauseAt = partition;
        }

        static void release()
        {
            pauseAt = 0;
            released.countDown();
        }

        /** Waits until the anticompaction is held; false if it was not within a minute. */
        static boolean awaitHeld()
        {
            return Uninterruptibles.awaitUninterruptibly(held, 1, TimeUnit.MINUTES);
        }

        @SuppressWarnings("unused")
        public static boolean writeNextPartition(@SuperCall Callable<Boolean> zuper) throws Exception
        {
            if (pauseAt > 0 && partitions.incrementAndGet() == pauseAt)
            {
                held.countDown();
                released.await(5, TimeUnit.MINUTES);
                // the node is taken to have crashed here, so this anticompaction must not finish
                throw new RuntimeException("injected crash in the middle of cursor anticompaction");
            }
            return zuper.call();
        }
    }

    /**
     * The cursor counters of each node: validations run and fallen back, anticompactions run and
     * fallen back, and partitions that cursor anticompaction wrote.
     */
    protected static final class Counts
    {
        final long[][] perNode = new long[NODES][];

        static Counts now()
        {
            Counts counts = new Counts();
            long[][] perNode = cursorCounts(cluster);
            System.arraycopy(perNode, 0, counts.perNode, 0, NODES);
            return counts;
        }

        long delta(Counts before, int node, int counter)
        {
            return perNode[node - 1][counter] - before.perNode[node - 1][counter];
        }
    }

    /**
     * The storage the cursor paths are built for, set here so the tests do not depend on
     * {@code -Djvm_dtests.latest}: UCS, the trie memtable, and memtables off heap.
     */
    static IInstanceConfig productionStorage(IInstanceConfig config)
    {
        return config.set("memtable", Map.of("configurations", Map.of("default", Map.of("class_name", "TrieMemtable"))))
                     .set("memtable_allocation_type", "offheap_objects")
                     .set("default_compaction", Map.of("class_name", "UnifiedCompactionStrategy",
                                                       "parameters", Map.of("scaling_parameters", "L10")));
    }

    /** Every node of {@code on} runs with {@link #productionStorage}. */
    static void assertProductionStorage(Cluster on)
    {
        for (int node = 1; node <= on.size(); node++)
        {
            int n = node;
            on.get(node).runOnInstance(() -> {
                assertEquals("node " + n + " memtable allocation", org.apache.cassandra.config.Config.MemtableAllocationType.offheap_objects,
                             DatabaseDescriptor.getMemtableAllocationType());
                assertTrue("node " + n + " memtable: " + DatabaseDescriptor.getMemtableConfigurations(),
                           String.valueOf(DatabaseDescriptor.getMemtableConfigurations()).contains("TrieMemtable"));
                assertTrue("node " + n + " compaction: " + DatabaseDescriptor.getDefaultCompaction(),
                           DatabaseDescriptor.getDefaultCompaction().class_name.contains("UnifiedCompactionStrategy"));
            });
        }
    }

    /** The indexes of the values {@link #cursorCounts} gives for each node. */
    static final int VALIDATIONS = 0, VALIDATION_FALLBACKS = 1, ANTICOMPACTIONS = 2, ANTICOMPACTION_FALLBACKS = 3, PARTITIONS_WRITTEN = 4, BYTES_STREAMED_IN = 5;

    /**
     * For each node of {@code on}: cursor validations run and fallen back, cursor anticompactions
     * run and fallen back, partitions that cursor anticompaction wrote, and bytes streamed in; null
     * for a node that is down.
     */
    static long[][] cursorCounts(Cluster on)
    {
        long[][] counts = new long[on.size()][];
        for (int node = 1; node <= on.size(); node++)
        {
            if (on.get(node).isShutdown())
                continue;
            counts[node - 1] = on.get(node).callOnInstance(() -> new long[]{
                CassandraTableRepairManager.cursorValidationsRun.sum(),
                CassandraTableRepairManager.cursorValidationFallbacks.sum(),
                CompactionManager.cursorAnticompactionsRun.sum(),
                CompactionManager.cursorAnticompactionFallbacks.sum(),
                CompactionManager.cursorAnticompactionPartitionsWritten.sum(),
                org.apache.cassandra.metrics.StreamingMetrics.totalIncomingBytes.getCount() });
        }
        return counts;
    }

    /** If the node ran cursor anticompaction, the cursor wrote partitions there. */
    private static void assertCursorWroteIfItRan(Counts after, Counts before, int node)
    {
        if (after.delta(before, node, ANTICOMPACTIONS) > 0)
            assertTrue("node " + node + " ran cursor anticompaction but the cursor wrote no partition",
                       after.delta(before, node, PARTITIONS_WRITTEN) > 0);
    }

    /** On every node in {@code nodes}: the cursor ran validation and anticompaction, with no fallback. */
    protected static void assertCursorRan(Counts before, int... nodes)
    {
        Counts after = Counts.now();
        for (int node : nodes)
        {
            assertTrue("node " + node + " ran no cursor validation", after.delta(before, node, VALIDATIONS) > 0);
            assertTrue("node " + node + " ran no cursor anticompaction", after.delta(before, node, ANTICOMPACTIONS) > 0);
            assertEquals("node " + node + " fell back in validation", 0, after.delta(before, node, VALIDATION_FALLBACKS));
            assertEquals("node " + node + " fell back in anticompaction", 0, after.delta(before, node, ANTICOMPACTION_FALLBACKS));
            assertCursorWroteIfItRan(after, before, node);
        }
    }

    private static int[] allNodes()
    {
        return new int[]{ 1, 2, 3, 4 };
    }

    protected static String newKeyspace()
    {
        String keyspace = "cursor_ir_" + keyspaces.incrementAndGet();
        cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        return keyspace;
    }

    protected static void createTable(String keyspace, String table)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int, c int, v text, PRIMARY KEY (k, c))");
        disableAutoCompactionUnlessUnderLoad(keyspace, table);
        if (underLoad)
            loadTable = keyspace + '.' + table;
    }

    /**
     * Stops the load to the scenario's table, for a scenario whose checks need the table to change
     * only by its own steps: an exact live set, or a preview of the repaired data in sync.
     */
    protected static void stopLoadOnScenarioTable()
    {
        loadTable = null;
    }

    /** Autocompaction stays on under load; otherwise it is off, so that only repair changes the sstables. */
    protected static void disableAutoCompactionUnlessUnderLoad(String keyspace, String table)
    {
        if (underLoad)
            return;
        for (int node = 1; node <= NODES; node++)
            if (!cluster.get(node).isShutdown())
                cluster.get(node).nodetoolResult("disableautocompaction", keyspace, table).asserts().success();
    }

    /**
     * Writes {@link #KEYS} keys to every replica, then a write that only node 1 receives for some
     * of the keys node 1 replicates, so repair has something to fix, and a deletion of a few keys.
     * Flushes every node.
     */
    protected static void writeWithInconsistency(String keyspace, String table, int round)
    {
        lastRound.put(keyspace + '.' + table, round);
        for (int k = 0; k < KEYS; k++)
            for (int c = 0; c < 3; c++)
                cluster.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k, c, v) VALUES (?, ?, ?)",
                                               ConsistencyLevel.ALL, k, c, "round" + round);
        for (int k = round; k < KEYS; k += 7)
            cluster.coordinator(1).execute("DELETE FROM " + keyspace + '.' + table + " WHERE k = ? AND c = ?", ConsistencyLevel.ALL, k, 1);
        cluster.get(1).runOnInstance(() -> {
            for (int k = 0; k < KEYS; k += 5)
                if (StorageService.instance.getNaturalReplicasForToken(keyspace, table, String.valueOf(k)).endpoints().contains(FBUtilities.getBroadcastAddressAndPort()))
                    org.apache.cassandra.cql3.QueryProcessor.executeInternal("INSERT INTO " + keyspace + '.' + table + " (k, c, v) VALUES (?, ?, ?)", k, 9, "only on node 1");
        });
        flushAll(keyspace);
    }

    protected static void flushAll(String keyspace)
    {
        for (int node = 1; node <= NODES; node++)
            if (!cluster.get(node).isShutdown())
                cluster.get(node).flush(keyspace);
    }

    /** Incremental repair of {@code keyspace}, or of {@code table} in it if not null, with {@code options} such as {@code -pr}. */
    protected static void repair(int node, String keyspace, String table, String... options)
    {
        List<String> command = new ArrayList<>();
        command.add("repair");
        command.addAll(Arrays.asList(options));
        command.add(keyspace);
        if (table != null)
            command.add(table);
        cluster.get(node).nodetoolResult(command.toArray(new String[0])).asserts().success();
    }

    /**
     * After a repair: no sstable is left pending once the sessions finalize, no sstable leaked, the
     * files on disk match the live sstables, and, if {@code wholeRingRepaired}, the replicas of
     * every key agree.
     */
    protected static void assertRepairedState(String keyspace, String table, boolean wholeRingRepaired)
    {
        awaitNoPendingSSTables(keyspace, table);
        assertNoSSTableLeak(cluster, keyspace, table);
        // the load writer flushes while it holds the read lock, and a flush in flight has files that are not live yet
        nodeRestart.writeLock().lock();
        try
        {
            assertOnDiskStateOnEveryNode(keyspace, table);
        }
        finally
        {
            nodeRestart.writeLock().unlock();
        }
        if (wholeRingRepaired)
            assertReplicasAgree(keyspace, table);
    }

    private static void assertOnDiskStateOnEveryNode(String keyspace, String table)
    {
        for (int node = 1; node <= NODES; node++)
        {
            cluster.get(node).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                // a flush or compaction in flight has files that are not live yet.  Nothing writes to
                // the table now, so after this flush a commit log flush of it writes no sstable, and
                // no compaction may run during the checks.
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                withNoCompactionRunning(cfs, () -> {
                    org.apache.cassandra.Util.assertOnDiskState(cfs, cfs.getLiveSSTables().size());
                    org.apache.cassandra.db.compaction.AntiCompactionAssertions.assertEveryFileBelongsToALiveSSTable(cfs);
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                        if (sstable.isRepaired() && sstable.getRepairedAt() > System.currentTimeMillis())
                            throw new AssertionError("repairedAt is in the future: " + sstable);
                });
            });
        }
    }

    protected static void awaitNoPendingSSTables(String keyspace, String table)
    {
        awaitNoPendingSSTables(cluster, keyspace, table);
    }

    /** Waits until no live node of {@code on} holds a pending sstable of the table, compacting to move finished sessions' sstables. */
    static void awaitNoPendingSSTables(Cluster on, String keyspace, String table)
    {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        while (true)
        {
            int pending = 0;
            for (int node = 1; node <= on.size(); node++)
                pending += on.get(node).isShutdown() ? 0 : on.get(node).callOnInstance(() -> {
                    int count = 0;
                    for (SSTableReader sstable : Keyspace.open(keyspace).getColumnFamilyStore(table).getLiveSSTables())
                        if (sstable.isPendingRepair())
                            count++;
                    return count;
                });
            if (pending == 0)
                return;
            if (System.nanoTime() > deadline)
                fail(pending + " sstables are still pending after the sessions finished");
            compactInBackground(on, keyspace, table);
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Runs one round of background compaction on every node.  It moves the sstables of a finished
     * session out of the pending set: to repaired once the session is finalized, back to
     * unrepaired once it failed.
     */
    protected static void compactInBackground(String keyspace, String table)
    {
        compactInBackground(cluster, keyspace, table);
    }

    static void compactInBackground(Cluster on, String keyspace, String table)
    {
        for (int node = 1; node <= on.size(); node++)
        {
            if (on.get(node).isShutdown())
                continue;
            boolean load = underLoad;
            on.get(node).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                cfs.enableAutoCompaction(true);
                if (!load)
                    cfs.disableAutoCompaction();
            });
        }
    }

    /** On the instance: turns autocompaction off and waits until no compaction of the table runs. */
    static void awaitNoCompaction(ColumnFamilyStore cfs)
    {
        cfs.disableAutoCompaction();
        awaitNoRunningCompaction(cfs);
    }

    /**
     * On the instance: runs {@code checks} while no compaction of the table runs.  It stops new
     * background compactions and waits for the running ones to end; it does not interrupt them,
     * as an interrupted compaction leaves files that are deleted later.
     */
    static void withNoCompactionRunning(ColumnFamilyStore cfs, Runnable checks)
    {
        cfs.getCompactionStrategyManager().pause();
        try
        {
            // a task taken just before the pause can start a moment later, so wait twice
            awaitNoRunningCompaction(cfs);
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
            awaitNoRunningCompaction(cfs);
            checks.run();
        }
        finally
        {
            cfs.getCompactionStrategyManager().resume();
        }
    }

    private static void awaitNoRunningCompaction(ColumnFamilyStore cfs)
    {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        while (true)
        {
            boolean running = false;
            for (org.apache.cassandra.db.compaction.CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
            {
                org.apache.cassandra.schema.TableMetadata metadata = holder.getCompactionInfo().getTableMetadata();
                running |= metadata != null && cfs.metadata().id.equals(metadata.id);
            }
            if (!running)
                return;
            if (System.nanoTime() > deadline)
                throw new AssertionError("a compaction of " + cfs + " is still running");
            Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The rows {@link #writeWithInconsistency} leaves after its last round {@code round}: rows 0 and
     * 2 of every key hold that round's value; row 1 too, except for the keys that round deleted it
     * from; and row 9 of every fifth key that {@code node1Keys} holds.
     */
    private static Map<Integer, Map<Integer, String>> expectedRows(int round, Set<Integer> node1Keys)
    {
        Map<Integer, Map<Integer, String>> expected = new HashMap<>();
        for (int k = 0; k < KEYS; k++)
        {
            Map<Integer, String> rows = new HashMap<>();
            rows.put(0, "round" + round);
            if (!(k >= round && (k - round) % 7 == 0))
                rows.put(1, "round" + round);
            rows.put(2, "round" + round);
            if (k % 5 == 0 && node1Keys.contains(k))
                rows.put(9, "only on node 1");
            expected.put(k, rows);
        }
        return expected;
    }

    private static void assertLoadKeysReplicated(int node, String keyspace, String table, Set<Integer> keys)
    {
        if (keys.isEmpty())
            return;
        List<Integer> list = new ArrayList<>(keys);
        List<Integer> foreign = cluster.get(node).callOnInstance(() -> {
            List<Integer> notReplicated = new ArrayList<>();
            for (Integer k : list)
                if (!StorageService.instance.getNaturalReplicasForToken(keyspace, table, String.valueOf(k)).endpoints().contains(FBUtilities.getBroadcastAddressAndPort()))
                    notReplicated.add(k);
            return notReplicated;
        });
        assertTrue("node " + node + " holds load keys it does not replicate: " + foreign, foreign.isEmpty());
    }

    /** Every replica of every key holds the same rows, and a node holds no row of a key it does not replicate. */
    protected static void assertReplicasAgree(String keyspace, String table)
    {
        assertReplicasAgree(keyspace, table, allNodes());
    }

    /**
     * As above, over the given nodes only, for when a node is down: each holds the rows the writes
     * left for the keys it replicates, and they agree.  Every key has three holders only when all
     * nodes are given.
     */
    protected static void assertReplicasAgree(String keyspace, String table, int... nodes)
    {
        Map<Integer, Map<Integer, String>> agreed = new HashMap<>();
        Map<Integer, Integer> holders = new HashMap<>();
        Map<Integer, Set<Integer>> replicatedByNode = new HashMap<>();
        for (int node : nodes)
        {
            replicatedByNode.put(node, cluster.get(node).callOnInstance(() -> {
                Set<Integer> keys = new HashSet<>();
                for (int k = 0; k < KEYS; k++)
                    if (StorageService.instance.getNaturalReplicasForToken(keyspace, table, String.valueOf(k)).endpoints().contains(FBUtilities.getBroadcastAddressAndPort()))
                        keys.add(k);
                return keys;
            }));
        }
        // the rows the writes leave, for the tables that writeWithInconsistency wrote
        Integer round = lastRound.get(keyspace + '.' + table);
        // the writes left rows only on node 1 for some keys, so node 1 must be given
        Map<Integer, Map<Integer, String>> expected = round == null ? null : expectedRows(round, replicatedByNode.get(1));
        for (int node : nodes)
        {
            int n = node;
            Object[][] rows = cluster.get(node).executeInternal("SELECT k, c, v FROM " + keyspace + '.' + table);
            Set<Integer> replicated = replicatedByNode.get(node);
            Map<Integer, Map<Integer, String>> local = new HashMap<>();
            Set<Integer> loadKeys = new HashSet<>();
            for (Object[] row : rows)
            {
                if ((Integer) row[0] >= LOAD_KEYS)
                    loadKeys.add((Integer) row[0]);
                else
                    local.computeIfAbsent((Integer) row[0], k -> new HashMap<>()).put((Integer) row[1], (String) row[2]);
            }
            // the load writes at consistency ONE all the time, so its rows are only checked to be on a replica
            assertLoadKeysReplicated(node, keyspace, table, loadKeys);
            for (Integer key : local.keySet())
                assertTrue("node " + n + " holds key " + key + " that it does not replicate", replicated.contains(key));
            for (Integer key : replicated)
            {
                Map<Integer, String> mine = local.getOrDefault(key, Map.of());
                if (expected != null)
                    assertEquals("node " + n + " holds other rows of key " + key + " than the writes left", expected.get(key), mine);
                Map<Integer, String> seen = agreed.putIfAbsent(key, mine);
                if (seen != null)
                    assertEquals("replicas disagree on key " + key + " (node " + n + ")", seen, mine);
                holders.merge(key, 1, Integer::sum);
            }
        }
        if (nodes.length == NODES)
            for (int k = 0; k < KEYS; k++)
                assertEquals("key " + k + " replicas", 3, (int) holders.getOrDefault(k, 0));
    }

    /** Plain incremental repair, run from every node in turn, so the whole ring is repaired. */
    @Test
    public void incrementalRepair()
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        writeWithInconsistency(keyspace, "t", 0);
        Counts before = Counts.now();
        for (int node = 1; node <= NODES; node++)
            repair(node, keyspace, null);
        assertCursorRan(before, allNodes());
        assertRepairedState(keyspace, "t", true);
    }

    /** Repair of each node's primary ranges, from every node. */
    @Test
    public void primaryRangeRepair()
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        writeWithInconsistency(keyspace, "t", 0);
        Counts before = Counts.now();
        for (int node = 1; node <= NODES; node++)
            repair(node, keyspace, null, "-pr");
        assertCursorRan(before, allNodes());
        assertRepairedState(keyspace, "t", true);
    }

    /** Repair of one subrange of node 1's ranges. */
    @Test
    public void subrangeRepair()
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        writeWithInconsistency(keyspace, "t", 0);
        // a quarter of the first range node 1 owns that does not wrap around the ring
        String[] range = cluster.get(1).callOnInstance(() -> {
            for (org.apache.cassandra.dht.Range<org.apache.cassandra.dht.Token> owned : StorageService.instance.getLocalReplicas(keyspace).ranges())
            {
                long left = owned.left.getLongValue();
                long right = owned.right.getLongValue();
                if (right > left)
                    return new String[]{ String.valueOf(left), String.valueOf(left + (right - left) / 4) };
            }
            throw new AssertionError("node 1 owns no range that does not wrap");
        });
        Counts before = Counts.now();
        repair(1, keyspace, null, "-st", range[0], "-et", range[1]);
        Counts after = Counts.now();
        int ran = 0;
        for (int node = 1; node <= NODES; node++)
        {
            assertEquals("node " + node + " fell back in validation", 0, after.delta(before, node, VALIDATION_FALLBACKS));
            assertEquals("node " + node + " fell back in anticompaction", 0, after.delta(before, node, ANTICOMPACTION_FALLBACKS));
            if (after.delta(before, node, VALIDATIONS) > 0)
            {
                assertTrue("node " + node + " validated but ran no cursor anticompaction", after.delta(before, node, ANTICOMPACTIONS) > 0);
                ran++;
            }
            assertCursorWroteIfItRan(after, before, node);
        }
        assertEquals("the three replicas of the subrange must validate on the cursor path", 3, ran);
        assertRepairedState(keyspace, "t", false);
    }

    /**
     * Two keyspaces of five tables, one of which holds a collection that was dropped and not
     * re-added (CASSANDRA-21607, the one accepted fallback).  Only that table falls back, and it is
     * counted.
     */
    @Test
    public void manyTablesWithOneAllowedFallback()
    {
        for (int ks = 0; ks < 2; ks++)
        {
            String keyspace = newKeyspace();
            for (int t = 0; t < 5; t++)
            {
                createTable(keyspace, "t" + t);
                writeWithInconsistency(keyspace, "t" + t, t);
            }
            if (ks == 0)
            {
                cluster.schemaChange("CREATE TABLE " + keyspace + ".dropped (k int PRIMARY KEY, m map<text, text>, v text)");
                for (int node = 1; node <= NODES; node++)
                    cluster.get(node).nodetoolResult("disableautocompaction", keyspace, "dropped").asserts().success();
                for (int k = 0; k < 100; k++)
                    cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".dropped (k, m, v) VALUES (?, {'a': 'b'}, 'v')", ConsistencyLevel.ALL, k);
                flushAll(keyspace);
                cluster.schemaChange("ALTER TABLE " + keyspace + ".dropped DROP m");
            }
            for (int t = 0; t < 5; t++)
            {
                Counts before = Counts.now();
                for (int node = 1; node <= NODES; node++)
                    repair(node, keyspace, "t" + t);
                assertCursorRan(before, allNodes());
                assertRepairedState(keyspace, "t" + t, true);
            }
            if (ks == 0)
            {
                Counts before = Counts.now();
                for (int node = 1; node <= NODES; node++)
                    repair(node, keyspace, "dropped");
                Counts after = Counts.now();
                for (int node = 1; node <= NODES; node++)
                {
                    // a validation with no sstable of the session on this node still takes the cursor path
                    assertTrue("node " + node + ": the dropped collection table must count its validation fallbacks", after.delta(before, node, VALIDATION_FALLBACKS) > 0);
                    assertTrue("node " + node + ": the dropped collection table must count its anticompaction fallbacks", after.delta(before, node, ANTICOMPACTION_FALLBACKS) > 0);
                    assertEquals("node " + node + ": the dropped collection table never runs cursor anticompaction", 0, after.delta(before, node, ANTICOMPACTIONS));
                }
                awaitNoPendingSSTables(keyspace, "dropped");
            }
        }
    }

    /** A counter table with a dropped counter column takes the cursor path, with no fallback (CASSANDRA-21461). */
    @Test
    public void droppedCounterColumn()
    {
        String keyspace = newKeyspace();
        cluster.schemaChange("CREATE TABLE " + keyspace + ".counters (k int PRIMARY KEY, a counter, b counter)");
        for (int node = 1; node <= NODES; node++)
            cluster.get(node).nodetoolResult("disableautocompaction", keyspace, "counters").asserts().success();
        // written on each node directly: a counter write needs a leader, which this cluster, whose nodes share one address with different ports, cannot pick
        for (int node = 1; node <= NODES; node++)
            for (int k = 0; k < 200; k++)
                cluster.get(node).executeInternal("UPDATE " + keyspace + ".counters SET a = a + 1, b = b + 2 WHERE k = ?", k);
        flushAll(keyspace);
        cluster.schemaChange("ALTER TABLE " + keyspace + ".counters DROP a");
        Counts before = Counts.now();
        for (int node = 1; node <= NODES; node++)
            repair(node, keyspace, "counters");
        assertCursorRan(before, allNodes());
        awaitNoPendingSSTables(keyspace, "counters");
    }

    /** A preview of the repaired data after repair is in sync; after a repaired-data mismatch it must report it. */
    @Test
    public void previewAfterRepairReportsRepairedDataMismatch()
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        // a load write between the repair and the preview would put the replicas out of sync
        stopLoadOnScenarioTable();
        writeWithInconsistency(keyspace, "t", 0);
        for (int node = 1; node <= NODES; node++)
            repair(node, keyspace, null);
        assertRepairedState(keyspace, "t", true);

        Counts before = Counts.now();
        cluster.get(1).nodetoolResult("repair", "--validate", keyspace, "t").asserts().success().notificationContains("Repaired data is in sync");
        Counts after = Counts.now();
        for (int node = 1; node <= NODES; node++)
            assertEquals("node " + node + " fell back in preview validation", 0, after.delta(before, node, VALIDATION_FALLBACKS));
        assertTrue("the preview did not validate on the cursor path", after.delta(before, 1, VALIDATIONS) > 0);

        // a row in repaired data on node 1 only: preview of the repaired data must find it
        cluster.get(1).runOnInstance(() -> {
            for (int k = 0; k < KEYS; k++)
            {
                if (StorageService.instance.getNaturalReplicasForToken(keyspace, "t", String.valueOf(k)).endpoints().contains(FBUtilities.getBroadcastAddressAndPort()))
                {
                    org.apache.cassandra.cql3.QueryProcessor.executeInternal("INSERT INTO " + keyspace + ".t (k, c, v) VALUES (?, ?, ?)", k, 50, "repaired only on node 1");
                    break;
                }
            }
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore("t");
            Set<SSTableReader> before1 = new HashSet<>(cfs.getLiveSSTables());
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            Set<SSTableReader> fresh = new HashSet<>(cfs.getLiveSSTables());
            fresh.removeAll(before1);
            try
            {
                cfs.getCompactionStrategyManager().mutateRepaired(fresh, System.currentTimeMillis(), ActiveRepairService.NO_PENDING_REPAIR, false);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
        cluster.get(1).nodetoolResult("repair", "--validate", keyspace, "t").asserts().success().notificationContains("Repaired data is inconsistent");
    }

    /**
     * A session that fails on one node after anticompaction: node 3 never gets the finalize
     * message, then the session is failed there.  Its pending sstables go back to unrepaired,
     * nothing leaks, and a second repair succeeds on the cursor path.
     */
    @Test
    public void failedSession()
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        writeWithInconsistency(keyspace, "t", 0);
        // the finalize message stays dropped until the session has failed on node 3, so a late
        // message cannot finalize it there
        AtomicInteger finalizeDropped = dropFinalizeCommit(cluster, 3);
        String sessionId;
        try
        {
            Counts before = Counts.now();
            repair(1, keyspace, null);
            // node 1 may not be a replica of the history row, so read it from every replica
            TimeUUID session = (TimeUUID) cluster.coordinator(1).execute("SELECT parent_id FROM system_distributed.repair_history WHERE keyspace_name = ? AND columnfamily_name = ? ALLOW FILTERING",
                                                                         ConsistencyLevel.ALL, keyspace, "t")[0][0];
            sessionId = session.toString();
            // so the failure below has something to undo
            assertCursorAntiCompactedAndPending(before, 3, keyspace, "t", sessionId);
            String id = sessionId;
            awaitDropped(finalizeDropped, 3);
            // only node 3 never got the commit, so only node 3 ends failed
            awaitFinalizedExcept(cluster, id, 3);
            cluster.get(3).runOnInstance(() -> ActiveRepairService.instance().failSession(id, true));
        }
        finally
        {
            cluster.filters().reset();
        }
        assertBackToUnrepairedAndRepairsAgain(keyspace, sessionId);
    }

    /**
     * A session cancelled while its validations are held back: the pending sstables go back to
     * unrepaired, nothing leaks, and a second repair succeeds on the cursor path.
     */
    @Test
    public void cancelledSession() throws Exception
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        writeWithInconsistency(keyspace, "t", 0);
        // hold back every validation, so no node streams before the cancel; a cancel during streaming
        // moves the sstables back to unrepaired while they are sent, and the receiver rejects them
        cluster.filters().inbound().messagesMatching((from, to, msg) -> msg.verb() == Verb.VALIDATION_REQ.id).drop();
        // the held back validations never answer, so the repair may not return; do not wait on it for long
        Counts before = Counts.now();
        Thread repair = new Thread(() -> cluster.get(1).nodetoolResult("repair", keyspace, "t"));
        repair.setDaemon(true);
        repair.start();
        try
        {
            // wait for the session to reach node 2 and to be prepared there
            String sessionId = null;
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
            while (sessionId == null && System.nanoTime() < deadline)
            {
                sessionId = cluster.get(2).callOnInstance(() -> {
                    for (Map<String, String> info : ActiveRepairService.instance().consistent.local.sessionInfo(false, java.util.Collections.emptySet()))
                        if ("PREPARED".equals(info.get(LocalSessionInfo.STATE)))
                            return info.get(LocalSessionInfo.SESSION_ID);
                    return null;
                });
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
            assertTrue("the session never prepared on node 2", sessionId != null);
            // so the cancel below has something to undo
            assertCursorAntiCompactedAndPending(before, 2, keyspace, "t", sessionId);
            cluster.get(1).nodetoolResult("repair_admin", "cancel", "--session", sessionId, "--force").asserts().success();
            for (int node = 1; node <= NODES; node++)
            {
                String id = sessionId;
                cluster.get(node).runOnInstance(() -> ActiveRepairService.instance().failSession(id, true));
            }
            cluster.filters().reset();
            repair.join(TimeUnit.SECONDS.toMillis(30));
            assertBackToUnrepairedAndRepairsAgain(keyspace, sessionId);
        }
        finally
        {
            cluster.filters().reset();
        }
    }

    /**
     * Drops every finalize commit message to {@code node} of {@code on}, and counts the drops.  The
     * coordinator sends it once, so a caller waits for the drop before it fails the session there
     * and resets the filters; a message still in flight would reach the failed session.
     */
    static AtomicInteger dropFinalizeCommit(Cluster on, int node)
    {
        AtomicInteger dropped = new AtomicInteger();
        on.filters().inbound().to(node).messagesMatching((from, to, msg) -> {
            if (msg.verb() != Verb.FINALIZE_COMMIT_MSG.id)
                return false;
            dropped.incrementAndGet();
            return true;
        }).drop();
        return dropped;
    }

    /** Waits, for at most a minute, until {@link #dropFinalizeCommit} has dropped a message to {@code node}. */
    static void awaitDropped(AtomicInteger dropped, int node)
    {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
        while (dropped.get() == 0)
        {
            assertTrue("no finalize commit message to node " + node + " was dropped", System.nanoTime() < deadline);
            Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Waits, for at most a minute, until every node of {@code on} but {@code failing} that holds the
     * session has finalized it.  The coordinator sends the finalize commit to every participant;
     * a failure sent by {@code failing} before the commit reached a node would race with it there.
     */
    static void awaitFinalizedExcept(Cluster on, String sessionId, int failing)
    {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
        while (true)
        {
            List<String> notFinalized = new ArrayList<>();
            for (int node = 1; node <= on.size(); node++)
            {
                if (node == failing || on.get(node).isShutdown())
                    continue;
                String state = on.get(node).callOnInstance(() -> {
                    org.apache.cassandra.repair.consistent.LocalSession session =
                        ActiveRepairService.instance().consistent.local.getSession(TimeUUID.fromString(sessionId));
                    return session == null ? null : session.getState().name();
                });
                if (state != null && !"FINALIZED".equals(state))
                    notFinalized.add("node " + node + " " + state);
            }
            if (notFinalized.isEmpty())
                return;
            assertTrue("session " + sessionId + " did not finalize on every node but node " + failing + ": " + notFinalized,
                       System.nanoTime() < deadline);
            Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Before a session is cancelled or failed: {@code node} ran cursor anticompaction for it, with no
     * fallback and partitions written, and holds a live sstable pending for it.
     */
    private static void assertCursorAntiCompactedAndPending(Counts before, int node, String keyspace, String table, String sessionId)
    {
        Counts after = Counts.now();
        assertTrue("node " + node + " ran no cursor anticompaction", after.delta(before, node, ANTICOMPACTIONS) > 0);
        assertEquals("node " + node + " fell back in anticompaction", 0, after.delta(before, node, ANTICOMPACTION_FALLBACKS));
        assertTrue("node " + node + " ran cursor anticompaction but the cursor wrote no partition", after.delta(before, node, PARTITIONS_WRITTEN) > 0);
        int pending = cluster.get(node).callOnInstance(() -> {
            int count = 0;
            for (SSTableReader sstable : Keyspace.open(keyspace).getColumnFamilyStore(table).getLiveSSTables())
                if (sessionId.equals(String.valueOf(sstable.getPendingRepair())))
                    count++;
            return count;
        });
        assertTrue("node " + node + " holds no live sstable pending for session " + sessionId, pending > 0);
    }

    private static void assertBackToUnrepairedAndRepairsAgain(String keyspace, String sessionId)
    {
        for (int node = 1; node <= NODES; node++)
        {
            cluster.get(node).runOnInstance(() -> ActiveRepairService.instance().consistent.local.cleanup());
        }
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        while (true)
        {
            int pending = 0;
            for (int node = 1; node <= NODES; node++)
                pending += cluster.get(node).callOnInstance(() -> {
                    int count = 0;
                    for (SSTableReader sstable : Keyspace.open(keyspace).getColumnFamilyStore("t").getLiveSSTables())
                        if (sessionId.equals(String.valueOf(sstable.getPendingRepair())))
                            count++;
                    return count;
                });
            if (pending == 0)
                break;
            if (System.nanoTime() > deadline)
                fail(pending + " sstables are still pending for the failed session " + sessionId);
            compactInBackground(keyspace, "t");
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
        }
        assertNoSSTableLeak(cluster, keyspace, "t");

        // the other nodes' data is repaired already, so only some nodes may have anything to anticompact
        Counts before = Counts.now();
        for (int node = 1; node <= NODES; node++)
            repair(node, keyspace, "t");
        assertCursorValidationRanAndSomeAnticompaction(before);
        assertRepairedState(keyspace, "t", true);
    }

    /** On every node the cursor ran validation; some node ran cursor anticompaction; no node fell back. */
    private static void assertCursorValidationRanAndSomeAnticompaction(Counts before)
    {
        Counts after = Counts.now();
        long anticompactions = 0;
        for (int node = 1; node <= NODES; node++)
        {
            assertTrue("node " + node + " ran no cursor validation", after.delta(before, node, VALIDATIONS) > 0);
            assertEquals("node " + node + " fell back in validation", 0, after.delta(before, node, VALIDATION_FALLBACKS));
            assertEquals("node " + node + " fell back in anticompaction", 0, after.delta(before, node, ANTICOMPACTION_FALLBACKS));
            anticompactions += after.delta(before, node, ANTICOMPACTIONS);
            assertCursorWroteIfItRan(after, before, node);
        }
        assertTrue("no node ran cursor anticompaction", anticompactions > 0);
    }

    /**
     * A node is down: incremental repair needs every replica, so the repair fails.  With
     * {@code --force} it succeeds on the live replicas, on the cursor path, and marks nothing
     * repaired.  Once the node is back, repair succeeds on the cursor path everywhere.
     */
    @Test
    public void nodeDown() throws Exception
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        writeWithInconsistency(keyspace, "t", 0);
        nodeRestart.writeLock().lock();
        try
        {
            cluster.get(4).shutdown().get();
            try
            {
                cluster.get(1).nodetoolResult("repair", keyspace, "t").asserts().failure();

                // with --force the repair runs on the live replicas, and marks nothing repaired
                Counts beforeForced = Counts.now();
                repair(1, keyspace, "t", "--force");
                assertCursorRan(beforeForced, 1, 2, 3);
                awaitNoPendingSSTables(keyspace, "t");
                assertNothingRepairedAndOnDiskStateMatches(keyspace, "t", 1, 2, 3);
                assertReplicasAgree(keyspace, "t", 1, 2, 3);
            }
            finally
            {
                cluster.get(4).startup();
            }
        }
        finally
        {
            nodeRestart.writeLock().unlock();
        }
        disableAutoCompactionUnlessUnderLoad(keyspace, "t");
        Counts before = Counts.now();
        for (int node = 1; node <= NODES; node++)
            repair(node, keyspace, "t");
        assertCursorRan(before, allNodes());
        assertRepairedState(keyspace, "t", true);
    }

    /**
     * On each of {@code nodes}: no live sstable is repaired or pending, and the files on disk are
     * those of the live sstables.  The caller holds {@link #nodeRestart}'s write lock under load.
     */
    private static void assertNothingRepairedAndOnDiskStateMatches(String keyspace, String table, int... nodes)
    {
        for (int node : nodes)
        {
            int n = node;
            cluster.get(node).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                // a flush or compaction in flight has files that are not live yet.  Nothing writes to
                // the table now, so after this flush a commit log flush of it writes no sstable, and
                // no compaction may run during the checks.
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                withNoCompactionRunning(cfs, () -> {
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                        assertTrue("node " + n + ": " + sstable + " is repaired or pending after a forced repair",
                                   !sstable.isRepaired() && !sstable.isPendingRepair());
                    org.apache.cassandra.Util.assertOnDiskState(cfs, cfs.getLiveSSTables().size());
                    org.apache.cassandra.db.compaction.AntiCompactionAssertions.assertEveryFileBelongsToALiveSSTable(cfs);
                });
            });
        }
    }

    /**
     * A node goes down in the middle of a session: node 4 has prepared the session (its
     * anticompaction is done) and its validation is held back when it stops.  The session fails;
     * once node 4 is back, the session's pending sstables go back to unrepaired on every node,
     * nothing leaks, and a second repair succeeds on the cursor path.
     */
    @Test
    public void nodeDownInTheMiddleOfASession() throws Exception
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        writeWithInconsistency(keyspace, "t", 0);
        Counts before = Counts.now();
        cluster.filters().inbound().to(4).messagesMatching((from, to, msg) -> msg.verb() == Verb.VALIDATION_REQ.id).drop();
        // node 4 never answers, so the repair may not return; do not wait on it for long
        Thread repair = new Thread(() -> cluster.get(1).nodetoolResult("repair", keyspace, "t"));
        repair.setDaemon(true);
        repair.start();
        String sessionId = null;
        try
        {
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
            while (sessionId == null && System.nanoTime() < deadline)
            {
                sessionId = cluster.get(4).callOnInstance(() -> {
                    for (Map<String, String> info : ActiveRepairService.instance().consistent.local.sessionInfo(false, java.util.Collections.emptySet()))
                        if ("PREPARED".equals(info.get(LocalSessionInfo.STATE)) || "REPAIRING".equals(info.get(LocalSessionInfo.STATE)))
                            return info.get(LocalSessionInfo.SESSION_ID);
                    return null;
                });
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
            assertTrue("the session never prepared on node 4", sessionId != null);
            Counts prepared = Counts.now();
            assertTrue("node 4 did not anticompact on the cursor path before it stopped", prepared.delta(before, 4, ANTICOMPACTIONS) > 0);
            assertCursorWroteIfItRan(prepared, before, 4);
            assertEquals("node 4 fell back in anticompaction", 0, prepared.delta(before, 4, ANTICOMPACTION_FALLBACKS));
            nodeRestart.writeLock().lock();
            cluster.get(4).shutdown().get();
        }
        finally
        {
            cluster.filters().reset();
            try
            {
                if (cluster.get(4).isShutdown())
                    cluster.get(4).startup();
            }
            finally
            {
                if (nodeRestart.isWriteLockedByCurrentThread())
                    nodeRestart.writeLock().unlock();
            }
        }
        repair.join(TimeUnit.SECONDS.toMillis(30));
        disableAutoCompactionUnlessUnderLoad(keyspace, "t");
        for (int node = 1; node <= NODES; node++)
        {
            String id = sessionId;
            cluster.get(node).runOnInstance(() -> ActiveRepairService.instance().failSession(id, true));
        }
        assertBackToUnrepairedAndRepairsAgain(keyspace, sessionId);
    }

    /** Five rounds of write, flush and repair, from a different node each round. */
    @Test
    public void fiveRounds()
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        for (int round = 0; round < 5; round++)
        {
            writeWithInconsistency(keyspace, "t", round);
            Counts before = Counts.now();
            for (int node = 1; node <= NODES; node++)
                repair(1 + (node + round) % NODES, keyspace, null);
            assertCursorRan(before, allNodes());
            assertRepairedState(keyspace, "t", true);
        }
    }

    /**
     * Each combination of cursor_compaction_enabled and cursor_repair_enabled: cursor_repair_enabled
     * alone decides whether validation and anticompaction take the cursor path.
     */
    @Test
    public void settingsMatrix()
    {
        try
        {
            for (boolean cursorCompaction : new boolean[]{ true, false })
            {
                for (boolean cursorRepair : new boolean[]{ true, false })
                {
                    for (int node = 1; node <= NODES; node++)
                        cluster.get(node).runOnInstance(() -> {
                            DatabaseDescriptor.setCursorCompactionEnabled(cursorCompaction);
                            DatabaseDescriptor.setCursorRepairEnabled(cursorRepair);
                        });
                    String keyspace = newKeyspace();
                    createTable(keyspace, "t");
                    writeWithInconsistency(keyspace, "t", 0);
                    Counts before = Counts.now();
                    for (int node = 1; node <= NODES; node++)
                        repair(node, keyspace, null);
                    Counts after = Counts.now();
                    String where = "cursor_compaction_enabled=" + cursorCompaction + " cursor_repair_enabled=" + cursorRepair;
                    for (int node = 1; node <= NODES; node++)
                    {
                        assertEquals(where + ": node " + node + " fell back in validation", 0, after.delta(before, node, VALIDATION_FALLBACKS));
                        assertEquals(where + ": node " + node + " fell back in anticompaction", 0, after.delta(before, node, ANTICOMPACTION_FALLBACKS));
                        assertEquals(where + ": node " + node + " cursor validation", cursorRepair, after.delta(before, node, VALIDATIONS) > 0);
                        assertEquals(where + ": node " + node + " cursor anticompaction", cursorRepair, after.delta(before, node, ANTICOMPACTIONS) > 0);
                        assertEquals(where + ": node " + node + " cursor partitions written", cursorRepair, after.delta(before, node, PARTITIONS_WRITTEN) > 0);
                    }
                    assertRepairedState(keyspace, "t", true);
                }
            }
        }
        finally
        {
            for (int node = 1; node <= NODES; node++)
                cluster.get(node).runOnInstance(() -> {
                    DatabaseDescriptor.setCursorCompactionEnabled(true);
                    DatabaseDescriptor.setCursorRepairEnabled(true);
                });
        }
    }

    /**
     * A node crashes while cursor anticompaction writes its outputs.  The table's files on node
     * {@link PauseCursorAntiCompaction#NODE} are copied while its anticompaction is held in the
     * middle of a write; the node is stopped and the copy is put back, so the node starts from the
     * files as they were at that moment: the originals, the partly written outputs and the
     * transaction log.  The held anticompaction then fails, so the session fails, as it would
     * when a node crashes.  After the restart only the originals are live and no other file is
     * left, and the next repair succeeds on the cursor path.
     */
    @Test
    public void crashWhileCursorAntiCompactionWrites() throws Exception
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        // the load would flush new sstables after the originals are recorded
        stopLoadOnScenarioTable();
        writeWithInconsistency(keyspace, "t", 0);
        int node = PauseCursorAntiCompaction.NODE;
        // background compaction must not change the originals before the repair takes them
        cluster.get(node).runOnInstance(() -> awaitNoCompaction(Keyspace.open(keyspace).getColumnFamilyStore("t")));
        Set<String> originals = liveSSTables(node, keyspace, "t");
        List<String> directories = cluster.get(node).callOnInstance(() -> {
            List<String> dirs = new ArrayList<>();
            for (org.apache.cassandra.io.util.File dir : Keyspace.open(keyspace).getColumnFamilyStore("t").getDirectories().getCFDirectories())
                dirs.add(dir.toString());
            return dirs;
        });
        // the injected crash reaches the compaction thread's handler; it is expected
        cluster.setUncaughtExceptionsFilter((n, t) -> {
            for (Throwable cause = t; cause != null; cause = cause.getCause())
                if (String.valueOf(cause.getMessage()).contains("injected crash"))
                    return true;
            return false;
        });
        cluster.get(node).runOnInstance(() -> PauseCursorAntiCompaction.pauseBefore(20));
        Thread repair = new Thread(() -> cluster.get(1).nodetoolResult("repair", keyspace, "t"));
        repair.setDaemon(true);
        repair.start();
        java.nio.file.Path copy = java.nio.file.Files.createTempDirectory("cursor-anticompaction-crash");
        try
        {
            assertTrue("the cursor anticompaction was not held", cluster.get(node).callOnInstance(() -> PauseCursorAntiCompaction.awaitHeld()));
            for (int i = 0; i < directories.size(); i++)
                copyFiles(java.nio.file.Paths.get(directories.get(i)), copy.resolve(String.valueOf(i)), false);
            assertCrashState(copy, originals);
        }
        finally
        {
            cluster.get(node).runOnInstance(() -> PauseCursorAntiCompaction.release());
        }
        repair.join(TimeUnit.MINUTES.toMillis(2));
        // the failed session's pending sstables on the other nodes go back to unrepaired
        String failed = cluster.get(1).callOnInstance(() -> {
            for (Map<String, String> info : ActiveRepairService.instance().consistent.local.sessionInfo(true, java.util.Collections.emptySet()))
                if (info.get(LocalSessionInfo.TABLES).contains(keyspace + ".t"))
                    return info.get(LocalSessionInfo.SESSION_ID);
            return null;
        });
        nodeRestart.writeLock().lock();
        try
        {
            cluster.get(node).shutdown().get();
            for (int i = 0; i < directories.size(); i++)
                copyFiles(copy.resolve(String.valueOf(i)), java.nio.file.Paths.get(directories.get(i)), true);
            org.apache.cassandra.io.util.FileUtils.deleteRecursive(new org.apache.cassandra.io.util.File(copy));
            cluster.get(node).startup();
        }
        finally
        {
            nodeRestart.writeLock().unlock();
        }
        disableAutoCompactionUnlessUnderLoad(keyspace, "t");

        assertEquals("after the restart, the live sstables are not the originals", originals, liveSSTables(node, keyspace, "t"));
        if (failed != null)
            for (int n = 1; n <= NODES; n++)
                cluster.get(n).runOnInstance(() -> ActiveRepairService.instance().failSession(failed, true));
        awaitNoPendingSSTables(keyspace, "t");
        cluster.get(node).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore("t");
            org.apache.cassandra.Util.assertOnDiskState(cfs, cfs.getLiveSSTables().size());
            org.apache.cassandra.db.compaction.AntiCompactionAssertions.assertEveryFileBelongsToALiveSSTable(cfs);
        });

        Counts before = Counts.now();
        for (int n = 1; n <= NODES; n++)
            repair(n, keyspace, null);
        Counts after = Counts.now();
        assertTrue("node " + node + " did not anticompact its originals on the cursor path", after.delta(before, node, ANTICOMPACTIONS) > 0);
        assertCursorValidationRanAndSomeAnticompaction(before);
        assertRepairedState(keyspace, "t", true);
    }

    /**
     * The copy holds the files of a write in progress: a transaction log, and a data file that
     * is not one of the originals.
     */
    private static void assertCrashState(java.nio.file.Path copy, Set<String> originals) throws java.io.IOException
    {
        boolean txnLog = false;
        boolean newData = false;
        try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.walk(copy))
        {
            for (java.nio.file.Path file : (Iterable<java.nio.file.Path>) files::iterator)
            {
                String name = file.getFileName().toString();
                txnLog |= name.contains("_txn_");
                if (name.endsWith("-Data.db"))
                    newData |= !originals.contains(name.substring(0, name.length() - "-Data.db".length()));
            }
        }
        assertTrue("the copy holds no transaction log, so it is not a crash in the middle of a write", txnLog);
        assertTrue("the copy holds no data file other than the originals", newData);
    }

    /** The base names of the live sstables of a table on a node. */
    private static Set<String> liveSSTables(int node, String keyspace, String table)
    {
        return cluster.get(node).callOnInstance(() -> {
            Set<String> names = new HashSet<>();
            for (SSTableReader sstable : Keyspace.open(keyspace).getColumnFamilyStore(table).getLiveSSTables())
                names.add(sstable.descriptor.baseFile().name());
            return names;
        });
    }

    /** Copies the files, not the subdirectories, of {@code from} into {@code to}; with {@code replace}, deletes the files of {@code to} first. */
    private static void copyFiles(java.nio.file.Path from, java.nio.file.Path to, boolean replace) throws java.io.IOException
    {
        java.nio.file.Files.createDirectories(to);
        if (replace)
        {
            try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.list(to))
            {
                for (java.nio.file.Path file : (Iterable<java.nio.file.Path>) files::iterator)
                    if (java.nio.file.Files.isRegularFile(file))
                        java.nio.file.Files.delete(file);
            }
        }
        try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.list(from))
        {
            for (java.nio.file.Path file : (Iterable<java.nio.file.Path>) files::iterator)
                if (java.nio.file.Files.isRegularFile(file))
                    java.nio.file.Files.copy(file, to.resolve(file.getFileName()));
        }
    }

    /**
     * Two incremental repairs of separate subranges of node 1 and a preview of the repaired data on
     * one table, first on the legacy path and then on the cursor path.  The second repair and the
     * preview start once the first repair's anticompaction has finished on every node, so they run
     * while the first repair validates and syncs.  At least one of the two repairs succeeds on each
     * path.  A repair that fails must fail for a known contention reason (an sstable it needs is
     * held by the other session, or its anticompaction was interrupted), found in its own nodetool
     * output or in the log lines that name one of its sessions.  On the cursor path, that text must not
     * name cursor code or an error the legacy path cannot raise, and no node's log may hold an
     * entry with a stack frame in cursor code, other than an interrupted compaction (stack frames
     * name no session, so the logs are searched whole).  The preview succeeds, or fails because
     * it met sstables still pending for the first repair; which one depends on whether the first
     * repair finished first, so it is not compared between paths.  Nothing falls back, nothing
     * leaks, no sstable stays pending, and a repair afterwards succeeds.
     */
    @Test
    public void concurrentSessions() throws Exception
    {
        try
        {
            for (boolean cursor : new boolean[]{ false, true })
            {
                for (int node = 1; node <= NODES; node++)
                    cluster.get(node).runOnInstance(() -> DatabaseDescriptor.setCursorRepairEnabled(cursor));
                assertConcurrentSessions(cursor);
            }
        }
        finally
        {
            for (int node = 1; node <= NODES; node++)
                cluster.get(node).runOnInstance(() -> DatabaseDescriptor.setCursorRepairEnabled(true));
        }
    }

    /** The known reasons an incremental repair fails when another session on the same table holds or interrupts its work, by log pattern. */
    private static final Map<String, String> CONTENTION_REASONS = Map.of(
        "an sstable is held by another session", "unable to acquire exclusive access|intersecting sstables belonging to another incremental repair session",
        "anticompaction was interrupted", "Anticompaction for session .* has been stopped by request|Compaction interrupted: Anticompaction");

    /** The known reasons a preview of the repaired data fails while an incremental repair runs or finishes, by log pattern. */
    private static final Map<String, String> PREVIEW_REASONS = Map.of(
        "the preview met sstables pending for a running session", "is marked pending for non-finalized incremental repair session .*, failing preview repair",
        "validation was interrupted", "Compaction interrupted: Validation",
        "an intersecting incremental repair finished during the preview", "An intersecting incremental repair with session id = .* finished, preview repair might not be accurate");

    /** A session id in the nodetool output of a repair: the parent session, and the repair sessions that failed. */
    private static final java.util.regex.Pattern SESSION_ID = java.util.regex.Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

    /** What a cursor-path failure must not name: a cursor class in a stack frame or message, or an error that only a bug raises. */
    private static final java.util.regex.Pattern NOT_FROM_LEGACY = java.util.regex.Pattern.compile(
        "org\\.apache\\.cassandra\\.\\S*Cursor|NullPointerException|AssertionError|ClassCastException|IndexOutOfBoundsException|IllegalStateException|IllegalArgumentException");

    /**
     * A stack frame in one of the classes that cursor compaction, validation and anticompaction
     * run, or their inner classes; not other classes named for cursors, such as TreeCursor.
     */
    static final String CURSOR_STACK_FRAME = "at org\\.apache\\.cassandra\\.(" +
                                             "db\\.compaction\\.(CursorCompactor|CursorCompactionPipeline|CursorCounterContexts|DigestingCursorMergeSink|StatefulCursor)" +
                                             "|db\\.repair\\.CursorValidationIterator" +
                                             "|io\\.sstable\\.(SSTableCursorReader|SSTableCursorWriter|SSTableCursorKeyReader|CursorIndexWriter|BigCursorIndexWriter|CursorMergeSink|format\\.bti\\.BtiCursorIndexWriter)" +
                                             ")[.$]";

    /** The pattern finds frames of the cursor classes, and not of other classes named for cursors. */
    @Test
    public void cursorStackFramePattern()
    {
        java.util.regex.Pattern frame = java.util.regex.Pattern.compile(CURSOR_STACK_FRAME);
        assertTrue(frame.matcher("\tat org.apache.cassandra.db.compaction.CursorCompactor.writeNextPartition(CursorCompactor.java:812)").find());
        assertTrue(frame.matcher("\tat org.apache.cassandra.db.compaction.CursorCompactor$PartitionMerger.merge(CursorCompactor.java:90)").find());
        assertTrue(frame.matcher("\tat org.apache.cassandra.io.sstable.SSTableCursorReader.readRow(SSTableCursorReader.java:40)").find());
        assertTrue(frame.matcher("\tat org.apache.cassandra.db.repair.CursorValidationIterator.next(CursorValidationIterator.java:200)").find());
        assertTrue(!frame.matcher("\tat org.apache.cassandra.utils.btree.TreeCursor.seekTo(TreeCursor.java:120)").find());
        assertTrue(!frame.matcher("\tat org.apache.cassandra.db.tries.InMemoryTrie$MemtableCursor.advance(InMemoryTrie.java:900)").find());
        assertTrue(!frame.matcher("\tat org.apache.cassandra.db.repair.CassandraTableRepairManager.validate(CursorValidationIterator.java:10)").find());
        assertTrue(!frame.matcher("org.apache.cassandra.db.repair.CursorValidationUnsupportedException: unsupported").find());
    }

    /** The first line of a log entry in the instance logs: the level, then the thread. */
    private static final java.util.regex.Pattern LOG_ENTRY_START = java.util.regex.Pattern.compile("^(TRACE|DEBUG|INFO|WARN|ERROR)\\s+\\[");
    /** The line that names an exception: at the top of a stack trace, or after "Caused by: " or "Suppressed: ". */
    private static final java.util.regex.Pattern EXCEPTION_LINE = java.util.regex.Pattern.compile("^(Caused by: |Suppressed: )?([\\w.$]+(Exception|Error))(:|$)");

    /**
     * The log entries, each a message line and its stack trace lines ("Caused by" and "Suppressed"
     * included), that hold a stack frame in cursor code.  An entry whose exception, or an exception
     * in its cause chain, is CompactionInterruptedException is left out: a session's anticompaction
     * stops a running compaction, and the legacy path logs the same entry at the same stop points,
     * with its own frames.
     */
    static List<String> entriesWithCursorFrames(List<String> lines)
    {
        java.util.regex.Pattern frame = java.util.regex.Pattern.compile(CURSOR_STACK_FRAME);
        List<String> found = new ArrayList<>();
        List<String> entry = new ArrayList<>();
        for (int i = 0; i <= lines.size(); i++)
        {
            if (i == lines.size() || LOG_ENTRY_START.matcher(lines.get(i)).find())
            {
                boolean interrupted = false;
                boolean cursorFrame = false;
                for (String line : entry)
                {
                    java.util.regex.Matcher named = EXCEPTION_LINE.matcher(line.trim());
                    interrupted |= named.find() && named.group(2).equals("org.apache.cassandra.db.compaction.CompactionInterruptedException");
                    cursorFrame |= frame.matcher(line).find();
                }
                if (cursorFrame && !interrupted)
                    found.add(String.join("\n", entry));
                entry.clear();
            }
            if (i < lines.size())
                entry.add(lines.get(i));
        }
        return found;
    }

    /** An interrupted compaction is skipped, also as a cause; any other exception with a cursor frame is found. */
    @Test
    public void entriesWithCursorFramesSkipsOnlyInterruptedCompactions()
    {
        List<String> interrupted = List.of("DEBUG [CompactionExecutor:1] node1 2026-10-11T10:00:00,000 CompactionTask.java:389 - Unexpected exception in compaction",
                                           "org.apache.cassandra.db.compaction.CompactionInterruptedException: Compaction interrupted: Compaction(...)",
                                           "\tat org.apache.cassandra.db.compaction.CursorCompactor.mergeRows(CursorCompactor.java:1098)",
                                           "\tat org.apache.cassandra.db.compaction.CompactionTask.runMayThrow(CompactionTask.java:300)",
                                           "INFO  [CompactionExecutor:1] node1 2026-10-11T10:00:00,001 ExecutionFailure.java:60 - Compaction interrupted: Compaction(...)");
        assertTrue(entriesWithCursorFrames(interrupted).isEmpty());

        List<String> other = List.of("ERROR [CompactionExecutor:1] node1 2026-10-11T10:00:00,000 CassandraDaemon.java:200 - Exception in thread",
                                     "java.lang.IllegalStateException: bad state",
                                     "\tat org.apache.cassandra.db.compaction.CursorCompactor.mergeRows(CursorCompactor.java:1098)",
                                     "INFO  [main] node1 2026-10-11T10:00:00,001 Foo.java:1 - next entry");
        assertEquals(1, entriesWithCursorFrames(other).size());

        // an interrupted compaction in the cause chain is skipped too
        List<String> caused = List.of("ERROR [CompactionExecutor:1] node1 2026-10-11T10:00:00,000 CassandraDaemon.java:200 - Exception in thread",
                                      "java.lang.RuntimeException: wrapped",
                                      "\tat org.apache.cassandra.db.compaction.CursorCompactor.mergeRows(CursorCompactor.java:1098)",
                                      "Caused by: org.apache.cassandra.db.compaction.CompactionInterruptedException: Compaction interrupted",
                                      "\t... 12 more");
        assertTrue(entriesWithCursorFrames(caused).isEmpty());

        // a suppressed exception and its frames belong to the entry, and are found
        List<String> suppressed = List.of("WARN  [CompactionExecutor:1] node1 2026-10-11T10:00:00,000 Foo.java:1 - cleanup failed",
                                          "java.io.IOException: disk",
                                          "\tat org.apache.cassandra.io.util.FileUtils.close(FileUtils.java:10)",
                                          "\tSuppressed: java.lang.AssertionError: bad",
                                          "\t\tat org.apache.cassandra.io.sstable.SSTableCursorWriter.close(SSTableCursorWriter.java:50)");
        assertEquals(1, entriesWithCursorFrames(suppressed).size());
    }

    private static void assertConcurrentSessions(boolean cursor) throws Exception
    {
        String path = cursor ? "cursor path: " : "legacy path: ";
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        writeWithInconsistency(keyspace, "t", 0);
        List<String[]> subranges = cluster.get(1).callOnInstance(() -> {
            List<String[]> owned = new ArrayList<>();
            for (org.apache.cassandra.dht.Range<org.apache.cassandra.dht.Token> range : StorageService.instance.getLocalReplicas(keyspace).ranges())
                if (owned.size() < 2 && !range.isWrapAround())
                    owned.add(new String[]{ range.left.toString(), range.right.toString() });
            return owned;
        });
        assertEquals("node 1 needs two ranges that do not wrap", 2, subranges.size());
        long[] marks = new long[NODES];
        for (int node = 1; node <= NODES; node++)
            marks[node - 1] = cluster.get(node).logs().mark();
        Counts before = Counts.now();
        List<String> incremental = java.util.Collections.synchronizedList(new ArrayList<>());
        List<String> preview = java.util.Collections.synchronizedList(new ArrayList<>());
        List<Thread> threads = new ArrayList<>();
        threads.add(incrementalRepair(keyspace, subranges.get(0), incremental));
        awaitAntiCompactionDone(keyspace);
        threads.add(incrementalRepair(keyspace, subranges.get(1), incremental));
        Thread previewThread = new Thread(() -> preview.add(outcome("preview", cluster.get(2).nodetoolResult("repair", "--validate", keyspace, "t"))));
        previewThread.start();
        threads.add(previewThread);
        for (Thread thread : threads)
            thread.join(TimeUnit.MINUTES.toMillis(5));
        logger.info("concurrent sessions, {}{} {}", path, incremental, preview);
        assertEquals(path + "a session did not return: " + incremental + preview, 3, incremental.size() + preview.size());
        assertTrue(path + "no incremental repair succeeded: " + incremental, incremental.stream().anyMatch(o -> o.endsWith(" succeeded")));

        // classify each failure by its own output and the log lines that name its session
        for (String outcome : incremental)
        {
            if (outcome.endsWith(" succeeded"))
                continue;
            String text = failureText(outcome, marks);
            assertTrue(path + "an incremental repair failed for no known contention reason: " + outcome, !reasonsIn(text, CONTENTION_REASONS).isEmpty());
            java.util.regex.Matcher unexpected = NOT_FROM_LEGACY.matcher(text);
            if (cursor && unexpected.find())
                fail(path + "an incremental repair failed with " + unexpected.group() + ", which the legacy path does not raise: " + text);
        }
        if (cursor)
            for (int node = 1; node <= NODES; node++)
            {
                List<String> entries = entriesWithCursorFrames(cluster.get(node).logs().grep(marks[node - 1], ".*").getResult());
                assertTrue(path + "node " + node + " logged a stack frame in cursor code: " + entries, entries.isEmpty());
            }
        // the preview fails on sstables of the other session, so its reason names that session;
        // it is the only preview, so its reasons are looked for in the whole logs
        if (!preview.get(0).endsWith(" succeeded"))
            assertTrue(path + "the preview failed for no known reason: " + preview, !reasonsInLogs(marks, PREVIEW_REASONS).isEmpty());

        Counts after = Counts.now();
        long written = 0;
        for (int node = 1; node <= NODES; node++)
        {
            assertEquals(path + "node " + node + " fell back in validation", 0, after.delta(before, node, VALIDATION_FALLBACKS));
            assertEquals(path + "node " + node + " fell back in anticompaction", 0, after.delta(before, node, ANTICOMPACTION_FALLBACKS));
            if (!cursor)
                assertEquals(path + "node " + node + " ran cursor validation", 0, after.delta(before, node, VALIDATIONS));
            written += after.delta(before, node, PARTITIONS_WRITTEN);
        }
        // an interrupted anticompaction writes nothing, but one that succeeded wrote partitions
        assertEquals(path + "cursor partitions written", cursor, written > 0);
        awaitNoPendingSSTables(keyspace, "t");
        assertNoSSTableLeak(cluster, keyspace, "t");

        before = Counts.now();
        for (int node = 1; node <= NODES; node++)
            repair(node, keyspace, null);
        if (cursor)
            assertCursorValidationRanAndSomeAnticompaction(before);
        assertRepairedState(keyspace, "t", true);
    }

    /** One repair's nodetool output, and the lines of every node's log since {@code marks} that name a session id from it. */
    private static String failureText(String outcome, long[] marks)
    {
        StringBuilder text = new StringBuilder(outcome);
        Set<String> ids = new HashSet<>();
        java.util.regex.Matcher id = SESSION_ID.matcher(outcome);
        while (id.find())
            ids.add(id.group());
        for (String session : ids)
            for (int node = 1; node <= NODES; node++)
                for (String line : cluster.get(node).logs().grep(marks[node - 1], session).getResult())
                    text.append('\n').append(line);
        return text.toString();
    }

    private static Set<String> reasonsInLogs(long[] marks, Map<String, String> known)
    {
        Set<String> found = new HashSet<>();
        for (Map.Entry<String, String> reason : known.entrySet())
            for (int node = 1; node <= NODES; node++)
                if (!cluster.get(node).logs().grep(marks[node - 1], reason.getValue()).getResult().isEmpty())
                    found.add(reason.getKey());
        return found;
    }

    private static Set<String> reasonsIn(String text, Map<String, String> known)
    {
        Set<String> found = new HashSet<>();
        for (Map.Entry<String, String> reason : known.entrySet())
            if (java.util.regex.Pattern.compile(reason.getValue()).matcher(text).find())
                found.add(reason.getKey());
        return found;
    }

    private static Thread incrementalRepair(String keyspace, String[] range, List<String> outcomes)
    {
        Thread thread = new Thread(() -> outcomes.add(outcome("incremental " + range[0] + ".." + range[1],
                                                              cluster.get(1).nodetoolResult("repair", "-st", range[0], "-et", range[1], keyspace, "t"))));
        thread.start();
        return thread;
    }

    /**
     * Waits until the incremental session of the table has finished its anticompaction on every
     * node it runs on: no node has it preparing, and on node 1, its coordinator, it is past
     * prepared.
     */
    private static void awaitAntiCompactionDone(String keyspace)
    {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        while (true)
        {
            boolean started = false;
            boolean preparing = false;
            for (int node = 1; node <= NODES; node++)
            {
                List<String> states = cluster.get(node).callOnInstance(() -> {
                    List<String> found = new ArrayList<>();
                    for (Map<String, String> info : ActiveRepairService.instance().consistent.local.sessionInfo(true, java.util.Collections.emptySet()))
                        if (info.get(LocalSessionInfo.TABLES).contains(keyspace + ".t"))
                            found.add(info.get(LocalSessionInfo.STATE));
                    return found;
                });
                // node 1 moves past PREPARED only once every node has prepared
                started |= node == 1 && !states.isEmpty() && !states.contains("PREPARED");
                preparing |= states.contains("PREPARING");
            }
            if (started && !preparing)
                return;
            assertTrue("the first session did not finish its anticompaction", System.nanoTime() < deadline);
            Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        }
    }

    private static String outcome(String what, org.apache.cassandra.distributed.api.NodeToolResult result)
    {
        return what + (result.getRc() == 0 ? " succeeded" : " failed: " + result.getStdout() + result.getStderr() + result.getError());
    }

    /** A table written with lightweight transactions: inserts if not exists, then conditional updates. */
    @Test
    public void lightweightTransactions()
    {
        String keyspace = newKeyspace();
        createTable(keyspace, "t");
        for (int k = 0; k < KEYS; k++)
            for (int c = 0; c < 3; c++)
                cluster.coordinator(1 + k % NODES).execute("INSERT INTO " + keyspace + ".t (k, c, v) VALUES (?, ?, ?) IF NOT EXISTS", ConsistencyLevel.QUORUM, k, c, "inserted");
        for (int k = 0; k < KEYS; k += 3)
            cluster.coordinator(1 + k % NODES).execute("UPDATE " + keyspace + ".t SET v = ? WHERE k = ? AND c = ? IF v = ?", ConsistencyLevel.QUORUM, "updated", k, 1, "inserted");
        flushAll(keyspace);
        Counts before = Counts.now();
        for (int node = 1; node <= NODES; node++)
            repair(node, keyspace, null);
        assertCursorRan(before, allNodes());
        assertRepairedState(keyspace, "t", true);
        for (int node = 1; node <= NODES; node++)
        {
            Object[][] rows = cluster.get(node).executeInternal("SELECT k, c, v FROM " + keyspace + ".t");
            for (Object[] row : rows)
            {
                int k = (Integer) row[0];
                int c = (Integer) row[1];
                if (k < LOAD_KEYS)
                    assertEquals("node " + node + " key " + k + " row " + c, k % 3 == 0 && c == 1 ? "updated" : "inserted", row[2]);
            }
        }
    }
}
