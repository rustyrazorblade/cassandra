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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AntiCompactionAssertions;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.differential.InputCopy;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@code performAnticompaction} on a node with two data directories, on BIG and BTI, with TWCS,
 * UCS, LCS and STCS: the session anticompacts each directory's sstables in their own groups.  One
 * table is anticompacted on the legacy path; its inputs are then put back under the same
 * descriptors, so the strategy groups them the same way, and anticompacted on the cursor path.
 * The outputs must match per repair state, in data bytes and logical content.  The directory an
 * output goes to depends on free space, so it is not compared.
 */
public class CursorAntiCompactionDataDirectoriesTest extends TestBaseImpl
{
    private static final String[] STRATEGIES = { "TimeWindowCompactionStrategy", "UnifiedCompactionStrategy",
                                                 "LeveledCompactionStrategy", "SizeTieredCompactionStrategy" };

    private static Cluster cluster;

    @BeforeClass
    public static void start() throws Exception
    {
        cluster = init(Cluster.build(1)
                              .withNodeProvisionStrategy(INodeProvisionStrategy.Strategy.OneNetworkInterface)
                              .withDynamicPortAllocation(true)
                              .withDataDirCount(2)
                              .withConfig(config -> CursorIncrementalRepairTestBase.productionStorage(config).set("cursor_compaction_enabled", true)
                                                          .set("cursor_repair_enabled", true))
                              .start());
        CursorIncrementalRepairTestBase.assertProductionStorage(cluster);
    }

    @AfterClass
    public static void close()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void big()
    {
        for (String strategy : STRATEGIES)
            assertMatchesAcrossDataDirectories("big", strategy);
    }

    @Test
    public void bti()
    {
        for (String strategy : STRATEGIES)
            assertMatchesAcrossDataDirectories("bti", strategy);
    }

    private void assertMatchesAcrossDataDirectories(String format, String strategy)
    {
        String table = format + '_' + strategy.toLowerCase();
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + table + " (k int, c int, v text, PRIMARY KEY (k, c)) WITH compaction = {'class': '" + strategy + "'}");
        cluster.get(1).runOnInstance(() -> {
            String where = format + ' ' + strategy + ": ";
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            cfs.disableAutoCompaction();
            // three overlapping flushes
            for (int round = 0; round < 3; round++)
            {
                for (int k = round * 50; k < 400; k++)
                    for (int c = 0; c < 4; c++)
                        org.apache.cassandra.cql3.QueryProcessor.executeInternal("INSERT INTO " + KEYSPACE + '.' + table + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                                                                 k, c + round, "round" + round, 1000L + round);
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            }
            Set<String> directories = new HashSet<>();
            for (SSTableReader sstable : cfs.getLiveSSTables())
                directories.add(sstable.descriptor.directory.parent().toString());
            assertEquals(where + "the flushes must spread the sstables over both data directories", 2, directories.size());

            RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(FBUtilities.getBroadcastAddressAndPort());
            builder.add(Replica.fullReplica(FBUtilities.getBroadcastAddressAndPort(), range(Long.MIN_VALUE / 2, Long.MIN_VALUE / 4)));
            builder.add(Replica.fullReplica(FBUtilities.getBroadcastAddressAndPort(), range(Long.MAX_VALUE / 4, Long.MAX_VALUE / 2)));
            builder.add(Replica.transientReplica(FBUtilities.getBroadcastAddressAndPort(), range(0, Long.MAX_VALUE / 8)));
            RangesAtEndpoint ranges = builder.build();

            long groups = cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(cfs.getLiveSSTables()).size();
            assertTrue(where + "the session must anticompact a group in each data directory, got " + groups, groups >= 2);
            java.nio.file.Path scratch = null;
            try
            {
                scratch = Files.createTempDirectory("anticompaction-data-directories");
                InputCopy inputs = InputCopy.copyLiveSet(cfs, scratch);

                long runsBefore = CompactionManager.cursorAnticompactionsRun.sum();
                long fallbacksBefore = CompactionManager.cursorAnticompactionFallbacks.sum();
                long writtenBefore = CompactionManager.cursorAnticompactionPartitionsWritten.sum();
                anticompact(cfs, ranges, false);
                assertEquals(where + "the legacy run took the cursor path", 0, CompactionManager.cursorAnticompactionsRun.sum() - runsBefore);
                Map<String, List<String>> legacyOutputs = outputs(cfs);

                InputCopy.restoreLiveSet(cfs, inputs, true);
                assertEquals(where + "the restored inputs group differently", groups,
                             cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(cfs.getLiveSSTables()).size());
                anticompact(cfs, ranges, true);
                assertEquals(where + "the cursor path must run once for each group", groups, CompactionManager.cursorAnticompactionsRun.sum() - runsBefore);
                assertEquals(where + "cursor anticompaction fell back", 0, CompactionManager.cursorAnticompactionFallbacks.sum() - fallbacksBefore);
                assertTrue(where + "the cursor wrote no partition", CompactionManager.cursorAnticompactionPartitionsWritten.sum() - writtenBefore > 0);
                Map<String, List<String>> cursorOutputs = outputs(cfs);

                assertEquals(where + "the outputs differ by repair state", legacyOutputs.keySet(), cursorOutputs.keySet());
                assertEquals(where + "the outputs differ", legacyOutputs, cursorOutputs);
                org.apache.cassandra.Util.assertOnDiskState(cfs, cfs.getLiveSSTables().size());
                AntiCompactionAssertions.assertEveryFileBelongsToALiveSSTable(cfs);
            }
            catch (java.io.IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                DatabaseDescriptor.setCursorRepairEnabled(true);
                if (scratch != null)
                    org.apache.cassandra.io.util.FileUtils.deleteRecursive(new org.apache.cassandra.io.util.File(scratch));
            }
        });
    }

    private static Range<Token> range(long left, long right)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }

    /** Anticompacts the live set in a session of its own, on one path. */
    private static void anticompact(ColumnFamilyStore cfs, RangesAtEndpoint ranges, boolean cursor)
    {
        DatabaseDescriptor.setCursorRepairEnabled(cursor);
        TimeUUID session = TimeUUID.Generator.nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(session, FBUtilities.getBroadcastAddressAndPort(), List.of(cfs),
                                                                   ImmutableSet.copyOf(ranges.ranges()), true, ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                   true, PreviewKind.NONE);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
        try
        {
            CompactionManager.instance.performAnticompaction(cfs, ranges, Refs.ref(sstables), txn, session, () -> false);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            ActiveRepairService.instance().removeParentRepairSession(session);
        }
    }

    /**
     * For each repair state, the data bytes and logical content of the live sstables in it,
     * sorted, so two tables written alike compare equal.
     */
    private static Map<String, List<String>> outputs(ColumnFamilyStore cfs)
    {
        Map<String, List<String>> outputs = new TreeMap<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            String key = "pending=" + sstable.isPendingRepair() + "|transient=" + sstable.isTransient() + "|repaired=" + sstable.isRepaired();
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                ByteArrayOutputStream json = new ByteArrayOutputStream();
                JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, sstable.metadata(), json);
                byte[] data = Files.readAllBytes(sstable.descriptor.fileFor(SSTableFormat.Components.DATA).toPath());
                outputs.computeIfAbsent(key, k -> new ArrayList<>())
                       .add(java.util.Arrays.hashCode(data) + ":" + data.length + ":" + json.toString(StandardCharsets.UTF_8));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        outputs.values().forEach(list -> list.sort(String::compareTo));
        return outputs;
    }
}
