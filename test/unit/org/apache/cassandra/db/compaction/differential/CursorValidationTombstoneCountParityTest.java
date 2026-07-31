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
package org.apache.cassandra.db.compaction.differential;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.DigestingCursorMergeSink;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.repair.ValidationCompactionController;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Parity coverage for the cursor validation path's pre-purge top-partitions-by-tombstones count
 * (CASSANDRA-21452, N2 rework): the count {@link CursorCompactor#mergeNextPartition} feeds a
 * {@link TopPartitionTracker.Collector} must equal, per partition, what the legacy
 * {@link TopPartitionTracker.TombstoneCounter} produces when applied to the merged
 * ({@link UnfilteredPartitionIterators#merge}) stream before the purger runs - exactly how
 * {@code CompactionIterator} wires it on the legacy validation path. This is the pre-purge
 * behaviour: a gc-purgeable tombstone is still counted (only shadowing removes it), so the two
 * paths agree even when {@code gc_grace_seconds=0} would let the purger drop the tombstones.
 */
public class CursorValidationTombstoneCountParityTest extends CQLTester
{
    // gc_grace_seconds=0 tables plus a far-future nowInSec make every tombstone purgeable - the
    // scenario that used to diverge (the old sink counted post-purge survivors). Pre-purge counting
    // must still match legacy here.
    private static long farFutureNowInSec()
    {
        return FBUtilities.nowInSeconds() + java.util.concurrent.TimeUnit.DAYS.toSeconds(30);
    }

    @Test
    public void fullyDeletedPartitionCellTombstonesMatchLegacy() throws Throwable
    {
        // The exact scenario used to demonstrate the divergence: a partition whose every cell is
        // individually deleted (100 cell tombstones), gc_grace_seconds=0, validated after grace.
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 100; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "v" + ck);
        flush();
        for (long ck = 0; ck < 100; ck++)
            execute("DELETE v FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        flush();

        Map<DecoratedKey, Long> legacy = legacyCounts(cfs, farFutureNowInSec());
        Map<DecoratedKey, Long> cursor = cursorCounts(cfs, farFutureNowInSec());
        assertFalse("test must actually produce tracked tombstones", legacy.isEmpty());
        assertEquals(legacy, cursor);
    }

    @Test
    public void mixedTombstoneShapesMatchLegacy() throws Throwable
    {
        // Cell tombstones, row deletions, range tombstones, a partition deletion (fully-purged
        // partition), and a boundary marker - all purgeable (gc_grace=0), validated far in the
        // future. Pre-purge counts must still match legacy TombstoneCounter across all shapes.
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, w text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 12; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?)", pk, ck, "v" + ck, "w" + ck);
        flush();

        for (long pk = 0; pk < 12; pk++)
        {
            execute("DELETE v FROM %s WHERE pk = ? AND ck = ?", pk, 0L);          // cell tombstone
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, 1L);            // row deletion
            execute("DELETE FROM %s WHERE pk = ? AND ck >= 3 AND ck <= 10", pk);  // range tombstone
        }
        execute("DELETE FROM %s WHERE pk = ?", 3L);                               // partition deletion (fully purged)
        flush();
        for (long pk = 0; pk < 12; pk++)                                          // overlapping RT -> boundary on merge
            execute("DELETE FROM %s WHERE pk = ? AND ck >= 7 AND ck <= 14", pk);
        flush();

        Map<DecoratedKey, Long> legacy = legacyCounts(cfs, farFutureNowInSec());
        Map<DecoratedKey, Long> cursor = cursorCounts(cfs, farFutureNowInSec());
        assertFalse("test must actually produce tracked tombstones", legacy.isEmpty());
        assertEquals(legacy, cursor);
    }

    @Test
    public void ttlAndCounterTombstonesMatchLegacy() throws Throwable
    {
        // Expired TTL cells (must NOT be counted as tombstones pre-purge - legacy still sees them as
        // expiring) alongside real counter-column tombstones. gc_grace large so nothing purges.
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 8; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", ck + 1, pk, ck);
        flush();
        for (long pk = 0; pk < 8; pk++)
            for (long ck = 0; ck < 6; ck++)
                if (ck % 2 == 0)
                    execute("DELETE c FROM %s WHERE pk = ? AND ck = ?", pk, ck);   // counter-column tombstones
        flush();

        Map<DecoratedKey, Long> legacy = legacyCounts(cfs, farFutureNowInSec());
        Map<DecoratedKey, Long> cursor = cursorCounts(cfs, farFutureNowInSec());
        assertFalse("test must actually produce tracked tombstones", legacy.isEmpty());
        assertEquals(legacy, cursor);
    }

    /**
     * Legacy per-partition tombstone counts: the real {@link TopPartitionTracker.TombstoneCounter}
     * applied to the merged, pre-purge stream, exactly as {@code CompactionIterator} wires it before
     * {@code GarbageSkipper}/{@code Purger}.
     */
    private Map<DecoratedKey, Long> legacyCounts(ColumnFamilyStore cfs, long nowInSec) throws Exception
    {
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        long gcBefore = cfs.getDefaultGcBefore(nowInSec);
        try (WideThresholds ignored = new WideThresholds();
             ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            TopPartitionTracker.Collector collector = new TopPartitionTracker.Collector(Collections.emptyList());
            AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
            try (UnfilteredPartitionIterator merged =
                     Transformation.apply(UnfilteredPartitionIterators.merge(scanners.scanners, null),
                                           new TopPartitionTracker.TombstoneCounter(collector, nowInSec)))
            {
                while (merged.hasNext())
                {
                    try (UnfilteredRowIterator partition = merged.next())
                    {
                        while (partition.hasNext())
                            partition.next();
                    }
                }
            }
            finally
            {
                scanners.close();
            }
            return counts(collector);
        }
    }

    private Map<DecoratedKey, Long> cursorCounts(ColumnFamilyStore cfs, long nowInSec) throws Exception
    {
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        long gcBefore = cfs.getDefaultGcBefore(nowInSec);
        try (WideThresholds ignored = new WideThresholds();
             ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            assertTrue("cursor validation must be supported here or the parity check is vacuous",
                       CursorCompactor.isValidationSupported(sstables, controller));

            TopPartitionTracker.Collector collector = new TopPartitionTracker.Collector(Collections.emptyList());
            Map<SSTableReader, List<PartitionPositionBounds>> boundsBySSTable = new HashMap<>();
            for (SSTableReader sstable : sstables)
                boundsBySSTable.put(sstable, Collections.singletonList(sstable.getPositionsForFullRange()));

            DigestingCursorMergeSink sink = new DigestingCursorMergeSink(cfs.metadata());
            CursorCompactor compactor = new CursorCompactor(OperationType.VALIDATION, boundsBySSTable, controller,
                                                            nowInSec, nextTimeUUID());
            compactor.setTopPartitionCollector(collector);
            try
            {
                while (compactor.mergeNextPartition(sink))
                    sink.takePartitionDigest();
            }
            finally
            {
                compactor.close();
            }
            return counts(collector);
        }
    }

    private static Map<DecoratedKey, Long> counts(TopPartitionTracker.Collector collector) throws Exception
    {
        Field f = TopPartitionTracker.Collector.class.getDeclaredField("tombstones");
        f.setAccessible(true);
        TopPartitionTracker.TopHolder holder = (TopPartitionTracker.TopHolder) f.get(collector);
        Map<DecoratedKey, Long> counts = new HashMap<>();
        for (TopPartitionTracker.TopPartition tp : holder.top)
            counts.put(tp.key, tp.value);
        return counts;
    }

    /**
     * Widens the top-partition tracking thresholds (min tracked = 1, cap effectively unbounded) so
     * that every partition with at least one tombstone is retained for an exact per-partition
     * comparison, restoring the previous values on close.
     */
    private static final class WideThresholds implements AutoCloseable
    {
        private final long prevMin = DatabaseDescriptor.getMinTrackedPartitionTombstoneCount();
        private final int prevMax = DatabaseDescriptor.getMaxTopTombstonePartitionCount();

        WideThresholds()
        {
            DatabaseDescriptor.setMinTrackedPartitionTombstoneCount(1);
            DatabaseDescriptor.setMaxTopTombstonePartitionCount(1_000_000);
        }

        @Override
        public void close()
        {
            DatabaseDescriptor.setMinTrackedPartitionTombstoneCount(prevMin);
            DatabaseDescriptor.setMaxTopTombstonePartitionCount(prevMax);
        }
    }
}
