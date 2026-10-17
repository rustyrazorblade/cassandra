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

package org.apache.cassandra.db;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link ColumnFamilyStore#getApproximateSSTableKeyCount()} caches its value until the tracker reports a change
 * to the sstables. These tests check that the cached value is reused, that every change to the sstables clears
 * it, and that a change during a compute does not leave a stale value cached.
 */
@RunWith(BMUnitRunner.class)
public class ApproximateSSTableKeyCountTest extends CQLTester
{
    /** Run once by the Byteman rules below, then cleared. */
    static volatile Runnable duringKeyCount;
    static volatile Runnable beforeUnmarkCompacting;

    public static void runDuringKeyCount()
    {
        Runnable hook = duringKeyCount;
        duringKeyCount = null;
        if (hook != null)
            hook.run();
    }

    public static void runBeforeUnmarkCompacting()
    {
        Runnable hook = beforeUnmarkCompacting;
        beforeUnmarkCompacting = null;
        if (hook != null)
            hook.run();
    }

    @After
    public void clearHooks()
    {
        duringKeyCount = null;
        beforeUnmarkCompacting = null;
    }

    /**
     * Recomputes the value the way the gauge did before this patch: load every component of each sstable's
     * Statistics.db and fold the estimators with {@code merge()}. It does not call the patched code.
     */
    private static long uncached(ColumnFamilyStore cfs) throws Exception
    {
        try (ColumnFamilyStore.RefViewFragment fragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            if (fragment.sstables.isEmpty())
                return -1;

            ICardinality merged = null;
            for (SSTableReader sstable : fragment.sstables)
            {
                ICardinality estimator = StatsComponent.load(sstable.descriptor).compactionMetadata().cardinalityEstimator;
                merged = merged == null ? estimator : merged.merge(estimator);
            }
            return merged.cardinality();
        }
    }

    /** True if the serialized estimator is in sparse form. The format tag follows the version int, p and sp. */
    private static boolean isSparse(SSTableReader sstable) throws Exception
    {
        byte[] bytes = StatsComponent.load(sstable.descriptor).compactionMetadata().cardinalityEstimator.getBytes();
        return bytes[6] == 1;
    }

    /** Rewrites the sstable's Statistics.db so its estimator holds {@code distinct} made-up keys. */
    private static void rewriteEstimator(SSTableReader sstable, int distinct) throws Exception
    {
        HyperLogLogPlus estimator = new HyperLogLogPlus(13, 25);
        for (long i = 0; i < distinct; i++)
            estimator.offer(Long.MAX_VALUE - i);

        Map<MetadataType, MetadataComponent> components =
            sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, EnumSet.allOf(MetadataType.class));
        components.put(MetadataType.COMPACTION, new CompactionMetadata(estimator));
        sstable.descriptor.getMetadataSerializer().rewriteSSTableMetadata(sstable.descriptor, components);
    }

    private void insert(int from, int to)
    {
        for (int i = from; i < to; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
    }

    @Test
    public void testNoSSTablesReturnsMinusOne() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertEquals(-1, cfs.getApproximateSSTableKeyCount());
        assertEquals(uncached(cfs), cfs.getApproximateSSTableKeyCount());
    }

    @Test
    public void testMatchesUncachedAndIsStableBetweenChanges() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();

        long first = cfs.getApproximateSSTableKeyCount();
        assertTrue("expected a positive estimate, got " + first, first > 0);
        assertEquals(uncached(cfs), first);

        for (int i = 0; i < 5; i++)
            assertEquals(first, cfs.getApproximateSSTableKeyCount());
    }

    @Test
    public void testCachedValueIsReusedWithoutRecomputing() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();
        long cached = cfs.getApproximateSSTableKeyCount();

        // Change the estimator on disk without telling the tracker. A recompute would now see about 100000
        // keys, so getting the old value back shows the cached value was used.
        rewriteEstimator(cfs.getLiveSSTables().iterator().next(), 100000);
        long recomputed = SSTableReader.getApproximateKeyCount(cfs.getLiveSSTables());
        assertTrue("recomputed " + recomputed + " should be far above " + cached, recomputed > cached * 10);

        assertEquals(cached, cfs.getApproximateSSTableKeyCount());
    }

    @Test
    public void testMemtableWritesAreSeenWithoutClearingTheCache() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();
        long sstableCount = cfs.getApproximateSSTableKeyCount();
        long gaugeBefore = cfs.metric.estimatedPartitionCount.getValue();

        // Writes that stay in the memtable do not change the sstables, so the sstable count stays cached,
        // while the gauge still adds the live memtable count on every read.
        insert(500, 1000);
        assertEquals(sstableCount, cfs.getApproximateSSTableKeyCount());
        assertEquals(uncached(cfs), sstableCount);
        long gaugeAfter = cfs.metric.estimatedPartitionCount.getValue();
        assertEquals(gaugeBefore + 500, gaugeAfter);
    }

    @Test
    public void testClearedByFlush() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();
        long afterFirstFlush = cfs.getApproximateSSTableKeyCount();

        insert(500, 2000);
        flush();
        long afterSecondFlush = cfs.getApproximateSSTableKeyCount();

        assertEquals(uncached(cfs), afterSecondFlush);
        assertTrue(afterSecondFlush > afterFirstFlush);
    }

    @Test
    public void testClearedByCompaction() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        insert(0, 500);
        flush();
        insert(500, 1000);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        // Give one sstable an estimator with extra made-up keys, then cache the count. Compaction writes a
        // new estimator from the real keys, so the count must drop afterwards.
        rewriteEstimator(cfs.getLiveSSTables().iterator().next(), 100000);
        long beforeCompaction = cfs.getApproximateSSTableKeyCount();
        assertEquals(uncached(cfs), beforeCompaction);

        compact();

        assertEquals(1, cfs.getLiveSSTables().size());
        long afterCompaction = cfs.getApproximateSSTableKeyCount();
        assertEquals(uncached(cfs), afterCompaction);
        assertTrue(afterCompaction < beforeCompaction);
    }

    @Test
    public void testClearedByTruncate() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();
        assertTrue(cfs.getApproximateSSTableKeyCount() > 0);

        cfs.truncateBlocking();

        assertEquals(-1, cfs.getApproximateSSTableKeyCount());
    }

    @Test
    public void testClearedByImport() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Write an sstable and move it out of the table, to import it again later.
        insert(500, 2000);
        flush();
        Set<SSTableReader> toImport = new HashSet<>(cfs.getLiveSSTables());
        cfs.clearUnsafe();
        File importDir = moveToImportDir(toImport);

        insert(0, 500);
        flush();
        long beforeImport = cfs.getApproximateSSTableKeyCount();
        assertEquals(uncached(cfs), beforeImport);

        SSTableImporter importer = new SSTableImporter(cfs);
        assertTrue(importer.importNewSSTables(SSTableImporter.Options.options(importDir.toString()).build()).isEmpty());
        assertEquals(2, cfs.getLiveSSTables().size());

        long afterImport = cfs.getApproximateSSTableKeyCount();
        assertEquals(uncached(cfs), afterImport);
        assertTrue(afterImport > beforeImport);
    }

    private static File moveToImportDir(Set<SSTableReader> sstables) throws Exception
    {
        SSTableReader first = sstables.iterator().next();
        Path temp = Files.createTempDirectory("keycounttest");
        Path dir = temp.resolve(first.descriptor.directory.parent().name()).resolve(first.descriptor.directory.name());
        Files.createDirectories(dir);
        for (SSTableReader sstable : sstables)
        {
            sstable.selfRef().release();
            for (File f : sstable.descriptor.directory.tryList())
                if (f.toString().contains(sstable.descriptor.baseFile().toString()))
                    Files.move(f.toPath(), dir.resolve(f.name()));
        }
        PathUtils.deleteRecursiveOnExit(temp);
        return new File(dir);
    }

    @Test
    @BMRule(name = "flush while the key count is computed",
            targetClass = "org.apache.cassandra.io.sstable.format.SSTableReader",
            targetMethod = "getApproximateKeyCount",
            targetLocation = "AT ENTRY",
            action = "org.apache.cassandra.db.ApproximateSSTableKeyCountTest.runDuringKeyCount();")
    public void testFlushDuringComputeDoesNotCacheStaleValue() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        insert(0, 500);
        flush();

        // The compute has already chosen its sstables when this flush adds a new one.
        duringKeyCount = () -> {
            insert(500, 2000);
            flush();
        };
        long raced = cfs.getApproximateSSTableKeyCount();
        assertNull("the hook did not run", duringKeyCount);
        assertEquals(2, cfs.getLiveSSTables().size());

        long next = cfs.getApproximateSSTableKeyCount();
        assertEquals(uncached(cfs), next);
        assertTrue("the value from before the flush was cached: " + raced + " vs " + next, next > raced);
    }

    @Test
    @BMRule(name = "read the key count after a compaction commits and before it unmarks its sstables",
            targetClass = "org.apache.cassandra.db.lifecycle.LifecycleTransaction",
            targetMethod = "doPostCleanup",
            targetLocation = "AT ENTRY",
            action = "org.apache.cassandra.db.ApproximateSSTableKeyCountTest.runBeforeUnmarkCompacting();")
    public void testCompactedSSTablesAreNotCachedBeforeUnmark() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        insert(0, 500);
        flush();
        insert(0, 500);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        rewriteEstimator(cfs.getLiveSSTables().iterator().next(), 100000);

        // After the commit and its notification, the compacted sstables stay in the CANONICAL set until they
        // are unmarked, and unmarking sends no notification. Hold a reference to them like an in-flight read
        // would, so the key count can be read in that window.
        long[] duringWindow = { Long.MIN_VALUE };
        try (Refs<SSTableReader> refs = Refs.ref(cfs.getLiveSSTables()))
        {
            beforeUnmarkCompacting = () -> duringWindow[0] = cfs.getApproximateSSTableKeyCount();
            compact();
        }
        assertNull("the hook did not run", beforeUnmarkCompacting);
        assertEquals(1, cfs.getLiveSSTables().size());

        long after = cfs.getApproximateSSTableKeyCount();
        assertEquals(uncached(cfs), after);
        assertTrue("the window was not seen: " + duringWindow[0] + " vs " + after, duringWindow[0] > after);
    }

    @Test
    public void testFallsBackToIndexSummaryWhenEstimatorAbsent() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // An sstable whose Statistics.db carries no cardinality estimator falls back to the per-sstable
        // index-summary estimate. (CASSANDRA-10676)
        Map<MetadataType, MetadataComponent> components =
            sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, EnumSet.allOf(MetadataType.class));
        components.remove(MetadataType.COMPACTION);
        sstable.descriptor.getMetadataSerializer().rewriteSSTableMetadata(sstable.descriptor, components);

        long fallback = SSTableReader.getApproximateKeyCount(Collections.singletonList(sstable));
        assertEquals(sstable.estimatedKeys(), fallback);
    }

    @Test
    public void testMatchesOldComputationForSparseAndNormalEstimators() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Small sstables keep a sparse estimator and large ones a normal one, so the merge sees both forms and
        // both orders. Some key ranges overlap.
        int[][] ranges = { { 0, 10 }, { 0, 10000 }, { 5, 60 }, { 9000, 17000 }, { 16990, 17000 } };
        for (int[] range : ranges)
        {
            insert(range[0], range[1]);
            flush();
        }
        assertEquals(ranges.length, cfs.getLiveSSTables().size());

        int sparse = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            // the three sstables with 10, 55 and 10 keys stay sparse; the 10000 and 8000 key ones do not
            boolean small = sstable.estimatedKeys() < 1000;
            assertEquals("estimator form for " + sstable, small, isSparse(sstable));
            if (small)
                sparse++;
        }
        assertEquals(3, sparse);

        long expected = uncached(cfs);
        assertTrue("expected a positive estimate, got " + expected, expected > 0);
        try (ColumnFamilyStore.RefViewFragment fragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            assertEquals(expected, SSTableReader.getApproximateKeyCount(fragment.sstables));
        }
        assertEquals(expected, cfs.getApproximateSSTableKeyCount());
    }

    @Test
    public void testGaugeAgreesWithUncachedComputation() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();
        // Left in the memtable on purpose: the gauge adds the live memtable count to the cached sstable count.
        insert(500, 750);

        long memtablePartitions = 0;
        for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
            memtablePartitions += memtable.partitionCount();

        assertEquals(uncached(cfs) + memtablePartitions,
                     cfs.metric.estimatedPartitionCount.getValue().longValue());
    }
}
