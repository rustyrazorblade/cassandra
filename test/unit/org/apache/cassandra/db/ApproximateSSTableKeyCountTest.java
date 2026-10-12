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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import com.clearspring.analytics.stream.cardinality.ICardinality;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link ColumnFamilyStore#getApproximateSSTableKeyCount()} memoises against the sstable set, which is only
 * valid because sstables are immutable. These tests pin both halves of that: the memoised value always agrees
 * with recomputing from scratch, and it is recomputed whenever the sstable set changes.
 */
public class ApproximateSSTableKeyCountTest extends CQLTester
{
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

        // Repeated reads with no sstable change must return the same value.
        for (int i = 0; i < 5; i++)
            assertEquals(first, cfs.getApproximateSSTableKeyCount());
    }

    @Test
    public void testMemtableWritesDoNotStaleTheCache() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();
        long afterFlush = cfs.getApproximateSSTableKeyCount();

        // Writes that stay in the memtable do not change the sstable set, so the sstable half must not move.
        insert(500, 1000);
        assertEquals(afterFlush, cfs.getApproximateSSTableKeyCount());
        assertEquals(uncached(cfs), cfs.getApproximateSSTableKeyCount());
    }

    @Test
    public void testInvalidatedByFlush() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();
        long afterFirstFlush = cfs.getApproximateSSTableKeyCount();

        insert(500, 2000);
        flush();
        long afterSecondFlush = cfs.getApproximateSSTableKeyCount();

        assertNotEquals("a second flush must invalidate the memoised count", afterFirstFlush, afterSecondFlush);
        assertEquals(uncached(cfs), afterSecondFlush);
        assertTrue(afterSecondFlush > afterFirstFlush);
    }

    @Test
    public void testInvalidatedByCompaction() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();
        insert(500, 1000);
        flush();

        long beforeCompaction = cfs.getApproximateSSTableKeyCount();
        assertEquals(uncached(cfs), beforeCompaction);
        assertTrue(cfs.getLiveSSTables().size() > 1);

        compact();

        assertEquals(1, cfs.getLiveSSTables().size());
        assertEquals("compaction must invalidate the memoised count", uncached(cfs), cfs.getApproximateSSTableKeyCount());
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
        // Left in the memtable on purpose: the gauge adds the live memtable count to the memoised sstable half.
        insert(500, 750);

        long memtablePartitions = 0;
        for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
            memtablePartitions += memtable.partitionCount();

        assertEquals(uncached(cfs) + memtablePartitions,
                     cfs.metric.estimatedPartitionCount.getValue().longValue());
    }
}
