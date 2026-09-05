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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;

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
    /** Recomputes the value the way the gauge did before it was memoised. */
    private static long uncached(ColumnFamilyStore cfs)
    {
        try (ColumnFamilyStore.RefViewFragment fragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            return SSTableReader.getApproximateKeyCount(fragment.sstables);
        }
    }

    private void insert(int from, int to)
    {
        for (int i = from; i < to; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
    }

    @Test
    public void testNoSSTablesReturnsMinusOne()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertEquals(-1, cfs.getApproximateSSTableKeyCount());
        assertEquals(uncached(cfs), cfs.getApproximateSSTableKeyCount());
    }

    @Test
    public void testMatchesUncachedAndIsStableBetweenChanges()
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
    public void testMemtableWritesDoNotStaleTheCache()
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
    public void testInvalidatedByFlush()
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
    public void testInvalidatedByCompaction()
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
    public void testGaugeAgreesWithUncachedComputation()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        insert(0, 500);
        flush();
        // Left in the memtable on purpose: the gauge adds the live memtable count to the memoised sstable half.
        insert(500, 750);

        long memtablePartitions = 0;
        for (org.apache.cassandra.db.memtable.Memtable memtable : cfs.getTracker().getView().getAllMemtables())
            memtablePartitions += memtable.partitionCount();

        assertEquals(uncached(cfs) + memtablePartitions,
                     cfs.metric.estimatedPartitionCount.getValue().longValue());
    }
}
