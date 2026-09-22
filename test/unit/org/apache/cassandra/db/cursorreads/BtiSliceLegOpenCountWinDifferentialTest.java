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

package org.apache.cassandra.db.cursorreads;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Locks the b1 forward-slice lazy leg-open win, and its safety boundary, at the differential level
 * under BTI -- so the perf contract does not rest only on the config-gated count-only
 * {@code SSTablesIteratedTest}.
 *
 * The scenarios build several overlapping-candidate sstable legs (all intersect the queried slice)
 * whose covered clustering ranges are disjoint and ascending, so a deferred leg is opened only when
 * the merge actually reaches its data.
 *
 * <ul>
 *   <li>The count-win: a limit-bounded slice over N such legs opens strictly fewer than N legs
 *       ({@link CursorReads#sstableLegsServed}), because the limit stops the merge before it reaches
 *       the higher legs' data.</li>
 *   <li>The negative boundary guard: a deferred leg that begins strictly after the slice start, and
 *       carries a range tombstone that also opens strictly after the slice start, is NOT force-opened
 *       at merge setup ({@link CursorReads#sstableLegsForceOpened} stays zero) -- and the result is
 *       still byte-identical to the iterator path, proving the tombstone is applied lazily when the
 *       merge reaches it rather than being dropped.</li>
 * </ul>
 *
 * On BTI the force-open counter is always zero (a BTI leg cannot be both deferred and spanning the
 * merge start; see the force-open call site in {@code CursorReads.mergeLegs}).  This class pins BTI
 * so the guard runs on the format we prioritize.
 */
public class BtiSliceLegOpenCountWinDifferentialTest extends CursorReadDifferentialTester
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void pinBtiFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /**
     * Four overlapping-candidate legs with disjoint ascending clustering ranges.  A full-partition
     * read with {@code LIMIT 1} produces only the lowest row, so the merge opens only the lowest
     * leg and the other three stay deferred and uncounted.
     */
    @Test
    public void limitBoundedSliceOpensFewerLegsThanCandidates() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        final int legs = 4;
        for (int leg = 0; leg < legs; leg++)
        {
            long base = leg * 100L;
            for (long ck = base; ck < base + 50; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "v" + ck);
            flush();
        }
        assertEquals(legs, cfs.getLiveSSTables().size());

        // correctness first: the full (unbounded) read is byte-identical on both paths
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).build());

        // the win: a single LIMIT 1 cursor read opens strictly fewer legs than the candidate count
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long servedBefore = CursorReads.sstableLegsServed();
            assertEquals(1, execute("SELECT * FROM %s WHERE pk = 1 LIMIT 1").size());
            long served = CursorReads.sstableLegsServed() - servedBefore;
            assertTrue("LIMIT 1 over " + legs + " overlapping-candidate legs opened " + served
                       + " legs; the lazy leg-open win did not engage (expected < " + legs + ")",
                       served < legs);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /**
     * A deferred leg beginning strictly after the slice start, carrying a range tombstone that also
     * opens strictly after the slice start, must not be force-opened at merge setup.  The tombstone
     * is applied lazily when the merge reaches it, so the result stays byte-identical while the
     * force-open counter stays zero.
     */
    @Test
    public void deferredLegWithLaterRangeTombstoneIsNotForceOpened() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // low leg: ck in [0, 50) -- covers the slice start
        for (long ck = 0; ck < 50; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "lo" + ck);
        flush();

        // high leg: ck in [100, 200) with a range tombstone opening at ck=100 (strictly after the
        // slice start), so the deferred high leg neither spans the merge start nor needs seeding
        for (long ck = 100; ck < 200; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "hi" + ck);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 100L, 150L);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        // slice [0, 200] intersects both legs, so the high leg is a deferred candidate whose covered
        // range begins strictly after the slice start
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(0L).toIncl(200L).build());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long forceOpenedBefore = CursorReads.sstableLegsForceOpened();
            execute("SELECT * FROM %s WHERE pk = 1 AND ck >= 0 AND ck <= 200");
            assertEquals("a deferred leg beginning strictly after the slice start was force-opened",
                         0, CursorReads.sstableLegsForceOpened() - forceOpenedBefore);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }
}
