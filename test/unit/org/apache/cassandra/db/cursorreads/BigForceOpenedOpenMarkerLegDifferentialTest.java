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
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Locks the force-open pre-sort guard for the branch a force-opened leg lands on when its FIRST
 * unfiltered is an OPEN range-tombstone bound marker: it is opened past its header at
 * {@code UNFILTERED_END} (state 128), not {@code CELL_HEADER_START}. The other force-open branch (a
 * row-with-cells landing at {@code CELL_HEADER_START}) is already covered by the merge-seek suites;
 * this exercises the range-tombstone-marker branch that had no coverage.
 *
 * The force-open at merge setup is BIG-format only and needs a primed key cache: the per-row
 * key-cache lower bound ({@code KeyCacheSupport}, which {@code BigTableReader} supplies and
 * {@code BtiTableReader} does not) lets a leg sit deferred yet span the merge start, which is the
 * only shape that reaches the force-open call site in {@code CursorReads.mergeLegs}. The fixture
 * therefore pins BIG, promotes an on-heap row index ({@code column_index_cache_size = 0}), shrinks
 * index blocks so the partition is multi-block/indexed, enables the key cache, and warms it with a
 * point read before the differential.
 *
 * The force-open leg's first on-disk unfiltered is an open range-tombstone bound at the partition's
 * lowest clustering. Its base rows under the range delete are dropped at flush by the memtable's own
 * deletion filter (they predate the delete); resurrections written OVER the delete (later timestamp)
 * survive and fill the tombstone interior, so the floor of the partition is the open marker itself
 * rather than a leading row. Correctness is proved by {@link CursorReadDifferentialTester}: the
 * cursor path must match the iterator path record-for-record and byte-for-byte. The scenario also
 * asserts {@link CursorReads#sstableLegsForceOpened} advanced, so it cannot silently stop exercising
 * the force-open path.
 */
public class BigForceOpenedOpenMarkerLegDifferentialTest extends CursorReadDifferentialTester
{
    /** Wide enough, with 1KiB blocks, that each leg spans many row-index blocks (is indexed). */
    private static final int WIDE_ROWS = 2000;
    /** The range delete's exclusive upper bound: it opens at ck=0 and closes below the tail rows. */
    private static final long RT_CLOSE = 1900L;
    /** Rows resurrected OVER the delete (later timestamp), so the tombstone interior is not empty. */
    private static final long RESURRECT_TO = 1600L;
    /** A key cache capacity the fixture's entries fit inside. */
    private static final long KEY_CACHE_CAPACITY_BYTES = 1L << 20;
    /** Big enough that the whole promoted row index is held ON HEAP in the cached entry, so
     *  {@code BigTableReader.getLowerBoundPrefixFromCache} can read its first block's lower bound. */
    private static final int COLUMN_INDEX_CACHE_SIZE_KIB = 100 * 1024;

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexCacheSize;
    private int originalColumnIndexSize;
    private long originalKeyCacheCapacity;

    @Before
    public void pinBigAndPrimeCacheSetup()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("big");
        // Hold the whole promoted row index ON HEAP in the cached entry, so the per-row key-cache
        // lower bound is available (an off-heap shallow entry would return null).
        originalColumnIndexCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        DatabaseDescriptor.setColumnIndexCacheSize(COLUMN_INDEX_CACHE_SIZE_KIB);
        // Small blocks, so the wide partition spans many blocks and its entry is indexed.
        originalColumnIndexSize = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        // Enable the key cache regardless of yaml; the force-open relies on what it holds.
        originalKeyCacheCapacity = CacheService.instance.keyCache.getCapacity();
        if (originalKeyCacheCapacity == 0)
            CacheService.instance.keyCache.setCapacity(KEY_CACHE_CAPACITY_BYTES);
        CacheService.instance.invalidateKeyCache();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setColumnIndexCacheSize(originalColumnIndexCacheSize);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSize);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        CacheService.instance.keyCache.setCapacity(originalKeyCacheCapacity);
        CacheService.instance.invalidateKeyCache();
    }

    /**
     * Two overlapping BIG legs on one wide partition. The lower-timestamp leg (leg B) contributes a
     * leading row; the force-open target (leg A) leads with an open range-tombstone bound. With the
     * key cache primed both legs are deferred yet span the merge start, so both are force-opened at
     * merge setup: leg A lands at {@code UNFILTERED_END} (state 128), the branch under test.
     */
    @Test
    public void forceOpenedLegLeadingWithOpenRangeTombstoneMarker() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // leg B: older rows across the whole partition (shadowed by leg A's newer data), so the
        // merge core runs with a second candidate whose first unfiltered is an ordinary row.
        for (long ck = 0; ck < WIDE_ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 500", 1L, ck, "b" + ck);
        flush();

        // leg A: base rows, then a range delete opening at ck=0 (its open bound is the partition's
        // first unfiltered), then resurrections written OVER the delete so the interior is not
        // emptied at flush. Rows [RESURRECT_TO, RT_CLOSE) merge to empty; [RT_CLOSE, WIDE_ROWS)
        // survive at the base timestamp outside the tombstone.
        for (long ck = 0; ck < WIDE_ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "a" + ck);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 0L, RT_CLOSE);
        for (long ck = 0; ck < RESURRECT_TO; ck++)
            execute("UPDATE %s USING TIMESTAMP 3000 SET v = ? WHERE pk = ? AND ck = ?", "r" + ck, 1L, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        // Warm the key cache: a point read populates the on-heap row index entry for pk=1 in every
        // leg, which is what makes both legs deferred-yet-spanning and thus force-opened.
        execute("SELECT * FROM %s WHERE pk = 1");

        long now = FBUtilities.nowInSeconds();
        long forceOpenedBefore = CursorReads.sstableLegsForceOpened();
        long mergesBefore = CursorReads.cursorMergesServed();

        // slice start at ck=0 (non-BOTTOM) is at/after the legs' data start, so a deferred leg spans
        // the merge start and is force-opened.
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(0L).toIncl((long) WIDE_ROWS).build());

        assertTrue("the cursor-level merge core did not run; the scenario proves nothing about the "
                   + "force-open pre-sort guard",
                   CursorReads.cursorMergesServed() - mergesBefore > 0);
        assertTrue("no leg was force-opened; the force-open path (and its UNFILTERED_END pre-sort "
                   + "branch) was never exercised. force-open delta="
                   + (CursorReads.sstableLegsForceOpened() - forceOpenedBefore),
                   CursorReads.sstableLegsForceOpened() - forceOpenedBefore > 0);
    }
}
