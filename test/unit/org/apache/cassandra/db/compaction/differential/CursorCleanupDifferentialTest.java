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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Cursor-vs-legacy parity for CLEANUP across the CQL data-type and tombstone shapes cursor
 * compaction supports.
 *
 * Cleanup drops data at PARTITION granularity: both paths restrict reading to the owned token
 * ranges and rewrite whatever falls inside them untouched (see
 * {@code CompactionManager.CleanupStrategy.Bounded}), so there is no row-level ownership filter to
 * test - {@link #partitionGranularity} pins that as an explicit invariant rather than an
 * assumption. What this class does test is that every supported value shape survives the
 * cursor-path rewrite byte-identically to the legacy one, and that partitions outside the owned
 * ranges are dropped identically by both.
 */
public class CursorCleanupDifferentialTest extends CursorCleanupDifferentialTester
{
    /** Owns the lower half of the partitions present, dropping the upper half. */
    private Collection<Range<Token>> ownLowerHalf(ColumnFamilyStore cfs)
    {
        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        assertTrue("scenario needs at least 4 partitions to split", keys.size() >= 4);
        return rangesOwning(keys, keys.subList(0, keys.size() / 2));
    }

    @Test
    public void simpleTable() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 20; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "v" + pk + '-' + ck);
        flush();

        CleanupOutcome out = assertCursorCleanupMatchesLegacy(cfs, ownLowerHalf(cfs));
        assertEquals("cleanup should have produced one surviving sstable", 1, out.captured.sstables.size());
    }

    /**
     * Cleanup keeps or drops whole partitions, never individual clustering rows: a range whose
     * right bound is a partition's own token still yields every row of that partition, and a
     * partition just outside contributes none. If cursor cleanup ever grew a row-level filter this
     * fails, and so would the byte comparison in every other scenario here.
     */
    @Test
    public void partitionGranularity() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 8; pk++)
            for (long ck = 0; ck < 50; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, ck);
        flush();

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        // own exactly one partition, with the range boundary landing ON its token
        DecoratedKey kept = keys.get(3);
        CleanupOutcome out = assertCursorCleanupMatchesLegacy(cfs, rangesOwning(keys, List.of(kept)));

        // that partition survived whole - all 50 rows, no partial partition - and it is the only one
        assertEquals(1, out.captured.sstables.size());
        assertEquals(List.of(kept), out.survivingKeys);
        assertEquals(50L, out.survivingRows);
    }

    @Test
    public void collections() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, " +
                    "l list<text>, s set<int>, m map<text, bigint>, " +
                    "fl frozen<list<text>>, fs frozen<set<int>>, fm frozen<map<text, bigint>>, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 12; pk++)
        {
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, l, s, m, fl, fs, fm) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        pk, ck,
                        list("a" + ck, "b" + ck), set((int) ck, (int) ck + 100), map("k" + ck, ck),
                        list("fa" + ck, "fb" + ck), set((int) ck + 7), map("fk" + ck, ck * 3));
            // partial updates so the collections carry their own complex deletions and appends
            execute("UPDATE %s SET l = l + ? WHERE pk = ? AND ck = ?", list("appended"), pk, 0L);
            execute("UPDATE %s SET s = s - ? WHERE pk = ? AND ck = ?", set(100), pk, 1L);
            execute("DELETE m['k2'] FROM %s WHERE pk = ? AND ck = ?", pk, 2L);
        }
        flush();

        assertCursorCleanupMatchesLegacy(cfs, ownLowerHalf(cfs));
    }

    @Test
    public void counters() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 12; pk++)
            for (long ck = 0; ck < 5; ck++)
            {
                execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 - ? WHERE pk = ? AND ck = ?", ck + 1, ck, pk, ck);
                execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 7L, pk, ck);
            }
        flush();

        assertCursorCleanupMatchesLegacy(cfs, ownLowerHalf(cfs));
    }

    /**
     * Static rows across the ownership boundary. The static-only partitions (no clustering rows at
     * all) are placed DETERMINISTICALLY on opposite sides of it - chosen by actual token, not by
     * hoping murmur scattered two arbitrary keys the right way - so the scenario cannot silently
     * decay into "both happened to land on the kept side".
     */
    @Test
    public void staticColumns() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, st text static, sn bigint static, v text, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        List<Long> pks = pksInTokenOrder(cfs, 14);
        // the two lowest-token and two highest-token keys become static-only partitions, so one
        // pair is certain to be owned and the other certain to be dropped by ownLowerHalf
        List<Long> staticOnly = List.of(pks.get(0), pks.get(1), pks.get(pks.size() - 2), pks.get(pks.size() - 1));
        for (long pk : pks)
        {
            execute("INSERT INTO %s (pk, st, sn) VALUES (?, ?, ?)", pk, "static-" + pk, pk * 11);
            if (staticOnly.contains(pk))
                continue;
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        }
        flush();

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        int ownedCount = keys.size() / 2;
        List<DecoratedKey> owned = keys.subList(0, ownedCount);
        assertTrue("scenario setup: the two lowest-token static-only partitions must be owned",
                   pkOf(owned.get(0)) == staticOnly.get(0) && pkOf(owned.get(1)) == staticOnly.get(1));
        assertTrue("scenario setup: the two highest-token static-only partitions must be dropped",
                   pkOf(keys.get(keys.size() - 1)) == staticOnly.get(3)
                   && pkOf(keys.get(keys.size() - 2)) == staticOnly.get(2));

        CleanupOutcome out = assertCursorCleanupMatchesLegacy(cfs, rangesOwning(keys, owned));
        assertEquals(owned, out.survivingKeys);
    }

    /**
     * A static-only partition as the HIGHEST-token owned key: the last partition written to the
     * output has no clustering rows at all, so the last-written-clustering and {@code setLast}
     * bookkeeping runs at the bounds edge on a partition whose only content is its static row.
     */
    @Test
    public void staticOnlyPartitionIsHighestOwnedKey() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, st text static, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        List<Long> pks = pksInTokenOrder(cfs, 12);
        int ownedCount = pks.size() / 2;
        long boundaryPk = pks.get(ownedCount - 1); // highest-token OWNED key: static-only
        for (long pk : pks)
        {
            execute("INSERT INTO %s (pk, st) VALUES (?, ?)", pk, "static-" + pk);
            if (pk == boundaryPk)
                continue;
            for (long ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        }
        flush();

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        List<DecoratedKey> owned = keys.subList(0, ownedCount);
        assertEquals("scenario setup: the boundary partition must be the highest owned key",
                     boundaryPk, pkOf(owned.get(owned.size() - 1)));

        CleanupOutcome out = assertCursorCleanupMatchesLegacy(cfs, rangesOwning(keys, owned));
        assertEquals(owned, out.survivingKeys);
    }

    /**
     * Disjoint owned ranges - the production norm under vnodes, where a node's ownership is many
     * scattered token ranges rather than one span. This is the only cleanup scenario that drives
     * {@code StatefulCursor}'s multi-segment path (mid-file seek between segments, gap byte
     * accounting) under a WRITING compactor; every other scenario here normalizes to a single
     * contiguous byte segment.
     */
    @Test
    public void disjointOwnedRanges() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 8; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "value-" + pk + '-' + ck);
        flush();

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        assertEquals(8, keys.size());
        // two separated islands of ownership: token-order indexes {0,1} and {5,6}, with {2,3,4}
        // dropped between them and {7} dropped after
        List<DecoratedKey> owned = List.of(keys.get(0), keys.get(1), keys.get(5), keys.get(6));
        Collection<Range<Token>> ranges = rangesOwning(keys, owned);

        SSTableReader input = cfs.getLiveSSTables().iterator().next();
        assertTrue("scenario must produce at least two disjoint byte segments, or it does not " +
                   "exercise the multi-segment cursor path at all",
                   input.getPositionsForRanges(ranges).size() >= 2);

        CleanupOutcome out = assertCursorCleanupMatchesLegacy(cfs, ranges);
        assertEquals(owned, out.survivingKeys);
        assertEquals(4 * 6L, out.survivingRows);
    }

    /**
     * The one silent cursor-to-legacy fallback: an owned range that intersects the sstable's
     * {@code [first, last]} token span - so {@code doCleanupOne} does not take its whole-sstable-drop
     * shortcut - but contains no actual key, leaving {@code getPositionsForRanges} with no byte
     * segments to hand the cursors. The fallback is load-bearing rather than cosmetic:
     * {@code StatefulCursor.positionAt} asserts its bounds are non-empty and this codebase runs tests
     * with {@code -ea}, so without the guard this scenario dies on an assertion instead of cleaning up.
     * <p>
     * Deliberately NOT a differential comparison: with the fallback taken both "paths" are the same
     * legacy code, so comparing them would prove nothing. What is asserted is the routing decision
     * itself, plus the outcome - the sstable holds nothing this node owns, so it is dropped whole.
     */
    @Test
    public void ownedRangeWithNoKeysFallsBackToLegacy() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 8; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        flush();

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        SSTableReader input = cfs.getLiveSSTables().iterator().next();

        // The single token immediately after a mid-file partition's: inside the sstable's span,
        // occupied by no key. Range is (left, right], so this owns exactly that one empty token.
        Token afterThirdKey = keys.get(2).getToken().increaseSlightly();
        Collection<Range<Token>> ranges = List.of(new Range<>(keys.get(2).getToken(), afterThirdKey));

        assertTrue("scenario setup: the range must intersect the sstable's token span, or cleanup " +
                   "drops the sstable before ever reaching the cursor/legacy routing decision",
                   input.getBounds().intersects(ranges));
        assertTrue("scenario setup: the range must resolve to NO byte segments, or this does not " +
                   "exercise the empty-bounds fallback at all",
                   input.getPositionsForRanges(ranges).isEmpty());

        boolean tookCursorPath = runCleanupWithCursorEnabled(cfs, ranges);

        assertFalse("cursor cleanup must fall back to legacy when the owned ranges resolve to no " +
                    "byte segments - StatefulCursor.positionAt asserts non-empty bounds", tookCursorPath);
        assertTrue("the sstable holds nothing this node owns, so cleanup must drop it entirely",
                   cfs.getLiveSSTables().isEmpty());
    }

    /**
     * Purgeable tombstones and surviving rows in the SAME partition, so a partition is written but
     * only after its leading content purges away - exercising the late partition/row start paths
     * under the single-output writer provider, which the whole-partition purge scenario does not.
     */
    @Test
    public void partialPartitionPurge() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        List<Long> pks = pksInTokenOrder(cfs, 12);
        for (long pk : pks)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, ck);

        long deletedAt = FBUtilities.nowInSeconds();
        for (long pk : pks)
        {
            // leading rows of every partition purge away; ck >= 5 survives
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", pk, 0L, 3L);  // range tombstone
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, 3L);                  // row tombstone
            execute("DELETE v FROM %s WHERE pk = ? AND ck = ?", pk, 4L);                // cell tombstone
        }
        flush();

        while (FBUtilities.nowInSeconds() <= deletedAt)
            Thread.sleep(50);

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        int ownedCount = keys.size() / 2;
        CleanupOutcome out = assertCursorCleanupMatchesLegacy(cfs, rangesOwning(keys, keys.subList(0, ownedCount)));

        assertEquals("every owned partition still has surviving rows", keys.subList(0, ownedCount), out.survivingKeys);
        // ck 0-2 purged with their range tombstone, ck 3 purged with its row tombstone, ck 4
        // survives as a row whose only cell was deleted, ck 5-9 untouched
        assertEquals(ownedCount * 6L, out.survivingRows);
    }

    /**
     * Genuinely EXPIRED TTL cells, which take a different serialization and purge path from
     * live-but-will-expire ones. The wait happens before the FIRST run, so both paths evaluate
     * expiry on the same side of the boundary and cannot legitimately disagree.
     */
    @Test
    public void expiredTtlCells() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, w text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        long writtenAt = FBUtilities.nowInSeconds();
        for (long pk = 0; pk < 12; pk++)
            for (long ck = 0; ck < 5; ck++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TTL 1", pk, ck, "expires" + ck);
                execute("INSERT INTO %s (pk, ck, w) VALUES (?, ?, ?)", pk, ck, "live" + ck);
            }
        flush();

        // TTL 1 means localExpirationTime == writtenAt + 1; wait past it so BOTH runs see the
        // cells as expired rather than one racing the boundary
        while (FBUtilities.nowInSeconds() <= writtenAt + 1)
            Thread.sleep(50);

        assertCursorCleanupMatchesLegacy(cfs, ownLowerHalf(cfs));
    }

    @Test
    public void tombstonesRetained() throws Exception
    {
        // large gc_grace: nothing is purgeable, so every tombstone kind must be rewritten as-is
        createTable("CREATE TABLE %s (pk bigint, ck1 bigint, ck2 text, v bigint, PRIMARY KEY (pk, ck1, ck2)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 12; pk++)
            for (long ck1 = 0; ck1 < 5; ck1++)
                for (int ck2 = 0; ck2 < 3; ck2++)
                    execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (?, ?, ?, ?)", pk, ck1, "c" + ck2, ck1);

        for (long pk = 0; pk < 12; pk += 2)
        {
            execute("DELETE FROM %s WHERE pk = ? AND ck1 = ? AND ck2 = ?", pk, 1L, "c1");   // row delete
            execute("DELETE v FROM %s WHERE pk = ? AND ck1 = ? AND ck2 = ?", pk, 2L, "c0"); // cell delete
            execute("DELETE FROM %s WHERE pk = ? AND ck1 >= ? AND ck1 < ?", pk, 3L, 5L);    // range delete
        }
        execute("DELETE FROM %s WHERE pk = ?", 7L);                                          // partition delete
        flush();

        assertCursorCleanupMatchesLegacy(cfs, ownLowerHalf(cfs));
    }

    /**
     * Purge and ownership interact: the HIGHEST-token owned partition is deleted purgeably, so the
     * merge loop writes nothing for the last partition it reads. The last key written to the output
     * is then not the last key read - exactly the bookkeeping a cursor stopped early by its range
     * bounds gets wrong if {@code StatefulCursor.readPartitionHeader} does not leave
     * {@code prevPartition} on the last partition actually read.
     */
    @Test
    public void tombstonesPurged() throws Exception
    {
        // gc_grace 0: the tombstones below are purgeable once the wall clock ticks past them
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        List<Long> pks = pksInTokenOrder(cfs, 12);
        for (long pk : pks)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, ck);

        int ownedCount = pks.size() / 2;
        List<Long> deleted = List.of(pks.get(1), pks.get(ownedCount - 1));
        long deletedAt = FBUtilities.nowInSeconds();
        for (long pk : deleted)
            execute("DELETE FROM %s WHERE pk = ?", pk);
        flush();

        // purge requires localDeletionTime < gcBefore, and gcBefore is nowInSec at gc_grace 0
        while (FBUtilities.nowInSeconds() <= deletedAt)
            Thread.sleep(50);

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        CleanupOutcome out = assertCursorCleanupMatchesLegacy(cfs, rangesOwning(keys, keys.subList(0, ownedCount)));

        List<DecoratedKey> expected = new ArrayList<>(keys.subList(0, ownedCount));
        expected.removeIf(key -> deleted.contains(LongType.instance.compose(key.getKey())));
        assertFalse("scenario must leave surviving partitions to write", expected.isEmpty());
        assertEquals("owned-but-purged partitions must be dropped by both paths", expected, out.survivingKeys);
    }

    private static long pkOf(DecoratedKey key)
    {
        return LongType.instance.compose(key.getKey());
    }

    /** The first {@code count} non-negative pk values, ordered by the token they hash to. */
    private static List<Long> pksInTokenOrder(ColumnFamilyStore cfs, int count)
    {
        List<Long> pks = new ArrayList<>();
        for (long pk = 0; pk < count; pk++)
            pks.add(pk);
        pks.sort(Comparator.comparing(pk -> cfs.getPartitioner().decorateKey(LongType.instance.decompose(pk))));
        return pks;
    }

    @Test
    public void expiringCells() throws Exception
    {
        // long TTLs only: an expiry boundary inside the test window would let the two runs (seconds
        // apart) legitimately disagree, which is a harness limitation, not a cleanup one
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, w text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 12; pk++)
            for (long ck = 0; ck < 5; ck++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TTL 864000", pk, ck, "ttl" + ck);
                execute("INSERT INTO %s (pk, ck, w) VALUES (?, ?, ?)", pk, ck, "live" + ck);
            }
        flush();

        assertCursorCleanupMatchesLegacy(cfs, ownLowerHalf(cfs));
    }

    /**
     * Multi-component clustering keys in partitions large enough to genuinely cross the
     * column-index block threshold, so the rewritten output has to rebuild a MULTI-BLOCK promoted
     * index (BIG) or a deep {@code Rows.db} trie (BTI) from a truncated key set - the machinery
     * {@link BtiCursorCleanupDifferentialTest} exists to cover. The crossing is asserted from the
     * input's own per-partition byte span, not assumed from the row count.
     */
    @Test
    public void multiComponentClusteringAndWidePartitions() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck1 text, ck2 int, ck3 bigint, v text, " +
                    "PRIMARY KEY (pk, ck1, ck2, ck3)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, ck3 ASC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // ~200 bytes of value per row x 12*8*8 rows = ~150KB per partition, comfortably past the
        // 64KiB default column_index_size
        String padding = "x".repeat(200);
        for (long pk = 0; pk < 8; pk++)
            for (int ck1 = 0; ck1 < 12; ck1++)
                for (int ck2 = 0; ck2 < 8; ck2++)
                    for (long ck3 = 0; ck3 < 8; ck3++)
                        execute("INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (?, ?, ?, ?, ?)",
                                pk, "c" + ck1, ck2, ck3, "value-" + ck1 + '-' + ck2 + '-' + ck3 + '-' + padding);
        flush();

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        SSTableReader input = cfs.getLiveSSTables().iterator().next();
        int columnIndexSize = DatabaseDescriptor.getColumnIndexSize(64 * 1024);
        // byte span of a single partition, format-neutrally: the positions of a range covering
        // exactly one key. Crossing column_index_size is what forces a multi-block index.
        List<SSTableReader.PartitionPositionBounds> onePartition =
            input.getPositionsForRanges(rangesOwning(keys, List.of(keys.get(0))));
        assertEquals(1, onePartition.size());
        long partitionBytes = onePartition.get(0).upperPosition - onePartition.get(0).lowerPosition;
        assertTrue("scenario must produce partitions past the column index block threshold " +
                   "(" + partitionBytes + " bytes vs column_index_size " + columnIndexSize + "); " +
                   "otherwise the output never builds a multi-block index and this class's premise is void",
                   partitionBytes > 2L * columnIndexSize);

        assertCursorCleanupMatchesLegacy(cfs, ownLowerHalf(cfs));
    }

    /** Cleanup that drops nothing still has to rewrite the sstable identically on both paths. */
    @Test
    public void everythingOwned() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 12; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        flush();

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        CleanupOutcome out = assertCursorCleanupMatchesLegacy(cfs, rangesOwning(keys, keys));
        assertEquals(1, out.captured.sstables.size());
        assertEquals("nothing should have been dropped", keys, out.survivingKeys);
        assertEquals(48L, out.survivingRows);
    }

    /**
     * A table with a secondary index selects {@code CleanupStrategy.Full}, which drops out-of-range
     * partitions itself and notifies the index manager of each one. That notification has no cursor
     * equivalent, so cursor cleanup must refuse the table outright rather than silently skipping
     * the notifications and leaving index entries pointing at deleted rows.
     */
    @Test
    public void indexedTableIsRejected() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s (v)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 8; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, ck);
        flush();

        assertTrue("scenario did not actually create an index", cfs.indexManager.hasIndexes());
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        try (CompactionController controller = new CompactionController(cfs, cfs.getLiveSSTables(), gcBefore))
        {
            assertFalse("cursor cleanup must not accept an indexed table: it cannot notify the index " +
                        "manager of partitions cleanup removes",
                        CursorCompactor.isCleanupSupported(cfs.getLiveSSTables(), controller));
        }
    }

    /**
     * Cursor cleanup must not be reachable for a table whose compaction params enable overlapping
     * tombstone sources: the legacy path applies {@code CompactionIterator.GarbageSkipper} there
     * and the cursor merge loop has no equivalent.
     */
    @Test
    public void overlappingTombstonesTableIsRejected() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH compaction = {'class': 'SizeTieredCompactionStrategy', " +
                    "'provide_overlapping_tombstones': 'row'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 8; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, ck);
        flush();

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        try (CompactionController controller = new CompactionController(cfs, cfs.getLiveSSTables(), gcBefore))
        {
            assertFalse("cursor cleanup must not accept a table with overlapping tombstone sources",
                        CursorCompactor.isCleanupSupported(cfs.getLiveSSTables(), controller));
        }
    }

    /**
     * A repaired sstable's transient ranges are excluded from the scan, so both paths must drop the
     * partitions in them even though those ranges are still "owned".
     */
    @Test
    public void repairedTransientRangesAreDropped() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 16; pk++)
            for (long ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        flush();
        markRepaired(cfs);

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        List<Range<Token>> owned = new ArrayList<>(rangesOwning(keys, keys));
        // the highest-token owned range is transient: repaired data there is dropped
        List<Range<Token>> transient_ = List.of(owned.get(owned.size() - 1));

        CleanupOutcome out = assertCursorCleanupMatchesLegacy(cfs, owned, transient_);
        assertEquals(1, out.captured.sstables.size());
        assertEquals("the transient range's partition should have been dropped",
                     keys.subList(0, keys.size() - 1), out.survivingKeys);
    }

    private static void markRepaired(ColumnFamilyStore cfs) throws Exception
    {
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            sstable.descriptor.getMetadataSerializer()
                              .mutateRepairMetadata(sstable.descriptor, FBUtilities.nowInSeconds(), null, false);
            sstable.reloadSSTableMetadata();
        }
    }
}
