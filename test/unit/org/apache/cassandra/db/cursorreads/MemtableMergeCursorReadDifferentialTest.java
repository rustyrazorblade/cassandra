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

import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * M2.3 (CASSANDRA-20428, Phase 3) differential scenarios for the MEMTABLE leg joining the
 * cursor-level merge through the object-backed adapter ({@code MemtableMergeLeg}): every scenario
 * asserts byte-identical canonical records and intra-node ReadResponse serialization against the
 * iterator path (base harness), PLUS the memtable-specific effectiveness guards:
 * <ul>
 *   <li>{@link CursorReads#memtableLegsCursorMerged()} advanced by exactly the expected count —
 *       the memtable leg genuinely joined the cursor merge; a quiet fallback to the pre-M2.3
 *       "cursor-merge the sstables, object-merge the memtable above" staging would produce
 *       byte-identical results and fail exactly this;</li>
 *   <li>{@link CursorReads#memtableRowsReused()} / {@link CursorReads#memtableCellsReused()} —
 *       the zero-copy escape hatches actually engaged where they must (and did NOT engage where
 *       shadowing forbids them), which correctness comparison alone cannot see.</li>
 * </ul>
 * Scenario surface per the M2.3 charter: memtable-only rows merging with sstable rows, tie-breaks
 * in both directions (memtable newer / memtable older / exact-timestamp value ties), a
 * memtable-sourced range tombstone crossing into and out of sstable-sourced data, memtable data
 * shadowed by sstable data (partition deletion, row deletion and range tombstone — proving
 * correct DROPPING, not just correct merging), complex columns and statics across the
 * memtable/sstable seam, column-subset selection (the canSkipValue EMPTY-vs-full-value tie
 * asymmetry), the single-sstable+memtable routing (2-leg merges), the no-surviving-sstable-leg
 * fallback, and two deliberate-corruption mutation tests for the adapter specifically (a wrong
 * merge decision involving the memtable leg, and a wrong escape-hatch reuse).
 *
 * The memtable-config half of the charter is covered by the subclasses:
 * {@code OffheapObjectsMemtableMergeCursorReadDifferentialTest} runs this whole corpus under
 * {@code offheap_objects} (heap_buffers is the default config this base class runs under), and
 * {@code BtiMemtableMergeCursorReadDifferentialTest} runs it with BTI sstable legs (seek-active).
 */
public class MemtableMergeCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private Supplier<SinglePartitionReadCommand> fullPartition(ColumnFamilyStore cfs, long now, Object... key)
    {
        return () -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build();
    }

    /**
     * Full differential assertion plus the merge-served / legs-merged guards for a memtable+sstable
     * merged read. The base harness executes the cursor path exactly TWICE per call (canonical
     * records + response bytes), hence the doubling of every per-run expectation.
     */
    protected void assertMemtableMergedMatches(ColumnFamilyStore cfs,
                                               Supplier<SinglePartitionReadCommand> command,
                                               int expectedSstableLegsPerRun,
                                               int expectedMemtableLegsPerRun)
    {
        long mergesBefore = CursorReads.cursorMergesServed();
        long sstableLegsBefore = CursorReads.sstableLegsCursorMerged();
        long memtableLegsBefore = CursorReads.memtableLegsCursorMerged();

        assertCursorReadMatchesIterator(cfs, command);

        assertEquals("cursor-level merges served across the harness's two cursor runs",
                     2L, CursorReads.cursorMergesServed() - mergesBefore);
        assertEquals("sstable legs cursor-merged across the harness's two cursor runs",
                     2L * expectedSstableLegsPerRun, CursorReads.sstableLegsCursorMerged() - sstableLegsBefore);
        assertEquals("memtable legs cursor-merged across the harness's two cursor runs — the " +
                     "M2.3 silent-fallback guard: a quiet return to object-merging the memtable " +
                     "above the cursor merge passes every correctness check and fails exactly this",
                     2L * expectedMemtableLegsPerRun, CursorReads.memtableLegsCursorMerged() - memtableLegsBefore);
    }

    // ---------------------------------------------------------------- basic memtable+sstable merges

    /**
     * Memtable-only rows interleaving with sstable rows, plus collisions — a single sstable leg
     * PLUS the memtable now engages the merge core (before M2.3, a 1-sstable read completed
     * per-leg and object-merged with the memtable). Also pins the whole-row escape hatch: each
     * memtable-solo row must be emitted as the memtable's own row object (counted by
     * memtableRowsReused), and colliding rows where memtable cells win must reuse the cell
     * objects (memtableCellsReused).
     */
    @Test
    public void memtableRowsJoinTheMergeWithASingleSSTableLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sstable: ck 0..9
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP 1000", ck, ck, "sst" + ck);
        flush();
        // memtable: collisions on ck 5..9 (newer), memtable-only ck 10..14
        for (long ck = 5; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP 2000", ck, ck + 100, "mem" + ck);
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        long rowsReusedBefore = CursorReads.memtableRowsReused();
        long cellsReusedBefore = CursorReads.memtableCellsReused();

        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), 1, 1);

        // 5 memtable-solo rows (ck 10..14) per cursor run, 2 runs: the whole-row escape hatch
        // must have emitted each as the live row object
        assertEquals("memtable-solo rows must be emitted through the whole-row escape hatch",
                     2L * 5, CursorReads.memtableRowsReused() - rowsReusedBefore);
        // 5 colliding rows (ck 5..9) x 2 cells, memtable wins every collision: the cell escape
        // hatch must have reused every winning memtable cell object
        assertEquals("memtable-won cells in collided rows must be emitted through the cell escape hatch",
                     2L * 5 * 2, CursorReads.memtableCellsReused() - cellsReusedBefore);
    }

    /** Tie-breaks in both directions: memtable newer than the sstable data on some rows, OLDER
     *  (via USING TIMESTAMP) on others — the memtable leg must win exactly where the object merge
     *  makes it win, and lose exactly where it loses. Two sstable legs so the memtable merges
     *  into a real multi-sstable merge as well. */
    @Test
    public void memtableWinsAndLosesTimestampCollisions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 12; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP 1000", ck, ck, "a" + ck);
        flush();
        for (long ck = 0; ck < 12; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP 2000", ck, ck + 50, "b" + ck);
        flush();
        // memtable NEWER on ck 0..5 (wins over both legs)
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP 3000", ck, ck + 200, "mem-new" + ck);
        // memtable OLDER on ck 6..11 (loses to both legs)
        for (long ck = 6; ck < 12; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP 500", ck, ck + 300, "mem-old" + ck);
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), 2, 1);
    }

    /** Exact-timestamp ties between the memtable and an sstable leg, value tie-break in both
     *  directions (memtable value greater on one row, smaller on another), plus a
     *  memtable-tombstone-vs-sstable-live tie (CASSANDRA-14592: the tombstone wins). */
    @Test
    public void exactTimestampTiesBetweenMemtableAndSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 0, 'bbb') USING TIMESTAMP 1000"); // sstable greater
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 1, 'aaa') USING TIMESTAMP 1000"); // sstable lesser
        execute("INSERT INTO %s (pk, ck, v2) VALUES (1, 2, 'live') USING TIMESTAMP 1000"); // vs memtable tombstone
        flush();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 0, 'aaa') USING TIMESTAMP 1000");  // memtable must LOSE
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 1, 'bbb') USING TIMESTAMP 1000");  // memtable must WIN
        execute("DELETE v2 FROM %s USING TIMESTAMP 1000 WHERE pk = 1 AND ck = 2");         // tombstone beats live
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), 1, 1);
    }

    // ---------------------------------------------------------------- range tombstones

    /**
     * A memtable-SOURCED range tombstone crossing into and out of sstable-sourced rows: the
     * memtable's markers (including the pre-clipped stream's artificial slice-bound markers on
     * sliced reads) must join the merge's cross-leg open-marker set as normal contributions.
     * Probed as a full-partition read, a slice starting INSIDE the memtable RT's coverage (the
     * artificial open marker at the slice start must carry the memtable deletion), and a slice
     * ending inside it.
     */
    @Test
    public void memtableRangeTombstoneShadowsSSTableRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 1000", ck, "base" + ck);
        flush();
        // memtable: RT over [10, 25), plus a reinsert INSIDE it and rows outside it
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck >= 10 AND ck < 25");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 12, 'resurrected') USING TIMESTAMP 3000");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 30, 'mem30') USING TIMESTAMP 2500");
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), 1, 1);
        // slice starting inside the memtable RT: the artificial open at the slice start must
        // carry the memtable-sourced deletion (crossing INTO sstable data)
        assertMemtableMergedMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(15L).toIncl(35L).build(), 1, 1);
        // slice ending inside the memtable RT (crossing OUT of sstable data)
        assertMemtableMergedMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(0L).toExcl(20L).build(), 1, 1);
    }

    // ---------------------------------------------------------------- memtable data dropped

    /**
     * Memtable data SHADOWED by sstable data must be correctly DROPPED, not just correctly
     * merged — via an sstable partition deletion, an sstable row deletion, and an sstable range
     * tombstone, each newer than the memtable's (deliberately old, USING TIMESTAMP) writes. Also
     * pins that the whole-row escape hatch does NOT engage under a non-live active deletion:
     * reusing the memtable row object there would resurrect shadowed data (exactly what the
     * TEST_FORCE_MEMTABLE_ROW_REUSE mutation test proves the harness would catch).
     */
    @Test
    public void memtableDataShadowedBySSTableDataIsDropped() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // pk=1: partition deletion at ts 2000 (plus one post-delete row so the read has content)
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 100, 'survivor') USING TIMESTAMP 3000");
        // pk=2: row deletion at ck=5, and an RT over [10, 20), both ts 2000
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 2 AND ck = 5");
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 2 AND ck >= 10 AND ck < 20");
        flush();
        // memtable: older writes under all three deletion shapes
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 1000", ck, "shadowed" + ck);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (2, 5, 'under-row-delete') USING TIMESTAMP 1000");
        for (long ck = 10; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (2, ?, ?) USING TIMESTAMP 1000", ck, "under-rt" + ck);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (2, 30, 'clear') USING TIMESTAMP 1000");
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();

        // partition deletion: every memtable row is under the non-live active deletion, so the
        // whole-row escape hatch must never fire
        long rowsReusedBefore = CursorReads.memtableRowsReused();
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), 1, 1);
        assertEquals("the whole-row escape hatch must not engage under a non-live active deletion",
                     0L, CursorReads.memtableRowsReused() - rowsReusedBefore);

        // row deletion + range tombstone shadowing (pk=2): shadowed memtable rows dropped, the
        // out-of-range row (ck=30) survives via the whole-row escape hatch
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 2L), 1, 1);
    }

    // ---------------------------------------------------------------- complex columns and statics

    /** Complex-column reconciliation across the memtable/sstable seam: a memtable full-map
     *  overwrite (complex deletion) shadowing sstable cells, a memtable deletion-only complex
     *  column, memtable element overwrites and same-timestamp element ties — and the reverse
     *  direction, an SSTABLE complex deletion shadowing older memtable cells. */
    @Test
    public void complexColumnsAcrossMemtableAndSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, m, v) VALUES (1, ?, ?, ?)", ck, map("k" + ck, ck, "x", 1L), "v" + ck);
        // ck=4: sstable-side complex deletion NEWER than the memtable cells written below
        execute("DELETE m FROM %s USING TIMESTAMP 5000 WHERE pk = 1 AND ck = 4");
        flush();
        // memtable: full-map overwrite (complex deletion + cells), element overwrite, deletion-only
        // column, and cells UNDER the sstable's ck=4 complex deletion
        execute("UPDATE %s SET m = ? WHERE pk = 1 AND ck = 0", map("fresh", 9L));
        execute("UPDATE %s SET m[?] = ? WHERE pk = 1 AND ck = 1", "x", 100L);
        execute("DELETE m FROM %s WHERE pk = 1 AND ck = 2");
        execute("UPDATE %s USING TIMESTAMP 100 SET m[?] = ? WHERE pk = 1 AND ck = 4", "stale", 1L);
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), 1, 1);
    }

    /** Static rows diverging between the memtable and sstable legs, merged through the
     *  object-level static merge over all legs (memtable static included). */
    @Test
    public void staticRowDivergenceAcrossMemtableAndSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 text static, s2 bigint static, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("UPDATE %s SET s1 = 'old-s1', s2 = 1 WHERE pk = 1");
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", ck, "old" + ck);
        flush();
        execute("UPDATE %s SET s1 = 'mem-s1' WHERE pk = 1"); // s2 stays from the sstable
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'newer')");
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), 1, 1);
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).columns("s1", "s2").build());
    }

    /** Column-subset selection across the seam: on an exact-timestamp tie for a fetched-but-not-
     *  queried column, the sstable leg reconciles on an EMPTY value (canSkipValue skips it at the
     *  byte level) while the memtable leg keeps its full value — today's object-path asymmetry,
     *  which the merged read must reproduce byte-for-byte. */
    @Test
    public void columnSubsetSelectionAcrossMemtableAndSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP 1000", ck, ck, "sst" + ck);
        flush();
        // exact-ts ties on v2 (non-queried below) and newer v1 values on even rows
        for (long ck = 0; ck < 8; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP 1000", ck, ck + 50, "mem" + ck);
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMemtableMergedMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).columns("v1").build(), 1, 1);
    }

    // ---------------------------------------------------------------- routing edges

    /**
     * Memtable data with candidate sstables that do NOT contain the queried partition: the gate
     * passes (there are sstable candidates) but no sstable leg survives, so the memtable
     * iterators must fall back to the object path exactly as before M2.3 — no merge, identical
     * results.
     */
    @Test
    public void memtableFallsBackWhenNoSSTableLegSurvives() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sstable contains many partitions, none of them pk=1 (broad token coverage so pk=1's
        // token is, in practice, inside some candidate's key range — the absentPartition pattern)
        for (long pk = 2; pk < 66; pk++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, 0, ?)", pk, "other" + pk);
        flush();
        // memtable contains pk=1
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?)", ck, "mem" + ck);
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = fullPartition(cfs, now, 1L);
        // as in BasicCursorReadDifferentialTest.absentPartition: if pk=1's token falls outside
        // every sstable's covered range the gate is (correctly) closed and this proves nothing
        org.junit.Assume.assumeFalse("pk=1 not covered by any sstable's key range",
                                     liveSSTablesFor(cfs, cmd.get()).isEmpty());
        long mergesBefore = CursorReads.cursorMergesServed();
        long memtableLegsBefore = CursorReads.memtableLegsCursorMerged();
        // expectCursorServedLegs=false: the candidate sstable does not contain pk=1
        assertCursorReadMatchesIterator(cfs, cmd, false);
        assertEquals("no cursor merge may run without a surviving sstable leg",
                     mergesBefore, CursorReads.cursorMergesServed());
        assertEquals("no memtable leg may be counted as cursor-merged on the fallback path",
                     memtableLegsBefore, CursorReads.memtableLegsCursorMerged());
    }

    // ---------------------------------------------------------------- mutation tests

    /**
     * The M2.3 negative tests (Gap C deliberate-corruption discipline, applied to the adapter):
     * <ol>
     *   <li>TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS skews every timestamp the adapter presents to
     *       reconciliation, flipping an exact-tie collision the memtable must LOSE (by value)
     *       into a timestamp win — proving the harness catches a wrong merge decision involving
     *       the memtable leg;</li>
     *   <li>TEST_FORCE_MEMTABLE_ROW_REUSE forces the whole-row escape hatch under a non-live
     *       active deletion, resurrecting shadowed memtable rows — proving the harness catches a
     *       wrong escape-hatch reuse.</li>
     * </ol>
     */
    @Test
    public void deliberateMemtableAdapterCorruptionIsDetected() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // pk=1: exact-timestamp collisions the memtable must LOSE by value tie-break — a +1
        // timestamp skew flips them into memtable wins
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, 'zzz-sstable') USING TIMESTAMP 1000", ck);
        // pk=2: partition deletion newer than the memtable rows written below — honest merging
        // drops them; a forced whole-row reuse resurrects them
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 2");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (2, 100, 'survivor') USING TIMESTAMP 3000");
        flush();
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, 'aaa-memtable') USING TIMESTAMP 1000", ck);
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (2, ?, ?) USING TIMESTAMP 1000", ck, "shadowed" + ck);
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> tieRead = fullPartition(cfs, now, 1L);
        Supplier<SinglePartitionReadCommand> shadowedRead = fullPartition(cfs, now, 2L);

        // sanity: both pass when the adapter is honest
        assertMemtableMergedMatches(cfs, tieRead, 1, 1);
        assertMemtableMergedMatches(cfs, shadowedRead, 1, 1);

        expectHarnessDetection(cfs, tieRead, () -> CursorReads.TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS = true,
                               () -> CursorReads.TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS = false,
                               "memtable-timestamp-skew");
        expectHarnessDetection(cfs, shadowedRead, () -> CursorReads.TEST_FORCE_MEMTABLE_ROW_REUSE = true,
                               () -> CursorReads.TEST_FORCE_MEMTABLE_ROW_REUSE = false,
                               "forced-memtable-row-reuse");

        // and both pass again once the corruption is removed
        assertMemtableMergedMatches(cfs, tieRead, 1, 1);
        assertMemtableMergedMatches(cfs, shadowedRead, 1, 1);
    }

    protected void expectHarnessDetection(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd,
                                          Runnable corrupt, Runnable restore, String label)
    {
        corrupt.run();
        try
        {
            assertCursorReadMatchesIterator(cfs, cmd);
            fail("differential harness FAILED TO DETECT deliberate " + label + " corruption of the memtable adapter");
        }
        catch (AssertionError expected)
        {
            if (expected.getMessage() != null && expected.getMessage().contains("FAILED TO DETECT"))
                throw expected;
            assertTrue("harness failed for an unexpected reason: " + expected.getMessage(),
                       expected.getMessage() != null
                       && (expected.getMessage().contains("LOGICAL divergence")
                           || expected.getMessage().contains("BYTE divergence")));
        }
        finally
        {
            restore.run();
        }
    }
}
