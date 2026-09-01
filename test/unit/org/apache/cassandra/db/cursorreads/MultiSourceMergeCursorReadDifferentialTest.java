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
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * M2.1 (CASSANDRA-20428, Phase 3) differential scenarios for the CURSOR-LEVEL merge across
 * {@code >= 2} overlapping sstable legs ({@code CursorReadMerger}): every scenario asserts
 * byte-identical canonical records and intra-node ReadResponse serialization against the iterator
 * path (base harness), PLUS the merge-specific effectiveness guards:
 * <ul>
 *   <li>{@link CursorReads#cursorMergesServed()} advanced — the merge core actually ran, no
 *       silent per-leg fallback;</li>
 *   <li>{@link CursorReads#sstableLegsCursorMerged()} advanced by exactly the expected leg count;</li>
 *   <li>{@link CursorReads#unfilteredsMaterialized()} equals the MERGED output size, not
 *       Sigma(per-source rows) — a fallback to per-leg-materialize-then-object-merge would pass
 *       every correctness check (the result is still right!) and fail exactly this
 *       (the anti-fallback-by-correctness-alone pattern of M1's seek-effectiveness guard).</li>
 * </ul>
 * Cross-source reconciliation edge cases covered per the M2 design: exact-timestamp cell ties
 * (value tie-break direction, the CASSANDRA-14592 expiring/tombstone-beats-live rules, lower-TTL
 * preference), partition-delete + reinsert split across legs, range tombstones opened in one leg
 * and closed in another (bound-to-boundary synthesis at merge), per-leg complex deletions vs
 * cells, static-row divergence, column-subset (canSkipValue) selection, and a {@code Slices.NONE}
 * partition-deletion-only inclusion leg. Also the merge core's own deliberate-corruption mutation
 * tests (wrong merge DECISION, not just wrong materialization) and a per-leg validation scenario
 * whose corruption is SHADOWED out of the merged output (only per-leg validation can catch it —
 * merged-emission validation alone would pass).
 */
public class MultiSourceMergeCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private Supplier<SinglePartitionReadCommand> fullPartition(ColumnFamilyStore cfs, long now, Object... key)
    {
        return () -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build();
    }

    /**
     * The full differential assertion plus the merge-served / legs-merged guards. The base
     * harness executes the cursor path exactly TWICE per call (canonical records + response
     * bytes), hence the doubling of every per-run expectation.
     */
    protected void assertMergedCursorReadMatchesIterator(ColumnFamilyStore cfs,
                                                         Supplier<SinglePartitionReadCommand> command,
                                                         int expectedMergedLegsPerRun)
    {
        long mergesBefore = CursorReads.cursorMergesServed();
        long legsBefore = CursorReads.sstableLegsCursorMerged();

        assertCursorReadMatchesIterator(cfs, command);

        assertEquals("cursor-level merges served across the harness's two cursor runs",
                     2L, CursorReads.cursorMergesServed() - mergesBefore);
        assertEquals("sstable legs cursor-merged across the harness's two cursor runs",
                     2L * expectedMergedLegsPerRun, CursorReads.sstableLegsCursorMerged() - legsBefore);
    }

    /** Emitted (merged, post-slice) rows + markers on the ITERATOR path — the merged-output size
     *  the effectiveness guard compares materialization against. */
    private long countEmittedUnfiltereds(Supplier<SinglePartitionReadCommand> command)
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        long count = 0;
        for (String record : canonicalRecords(command.get()))
        {
            if (record.startsWith("ROW ") || record.startsWith("MARKER "))
                count++;
        }
        return count;
    }

    // ---------------------------------------------------------------- effectiveness

    /**
     * The seam-(ii) guard: on a 5-way shadow-heavy overlap, the merge must materialize ONLY the
     * merged winners — asserted as unfilteredsMaterialized == merged output exactly (full-partition
     * read: no artificial slice markers, so the merged stream IS the emitted stream), which is far
     * below Sigma(per-leg rows). Correctness alone cannot catch a fallback to per-leg
     * materialization; this does.
     */
    @Test
    public void shadowHeavyFiveLegMergeAvoidsMaterializingLosers() throws Throwable
    {
        int sources = 5;
        int rows = 48;
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < sources; round++)
        {
            for (long ck = 0; ck < rows; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)",
                        1L, ck, ck * 10 + round, "shadow-" + round + "-" + ck);
            if (round == sources - 1)
            {
                execute("DELETE FROM %s WHERE pk = ? AND ck = 3", 1L);                 // row tombstone
                execute("DELETE v2 FROM %s WHERE pk = ? AND ck = 5", 1L);              // cell tombstone
                execute("DELETE FROM %s WHERE pk = ? AND ck >= 30 AND ck < 38", 1L);   // range tombstone
            }
            flush();
        }
        assertEquals(sources, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = fullPartition(cfs, now, 1L);
        // merged output: the 8 range-tombstoned rows merge to empty and drop out; the RT itself
        // contributes its open/close markers; the row-tombstoned row survives as a deletion-carrying row
        long emitted = countEmittedUnfiltereds(cmd);
        assertTrue("workload sanity: expected a non-trivial merged output, got " + emitted,
                   emitted >= rows - 8);

        long materializedBefore = CursorReads.unfilteredsMaterialized();
        assertMergedCursorReadMatchesIterator(cfs, cmd, sources);
        long materialized = CursorReads.unfilteredsMaterialized() - materializedBefore;

        assertEquals("merged-mode materialization must equal the MERGED output (per cursor run), " +
                     "not Sigma(per-leg rows)=" + (long) sources * rows + " — a per-leg-materialize " +
                     "fallback would still pass the differential comparison but fail here",
                     2L * emitted, materialized);
    }

    /** Tie-heavy variant: every cell reconciliation is an exact-timestamp value tie (the COMPARE
     *  arm), across 5 legs. */
    @Test
    public void tieHeavyFiveLegMerge() throws Throwable
    {
        int sources = 5;
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < sources; round++)
        {
            for (long ck = 0; ck < 24; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000",
                        1L, ck, ck * 10 + round, "tie-" + round + "-" + ck);
            flush();
        }
        assertEquals(sources, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L), sources);
    }

    // ---------------------------------------------------------------- reconciliation edge cases

    /**
     * Exact-timestamp tie-breaks across legs, including the direction-sensitive cases: the
     * greater VALUE must win regardless of which leg (older or newer sstable) carries it, and the
     * CASSANDRA-14592 rules — tombstone/expiring beats live at the same timestamp, tombstone
     * beats expiring, greater expiration time wins, lower TTL wins on full expiration ties.
     */
    @Test
    public void exactTimestampTieBreaksAcrossLegs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, v3 text, v4 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // leg 1 (flushed first)
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 0, 'bbb') USING TIMESTAMP 1000");    // greater value in OLDER leg
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 1, 'aaa') USING TIMESTAMP 1000");    // lesser value in older leg
        execute("INSERT INTO %s (pk, ck, v2) VALUES (1, 2, 'live') USING TIMESTAMP 1000");   // live vs tombstone tie
        execute("INSERT INTO %s (pk, ck, v3) VALUES (1, 3, 'live') USING TIMESTAMP 1000");   // live vs expiring tie
        execute("INSERT INTO %s (pk, ck, v4) VALUES (1, 4, 'ttl-hi') USING TIMESTAMP 1000 AND TTL 100000"); // expiring both sides
        flush();

        // leg 2
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 0, 'aaa') USING TIMESTAMP 1000");    // must LOSE to leg 1's 'bbb'
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 1, 'bbb') USING TIMESTAMP 1000");    // must WIN over leg 1's 'aaa'
        execute("DELETE v2 FROM %s USING TIMESTAMP 1000 WHERE pk = 1 AND ck = 2");           // tombstone beats live
        execute("INSERT INTO %s (pk, ck, v3) VALUES (1, 3, 'exp') USING TIMESTAMP 1000 AND TTL 100000"); // expiring beats live
        execute("INSERT INTO %s (pk, ck, v4) VALUES (1, 4, 'ttl-lo') USING TIMESTAMP 1000 AND TTL 50000"); // lower expiration time
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L), 2);
    }

    /** Partition deletion in a middle leg, older shadowed rows in another, reinsert in a third —
     *  the partition-deletion shadowing must happen INSIDE the merge exactly as the object merge
     *  would apply it. */
    @Test
    public void partitionDeleteAndReinsertSplitAcrossLegs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // leg 1: old rows fully shadowed by the later partition delete, PLUS one post-delete row so
        // the leg's maxTimestamp keeps it in the merge (not eliminated by mostRecentPartitionTombstone)
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 1000", ck, "old" + ck);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 100, 'survivor') USING TIMESTAMP 4000");
        flush();
        // leg 2: the partition deletion
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1");
        flush();
        // leg 3: the reinsert
        for (long ck = 2; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 3000", ck, "new" + ck);
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L), 3);
    }

    /**
     * Range-tombstone interactions across legs: an RT closed in one leg exactly where another
     * leg's RT opens (bound + bound to BOUNDARY synthesis at merge), overlapping RTs with
     * different deletion times (supersedes-max transitions the per-source streams never carried),
     * and an RT from one leg shadowing row data from another. Slices probed at and across the
     * transition points, including a slice starting inside merged-RT coverage.
     */
    @Test
    public void rangeTombstonesSpanningSourceTransitions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 60; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 1000", ck, "base" + ck);
        // leg 1: RT [10, 20)
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck >= 10 AND ck < 20");
        flush();
        // leg 2: RT [20, 30) — opens exactly where leg 1's closes: boundary synthesis at ck=20;
        // plus an overlapping higher-timestamp RT [15, 25) in the same leg for boundary markers
        // on disk AND cross-leg supersedes-max transitions; plus newer rows inside leg 1's range
        execute("DELETE FROM %s USING TIMESTAMP 2500 WHERE pk = 1 AND ck >= 20 AND ck < 30");
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = 1 AND ck >= 15 AND ck < 25");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 12, 'resurrected') USING TIMESTAMP 5000");
        flush();
        // leg 3: RT [40, 50) far from the others, and rows shadowed by leg 2's RTs
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck >= 40 AND ck < 50");
        for (long ck = 16; ck < 28; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 2200", ck, "mid" + ck);
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L), 3);
        // slice starting inside merged-RT coverage: artificial open marker carries the MERGED max
        assertMergedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(17L).toIncl(45L).build(), 3);
        // slice ending exactly at the cross-leg boundary position
        assertMergedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(5L).toExcl(20L).build(), 3);
    }

    /** Complex-column reconciliation across legs: complex deletions vs older cells from another
     *  leg, deletion-only complex columns, element-level overwrites, exact-tie map elements. */
    @Test
    public void complexColumnsAcrossLegs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // leg 1: base maps
        for (long ck = 0; ck < 6; ck++)
            execute("INSERT INTO %s (pk, ck, m, s, v) VALUES (1, ?, ?, ?, ?)",
                    ck, map("k" + ck, ck, "x", 1L), set((int) ck, 7), "v" + ck);
        flush();
        // leg 2: full-map overwrite (complex deletion shadowing leg 1's cells), element
        // overwrites, a deletion-only complex column, and a set delete
        execute("UPDATE %s SET m = ? WHERE pk = 1 AND ck = 0", map("fresh", 9L));
        execute("UPDATE %s SET m[?] = ? WHERE pk = 1 AND ck = 1", "x", 100L);
        execute("DELETE m FROM %s WHERE pk = 1 AND ck = 2");
        execute("DELETE s FROM %s WHERE pk = 1 AND ck = 3");
        flush();
        // leg 3: cells written UNDER leg 2's complex deletions (older timestamps arrive from a
        // different source) plus same-timestamp element ties
        execute("UPDATE %s USING TIMESTAMP 100 SET m[?] = ? WHERE pk = 1 AND ck = 0", "stale", 1L);
        execute("UPDATE %s USING TIMESTAMP 100 SET m[?] = ? WHERE pk = 1 AND ck = 2", "stale", 2L);
        execute("UPDATE %s SET v = ? WHERE pk = 1 AND ck = 2", "post-delete");
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L), 3);
    }

    /** Static rows diverging across legs (newer static in one leg, newer regulars in another),
     *  merged through the object-level static merge under the merged partition deletion. */
    @Test
    public void staticRowDivergenceAcrossLegs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 text static, s2 bigint static, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("UPDATE %s SET s1 = 'old-s1', s2 = 1 WHERE pk = 1");
        for (long ck = 0; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", ck, "old" + ck);
        flush();
        execute("UPDATE %s SET s1 = 'new-s1' WHERE pk = 1"); // s2 stays from leg 1
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 'newer')");
        flush();
        // third leg: statics only, no regular rows for this partition... plus another partition so
        // the sstable is not empty of clustering data
        execute("UPDATE %s SET s2 = 22 WHERE pk = 1");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 0, 'other')");
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L), 3);
        // static-only selection across the merged legs
        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).columns("s1", "s2").build());
    }

    /** Column-subset selection over merged legs: fetched-but-not-queried columns must reconcile
     *  on EMPTY values per leg (canSkipValue below the merge), matching the iterator path's
     *  deserialization-time filtering. */
    @Test
    public void columnSubsetSelectionAcrossLegs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, v3 int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 12; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2, v3) VALUES (1, ?, ?, ?, ?)", ck, ck, "a" + ck, (int) ck);
        flush();
        for (long ck = 0; ck < 12; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2, v3) VALUES (1, ?, ?, ?, ?)", ck, ck + 100, "b" + ck, (int) ck + 100);
        // rows where the row liveness is NEWER than the non-queried columns' cells: the
        // CASSANDRA-7085 skip drops those cells entirely per leg, below the merge
        for (long ck = 0; ck < 12; ck += 3)
            execute("INSERT INTO %s (pk, ck) VALUES (1, ?)", ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).columns("v1").build(), 2);
        assertMergedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).columns("v2", "v3").fromIncl(2L).toIncl(9L).build(), 2);
    }

    /**
     * A {@code Slices.NONE} inclusion leg: a non-intersecting sstable carrying a partition-level
     * deletion joins the merge through its deletion alone (its rows are outside the slice and
     * must contribute NOTHING else), shadowing older rows from an intersecting leg.
     */
    @Test
    public void slicesNoneDeletionLegJoinsMerge() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // leg A: rows inside the queried slice, written AFTER the partition delete (reinsert) plus
        // rows before it (shadowed)
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 1000", ck, "old" + ck);
        for (long ck = 4; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 3000", ck, "new" + ck);
        flush();
        // leg B: partition delete + rows far OUTSIDE the queried slice, so the leg does not
        // intersect it and is included via the partition-deletion branch (Slices.NONE)
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1");
        for (long ck = 100; ck < 105; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 2500", ck, "far" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        // the slice [0, 10) intersects only leg A's clustering range; leg B joins as Slices.NONE
        assertMergedCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(0L).toExcl(10L).build(), 2);
    }

    // ---------------------------------------------------------------- single-leg routing guard

    /** Single-leg reads must keep the per-leg path bit-for-bit: the merge counter must NOT move —
     *  both for a single-candidate table and for a multi-candidate read where only one sstable
     *  actually contains the partition. */
    @Test
    public void singleLegReadsDoNotEngageTheMergeCore() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?)", ck, "v" + ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        long mergesBefore = CursorReads.cursorMergesServed();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        assertEquals("single-candidate read must not engage the merge core",
                     mergesBefore, CursorReads.cursorMergesServed());

        // second sstable for a DIFFERENT partition: pk=1 reads now see two candidates but only one
        // leg contains the partition — still no merge
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (2, ?, ?)", ck, "w" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        mergesBefore = CursorReads.cursorMergesServed();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        assertEquals("multi-candidate read with a single actual leg must not engage the merge core",
                     mergesBefore, CursorReads.cursorMergesServed());
    }

    // ---------------------------------------------------------------- mutation tests

    /**
     * The merge-core mutation test (Gap C deliberate-corruption pattern): invert the shared
     * resolveRegular verdict inside the merge so reconciliation picks the LOSING cell, and prove
     * the differential harness FAILS — i.e. it would actually catch a wrong merge decision, not
     * just a wrong per-leg walk (which {@code CursorReadGateTest.deliberateCorruptionIsDetected}
     * already proves via the timestamp hook — a hook that since M2.1 also applies inside the
     * merge sink, covered here too).
     */
    @Test
    public void deliberateMergeDecisionCorruptionIsDetected() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?)", ck, ck, "old" + ck);
        flush();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?)", ck, ck + 100, "new" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = fullPartition(cfs, now, 1L);

        // sanity: passes when the merge is honest
        assertMergedCursorReadMatchesIterator(cfs, cmd, 2);

        expectHarnessDetection(cfs, cmd, () -> CursorReads.TEST_CORRUPT_MERGE_DECISIONS = true,
                               () -> CursorReads.TEST_CORRUPT_MERGE_DECISIONS = false,
                               "wrong-merge-decision");
        expectHarnessDetection(cfs, cmd, () -> CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS = true,
                               () -> CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS = false,
                               "merged-cell-timestamp");

        // and it must pass again once the corruption is removed
        assertMergedCursorReadMatchesIterator(cfs, cmd, 2);
    }

    private void expectHarnessDetection(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd,
                                        Runnable corrupt, Runnable restore, String label)
    {
        corrupt.run();
        try
        {
            assertCursorReadMatchesIterator(cfs, cmd);
            fail("differential harness FAILED TO DETECT deliberate " + label + " corruption of the merge core");
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

    // ---------------------------------------------------------------- per-leg validation

    /**
     * Corruption SHADOWED out of the merged output: an invalid row deletion in an older leg loses
     * the deletion supersedes-max to a newer valid one, so the MERGED stream is fully valid —
     * only per-leg validation (which the iterator path gets for free by validating each leg below
     * its merge) can catch it. Under {@code exception} both paths must refuse the read and mark
     * the sstable suspect; under {@code disabled} both must serve identical bytes.
     * (Unlike the single-leg corrupted-tombstone suite, the DIAGNOSIS message is not asserted
     * identical here: the merge validates descriptor state per leg rather than materializing the
     * losing leg's row just for the error string — verdict and side effects are the parity
     * surface for multi-leg corruption.)
     */
    @Test
    public void shadowedCorruptionInLosingLegFailsBothPaths() throws Throwable
    {
        Config.CorruptedTombstoneStrategy saved = DatabaseDescriptor.getCorruptedTombstoneStrategy();
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 1000", ck, "v" + ck);
            // CASSANDRA-14227 fixture: row deletion with a negative local deletion time
            RowUpdateBuilder.deleteRowAt(cfs.metadata(), 1000, -1, 1L, 2L).apply();
            flush();
            // newer VALID row deletion at the same clustering in a second leg: it wins the merge,
            // so the invalid one never reaches the merged output
            RowUpdateBuilder.deleteRowAt(cfs.metadata(), 2000, FBUtilities.nowInSeconds(), 1L, 2L).apply();
            flush();
            assertEquals(2, cfs.getLiveSSTables().size());

            long now = FBUtilities.nowInSeconds();
            Supplier<SinglePartitionReadCommand> cmd = fullPartition(cfs, now, 1L);

            DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.disabled);
            assertMergedCursorReadMatchesIterator(cfs, cmd, 2);

            DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
            DatabaseDescriptor.setCursorReadsEnabled(false);
            expectCorruptRead(cmd);
            assertSuspectAndReset(cfs);

            DatabaseDescriptor.setCursorReadsEnabled(true);
            try
            {
                long mergesBefore = CursorReads.cursorMergesServed();
                expectCorruptRead(cmd);
                assertSuspectAndReset(cfs);
                // the merge threw mid-run: it never completed, so the merges-served counter is the
                // wrong guard here — the suspect-mark above proves the CURSOR path (per-leg merge
                // validation) did the refusing, and the counter must not claim a completed merge
                assertEquals(mergesBefore, CursorReads.cursorMergesServed());
            }
            finally
            {
                DatabaseDescriptor.setCursorReadsEnabled(false);
            }
        }
        finally
        {
            DatabaseDescriptor.setCorruptedTombstoneStrategy(saved);
        }
    }

    /** Demands the read fail with CorruptSSTableException (MarshalException diagnosis inside). */
    private void expectCorruptRead(Supplier<SinglePartitionReadCommand> command)
    {
        try
        {
            canonicalRecords(command.get());
        }
        catch (Throwable t)
        {
            for (Throwable c = t; c != null; c = c.getCause())
            {
                if (c instanceof CorruptSSTableException || c instanceof MarshalException)
                    return;
            }
            throw new AssertionError("read of an invalid deletion failed, but not with " +
                                     "CorruptSSTableException/MarshalException", t);
        }
        fail("read of a leg with an invalid deletion SUCCEEDED under corrupted_tombstone_strategy=exception" +
             " (cursor_reads_enabled=" + DatabaseDescriptor.cursorReadsEnabled() + ')');
    }

    private static void assertSuspectAndReset(ColumnFamilyStore cfs)
    {
        boolean any = false;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.isMarkedSuspect())
            {
                any = true;
                sstable.unmarkSuspect();
            }
        }
        assertTrue("no sstable was marked suspect by the failed read", any);
    }
}
