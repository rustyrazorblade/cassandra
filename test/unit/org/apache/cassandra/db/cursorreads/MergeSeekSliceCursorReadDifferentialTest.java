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

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * M2.2 (CASSANDRA-20428) differential scenarios for the M1 row-index seek INTEGRATED INTO the
 * M2.1 multi-leg cursor merge: narrow mid-partition slices on merged wide partitions, slice
 * bounds landing inside range tombstones opened in one leg while the merge's cross-leg
 * open-marker set must reconcile them against other legs' contributions (the correctness-critical
 * surface of this increment — each seeked leg's open-RT state is seeded from its OWN row index's
 * {@code IndexInfo.openDeletion}), in-slice closes that must consume a SEEDED open, a merged read
 * mixing BIG and BTI legs (each handled per its own format), and the compounding of the seek
 * (slice start side) with the M2.1 merge-core end-stop (slice end side).
 *
 * THIS class runs under the default (BIG) format, where no leg can seek: it proves the scenario
 * corpus is served correctly by the eager merged walk, and asserts ZERO seeks are issued.
 * {@link BtiMergeSeekSliceCursorReadDifferentialTest} re-runs the corpus under BTI with the
 * seek-effectiveness guard active ({@link #seekCapableFormat()}), per the same split M1's
 * {@code SeekSliceCursorReadDifferentialTest}/{@code BtiSeekSliceCursorReadDifferentialTest}
 * pair established. The guard exists because the differential comparison alone CANNOT catch a
 * merged path that silently falls back to the eager walk (byte-identical output by design): on a
 * seek-capable format every scenario additionally asserts the exact per-leg seek count and a
 * block-granular bound on MERGED materialization.
 *
 * The wrong-seed mutation test ({@link #deliberateSeekSeedCorruptionIsDetected}) proves the
 * harness genuinely detects a per-leg open-marker seed that is dropped or carries a wrong value —
 * the failure mode that would silently produce wrong merged range-tombstone output for exactly
 * the query class this feature targets.
 *
 * All wide partitions are written with 1KiB index blocks ({@code column_index_size}) so
 * {@link #WIDE_ROWS} rows span dozens of row-index blocks.
 */
public class MergeSeekSliceCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    protected static final int WIDE_ROWS = 2000;

    /**
     * MERGED-materialization cap per cursor run for the narrow-slice scenarios: room for the
     * slice's own merged rows plus the pre-slice stretch from the EARLIEST leg's 1KiB floor block
     * start (merged winners only — rows merging to empty under a range tombstone materialize
     * nothing) plus markers, across misaligned per-leg floor blocks — while staying ~16x below
     * {@link #WIDE_ROWS}, so a silent fallback to the unseeked merged walk fails loudly.
     */
    protected static final long MERGED_NARROW_SLICE_CAP = 120;

    private int originalColumnIndexSizeKiB;

    @Before
    public void shrinkIndexBlocks()
    {
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
    }

    @After
    public void restoreIndexBlocks()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    /** Overridden to true by the BTI subclass: scenarios then enforce the seek/bounded-merged-
     *  materialization guard. */
    protected boolean seekCapableFormat()
    {
        return false;
    }

    /**
     * The full differential assertion (identical records + identical intra-node ReadResponse
     * bytes) plus the M2.1 merge guards (merge served, legs merged) plus, on a seek-capable
     * format, the M2.2 seek-effectiveness guard. The harness executes the cursor path exactly
     * TWICE per call (canonical records + response bytes), hence the doubling.
     *
     * @param expectedSeeksPerRun legs expected to issue a row-index seek in ONE cursor execution
     *                            on a seek-capable format (asserted 0 on BIG regardless)
     * @param maxMaterializedPerRun cap on MERGED unfiltereds materialized in one cursor
     *                            execution on a seek-capable format, or -1 for none
     * @param expectedMergedLegsPerRun legs expected to enter the cursor-level merge per run
     */
    protected void assertMergeSeekCursorReadMatchesIterator(ColumnFamilyStore cfs,
                                                            Supplier<SinglePartitionReadCommand> command,
                                                            int expectedSeeksPerRun,
                                                            long maxMaterializedPerRun,
                                                            int expectedMergedLegsPerRun)
    {
        long seeksBefore = CursorReads.sstableLegRowIndexSeeks();
        long materializedBefore = CursorReads.unfilteredsMaterialized();
        long mergesBefore = CursorReads.cursorMergesServed();
        long legsBefore = CursorReads.sstableLegsCursorMerged();

        assertCursorReadMatchesIterator(cfs, command);

        assertEquals("cursor-level merges served across the harness's two cursor runs",
                     2L, CursorReads.cursorMergesServed() - mergesBefore);
        assertEquals("sstable legs cursor-merged across the harness's two cursor runs",
                     2L * expectedMergedLegsPerRun, CursorReads.sstableLegsCursorMerged() - legsBefore);

        long seeks = CursorReads.sstableLegRowIndexSeeks() - seeksBefore;
        long materialized = CursorReads.unfilteredsMaterialized() - materializedBefore;
        if (!seekCapableFormat())
        {
            assertEquals("row-index seek issued on a format the cursor path does not seek on", 0, seeks);
            return;
        }
        assertEquals("merged-mode row-index seeks across the harness's two cursor runs",
                     2L * expectedSeeksPerRun, seeks);
        if (maxMaterializedPerRun >= 0)
            assertTrue("merged cursor path materialized " + materialized + " unfiltereds over two runs, bound 2 x "
                       + maxMaterializedPerRun + " — the per-leg seek/end-stop did not actually bound the merged walk",
                       materialized <= 2 * maxMaterializedPerRun);
    }

    private void createWideTable()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
    }

    private void insertWideRows(long timestamp) throws Throwable
    {
        for (long ck = 0; ck < WIDE_ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP " + timestamp,
                    1L, ck, ck, "value-" + ck);
    }

    /**
     * The headline M2.2 shape: a narrow mid-partition slice on a 2-leg merged wide partition,
     * with BOTH slice bounds set — the seek bounds the walk's start (each leg jumps to its floor
     * block instead of walking ~1000 pre-slice rows) and Finding 3's merge-core end-stop bounds
     * its end, compounding to a block-granular merged walk.
     */
    @Test
    public void narrowMidSliceOnMergedWidePartition() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        insertWideRows(1000);
        flush();
        for (long ck = 0; ck < WIDE_ROWS; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET v2 = ? WHERE pk = ? AND ck = ?", "second-" + ck, 1L, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergeSeekCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1000L).toIncl(1010L).build(),
            2, MERGED_NARROW_SLICE_CAP, 2);
        // exclusive-bound variant landing between rows
        assertMergeSeekCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromExcl(1499L).toExcl(1503L).build(),
            2, MERGED_NARROW_SLICE_CAP, 2);
        // tail slice with NO end bound: isolates the seek from the end-stop under the merge
        assertMergeSeekCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(WIDE_ROWS - 10L).build(),
            2, MERGED_NARROW_SLICE_CAP, 2);
    }

    /**
     * The correctness-critical surface: slice bounds inside range tombstones that are open AT THE
     * SEEK POINT of one leg while the merge must reconcile them against the other leg's
     * contributions. Each leg's seed comes from its OWN row index; the merged artificial open
     * marker at the slice start must be the supersedes-max across the seeds, seeded shadowing
     * must suppress the other leg's older rows, resurrections written OVER a seeded deletion must
     * survive, and an in-slice close marker must correctly consume a SEEDED open from the
     * cross-leg set (the close-must-match-an-open path with no in-stream open to match).
     */
    @Test
    public void sliceBoundsInsideOpenRangeTombstonesReconciledAcrossLegs() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // leg 1: base rows + RT1 [400, 1600) at ts 3000, opened many index blocks before any of
        // the slice starts below — only the row index's openDeletion can tell a seeked leg it is
        // inside RT1
        insertWideRows(1000);
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 400L, 1600L);
        flush();
        // leg 2: newer-but-still-shadowed updates for every row (ts 2000 < RT1's 3000), its own
        // RT2 [900, 1100) ts 2500 and RT3 [1500, 1700) ts 4000, plus resurrections written over
        // RT1+RT2 (ts 5000)
        for (long ck = 0; ck < WIDE_ROWS; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET v2 = ? WHERE pk = ? AND ck = ?", "second-" + ck, 1L, ck);
        execute("DELETE FROM %s USING TIMESTAMP 2500 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 900L, 1100L);
        execute("DELETE FROM %s USING TIMESTAMP 4000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 1500L, 1700L);
        for (long ck = 1002; ck < 1006; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 5000",
                    1L, ck, ck, "resurrected-" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        // start inside RT1 (leg 1's seed) AND RT2 (leg 2's seed): merged artificial open =
        // max(3000, 2500) = RT1's; resurrections survive, everything else in-slice merges to empty
        assertMergeSeekCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1000L).toIncl(1010L).build(),
            2, MERGED_NARROW_SLICE_CAP, 2);
        // RT2 closes at 1100, IN-slice: the close must consume leg 2's SEEDED open (no in-stream
        // open preceded it) while RT1 stays open across it — merged open value unchanged, so the
        // merged stream must emit NO marker there, exactly like RangeTombstoneMarker.Merger
        assertMergeSeekCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1050L).toIncl(1150L).build(),
            2, MERGED_NARROW_SLICE_CAP, 2);
        // start inside RT1 AND RT3: merged open = RT3's 4000; RT1's in-slice close at 1600 must
        // consume leg 1's seed without changing the merged value
        assertMergeSeekCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1550L).toIncl(1650L).build(),
            2, MERGED_NARROW_SLICE_CAP, 2);
        // start inside RT1 only: leg 2 seeks too but carries no open deletion at its floor block
        // (RT2 starts at 900) — a null seed next to a real one
        assertMergeSeekCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(700L).toIncl(720L).build(),
            2, MERGED_NARROW_SLICE_CAP, 2);
    }

    /**
     * A merged read mixing BIG and BTI legs (possible after a format change: different sstables
     * of one table in one read), each handled per its own format: the BTI leg seeks with its
     * open-marker seed, the BIG leg keeps the eager walk from the partition start, and the seeded
     * deletion must shadow the BIG leg's older in-slice rows ACROSS the format boundary. Formats
     * are set explicitly per flush, so this scenario is identical under the base and BTI
     * subclasses (and its assertions do not depend on {@link #seekCapableFormat()}).
     */
    @Test
    public void mixedBigAndBtiLegsInOneMergedRead() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        SSTableFormat<?, ?> original = DatabaseDescriptor.getSelectedSSTableFormat();
        try
        {
            // leg 1, BIG: base rows plus resurrections written over leg 2's RT
            DatabaseDescriptor.setSelectedSSTableFormat("big");
            insertWideRows(1000);
            for (long ck = 1002; ck < 1006; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 5000",
                        1L, ck, ck, "resurrected-" + ck);
            flush();
            // leg 2, BTI: newer-but-shadowed updates and RT [800, 1200) ts 3000 spanning the slice
            DatabaseDescriptor.setSelectedSSTableFormat("bti");
            for (long ck = 0; ck < WIDE_ROWS; ck++)
                execute("UPDATE %s USING TIMESTAMP 2000 SET v2 = ? WHERE pk = ? AND ck = ?", "mixed-" + ck, 1L, ck);
            execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 800L, 1200L);
            flush();
        }
        finally
        {
            DatabaseDescriptor.setSelectedSSTableFormat(original);
        }
        assertEquals(2, cfs.getLiveSSTables().size());
        assertEquals("expected exactly one BTI leg next to one BIG leg",
                     1, cfs.getLiveSSTables().stream().filter(s -> s instanceof BtiTableReader).count());

        long now = FBUtilities.nowInSeconds();
        long seeksBefore = CursorReads.sstableLegRowIndexSeeks();
        long materializedBefore = CursorReads.unfilteredsMaterialized();
        long mergesBefore = CursorReads.cursorMergesServed();

        assertCursorReadMatchesIterator(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1000L).toIncl(1010L).build());

        assertEquals("mixed-format read must be served by the cursor-level merge core",
                     2L, CursorReads.cursorMergesServed() - mergesBefore);
        assertEquals("exactly the BTI leg must seek in a mixed-format merge (per cursor run)",
                     2L, CursorReads.sstableLegRowIndexSeeks() - seeksBefore);
        // the BIG leg walks from the partition start, so the merged stream spans the partition
        // prefix — but the merge-core end-stop must still cut the walk at the slice end
        long materialized = CursorReads.unfilteredsMaterialized() - materializedBefore;
        assertTrue("merged materialization " + materialized + " over two runs exceeds the slice-end-stop bound"
                   + " — the end-stop stopped bounding the mixed-format merged walk",
                   materialized <= 2 * 1100);
    }

    /**
     * The M2.2 mutation test (Gap C / M2.1 deliberate-corruption discipline): prove the
     * differential harness actually CATCHES a wrong per-leg open-marker seed — both a seed that
     * is silently DROPPED at merge start (the seeded deletion's shadowing and the artificial
     * slice-start open marker vanish; rows it deleted resurrect) and a seed carrying a SKEWED
     * value (the merged artificial open marker's deletion time is wrong). The scenario is built
     * so the seeded tombstone's close sits beyond the slice end: the corruption then produces
     * silently-wrong OUTPUT rather than an internal close-matching failure, which is exactly the
     * dangerous case the harness must detect. Only meaningful where seeks happen, hence the
     * format assumption.
     */
    @Test
    public void deliberateSeekSeedCorruptionIsDetected() throws Throwable
    {
        Assume.assumeTrue("seed corruption is only reachable on a seek-capable format", seekCapableFormat());

        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // leg 1 (wide, indexed, seeks): RT [100, 1900) ts 2000, closing far past the slice end so
        // the merge never processes the close — then rows RESURRECTED OVER the tombstone
        // (ts 3000 > 2000, v1 only) filling the many index blocks between the RT's open marker
        // and the slice start. The block gap is what makes the seed LOAD-BEARING: the floor block
        // for the slice start contains only resurrected rows, so the seeked stream never sees the
        // open marker and the row index's openDeletion is the ONLY source of the open deletion.
        // (Rows written BELOW a same-memtable range delete are dropped at flush by the memtable's
        // own deletion filter, so writing base rows first and deleting them would leave the RT
        // interior EMPTY of elements — the floor block would then start at the open marker itself
        // and an in-stream marker would mask a dropped seed. A first version of this test failed
        // to detect the drop for exactly that reason.)
        insertWideRows(1000);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 100L, 1900L);
        for (long ck = 100; ck < 1600; ck++)
            execute("UPDATE %s USING TIMESTAMP 3000 SET v1 = ? WHERE pk = ? AND ck = ?", ck, 1L, ck);
        flush();
        // leg 2 (narrow, unindexed, cannot seek): v2-only updates in-slice UNDER the other leg's
        // seeded tombstone (ts 1500 < 2000 — must stay shadowed ACROSS legs purely via the seed),
        // plus one written over it (ts 2500 — must survive)
        for (long ck = 1000; ck < 1006; ck++)
            execute("UPDATE %s USING TIMESTAMP 1500 SET v2 = ? WHERE pk = ? AND ck = ?", "shadowed-" + ck, 1L, ck);
        execute("UPDATE %s USING TIMESTAMP 2500 SET v2 = ? WHERE pk = ? AND ck = ?", "post-delete", 1L, 1003L);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(1000L).toIncl(1010L).build();

        // sanity: passes when the seed is honest, and the seeded leg really seeked
        assertMergeSeekCursorReadMatchesIterator(cfs, cmd, 1, MERGED_NARROW_SLICE_CAP, 2);

        expectHarnessDetection(cfs, cmd, () -> CursorReads.TEST_DROP_MERGE_SEEK_OPEN_MARKER = true,
                               () -> CursorReads.TEST_DROP_MERGE_SEEK_OPEN_MARKER = false,
                               "dropped-seek-open-marker-seed");
        expectHarnessDetection(cfs, cmd, () -> CursorReads.TEST_SKEW_MERGE_SEEK_OPEN_MARKER = true,
                               () -> CursorReads.TEST_SKEW_MERGE_SEEK_OPEN_MARKER = false,
                               "skewed-seek-open-marker-seed");

        // and it must pass again once the corruption is removed
        assertMergeSeekCursorReadMatchesIterator(cfs, cmd, 1, MERGED_NARROW_SLICE_CAP, 2);
    }

    private void expectHarnessDetection(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd,
                                        Runnable corrupt, Runnable restore, String label)
    {
        corrupt.run();
        try
        {
            assertCursorReadMatchesIterator(cfs, cmd);
            fail("differential harness FAILED TO DETECT deliberate " + label + " corruption of the merge seek seeding");
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
