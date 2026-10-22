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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Differential scenarios for the MEMTABLE leg ({@code NativeMemtableMergeLeg}) joining the
 * cursor-level merge: every scenario asserts byte-identical
 * canonical records and intra-node ReadResponse serialization against the iterator path (base
 * harness), plus the memtable-specific effectiveness guards:
 * <ul>
 *   <li>{@link CursorReads#memtableLegsCursorMerged()} advanced by exactly the expected count —
 *       the memtable leg genuinely joined the cursor merge; a quiet fallback to
 *       "cursor-merge the sstables, object-merge the memtable above" staging would produce
 *       byte-identical results and fail exactly this;</li>
 *   <li>{@link CursorReads#memtableRowsReused()} / {@link CursorReads#memtableCellsReused()} —
 *       the zero-copy escape hatches actually engaged where they must (and did NOT engage where
 *       shadowing forbids them), which correctness comparison alone cannot see.</li>
 * </ul>
 * Scenario surface: memtable-only rows merging with sstable rows, tie-breaks in both directions
 * (memtable newer / memtable older / exact-timestamp value ties), a memtable-sourced range
 * tombstone crossing into and out of sstable-sourced data, memtable data shadowed by sstable data
 * (partition deletion, row deletion and range tombstone — proving correct dropping, not just
 * correct merging), complex columns and statics across the memtable/sstable boundary,
 * column-subset selection (the canSkipValue EMPTY-vs-full-value tie asymmetry), the
 * single-sstable+memtable routing (2-leg merges), the no-surviving-sstable-leg fallback, and two
 * corruption mutation tests for the adapter (a wrong merge decision involving the memtable leg,
 * and a wrong escape-hatch reuse).
 *
 * Runs once per sstable format, BIG and BTI (BTI legs seek), and per memtable implementation:
 * {@code trie}, {@code skiplist} and {@code skiplist_sharded}.
 * {@code OffheapObjectsMemtableMergeCursorReadDifferentialTest} and
 * {@code OffheapBuffersMemtableMergeCursorReadDifferentialTest} run this whole corpus under
 * {@code offheap_objects} and {@code offheap_buffers} (heap_buffers is the default config this
 * base class runs under).
 *
 * <p>The replica response of every scenario reads the memtable with no row or cell object
 * ({@link #assertReplicaResponseBuildsNoMemtableObject}).
 */
public class MemtableMergeCursorReadDifferentialTest extends SSTableFormatParameterizedTester
{
    /** The memtable configurations of test/conf/cassandra.yaml the corpus runs against. */
    private static final String[] MEMTABLES = { "trie", "skiplist", "skiplist_sharded" };

    @Parameterized.Parameter(1)
    public String memtable;

    @Parameterized.Parameters(name = "format={0}, memtable={1}")
    public static Collection<Object[]> formats()
    {
        List<Object[]> parameters = new ArrayList<>();
        for (Object[] format : SSTableFormatParameterizedTester.formats())
        {
            for (String memtable : MEMTABLES)
                parameters.add(new Object[]{ format[0], memtable });
        }
        return parameters;
    }

    /** Every table of the corpus uses this run's memtable implementation. */
    @Override
    protected String createTable(String query)
    {
        String option = "memtable = '" + memtable + "'";
        return super.createTable(query + (query.contains(" WITH ") ? " AND " : " WITH ") + option);
    }

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
        assertReplicaResponseBuildsNoMemtableObject(cfs, command);

        assertEquals("cursor-level merges served across the harness's two cursor runs",
                     2L, CursorReads.cursorMergesServed() - mergesBefore);
        assertEquals("sstable legs cursor-merged across the harness's two cursor runs",
                     2L * expectedSstableLegsPerRun, CursorReads.sstableLegsCursorMerged() - sstableLegsBefore);
        assertEquals("memtable legs cursor-merged across the harness's two cursor runs — the " +
                     "silent-fallback guard: a quiet return to object-merging the memtable " +
                     "above the cursor merge passes every correctness check and fails exactly this",
                     2L * expectedMemtableLegsPerRun, CursorReads.memtableLegsCursorMerged() - memtableLegsBefore);
    }

    // ---------------------------------------------------------------- basic memtable+sstable merges

    /**
     * Memtable-only rows interleaving with sstable rows, plus collisions — a single sstable leg
     * plus the memtable engages the merge core. Also pins the whole-row escape hatch: each
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
     * tombstone, each newer than the memtable's (old, USING TIMESTAMP) writes. Also pins that the
     * whole-row escape hatch does NOT engage under a non-live active deletion: reusing the
     * memtable row object there would resurrect shadowed data (what the
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

    /** Complex-column reconciliation across the memtable/sstable boundary: a memtable full-map
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

    /** Column-subset selection across the boundary: on an exact-timestamp tie for a fetched-but-not-
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
     * Memtable data with a candidate sstable that does NOT contain the queried partition: the gate
     * passes (there is a candidate), so a cursor merge forms over the memtable leg plus the
     * deferred sstable leg.  The deferred leg's lookup resolves the partition absent inside the
     * merge, so no sstable leg data is ever served and the results are identical to the iterator.
     */
    @Test
    public void memtableMergesWithAnAbsentCandidateSSTableLeg() throws Throwable
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
        long servedBefore = CursorReads.sstableLegsServed();
        long missedBefore = CursorReads.sstableLegsWithoutPartition();
        // expectCursorServedLegs=false: the candidate sstable does not contain pk=1.  The
        // deferred-leg gate still forms a cursor merge (memtable leg plus the deferred sstable
        // leg); the leg's lookup resolves the partition absent inside the merge, so no sstable
        // leg data is ever served.  This holds for both BIG and BTI.
        assertCursorReadMatchesIterator(cfs, cmd, false);
        assertEquals("the candidate sstable leg must resolve absent - no sstable leg data served",
                     servedBefore, CursorReads.sstableLegsServed());
        assertTrue("the candidate sstable leg must be looked up and found absent on each cursor run",
                   CursorReads.sstableLegsWithoutPartition() - missedBefore >= 2);
    }

    // ---------------------------------------------------------------- the replica response

    /**
     * The replica response, with cursor reads on, read again on its own: its memtable legs give
     * their cells from the memtable's own storage, so no memtable row or cell is reused or cloned
     * and no row is built.
     */
    protected static void assertReplicaResponseBuildsNoMemtableObject(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    {
        SinglePartitionReadCommand probe = command.get();
        if (!CursorReadOracle.transcodeEligible(probe, cfs))
            return;
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long rowsReused = CursorReads.memtableRowsReused();
            long cellsReused = CursorReads.memtableCellsReused();
            long rowsCloned = CursorReads.memtableRowsCloned();
            long rowsBuilt = CursorReads.mergeSinkRowsBuilt();
            long streamed = CursorReads.memtableLegsStreamed();
            long served = CursorReads.transcodeResponsesServed();
            replicaResponseBytes(command.get());
            assertEquals("the replica response was not streamed", 1, CursorReads.transcodeResponsesServed() - served);
            assertTrue("no memtable leg was read from the memtable's own storage", CursorReads.memtableLegsStreamed() > streamed);
            assertEquals("memtable rows reused by the replica response", 0, CursorReads.memtableRowsReused() - rowsReused);
            assertEquals("memtable cells reused by the replica response", 0, CursorReads.memtableCellsReused() - cellsReused);
            assertEquals("memtable rows cloned by the replica response", 0, CursorReads.memtableRowsCloned() - rowsCloned);
            assertEquals("rows built for the replica response", 0, CursorReads.mergeSinkRowsBuilt() - rowsBuilt);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** The differential for a read of a partition only the memtable holds: no sstable is read. */
    protected void assertMemtableOnlyMatches(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    {
        assertCursorReadMatchesIteratorReadingNoSSTable(cfs, command);
        assertReplicaResponseBuildsNoMemtableObject(cfs, command);
    }

    private static SinglePartitionReadCommand command(ColumnFamilyStore cfs, long now, Object key, ColumnFilter columns,
                                                      Slices slices, boolean reversed)
    {
        SinglePartitionReadCommand base = (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build();
        return SinglePartitionReadCommand.create(cfs.metadata(), now, columns, RowFilter.none(), DataLimits.NONE,
                                                 base.partitionKey(), new ClusteringIndexSliceFilter(slices, reversed));
    }

    private static Slices slices(ClusteringComparator comparator, long... bounds)
    {
        Slices.Builder builder = new Slices.Builder(comparator);
        for (int i = 0; i < bounds.length; i += 2)
            builder.add(Slice.make(comparator.make(bounds[i]), comparator.make(bounds[i + 1])));
        return builder.build();
    }

    private static ColumnMetadata column(ColumnFamilyStore cfs, String name)
    {
        return cfs.metadata().getColumn(ByteBufferUtil.bytes(name));
    }

    /** Each read shape over one partition: full, slice, reversed, names and several slices. */
    private void assertEveryShapeMatches(ColumnFamilyStore cfs, long now, long pk, int sstableLegs)
    {
        TableMetadata metadata = cfs.metadata();
        List<Supplier<SinglePartitionReadCommand>> reads = new ArrayList<>();
        reads.add(fullPartition(cfs, now, pk));
        reads.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).fromIncl(3L).toExcl(17L).build());
        reads.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).reverse().build());
        reads.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).fromIncl(2L).toIncl(12L).reverse().build());
        reads.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).includeRow(1L).includeRow(6L).includeRow(13L).build());
        reads.add(() -> command(cfs, now, pk, ColumnFilter.all(metadata), slices(metadata.comparator, 0, 4, 8, 11, 15, 30), false));
        reads.add(() -> command(cfs, now, pk, ColumnFilter.all(metadata), slices(metadata.comparator, 0, 4, 8, 11, 15, 30), true));
        for (Supplier<SinglePartitionReadCommand> read : reads)
        {
            try
            {
                if (sstableLegs == 0)
                    assertMemtableOnlyMatches(cfs, read);
                else if (sstablesRead(cfs, read, false) > 0)
                    assertCursorReadMatchesIterator(cfs, read);
                else
                    assertCursorReadMatchesIteratorReadingNoSSTable(cfs, read);
                assertReplicaResponseBuildsNoMemtableObject(cfs, read);
            }
            catch (AssertionError e)
            {
                throw new AssertionError(read.get().toCQLString() + ": " + e.getMessage(), e);
            }
        }
    }

    // ---------------------------------------------------------------- the memtable leg's own filtering

    /** A partition only the memtable holds, every read shape: the lone memtable leg. */
    @Test
    public void memtableOnlyPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, v2 text, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (2, 0, 0)");
        flush();
        execute("UPDATE %s SET s = 'static' WHERE pk = 1");
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (1, ?, ?, ?, ?)", ck, ck, "mem" + ck, map("a", ck, "b", ck + 1));
        execute("DELETE FROM %s WHERE pk = 1 AND ck >= 5 AND ck < 9");
        execute("DELETE v2 FROM %s WHERE pk = 1 AND ck = 10");
        execute("DELETE m['a'] FROM %s WHERE pk = 1 AND ck = 11");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 30, 30) USING TTL 100000");
        long now = FBUtilities.nowInSeconds();
        assertEveryShapeMatches(cfs, now, 1L, 0);
    }

    /**
     * A partition only the memtable holds, with a complex deletion and a row deletion equal to its
     * partition deletion (one batch: the same local deletion time).  The iterator path reads the
     * lone memtable as is and keeps both, where a merge of several sources drops a deletion that
     * does not supersede the active one; the lone memtable leg is read as stored.
     */
    @Test
    public void memtableOnlyDeletionsEqualToThePartitionDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, l list<bigint>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (2, 0, 0)");
        flush();
        // the list overwrite at 2001 stores a complex deletion at 2000
        execute("BEGIN UNLOGGED BATCH " +
                "DELETE FROM %1$s USING TIMESTAMP 2000 WHERE pk = 1; " +
                "UPDATE %1$s USING TIMESTAMP 2001 SET l = [1, 2] WHERE pk = 1 AND ck = 1; " +
                "DELETE FROM %1$s USING TIMESTAMP 2000 WHERE pk = 1 AND ck = 2; " +
                "INSERT INTO %1$s (pk, ck, v1) VALUES (1, 3, 3) USING TIMESTAMP 2001; " +
                "APPLY BATCH");
        long now = FBUtilities.nowInSeconds();
        assertEveryShapeMatches(cfs, now, 1L, 0);
    }

    /**
     * Range tombstones in the memtable that overlap, with the same deletion time (the boundary
     * between them changes nothing and {@code RowAndDeletionMergeIterator} drops it,
     * CASSANDRA-14894), and one newer range inside an older one, over memtable and sstable rows.
     */
    @Test
    public void overlappingMemtableRangeTombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 1000", ck, "sst" + ck);
        flush();
        // one batch: the same local deletion time, so the boundary between the two ranges is a no-op
        execute("BEGIN UNLOGGED BATCH USING TIMESTAMP 2000 " +
                "DELETE FROM %1$s WHERE pk = 1 AND ck >= 2 AND ck < 10; " +
                "DELETE FROM %1$s WHERE pk = 1 AND ck >= 6 AND ck < 14; " +
                "APPLY BATCH");
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = 1 AND ck >= 16 AND ck < 25");
        execute("DELETE FROM %s USING TIMESTAMP 4000 WHERE pk = 1 AND ck >= 18 AND ck < 20");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 7, 'over-rt') USING TIMESTAMP 2500");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 19, 'under-rt') USING TIMESTAMP 3500");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 22, 'over-rt') USING TIMESTAMP 3500");
        long now = FBUtilities.nowInSeconds();
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), 1, 1);
        assertEveryShapeMatches(cfs, now, 1L, 1);
    }

    /** A memtable partition deletion with rows above and below it, and a static row. */
    @Test
    public void memtablePartitionDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 1000", ck, "sst" + ck);
        flush();
        for (long ck = 0; ck < 20; ck += 3)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 1500", ck, "mem-old" + ck);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1");
        for (long ck = 1; ck < 20; ck += 4)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 2500", ck, "mem-new" + ck);
        execute("UPDATE %s USING TIMESTAMP 1800 SET s = 'under' WHERE pk = 1");
        execute("DELETE FROM %s USING TIMESTAMP 1900 WHERE pk = 1 AND ck >= 3 AND ck < 8");
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = 1 AND ck >= 9 AND ck < 12");
        long now = FBUtilities.nowInSeconds();
        assertEveryShapeMatches(cfs, now, 1L, 1);
        // the partition deletion is newer than the sstable: the iterator path skips it
        execute("UPDATE %s USING TIMESTAMP 2600 SET s = 'over' WHERE pk = 2");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (2, 1, 'x') USING TIMESTAMP 2600");
        assertEveryShapeMatches(cfs, now, 2L, 0);
    }

    /**
     * Columns fetched but not queried, and sub-selections of a map: the memtable leg keeps the
     * cells {@code BTreeRow.filter} keeps, with the skipped (empty) value for a cell that is not
     * queried, and drops a cell that is not queried and older than the row.
     */
    @Test
    public void fetchedButNotQueriedAndSubSelections() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 12; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (1, ?, ?, ?, ?) USING TIMESTAMP 1000", ck, ck, "sst" + ck, map("a", ck, "c", ck));
        flush();
        for (long ck = 0; ck < 12; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (1, ?, ?, ?, ?) USING TIMESTAMP 2000", ck, ck + 50, "mem" + ck, map("a", ck + 5, "b", ck));
        // cells older than their row: not queried, so dropped
        for (long ck = 1; ck < 12; ck += 4)
        {
            execute("INSERT INTO %s (pk, ck) VALUES (1, ?) USING TIMESTAMP 3000", ck);
            execute("UPDATE %s USING TIMESTAMP 900 SET v2 = 'old', m['z'] = 1 WHERE pk = 1 AND ck = ?", ck);
        }
        long now = FBUtilities.nowInSeconds();
        TableMetadata metadata = cfs.metadata();
        ColumnMetadata m = column(cfs, "m");
        ColumnFilter queriedV1 = ColumnFilter.allRegularColumnsBuilder(metadata, false).add(column(cfs, "v1")).build();
        ColumnFilter elementA = ColumnFilter.allRegularColumnsBuilder(metadata, false)
                                            .select(m, CellPath.create(ByteBufferUtil.bytes("a"))).build();
        ColumnFilter sliceAToB = ColumnFilter.selectionBuilder()
                                             .slice(m, CellPath.create(ByteBufferUtil.bytes("a")), CellPath.create(ByteBufferUtil.bytes("b")))
                                             .add(column(cfs, "v2")).build();
        for (ColumnFilter columns : new ColumnFilter[]{ queriedV1, elementA, sliceAToB })
        {
            for (boolean reversed : new boolean[]{ false, true })
            {
                Supplier<SinglePartitionReadCommand> read = () -> command(cfs, now, 1L, columns, Slices.ALL, reversed);
                assertMemtableMergedMatches(cfs, read, 1, 1);
            }
        }
    }

    /**
     * A simple column and a map dropped and added again: cells from before the drop in the
     * sstables, cells after it in the memtable.  The dropped columns make every memtable row go
     * through the leg's {@code BTreeRow.filter}.  A schema change switches the memtable and
     * flushes it in the background, so the test waits for that flush: the rows written before it
     * are read from an sstable, and no source changes between the two paths' reads.
     */
    @Test
    public void droppedAndReAddedColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (1, ?, ?, ?, ?)", ck, "sst" + ck, "sst" + ck, map("a", ck));
        flush();
        for (long ck = 0; ck < 10; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (1, ?, ?, ?, ?)", ck, "mem" + ck, "mem" + ck, map("b", ck));
        // a dropped collection not added again stays on the iterator path (CASSANDRA-21607)
        execute("ALTER TABLE %s DROP v2");
        execute("ALTER TABLE %s DROP m");
        execute("ALTER TABLE %s ADD m map<text, bigint>");
        execute("ALTER TABLE %s ADD v2 text");
        // flushes finish in order, so this one, of another partition, waits for the ones the
        // schema changes started
        execute("INSERT INTO %s (pk, ck, v1) VALUES (99, 0, 'other')");
        flush();
        assertTrue("a memtable is still flushing", cfs.getTracker().getView().flushingMemtables.isEmpty());
        for (long ck = 1; ck < 10; ck += 3)
            execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (1, ?, ?, ?, ?)", ck, "new" + ck, "new" + ck, map("c", ck));
        execute("UPDATE %s SET m['d'] = 1 WHERE pk = 1 AND ck = 2");
        long now = FBUtilities.nowInSeconds();
        // the sstable of partition 99 holds no row of partition 1
        int sstableLegs = cfs.getLiveSSTables().size() - 1;
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), sstableLegs, 1);
        assertMemtableMergedMatches(cfs, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).reverse().build(), sstableLegs, 1);
        assertMemtableMergedMatches(cfs, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).columns("v1").build(), sstableLegs, 1);
        assertEveryShapeMatches(cfs, now, 1L, sstableLegs);
    }

    /** TTLs in the memtable read before and after they expire, on cells, elements and liveness. */
    @Test
    public void memtableTtlsBeforeAndAfterExpiry() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 16; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?)", ck, "sst" + ck);
        flush();
        for (long ck = 0; ck < 16; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, m) VALUES (1, ?, ?, ?) USING TTL 100", ck, "ttl" + ck, map("a", ck));
        execute("UPDATE %s USING TTL 100 SET m['x'] = 1 WHERE pk = 1 AND ck = 3");
        long now = FBUtilities.nowInSeconds();
        assertEveryShapeMatches(cfs, now, 1L, 1);
        assertEveryShapeMatches(cfs, now + 1000, 1L, 1);
    }

    /** Counter cells in the memtable merged with counter shards in sstables, and alone. */
    @Test
    public void memtableCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 12; ck++)
            execute("UPDATE %s SET c = c + 1 WHERE pk = 1 AND ck = ?", ck);
        flush();
        for (long ck = 0; ck < 12; ck += 2)
            execute("UPDATE %s SET c = c + 5 WHERE pk = 1 AND ck = ?", ck);
        execute("UPDATE %s SET c = c + 3 WHERE pk = 2 AND ck = 1");
        long now = FBUtilities.nowInSeconds();
        assertMemtableMergedMatches(cfs, fullPartition(cfs, now, 1L), 1, 1);
        assertEveryShapeMatches(cfs, now, 1L, 1);
        assertEveryShapeMatches(cfs, now, 2L, 0);
    }

    // ---------------------------------------------------------------- mutation tests

    /**
     * The negative tests (corruption discipline, applied to the adapter):
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
            fail("differential harness FAILED TO DETECT " + label + " corruption of the memtable adapter");
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
