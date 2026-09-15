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
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * M3.0 (CASSANDRA-20428, Phase 4 scaffolding): differential scenarios for the query shapes M3.1+
 * (limit-bounded production) and M3.2 (filter-aware production) will change, run TODAY over the
 * existing merge-everything-then-filter path. Every scenario must pass NOW — the top-of-stack
 * {@code limits().filter} / {@code rowFilter().filter} transformations are untouched by the cursor
 * path, so byte-identity is expected — and this corpus is the safety net those increments extend:
 * when a production bound below the merge starts stopping row production early, or a pushed-down
 * filter starts abandoning row groups, these exact scenarios must STILL byte-match, page-match and
 * (via {@link ScanMetricsParityTest}) metric-match.
 *
 * Scenario map (each name states the boundary it pins down; workload details in
 * {@link #loadMergedWorkload}):
 * <ul>
 *   <li>LIMIT cutoffs over a 3-source merged partition: mid-partition; exactly AT and exactly
 *       PAST a shadowed (row-tombstoned) row — dead rows are emitted but not counted by the
 *       {@code CQLCounter}; landing ON a row whose cells merge from all three legs
 *       (the mid-row-group case); landing on a live row INSIDE an open range tombstone (the
 *       stream then ends mid-open-RT and {@code RTBoundCloser} synthesizes the artificial close —
 *       the design's mandatory "LIMIT lands inside an open RT" scenario); and LIMIT over gcable
 *       tombstones interleaved with live rows (purged by {@code withoutPurgeableTombstones}
 *       between the merge and the counter — the design's verified-but-must-test interaction).</li>
 *   <li>RowFilter shapes, per the design's cursor-evaluable split: {@code SimpleExpression} with
 *       a value-comparing operator on a clustering column and on a simple regular column (the
 *       shapes M3.2 will push down), a filter whose column is shadowed by a range tombstone on
 *       some rows (liveness-at-nowInSec must gate the value comparison), and the exotic/fallback
 *       shapes that must NEVER be pushed down: multi-cell CONTAINS (needs materialized
 *       {@code ComplexColumnData}) and complex-map MAP_ELEMENT (by-path cell lookup).</li>
 *   <li>Paging over the merged partition through the real {@code SinglePartitionPager}: page
 *       boundaries crossing tombstone/RT regions, and a PER PARTITION LIMIT resumed mid-partition
 *       across pages — per-page records AND serialized paging states must match between paths.</li>
 * </ul>
 */
public class LateMaterializationDifferentialTest extends CursorReadDifferentialTester
{
    private static final int ROWS = 128;
    private static final int SOURCES = 3;
    /** ck range [16,32) where the final round writes ONLY v1, so the merged row assembles v1 from
     *  leg 2, v2 from leg 1 and flag from leg 0 — a genuine multi-leg row group. */
    private static final int GROUP_LO = 16, GROUP_HI = 32;
    // deletes issued by the final round (later timestamps than all inserts)
    private static final int ROW_TOMBSTONE_CK = 3;
    private static final int CELL_TOMBSTONE_CK = 5;
    private static final int RT1_LO = 40, RT1_HI = 48;
    private static final int RT2_LO = 60, RT2_HI = 70;
    private static final int REINSERT_NO_FLAG_CK = 62; // re-written after RT2, without flag
    private static final int REINSERT_WITH_FLAG_CK = 66; // re-written after RT2, with flag
    /** live rows: 128 - 1 (row tombstone) - 8 (RT1) - 10 (RT2) + 2 (re-inserts) */
    private static final int LIVE_ROWS = ROWS - 1 - (RT1_HI - RT1_LO) - (RT2_HI - RT2_LO) + 2;

    // ---------------------------------------------------------------- LIMIT scenarios
    // (each asserts byte-identity AND that the M3.1 production bound actually engaged — the bound
    // stops production exactly where the top-level counter stops consuming, so a silently
    // non-engaging bound would be byte-identical and only the counter guard catches it)

    @Test
    public void limitLandsMidPartition() throws Throwable
    {
        Workload w = loadMergedWorkload();
        assertCursorMatchesWithBoundEngaged(w.cfs, w.limited(10));
    }

    @Test
    public void limitLandsAtShadowedRowBoundary() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // live rows in clustering order are ck 0,1,2 then DEAD ck 3, then ck 4...
        // LIMIT 3: the counter stops exactly on the row BEFORE the shadowed row
        assertCursorMatchesWithBoundEngaged(w.cfs, w.limited(3));
        // LIMIT 4: the counter must step OVER the emitted-but-uncounted dead row and stop on ck 4
        assertCursorMatchesWithBoundEngaged(w.cfs, w.limited(4));
    }

    @Test
    public void limitLandsMidRowGroup() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // counted rows: ck 0,1,2 (3) then ck 4..20 (17) -> the 20th counted row is ck 20, inside
        // [GROUP_LO, GROUP_HI) where the merged row assembles cells from all three legs
        int limit = 20;
        assertTrue("limit must land inside the multi-leg row-group band", 20 >= GROUP_LO && 20 < GROUP_HI);
        assertCursorMatchesWithBoundEngaged(w.cfs, w.limited(limit));
    }

    @Test
    public void limitLandsInsideOpenRangeTombstone() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // counted live rows: 39 up to ck 39 (0..39 minus dead ck 3), 12 more at ck 48..59 (=51),
        // then ck 62 — the re-inserted live row INSIDE the open RT [60,70) — is the 52nd.
        // The counter stops there, mid-open-RT; RTBoundCloser must synthesize the same close on
        // both paths.
        assertCursorMatchesWithBoundEngaged(w.cfs, w.limited(52));
    }

    @Test
    public void limitOverGcableTombstones() throws Throwable
    {
        // gc_grace_seconds = 0: the second source's row tombstones are purgeable at read time and
        // are dropped by withoutPurgeableTombstones BETWEEN the merge and the counter — the counter
        // never sees them, but the merge does. LIMIT must land identically on both paths.
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0 AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 64; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck * 10, "base-" + ck);
        flush();
        for (long ck = 0; ck < 64; ck += 4)
            execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", ck); // every 4th row: gcable tombstone
        for (long ck = 1; ck < 64; ck += 4)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck * 100); // overlap on survivors
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds() + 10; // strictly after the deletes' ldt => purgeable
        Supplier<SinglePartitionReadCommand> cmd =
            () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).withLimit(10).build();
        // workload sanity: the tombstones must actually be purged at this nowInSec
        UntypedResultSet visible = execute("SELECT * FROM %s WHERE pk = 0");
        assertEquals(48, visible.size());
        assertCursorMatchesWithBoundEngaged(cfs, cmd);
    }

    // ---------------------------------------------------------------- RowFilter scenarios

    @Test
    public void filterOnClusteringColumn() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // SimpleExpression + value-comparing operator + clustering column: the cursor-evaluable
        // shape M3.2 evaluates from the descriptor's clustering wire bytes
        assertCursorMatchesWithoutBound(w.cfs, w.filtered("ck", Operator.EQ, 7L));
    }

    @Test
    public void filterOnRegularColumnDiscardingMost() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // v1 of ck 7 after the final overwrite round: matches exactly one merged row
        assertCursorMatchesWithoutBound(w.cfs, w.filtered("v1", Operator.EQ, v1Value(7, SOURCES - 1)));
    }

    @Test
    public void filterOnCellShadowedByRangeTombstone() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // flag=1 was written in round 0 for every row and never rewritten; the RT2 re-inserts wrote
        // it back only for ck 66. So the filter keeps every live row EXCEPT ck 62, whose flag cell
        // is RT-shadowed while the row itself is live — the filter's cell-liveness-at-nowInSec
        // check is what drops it, exactly the semantics a pushed-down evaluation must twin.
        UntypedResultSet withFlag = execute("SELECT ck FROM %s WHERE pk = 0 AND flag = 1 ALLOW FILTERING");
        assertEquals(LIVE_ROWS - 1, withFlag.size());
        assertCursorMatchesWithoutBound(w.cfs, w.filtered("flag", Operator.EQ, 1));
    }

    @Test
    public void filterMultiCellContainsFallbackShape() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // multi-cell set CONTAINS: needs a materialized ComplexColumnData => per the design this
        // shape must keep today's materialize-then-filter behavior forever (fallback, not pushdown)
        assertCursorMatchesWithoutBound(w.cfs, w.filtered("tags", Operator.CONTAINS, "common"));
    }

    @Test
    public void filterComplexMapElementFallbackShape() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // MAP_ELEMENT on a complex (multi-cell) map column: by-path cell lookup, fallback shape
        ColumnMetadata mapColumn = w.cfs.metadata().getColumn(ByteBufferUtil.bytes("m"));
        Supplier<SinglePartitionReadCommand> cmd = () -> {
            SinglePartitionReadCommand base = w.base();
            RowFilter rowFilter = RowFilter.create(true);
            rowFilter.addMapEquality(mapColumn, ByteBufferUtil.bytes("stable"), Operator.EQ, ByteBufferUtil.bytes("x"));
            return SinglePartitionReadCommand.create(w.cfs.metadata(), base.nowInSec(),
                                                     ColumnFilter.all(w.cfs.metadata()), rowFilter,
                                                     DataLimits.NONE, base.partitionKey(),
                                                     base.clusteringIndexFilter());
        };
        // sanity: the map element actually matches a known subset (every live ck % 4 == 0 row that
        // got the round-1 map write and survived the deletes)
        UntypedResultSet matching = execute("SELECT ck FROM %s WHERE pk = 0 AND m['stable'] = 'x' ALLOW FILTERING");
        assertFalse("map-element filter matches nothing — workload shape drifted", matching.isEmpty());
        assertCursorMatchesWithoutBound(w.cfs, cmd);
    }

    // ---------------------------------------------------------------- paging scenarios

    @Test
    public void pagingAcrossMergedPartition() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // The unfiltered pager counts EMITTED rows (assumeLiveData=true), so the row-tombstoned
        // ck 3 counts alongside the 111 live rows: 112 emitted rows = exactly 7 x 16, which forces
        // an 8th, empty page for the pager to detect exhaustion. Boundaries cross the shadowed
        // row, both RT regions and the multi-leg row-group band. Note the M3.1 bound twins each
        // page command's LIVE-row counter, which stops at-or-after the pager's own EMITTED-row
        // counter — dead rows make the bound produce slightly more than the page returns, never
        // less (the conservative direction).
        long stopped = CursorReads.mergesStoppedByLimit();
        assertPagedReadMatchesIterator(w.cfs, w.base, 16, 8);
        assertTrue("M3.1 production bound did not engage during the paged sequence",
                   CursorReads.mergesStoppedByLimit() > stopped);
    }

    @Test
    public void perPartitionLimitWithPagingResume() throws Throwable
    {
        Workload w = loadMergedWorkload();
        // PER PARTITION LIMIT 25 paged 10 at a time: page 3 resumes mid-partition with only 5
        // remaining in the partition — the paging-seeded counter shape M3.1's bound must twin
        Supplier<SinglePartitionReadCommand> cmd = () -> {
            SinglePartitionReadCommand base = w.base();
            return SinglePartitionReadCommand.create(w.cfs.metadata(), base.nowInSec(),
                                                     ColumnFilter.all(w.cfs.metadata()), RowFilter.none(),
                                                     DataLimits.cqlLimits(DataLimits.NO_LIMIT, 25),
                                                     base.partitionKey(), base.clusteringIndexFilter());
        };
        long stopped = CursorReads.mergesStoppedByLimit();
        assertPagedReadMatchesIterator(w.cfs, cmd, 10, 3);
        assertTrue("M3.1 production bound did not engage during the paging-resume sequence",
                   CursorReads.mergesStoppedByLimit() > stopped);
    }

    // ---------------------------------------------------------------- M3.1 engagement guards

    /** Byte-identity plus the M3.1 engagement guard: the production bound must actually have
     *  stopped a merge during the cursor run (limit strictly below the scenario's live-row count),
     *  because a bound that silently fails to engage produces byte-identical output — only the
     *  {@link CursorReads#mergesStoppedByLimit} counter can tell the difference. */
    private void assertCursorMatchesWithBoundEngaged(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long stopped = CursorReads.mergesStoppedByLimit();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertTrue("M3.1 production bound did not engage for this LIMIT scenario (silent non-engagement)",
                   CursorReads.mergesStoppedByLimit() > stopped);
    }

    /** Byte-identity plus the inverse guard: the bound must NOT have engaged (RowFilter present,
     *  or no limit) — engaging where the gate says not to is exactly the under-production risk
     *  {@code CursorReads.limitBoundFor}'s conservative scoping exists to prevent. */
    private void assertCursorMatchesWithoutBound(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long stopped = CursorReads.mergesStoppedByLimit();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertEquals("M3.1 production bound must not engage for this scenario",
                     stopped, CursorReads.mergesStoppedByLimit());
    }

    // ---------------------------------------------------------------- workload

    private static long v1Value(long ck, int round)
    {
        return ck * 10 + round;
    }

    protected final class Workload
    {
        final ColumnFamilyStore cfs;
        final long nowInSec;
        final Supplier<SinglePartitionReadCommand> base;

        Workload(ColumnFamilyStore cfs, long nowInSec)
        {
            this.cfs = cfs;
            this.nowInSec = nowInSec;
            this.base = () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(nowInSec).build();
        }

        SinglePartitionReadCommand base()
        {
            return base.get();
        }

        Supplier<SinglePartitionReadCommand> limited(int limit)
        {
            return () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(nowInSec)
                                                          .withLimit(limit).build();
        }

        Supplier<SinglePartitionReadCommand> filtered(String column, Operator op, Object value)
        {
            return () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(nowInSec)
                                                          .filterOn(column, op, value).build();
        }
    }

    /**
     * One 128-row partition merged from 3 fully-overlapping sstables:
     * round 0 writes v1, v2 and flag for every row; round 1 overwrites v1/v2 and adds multi-cell
     * collections (tags on even cks, m on ck%4==0); round 2 overwrites v1/v2 EXCEPT in
     * [{@value #GROUP_LO},{@value #GROUP_HI}) where it writes only v1 (multi-leg row groups), then
     * issues the deletes: a row tombstone (ck {@value #ROW_TOMBSTONE_CK}), a cell tombstone on v2
     * (ck {@value #CELL_TOMBSTONE_CK}), range tombstones [{@value #RT1_LO},{@value #RT1_HI}) and
     * [{@value #RT2_LO},{@value #RT2_HI}), and two post-RT re-inserts inside the second range
     * (ck {@value #REINSERT_NO_FLAG_CK} without flag, ck {@value #REINSERT_WITH_FLAG_CK} with it).
     */
    private Workload loadMergedWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, flag int, " +
                    "tags set<text>, m map<text, text>, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < SOURCES; round++)
        {
            for (long ck = 0; ck < ROWS; ck++)
            {
                boolean finalRound = round == SOURCES - 1;
                if (round == 0)
                    execute("INSERT INTO %s (pk, ck, v1, v2, flag) VALUES (0, ?, ?, ?, 1)",
                            ck, v1Value(ck, round), "r" + round + "-" + ck);
                else if (finalRound && ck >= GROUP_LO && ck < GROUP_HI)
                    execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, v1Value(ck, round));
                else
                    execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)",
                            ck, v1Value(ck, round), "r" + round + "-" + ck);
                if (round == 1 && ck % 2 == 0)
                    execute("UPDATE %s SET tags = tags + ? WHERE pk = 0 AND ck = ?",
                            set("common", "t" + ck), ck);
                if (round == 1 && ck % 4 == 0)
                    execute("UPDATE %s SET m = m + ? WHERE pk = 0 AND ck = ?",
                            map("stable", "x", "k" + ck, "v" + ck), ck);
            }
            if (round == SOURCES - 1)
            {
                execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", (long) ROW_TOMBSTONE_CK);
                execute("DELETE v2 FROM %s WHERE pk = 0 AND ck = ?", (long) CELL_TOMBSTONE_CK);
                execute("DELETE FROM %s WHERE pk = 0 AND ck >= ? AND ck < ?", (long) RT1_LO, (long) RT1_HI);
                execute("DELETE FROM %s WHERE pk = 0 AND ck >= ? AND ck < ?", (long) RT2_LO, (long) RT2_HI);
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)",
                        (long) REINSERT_NO_FLAG_CK, v1Value(REINSERT_NO_FLAG_CK, 9), "reinsert-" + REINSERT_NO_FLAG_CK);
                execute("INSERT INTO %s (pk, ck, v1, v2, flag) VALUES (0, ?, ?, ?, 1)",
                        (long) REINSERT_WITH_FLAG_CK, v1Value(REINSERT_WITH_FLAG_CK, 9), "reinsert-" + REINSERT_WITH_FLAG_CK);
            }
            flush();
        }
        assertEquals("expected exactly SOURCES overlapping sstables", SOURCES, cfs.getLiveSSTables().size());
        assertEquals("memtable must be empty so every read merges all sstable legs",
                     0, cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize());

        // workload-shape sanity: the scenarios' counted-row arithmetic above depends on these
        UntypedResultSet all = execute("SELECT ck, v1, v2 FROM %s WHERE pk = 0");
        assertEquals(LIVE_ROWS, all.size());
        UntypedResultSet grouped = execute("SELECT v1, v2 FROM %s WHERE pk = 0 AND ck = 20");
        assertEquals("row group band must take v1 from the final leg", v1Value(20, SOURCES - 1), grouped.one().getLong("v1"));
        assertEquals("row group band must take v2 from the middle leg", "r1-20", grouped.one().getString("v2"));
        UntypedResultSet cellTombstoned = execute("SELECT v1, v2 FROM %s WHERE pk = 0 AND ck = ?", (long) CELL_TOMBSTONE_CK);
        assertFalse("v2 must be cell-tombstoned", cellTombstoned.one().has("v2"));
        UntypedResultSet reinserted = execute("SELECT ck FROM %s WHERE pk = 0 AND ck >= ? AND ck < ?",
                                              (long) RT2_LO, (long) RT2_HI);
        assertEquals("exactly the two re-inserted rows survive inside RT2", 2, reinserted.size());

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }
}
