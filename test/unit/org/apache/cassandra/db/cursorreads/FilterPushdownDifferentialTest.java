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

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * M3.2a (CASSANDRA-20428): differential scenarios for RowFilter pushdown's engagement gate
 * ({@code CursorReads.filterPushdownFor}) and the partition-level short-circuit (static-column /
 * partition-key-column expressions evaluated against the merged static row inside
 * {@code CursorReads.mergeLegs}, skipping the row-group merge entirely when they fail).
 *
 * Three scenario families, each asserting byte-identity PLUS the counter guards that make silent
 * non-engagement (or silent wrong-engagement) fail loudly:
 * <ul>
 *   <li><b>Engaged, partition kept</b>: strict, reconciliation-free SIMPLE expressions on the
 *       static column / partition key that PASS — {@code filterPushdownEngaged} must advance,
 *       {@code partitionsSkippedByFilter} must NOT (a wrongly-firing short-circuit would drop rows
 *       the top filter keeps — under-production the byte comparison also catches).</li>
 *   <li><b>Engaged, partition dropped</b>: the same shapes with a failing verdict — BOTH counters
 *       must advance AND {@code unfilteredsMaterialized} must not move during the cursor run (the
 *       short-circuit's whole point is that the dropped partition's rows are never materialized;
 *       byte-identity alone cannot see the difference). Includes the merged-static trap: the
 *       filtered static value is overwritten across legs, so a per-leg (non-merged) evaluation
 *       would compute the wrong verdict. Metrics parity on the drop case — with static-row
 *       tombstones present — is asserted via {@link ScanMetricsCapture}, proving the M3.2 plan's
 *       claim that dropped partitions need no accumulator: the iterator path's filter closes the
 *       partition before its rows ever reach the (pull-based) {@code withMetricsRecording} stage,
 *       so neither path counts them, while the static row — pulled BY the partition-level check —
 *       is counted by both.</li>
 *   <li><b>Mandatory fallback</b>: every unpushable shape from the M3.2 plan — multi-cell
 *       CONTAINS, complex MAP_ELEMENT, counter column, needsReconciliation/non-strict filters,
 *       active query-size-tracking config, active purgeable-tombstone-recording config — must
 *       leave {@code filterPushdownEngaged} untouched while the query is STILL served (cursor legs
 *       advance; for the counter table, the whole-table gate falls back unchanged instead). The
 *       "fallback never blocks the cursor path" property is asserted, not assumed.</li>
 * </ul>
 */
public class FilterPushdownDifferentialTest extends CursorReadDifferentialTester
{
    private static final int ROWS = 64;
    private static final long S_ROUND0 = 100L;
    private static final long S_FINAL = 200L;
    private static final long S_NEVER = 999L;

    // ---------------------------------------------------------------- engaged, partition kept

    @Test
    public void staticFilterKeepsPartition() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        // s was S_ROUND0 in leg 0 and overwritten to S_FINAL in leg 1: the verdict REQUIRES the
        // merged static row (leg 0 alone would say false)
        assertPushdownKeepsPartition(w.cfs, w.strictFiltered(f -> f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(S_FINAL))));
    }

    @Test
    public void partitionKeyFilterKeepsPartition() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        assertPushdownKeepsPartition(w.cfs, w.strictFiltered(f -> f.add(w.col("pk"), Operator.EQ, ByteBufferUtil.bytes(0L))));
    }

    @Test
    public void staticFilterKeepsPartitionWithRowLevelExpressionRidingAlong() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        // partition-level PASSES, row-level (v1) drops most rows — at production since M3.2c
        // (regular-column pushdown), above the merge before that; either way the context
        // attaches, the short-circuit must not fire, and byte identity pins both verdicts
        assertPushdownKeepsPartition(w.cfs, w.strictFiltered(f -> {
            f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(S_FINAL));
            f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(70L));
        }));
    }

    // ---------------------------------------------------------------- engaged, partition dropped

    @Test
    public void staticFilterDropsPartition() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        assertPushdownSkipsPartition(w.cfs, w.strictFiltered(f -> f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(S_NEVER))));
    }

    @Test
    public void staleStaticValueMustNotMatch() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        // the merged-static trap in the DROP direction: S_ROUND0 is leg 0's (superseded) value —
        // a per-leg evaluation would say "match" and KEEP the partition's rows; the merged static
        // row says s = S_FINAL, so the top filter drops the partition and the short-circuit must
        // reach the identical verdict
        assertPushdownSkipsPartition(w.cfs, w.strictFiltered(f -> f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(S_ROUND0))));
    }

    @Test
    public void partitionKeyFilterDropsPartition() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        assertPushdownSkipsPartition(w.cfs, w.strictFiltered(f -> f.add(w.col("pk"), Operator.EQ, ByteBufferUtil.bytes(5L))));
    }

    @Test
    public void failingStaticFilterWithRowLevelExpressionStillShortCircuits() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        // partition-level FAILS while a row-level expression is present: RowFilter.filter's own
        // partition check runs before any row-level evaluation, so the short-circuit fires exactly
        // the same regardless of the row-level expression
        assertPushdownSkipsPartition(w.cfs, w.strictFiltered(f -> {
            f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(S_NEVER));
            f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(70L));
        }));
    }

    @Test
    public void staticFilterDropsPartitionWithMemtableLeg() throws Throwable
    {
        // the final static overwrite lives in the MEMTABLE (unflushed): the merged static row the
        // short-circuit evaluates is assembled across sstable AND memtable legs (M2.3 adapter)
        Workload w = loadStaticWorkload(false);
        execute("INSERT INTO %s (pk, s) VALUES (0, ?)", 300L);
        assertPushdownSkipsPartition(w.cfs, w.strictFiltered(f -> f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(S_FINAL))));
        assertPushdownKeepsPartition(w.cfs, w.strictFiltered(f -> f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(300L))));
    }

    /**
     * The metrics-parity proof for the M3.2 plan's "no accumulator needed for dropped partitions"
     * claim, on a dropped partition that ALSO carries static-row tombstones (the s2 static cell is
     * deleted in leg 1) and row tombstones (every 8th row): the static row IS pulled by the top
     * filter's partition-level check on both paths (its dead cell reaches
     * {@code withMetricsRecording}), while the dropped rows are pulled on NEITHER (iterator path:
     * the filter closes the partition first; cursor path: the short-circuit never materializes
     * them) — so every scan metric, warning and histogram observation must be identical.
     */
    @Test
    public void scanMetricsParityOnDroppedPartitionWithStaticTombstones() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        Supplier<SinglePartitionReadCommand> cmd =
            w.strictFiltered(f -> f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(S_NEVER)));

        DatabaseDescriptor.setCursorReadsEnabled(false);
        ScanMetricsCapture.Snapshot iterator = ScanMetricsCapture.capture(w.cfs, () -> consume(cmd.get()));

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = cmd.get();
            assertTrue("scenario is not supported by the cursor read gate; this parity run would " +
                       "silently compare iterator vs iterator",
                       CursorReads.isReadSupported(probe, w.cfs, liveSSTablesFor(w.cfs, probe)));
            long servedBefore = CursorReads.sstableLegsServed();
            long skippedBefore = CursorReads.partitionsSkippedByFilter();
            ScanMetricsCapture.Snapshot cursor = ScanMetricsCapture.capture(w.cfs, () -> consume(cmd.get()));
            assertTrue("cursor path did not actually serve any sstable leg (silent fallback?)",
                       CursorReads.sstableLegsServed() - servedBefore > 0);
            assertTrue("the partition-level short-circuit did not fire — this parity run proved nothing",
                       CursorReads.partitionsSkippedByFilter() - skippedBefore > 0);

            ScanMetricsCapture.assertParity("iterator vs cursor, partition-level drop with static tombstones",
                                            iterator, cursor);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    // ---------------------------------------------------------------- mandatory fallback shapes

    @Test
    public void multiCellContainsFallsBack() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        // SIMPLE kind but a COMPLEX column: multi-cell set CONTAINS needs a materialized
        // ComplexColumnData — unpushable by construction, forever
        assertServedWithoutPushdown(w.cfs, w.strictFiltered(f -> f.add(w.col("tags"), Operator.CONTAINS, ByteBufferUtil.bytes("common"))));
    }

    @Test
    public void complexMapElementFallsBack() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        // MAP_ELEMENT kind (by-path complex cell lookup) — not Kind.SIMPLE, unpushable
        assertServedWithoutPushdown(w.cfs, w.strictFiltered(f -> f.addMapEquality(w.col("m"), ByteBufferUtil.bytes("stable"),
                                                                                  Operator.EQ, ByteBufferUtil.bytes("x"))));
    }

    @Test
    public void counterColumnFallsBack() throws Throwable
    {
        // counter TABLES are outside the cursor read gate entirely (CursorReads.isReadSupported),
        // so this asserts the whole-table fallback stays unchanged AND the pushdown gate's
        // defensive counter check never engages — there is no cursor-served leg to assert on here,
        // by design
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 8; ck++)
            execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", ck + 1, 0L, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        ColumnMetadata counterColumn = cfs.metadata().getColumn(ByteBufferUtil.bytes("c"));
        Supplier<SinglePartitionReadCommand> cmd = () -> {
            SinglePartitionReadCommand base = (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build();
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(counterColumn, Operator.EQ, ByteBufferUtil.bytes(3L));
            return SinglePartitionReadCommand.create(cfs.metadata(), base.nowInSec(), ColumnFilter.all(cfs.metadata()),
                                                     rowFilter, DataLimits.NONE, base.partitionKey(),
                                                     base.clusteringIndexFilter());
        };
        long engagedBefore = CursorReads.filterPushdownEngaged();
        assertFallsBackUnchanged(cfs, cmd);
        assertEquals("pushdown must never engage on a counter-column filter",
                     engagedBefore, CursorReads.filterPushdownEngaged());
    }

    @Test
    public void needsReconciliationFilterFallsBack() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        // RowFilter.create(true): needsReconciliation — the purge-before-evaluate semantics
        // differ (RowFilter.filter skips the row purge), gated out wholesale. This is also the
        // shape Util.cmd's filterOn builds, so every pre-M3.2 filtered differential scenario
        // remains a fallback scenario by construction.
        assertServedWithoutPushdown(w.cfs, w.filtered(f -> f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(70L))));
        // and the non-strict variant: needsReconciliation + an intersection on two mutable
        // regular columns downgrades to a union at the coordinator (CASSANDRA-19018) — isStrict()
        // false, unpushable
        Supplier<SinglePartitionReadCommand> nonStrict = w.filtered(f -> {
            f.add(w.col("v1"), Operator.EQ, ByteBufferUtil.bytes(70L));
            f.add(w.col("v2"), Operator.EQ, ByteBufferUtil.bytes("r1-7"));
        });
        assertFalse("workload drift: this filter shape must be non-strict", nonStrict.get().rowFilter().isStrict());
        assertServedWithoutPushdown(w.cfs, nonStrict);
    }

    @Test
    public void activeSizeTrackingConfigFallsBack() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        DataStorageSpec.LongBytesBound originalWarn = DatabaseDescriptor.getLocalReadSizeWarnThreshold();
        DatabaseDescriptor.setLocalReadSizeWarnThreshold(new DataStorageSpec.LongBytesBound("100MiB"));
        try
        {
            Supplier<SinglePartitionReadCommand> pushable =
                w.strictFiltered(f -> f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(S_FINAL)));
            // tracking engages per command (trackWarnings) + config (threshold set): the exact
            // condition ReadCommand.shouldTrackSize applies
            Supplier<SinglePartitionReadCommand> tracked = () -> {
                SinglePartitionReadCommand c = pushable.get();
                c.trackWarnings();
                return c;
            };
            assertServedWithoutPushdown(w.cfs, tracked);
        }
        finally
        {
            DatabaseDescriptor.setLocalReadSizeWarnThreshold(originalWarn);
        }
    }

    @Test
    public void activePurgeableTombstoneRecordingConfigFallsBack() throws Throwable
    {
        Workload w = loadStaticWorkload(true);
        Config.TombstonesMetricGranularity original = DatabaseDescriptor.getPurgeableTobmstonesMetricGranularity();
        DatabaseDescriptor.setPurgeableTobmstonesMetricGranularity(Config.TombstonesMetricGranularity.row);
        try
        {
            assertServedWithoutPushdown(w.cfs, w.strictFiltered(f -> f.add(w.col("s"), Operator.EQ, ByteBufferUtil.bytes(S_FINAL))));
        }
        finally
        {
            DatabaseDescriptor.setPurgeableTobmstonesMetricGranularity(original);
        }
    }

    // ---------------------------------------------------------------- guards

    /** Byte-identity + engagement guard for a query the gate must push down WITHOUT dropping the
     *  partition: the context attached ({@code filterPushdownEngaged} advanced — a silently
     *  disengaging gate is byte-identical and fails exactly this), the short-circuit did NOT fire.
     *  Every caller in this file uses an UNLIMITED command ({@code strictFiltered}), so
     *  {@code mergesStoppedByLimit} must stay untouched regardless of M3.2d's filter+limit
     *  composition (no {@code DataLimits.Counter} bound exists for an unlimited query in the first
     *  place) — the dedicated composed engagement scenarios live in
     *  {@code LimitedFilterPushdownDifferentialTest}. */
    private void assertPushdownKeepsPartition(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long engagedBefore = CursorReads.filterPushdownEngaged();
        long skippedBefore = CursorReads.partitionsSkippedByFilter();
        long limitStoppedBefore = CursorReads.mergesStoppedByLimit();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertTrue("filter pushdown did not engage for a pushable filter shape (silent non-engagement)",
                   CursorReads.filterPushdownEngaged() > engagedBefore);
        assertEquals("partition-level short-circuit fired for a partition the filter KEEPS",
                     skippedBefore, CursorReads.partitionsSkippedByFilter());
        assertEquals("no production limit bound exists for an unlimited command",
                     limitStoppedBefore, CursorReads.mergesStoppedByLimit());
    }

    /** Byte-identity + the full drop-side guard set: context attached, short-circuit fired, and
     *  ZERO unfiltereds materialized by the cursor runs — the allocation property that IS the
     *  short-circuit's payoff, invisible to the byte comparison. */
    private void assertPushdownSkipsPartition(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long engagedBefore = CursorReads.filterPushdownEngaged();
        long skippedBefore = CursorReads.partitionsSkippedByFilter();
        long materializedBefore = CursorReads.unfilteredsMaterialized();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertTrue("filter pushdown did not engage for a pushable filter shape (silent non-engagement)",
                   CursorReads.filterPushdownEngaged() > engagedBefore);
        assertTrue("partition-level short-circuit did not fire for a partition the filter DROPS",
                   CursorReads.partitionsSkippedByFilter() > skippedBefore);
        assertEquals("a skipped partition must not materialize a single unfiltered",
                     materializedBefore, CursorReads.unfilteredsMaterialized());
    }

    /** The fallback contract, asserted not assumed: the pushdown gate declined (counter untouched)
     *  yet the query was STILL cursor-served ({@code assertCursorReadMatchesIterator}'s own
     *  {@code sstableLegsServed} guard) with byte-identical results. */
    private void assertServedWithoutPushdown(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> cmd)
    {
        long engagedBefore = CursorReads.filterPushdownEngaged();
        long skippedBefore = CursorReads.partitionsSkippedByFilter();
        assertCursorReadMatchesIterator(cfs, cmd);
        assertEquals("pushdown engaged for a mandatory-fallback filter shape",
                     engagedBefore, CursorReads.filterPushdownEngaged());
        assertEquals("short-circuit fired for a mandatory-fallback filter shape",
                     skippedBefore, CursorReads.partitionsSkippedByFilter());
    }

    /** Full executeLocally consumption — metric recording happens partly at iteration, partly at close. */
    private void consume(SinglePartitionReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
        }
    }

    // ---------------------------------------------------------------- workload

    protected final class Workload
    {
        final ColumnFamilyStore cfs;
        final long nowInSec;

        Workload(ColumnFamilyStore cfs, long nowInSec)
        {
            this.cfs = cfs;
            this.nowInSec = nowInSec;
        }

        ColumnMetadata col(String name)
        {
            ColumnMetadata column = cfs.metadata().getColumn(ByteBufferUtil.bytes(name));
            assertTrue("no such column: " + name, column != null);
            return column;
        }

        /** A pk=0 read carrying a STRICT, reconciliation-free filter (the pushable flavor —
         *  {@code RowFilter.create(false)}; note Util's own filterOn builds create(true)). */
        Supplier<SinglePartitionReadCommand> strictFiltered(Consumer<RowFilter> expressions)
        {
            return filtered(false, expressions);
        }

        /** A pk=0 read carrying a needsReconciliation filter ({@code RowFilter.create(true)} —
         *  the mandatory-fallback flavor). */
        Supplier<SinglePartitionReadCommand> filtered(Consumer<RowFilter> expressions)
        {
            return filtered(true, expressions);
        }

        private Supplier<SinglePartitionReadCommand> filtered(boolean needsReconciliation, Consumer<RowFilter> expressions)
        {
            return () -> {
                SinglePartitionReadCommand base =
                    (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(nowInSec).build();
                RowFilter rowFilter = RowFilter.create(needsReconciliation);
                expressions.accept(rowFilter);
                return SinglePartitionReadCommand.create(cfs.metadata(), base.nowInSec(),
                                                         ColumnFilter.all(cfs.metadata()), rowFilter,
                                                         DataLimits.NONE, base.partitionKey(),
                                                         base.clusteringIndexFilter());
            };
        }
    }

    /**
     * One {@value #ROWS}-row partition (pk=0) over two overlapping sstables, with the static
     * surface the partition-level scenarios need:
     * leg 0 writes s = {@value #S_ROUND0}, s2 = 'keep', every row (v1 = ck*10, v2, flag) plus
     * multi-cell collections (tags on even cks, m on ck%4==0);
     * leg 1 OVERWRITES s to {@value #S_FINAL} (the merged-static trap: evaluating either leg's
     * static row alone gives the wrong verdict for both the keep and the drop direction), DELETES
     * the s2 static cell (a static-row tombstone for the metrics-parity drop scenario),
     * row-tombstones every 8th ck and overwrites v1 on survivors.
     * When {@code flushFinal} is false the leg-1 writes stay in the MEMTABLE (the M2.3
     * memtable-leg merge shape); the caller then owns any further memtable writes.
     */
    private Workload loadStaticWorkload(boolean flushFinal) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s bigint static, s2 text static, " +
                    "v1 bigint, v2 text, tags set<text>, m map<text, text>, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, s, s2) VALUES (0, ?, ?)", S_ROUND0, "keep");
        for (long ck = 0; ck < ROWS; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck * 10, "r0-" + ck);
            if (ck % 2 == 0)
                execute("UPDATE %s SET tags = tags + ? WHERE pk = 0 AND ck = ?", set("common", "t" + ck), ck);
            if (ck % 4 == 0)
                execute("UPDATE %s SET m = m + ? WHERE pk = 0 AND ck = ?", map("stable", "x", "k" + ck, "v" + ck), ck);
        }
        flush();

        execute("INSERT INTO %s (pk, s) VALUES (0, ?)", S_FINAL);
        execute("DELETE s2 FROM %s WHERE pk = 0");
        for (long ck = 0; ck < ROWS; ck++)
        {
            if (ck % 8 == 0)
                execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", ck);
            else
                execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck * 10);
        }
        if (flushFinal)
        {
            flush();
            assertEquals("expected exactly 2 overlapping sstables", 2, cfs.getLiveSSTables().size());
            assertEquals("memtable must be empty so every read merges only sstable legs",
                         0, cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize());
        }
        else
        {
            assertEquals("expected exactly 1 sstable under the memtable", 1, cfs.getLiveSSTables().size());
            assertTrue("leg-1 writes must still live in the memtable",
                       cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize() > 0);
        }

        // workload-shape sanity: the static value the scenarios test against, and the tombstones
        // the parity scenario depends on
        UntypedResultSet statics = execute("SELECT s, s2 FROM %s WHERE pk = 0 LIMIT 1");
        assertEquals(S_FINAL, statics.one().getLong("s"));
        assertFalse("s2 must be tombstoned", statics.one().has("s2"));
        assertEquals("v1 filter target must match exactly one row",
                     1, execute("SELECT ck FROM %s WHERE pk = 0 AND v1 = 70 ALLOW FILTERING").size());

        return new Workload(cfs, FBUtilities.nowInSeconds());
    }
}
