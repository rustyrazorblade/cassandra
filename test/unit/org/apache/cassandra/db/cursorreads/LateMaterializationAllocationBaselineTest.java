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

import java.lang.management.ManagementFactory;
import java.util.function.Supplier;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * M3.0/M3.1 (CASSANDRA-20428, Phase 4): allocation measurement of the LIMIT, RowFilter, paging
 * and replica-response-serialization shapes. Started life (M3.0) as a pure BASELINE recording of
 * the pre-M3.1 eager-production behavior; with M3.1 (the limit-driven production bound) landed,
 * the LIMIT and paging materialization assertions are converted — per the baseline's own embedded
 * instructions — into the increment's PAYOFF GATES: materialization on bounded shapes must scale
 * with rows RETURNED, engagement is guarded through {@link CursorReads#mergesStoppedByLimit} in
 * both directions (bounded shapes trip it, unlimited/filtered shapes must not), and the filter
 * shapes keep their M3.0 eager-production assertions untouched until M3.2. Allocation ratios stay
 * logged (not asserted) exactly as in {@link OverlapMergeAllocationBaselineTest}, the M2.0
 * precedent.
 *
 * WHAT IS BEING MEASURED, per shape, over a {@value #WIDE_ROWS}-row partition merged from
 * {@value #SOURCES} fully-overlapping sstables (the design's target shape — "a LIMIT 1 over the
 * wide partition still merges and materializes the entire queried slice today"):
 * <ul>
 *   <li><b>Allocation per pass</b>, iterator vs cursor path, via the established
 *       {@code com.sun.management.ThreadMXBean} warmup + min-of-N methodology with exact
 *       production-counter accounting in both directions (silent-fallback guard). NOTE: on
 *       LIMIT/paged shapes the cursor path is EXPECTED to allocate more than the iterator path
 *       today — the iterator path stops consuming at the limit while the cursor merge is eager —
 *       that asymmetry IS the baseline finding M3.1 exists to fix, so it is recorded, not
 *       asserted against.</li>
 *   <li><b>Materialization amplification</b>: {@link CursorReads#unfilteredsMaterialized} per pass
 *       (merged winners actually materialized) vs rows the query returns after limits/filters.
 *       LIMIT 1 returning 1 row while materializing {@value #WIDE_ROWS} is the precise avoidable
 *       cost of seam (iii); asserted EXACTLY EQUAL to today's eager behavior so any change —
 *       intended (M3.1 lands) or accidental — fails this test and forces the numbers to be
 *       re-recorded.</li>
 *   <li><b>Response serialization</b> ({@code LocalDataResponse.build}'s exact encoding via
 *       {@code serializerForIntraNode().serialize}) measured as its own pass shape — the first
 *       time response serialization enters the allocation scope (Phase 0 explicitly excluded it;
 *       seam (iv)'s step-change will be judged on this shape).</li>
 *   <li><b>Memtable-hot overlap</b> (2 sstable legs + 1 memtable leg in the same cursor merge) —
 *       closing the gate-scope gap M2 carried forward for memtable-hot workloads.</li>
 * </ul>
 */
public class LateMaterializationAllocationBaselineTest extends CursorReadDifferentialTester
{
    protected static final int WIDE_ROWS = 1024;
    protected static final int SOURCES = 3;
    private static final int WARMUP_PASSES = 12;
    private static final int MEASURED_PASSES = 8;
    protected static final int PAGE_SIZE = 128;

    /** Blackhole so pass consumption cannot be dead-code-eliminated. */
    private static volatile long sink;

    @FunctionalInterface
    private interface Pass
    {
        /** Runs one workload pass; returns the "output size" for the shape (rows returned for
         *  consume/pager passes, serialized bytes for serialization passes). */
        long run() throws Throwable;
    }

    private static final class ShapeResult
    {
        long iteratorBest, cursorBest;
        long materializedPerPass, output;
        long sstableLegsPerPass, memtableLegsPerPass, mergesPerPass;
        /** M3.1: merges whose production the limit bound stopped, per pass — the engagement guard
         *  (>0 where the bound must engage, ==0 where it must not). */
        long stoppedByLimitPerPass;

        double ratio()
        {
            return (double) cursorBest / iteratorBest;
        }
    }

    // ---------------------------------------------------------------- scenarios

    @Test
    public void limitShapesOverWideMergedPartition() throws Throwable
    {
        ColumnFamilyStore cfs = loadWideWorkload(SOURCES, true);
        long now = FBUtilities.nowInSeconds();

        Supplier<SinglePartitionReadCommand> fullCmd = wideCommand(cfs, now, -1, null, null);
        Supplier<SinglePartitionReadCommand> limit16Cmd = wideCommand(cfs, now, 16, null, null);
        Supplier<SinglePartitionReadCommand> limit1Cmd = wideCommand(cfs, now, 1, null, null);
        ShapeResult full = measureShape(cfs, fullCmd, () -> consumePass(fullCmd));
        ShapeResult limit16 = measureShape(cfs, limit16Cmd, () -> consumePass(limit16Cmd));
        ShapeResult limit1 = measureShape(cfs, limit1Cmd, () -> consumePass(limit1Cmd));

        logShape("full slice (reference)", full, "rows");
        logShape("LIMIT 16", limit16, "rows");
        logShape("LIMIT 1", limit1, "rows");

        // M3.1 PAYOFF GATE (converted from the M3.0 baseline recording, per its own instructions):
        // the merge's production is bounded by the query's limit, so materialization scales with
        // rows RETURNED, not with the slice size. Exact equality — this workload has no dead rows
        // or markers, so the bound must stop after precisely `limit` merged rows.
        assertEquals("full-slice pass must return every row", WIDE_ROWS, full.output);
        assertEquals(16, limit16.output);
        assertEquals(1, limit1.output);
        assertEquals("unlimited production must stay eager (full slice materialized)",
                     WIDE_ROWS, full.materializedPerPass);
        assertEquals("LIMIT 16 must materialize exactly the rows it returns (M3.1 bound)",
                     16, limit16.materializedPerPass);
        assertEquals("LIMIT 1 must materialize exactly the row it returns (M3.1 bound)",
                     1, limit1.materializedPerPass);
        // engagement guard: the bound tripped exactly once per bounded merge, never on the
        // unbounded shape (a silently non-engaging bound is byte-identical — only this catches it)
        assertEquals("the production bound must not engage without a limit", 0, full.stoppedByLimitPerPass);
        assertEquals("the production bound did not stop the LIMIT 16 merge", 1, limit16.stoppedByLimitPerPass);
        assertEquals("the production bound did not stop the LIMIT 1 merge", 1, limit1.stoppedByLimitPerPass);
    }

    @Test
    public void filterShapesOverWideMergedPartition() throws Throwable
    {
        ColumnFamilyStore cfs = loadWideWorkload(SOURCES, true);
        long now = FBUtilities.nowInSeconds();

        // v3 == ck for every row: EQ discards all but one row AFTER full merge+materialization
        Supplier<SinglePartitionReadCommand> discardMostCmd = wideCommand(cfs, now, -1, "v3", 7);
        // v8 == 42 on every row: same evaluation cost, nothing discarded
        Supplier<SinglePartitionReadCommand> discardNoneCmd = wideCommand(cfs, now, -1, "v8", 42);
        // clustering-predicate flavor (evaluated against clustering values, not cell values)
        Supplier<SinglePartitionReadCommand> clusteringCmd = wideCommand(cfs, now, -1, "ck", 500L);
        ShapeResult discardMost = measureShape(cfs, discardMostCmd, () -> consumePass(discardMostCmd));
        ShapeResult discardNone = measureShape(cfs, discardNoneCmd, () -> consumePass(discardNoneCmd));
        ShapeResult clustering = measureShape(cfs, clusteringCmd, () -> consumePass(clusteringCmd));

        logShape("filter v3=7 (discards ~all)", discardMost, "rows");
        logShape("filter v8=42 (discards none)", discardNone, "rows");
        logShape("filter ck=500 (clustering, discards ~all)", clustering, "rows");

        assertEquals("v3=7 must match exactly one row", 1, discardMost.output);
        assertEquals("v8=42 must match every row", WIDE_ROWS, discardNone.output);
        assertEquals("ck=500 must match exactly one row", 1, clustering.output);
        for (ShapeResult r : new ShapeResult[]{ discardMost, discardNone, clustering })
        {
            // NOTE: wideCommand()'s filterOn builds a needsReconciliation RowFilter
            // (AbstractReadCommandBuilder.filter = RowFilter.create(true)) — the MANDATORY-FALLBACK
            // shape filterPushdownFor always declines, on purpose or not (M3.2a-c never touched
            // this file). These three scenarios measure that fallback shape's eager cost, not the
            // M3.2-pushable shape; the pushable+limited shape is measured separately below by
            // {@link #filteredLimitShapesOverWideMergedPartition}, which builds a genuinely STRICT
            // filter the way the cursorreads differential Workload classes do.
            assertEquals("filters run ABOVE the merge today: materialization must not depend on " +
                         "the filter verdicts (this fallback shape is out of M3.2's pushable scope " +
                         "entirely, not just deferred to a later sub-increment)",
                         WIDE_ROWS, r.materializedPerPass);
            // M3.1 scoping guard: the limit bound must NOT engage when a RowFilter is present AND
            // is not fully pushable — the filter drops rows below the counter, so a filter-blind
            // bound could under-produce
            assertEquals("the production bound must not engage for a filtered query whose filter " +
                         "is not fully pushable",
                         0, r.stoppedByLimitPerPass);
        }
    }

    @Test
    public void filteredLimitShapesOverWideMergedPartition() throws Throwable
    {
        // M3.2d: the headline new performance win of M3.2 — before this increment, a filtered+
        // limited query got NO production bound at all (limitBoundFor declined on any non-empty
        // rowFilter; row-level filter pushdown declined on any non-unlimited query), so it paid the
        // SAME full-WIDE_ROWS eager materialization cost as an unfiltered, unlimited read. This
        // scenario builds a genuinely STRICT, fully-pushable filter directly (NOT via wideCommand's
        // needsReconciliation-shaped filterOn — see filterShapesOverWideMergedPartition's note)
        // keeping the SECOND HALF of the partition (v3 >= 500): a LIMIT must now walk PAST the
        // first 500 filter-dropped rows to find its matches, composing both bounds.
        ColumnFamilyStore cfs = loadWideWorkload(SOURCES, true);
        long now = FBUtilities.nowInSeconds();
        long keepFrom = 500;
        long expectedMatches = WIDE_ROWS - keepFrom;

        Supplier<SinglePartitionReadCommand> filteredUnlimitedCmd = filteredLimitedCommand(cfs, now, -1, keepFrom);
        Supplier<SinglePartitionReadCommand> filteredLimit16Cmd = filteredLimitedCommand(cfs, now, 16, keepFrom);
        Supplier<SinglePartitionReadCommand> filteredLimit1Cmd = filteredLimitedCommand(cfs, now, 1, keepFrom);
        ShapeResult filteredUnlimited = measureShape(cfs, filteredUnlimitedCmd, () -> consumePass(filteredUnlimitedCmd));
        ShapeResult filteredLimit16 = measureShape(cfs, filteredLimit16Cmd, () -> consumePass(filteredLimit16Cmd));
        ShapeResult filteredLimit1 = measureShape(cfs, filteredLimit1Cmd, () -> consumePass(filteredLimit1Cmd));

        logShape("filtered (v3>=500) unlimited (reference)", filteredUnlimited, "rows");
        logShape("filtered (v3>=500) + LIMIT 16", filteredLimit16, "rows");
        logShape("filtered (v3>=500) + LIMIT 1", filteredLimit1, "rows");

        assertEquals("filtered-unlimited pass must return every matching row", expectedMatches, filteredUnlimited.output);
        assertEquals(16, filteredLimit16.output);
        assertEquals(1, filteredLimit1.output);

        // M3.2d PAYOFF GATE: unfilteredsMaterialized() counts only rows that actually reach the
        // OUTPUT (endRow()/addRow() on the base materializer) — a filter-dropped or filter-
        // abandoned row never does, regardless of how many rows the merge had to WALK past to find
        // its keepers (that walk cost shows up in the allocation numbers logged above, not this
        // exact-materialization count). So materialization must equal EXACTLY the rows the query
        // returns, identical in shape to the pure-LIMIT M3.1 gate above, now proven for the
        // filtered+limited composition specifically.
        assertEquals("unlimited filtered production must stay eager (every matching row materialized)",
                     expectedMatches, filteredUnlimited.materializedPerPass);
        assertEquals("filtered LIMIT 16 must materialize exactly the rows it returns (M3.2d bound)",
                     16, filteredLimit16.materializedPerPass);
        assertEquals("filtered LIMIT 1 must materialize exactly the row it returns (M3.2d bound)",
                     1, filteredLimit1.materializedPerPass);
        // engagement guard: the composed bound tripped exactly once per bounded merge, never on
        // the unbounded (unlimited) shape — a silently non-engaging bound is byte-identical and
        // only this catches it
        assertEquals("the composed bound must not engage without a limit", 0, filteredUnlimited.stoppedByLimitPerPass);
        assertEquals("the composed bound did not stop the filtered LIMIT 16 merge", 1, filteredLimit16.stoppedByLimitPerPass);
        assertEquals("the composed bound did not stop the filtered LIMIT 1 merge", 1, filteredLimit1.stoppedByLimitPerPass);
    }

    @Test
    public void pagedReadOverWideMergedPartition() throws Throwable
    {
        ColumnFamilyStore cfs = loadWideWorkload(SOURCES, true);
        long now = FBUtilities.nowInSeconds();

        Supplier<SinglePartitionReadCommand> baseCmd = wideCommand(cfs, now, -1, null, null);
        ShapeResult paged = measureShape(cfs, baseCmd, () -> pagerPass(cfs, baseCmd, PAGE_SIZE));
        logShape(String.format("paged read (page size %d, full partition)", PAGE_SIZE), paged, "rows");

        assertEquals("paged sequence must return every row exactly once", WIDE_ROWS, paged.output);
        // M3.1 PAYOFF GATE (converted from the M3.0 baseline recording): each page's merge now
        // stops at the page's row budget instead of re-merging to the partition end. What remains
        // per page is the PRE-slice walk to the resume point — rows the slicer discards below the
        // counter: under BIG the cursor still walks from the partition head (no intra-partition
        // seek), so page k materializes k*PAGE_SIZE skipped rows plus the PAGE_SIZE it returns
        // (head-sum: 128+256+...+1024 = 4608, down from the baseline's 8 x 1024 = 8192); under
        // BTI the M1/M2.2 seek skips the prefix too, leaving ~rows-returned plus the block-
        // granularity overshoot (the BTI subclass records the measured value). Asserted exactly
        // so any production drift re-records the number.
        assertEquals("per-page materialization under the M3.1 production bound changed",
                     expectedPagedMaterialization(), paged.materializedPerPass);
        assertEquals("every data page's merge must be stopped by its page-limit bound",
                     WIDE_ROWS / PAGE_SIZE, paged.stoppedByLimitPerPass);
    }

    @Test
    public void responseSerializationOverWideMergedPartition() throws Throwable
    {
        ColumnFamilyStore cfs = loadWideWorkload(SOURCES, true);
        long now = FBUtilities.nowInSeconds();

        Supplier<SinglePartitionReadCommand> baseCmd = wideCommand(cfs, now, -1, null, null);
        ShapeResult consume = measureShape(cfs, baseCmd, () -> consumePass(baseCmd));
        ShapeResult serialize = measureShape(cfs, baseCmd, () -> serializePass(baseCmd));

        logShape("full slice, consume only (reference)", consume, "rows");
        logShape("full slice + intra-node response serialization", serialize, "bytes");
        logger.info("response-serialization overhead per pass (allocation on top of local " +
                    "execution): iterator ~{}B cursor ~{}B",
                    serialize.iteratorBest - consume.iteratorBest,
                    serialize.cursorBest - consume.cursorBest);

        assertTrue("serialization pass produced no bytes", serialize.output > 0);
        assertEquals(WIDE_ROWS, serialize.materializedPerPass);
        assertEquals("the M3.1 production bound must not engage on an unlimited read",
                     0, serialize.stoppedByLimitPerPass);
    }

    @Test
    public void memtableHotOverlap() throws Throwable
    {
        // 2 flushed rounds + 1 UNFLUSHED round: the memtable leg joins the cursor merge (M2.3),
        // closing the memtable-hot measurement gap M2 carried forward
        ColumnFamilyStore cfs = loadWideWorkload(SOURCES, false);
        long now = FBUtilities.nowInSeconds();
        assertTrue("memtable must carry the final round's data",
                   cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize() > 0);

        Supplier<SinglePartitionReadCommand> baseCmd = wideCommand(cfs, now, -1, null, null);
        ShapeResult result = measureShape(cfs, baseCmd, () -> consumePass(baseCmd));
        logShape("memtable-hot full slice (2 sstable legs + 1 memtable leg)", result, "rows");

        assertEquals(WIDE_ROWS, result.output);
        assertEquals(WIDE_ROWS, result.materializedPerPass);
        assertTrue("memtable leg did not join the cursor merge (silent fallback to the object path?)",
                   result.memtableLegsPerPass > 0);
        assertEquals("the M3.1 production bound must not engage on an unlimited read",
                     0, result.stoppedByLimitPerPass);
    }

    /** Merged unfiltereds materialized by one full paging sequence under the M3.1 production
     *  bound, per format: BIG has no intra-partition seek, so page k still walks (and
     *  materializes) the k*PAGE_SIZE rows before its resume point, then the bound stops it after
     *  the PAGE_SIZE rows the page returns — the head-sum PAGE_SIZE*(1+2+...+pages). The BTI
     *  subclass overrides with its seek-assisted expectation (rows returned + block-granularity
     *  overshoot). */
    protected long expectedPagedMaterialization()
    {
        long pages = WIDE_ROWS / PAGE_SIZE;
        return PAGE_SIZE * pages * (pages + 1) / 2;
    }

    // ---------------------------------------------------------------- passes

    private long consumePass(Supplier<SinglePartitionReadCommand> command) throws Throwable
    {
        long rows = 0;
        SinglePartitionReadCommand cmd = command.get();
        try (ReadExecutionController controller = cmd.executionController();
             UnfilteredPartitionIterator partitions = cmd.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        if (partition.next().isRow())
                            rows++;
                }
            }
        }
        sink += rows;
        return rows;
    }

    private long pagerPass(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command, int pageSize) throws Throwable
    {
        long rows = 0;
        SinglePartitionPager pager = (SinglePartitionPager) command.get().getPager(null, ProtocolVersion.CURRENT);
        int guard = 0;
        while (!pager.isExhausted())
        {
            assertTrue("paging did not terminate", guard++ < 1000);
            try (ReadExecutionController controller = pager.executionController();
                 UnfilteredPartitionIterator page = pager.fetchPageUnfiltered(cfs.metadata(), pageSize, controller))
            {
                while (page.hasNext())
                {
                    try (UnfilteredRowIterator partition = page.next())
                    {
                        while (partition.hasNext())
                            if (partition.next().isRow())
                                rows++;
                    }
                }
            }
        }
        sink += rows;
        return rows;
    }

    private long serializePass(Supplier<SinglePartitionReadCommand> command) throws Throwable
    {
        SinglePartitionReadCommand cmd = command.get();
        try (ReadExecutionController controller = cmd.executionController();
             UnfilteredPartitionIterator partitions = cmd.executeLocally(controller);
             DataOutputBuffer buffer = new DataOutputBuffer())
        {
            // the exact LocalDataResponse.build encoding (see ReadResponse.LocalDataResponse)
            UnfilteredPartitionIterators.serializerForIntraNode()
                                        .serialize(partitions, cmd.columnFilter(), buffer, MessagingService.current_version);
            sink += buffer.getLength();
            return buffer.getLength();
        }
    }

    // ---------------------------------------------------------------- measurement core
    // (the OverlapMergeAllocationBaselineTest methodology, generalized over pass shapes: one
    // instrumented cursor pass establishes the exact per-pass counter expectations, then warmup
    // both paths, min-of-N measured passes, exact accounting in both directions)

    private ShapeResult measureShape(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> probe, Pass pass) throws Throwable
    {
        com.sun.management.ThreadMXBean bean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", bean != null);

        ShapeResult result = new ShapeResult();
        try
        {
            // silent-fallback guard at the source: the measured shape must pass the gate (probed
            // once, OUTSIDE the measured passes, so the probe itself contaminates neither path)
            DatabaseDescriptor.setCursorReadsEnabled(true);
            SinglePartitionReadCommand probeCommand = probe.get();
            assertTrue("command shape is not supported by the cursor read gate — this measurement " +
                       "would silently compare iterator vs iterator: " + probeCommand,
                       CursorReads.isReadSupported(probeCommand, cfs, liveSSTablesFor(cfs, probeCommand)));

            // instrumented cursor pass: per-pass counter expectations + materialization accounting
            long legsBefore = CursorReads.sstableLegsServed();
            long memBefore = CursorReads.memtableLegsCursorMerged();
            long mergesBefore = CursorReads.cursorMergesServed();
            long materializedBefore = CursorReads.unfilteredsMaterialized();
            long stoppedBefore = CursorReads.mergesStoppedByLimit();
            result.output = pass.run();
            result.sstableLegsPerPass = CursorReads.sstableLegsServed() - legsBefore;
            result.memtableLegsPerPass = CursorReads.memtableLegsCursorMerged() - memBefore;
            result.mergesPerPass = CursorReads.cursorMergesServed() - mergesBefore;
            result.materializedPerPass = CursorReads.unfilteredsMaterialized() - materializedBefore;
            result.stoppedByLimitPerPass = CursorReads.mergesStoppedByLimit() - stoppedBefore;
            assertTrue("cursor pass served no sstable legs (silent fallback?)", result.sstableLegsPerPass > 0);
            assertTrue("multi-leg pass did not route through the cursor merge", result.mergesPerPass > 0);

            // JIT warmup, both paths
            DatabaseDescriptor.setCursorReadsEnabled(false);
            for (int i = 0; i < WARMUP_PASSES; i++)
                pass.run();
            DatabaseDescriptor.setCursorReadsEnabled(true);
            long warmBefore = CursorReads.sstableLegsServed();
            for (int i = 0; i < WARMUP_PASSES; i++)
                pass.run();
            assertEquals("cursor warmup did not serve the expected sstable legs (silent fallback?)",
                         WARMUP_PASSES * result.sstableLegsPerPass,
                         CursorReads.sstableLegsServed() - warmBefore);

            // measured, iterator path — must never consult the cursor path
            DatabaseDescriptor.setCursorReadsEnabled(false);
            long servedBeforeIterator = CursorReads.sstableLegsServed();
            long missedBeforeIterator = CursorReads.sstableLegsWithoutPartition();
            result.iteratorBest = measureBest(bean, pass);
            assertEquals("iterator-path measurement unexpectedly ran the cursor path",
                         servedBeforeIterator, CursorReads.sstableLegsServed());
            assertEquals("iterator-path measurement unexpectedly consulted the cursor path",
                         missedBeforeIterator, CursorReads.sstableLegsWithoutPartition());

            // measured, cursor path — exact per-pass leg accounting
            DatabaseDescriptor.setCursorReadsEnabled(true);
            long servedBeforeCursor = CursorReads.sstableLegsServed();
            long memBeforeCursor = CursorReads.memtableLegsCursorMerged();
            result.cursorBest = measureBest(bean, pass);
            assertEquals("cursor measurement did not serve the expected sstable legs (silent fallback?)",
                         MEASURED_PASSES * result.sstableLegsPerPass,
                         CursorReads.sstableLegsServed() - servedBeforeCursor);
            assertEquals("cursor measurement did not merge the expected memtable legs",
                         MEASURED_PASSES * result.memtableLegsPerPass,
                         CursorReads.memtableLegsCursorMerged() - memBeforeCursor);

            assertTrue("no allocation measured", result.iteratorBest > 0 && result.cursorBest > 0);
            return result;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private long measureBest(com.sun.management.ThreadMXBean bean, Pass pass) throws Throwable
    {
        long tid = Thread.currentThread().getId();
        long best = Long.MAX_VALUE;
        for (int i = 0; i < MEASURED_PASSES; i++)
        {
            long before = bean.getThreadAllocatedBytes(tid);
            pass.run();
            best = Math.min(best, bean.getThreadAllocatedBytes(tid) - before);
        }
        return best;
    }

    private void logShape(String label, ShapeResult r, String outputUnit)
    {
        logger.info("late-materialization baseline [{}]:\n" +
                    "  allocation/pass: iterator={}B cursor={}B ratio={}\n" +
                    "  merged unfiltereds materialized/pass={} output={} {} (amplification={})\n" +
                    "  legs/pass: sstable={} memtable={} merges={}",
                    label, r.iteratorBest, r.cursorBest, String.format("%.4f", r.ratio()),
                    r.materializedPerPass, r.output, outputUnit,
                    outputUnit.equals("rows") && r.output > 0
                        ? String.format("%.1fx", (double) r.materializedPerPass / r.output) : "n/a",
                    r.sstableLegsPerPass, r.memtableLegsPerPass, r.mergesPerPass);
    }

    // ---------------------------------------------------------------- workload

    private Supplier<SinglePartitionReadCommand> wideCommand(ColumnFamilyStore cfs, long now,
                                                             int limit, String filterColumn, Object filterValue)
    {
        return () -> {
            AbstractReadCommandBuilder builder = Util.cmd(cfs, 0L).withNowInSeconds(now);
            if (limit > 0)
                builder = builder.withLimit(limit);
            if (filterColumn != null)
                builder = builder.filterOn(filterColumn, Operator.EQ, filterValue);
            return (SinglePartitionReadCommand) builder.build();
        };
    }

    /**
     * M3.2d: a genuinely STRICT, reconciliation-free, fully-pushable filter ({@code v3 >= keepFrom}
     * on the regular int column that mirrors {@code ck}) combined with an optional LIMIT — built
     * directly via {@code RowFilter.create(false)} + {@code SinglePartitionReadCommand.create},
     * mirroring the {@code Workload} pattern the cursorreads differential test classes use, since
     * {@link #wideCommand}'s {@code filterOn} builds the UNPUSHABLE {@code needsReconciliation}
     * shape (see {@code filterShapesOverWideMergedPartition}'s note).
     */
    private Supplier<SinglePartitionReadCommand> filteredLimitedCommand(ColumnFamilyStore cfs, long now,
                                                                        int limit, long keepFrom)
    {
        DataLimits limits = limit > 0 ? DataLimits.cqlLimits(limit) : DataLimits.NONE;
        ColumnMetadata v3 = cfs.metadata().getColumn(ByteBufferUtil.bytes("v3"));
        return () -> {
            SinglePartitionReadCommand base = (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build();
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(v3, Operator.GTE, ByteBufferUtil.bytes((int) keepFrom));
            return SinglePartitionReadCommand.create(cfs.metadata(), base.nowInSec(),
                                                     ColumnFilter.all(cfs.metadata()), rowFilter,
                                                     limits, base.partitionKey(), base.clusteringIndexFilter());
        };
    }

    /**
     * One {@value #WIDE_ROWS}-row partition written in {@value #SOURCES} fully-overlapping rounds
     * (shadow-heavy: natural timestamps, each round rewrites all 8 value columns). v3 mirrors ck
     * (per-row-unique filter target); v8 is the constant 42 (match-everything filter target).
     * When {@code flushAll} is false the final round stays in the memtable (memtable-hot variant).
     */
    private ColumnFamilyStore loadWideWorkload(int sources, boolean flushAll) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, " +
                    "v1 bigint, v2 text, v3 int, v4 text, v5 double, v6 bigint, v7 text, v8 int, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < sources; round++)
        {
            for (long ck = 0; ck < WIDE_ROWS; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, v3, v4, v5, v6, v7, v8) " +
                        "VALUES (0, ?, ?, ?, ?, ?, ?, ?, ?, 42)",
                        ck, ck * 10 + round, "r" + round + "-" + ck, (int) ck,
                        "text-" + round + "-" + ck, round + 0.5, ck + round, "t" + round + "-" + ck);
            if (flushAll || round < sources - 1)
                flush();
        }
        int expectedSSTables = flushAll ? sources : sources - 1;
        assertEquals("unexpected sstable count", expectedSSTables, cfs.getLiveSSTables().size());
        if (flushAll)
            assertEquals("memtable must be empty for the flushed variant",
                         0, cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize());

        // workload sanity: last round's values must win everywhere (overlap is real)
        UntypedResultSet winner = execute("SELECT v1 FROM %s WHERE pk = 0 AND ck = 7");
        assertEquals(7 * 10 + (sources - 1), winner.one().getLong("v1"));
        UntypedResultSet count = execute("SELECT ck FROM %s WHERE pk = 0");
        assertEquals(WIDE_ROWS, count.size());
        return cfs;
    }

    private static com.sun.management.ThreadMXBean threadMXBean()
    {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof com.sun.management.ThreadMXBean))
            return null;
        com.sun.management.ThreadMXBean sunBean = (com.sun.management.ThreadMXBean) bean;
        if (!sunBean.isThreadAllocatedMemorySupported())
            return null;
        if (!sunBean.isThreadAllocatedMemoryEnabled())
            sunBean.setThreadAllocatedMemoryEnabled(true);
        return sunBean;
    }
}
