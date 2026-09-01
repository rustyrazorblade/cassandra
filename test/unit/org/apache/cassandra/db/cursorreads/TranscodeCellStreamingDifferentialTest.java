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
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * M3.3a-ii (CASSANDRA-20428, Phase 4 seam iv, the allocation-win slice) differential scenarios for
 * {@link CursorReads.TranscodeMergeSink}'s streaming {@code addCellFromWire} path — sstable-winner
 * cell values now stream straight from {@code CursorReads.MergeLeg#stageCellValue} (or, for a
 * tie-broken winner, from the merge's own tie-break scratch) directly into
 * {@link org.apache.cassandra.db.rows.ResponseWireWriter}'s row body, instead of a
 * {@code byte[]}/{@code Cell} object being materialized first ({@code CursorReadMerger.mergeCellGroup}
 * calling {@code sink.addCell(Cell)}, M3.3a-i's only mechanism).
 * <p>
 * EXTENDS {@link TranscodeWireFormatDifferentialTest} rather than duplicating its corpus: every one
 * of that class's 13 scenarios (and the same set again under BTI, via
 * {@link BtiTranscodeWireFormatDifferentialTest}'s inheritance pattern) is inherited here UNCHANGED
 * and re-runs automatically — since {@code TranscodeMergeSink} now unconditionally overrides
 * {@code wantsWireStreamedCells()} to {@code true} (no toggle — M3.3a-ii is not gated behind a
 * separate flag from M3.3a-i, both live on the same sink), EVERY inherited scenario already
 * exercises the NEW streaming code path, not just the ones added here. What this class adds are
 * scenarios the M3.3a-ii plan specifically flagged as needing THEIR OWN coverage: value-length
 * SHAPES (fixed vs. variable, and specifically shapes wide enough to force multiple transfer-buffer
 * chunks — the risk {@code CellValueCapture}'s "single full-array chunk" assumption raises, which
 * this streaming path deliberately bypasses via {@code MergeLeg#stageCellValue}'s general
 * chunked-write contract instead), a tie-break scenario proving the STREAMED value is correct
 * regardless of which physical leg produced the winning bytes, and the streaming-specific negative
 * control ({@link CursorReads#TEST_CORRUPT_STREAMED_CELL_VALUE}). A dedicated allocation-gate
 * assertion (the concrete payoff evidence) closes the class.
 */
public class TranscodeCellStreamingDifferentialTest extends TranscodeWireFormatDifferentialTest
{
    @After
    public void resetStreamingHooks()
    {
        CursorReads.TEST_CORRUPT_STREAMED_CELL_VALUE = false;
    }

    // ---------------------------------------------------------------- value-length shapes

    /**
     * A FIXED-length value type ({@code vector<float, N>} — {@code valueLengthIfFixed()} is the
     * exact shape {@code SSTableCursorReader}'s own class javadoc calls out) wide enough (2000 * 4 =
     * 8000 bytes) to exceed the value-transfer scratch buffer (4096 bytes, {@code ValueTransfer
     * .transferBuffer}), so {@code stageCellValue} streams it in multiple chunks. This is exactly
     * the shape {@code CellValueCapture}'s fixed-length arm (used by the OLD, materializing
     * {@code cellValue()} path) can only handle because it deliberately makes the pre-sized target
     * array ITSELF the transfer buffer, forcing a single full-array chunk; the NEW streaming path
     * has no such target to pre-size against — it writes straight into {@code ResponseWireWriter}'s
     * growing {@code rowBody} buffer via the general, multi-chunk-safe contract
     * {@code CellValueScratch}'s tie-break staging already proves out. A wrong assumption here
     * (e.g. treating the destination as a fixed-size array) would silently truncate or corrupt the
     * value; the byte-identity oracle below would catch it.
     */
    @Test
    public void wideFixedLengthValueStreamsAcrossMultipleTransferChunks() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v vector<float, 2000>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        StringBuilder vectorLiteral = new StringBuilder();
        for (int i = 0; i < 2000; i++)
        {
            if (i > 0)
                vectorLiteral.append(',');
            vectorLiteral.append(i).append(".5");
        }
        String literal = "[" + vectorLiteral + "]";

        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, " + literal + ")", 1L, ck);
        flush();
        for (long ck = 5; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, " + literal + ")", 1L, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    /**
     * A VARIABLE-length value (blob) wide enough (10,000 bytes) to exceed the same 4096-byte
     * transfer buffer, forcing {@code stageCellValue}'s variable-length arm (the vint length
     * mirrored first, then multiple raw-byte chunks) to stream across several calls into
     * {@code ResponseWireWriter}'s {@code rowBody}. Covers the same "wide value, multiple
     * materialization passes under the old path" risk for the variable-length arm specifically.
     */
    @Test
    public void wideVariableLengthValueStreamsAcrossMultipleTransferChunks() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v blob, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, wideBlob(10_000, (int) ck));
        flush();
        for (long ck = 5; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, wideBlob(10_000, (int) ck));
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    private static ByteBuffer wideBlob(int length, int seed)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
            bytes[i] = (byte) (i + seed);
        return ByteBuffer.wrap(bytes);
    }

    // ---------------------------------------------------------------- tie-break cross-leg streaming

    /**
     * Forces exact-timestamp metadata ties between two legs on the SAME cell (identical timestamp,
     * no TTL/deletion — {@code CursorCompactor.resolveRegular}'s COMPARE outcome), with the value
     * alternating which leg holds the lexicographically greater bytes across different rows — so
     * across this one partition, the merge's tie-break winner comes from leg A on some rows and leg
     * B on others. This is the {@code stagedWinnerValue != null} branch of
     * {@code CursorReadMerger.mergeCellGroup} (the winner's bytes already consumed into the
     * tie-break scratch during comparison, then REPLAYED via {@code CellValueScratch#streamTo}
     * rather than streamed fresh from a leg cursor) — proving the replayed bytes are correct
     * regardless of which physical leg produced them, not just that streaming works for the
     * more common untied ({@code stagedWinnerValue == null}) case the wide-value scenarios above
     * exercise.
     */
    @Test
    public void exactTimestampTieBreakStreamsCorrectlyRegardlessOfWinningLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // seed every row so BOTH legs carry a cell for it (same partition/row, no shadowing), then
        // update each leg's cell at an IDENTICAL timestamp per row so reconciliation must compare
        // VALUES — alternating which side is lexicographically greater so the tie-break winner
        // alternates between leg A and leg B across rows
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "seed");
        flush();

        // comfortably later than the seed insert's implicit "now" timestamp, so both tie updates
        // supersede it (a fixed small literal like 5000 would instead lose to "now")
        long tieTimestamp = FBUtilities.timestampMicros() + 1_000_000_000L;
        for (long ck = 0; ck < 20; ck++)
        {
            // even rows: leg A (this flush round) gets the LESSER value ("AAA" < "BBB")
            String value = (ck % 2 == 0) ? "AAA-" + ck : "ZZZ-" + ck;
            execute("UPDATE %s USING TIMESTAMP " + tieTimestamp + " SET v = ? WHERE pk = ? AND ck = ?", value, 1L, ck);
        }
        flush();
        for (long ck = 0; ck < 20; ck++)
        {
            // leg B (this flush round) gets the OPPOSITE side of the tie at the SAME timestamp
            String value = (ck % 2 == 0) ? "ZZZ-" + ck : "AAA-" + ck;
            execute("UPDATE %s USING TIMESTAMP " + tieTimestamp + " SET v = ? WHERE pk = ? AND ck = ?", value, 1L, ck);
        }
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());

        // sanity: the tie really is a tie at the CQL level too (greater value visible either way)
        for (long ck = 0; ck < 4; ck++)
        {
            String expected = "ZZZ-" + ck;
            assertEquals(expected, execute("SELECT v FROM %s WHERE pk = ? AND ck = ?", 1L, ck).one().getString("v"));
        }

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    // ---------------------------------------------------------------- negative control

    @Test
    public void corruptedStreamedValueHookIsCaughtByTheHarness() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "value-" + ck);
        flush();
        for (long ck = 10; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "value-" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        CursorReads.TEST_CORRUPT_STREAMED_CELL_VALUE = true;
        try
        {
            assertTranscodeMatchesMaterializedOracle(cfs, 1L);
            fail("expected the byte-comparison harness to catch the deliberately corrupted streamed value");
        }
        catch (AssertionError expected)
        {
            assertTrue("expected a BYTE divergence assertion, got: " + expected.getMessage(),
                       expected.getMessage().contains("BYTE divergence"));
        }
        finally
        {
            CursorReads.TEST_CORRUPT_STREAMED_CELL_VALUE = false;
        }
    }

    // ---------------------------------------------------------------- the payoff: allocation gate

    /**
     * The concrete evidence this increment exists for: {@link CursorReads#sstableCellValuesMaterialized()}
     * — advanced ONLY by {@code CursorReadMerger.mergeCellGroup}'s pre-M3.3a-ii, non-streaming
     * branch — must advance by exactly ZERO when the SAME merge runs through
     * {@code TranscodeMergeSink} (streaming), while the reference {@code MaterializingMergeSink}
     * run over the identical legs/workload advances it once per sstable-won cell. Exact and
     * deterministic (a production counter, not a statistical measurement), so it is the primary
     * assertion; a secondary {@code ThreadMXBean}-based measurement (this ticket's standing
     * methodology, {@code CursorReadAllocationGateTest}) corroborates with real before/after byte
     * counts, logged for the record rather than gated (thread-allocated-bytes includes merge/CQL
     * plumbing overhead beyond just cell values, so it is a looser signal than the counter).
     */
    @Test
    public void sstableCellValueMaterializationDropsToZeroOnStreamingPath() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        int rowsPerLeg = 200;
        for (long ck = 0; ck < rowsPerLeg; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "value-" + ck);
        flush();
        for (long ck = rowsPerLeg; ck < rowsPerLeg * 2; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "value-" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        TableMetadata metadata = cfs.metadata();
        SinglePartitionReadCommand probe = (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(FBUtilities.nowInSeconds()).build();
        DecoratedKey dk = probe.partitionKey();
        ColumnFilter columnFilter = probe.columnFilter();
        Slices slices = probe.clusteringIndexFilter().getSlices(metadata);
        List<SSTableReader> sstables = liveSSTablesFor(cfs, probe);
        assertEquals(2, sstables.size());

        // reference: the pre-M3.3a-ii path (MaterializingMergeSink, via the untouched production
        // mergeLegs) — every sstable-won cell (2 columns x 400 rows = 800) must materialize a
        // byte[]/Cell object
        List<CursorReads.PendingLeg> refLegs = openAllLegs(sstables, metadata, dk, slices, columnFilter);
        long refBefore = CursorReads.sstableCellValuesMaterialized();
        try (UnfilteredRowIterator refIter = CursorReads.mergeLegs(refLegs, metadata, dk, slices, columnFilter, null, null))
        {
            while (refIter.hasNext())
                refIter.next();
        }
        long refDelta = CursorReads.sstableCellValuesMaterialized() - refBefore;

        // candidate: the NEW M3.3a-ii path (TranscodeMergeSink) over the IDENTICAL legs/workload —
        // must materialize ZERO byte[]/Cell objects for those same sstable-won cells
        List<CursorReads.PendingLeg> headerLegs = openAllLegs(sstables, metadata, dk, slices, columnFilter);
        UnfilteredRowIterator headerIter = CursorReads.mergeLegs(headerLegs, metadata, dk, slices, columnFilter, null, null);
        RegularAndStaticColumns cols = headerIter.columns();
        EncodingStats stats = headerIter.stats();
        headerIter.close();
        SerializationHeader header = new SerializationHeader(false, metadata, cols, stats);

        List<CursorReads.PendingLeg> candLegs = openAllLegs(sstables, metadata, dk, slices, columnFilter);
        long candBefore = CursorReads.sstableCellValuesMaterialized();
        transcodeCandidate(candLegs, metadata, dk, slices, columnFilter, header, 0, Long.MIN_VALUE, false, Long.MIN_VALUE);
        long candDelta = CursorReads.sstableCellValuesMaterialized() - candBefore;

        logger.info("M3.3a-ii allocation gate: sstable-won cell materializations — " +
                    "reference(MaterializingMergeSink)={} candidate(TranscodeMergeSink)={}", refDelta, candDelta);
        assertEquals("expected one materialization per sstable-won cell (2 cols x " + (rowsPerLeg * 2) + " rows) " +
                     "on the reference (pre-M3.3a-ii) path", (long) rowsPerLeg * 2 * 2, refDelta);
        assertEquals("M3.3a-ii streaming path must materialize ZERO sstable-won cell byte[]/Cell objects",
                     0L, candDelta);

        secondaryThreadAllocationCorroboration(sstables, metadata, dk, slices, columnFilter, header);
    }

    /** Informational only (logged, not gated — see the class-level javadoc for why): repeated-pass
     *  {@code ThreadMXBean} thread-allocated-bytes comparison between the two sinks, this ticket's
     *  standing measurement methodology ({@code CursorReadAllocationGateTest}), reported so the
     *  before/after numbers this increment's report needs are backed by two independent methods. */
    private void secondaryThreadAllocationCorroboration(List<SSTableReader> sstables, TableMetadata metadata,
                                                         DecoratedKey dk, Slices slices, ColumnFilter columnFilter,
                                                         SerializationHeader header) throws Throwable
    {
        java.lang.management.ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        Assume.assumeTrue(threadBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) threadBean;
        if (!bean.isThreadAllocatedMemorySupported())
            return;
        if (!bean.isThreadAllocatedMemoryEnabled())
            bean.setThreadAllocatedMemoryEnabled(true);
        long tid = Thread.currentThread().getId();

        int warmup = 10;
        int measured = 5;
        for (int i = 0; i < warmup; i++)
        {
            List<CursorReads.PendingLeg> legs = openAllLegs(sstables, metadata, dk, slices, columnFilter);
            try (UnfilteredRowIterator iter = CursorReads.mergeLegs(legs, metadata, dk, slices, columnFilter, null, null))
            {
                while (iter.hasNext())
                    iter.next();
            }
            transcodeCandidate(openAllLegs(sstables, metadata, dk, slices, columnFilter), metadata, dk, slices,
                               columnFilter, header, 0, Long.MIN_VALUE, false, Long.MIN_VALUE);
        }

        long refBest = Long.MAX_VALUE;
        for (int i = 0; i < measured; i++)
        {
            List<CursorReads.PendingLeg> legs = openAllLegs(sstables, metadata, dk, slices, columnFilter);
            long before = bean.getThreadAllocatedBytes(tid);
            try (UnfilteredRowIterator iter = CursorReads.mergeLegs(legs, metadata, dk, slices, columnFilter, null, null))
            {
                while (iter.hasNext())
                    iter.next();
            }
            refBest = Math.min(refBest, bean.getThreadAllocatedBytes(tid) - before);
        }
        long candBest = Long.MAX_VALUE;
        for (int i = 0; i < measured; i++)
        {
            List<CursorReads.PendingLeg> legs = openAllLegs(sstables, metadata, dk, slices, columnFilter);
            long before = bean.getThreadAllocatedBytes(tid);
            transcodeCandidate(legs, metadata, dk, slices, columnFilter, header, 0, Long.MIN_VALUE, false, Long.MIN_VALUE);
            candBest = Math.min(candBest, bean.getThreadAllocatedBytes(tid) - before);
        }
        logger.info("M3.3a-ii allocation gate (secondary, ThreadMXBean, informational): " +
                    "reference(MaterializingMergeSink)={}B candidate(TranscodeMergeSink)={}B ratio={}",
                    refBest, candBest, String.format("%.4f", (double) candBest / refBest));
    }
}
