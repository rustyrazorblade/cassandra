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

package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.ResponseWireWriter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.bti.BtiCursorSeekSupport;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.util.ArrayBackedDataOutput;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * PHASE 1 SPIKE of the cursor read path (flag-gated, off by default: {@code cassandra.cursor_reads_enabled}
 * / {@code Config.cursor_reads_enabled}).
 *
 * Serves the SSTABLE legs of a supported single-partition read through {@link SSTableCursorReader}
 * instead of {@code SSTableIterator}, materializing the result into ordinary {@code Row}/{@code Cell}
 * objects at the {@link UnfilteredRowIterator} boundary — "seam (i)" of the cursor-read-path explainer.
 * The point of this phase is CORRECTNESS of a cursor-served read end to end (verified by the
 * differential harness in test/unit/org/apache/cassandra/db/cursorreads/), not allocation wins:
 * the materialization here deliberately allocates the same object inventory the iterator path does.
 *
 * Deliberate Phase 1 shortcuts (all called out in the journal):
 * <ul>
 *   <li>The MEMTABLE legs of a merged read stay on the existing object-based path untouched; only
 *       sstable legs are cursor-served. The object-backed memtable cursor adapter is Phase 3.</li>
 *   <li>Slice filtering happens on the MATERIALIZED stream, mirroring
 *       {@code AbstractSSTableIterator.ForwardReader}'s slice semantics exactly (including
 *       artificial open/close markers at slice bounds). Since M1 (Phase 2), materialization on
 *       BTI-format sstables is BOUNDED: a single-slice read seeks the cursor to the row-index
 *       block containing the slice start (seeding the open-range-tombstone state from the index,
 *       exactly as {@code SSTableIterator.ForwardIndexedReader.setForSlice} does) and stops
 *       materializing at the first unfiltered at-or-past the slice end. BIG format keeps the
 *       eager whole-partition walk (its block index seek is a possible later follow-on). Since
 *       M2.2 the same per-leg seek applies inside the multi-leg cursor merge, with each seeked
 *       leg's open-marker state seeded into the merge's cross-leg range-tombstone set.</li>
 *   <li>The (bounded) materialization happens EAGERLY at iterator creation; the iterator path is
 *       lazy. Correctness-equivalent.</li>
 * </ul>
 *
 * See {@link #isReadSupported} for the support gate; anything outside it takes the iterator path
 * with zero behavior change.
 */
public final class CursorReads
{
    private static final Logger logger = LoggerFactory.getLogger(CursorReads.class);

    private CursorReads()
    {
    }

    /** Sstable legs actually served via a cursor (a cursor was opened and the partition read).
     *  The differential harness uses this to prove the cursor path really ran (no silent fallback). */
    private static final AtomicLong SSTABLE_LEGS_SERVED = new AtomicLong();
    /** Gated-in legs where the sstable turned out not to contain the partition (no cursor opened). */
    private static final AtomicLong SSTABLE_LEGS_WITHOUT_PARTITION = new AtomicLong();
    /** Sstable legs where the BTI row index produced an actual FORWARD seek into the partition
     *  (M1/Phase 2). Seek-effectiveness guard: tests assert seeked scenarios really seeked rather
     *  than scanning from partition start (a correct-but-unseeked run has identical output, so
     *  the differential comparison alone cannot catch it). Since M2.2 this counts merged-mode
     *  legs too ({@link PendingLeg#seekForMerge}): a merged read over N indexed BTI legs with a
     *  real slice start advances this by N, so merged-slice tests can assert per-leg seeks the
     *  same way single-leg tests always have. */
    private static final AtomicLong SSTABLE_LEG_ROW_INDEX_SEEKS = new AtomicLong();
    /** Unfiltereds (rows + range tombstone markers; the static row is excluded) FULLY materialized
     *  by cursor-served legs, summed across legs. The other half of the seek-effectiveness guard:
     *  bounded-materialization tests assert this stays small relative to the partition's row count
     *  when the row-index seek and/or the slice-end stop applies.
     *  M2.1 (cursor-level merge): for a MERGED multi-leg read this counts the MERGED output
     *  (winners actually materialized), NOT Sigma(per-leg rows) — that difference IS the merge's
     *  effectiveness guard: a silent fallback to per-leg-materialize-then-object-merge would still
     *  produce byte-identical results but would inflate this counter back to the per-leg sum. */
    private static final AtomicLong UNFILTEREDS_MATERIALIZED = new AtomicLong();
    /** Merged multi-leg reads actually served by the cursor-level merge core (M2.1) — one per
     *  query that routed >= 2 pending sstable legs through {@link #mergeLegs}. The
     *  silent-fallback guard for the merge capability, same discipline as {@link #sstableLegsServed}:
     *  multi-leg differential scenarios assert this advanced; single-leg scenarios assert it did NOT
     *  (they must keep taking the per-leg path bit-for-bit). */
    private static final AtomicLong CURSOR_MERGES_SERVED = new AtomicLong();
    /** Sstable legs that entered a cursor-level merge (each such leg is also counted in
     *  {@link #sstableLegsServed}). */
    private static final AtomicLong SSTABLE_LEGS_CURSOR_MERGED = new AtomicLong();
    /** M2.3: memtable legs that joined a cursor-level merge (served through the object-backed
     *  {@link MemtableMergeLeg} adapter instead of the pre-M2.3 object-level merge-after-the-fact).
     *  The memtable half of the silent-fallback guard: memtable+sstable differential scenarios
     *  assert this advanced — a quiet fallback to "cursor-merge the sstables, object-merge the
     *  memtable above" would produce byte-identical results and fail exactly this. */
    private static final AtomicLong MEMTABLE_LEGS_CURSOR_MERGED = new AtomicLong();
    /** M2.3: merged rows emitted by REUSING the memtable's own already-live {@code Row} object
     *  (the single-version fast path mirroring {@code Row.Merger.merge}'s
     *  {@code rowsToMerge == 1 && activeDeletion.isLive()} arm). The allocation-property guard for
     *  the escape hatch: correctness checks cannot see the difference between reusing and
     *  rebuilding an identical row — this counter can. */
    private static final AtomicLong MEMTABLE_ROWS_REUSED = new AtomicLong();
    /** M2.3: merged cells emitted by reusing the memtable's own already-live {@code Cell} object
     *  (the cell-level escape hatch, mirroring {@code Cells.reconcile} returning the winning cell
     *  object rather than rebuilding it). */
    private static final AtomicLong MEMTABLE_CELLS_REUSED = new AtomicLong();
    /** M3.1/M3.2d: cursor merges whose production was BOUNDED by the query's limit — the
     *  {@link LimitingMergeSink} twin of the top-level {@code DataLimits} counter tripped, so the
     *  merge stopped producing instead of materializing the slice's whole tail. The
     *  new-capability guard for the production bound (same discipline as the merge/seek counters):
     *  LIMIT-shaped scenarios assert this advanced — a silently non-engaging bound produces
     *  byte-identical results and fails exactly this — and unbounded scenarios (and limited
     *  scenarios whose filter is NOT fully pushable — {@link #filterPushdownFor} declines, so
     *  {@link #limitBoundFor} keeps its own pre-M3.2d disengagement too) assert it did NOT (the
     *  bound must not engage where its counting could diverge from the top counter). Since M3.2d
     *  a filtered+limited query with a FULLY pushable filter also advances this counter — the
     *  filter and limit bounds compose (see {@link RowLevelFilterProbe}) rather than excluding
     *  each other, so filtered-and-pushable LIMIT scenarios assert this advanced too, not that it
     *  didn't. */
    private static final AtomicLong MERGES_STOPPED_BY_LIMIT = new AtomicLong();
    /** M3.2a: cursor merges that ran with a RowFilter pushdown context attached — the query's
     *  filter passed {@link #filterPushdownFor}'s gate AND the merge actually consumed the context
     *  (single-leg reads compute but never attach one, matching M3.1's {@code completeSingleLeg}
     *  scope choice). The new-capability guard for filter pushdown, same discipline as
     *  {@link #MERGES_STOPPED_BY_LIMIT}: pushable-filter merged scenarios assert this advanced —
     *  a silently disengaging gate produces byte-identical results and fails exactly this — and
     *  every mandatory-fallback shape (non-SIMPLE expression, complex/counter column,
     *  reconciliation/non-strict filter, active size-tracking or purgeable-tombstone-recording
     *  config) asserts it did NOT advance while the cursor path still served the query. */
    private static final AtomicLong FILTER_PUSHDOWNS_ENGAGED = new AtomicLong();
    /** M3.2a: merged partitions whose row-group merge was skipped ENTIRELY because a
     *  partition-level (static-column or partition-key-column) filter expression failed against
     *  the partition key + merged static row — the cursor twin of
     *  {@code RowFilter.filter}'s partition short-circuit, which closes the partition without
     *  iterating a single row. The merge emits the same empty-with-static-row shape a
     *  no-row-legs merge already produces; the authoritative top-level filter then drops it
     *  identically on both paths. Partition-skip scenarios assert this advanced; kept-partition
     *  scenarios assert it did NOT. */
    private static final AtomicLong PARTITIONS_SKIPPED_BY_FILTER = new AtomicLong();
    /** M3.2b/M3.2c/M3.2d: merged row groups DROPPED at production because a row-level filter
     *  expression failed — a clustering-column expression against the group's descriptor wire
     *  bytes (M3.2b), or a regular-column expression at winner resolution (M3.2c) — the row-level
     *  analog of {@link #PARTITIONS_SKIPPED_BY_FILTER}, and the new-capability guard for row-level
     *  pushdown: row-filtered scenarios assert this advanced (a silently non-evaluating
     *  probe produces byte-identical results — the top filter still drops the rows — and fails
     *  exactly this), while pass-all filters and queries whose limit shape can never get a
     *  production bound (a GROUP BY limit that is not unlimited — see
     *  {@code FilterPushdown.rowLevelPushdownEligible}) assert it did NOT. Since M3.2d a
     *  CQL_LIMIT/CQL_PAGING_LIMIT query with a fully pushable filter is ALSO eligible (the row-level
     *  gate composes with the limit bound instead of requiring its absence — see
     *  {@link RowLevelFilterProbe}), so filtered-and-limited scenarios assert this advanced too
     *  when the query's filter drops rows. Each dropped group also feeds the scan-stats accumulator
     *  so the metrics the top-of-stack {@code MetricRecording} would have recorded for these rows
     *  are preserved exactly (see {@link ScanStatsAccumulator}). */
    private static final AtomicLong ROWS_DROPPED_BY_FILTER = new AtomicLong();
    /** M3.2c/M3.2d: the subset of {@link #ROWS_DROPPED_BY_FILTER} dropped because a REGULAR-column
     *  expression failed at winner resolution (a dead, shadowed, value-failing or absent filter
     *  column) — every such drop is a row ABANDONMENT: unlike clustering rejection, the failure
     *  can surface mid-cell-walk or at row end, after part of the row was already merged. The
     *  new-capability guard for regular-column pushdown: regular-filtered scenarios assert this
     *  advanced (a silently non-evaluating verdict is byte-identical — the top filter still drops
     *  the rows — and fails exactly this), while clustering-only, non-boundable-limit-shape and
     *  non-engaged shapes assert it did NOT — see {@link #ROWS_DROPPED_BY_FILTER}'s note on the
     *  M3.2d composition with LIMIT. */
    private static final AtomicLong ROWS_DROPPED_BY_REGULAR_FILTER = new AtomicLong();
    /**
     * M3.3a-ii: sstable-winner cell values materialized into a {@code byte[]}/{@code Cell} object
     * — advanced ONLY by {@code CursorReadMerger.mergeCellGroup}'s pre-M3.3a-ii (non-streaming)
     * branch, taken when the sink does not opt into {@code wantsWireStreamedCells()}. The
     * allocation-property guard for the new streaming path, same discipline as
     * {@link #MEMTABLE_CELLS_REUSED}: correctness checks (the byte-identity differential harness)
     * cannot see the difference between streaming a value and materializing then discarding it —
     * this counter can. A workload driven entirely through a streaming sink (only
     * {@code CursorReads.TranscodeMergeSink} today) must advance this by exactly ZERO for its
     * sstable-won cells, while the SAME workload driven through a non-streaming sink (every M3.1/
     * M3.2 sink) advances it once per sstable-won cell — the concrete before/after evidence
     * {@code CursorReadAllocationGateTest}-style ThreadMXBean measurement would otherwise have to
     * infer statistically.
     */
    private static final AtomicLong SSTABLE_CELL_VALUES_MATERIALIZED = new AtomicLong();

    static void countSstableCellValueMaterialized()
    {
        SSTABLE_CELL_VALUES_MATERIALIZED.incrementAndGet();
    }

    @VisibleForTesting
    public static long sstableCellValuesMaterialized()
    {
        return SSTABLE_CELL_VALUES_MATERIALIZED.get();
    }

    /**
     * M3.3b-i: {@code ReadResponse}s actually served via the transcode fast path from a genuine
     * replica-serving call site ({@code ReadCommandVerbHandler.doRead} today) — advanced ONLY once
     * {@code SinglePartitionReadCommand.queryStorageToResponseBytes}'s own layer-2 gate has fully
     * passed and the merge has actually run, never merely because the gate was attempted. The
     * silent-fallback guard for THIS increment's capability, same discipline as every prior
     * M-milestone counter on this ticket: eligible-shape scenarios (unbounded, unfiltered,
     * multi-leg) assert this advanced; every declining scenario (digest, LIMIT, filtered,
     * single-leg, 2i-indexed, repaired-status-tracking) asserts it stayed flat while the response
     * is still produced correctly via the base-class default path.
     */
    private static final AtomicLong TRANSCODE_RESPONSES_SERVED = new AtomicLong();

    static void countTranscodeResponseServed()
    {
        TRANSCODE_RESPONSES_SERVED.incrementAndGet();
    }

    @VisibleForTesting
    public static long transcodeResponsesServed()
    {
        return TRANSCODE_RESPONSES_SERVED.get();
    }

    public static long sstableLegsServed()
    {
        return SSTABLE_LEGS_SERVED.get();
    }

    public static long cursorMergesServed()
    {
        return CURSOR_MERGES_SERVED.get();
    }

    public static long sstableLegsCursorMerged()
    {
        return SSTABLE_LEGS_CURSOR_MERGED.get();
    }

    public static long memtableLegsCursorMerged()
    {
        return MEMTABLE_LEGS_CURSOR_MERGED.get();
    }

    public static long memtableRowsReused()
    {
        return MEMTABLE_ROWS_REUSED.get();
    }

    public static long memtableCellsReused()
    {
        return MEMTABLE_CELLS_REUSED.get();
    }

    public static long mergesStoppedByLimit()
    {
        return MERGES_STOPPED_BY_LIMIT.get();
    }

    public static long filterPushdownEngaged()
    {
        return FILTER_PUSHDOWNS_ENGAGED.get();
    }

    public static long partitionsSkippedByFilter()
    {
        return PARTITIONS_SKIPPED_BY_FILTER.get();
    }

    public static long rowsDroppedByFilter()
    {
        return ROWS_DROPPED_BY_FILTER.get();
    }

    public static long rowsDroppedByRegularColumnFilter()
    {
        return ROWS_DROPPED_BY_REGULAR_FILTER.get();
    }

    /** Counter hooks for {@code CursorReadMerger}'s escape-hatch emissions. */
    static void countMemtableRowReuse()
    {
        MEMTABLE_ROWS_REUSED.incrementAndGet();
    }

    static void countMemtableCellReuse()
    {
        MEMTABLE_CELLS_REUSED.incrementAndGet();
    }

    public static long sstableLegsWithoutPartition()
    {
        return SSTABLE_LEGS_WITHOUT_PARTITION.get();
    }

    public static long sstableLegRowIndexSeeks()
    {
        return SSTABLE_LEG_ROW_INDEX_SEEKS.get();
    }

    public static long unfilteredsMaterialized()
    {
        return UNFILTEREDS_MATERIALIZED.get();
    }

    /**
     * TEST ONLY: deliberately corrupts materialized cell timestamps (+1) so the differential
     * harness can prove it detects a broken cursor path (the "mutation test" the compaction
     * harness methodology requires). Never set outside tests. Applies on BOTH the per-leg
     * (Phase 1) materialization and the M2.1 merge sink, so the harness provably detects a wrong
     * merged materialization, not just a wrong per-leg walk.
     */
    @VisibleForTesting
    public static volatile boolean TEST_CORRUPT_CELL_TIMESTAMPS = false;

    /**
     * TEST ONLY (M2.1): deliberately inverts the cell reconciliation verdict
     * ({@code CellLivenessInfo.resolve}'s LEFT/RIGHT) inside the cursor-level merge core, so
     * the differential harness can prove it detects a wrong MERGE DECISION specifically (an older
     * cell winning reconciliation), not merely a wrong per-cell materialization. Never set outside
     * tests.
     */
    @VisibleForTesting
    public static volatile boolean TEST_CORRUPT_MERGE_DECISIONS = false;

    /**
     * TEST ONLY (M2.2): silently DROPS every merged leg's row-index open-marker seed — the seek
     * still happens, but the leg's open-range-tombstone deletion at the seek point never reaches
     * the merge's cross-leg open-marker set (or the post-merge slicer's
     * {@code openMarkerAtStart}). This is exactly the wrong-seed failure mode the M2.2
     * integration must not have: rows shadowed by an RT spanning the seek point resurrect and
     * the artificial open marker at the slice start disappears. Exists so the differential
     * harness can prove it would CATCH that failure. Never set outside tests.
     */
    @VisibleForTesting
    public static volatile boolean TEST_DROP_MERGE_SEEK_OPEN_MARKER = false;

    /**
     * TEST ONLY (M2.2): SKEWS every merged leg's row-index open-marker seed
     * ({@code markedForDeleteAt + 1}) — the seed arrives, but with the wrong deletion value, so
     * the merged artificial open marker at the slice start (and any shadowing decision the seeded
     * deletion participates in) carries a wrong timestamp. Proves the harness detects a wrong
     * seed VALUE, not merely a missing seed. Never set outside tests.
     */
    @VisibleForTesting
    public static volatile boolean TEST_SKEW_MERGE_SEEK_OPEN_MARKER = false;

    /**
     * TEST ONLY (M2.3): SKEWS every timestamp the memtable adapter presents to the merge
     * ({@code +1} on the descriptor's row liveness and on each parked cell's liveness) — the
     * memtable leg's data is intact, but every reconciliation decision it participates in sees a
     * wrong timestamp, so a memtable-vs-sstable collision resolves the wrong way. Proves the
     * differential harness detects a wrong merge DECISION involving the memtable leg specifically
     * (the M2.3 analog of {@code TEST_CORRUPT_MERGE_DECISIONS}, which only sits on the cell
     * reconciliation verdict). Never set outside tests.
     */
    @VisibleForTesting
    public static volatile boolean TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS = false;

    /**
     * TEST ONLY (M2.3): FORCES the memtable whole-row reuse fast path even when the merge's
     * active deletion is not live — i.e. reuses the memtable's row object in exactly the
     * situation the escape hatch must NOT fire (data shadowed by another source's partition/range
     * deletion would resurrect instead of being dropped). Proves the harness would catch a wrong
     * escape-hatch reuse, not just a wrong rebuild. Never set outside tests.
     */
    @VisibleForTesting
    public static volatile boolean TEST_FORCE_MEMTABLE_ROW_REUSE = false;

    /**
     * TEST ONLY (M3.2b): silently SKIPS the tombstone contributions of every filter-dropped row
     * group in the scan-stats accumulator — the rows still drop correctly (byte-identical results,
     * the top filter would have dropped them anyway), but the tombstone counts
     * {@code withMetricsRecording} folds in (histograms, warn/abort thresholds, warning text) go
     * quietly missing, exactly the silent under-reporting failure mode the scan-metrics parity
     * harness exists to catch. Proves {@code ScanMetricsCapture} detects broken dropped-row
     * accounting specifically, not just broken bytes. Never set outside tests.
     */
    @VisibleForTesting
    public static volatile boolean TEST_SKEW_DROPPED_ROW_ACCOUNTING = false;

    /**
     * TEST ONLY (M3.3a-i): deliberately SKEWS the row liveness timestamp {@link TranscodeMergeSink}
     * hands {@link ResponseWireWriter} (+1, applied post-purge) — a delta-encoded-field negative
     * control proving the byte-comparison harness catches a wrong ENCODED VALUE, not just a wrong
     * flag/shape. Never set outside tests.
     */
    @VisibleForTesting
    public static volatile boolean TEST_TRANSCODE_SKEW_TIMESTAMP = false;

    /**
     * TEST ONLY (M3.3a-i): deliberately reports a non-live row deletion as LIVE to
     * {@link ResponseWireWriter#startRow} — drops the {@code HAS_DELETION} bit that should be set,
     * a wrong-flags-byte negative control proving the byte-comparison harness catches a wrong FLAG,
     * not just a wrong value. Never set outside tests.
     */
    @VisibleForTesting
    public static volatile boolean TEST_TRANSCODE_WRONG_FLAGS = false;

    /**
     * TEST ONLY (M3.3a-ii): deliberately flips the low bit of the first byte of a sstable-winner
     * cell's value when {@link TranscodeMergeSink} streams it directly from the winning leg's
     * cursor (the new {@code addCellFromWire} path — {@code CursorReadMerger.mergeCellGroup}
     * calling {@code WinnerCellValueSource.streamValue} with no prior tie-break scratch staging)
     * — a corruption negative control specific to the STREAMING mechanism itself, proving the
     * byte-comparison harness catches a wrong value on this new path distinctly from the
     * pre-existing materializing-path coverage ({@link #TEST_CORRUPT_CELL_TIMESTAMPS},
     * {@link #TEST_TRANSCODE_SKEW_TIMESTAMP}, {@link #TEST_TRANSCODE_WRONG_FLAGS}, none of which
     * touch a streamed VALUE). Never set outside tests.
     */
    @VisibleForTesting
    public static volatile boolean TEST_CORRUPT_STREAMED_CELL_VALUE = false;

    /**
     * Phase 1 support gate, mirroring the spirit of {@link CursorCompactor#isSupported}: returns
     * true ONLY for the narrow slice this phase serves. Everything else falls back to the iterator
     * path before any cursor state is created.
     *
     * Gate floor inherited from cursor compaction ({@link CursorCompactor#unsupportedMetadata}):
     * no secondary indexes, partitioner must support reusable keys, no Accord keyspace. On top:
     * <ul>
     *   <li>flag off (default) → false</li>
     *   <li>single-partition slice reads only ({@link ClusteringIndexSliceFilter}); names filters
     *       take {@code queryMemtableAndSSTablesInTimestampOrder} and are out of scope</li>
     *   <li>ascending order only (no reversed queries — the cursor is forward-only)</li>
     *   <li>full-partition or a single slice; multi-slice filters (size > 1) fall back. They would
     *       likely work through the same ForwardReader-mirroring logic but Phase 1 keeps the
     *       verified surface minimal.</li>
     *   <li>no row cache on the table (a cursor result cannot populate {@code CachedBTreePartition}
     *       without materializing anyway; explainer §2d)</li>
     *   <li>no counter tables: {@code copyCellValue} streams raw context bytes and does not apply
     *       {@code DeserializationHelper.maybeClearCounterValue}'s LOCAL-shard clearing</li>
     *   <li>no materialized views: legacy shadowable row deletions are rejected mid-read by
     *       {@link SSTableCursorReader} (same caution as {@code isValidationSupported})</li>
     *   <li>all candidate sstables at the latest format version, none carrying dropped
     *       complex/counter ghost header columns (same checks as the compaction gate)</li>
     *   <li>at least one sstable leg — memtable-only reads stay on the untouched object path</li>
     * </ul>
     */
    public static boolean isReadSupported(SinglePartitionReadCommand command,
                                          ColumnFamilyStore cfs,
                                          List<SSTableReader> sstables)
    {
        if (!DatabaseDescriptor.cursorReadsEnabled())
            return false;

        TableMetadata metadata = cfs.metadata();
        if (CursorCompactor.unsupportedMetadata(metadata))
            return false;
        if (metadata.isCounter() || metadata.isView())
            return false;
        if (cfs.isRowCacheEnabled())
            return false;

        ClusteringIndexFilter filter = command.clusteringIndexFilter();
        if (!(filter instanceof ClusteringIndexSliceFilter))
            return false;
        if (filter.isReversed())
            return false;
        Slices slices = filter.getSlices(metadata);
        if (slices.size() > 1)
            return false;

        if (sstables.isEmpty())
            return false;
        for (SSTableReader sstable : sstables)
        {
            if (!sstable.descriptor.version.isLatestVersion())
                return false;
            if (CursorCompactor.unsupportedHeaderColumns(metadata, sstable))
                return false;
        }
        return true;
    }

    /**
     * Cursor-served replacement for {@code sstable.rowIterator(key, slices, columnFilter, false, listener)}
     * (and, with {@code Slices.NONE}, for the skipped-non-static-content shape). Must only be called
     * for commands that passed {@link #isReadSupported}. Since M2.1 this is the SINGLE-leg
     * composition of {@link #openLeg} + {@link #completeSingleLeg}; multi-leg reads route their
     * pending legs through {@link #mergeLegs} instead.
     */
    public static UnfilteredRowIterator sstableRowIterator(SSTableReader sstable,
                                                           TableMetadata metadata,
                                                           DecoratedKey key,
                                                           Slices slices,
                                                           ColumnFilter columnFilter,
                                                           SSTableReadsListener listener)
    {
        PendingLeg leg = openLeg(sstable, metadata, key, slices, columnFilter, listener, new ValueTransfer());
        if (leg == null)
            // mirrors BigTableReader/BtiTableReader.rowIterator with a null index entry
            return absentPartitionIterator(metadata, key, sstable);
        return completeSingleLeg(leg);
    }

    /**
     * The substitute for a gated-in sstable leg whose sstable turns out not to contain the queried
     * partition: an EMPTY iterator that still reports the SSTABLE's {@link EncodingStats} —
     * because that is what the iterator path's own substitute does
     * ({@code UnfilteredRowIteratorWithLowerBound.stats()} returns {@code sstable.stats()} even
     * when the partition is absent), and the merged read response's stats header is
     * {@code EncodingStats.merge} over every merged iterator's stats.
     *
     * Found by the M2.3 differential harness ({@code memtableFallsBackWhenNoSSTableLegSurvives}):
     * the previous {@code noRowsIterator} substitute reported {@code EncodingStats.NO_STATS},
     * which silently diverged the intra-node response header (vint timestamp deltas are
     * header-relative) whenever ANOTHER source — a memtable, or another sstable — still produced
     * the partition. Latent since Phase 1: with no other source producing data, the response
     * carries no partition at all and the stats never serialize, which is why the original
     * absent-partition scenario could not see it.
     */
    public static UnfilteredRowIterator absentPartitionIterator(TableMetadata metadata, DecoratedKey key, SSTableReader sstable)
    {
        UnfilteredRowIterator empty = UnfilteredRowIterators.noRowsIterator(metadata, key, Rows.EMPTY_STATIC_ROW,
                                                                            DeletionTime.LIVE, false);
        return new WrappingUnfilteredRowIterator()
        {
            @Override
            public UnfilteredRowIterator wrapped()
            {
                return empty;
            }

            @Override
            public EncodingStats stats()
            {
                return sstable.stats();
            }
        };
    }

    /**
     * M2.1 leg-open phase: performs the partition lookup (with the exact listener/metrics
     * notifications the iterator path makes), opens a cursor, reads the partition header
     * (validating the partition-level deletion) and materializes the static row — the cheap
     * prefix a caller needs to drive the {@code mostRecentPartitionTombstone} elimination loop —
     * WITHOUT materializing any rows. Rows are produced later by {@link #completeSingleLeg}
     * (exactly Phase 1/M1 behavior) or, for {@code >= 2} legs, by {@link #mergeLegs}'s cursor-level
     * merge. Legs whose slices are empty ({@code Slices.NONE} shapes) never need rows at all and
     * have their cursor closed before this returns.
     *
     * @param transfer the query's shared {@link ValueTransfer} scratch — ONE instance per read,
     *                 passed to every {@code openLeg} call of that read, so a merged query's S
     *                 legs share one transfer buffer / value capture instead of allocating S
     * @return the pending leg, or null when the sstable does not contain the partition (counted in
     *         {@link #sstableLegsWithoutPartition()}; no cursor is opened)
     */
    public static PendingLeg openLeg(SSTableReader sstable,
                                     TableMetadata metadata,
                                     DecoratedKey key,
                                     Slices slices,
                                     ColumnFilter columnFilter,
                                     SSTableReadsListener listener,
                                     ValueTransfer transfer)
    {
        long position;
        BtiCursorSeekSupport.PartitionEntry btiEntry = null;
        if (sstable instanceof BtiTableReader)
        {
            BtiTableReader bti = (BtiTableReader) sstable;
            // Same lookup (and listener/metrics notifications) BtiTableReader.rowIterator itself
            // performs, but keeping the index entry so the row index is usable for the M1 seek.
            btiEntry = BtiCursorSeekSupport.exactPartitionEntry(bti, key, listener);
            if (btiEntry == null)
            {
                SSTABLE_LEGS_WITHOUT_PARTITION.incrementAndGet();
                return null;
            }
            position = btiEntry.dataPosition;
        }
        else
        {
            // Same lookup the iterator path performs: notifies the listener (metrics parity) and
            // updates the key cache / bloom filter stats identically.
            position = sstable.getPosition(key, SSTableReader.Operator.EQ, listener);
            if (position < 0)
            {
                SSTABLE_LEGS_WITHOUT_PARTITION.incrementAndGet();
                return null;
            }
        }

        PendingLeg leg = new PendingLeg(sstable, metadata, key, slices, columnFilter, btiEntry, transfer);
        try
        {
            leg.open(position);
        }
        catch (RuntimeException | Error e)
        {
            leg.close();
            throw e;
        }
        catch (IOException e)
        {
            leg.close();
            // SSTableCursorReader wraps corruption in CorruptSSTableException internally; anything
            // surfacing here is unexpected IO.
            throw new RuntimeException("cursor read failed for " + sstable, e);
        }
        SSTABLE_LEGS_SERVED.incrementAndGet();
        return leg;
    }

    /**
     * Finishes a single pending leg exactly as Phase 1/M1 did when the whole read happened in one
     * call: BTI row-index seek + slice-end stop when applicable, eager (bounded) materialization,
     * cursor closed, result wrapped in the slice-applying iterator. Byte-identical to the
     * pre-M2.1 {@link #sstableRowIterator} output by construction — single-leg reads must keep
     * taking this path bit-for-bit (the merge core engages only for {@code >= 2} legs).
     */
    public static UnfilteredRowIterator completeSingleLeg(PendingLeg leg)
    {
        MaterializedPartition materialized;
        try
        {
            materialized = leg.completeSingle();
        }
        catch (RuntimeException | Error e)
        {
            leg.close();
            throw e;
        }
        catch (IOException e)
        {
            leg.close();
            throw new RuntimeException("cursor read failed for " + leg.sstable, e);
        }
        leg.close();
        return new SlicedMaterializedIterator(leg.metadata, leg.key, leg.sstable, leg.sstable.stats(),
                                              leg.columnFilter, leg.legSlices, materialized,
                                              true); // single leg: this iterator IS the leg's per-leg validation surface
    }

    /**
     * M3.1 engagement gate for the cursor merge's production bound: returns a fresh DETACHED
     * counter twinning the EXACT counter {@code ReadCommand.executeLocally} applies above the
     * merge ({@code limits().filter(iterator, nowInSec(), selectsFullPartition())},
     * {@code ReadCommand.java:581} — same limits object, same {@code newCounter} arguments), or
     * null when the bound must not engage and production stays unbounded (today's behavior,
     * always correct). Each decline is conservative — the bound's one failure mode is
     * under-production (stopping before the top counter would have), so any query shape whose
     * top-of-stack counting this twin cannot reproduce EXACTLY falls back to unbounded:
     * <ul>
     *   <li>only CQL_LIMIT / CQL_PAGING_LIMIT kinds — the GROUP BY counters count group
     *       boundaries, a different machine this twin does not reproduce (M3+ material);</li>
     *   <li>not for unlimited limits (nothing to bound; {@code DataLimits.NONE} lands here);</li>
     *   <li>a non-empty {@code RowFilter} no longer blocks outright (M3.2d): the filter sits
     *       BELOW the counter ({@code ReadCommand.java:563} vs {@code :581}) and drops rows
     *       before they are counted, so a filter-BLIND bound would stop while the counter still
     *       wants rows — UNLESS every one of those rows is ALSO one {@link #filterPushdownFor}
     *       pushes into the merge, in which case the merge drops exactly the rows the top filter
     *       would and the production-side count equals what survives to the counter. Declined
     *       whenever {@link #filterPushdownFor} itself would decline (any unpushable expression):
     *       filtering then stays entirely at the top of the stack, and the production-side row
     *       count is NOT the same population the top counter will see, so a filter-blind bound
     *       would risk under-production. See {@link RowLevelFilterProbe} and
     *       {@link LimitingMergeSink} for how the two bounds compose once both engage — a row must
     *       survive the filter (an emitted {@code endRow}/{@code addRow}) before it can ever reach
     *       the counter, so the merge keeps pulling past filter-dropped rows until the counter's
     *       real requirement of SURVIVING rows is met, never stopping early on a merely-produced
     *       count;</li>
     *   <li>not under MV strict liveness (MV tables are gated out of cursor reads entirely —
     *       purely defensive).</li>
     * </ul>
     *
     * CALLER CONTRACT (the final-stream condition): pass the returned counter into
     * {@link #mergeLegs} ONLY for a read whose merged iterator is consumed by
     * {@code executeLocally}'s stack — i.e. reads routed through
     * {@code SinglePartitionReadCommand.queryStorage}. The other {@code queryMemtableAndDisk}
     * entry points are documented to return the UN-limited partition contents (their callers —
     * counter locks, index searchers, cache warming — consume everything and apply no counter),
     * so a bounded merge there would under-produce; those routes must pass null.
     */
    static DataLimits.Counter limitBoundFor(SinglePartitionReadCommand command)
    {
        DataLimits limits = command.limits();
        if (!isBoundableLimitKind(limits.kind()))
            return null;
        if (limits.isUnlimited())
            return null;
        // M3.2d: a non-empty filter only blocks the bound when it is NOT fully pushable —
        // filterPushdownFor's gate is the exact twin (same expression-shape checks, same
        // needsReconciliation/isStrict/query-size-tracking/purgeable-tombstone-recording
        // exclusions) that decides whether production-side filtering can keep the merge's row
        // count equal to what survives to the top counter. Calling it here (rather than
        // threading a pre-computed result through) keeps the two gates independently testable
        // and cannot recurse: filterPushdownFor's own FilterPushdown construction never calls
        // back into limitBoundFor (see isBoundableLimitKind's shared use instead).
        if (!command.rowFilter().isEmpty() && filterPushdownFor(command) == null)
            return null;
        if (command.metadata().enforceStrictLiveness())
            return null;
        return limits.newCounter(command.nowInSec(), false, command.selectsFullPartition(),
                                 command.metadata().enforceStrictLiveness());
    }

    /** Whether {@code kind} is a limit shape {@link #limitBoundFor} ever engages for (CQL_LIMIT
     *  or CQL_PAGING_LIMIT — GROUP BY counters count group boundaries, a different machine no
     *  twin here reproduces). Shared by {@link #limitBoundFor} and {@link FilterPushdown}'s
     *  row-level eligibility so the two can never drift on what "this query even has a boundable
     *  limit shape" means — {@code FilterPushdown} needs this WITHOUT re-deriving filter
     *  pushability (it is constructed only once {@link #filterPushdownFor}'s own gate already
     *  passed), which is what keeps the two gates from calling back into each other. */
    private static boolean isBoundableLimitKind(DataLimits.Kind kind)
    {
        return kind == DataLimits.Kind.CQL_LIMIT || kind == DataLimits.Kind.CQL_PAGING_LIMIT;
    }

    /**
     * M3.2a engagement gate for RowFilter pushdown into the cursor merge: returns a pushdown
     * context for {@link #mergeLegs}, or null when pushdown must not engage — in which case the
     * query is STILL served by the cursor path, with filtering staying entirely at the top-of-stack
     * {@code rowFilter().filter} exactly as today (disengaging the pushdown FEATURE never blocks
     * or fails the query; same fallback philosophy as every other gate on this ticket). The
     * authoritative top-level filter stays in place and authoritative either way — the pushdown is
     * a production BOUND, per the M3.2 plan's governing decision: the merge may only skip
     * producing what the top filter would provably drop, so the one failure mode is
     * under-production, which the differential harness catches as a byte divergence.
     *
     * All-or-nothing per query: if ANY expression is unpushable the whole gate disengages
     * (partial-predicate pushdown is a documented later increment). Engages only when:
     * <ul>
     *   <li>the filter is non-empty (nothing to push otherwise), needs no coordinator
     *       reconciliation and is strict — {@code needsReconciliation()} changes the
     *       purge-before-evaluate semantics ({@code RowFilter.filter} skips the row purge to keep
     *       information replica filtering protection needs) and a non-strict filter means
     *       CASSANDRA-19018's intersection-to-union downgrade is in play at the coordinator;
     *       neither subtlety is replicated below the merge, both are gated;</li>
     *   <li>every expression is exactly a {@code RowFilter.SimpleExpression} (the
     *       {@code Kind.SIMPLE} shape — excludes MAP_ELEMENT's by-path complex lookup, CUSTOM's
     *       index coupling and USER's arbitrary row-level code) on a non-complex column (multi-cell
     *       CONTAINS/CONTAINS_KEY need a materialized {@code ComplexColumnData}) that is not a
     *       counter (defensive: counter TABLES are outside {@link #isReadSupported} entirely, like
     *       {@code limitBoundFor}'s MV check);</li>
     *   <li>the query-size-tracking stage is not active for this command (its accounting walks the
     *       object stream below the top filter; while M3.2a's partition-level short-circuit is
     *       provably invisible to it — a filter-closed partition's rows are never pulled through
     *       the lower stack on the iterator path either — the M3.2 plan gates the whole feature on
     *       it conservatively rather than reasoning per-sub-increment) and the
     *       purgeable-tombstone-recording config is disabled (same reasoning);</li>
     *   <li>CALLER CONTRACT, exactly {@code limitBoundFor}'s: the context may reach
     *       {@link #mergeLegs} ONLY for a read whose merged iterator is consumed by
     *       {@code executeLocally}'s stack (the {@code finalLimitedStream} routing gate in
     *       {@code SinglePartitionReadCommand.queryMemtableAndDiskInternal}) — the other
     *       {@code queryMemtableAndDisk} entry points are documented to return unfiltered
     *       partition contents and must pass null.</li>
     * </ul>
     *
     * Since M3.2a the context's partition-level effect is the partition-level short-circuit
     * ({@link FilterPushdown#partitionLevelMatches}), consumption-independent and always active
     * once the gate engages. Since M3.2b/M3.2c row-level (clustering + regular column)
     * expressions additionally drop rows at production; since M3.2d that row-level dropping
     * COOPERATES with {@code limitBoundFor}'s production bound rather than requiring its absence
     * — see {@link FilterPushdown#rowLevelPushdownEligible} for the exact condition, and
     * {@code limitBoundFor} for the matching lift of ITS OWN non-empty-filter disengagement.
     */
    static FilterPushdown filterPushdownFor(SinglePartitionReadCommand command)
    {
        RowFilter rowFilter = command.rowFilter();
        if (rowFilter.isEmpty())
            return null;
        if (rowFilter.needsReconciliation() || !rowFilter.isStrict())
            return null;
        if (command.metadata().enforceStrictLiveness())
            return null; // MV tables are outside isReadSupported entirely; purely defensive, like limitBoundFor's check
        for (RowFilter.Expression e : rowFilter.getExpressions())
        {
            // exact-class check == Kind.SIMPLE (Kind is not visible outside the filter package;
            // every other Expression subclass reports a different kind, and an unknown future
            // subclass must disengage, not engage)
            if (e.getClass() != RowFilter.SimpleExpression.class)
                return null;
            ColumnMetadata column = e.column();
            if (column.isComplex() || column.type.isCounter())
                return null;
            // M3.2b/M3.2c: clustering AND regular expressions are evaluated by a TWIN (window
            // bytes against operator.isSatisfiedBy) rather than the real
            // SimpleExpression.isSatisfiedBy, so only operator families whose non-complex
            // evaluation is exactly "operator.isSatisfiedBy(column.type, foundValue, value)" may
            // engage — both the column-value family and the frozen-collection CONTAINS family
            // reduce to that call (regular columns additionally twin getValue's
            // cell-live-at-nowInSec gate; see RowLevelFilterProbe). Anything else (including
            // future operators) must disengage the whole gate, not engage a wrong twin.
            // Static/partition-key columns are exempt: partition-level evaluation reuses the REAL
            // isSatisfiedBy (M3.2a), never a twin.
            if ((column.kind == ColumnMetadata.Kind.CLUSTERING || column.kind == ColumnMetadata.Kind.REGULAR)
                && !(e.operator().appliesToColumnValues()
                     || e.operator().appliesToCollectionElements()
                     || e.operator().appliesToMapKeys()))
                return null;
        }
        if (querySizeTrackingActive(command))
            return null;
        if (DatabaseDescriptor.getPurgeableTobmstonesMetricGranularity() != Config.TombstonesMetricGranularity.disabled)
            return null;
        return new FilterPushdown(command, rowFilter.getExpressions());
    }

    /** Twin of {@code ReadCommand.shouldTrackSize} + {@code withQuerySizeTracking}'s own
     *  threshold check — true iff the size-tracking transformation would actually engage for this
     *  command's {@code executeLocally} run. Package-private (not private) since M3.3b-i's
     *  {@code SinglePartitionReadCommand.queryStorageToResponseBytes} gate reuses it directly:
     *  query-size tracking walks the object stream {@code executeLocally} builds, which the
     *  transcode path never does, so a command with tracking active must decline. */
    static boolean querySizeTrackingActive(SinglePartitionReadCommand command)
    {
        return command.isTrackingWarnings()
               && !SchemaConstants.isSystemKeyspace(command.metadata().keyspace)
               && (DatabaseDescriptor.getLocalReadSizeWarnThreshold() != null
                   || DatabaseDescriptor.getLocalReadSizeFailThreshold() != null);
    }

    /**
     * M3.2a: the per-query filter pushdown context {@link #filterPushdownFor} builds for
     * {@link #mergeLegs}. Holds the gate-approved expressions split exactly the way
     * {@code RowFilter.filter}'s transformation splits them: partition-level expressions (static
     * or partition-key columns) are the ones the merge can evaluate before any row-group work.
     */
    static final class FilterPushdown
    {
        private final TableMetadata metadata;
        private final List<RowFilter.Expression> partitionLevelExpressions;
        /** M3.2b: the CLUSTERING-column subset of the row-level expressions — the ones the merge
         *  evaluates at row-group formation. */
        private final List<RowFilter.Expression> clusteringExpressions;
        /** M3.2c: the REGULAR-column subset of the row-level expressions — the ones the merge
         *  evaluates at winner resolution inside the cell walk (value-window pushdown). */
        private final List<RowFilter.Expression> regularExpressions;
        /**
         * M3.2b/M3.2c/M3.2d: whether row-level pushdown may actually DROP rows for this query.
         * Requires row-level (clustering- or regular-column) expressions AND either the query is
         * unlimited ({@code limits().isUnlimited()}) or a production LIMIT bound WILL be attached
         * for it ({@code isBoundableLimitKind(limits().kind())} — the twin
         * {@code limitBoundFor} itself is built from). Both arms exist for the SAME underlying
         * reason: the top-level {@code DataLimits} counter sits ABOVE {@code withMetricsRecording},
         * so a limited query stops SCANNING (and metric-counting) at the point the limit is
         * satisfied — an eager merge that dropped and accounted the whole slice regardless would
         * over-count dropped rows the iterator path never scans (and could abort on tombstones the
         * iterator path never reaches). Before M3.2d the only way to avoid that was declining
         * row-level pushdown outright for every limited query; since M3.2d, {@code mergeLegs} wires
         * {@link RowLevelFilterProbe} to wrap {@link LimitingMergeSink} (or vice-versa depending on
         * which engages) so the SAME production bound that stops the top counter also stops THIS
         * merge — the merge never scans (or accounts) past the point the limit is satisfied, so the
         * over-count/mis-abort risk this eligibility check exists to prevent cannot arise. The
         * second arm's "will be attached" check deliberately does NOT re-derive filter pushability
         * or call back into {@code limitBoundFor}/{@code filterPushdownFor} — this constructor only
         * ever runs after {@link #filterPushdownFor}'s own gate (including its
         * {@code enforceStrictLiveness} check) already passed for this exact command, so the only
         * residual question is the limit's OWN shape, which {@code isBoundableLimitKind} answers
         * without recursing. NOT part of the gate function itself: the context still attaches for
         * the M3.2a partition-level short-circuit, which is consumption-independent (a dropped
         * partition's rows reach the metrics stage on neither path regardless of limits — proven by
         * FINDING #21's parity test).
         */
        private final boolean rowLevelPushdownEligible;
        private final long nowInSec;
        /** Set by {@link #activateScanAccounting}; null until then (and always null when
         *  {@link #rowLevelPushdownEligible} is false). */
        private ScanStatsAccumulator scanStats;

        private FilterPushdown(SinglePartitionReadCommand command, List<RowFilter.Expression> expressions)
        {
            this.metadata = command.metadata();
            // same split as RowFilter.filter's transformation build: static or partition-key
            // columns are partition-level, everything else is row-level; the row-level set is
            // further split so the CLUSTERING subset can engage at group formation (M3.2b) and
            // the REGULAR subset at winner resolution (M3.2c)
            List<RowFilter.Expression> partitionLevel = new ArrayList<>(expressions.size());
            List<RowFilter.Expression> clustering = new ArrayList<>(expressions.size());
            List<RowFilter.Expression> regular = new ArrayList<>(expressions.size());
            for (RowFilter.Expression e : expressions)
            {
                if (e.column().isStatic() || e.column().isPartitionKey())
                    partitionLevel.add(e);
                else if (e.column().kind == ColumnMetadata.Kind.CLUSTERING)
                    clustering.add(e);
                else
                    regular.add(e);
            }
            this.partitionLevelExpressions = partitionLevel;
            this.clusteringExpressions = clustering;
            this.regularExpressions = regular;
            // M3.2d: eligible when unlimited (M3.2b/c's original condition, unchanged) OR when a
            // production LIMIT bound will actually attach (limitBoundFor's own kind check —
            // enforceStrictLiveness is already known false here, since filterPushdownFor checked
            // it before this constructor ever ran, so isBoundableLimitKind is the only residual
            // question; see the field javadoc for the full argument).
            DataLimits limits = command.limits();
            this.rowLevelPushdownEligible = (!clustering.isEmpty() || !regular.isEmpty())
                                            && (limits.isUnlimited() || isBoundableLimitKind(limits.kind()));
            this.nowInSec = command.nowInSec();
        }

        /**
         * M3.2b: creates and attaches the per-execution {@link ScanStatsAccumulator} when
         * row-level pushdown is eligible for this query. Called by the SAME
         * {@code finalLimitedStream}-gated call sites that pass this context into
         * {@link #mergeLegs} — and only the MERGED ones: single-leg reads never evaluate
         * row-level expressions (M3.1's {@code completeSingleLeg} scope choice), so they must not
         * carry an accumulator either. The accumulator lands on the {@link ReadExecutionController}
         * so {@code ReadCommand.withMetricsRecording} — created later in the same
         * {@code executeLocally} run, after the eager merge has already completed — can fold the
         * dropped-row contributions into its totals.
         */
        void activateScanAccounting(ReadExecutionController controller, ColumnFamilyStore cfs,
                                    SinglePartitionReadCommand command)
        {
            if (!rowLevelPushdownEligible)
                return;
            scanStats = new ScanStatsAccumulator(command, cfs, controller);
            controller.attachScanStats(scanStats);
        }

        ScanStatsAccumulator scanStats()
        {
            return scanStats;
        }

        List<RowFilter.Expression> clusteringExpressions()
        {
            return clusteringExpressions;
        }

        List<RowFilter.Expression> regularExpressions()
        {
            return regularExpressions;
        }

        TableMetadata tableMetadata()
        {
            return metadata;
        }

        long queryNowInSec()
        {
            return nowInSec;
        }

        /**
         * The cursor twin of {@code RowFilter.filter}'s partition short-circuit: every
         * partition-level expression evaluated through its REAL {@code isSatisfiedBy} against the
         * partition key and the already-materialized merged static row — same method, same
         * arguments, same no-purge-before-partition-level-check semantics as the transformation's
         * {@code applyToPartition}. The verdict computed here (pre-{@code
         * withoutPurgeableTombstones}) cannot differ from the top filter's (post-purge): expression
         * evaluation only consults values live at {@code nowInSec} ({@code Expression.getValue}'s
         * liveness gate) and the purge stage only drops what is NOT live at {@code nowInSec}.
         *
         * @return false when the whole partition provably fails the top-level filter — the caller
         *         then skips the row-group merge entirely and emits the empty-with-static-row
         *         shape, which the authoritative top filter drops identically on both paths
         */
        boolean partitionLevelMatches(DecoratedKey key, Row mergedStatic)
        {
            for (int i = 0; i < partitionLevelExpressions.size(); i++)
            {
                if (!partitionLevelExpressions.get(i).isSatisfiedBy(metadata, key, mergedStatic, nowInSec))
                    return false;
            }
            return true;
        }
    }

    /**
     * M3.2b: the per-execution scan-stats twin of {@code ReadCommand.withMetricsRecording}'s
     * {@code MetricRecording} for rows the cursor merge DROPS at production (clustering-column
     * filter pushdown). On the iterator path those rows flow through
     * {@code withoutPurgeableTombstones} and then {@code MetricRecording} — which sits BELOW the
     * top-level row filter — before the filter discards them, feeding the tombstone/live-row
     * histograms, {@code totalRowsRead}, the per-partition top-K samplers, the warn threshold
     * (whose ClientWarn/log text embeds the exact counts) and the
     * {@link TombstoneOverwhelmingException} abort. A merge that drops those rows without this
     * accounting would silently under-report every one of those — blocking-severity by this
     * ticket's standing bar.
     *
     * Three coupled pieces, each twinning a specific stage of the real stack:
     * <ul>
     *   <li><b>The gcable-purge twin</b> ({@link #shouldPurge(long, long)}/{@link #cellPurged}):
     *       {@code withoutPurgeableTombstones} sits BETWEEN the merge and {@code MetricRecording}
     *       ({@code ReadCommand.executeLocally}'s stack order), so a dropped row's contributions
     *       are classified on what SURVIVES the identical purge predicate — same {@code nowInSec}
     *       (purge disabled entirely when 0), same {@code cfs.gcBefore(nowInSec)}, same
     *       {@code onlyPurgeRepairedTombstones} config, same {@code oldestUnrepairedTombstone}
     *       (read from the controller LAZILY, since the elimination loop updates it before the
     *       merge runs), same constant-true purge evaluator, and the same expired-cell-to-
     *       tombstone conversion with RE-purge at {@code localDeletionTime - ttl}
     *       ({@code AbstractCell.purge}'s hijack).</li>
     *   <li><b>The dropped-row classification</b> ({@link #beginDroppedRow} ... {@link #endDroppedRow}):
     *       {@code MetricRecording.applyToRow}'s exact three-way logic on post-purge content —
     *       surviving dead cells count as tombstones (guarded by the row-level
     *       {@code hasDeletion(nowInSec)} gate, twinned via the same
     *       {@code minDeletionTime}-contribution semantics {@code BTreeRow} uses), then
     *       {@code hasLiveData} counts the live row, else the PK-deletion-only arm counts one
     *       tombstone. Cell liveness uses CELL semantics ({@code Cell.isLive}), row liveness uses
     *       {@code LivenessInfo.isLive} — the two differ and the twin keeps them apart.</li>
     *   <li><b>The production-time abort twin</b> ({@link #productionAbort}): the cursor merge is
     *       EAGER (it completes at iterator creation, before {@code MetricRecording} even exists),
     *       so a threshold crossing that involves dropped rows can never rely on the pull-side
     *       check. This accumulator therefore maintains a COMBINED tombstone/live-row count over
     *       the exact surface {@code MetricRecording} would scan — the merged static row first,
     *       then every emitted-surface element (including the slicer's artificial slice-bound
     *       markers, purge-twinned) interleaved with the dropped rows in clustering order — and
     *       aborts at the same crossing element with the full side-effect set: the same Tracing
     *       message, {@code metric.tombstoneFailures.inc()}, the same {@code MessageParams} under
     *       {@code trackWarnings}, the {@code respectTombstoneThresholds} system-keyspace
     *       exemption, the same exception message (count is always {@code failureThreshold + 1}
     *       at the crossing; last-scanned clustering matches because the counting order matches),
     *       and — because on the iterator path the abort unwinds through {@code close()} and
     *       {@code MetricRecording.onClose} still records the partial counts — the same
     *       at-abort recordings: read latency, both histograms at their at-abort values,
     *       {@code totalRowsRead}, and the warn-block side effects. Exactly one site ever throws
     *       per query: if the combined production-time count never crosses, the pull-side check
     *       (which sees emitted-only counts plus this accumulator's FINAL dropped totals) can
     *       never cross either, since its ceiling is the production-time total.</li>
     * </ul>
     *
     * Engagement requires {@code limits().isUnlimited()} (see
     * {@code FilterPushdown.rowLevelPushdownEligible}) so full consumption is guaranteed and
     * the eager combined count equals what the pull side would have scanned.
     */
    static final class ScanStatsAccumulator
    {
        private final SinglePartitionReadCommand command;
        private final TableMetrics metric;
        private final ReadExecutionController controller;
        private final long nowInSec;
        private final long gcBefore;
        private final boolean onlyPurgeRepairedTombstones;
        private final boolean purgeEnabled;
        private final int failureThreshold;
        private final int warningThreshold;
        private final boolean respectTombstoneThresholds;
        private final long queryStartNanos;

        // dropped-row totals folded into MetricRecording (pull side)
        private int droppedLiveRows;
        private int droppedTombstones;
        private int sampledDroppedLiveRows;
        private int sampledDroppedTombstones;

        // production-order combined counts driving the abort twin (statics + emitted surface +
        // dropped rows, counted in the exact order MetricRecording would scan them)
        private int combinedLiveRows;
        private int combinedTombstones;

        // streaming classification state for the dropped row currently being walked; the
        // clustering carrier is EITHER the source leg (metadata-only walks — materialized lazily
        // on the abort path alone) OR an already-materialized clustering (M3.2c abandoned rows,
        // whose clustering may already exist), never both
        private MergeLeg droppedClusteringSource;
        private Clustering<?> droppedClustering;
        private boolean droppedOnSurface;
        private boolean droppedPkLive;
        private boolean droppedHasDeletion;
        private boolean droppedAnyLiveCell;
        private int droppedDeadCells;

        ScanStatsAccumulator(SinglePartitionReadCommand command, ColumnFamilyStore cfs,
                             ReadExecutionController controller)
        {
            this.command = command;
            this.metric = cfs.metric;
            this.controller = controller;
            this.nowInSec = command.nowInSec();
            this.purgeEnabled = nowInSec != 0; // withoutPurgeableTombstones is a no-op at nowInSec == 0
            this.gcBefore = purgeEnabled ? cfs.gcBefore(nowInSec) : Long.MIN_VALUE;
            this.onlyPurgeRepairedTombstones = cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones();
            this.failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            this.warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();
            this.respectTombstoneThresholds = !SchemaConstants.isLocalSystemKeyspace(command.metadata().keyspace);
            this.queryStartNanos = nanoTime();
        }

        // ---- MetricRecording consumption (pull side) ----

        int droppedLiveRows()
        {
            return droppedLiveRows;
        }

        int droppedTombstones()
        {
            return droppedTombstones;
        }

        /** Fold-once accessor for {@code MetricRecording.onPartitionClose}'s samplers: returns the
         *  dropped live rows not yet folded into a partition sample and marks them folded. */
        int unsampledDroppedLiveRows()
        {
            int unsampled = droppedLiveRows - sampledDroppedLiveRows;
            sampledDroppedLiveRows = droppedLiveRows;
            return unsampled;
        }

        int unsampledDroppedTombstones()
        {
            int unsampled = droppedTombstones - sampledDroppedTombstones;
            sampledDroppedTombstones = droppedTombstones;
            return unsampled;
        }

        // ---- gcable-purge twin (withoutPurgeableTombstones' exact predicate) ----

        private boolean shouldPurge(long timestamp, long localDeletionTime)
        {
            // PurgeFunction's purger with WithoutPurgeableTombstones' inputs: constant-true
            // evaluator, no gc-grace ignoring on the read path
            return purgeEnabled
                   && !(onlyPurgeRepairedTombstones && localDeletionTime >= controller.oldestUnrepairedTombstone())
                   && localDeletionTime < gcBefore;
        }

        private boolean shouldPurge(DeletionTime dt)
        {
            return !dt.isLive() && shouldPurge(dt.markedForDeleteAt(), dt.localDeletionTime());
        }

        /** The full cell-purge verdict, including {@code AbstractCell.purge}'s expired-cell-to-
         *  tombstone conversion: an expired expiring cell that survives the first test is
         *  converted to a tombstone at {@code localDeletionTime - ttl} and purge-tested AGAIN at
         *  that earlier time (which matters when {@code onlyPurgeRepairedTombstones} makes the
         *  predicate non-monotonic in the deletion time). */
        private boolean cellPurged(long timestamp, long localDeletionTime, int ttl)
        {
            if (shouldPurge(timestamp, localDeletionTime))
                return true;
            return ttl != Cell.NO_TTL && shouldPurge(timestamp, localDeletionTime - ttl);
        }

        /** {@code Cell.isLive(nowInSec, localDeletionTime, ttl)}'s exact formula — CELL liveness,
         *  deliberately distinct from {@code LivenessInfo.isLive}. */
        private boolean cellLive(long localDeletionTime, int ttl)
        {
            return localDeletionTime == Cell.NO_DELETION_TIME || (ttl != Cell.NO_TTL && nowInSec < localDeletionTime);
        }

        // ---- emitted-surface accounting (combined counts only; MetricRecording itself counts
        //      these on the pull side, so they never touch the dropped totals) ----

        /**
         * Classifies a merged, ABOUT-TO-BE-EMITTED row (or the merged static row) exactly as
         * {@code MetricRecording.applyToRow} will after the purge stage, feeding only the
         * combined production-order counters. Two passes over the row: the first computes the
         * post-purge {@code hasDeletion(nowInSec)} gate (the same {@code minDeletionTime}
         * contributions {@code BTreeRow} tracks: a surviving dead cell — plain or converted —
         * always trips it, a surviving non-live row/complex deletion always trips it, a retained
         * expired PK liveness trips it, live data never does), the second counts in
         * MetricRecording's order: surviving dead cells first (each an abort check), then the
         * live-row / PK-deletion-only verdict.
         */
        void accountRow(Row row)
        {
            LivenessInfo pk = row.primaryKeyLivenessInfo();
            boolean pkLive = pk.isLive(nowInSec);
            boolean hasDeletion = false;
            if (!pk.isEmpty() && !pkLive
                && !shouldPurge(pk.timestamp(), pk.localExpirationTime())
                && pk.isExpiring() && nowInSec >= pk.localExpirationTime())
                hasDeletion = true;
            if (!row.deletion().isLive() && !shouldPurge(row.deletion().time()))
                hasDeletion = true;
            boolean anyLiveCell = false;
            for (Cell<?> cell : row.cells())
            {
                if (cellLive(cell.localDeletionTime(), cell.ttl()))
                    anyLiveCell = true;
                else if (!cellPurged(cell.timestamp(), cell.localDeletionTime(), cell.ttl()))
                    hasDeletion = true;
            }
            if (!hasDeletion)
            {
                for (ColumnData cd : row)
                {
                    if (cd.column().isComplex()
                        && !((ComplexColumnData) cd).complexDeletion().isLive()
                        && !shouldPurge(((ComplexColumnData) cd).complexDeletion()))
                    {
                        hasDeletion = true;
                        break;
                    }
                }
            }

            boolean hasTombstones = false;
            if (hasDeletion)
            {
                for (Cell<?> cell : row.cells())
                {
                    if (!cellLive(cell.localDeletionTime(), cell.ttl())
                        && !cellPurged(cell.timestamp(), cell.localDeletionTime(), cell.ttl()))
                    {
                        hasTombstones = true;
                        combinedTombstone(row.clustering());
                    }
                }
            }
            if (pkLive || anyLiveCell)
                ++combinedLiveRows;
            else if (!pkLive && hasDeletion && !hasTombstones)
                combinedTombstone(row.clustering());
        }

        /** An emitted range-tombstone marker, purge-twinned: {@code PurgeFunction.applyToMarker}
         *  drops a bound whose deletion purges and a boundary BOTH of whose deletions purge (one
         *  purged side degrades it to a bound — still one marker, still one tombstone). */
        void accountMarker(RangeTombstoneMarker marker)
        {
            boolean survives;
            if (marker.isBoundary())
            {
                RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                survives = !shouldPurge(boundary.closeDeletionTime(false)) || !shouldPurge(boundary.openDeletionTime(false));
            }
            else
            {
                survives = !shouldPurge(((RangeTombstoneBoundMarker) marker).deletionTime());
            }
            if (survives)
                combinedTombstone(marker.clustering());
        }

        /** An artificial slice-bound marker the slicer will synthesize (open at the slice start /
         *  close at the slice end when a range deletion covers the bound) — the iterator path's
         *  merged stream carries the identical artificial markers through the purge stage into
         *  MetricRecording. */
        void accountArtificialMarker(ClusteringBound<?> bound, DeletionTime openDeletion)
        {
            if (!shouldPurge(openDeletion))
                combinedTombstone(bound);
        }

        // ---- dropped-row streaming classification (fed by RowLevelFilterProbe) ----

        void beginDroppedRow(MergeLeg clusteringSource, boolean onEmittedSurface,
                             LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            droppedClusteringSource = clusteringSource;
            droppedClustering = null;
            beginDropped(onEmittedSurface, mergedLiveness, mergedRowDeletion);
        }

        /** M3.2c variant for ABANDONED rows whose clustering is already materialized (a started
         *  row, or an escape-hatch row object) — no leg descriptor needed on the abort path. */
        void beginDroppedRow(Clustering<?> clustering, boolean onEmittedSurface,
                             LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            droppedClusteringSource = null;
            droppedClustering = clustering;
            beginDropped(onEmittedSurface, mergedLiveness, mergedRowDeletion);
        }

        private void beginDropped(boolean onEmittedSurface, LivenessInfo mergedLiveness,
                                  DeletionTime mergedRowDeletion)
        {
            droppedOnSurface = onEmittedSurface;
            droppedPkLive = mergedLiveness != null && mergedLiveness.isLive(nowInSec);
            droppedHasDeletion = false;
            droppedAnyLiveCell = false;
            droppedDeadCells = 0;
            if (mergedLiveness != null && !droppedPkLive
                && !shouldPurge(mergedLiveness.timestamp(), mergedLiveness.localExpirationTime())
                && mergedLiveness.isExpiring() && nowInSec >= mergedLiveness.localExpirationTime())
                droppedHasDeletion = true;
            if (!mergedRowDeletion.isLive() && !shouldPurge(mergedRowDeletion))
                droppedHasDeletion = true;
        }

        void droppedComplexDeletion(DeletionTime complexDeletion)
        {
            if (!complexDeletion.isLive() && !shouldPurge(complexDeletion))
                droppedHasDeletion = true;
        }

        void droppedCell(CellLivenessInfo winnerLiveness)
        {
            droppedCell(winnerLiveness.timestamp(), winnerLiveness.localDeletionTime(), winnerLiveness.ttl());
        }

        /** M3.2c primitive variant, fed by the abandoned-row replay of already-materialized cells
         *  (and by the escape-hatch row walk) — the same classification as the LivenessInfo form. */
        void droppedCell(long timestamp, long localDeletionTime, int ttl)
        {
            if (cellLive(localDeletionTime, ttl))
                droppedAnyLiveCell = true;
            else if (!cellPurged(timestamp, localDeletionTime, ttl))
            {
                ++droppedDeadCells;
                droppedHasDeletion = true;
            }
        }

        void endDroppedRow()
        {
            if (!droppedOnSurface)
            {
                droppedClusteringSource = null;
                droppedClustering = null;
                return; // at-or-before the slice start: reaches the metrics stage on neither path
            }
            boolean skewAccounting = TEST_SKEW_DROPPED_ROW_ACCOUNTING;
            boolean hasTombstones = false;
            if (droppedHasDeletion && droppedDeadCells > 0)
            {
                hasTombstones = true;
                if (!skewAccounting)
                {
                    for (int i = 0; i < droppedDeadCells; i++)
                        droppedTombstone();
                }
            }
            if (droppedPkLive || droppedAnyLiveCell)
            {
                ++droppedLiveRows;
                ++combinedLiveRows;
            }
            else if (!droppedPkLive && droppedHasDeletion && !hasTombstones && !skewAccounting)
            {
                droppedTombstone(); // MetricRecording's PK-deletion-only arm
            }
            droppedClusteringSource = null;
            droppedClustering = null;
        }

        private void droppedTombstone()
        {
            ++droppedTombstones;
            ++combinedTombstones;
            if (combinedTombstones > failureThreshold && respectTombstoneThresholds)
                // clustering materialized only on this exceptional path, from the group's own
                // still-loaded descriptor (or reused when the abandoned row already materialized
                // it) — the message twin needs the real values
                productionAbort(droppedClustering != null
                                ? droppedClustering
                                : droppedClusteringSource.materializeClusteringPrefix());
        }

        private void combinedTombstone(ClusteringPrefix<?> position)
        {
            ++combinedTombstones;
            if (combinedTombstones > failureThreshold && respectTombstoneThresholds)
                productionAbort(position);
        }

        // ---- the production-time abort twin ----

        /**
         * {@code MetricRecording.countTombstone}'s abort arm PLUS the {@code onClose} recordings
         * that the iterator path still performs while the abort unwinds through {@code close()} —
         * both twinned here because when the merge aborts at production time,
         * {@code withMetricsRecording} is never even created for this query.
         */
        private void productionAbort(ClusteringPrefix<?> position)
        {
            String query = command.toCQLString();
            Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)",
                          failureThreshold, query);
            metric.tombstoneFailures.inc();
            if (command.isTrackingWarnings())
            {
                MessageParams.remove(ParamType.TOMBSTONE_WARNING);
                MessageParams.add(ParamType.TOMBSTONE_FAIL, combinedTombstones);
            }

            // MetricRecording.onPartitionClose's at-abort sampling: the iterator path's abort
            // unwinds through the partition close, sampling the partial counts scanned so far
            if (combinedLiveRows > 0)
                metric.topReadPartitionRowCount.addSample(command.partitionKey().getKey(), combinedLiveRows);
            metric.topReadPartitionTombstoneCount.addSample(command.partitionKey().getKey(), combinedTombstones);

            // MetricRecording.onClose's at-abort recordings, in its order: latency, histograms,
            // totals, then the warn block (which at abort emits the warning text but not the
            // tombstoneWarnings tick, since the count is past the failure threshold)
            metric.readLatency.addNano(nanoTime() - queryStartNanos); // SinglePartitionReadCommand.recordLatency
            metric.tombstoneScannedHistogram.update(combinedTombstones);
            metric.liveScannedHistogram.update(combinedLiveRows);
            metric.totalRowsRead.inc(combinedLiveRows);
            boolean warnTombstones = combinedTombstones > warningThreshold && respectTombstoneThresholds;
            if (warnTombstones)
            {
                String msg = String.format("Read %d live rows and %d tombstone cells for query %1.512s; token %s (see tombstone_warn_threshold)",
                                           combinedLiveRows, combinedTombstones, query,
                                           command.partitionKey().getToken());
                if (command.isTrackingWarnings())
                    MessageParams.add(ParamType.TOMBSTONE_WARNING, combinedTombstones);
                else
                    ClientWarn.instance.warn(msg);
                if (combinedTombstones < failureThreshold)
                    metric.tombstoneWarnings.inc();
                logger.warn(msg);
            }
            Tracing.trace("Read {} live rows and {} tombstone cells{}",
                          combinedLiveRows, combinedTombstones,
                          (warnTombstones ? " (see tombstone_warn_threshold)" : ""));

            throw new TombstoneOverwhelmingException(combinedTombstones, query, command.metadata(),
                                                     command.partitionKey(), position);
        }
    }

    /**
     * M3.2b/M3.2c: the row-level filter probe + emitted-surface accounting decorator — one object
     * implementing BOTH merge-core collaborator seams, so dropped-row accounting (fed through the
     * {@code FilterProbe} hooks) and emitted-element accounting (observed through the
     * {@code MergeSink} events) interleave in exact stream order, which is what makes the
     * accumulator's combined production-order counts equal what {@code MetricRecording} would
     * scan on the iterator path.
     *
     * Since M3.2d this is also, when a production LIMIT bound is engaged for the same query, the
     * OUTER layer of a two-decorator sink chain: {@code CursorReads.mergeLegs} constructs this
     * probe with {@code next} pointing at {@link LimitingMergeSink} (itself wrapping the base
     * materializer) instead of at the base materializer directly. This probe's own
     * {@code MergeSink} methods still forward every event to {@code next} unconditionally (nothing
     * here special-cases the composed case), which is sufficient: {@code CursorReadMerger} only
     * ever calls {@code endRow()}/{@code addRow()} on the TOP-level sink for a row group it did NOT
     * abandon, so a filter-dropped or filter-abandoned row's events reach at most
     * {@code startRow}/{@code addComplexDeletion}/{@code addCell} — never {@code endRow} — and
     * {@link LimitingMergeSink}'s counter, driven only by {@code endRow}/{@code addRow}, therefore
     * never counts a row this probe rejected. See {@link CursorReads#mergeLegs} for the
     * construction and {@code limitBoundFor}/{@code FilterPushdown.rowLevelPushdownEligible} for
     * the gates that decide when both engage together.
     *
     * The filter verdict ({@link #rowGroupMatches}) is the M3.2 plan's clustering twin: each
     * gate-approved CLUSTERING-column {@code SimpleExpression} evaluates as
     * {@code operator.isSatisfiedBy(column.type, componentWindow, value)} — exactly what
     * {@code SimpleExpression.isSatisfiedBy} reduces to for a non-complex, non-counter clustering
     * column ({@code getValue} returns {@code clustering.bufferAt(position)}; a null component
     * fails the expression) — over a window into the descriptor's clustering wire bytes, decoded
     * with the same {@code serializeValuesWithoutSize} walk {@code readClusteringValues} uses but
     * WITHOUT materializing any component. The verdict cannot differ from the top filter's: the
     * purge that precedes row-level evaluation up top never changes clustering bytes (and a row it
     * purges to nothing is dropped up top regardless of the verdict), and the group's first-sorted
     * leg — whose bytes the emitted clustering would have come from — is the evaluation source.
     * ALIASING AUDIT (FINDING #15 discipline): the per-expression {@code ByteBuffer.wrap} window
     * is handed only to {@code Operator.isSatisfiedBy(AbstractType, ByteBuffer, ByteBuffer)}
     * implementations, which compare/deserialize within the call and retain nothing — the same
     * contract the iterator path relies on when it passes {@code clustering.bufferAt} views; the
     * window object itself is discarded after the call (one small wrap allocation per evaluated
     * expression, the same shape {@code bufferAt} itself allocates on the iterator path).
     *
     * The emitted-surface simulation twins {@code SlicedMaterializedIterator} exactly: elements
     * at-or-before the slice start don't count (non-strict skip), skipped and emitted markers both
     * update the open-marker state, the artificial open marker at the slice start counts when a
     * range deletion covers it at slice-open time, and the artificial close at the slice end
     * counts when one is still open at exhaustion ({@link #finishPartition}).
     *
     * M3.2c adds the REGULAR-column verdicts, which resolve at winner resolution inside the cell
     * walk — after part of the row may already have been merged and emitted. To make a mid-walk
     * (or row-end) abandonment account EXACTLY like a group rejected before any cell work, the
     * probe tracks the current candidate row's emitted content as it flows through the
     * {@code MergeSink} events (shell from {@code startRow}, complex deletions and cells as
     * references to their already-immutable emitted forms) and REPLAYS it into the accumulator's
     * streaming classification when {@code abandonRowGroup} fires; the merge core then streams
     * the row's remaining winners through the ordinary dropped-group hooks. Tracking engages only
     * when regular expressions exist — clustering-only probes carry none of the cost.
     */
    static final class RowLevelFilterProbe implements CursorReadMerger.MergeSink, CursorReadMerger.FilterProbe
    {
        /**
         * M3.2d: the MergeSink events this probe observes ({@code startRow}/{@code addComplexDeletion}/
         * {@code addCell}/{@code endRow}/{@code addRow}/{@code addRangeTombstoneMarker}) are forwarded
         * here — {@link #base} directly when no production bound is engaged (every pre-M3.2d
         * behavior), or {@link LimitingMergeSink} wrapping {@link #base} when one is (the composed
         * case {@link CursorReads#mergeLegs} wires). Forwarding through {@code next} rather than
         * straight to {@code base} is what makes {@code LimitingMergeSink}'s counting see the SAME
         * event stream this probe does — in particular, {@code next.endRow()}/{@code next.addRow()}
         * only ever fire for a row {@code CursorReadMerger} did NOT abandon (see
         * {@code CursorReadMerger.mergeRowGroup}: an abandoned row's {@code rowStarted} is cleared
         * before the {@code sink.endRow()} call site is even reached), so a filter-abandoned row can
         * never reach {@code LimitingMergeSink}'s counter either — the composition this ticket's
         * M3.2d correctness argument depends on falls out of the existing abandonment control flow,
         * not new logic in either decorator.
         */
        private final CursorReadMerger.MergeSink next;
        /** The base materializer, ALWAYS reached directly (never through {@link #next}) for the two
         *  needs no {@code MergeSink} interface method can serve: {@link MaterializingMergeSink#abandonRow()}
         *  (M3.2c's build-and-discard, not part of the MergeSink event grammar) and the
         *  materialized-row peek ({@code materializedCount()}/{@code unfiltereds()}) this probe's
         *  own {@link #accountEmittedRow} needs regardless of what {@link #next} is. */
        private final MaterializingMergeSink base;
        private final ScanStatsAccumulator acc;
        private final ClusteringComparator comparator;
        private final ClusteringBound<?> sliceStart; // null when the slice starts at BOTTOM
        private final ClusteringBound<?> sliceEnd;   // the artificial-close position at exhaustion
        private final RowFilter.Expression[] expressions; // gate-approved CLUSTERING expressions
        /** per-evaluation component windows indexed by clustering position (reusable) */
        private final int[] valueOffsets;
        private final int[] valueLengths;
        private final int maxPosition;

        // ---- M3.2c regular-column state ----
        /** gate-approved REGULAR-column expressions (empty = every M3.2b behavior) */
        private final RowFilter.Expression[] regularExpressions;
        /** the DISTINCT filter columns behind {@link #regularExpressions} — AND semantics are
         *  tracked per column (a column's single winner evaluates every expression on it at once) */
        private final ColumnMetadata[] regularColumns;
        private final TableMetadata metadata;   // for the real isSatisfiedBy on escape-hatch rows
        private final DecoratedKey key;         // idem
        private final long nowInSec;

        // per-row-group tracking (reset in rowGroupMatches, which runs for EVERY probed row
        // group before any merge work); buffers populate only when regularExpressions exist
        private int matchedRegularColumns;
        private boolean sawStartRow;
        private Clustering<?> trackedClustering;
        private LivenessInfo trackedLiveness;
        private DeletionTime trackedRowDeletion;
        private final List<Cell<?>> trackedCells = new ArrayList<>();
        private final List<DeletionTime> trackedComplexDeletions = new ArrayList<>();

        private boolean pastSliceStart; // sticky: the merged stream is clustering-ordered
        private boolean sliceOpened;    // the artificial-open accounting fired
        private DeletionTime simOpenMarker;

        RowLevelFilterProbe(CursorReadMerger.MergeSink next, MaterializingMergeSink base,
                            FilterPushdown pushdown, DecoratedKey key,
                            ClusteringComparator comparator, Slice slice)
        {
            this.next = next;
            this.base = base;
            this.acc = pushdown.scanStats();
            this.comparator = comparator;
            this.sliceStart = slice.start().isBottom() ? null : slice.start();
            this.sliceEnd = slice.end();
            this.pastSliceStart = sliceStart == null;
            this.expressions = pushdown.clusteringExpressions().toArray(new RowFilter.Expression[0]);
            int max = 0;
            for (RowFilter.Expression e : expressions)
                max = Math.max(max, e.column().position());
            this.maxPosition = max;
            this.valueOffsets = new int[maxPosition + 1];
            this.valueLengths = new int[maxPosition + 1];
            this.regularExpressions = pushdown.regularExpressions().toArray(new RowFilter.Expression[0]);
            List<ColumnMetadata> distinct = new ArrayList<>(regularExpressions.length);
            for (RowFilter.Expression e : regularExpressions)
            {
                boolean seen = false;
                for (ColumnMetadata c : distinct)
                    seen |= sameColumn(c, e.column());
                if (!seen)
                    distinct.add(e.column());
            }
            this.regularColumns = distinct.toArray(new ColumnMetadata[0]);
            this.metadata = pushdown.tableMetadata();
            this.key = key;
            this.nowInSec = pushdown.queryNowInSec();
        }

        /** Seeds the open-marker simulation with the merged row-index seek state — the same value
         *  the slicer receives as {@code MaterializedPartition.openMarkerAtStart}. */
        void initOpenMarker(DeletionTime openMarkerAtStart)
        {
            this.simOpenMarker = openMarkerAtStart;
        }

        // ---- FilterProbe: the clustering verdict ----

        /** Component-window sentinel: null component (expression provably fails). */
        private static final int NULL_COMPONENT = -1;
        /** Component-window sentinel: empty component (evaluates against an empty buffer). */
        private static final int EMPTY_COMPONENT = -2;

        @Override
        public boolean rowGroupMatches(UnfilteredDescriptor descriptor)
        {
            // M3.2c: this hook runs for EVERY probed row group before any merge work, so it
            // doubles as the per-row reset point for the regular-column tracking state
            if (regularExpressions.length > 0)
            {
                matchedRegularColumns = 0;
                sawStartRow = false;
                trackedClustering = null;
                trackedLiveness = null;
                trackedRowDeletion = null;
                trackedCells.clear();
                trackedComplexDeletions.clear();
            }
            if (expressions.length == 0)
                return true; // regular-only filter: no clustering verdict, no wire walk

            byte[] data = descriptor.clusteringBytes();
            int limit = descriptor.clusteringLength();
            AbstractType<?>[] types = descriptor.clusteringTypes();
            // single wire walk up to the highest filtered position — the same header-bit and
            // length semantics as readClusteringValues, no component materialized
            long header = 0;
            int offset = 0;
            for (int i = 0; i <= maxPosition; i++)
            {
                if ((i % 32) == 0)
                {
                    if (offset >= limit)
                        throw new IllegalStateException("truncated clustering bytes: header block at " + offset + " past limit " + limit);
                    header = VIntCoding.getUnsignedVInt(data, ByteArrayAccessor.instance, offset, limit);
                    offset += PartitionMaterializer.vintSize(data, offset);
                }
                if ((header & (1L << ((i * 2) + 1))) != 0) // null bit
                {
                    valueOffsets[i] = NULL_COMPONENT;
                    continue;
                }
                if ((header & (1L << (i * 2))) != 0) // empty bit
                {
                    valueOffsets[i] = EMPTY_COMPONENT;
                    continue;
                }
                int length = types[i].valueLengthIfFixed();
                if (length < 0)
                {
                    if (offset >= limit)
                        throw new IllegalStateException("truncated clustering bytes: length vint for component " + i + " at " + offset + " past limit " + limit);
                    length = VIntCoding.checkedCast(VIntCoding.getUnsignedVInt(data, ByteArrayAccessor.instance, offset, limit));
                    offset += PartitionMaterializer.vintSize(data, offset);
                }
                if (offset + length > limit)
                    throw new IllegalStateException("truncated clustering bytes: component " + i + " of length " + length + " at " + offset + " past limit " + limit);
                valueOffsets[i] = offset;
                valueLengths[i] = length;
                offset += length;
            }

            for (RowFilter.Expression e : expressions)
            {
                int position = e.column().position();
                int valueOffset = valueOffsets[position];
                if (valueOffset == NULL_COMPONENT)
                    return false; // getValue would return null -> the top filter drops the row
                ByteBuffer window = valueOffset == EMPTY_COMPONENT
                                    ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                                    : ByteBuffer.wrap(data, valueOffset, valueLengths[position]);
                if (!e.operator().isSatisfiedBy(e.column().type, window, e.getIndexValue()))
                    return false;
            }
            return true;
        }

        // ---- FilterProbe: dropped-group accounting hooks (delegating classification to the
        //      accumulator, in stream order) ----

        @Override
        public void beginDroppedRow(MergeLeg clusteringSource, boolean onEmittedSurface,
                                    LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            ROWS_DROPPED_BY_FILTER.incrementAndGet();
            if (onEmittedSurface)
                openSurface(); // the artificial slice-start open precedes this row's counts
            acc.beginDroppedRow(clusteringSource, onEmittedSurface, mergedLiveness, mergedRowDeletion);
        }

        @Override
        public void droppedComplexDeletion(DeletionTime mergedComplexDeletion)
        {
            acc.droppedComplexDeletion(mergedComplexDeletion);
        }

        @Override
        public void droppedCell(CellLivenessInfo winnerLiveness)
        {
            acc.droppedCell(winnerLiveness);
        }

        @Override
        public void endDroppedRow()
        {
            acc.endDroppedRow();
        }

        // ---- FilterProbe: the M3.2c regular-column verdicts ----

        @Override
        public boolean filtersRegularColumn(ColumnMetadata column)
        {
            for (ColumnMetadata c : regularColumns)
            {
                if (sameColumn(c, column))
                    return true;
            }
            return false;
        }

        @Override
        public boolean cellIsLive(CellLivenessInfo winnerLiveness)
        {
            // Cell.isLive(nowInSec) — the accumulator already twins the exact formula
            return acc.cellLive(winnerLiveness.localDeletionTime(), winnerLiveness.ttl());
        }

        @Override
        public boolean regularCellMatches(ColumnMetadata column, ByteBuffer valueWindow)
        {
            // The regular-column twin of SimpleExpression.isSatisfiedBy for a live simple cell:
            // getValue returned the cell's buffer (the caller established liveness), so each
            // expression on the column reduces to operator.isSatisfiedBy(type, value, target).
            // The verdict cannot differ from the top filter's: the purge that precedes row-level
            // evaluation up top removes exactly the cells that are NOT live at nowInSec, and
            // getValue only ever uses live cells — a live winner survives the purge with these
            // same bytes. ALIASING AUDIT: `valueWindow` views reusable scratch (or the memtable
            // cell's buffer); it is consumed within the isSatisfiedBy calls below, which retain
            // nothing, and is never stored — see the mergeCellGroup call-site audit.
            for (RowFilter.Expression e : regularExpressions)
            {
                if (sameColumn(e.column(), column)
                    && !e.operator().isSatisfiedBy(e.column().type, valueWindow, e.getIndexValue()))
                    return false;
            }
            ++matchedRegularColumns;
            return true;
        }

        @Override
        public boolean rowRegularFiltersSatisfied()
        {
            return matchedRegularColumns >= regularColumns.length;
        }

        @Override
        public void abandonRowGroup(MergeLeg clusteringSource, boolean onEmittedSurface,
                                    CellLivenessInfo failedWinnerLiveness)
        {
            ROWS_DROPPED_BY_FILTER.incrementAndGet();
            ROWS_DROPPED_BY_REGULAR_FILTER.incrementAndGet();
            if (onEmittedSurface)
                openSurface(); // the artificial slice-start open precedes this row's counts
            if (sawStartRow)
            {
                // discard the partially-built output row (build-and-reset; the discarded partial
                // row is the bounded wasted allocation the plan prices for late resolution) and
                // open the dropped accounting on the already-materialized shell. Always against
                // base directly — abandonRow() resets the shared row builder, a MaterializingMergeSink
                // primitive outside the MergeSink event grammar, so it must reach the true base
                // regardless of whether LimitingMergeSink is also in the forwarding chain.
                base.abandonRow();
                acc.beginDroppedRow(trackedClustering, onEmittedSurface,
                                    trackedLiveness.isEmpty() ? null : trackedLiveness,
                                    trackedRowDeletion);
            }
            else
            {
                // nothing was emitted yet: the merged shell was provably empty (an eager start
                // would have fired otherwise), exactly the null/LIVE shell the accounting expects
                acc.beginDroppedRow(clusteringSource, onEmittedSurface, null, DeletionTime.LIVE);
            }
            // replay the row content emitted before the failure — all already-immutable emitted
            // forms, classified exactly as the metadata-only walk would have classified them
            for (int i = 0; i < trackedComplexDeletions.size(); i++)
                acc.droppedComplexDeletion(trackedComplexDeletions.get(i));
            for (int i = 0; i < trackedCells.size(); i++)
            {
                Cell<?> cell = trackedCells.get(i);
                acc.droppedCell(cell.timestamp(), cell.localDeletionTime(), cell.ttl());
            }
            // the failing winner itself (dead or value-failing — part of the merged row on the
            // iterator path; null for shadowed/absent failures, which reach the row on neither path)
            if (failedWinnerLiveness != null)
                acc.droppedCell(failedWinnerLiveness);
            // the merge core now streams the row's remaining winners through droppedCell /
            // droppedComplexDeletion and closes with endDroppedRow
        }

        @Override
        public boolean existingRowMatches(Row row)
        {
            // escape-hatch rows are live objects, so the REAL production evaluation applies —
            // same method, same arguments the top filter's transformation would use (minus the
            // preceding purge, which cannot change the verdict: getValue's liveness gate already
            // rejects everything the purge would have removed)
            for (RowFilter.Expression e : regularExpressions)
            {
                if (!e.isSatisfiedBy(metadata, key, row, nowInSec))
                    return false;
            }
            return true;
        }

        @Override
        public void abandonExistingRow(Row row, boolean onEmittedSurface)
        {
            ROWS_DROPPED_BY_FILTER.incrementAndGet();
            ROWS_DROPPED_BY_REGULAR_FILTER.incrementAndGet();
            if (onEmittedSurface)
                openSurface();
            LivenessInfo pk = row.primaryKeyLivenessInfo();
            acc.beginDroppedRow(row.clustering(), onEmittedSurface,
                                pk.isEmpty() ? null : pk, row.deletion().time());
            for (ColumnData cd : row)
            {
                if (cd.column().isComplex())
                    acc.droppedComplexDeletion(((ComplexColumnData) cd).complexDeletion());
            }
            for (Cell<?> cell : row.cells())
                acc.droppedCell(cell.timestamp(), cell.localDeletionTime(), cell.ttl());
            acc.endDroppedRow();
        }

        // ---- MergeSink: emitted-surface accounting around the materializer ----

        @Override
        public void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            if (regularExpressions.length > 0)
            {
                // all three are the immutable per-row outputs the emitting path just built —
                // safe to retain until the row resolves (emitted or abandoned)
                sawStartRow = true;
                trackedClustering = clustering;
                trackedLiveness = mergedLiveness;
                trackedRowDeletion = mergedRowDeletion;
            }
            next.startRow(clustering, mergedLiveness, mergedRowDeletion);
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion)
        {
            if (regularExpressions.length > 0)
                trackedComplexDeletions.add(mergedComplexDeletion); // immutable copy (see mergeCellGroup)
            next.addComplexDeletion(column, mergedComplexDeletion);
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            if (regularExpressions.length > 0)
                trackedCells.add(cell); // materialized (or live memtable) cell — immutable
            next.addCell(cell);
        }

        @Override
        public void endRow()
        {
            // M3.2d: peek/count against base directly (never next) — next.endRow() may cascade
            // through LimitingMergeSink first, but base.endRow() is called AT MOST once per this
            // call regardless (LimitingMergeSink forwards straight to base), so this peek and
            // LimitingMergeSink's own peek observe the identical single transition.
            long before = base.materializedCount();
            next.endRow();
            if (base.materializedCount() != before)
                accountEmittedRow();
        }

        @Override
        public void addRow(Row row)
        {
            long before = base.materializedCount();
            next.addRow(row);
            if (base.materializedCount() != before)
                accountEmittedRow();
        }

        private void accountEmittedRow()
        {
            List<Unfiltered> unfiltereds = base.unfiltereds();
            Row row = (Row) unfiltereds.get(unfiltereds.size() - 1);
            if (!countsTowardSurface(row.clustering()))
                return;
            openSurface();
            acc.accountRow(row);
        }

        @Override
        public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            next.addRangeTombstoneMarker(marker);
            if (countsTowardSurface(marker.clustering()))
            {
                openSurface(); // uses the pre-update open-marker state, like the slicer's open
                acc.accountMarker(marker);
            }
            // both skipped and emitted markers drive the slicer's open-marker tracking
            simOpenMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        /**
         * M3.2d: delegates to {@link #next} rather than overriding the {@code MergeSink} default
         * (unconditional {@code true}) — when a production bound is ALSO engaged, {@code next} is
         * {@link LimitingMergeSink}, whose {@code wantsMore()} is the query's actual production
         * bound; {@code CursorReadMerger.mergeUnfiltereds} consults exactly this method (via the
         * TOP-level {@code sink} field, which is this probe whenever row-level pushdown is
         * engaged) before every row group, so the bound continues to stop the merge from pulling
         * further once satisfied even though this probe — not {@code LimitingMergeSink} — is the
         * object {@code CursorReadMerger} holds. When no production bound is engaged, {@code next}
         * is {@link #base} directly, whose inherited default is {@code true} — unbounded
         * production, identical to every pre-M3.2d behavior.
         */
        @Override
        public boolean wantsMore()
        {
            return next.wantsMore();
        }

        /** The slicer-emission twin ({@code SlicedMaterializedIterator.computeNextInSlice}'s
         *  NON-strict pre-slice skip): only elements strictly after the slice start reach the
         *  metrics stage. Sticky — the merged stream is clustering-ordered. */
        private boolean countsTowardSurface(ClusteringPrefix<?> clustering)
        {
            if (!pastSliceStart)
                pastSliceStart = comparator.compare(clustering, (ClusteringPrefix<?>) sliceStart) > 0;
            return pastSliceStart;
        }

        /** Fires the artificial slice-start open marker's accounting exactly once, at the moment
         *  the emitted slice opens (first on-surface element, or exhaustion). */
        private void openSurface()
        {
            if (sliceOpened)
                return;
            sliceOpened = true;
            if (sliceStart != null && simOpenMarker != null)
                acc.accountArtificialMarker(sliceStart, simOpenMarker);
        }

        /** Called after the merge loop completes normally: the slicer opens the slice even when
         *  nothing survived to be emitted, and artificially closes a still-open range deletion at
         *  the slice end — both reach MetricRecording on the iterator path. */
        void finishPartition()
        {
            openSurface();
            if (simOpenMarker != null)
                acc.accountArtificialMarker(sliceEnd, simOpenMarker);
        }
    }

    /**
     * M2.1: the cursor-level k-way merge across {@code >= 2} pending sstable legs of one
     * partition read. Reconciles at the descriptor/byte level BELOW materialization — only merge
     * winners are materialized into {@code Row}/{@code Cell} objects — and returns ONE iterator
     * whose emitted stream is byte-identical to what the object-level merge of the per-leg
     * iterators would have produced for these legs (the memtable legs, if any, still object-merge
     * with this result above, exactly as before; {@code EncodingStats.merge} associativity is what
     * makes that staging sound). Legs are always closed before this returns.
     *
     * Since M2.2, each indexed BTI leg of a single-slice read seeks independently to its row-index
     * floor block for the slice start before the merge runs ({@link PendingLeg#seekForMerge}),
     * with its open-range-tombstone state at the seek point seeded into the merge's cross-leg
     * open-marker set — so a narrow slice of a wide merged partition no longer walks every leg
     * from the partition start. BIG legs keep the eager walk (M1's scope decision; a merge may
     * mix both). Combined with the merge core's slice end-stop, the k-way merge is bounded on
     * both sides: seek to the start, stop at the end.
     *
     * Deliberately ABSENT compaction behaviors (see the M2 design's Q3 analysis in the journal;
     * this merge is reconciliation-only): no purge decisions at any level (reads purge only above
     * the merge via {@code withoutPurgeableTombstones}), no expired-TTL-to-tombstone conversion,
     * no strict-liveness/MV row skipping (MV tables are gated out), and corrupted-tombstone
     * validation stays at Gap C's per-leg, in-slice placement rather than compaction's
     * eager-every-element placement.
     *
     * @param productionBound M3.1: a DETACHED {@code DataLimits} counter twinning the query's
     *                        authoritative limit ({@link #limitBoundFor}), or null for unbounded
     *                        production (every pre-M3.1 behavior). Non-null ONLY when the caller
     *                        has verified this merge's output is the final merged stream
     *                        {@code ReadCommand.executeLocally}'s counter consumes — see
     *                        {@code limitBoundFor}'s contract. The authoritative
     *                        {@code limits().filter} above stays in place either way; the bound
     *                        only stops the merge from producing rows that counter would never
     *                        consume. Since M3.2d this MAY be non-null at the same time as
     *                        {@code filterPushdown} (a filtered+limited query whose filter is
     *                        fully pushable) — see {@code filterPushdown}'s note on how the two
     *                        compose.
     * @param filterPushdown  M3.2a: the query's filter pushdown context ({@link #filterPushdownFor}),
     *                        or null when pushdown is disengaged (every pre-M3.2 behavior —
     *                        filtering stays entirely at the top-of-stack {@code rowFilter().filter}).
     *                        Non-null under the same final-stream caller contract as
     *                        {@code productionBound}. In M3.2a its only effect is the
     *                        partition-level short-circuit: when a static/partition-key expression
     *                        fails against the merged static row, the row-group merge is skipped
     *                        entirely and the merge emits the same empty-with-static-row shape a
     *                        no-row-legs merge produces — the authoritative top filter then drops
     *                        the partition identically on both paths. Since M3.2b/c its row-level
     *                        (clustering/regular) expressions may also drop individual rows once
     *                        {@code filterPushdown.scanStats() != null} (row-level pushdown
     *                        eligible). Since M3.2d, when BOTH {@code productionBound} and a
     *                        row-level-eligible {@code filterPushdown} are non-null, this method
     *                        wires {@link RowLevelFilterProbe} as the OUTERMOST sink, forwarding
     *                        through {@link LimitingMergeSink} — so the limit counter only ever
     *                        counts rows that ALSO survive the filter (a filter-abandoned row's
     *                        {@code endRow}/{@code addRow} never reaches the sink chain at all —
     *                        see {@code CursorReadMerger.mergeRowGroup}), and the merge keeps
     *                        pulling past filter-dropped rows until the limit's real requirement of
     *                        SURVIVING rows is satisfied, not merely produced rows.
     */
    public static UnfilteredRowIterator mergeLegs(List<? extends MergeLeg> legs,
                                                  TableMetadata metadata,
                                                  DecoratedKey key,
                                                  Slices slices,
                                                  ColumnFilter columnFilter,
                                                  DataLimits.Counter productionBound,
                                                  FilterPushdown filterPushdown)
    {
        assert legs.size() >= 2 : "the merge core engages only for >= 2 legs";
        try
        {
            MergeContext<MaterializingMergeSink> ctx = runMergeLegs(legs, metadata, key, slices, columnFilter,
                                                                     productionBound, filterPushdown,
                                                                     MaterializingMergeSink::new);
            MaterializingMergeSink sink = ctx.sink;
            UNFILTEREDS_MATERIALIZED.addAndGet(sink.materializedCount());
            CURSOR_MERGES_SERVED.incrementAndGet();
            if (ctx.limitingSink != null && !ctx.limitingSink.wantsMore())
                MERGES_STOPPED_BY_LIMIT.incrementAndGet();
            int sstableLegCount = 0;
            SSTableReader attribution = null;
            for (MergeLeg leg : legs)
            {
                if (leg.sstableOrNull() != null)
                {
                    sstableLegCount++;
                    if (attribution == null)
                        attribution = leg.sstableOrNull();
                }
            }
            SSTABLE_LEGS_CURSOR_MERGED.addAndGet(sstableLegCount);
            MEMTABLE_LEGS_CURSOR_MERGED.addAndGet(legs.size() - sstableLegCount);
            assert attribution != null : "the gate requires at least one sstable leg";

            // Associative min-merge over the same per-leg stats the per-leg iterators would have
            // reported (sstable stats for sstable legs, the memtable iterator's own stats for
            // memtable legs), so the response's stats total is identical to the object merge's.
            EncodingStats stats = EncodingStats.merge(legs, MergeLeg::legStats);
            MaterializedPartition partition = new MaterializedPartition(ctx.mergedDeletion, ctx.mergedStatic,
                                                                       sink.unfiltereds(), ctx.openMarkerAtStart);
            // validateOnEmission = false: per-leg validation inside the merge (Gap C's in-slice
            // surface, attributed to the real leg) is the merged read's ENTIRE validation, exactly
            // like the iterator path, which never re-validates its merged output above the
            // per-leg iterators
            return new SlicedMaterializedIterator(metadata, key, attribution, stats,
                                                  columnFilter, ctx.emitSlices, partition, false);
        }
        finally
        {
            for (MergeLeg leg : legs)
                leg.close();
        }
    }

    /**
     * M3.3a-i: the sink-injection seam {@link #mergeLegs} needed for the transcode wire-format
     * oracle harness — {@code mergeLegs} hardcoded {@code new MaterializingMergeSink()} with no way
     * to plug a different {@code CursorReadMerger.MergeSink} implementation. This overload runs the
     * IDENTICAL leg-setup/limit/filter-pushdown-composition machinery {@code mergeLegs} does (via
     * the shared {@link #runMergeLegs}), parameterized by a {@link MergeSinkFactory} instead of a
     * hardcoded sink type — {@code mergeLegs} itself is now just this overload called with
     * {@code MaterializingMergeSink::new} plus its own materialization-specific tail (building the
     * {@code MaterializedPartition}/{@code SlicedMaterializedIterator}, which only a
     * {@code MaterializingMergeSink} can feed). The PUBLIC {@code mergeLegs} signature and behavior
     * are unchanged by this refactor (see its own javadoc); this overload is
     * {@code @VisibleForTesting} and is the seam {@code CursorReads.TranscodeMergeSink}'s
     * differential test suite drives directly with real legs (e.g. {@link #openLeg}-opened
     * {@link PendingLeg}s / {@link MemtableMergeLeg}s) to exercise the real merge core against a
     * non-materializing sink, exactly as production does. No production call site uses this
     * overload directly.
     *
     * @return a context carrying the fully-populated sink plus the per-partition state that only
     *         exists OUTSIDE the sink (the merged partition deletion/static row/start-of-merge open
     *         marker and the emitted slices) — everything a caller needs to assemble either a
     *         {@code MaterializedPartition} (as {@code mergeLegs} does) or, for
     *         {@code TranscodeMergeSink}, the wire-format partition envelope around the sink's
     *         already-written row/marker bytes.
     */
    @VisibleForTesting
    public static <S extends CursorReadMerger.MergeSink> MergeContext<S> mergeLegsWithSink(
        List<? extends MergeLeg> legs,
        TableMetadata metadata,
        DecoratedKey key,
        Slices slices,
        ColumnFilter columnFilter,
        DataLimits.Counter productionBound,
        FilterPushdown filterPushdown,
        MergeSinkFactory<S> sinkFactory)
    {
        assert legs.size() >= 2 : "the merge core engages only for >= 2 legs";
        try
        {
            return runMergeLegs(legs, metadata, key, slices, columnFilter, productionBound, filterPushdown, sinkFactory);
        }
        finally
        {
            for (MergeLeg leg : legs)
                leg.close();
        }
    }

    /**
     * The shared core of {@link #mergeLegs}/{@link #mergeLegsWithSink}: everything from the
     * partition-deletion/static-row merge through {@code CursorReadMerger.mergeUnfiltereds()},
     * generic over the sink type — sink-specific post-processing (production's counters,
     * {@code MaterializedPartition} assembly) stays in each caller, since it depends on the
     * CONCRETE sink type ({@code MaterializingMergeSink.unfiltereds()}/{@code materializedCount()}
     * have no equivalent on the generic {@code CursorReadMerger.MergeSink} interface). Deliberately
     * does NOT own leg closing — both callers wrap this in their own try/finally, exactly
     * preserving {@code mergeLegs}'s original close-after-everything-that-reads-legs ordering.
     */
    private static <S extends CursorReadMerger.MergeSink> MergeContext<S> runMergeLegs(
        List<? extends MergeLeg> legs,
        TableMetadata metadata,
        DecoratedKey key,
        Slices slices,
        ColumnFilter columnFilter,
        DataLimits.Counter productionBound,
        FilterPushdown filterPushdown,
        MergeSinkFactory<S> sinkFactory)
    {
        // supersedes-max over every leg, mirroring UnfilteredRowIterators.merge's
        // collectPartitionLevelDeletion over the per-leg iterators
        DeletionTime mergedDeletion = DeletionTime.LIVE;
        for (int i = 0; i < legs.size(); i++)
        {
            DeletionTime legDeletion = legs.get(i).partitionLevelDeletion();
            if (!mergedDeletion.supersedes(legDeletion))
                mergedDeletion = legDeletion;
        }

        Row mergedStatic = mergeStaticRows(legs, columnFilter.fetchedColumns().statics, mergedDeletion);

        // M3.2a: partition-level filter short-circuit, evaluated exactly where the merged
        // static row first exists — the cursor twin of RowFilter.filter's applyToPartition
        // check, which closes the whole partition against key + static row before iterating a
        // single row. On failure the row legs simply never join the merge below: the emitted
        // iterator is the same empty-with-static-row shape a no-row-legs merge produces, and
        // the authoritative top-level filter drops it identically on both paths (rows the
        // merge skipped were provably never going to be consumed). The engagement counter
        // advances whenever a context ATTACHED, matched or not — that is the silent-fallback
        // guard; the skip counter separately guards the short-circuit itself.
        boolean partitionSkippedByFilter = false;
        if (filterPushdown != null)
        {
            FILTER_PUSHDOWNS_ENGAGED.incrementAndGet();
            if (!filterPushdown.partitionLevelMatches(key, mergedStatic))
            {
                partitionSkippedByFilter = true;
                PARTITIONS_SKIPPED_BY_FILTER.incrementAndGet();
            }
        }

        // Only legs with a non-empty slice contribute rows (a Slices.NONE leg's iterator emits
        // no unfiltereds on the iterator path either — it joins the merge through its
        // partition deletion and static row alone; its cursor is already closed). M2.3:
        // memtable legs always carry the query's real slices, so they always contribute rows.
        // A partition skipped by the M3.2a filter short-circuit contributes NO row legs at all
        // (their cursors are closed unread in the finally below, exactly like eliminated legs).
        List<MergeLeg> rowLegs = new ArrayList<>(legs.size());
        if (!partitionSkippedByFilter)
        {
            for (MergeLeg leg : legs)
            {
                if (!leg.legSlices().isEmpty())
                    rowLegs.add(leg);
            }
        }

        S sink = sinkFactory.newSink();
        LimitingMergeSink limitingSink = null;
        DeletionTime openMarkerAtStart = null;
        if (!rowLegs.isEmpty())
        {
            Slice slice = rowLegs.get(0).legSlices().get(0);
            try
            {
                for (MergeLeg leg : rowLegs)
                {
                    leg.enterMergeMode();
                    // M2.2: per-leg BTI row-index seek — happens HERE, after the elimination
                    // loop confirmed the leg participates (analogous to completeSingleLeg's
                    // seek placement), so the mostRecentPartitionTombstone elimination still
                    // paid only the header+static open cost for eliminated legs. No-op for
                    // memtable legs (already in memory, pre-clipped to the slice).
                    leg.seekForMerge();
                }
                // M3.1: wrap the sink in the limit twin. Seeding the counter with the
                // partition key and MERGED static row here mirrors what the top counter's
                // attach does per partition (and is what applies CQLPagingLimits' resume seed).
                // NOTE: LimitingMergeSink wraps MaterializingMergeSink concretely (its own
                // materializedCount()/unfiltereds() bookkeeping needs it) — so a non-materializing
                // S (TranscodeMergeSink) cannot compose with a non-null productionBound in THIS
                // slice; every M3.3a-i test scenario passes productionBound == null.
                CursorReadMerger.MergeSink mergeSink = sink;
                if (productionBound != null)
                {
                    productionBound.countPartition(key, mergedStatic);
                    limitingSink = new LimitingMergeSink((MaterializingMergeSink) sink, productionBound,
                                                         metadata.comparator, slice);
                    mergeSink = limitingSink;
                }
                // M3.2b/M3.2c/M3.2d: row-level (clustering + regular column) filter pushdown +
                // its scan-stats accounting. RowLevelFilterProbe is always the OUTERMOST sink
                // when engaged, forwarding materialization events through whatever `mergeSink`
                // already is — the base sink directly (no production bound), or
                // LimitingMergeSink wrapping it (production bound also engaged, M3.2d's
                // composed case). It always peeks/abandons against `sink` cast to
                // MaterializingMergeSink DIRECTLY regardless — see RowLevelFilterProbe's `next`
                // vs `base` fields (same composability note as above: M3.3a-i never engages this
                // with a non-materializing S). This composition is why a filter-abandoned row can
                // never count toward the limit: CursorReadMerger only ever calls the top-level
                // sink's endRow()/addRow() for a row it did NOT abandon (mergeRowGroup clears
                // rowStarted before reaching that call site on abandonment), so
                // LimitingMergeSink's counter — nested inside this probe's forwarding chain —
                // never even sees an abandoned row, let alone counts it. wantsMore() composes
                // the same way: this probe delegates to `mergeSink.wantsMore()` (see its own
                // override), so CursorReadMerger's per-row-group wantsMore() check still stops
                // production at the production bound even though `sink` (CursorReadMerger's own
                // field) now holds this probe rather than LimitingMergeSink directly.
                CursorReadMerger.FilterProbe probe = null;
                RowLevelFilterProbe filterProbe = null;
                if (filterPushdown != null && filterPushdown.scanStats() != null)
                {
                    filterProbe = new RowLevelFilterProbe(mergeSink, (MaterializingMergeSink) sink, filterPushdown,
                                                          key, metadata.comparator, slice);
                    mergeSink = filterProbe;
                    probe = filterProbe;
                }
                CursorReadMerger merger = new CursorReadMerger(rowLegs.toArray(new MergeLeg[0]), metadata,
                                                              mergedDeletion, slice, mergeSink, probe);
                // supersedes-max over the seeked legs' row-index open-deletion seeds: what N
                // per-leg iterators' artificial slice-start opens would have merged to — the
                // post-merge slicer synthesizes the identical artificial open marker from it
                openMarkerAtStart = merger.openMarkerAtMergeStart();
                if (filterProbe != null)
                {
                    filterProbe.initOpenMarker(openMarkerAtStart);
                    // the merged static row is the FIRST thing MetricRecording scans on the
                    // pull side (applyToStatic precedes every row), so the combined
                    // production-order count twins that order by accounting it before any
                    // stream element
                    filterPushdown.scanStats().accountRow(mergedStatic);
                }
                // M3.3a-i: TranscodeMergeSink needs this same seek-state seed for its own
                // slice-boundary open-marker tracking (see its class javadoc) — an instanceof check
                // rather than a MergeSink-interface method since this is specific to ONE sink
                // implementation, exactly like RowLevelFilterProbe's initOpenMarker isn't part of
                // MergeSink either (it's FilterProbe-specific, called the same explicit way here).
                if (sink instanceof TranscodeMergeSink)
                    ((TranscodeMergeSink) sink).initOpenMarker(openMarkerAtStart);
                merger.mergeUnfiltereds();
                if (filterProbe != null)
                    filterProbe.finishPartition();
            }
            catch (IOException e)
            {
                throw new RuntimeException("cursor merge failed for " + key + " over " + legs.size() + " legs", e);
            }
        }

        Slices emitSlices = rowLegs.isEmpty() ? Slices.NONE : rowLegs.get(0).legSlices();
        return new MergeContext<>(sink, mergedDeletion, mergedStatic, openMarkerAtStart, emitSlices, limitingSink);
    }

    /**
     * @see #mergeLegsWithSink
     *
     * DELIBERATELY UNBOUNDED (not {@code <S extends CursorReadMerger.MergeSink>}, even though every
     * real use satisfies that bound via {@link #mergeLegsWithSink}'s own type parameter): a bound
     * referencing the package-private {@code CursorReadMerger.MergeSink} here would leak into this
     * PUBLIC interface's erased SAM descriptor. A caller outside {@code org.apache.cassandra.db}
     * (the M3.3a-i test suite) instantiating {@code MergeSinkFactory<TranscodeMergeSink>} via a
     * lambda compiles fine either way (source-level generics don't require naming the bound), but
     * bounding it here makes the JVM verifier resolve the erased return type
     * {@code CursorReadMerger$MergeSink} when linking that lambda's synthetic SAM implementation —
     * an {@code IllegalAccessError} at test RUNTIME (caught by this increment's own test run, not a
     * hypothetical), since that class is inaccessible from the test's package. Erasing to
     * {@code Object} here sidesteps it entirely; {@link #mergeLegsWithSink}'s bound still enforces
     * the real constraint at every call site, in the (same-package, always-accessible) checkcast
     * {@code CursorReads} itself emits.
     */
    @FunctionalInterface
    public interface MergeSinkFactory<S>
    {
        S newSink();
    }

    /**
     * @see #mergeLegsWithSink
     *
     * Also DELIBERATELY UNBOUNDED, for the identical reason as {@link MergeSinkFactory}: the public
     * {@code sink} field below would otherwise erase to {@code CursorReadMerger$MergeSink}, and a
     * cross-package field read needing a checkcast to that type hits the same accessibility wall.
     */
    @VisibleForTesting
    public static final class MergeContext<S>
    {
        public final S sink;
        public final DeletionTime mergedDeletion;
        public final Row mergedStatic;
        public final DeletionTime openMarkerAtStart;
        public final Slices emitSlices;
        /** null unless a production bound was engaged (never the case for M3.3a-i's own test
         *  scenarios, which always pass a null {@code productionBound} — see {@link #runMergeLegs}'s
         *  composability note); production {@link #mergeLegs} consults this for its own counter. */
        final LimitingMergeSink limitingSink;

        private MergeContext(S sink, DeletionTime mergedDeletion, Row mergedStatic,
                             DeletionTime openMarkerAtStart, Slices emitSlices, LimitingMergeSink limitingSink)
        {
            this.sink = sink;
            this.mergedDeletion = mergedDeletion;
            this.mergedStatic = mergedStatic;
            this.openMarkerAtStart = openMarkerAtStart;
            this.emitSlices = emitSlices;
            this.limitingSink = limitingSink;
        }
    }

    /**
     * Verbatim mirror of {@code UnfilteredRowIterators.UnfilteredRowMergeIterator.mergeStaticRows}
     * over the legs' already-materialized (Phase 1 machinery, per-leg column-filtered) static rows:
     * the merged static row this iterator reports must equal what the object merge would have
     * produced from the per-leg iterators, so the outer merge with any memtable legs stays
     * byte-identical. Statics are one row per leg — the allocation win lives in the row/cell
     * merge, so the object-level Row.Merger is used here deliberately.
     */
    private static Row mergeStaticRows(List<? extends MergeLeg> legs, Columns statics, DeletionTime partitionDeletion)
    {
        if (statics.isEmpty())
            return Rows.EMPTY_STATIC_ROW;

        boolean allEmpty = true;
        for (MergeLeg leg : legs)
            allEmpty &= leg.staticRow().isEmpty();
        if (allEmpty)
            return Rows.EMPTY_STATIC_ROW;

        Row.Merger merger = new Row.Merger(legs.size(), statics.hasComplex());
        for (int i = 0; i < legs.size(); i++)
            merger.add(i, legs.get(i).staticRow());
        Row merged = merger.merge(partitionDeletion);
        return merged == null ? Rows.EMPTY_STATIC_ROW : merged;
    }

    /** Closes every leg, suppressing secondary failures — the call-site exception-path analog of
     *  {@code InputCollector.close()} for legs whose iterator does not exist yet. */
    public static void closeAll(List<? extends AutoCloseable> legs, Throwable cause)
    {
        if (legs == null)
            return;
        for (AutoCloseable leg : legs)
        {
            try
            {
                leg.close();
            }
            catch (Throwable t)
            {
                cause.addSuppressed(t);
            }
        }
    }

    /**
     * M3.3b-i: the non-exceptional twin of {@link #closeAll} — closes every already-opened
     * leg/iterator when {@code SinglePartitionReadCommand.queryStorageToResponseBytes}'s own gate
     * DECLINES after some legs are already open (e.g. the hard leg-count >= 2 requirement fails
     * once the real candidate count is known). This is the normal "not eligible, fall back" path,
     * not an error path, so there is no in-flight exception to attach suppressed failures to.
     */
    static void closeAllQuietly(List<? extends AutoCloseable> items)
    {
        if (items == null)
            return;
        RuntimeException failure = null;
        for (AutoCloseable item : items)
        {
            try
            {
                item.close();
            }
            catch (Exception e)
            {
                if (failure == null)
                    failure = new RuntimeException("failed to close a cursor-read leg/iterator while declining the M3.3b-i transcode path", e);
                else
                    failure.addSuppressed(e);
            }
        }
        if (failure != null)
            throw failure;
    }

    /**
     * Copies a (possibly reusable) DeletionTime into an immutable one, preserving the exact
     * unsigned-int local-deletion-time representation. NOT {@code DeletionTime.build(mfda, ldt)}:
     * build() takes the LONG domain and classifies {@code localDeletionTime() == Long.MAX_VALUE}
     * (the LIVE sentinel) as an InvalidDeletionTime — a bug the differential harness caught on its
     * very first run (live partition deletions came back as ldt=MAX_DELETION_TIME).
     */
    static DeletionTime copyOf(DeletionTime deletionTime)
    {
        return deletionTime.isLive()
               ? DeletionTime.LIVE
               : DeletionTime.buildUnsafeWithUnsignedInteger(deletionTime.markedForDeleteAt(),
                                                             deletionTime.localDeletionTimeUnsignedInteger());
    }

    /** The materialized content of one sstable's partition (whole partition, or — on a seeked
     *  and/or end-bounded BTI read — the contiguous stretch of it covering the queried slice), or,
     *  since M2.1, of the MERGED stream of several sstables' partitions. */
    static final class MaterializedPartition
    {
        final DeletionTime partitionDeletion;
        final Row staticRow;
        final List<Unfiltered> unfiltereds;
        /** The range-tombstone deletion open at the first materialized unfiltered, per the BTI row
         *  index — non-null ONLY when a row-index seek was actually issued (mirrors
         *  {@code ForwardIndexedReader.setForSlice} setting {@code openMarker} only when it
         *  seeks). For an M2.2 merged partition: the supersedes-max over the seeked legs' seeds
         *  (see {@code CursorReadMerger.openMarkerAtMergeStart}). Null means "track open markers
         *  from the materialized stream alone". */
        final DeletionTime openMarkerAtStart;

        MaterializedPartition(DeletionTime partitionDeletion, Row staticRow, List<Unfiltered> unfiltereds,
                              DeletionTime openMarkerAtStart)
        {
            this.partitionDeletion = partitionDeletion;
            this.staticRow = staticRow;
            this.unfiltereds = unfiltereds;
            this.openMarkerAtStart = openMarkerAtStart;
        }
    }

    /**
     * Per-QUERY cell-value transfer scratch, shared by every cursor-served leg of one
     * single-partition read instead of being allocated per leg: the 4KB bounce buffer for
     * variable-length value chunking and the one-copy {@link CellValueCapture}. Sharing is safe
     * because both are pure working storage scoped to a SINGLE value-copy call — every use is a
     * {@code prepare*}/{@code copyCellValue}/{@code finish} sequence completed before the call
     * returns (see {@code PendingLeg.cellValue}/{@code stageCellValue} and
     * {@code materializeRowContents}), results land in per-leg/per-cell state
     * ({@code pendingValue}, the built cell), and the whole read — leg opening, the k-way merge's
     * strictly sequential per-leg value consumption, single-leg completion — runs on the one
     * calling read thread, so no two legs ever have transfer state in flight at the same time.
     */
    public static final class ValueTransfer
    {
        final CellValueCapture valueCapture = new CellValueCapture();
        // chunking buffer for variable-length cell values only; fixed-length values use the final
        // value array itself as the transfer buffer (see CellValueCapture)
        final byte[] transferBuffer = new byte[4096];
    }

    /**
     * Drives {@link SSTableCursorReader} over one partition and materializes rows/markers,
     * mirroring the filtering behavior of {@code UnfilteredSerializer.readSimpleColumn} /
     * {@code readComplexColumn} / {@code AbstractSSTableIterator.readStaticRow} for the query's
     * {@link ColumnFilter} so the produced objects match the iterator path's cell for cell.
     */
    private static final class PartitionMaterializer
    {
        private final SSTableCursorReader cursor;
        private final SSTableReader sstable;
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final ColumnFilter columnFilter;
        // Same helper type the iterator path drives its filtering with; using the real thing keeps
        // the includes/canSkipValue/isDropped semantics identical by construction.
        private final DeserializationHelper filterHelper;
        private final List<AbstractType<?>> clusteringTypes;
        private final PartitionDescriptor pHeader;
        private final UnfilteredDescriptor uDesc;
        // per-query shared value-transfer scratch (see ValueTransfer): one instance serves every
        // leg of a merged read instead of S per-leg copies of a 4KB buffer + capture object
        private final ValueTransfer transfer;

        // ONE row builder for the whole partition, reused across the static row and every regular
        // row: build() resets it (BTreeRow.Builder.reset), and newRow() asserts the previous build()
        // happened — the exact reuse contract the iterator path relies on in UnfilteredDeserializer
        // (a single sortedBuilder per deserializer, journal Gap B: a fresh builder per row was one of
        // the three allocation regressions the gate caught). Also receives complex deletions as
        // materializeRowContents enters each complex column, in disk order.
        private final Row.Builder rowBuilder = BTreeRow.sortedBuilder();

        // The complex column the current row's cell walk last entered (reset per row): gates the
        // once-per-column work in materializeRowContents — tester-cache priming and the complex
        // deletion's addition to the row being built.
        private ColumnMetadata currentComplexColumn;

        PartitionMaterializer(SSTableCursorReader cursor, SSTableReader sstable, TableMetadata metadata, DecoratedKey key, ColumnFilter columnFilter, ValueTransfer transfer)
        {
            this.cursor = cursor;
            this.sstable = sstable;
            this.metadata = metadata;
            this.key = key;
            this.columnFilter = columnFilter;
            this.transfer = transfer;
            this.filterHelper = new DeserializationHelper(metadata,
                                                          sstable.descriptor.version.correspondingMessagingVersion(),
                                                          DeserializationHelper.Flag.LOCAL,
                                                          columnFilter);
            this.clusteringTypes = sstable.header.clusteringTypes();
            this.pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            this.uDesc = new UnfilteredDescriptor(clusteringTypes.toArray(new AbstractType<?>[0]));

            // Deletion-only complex columns must surface as positions of their own
            // (compaction's pattern): the cursor holds the current column's deletion in
            // CellCursor.complexDeletion, and materializeRowContents records it on entering
            // each complex column, mirroring UnfilteredSerializer.readComplexColumn.
            cursor.pauseAtEmptyComplexColumns(true);
        }

        // set by openPartition; consumed by readRows / the merge path
        private DeletionTime partitionDeletion;
        private Row staticRow = Rows.EMPTY_STATIC_ROW;

        DeletionTime partitionDeletion()
        {
            return partitionDeletion;
        }

        Row staticRow()
        {
            return staticRow;
        }

        /**
         * The leg-open phase (M2.1 split of the former one-shot {@code read}): seeks to the
         * partition, reads and validates the header, materializes the static row. Returns the
         * cursor state at the first unfiltered (or {@code PARTITION_END}); rows are produced
         * separately by {@link #readRows} or consumed descriptor-level by the merge core.
         */
        int openPartition(long position) throws IOException
        {
            // The cursor was constructed over a single bound starting at this partition, so it is
            // already positioned; see PendingLeg.open.
            if (cursor.state() != PARTITION_START)
                throw new IllegalStateException("seek to partition at " + position + " yielded state " + cursor.state());
            int state = cursor.readPartitionHeader(pHeader);
            // corrupted_tombstone_strategy check on the partition-level deletion, mirroring
            // AbstractSSTableIterator's `partitionLevelDeletion.validate() -> handleInvalid` and the
            // shipped cursor-compaction precedent (StatefulCursor.validateInvalidPartitionDeletion).
            // NOTE (journal 2026-08-07, Gap C): validating the DESCRIPTOR's reusable deletion time
            // range-checks the on-disk unsigned representation; the iterator path's own check can
            // never fire for the latest (uint-ldt) format: its modern serializer deserializes into an
            // unclassified ImmutableDeletionTime whose validate() is constant-true. Descriptor
            // semantics here are strictly more protective and match cursor compaction; the corner is
            // unreachable through any legitimate write path (only raw bit corruption of the exact
            // INVALID sentinel differs).
            if (!pHeader.deletionTime().validate())
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "partitionLevelDeletion=" + pHeader.deletionTime());
            partitionDeletion = copyOf(pHeader.deletionTime());
            staticRow = Rows.EMPTY_STATIC_ROW;

            if (state == STATIC_ROW_START)
            {
                // mirrors AbstractSSTableIterator.readStaticRow: when no static column is fetched
                // the row is skipped wholesale and stays EMPTY_STATIC_ROW
                if (columnFilter.fetchedColumns().statics.isEmpty())
                {
                    state = cursor.skipStaticRow(false);
                }
                else
                {
                    state = cursor.readStaticRowHeader(uDesc);
                    rowBuilder.newRow(Clustering.STATIC_CLUSTERING);
                    state = materializeRowContents(state);
                    staticRow = rowBuilder.build();
                }
                if (state == UNFILTERED_END)
                    state = cursor.continueReading();
            }
            return state;
        }

        /**
         * The row phase for a SINGLE cursor-served leg — exactly the pre-M2.1 behavior.
         *
         * @param seekPoint when non-null (BTI, indexed partition, single slice with a real start
         *                  bound), the row-index floor block for the slice start: the cursor jumps
         *                  there after the partition header / static row instead of walking every
         *                  row before it. Rows between the block start and the slice start are
         *                  still materialized (bounded by one index block) and skipped by
         *                  {@link SlicedMaterializedIterator}'s pre-slice logic, exactly like the
         *                  iterator path's in-block {@code handlePreSliceData}.
         * @param endBound when non-null, materialization stops at the first unfiltered
         *                 at-or-past this bound (strict-end comparison, the exact mirror of
         *                 {@code ForwardReader.computeNext}'s {@code compareNextTo(end) >= 0}
         *                 cutoff — such elements are never emitted, validated or open-marker
         *                 tracked by the iterator path either).
         */
        MaterializedPartition readRows(int state, boolean readRows,
                                       BtiCursorSeekSupport.SeekPoint seekPoint,
                                       ClusteringBound<?> endBound) throws IOException
        {
            List<Unfiltered> unfiltereds = new ArrayList<>();

            DeletionTime openMarkerAtStart = null;
            if (readRows && seekPoint != null && isState(state, ROW_START | TOMBSTONE_START))
            {
                // The current unfiltered's flags byte is already consumed, so its start is one
                // byte behind the cursor. Mirror ForwardIndexedReader.setForSlice: only seek
                // FORWARD; when the floor block is the one already being read (block 0 for a
                // slice starting before the second block), keep reading sequentially and leave
                // open-marker tracking entirely to the materialized stream.
                long currentUnfilteredStart = cursor.position() - 1;
                if (seekPoint.dataPosition > currentUnfilteredStart)
                {
                    state = cursor.seekUnfiltered(seekPoint.dataPosition);
                    openMarkerAtStart = seekPoint.openMarker == null ? null : copyOf(seekPoint.openMarker);
                    SSTABLE_LEG_ROW_INDEX_SEEKS.incrementAndGet();
                }
            }

            long materializedCount = 0;
            readLoop:
            while (readRows && !isState(state, PARTITION_END | DONE))
            {
                switch (state)
                {
                    case ROW_START:
                    {
                        state = cursor.readRowHeader(uDesc);
                        Clustering<?> clustering = (Clustering<?>) toClusteringPrefix();
                        // M1 end-stop (see read()'s javadoc): at-or-past the slice end, nothing
                        // further can be emitted — stop before materializing this row's cells
                        if (endBound != null && metadata.comparator.compare(clustering, endBound) >= 0)
                            break readLoop;
                        rowBuilder.newRow(clustering);
                        state = materializeRowContents(state);
                        materializedCount++;
                        Row row = rowBuilder.build();
                        // mirrors ForwardReader.computeNext's next.isEmpty() skip
                        if (!row.isEmpty())
                            unfiltereds.add(row);
                        break;
                    }
                    case TOMBSTONE_START:
                    {
                        state = cursor.readTombstoneMarker(uDesc);
                        RangeTombstoneMarker marker = materializeMarker();
                        // M1 end-stop: a marker at-or-past the slice end is never emitted (the
                        // artificial close at the slice end comes from the open-marker state)
                        if (endBound != null && metadata.comparator.compare(marker.clustering(), endBound) >= 0)
                            break readLoop;
                        materializedCount++;
                        unfiltereds.add(marker);
                        break;
                    }
                    default:
                        throw new IllegalStateException("unexpected cursor state " + state);
                }
                if (state == UNFILTERED_END)
                    state = cursor.continueReading();
            }

            UNFILTEREDS_MATERIALIZED.addAndGet(materializedCount);
            return new MaterializedPartition(partitionDeletion, staticRow, unfiltereds, openMarkerAtStart);
        }

        /**
         * Reads liveness/deletion off the just-loaded {@link UnfilteredDescriptor} and then walks
         * the cell states, adding materialized cells to the reused {@link #rowBuilder} (the caller
         * has already called {@code newRow} on it). Returns the cursor state after the row's cells
         * are exhausted ({@code UNFILTERED_END}).
         */
        private int materializeRowContents(int state) throws IOException
        {
            // Mirrors UnfilteredSerializer.deserializeRowBody's hasTimestamp branch: a row without a
            // timestamp keeps the LivenessInfo.EMPTY singleton (the write side sets HAS_TIMESTAMP
            // iff !isEmpty(), so the sentinel round-trips exactly) instead of allocating an
            // equal-but-distinct empty instance per row.
            LivenessInfo rowLiveness = uDesc.livenessInfo().isEmpty()
                                       ? LivenessInfo.EMPTY
                                       : LivenessInfo.withExpirationTime(uDesc.livenessInfo().timestamp(),
                                                                         uDesc.livenessInfo().ttl(),
                                                                         uDesc.livenessInfo().localExpirationTime());
            rowBuilder.addPrimaryKeyLivenessInfo(rowLiveness);
            DeletionTime rowDeletion = uDesc.deletionTime();
            rowBuilder.addRowDeletion(rowDeletion.isLive()
                                      ? Row.Deletion.LIVE
                                      : Row.Deletion.regular(copyOf(rowDeletion)));

            SSTableCursorReader.CellCursor cc = cursor.cellCursor();
            currentComplexColumn = null;
            while (true)
            {
                if (isState(state, UNFILTERED_END | PARTITION_END | DONE))
                    return state;
                if (state == CELL_END)
                {
                    state = cursor.continueReading();
                    continue;
                }
                if (state != CELL_HEADER_START)
                    throw new IllegalStateException("unexpected cursor state " + state);

                state = cursor.readCellHeader();
                if (!isState(state, CELL_VALUE_START | CELL_END))
                    continue; // row tail consumed by the dropped-column filter: no position surfaced

                ColumnMetadata column = cc.cellColumn;
                if (column.isComplex() && !sameColumn(column, currentComplexColumn))
                {
                    // Entering a new complex column — its first cell, or a deletion-only position
                    // (pauseAtEmptyComplexColumns). Mirrors UnfilteredSerializer.readComplexColumn:
                    // prime the per-column tester/dropped caches and record the column's surviving
                    // deletion. The cursor already applied the sstable's dropped-column horizon to
                    // cc.complexDeletion; the helper's check covers the table-metadata horizon.
                    currentComplexColumn = column;
                    filterHelper.startOfComplexColumn(column);
                    if (filterHelper.includes(column)
                        && !cc.complexDeletion.isLive()
                        && !filterHelper.isDroppedComplexDeletion(cc.complexDeletion))
                    {
                        rowBuilder.addComplexDeletion(column, copyOf(cc.complexDeletion));
                    }
                }
                if (!cc.producedCell)
                    continue; // deletion-only complex column: its deletion was recorded above

                if (!filterHelper.includes(column))
                {
                    // mirrors readSimpleColumn/readComplexColumn's skip branch: a non-fetched
                    // column materializes nothing, so skip the value without snapshotting cell
                    // state or copying the cell path (the iterator path allocates nothing here)
                    if (state == CELL_VALUE_START)
                        state = cursor.skipCellValue();
                    continue;
                }

                // snapshot reusable cell state before any further cursor advance
                AbstractType<?> cellType = cc.cellType;
                long timestamp = cc.cellLiveness.timestamp();
                int ttl = cc.cellLiveness.ttl();
                long localDeletionTime = cc.cellLiveness.localDeletionTime();
                CellPath path = cc.cellPathLength < 0
                                ? null
                                : CellPath.create(ByteBuffer.wrap(Arrays.copyOf(cc.cellPathBuffer, cc.cellPathLength)));

                byte[] value = ByteArrayAccessor.instance.empty();
                if (state == CELL_VALUE_START)
                {
                    // mirrors Cell.Serializer.deserialize: a fetched-but-not-queried column (or
                    // non-queried collection path) keeps the cell but skips its value
                    if (filterHelper.canSkipValue(column) || (path != null && filterHelper.canSkipValue(path)))
                    {
                        state = cursor.skipCellValue();
                    }
                    else
                    {
                        int fixedLength = cellType.valueLengthIfFixed();
                        if (fixedLength >= 0)
                        {
                            // the final value array IS the transfer buffer: copyCellValue's single
                            // readFully lands the bytes in place, one copy total
                            byte[] target = fixedLength == 0 ? ByteArrayAccessor.instance.empty() : new byte[fixedLength];
                            state = cursor.copyCellValue(transfer.valueCapture.prepareFixed(target), target);
                        }
                        else
                        {
                            state = cursor.copyCellValue(transfer.valueCapture.prepareVariable(), transfer.transferBuffer);
                        }
                        value = transfer.valueCapture.finish();
                    }
                }

                if (TEST_CORRUPT_CELL_TIMESTAMPS)
                    timestamp += 1;

                Cell<byte[]> cell = ByteArrayAccessor.instance.factory()
                                                     .cell(column, timestamp, ttl, localDeletionTime, value, path);
                if (filterHelper.includes(cell, rowLiveness) && !filterHelper.isDropped(cell, column.isComplex()))
                    rowBuilder.addCell(cell);
            }
        }

        /** Encoded size in bytes of the unsigned vint starting at {@code data[offset]}. */
        private static int vintSize(byte[] data, int offset)
        {
            int firstByte = data[offset];
            return firstByte >= 0 ? 1 : 1 + VIntCoding.numberOfExtraBytesToRead(firstByte);
        }

        private RangeTombstoneMarker materializeMarker()
        {
            ClusteringPrefix<?> prefix = toClusteringPrefix();
            if (uDesc.isBoundary())
            {
                // loadTombstone reads CLOSE (deletionTime) then OPEN (deletionTime2)
                return new RangeTombstoneBoundaryMarker((ClusteringBoundary<?>) prefix,
                                                        copyOf(uDesc.deletionTime()),
                                                        copyOf(uDesc.deletionTime2()));
            }
            return new RangeTombstoneBoundMarker((ClusteringBound<?>) prefix,
                                                 copyOf(uDesc.deletionTime()));
        }

        /**
         * Allocation-lean equivalent of {@link ClusteringDescriptor#toClusteringPrefix(List)}: decodes
         * the descriptor's clustering bytes straight off its backing array instead of round-tripping
         * through {@code Clustering.serializer.deserialize} / a fresh {@code DataInputBuffer} wrapper
         * per row (journal Gap B). Deliberately implemented HERE rather than in
         * {@code ClusteringDescriptor}: that class is shared with shipped cursor-compaction code
         * (MetadataCollector, DigestingCursorMergeSink), which this fix must not touch.
         */
        private ClusteringPrefix<?> toClusteringPrefix()
        {
            if (uDesc.clusteringKind() == ClusteringPrefix.Kind.CLUSTERING)
            {
                return clusteringTypes.isEmpty()
                       ? ByteArrayAccessor.factory.clustering()
                       : ByteArrayAccessor.factory.clustering(readClusteringValues(clusteringTypes.size()));
            }
            int bound = uDesc.clusteringColumnsBound();
            return bound == 0
                   ? ByteArrayAccessor.factory.bound(uDesc.clusteringKind())
                   : ByteArrayAccessor.factory.boundOrBoundary(uDesc.clusteringKind(), readClusteringValues(bound));
        }

        /** Zero-component bound/boundary sentinel: the factory builds these from its per-kind
         *  singletons, so no values array is ever decoded for them. */
        private static final byte[][] NO_CLUSTERING_VALUES = new byte[0][];

        /**
         * M2.1: decodes the descriptor's bound/boundary clustering VALUES, kind-agnostic — the
         * merged range-tombstone marker's kind is computed by the merge (a close in one source
         * plus an open in another becomes a boundary, etc., exactly like compaction's
         * mergeRangeTombstones stomping the descriptor kind before writing), and the merge may
         * need the same values under TWO kinds (the validation gate's group position and the
         * emitted marker), so values are decoded once here and prefixes of any kind are built
         * over them by {@code CursorReadMerger.boundOrBoundary}.
         */
        private byte[][] boundValues()
        {
            int bound = uDesc.clusteringColumnsBound();
            return bound == 0 ? NO_CLUSTERING_VALUES : readClusteringValues(bound);
        }

        /**
         * Mirrors {@code ClusteringPrefix.Serializer.deserializeValuesWithoutSize} over the
         * descriptor's raw byte array — same wire format ({@code readUnfilteredClustering} stores a
         * header vint per 32-component block, then raw bytes for fixed-length components and
         * vint-length-prefixed bytes for variable-length ones), same null/empty header-bit semantics
         * ({@code 1L << (i*2+1)} = null, {@code 1L << (i*2)} = empty; Java's mod-64 shift matches
         * the serializer's own indexing for components >= 32). The only allocations are the ones the
         * iterator path also makes: the values array and one {@code byte[]} per non-null/non-empty
         * component.
         */
        private byte[][] readClusteringValues(int size)
        {
            byte[] data = uDesc.clusteringBytes();
            int limit = uDesc.clusteringLength();
            int maxValueSize = DatabaseDescriptor.getMaxValueSize();
            byte[][] values = new byte[size][];
            long header = 0;
            int offset = 0;
            for (int i = 0; i < size; i++)
            {
                if ((i % 32) == 0)
                {
                    if (offset >= limit)
                        throw new IllegalStateException("truncated clustering bytes: header block at " + offset + " past limit " + limit);
                    header = VIntCoding.getUnsignedVInt(data, ByteArrayAccessor.instance, offset, limit);
                    offset += vintSize(data, offset);
                }
                if ((header & (1L << ((i * 2) + 1))) != 0) // null bit
                {
                    values[i] = null;
                    continue;
                }
                if ((header & (1L << (i * 2))) != 0) // empty bit
                {
                    values[i] = ByteArrayUtil.EMPTY_BYTE_ARRAY;
                    continue;
                }
                int length = clusteringTypes.get(i).valueLengthIfFixed();
                if (length < 0)
                {
                    if (offset >= limit)
                        throw new IllegalStateException("truncated clustering bytes: length vint for component " + i + " at " + offset + " past limit " + limit);
                    length = VIntCoding.checkedCast(VIntCoding.getUnsignedVInt(data, ByteArrayAccessor.instance, offset, limit));
                    if (length > maxValueSize)
                        throw new IllegalStateException(String.format("Corrupt value length %d encountered, as it exceeds the maximum of %d, " +
                                                                      "which is set via max_value_size in cassandra.yaml",
                                                                      length, maxValueSize));
                    offset += vintSize(data, offset);
                }
                if (offset + length > limit)
                    throw new IllegalStateException("truncated clustering bytes: component " + i + " of length " + length + " at " + offset + " past limit " + limit);
                values[i] = Arrays.copyOfRange(data, offset, offset + length);
                offset += length;
            }
            return values;
        }
    }

    // Mirrors AbstractCell.hasInvalidDeletions(), where ttl != NO_TTL is the reference's
    // isExpiring(). Deliberately a copy of StatefulCursor's identical descriptor-level helper
    // (compaction-package-private): two trivial predicate lines beat another shared-code
    // visibility bump.
    static boolean hasInvalidCellDeletion(int ttl, long localExpirationTime)
    {
        return ttl < 0
               || localExpirationTime == Cell.INVALID_DELETION_TIME
               || localExpirationTime < 0
               || (ttl != Cell.NO_TTL && localExpirationTime == Cell.NO_DELETION_TIME);
    }

    // Mirrors the primary-key liveness clause of AbstractRow.hasInvalidDeletions() (see the
    // StatefulCursor copy note above).
    static boolean hasInvalidRowLiveness(int ttl, long localExpirationTime)
    {
        return ttl != Cell.NO_TTL && (ttl < 0 || localExpirationTime < 0);
    }

    /**
     * Column identity across sources (copy of {@link ColumnMetadata#sameName}): different sstables
     * can carry different ColumnMetadata instances for the same column in their open-time
     * serialization headers, so reference identity alone is wrong across sources; identity stays
     * the fast path.
     */
    static boolean sameColumn(ColumnMetadata a, ColumnMetadata b)
    {
        return a == b || (a != null && b != null && a.name.equals(b.name));
    }

    /**
     * M2.3: the merge-source contract {@code CursorReadMerger} (and {@link #mergeLegs}) consumes —
     * extracted from {@link PendingLeg}'s previously-concrete surface now that a second
     * implementation exists (the M2.1 journal entry explicitly deferred the interface to this
     * moment: "extract when the second implementation exists"). Two implementations:
     * <ul>
     *   <li>{@link PendingLeg} — byte-backed: an sstable leg driven by {@link SSTableCursorReader}
     *       over reusable descriptors, values materialized through the Phase 1
     *       {@code CellValueCapture} machinery;</li>
     *   <li>{@link MemtableMergeLeg} — object-backed: wraps the exact {@code UnfilteredRowIterator}
     *       the memtable leg contributes to the object-level merge today, presenting each live
     *       {@code Row}/{@code Cell}/marker as descriptor-shaped state, with
     *       {@link #existingCell()}/{@link #consumeExistingRow()} escape hatches so memtable-won
     *       data is emitted as the already-live object instead of a rebuilt copy.</li>
     * </ul>
     *
     * Contract notes (the behaviors {@code CursorReadMerger} relies on):
     * <ul>
     *   <li>{@link #cursorState()} speaks {@link SSTableCursorReader.State} regardless of backing:
     *       {@code ROW_START}/{@code TOMBSTONE_START} before {@link #readUnfilteredHeader()},
     *       {@code UNFILTERED_END} once the current unfiltered is fully consumed (then
     *       {@link #continueReading()} advances), {@code PARTITION_END}/{@code DONE} at
     *       exhaustion.</li>
     *   <li>{@link #unfiltered()} is a REUSABLE descriptor valid until the next header load; its
     *       clustering bytes are always in {@code serializeValuesWithoutSize} wire form with the
     *       descriptor's own {@code clusteringTypes()}, so the shared
     *       {@code ClusteringComparator.compare(descriptor, descriptor)} works across leg
     *       kinds.</li>
     *   <li>Cell positions surface in merge order (column order; a complex column's deletion-only
     *       position before its cells; cells in path order), pre-filtered per leg exactly as the
     *       leg's data reaches the object merge today.</li>
     *   <li>The escape hatches return null on byte-backed legs; non-null returns are immutable,
     *       already heap-safe objects the merged output may retain.</li>
     * </ul>
     */
    interface MergeLeg extends AutoCloseable
    {
        // ---- partition-level surface (consumed by mergeLegs) ----
        DeletionTime partitionLevelDeletion();
        Row staticRow();
        Slices legSlices();
        EncodingStats legStats();
        /** The backing sstable, or null for an object-backed (memtable) leg. */
        SSTableReader sstableOrNull();
        void enterMergeMode();
        void seekForMerge() throws IOException;
        DeletionTime mergeSeekOpenMarker();

        // ---- unfiltered walk (consumed by CursorReadMerger) ----
        int cursorState();
        UnfilteredDescriptor unfiltered();
        void readUnfilteredHeader() throws IOException;
        void continueReading() throws IOException;
        ClusteringPrefix<?> materializeClusteringPrefix();
        byte[][] materializeBoundValues();
        /**
         * M2.3 whole-row escape hatch: when this leg's current row can stand as the merged row
         * AS-IS (the caller has verified the {@code Row.Merger} single-version fast-path
         * conditions: this leg is the group's only contributor and the active deletion is live),
         * returns the already-live row object and consumes the leg's current unfiltered
         * (state advances to {@code UNFILTERED_END}). Returns null on byte-backed legs, which
         * always take the general cell-walk path.
         */
        Row consumeExistingRow();

        // ---- cell walk ----
        boolean parkedAtCellPosition();
        boolean needsCellAdvance();
        void ensureParkedAtCell(boolean validateCells) throws IOException;
        void advancePastCellPosition() throws IOException;
        boolean cellProduced();
        ColumnMetadata cellColumn();
        CellLivenessInfo cellLiveness();
        ByteBuffer cellPathWindow();
        DeletionTime cellComplexDeletion();
        CellPath cellPath();
        /** M2.3 cell escape hatch: the parked cell as an already-live object the merged output can
         *  reuse directly (mirroring {@code Cells.reconcile} returning the winner object), or null
         *  on byte-backed legs. */
        Cell<?> existingCell();
        byte[] cellValue() throws IOException;
        void stageCellValue(DataOutputPlus scratch) throws IOException;
        void discardCellValue() throws IOException;
        /**
         * M3.3a-ii: whether the parked cell has ANY value bytes on the wire at all — mirrors
         * {@code Cell.Serializer.HAS_EMPTY_VALUE_MASK} — decidable BEFORE consuming
         * {@link #cellValue()}/{@link #stageCellValue}, so a streaming {@code MergeSink} can
         * learn this without paying for staging first. Must be called before either of those
         * consumes the parked cell's value (afterward, the answer is meaningless).
         */
        boolean cellHasValue();

        // ---- per-leg corrupted-tombstone validation (Gap C in-slice placement; no-ops on
        // memtable legs: the iterator path never applies UnfilteredValidation to memtable data,
        // only to sstable-attributed reads) ----
        void validateRowHeader();
        void validateMarkerHeader();

        @Override
        void close();
    }

    /**
     * One OPENED sstable leg of a cursor-served single-partition read: cursor open, partition
     * header read (deletion validated per Gap C), static row materialized — rows not yet touched.
     * The call site collects these across the {@code mostRecentPartitionTombstone} elimination
     * loop (which consumes {@link #partitionLevelDeletion()} while deciding which legs join) and
     * then finishes them through {@link CursorReads#completeSingleLeg} (1 leg — Phase 1/M1
     * behavior, bit-for-bit) or {@link CursorReads#mergeLegs} ({@code >= 2} legs — the M2.1
     * cursor-level merge).
     *
     * In merge mode this class is the byte-backed {@link MergeLeg} implementation (the interface
     * was extracted in M2.3 when the memtable adapter became its second implementation).
     */
    public static final class PendingLeg implements MergeLeg
    {
        final SSTableReader sstable;
        final TableMetadata metadata;
        final DecoratedKey key;
        final Slices legSlices;
        final ColumnFilter columnFilter;
        private final BtiCursorSeekSupport.PartitionEntry btiEntry;
        /** the query's shared value-transfer scratch (one instance across all of a read's legs) */
        private final ValueTransfer transfer;

        private SSTableCursorReader cursor; // null once closed
        private PartitionMaterializer materializer;
        private int openState;
        /** see {@link #mergeSeekOpenMarker()} */
        private DeletionTime mergeSeekOpenMarker;

        // ---- merge-mode cell-walk state (meaningful only after enterMergeMode) ----
        /** The physically-parked cell was read off the wire but not yet filter-evaluated (set when
         *  a synthetic park interposed before it). */
        private boolean evaluatePendingCell;
        private CellPath pendingPath;
        /** Zero-copy filter-test view over the cell cursor's reusable path window (see
         *  {@link #filterPathView}); re-created only when the window is re-wrapped over a
         *  grown path buffer, never per cell. */
        private CellPath filterPathView;
        private ByteBuffer filterPathWindow;
        /** canSkipValue verdict for the parked cell: its value must reconcile as EMPTY (the
         *  iterator path never deserializes it) and must not be materialized if the cell wins. */
        private boolean pendingValueSkip;
        private byte[] pendingValue;
        private boolean pendingValueMaterialized;
        private ColumnMetadata primedComplexColumn;
        /**
         * Synthetic deletion-only park: a complex column whose deletion survives the per-leg
         * filters but whose cells were ALL filtered out per-leg. The iterator path still emits a
         * deletion-only ComplexColumnData for it, so it must enter the merge as a deletion-only
         * POSITION at its column-order place — this leg parks here synthetically (no underlying
         * cursor position) before its next real position.
         */
        private boolean syntheticPark;
        private ColumnMetadata syntheticColumn;
        private final DeletionTime.ReusableDeletionTime syntheticDeletion = DeletionTime.ReusableDeletionTime.live();
        /** syntheticColumn/Deletion captured while walking the column's cells; becomes a synthetic
         *  park if no cell of the column survives the per-leg filters. */
        private boolean stashPending;

        PendingLeg(SSTableReader sstable, TableMetadata metadata, DecoratedKey key,
                   Slices legSlices, ColumnFilter columnFilter, BtiCursorSeekSupport.PartitionEntry btiEntry,
                   ValueTransfer transfer)
        {
            this.sstable = sstable;
            this.metadata = metadata;
            this.key = key;
            this.legSlices = legSlices;
            this.columnFilter = columnFilter;
            this.btiEntry = btiEntry;
            this.transfer = transfer;
        }

        void open(long position) throws IOException
        {
            // The bounds constructor seeks to the partition and validates the end-of-partition
            // marker before it, which is the reader's own single implementation of that seek; the
            // upper bound is the file length, so reading past this partition behaves as before.
            cursor = new SSTableCursorReader(sstable,
                                             Collections.singletonList(new PartitionPositionBounds(position, sstable.uncompressedLength())),
                                             null);
            materializer = new PartitionMaterializer(cursor, sstable, metadata, key, columnFilter, transfer);
            openState = materializer.openPartition(position);
            if (legSlices.isEmpty())
            {
                // a Slices.NONE leg (partition-deletion check / statics-only shapes) never needs
                // rows — on the iterator path its Slices.NONE iterator emits no unfiltereds either
                SSTableCursorReader toClose = cursor;
                cursor = null;
                toClose.close();
            }
        }

        public DeletionTime partitionLevelDeletion()
        {
            return materializer.partitionDeletion();
        }

        public Row staticRow()
        {
            return materializer.staticRow();
        }

        public Slices legSlices()
        {
            return legSlices;
        }

        public EncodingStats legStats()
        {
            return sstable.stats();
        }

        public SSTableReader sstableOrNull()
        {
            return sstable;
        }

        /** Byte-backed legs never stand in for a whole merged row: descriptor state is not a
         *  reusable live object, so the general materializing path always runs. */
        public Row consumeExistingRow()
        {
            return null;
        }

        /** Byte-backed legs have no live cell object to reuse; winners materialize through
         *  {@link #cellValue()}/{@link #cellPath()} as in M2.1. */
        public Cell<?> existingCell()
        {
            return null;
        }

        /** Phase 1/M1 single-leg completion; see {@link CursorReads#completeSingleLeg}. */
        MaterializedPartition completeSingle() throws IOException
        {
            if (cursor == null) // Slices.NONE leg: no rows by construction
                return new MaterializedPartition(partitionLevelDeletion(), staticRow(), new ArrayList<>(), null);

            BtiCursorSeekSupport.SeekPoint seekPoint = null;
            ClusteringBound<?> endBound = null;
            if (btiEntry != null && btiEntry.isIndexed() && legSlices.size() == 1)
            {
                // M1 (Phase 2) bounded materialization, BTI only. The gate already limits
                // supported reads to a single slice; a full-partition slice (BOTTOM..TOP)
                // naturally leaves both of these null and keeps the eager walk.
                Slice slice = legSlices.get(0);
                if (!slice.end().isTop())
                    endBound = slice.end();
                if (!slice.start().isBottom())
                    seekPoint = BtiCursorSeekSupport.floorBlock((BtiTableReader) sstable, btiEntry,
                                                                metadata.comparator, slice.start());
            }
            return materializer.readRows(openState, !legSlices.isEmpty(), seekPoint, endBound);
        }

        /**
         * Switches this leg's cursor to merge consumption: deletion-only complex columns stay
         * sortable POSITIONS (compaction's {@code pauseAtEmptyComplexColumns} pattern, already on
         * for Phase 1's own walk), but merged rows are built by the merge sink and complex
         * deletions are reconciled across sources first — the Phase 1 per-column side effect
         * lives in {@code materializeRowContents}, which merge consumption never runs. Since M2.2
         * the M1 row-index seek applies to merged legs too, via {@link #seekForMerge} (called
         * right after this).
         */
        public void enterMergeMode()
        {
            assert cursor != null;
            cursor.pauseAtEmptyComplexColumns(true);
        }

        /**
         * M2.2: the merge-mode twin of {@link #completeSingle}'s M1 row-index seek — same gate
         * (BTI, row-indexed partition, single slice with a real start bound), same forward-only
         * floor-block jump, same {@code IndexInfo.openDeletion} open-marker payload. Positions
         * this leg's cursor at its floor block for the slice start so the k-way merge never walks
         * the partition prefix; rows between the block start and the slice start (bounded by one
         * index block) still enter the merge and are dropped by the post-merge slicer's pre-slice
         * skip, exactly like the iterator path's in-block {@code handlePreSliceData} per leg.
         *
         * The one piece of leg state a mid-partition entry cannot recover from the stream — the
         * range-tombstone deletion open at the seek point — is retained in
         * {@link #mergeSeekOpenMarker} for {@code CursorReadMerger} to seed into its cross-leg
         * open-marker set (the merge-level analog of the single-leg path seeding
         * {@code MaterializedPartition.openMarkerAtStart}). Misaligned per-leg seek points are
         * safe: a leg's seed is valid for every clustering position from its floor block's
         * separator onward (all earlier positions are before the slice start, whose merged output
         * the slicer discards), and any close marker for the seeded deletion arrives in this
         * leg's own post-seek stream.
         *
         * No-op for BIG legs (no row-index seek mechanism, same M1 scope decision), unindexed
         * partitions, full-partition slices, and legs already positioned at-or-past the floor
         * block — a merge may freely mix seeked BTI legs with unseeked BIG legs, each handled
         * per its own format.
         */
        public void seekForMerge() throws IOException
        {
            if (btiEntry == null || !btiEntry.isIndexed() || legSlices.size() != 1)
                return;
            Slice slice = legSlices.get(0);
            if (slice.start().isBottom())
                return;
            // PARTITION_END/DONE: the partition has no unfiltereds to seek over
            if (!isState(cursor.state(), ROW_START | TOMBSTONE_START))
                return;
            BtiCursorSeekSupport.SeekPoint seekPoint = BtiCursorSeekSupport.floorBlock((BtiTableReader) sstable, btiEntry,
                                                                                      metadata.comparator, slice.start());
            // The current unfiltered's flags byte is already consumed, so its start is one byte
            // behind the cursor. Mirror ForwardIndexedReader.setForSlice (and the single-leg
            // readRows seek): only seek FORWARD — when the floor block is the one already being
            // read, keep the sequential walk and let the merge's open-marker set build from the
            // stream alone.
            if (seekPoint.dataPosition <= cursor.position() - 1)
                return;
            cursor.seekUnfiltered(seekPoint.dataPosition);
            SSTABLE_LEG_ROW_INDEX_SEEKS.incrementAndGet();
            if (TEST_DROP_MERGE_SEEK_OPEN_MARKER || seekPoint.openMarker == null)
                return;
            mergeSeekOpenMarker = TEST_SKEW_MERGE_SEEK_OPEN_MARKER
                                  ? DeletionTime.buildUnsafeWithUnsignedInteger(seekPoint.openMarker.markedForDeleteAt() + 1,
                                                                               seekPoint.openMarker.localDeletionTimeUnsignedInteger())
                                  : copyOf(seekPoint.openMarker);
        }

        /** The open-range-tombstone deletion at this leg's merge-mode seek point (immutable copy
         *  of the BTI row index's {@code IndexInfo.openDeletion}), or null when no merge-mode
         *  seek was issued or no deletion is open at the target block start. */
        public DeletionTime mergeSeekOpenMarker()
        {
            return mergeSeekOpenMarker;
        }

        // ---- merge-source surface (consumed by CursorReadMerger) ----

        int openStateAfterHeader()
        {
            return openState;
        }

        public int cursorState()
        {
            return cursor.state();
        }

        public UnfilteredDescriptor unfiltered()
        {
            return materializer.uDesc;
        }

        /** Loads the row/marker header at the current unfiltered start into {@link #unfiltered()}
         *  and resets the per-row cell-walk state. */
        public void readUnfilteredHeader() throws IOException
        {
            int s = cursor.state();
            if (s == ROW_START)
                cursor.readRowHeader(materializer.uDesc);
            else if (s == TOMBSTONE_START)
                cursor.readTombstoneMarker(materializer.uDesc);
            else
                throw new IllegalStateException("unexpected cursor state " + s);
            primedComplexColumn = null;
            stashPending = false;
            syntheticPark = false;
            evaluatePendingCell = false;
        }

        public void continueReading() throws IOException
        {
            cursor.continueReading();
        }

        public ClusteringPrefix<?> materializeClusteringPrefix()
        {
            return materializer.toClusteringPrefix();
        }

        /** The current bound/boundary descriptor's clustering values, decoded ONCE for reuse
         *  under different prefix kinds (see {@code CursorReadMerger.mergeMarkerGroup}). */
        public byte[][] materializeBoundValues()
        {
            return materializer.boundValues();
        }

        public boolean parkedAtCellPosition()
        {
            return syntheticPark || isState(cursor.state(), CELL_VALUE_START | CELL_END);
        }

        public boolean needsCellAdvance()
        {
            return !syntheticPark && (evaluatePendingCell || cursor.state() == CELL_HEADER_START);
        }

        /**
         * Parks this leg at its next merge-relevant cell position, applying the query's
         * {@code DeserializationHelper}/{@code ColumnFilter} rules per leg, BELOW reconciliation —
         * exactly where the iterator path applies them (at deserialization): non-fetched columns
         * and tester-excluded paths are skipped without snapshotting, dropped-column cells are
         * skipped, fetched-but-not-queried cells whose timestamp the row liveness already covers
         * are skipped entirely (CASSANDRA-7085 rule), and surviving fetched-not-queried cells are
         * marked value-skippable so reconciliation sees EMPTY values just as the iterator path
         * does. Positions surfaced: real cells, natural deletion-only complex columns, and
         * synthetic deletion-only parks (see {@link #syntheticPark}).
         *
         * @param validateCells when true (corrupted_tombstone_strategy enabled AND the row is
         *        in-slice), surviving cells/complex deletions are validated per leg — mirroring
         *        the iterator path's per-leg {@code maybeValidateUnfiltered} coverage, which
         *        validates each leg's materialized row including cells that later LOSE the merge
         */
        public void ensureParkedAtCell(boolean validateCells) throws IOException
        {
            if (syntheticPark)
                return;
            DeserializationHelper helper = materializer.filterHelper;
            boolean evaluate = evaluatePendingCell;
            evaluatePendingCell = false;
            for (;;)
            {
                if (!evaluate)
                {
                    if (cursor.state() != CELL_HEADER_START)
                    {
                        // row exhausted (or already parked): a still-pending stashed complex
                        // deletion must surface as a deletion-only position first
                        if (stashPending && !isState(cursor.state(), CELL_VALUE_START | CELL_END))
                            issueSyntheticPark();
                        return;
                    }
                    cursor.readCellHeader();
                    if (!isState(cursor.state(), CELL_VALUE_START | CELL_END))
                    {
                        // the cursor's internal dropped-column filter consumed the row tail
                        if (stashPending)
                            issueSyntheticPark();
                        return;
                    }
                }
                evaluate = false;

                SSTableCursorReader.CellCursor cc = cursor.cellCursor();
                ColumnMetadata column = cc.cellColumn;
                if (!cc.producedCell)
                {
                    // natural deletion-only complex-column position (pauseAtEmptyComplexColumns)
                    if (stashPending && !sameColumn(syntheticColumn, column))
                    {
                        issueSyntheticPark();
                        evaluatePendingCell = true;
                        return;
                    }
                    primeComplexColumn(helper, column);
                    if (helper.includes(column))
                    {
                        if (validateCells && !cc.complexDeletion.isLive()
                            && !helper.isDroppedComplexDeletion(cc.complexDeletion)
                            && !cc.complexDeletion.validate())
                            UnfilteredValidation.handleInvalid(metadata, key, sstable,
                                                              "complexDeletion=" + cc.complexDeletion);
                        return; // parks; the contribution is folded (dropped-filtered) by the merge
                    }
                    // non-fetched column: consume the position, materializing nothing (parity with
                    // the iterator path's skipped column)
                    if (cursor.state() == CELL_END)
                        cursor.continueReading();
                    continue;
                }

                boolean isComplex = column.isComplex();
                if (isComplex && !sameColumn(column, primedComplexColumn))
                {
                    // entering a new complex column run: a stash from a previous column must park
                    // before this column's first cell to keep column order
                    if (stashPending)
                    {
                        issueSyntheticPark();
                        evaluatePendingCell = true;
                        return;
                    }
                    primeComplexColumn(helper, column);
                    if (helper.includes(column) && !cc.complexDeletion.isLive()
                        && !helper.isDroppedComplexDeletion(cc.complexDeletion))
                    {
                        stashPending = true;
                        syntheticColumn = column;
                        syntheticDeletion.reset(cc.complexDeletion);
                        if (validateCells && !cc.complexDeletion.validate())
                            UnfilteredValidation.handleInvalid(metadata, key, sstable,
                                                              "complexDeletion=" + cc.complexDeletion);
                    }
                }
                else if (!isComplex && stashPending)
                {
                    issueSyntheticPark();
                    evaluatePendingCell = true;
                    return;
                }

                pendingPath = null;
                pendingValue = null;
                pendingValueMaterialized = false;
                pendingValueSkip = false;
                if (!helper.includes(column))
                {
                    skipParkedCell();
                    continue;
                }
                CellPath path = null;
                if (isComplex)
                {
                    // filter tests only READ the path during the call (Tester.hasSubselection
                    // compares bytes via the column's cellPathComparator and retains nothing) and
                    // are no-ops entirely when no per-path subselection is active (null tester),
                    // so a zero-copy view over the reusable path window suffices here — the real
                    // materialization (copy + wrap + create) stays lazy in cellPath(), paid only
                    // for cells that WIN the merge and are actually emitted
                    path = filterPathView(cc);
                    if (!helper.includes(path))
                    {
                        skipParkedCell();
                        continue;
                    }
                }
                if (helper.isDropped(column, cc.cellLiveness.timestamp(), isComplex))
                {
                    skipParkedCell();
                    continue;
                }
                // DeserializationHelper.includes(cell, rowLiveness): a fetched-but-not-queried
                // cell is skipped entirely when the row's own liveness already proves the row
                long rowTimestamp = materializer.uDesc.livenessInfo().timestamp();
                pendingValueSkip = isComplex ? helper.canSkipValue(path) : helper.canSkipValue(column);
                if (pendingValueSkip && cc.cellLiveness.timestamp() < rowTimestamp)
                {
                    skipParkedCell();
                    continue;
                }
                if (stashPending && sameColumn(syntheticColumn, column))
                    stashPending = false; // a surviving cell's park carries the column deletion itself
                if (validateCells && hasInvalidCellDeletion(cc.cellLiveness.ttl(), cc.cellLiveness.localDeletionTime()))
                    UnfilteredValidation.handleInvalid(metadata, key, sstable, "cellLiveness=" + cc.cellLiveness);
                return;
            }
        }

        private void primeComplexColumn(DeserializationHelper helper, ColumnMetadata column)
        {
            helper.startOfComplexColumn(column);
            primedComplexColumn = column;
        }

        private void issueSyntheticPark()
        {
            syntheticPark = true;
            stashPending = false;
        }

        private void skipParkedCell() throws IOException
        {
            if (cursor.state() == CELL_VALUE_START)
                cursor.skipCellValue();
            if (cursor.state() == CELL_END)
                cursor.continueReading();
        }

        /** Consumes the current cell position after its merge group was reconciled. */
        public void advancePastCellPosition() throws IOException
        {
            if (syntheticPark)
            {
                syntheticPark = false;
                // the physically-parked position behind the synthetic park was read but never
                // evaluated; the next ensureParkedAtCell resumes the filter walk from it
                if (isState(cursor.state(), CELL_VALUE_START | CELL_END))
                    evaluatePendingCell = true;
                return;
            }
            if (cursor.state() == CELL_VALUE_START)
                throw new IllegalStateException("cell value neither consumed nor skipped before advancing");
            if (cursor.state() == CELL_END)
                cursor.continueReading();
        }

        public boolean cellProduced()
        {
            return !syntheticPark && cursor.cellCursor().producedCell;
        }

        public ColumnMetadata cellColumn()
        {
            return syntheticPark ? syntheticColumn : cursor.cellCursor().cellColumn;
        }

        public CellLivenessInfo cellLiveness()
        {
            return cursor.cellCursor().cellLiveness;
        }

        public ByteBuffer cellPathWindow()
        {
            return cursor.cellCursor().cellPathWindow();
        }

        /** This leg's complex-deletion contribution at the current parked position — already
         *  per-leg filtered (LIVE when the dropped-column filter discards it, mirroring the
         *  iterator path's readComplexColumn). */
        public DeletionTime cellComplexDeletion()
        {
            if (syntheticPark)
                return syntheticDeletion;
            SSTableCursorReader.CellCursor cc = cursor.cellCursor();
            if (cc.complexDeletion.isLive() || materializer.filterHelper.isDroppedComplexDeletion(cc.complexDeletion))
                return DeletionTime.LIVE;
            return cc.complexDeletion;
        }

        /** The parked cell's path, materialized once (null for simple columns). */
        public CellPath cellPath()
        {
            if (pendingPath != null)
                return pendingPath;
            SSTableCursorReader.CellCursor cc = cursor.cellCursor();
            return cc.cellPathLength < 0 ? null : materializePendingPath(cc);
        }

        private CellPath materializePendingPath(SSTableCursorReader.CellCursor cc)
        {
            pendingPath = CellPath.create(ByteBuffer.wrap(Arrays.copyOf(cc.cellPathBuffer, cc.cellPathLength)));
            return pendingPath;
        }

        /**
         * Filter-test view of the parked cell's path: wraps the cell cursor's reusable path
         * window with no copying. The window's contents are only stable until the next cell
         * header is read, so this view must never escape the immediate filter-test call —
         * anything that outlives the park goes through {@link #cellPath()} instead.
         */
        private CellPath filterPathView(SSTableCursorReader.CellCursor cc)
        {
            ByteBuffer window = cc.cellPathWindow();
            if (window != filterPathWindow)
            {
                filterPathWindow = window;
                filterPathView = CellPath.create(window);
            }
            return filterPathView;
        }

        /**
         * The parked cell's RAW value bytes, materialized once through the Phase 1 one-copy
         * {@link CellValueCapture} machinery: empty for valueless cells and for value-skippable
         * (fetched-but-not-queried) cells — the iterator path reconciles those on EMPTY values,
         * so the merge must too.
         */
        public byte[] cellValue() throws IOException
        {
            if (pendingValueMaterialized)
                return pendingValue;
            byte[] value = ByteArrayAccessor.instance.empty();
            if (cursor.state() == CELL_VALUE_START)
            {
                if (pendingValueSkip)
                {
                    cursor.skipCellValue();
                }
                else
                {
                    SSTableCursorReader.CellCursor cc = cursor.cellCursor();
                    int fixedLength = cc.cellType.valueLengthIfFixed();
                    if (fixedLength >= 0)
                    {
                        byte[] target = fixedLength == 0 ? ByteArrayAccessor.instance.empty() : new byte[fixedLength];
                        cursor.copyCellValue(transfer.valueCapture.prepareFixed(target), target);
                        value = transfer.valueCapture.finish();
                    }
                    else
                    {
                        cursor.copyCellValue(transfer.valueCapture.prepareVariable(), transfer.transferBuffer);
                        value = transfer.valueCapture.finish();
                    }
                }
            }
            pendingValue = value;
            pendingValueMaterialized = true;
            return value;
        }

        /**
         * Streams the parked cell's RAW value bytes — the same form {@link #cellValue()}
         * produces: length vint stripped, nothing written for valueless and value-skippable
         * cells — into {@code scratch} WITHOUT materializing a final value array. Used by the
         * merge's tie-break comparison ({@code CursorReadMerger.mergeCellGroup}'s COMPARE arm,
         * compaction's {@code tempCellBuffer} pattern), where most staged values lose the tie
         * and a freshly-allocated array would be immediate garbage. Consumes the cursor's value
         * exactly like {@link #cellValue()}; the caller promotes the winning scratch bytes to a
         * real array itself, and a loser's bytes are simply overwritten by the next stage.
         */
        public void stageCellValue(DataOutputPlus scratch) throws IOException
        {
            if (pendingValueMaterialized)
            {
                // already materialized (defensive: cannot happen mid-tie today, where staging
                // always precedes emission-time materialization)
                scratch.write(pendingValue, 0, pendingValue.length);
                return;
            }
            if (cursor.state() == CELL_VALUE_START)
            {
                if (pendingValueSkip)
                    cursor.skipCellValue(); // reconciles as EMPTY, like cellValue()
                else
                    cursor.copyCellValue(scratch, transfer.transferBuffer);
            }
        }

        /**
         * M3.3a-ii: whether the parked cell has any value bytes at all, from the OUTPUT's point of
         * view — mirrors {@link #cellValue()}'s own gating exactly (same three conditions, checked
         * BEFORE consuming): a synthetic (deletion-only) park never has a value; a cell whose wire
         * position carries no value section ({@code cursor.state() != CELL_VALUE_START}, i.e. a
         * tombstone or an intentionally empty value) has none; and a value-SKIPPABLE cell
         * ({@link #pendingValueSkip} — fetched-but-not-queried, {@code cellValue()}'s own
         * "reconciles as EMPTY" case) has none EITHER, even though real bytes sit on the wire,
         * because {@link #stageCellValue}/{@link #cellValue()} never expose them for such a cell.
         */
        public boolean cellHasValue()
        {
            return !syntheticPark && cursor.state() == CELL_VALUE_START && !pendingValueSkip;
        }

        /** Discards the parked cell's pending value bytes, if any remain unconsumed. */
        public void discardCellValue() throws IOException
        {
            if (!syntheticPark && cursor.state() == CELL_VALUE_START)
                cursor.skipCellValue();
        }

        // ---- per-leg corrupted-tombstone validation (merge mode; Gap C in-slice placement) ----

        public void validateRowHeader()
        {
            UnfilteredDescriptor uDesc = materializer.uDesc;
            if (!uDesc.deletionTime().validate())
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "rowDeletion=" + uDesc.deletionTime());
            if (hasInvalidRowLiveness(uDesc.livenessInfo().ttl(), uDesc.livenessInfo().localExpirationTime()))
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "rowLiveness=" + uDesc.livenessInfo());
        }

        public void validateMarkerHeader()
        {
            UnfilteredDescriptor uDesc = materializer.uDesc;
            if (!uDesc.deletionTime().validate())
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "rangeTombstoneDeletion=" + uDesc.deletionTime());
            if (uDesc.isBoundary() && !uDesc.deletionTime2().validate())
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "rangeTombstoneDeletion2=" + uDesc.deletionTime2());
        }

        @Override
        public void close()
        {
            if (cursor != null)
            {
                SSTableCursorReader toClose = cursor;
                cursor = null;
                toClose.close();
            }
        }
    }

    /**
     * The M2.1 read-side merge sink: clustering-first events (see {@code CursorReadMerger.MergeSink})
     * materialized through the exact Phase 1 machinery — ONE reused {@code BTreeRow.sortedBuilder}
     * across all merged rows (journal Gap B's builder-reuse discipline), rows that merge to empty
     * dropped exactly like {@code Row.Merger} returning null.
     */
    static final class MaterializingMergeSink implements CursorReadMerger.MergeSink
    {
        private final Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        private final List<Unfiltered> unfiltereds = new ArrayList<>();
        private long materializedCount;

        @Override
        public void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedDeletion)
        {
            rowBuilder.newRow(clustering);
            rowBuilder.addPrimaryKeyLivenessInfo(mergedLiveness);
            rowBuilder.addRowDeletion(mergedDeletion.isLive()
                                      ? Row.Deletion.LIVE
                                      : Row.Deletion.regular(mergedDeletion));
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion)
        {
            rowBuilder.addComplexDeletion(column, mergedComplexDeletion);
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            rowBuilder.addCell(cell);
        }

        @Override
        public void endRow()
        {
            Row row = rowBuilder.build();
            // mirrors Row.Merger.merge returning null for a row that merged to nothing
            if (!row.isEmpty())
            {
                unfiltereds.add(row);
                materializedCount++;
            }
        }

        /** M3.2c: discards the partially-built row of an ABANDONED group (a regular-column filter
         *  failure discovered mid-cell-walk or at row end). Build-and-discard is what resets the
         *  shared sorted builder for the next group — the discarded partial row is the bounded
         *  wasted allocation the M3.2 plan prices for late-resolving filter columns. */
        void abandonRow()
        {
            rowBuilder.build();
        }

        @Override
        public void addRow(Row row)
        {
            // M2.3 whole-row escape hatch: the merged row IS this already-live (memtable) object —
            // mirrors Row.Merger.merge's single-version fast path returning the row unchanged.
            // The empty-drop is defensive only: memtable iterators never emit empty rows.
            if (!row.isEmpty())
            {
                unfiltereds.add(row);
                materializedCount++;
            }
        }

        @Override
        public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            unfiltereds.add(marker);
            materializedCount++;
        }

        List<Unfiltered> unfiltereds()
        {
            return unfiltereds;
        }

        long materializedCount()
        {
            return materializedCount;
        }
    }

    /**
     * M3.1: the production-bound decorator around {@link MaterializingMergeSink} — a CONSERVATIVE
     * bound, not a counter replacement. It drives a detached twin of the authoritative
     * {@code DataLimits} counter ({@link #limitBoundFor} builds it with the exact
     * {@code newCounter} arguments {@code ReadCommand.executeLocally} will use above) over every
     * merged row the slicer will EMIT, and flips {@link #wantsMore()} once that counter says it is
     * done — so the merge stops producing exactly where the top counter will stop consuming.
     * Because the authoritative counter stays in place above, over-production here is invisible
     * (the counter discards the excess exactly as it discards today's full-slice production);
     * under-production is the only failure mode and is byte-divergent by construction.
     *
     * Twin-fidelity notes (each one is a place a hand-rolled twin could drift, avoided by REUSING
     * the production counter object itself):
     * <ul>
     *   <li><b>Liveness</b>: rows are counted via {@code Counter.countRow} → {@code isLive(row)} →
     *       {@code row.hasLiveData(nowInSec, enforceStrictLiveness)} ({@code BTreeRow.java:206}) —
     *       the exact predicate, {@code nowInSec} included, the top counter applies. The merge
     *       CORE stays nowInSec-free (FINDING #12's no-purge-machinery property); the liveness
     *       semantics live entirely in this decorator, on already-materialized rows. The
     *       {@code withoutPurgeableTombstones} stage between the merge and the top counter cannot
     *       flip a verdict: it only removes data that is not live at the same nowInSec.</li>
     *   <li><b>Emitted-surface filter</b>: rows at-or-before the slice start are materialized by
     *       the merge (BIG legs walk from the partition head) but DISCARDED by
     *       {@code SlicedMaterializedIterator}'s non-strict pre-slice skip, so the top counter
     *       never sees them — counting one would stop production early. {@code countsTowardLimit}
     *       twins the slicer's exact predicate (count iff strictly after the slice start; the
     *       merge core's slice end-stop already guarantees nothing at-or-past the end reaches this
     *       sink). Sticky: the merged stream is clustering-ordered.</li>
     *   <li><b>Paging resume</b>: {@code CQLPagingLimits}' counter seeds its per-partition count
     *       at {@code countPartition} (called by {@code mergeLegs} with the real key and merged
     *       static row before any row event) — production code, not a twin.</li>
     *   <li><b>Empty-merged rows</b>: a group that merges to nothing emits no events; a row the
     *       inner sink drops as empty is never counted (an empty row has no live data — counting
     *       it would be a no-op anyway, but the emitted row object is also what countRow needs).</li>
     *   <li><b>Markers and statics</b> never count and never stop: {@code CQLCounter} counts rows
     *       only, and its static-row arm fires at partition CLOSE (can never stop production
     *       early), so marker events pass straight through.</li>
     * </ul>
     *
     * <b>M3.2d composition</b>: this class is UNCHANGED by M3.2d — it always wraps the base
     * {@link MaterializingMergeSink} directly ({@code inner} is that base, never
     * {@link RowLevelFilterProbe}). When row-level filter pushdown is ALSO engaged for the same
     * query, {@code CursorReads.mergeLegs} instead makes {@code RowLevelFilterProbe} the outer
     * sink and THIS object its {@code next} — so {@code endRow()}/{@code addRow()} here still only
     * ever fire for a row {@code CursorReadMerger} did not abandon (the filter probe forwards every
     * event unconditionally; it is {@code CursorReadMerger} itself that never calls the top sink's
     * {@code endRow} for an abandoned row). The net effect: {@link #counter} only ever counts rows
     * that also survive the filter, without this class needing to know filtering is happening at
     * all.
     */
    static final class LimitingMergeSink implements CursorReadMerger.MergeSink
    {
        private final MaterializingMergeSink inner;
        private final DataLimits.Counter counter;
        private final ClusteringComparator comparator;
        /** The emitted slice's start bound, or null when the slice starts at BOTTOM. */
        private final ClusteringBound<?> sliceStart;
        /** Sticky in-slice latch — the merged stream is clustering-ordered, so once one row is
         *  strictly past the slice start every later row is too and the comparison is skipped. */
        private boolean pastSliceStart;
        /** Whether the row currently being streamed (startRow..endRow) is on the emitted surface. */
        private boolean currentRowCounts;

        LimitingMergeSink(MaterializingMergeSink inner, DataLimits.Counter counter,
                          ClusteringComparator comparator, Slice slice)
        {
            this.inner = inner;
            this.counter = counter;
            this.comparator = comparator;
            this.sliceStart = slice.start().isBottom() ? null : slice.start();
            this.pastSliceStart = sliceStart == null;
        }

        /** The slicer-emission twin: {@code SlicedMaterializedIterator.computeNextInSlice} skips
         *  pre-slice data with a NON-strict comparison, so only rows strictly after the slice
         *  start reach the top counter. */
        private boolean countsTowardLimit(ClusteringPrefix<?> clustering)
        {
            if (!pastSliceStart)
                pastSliceStart = comparator.compare(clustering, (ClusteringPrefix<?>) sliceStart) > 0;
            return pastSliceStart;
        }

        @Override
        public void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            currentRowCounts = countsTowardLimit(clustering);
            inner.startRow(clustering, mergedLiveness, mergedRowDeletion);
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion)
        {
            inner.addComplexDeletion(column, mergedComplexDeletion);
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            inner.addCell(cell);
        }

        @Override
        public void endRow()
        {
            long before = inner.materializedCount();
            inner.endRow();
            if (currentRowCounts && inner.materializedCount() != before)
            {
                List<Unfiltered> unfiltereds = inner.unfiltereds();
                counter.countRow((Row) unfiltereds.get(unfiltereds.size() - 1));
            }
        }

        @Override
        public void addRow(Row row)
        {
            inner.addRow(row);
            if (countsTowardLimit(row.clustering()))
                counter.countRow(row);
        }

        @Override
        public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            inner.addRangeTombstoneMarker(marker);
        }

        @Override
        public boolean wantsMore()
        {
            // isDoneForPartition == "the top counter would have stopped": the row limit is
            // reached OR the (possibly paging-seeded) per-partition limit is — exactly the
            // stop()/stopInPartition() conditions of CQLCounter.incrementRowCount
            return !counter.isDoneForPartition();
        }
    }

    /**
     * M3.3b-i (CASSANDRA-20428): wire-level twin of {@code ReadCommand.withMetricsRecording}'s
     * tombstone/live-row accounting and tombstone-overwhelming abort, hooked into
     * {@link TranscodeMergeSink}'s existing purge-aware row/cell/marker call sites.
     * {@code SinglePartitionReadCommand.queryStorageToResponseBytes}'s response path never runs
     * {@code executeLocally}'s stack (that is the entire point of the transcode path — no
     * object materialization), so without this twin a transcode-served read would silently skip
     * BOTH the {@code tombstone_failure_threshold} abort AND the scan histograms/counters a
     * materializing read always produces. Every predicate below is taken from real source rather
     * than re-derived by feel:
     * <ul>
     *   <li>{@link Cell#isLive(long, long, int)} for per-cell liveness;</li>
     *   <li>{@code BTreeRow}'s {@code minDeletionTime} family (one contribution per liveness-info/
     *       row-deletion/complex-deletion/cell) for the row-level {@code hasDeletion(nowInSec)}
     *       twin the "primary-key-only deletion, no cell tombstone" branch needs;</li>
     *   <li>{@code ReadCommand}'s package-private {@code MetricRecording}/{@code countTombstone}
     *       for the threshold/count/histogram shape itself.</li>
     * </ul>
     * Not a byte-identity concern (this never touches the wire) — a count/exception-parity
     * concern, verified by a dedicated differential scenario that forces the SAME abort from both
     * the gate-on (this class) and gate-off (real {@code MetricRecording}) paths.
     */
    static final class TombstoneScanGuard
    {
        private final ReadCommand command;
        private final TableMetrics metric;
        private final long startTimeNanos;
        private final long nowInSec;
        private final DecoratedKey key;
        private final int failureThreshold;
        private final int warningThreshold;
        private final boolean respectTombstoneThresholds;

        private int liveRows;
        private int tombstones;

        // per-row scratch, reset by startRow / wholeRow
        private long rowMinDeletionTime;
        private boolean rowLivenessLive;
        private boolean rowHasTombstoneCell;
        private ClusteringPrefix<?> rowClustering;

        TombstoneScanGuard(ReadCommand command, TableMetrics metric, long startTimeNanos, long nowInSec, DecoratedKey key)
        {
            this.command = command;
            this.metric = metric;
            this.startTimeNanos = startTimeNanos;
            this.nowInSec = nowInSec;
            this.key = key;
            this.failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            this.warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();
            this.respectTombstoneThresholds = !SchemaConstants.isLocalSystemKeyspace(command.metadata().keyspace);
        }

        void startRow(ClusteringPrefix<?> clustering, LivenessInfo liveness, DeletionTime rowDeletion)
        {
            rowClustering = clustering;
            rowMinDeletionTime = Math.min(minDeletionTime(liveness), minDeletionTime(rowDeletion));
            rowLivenessLive = liveness.isLive(nowInSec);
            rowHasTombstoneCell = false;
        }

        /** Only ever called for a genuinely non-live complex deletion (see {@link TranscodeMergeSink
         *  #addComplexDeletion}'s own early return) — {@code minDeletionTime} of a non-live
         *  {@code DeletionTime} is always {@code Long.MIN_VALUE}. */
        void complexDeletion()
        {
            rowMinDeletionTime = Long.MIN_VALUE;
        }

        /** @param timestamp/ttl/localDeletionTime the POST-purge values {@link TranscodeMergeSink}
         *          is about to write — exactly what a materialize-then-purge {@code Cell} would
         *          report, including the expired-but-not-gcable convert-to-tombstone hijack. */
        void cell(long timestamp, int ttl, long localDeletionTime)
        {
            boolean isTombstone = localDeletionTime != Cell.NO_DELETION_TIME && ttl == Cell.NO_TTL;
            rowMinDeletionTime = Math.min(rowMinDeletionTime, isTombstone ? Long.MIN_VALUE : localDeletionTime);
            boolean isLive = localDeletionTime == Cell.NO_DELETION_TIME || (ttl != Cell.NO_TTL && nowInSec < localDeletionTime);
            if (!isLive)
            {
                rowHasTombstoneCell = true;
                countTombstone(rowClustering);
            }
        }

        void endRow()
        {
            boolean rowHasDeletion = nowInSec >= rowMinDeletionTime;
            if (rowLivenessLive)
                ++liveRows;
            else if (rowHasDeletion && !rowHasTombstoneCell)
                countTombstone(rowClustering);
        }

        /** The M2.3 whole-row escape hatch: a REAL, already-purged {@code Row} object exists, so
         *  use the real predicates directly instead of the hand-mirrored per-event formulas above. */
        void wholeRow(Row row)
        {
            boolean hasTombstones = false;
            boolean rowHasDeletion = row.hasDeletion(nowInSec);
            if (rowHasDeletion)
            {
                for (Cell<?> cell : row.cells())
                {
                    if (!cell.isLive(nowInSec))
                    {
                        countTombstone(row.clustering());
                        hasTombstones = true;
                    }
                }
            }
            if (row.hasLiveData(nowInSec, false))
                ++liveRows;
            else if (!row.primaryKeyLivenessInfo().isLive(nowInSec) && rowHasDeletion && !hasTombstones)
                countTombstone(row.clustering());
        }

        void marker(ClusteringPrefix<?> clustering)
        {
            countTombstone(clustering);
        }

        private static long minDeletionTime(LivenessInfo info)
        {
            return info.isExpiring() ? info.localExpirationTime() : Cell.MAX_DELETION_TIME;
        }

        private static long minDeletionTime(DeletionTime dt)
        {
            return dt.isLive() ? Cell.MAX_DELETION_TIME : Long.MIN_VALUE;
        }

        private void countTombstone(ClusteringPrefix<?> clustering)
        {
            ++tombstones;
            if (tombstones > failureThreshold && respectTombstoneThresholds)
            {
                String query = command.toCQLString();
                Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)", failureThreshold, query);
                metric.tombstoneFailures.inc();
                if (command.isTrackingWarnings())
                {
                    MessageParams.remove(ParamType.TOMBSTONE_WARNING);
                    MessageParams.add(ParamType.TOMBSTONE_FAIL, tombstones);
                }
                throw new TombstoneOverwhelmingException(tombstones, query, command.metadata(), key, clustering);
            }
        }

        /** Must be called once, after the merge this guard was attached to has fully completed
         *  (mirrors {@link TranscodeMergeSink#finishPartition}'s own contract) — the
         *  {@code MetricRecording.onClose()} twin: latency, histograms, counters, warn logging. */
        void finish()
        {
            command.recordLatency(metric, nanoTime() - startTimeNanos);
            metric.tombstoneScannedHistogram.update(tombstones);
            metric.liveScannedHistogram.update(liveRows);
            metric.totalRowsRead.inc(liveRows);
            if (liveRows > 0)
                metric.topReadPartitionRowCount.addSample(key.getKey(), liveRows);
            if (tombstones > 0)
                metric.topReadPartitionTombstoneCount.addSample(key.getKey(), tombstones);

            boolean warnTombstones = tombstones > warningThreshold && respectTombstoneThresholds;
            if (warnTombstones)
            {
                String msg = String.format(
                    "Read %d live rows and %d tombstone cells for query %1.512s; token %s (see tombstone_warn_threshold)",
                    liveRows, tombstones, command.toCQLString(), key.getToken());
                if (command.isTrackingWarnings())
                    MessageParams.add(ParamType.TOMBSTONE_WARNING, tombstones);
                else
                    ClientWarn.instance.warn(msg);
                if (tombstones < failureThreshold)
                    metric.tombstoneWarnings.inc();
                logger.warn(msg);
            }
            Tracing.trace("Read {} live rows and {} tombstone cells{}",
                          liveRows, tombstones, (warnTombstones ? " (see tombstone_warn_threshold)" : ""));
        }
    }

    /**
     * M3.3a-i (CASSANDRA-20428, Phase 4 seam iv, wire-format-only slice): transcodes merge events
     * DIRECTLY into {@code ReadResponse} (MESSAGING-flavor) wire bytes via {@link ResponseWireWriter},
     * instead of materializing {@code Row}/{@code Cell} objects first — the whole point of M3.3a. In
     * THIS slice {@link #addCell} still receives an already-materialized {@code Cell<?>} exactly as
     * the merge core calls it today (M3.3a-ii's job is byte-level streaming for sstable-won cell
     * VALUES instead, via a future {@code MergeSink} default-method extension); the target here is
     * proving the WIRE FORMAT reconstruction itself — headers, delta encoding, flags/extended-flags,
     * column-subset bitmap, complex deletions, range tombstone marker framing, EOP — is correct.
     * {@code RowFilter} composition ({@link RowLevelFilterProbe}) is explicitly OUT of scope: a wire
     * writer that already flushed bytes cannot un-flush them the way
     * {@code MaterializingMergeSink.abandonRow()}'s build-and-reset can (M3.3a's own risk note #2);
     * {@link ResponseWireWriter}'s row-local staging (never flushes before {@code endRow} confirms
     * completion) sets up for that composition without needing rework, but nothing here composes
     * with the probe yet.
     *
     * <b>The gcable-purge twin</b>: {@code ReadCommand.withoutPurgeableTombstones} purges gcable
     * tombstones ABOVE the merge today (nowInSec, gcBefore, oldestUnrepairedTombstone,
     * onlyPurgeRepairedTombstones, a constant-true purge evaluator — see
     * {@code org.apache.cassandra.db.partitions.PurgeFunction}, read directly for this class rather
     * than assumed); the transcode path must apply the IDENTICAL purge decisions at emission, since
     * the merge core itself stays purge-free (FINDING #12's "no purge machinery in the merger"
     * property — unchanged by this class). This mirrors {@link ScanStatsAccumulator}'s own purge
     * twin (built for a different purpose, M3.2b's dropped-row scan-metrics accounting) for the
     * {@code shouldPurge} predicate itself, but — unlike that accumulator, which only ever
     * CLASSIFIES already-decided content for metrics — this class must decide WHAT BYTES SURVIVE, so
     * it reuses the REAL production purge primitives wherever an object already exists to call them
     * on, rather than re-deriving their effect by hand:
     * <ul>
     *   <li>PK liveness / row deletion / complex deletion: {@link DeletionPurger#shouldPurge}'s
     *       default methods (the exact ones {@code PurgeFunction}'s own {@code purger} field calls)
     *       decide purge-to-{@code LIVE}/{@code EMPTY} — verified against {@code BTreeRow.purge} and
     *       {@code ComplexColumnData.purge}'s exact formulas (read directly, not assumed).</li>
     *   <li>Cells: {@link Cell#purge(DeletionPurger, long)} — the REAL {@code AbstractCell.purge},
     *       called directly on the already-materialized {@code Cell<?>} object {@link #addCell}
     *       receives, including its expired-but-not-purgeable-to-tombstone conversion hijack
     *       ({@code AbstractCell.purge}'s {@code BufferCell.tombstone(...).purge(...)} re-test at
     *       {@code localDeletionTime - ttl}). No hand-rolled twin of that conversion exists here —
     *       this IS the production method, eliminating the risk of a subtly wrong reimplementation
     *       for what the M3.3a plan flags as one of the two hardest sub-problems.</li>
     *   <li>Range tombstone markers: {@code PurgeFunction.applyToMarker}'s exact combining logic
     *       (read from source) is hand-mirrored, since that method is {@code protected} on an
     *       unrelated {@code Transformation} base with no standalone entry point — a boundary drops
     *       entirely only when BOTH sides purge, degrades to the surviving side's bound marker when
     *       only one does, and passes through unchanged otherwise; a plain bound drops iff its
     *       deletion purges. {@code reversed} is hardcoded {@code false} throughout, matching
     *       {@link ScanStatsAccumulator#accountMarker}'s own precedent (cursor reads do not yet
     *       support reversed-order queries).</li>
     * </ul>
     * A dropped-to-nothing row (empty liveness, live deletion, zero surviving columns after purge —
     * the SAME emptiness test {@code ResponseWireWriter.endRow}/{@code MaterializingMergeSink.endRow}
     * already apply) and a dropped-to-nothing marker never reach the wire at all, exactly like the
     * iterator path's purged stream.
     *
     * <b>Slice-boundary trimming and artificial markers</b> (found missing, then fixed, while
     * extending this class's own test corpus with a sliced scenario — recorded honestly per this
     * ticket's standing practice): on the {@code MaterializingMergeSink} path, the raw merge stream
     * (which a BIG-format leg walks EAGERLY from the partition start, per M1's scope decision — see
     * {@code CursorReadMerger}'s class javadoc) is NOT itself slice-trimmed; {@code
     * SlicedMaterializedIterator} does that trimming, and SYNTHESIZES the artificial open/close
     * range-tombstone markers at the slice bounds, as a POST-PROCESSING pass over {@code
     * MaterializingMergeSink.unfiltereds()} — a pass this sink has no equivalent of if it only
     * forwards events as they stream in. {@link #admitOrSkip} + {@link #finishPartition} replicate
     * that exact algorithm (verified against {@code SlicedMaterializedIterator.computeNextInSlice}
     * line-by-line) in STREAMING form: a sticky {@code pastSliceStart} latch plus a running {@code
     * openMarker} state (seeded via {@link #initOpenMarker}, called by {@code runMergeLegs} at the
     * same point it already seeds {@link RowLevelFilterProbe} — {@code CursorReadMerger} itself
     * neither knows nor needs to know this happens). ORDER note: production applies purging
     * ({@code withoutPurgeableTombstones}) AFTER slicing (it runs on {@code SlicedMaterializedIterator}'s
     * OUTPUT, synthetic markers included) — this class applies its purge twin per real marker
     * BEFORE computing slice admission/tracking from the RAW (pre-purge) marker shape, then
     * purge-tests synthetic markers too at the point they're written. Both orders are proven to
     * produce identical bytes: {@code shouldPurge} is a pure function of a deletion's own
     * (timestamp, localDeletionTime) pair with no cross-call state, and a boundary-to-bound
     * downgrade always preserves exactly the side that determines {@code isOpen()} — so tracking
     * state (open vs not) never diverges regardless of which order purging runs in.
     */
    // public: CursorReads.TranscodeMergeSink and CursorReads.mergeLegsWithSink are the M3.3a-i
    // test seam — TranscodeWireFormatDifferentialTest (org.apache.cassandra.db.cursorreads, a
    // different package) constructs this sink directly, mirroring the already-public
    // CursorReads.PendingLeg/MemtableMergeLeg precedent. Since M3.3b-i, ONE production call site
    // also reaches this class: SinglePartitionReadCommand.queryStorageToResponseBytes, via
    // CursorReads.buildTranscodeResponseBytes — still entirely behind cursor_reads_enabled
    // (default off) plus that method's own layer-2 gate.
    public static final class TranscodeMergeSink implements CursorReadMerger.MergeSink
    {
        private final ResponseWireWriter writer;
        private final DeletionPurger purger;
        private final long nowInSec;
        private final boolean purgeEnabled;
        private long eventsWritten;

        // ---- slice-boundary state (see class javadoc) ----
        private final ClusteringComparator comparator;
        /** null when the slice starts at BOTTOM (no pre-slice trimming/synthesis needed). */
        private final ClusteringBound<?> sliceStart;
        private final ClusteringBound<?> sliceEnd;
        /** Sticky: once true, every remaining event this partition is in-slice. Starts true when
         *  the slice starts at BOTTOM (nothing to trim). */
        private boolean pastSliceStart;
        /** Running "is a range tombstone currently open" state, exactly {@code
         *  SlicedMaterializedIterator.openMarker}: null = no open marker. Seeded by
         *  {@link #initOpenMarker}, updated from every RAW (pre-purge) marker this sink sees,
         *  admitted or not. */
        private DeletionTime openMarker;
        /** Whether the row currently being staged (between startRow/endRow) was admitted by the
         *  slice — addComplexDeletion/addCell/endRow no-op when false. */
        private boolean currentRowAdmitted;
        /** M3.3b-i: optional tombstone/live-row accounting twin, attached only by the production
         *  call site ({@link #attachScanGuard}) — null (the default, including every M3.3a-i test
         *  scenario) means no accounting happens, exactly pre-M3.3b-i behavior. */
        private TombstoneScanGuard scanGuard;

        /** M3.3b-i: attaches the tombstone/live-row accounting twin. Must be called before any
         *  merge event reaches this sink (mirrors {@link #initOpenMarker}'s own ordering
         *  contract) — never called by any M3.3a-i test scenario. */
        void attachScanGuard(TombstoneScanGuard scanGuard)
        {
            this.scanGuard = scanGuard;
        }

        /** Purge-disabled convenience constructor: {@code nowInSec == 0} is exactly
         *  {@code ReadCommand.nowInSec() == 0}'s own disablement condition in
         *  {@code withoutPurgeableTombstones} (a no-op then too). */
        public TranscodeMergeSink(ResponseWireWriter writer, ClusteringComparator comparator, Slice slice)
        {
            this(writer, comparator, slice, 0, Long.MIN_VALUE, false, Long.MIN_VALUE);
        }

        /**
         * @param nowInSec 0 disables purging entirely, matching {@code ReadCommand}'s own gate
         */
        public TranscodeMergeSink(ResponseWireWriter writer, ClusteringComparator comparator, Slice slice,
                                  long nowInSec, long gcBefore, boolean onlyPurgeRepairedTombstones,
                                  long oldestUnrepairedTombstone)
        {
            this.writer = writer;
            this.comparator = comparator;
            this.sliceStart = slice.start().isBottom() ? null : slice.start();
            this.sliceEnd = slice.end();
            this.pastSliceStart = (sliceStart == null);
            this.nowInSec = nowInSec;
            this.purgeEnabled = nowInSec != 0;
            // PurgeFunction's own purger field, verbatim, specialized to the read path's inputs:
            // ignoreGcGraceSeconds is compaction-only (shouldIgnoreGcGrace defaults false and
            // ReadCommand never overrides it) and the purge evaluator is
            // WithoutPurgeableTombstones' constant-true `time -> true` — both elided.
            this.purger = (timestamp, localDeletionTime) ->
                          purgeEnabled
                          && !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone)
                          && localDeletionTime < gcBefore;
        }

        /** Seeds the slice-start open-marker state with the merged row-index seek state — the exact
         *  same value {@code SlicedMaterializedIterator} receives as
         *  {@code MaterializedPartition.openMarkerAtStart} and {@link RowLevelFilterProbe} receives
         *  via its own {@code initOpenMarker}. Must be called (by {@code runMergeLegs}, mirroring
         *  the existing {@code RowLevelFilterProbe} precedent) after the {@code CursorReadMerger} is
         *  constructed but before {@code mergeUnfiltereds()} runs — this value does not exist
         *  earlier. A no-op call (never invoked) leaves {@code openMarker} at its null default,
         *  correct for the {@code rowLegs.isEmpty()} case where no merge — and so no seek state —
         *  exists at all. */
        public void initOpenMarker(DeletionTime openMarkerAtStart)
        {
            this.openMarker = openMarkerAtStart;
        }

        /** Must be called once, after the merge this sink was driving has fully completed (i.e.
         *  after {@code CursorReadMerger.mergeUnfiltereds()} returns) — the streaming analog of
         *  {@code SlicedMaterializedIterator.computeNextInSlice}'s "slice exhausted: artificially
         *  close an open range tombstone at the slice end" tail. Not part of the {@code MergeSink}
         *  contract (mirrors {@code RowLevelFilterProbe.finishPartition}'s same shape); the M3.3a-i
         *  test harness calls this explicitly, the same way it would be wired in M3.3b. */
        public void finishPartition()
        {
            if (!pastSliceStart)
            {
                // No admitted row/marker event ever arrived after the slice start (e.g. every row in
                // range shadowed to nothing, and the only real marker sat at-or-before the start) —
                // admitOrSkip's transition never fired. computeNextInSlice's pre-slice-skip loop
                // does NOT depend on finding a real next-in-slice element to check the synthetic
                // open: it runs this same check unconditionally once the loop exits, list exhaustion
                // included (verified against source, not assumed) — so this call must perform that
                // exact same transition here before considering the slice-end close.
                pastSliceStart = true;
                if (openMarker != null)
                    purgeAndWriteMarker(new RangeTombstoneBoundMarker(sliceStart, openMarker));
            }
            if (openMarker == null)
                return;
            DeletionTime toClose = openMarker;
            openMarker = null;
            purgeAndWriteMarker(new RangeTombstoneBoundMarker(sliceEnd, toClose));
        }

        /**
         * The slice-admission decision for a row or marker's clustering — {@code
         * SlicedMaterializedIterator.computeNextInSlice}'s pre-slice-skip loop, replayed one element
         * at a time instead of over a materialized list. Returns false (drop, caller must not
         * forward) for anything at-or-before the slice start (non-strict — matches the boundary
         * comparator's own inclusive/exclusive encoding, so no separate inclusive-bound handling is
         * needed here). On the transition into the slice, emits the synthetic OPEN marker first
         * (purge-tested like any other marker) if a deletion is running open at that point.
         */
        private boolean admitOrSkip(ClusteringPrefix<?> clustering)
        {
            if (pastSliceStart)
                return true;
            if (sliceStart != null && comparator.compare(clustering, sliceStart) <= 0)
                return false;
            pastSliceStart = true;
            if (openMarker != null)
                purgeAndWriteMarker(new RangeTombstoneBoundMarker(sliceStart, openMarker));
            return true;
        }

        /** The purge twin for ANY {@code RangeTombstoneMarker} reaching the wire — real markers
         *  admitted by {@link #admitOrSkip}, and the synthetic slice-boundary markers it/{@link
         *  #finishPartition} construct (production purge-tests those too: they are ordinary {@code
         *  RangeTombstoneMarker} objects by the time {@code withoutPurgeableTombstones} sees them). */
        private void purgeAndWriteMarker(RangeTombstoneMarker marker)
        {
            try
            {
                if (marker.isBoundary())
                {
                    RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                    boolean purgeClose = purger.shouldPurge(boundary.closeDeletionTime(false));
                    boolean purgeOpen = purger.shouldPurge(boundary.openDeletionTime(false));
                    if (purgeClose && purgeOpen)
                        // both sides gcable: the whole marker drops — PurgeFunction.applyToMarker's
                        // exact verdict
                        return;
                    if (purgeClose)
                        writer.writeMarker(boundary.createCorrespondingOpenMarker(false));
                    else if (purgeOpen)
                        writer.writeMarker(boundary.createCorrespondingCloseMarker(false));
                    else
                        writer.writeMarker(marker);
                }
                else
                {
                    if (purger.shouldPurge(((RangeTombstoneBoundMarker) marker).deletionTime()))
                        return;
                    writer.writeMarker(marker);
                }
                eventsWritten++;
                if (scanGuard != null)
                    scanGuard.marker(marker.clustering());
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        /** Diagnostic only (not part of the {@code MergeSink} contract): rows/markers that actually
         *  reached the wire, post-purge — mirrors {@code MaterializingMergeSink.materializedCount()}
         *  closely enough for test sanity checks (e.g. "the shadowed-row scenario wrote nothing"). */
        long eventsWritten()
        {
            return eventsWritten;
        }

        @Override
        public void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            currentRowAdmitted = admitOrSkip(clustering);
            if (!currentRowAdmitted)
                return;
            LivenessInfo liveness = purger.shouldPurge(mergedLiveness, nowInSec) ? LivenessInfo.EMPTY : mergedLiveness;
            DeletionTime deletion = purger.shouldPurge(mergedRowDeletion) ? DeletionTime.LIVE : mergedRowDeletion;
            if (scanGuard != null)
                scanGuard.startRow(clustering, liveness, deletion);
            if (TEST_TRANSCODE_SKEW_TIMESTAMP && !liveness.isEmpty())
                liveness = LivenessInfo.withExpirationTime(liveness.timestamp() + 1, liveness.ttl(), liveness.localExpirationTime());
            if (TEST_TRANSCODE_WRONG_FLAGS && !deletion.isLive())
                deletion = DeletionTime.LIVE;
            try
            {
                writer.startRow(clustering, liveness, deletion);
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion)
        {
            if (!currentRowAdmitted)
                return;
            DeletionTime deletion = purger.shouldPurge(mergedComplexDeletion) ? DeletionTime.LIVE : mergedComplexDeletion;
            if (deletion.isLive())
                // matches the MergeSink grammar: a live complex deletion is never announced, the
                // column is opened lazily by its first surviving addCell instead (ResponseWireWriter
                // mirrors this exactly — see its addCell)
                return;
            if (scanGuard != null)
                scanGuard.complexDeletion();
            try
            {
                writer.addComplexDeletion(column, deletion);
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            if (!currentRowAdmitted)
                return;
            Cell<?> purged = cell.purge(purger, nowInSec);
            if (purged == null)
                // fully purged: dropped from the wire entirely, not counted toward the row's
                // column-subset/complex-marker bookkeeping either
                return;
            if (scanGuard != null)
                scanGuard.cell(purged.timestamp(), purged.ttl(), purged.localDeletionTime());
            try
            {
                writer.addCell(purged);
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public boolean wantsWireStreamedCells()
        {
            return true;
        }

        /**
         * M3.3a-ii outcome of {@link #purgeCellLiveness}: mirrors the three branches of
         * {@code AbstractCell.purge(DeletionPurger, long)} (see that method's own javadoc for the
         * decision tree this hand-mirrors — verified against source, per this class's standing
         * purge-twin discipline, not assumed).
         */
        private enum CellPurgeOutcome { UNCHANGED, CONVERT_TO_TOMBSTONE, DROP }

        /** Set by {@link #purgeCellLiveness} on a {@code CONVERT_TO_TOMBSTONE} outcome only; read
         *  immediately after by its one caller ({@link #addCellFromWire}) — avoids a per-cell
         *  result-tuple allocation for what {@code AbstractCell.purge}'s object-returning version
         *  gets for free from its own return value. */
        private long convertedLocalDeletionTime;

        /**
         * Hand-mirrors {@code AbstractCell.purge(DeletionPurger, long)}'s exact decision tree,
         * operating on the winner's liveness metadata alone (timestamp/ttl/localDeletionTime) so
         * the purge/convert/drop decision can be made — and, critically, ACTED ON — before its
         * value bytes are ever touched. This is what lets streaming and purging compose: the
         * pre-M3.3a-ii path could afford to purge an already-materialized {@code Cell} object
         * (see {@link #addCell}) because materialization always happened first regardless; the
         * whole point of {@link #addCellFromWire} is that it must NOT, so this decision has to be
         * value-independent by construction. {@code AbstractCell.purge} read directly from source
         * to confirm: not live → shouldPurge(original timestamp, original localDeletionTime) →
         * DROP; else, if expiring, convert to a tombstone at the adjusted (write-time)
         * localDeletionTime and check shouldPurge AGAIN with that adjusted value → DROP or
         * CONVERT; else (already a plain, non-expiring tombstone) → UNCHANGED.
         */
        private CellPurgeOutcome purgeCellLiveness(long timestamp, int ttl, long localDeletionTime)
        {
            boolean isLive = localDeletionTime == Cell.NO_DELETION_TIME
                             || (ttl != Cell.NO_TTL && nowInSec < localDeletionTime);
            if (isLive)
                return CellPurgeOutcome.UNCHANGED;
            if (purger.shouldPurge(timestamp, localDeletionTime))
                return CellPurgeOutcome.DROP;
            if (ttl != Cell.NO_TTL)
            {
                long adjustedLocalDeletionTime = localDeletionTime - ttl;
                if (purger.shouldPurge(timestamp, adjustedLocalDeletionTime))
                    return CellPurgeOutcome.DROP;
                convertedLocalDeletionTime = adjustedLocalDeletionTime;
                return CellPurgeOutcome.CONVERT_TO_TOMBSTONE;
            }
            return CellPurgeOutcome.UNCHANGED;
        }

        /**
         * M3.3a-ii: the streaming twin of {@link #addCell} — same purge-twin contract (identical
         * outcome to {@code cell.purge(purger, nowInSec)}, verified via {@link #purgeCellLiveness}
         * against real source rather than re-derived by feel), but the purge/convert/drop decision
         * is made BEFORE the value is ever streamed, so a dropped or converted-to-tombstone cell
         * never reaches {@code source} at all — {@code CursorReadMerger.mergeCellGroup}'s
         * {@code discardCellValue()} safety net (called unconditionally right after this returns)
         * is what actually reclaims the leg's unconsumed bytes in that case.
         */
        @Override
        public void addCellFromWire(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime,
                                    CellPath path, CellValueSource source) throws IOException
        {
            if (!currentRowAdmitted)
                return;
            CellPurgeOutcome outcome = purgeCellLiveness(timestamp, ttl, localDeletionTime);
            if (outcome == CellPurgeOutcome.DROP)
                // fully purged: dropped from the wire entirely, not counted toward the row's
                // column-subset/complex-marker bookkeeping either — the leg's still-unconsumed
                // value bytes (if any) are reclaimed by mergeCellGroup's discardCellValue() net
                return;
            boolean forceEmpty = outcome == CellPurgeOutcome.CONVERT_TO_TOMBSTONE;
            if (forceEmpty)
            {
                // AbstractCell.purge's "hijack": an expired-but-not-yet-gcable cell is re-emitted
                // as a plain tombstone — no TTL, value dropped, localDeletionTime moved back to
                // the write-time value purgeCellLiveness computed
                ttl = LivenessInfo.NO_TTL;
                localDeletionTime = convertedLocalDeletionTime;
            }
            if (scanGuard != null)
                scanGuard.cell(timestamp, ttl, localDeletionTime);
            boolean hasValue = !forceEmpty && source.hasValue();
            try
            {
                writer.addCellFromWire(column, timestamp, ttl, localDeletionTime, path, hasValue, source);
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void endRow()
        {
            if (!currentRowAdmitted)
                return;
            if (scanGuard != null)
                scanGuard.endRow();
            try
            {
                if (writer.endRow())
                    eventsWritten++;
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void addRow(Row row)
        {
            if (!admitOrSkip(row.clustering()))
                return;
            // M2.3 whole-row escape hatch: already a live object — purge it with the REAL
            // Row.purge (enforceStrictLiveness is always false: MV tables are gated out of cursor
            // reads), then hand the result to the writer's own real-Row passthrough.
            Row purged = row.purge(purger, nowInSec, false);
            if (purged == null || purged.isEmpty())
                return;
            if (scanGuard != null)
                scanGuard.wholeRow(purged);
            try
            {
                writer.writeRow(purged);
                eventsWritten++;
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            boolean admitted = admitOrSkip(marker.clustering());
            // Running open-marker tracking updates from the RAW marker unconditionally — matching
            // SlicedMaterializedIterator.updateOpenMarker, which runs entirely before purging exists
            // in the pipeline (see class javadoc's order note) — independent of both admission and
            // purging.
            DeletionTime rawOpenAfter = marker.isOpen(false) ? CursorReads.copyOf(marker.openDeletionTime(false)) : null;
            if (!admitted)
            {
                openMarker = rawOpenAfter;
                return;
            }
            purgeAndWriteMarker(marker);
            openMarker = rawOpenAfter;
        }
    }

    /**
     * M3.3b-i (CASSANDRA-20428): production counterpart of the M3.3a-i test harness's
     * {@code TranscodeWireFormatDifferentialTest.transcodeCandidate} helper — drives the real
     * merge core via {@link #mergeLegsWithSink} with a {@link TranscodeMergeSink}, producing the
     * FULL {@code UnfilteredPartitionIterators.Serializer} envelope wire bytes for exactly the one
     * partition a {@code SinglePartitionReadCommand} ever returns ({@code
     * SingletonUnfilteredPartitionIterator} always returns exactly one partition, live or fully
     * empty — never zero, never more than one). The caller ({@code
     * SinglePartitionReadCommand.queryStorageToResponseBytes}) is responsible for having already
     * verified the layer-1/layer-2 gate and for computing the purge parameters identically to how
     * {@code ReadCommand.withoutPurgeableTombstones} would.
     *
     * @param legs       >= 2 legs (the merge core's own hard requirement — enforced by
     *                   {@link #mergeLegsWithSink} regardless).
     * @param extraStats additional {@code EncodingStats} contributions the merge itself never sees
     *                   — one entry per gated-in candidate sstable whose {@code openLeg} returned
     *                   null (the partition turned out absent from that sstable). The OBJECT path's
     *                   {@code queryMemtableAndDiskInternal} folds these in via a separate
     *                   {@code CursorReads.absentPartitionIterator} placeholder merged ABOVE the
     *                   cursor merge (see that method's own {@code leg == null} branch); since that
     *                   placeholder contributes no rows and only a LIVE partition deletion/empty
     *                   static row, its ONLY observable effect on the final response is its stats
     *                   contribution to the merged {@code EncodingStats} header — which is what this
     *                   parameter reproduces, without needing an actual placeholder iterator.
     * @param scanGuard  optional tombstone/live-row accounting twin (see {@link TombstoneScanGuard});
     *                   null disables it entirely (matches every M3.3a-i test scenario's behavior).
     */
    static ByteBuffer buildTranscodeResponseBytes(List<? extends MergeLeg> legs,
                                                   TableMetadata metadata,
                                                   DecoratedKey key,
                                                   Slices slices,
                                                   ColumnFilter columnFilter,
                                                   long nowInSec,
                                                   long gcBefore,
                                                   boolean onlyPurgeRepairedTombstones,
                                                   long oldestUnrepairedTombstone,
                                                   List<EncodingStats> extraStats,
                                                   TombstoneScanGuard scanGuard) throws IOException
    {
        RegularAndStaticColumns cols = columnFilter.fetchedColumns();
        List<EncodingStats> allStats = new ArrayList<>(legs.size() + (extraStats == null ? 0 : extraStats.size()));
        for (MergeLeg leg : legs)
            allStats.add(leg.legStats());
        if (extraStats != null)
            allStats.addAll(extraStats);
        EncodingStats stats = EncodingStats.merge(allStats, Function.identity());
        SerializationHeader header = new SerializationHeader(false, metadata, cols, stats);

        // DataOutputBuffer's own default is 128 bytes, and growth doubles and copies the whole
        // contents each time, so a response of any size climbs there one memcpy at a time. The
        // default response path has always avoided that by sizing from a moving average of past
        // response sizes; this path produces the same bytes, so it uses the same estimate.
        DataOutputBuffer rowEvents = new DataOutputBuffer(ReadResponse.responseBufferInitialSize());
        ResponseWireWriter writer = new ResponseWireWriter(rowEvents, header, MessagingService.current_version);
        Slice slice = slices.get(0);
        TranscodeMergeSink sink = new TranscodeMergeSink(writer, metadata.comparator, slice, nowInSec, gcBefore,
                                                          onlyPurgeRepairedTombstones, oldestUnrepairedTombstone);
        if (scanGuard != null)
            sink.attachScanGuard(scanGuard);
        MergeSinkFactory<TranscodeMergeSink> factory = () -> sink;
        MergeContext<TranscodeMergeSink> ctx = mergeLegsWithSink(legs, metadata, key, slices, columnFilter, null, null, factory);
        // the streaming analog of SlicedMaterializedIterator's "slice exhausted" tail — must run
        // once the merge has fully completed, closing any still-open range tombstone at the slice
        // end with a purge-tested synthetic marker
        ctx.sink.finishPartition();
        if (scanGuard != null)
            scanGuard.finish();

        boolean hasStatic = !ctx.mergedStatic.isEmpty();
        boolean isEmptyPartition = ctx.mergedDeletion.isLive() && !hasStatic && rowEvents.getLength() == 0;

        // The envelope holds the row events verbatim plus the partition header, so its size is
        // known here rather than estimated: the row-event length is exact, and the allowance on
        // top covers the key, flags, deletion, column subset and static row. Undersizing costs at
        // most one growth; the estimate alone would leave the whole response to climb from
        // DATA_RESPONSE_BUFFER_INITIAL_SIZE_MAX.
        try (DataOutputBuffer out = new DataOutputBuffer(rowEvents.getLength() + ReadResponse.responseBufferInitialSize()))
        {
            // UnfilteredPartitionIterators.Serializer's own envelope: the legacy isForThrift
            // placeholder, then "has next partition" (always true — exactly one partition), the
            // partition itself, then "has next partition" again (always false — SinglePartition
            // ReadCommand's queryStorage() always returns a SingletonUnfilteredPartitionIterator).
            out.writeBoolean(false);
            out.writeBoolean(true);
            ResponseWireWriter envelope = new ResponseWireWriter(out, header, MessagingService.current_version);
            envelope.writeKey(key.getKey());
            envelope.writeHeader(isEmptyPartition, false, ctx.mergedDeletion, hasStatic, columnFilter);
            if (!isEmptyPartition)
            {
                if (hasStatic)
                    envelope.writeStaticRow(ctx.mergedStatic);
                out.write(rowEvents.getData(), 0, rowEvents.getLength());
                envelope.writeEndOfPartition();
            }
            out.writeBoolean(false);
            ReadResponse.updateEstimatedResponseBytes(out.getLength());
            return out.buffer(false);
        }
    }

    /**
     * Receives {@link SSTableCursorReader#copyCellValue}'s wire-form output directly into the final
     * cell value array, replacing the previous scratch-buffer round trip (a {@code DataOutputBuffer}
     * copy of the whole wire value, then a re-parse and second copy into the value array — one more
     * memcpy per value byte than necessary, the same pattern class journal Gap B fixed).
     * copyCellValue makes exactly two kinds of calls on its writer: {@link #writeUnsignedVInt32}
     * with the already-decoded value length (variable-length types only — this mirrors the wire's
     * length vint, which the materialized value must NOT contain, so intercepting it here replaces
     * the old strip-the-vint re-parse), then {@link #write(byte[], int, int)} per transfer chunk.
     * <ul>
     *   <li>variable-length values ({@link #prepareVariable}): the value array is allocated when
     *       the length arrives and chunks are copied straight into it — file page → transfer
     *       buffer → value, two copies (down from three);</li>
     *   <li>fixed-length values ({@link #prepareFixed}): the length is known up front, so the
     *       final array itself is passed to copyCellValue as the transfer buffer — its single
     *       {@code readFully} lands the bytes in place, file page → value, ONE copy (matching the
     *       iterator path's {@code ByteArrayAccessor.read}), and {@link #write} only validates
     *       that the copy loop really made the single full-array pass that makes this sound.</li>
     * </ul>
     * Every other output method throws {@link UnsupportedOperationException}: if the cursor
     * reader's copy loop ever changes shape, this fails loudly instead of corrupting values.
     */
    private static final class CellValueCapture implements DataOutputPlus, ArrayBackedDataOutput
    {
        private byte[] target;
        private int written;
        private boolean fixed;

        /**
         * The value array is always known before a single value byte is read — for a fixed-length
         * type from {@link #prepareFixed}, for a variable-length one from the wire's length vint,
         * which {@code copyCellContents} mirrors to {@link #writeUnsignedVInt32} before it copies
         * anything. So both arms can take {@code copyCellContents}' array-backed fast path and
         * land the bytes in the value array in one copy; the chunked {@link #write} below is left
         * as the fallback.
         */
        @Override
        public boolean hasArray()
        {
            return target != null;
        }

        @Override
        public void readFully(DataInputPlus in, int length) throws IOException
        {
            // the same single-full-array-pass shape write() validates, for the same reason: a
            // partial or repeated pass would leave the value array holding the wrong bytes
            if (target == null || written != 0 || length != target.length)
                throw new IllegalStateException("unexpected value read: " + length + " bytes into "
                                                + (target == null ? "no target" : target.length + " already " + written));
            in.readFully(target, 0, length);
            written = length;
        }

        /** Variable-length mode: the array is allocated when the wire's length vint is mirrored. */
        CellValueCapture prepareVariable()
        {
            target = null;
            written = 0;
            fixed = false;
            return this;
        }

        /** Fixed-length mode; {@code value} must ALSO be passed to copyCellValue as its transfer buffer. */
        CellValueCapture prepareFixed(byte[] value)
        {
            target = value;
            written = 0;
            fixed = true;
            return this;
        }

        /** The captured value array, fully written. */
        byte[] finish()
        {
            byte[] value = target;
            if (value == null || written != value.length)
                throw new IllegalStateException("incomplete cell value capture: " + written + " of "
                                                + (value == null ? "unknown" : String.valueOf(value.length)) + " bytes");
            target = null;
            return value;
        }

        @Override
        public void writeUnsignedVInt32(int length)
        {
            if (fixed || target != null)
                throw new IllegalStateException("unexpected value length vint");
            target = length == 0 ? ByteArrayAccessor.instance.empty() : new byte[length];
        }

        @Override
        public void write(byte[] buffer, int offset, int length)
        {
            if (fixed)
            {
                // the transfer buffer IS the target and readFully already placed the bytes; a
                // single full-array chunk at offset 0 is the only shape that cannot have
                // clobbered them (a second chunk's readFully would overwrite the first)
                if (buffer != target || offset != 0 || written != 0 || length != target.length)
                    throw new IllegalStateException("unexpected fixed-length value chunking");
                written = length;
                return;
            }
            System.arraycopy(buffer, offset, target, written, length);
            written += length;
        }

        @Override
        public void write(int b)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(byte[] buffer)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(ByteBuffer buffer)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeByte(int v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeLong(long v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeUTF(String s)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Applies the query's slices to the materialized partition with EXACTLY the semantics of
     * {@code AbstractSSTableIterator.ForwardReader} (pre-slice skip with open-marker tracking,
     * artificial {@link RangeTombstoneBoundMarker}s at slice start/end when a range tombstone
     * covers the bound, non-strict start / strict end comparisons), so the emitted unfiltered
     * stream is identical to the iterator path's — including for {@code ReadResponse} byte
     * comparison in the differential harness.
     */
    private static final class SlicedMaterializedIterator implements UnfilteredRowIterator
    {
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final SSTableReader sstable;
        private final EncodingStats stats;
        private final ColumnFilter columnFilter;
        private final Slices slices;
        private final MaterializedPartition partition;
        private final ClusteringComparator comparator;
        private final boolean validateOnEmission;

        private int sliceIdx;            // next slice to open
        private int listIdx;             // position in partition.unfiltereds
        private ClusteringBound<?> start; // null once we're past the current slice's start (or the slice starts at BOTTOM)
        private ClusteringBound<?> end;
        private boolean sliceOpen;
        private DeletionTime openMarker;
        private Unfiltered next;

        /**
         * @param sstable for a single leg, the leg's sstable; for an M2.1 merged partition, the
         *                first leg's — the attribution of emission-time corrupted-tombstone
         *                validation, which only single-leg mode performs
         * @param stats   the stats this iterator must report: the leg sstable's own for a single
         *                leg, {@code EncodingStats.merge} over the legs for a merged partition —
         *                so the outer object merge's stats total stays identical either way
         * @param validateOnEmission true for a single leg, where THIS iterator is the leg's
         *                emission surface and so owns Gap C's per-leg in-slice validation; false
         *                for an M2.1 merged partition, whose legs were each already validated
         *                inside the merge (same in-slice surface, attributed to the real leg) —
         *                the iterator path never re-validates its merged output above the per-leg
         *                iterators, so validating here a second time would both duplicate the
         *                work and reject reads the iterator path serves (a deletion opened
         *                OUTSIDE the slice surviving into an in-slice merged marker is validated
         *                by neither path's per-leg pass)
         */
        SlicedMaterializedIterator(TableMetadata metadata,
                                   DecoratedKey key,
                                   SSTableReader sstable,
                                   EncodingStats stats,
                                   ColumnFilter columnFilter,
                                   Slices slices,
                                   MaterializedPartition partition,
                                   boolean validateOnEmission)
        {
            this.metadata = metadata;
            this.key = key;
            this.sstable = sstable;
            this.stats = stats;
            this.columnFilter = columnFilter;
            this.slices = slices;
            this.partition = partition;
            this.comparator = metadata.comparator;
            this.validateOnEmission = validateOnEmission;
            // Seeded from the BTI row index when the materialization seeked mid-partition: the
            // range tombstone open at the first materialized element cannot be discovered from
            // the (truncated) stream itself. Exactly ForwardIndexedReader.setForSlice's
            // `openMarker = indexInfo.openDeletion`; null for unseeked materializations.
            this.openMarker = partition.openMarkerAtStart;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return false;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return columnFilter.fetchedColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return key;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return partition.partitionDeletion;
        }

        @Override
        public Row staticRow()
        {
            return partition.staticRow;
        }

        @Override
        public EncodingStats stats()
        {
            return stats;
        }

        @Override
        public boolean hasNext()
        {
            while (next == null)
            {
                if (!sliceOpen)
                {
                    if (sliceIdx >= slices.size())
                        return false;
                    setForSlice(slices.get(sliceIdx++));
                }
                // returns null only after setting sliceOpen = false, so a null just loops back
                // to open the next slice (or hit the sliceIdx exhaustion check above)
                next = computeNextInSlice();
            }
            return true;
        }

        @Override
        public Unfiltered next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            Unfiltered toReturn = next;
            next = null;
            return toReturn;
        }

        private void setForSlice(Slice slice)
        {
            start = slice.start().isBottom() ? null : slice.start();
            end = slice.end();
            sliceOpen = true;
        }

        // mirrors ForwardReader.hasNextInternal + handlePreSliceData + computeNext; only called
        // with sliceOpen == true, and clears sliceOpen when the slice is exhausted
        private Unfiltered computeNextInSlice()
        {
            if (start != null)
            {
                // Skip pre-slice data with a NON-strict comparison (see handlePreSliceData's
                // comment on RT start markers equal to the slice start), tracking the open marker.
                while (listIdx < partition.unfiltereds.size()
                       && comparator.compare(partition.unfiltereds.get(listIdx).clustering(), start) <= 0)
                {
                    Unfiltered skipped = partition.unfiltereds.get(listIdx++);
                    if (skipped.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                        updateOpenMarker((RangeTombstoneMarker) skipped);
                }
                ClusteringBound<?> sliceStart = start;
                start = null;
                if (openMarker != null)
                    return new RangeTombstoneBoundMarker(sliceStart, openMarker);
            }

            // in-slice: strict end comparison
            if (listIdx < partition.unfiltereds.size()
                && comparator.compare(partition.unfiltereds.get(listIdx).clustering(), end) < 0)
            {
                Unfiltered unfiltered = partition.unfiltereds.get(listIdx++);
                // Single-leg mode: corrupted_tombstone_strategy check, applied EXACTLY where the
                // iterator path applies it (ForwardReader.computeNext, AbstractSSTableIterator:534):
                // each IN-SLICE unfiltered as it is emitted. Deliberately NOT validated, matching the
                // iterator path's behavior: pre-slice skipped data (handlePreSliceData deserializes
                // markers without validating), the artificial slice-bound markers synthesized from the
                // open-marker state, the static row, and anything at/past the slice end. Emission-time
                // (not materialization-time) placement keeps a corrupted row OUTSIDE the queried slice
                // from failing a cursor read that the iterator path would have served.
                // Merged mode skips this entirely: each leg was already validated INSIDE the merge on
                // the same in-slice surface (Gap C placement, attributed to its real leg), and the
                // iterator path never validates its merged output above the per-leg iterators — see
                // the validateOnEmission constructor javadoc.
                if (validateOnEmission)
                    UnfilteredValidation.maybeValidateUnfiltered(unfiltered, metadata, key, sstable);
                if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                    updateOpenMarker((RangeTombstoneMarker) unfiltered);
                return unfiltered;
            }

            // slice exhausted: artificially close an open range tombstone at the slice end
            sliceOpen = false;
            if (openMarker != null)
                return new RangeTombstoneBoundMarker(end, openMarker);
            return null;
        }

        private void updateOpenMarker(RangeTombstoneMarker marker)
        {
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        @Override
        public void close()
        {
            // fully materialized; the cursor was closed at creation
        }
    }
}
