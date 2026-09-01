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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.PreSortedBubbleInsert;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellLivenessInfo.Resolution;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

/**
 * M2.1 (CASSANDRA-20428, Phase 3): the WITHIN-PARTITION cursor-level k-way merge across the
 * sstable legs of a single-partition read — the copy-adapt of {@code CursorCompactor}'s merge
 * loop resolved by the M2 design's FINDING #10. Reads merge exactly ONE partition across cursors
 * the call site has already positioned at the query key, so none of compaction's partition-level
 * k-way machinery (prepareAndSortForPartitionMerge, key-order checks, writer rollover) appears
 * here; what transfers is the unfiltered-level sort/merge-limit walk, row liveness/deletion
 * reconciliation, the range-tombstone open-marker set, and the cell-level merge with value-compare
 * tie-breaks. The invariant-bearing LEAF primitives are shared, not copied:
 * {@link ClusteringComparator#compare(org.apache.cassandra.io.sstable.ClusteringDescriptor,
 * org.apache.cassandra.io.sstable.ClusteringDescriptor)},
 * {@link CellLivenessInfo#resolve}, {@link CursorCompactor#comparePaths} and
 * {@link PreSortedBubbleInsert}. The copied SHAPE of the row/cell/RT
 * loops — above all the open-marker set maintenance — is the accepted debt the design records:
 * the differential harness is the tripwire on this side, compaction's suites on its own.
 *
 * <b>Compaction behaviors deliberately ABSENT</b> (the design's Q3 list; each notes where the
 * compaction branch sits so its absence here is checkable):
 * <ul>
 *   <li><b>No purge, at any level</b>: no partition-deletion purge (CursorCompactor's
 *       maybePurgedOutputDeletion), no row-deletion/liveness purge (mergeRows'
 *       purger.shouldPurge arms), no complex-deletion purge, no cell purge (mergeCells'
 *       {@code purger.shouldPurge} disjunct), no RT purge (mergeRangeTombstones'
 *       shouldPurgeClose/Open splits). Reads drop gcable tombstones only ABOVE the merge, in
 *       {@code ReadCommand.withoutPurgeableTombstones}, which keeps doing that job unchanged.</li>
 *   <li><b>No expired-TTL-to-tombstone conversion</b> (mergeCells' {@code ttlToTombstone} +
 *       value-drop block): the iterator read path serves expired cells exactly as stored, so the
 *       merge must too.</li>
 *   <li><b>No strict-liveness row skipping</b> (mergeRows' {@code enforceStrictLiveness} arm):
 *       materialized-view tables are gated out of cursor reads entirely.</li>
 *   <li><b>No eager corrupted-tombstone validation</b> ({@code StatefulCursor}'s per-element
 *       checks): validation keeps Gap C's read-path placement — per leg, IN-SLICE elements only,
 *       via {@code CursorReads.MergeLeg}'s validate hooks — so a corrupted element outside the
 *       queried slice does not fail a read the iterator path would have served.</li>
 *   <li><b>No counter-cell merging</b> (mergeCounterCells): counter tables are gated out.</li>
 * </ul>
 *
 * <b>Genuinely new relative to compaction</b>: per-leg {@code ColumnFilter}/
 * {@code DeserializationHelper} filtering happens BELOW reconciliation (compaction reads every
 * column; the iterator read path filters at deserialization, below its merge) — implemented in
 * {@code PendingLeg.ensureParkedAtCell}, including surfacing a complex column's surviving
 * deletion as a deletion-only merge position even when every one of its cells was filtered out
 * per leg.
 *
 * Since M2.3 the legs are {@code CursorReads.MergeLeg}s, not concrete sstable legs: byte-backed
 * sstable legs ({@code CursorReads.PendingLeg}) and the object-backed memtable adapter
 * ({@code MemtableMergeLeg}) join the SAME within-partition merge, so a memtable+sstable read no
 * longer needs a second, object-level merge above this one. Memtable-won data is emitted through
 * the {@code existingCell()}/{@code consumeExistingRow()} escape hatches as the already-live
 * objects (see {@code MergeSink.addRow} and the M2 design's Q2 answer) — without that, memtable-hot
 * reads would allocate MORE than the object path they replace. A memtable leg never seeks and is
 * never validated (matching the iterator path, which applies {@code UnfilteredValidation} only to
 * sstable-attributed data); it participates in the open-marker set as a normal, always-current
 * source. Since M2.2
 * the eager whole-partition walk is gone for indexed BTI legs of a single-slice read: each such
 * leg enters the merge already seeked to its row-index floor block for the slice start
 * ({@code CursorReads.MergeLeg.seekForMerge}), and its open-range-tombstone deletion at the
 * seek point ({@code IndexInfo.openDeletion} — the exact payload
 * {@code ForwardIndexedReader.setForSlice} seeds per leg on the iterator path) is seeded into
 * {@link #openMarkers} by this constructor, so cross-leg reconciliation of range tombstones
 * spanning the seek points stays exact. Per-leg seek points may be MISALIGNED (different floor
 * blocks per leg, or unseeked BIG legs mixed in): that is safe because every leg's post-seek
 * stream contains all of that leg's elements strictly after the slice start (its floor block's
 * separator is {@code <=} the slice start), so any merged element the slicer can EMIT has every
 * leg's contribution; under-reconciled elements exist only before the slice start, where the
 * slicer discards them — and a leg's seeded deletion is genuinely open at every clustering
 * position from its floor block's separator to its in-stream close, so seeded shadowing is never
 * wrong for emitted elements either.
 */
final class CursorReadMerger
{
    /**
     * The read-side merge sink: CLUSTERING-FIRST events (the merge knows the group's clustering
     * the moment the row group forms), unlike compaction's writer-wire-shaped
     * {@code CursorMergeSink} whose clustering arrives only at {@code writeRowEnd} and forces
     * non-writer consumers into staging gymnastics (see DigestingCursorMergeSink's javadoc).
     * Shaped so Phase 1's proven materializer — one reused {@code BTreeRow.sortedBuilder},
     * {@code CellValueCapture} one-copy value extraction — plugs in with {@code newRow()} up
     * front and zero staging ({@code CursorReads.MaterializingMergeSink}).
     *
     * Event grammar per partition: ({@code startRow} ({@code addComplexDeletion} |
     * {@code addCell})* {@code endRow} | {@code addRangeTombstoneMarker})*, in clustering order;
     * complex deletions and cells arrive in column/path order within their row. A row group that
     * merges to NOTHING (empty shell, every cell shadowed or filtered) emits no events at all —
     * the merge-side analog of Row.Merger returning null. All arguments are immutable (already
     * copied off any reusable descriptor state).
     */
    interface MergeSink
    {
        void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion);

        void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion);

        /** Since M2.3 the cell may be a REUSED already-live object from a memtable leg (a
         *  {@code Cell<?>} of any accessor), not only a freshly-built {@code Cell<byte[]>} —
         *  exactly like the object merge, whose merged rows reference winning memtable cell
         *  objects directly ({@code Cells.reconcile} returns the winner, never a rebuild). */
        void addCell(Cell<?> cell);

        /** Ends the current row (only started rows are ended; a group merging to nothing emits
         *  neither startRow nor endRow — sinks keep an empty-row drop only as a defensive net). */
        void endRow();

        /** M2.3 whole-row escape hatch: emits an already-live row object AS the merged row —
         *  the sink-event mirror of {@code Row.Merger.merge}'s single-version fast path
         *  ({@code rowsToMerge == 1 && activeDeletion.isLive()} returns the row unchanged).
         *  Never combined with startRow/endRow for the same row group. */
        void addRow(Row row);

        void addRangeTombstoneMarker(RangeTombstoneMarker marker);

        /**
         * M3.1 production bound: consulted by {@link #mergeUnfiltereds} before each merge group —
         * the sink-side twin of the slice end-stop's "check per group, stop the loop early"
         * precedent. {@code false} means the sink is confident nothing it could still receive can
         * reach the query result (e.g. the query's {@code DataLimits} counter would already have
         * stopped consuming), so the merge stops PRODUCING: remaining groups are never sorted,
         * merged, validated or materialized. Default TRUE — unbounded production, the pre-M3.1
         * behavior of every sink that does not opt in.
         */
        default boolean wantsMore()
        {
            return true;
        }

        /**
         * M3.3a-ii capability flag: true iff this sink wants a resolved SSTABLE-winner cell's
         * value streamed directly from its source ({@link #addCellFromWire}) instead of receiving
         * a fully materialized {@link Cell} object via {@link #addCell}. Default false — every
         * M3.1/M3.2 sink ({@code MaterializingMergeSink}, {@code LimitingMergeSink},
         * {@code RowLevelFilterProbe}) never overrides this, so {@code mergeCellGroup}'s
         * sstable-winner materialization path for them is completely unchanged: same code, same
         * bytes, same allocations as before this capability existed.
         * <p>
         * Chosen as an explicit capability flag rather than an {@code instanceof} check on a
         * specific sink type: an {@code instanceof} would force this read-owned merge core to
         * import and know about a concrete sink implementation, breaking the abstraction
         * {@code MergeSink} exists to provide (the same reasoning that already governs
         * {@link #wantsMore()}). It is also strictly cheaper for the three existing sinks — with
         * the flag, {@code mergeCellGroup}'s call site for them is the exact pre-M3.3a-ii code,
         * untouched, so no {@link CellValueSource} view is ever constructed on their behalf; an
         * unconditional {@code addCellFromWire} call (relying purely on the default method to
         * "do the old thing") would still be correct, but would add a virtual dispatch and a
         * view-object touch to a hot path this ticket has spent several increments keeping
         * allocation-free, for sinks that can never use it.
         */
        default boolean wantsWireStreamedCells()
        {
            return false;
        }

        /**
         * M3.3a-ii: called by {@code CursorReadMerger.mergeCellGroup} for a resolved SSTABLE
         * winner INSTEAD OF building a {@link Cell} object + {@link #addCell(Cell)}, but only
         * when {@link #wantsWireStreamedCells()} is true (never for a sink that leaves it at the
         * default false). {@code source} exposes wherever the winner's raw wire-form value bytes
         * currently live — still unconsumed on the winning leg's cursor, or already staged in the
         * merge's tie-break scratch from an earlier COMPARE resolution — streamable directly into
         * an implementation's own destination without ever forming an intermediate
         * {@code byte[]}/{@code Cell} object.
         * <p>
         * The DEFAULT implementation exists for interface completeness (a sink that overrides
         * {@link #wantsWireStreamedCells()} to true without also overriding this method still
         * behaves correctly, just without the allocation win): it materializes a real
         * {@link Cell} exactly as the pre-M3.3a-ii code path did and forwards to {@link #addCell}.
         * In practice only {@code CursorReads.TranscodeMergeSink} ever overrides
         * {@code wantsWireStreamedCells()}, and it overrides this method too, so this default body
         * is never actually invoked — kept correct anyway, on the same "the interface must make
         * sense standalone" principle every other default method here follows.
         */
        default void addCellFromWire(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime,
                                     CellPath path, CellValueSource source) throws IOException
        {
            byte[] value = source.hasValue() ? source.materialize() : ByteArrayAccessor.instance.empty();
            addCell(ByteArrayAccessor.instance.factory().cell(column, timestamp, ttl, localDeletionTime, value, path));
        }
    }

    /**
     * M3.2b/M3.2c filter-pushdown probe: the merge core's hook POINTS for row-level
     * (clustering-column and regular-column) filter pushdown and its scan-metrics accounting —
     * deliberately semantics-free on the merge side, exactly like {@link MergeSink}: the core
     * never sees {@code nowInSec}, purge predicates or operator evaluation (FINDING #12's "no
     * purge machinery in the merger" property). The probe implementation
     * ({@code CursorReads.RowLevelFilterProbe}) owns all of that.
     *
     * Event grammar per DROPPED row group (a group {@link #rowGroupMatches} rejected):
     * {@code beginDroppedRow (droppedComplexDeletion | droppedCell)* endDroppedRow} — mirroring
     * the sink grammar of the group the merge would otherwise have emitted, but at METADATA level
     * only: no clustering, cell value or path is ever materialized for a dropped group (values are
     * skipped at the byte level), which is the entire allocation payoff of the pushdown. All
     * {@code LivenessInfo}/{@code DeletionTime} arguments are REUSABLE descriptor state, valid
     * only for the duration of the call — implementations must extract primitives, never retain.
     *
     * M3.2c adds the regular-column verdict, which — unlike the clustering verdict — resolves at
     * WINNER resolution inside the cell walk ({@code mergeCellGroup}), potentially after some of
     * the row's earlier cells were already merged and emitted. A row failing there (or failing at
     * row end because a filter column never surfaced a surviving winner) is ABANDONED mid-flight:
     * the probe discards the partially-built output row, converts the already-emitted content into
     * dropped-row accounting (it observed every sink event for the row), the merge walks the
     * remaining cell groups through the metadata-only accounting path, and the core closes the
     * cycle with {@link #endDroppedRow}. Cells materialized before a late-resolving failure are
     * bounded wasted allocation (measured by the allocation gate), never a correctness or
     * accounting gap.
     */
    interface FilterProbe
    {
        /**
         * The clustering-column filter verdict for a just-formed row group, evaluated from the
         * descriptor's clustering wire bytes BEFORE any cell work, clustering materialization or
         * escape-hatch emission. {@code false} means the group provably fails the query's
         * top-level filter and must not be produced; the merge then drives the accounting walk
         * ({@code accountDroppedRowGroup}) instead of the emitting merge.
         */
        boolean rowGroupMatches(UnfilteredDescriptor descriptor);

        /**
         * Opens a dropped row group's accounting.
         *
         * @param clusteringSource the leg whose descriptor holds the group's clustering (the
         *                         group's first-sorted leg — the same leg whose bytes the emitted
         *                         clustering would have come from); the probe may materialize the
         *                         clustering from it lazily on its exceptional (abort) path only
         * @param onEmittedSurface whether the group is on the emitted surface (strictly after the
         *                         slice start — rows at-or-before it are discarded by the slicer
         *                         and reach the metrics stage on NEITHER path, so they must
         *                         contribute nothing)
         * @param mergedLiveness   the merged row liveness AFTER active-deletion shadowing, or null
         *                         when it merged to empty (the {@code livenessOut} the emitting
         *                         path would have passed to {@code startRow}); reusable
         * @param mergedRowDeletion the merged row deletion the emitting path would have emitted
         *                         ({@code DeletionTime.LIVE} when the active deletion took over);
         *                         reusable
         */
        void beginDroppedRow(CursorReads.MergeLeg clusteringSource, boolean onEmittedSurface,
                             LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion);

        /** A merged complex deletion that survived active-deletion suppression in the dropped
         *  group (what {@code addComplexDeletion} would have emitted). Reusable argument. */
        void droppedComplexDeletion(DeletionTime mergedComplexDeletion);

        /** A merged cell position whose winner survived shadowing in the dropped group (what
         *  {@code addCell} would have emitted), identified by the winner's liveness METADATA only
         *  — on a full-metadata tie every contender shares the same liveness classification, so
         *  winner identity (and therefore value comparison) is irrelevant to accounting. */
        void droppedCell(CellLivenessInfo winnerLiveness);

        /** Closes the dropped group's accounting (classification happens here, where the full
         *  row-level picture — dead cells, live cells, deletions — is known). Also closes an
         *  M3.2c ABANDONED row's accounting (see {@link #abandonRowGroup}). */
        void endDroppedRow();

        // ---- M3.2c regular-column (value-window) verdicts ----

        /** Whether {@code column} carries a gate-approved REGULAR-column filter expression whose
         *  merged winner must be evaluated at resolution time ({@code mergeCellGroup}). Constant
         *  false when the query has no regular-column expressions — every M3.2b behavior. */
        boolean filtersRegularColumn(ColumnMetadata column);

        /** CELL liveness of a resolved winner at the query's {@code nowInSec} ({@code Cell.isLive}
         *  semantics — the probe owns the formula so the core stays nowInSec-free). A dead winner
         *  fails its expression without its value ever being staged ({@code RowFilter.Expression
         *  .getValue} returns null for a cell not live at nowInSec). Reusable argument. */
        boolean cellIsLive(CellLivenessInfo winnerLiveness);

        /**
         * The regular-column filter verdict for a LIVE resolved winner of {@code column}:
         * every gate-approved expression on the column evaluates as
         * {@code operator.isSatisfiedBy(column.type, valueWindow, value)} — exactly what
         * {@code SimpleExpression.isSatisfiedBy} reduces to for a live simple cell. A passing
         * verdict marks the column satisfied for the current row group's AND-semantics check
         * ({@link #rowRegularFiltersSatisfied}).
         *
         * ALIASING AUDIT (FINDING #15 discipline, recorded per call site): {@code valueWindow} is
         * a VIEW over reusable storage — the merge's tie-break value scratch for sstable winners
         * ({@code CellValueScratch.valueWindow()}), or the memtable winner's own stable cell
         * buffer — handed only to {@code Operator.isSatisfiedBy(AbstractType, ByteBuffer,
         * ByteBuffer)} implementations, which compare/deserialize within the call and retain
         * nothing (the same contract the iterator path relies on when passing
         * {@code cell.buffer()} views). Implementations must not retain the window past the call.
         */
        boolean regularCellMatches(ColumnMetadata column, ByteBuffer valueWindow);

        /** AND-semantics closure for the current row group: true iff every regular-column filter
         *  expression's column surfaced a surviving, live, value-passing winner in this row. A
         *  filter column with NO surviving merged cell fails the top filter too
         *  ({@code getValue} returns null for an absent cell), so a row ending unsatisfied must be
         *  abandoned even though every resolved winner passed. Constant true when the query has no
         *  regular-column expressions. */
        boolean rowRegularFiltersSatisfied();

        /**
         * Abandons the row group being EMITTED because a regular-column expression failed — at
         * winner resolution (a shadowed, dead or value-failing winner) or at row end (a filter
         * column that never surfaced a winner). The probe discards the partially-built output row
         * (it saw every sink event for it), opens the dropped-row accounting for this group and
         * REPLAYS the already-emitted content into it, so the accounting classification is
         * identical to a group rejected before any cell work. The merge then routes the group's
         * remaining cell groups through the metadata-only accounting walk and closes with
         * {@link #endDroppedRow}.
         *
         * @param clusteringSource      the leg whose descriptor holds the group's clustering
         *                              (still loaded — the abort path may materialize from it)
         * @param onEmittedSurface      same semantics as {@link #beginDroppedRow}'s flag
         * @param failedWinnerLiveness  the failing winner's liveness when that winner WOULD have
         *                              been part of the merged row (a dead or value-failing
         *                              winner — it reaches the metrics stage on the iterator
         *                              path), or null when it would not (a shadowed winner, or a
         *                              row-end absence failure); reusable
         */
        void abandonRowGroup(CursorReads.MergeLeg clusteringSource, boolean onEmittedSurface,
                             CellLivenessInfo failedWinnerLiveness);

        /** The M2.3 whole-row escape hatch's regular-column verdict: the single-contributor
         *  merged row IS this already-live object, so the REAL {@code Expression.isSatisfiedBy}
         *  evaluates it directly (same method the top filter would call). Constant true when the
         *  query has no regular-column expressions. Never called for a group whose clustering
         *  verdict already failed ({@link #rowGroupMatches} runs first). */
        boolean existingRowMatches(Row row);

        /** Dropped-row accounting for an escape-hatch row {@link #existingRowMatches} rejected:
         *  the full begin/replay/end cycle over the live row object's own content (nothing was
         *  emitted for it, so there is no partial output to discard). */
        void abandonExistingRow(Row row, boolean onEmittedSurface);
    }

    private final CursorReads.MergeLeg[] legs;
    private final boolean[] equalsNext;
    private final ClusteringComparator comparator;
    private final MergeSink sink;
    /** M3.2b clustering filter pushdown, or null when disengaged (every pre-M3.2b behavior). */
    private final FilterProbe filterProbe;

    /** supersedes-max of every leg's partition-level deletion (including Slices.NONE legs). */
    private final DeletionTime mergedPartitionDeletion;
    /** The deletion currently shadowing merged rows: the partition deletion, or the open range
     *  deletion when one supersedes it — compaction's activeDeletion, verbatim. */
    private DeletionTime activeDeletion;

    // Range-tombstone open-marker set — the copy-adapted shape of CursorCompactor:1465-1695
    // (RangeTombstoneMarker.Merger semantics: the merged stream emits a marker exactly where the
    // supersedes-max over the per-source open deletions changes). Since M2.2 the set starts
    // seeded with each seeked leg's open deletion at its seek point (see the constructor).
    private DeletionTime activeOpenRangeDeletion = DeletionTime.LIVE;
    private final List<DeletionTime.ReusableDeletionTime> openMarkers = new ArrayList<>();
    private final ArrayDeque<DeletionTime.ReusableDeletionTime> reusableMarkersPool = new ArrayDeque<>();

    /** M2.2: the merged open-range-tombstone deletion at the merge's START position — the
     *  supersedes-max over the legs' row-index seek seeds after the partition-deletion filter
     *  (what N per-leg iterators' artificial slice-start open markers would merge to), or null
     *  when no seeked leg carries one. The merged read's twin of the single-leg
     *  {@code MaterializedPartition.openMarkerAtStart}; captured at construction, immutable. */
    private final DeletionTime openMarkerAtMergeStart;

    // current output complex column state (reset per row), mirroring mergeCells' column-entry fold
    private ColumnMetadata currentComplexColumn;
    private final DeletionTime.ReusableDeletionTime mergedComplexDeletion = DeletionTime.ReusableDeletionTime.live();

    // Lazy row-start state (reset per row group): a group whose merged shell (liveness + row
    // deletion) is empty starts its output row only when the first surviving cell/complex
    // deletion arrives, so a group that merges to NOTHING — fully shadowed by other sources'
    // deletions, or every cell filtered away per leg — materializes no clustering and never
    // touches the sink's row builder. See mergeRowGroup/ensureRowStarted.
    private CursorReads.MergeLeg rowClusteringLeg;
    private Clustering<?> rowClustering;
    private boolean rowStarted;

    // M3.2c regular-column pushdown state (reset per row group; meaningful only with a probe):
    // rowOnEmittedSurface is the group's slicer-emission verdict (strictly after the slice
    // start), computed ONCE per probed row group from the descriptor-level comparison in
    // mergeUnfiltereds — needed at abandonment time, when the failure may surface mid-cell-walk;
    // rowAbandonedByFilter flips when a regular-column expression fails, switching the rest of
    // the row's cell walk from the emitting merge to the metadata-only accounting walk.
    private boolean rowOnEmittedSurface;
    private boolean rowAbandonedByFilter;

    // Gap C validation placement: per leg, in-slice only. The bounds are the query's single
    // slice (the gate admits at most one); null start/end = BOTTOM/TOP.
    private final boolean validationEnabled;
    private final ClusteringBound<?> sliceStart;
    private final ClusteringBound<?> sliceEnd;

    // hoisted so sorting never allocates a lambda per call (compaction's cellComparator pattern);
    // one PreSortedBubbleInsert per comparison kind, the insert sort shared with compaction
    private static final PreSortedBubbleInsert<CursorReads.MergeLeg> CLUSTERING_SORT =
        new PreSortedBubbleInsert<>(CursorReadMerger::compareByClustering);
    private final PreSortedBubbleInsert<CursorReads.MergeLeg> cellSort =
        new PreSortedBubbleInsert<>(this::compareByCell);

    // Tie-break value staging (compaction's tempCellBuffer1/tempCellBuffer2 discipline): scratch1
    // holds the running winner's staged bytes, scratch2 receives each challenger's; on a
    // challenger win the two SWAP, so a loser's bytes are never promoted to a real array — they
    // are simply overwritten by the next tie. Non-final because of that swap.
    private CellValueScratch tieValueScratch1 = new CellValueScratch();
    private CellValueScratch tieValueScratch2 = new CellValueScratch();

    // M3.3a-ii: the single reused CellValueSource view handed to a streaming sink
    // (MergeSink#addCellFromWire) — never allocated per cell, only ever constructed (and only
    // ever touched) when sink.wantsWireStreamedCells() is true, so the three non-streaming sinks
    // never pay for it. TEST_CORRUPT_STREAMED_CELL_VALUE routes the fresh-leg case through this
    // dedicated scratch instead of straight to the destination — see WinnerCellValueSource.
    private final WinnerCellValueSource winnerCellValueSource = new WinnerCellValueSource();
    private final CellValueScratch testCorruptionScratch = new CellValueScratch();

    CursorReadMerger(CursorReads.MergeLeg[] legs,
                     TableMetadata metadata,
                     DeletionTime mergedPartitionDeletion,
                     Slice slice,
                     MergeSink sink,
                     FilterProbe filterProbe)
    {
        this.legs = legs;
        this.equalsNext = new boolean[legs.length];
        this.comparator = metadata.comparator;
        this.mergedPartitionDeletion = mergedPartitionDeletion;
        this.activeDeletion = mergedPartitionDeletion;
        this.sink = sink;
        this.filterProbe = filterProbe;
        this.sliceStart = slice.start().isBottom() ? null : slice.start();
        this.sliceEnd = slice.end().isTop() ? null : slice.end();
        this.validationEnabled = DatabaseDescriptor.getCorruptedTombstoneStrategy() != Config.CorruptedTombstoneStrategy.disabled;

        // M2.2: seed the open-marker set with each seeked leg's open deletion at its seek point.
        // Each seed joins the set exactly as that leg's in-stream OPEN would have (the set is a
        // multiset of per-source contributions; a matching close in the leg's post-seek stream
        // consumes it by value, compaction's own close-matching semantics — equal deletions are
        // interchangeable). addOpenRangeDeletion applies the partition-deletion filter, symmetric
        // with the filter the seed's eventual close hits in removeOpenRangeDeletion, so a seed
        // shadowed by the merged partition deletion is dropped on both sides consistently
        // (RangeTombstoneMarker.Merger parity).
        for (CursorReads.MergeLeg leg : legs)
        {
            DeletionTime seed = leg.mergeSeekOpenMarker();
            if (seed != null)
                addOpenRangeDeletion(seed);
        }
        // a seeded open supersedes the partition deletion by the filter above, so it is the
        // active shadowing deletion at merge start — the same formula mergeUnfiltereds applies
        // after every marker group
        if (activeOpenRangeDeletion != DeletionTime.LIVE)
            activeDeletion = activeOpenRangeDeletion;
        this.openMarkerAtMergeStart = activeOpenRangeDeletion == DeletionTime.LIVE
                                      ? null
                                      : CursorReads.copyOf(activeOpenRangeDeletion);
    }

    /** See {@link #openMarkerAtMergeStart} — consumed by {@code CursorReads.mergeLegs} for the
     *  post-merge slicer's artificial slice-start open marker. */
    DeletionTime openMarkerAtMergeStart()
    {
        return openMarkerAtMergeStart;
    }

    /**
     * Drives the whole within-partition merge: the adapted shape of CursorCompactor's
     * mergePartitions unfiltered loop (static rows and the partition deletion are reconciled by
     * the caller; every leg enters positioned at its first unfiltered or partition end).
     */
    void mergeUnfiltereds() throws IOException
    {
        // Slice end-stop (the merge-core analog of the single-leg readRows endBound cutoff): an
        // element at-or-past the slice end is never emitted, never validated (isInSlice is
        // strictly-before-end) and never open-marker tracked downstream — the iterator path's
        // ForwardReader stops at compareNextTo(end) >= 0 without ever deserializing past it — so
        // the k-way merge can stop outright at the FIRST group there instead of merging the
        // partition's whole tail only for SlicedMaterializedIterator to discard it. The end bound
        // is staged ONCE as a descriptor holding the same serialized-values wire form the legs'
        // descriptors hold, so the per-group check is the shared descriptor-level comparator —
        // no per-group clustering materialization (which the pre-emission marker path never pays).
        final ClusteringDescriptor sliceEndStop = sliceEnd == null ? null : sliceBoundDescriptor(sliceEnd);
        // M3.2b: the slice START staged the same way, so a filter-dropped group's emitted-surface
        // verdict (strictly after the slice start — the slicer's NON-strict pre-slice skip, the
        // exact predicate LimitingMergeSink twins on materialized clusterings) is a descriptor
        // -level comparison with no clustering materialization. Only needed when a probe is
        // attached: groups at-or-before the slice start reach the metrics stage on neither path.
        final ClusteringDescriptor sliceStartStop = filterProbe == null || sliceStart == null
                                                    ? null : sliceBoundDescriptor(sliceStart);
        int prevMergeLimit = legs.length;
        for (;;)
        {
            // M3.1 production bound (the sink-driven analog of the slice end-stop below): once the
            // sink no longer wants rows — its LIMIT twin says the query's counter would already
            // have stopped consuming — the remaining tail is never even sorted. Checked BEFORE the
            // group so a bound that was already satisfied on entry (e.g. a paging resume with an
            // exhausted per-partition budget) merges nothing at all. Under-production is the only
            // failure mode here and it is byte-divergent (a truncated result), which is exactly
            // what the differential harness compares; stopping late merely wastes allocation.
            if (!sink.wantsMore())
                return;
            int mergeLimit = prepareAndSortUnfilteredForMerge(prevMergeLimit);
            if (mergeLimit == 0)
                return;
            if (sliceEndStop != null && ClusteringComparator.compare(legs[0].unfiltered(), sliceEndStop) >= 0)
                return; // merge minimum at-or-past the slice end: nothing further can be emitted
            // Row vs marker via the descriptor's clustering kind (CLUSTERING = row, bound/boundary
            // = marker; statics never reach the merge) — kind-based rather than wire-flags-based
            // since M2.3, so the object-backed memtable leg needs no synthetic flags byte. For
            // byte-backed legs the two are equivalent by construction: loadRow always sets
            // CLUSTERING, loadTombstone always sets a bound/boundary kind.
            UnfilteredDescriptor minimum = legs[0].unfiltered();
            if (minimum.clusteringKind() == ClusteringPrefix.Kind.CLUSTERING)
            {
                // M3.2b: clustering-column filter verdict at group formation, from the
                // descriptor's clustering wire bytes — BEFORE the escape hatch, any cell work or
                // (on default validation-disabled configs) any clustering materialization. A
                // rejected group takes the metadata-only accounting walk instead of the emitting
                // merge: it allocates nothing and its cells' values are skipped at the byte
                // level, but its scan-metrics contributions (what MetricRecording would have
                // counted for it below the top-level filter) are still recorded.
                // M3.2c: the group's emitted-surface verdict is computed HERE for every probed
                // group (not just clustering-rejected ones) because a regular-column failure can
                // surface later, mid-cell-walk, when the descriptor comparison is still the only
                // allocation-free way to answer it — the abandoned row may never have
                // materialized a clustering.
                rowOnEmittedSurface = sliceStartStop == null
                                      || ClusteringComparator.compare(minimum, sliceStartStop) > 0;
                if (filterProbe != null && !filterProbe.rowGroupMatches(minimum))
                {
                    accountDroppedRowGroup(mergeLimit, rowOnEmittedSurface);
                }
                else
                {
                    mergeRowGroup(mergeLimit);
                }
            }
            else if (minimum.isStartBound() || minimum.isEndBound() || minimum.isBoundary())
            {
                mergeMarkerGroup(mergeLimit);
                activeDeletion = activeOpenRangeDeletion == DeletionTime.LIVE
                                 ? mergedPartitionDeletion
                                 : activeOpenRangeDeletion;
            }
            else
            {
                throw new IllegalStateException("Unexpected unfiltered type (not row or tombstone): " + minimum.clusteringKind());
            }
            continueReadingAfterMerge(mergeLimit);
            prevMergeLimit = mergeLimit;
        }
    }

    // ---------------------------------------------------------------- sort and merge limits
    // (the adapted prepareAndSortUnfilteredForMerge / prepareAndSortCellsForMerge /
    //  sortPerturbedCursors trio; the insert sort itself is compaction's own
    //  PreSortedBubbleInsert, shared not copied)

    private int prepareAndSortUnfilteredForMerge(int prevMergeLimit) throws IOException
    {
        for (int i = 0; i < prevMergeLimit; i++)
        {
            CursorReads.MergeLeg leg = legs[i];
            int state = leg.cursorState();
            if (isState(state, ROW_START | TOMBSTONE_START))
                leg.readUnfilteredHeader();
            else if (!isState(state, PARTITION_END | DONE))
                throw new IllegalStateException("Leg in an unexpected state before unfiltered sort: " + state);
        }
        CLUSTERING_SORT.sortPerturbed(legs, equalsNext, prevMergeLimit, legs.length);
        if (isState(legs[0].cursorState(), PARTITION_END | DONE))
            return 0;
        int mergeLimit = 1;
        for (; mergeLimit < legs.length; mergeLimit++)
        {
            if (!equalsNext[mergeLimit - 1])
                break;
        }
        return mergeLimit;
    }

    private int prepareAndSortCellsForMerge(int rowMergeLimit, int prevCellMergeLimit)
    {
        cellSort.sortPerturbed(legs, equalsNext, prevCellMergeLimit, rowMergeLimit);
        if (!legs[0].parkedAtCellPosition())
            return 0;
        int cellMergeLimit = 1;
        for (; cellMergeLimit < rowMergeLimit; cellMergeLimit++)
        {
            if (!equalsNext[cellMergeLimit - 1])
                break;
        }
        return cellMergeLimit;
    }

    private static int compareByClustering(CursorReads.MergeLeg c1, CursorReads.MergeLeg c2)
    {
        if (c1 == c2)
            return 0;
        boolean done1 = isState(c1.cursorState(), PARTITION_END | DONE);
        boolean done2 = isState(c2.cursorState(), PARTITION_END | DONE);
        if (done1 && done2)
            return 0;
        if (done1)
            return 1;
        if (done2)
            return -1;
        // both are past their header (UNFILTERED_END or CELL_HEADER_START): the shared
        // descriptor-level comparator (already public static) decides
        return ClusteringComparator.compare(c1.unfiltered(), c2.unfiltered());
    }

    private int compareByCell(CursorReads.MergeLeg c1, CursorReads.MergeLeg c2)
    {
        if (c1 == c2)
            return 0;
        boolean parked1 = c1.parkedAtCellPosition();
        boolean parked2 = c2.parkedAtCellPosition();
        if (!parked1 && !parked2)
            return 0;
        if (!parked1)
            return 1;
        if (!parked2)
            return -1;
        ColumnMetadata col1 = c1.cellColumn();
        ColumnMetadata col2 = c2.cellColumn();
        int byColumn = col1.compareTo(col2);
        if (byColumn != 0 || !col1.isComplex())
            return byColumn;
        // same complex column: deletion-only positions (no cell) sort before any cell so the
        // column's deletion contributors group ahead of its cells
        boolean produced1 = c1.cellProduced();
        boolean produced2 = c2.cellProduced();
        if (!produced1 || !produced2)
            return Boolean.compare(produced1, produced2);
        return CursorCompactor.comparePaths(col1, c1.cellPathWindow(), c2.cellPathWindow());
    }

    private void continueReadingAfterMerge(int mergeLimit) throws IOException
    {
        for (int i = 0; i < mergeLimit; i++)
        {
            if (legs[i].cursorState() == UNFILTERED_END)
                legs[i].continueReading();
        }
    }

    // ---------------------------------------------------------------- row groups

    /** The adapted shape of CursorCompactor.mergeRows (Row.Merger.merge semantics), purge-free. */
    private void mergeRowGroup(int rowMergeLimit) throws IOException
    {
        // M2.3 whole-row escape hatch: a single-contributor group under a live active deletion is
        // exactly Row.Merger.merge's fast path ("If for this clustering we have only one row
        // version and have no activeDeletion (i.e. nothing to filter out), then we can just return
        // that single row", Row.java) — when that only contributor is an object-backed memtable
        // leg, the merged row IS the leg's already-live row object, emitted with zero
        // decomposition, zero rebuild and zero allocation. Byte-backed legs return null and take
        // the general path unchanged. Validation is irrelevant here by construction: the iterator
        // path never applies corrupted-tombstone validation to memtable data.
        // TEST_FORCE_MEMTABLE_ROW_REUSE deliberately breaks the isLive() condition so the
        // differential harness can prove it catches a wrong reuse (shadowed data resurrecting).
        if (rowMergeLimit == 1 && (activeDeletion.isLive() || CursorReads.TEST_FORCE_MEMTABLE_ROW_REUSE))
        {
            Row existing = legs[0].consumeExistingRow();
            if (existing != null)
            {
                // M3.2c: the escape-hatch row is already a live object, so its regular-column
                // verdict uses the REAL Expression.isSatisfiedBy (not the value-window twin);
                // clustering expressions already passed at group formation (rowGroupMatches runs
                // before mergeRowGroup). A failing row is never emitted — the probe runs the full
                // dropped-row accounting cycle over the object's own content instead.
                if (filterProbe != null && !filterProbe.existingRowMatches(existing))
                {
                    filterProbe.abandonExistingRow(existing, rowOnEmittedSurface);
                    return;
                }
                sink.addRow(existing);
                CursorReads.countMemtableRowReuse();
                return;
            }
        }

        UnfilteredDescriptor first = legs[0].unfiltered();
        LivenessInfo mergedInfo = first.livenessInfo();
        DeletionTime mergedDeletion = first.deletionTime();
        for (int i = 1; i < rowMergeLimit; i++)
        {
            UnfilteredDescriptor other = legs[i].unfiltered();
            if (other.livenessInfo().supersedes(mergedInfo))
                mergedInfo = other.livenessInfo();
            if (other.deletionTime().supersedes(mergedDeletion))
                mergedDeletion = other.deletionTime();
        }

        // AT MOST one clustering materialization per merged group (vs one per source row on the
        // per-leg path), and NONE for a group that merges to nothing: materialization is deferred
        // to the row's actual start (startRow/ensureRowStarted below). The clustering's source leg
        // is captured NOW, before the cell sort reorders legs[]: every group member's clustering
        // compares equal, but equal clusterings need not be byte-identical (e.g. decimals), and
        // output bytes must keep coming from the group's first-sorted leg, exactly as before.
        rowClusteringLeg = legs[0];
        rowClustering = null;
        rowStarted = false;
        // isInSlice's comparator walks exist only to gate validation (Gap C's in-slice placement),
        // so they — and the eager clustering materialization feeding them — are short-circuited
        // away entirely on the default validation-disabled configs
        boolean validateInSlice = false;
        if (validationEnabled)
        {
            rowClustering = (Clustering<?>) rowClusteringLeg.materializeClusteringPrefix();
            validateInSlice = isInSlice(rowClustering);
        }
        if (validateInSlice)
        {
            // per-leg parity with the iterator path, which validates EACH leg's deserialized row
            // (winners and merge losers alike) below its merge
            for (int i = 0; i < rowMergeLimit; i++)
                legs[i].validateRowHeader();
        }

        // Row.Merger.merge / BTreeRow.purge analog WITHOUT the purge arms: the merged row
        // deletion survives only when it supersedes the active partition/range deletion, and
        // liveness shadowed by the resulting active deletion is dropped
        DeletionTime rowActiveDeletion = activeDeletion;
        DeletionTime rowDeletionOut;
        if (mergedDeletion.supersedes(rowActiveDeletion))
        {
            rowActiveDeletion = mergedDeletion;
            rowDeletionOut = CursorReads.copyOf(mergedDeletion);
        }
        else
        {
            // the partition/range deletion takes over
            rowDeletionOut = DeletionTime.LIVE;
        }
        LivenessInfo livenessOut;
        if (mergedInfo.isEmpty() || rowActiveDeletion.deletes(mergedInfo))
            livenessOut = LivenessInfo.EMPTY;
        else
            livenessOut = LivenessInfo.withExpirationTime(mergedInfo.timestamp(), mergedInfo.ttl(),
                                                          mergedInfo.localExpirationTime());

        // a surviving shell makes the merged row non-empty no matter what its cells do: start it
        // now; an empty shell defers to the first surviving cell/complex deletion (ensureRowStarted)
        // so a group merging to nothing emits nothing and allocates nothing
        if (!livenessOut.isEmpty() || !rowDeletionOut.isLive())
            startRow(livenessOut, rowDeletionOut);

        currentComplexColumn = null;
        rowAbandonedByFilter = false;
        int cellMergeLimit = rowMergeLimit;
        for (;;)
        {
            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (legs[i].needsCellAdvance())
                    legs[i].ensureParkedAtCell(validateInSlice);
            }
            cellMergeLimit = prepareAndSortCellsForMerge(rowMergeLimit, cellMergeLimit);
            if (cellMergeLimit == 0)
                break;
            // M3.2c: once a regular-column expression failed mid-walk the row is being ABANDONED —
            // the remaining cell groups take the metadata-only accounting walk (same winner
            // resolution, no value staging), streaming their surviving winners into the
            // dropped-row accounting the probe opened at abandonment.
            if (rowAbandonedByFilter)
                accountDroppedCellGroup(cellMergeLimit, rowMergeLimit, rowActiveDeletion);
            else
                mergeCellGroup(cellMergeLimit, rowMergeLimit, rowActiveDeletion);
            for (int i = 0; i < cellMergeLimit; i++)
                legs[i].advancePastCellPosition();
        }
        if (rowAbandonedByFilter)
        {
            // mid-walk abandonment: the probe opened the dropped accounting at the failure point
            // and every remaining winner streamed into it above; classification closes here
            rowAbandonedByFilter = false;
            filterProbe.endDroppedRow();
        }
        else if (rowStarted && filterProbe != null && !filterProbe.rowRegularFiltersSatisfied())
        {
            // M3.2c row-end absence failure: a filter column never surfaced a surviving winner in
            // this (fully-merged, otherwise-emittable) row, so getValue would return null and the
            // top filter would drop it. Only a STARTED row can need this: an unstarted row merged
            // to nothing, is emitted by neither path, and reaches the metrics stage on neither.
            abandonRowByFilter(null);
            rowAbandonedByFilter = false;
            filterProbe.endDroppedRow();
        }
        else if (rowStarted)
        {
            sink.endRow();
        }
    }

    /** M3.2c: routes a regular-column expression failure into the probe's abandonment cycle (see
     *  {@link FilterProbe#abandonRowGroup}) and flips the row to accounting mode. The sink's
     *  partial output row is discarded by the probe; {@code rowStarted} is cleared so no endRow
     *  ever reaches the sink for an abandoned group. */
    private void abandonRowByFilter(CellLivenessInfo failedWinnerLiveness)
    {
        rowAbandonedByFilter = true;
        filterProbe.abandonRowGroup(rowClusteringLeg, rowOnEmittedSurface, failedWinnerLiveness);
        rowStarted = false;
    }

    /** Starts the output row: materializes the group's clustering (once, from the leg captured at
     *  group formation) and opens the sink's row. */
    private void startRow(LivenessInfo livenessOut, DeletionTime rowDeletionOut)
    {
        if (rowClustering == null)
            rowClustering = (Clustering<?>) rowClusteringLeg.materializeClusteringPrefix();
        sink.startRow(rowClustering, livenessOut, rowDeletionOut);
        rowStarted = true;
    }

    /** Deferred start for a row group whose merged shell was empty: EMPTY liveness and LIVE row
     *  deletion are exactly what the eager path passed the sink for such a group. */
    private void ensureRowStarted()
    {
        if (!rowStarted)
            startRow(LivenessInfo.EMPTY, DeletionTime.LIVE);
    }

    // ---------------------------------------------------------------- cell groups

    /**
     * The adapted shape of CursorCompactor.mergeCells: winner selection via the SHARED
     * {@link CellLivenessInfo#resolve} decision table, value comparison only on full
     * metadata ties (left/current winner keeps equal values, exactly Cells.resolveRegular), and
     * shadowed-cell dropping against the active partition/range/complex deletion. Purge-free,
     * no TTL-expiry conversion (both compaction-only; see the class javadoc), and materializing —
     * the winner becomes a real Cell through the leg's Phase 1 value/path machinery; losers'
     * values are skipped at the byte level without ever being materialized.
     */
    private void mergeCellGroup(int cellMergeLimit, int rowMergeLimit, DeletionTime rowActiveDeletion) throws IOException
    {
        CursorReads.MergeLeg winner = legs[0];
        ColumnMetadata column = winner.cellColumn();
        DeletionTime effectiveDeletion = rowActiveDeletion;
        if (column.isComplex())
        {
            if (!CursorReads.sameColumn(currentComplexColumn, column))
            {
                currentComplexColumn = column;
                // On entering a new output complex column, every row-group source that owns it is
                // parked at it (column-ordered streams; this column is the merge minimum, and
                // deletion-only positions sort ahead of its cells) — so the merged complex
                // deletion is computable up front, before any of the column's cells is emitted.
                // Contributions arrive per-leg filtered (dropped-column rule) from
                // PendingLeg.cellComplexDeletion.
                mergedComplexDeletion.resetLive();
                for (int i = 0; i < rowMergeLimit; i++)
                {
                    CursorReads.MergeLeg leg = legs[i];
                    if (leg.parkedAtCellPosition() && CursorReads.sameColumn(leg.cellColumn(), column))
                    {
                        DeletionTime contribution = leg.cellComplexDeletion();
                        if (contribution.supersedes(mergedComplexDeletion))
                            mergedComplexDeletion.reset(contribution);
                    }
                }
                // survives only when it STRICTLY supersedes the active deletion — on exact
                // equality the iterator drops it (ColumnDataReducer, Row.java) — and NO purge:
                // a gcable complex deletion still shadows and still reaches the merged output
                if (!rowActiveDeletion.isLive() && !mergedComplexDeletion.supersedes(rowActiveDeletion))
                    mergedComplexDeletion.resetLive();
                if (!mergedComplexDeletion.isLive())
                {
                    ensureRowStarted();
                    sink.addComplexDeletion(column, CursorReads.copyOf(mergedComplexDeletion));
                }
            }
            if (!mergedComplexDeletion.isLive() && mergedComplexDeletion.supersedes(rowActiveDeletion))
                effectiveDeletion = mergedComplexDeletion;

            if (!winner.cellProduced())
                return; // deletion-only group: contribution already folded into the merged deletion
        }

        // non-null iff tieValueScratch1 holds the CURRENT winner's staged (already-consumed)
        // value bytes — compaction's tempCellBuffer invariant, verbatim
        CellValueScratch stagedWinnerValue = null;
        for (int i = 1; i < cellMergeLimit; i++)
        {
            CursorReads.MergeLeg challenger = legs[i];
            Resolution resolution = CellLivenessInfo.resolve(winner.cellLiveness(),
                                                             challenger.cellLiveness());
            if (CursorReads.TEST_CORRUPT_MERGE_DECISIONS)
            {
                // mutation-test hook: pick the LOSER of reconciliation so the differential
                // harness can prove it detects a wrong merge DECISION
                if (resolution == Resolution.LEFT)
                    resolution = Resolution.RIGHT;
                else if (resolution == Resolution.RIGHT)
                    resolution = Resolution.LEFT;
            }
            if (resolution == Resolution.LEFT)
            {
                challenger.discardCellValue();
            }
            else if (resolution == Resolution.RIGHT)
            {
                winner.discardCellValue();
                winner = challenger;
                stagedWinnerValue = null; // the new winner's value is still unconsumed on its cursor
            }
            else // COMPARE: full metadata tie — greater value wins, current winner keeps ties
            {
                if (effectiveDeletion.deletesCellAt(challenger.cellLiveness().timestamp()))
                {
                    // a shadowed challenger can at best tie, and the tie keeps the winner: skip
                    // its value without materializing (compaction's exact short-circuit)
                    challenger.discardCellValue();
                }
                else
                {
                    // Matches Cells.resolveRegular: compareValues(left, right) >= 0 ? left : right
                    // — the challenger only wins with a STRICTLY greater value. Raw value bytes
                    // (the leg machinery strips the wire's length vint), plain unsigned order.
                    // Staged into reusable scratch buffers, NOT materialized: only the group's
                    // final winner ever becomes a real byte[] (at emission below); a loser's
                    // bytes stay in scratch until the next tie overwrites them.
                    if (stagedWinnerValue == null)
                    {
                        tieValueScratch1.clear();
                        winner.stageCellValue(tieValueScratch1);
                        stagedWinnerValue = tieValueScratch1;
                    }
                    tieValueScratch2.clear();
                    challenger.stageCellValue(tieValueScratch2);
                    if (CellValueScratch.compare(tieValueScratch1, tieValueScratch2) < 0)
                    {
                        // challenger wins: swap the scratches so scratch1 keeps holding the
                        // running winner's bytes (compaction's exact swap discipline)
                        CellValueScratch swap = tieValueScratch1;
                        tieValueScratch1 = tieValueScratch2;
                        tieValueScratch2 = swap;
                        stagedWinnerValue = tieValueScratch1;
                        winner = challenger;
                    }
                }
            }
        }

        // M3.2c: whether this group's column carries a regular-column filter expression whose
        // winner must be evaluated at resolution (constant false without regular pushdown)
        boolean filtered = filterProbe != null && filterProbe.filtersRegularColumn(column);

        if (effectiveDeletion.deletesCellAt(winner.cellLiveness().timestamp()))
        {
            // shadowed by the active partition/range/complex deletion: dropped from the merged
            // row, exactly like Row.Merger.ColumnDataReducer — and unlike compaction, NEVER
            // dropped for being purgeable (no purger here)
            winner.discardCellValue();
            // M3.2c: a shadowed winner never reaches the merged row, so a filter column's
            // expression finds NO cell there — getValue returns null and the top filter drops
            // the row. The shadowed winner itself reaches the metrics stage on neither path
            // (null liveness: it contributes nothing to the dropped-row accounting).
            if (filtered)
                abandonRowByFilter(null);
            return;
        }

        if (filtered)
        {
            boolean matches = false;
            if (filterProbe.cellIsLive(winner.cellLiveness()))
            {
                ByteBuffer window;
                Cell<?> memtableWinner = winner.existingCell();
                if (memtableWinner != null)
                {
                    // memtable winner: the live cell object's own (stable) buffer, evaluated
                    // directly — no windowing needed, and the object stays intact for the
                    // escape-hatch emission below
                    window = memtableWinner.buffer();
                }
                else
                {
                    // sstable winner: consume the value bytes into the tie-break scratch (unless
                    // a tie already staged them there) and evaluate over a reusable window
                    if (stagedWinnerValue == null)
                    {
                        tieValueScratch1.clear();
                        winner.stageCellValue(tieValueScratch1);
                        stagedWinnerValue = tieValueScratch1;
                    }
                    window = stagedWinnerValue.valueWindow();
                }
                // ALIASING AUDIT (FINDING #15 discipline, recorded at this call site): `window`
                // is a view over reusable storage (the tie scratch, overwritten by the next
                // stage) or the memtable cell's buffer; it is handed only to the probe's
                // Operator.isSatisfiedBy evaluation, which compares within the call and retains
                // nothing, and the view is dropped on return — nothing outlives this call. A
                // PASSING sstable winner's staged bytes promote to the one real array at
                // emission below through the existing stagedWinnerValue tie-break pattern (one
                // bounded extra memcpy on filter columns only).
                matches = filterProbe.regularCellMatches(column, window);
            }
            if (!matches)
            {
                // a dead winner (getValue null) or a value mismatch: the row provably fails the
                // top filter. The failing winner itself IS part of the merged row on the
                // iterator path (unlike the shadowed case), so its liveness feeds the dropped
                // accounting — a dead winner counts as a tombstone, a live mismatch as live data.
                winner.discardCellValue(); // no-op when staging already consumed the bytes
                abandonRowByFilter(winner.cellLiveness());
                return;
            }
        }

        // M2.3 cell escape hatch: a memtable-won cell is emitted as its already-live object —
        // exactly what the object merge does (Cells.reconcile returns the winning cell object,
        // never a rebuild). Its value was never consumed off any cursor (staging reads the live
        // object non-destructively), so the object is intact even after a tie-break; the staged
        // scratch bytes are simply abandoned. TEST_CORRUPT_CELL_TIMESTAMPS deliberately does not
        // apply here — it corrupts MATERIALIZED cells, and the M2.3 memtable-specific hooks
        // (TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS / TEST_FORCE_MEMTABLE_ROW_REUSE) own this surface.
        Cell<?> existingWinner = winner.existingCell();
        if (existingWinner != null)
        {
            ensureRowStarted();
            sink.addCell(existingWinner);
            CursorReads.countMemtableCellReuse();
            return;
        }

        long timestamp = winner.cellLiveness().timestamp();
        int ttl = winner.cellLiveness().ttl();
        long localDeletionTime = winner.cellLiveness().localDeletionTime();
        CellPath path = winner.cellPath();
        if (CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS)
            timestamp += 1;
        ensureRowStarted();
        if (sink.wantsWireStreamedCells())
        {
            // M3.3a-ii: stream the winner's value bytes straight from wherever they currently
            // live into the sink's own destination — the actual allocation win this seam exists
            // for, replacing the byte[]/Cell materialization below entirely for sinks that opt in.
            if (stagedWinnerValue != null)
                winnerCellValueSource.resetToScratch(stagedWinnerValue);
            else
                winnerCellValueSource.resetToLeg(winner);
            sink.addCellFromWire(winner.cellColumn(), timestamp, ttl, localDeletionTime, path, winnerCellValueSource);
            // Safety net, mirroring the discardCellValue() idiom used everywhere else in this
            // method: a no-op if the sink already streamed the leg's value (state has moved past
            // CELL_VALUE_START), or if the value lived in scratch all along (this leg was never
            // touched by this cell) — but mandatory if the sink chose not to consume it at all
            // (e.g. TranscodeMergeSink dropping a purged cell, or the row not being admitted),
            // since advancePastCellPosition() throws if a value is left unconsumed.
            winner.discardCellValue();
        }
        else
        {
            // a tie winner's value bytes were consumed into scratch during the comparison:
            // promote them to the one real array the group allocates (one extra memcpy for the
            // single winner, against one whole allocation saved per tie loser)
            byte[] value = stagedWinnerValue != null ? stagedWinnerValue.toValueArray() : winner.cellValue();
            CursorReads.countSstableCellValueMaterialized();
            sink.addCell(ByteArrayAccessor.instance.factory()
                                          .cell(winner.cellColumn(), timestamp, ttl, localDeletionTime, value, path));
        }
    }

    // ---------------------------------------------------------------- dropped row groups (M3.2b)

    /**
     * The accounting twin of {@link #mergeRowGroup} for a group the {@link FilterProbe} rejected:
     * runs the IDENTICAL metadata-level winner resolution — group liveness/deletion fold,
     * active-deletion shadowing, per-leg in-slice validation (Gap C parity: the iterator path
     * validates each leg's deserialized row below its merge, including rows the filter later
     * drops), complex-deletion reconciliation, cell winner selection and shadow-dropping — but
     * emits NOTHING: no clustering, cell or path is materialized, values are skipped at the byte
     * level, and the surviving merged content is reported to the probe as metadata for scan-metric
     * classification. The one deliberate difference from the emitting path: on a full-metadata
     * cell tie ({@code Resolution.COMPARE}) no value comparison happens at all — metadata-tied
     * cells share their liveness classification regardless of which wins the tie (verified against
     * {@code CellLivenessInfo.resolve}: COMPARE is only returned when timestamp,
     * tombstone/expiring class, localDeletionTime and ttl all agree), so the winner's identity is
     * irrelevant to accounting. {@code TEST_CORRUPT_MERGE_DECISIONS} deliberately does not apply
     * here for the same reason: inverting a non-tie pick would change classification, but the hook
     * exists to corrupt EMITTED merge decisions, and the accounting walk's own negative control is
     * {@code CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING}.
     *
     * The M2.3 whole-row escape hatch is deliberately NOT taken for dropped groups: the general
     * walk works for object-backed legs too (their cell surface is the same), and one accounting
     * code path beats two.
     */
    private void accountDroppedRowGroup(int rowMergeLimit, boolean onEmittedSurface) throws IOException
    {
        UnfilteredDescriptor first = legs[0].unfiltered();
        LivenessInfo mergedInfo = first.livenessInfo();
        DeletionTime mergedDeletion = first.deletionTime();
        for (int i = 1; i < rowMergeLimit; i++)
        {
            UnfilteredDescriptor other = legs[i].unfiltered();
            if (other.livenessInfo().supersedes(mergedInfo))
                mergedInfo = other.livenessInfo();
            if (other.deletionTime().supersedes(mergedDeletion))
                mergedDeletion = other.deletionTime();
        }

        rowClusteringLeg = legs[0];
        rowClustering = null;
        rowStarted = false;
        boolean validateInSlice = false;
        if (validationEnabled)
        {
            rowClustering = (Clustering<?>) rowClusteringLeg.materializeClusteringPrefix();
            validateInSlice = isInSlice(rowClustering);
        }
        if (validateInSlice)
        {
            for (int i = 0; i < rowMergeLimit; i++)
                legs[i].validateRowHeader();
        }

        // identical shell computation to mergeRowGroup, minus the output-copy allocations: the
        // probe receives the merged (post-shadowing) shell as reusable metadata
        DeletionTime rowActiveDeletion = activeDeletion;
        DeletionTime rowDeletionOut;
        if (mergedDeletion.supersedes(rowActiveDeletion))
        {
            rowActiveDeletion = mergedDeletion;
            rowDeletionOut = mergedDeletion;
        }
        else
        {
            rowDeletionOut = DeletionTime.LIVE;
        }
        LivenessInfo livenessOut = mergedInfo.isEmpty() || rowActiveDeletion.deletes(mergedInfo)
                                   ? null
                                   : mergedInfo;

        filterProbe.beginDroppedRow(rowClusteringLeg, onEmittedSurface, livenessOut, rowDeletionOut);
        currentComplexColumn = null;
        int cellMergeLimit = rowMergeLimit;
        for (;;)
        {
            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (legs[i].needsCellAdvance())
                    legs[i].ensureParkedAtCell(validateInSlice);
            }
            cellMergeLimit = prepareAndSortCellsForMerge(rowMergeLimit, cellMergeLimit);
            if (cellMergeLimit == 0)
                break;
            accountDroppedCellGroup(cellMergeLimit, rowMergeLimit, rowActiveDeletion);
            for (int i = 0; i < cellMergeLimit; i++)
                legs[i].advancePastCellPosition();
        }
        filterProbe.endDroppedRow();
    }

    /**
     * The accounting twin of {@link #mergeCellGroup}: same winner selection (minus the tie-break
     * value comparison — see {@link #accountDroppedRowGroup}), same complex-deletion fold and
     * suppression, same shadowed-winner drop; every value discarded at the byte level, the
     * surviving winner reported to the probe as liveness metadata only.
     */
    private void accountDroppedCellGroup(int cellMergeLimit, int rowMergeLimit, DeletionTime rowActiveDeletion) throws IOException
    {
        CursorReads.MergeLeg winner = legs[0];
        ColumnMetadata column = winner.cellColumn();
        DeletionTime effectiveDeletion = rowActiveDeletion;
        if (column.isComplex())
        {
            if (!CursorReads.sameColumn(currentComplexColumn, column))
            {
                currentComplexColumn = column;
                mergedComplexDeletion.resetLive();
                for (int i = 0; i < rowMergeLimit; i++)
                {
                    CursorReads.MergeLeg leg = legs[i];
                    if (leg.parkedAtCellPosition() && CursorReads.sameColumn(leg.cellColumn(), column))
                    {
                        DeletionTime contribution = leg.cellComplexDeletion();
                        if (contribution.supersedes(mergedComplexDeletion))
                            mergedComplexDeletion.reset(contribution);
                    }
                }
                if (!rowActiveDeletion.isLive() && !mergedComplexDeletion.supersedes(rowActiveDeletion))
                    mergedComplexDeletion.resetLive();
                if (!mergedComplexDeletion.isLive())
                    filterProbe.droppedComplexDeletion(mergedComplexDeletion);
            }
            if (!mergedComplexDeletion.isLive() && mergedComplexDeletion.supersedes(rowActiveDeletion))
                effectiveDeletion = mergedComplexDeletion;

            if (!winner.cellProduced())
                return; // deletion-only group: contribution already folded into the merged deletion
        }

        for (int i = 1; i < cellMergeLimit; i++)
        {
            CursorReads.MergeLeg challenger = legs[i];
            Resolution resolution = CellLivenessInfo.resolve(winner.cellLiveness(),
                                                             challenger.cellLiveness());
            if (resolution == Resolution.RIGHT)
            {
                winner.discardCellValue();
                winner = challenger;
            }
            else
            {
                // LEFT keeps the winner; COMPARE is a full metadata tie whose contenders share
                // liveness classification, so the current winner stands for accounting purposes
                // and neither side's value bytes are ever staged or compared
                challenger.discardCellValue();
            }
        }

        boolean shadowed = effectiveDeletion.deletesCellAt(winner.cellLiveness().timestamp());
        if (!shadowed)
            filterProbe.droppedCell(winner.cellLiveness());
        winner.discardCellValue();
    }

    // ---------------------------------------------------------------- range tombstone groups

    /**
     * The adapted shape of CursorCompactor.mergeRangeTombstones (RangeTombstoneMarker.Merger
     * semantics): the merged stream carries a marker exactly where the supersedes-max over the
     * sources' open deletions changes — a bound when one side of the transition is LIVE, a
     * boundary otherwise. Purge-free: compaction's shouldPurgeClose/shouldPurgeOpen splits (which
     * can turn a boundary into a bound or drop the marker) do not exist on the read path.
     */
    private void mergeMarkerGroup(int rangeTombstoneMergeLimit) throws IOException
    {
        DeletionTime previousDeletionInMerged = DeletionTime.LIVE;
        if (activeOpenRangeDeletion != DeletionTime.LIVE)
            previousDeletionInMerged = reusableCopy(activeOpenRangeDeletion);
        try
        {
            updateOpenMarkers(rangeTombstoneMergeLimit);
            DeletionTime newDeletionInMerged = activeOpenRangeDeletion;

            // The group's clustering values are decoded AT MOST ONCE per group and shared between
            // the validation gate's position and the emitted marker's prefix — only the prefix
            // Kind ever differs between the two uses, never the values, and the component arrays
            // are immutable once materialized.
            byte[][] groupClusteringValues = null;

            if (validationEnabled)
            {
                // markers are validated per leg when in-slice (Gap C placement); the group's
                // slice position needs its clustering, materialized here only on
                // validation-enabled configs
                groupClusteringValues = legs[0].materializeBoundValues();
                ClusteringPrefix<?> groupPosition = boundOrBoundary(legs[0].unfiltered().clusteringKind(), groupClusteringValues);
                if (isInSlice(groupPosition))
                {
                    for (int i = 0; i < rangeTombstoneMergeLimit; i++)
                        legs[i].validateMarkerHeader();
                }
            }

            if (previousDeletionInMerged.equals(newDeletionInMerged))
                return; // the merged open deletion did not change: no marker in the merged stream

            if (groupClusteringValues == null)
                groupClusteringValues = legs[0].materializeBoundValues();
            boolean isBeforeClustering = legs[0].unfiltered().clusteringKind().comparedToClustering < 0;
            if (previousDeletionInMerged == DeletionTime.LIVE)
            {
                ClusteringPrefix.Kind kind = isBeforeClustering ? ClusteringPrefix.Kind.INCL_START_BOUND
                                                                : ClusteringPrefix.Kind.EXCL_START_BOUND;
                sink.addRangeTombstoneMarker(new RangeTombstoneBoundMarker((ClusteringBound<?>) boundOrBoundary(kind, groupClusteringValues),
                                                                           CursorReads.copyOf(newDeletionInMerged)));
            }
            else if (newDeletionInMerged == DeletionTime.LIVE)
            {
                ClusteringPrefix.Kind kind = isBeforeClustering ? ClusteringPrefix.Kind.EXCL_END_BOUND
                                                                : ClusteringPrefix.Kind.INCL_END_BOUND;
                sink.addRangeTombstoneMarker(new RangeTombstoneBoundMarker((ClusteringBound<?>) boundOrBoundary(kind, groupClusteringValues),
                                                                           CursorReads.copyOf(previousDeletionInMerged)));
            }
            else
            {
                ClusteringPrefix.Kind kind = isBeforeClustering ? ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY
                                                                : ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY;
                sink.addRangeTombstoneMarker(new RangeTombstoneBoundaryMarker((ClusteringBoundary<?>) boundOrBoundary(kind, groupClusteringValues),
                                                                              CursorReads.copyOf(previousDeletionInMerged),
                                                                              CursorReads.copyOf(newDeletionInMerged)));
            }
        }
        finally
        {
            if (previousDeletionInMerged != DeletionTime.LIVE)
                reusableMarkersPool.offer((DeletionTime.ReusableDeletionTime) previousDeletionInMerged);
        }
    }

    /** Copy-adapt of CursorCompactor.updateOpenMarkers: RangeTombstoneMarker.Merger's
     *  updateOpenMarkers, plus the close-must-match-an-open sanity check. */
    private void updateOpenMarkers(int rangeTombstoneMergeLimit)
    {
        for (int i = 0; i < rangeTombstoneMergeLimit; i++)
        {
            UnfilteredDescriptor marker = legs[i].unfiltered();
            if (marker.isStartBound())
            {
                addOpenRangeDeletion(marker.deletionTime());
            }
            else if (marker.isEndBound())
            {
                removeOpenRangeDeletion(marker.deletionTime(), marker);
            }
            else if (marker.isBoundary())
            {
                removeOpenRangeDeletion(marker.deletionTime(), marker);
                addOpenRangeDeletion(marker.deletionTime2());
            }
            else
            {
                throw new IllegalStateException("Unexpected bound type:" + marker.clusteringKind());
            }
        }

        if (activeOpenRangeDeletion == null)
            recalculateActiveOpen();
    }

    private void recalculateActiveOpen()
    {
        // the active open was invalidated by a close matching it: scan the set for the new max
        int size = openMarkers.size();
        if (size == 0)
        {
            activeOpenRangeDeletion = DeletionTime.LIVE;
            return;
        }
        DeletionTime maxOpenDeletion = openMarkers.get(0);
        for (int i = 1; i < size; i++)
        {
            DeletionTime openDeletion = openMarkers.get(i);
            if (openDeletion.supersedes(maxOpenDeletion))
                maxOpenDeletion = openDeletion;
        }
        activeOpenRangeDeletion = maxOpenDeletion;
    }

    private void addOpenRangeDeletion(DeletionTime openRangeDeletion)
    {
        // markers shadowed by the partition-level deletion never surface in the merged stream
        // (RangeTombstoneMarker.Merger's partition-deletion filter)
        if (!mergedPartitionDeletion.isLive() && !openRangeDeletion.supersedes(mergedPartitionDeletion))
            return;

        DeletionTime.ReusableDeletionTime reusable = reusableCopy(openRangeDeletion);
        openMarkers.add(reusable);
        if (activeOpenRangeDeletion != null && // invalidated by a remove: full rescan pending
            (activeOpenRangeDeletion == DeletionTime.LIVE || reusable.supersedes(activeOpenRangeDeletion)))
            activeOpenRangeDeletion = reusable;
    }

    private void removeOpenRangeDeletion(DeletionTime closeRangeDeletion, UnfilteredDescriptor marker)
    {
        // symmetric with addOpenRangeDeletion's partition-deletion filter (a close's deletion
        // always equals its open's)
        if (!mergedPartitionDeletion.isLive() && !closeRangeDeletion.supersedes(mergedPartitionDeletion))
            return;

        int size = openMarkers.size();
        int j = 0;
        DeletionTime.ReusableDeletionTime matched = null;
        for (; j < size; j++)
        {
            matched = openMarkers.get(j);
            if (matched.equals(closeRangeDeletion))
                break;
        }
        if (j == size)
            throw new IllegalStateException("Expected an open marker for this closing marker:" + marker);

        reusableMarkersPool.offer(matched);
        if (activeOpenRangeDeletion == matched)
            activeOpenRangeDeletion = null; // trigger recalculation
        if (size == 1)
        {
            openMarkers.clear();
        }
        else
        {
            // avoid the array copy: swap in the last element
            DeletionTime.ReusableDeletionTime last = openMarkers.remove(size - 1);
            if (j != size - 1)
                openMarkers.set(j, last);
        }
    }

    private DeletionTime.ReusableDeletionTime reusableCopy(DeletionTime deletionTime)
    {
        DeletionTime.ReusableDeletionTime reusable = reusableMarkersPool.pollLast();
        if (reusable == null)
            reusable = DeletionTime.ReusableDeletionTime.copy(deletionTime);
        else
            reusable.reset(deletionTime);
        return reusable;
    }

    /**
     * Builds a bound/boundary prefix of {@code kind} over already-decoded clustering values —
     * mirrors the branch shape of the leg materializer's own prefix construction (empty values =
     * the factory's per-kind singleton bound) so a single {@code materializeBoundValues} call can
     * back prefixes of different kinds. The values array is shared, never copied: prefix
     * component arrays are immutable after materialization.
     */
    private static ClusteringPrefix<?> boundOrBoundary(ClusteringPrefix.Kind kind, byte[][] values)
    {
        return values.length == 0
               ? ByteArrayAccessor.factory.bound(kind)
               : ByteArrayAccessor.factory.boundOrBoundary(kind, values);
    }

    // ---------------------------------------------------------------- slice position

    /**
     * Whether an element at {@code position} is on the emission surface the iterator path
     * validates: strictly after the slice start (handlePreSliceData's NON-strict skip consumes
     * clustering {@code <= start} without validating) and strictly before the slice end
     * (ForwardReader stops at {@code >= end}).
     */
    private boolean isInSlice(ClusteringPrefix<?> position)
    {
        if (sliceStart != null && comparator.compare(position, (ClusteringPrefix<?>) sliceStart) <= 0)
            return false;
        return sliceEnd == null || comparator.compare(position, (ClusteringPrefix<?>) sliceEnd) < 0;
    }

    /**
     * Stages a slice bound as a {@link ClusteringDescriptor} so the merge loop's end-stop (and,
     * since M3.2b, the filter probe's emitted-surface check against the slice start) can use the
     * shared descriptor-level comparator against each group's minimum, allocation-free per group.
     * The bound's values are serialized with {@code serializeValuesWithoutSize} — the exact
     * wire form {@code readUnfilteredClustering} loads into every leg's descriptor buffer (header
     * vint per 32-component block, raw bytes for fixed-length components, length-vint-prefixed for
     * variable) — and with the LEGS' clustering types, since
     * {@link ClusteringComparator#compare(ClusteringDescriptor, ClusteringDescriptor)} decodes
     * both buffers with the first argument's (a leg's) types.
     */
    private ClusteringDescriptor sliceBoundDescriptor(ClusteringBound<?> bound) throws IOException
    {
        AbstractType<?>[] types = legs[0].unfiltered().clusteringTypes();
        try (DataOutputBuffer out = new DataOutputBuffer(64))
        {
            ClusteringPrefix.serializer.serializeValuesWithoutSize(bound, out, MessagingService.current_version,
                                                                   Arrays.asList(types));
            return new SliceBoundDescriptor(types, bound, out.getData(), out.getLength());
        }
    }

    /** A {@link ClusteringDescriptor} loaded from a query-side bound instead of from the data
     *  file — comparison-only (never handed to any cursor), see {@link #sliceBoundDescriptor}. */
    private static final class SliceBoundDescriptor extends ClusteringDescriptor
    {
        SliceBoundDescriptor(AbstractType<?>[] types, ClusteringBound<?> bound, byte[] serializedValues, int length)
        {
            super(types);
            clusteringKind(bound.kind());
            clusteringColumnsBound = bound.size();
            overwrite(serializedValues, length);
        }
    }

    // ---------------------------------------------------------------- tie-break value staging

    /**
     * Reusable growable staging buffer for the cell tie-break comparison — the read-side analog
     * of compaction's {@code tempCellBuffer1/2} {@code DataOutputBuffer}s, but capturing the RAW
     * value bytes the way {@code CursorReads.CellValueCapture} does: the wire's length vint
     * (variable-length types only) is intercepted for pre-sizing and NOT written, so the staged
     * bytes compare exactly like the final-form arrays {@code PendingLeg.cellValue()} produces
     * (no vint-skip arithmetic at compare time, unlike compaction's wire-form buffers). Only the
     * two calls {@code copyCellValue} makes on its writer are supported; everything else fails
     * loudly (the CellValueCapture discipline: a copy-loop shape change breaks the build of the
     * comparison, not the comparison's result).
     */
    static final class CellValueScratch implements DataOutputPlus
    {
        private byte[] buffer = new byte[64];
        private int length;
        /** M3.3a-ii: whether {@link #writeUnsignedVInt32} was called while staging the bytes
         *  currently held — i.e. whether the ORIGINAL wire value was variable-length (a vint
         *  preceded it). Needed to replay ({@link #streamTo}) the exact same shape
         *  {@code stageCellValue} would have written to a fresh destination: the vint itself is
         *  never retained in {@link #buffer} (see the class javadoc), only whether one occurred. */
        private boolean sawVIntLength;
        /** M3.2c: reusable filter-evaluation view over {@link #buffer}; re-wrapped only when the
         *  grow-only buffer is replaced, never per evaluation (the {@code filterPathView}
         *  pattern). The window's contents are valid only until the next {@code clear()}/stage —
         *  it must never escape the immediate filter-evaluation call (aliasing audit at the
         *  {@code mergeCellGroup} call site). */
        private ByteBuffer window;

        void clear()
        {
            length = 0;
            sawVIntLength = false;
        }

        /** The number of raw value bytes currently staged (0 for a valueless cell — a tombstone,
         *  or a live cell of an intentionally empty value; see {@link CellValueSource#hasValue}). */
        int length()
        {
            return length;
        }

        /**
         * M3.3a-ii: replays these staged bytes into {@code dest} exactly as the original
         * {@code stageCellValue} call would have written them — the vint length first, but ONLY
         * if the original wire value was variable-length ({@link #sawVIntLength}), then the raw
         * bytes — so a {@link CellValueSource} sourced from an already-tie-broken scratch (rather
         * than a still-unconsumed leg cursor) produces byte-identical output to one streamed
         * fresh. {@code length() == 0} implies {@code !sawVIntLength} (a variable-length value
         * with a real length > 0 is the only case that ever calls {@link #writeUnsignedVInt32}
         * with a non-zero length — see {@code SSTableCursorReader#copyCellContents}), so nothing
         * is written for a valueless cell either way.
         */
        void streamTo(DataOutputPlus dest) throws IOException
        {
            if (sawVIntLength)
                dest.writeUnsignedVInt32(length);
            if (length > 0)
                dest.write(buffer, 0, length);
        }

        /** TEST ONLY (M3.3a-ii): flips the first staged byte's low bit — see
         *  {@code CursorReads.TEST_CORRUPT_STREAMED_CELL_VALUE}. Never called outside tests. */
        void corruptFirstByte()
        {
            if (length > 0)
                buffer[0] ^= 0x01;
        }

        /** See {@link #window}: position 0, limit {@code length} over the staged raw value bytes. */
        ByteBuffer valueWindow()
        {
            if (window == null || window.array() != buffer)
                window = ByteBuffer.wrap(buffer);
            window.limit(length).position(0);
            return window;
        }

        /** Promotes the staged bytes to a final value array — called for a group's ONE winner only. */
        byte[] toValueArray()
        {
            return length == 0 ? ByteArrayAccessor.instance.empty() : Arrays.copyOf(buffer, length);
        }

        /** Plain unsigned lexicographic order over the staged raw bytes, exactly
         *  {@code Arrays.compareUnsigned} over the arrays the old code allocated. */
        static int compare(CellValueScratch a, CellValueScratch b)
        {
            return Arrays.compareUnsigned(a.buffer, 0, a.length, b.buffer, 0, b.length);
        }

        @Override
        public void writeUnsignedVInt32(int valueLength)
        {
            // the wire's value-length vint, mirrored by copyCellValue for variable-length types:
            // pre-size only — the vint is NOT part of the raw value bytes being compared
            sawVIntLength = true;
            ensureCapacity(length + valueLength);
        }

        @Override
        public void write(byte[] chunk, int offset, int chunkLength)
        {
            ensureCapacity(length + chunkLength);
            System.arraycopy(chunk, offset, buffer, length, chunkLength);
            length += chunkLength;
        }

        private void ensureCapacity(int size)
        {
            if (buffer.length < size)
                buffer = Arrays.copyOf(buffer, Math.max(size, buffer.length * 2)); // grow-only, amortized
        }

        @Override
        public void write(int b)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(byte[] chunk)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(ByteBuffer chunk)
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
     * M3.3a-ii: the reused {@link CellValueSource} view {@code mergeCellGroup} hands a streaming
     * sink for the current cell group's final winner. Mirrors {@code mergeCellGroup}'s own
     * {@code stagedWinnerValue} invariant (see that method's comments): after the tie-break loop,
     * EITHER the winner's value bytes are already staged in a {@link CellValueScratch} (a tie
     * occurred and this leg won it) OR they are still unconsumed on the winning leg's cursor
     * (never touched, single contributor or an outright — non-tie — resolution) — never both,
     * never neither. Reset immediately before the {@code addCellFromWire} call that receives it;
     * not valid afterward (the same reuse discipline the interface itself documents).
     */
    private final class WinnerCellValueSource implements CellValueSource
    {
        private CellValueScratch scratch;
        private CursorReads.MergeLeg leg;

        void resetToScratch(CellValueScratch scratch)
        {
            this.scratch = scratch;
            this.leg = null;
        }

        void resetToLeg(CursorReads.MergeLeg leg)
        {
            this.scratch = null;
            this.leg = leg;
        }

        @Override
        public boolean hasValue() throws IOException
        {
            // A zero-length staged scratch and "no value at all" are indistinguishable on the
            // wire (see CellValueScratch#streamTo) and are therefore indistinguishable here too —
            // correct either way, since both mean nothing is written.
            return scratch != null ? scratch.length() > 0 : leg.cellHasValue();
        }

        @Override
        public void streamValue(DataOutputPlus dest) throws IOException
        {
            if (scratch != null)
            {
                scratch.streamTo(dest);
                return;
            }
            if (CursorReads.TEST_CORRUPT_STREAMED_CELL_VALUE)
            {
                // TEST ONLY: route the FRESH-LEG case (the actual new M3.3a-ii mechanism — a leg's
                // cursor streamed directly into the wire for the first time ever, as opposed to
                // the tie-break scratch replay above, which reuses machinery FINDING #15 already
                // exercises in production) through a dedicated scratch so the corruption can flip
                // a byte in the value BEFORE it reaches the wire. Proves the byte-comparison
                // harness catches a wrong value introduced specifically by this new streaming
                // path, distinct from the pre-existing TEST_CORRUPT_CELL_TIMESTAMPS/
                // TEST_TRANSCODE_* hooks (which target timestamps and flags, not streamed value
                // bytes).
                testCorruptionScratch.clear();
                leg.stageCellValue(testCorruptionScratch);
                testCorruptionScratch.corruptFirstByte();
                testCorruptionScratch.streamTo(dest);
                return;
            }
            leg.stageCellValue(dest);
        }

        @Override
        public byte[] materialize() throws IOException
        {
            return scratch != null ? scratch.toValueArray() : leg.cellValue();
        }
    }
}
