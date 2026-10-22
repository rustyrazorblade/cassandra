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
import java.util.Arrays;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.CursorCounterContexts;
import org.apache.cassandra.db.compaction.IteratorMergeOrder;
import org.apache.cassandra.db.compaction.PreSortedBubbleInsert;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellLivenessInfo.Resolution;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.OpenRangeDeletions;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.bti.BtiCursorSeekSupport;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

/**
 * The within-partition cursor-level k-way merge across the sstable legs of a single-partition read,
 * adapted from {@code CursorCompactor}'s merge loop.  Reads merge exactly one partition across
 * cursors the call site has already positioned at the query key, so none of compaction's
 * partition-level k-way machinery appears here.  What transfers is the unfiltered-level sort/merge
 * walk, row liveness/deletion reconciliation, the range-tombstone open-marker set, and the
 * cell-level merge with value-compare tie-breaks.  The leaf primitives are shared, not copied:
 * {@link ClusteringComparator#compare(org.apache.cassandra.io.sstable.ClusteringDescriptor,
 * org.apache.cassandra.io.sstable.ClusteringDescriptor)},
 * {@link CellLivenessInfo#resolve}, {@link CursorCompactor#comparePaths} and
 * {@link PreSortedBubbleInsert}.  The differential harness guards the copied loop shape on this side.
 *
 * <b>Compaction behaviors absent here</b>:
 * <ul>
 *   <li><b>No purge, at any level</b>: no partition-deletion, row-deletion/liveness, complex-
 *       deletion, cell, or range-tombstone purge.  Reads drop gcable tombstones only above the
 *       merge, in {@code ReadCommand.withoutPurgeableTombstones}.</li>
 *   <li><b>No expired-TTL-to-tombstone conversion</b>: the iterator read path serves expired cells
 *       as stored, so the merge must too.</li>
 *   <li><b>No strict-liveness row skipping</b>: materialized-view tables are gated out of cursor
 *       reads entirely.</li>
 *   <li><b>No eager corrupted-tombstone validation</b>: validation happens per leg, on in-slice
 *       elements only, via {@code CursorReads.MergeLeg}'s validate hooks, so a corrupted element
 *       outside the queried slice does not fail a read the iterator path would have served.</li>
 *   <li><b>Counter-cell merging</b>: counter tables ARE served (CASSANDRA-20428).  A counter cell
 *       group folds every surviving shard set with {@link CursorCounterContexts} (the byte-exact
 *       primitive shared with cursor compaction), applies the deserialization-time marked-local
 *       clear, honours 7346 tombstone supremacy, and handles the read-only empty-value case
 *       (CASSANDRA-10657) — see {@code mergeCounterCells}.  Counters stay off the transcode
 *       fast path, so a counter read always routes through this merge/materialize path.</li>
 * </ul>
 *
 * <b>New relative to compaction</b>: per-leg {@code ColumnFilter}/{@code DeserializationHelper}
 * filtering happens below reconciliation (compaction reads every column; the iterator read path
 * filters at deserialization), implemented in {@code PendingLeg.ensureParkedAtCell}.  This includes
 * surfacing a complex column's surviving deletion as a deletion-only merge position even when every
 * one of its cells was filtered out per leg.
 *
 * The legs are {@code CursorReads.MergeLeg}s: byte-backed sstable legs
 * ({@code CursorReads.PendingLeg}), memtable legs ({@code NativeMemtableMergeLeg}) and the
 * object-backed adapter of an iterator source ({@code MemtableMergeLeg}) join the same merge, so a
 * memtable+sstable read needs no second, object-level merge above this one.  For a sink that keeps
 * row objects, memtable-won data is emitted through the {@code existingCell()}/{@code
 * consumeExistingRow()} escape hatches as the iterator path's own objects; for a sink that streams
 * cells, a memtable leg gives its cells from the memtable's storage like an sstable leg.  A
 * memtable leg never seeks and is never validated, and participates in the open-marker set as a
 * normal, always-current source.
 *
 * For indexed BTI legs, the eager whole-partition walk is gone: each leg enters the merge already
 * seeked to its row-index floor block for the first slice start
 * ({@code CursorReads.MergeLeg.seekForMerge}), and its open-range-tombstone deletion at the seek
 * point ({@code IndexInfo.openDeletion}, the payload {@code ForwardIndexedReader.setForSlice} seeds
 * per leg on the iterator path) is seeded into {@code openRanges} by this constructor, so cross-leg
 * reconciliation of range tombstones spanning the seek points stays exact.  Per-leg seek points may
 * be misaligned (different floor blocks, or unseeked BIG legs mixed in).  That is safe: every leg's
 * post-seek stream contains all of that leg's elements strictly after the slice start, so any merged
 * element the slicer can emit has every leg's contribution.  Under-reconciled elements exist only
 * before the slice start, where the slicer discards them, and a leg's seeded deletion is genuinely
 * open at every position from its floor block's separator to its in-stream close.
 *
 * The merge reads one slice at a time.  At each later slice ({@link #moveToSlice}) every leg whose
 * floor block for the slice start lies past its current position seeks forward to it and swaps
 * its open deletion in {@code openRanges} for the block's.  Rows at or before the current slice
 * start are skipped without reading their cells.
 *
 * The merge runs one group per {@link #advance} call.  A forward read pulls groups only as its
 * reader asks for rows, so a read that stops early (a limit, a page) never merges the rest of the
 * partition.  {@link #mergeUnfiltereds} pushes groups into a sink until the sink stops wanting
 * more.
 */
final class CursorReadMerger
{
    /**
     * The read-side merge sink: clustering-first events (the merge knows the group's clustering the
     * moment the row group forms), unlike compaction's writer-wire-shaped {@code CursorMergeSink}
     * whose clustering arrives only at {@code writeRowEnd}.  This lets the materializer plug in with
     * {@code newRow()} up front and no staging ({@code CursorReads.MaterializingMergeSink}).
     *
     * Event grammar per partition: ({@code startRow} ({@code addComplexDeletion} |
     * {@code addCell})* {@code endRow} | {@code addRangeTombstoneMarker})*, in clustering order;
     * complex deletions and cells arrive in column/path order within their row.  A row group that
     * merges to nothing emits no events at all, like {@code Row.Merger} returning null.  All
     * arguments are immutable (already copied off any reusable descriptor state).
     * {@code addComplexDeletion} with a live deletion is a column the row keeps with no cells.
     */
    interface MergeSink
    {
        void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion);

        void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion);

        /** The cell may be a reused already-live object from a memtable leg (a {@code Cell<?>} of
         *  any accessor), not only a freshly-built {@code Cell<byte[]>}, like the object merge,
         *  whose merged rows reference winning memtable cell objects directly. */
        void addCell(Cell<?> cell);

        /** Ends the current row (only started rows are ended; a group merging to nothing emits
         *  neither startRow nor endRow, and sinks keep an empty-row drop only as a defensive net). */
        void endRow();

        /** Whole-row escape hatch: emits an already-live row object as the merged row, mirroring
         *  {@code Row.Merger.merge}'s single-version fast path.  Never combined with startRow/endRow
         *  for the same row group. */
        void addRow(Row row);

        /**
         * Whether the sink's row filter rejects {@code row}, the merged row of a group the merge
         * reads as stored, given before the merge reads any of its cells.  When true the sink has
         * done for the row all it does for a rejected row it is given cell by cell, and the merge
         * skips the row.  Default false: the merge gives the row as usual.
         */
        default boolean dropsRowByFilter(Row row)
        {
            return false;
        }

        void addRangeTombstoneMarker(RangeTombstoneMarker marker);

        /**
         * The descriptors the merge stages its slice bounds in, for clusterings of {@code types}.
         * A sink that serves many partitions of one read may return the same ones each time, as
         * one merge uses them at a time.  Default: new ones.
         */
        default SliceBounds sliceBounds(AbstractType<?>[] types)
        {
            return new SliceBounds(types);
        }

        /**
         * Consulted by {@link #mergeUnfiltereds} before each merge group.  {@code false} stops the
         * merge: remaining groups are never sorted, merged, validated or materialized.  Default
         * true.  A read through {@code CursorReads.mergeLegs} ignores this; its iterator calls
         * {@link #advance} only when its reader asks for more.
         */
        default boolean wantsMore()
        {
            return true;
        }

        /**
         * Discards the row started by {@link #startRow}, for a row group a regular-column filter
         * rejected part way through its cells.  Only a sink that can still drop a started row
         * supports this.
         */
        default void abandonRow()
        {
            throw new UnsupportedOperationException(getClass().getSimpleName() + " cannot discard a started row");
        }

        /**
         * Capability flag: true iff this sink wants a resolved sstable-winner cell's value streamed
         * directly from its source ({@link #addCellFromWire}) instead of a fully materialized
         * {@link Cell} object via {@link #addCell}.  Default false: {@code MaterializingMergeSink}
         * and {@code RowLevelFilterProbe} never override it, so {@code mergeCellGroup}'s
         * sstable-winner materialization path for them is unchanged.
         * <p>
         * A capability flag rather than an {@code instanceof} check keeps the read-owned merge core
         * from importing a concrete sink implementation, the same reasoning that governs
         * {@link #wantsMore()}.  It is also cheaper for the sinks that do not stream: no
         * {@link CellValueSource} view is ever constructed on their behalf.
         */
        default boolean wantsWireStreamedCells()
        {
            return false;
        }

        /**
         * The form of {@link #startRow} a sink that streams cells ({@link #wantsWireStreamedCells()})
         * receives: the clustering is the descriptor of the leg the row group takes it from, and the
         * liveness and deletion may be the merge's reusable state.  All three stay valid until
         * {@link #endRow}.  The merge calls it only for rows after the start of the current slice.
         */
        default void startRow(UnfilteredDescriptor clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            throw new UnsupportedOperationException(getClass().getSimpleName() + " takes a built clustering");
        }

        /**
         * Called by {@code CursorReadMerger.mergeCellGroup} for a resolved sstable winner instead of
         * building a {@link Cell} object and calling {@link #addCell(Cell)}, but only when
         * {@link #wantsWireStreamedCells()} is true.  {@code source} exposes wherever the winner's
         * raw wire-form value bytes currently live (still unconsumed on the winning leg's cursor, or
         * already staged in the merge's tie-break scratch), streamable directly into the sink's own
         * destination without an intermediate {@code byte[]}/{@code Cell} object.
         * <p>
         * The default implementation materializes a real {@link Cell} and forwards to
         * {@link #addCell}, for interface completeness.  Only {@code ResponseSink}
         * overrides {@code wantsWireStreamedCells()}, and it overrides this method too, so this
         * default body is never actually invoked.
         */
        default void addCellFromWire(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime,
                                     CellPath path, CellValueSource source) throws IOException
        {
            byte[] value = source.hasValue() ? source.materialize() : ByteArrayAccessor.instance.empty();
            addCell(ByteArrayAccessor.instance.factory().cell(column, timestamp, ttl, localDeletionTime, value, path));
        }
    }

    /**
     * Filter-pushdown probe: the merge core's hook points for row-level (clustering-column and
     * regular-column) filter pushdown and its scan-metrics accounting.  Semantics-free on the merge
     * side, like {@link MergeSink}: the core never sees {@code nowInSec}, purge predicates or
     * operator evaluation.  The probe implementation ({@code CursorReads.RowLevelFilterProbe}) owns
     * all of that.
     *
     * Event grammar per dropped row group (a group {@link #rowGroupMatches} rejected):
     * {@code beginDroppedRow (droppedComplexDeletion | droppedCell)* endDroppedRow}, mirroring the
     * sink grammar of the group the merge would otherwise have emitted, but at metadata level only:
     * no clustering, cell value or path is ever materialized for a dropped group, which is the
     * allocation payoff of the pushdown.  All {@code LivenessInfo}/{@code DeletionTime} arguments are
     * reusable descriptor state, valid only for the duration of the call.
     *
     * The regular-column verdict, unlike the clustering verdict, resolves at winner resolution
     * inside the cell walk ({@code mergeCellGroup}), potentially after some of the row's earlier
     * cells were already merged and emitted.  A row failing there (or failing at row end because a
     * filter column never surfaced a surviving winner) is abandoned mid-flight: the probe discards
     * the partially-built output row, replays the already-emitted content into dropped-row
     * accounting, the merge walks the remaining cell groups through the metadata-only accounting
     * path, and the core closes with {@link #endDroppedRow}.  Cells materialized before a
     * late-resolving failure are bounded wasted allocation, never a correctness gap.
     */
    interface FilterProbe
    {
        /**
         * The clustering-column filter verdict for a just-formed row group, evaluated from the
         * descriptor's clustering wire bytes before any cell work, clustering materialization or
         * escape-hatch emission.  {@code false} means the group provably fails the query's top-level
         * filter and must not be produced; the merge then drives the accounting walk instead.
         */
        boolean rowGroupMatches(UnfilteredDescriptor descriptor);

        /**
         * Opens a dropped row group's accounting.
         *
         * @param clusteringSource the leg whose descriptor holds the group's clustering (the
         *                         group's first-sorted leg); the probe may materialize the
         *                         clustering from it lazily on its abort path only
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
         *  {@code addCell} would have emitted), identified by the winner's liveness metadata only.
         *  On a full-metadata tie every contender shares the same liveness classification, so winner
         *  identity is irrelevant to accounting. */
        void droppedCell(CellLivenessInfo winnerLiveness);

        /** Closes the dropped group's accounting (classification happens here, where the full
         *  row-level picture is known).  Also closes an abandoned row's accounting (see
         *  {@link #abandonRowGroup}). */
        void endDroppedRow();

        // ---- regular-column (value-window) verdicts ----

        /** Whether {@code column} carries a gate-approved regular-column filter expression whose
         *  merged winner must be evaluated at resolution time ({@code mergeCellGroup}).  Constant
         *  false when the query has no regular-column expressions. */
        boolean filtersRegularColumn(ColumnMetadata column);

        /** Cell liveness of a resolved winner at the query's {@code nowInSec} ({@code Cell.isLive}
         *  semantics; the probe owns the formula so the core stays nowInSec-free).  A dead winner
         *  fails its expression without its value ever being staged.  Reusable argument. */
        boolean cellIsLive(CellLivenessInfo winnerLiveness);

        /**
         * The regular-column filter verdict for a live resolved winner of {@code column}: every
         * gate-approved expression on the column evaluates as
         * {@code operator.isSatisfiedBy(column.type, valueWindow, value)}, what
         * {@code SimpleExpression.isSatisfiedBy} reduces to for a live simple cell.  A passing
         * verdict marks the column satisfied for the current row group's AND-semantics check
         * ({@link #rowRegularFiltersSatisfied}).
         *
         * {@code valueWindow} is a view over reusable storage (the merge's tie-break value scratch,
         * or the memtable winner's own cell buffer), handed only to
         * {@code Operator.isSatisfiedBy(AbstractType, ByteBuffer, ByteBuffer)}, which retains
         * nothing.  Implementations must not retain the window past the call.
         */
        boolean regularCellMatches(ColumnMetadata column, ByteBuffer valueWindow);

        /** AND-semantics closure for the current row group: true iff every regular-column filter
         *  expression's column surfaced a surviving, live, value-passing winner in this row.  A
         *  filter column with no surviving merged cell fails the top filter too, so a row ending
         *  unsatisfied must be abandoned even when every resolved winner passed.  Constant true when
         *  the query has no regular-column expressions. */
        boolean rowRegularFiltersSatisfied();

        /**
         * Abandons the row group being emitted because a regular-column expression failed, at winner
         * resolution (a shadowed, dead or value-failing winner) or at row end (a filter column that
         * never surfaced a winner).  The probe discards the partially-built output row, opens the
         * dropped-row accounting and replays the already-emitted content into it, so the
         * classification matches a group rejected before any cell work.  The merge then routes the
         * remaining cell groups through the metadata-only accounting walk and closes with
         * {@link #endDroppedRow}.
         *
         * @param clusteringSource      the leg whose descriptor holds the group's clustering (still
         *                              loaded; the abort path may materialize from it)
         * @param onEmittedSurface      same semantics as {@link #beginDroppedRow}'s flag
         * @param failedWinnerLiveness  the failing winner's liveness when that winner would have been
         *                              part of the merged row (a dead or value-failing winner), or
         *                              null when it would not (a shadowed winner, or a row-end
         *                              absence failure); reusable
         */
        void abandonRowGroup(CursorReads.MergeLeg clusteringSource, boolean onEmittedSurface,
                             CellLivenessInfo failedWinnerLiveness);

        /** The whole-row escape hatch's regular-column verdict: the single-contributor merged row is
         *  this already-live object, so the real {@code Expression.isSatisfiedBy} evaluates it
         *  directly.  Constant true when the query has no regular-column expressions.  Never called
         *  for a group whose clustering verdict already failed. */
        boolean existingRowMatches(Row row);

        /** Dropped-row accounting for an escape-hatch row {@link #existingRowMatches} rejected:
         *  the full begin/replay/end cycle over the live row object's own content (nothing was
         *  emitted for it, so there is no partial output to discard). */
        void abandonExistingRow(Row row, boolean onEmittedSurface);
    }

    private final CursorReads.MergeLeg[] legs;
    /** The legs in the order the iterator path gives their sources to its merge. */
    private final CursorReads.MergeLeg[] sourceOrder;
    /** Scratch: a row group's legs in the order the row merge gave them, kept during the cell walk. */
    private final CursorReads.MergeLeg[] groupOrder;
    /**
     * For a merge of legs that are already sliced, the order of the iterator path's merge, which
     * gives each group's legs; null otherwise.  The legs of each group go to the front of
     * {@link #legs} in that order.
     */
    private final IteratorMergeOrder<OrderedLeg> order;
    private final OrderedLeg[] orderedGroup;
    // The cell walk of a group of several legs (walkCellGroupsAsRowMerger): the column merge and
    // the cell merge of a complex column, the group's legs in source order, the legs of the
    // current column, and that complex column.  foldLegs/foldCount are the legs a complex
    // deletion folds over.
    private final IteratorMergeOrder<CursorReads.MergeLeg> columnOrder;
    private final IteratorMergeOrder<CursorReads.MergeLeg> cellOrder;
    /**
     * The only leg of a merge of one leg with no lower bound, read in order without {@link #order}:
     * each group is the leg's next unfiltered, as {@code MergeIterator} gives a lone source.  Null
     * otherwise.
     */
    private final OrderedLeg oneLeg;
    private final OrderSources oneLegSources;
    private boolean oneLegFinished;
    private final CursorReads.MergeLeg[] rowLegs;
    private final CursorReads.MergeLeg[] columnLegs;
    private ColumnMetadata complexColumn;
    private CursorReads.MergeLeg[] foldLegs;
    private int foldCount;

    /** A leg as {@link #order} reads it, with the lower bound the iterator path's iterator gives first. */
    private static final class OrderedLeg
    {
        final CursorReads.MergeLeg leg;
        final UnfilteredDescriptor lowerBound;

        OrderedLeg(CursorReads.MergeLeg leg)
        {
            this.leg = leg;
            this.lowerBound = leg.lowerBoundForMerge();
        }
    }
    /** The only leg of a single-leg read, or null for a merge.  A single leg is read as stored,
     *  see {@link #advanceSingle}. */
    private final CursorReads.PendingLeg singleLeg;
    /**
     * Whether the read has one source, which the iterator path reads without a merge: a single
     * sstable leg ({@link #singleLeg}), or one memtable (or other object source) merged as the only
     * leg.  Its row, complex and cell data are kept as the source gives them: no deletion shadows
     * them here, as {@code UnfilteredRowIterators.merge} returns a lone iterator unchanged.
     */
    private final boolean readAsStored;
    private final boolean[] equalsNext;
    private final ClusteringComparator comparator;
    private final MergeSink sink;
    /** {@link MergeSink#wantsWireStreamedCells()}, read once: a sink's answer never changes. */
    private final boolean streamCells;
    /** Clustering filter pushdown, or null when disengaged. */
    private final FilterProbe filterProbe;

    /** supersedes-max of every leg's partition-level deletion (including Slices.NONE legs). */
    private final DeletionTime mergedPartitionDeletion;
    /** The deletion currently shadowing merged rows: the partition deletion, or the open range
     *  deletion when one supersedes it — compaction's activeDeletion, verbatim. */
    private DeletionTime activeDeletion;

    // Range-tombstone open-marker set, shared with CursorCompactor.
    private final OpenRangeDeletions openRanges = new OpenRangeDeletions();

    // current output complex column state (reset per row), mirroring mergeCells' column-entry fold
    private ColumnMetadata currentComplexColumn;
    private final DeletionTime.ReusableDeletionTime mergedComplexDeletion = DeletionTime.ReusableDeletionTime.live();
    // set by foldComplexDeletion when a newly entered complex column's folded deletion survives,
    // which each cell path reports to its own consumer (the sink, or the probe)
    private boolean complexDeletionNeedsReport;

    // Lazy row-start state (reset per row group): a group whose merged shell (liveness + row
    // deletion) is empty starts its output row only when the first surviving cell/complex
    // deletion arrives, so a group that merges to NOTHING — fully shadowed by other sources'
    // deletions, or every cell filtered away per leg — materializes no clustering and never
    // touches the sink's row builder. See mergeRowGroup/ensureRowStarted.
    private CursorReads.MergeLeg rowClusteringLeg;
    private Clustering<?> rowClustering;
    private boolean rowStarted;

    // The row group's merged shell, left here by prepareRowGroup rather than converted, because
    // the emitting and accounting paths need it in different forms: the sink takes stable copies,
    // the probe takes the reusable originals. groupValidateInSlice carries the per-group
    // validation verdict into the cell walk.
    private LivenessInfo groupMergedInfo;
    private DeletionTime groupMergedDeletion;
    private boolean groupDeletionSurvives;
    private boolean groupValidateInSlice;

    // Regular-column pushdown state (reset per row group; meaningful only with a probe):
    // rowAbandonedByFilter flips when a regular-column expression fails, switching the rest of
    // the row's cell walk from the emitting merge to the metadata-only accounting walk.  Every
    // row group that reaches the probe is in the slice (rows at or before the slice start are
    // skipped first), so the probe's onEmittedSurface argument is always true.
    private boolean rowAbandonedByFilter;

    // Validation runs per leg, in-slice only. A slice read has one slice; a names read has one
    // point slice per requested clustering. The merge reads one slice at a time: sliceIndex is the
    // current slice, and moveToSlice moves it forward only.
    private final boolean validationEnabled;
    private final Slices slices;
    private int sliceIndex;

    // Set by prepare() and moveToSlice().  sliceEndStop and sliceStartStop are the current slice's
    // bounds in the legs' wire form (see SliceBoundDescriptor), null for BOTTOM and for legs that
    // are already sliced; the two descriptors behind them are reused for every slice.
    // sliceDone turns true once the current slice has nothing left; finished turns true once
    // every leg is exhausted.
    private SliceBoundDescriptor sliceStartDescriptor;
    private SliceBoundDescriptor sliceEndDescriptor;
    private ClusteringDescriptor sliceEndStop;
    private ClusteringDescriptor sliceStartStop;
    /** The current slice ends at TOP.  No row is at or past TOP, so only markers are compared
     *  with {@link #sliceEndStop}. */
    private boolean sliceEndsAtTop;
    /** The single leg has read an unfiltered past the current slice's start.  The leg is in
     *  clustering order, so nothing after it is at or before the start. */
    private boolean pastSliceStart;
    private boolean prepared;
    private boolean sliceDone;
    private boolean finished;
    /** The open range deletion right after the last {@link #moveToSlice} seek, or null. */
    private DeletionTime openDeletionAtSeek;

    /** Merges in reverse clustering order, over legs that are already sliced; see {@link #forReversedMerge}. */
    private final boolean reversed;
    /** The legs give only in-slice unfiltereds, so this merge applies no slice bounds. */
    private final boolean legsAlreadySliced;
    private final PreSortedBubbleInsert<CursorReads.MergeLeg> cellSort =
        new PreSortedBubbleInsert<>(this::compareByCell);

    // Tie-break value staging (compaction's tempCellBuffer1/tempCellBuffer2 discipline): scratch1
    // holds the running winner's staged bytes, scratch2 receives each challenger's; on a
    // challenger win the two SWAP, so a loser's bytes are never promoted to a real array — they
    // are simply overwritten by the next tie. Non-final because of that swap.
    // created at the first tie: most reads never compare two cell values
    private CellValueScratch tieValueScratch1;
    private CellValueScratch tieValueScratch2;

    // The single reused CellValueSource view handed to a streaming sink
    // (MergeSink#addCellFromWire) — never allocated per cell, only ever constructed (and only
    // ever touched) when sink.wantsWireStreamedCells() is true, so the three non-streaming sinks
    // never pay for it. TEST_CORRUPT_STREAMED_CELL_VALUE routes the fresh-leg case through this
    // dedicated scratch instead of straight to the destination — see WinnerCellValueSource.
    private final WinnerCellValueSource winnerCellValueSource = new WinnerCellValueSource();
    private CellValueScratch testCorruptionScratch;

    // Counter merge state (see mergeCounterCells): counterContexts holds the merge and
    // marked-local clear shared with compaction; created at the first counter cell group.
    private CursorCounterContexts counterContexts;
    private CellValueScratch counterFoldScratch;
    private CellValueScratch counterInputScratch;

    CursorReadMerger(CursorReads.MergeLeg[] legs,
                     TableMetadata metadata,
                     DeletionTime mergedPartitionDeletion,
                     Slices slices,
                     MergeSink sink,
                     FilterProbe filterProbe)
    {
        this(legs, null, metadata, mergedPartitionDeletion, slices, sink, filterProbe, false, false);
    }

    /**
     * The merge core for a reverse read of several sources.  Each leg gives its own unfiltereds in
     * reverse clustering order, already sliced and with its own slice-bound markers, like the
     * iterator path's per-sstable reverse iterators that {@code UnfilteredRowIterators.merge}
     * merges.  So this merge applies no slice bounds and never seeks.
     */
    static CursorReadMerger forReversedMerge(CursorReads.MergeLeg[] legs, TableMetadata metadata,
                                             DeletionTime mergedPartitionDeletion, MergeSink sink)
    {
        return new CursorReadMerger(legs, null, metadata, mergedPartitionDeletion, Slices.ALL, sink, null, true, true);
    }

    /** Merges in clustering order, over legs that are already sliced ({@code CursorReads.ForwardMergeLeg}). */
    static CursorReadMerger forSlicedMerge(CursorReads.MergeLeg[] legs, TableMetadata metadata,
                                           DeletionTime mergedPartitionDeletion, MergeSink sink, FilterProbe filterProbe)
    {
        return new CursorReadMerger(legs, null, metadata, mergedPartitionDeletion, Slices.ALL, sink, filterProbe, false, true);
    }

    /**
     * The merge core for a read with exactly one sstable leg and no memtable data.  The iterator
     * path serves that shape from the one sstable iterator without any merge, so this reads the
     * leg as stored: no shadowing, no marker reconciliation.  The caller has already seeked the
     * leg.  A sink that builds rows gets each one whole, and its caller validates it, like the
     * sstable iterator; for a sink that streams cells, this validates the leg as it reads.
     */
    static CursorReadMerger forSingleLeg(CursorReads.PendingLeg leg, MergeSink sink)
    {
        return new CursorReadMerger(new CursorReads.MergeLeg[]{ leg }, leg, leg.metadata, leg.partitionLevelDeletion(),
                                    leg.legSlices(), sink, null, false, false);
    }

    /** {@link #forSingleLeg} with no slice bounds, for a reverse read that has picked the in-slice
     *  unfiltereds itself and seeks the leg to each one. */
    static CursorReadMerger forSingleLegAlreadySliced(CursorReads.PendingLeg leg, MergeSink sink)
    {
        return new CursorReadMerger(new CursorReads.MergeLeg[]{ leg }, leg, leg.metadata, leg.partitionLevelDeletion(),
                                    Slices.ALL, sink, null, false, true);
    }

    private CursorReadMerger(CursorReads.MergeLeg[] legs,
                             CursorReads.PendingLeg singleLeg,
                             TableMetadata metadata,
                             DeletionTime mergedPartitionDeletion,
                             Slices slices,
                             MergeSink sink,
                             FilterProbe filterProbe,
                             boolean reversed,
                             boolean legsAlreadySliced)
    {
        this.legs = legs;
        this.sourceOrder = legs.clone();
        this.groupOrder = new CursorReads.MergeLeg[legs.length];
        this.rowLegs = new CursorReads.MergeLeg[legs.length];
        this.columnLegs = new CursorReads.MergeLeg[legs.length];
        // the column and cell merges of a row group are of several legs only
        this.columnOrder = legs.length > 1 ? new IteratorMergeOrder<>(legs.length, new ColumnSources()) : null;
        this.cellOrder = legs.length > 1 ? new IteratorMergeOrder<>(legs.length, new CellSources()) : null;
        this.reversed = reversed;
        OrderedLeg lone = null;
        if (singleLeg == null)
        {
            OrderedLeg[] ordered = new OrderedLeg[legs.length];
            boolean[] lowerBounds = new boolean[legs.length];
            for (int i = 0; i < legs.length; i++)
            {
                ordered[i] = new OrderedLeg(legs[i]);
                lowerBounds[i] = ordered[i].lowerBound != null;
            }
            if (legs.length == 1 && !lowerBounds[0])
            {
                lone = ordered[0];
                this.order = null;
            }
            else
            {
                this.order = new IteratorMergeOrder<>(ordered, lowerBounds, new OrderSources());
            }
            this.orderedGroup = new OrderedLeg[legs.length];
        }
        else
        {
            this.order = null;
            this.orderedGroup = null;
        }
        this.oneLeg = lone;
        this.oneLegSources = lone == null ? null : new OrderSources();
        this.legsAlreadySliced = legsAlreadySliced;
        this.singleLeg = singleLeg;
        this.readAsStored = singleLeg != null || (legs.length == 1 && legs[0].sstableOrNull() == null);
        this.equalsNext = new boolean[legs.length];
        this.comparator = metadata.comparator;
        this.mergedPartitionDeletion = mergedPartitionDeletion;
        this.activeDeletion = mergedPartitionDeletion;
        this.sink = sink;
        this.streamCells = sink.wantsWireStreamedCells();
        this.filterProbe = filterProbe;
        this.slices = slices;
        this.validationEnabled = DatabaseDescriptor.getCorruptedTombstoneStrategy() != Config.CorruptedTombstoneStrategy.disabled;
    }

    /**
     * Merges groups into the sink until it stops wanting more or the partition ends.  The
     * adapted shape of CursorCompactor's mergePartitions unfiltered loop (static rows and the
     * partition deletion are reconciled by the caller; every leg enters positioned at its first
     * unfiltered or partition end).  Serves one slice only: the sinks this drives track the open
     * range deletion from the stream, which a seek to a later slice would hide from them.
     */
    void mergeUnfiltereds() throws IOException
    {
        assert slices.size() == 1 : "mergeUnfiltereds reads one slice, not " + slices.size();
        prepare();
        while (sink.wantsMore())
        {
            if (!advance())
                return;
        }
    }

    /**
     * Stages the first slice's bounds.  Called once, before the first {@link #advance}.  The
     * caller has already seeked the legs for the first slice.
     */
    void prepare() throws IOException
    {
        if (prepared)
            return;
        prepared = true;
        // The bounds are staged in the legs' wire form, so the per-group checks are the shared
        // descriptor-level comparator with no clustering materialization.  They are built with
        // the LEGS' clustering types, since ClusteringComparator.compare(ClusteringDescriptor,
        // ClusteringDescriptor) decodes both buffers with the first argument's (a leg's) types.
        SliceBounds bounds = sink.sliceBounds(legs[0].unfiltered().clusteringTypes());
        sliceStartDescriptor = bounds.start;
        sliceEndDescriptor = bounds.end;
        stageSlice(0);
    }

    /**
     * Stages slice {@code index}'s bounds.  Slice end-stop: an element at or past the slice end is
     * never emitted, never validated and never open-marker tracked downstream.  The iterator
     * path's ForwardReader stops at compareNextTo(end) >= 0 without deserializing past it, so the
     * merge stops at the FIRST group there.  Slice start: a row at or before it is skipped without
     * being merged, like AbstractSSTableIterator.handlePreSliceData.
     */
    private void stageSlice(int index)
    {
        sliceIndex = index;
        Slice slice = slices.get(index);
        if (legsAlreadySliced)
        {
            sliceStartStop = null;
            sliceEndStop = null;
            sliceEndsAtTop = false;
            pastSliceStart = true;
            return;
        }
        sliceStartStop = slice.start().isBottom() ? null : sliceStartDescriptor.load(slice.start());
        // TOP is staged too: a range deletion that runs to the end of the partition closes at a
        // bound equal to TOP, and the iterator path stops there
        sliceEndStop = sliceEndDescriptor.load(slice.end());
        sliceEndsAtTop = slice.end().isTop();
        pastSliceStart = sliceStartStop == null;
    }

    /** Whether {@code current} is at or past the current slice's end. */
    private boolean isAtOrPastSliceEnd(UnfilteredDescriptor current)
    {
        if (sliceEndStop == null)
            return false;
        if (sliceEndsAtTop && current.clusteringKind() == ClusteringPrefix.Kind.CLUSTERING)
            return false;
        return ClusteringComparator.compare(current, sliceEndStop) >= 0;
    }

    /**
     * Merges exactly one group of the current slice: a row group yields at most one row, a marker
     * group at most one marker, either possibly none.  Returns false once the current slice has
     * no group left: every leg is exhausted, or the next group is at or past the slice end.
     * Deferred legs open here, only when a group reaches them.
     */
    boolean advance() throws IOException
    {
        assert prepared : "prepare() must run before advance()";
        if (finished || sliceDone)
            return false;
        boolean advanced = singleLeg != null ? advanceSingle() : advanceInOrder();
        if (!advanced)
            sliceDone = true;
        return advanced;
    }

    /**
     * Moves the read of a single leg to slice {@code index}, which must come after the current
     * one, once the current slice has nothing left.  A BTI leg whose row-index floor block for the
     * new slice start lies past its current position seeks forward to it, like the iterator path's
     * ForwardIndexedReader.setForSlice.  A merge reads legs that are already sliced.
     *
     * @return true when a leg seeked; {@link #openDeletionAtSeek} then holds the open range
     *         deletion at the new read position, which replaces the one the reader has tracked
     */
    boolean moveToSlice(int index) throws IOException
    {
        assert prepared && singleLeg != null && index > sliceIndex && index < slices.size() : "slice " + index + " after " + sliceIndex;
        stageSlice(index);
        sliceDone = false;
        if (finished || sliceStartStop == null)
            return false;
        return seekSingleLeg();
    }

    /** See {@link #moveToSlice}: the open range deletion at the position the seek moved to. */
    DeletionTime openDeletionAtSeek()
    {
        return openDeletionAtSeek;
    }

    private boolean seekSingleLeg() throws IOException
    {
        BtiCursorSeekSupport.SeekPoint point = singleLeg.seekPointFor(slices.get(sliceIndex).start());
        if (point == null)
            return false;
        singleLeg.seekTo(point);
        // a single leg is read as stored: the reader's open deletion is the block's, as on the
        // iterator path, where setForSlice replaces openMarker with indexInfo.openDeletion
        openDeletionAtSeek = point.openMarker == null ? null : CursorReads.copyOf(point.openMarker);
        return true;
    }

    /**
     * One leg, read as stored: each row and marker goes to the sink as the sstable holds it,
     * exactly what the iterator path's single sstable iterator returns.  The leg's own partition
     * and range deletions do not shadow its rows here, because that path never merges them.  A row
     * at or before the slice start is skipped without being read, like handlePreSliceData; a
     * marker there still goes to the sink, whose reader tracks the open range deletion from it.
     */
    private boolean advanceSingle() throws IOException
    {
        int state = singleLeg.cursorState();
        if (state == ROW_START && !pastSliceStart)
            state = singleLeg.skipRowsAtOrBefore(sliceStartStop);
        if (isState(state, ROW_START | TOMBSTONE_START))
            singleLeg.readUnfilteredHeader();
        else if (isState(state, PARTITION_END | DONE))
        {
            finished = true;
            return false;
        }
        // otherwise the header is already loaded: the previous call stopped at a slice end on it
        UnfilteredDescriptor current = singleLeg.unfiltered();
        boolean isRow = current.clusteringKind() == ClusteringPrefix.Kind.CLUSTERING;
        if (!pastSliceStart)
        {
            // a row at or before the slice start is before its end too, so it needs one comparison
            if (!isAtOrBeforeSliceStart(current))
                pastSliceStart = true;
            else if (isRow)
            {
                singleLeg.skipRow();
                singleLeg.continueReading();
                return true;
            }
        }
        if (isAtOrPastSliceEnd(current))
            return false;
        if (streamCells)
        {
            // A streaming sink builds no row object, so this read owns the leg's validation, as the
            // sstable iterator does for each in-slice unfiltered.
            if (isRow)
            {
                // the row's cells go to the sink as stored: prepareRowGroup and foldComplexDeletion
                // apply no deletion to a single leg
                streamStoredRow();
                if (singleLeg.cursorState() == UNFILTERED_END)
                    singleLeg.continueReading();
                return true;
            }
            if (validationEnabled && pastSliceStart)
                singleLeg.validateMarkerHeader();
        }
        Unfiltered unfiltered = singleLeg.readStoredUnfiltered();
        if (unfiltered.isRow())
            sink.addRow((Row) unfiltered);
        else
            sink.addRangeTombstoneMarker((RangeTombstoneMarker) unfiltered);
        return true;
    }

    /**
     * Merges the next group of a merge of legs that are already sliced, with the legs in the order
     * the iterator path's merge gives them ({@link #order}).
     */
    private boolean advanceInOrder() throws IOException
    {
        int mergeLimit = oneLeg != null ? nextOfOneLeg() : order.nextWithLowerBounds(orderedGroup);
        if (mergeLimit == 0)
        {
            finished = true;
            return false;
        }
        for (int i = 0; i < mergeLimit; i++)
            legs[i] = orderedGroup[i].leg;
        // the cell sort stops at the end of the group
        equalsNext[mergeLimit - 1] = false;
        UnfilteredDescriptor minimum = legs[0].unfiltered();
        if (minimum.clusteringKind() == ClusteringPrefix.Kind.CLUSTERING)
        {
            if (filterProbe != null && !filterProbe.rowGroupMatches(minimum))
                accountDroppedRowGroup(mergeLimit);
            else
                mergeRowGroup(mergeLimit);
        }
        else
        {
            mergeMarkerGroup(mergeLimit);
            activeDeletion = openRanges.active() == DeletionTime.LIVE ? mergedPartitionDeletion : openRanges.active();
        }
        continueReadingAfterMerge(mergeLimit);
        return true;
    }

    /** {@code IteratorMergeOrder.nextWithLowerBounds} for {@link #oneLeg}: the leg advances, as the
     *  order advances a source after each group, and is the group while it has an unfiltered. */
    private int nextOfOneLeg() throws IOException
    {
        if (oneLegFinished)
            return 0;
        if (!oneLegSources.advance(oneLeg, false))
        {
            oneLegFinished = true;
            return 0;
        }
        orderedGroup[0] = oneLeg;
        return 1;
    }

    /** What {@link #order} reads from the legs. */
    private final class OrderSources implements IteratorMergeOrder.LowerBoundSources<OrderedLeg>
    {
        public int compare(OrderedLeg a, boolean aIsLowerBound, OrderedLeg b, boolean bIsLowerBound)
        {
            int cmp = ClusteringComparator.compare(aIsLowerBound ? a.lowerBound : a.leg.unfiltered(),
                                                   bIsLowerBound ? b.lowerBound : b.leg.unfiltered());
            return reversed ? -cmp : cmp;
        }

        public boolean advance(OrderedLeg source, boolean afterLowerBound) throws IOException
        {
            CursorReads.MergeLeg leg = source.leg;
            // the iterator path's lazy iterator opens its sstable at its first unfiltered
            if (leg.isDeferred())
                leg.openForMerge(slices.get(0).start());
            while (true)
            {
                if (!leg.moveToNextUnfiltered())
                    return false;
                if (!holdsNothingFetched(leg))
                    return true;
                if (leg.cursorState() == UNFILTERED_END)
                    leg.continueReading();
            }
        }
    }

    /**
     * Whether the leg is at an sstable row that holds nothing the read fetches: no primary key
     * liveness, no row deletion, and no cell or complex deletion of a fetched column.  The iterator
     * path's sstable iterator reads such a row and drops it as empty
     * ({@code AbstractSSTableIterator.ForwardReader.computeNext}), so it is no version of that row
     * in {@code Row.Merger}.  A row that has something stays parked at its first cell position.
     */
    private boolean holdsNothingFetched(CursorReads.MergeLeg leg) throws IOException
    {
        if (leg.sstableOrNull() == null || leg.cursorState() != CELL_HEADER_START)
            return false;
        UnfilteredDescriptor row = leg.unfiltered();
        if (row.clusteringKind() != ClusteringPrefix.Kind.CLUSTERING || !row.livenessInfo().isEmpty() || !row.deletionTime().isLive())
            return false;
        leg.ensureParkedAtCell(validationEnabled);
        return !leg.parkedAtCellPosition();
    }

    /** Whether {@code position} is at or before the current slice's start (non-strict, like
     *  handlePreSliceData).  Elements after it and before the slice end are in the slice. */
    private boolean isAtOrBeforeSliceStart(ClusteringDescriptor position)
    {
        return sliceStartStop != null && ClusteringComparator.compare(position, sliceStartStop) <= 0;
    }

    // ---------------------------------------------------------------- sort and merge limits
    // (adapted from compaction's prepareAndSortCellsForMerge; the insert sort itself is
    //  compaction's own PreSortedBubbleInsert, shared not copied)

    private int prepareAndSortCellsForMerge(int rowMergeLimit, int prevCellMergeLimit)
    {
        // one leg is in order: the sort would move nothing
        if (rowMergeLimit == 1)
            return legs[0].parkedAtCellPosition() ? 1 : 0;
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
        // Whole-row escape hatch: a single-contributor group under a live active deletion is
        // exactly Row.Merger.merge's fast path ("If for this clustering we have only one row
        // version and have no activeDeletion (i.e. nothing to filter out), then we can just return
        // that single row", Row.java) — when that only contributor is an object-backed memtable
        // leg, the merged row IS the leg's already-live row object, emitted with zero
        // decomposition, zero rebuild and zero allocation. Byte-backed legs return null and take
        // the general path unchanged. Validation is irrelevant here by construction: the iterator
        // path never applies corrupted-tombstone validation to memtable data.
        // TEST_FORCE_MEMTABLE_ROW_REUSE breaks the isLive() condition so the
        // differential harness can prove it catches a wrong reuse (shadowed data resurrecting).
        if (rowMergeLimit == 1 && (activeDeletion.isLive() || readAsStored || CursorReads.TEST_FORCE_MEMTABLE_ROW_REUSE))
        {
            Row existing = legs[0].consumeExistingRow();
            if (existing != null)
            {
                // The escape-hatch row is already a live object, so its regular-column
                // verdict uses the real Expression.isSatisfiedBy (not the value-window form);
                // clustering expressions already passed at group formation (rowGroupMatches runs
                // before mergeRowGroup). A failing row is never emitted — the probe runs the full
                // dropped-row accounting cycle over the object's own content instead.
                if (filterProbe != null && !filterProbe.existingRowMatches(existing))
                {
                    filterProbe.abandonExistingRow(existing, true);
                    return;
                }
                sink.addRow(existing);
                CursorReads.countMemtableRowReuse();
                return;
            }
        }
        // A row read as stored that a memtable leg holds unchanged is the merged row: the sink's
        // row filter judges it as an object, and a row it rejects is skipped before its cells are read
        if (rowReadAsStored(rowMergeLimit))
        {
            Row stored = legs[0].unchangedRow();
            if (stored != null && sink.dropsRowByFilter(stored))
            {
                legs[0].skipRow();
                CursorReads.countMemtableRowFilteredBeforeCells();
                return;
            }
        }

        DeletionTime rowActiveDeletion = prepareRowGroup(rowMergeLimit);
        // liveness shadowed by the active deletion is dropped.  A sink that builds rows keeps what it
        // is handed, so it gets stable copies; a streaming sink is done with them at endRow.
        DeletionTime rowDeletionOut;
        LivenessInfo livenessOut;
        boolean livenessSurvives = !groupMergedInfo.isEmpty() && !rowActiveDeletion.deletes(groupMergedInfo);
        if (streamCells)
        {
            rowDeletionOut = groupDeletionSurvives ? groupMergedDeletion : DeletionTime.LIVE;
            livenessOut = livenessSurvives ? groupMergedInfo : LivenessInfo.EMPTY;
        }
        else
        {
            rowDeletionOut = groupDeletionSurvives ? CursorReads.copyOf(groupMergedDeletion) : DeletionTime.LIVE;
            livenessOut = livenessSurvives ? LivenessInfo.withExpirationTime(groupMergedInfo.timestamp(), groupMergedInfo.ttl(),
                                                                             groupMergedInfo.localExpirationTime())
                                           : LivenessInfo.EMPTY;
        }

        // a surviving shell makes the merged row non-empty no matter what its cells do: start it
        // now; an empty shell defers to the first surviving cell/complex deletion (ensureRowStarted)
        // so a group merging to nothing emits nothing and allocates nothing
        if (!livenessOut.isEmpty() || !rowDeletionOut.isLive())
            startRow(livenessOut, rowDeletionOut);

        rowAbandonedByFilter = false;
        walkCellGroups(rowMergeLimit, rowActiveDeletion, false);
        if (rowAbandonedByFilter)
        {
            // mid-walk abandonment: the probe opened the dropped accounting at the failure point
            // and every remaining winner streamed into it above; classification closes here
            rowAbandonedByFilter = false;
            filterProbe.endDroppedRow();
        }
        else if (rowStarted && filterProbe != null && !filterProbe.rowRegularFiltersSatisfied())
        {
            // Row-end absence failure: a filter column never surfaced a surviving winner in
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

    /**
     * {@link #mergeRowGroup} for a row of {@link #singleLeg}, read as stored, for a sink that
     * streams cells.  It does what that method does for a group of one leg read as stored with no
     * filter probe: no deletion shadows the row or its cells, and each cell goes to the sink as
     * the leg holds it.  The walk calls the leg as its own class rather than as a
     * {@code MergeLeg}, so each call goes straight to it.
     */
    private void streamStoredRow() throws IOException
    {
        CursorReads.PendingLeg leg = singleLeg;
        DeletionTime rowActiveDeletion = prepareRowGroup(1);
        boolean livenessSurvives = !groupMergedInfo.isEmpty() && !rowActiveDeletion.deletes(groupMergedInfo);
        DeletionTime rowDeletionOut = groupDeletionSurvives ? groupMergedDeletion : DeletionTime.LIVE;
        LivenessInfo livenessOut = livenessSurvives ? groupMergedInfo : LivenessInfo.EMPTY;
        if (!livenessOut.isEmpty() || !rowDeletionOut.isLive())
            startRow(livenessOut, rowDeletionOut);

        rowAbandonedByFilter = false;
        currentComplexColumn = null;
        foldLegs = legs;
        foldCount = 1;
        for (;;)
        {
            if (leg.needsCellAdvance())
                leg.ensureParkedAtCell(groupValidateInSlice);
            if (!leg.parkedAtCellPosition())
                break;
            streamStoredCell(leg, rowActiveDeletion);
            leg.advancePastCellPosition();
        }
        if (rowStarted)
            sink.endRow();
    }

    /** {@link #mergeCellGroup} for the cell {@code leg} is parked at, in {@link #streamStoredRow}. */
    private void streamStoredCell(CursorReads.PendingLeg leg, DeletionTime rowActiveDeletion) throws IOException
    {
        ColumnMetadata column = leg.cellColumn();
        if (column.type.isCounter())
        {
            mergeCounterCells(1, rowActiveDeletion);
            return;
        }
        DeletionTime effectiveDeletion = rowActiveDeletion;
        if (column.isComplex())
        {
            effectiveDeletion = foldComplexDeletion(column, 1, rowActiveDeletion);
            if (complexDeletionNeedsReport)
            {
                ensureRowStarted();
                sink.addComplexDeletion(column, CursorReads.copyOf(mergedComplexDeletion));
            }
            if (!leg.cellProduced())
            {
                // see mergeCellGroup: a row read as stored keeps a column with a live deletion and no cells
                if (leg.keepsColumnWithNoCells())
                {
                    ensureRowStarted();
                    sink.addComplexDeletion(column, DeletionTime.LIVE);
                }
                return;
            }
        }
        CellLivenessInfo liveness = leg.cellLiveness();
        long timestamp = liveness.timestamp();
        if (effectiveDeletion.deletesCellAt(timestamp))
        {
            leg.discardCellValue();
            return;
        }
        int ttl = liveness.ttl();
        long localDeletionTime = liveness.localDeletionTime();
        CellPath path = leg.cellPath();
        if (CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS)
            timestamp += 1;
        ensureRowStarted();
        winnerCellValueSource.resetToStoredLeg(leg);
        sink.addCellFromWire(column, timestamp, ttl, localDeletionTime, path, winnerCellValueSource);
        // see mergeCellGroup: a value the sink did not read must be skipped before the leg moves on
        leg.discardCellValue();
    }

    /**
     * Shared prologue of {@link #mergeRowGroup} and {@link #accountDroppedRowGroup}: folds the
     * group's liveness and row deletion across its legs, captures the clustering leg, runs the
     * in-slice validation, and settles the deletion the cell walk runs under.
     * <p>
     * AT MOST one clustering materialization per merged group (vs one per source row on the per-leg
     * path), and NONE for a group that merges to nothing: materialization is deferred to the row's
     * actual start ({@link #startRow}/{@link #ensureRowStarted}). The clustering's source leg is
     * captured NOW, before the cell sort reorders legs[]: every group member's clustering compares
     * equal, but equal clusterings need not be byte-identical (e.g. decimals), and output bytes
     * must come from the group's last-sorted leg, whose row Row.Merger is given last.
     * <p>
     * Row.Merger.merge / BTreeRow.purge analog WITHOUT the purge arms: the merged row deletion
     * survives only when it supersedes the active partition/range deletion. The merged shell is
     * left in {@link #groupMergedInfo}, {@link #groupMergedDeletion} and
     * {@link #groupDeletionSurvives} rather than converted here, because the two callers need it in
     * different forms: the sink takes stable copies, the probe takes the reusable originals.
     *
     * @return the active deletion in effect for this group's cell walk
     */
    private DeletionTime prepareRowGroup(int rowMergeLimit) throws IOException
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
        groupMergedInfo = mergedInfo;
        groupMergedDeletion = mergedDeletion;

        // Row.Merger keeps the clustering of the last row it is given, and the iterator path's
        // merge gives the equal rows in this order, so the last leg's clustering object is the
        // one it emits (a memtable's and an sstable's differ in heap size, which the read size
        // guardrail counts)
        rowClusteringLeg = legs[rowMergeLimit - 1];
        rowClustering = null;
        rowStarted = false;
        // a row group reaching here is in the current slice (each leg is read per slice, like
        // the iterator path's sstable iterator), so validation applies whenever it is enabled
        groupValidateInSlice = validationEnabled;
        if (groupValidateInSlice)
        {
            // per-leg parity with the iterator path, which validates EACH leg's deserialized row
            // (winners and merge losers alike) below its merge
            for (int i = 0; i < rowMergeLimit; i++)
                legs[i].validateRowHeader();
        }

        if (rowReadAsStored(rowMergeLimit))
        {
            // a single leg is read as stored, and so is a row only one leg holds with no deletion
            // active (Row.Merger returns that one version unchanged): its row deletion is kept and
            // shadows nothing
            groupDeletionSurvives = !mergedDeletion.isLive();
            return DeletionTime.LIVE;
        }
        groupDeletionSurvives = mergedDeletion.supersedes(activeDeletion);
        // when the merged row deletion does not survive, the partition/range deletion takes over
        return groupDeletionSurvives ? mergedDeletion : activeDeletion;
    }

    /**
     * Shared cell walk of {@link #mergeRowGroup} and {@link #accountDroppedRowGroup}: parks every
     * leg at its next cell position, forms the next cell group, dispatches it, then advances the
     * group past it. {@code accountingOnly} is a constant at each call site, so the dispatch folds
     * away once this inlines.
     */
    private void walkCellGroups(int rowMergeLimit, DeletionTime rowActiveDeletion, boolean accountingOnly) throws IOException
    {
        if (rowMergeLimit == 1)
        {
            walkCellGroupsInOrder(rowMergeLimit, rowActiveDeletion, accountingOnly);
            return;
        }
        // Row.Merger merges the columns of the group's rows in source order, with a merge of its
        // own: the cell walk starts from the legs in that order.  The row merge then goes on from
        // the order it gave the group's rows, so it is put back after the walk.
        System.arraycopy(legs, 0, groupOrder, 0, rowMergeLimit);
        int next = 0;
        for (int source = 0; source < sourceOrder.length && next < rowMergeLimit; source++)
        {
            for (int i = 0; i < rowMergeLimit; i++)
            {
                if (groupOrder[i] == sourceOrder[source])
                {
                    legs[next++] = sourceOrder[source];
                    break;
                }
            }
        }
        try
        {
            walkCellGroupsInOrder(rowMergeLimit, rowActiveDeletion, accountingOnly);
        }
        finally
        {
            System.arraycopy(groupOrder, 0, legs, 0, rowMergeLimit);
        }
    }

    private void walkCellGroupsInOrder(int rowMergeLimit, DeletionTime rowActiveDeletion, boolean accountingOnly) throws IOException
    {
        currentComplexColumn = null;
        foldLegs = legs;
        foldCount = rowMergeLimit;
        if (rowMergeLimit > 1)
        {
            walkCellGroupsAsRowMerger(rowMergeLimit, rowActiveDeletion, accountingOnly);
            return;
        }
        int cellMergeLimit = rowMergeLimit;
        for (;;)
        {
            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (legs[i].needsCellAdvance())
                    legs[i].ensureParkedAtCell(groupValidateInSlice);
            }
            cellMergeLimit = prepareAndSortCellsForMerge(rowMergeLimit, cellMergeLimit);
            if (cellMergeLimit == 0)
                break;
            // Once a regular-column expression failed mid-walk the row is being ABANDONED —
            // the remaining cell groups take the metadata-only accounting walk (same winner
            // resolution, no value staging), streaming their surviving winners into the
            // dropped-row accounting the probe opened at abandonment.
            if (accountingOnly || rowAbandonedByFilter)
                accountDroppedCellGroup(cellMergeLimit, rowMergeLimit, rowActiveDeletion);
            else
                mergeCellGroup(cellMergeLimit, rowMergeLimit, rowActiveDeletion);
            for (int i = 0; i < cellMergeLimit; i++)
                legs[i].advancePastCellPosition();
        }
    }

    /**
     * The cell walk of a group of several legs, in the order {@code Row.Merger} merges their
     * columns: a merge of the rows' columns, in source order, then, for a complex column, a merge
     * of the cells of the rows that hold it, in the order the column merge gave them.  That order
     * decides which of two equal cells wins.
     */
    private void walkCellGroupsAsRowMerger(int rowMergeLimit, DeletionTime rowActiveDeletion, boolean accountingOnly) throws IOException
    {
        for (int i = 0; i < rowMergeLimit; i++)
        {
            if (legs[i].needsCellAdvance())
                legs[i].ensureParkedAtCell(groupValidateInSlice);
        }
        System.arraycopy(legs, 0, rowLegs, 0, rowMergeLimit);
        columnOrder.reset(rowLegs, rowMergeLimit);
        int columnCount;
        while ((columnCount = columnOrder.next(columnLegs)) > 0)
        {
            ColumnMetadata column = columnLegs[0].cellColumn();
            if (!column.isComplex())
            {
                System.arraycopy(columnLegs, 0, legs, 0, columnCount);
                dispatchCellGroup(columnCount, rowMergeLimit, rowActiveDeletion, accountingOnly);
                moveOnFromCellGroup(columnCount);
                continue;
            }
            foldLegs = columnLegs;
            foldCount = columnCount;
            // the rows whose column holds a deletion and no cell go first, as one group: they give
            // the column's merged deletion and no cell
            int noCells = 0;
            for (int i = 0; i < columnCount; i++)
            {
                if (!columnLegs[i].cellProduced())
                    legs[noCells++] = columnLegs[i];
            }
            if (noCells > 0)
                dispatchCellGroup(noCells, rowMergeLimit, rowActiveDeletion, accountingOnly);
            complexColumn = column;
            cellOrder.reset(columnLegs, columnCount);
            int cellCount;
            while ((cellCount = cellOrder.next(legs)) > 0)
            {
                dispatchCellGroup(cellCount, rowMergeLimit, rowActiveDeletion, accountingOnly);
                moveOnFromCellGroup(cellCount);
            }
            for (int i = 0; i < columnCount; i++)
            {
                if (!columnLegs[i].cellProduced() && columnLegs[i].parkedAtCellPosition()
                    && CursorReads.sameColumn(columnLegs[i].cellColumn(), column))
                {
                    legs[0] = columnLegs[i];
                    moveOnFromCellGroup(1);
                }
            }
            foldLegs = legs;
            foldCount = rowMergeLimit;
        }
    }

    private void dispatchCellGroup(int cellMergeLimit, int rowMergeLimit, DeletionTime rowActiveDeletion, boolean accountingOnly) throws IOException
    {
        if (accountingOnly || rowAbandonedByFilter)
            accountDroppedCellGroup(cellMergeLimit, rowMergeLimit, rowActiveDeletion);
        else
            mergeCellGroup(cellMergeLimit, rowMergeLimit, rowActiveDeletion);
    }

    /** Moves the legs of a cell group to their next positions, as the cell walk does. */
    private void moveOnFromCellGroup(int cellMergeLimit) throws IOException
    {
        for (int i = 0; i < cellMergeLimit; i++)
        {
            legs[i].advancePastCellPosition();
            if (legs[i].needsCellAdvance())
                legs[i].ensureParkedAtCell(groupValidateInSlice);
        }
    }

    /** What {@link #columnOrder} reads: the column a leg is parked at.  The legs are moved by the walk. */
    private final class ColumnSources implements IteratorMergeOrder.Sources<CursorReads.MergeLeg>
    {
        public int compare(CursorReads.MergeLeg a, CursorReads.MergeLeg b)
        {
            return a.cellColumn().compareTo(b.cellColumn());
        }

        public boolean advance(CursorReads.MergeLeg leg)
        {
            return leg.parkedAtCellPosition();
        }
    }

    /** What {@link #cellOrder} reads: the path of the cell a leg is parked at in {@link #complexColumn}. */
    private final class CellSources implements IteratorMergeOrder.Sources<CursorReads.MergeLeg>
    {
        public int compare(CursorReads.MergeLeg a, CursorReads.MergeLeg b)
        {
            return CursorCompactor.comparePaths(complexColumn, a.cellPathWindow(), b.cellPathWindow());
        }

        public boolean advance(CursorReads.MergeLeg leg)
        {
            return leg.parkedAtCellPosition() && leg.cellProduced() && CursorReads.sameColumn(leg.cellColumn(), complexColumn);
        }
    }

    /** Routes a regular-column expression failure into the probe's abandonment cycle (see
     *  {@link FilterProbe#abandonRowGroup}) and flips the row to accounting mode. The sink's
     *  partial output row is discarded by the probe; {@code rowStarted} is cleared so no endRow
     *  ever reaches the sink for an abandoned group. */
    private void abandonRowByFilter(CellLivenessInfo failedWinnerLiveness)
    {
        rowAbandonedByFilter = true;
        filterProbe.abandonRowGroup(rowClusteringLeg, true, failedWinnerLiveness);
        rowStarted = false;
    }

    /** Starts the output row with the group's clustering, taken from the leg captured at group
     *  formation: as that leg's descriptor for a streaming sink, else built once. */
    private void startRow(LivenessInfo livenessOut, DeletionTime rowDeletionOut)
    {
        if (streamCells)
        {
            sink.startRow(rowClusteringLeg.unfiltered(), livenessOut, rowDeletionOut);
        }
        else
        {
            if (rowClustering == null)
                rowClustering = (Clustering<?>) rowClusteringLeg.materializeClusteringPrefix();
            sink.startRow(rowClustering, livenessOut, rowDeletionOut);
        }
        rowStarted = true;
    }

    /** Deferred start for a row group whose merged shell was empty: EMPTY liveness and LIVE row
     *  deletion are exactly what the eager path passed the sink for such a group. */
    private void ensureRowStarted()
    {
        if (!rowStarted)
            startRow(LivenessInfo.EMPTY, DeletionTime.LIVE);
    }

    /**
     * Whether the merged row is the one version a single source holds, unchanged: the iterator
     * path reads a lone source without a merge, and {@code Row.Merger.merge} returns a single
     * version as is when no partition or range deletion is active.
     */
    private boolean rowReadAsStored(int rowMergeLimit)
    {
        return rowMergeLimit == 1 && (readAsStored || activeDeletion.isLive());
    }

    // ---------------------------------------------------------------- cell groups

    /**
     * Shared complex-column prologue of {@link #mergeCellGroup} and
     * {@link #accountDroppedCellGroup}. On entering a new output complex column, every row-group
     * source that owns it is parked at it (column-ordered streams; this column is the merge
     * minimum, and deletion-only positions sort ahead of its cells), so the merged complex deletion
     * is computable up front, before any of the column's cells is emitted. Contributions arrive
     * per-leg filtered (dropped-column rule) from CursorReads.PendingLeg.cellComplexDeletion.
     * <p>
     * A folded deletion survives only when it STRICTLY supersedes the active deletion; on exact
     * equality the iterator drops it (ColumnDataReducer, Row.java). There is NO purge: a gcable
     * complex deletion still shadows and still reaches the merged output. Sets
     * {@link #complexDeletionNeedsReport} when this call entered a new column whose folded deletion
     * survives, which each caller then reports to its own consumer.
     *
     * @return the deletion this group's cells are shadowed against
     */
    private DeletionTime foldComplexDeletion(ColumnMetadata column, int rowMergeLimit, DeletionTime rowActiveDeletion)
    {
        complexDeletionNeedsReport = false;
        if (!CursorReads.sameColumn(currentComplexColumn, column))
        {
            currentComplexColumn = column;
            mergedComplexDeletion.resetLive();
            for (int i = 0; i < foldCount; i++)
            {
                CursorReads.MergeLeg leg = foldLegs[i];
                if (leg.parkedAtCellPosition() && CursorReads.sameColumn(leg.cellColumn(), column))
                {
                    DeletionTime contribution = leg.cellComplexDeletion();
                    if (contribution.supersedes(mergedComplexDeletion))
                        mergedComplexDeletion.reset(contribution);
                }
            }
            if (!rowActiveDeletion.isLive() && !mergedComplexDeletion.supersedes(rowActiveDeletion))
                mergedComplexDeletion.resetLive();
            complexDeletionNeedsReport = !mergedComplexDeletion.isLive();
        }
        // a row read as stored keeps its complex deletion, which shadows nothing
        if (rowReadAsStored(rowMergeLimit))
            return rowActiveDeletion;
        if (!mergedComplexDeletion.isLive() && mergedComplexDeletion.supersedes(rowActiveDeletion))
            return mergedComplexDeletion;
        return rowActiveDeletion;
    }

    /**
     * The adapted shape of CursorCompactor.mergeCells: winner selection via the SHARED
     * {@link CellLivenessInfo#resolve} decision table, value comparison only on full
     * metadata ties (left/current winner keeps equal values, exactly Cells.resolveRegular), and
     * shadowed-cell dropping against the active partition/range/complex deletion. Purge-free,
     * no TTL-expiry conversion (both compaction-only; see the class javadoc), and materializing —
     * the winner becomes a real Cell through the leg's value/path machinery; losers'
     * values are skipped at the byte level without ever being materialized.
     */
    private void mergeCellGroup(int cellMergeLimit, int rowMergeLimit, DeletionTime rowActiveDeletion) throws IOException
    {
        CursorReads.MergeLeg winner = legs[0];
        ColumnMetadata column = winner.cellColumn();
        if (column.type.isCounter())
        {
            // Counters do not reconcile to a single winner: every surviving shard set must fold
            // together (Cells.resolveCounter), so they take their own path.  Counter columns are
            // simple (never complex) and cannot appear in a row filter, so neither the complex
            // block below nor the FilterProbe applies.
            mergeCounterCells(cellMergeLimit, rowActiveDeletion);
            return;
        }
        DeletionTime effectiveDeletion = rowActiveDeletion;
        if (column.isComplex())
        {
            effectiveDeletion = foldComplexDeletion(column, rowMergeLimit, rowActiveDeletion);
            if (complexDeletionNeedsReport)
            {
                ensureRowStarted();
                sink.addComplexDeletion(column, CursorReads.copyOf(mergedComplexDeletion));
            }
            if (!winner.cellProduced())
            {
                // deletion-only group: contribution already folded into the merged deletion.  A
                // row read as stored keeps a column with a live deletion and no cells: see
                // PartitionMaterializer.keepsColumnWithNoCells
                if (rowReadAsStored(rowMergeLimit) && winner.keepsColumnWithNoCells())
                {
                    ensureRowStarted();
                    sink.addComplexDeletion(column, DeletionTime.LIVE);
                }
                return;
            }
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
                        ensureTieScratch();
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

        // Whether this group's column carries a regular-column filter expression whose
        // winner must be evaluated at resolution (constant false without regular pushdown)
        boolean filtered = filterProbe != null && filterProbe.filtersRegularColumn(column);

        if (effectiveDeletion.deletesCellAt(winner.cellLiveness().timestamp()))
        {
            // shadowed by the active partition/range/complex deletion: dropped from the merged
            // row, exactly like Row.Merger.ColumnDataReducer — and unlike compaction, NEVER
            // dropped for being purgeable (no purger here)
            winner.discardCellValue();
            // A shadowed winner never reaches the merged row, so a filter column's
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
                        ensureTieScratch();
                        tieValueScratch1.clear();
                        winner.stageCellValue(tieValueScratch1);
                        stagedWinnerValue = tieValueScratch1;
                    }
                    window = stagedWinnerValue.valueWindow();
                }
                // `window` is a view over reusable storage (the tie scratch, overwritten by the
                // next stage) or the memtable cell's buffer; it is handed only to the probe's
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

        // Cell escape hatch: a memtable-won cell is emitted as its already-live object —
        // exactly what the object merge does (Cells.reconcile returns the winning cell object,
        // never a rebuild). Its value was never consumed off any cursor (staging reads the live
        // object non-destructively), so the object is intact even after a tie-break; the staged
        // scratch bytes are simply abandoned. TEST_CORRUPT_CELL_TIMESTAMPS does not
        // apply here — it corrupts MATERIALIZED cells, and the memtable-specific hooks
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
        if (streamCells)
        {
            // Stream the winner's value bytes straight from wherever they currently
            // live into the sink's own destination — the allocation win, replacing the
            // byte[]/Cell materialization below entirely for sinks that opt in.
            if (stagedWinnerValue != null)
                winnerCellValueSource.resetToScratch(stagedWinnerValue);
            else
                winnerCellValueSource.resetToLeg(winner);
            sink.addCellFromWire(winner.cellColumn(), timestamp, ttl, localDeletionTime, path, winnerCellValueSource);
            // Safety net, mirroring the discardCellValue() idiom used everywhere else in this
            // method: a no-op if the sink already streamed the leg's value (state has moved past
            // CELL_VALUE_START), or if the value lived in scratch all along (this leg was never
            // touched by this cell) — but mandatory if the sink chose not to consume it at all
            // (e.g. ResponseSink dropping a purged cell, or the row not being admitted),
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

    private void ensureTieScratch()
    {
        if (tieValueScratch1 == null)
        {
            tieValueScratch1 = new CellValueScratch();
            tieValueScratch2 = new CellValueScratch();
        }
    }

    /**
     * The counter cell group of a row the filter probe dropped: the cell that would win is
     * reported to the probe as liveness metadata only.  A tombstone that is not shadowed wins
     * (CASSANDRA-7346); two tombstones resolve like regular cells.
     */
    private void accountDroppedCounterCellGroup(int cellMergeLimit, DeletionTime activeDeletion) throws IOException
    {
        CellLivenessInfo tombstoneLiveness = null;
        CellLivenessInfo liveLiveness = null;
        for (int i = 0; i < cellMergeLimit; i++)
        {
            CursorReads.MergeLeg leg = legs[i];
            CellLivenessInfo liveness = leg.cellLiveness();
            leg.discardCellValue();
            if (activeDeletion.deletesCellAt(liveness.timestamp()))
                continue;
            if (liveness.isTombstone())
            {
                // a full metadata tie shares its liveness, so no value is compared
                if (tombstoneLiveness == null || CellLivenessInfo.resolve(tombstoneLiveness, liveness) == Resolution.RIGHT)
                    tombstoneLiveness = liveness;
            }
            else if (liveLiveness == null)
            {
                liveLiveness = liveness;
            }
        }
        CellLivenessInfo winner = tombstoneLiveness != null ? tombstoneLiveness : liveLiveness;
        if (winner != null)
            filterProbe.droppedCell(winner);
    }

    /**
     * The counter cell group of an emitted row, reconciled the way {@code Row.Merger} reconciles
     * the versions of a counter column: {@code Cells.reconcile} folds them in the order the
     * group's legs come in, and gives back one of its two cells whenever it can
     * ({@code Cells.resolveCounter}): the tombstone of a live counter and a tombstone, the empty
     * value of an empty and a non-empty one, the cell whose context holds the other's with the
     * newer timestamp.  Otherwise it makes a new cell over the merged context, or over the
     * context of the cell that holds the other when the timestamp is the other's.  Two tombstones
     * reconcile like regular cells.  The emitted cell is then that leg's cell (a memtable's own
     * object, or a cell made like the sstable iterator's), or the new one, so its heap size is
     * the iterator path's.  Inputs the active deletion shadows are left out.  An sstable leg's
     * context first gets the marked-local clear the iterator path's deserializer applies.
     */
    private void mergeCounterCells(int cellMergeLimit, DeletionTime activeDeletion) throws IOException
    {
        if (counterContexts == null)
        {
            counterContexts = new CursorCounterContexts();
            counterFoldScratch = new CellValueScratch();
            counterInputScratch = new CellValueScratch();
        }
        ColumnMetadata column = legs[0].cellColumn();
        // the result so far: the leg whose cell it is, or a new cell when made is set
        int result = -1;
        boolean made = false;
        // the leg whose context a new cell holds, or -1 for a merged one
        int madeFrom = -1;
        long timestamp = 0;
        int ttl = Cell.NO_TTL;
        long localDeletionTime = Cell.NO_DELETION_TIME;
        boolean tombstone = false;
        CellLivenessInfo resultLiveness = null;
        CellValueScratch value = counterFoldScratch;
        for (int i = 0; i < cellMergeLimit; i++)
        {
            CursorReads.MergeLeg leg = legs[i];
            CellLivenessInfo liveness = leg.cellLiveness();
            if (activeDeletion.deletesCellAt(liveness.timestamp()))
            {
                leg.discardCellValue();
                continue;
            }
            CellValueScratch input = counterInputScratch;
            input.clear();
            leg.stageCellValue(input);
            boolean inputTombstone = liveness.isTombstone();
            if (!inputTombstone && input.length() > 0 && leg.sstableOrNull() != null)
            {
                // the deserialization-time marked-local clear (Flag.LOCAL), of an sstable's context only
                int cleared = counterContexts.clearMarkedLocal(input.buffer, 0, input.length());
                if (cleared >= 0)
                {
                    input.clear();
                    input.write(counterContexts.scratchBuffer(), 0, cleared);
                }
            }
            boolean take;
            if (result < 0 && !made)
            {
                take = true;
            }
            else if (!tombstone || !inputTombstone)
            {
                // Cells.resolveCounter: at least one side is a live counter
                if (tombstone || inputTombstone)
                {
                    take = inputTombstone;
                }
                else if (value.length() == 0 || input.length() == 0)
                {
                    take = value.length() != input.length() ? input.length() == 0 : !(timestamp > liveness.timestamp());
                }
                else
                {
                    CursorCounterContexts.MergeResult merged = counterContexts.merge(value.buffer, 0, value.length(),
                                                                                     input.buffer, 0, input.length());
                    long mergedTimestamp = Math.max(timestamp, liveness.timestamp());
                    take = false;
                    if (merged == CursorCounterContexts.MergeResult.LEFT_SUPERSET)
                    {
                        if (mergedTimestamp != timestamp)
                        {
                            // a new cell over the left context
                            if (!made)
                                madeFrom = result;
                            made = true;
                        }
                    }
                    else if (merged == CursorCounterContexts.MergeResult.RIGHT_SUPERSET && mergedTimestamp == liveness.timestamp())
                    {
                        take = true;
                    }
                    else
                    {
                        if (merged == CursorCounterContexts.MergeResult.RIGHT_SUPERSET)
                        {
                            madeFrom = i;
                            value.clear();
                            value.write(input.buffer, 0, input.length());
                        }
                        else
                        {
                            madeFrom = -1;
                            value.clear();
                            value.write(counterContexts.scratchBuffer(), 0, counterContexts.scratchLength());
                        }
                        made = true;
                    }
                    if (made && !take)
                    {
                        result = -1;
                        timestamp = mergedTimestamp;
                        ttl = Cell.NO_TTL;
                        localDeletionTime = Cell.NO_DELETION_TIME;
                        tombstone = false;
                        resultLiveness = null;
                    }
                }
            }
            else
            {
                // two tombstones: Cells.resolveRegular, the left one keeping an equal value
                Resolution resolution = CellLivenessInfo.resolve(resultLiveness, liveness);
                take = resolution == Resolution.RIGHT
                       || (resolution == Resolution.COMPARE && CellValueScratch.compare(value, input) < 0);
            }
            if (take)
            {
                result = i;
                made = false;
                madeFrom = -1;
                timestamp = liveness.timestamp();
                ttl = liveness.ttl();
                localDeletionTime = liveness.localDeletionTime();
                tombstone = inputTombstone;
                resultLiveness = liveness;
                value.clear();
                value.write(input.buffer, 0, input.length());
            }
        }
        if (result < 0 && !made)
            return; // every input was shadowed by the active deletion

        ensureRowStarted();
        if (!made)
        {
            Cell<?> existing = legs[result].existingCell();
            if (existing != null)
            {
                sink.addCell(existing);
                CursorReads.countMemtableCellReuse();
                return;
            }
            long ts = CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS ? timestamp + 1 : timestamp;
            CursorReads.countSstableCellValueMaterialized();
            sink.addCell(ByteArrayAccessor.instance.factory().cell(column, ts, ttl, localDeletionTime, value.toValueArray(), null));
            return;
        }
        // Cells.resolveCounter's new cell: over the context of the cell that held the other, or a
        // new merged context
        Cell<?> from = madeFrom < 0 ? null : legs[madeFrom].existingCell();
        ByteBuffer context = from != null ? from.buffer() : ByteBuffer.wrap(value.toValueArray());
        long ts = CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS ? timestamp + 1 : timestamp;
        CursorReads.countSstableCellValueMaterialized();
        sink.addCell(BufferCell.live(column, ts, context));
    }

    // ---------------------------------------------------------------- dropped row groups

    /**
     * The accounting counterpart of {@link #mergeRowGroup} for a group the {@link FilterProbe}
     * rejected: runs the IDENTICAL metadata-level winner resolution — group liveness/deletion
     * fold, active-deletion shadowing, per-leg in-slice validation (matching the iterator path,
     * which validates each leg's deserialized row below its merge, including rows the filter later
     * drops), complex-deletion reconciliation, cell winner selection and shadow-dropping — but
     * emits NOTHING: no clustering, cell or path is materialized, values are skipped at the byte
     * level, and the surviving merged content is reported to the probe as metadata for scan-metric
     * classification. The one difference from the emitting path: on a full-metadata
     * cell tie ({@code Resolution.COMPARE}) no value comparison happens at all — metadata-tied
     * cells share their liveness classification regardless of which wins the tie (COMPARE is only
     * returned when timestamp, tombstone/expiring class, localDeletionTime and ttl all agree), so
     * the winner's identity is irrelevant to accounting. {@code TEST_CORRUPT_MERGE_DECISIONS} does
     * not apply here for the same reason: inverting a non-tie pick would change classification, but
     * the hook exists to corrupt EMITTED merge decisions, and the accounting walk's own negative
     * control is {@code CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING}.
     *
     * The whole-row escape hatch is NOT taken for dropped groups: the general
     * walk works for object-backed legs too (their cell surface is the same), and one accounting
     * code path beats two.
     */
    private void accountDroppedRowGroup(int rowMergeLimit) throws IOException
    {
        DeletionTime rowActiveDeletion = prepareRowGroup(rowMergeLimit);
        // the same merged shell mergeRowGroup computes, minus the output-copy allocations: the
        // probe receives it as reusable metadata
        DeletionTime rowDeletionOut = groupDeletionSurvives ? groupMergedDeletion : DeletionTime.LIVE;
        LivenessInfo livenessOut = groupMergedInfo.isEmpty() || rowActiveDeletion.deletes(groupMergedInfo)
                                   ? null
                                   : groupMergedInfo;

        filterProbe.beginDroppedRow(rowClusteringLeg, true, livenessOut, rowDeletionOut);
        walkCellGroups(rowMergeLimit, rowActiveDeletion, true);
        filterProbe.endDroppedRow();
    }

    /**
     * The accounting counterpart of {@link #mergeCellGroup}: same winner selection (minus the tie-break
     * value comparison — see {@link #accountDroppedRowGroup}), same complex-deletion fold and
     * suppression, same shadowed-winner drop; every value discarded at the byte level, the
     * surviving winner reported to the probe as liveness metadata only.  A counter cell takes
     * {@link #accountDroppedCounterCellGroup}'s rule instead.
     */
    private void accountDroppedCellGroup(int cellMergeLimit, int rowMergeLimit, DeletionTime rowActiveDeletion) throws IOException
    {
        CursorReads.MergeLeg winner = legs[0];
        ColumnMetadata column = winner.cellColumn();
        DeletionTime effectiveDeletion = rowActiveDeletion;
        if (column.isComplex())
        {
            effectiveDeletion = foldComplexDeletion(column, rowMergeLimit, rowActiveDeletion);
            if (complexDeletionNeedsReport)
                filterProbe.droppedComplexDeletion(mergedComplexDeletion);
            if (!winner.cellProduced())
            {
                // deletion-only group: contribution already folded into the merged deletion
                return;
            }
        }
        else if (column.type.isCounter())
        {
            // counter cells fold rather than pick a winner by timestamp
            accountDroppedCounterCellGroup(cellMergeLimit, rowActiveDeletion);
            return;
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
        if (openRanges.active() != DeletionTime.LIVE)
            previousDeletionInMerged = openRanges.copyOf(openRanges.active());
        try
        {
            updateOpenMarkers(rangeTombstoneMergeLimit);
            DeletionTime newDeletionInMerged = openRanges.active();

            // markers are validated per leg when in the slice: after its start (the merge has
            // already stopped at its end)
            if (validationEnabled && !isAtOrBeforeSliceStart(legs[0].unfiltered()))
            {
                for (int i = 0; i < rangeTombstoneMergeLimit; i++)
                    legs[i].validateMarkerHeader();
            }

            if (previousDeletionInMerged.equals(newDeletionInMerged))
                return; // the merged open deletion did not change: no marker in the merged stream

            // RangeTombstoneMarker.Merger builds the merged marker over the clustering of the
            // last marker it is given, in its own accessor
            ClusteringPrefix<?> bound = legs[rangeTombstoneMergeLimit - 1].materializeClusteringPrefix();
            boolean isBeforeClustering = (bound.kind().comparedToClustering < 0) != reversed;
            if (previousDeletionInMerged == DeletionTime.LIVE)
            {
                DeletionTime open = CursorReads.copyOf(newDeletionInMerged);
                sink.addRangeTombstoneMarker(isBeforeClustering ? RangeTombstoneBoundMarker.inclusiveOpen(reversed, bound, open)
                                                                : RangeTombstoneBoundMarker.exclusiveOpen(reversed, bound, open));
            }
            else if (newDeletionInMerged == DeletionTime.LIVE)
            {
                DeletionTime close = CursorReads.copyOf(previousDeletionInMerged);
                sink.addRangeTombstoneMarker(isBeforeClustering ? RangeTombstoneBoundMarker.exclusiveClose(reversed, bound, close)
                                                                : RangeTombstoneBoundMarker.inclusiveClose(reversed, bound, close));
            }
            else if (bound.size() == 0)
            {
                // A boundary at the partition start (the end, reversed) cannot exist.  This group
                // lies before the slice start: a seeked leg's open deletion met another leg's
                // deletion from the partition start, which the merge is never asked to emit.  Only
                // the deletion open after it matters, so it goes to the sink as an opening bound.
                sink.addRangeTombstoneMarker(RangeTombstoneBoundMarker.inclusiveOpen(reversed, bound, CursorReads.copyOf(newDeletionInMerged)));
            }
            else
            {
                DeletionTime close = CursorReads.copyOf(previousDeletionInMerged);
                DeletionTime open = CursorReads.copyOf(newDeletionInMerged);
                sink.addRangeTombstoneMarker(isBeforeClustering ? RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(reversed, bound, close, open)
                                                                : RangeTombstoneBoundaryMarker.inclusiveCloseExclusiveOpen(reversed, bound, close, open));
            }
        }
        finally
        {
            if (previousDeletionInMerged != DeletionTime.LIVE)
                openRanges.recycle((DeletionTime.ReusableDeletionTime) previousDeletionInMerged);
        }
    }

    /** Feeds the group's merged markers to the shared open set. */
    private void updateOpenMarkers(int rangeTombstoneMergeLimit)
    {
        for (int i = 0; i < rangeTombstoneMergeLimit; i++)
        {
            if (reversed)
                openRanges.applyReversed(legs[i].unfiltered(), mergedPartitionDeletion);
            else
                openRanges.apply(legs[i].unfiltered(), mergedPartitionDeletion);
            legs[i].noteMarker();
        }
        openRanges.settle();
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
     * A {@link ClusteringDescriptor} loaded from a query-side slice bound instead of from the data
     * file, comparison-only (never handed to any cursor).  The bound is serialized in the exact
     * wire form every leg's descriptor holds, so the shared descriptor-level comparator compares
     * it against each group's minimum without allocating.  Reused for every slice.
     */
    static final class SliceBoundDescriptor extends ClusteringDescriptor
    {
        SliceBoundDescriptor(AbstractType<?>[] types)
        {
            super(types);
        }

        SliceBoundDescriptor load(ClusteringBound<?> bound)
        {
            storeClustering((byte) bound.kind().ordinal(), bound.size(), bound);
            return this;
        }
    }

    /** The two descriptors a merge stages the current slice's start and end in. */
    static final class SliceBounds
    {
        final AbstractType<?>[] types;
        final SliceBoundDescriptor start;
        final SliceBoundDescriptor end;

        SliceBounds(AbstractType<?>[] types)
        {
            this.types = types;
            this.start = new SliceBoundDescriptor(types);
            this.end = new SliceBoundDescriptor(types);
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
    static final class CellValueScratch implements DataOutputPlus, SSTableCursorReader.DirectValueTarget
    {
        private byte[] buffer = new byte[64];
        private int length;
        /** Whether {@link #writeUnsignedVInt32} was called while staging the bytes
         *  currently held — i.e. whether the ORIGINAL wire value was variable-length (a vint
         *  preceded it). Needed to replay ({@link #streamTo}) the exact same shape
         *  {@code stageCellValue} would have written to a fresh destination: the vint itself is
         *  never retained in {@link #buffer} (see the class javadoc), only whether one occurred. */
        private boolean sawVIntLength;
        /** Reusable filter-evaluation view over {@link #buffer}; re-wrapped only when the
         *  grow-only buffer is replaced, never per evaluation. The window's contents are valid
         *  only until the next {@code clear()}/stage — it must never escape the immediate
         *  filter-evaluation call (see the {@code mergeCellGroup} call site). */
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
         * Replays these staged bytes into {@code dest} exactly as the original
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

        /** TEST ONLY: flips the first staged byte's low bit — see
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

        @Override
        public void readValue(DataInputPlus in, int valueLength) throws IOException
        {
            ensureCapacity(length + valueLength);
            in.readFully(buffer, length, valueLength);
            length += valueLength;
        }

        /** The value bytes of a memtable cell in native memory. */
        @Override
        public void writeMemory(long address, int valueLength)
        {
            ensureCapacity(length + valueLength);
            MemoryUtil.getBytes(address, buffer, length, valueLength);
            length += valueLength;
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

        @Override
        public void writeBytes(String s)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeChars(String s)
        {
            throw new UnsupportedOperationException();
        }
            @Override
        public void writeBoolean(boolean v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeShort(int v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeChar(int v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeInt(int v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeFloat(float v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeDouble(double v)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * The reused {@link CellValueSource} view {@code mergeCellGroup} hands a streaming
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
        /** {@link #leg} when it is the single leg, called as its own class; else null. */
        private CursorReads.PendingLeg storedLeg;

        void resetToScratch(CellValueScratch scratch)
        {
            this.scratch = scratch;
            this.leg = null;
            this.storedLeg = null;
        }

        void resetToLeg(CursorReads.MergeLeg leg)
        {
            this.scratch = null;
            this.leg = leg;
            this.storedLeg = null;
        }

        void resetToStoredLeg(CursorReads.PendingLeg leg)
        {
            this.scratch = null;
            this.leg = leg;
            this.storedLeg = leg;
        }

        @Override
        public boolean hasValue() throws IOException
        {
            // A zero-length staged scratch and "no value at all" are indistinguishable on the
            // wire (see CellValueScratch#streamTo) and are therefore indistinguishable here too —
            // correct either way, since both mean nothing is written.
            if (scratch != null)
                return scratch.length() > 0;
            return storedLeg != null ? storedLeg.cellHasValue() : leg.cellHasValue();
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
                // TEST ONLY: route the FRESH-LEG case (a leg's cursor streamed directly into the
                // wire, as opposed to the tie-break scratch replay above) through a dedicated
                // scratch so the corruption can flip a byte in the value BEFORE it reaches the
                // wire. Proves the byte-comparison harness catches a wrong value introduced by
                // this streaming path, distinct from the TEST_CORRUPT_CELL_TIMESTAMPS/
                // TEST_TRANSCODE_* hooks (which target timestamps and flags, not streamed value
                // bytes).
                if (testCorruptionScratch == null)
                    testCorruptionScratch = new CellValueScratch();
                testCorruptionScratch.clear();
                leg.stageCellValue(testCorruptionScratch);
                testCorruptionScratch.corruptFirstByte();
                testCorruptionScratch.streamTo(dest);
                return;
            }
            if (storedLeg != null)
                storedLeg.stageCellValue(dest);
            else
                leg.stageCellValue(dest);
        }

        @Override
        public byte[] materialize() throws IOException
        {
            return scratch != null ? scratch.toValueArray() : leg.cellValue();
        }
    }
}
