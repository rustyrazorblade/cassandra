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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.ResponseWireWriter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteArrayUtil;

import io.netty.util.concurrent.FastThreadLocal;

import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

/**
 * Writes the replica data response of a forward single-partition cursor read straight from the
 * merge events to the response bytes, without building {@code Row} or {@code Cell} objects for
 * sstable data.  It applies what {@code ReadCommand.executeLocally} applies above the merge, to
 * each row and marker, in the same order:
 * <ol>
 *   <li>the slice bounds: elements at or before a slice start are dropped, and the range tombstone
 *       open at a slice start or end is opened or closed there, like the slicing iterator;</li>
 *   <li>query cancellation, like {@code QueryCancellationChecker};</li>
 *   <li>the purge of gcable tombstones, like {@code withoutPurgeableTombstones};</li>
 *   <li>the scan metrics and tombstone thresholds ({@link CursorReads.TombstoneScanGuard}), like
 *       {@code withMetricsRecording};</li>
 *   <li>the row filter, like {@code RowFilter.filter};</li>
 *   <li>the limit counter, the real {@link DataLimits.Counter} fed by each row's clustering and
 *       liveness;</li>
 *   <li>{@code RTBoundCloser}: a range tombstone still open when the limit stops the read is
 *       closed at the last row.</li>
 * </ol>
 * The merge runs only while {@link #wantsMore()} is true, so a read that reaches its limit merges
 * nothing past it.
 *
 * <p>The row filter is evaluated on the streamed cells when every row-level expression compares a
 * clustering column or a simple regular column value (the expressions the filter pushdown also
 * accepts).  For any other expression this sink builds each row, purges it with the real
 * {@code Row.purge} and evaluates the real {@code RowFilter.Expression.isSatisfiedBy} on it.
 *
 * <p>Purging reuses the production primitives: {@link DeletionPurger#shouldPurge} for liveness and
 * deletions, the decision tree of {@code AbstractCell.purge} for streamed cells, and
 * {@code Row.purge} for whole rows.  Range tombstone markers follow {@code PurgeFunction.applyToMarker}:
 * a boundary drops when both sides purge and becomes the surviving side's bound when one does.
 */
public final class ResponseSink implements CursorReadMerger.MergeSink
{
    private final ResponseWireWriter writer;
    private final DeletionPurger purger;
    private final long nowInSec;
    private final boolean purgeEnabled;
    private final ClusteringComparator comparator;
    /** A reverse read: the rows and markers arrive in reverse clustering order. */
    private final boolean reversed;
    private final boolean enforceStrictLiveness;
    /** Null in the test-only constructor. */
    private final CursorReads.TombstoneScanGuard scanGuard;
    /** Null when the read has no limit. */
    private final DataLimits.Counter counter;
    /** The counter needs each row's clustering: a GROUP BY limit. */
    private final boolean countByClustering;
    /** Null when the read has no row filter. */
    private final ResponseFilter filter;
    /** Builds rows for a filter this sink cannot evaluate on streamed cells; null otherwise. */
    private final CursorReads.MaterializingMergeSink rowBuilder;
    /** Checked for cancellation; null in the test-only constructor. */
    private final ReadCommand command;
    private long lastCancellationCheck;

    /** The iterator path would see one source (one leg and no absent-partition sstable). */
    boolean singleSource;
    /** Run once, when the merged partition is known to be non-empty (the top-partition sampler). */
    private Runnable onNonEmptyPartition;

    // ---- slice state ----
    private final Slices slices;
    private ClusteringBound<?> sliceStart; // null when the slice starts at BOTTOM
    private ClusteringBound<?> sliceEnd;
    /** Sticky within a slice: once true, every remaining element of the slice is in it. */
    private boolean pastSliceStart;
    /** The range deletion open in the merged stream, before purging; null when none. */
    private DeletionTime openMarker;

    // ---- per-row state, set by startRow ----
    private boolean rowAdmitted;
    /** The purged row has any content: liveness, deletion, a cell or a complex deletion. */
    private boolean rowHasContent;
    /** The purged row has live data, {@code Row.hasLiveData}. */
    private boolean rowLive;

    // ---- partition state ----
    private boolean dropEverything;  // a partition-level filter failed: count the first element only
    private boolean stopped;          // the limit stopped the read, or the first element was counted
    private boolean anyPurgeSurvivor; // an element survived the purge
    private boolean anyFilterSurvivor;// an element survived the row filter
    private boolean wroteUnfiltered;  // a row or marker reached the response
    // RTBoundCloser state, over what reached the response: the last row written, as an object or
    // as wire bytes, and the range deletion open after the last marker written
    private boolean lastRowWritten;
    private Clustering<?> lastRowClustering;
    private byte[] lastRowClusteringBytes = new byte[64];
    private int lastRowClusteringLength;
    private DeletionTime openOutputMarker;

    // ---- envelope state (see beginPartition / finishPartition) ----
    private ResponseBuffer out;
    private DecoratedKey key;
    private ColumnFilter columnFilter;
    private DeletionTime partitionDeletion;
    private Row staticRow;
    private boolean hasStatic;
    private int partitionStart;
    private int headerStart;

    /** Test-only: purge as configured, no accounting, no filter, no limit.  {@code nowInSec == 0}
     *  disables purging, matching {@code ReadCommand}'s own gate. */
    public ResponseSink(ResponseWireWriter writer, ClusteringComparator comparator, Slice slice,
                        long nowInSec, long gcBefore, boolean onlyPurgeRepairedTombstones,
                        long oldestUnrepairedTombstone)
    {
        this(writer, comparator, Slices.with(comparator, slice), false, false, nowInSec, gcBefore, onlyPurgeRepairedTombstones,
             oldestUnrepairedTombstone, null, null, false, null, null);
    }

    ResponseSink(ResponseWireWriter writer,
                 ClusteringComparator comparator,
                 Slices slices,
                 boolean reversed,
                 boolean enforceStrictLiveness,
                 long nowInSec,
                 long gcBefore,
                 boolean onlyPurgeRepairedTombstones,
                 long oldestUnrepairedTombstone,
                 CursorReads.TombstoneScanGuard scanGuard,
                 DataLimits.Counter counter,
                 boolean countByClustering,
                 ResponseFilter filter,
                 ReadCommand command)
    {
        this.writer = writer;
        this.comparator = comparator;
        this.reversed = reversed;
        this.enforceStrictLiveness = enforceStrictLiveness;
        this.nowInSec = nowInSec;
        this.purgeEnabled = nowInSec != 0;
        // PurgeFunction's purger, specialized to the read path's inputs: ignoreGcGraceSeconds is
        // compaction-only and the purge evaluator is constant-true, so both are elided
        this.purger = (timestamp, localDeletionTime) ->
                      purgeEnabled
                      && !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone)
                      && localDeletionTime < gcBefore;
        this.scanGuard = scanGuard;
        this.counter = counter;
        this.countByClustering = countByClustering;
        this.filter = filter;
        this.rowBuilder = filter != null && !filter.streamable ? new CursorReads.MaterializingMergeSink() : null;
        this.command = command;
        this.slices = slices;
        if (scanGuard != null)
            scanGuard.rowClusteringSource(this::rowClustering);
        beginSlice(0);
    }

    // ---------------------------------------------------------------- partition level

    /** Runs {@code hook} once, when the merged partition is known to be non-empty. */
    void onNonEmptyPartition(Runnable hook)
    {
        this.onNonEmptyPartition = hook;
    }

    /**
     * The merged partition deletion and static row are known, before purging and before any row
     * merges.  Like {@code UnfilteredRowIterator.isEmpty()} on the merged iterator, which does not
     * read a row when either is present.
     */
    void partitionMetadataKnown(DeletionTime mergedDeletion, Row mergedStatic)
    {
        if (!mergedDeletion.isLive() || !mergedStatic.isEmpty())
            noteNonEmpty();
    }

    private void noteNonEmpty()
    {
        if (onNonEmptyPartition == null)
            return;
        Runnable hook = onNonEmptyPartition;
        onNonEmptyPartition = null;
        hook.run();
    }

    /** Purges the partition deletion like {@code PurgeFunction.applyToDeletion}.  The merge itself
     *  still uses the unpurged deletion for shadowing. */
    DeletionTime purgePartitionDeletion(DeletionTime mergedDeletion)
    {
        return purger.shouldPurge(mergedDeletion) ? DeletionTime.LIVE : mergedDeletion;
    }

    /** Purges the merged static row like {@code PurgeFunction.applyToStatic} and counts it before
     *  any row, like {@code MetricRecording.applyToStatic}.  Returns the row to write. */
    Row purgeAndCountStaticRow(Row mergedStatic)
    {
        // an empty static row keeps its identity: the serializer writes any static row that is
        // not the EMPTY_STATIC_ROW singleton
        if (mergedStatic.isEmpty())
            return mergedStatic;
        Row purged = mergedStatic.purge(purger, nowInSec, false);
        if (purged == null)
            return Rows.EMPTY_STATIC_ROW;
        if (scanGuard != null)
            scanGuard.wholeRow(purged);
        return purged;
    }

    /**
     * Starts the partition in {@code out}: the envelope, the key, the partition header and the
     * static row, all known before the first row merges.  {@link #finishPartition} rewrites the
     * header as empty, or removes the partition, when the rows turn out to leave nothing.
     *
     * @param mergedDeletion the merged partition deletion, not yet purged
     * @param mergedStatic   the merged static row, not yet purged
     */
    void beginPartition(ResponseBuffer out, DecoratedKey key, ColumnFilter columnFilter,
                        DeletionTime mergedDeletion, Row mergedStatic) throws IOException
    {
        this.out = out;
        this.key = key;
        this.columnFilter = columnFilter;
        maybeCancel();
        // the isForThrift placeholder
        out.writeBoolean(false);
        partitionStart = out.getLength();
        partitionDeletion = purgePartitionDeletion(mergedDeletion);
        staticRow = purgeAndCountStaticRow(mergedStatic);
        // like UnfilteredRowIteratorSerializer: written unless it is the singleton, even when empty
        hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;

        if (filter != null && !filter.partitionMatches(key, staticRow))
        {
            // RowFilter drops the partition before reading a row.  The purge stage below it has
            // already read the first element that survives the purge, to see whether the
            // partition is empty, and the scan metrics counted it; nothing else is read.
            dropEverything = true;
            if (!partitionDeletion.isLive() || !staticRow.isEmpty())
                stopped = true; // the purge stage does not read a row when either is present
            return;
        }
        if (counter != null)
            counter.countPartition(key, staticRow);

        // "has next partition"
        out.writeBoolean(true);
        writer.writeKey(key.getKey());
        headerStart = out.getLength();
        writer.writeHeader(false, reversed, partitionDeletion, hasStatic, columnFilter);
        if (hasStatic)
            writer.writeStaticRow(staticRow);
    }

    /**
     * Ends the partition: closes a range tombstone the limit left open, like {@code RTBoundCloser},
     * then ends the envelope.  A partition that is empty after the purge, or whose rows all fail
     * the row filter, is removed, like {@code PurgeFunction} and {@code RowFilter} return null for
     * it.  One with nothing left is written in the single-byte empty form.
     *
     * @return whether the partition reached the response; false when it was removed
     */
    boolean finishPartition() throws IOException
    {
        if (dropEverything)
        {
            out.writeBoolean(false);
            return false;
        }
        if (openOutputMarker != null)
        {
            if (!lastRowWritten)
                // RTBoundCloser's own failure: a GROUP BY limit can drop the row that would close it
                throw new IllegalStateException(String.format("UnfilteredRowIterator for %s has an open RT bound as its last item",
                                                              command == null ? key : command.metadata()));
            Clustering<?> last = lastRowClustering != null ? lastRowClustering : decodeClustering(lastRowClusteringBytes, lastRowClusteringLength);
            writeToResponse(RangeTombstoneBoundMarker.inclusiveClose(reversed, last, openOutputMarker));
            openOutputMarker = null;
        }

        boolean emptyAfterPurge = partitionDeletion.isLive() && staticRow.isEmpty() && !anyPurgeSurvivor;
        boolean droppedByFilter = filter != null && filter.hasRowLevelExpressions() && !anyFilterSurvivor;
        if ((emptyAfterPurge && purgeEnabled) || droppedByFilter)
        {
            out.truncate(partitionStart);
            out.writeBoolean(false);
            return false;
        }
        if (partitionDeletion.isLive() && staticRow.isEmpty() && !wroteUnfiltered)
        {
            out.truncate(headerStart);
            writer.writeHeader(true, reversed, partitionDeletion, false, columnFilter);
        }
        else
        {
            writer.writeEndOfPartition();
        }
        out.writeBoolean(false);
        return true;
    }

    // ---------------------------------------------------------------- slices

    /** Seeds the open range deletion at the merge start: the row-index seek state.  Called before
     *  the first group merges. */
    public void initOpenMarker(DeletionTime openMarkerAtStart)
    {
        this.openMarker = openMarkerAtStart;
    }

    /** Opens slice {@code index}.  {@code openAfterSeek} replaces the tracked open range deletion
     *  when the merge seeked for this slice, like the slicing iterator's {@code setForSlice}. */
    void beginSlice(int index, boolean seeked, DeletionTime openAfterSeek)
    {
        beginSlice(index);
        if (seeked)
            openMarker = openAfterSeek;
    }

    /** The input is an iterator the iterator path would serialize as is: already sliced, with its
     *  own slice-bound markers, so no element is dropped and no marker is added. */
    void inputAlreadySliced()
    {
        pastSliceStart = true;
        openMarker = null;
    }

    private void beginSlice(int index)
    {
        Slice slice = slices.get(index);
        sliceStart = slice.start().isBottom() ? null : slice.start();
        sliceEnd = slice.end();
        pastSliceStart = sliceStart == null;
    }

    /**
     * The merge has no element left in the current slice.  Opens the slice if nothing in it did,
     * then closes a range tombstone still open at the slice end, both with synthetic markers, like
     * the slicing iterator's {@code computeNextInSlice}.  Not called when the limit stops the read.
     */
    public void finishSlice()
    {
        if (!pastSliceStart)
        {
            pastSliceStart = true;
            if (openMarker != null)
                syntheticMarker(new RangeTombstoneBoundMarker(sliceStart, openMarker));
        }
        if (openMarker != null)
            syntheticMarker(new RangeTombstoneBoundMarker(sliceEnd, openMarker));
    }

    /**
     * {@link #finishSlice} when slice {@code nextIndex} starts where this one ends, in a merge:
     * {@code markers}, from {@code CursorReadMerger.moveToAdjacentSlice}, replace this slice's
     * close and the next slice's open.  Then opens the next slice, past its start.
     */
    void finishSliceBeforeAdjacent(List<RangeTombstoneMarker> markers, int nextIndex, DeletionTime openInNextSlice)
    {
        if (!pastSliceStart)
        {
            pastSliceStart = true;
            if (openMarker != null)
                syntheticMarker(new RangeTombstoneBoundMarker(sliceStart, openMarker));
        }
        for (RangeTombstoneMarker marker : markers)
            syntheticMarker(marker);
        beginSlice(nextIndex);
        pastSliceStart = true;
        openMarker = openInNextSlice;
    }

    private void syntheticMarker(RangeTombstoneMarker marker)
    {
        noteNonEmpty();
        if (!stopped)
            purgeAndWriteMarker(marker);
    }

    /**
     * Slice admission for a row or marker: false for anything at or before the slice start.  On
     * the transition into the slice, first writes the synthetic open marker when a range deletion
     * is open there.
     */
    private boolean admitOrSkip(ClusteringPrefix<?> clustering)
    {
        if (pastSliceStart)
        {
            noteNonEmpty();
            return true;
        }
        if (comparator.compare(clustering, sliceStart) <= 0)
            return false;
        pastSliceStart = true;
        noteNonEmpty();
        if (openMarker != null)
            purgeAndWriteMarker(new RangeTombstoneBoundMarker(sliceStart, openMarker));
        return !stopped;
    }

    @Override
    public boolean wantsMore()
    {
        return !stopped;
    }

    // ---------------------------------------------------------------- rows

    /** The merge hands this sink each row's clustering as a descriptor (the method below). */
    @Override
    public void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
    {
        throw new UnsupportedOperationException("the merge hands a streaming sink each row's clustering as a descriptor");
    }

    /**
     * Opens a merged row.  The clustering is copied in its wire form and built only when something
     * needs the object: a GROUP BY limit, a clustering filter, a tombstone abort, or a range
     * tombstone closed at the last row.  {@code mergedLiveness} and {@code mergedRowDeletion} may
     * be the merge's reusable state, valid until {@link #endRow}.
     */
    @Override
    public void startRow(UnfilteredDescriptor clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
    {
        // the merge hands over only rows after the slice start
        rowAdmitted = enterSlice();
        if (!rowAdmitted)
            return;
        maybeCancel();
        loadRowClustering(clustering);
        if (rowBuilder != null)
        {
            // the built row keeps what it is given, so it gets stable copies
            LivenessInfo liveness = mergedLiveness.isEmpty()
                                    ? LivenessInfo.EMPTY
                                    : LivenessInfo.withExpirationTime(mergedLiveness.timestamp(), mergedLiveness.ttl(),
                                                                      mergedLiveness.localExpirationTime());
            rowBuilder.startRow(rowClustering(), liveness, CursorReads.copyOf(mergedRowDeletion));
            return;
        }
        LivenessInfo liveness = purger.shouldPurge(mergedLiveness, nowInSec) ? LivenessInfo.EMPTY : mergedLiveness;
        DeletionTime deletion = purger.shouldPurge(mergedRowDeletion) ? DeletionTime.LIVE : mergedRowDeletion;
        rowHasContent = !liveness.isEmpty() || !deletion.isLive();
        rowLive = liveness.isLive(nowInSec);
        if (scanGuard != null)
            scanGuard.startRow(null, liveness, deletion);
        if (filter != null)
            filter.startRow(filter.filtersClustering() ? rowClustering() : null);
        if (CursorReads.TEST_TRANSCODE_SKEW_TIMESTAMP && !liveness.isEmpty())
            liveness = LivenessInfo.withExpirationTime(liveness.timestamp() + 1, liveness.ttl(), liveness.localExpirationTime());
        if (CursorReads.TEST_TRANSCODE_WRONG_FLAGS && !deletion.isLive())
            deletion = DeletionTime.LIVE;
        try
        {
            writer.startRow(rowClusteringBytes, rowClusteringLength, liveness, deletion);
        }
        catch (IOException e)
        {
            throw new RuntimeException("response write failed", e);
        }
    }

    /** {@link #admitOrSkip} for a row the merge already found after the slice start. */
    private boolean enterSlice()
    {
        noteNonEmpty();
        if (pastSliceStart)
            return true;
        pastSliceStart = true;
        if (openMarker != null)
            purgeAndWriteMarker(new RangeTombstoneBoundMarker(sliceStart, openMarker));
        return !stopped;
    }

    // ---- the open row's clustering: wire bytes, built on demand ----
    private byte[] rowClusteringBytes = new byte[64];
    private int rowClusteringLength;
    /** The open row's clustering, once built from {@link #rowClusteringBytes}; null until then. */
    private Clustering<?> rowClusteringObject;
    /** The clustering types of the descriptors last checked against the response's, and the verdict. */
    private AbstractType<?>[] checkedTypes;
    private boolean sameTypes;
    private final DataOutputBuffer clusteringScratch = new DataOutputBuffer(64);

    /** Copies the descriptor's clustering bytes, which are the response's wire form when the leg
     *  serialized it with the same clustering types. */
    private void loadRowClustering(UnfilteredDescriptor clustering)
    {
        rowClusteringObject = null;
        AbstractType<?>[] types = clustering.clusteringTypes();
        if (types != checkedTypes)
        {
            checkedTypes = types;
            sameTypes = Arrays.equals(types, writer.clusteringTypes());
        }
        if (sameTypes)
        {
            int length = clustering.clusteringLength();
            if (rowClusteringBytes.length < length)
                rowClusteringBytes = new byte[Math.max(length, rowClusteringBytes.length * 2)];
            System.arraycopy(clustering.clusteringBytes(), 0, rowClusteringBytes, 0, length);
            rowClusteringLength = length;
            return;
        }
        // re-encode with the response's clustering types
        rowClusteringObject = (Clustering<?>) clustering.toClusteringPrefix(types);
        try
        {
            clusteringScratch.clear();
            Clustering.serializer.serialize(rowClusteringObject, clusteringScratch, MessagingService.current_version, writer.clusteringTypes());
        }
        catch (IOException e)
        {
            throw new RuntimeException("serializing to an in-memory buffer cannot fail", e);
        }
        rowClusteringBytes = Arrays.copyOf(clusteringScratch.getData(), Math.max(clusteringScratch.getLength(), rowClusteringBytes.length));
        rowClusteringLength = clusteringScratch.getLength();
    }

    /** The open row's clustering object, built from its bytes the first time it is asked for. */
    private Clustering<?> rowClustering()
    {
        if (rowClusteringObject == null)
            rowClusteringObject = decodeClustering(rowClusteringBytes, rowClusteringLength);
        return rowClusteringObject;
    }

    private Clustering<?> decodeClustering(byte[] bytes, int length)
    {
        try
        {
            return Clustering.serializer.deserialize(new DataInputBuffer(bytes, 0, length), MessagingService.current_version, writer.clusteringTypes());
        }
        catch (IOException e)
        {
            throw new RuntimeException("reading an in-memory buffer cannot fail", e);
        }
    }

    @Override
    public void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion)
    {
        if (!rowAdmitted)
            return;
        if (rowBuilder != null)
        {
            rowBuilder.addComplexDeletion(column, mergedComplexDeletion);
            return;
        }
        DeletionTime deletion = purger.shouldPurge(mergedComplexDeletion) ? DeletionTime.LIVE : mergedComplexDeletion;
        if (deletion.isLive())
            // a live complex deletion is never announced; the column is opened lazily by its
            // first surviving cell instead
            return;
        rowHasContent = true;
        if (scanGuard != null)
            scanGuard.complexDeletion();
        try
        {
            writer.addComplexDeletion(column, deletion);
        }
        catch (IOException e)
        {
            throw new RuntimeException("response write failed", e);
        }
    }

    @Override
    public void addCell(Cell<?> cell)
    {
        if (!rowAdmitted)
            return;
        if (rowBuilder != null)
        {
            rowBuilder.addCell(cell);
            return;
        }
        Cell<?> purged = cell.purge(purger, nowInSec);
        if (purged == null)
            // fully purged: dropped from the response entirely
            return;
        rowHasContent = true;
        boolean live = purged.isLive(nowInSec);
        rowLive |= live && !enforceStrictLiveness;
        if (scanGuard != null)
            scanGuard.cell(purged.timestamp(), purged.ttl(), purged.localDeletionTime());
        if (filter != null && filter.filtersColumn(purged.column()))
            filter.cellValue(purged.column(), live && !purged.isTombstone() ? purged.buffer() : null);
        try
        {
            writer.addCell(purged);
        }
        catch (IOException e)
        {
            throw new RuntimeException("response write failed", e);
        }
    }

    @Override
    public boolean wantsWireStreamedCells()
    {
        return true;
    }

    /** The three branches of {@code AbstractCell.purge(DeletionPurger, long)}. */
    private enum CellPurgeOutcome { UNCHANGED, CONVERT_TO_TOMBSTONE, DROP }

    /** The local deletion time of a cell {@link #purgeCellLiveness} converts to a tombstone. */
    private long convertedLocalDeletionTime;

    /**
     * {@code AbstractCell.purge(DeletionPurger, long)}'s decision tree on the cell's liveness
     * alone, so the decision is made before its value is read: not live and purgeable, drop; else
     * an expired expiring cell becomes a tombstone at the write time and is purge-tested again.
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

    /** A source over {@link #filterValue}, for a filtered cell whose value was read to evaluate it. */
    private final StagedValueSource stagedValueSource = new StagedValueSource();
    private final CursorReadMerger.CellValueScratch filterValue = new CursorReadMerger.CellValueScratch();

    /**
     * The streaming form of {@link #addCell}: the same purge outcome as {@code cell.purge}, decided
     * before the value is read, so a dropped or converted cell's value is never read.  The merge
     * discards the value the sink does not read.
     */
    @Override
    public void addCellFromWire(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime,
                                CellPath path, CellValueSource source) throws IOException
    {
        if (!rowAdmitted)
            return;
        if (rowBuilder != null)
        {
            byte[] value = source.hasValue() ? source.materialize() : ByteArrayUtil.EMPTY_BYTE_ARRAY;
            rowBuilder.addCell(org.apache.cassandra.db.marshal.ByteArrayAccessor.instance.factory()
                                                                                    .cell(column, timestamp, ttl, localDeletionTime, value, path));
            return;
        }
        CellPurgeOutcome outcome = purgeCellLiveness(timestamp, ttl, localDeletionTime);
        if (outcome == CellPurgeOutcome.DROP)
            return;
        boolean converted = outcome == CellPurgeOutcome.CONVERT_TO_TOMBSTONE;
        if (converted)
        {
            // an expired cell that is not yet gcable becomes a plain tombstone: no TTL, no value,
            // deleted at its write time
            ttl = LivenessInfo.NO_TTL;
            localDeletionTime = convertedLocalDeletionTime;
        }
        rowHasContent = true;
        boolean live = !converted && (localDeletionTime == Cell.NO_DELETION_TIME
                                      || (ttl != Cell.NO_TTL && nowInSec < localDeletionTime));
        rowLive |= live && !enforceStrictLiveness;
        if (scanGuard != null)
            scanGuard.cell(timestamp, ttl, localDeletionTime);
        boolean hasValue = !converted && source.hasValue();
        if (filter != null && filter.filtersColumn(column))
        {
            // the value is needed twice: once to evaluate, once to write
            filterValue.clear();
            if (hasValue)
                source.streamValue(filterValue);
            boolean tombstone = localDeletionTime != Cell.NO_DELETION_TIME && ttl == Cell.NO_TTL;
            filter.cellValue(column, live && !tombstone ? filterValue.valueWindow() : null);
            source = stagedValueSource;
        }
        writer.addCellFromWire(column, timestamp, ttl, localDeletionTime, path, hasValue, source);
    }

    @Override
    public void endRow()
    {
        if (!rowAdmitted)
            return;
        rowAdmitted = false;
        if (rowBuilder != null)
        {
            rowBuilder.endRow();
            Row built = (Row) rowBuilder.take();
            if (built != null)
                acceptRow(built);
            return;
        }
        if (!rowHasContent)
        {
            // purged to nothing: PurgeFunction drops it before anything else sees it
            writer.abandonRow();
            return;
        }
        anyPurgeSurvivor = true;
        if (scanGuard != null)
            scanGuard.endRow();
        if (dropEverything)
        {
            writer.abandonRow();
            stopped = true;
            return;
        }
        if (filter != null && !filter.streamedRowMatches(rowLive))
        {
            writer.abandonRow();
            return;
        }
        anyFilterSurvivor = true;
        if (counter != null && !counter.countRow(countByClustering ? rowClustering() : null, rowLive))
        {
            writer.abandonRow();
            stopIfCounted();
            return;
        }
        try
        {
            if (writer.endRow())
            {
                wroteUnfiltered = true;
                // RTBoundCloser's last row: swap the buffers rather than copy
                byte[] swap = lastRowClusteringBytes;
                lastRowClusteringBytes = rowClusteringBytes;
                lastRowClusteringLength = rowClusteringLength;
                rowClusteringBytes = swap;
                lastRowClustering = rowClusteringObject;
                lastRowWritten = true;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("response write failed", e);
        }
        stopIfCounted();
    }

    /** A whole row: a memtable row standing as the merged row, or a row this sink built. */
    @Override
    public void addRow(Row row)
    {
        if (!admitOrSkip(row.clustering()))
            return;
        maybeCancel();
        acceptRow(row);
    }

    /** Purge, scan metrics, row filter, limit and write for a row object, with the real
     *  {@code Row.purge} and {@code Row.hasLiveData}. */
    private void acceptRow(Row row)
    {
        Row purged = row.purge(purger, nowInSec, false);
        if (purged == null || purged.isEmpty())
            return;
        anyPurgeSurvivor = true;
        if (scanGuard != null)
            scanGuard.wholeRow(purged);
        if (dropEverything)
        {
            stopped = true;
            return;
        }
        if (filter != null && !filter.rowMatches(purged))
            return;
        anyFilterSurvivor = true;
        if (counter != null && !counter.countRow(purged.clustering(), purged.hasLiveData(nowInSec, enforceStrictLiveness)))
        {
            stopIfCounted();
            return;
        }
        try
        {
            writer.writeRow(purged);
        }
        catch (IOException e)
        {
            throw new RuntimeException("response write failed", e);
        }
        wroteUnfiltered = true;
        lastRowClustering = purged.clustering();
        lastRowWritten = true;
        stopIfCounted();
    }

    /** The counter has signalled the end of the partition: the iterator path pulls nothing more. */
    private void stopIfCounted()
    {
        if (counter != null && counter.stopSignalled())
            stopped = true;
    }

    // ---------------------------------------------------------------- markers

    @Override
    public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        boolean admitted = admitOrSkip(marker.clustering());
        // the open deletion follows every raw marker, admitted or not, purged or not
        DeletionTime rawOpenAfter = marker.isOpen(reversed) ? CursorReads.copyOf(marker.openDeletionTime(reversed)) : null;
        if (admitted)
            purgeAndWriteMarker(marker);
        openMarker = rawOpenAfter;
    }

    /** Purges a marker like {@code PurgeFunction.applyToMarker}, then counts and writes what is left. */
    private void purgeAndWriteMarker(RangeTombstoneMarker marker)
    {
        RangeTombstoneMarker purged;
        if (marker.isBoundary())
        {
            RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
            boolean purgeClose = purger.shouldPurge(boundary.closeDeletionTime(reversed));
            boolean purgeOpen = purger.shouldPurge(boundary.openDeletionTime(reversed));
            if (purgeClose && purgeOpen)
                return;
            purged = purgeClose ? boundary.createCorrespondingOpenMarker(reversed)
                                : purgeOpen ? boundary.createCorrespondingCloseMarker(reversed) : marker;
        }
        else
        {
            if (purger.shouldPurge(((RangeTombstoneBoundMarker) marker).deletionTime()))
                return;
            purged = marker;
        }
        anyPurgeSurvivor = true;
        if (scanGuard != null)
            scanGuard.marker(purged.clustering());
        if (dropEverything)
        {
            stopped = true;
            return;
        }
        // markers pass the row filter and the limit counter unchanged
        anyFilterSurvivor = true;
        writeToResponse(purged);
    }

    private void writeToResponse(RangeTombstoneMarker marker)
    {
        try
        {
            writer.writeMarker(marker);
        }
        catch (IOException e)
        {
            throw new RuntimeException("response write failed", e);
        }
        wroteUnfiltered = true;
        openOutputMarker = marker.isOpen(reversed) ? marker.openDeletionTime(reversed) : null;
        lastRowWritten = false;
    }

    // ---------------------------------------------------------------- cancellation

    /** {@code QueryCancellationChecker}: aborts a read whose command was cancelled.  The approximate
     *  clock moves every few milliseconds, so most rows skip the check. */
    private void maybeCancel()
    {
        if (command == null)
            return;
        long now = approxTime.now();
        if (lastCancellationCheck == now)
            return;
        lastCancellationCheck = now;
        if (command.isAborted())
            throw new QueryCancelledException(command);
    }

    // ---------------------------------------------------------------- helpers

    /** The value of a filtered cell, read once to evaluate the filter, replayed to the writer. */
    private final class StagedValueSource implements CellValueSource
    {
        @Override
        public boolean hasValue()
        {
            return filterValue.length() > 0;
        }

        @Override
        public void streamValue(DataOutputPlus dest) throws IOException
        {
            filterValue.streamTo(dest);
        }

        @Override
        public byte[] materialize()
        {
            return filterValue.toValueArray();
        }
    }

    /**
     * The bytes a response buffer allocates for a response of {@code length} bytes once its
     * thread's chunks are reused: the exact-size copy, plus the chunks past the ones a thread keeps.
     */
    @VisibleForTesting
    public static long responseBufferAllocation(int length)
    {
        int chunks = (length + ResponseBuffer.CHUNK_SIZE - 1) / ResponseBuffer.CHUNK_SIZE;
        return length + (long) Math.max(0, chunks - ResponseBuffer.CHUNKS_KEPT) * ResponseBuffer.CHUNK_SIZE;
    }

    /**
     * The response buffer, which can drop what it holds past a position so a partition's header can
     * be rewritten or the partition removed.  It writes into fixed-size chunks that each thread
     * reuses from read to read, so it never grows and copies an array, and {@link #buffer} copies
     * the response once into an array of its exact size.
     */
    static final class ResponseBuffer extends DataOutputBuffer
    {
        static final int CHUNK_SIZE = 64 << 10;
        /** Chunks kept per thread for the next read; more are allocated when a response needs them. */
        static final int CHUNKS_KEPT = 16;
        private static final FastThreadLocal<ArrayDeque<ByteBuffer>> CHUNKS = new FastThreadLocal<>()
        {
            @Override
            protected ArrayDeque<ByteBuffer> initialValue()
            {
                return new ArrayDeque<>();
            }
        };

        /** The filled chunks before the current one, each flipped: position 0, limit at its end. */
        private final List<ByteBuffer> filled = new ArrayList<>();
        private int filledLength;

        ResponseBuffer()
        {
            super(takeChunk(CHUNK_SIZE));
        }

        private static ByteBuffer takeChunk(int atLeast)
        {
            if (atLeast > CHUNK_SIZE)
                return ByteBuffer.allocate(atLeast);
            ByteBuffer chunk = CHUNKS.get().pollFirst();
            return chunk == null ? ByteBuffer.allocate(CHUNK_SIZE) : chunk;
        }

        private static void returnChunk(ByteBuffer chunk)
        {
            ArrayDeque<ByteBuffer> kept = CHUNKS.get();
            if (chunk.capacity() == CHUNK_SIZE && kept.size() < CHUNKS_KEPT)
            {
                chunk.clear();
                kept.addFirst(chunk);
            }
        }

        /** Moves to a new chunk with room for at least {@code count} bytes. */
        @Override
        protected void expandToFit(long count)
        {
            if (count <= 0)
                return;
            buffer.flip();
            filled.add(buffer);
            filledLength += buffer.limit();
            buffer = takeChunk(Math.toIntExact(count));
        }

        @Override
        public void readFully(DataInputPlus in, int length) throws IOException
        {
            while (length > 0)
            {
                if (!buffer.hasRemaining())
                    expandToFit(1);
                int chunk = Math.min(length, buffer.remaining());
                in.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(), chunk);
                buffer.position(buffer.position() + chunk);
                length -= chunk;
            }
        }

        @Override
        public int getLength()
        {
            return filledLength + buffer.position();
        }

        @Override
        public byte[] getData()
        {
            throw new UnsupportedOperationException("the response buffer is not one array");
        }

        void truncate(int length)
        {
            while (length < filledLength)
            {
                returnChunk(buffer);
                buffer = filled.remove(filled.size() - 1);
                filledLength -= buffer.limit();
                buffer.limit(buffer.capacity());
                buffer.position(buffer.limit());
            }
            buffer.position(length - filledLength);
        }

        /** The response, copied into an array of its exact size; the chunks go back for reuse. */
        @Override
        public ByteBuffer buffer(boolean duplicate)
        {
            byte[] bytes = new byte[getLength()];
            int offset = 0;
            for (ByteBuffer chunk : filled)
            {
                System.arraycopy(chunk.array(), chunk.arrayOffset(), bytes, offset, chunk.limit());
                offset += chunk.limit();
                returnChunk(chunk);
            }
            System.arraycopy(buffer.array(), buffer.arrayOffset(), bytes, offset, buffer.position());
            returnChunk(buffer);
            filled.clear();
            filledLength = 0;
            buffer = null;
            return ByteBuffer.wrap(bytes);
        }
    }

    /**
     * {@code RowFilter.filter} for one partition.  Partition-level expressions (static and
     * partition key columns) are evaluated on the purged static row.  Row-level expressions are
     * evaluated on the streamed cells when they are all {@code SimpleExpression}s comparing a
     * clustering column or a simple, non-counter regular column ({@link #streamable}); otherwise
     * on each built row with the real {@code isSatisfiedBy}.  With any expression, a row without
     * live data is dropped, unless the filter needs reconciliation (then the row is not purged
     * first).
     */
    static final class ResponseFilter
    {
        private final org.apache.cassandra.schema.TableMetadata metadata;
        private final long nowInSec;
        private final boolean needsReconciliation;
        private final RowFilter.Expression[] partitionLevel;
        private final RowFilter.Expression[] rowLevel;
        final boolean streamable;
        // streamable form: the clustering expressions, and the regular ones by distinct column
        private final RowFilter.Expression[] clusteringExpressions;
        private final ColumnMetadata[] regularColumns;
        private final RowFilter.Expression[][] regularExpressionsByColumn;
        private DecoratedKey key;

        // per-row state of the streamable form
        private boolean rowFailed;
        private int satisfiedColumns;

        ResponseFilter(SinglePartitionReadCommand command)
        {
            this.metadata = command.metadata();
            this.nowInSec = command.nowInSec();
            RowFilter rowFilter = command.rowFilter();
            this.needsReconciliation = rowFilter.needsReconciliation();
            List<RowFilter.Expression> partition = new ArrayList<>();
            List<RowFilter.Expression> row = new ArrayList<>();
            for (RowFilter.Expression e : rowFilter.getExpressions())
            {
                if (e.column().isStatic() || e.column().isPartitionKey())
                    partition.add(e);
                else
                    row.add(e);
            }
            this.partitionLevel = partition.toArray(new RowFilter.Expression[0]);
            this.rowLevel = row.toArray(new RowFilter.Expression[0]);

            boolean canStream = true;
            List<RowFilter.Expression> clustering = new ArrayList<>();
            List<ColumnMetadata> columns = new ArrayList<>();
            for (RowFilter.Expression e : rowLevel)
            {
                canStream &= streamable(e);
                if (e.column().kind == ColumnMetadata.Kind.CLUSTERING)
                    clustering.add(e);
                else if (indexOf(columns, e.column()) < 0)
                    columns.add(e.column());
            }
            this.streamable = canStream;
            this.clusteringExpressions = clustering.toArray(new RowFilter.Expression[0]);
            this.regularColumns = columns.toArray(new ColumnMetadata[0]);
            this.regularExpressionsByColumn = new RowFilter.Expression[regularColumns.length][];
            for (int i = 0; i < regularColumns.length; i++)
            {
                List<RowFilter.Expression> onColumn = new ArrayList<>();
                for (RowFilter.Expression e : rowLevel)
                {
                    if (e.column().kind != ColumnMetadata.Kind.CLUSTERING && CursorReads.sameColumn(e.column(), regularColumns[i]))
                        onColumn.add(e);
                }
                regularExpressionsByColumn[i] = onColumn.toArray(new RowFilter.Expression[0]);
            }
        }

        /** A simple comparison of a clustering column, or of a simple non-counter regular column,
         *  that reduces to {@code operator.isSatisfiedBy(type, value, expressionValue)}. */
        private static boolean streamable(RowFilter.Expression e)
        {
            if (e.getClass() != RowFilter.SimpleExpression.class)
                return false;
            ColumnMetadata column = e.column();
            if (column.isComplex() || column.type.isCounter())
                return false;
            if (column.kind != ColumnMetadata.Kind.CLUSTERING && column.kind != ColumnMetadata.Kind.REGULAR)
                return false;
            return e.operator().appliesToColumnValues()
                   || e.operator().appliesToCollectionElements()
                   || e.operator().appliesToMapKeys();
        }

        private static int indexOf(List<ColumnMetadata> columns, ColumnMetadata column)
        {
            for (int i = 0; i < columns.size(); i++)
            {
                if (CursorReads.sameColumn(columns.get(i), column))
                    return i;
            }
            return -1;
        }

        boolean hasRowLevelExpressions()
        {
            return rowLevel.length > 0;
        }

        /** Whether the streamed form needs each row's clustering. */
        boolean filtersClustering()
        {
            return clusteringExpressions.length > 0;
        }

        /** {@code RowFilter.filter}'s partition check, on the purged static row. */
        boolean partitionMatches(DecoratedKey key, Row staticRow)
        {
            this.key = key;
            for (RowFilter.Expression e : partitionLevel)
            {
                if (!e.isSatisfiedBy(metadata, key, staticRow, nowInSec))
                    return false;
            }
            return true;
        }

        /** {@code RowFilter.filter}'s row check on a purged row object. */
        boolean rowMatches(Row row)
        {
            Row purged = needsReconciliation ? row : row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
            if (purged == null)
                return false;
            for (RowFilter.Expression e : rowLevel)
            {
                if (!e.isSatisfiedBy(metadata, key, purged, nowInSec))
                    return false;
            }
            return true;
        }

        // ---- streamable form ----

        /** @param clustering the row's clustering, or null when {@link #filtersClustering()} is false */
        void startRow(Clustering<?> clustering)
        {
            rowFailed = false;
            satisfiedColumns = 0;
            for (RowFilter.Expression e : clusteringExpressions)
            {
                ByteBuffer value = clustering.bufferAt(e.column().position());
                if (value == null || !e.operator().isSatisfiedBy(e.column().type, value, e.getIndexValue()))
                {
                    rowFailed = true;
                    return;
                }
            }
        }

        boolean filtersColumn(ColumnMetadata column)
        {
            for (ColumnMetadata c : regularColumns)
            {
                if (CursorReads.sameColumn(c, column))
                    return true;
            }
            return false;
        }

        /** The purged cell of a filtered simple column.  {@code value} is null when the cell is not
         *  live, which fails its expressions like {@code getValue} returning null.  The value is a
         *  view over reusable storage, used only during this call. */
        void cellValue(ColumnMetadata column, ByteBuffer value)
        {
            if (rowFailed)
                return;
            if (value == null)
            {
                rowFailed = true;
                return;
            }
            for (int i = 0; i < regularColumns.length; i++)
            {
                if (!CursorReads.sameColumn(regularColumns[i], column))
                    continue;
                for (RowFilter.Expression e : regularExpressionsByColumn[i])
                {
                    if (!e.operator().isSatisfiedBy(e.column().type, value, e.getIndexValue()))
                    {
                        rowFailed = true;
                        return;
                    }
                }
                satisfiedColumns++;
                return;
            }
        }

        /** The streamed row's verdict: it has live data (unless the filter needs reconciliation),
         *  and every expression passed, each filtered column having a live cell. */
        boolean streamedRowMatches(boolean rowHasLiveData)
        {
            if (!needsReconciliation && !rowHasLiveData)
                return false;
            return !rowFailed && satisfiedColumns == regularColumns.length;
        }
    }
}
