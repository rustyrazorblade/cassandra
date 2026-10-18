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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.NativeCell;
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
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.memory.MemoryUtil;

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
 * <p>The row filter is evaluated on the streamed cells ({@link ResponseFilter}), with no row
 * built: simple, counter, frozen and non-frozen collection columns, and clustering columns.
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
    /** Checked for cancellation; null in the test-only constructor. */
    private final ReadCommand command;
    private long lastCancellationCheck;

    /** The iterator path would see one source (one leg and no absent-partition sstable). */
    boolean singleSource;
    /** A names read the iterator path reads in timestamp order, into a partition that filters the static row. */
    boolean namesRead;
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
    /** The row holds a complex column with no cells. */
    private boolean rowHasColumnWithNoCells;
    /** The purged row has a deletion, {@code Row.hasDeletion}: a row, complex or cell tombstone,
     *  or expired liveness. */
    private boolean rowHasDeletion;

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
        this.command = command;
        this.slices = slices;
        if (scanGuard != null)
            scanGuard.rowClusteringSource(this::rowClustering);
        // a paged read resumed after its last row has no slice: only the partition header is written
        if (!slices.isEmpty())
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
     * The static row of a names read, as the iterator path's timestamp-order read returns it from
     * its partition ({@code AbstractBTreePartition.staticRow}): an empty static row is dropped
     * unless the partition is deleted, and the row is filtered against the partition deletion.
     */
    private Row namesStaticRow(Row mergedStatic, DeletionTime mergedDeletion)
    {
        if (columnFilter.fetchedColumns().statics.isEmpty() || (mergedStatic.isEmpty() && mergedDeletion.isLive()))
            return Rows.EMPTY_STATIC_ROW;
        Row filtered = mergedStatic.filter(columnFilter, mergedDeletion, false, command.metadata());
        return filtered == null ? Rows.EMPTY_STATIC_ROW : filtered;
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
        staticRow = purgeAndCountStaticRow(namesRead ? namesStaticRow(mergedStatic, mergedDeletion) : mergedStatic);
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
        LivenessInfo liveness = purger.shouldPurge(mergedLiveness, nowInSec) ? LivenessInfo.EMPTY : mergedLiveness;
        DeletionTime deletion = purger.shouldPurge(mergedRowDeletion) ? DeletionTime.LIVE : mergedRowDeletion;
        rowHasContent = !liveness.isEmpty() || !deletion.isLive();
        rowLive = liveness.isLive(nowInSec);
        rowHasColumnWithNoCells = false;
        rowHasDeletion = !deletion.isLive() || (liveness.isExpiring() && nowInSec >= liveness.localExpirationTime());
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
        if (mergedComplexDeletion.isLive())
        {
            // the merge announces a live deletion only for a column the row keeps with no cells;
            // ComplexColumnData.purge keeps that column, so the row keeps it too
            rowHasContent = true;
            rowHasColumnWithNoCells = true;
            if (scanGuard != null)
                scanGuard.columnWithNoCells();
            if (filter != null)
                filter.complexColumnWritten(column);
            writeComplexDeletion(column, DeletionTime.LIVE);
            return;
        }
        DeletionTime deletion = purger.shouldPurge(mergedComplexDeletion) ? DeletionTime.LIVE : mergedComplexDeletion;
        if (deletion.isLive())
            // a purged deletion with no surviving cell leaves no column, so it is not announced;
            // the column is opened lazily by its first surviving cell instead
            return;
        rowHasContent = true;
        rowHasDeletion = true;
        if (scanGuard != null)
            scanGuard.complexDeletion();
        if (filter != null)
            filter.complexColumnWritten(column);
        writeComplexDeletion(column, deletion);
    }

    private void writeComplexDeletion(ColumnMetadata column, DeletionTime deletion)
    {
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
        Cell<?> purged = cell.purge(purger, nowInSec);
        if (purged == null)
            // fully purged: dropped from the response entirely
            return;
        rowHasContent = true;
        boolean live = purged.isLive(nowInSec);
        rowLive |= live && !enforceStrictLiveness;
        rowHasDeletion |= !live;
        if (scanGuard != null)
            scanGuard.cell(purged.timestamp(), purged.ttl(), purged.localDeletionTime());
        if (filter != null)
            filter.cell(purged.column(), purged.path(), purged.buffer(), live);
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
        rowHasDeletion |= !live;
        if (scanGuard != null)
            scanGuard.cell(timestamp, ttl, localDeletionTime);
        boolean hasValue = !converted && source.hasValue();
        if (filter != null && filter.filtersColumn(column))
        {
            // the value is needed twice: once to evaluate, once to write
            filterValue.clear();
            if (hasValue)
                source.streamValue(filterValue);
            filter.cell(column, path, filterValue.valueWindow(), live);
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
        if (!rowHasContent)
        {
            // purged to nothing: PurgeFunction drops it before anything else sees it
            writer.abandonRow();
            return;
        }
        anyPurgeSurvivor = true;
        if (rowHasColumnWithNoCells && !rowLive)
            rowLive = liveByColumnWithNoCells();
        if (scanGuard != null)
            scanGuard.endRow();
        if (dropEverything)
        {
            writer.abandonRow();
            stopped = true;
            return;
        }
        if (filter != null && !filter.streamedRowMatches(survivesPurgeAll(), !rowHasDeletion))
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

    /**
     * Whether {@code row.purge(DeletionPurger.PURGE_ALL, ...)}, which {@code RowFilter.filter}
     * runs before its expressions unless the filter needs reconciliation, leaves a row: a row with
     * no deletion is returned as is, any other keeps only its live liveness and live cells.
     */
    private boolean survivesPurgeAll()
    {
        return !rowHasDeletion || rowLive;
    }

    /**
     * {@code BTreeRow.hasLiveData} for a row with a complex column with no cells and no live cell:
     * a row with no deletion has live data if it holds any column.  Every cell of the row is dead,
     * so each other column holds a dead cell or a complex deletion, and either is a deletion.
     */
    private boolean liveByColumnWithNoCells()
    {
        if (enforceStrictLiveness)
            return false;
        LivenessInfo liveness = writer.rowLiveness();
        boolean hasDeletion = !writer.rowDeletion().isLive()
                              || (liveness.isExpiring() && nowInSec >= liveness.localExpirationTime())
                              || writer.rowColumnCount() > writer.rowColumnsWithNoCells();
        return !hasDeletion;
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

    /**
     * The row filter's verdict on a merged row the merge reads as stored, before its cells are
     * read: {@link #acceptRow} up to the filter, for a row the purge leaves unchanged.  A row the
     * filter keeps, or that the purge changes, is left to the merge, which gives it cell by cell.
     */
    @Override
    public boolean dropsRowByFilter(Row row)
    {
        if (filter == null || !filter.hasRowLevelExpressions() || !(row instanceof BTreeRow) || row.isEmpty())
            return false;
        // the row is its own purge, so no purged copy is made
        if (row.hasDeletion(nowInSec) && !purgeKeepsRow((BTreeRow) row))
            return false;
        if (filter.rowObjectMatches(row))
            return false;
        // what startRow and endRow do for a row the filter then rejects
        if (!enterSlice())
            return true;
        maybeCancel();
        anyPurgeSurvivor = true;
        if (scanGuard != null)
            scanGuard.wholeRow(row);
        if (dropEverything)
            stopped = true;
        return true;
    }

    /** Whether {@code row.purge(purger, nowInSec, false)} purges nothing and turns no expired cell into a tombstone. */
    private boolean purgeKeepsRow(BTreeRow row)
    {
        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        if ((!liveness.isEmpty() && purger.shouldPurge(liveness, nowInSec)) || purger.shouldPurge(row.deletion().time()))
            return false;
        for (int i = 0, count = row.columnCount(); i < count; i++)
        {
            ColumnData data = row.columnDataByIndex(i);
            if (!data.column().isComplex())
            {
                if (!purgeKeepsCell((Cell<?>) data))
                    return false;
                continue;
            }
            ComplexColumnData complex = (ComplexColumnData) data;
            if (purger.shouldPurge(complex.complexDeletion()))
                return false;
            for (int j = 0; j < complex.cellsCount(); j++)
            {
                if (!purgeKeepsCell(complex.getCellByIndex(j)))
                    return false;
            }
        }
        return true;
    }

    /** Whether {@code AbstractCell.purge} returns the cell itself. */
    private boolean purgeKeepsCell(Cell<?> cell)
    {
        return cell.isLive(nowInSec) || (!cell.isExpiring() && !purger.shouldPurge(cell.timestamp(), cell.localDeletionTime()));
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
        if (filter != null && !filter.rowObjectMatches(purged))
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
        // the open deletion follows every raw marker, admitted or not, purged or not
        DeletionTime rawOpenAfter = marker.isOpen(reversed) ? CursorReads.copyOf(marker.openDeletionTime(reversed)) : null;
        if (!pastSliceStart && rawOpenAfter != null && comparator.compare(marker.clustering(), sliceStart) == 0)
        {
            // A deletion that opens exactly at the slice start: the slicing iterator returns its
            // open marker at once, before it reads anything in the slice.  Writing it only when
            // the next element arrives would read one more element from each leg.
            openMarker = rawOpenAfter;
            pastSliceStart = true;
            noteNonEmpty();
            if (!stopped)
                purgeAndWriteMarker(new RangeTombstoneBoundMarker(sliceStart, openMarker));
            return;
        }
        boolean admitted = admitOrSkip(marker.clustering());
        // once the limit stops the read the iterator path emits nothing more; RTBoundCloser
        // closes an open deletion at the last row in finishPartition
        if (admitted && !stopped)
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

    /** Test only: fill each new, not zeroed response chunk with this byte, so a byte the response
     *  never wrote shows; 0 for off. */
    @VisibleForTesting
    public static volatile byte TEST_POISON_NEW_CHUNKS = 0;
    /** Test only: allocate response chunks the usual, zeroed way, as when the JVM does not export
     *  the uninitialized allocation. */
    @VisibleForTesting
    public static volatile boolean TEST_ZEROED_CHUNKS_ONLY = false;

    /** Whether new response chunks skip the JVM's zeroing. */
    @VisibleForTesting
    public static boolean uninitializedChunksAvailable()
    {
        return ResponseBuffer.ALLOCATE_UNINITIALIZED != null;
    }

    /**
     * The bytes a response buffer allocates for a response of {@code length} bytes, read after
     * read: the chunks it hands over plus the exact copy of the last, which add up to the length.
     */
    @VisibleForTesting
    public static long responseBufferAllocation(int length)
    {
        return length;
    }

    /**
     * The response buffer, which can drop what it holds past a position so a partition's header can
     * be rewritten or the partition removed.  It writes into fixed-size chunks that each thread
     * reuses from read to read, so it never grows and copies an array, and {@link #buffer} copies
     * the response once into an array of its exact size.
     */
    static final class ResponseBuffer extends DataOutputBuffer implements ResponseWireWriter.PatchableOutput
    {
        static final int CHUNK_SIZE = 64 << 10;
        /** {@code jdk.internal.misc.Unsafe.allocateUninitializedArray}, which the server's JVM
         *  options export; null when it is not reachable, and chunks are then zeroed as usual. */
        private static final MethodHandle ALLOCATE_UNINITIALIZED = uninitializedArrayAllocator();

        private static MethodHandle uninitializedArrayAllocator()
        {
            try
            {
                Class<?> unsafe = Class.forName("jdk.internal.misc.Unsafe");
                Object instance = unsafe.getMethod("getUnsafe").invoke(null);
                return MethodHandles.lookup()
                                    .findVirtual(unsafe, "allocateUninitializedArray", MethodType.methodType(Object.class, Class.class, int.class))
                                    .bindTo(instance);
            }
            catch (Throwable t)
            {
                return null;
            }
        }
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
                return newChunk(atLeast);
            ByteBuffer chunk = CHUNKS.get().pollFirst();
            return chunk == null ? newChunk(CHUNK_SIZE) : chunk;
        }

        /**
         * A chunk the JVM does not zero: every chunk is written in full before it is handed over,
         * except the last, which {@link #parts} copies to its exact size, so no unwritten byte is
         * ever reachable.  Zeroing was a large share of a long scan's time.
         */
        private static ByteBuffer newChunk(int size)
        {
            if (ALLOCATE_UNINITIALIZED == null || TEST_ZEROED_CHUNKS_ONLY)
                return ByteBuffer.allocate(size);
            byte[] chunk;
            try
            {
                chunk = (byte[]) (Object) ALLOCATE_UNINITIALIZED.invokeExact(byte.class, size);
            }
            catch (Throwable t)
            {
                throw new AssertionError(t);
            }
            if (TEST_POISON_NEW_CHUNKS != 0)
                Arrays.fill(chunk, TEST_POISON_NEW_CHUNKS);
            return ByteBuffer.wrap(chunk);
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

        @Override
        public void patchByte(int position, int value)
        {
            if (position >= filledLength)
            {
                buffer.put(position - filledLength, (byte) value);
                return;
            }
            int chunkStart = 0;
            for (ByteBuffer chunk : filled)
            {
                if (position < chunkStart + chunk.limit())
                {
                    chunk.put(position - chunkStart, (byte) value);
                    return;
                }
                chunkStart += chunk.limit();
            }
        }

        @Override
        public void insert(int position, byte[] bytes, int offset, int length) throws IOException
        {
            int end = getLength();
            byte[] tail = new byte[end - position];
            int copied = 0;
            int chunkStart = 0;
            for (ByteBuffer chunk : filled)
            {
                copied += copyOverlap(chunk.array(), chunk.arrayOffset(), chunkStart, chunk.limit(), position, tail, copied);
                chunkStart += chunk.limit();
            }
            copyOverlap(buffer.array(), buffer.arrayOffset(), chunkStart, buffer.position(), position, tail, copied);
            truncate(position);
            write(bytes, offset, length);
            write(tail, 0, tail.length);
        }

        /** Copies the part of a chunk at or past {@code from} into {@code into}; returns the bytes copied. */
        private static int copyOverlap(byte[] chunk, int arrayOffset, int chunkStart, int chunkLength, int from, byte[] into, int intoOffset)
        {
            int skip = Math.max(0, from - chunkStart);
            int length = chunkLength - skip;
            if (length <= 0)
                return 0;
            System.arraycopy(chunk, arrayOffset + skip, into, intoOffset, length);
            return length;
        }

        @Override
        public void truncate(int length)
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

        /**
         * The response.  One that fits in its first chunk is copied into an array of its exact
         * size and the chunk goes back for reuse; a larger one is handed over as its chunks,
         * which the response keeps, so it is never copied.
         */
        ByteBuffer[] parts()
        {
            if (filled.isEmpty())
            {
                byte[] bytes = Arrays.copyOfRange(buffer.array(), buffer.arrayOffset(), buffer.arrayOffset() + buffer.position());
                returnChunk(buffer);
                buffer = null;
                return new ByteBuffer[]{ ByteBuffer.wrap(bytes) };
            }
            ByteBuffer[] parts = new ByteBuffer[filled.size() + 1];
            filled.toArray(parts);
            // the last chunk is partly written: hand over an exact copy and keep the chunk
            parts[parts.length - 1] = ByteBuffer.wrap(Arrays.copyOfRange(buffer.array(), buffer.arrayOffset(),
                                                                         buffer.arrayOffset() + buffer.position()));
            returnChunk(buffer);
            filled.clear();
            filledLength = 0;
            buffer = null;
            return parts;
        }

        @Override
        public ByteBuffer buffer(boolean duplicate)
        {
            throw new UnsupportedOperationException("the response buffer is read through parts()");
        }
    }

    /**
     * {@code RowFilter.filter} for one partition, evaluated on the streamed cells, with no row
     * built.  Partition-level expressions (static and partition key columns) are evaluated on the
     * purged static row with the real {@code isSatisfiedBy}.  Each row-level expression is a
     * {@link StreamedExpression}: an evaluator that is given the cells of its column as the row
     * streams, and gives the verdict {@code isSatisfiedBy} gives on the row object.
     * <p>
     * Unless the filter needs reconciliation, {@code RowFilter.filter} evaluates the row purged of
     * every tombstone ({@code DeletionPurger.PURGE_ALL}): a row with no deletion is unchanged, any
     * other keeps only its live liveness and live cells, and a row left with nothing fails.  With
     * reconciliation the expressions see every cell of the response row, tombstones included,
     * with their empty values.
     */
    static final class ResponseFilter
    {
        private final TableMetadata metadata;
        private final long nowInSec;
        private final boolean needsReconciliation;
        private final RowFilter.Expression[] partitionLevel;
        private final boolean hasRowLevel;
        private final StreamedExpression[] clusteringExpressions;
        /** The filtered regular columns, each with its expressions. */
        private final FilteredColumn[] columns;
        private DecoratedKey key;

        // per-row state
        private boolean rowFailed;
        /** Direct buffers re-pointed at the value and path of a cell in native memory. */
        private final ByteBuffer nativeValue = MemoryUtil.getHollowDirectByteBuffer(ByteOrder.BIG_ENDIAN);
        private final ByteBuffer nativePath = MemoryUtil.getHollowDirectByteBuffer(ByteOrder.BIG_ENDIAN);
        private final CellPath nativePathCellPath = CellPath.create(nativePath);

        ResponseFilter(SinglePartitionReadCommand command)
        {
            this.metadata = command.metadata();
            this.nowInSec = command.nowInSec();
            RowFilter rowFilter = command.rowFilter();
            this.needsReconciliation = rowFilter.needsReconciliation();
            List<RowFilter.Expression> partition = new ArrayList<>();
            List<StreamedExpression> clustering = new ArrayList<>();
            List<ColumnMetadata> regularColumns = new ArrayList<>();
            List<List<StreamedExpression>> regularExpressions = new ArrayList<>();
            for (RowFilter.Expression e : rowFilter.getExpressions())
            {
                ColumnMetadata column = e.column();
                if (column.isStatic() || column.isPartitionKey())
                {
                    partition.add(e);
                    continue;
                }
                StreamedExpression streamed = StreamedExpression.of(e);
                if (column.kind == ColumnMetadata.Kind.CLUSTERING)
                {
                    clustering.add(streamed);
                    continue;
                }
                int index = indexOf(regularColumns, column);
                if (index < 0)
                {
                    regularColumns.add(column);
                    regularExpressions.add(new ArrayList<>());
                    index = regularColumns.size() - 1;
                }
                regularExpressions.get(index).add(streamed);
            }
            this.partitionLevel = partition.toArray(new RowFilter.Expression[0]);
            this.clusteringExpressions = clustering.toArray(new StreamedExpression[0]);
            this.columns = new FilteredColumn[regularColumns.size()];
            for (int i = 0; i < columns.length; i++)
                columns[i] = new FilteredColumn(regularColumns.get(i), regularExpressions.get(i).toArray(new StreamedExpression[0]));
            this.hasRowLevel = clusteringExpressions.length > 0 || columns.length > 0;
        }

        /**
         * Whether every row-level expression of the filter has a {@link StreamedExpression}.  A
         * user expression ({@code RowFilter.UserExpression}) is checked on a row object, so the
         * response is not written by this sink for it.
         */
        static boolean streamable(RowFilter rowFilter)
        {
            for (RowFilter.Expression e : rowFilter.getExpressions())
            {
                ColumnMetadata column = e.column();
                if (column.isStatic() || column.isPartitionKey())
                    continue;
                if (!StreamedExpression.supports(e))
                    return false;
            }
            return true;
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
            return hasRowLevel;
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

        /** @param clustering the row's clustering, or null when {@link #filtersClustering()} is false */
        void startRow(Clustering<?> clustering)
        {
            rowFailed = false;
            for (FilteredColumn column : columns)
                column.startRow();
            for (StreamedExpression e : clusteringExpressions)
            {
                // getValue on a clustering column: the clustering value, which may be null
                ByteBuffer value = clustering.bufferAt(e.column.position());
                if (value == null || !e.test(value))
                {
                    rowFailed = true;
                    return;
                }
            }
        }

        private FilteredColumn filtered(ColumnMetadata column)
        {
            for (FilteredColumn c : columns)
            {
                if (CursorReads.sameColumn(c.column, column))
                    return c;
            }
            return null;
        }

        boolean filtersColumn(ColumnMetadata column)
        {
            return filtered(column) != null;
        }

        /**
         * A cell of the purged response row.  {@code value} is the value written, empty for a
         * tombstone, and is a view over reusable storage, used only during this call.
         */
        void cell(ColumnMetadata column, CellPath path, ByteBuffer value, boolean live)
        {
            if (rowFailed)
                return;
            FilteredColumn filtered = filtered(column);
            if (filtered == null)
                return;
            if (!filtered.complex)
            {
                // getValue: null for a cell that is not live, which fails every expression
                if (!live)
                {
                    rowFailed = true;
                    return;
                }
                for (StreamedExpression e : filtered.expressions)
                {
                    if (!e.test(value))
                    {
                        rowFailed = true;
                        return;
                    }
                }
                filtered.inRow = true;
                return;
            }
            filtered.inRow = true;
            filtered.anyLiveCell |= live;
            // without reconciliation the expressions see the row purged of every tombstone
            if (live || needsReconciliation)
            {
                for (StreamedExpression e : filtered.expressions)
                    e.cell(path, value);
            }
        }

        /** The response row holds this complex column with a complex deletion, or with no cells. */
        void complexColumnWritten(ColumnMetadata column)
        {
            FilteredColumn filtered = filtered(column);
            if (filtered != null)
                filtered.inRow = true;
        }

        /**
         * The streamed row's verdict.
         *
         * @param survivesPurgeAll whether {@code PURGE_ALL} leaves a row, see {@link ResponseFilter}
         * @param noDeletion       the response row has no deletion, so {@code PURGE_ALL} leaves it unchanged
         */
        boolean streamedRowMatches(boolean survivesPurgeAll, boolean noDeletion)
        {
            if (!needsReconciliation && !survivesPurgeAll)
                return false;
            if (rowFailed)
                return false;
            for (FilteredColumn column : columns)
            {
                if (!column.complex)
                {
                    // getValue returned null: no live cell for the column
                    if (!column.inRow)
                        return false;
                    continue;
                }
                // whether the row the expressions see holds the column (getComplexColumnData != null)
                boolean present = needsReconciliation || noDeletion ? column.inRow : column.anyLiveCell;
                for (StreamedExpression e : column.expressions)
                {
                    if (!e.verdict(present))
                        return false;
                }
            }
            return true;
        }

        /**
         * {@code RowFilter.filter}'s row check on a row object the sink already purged (a memtable
         * row standing as the merged row, or a row of a lone object-backed source): its cells go
         * through the same evaluators, with no {@code Row.purge}.
         */
        boolean rowObjectMatches(Row purged)
        {
            startRow(clusteringExpressions.length > 0 ? purged.clustering() : null);
            boolean noDeletion = !purged.hasDeletion(nowInSec);
            for (FilteredColumn filtered : columns)
            {
                if (!filtered.complex)
                {
                    Cell<?> cell = purged.getCell(filtered.column);
                    if (cell != null)
                        cell(filtered.column, null, valueOf(cell), cell.isLive(nowInSec));
                    continue;
                }
                ComplexColumnData data = purged.getComplexColumnData(filtered.column);
                if (data == null)
                    continue;
                complexColumnWritten(filtered.column);
                for (int i = 0; i < data.cellsCount(); i++)
                {
                    Cell<?> cell = data.getCellByIndex(i);
                    cell(filtered.column, pathOf(cell), valueOf(cell), cell.isLive(nowInSec));
                }
            }
            boolean survivesPurgeAll = noDeletion || purged.hasLiveData(nowInSec, metadata.enforceStrictLiveness());
            return streamedRowMatches(survivesPurgeAll, noDeletion);
        }

        /** The cell's value; for a cell in native memory a view, valid until the next call. */
        private ByteBuffer valueOf(Cell<?> cell)
        {
            if (!(cell instanceof NativeCell))
                return cell.buffer();
            ((NativeCell) cell).pointAtValue(nativeValue);
            return nativeValue;
        }

        /** The cell's path; for a cell in native memory a view, valid until the next call. */
        private CellPath pathOf(Cell<?> cell)
        {
            if (!(cell instanceof NativeCell))
                return cell.path();
            ((NativeCell) cell).pointAtPath(nativePath);
            return nativePathCellPath;
        }
    }

    /** A filtered regular column, its expressions, and what the current row holds for it. */
    private static final class FilteredColumn
    {
        final ColumnMetadata column;
        final boolean complex;
        final StreamedExpression[] expressions;
        /** Simple: the column has a live cell that satisfied every expression.  Complex: the
         *  response row holds the column. */
        boolean inRow;
        /** Complex: the column has a live cell. */
        boolean anyLiveCell;

        FilteredColumn(ColumnMetadata column, StreamedExpression[] expressions)
        {
            this.column = column;
            this.complex = column.isComplex();
            this.expressions = expressions;
        }

        void startRow()
        {
            inRow = false;
            anyLiveCell = false;
            for (StreamedExpression e : expressions)
                e.startRow();
        }
    }

    /**
     * One row-level {@code RowFilter.Expression}, evaluated on the cells of its column as they
     * stream instead of on a row object.  For a clustering or simple column it tests the live
     * value; for a complex column it is given each cell the expression sees and gives its verdict
     * at the end of the row.
     */
    abstract static class StreamedExpression
    {
        final ColumnMetadata column;

        StreamedExpression(ColumnMetadata column)
        {
            this.column = column;
        }

        /** Whether {@link #of} has an evaluator for {@code e}, a clustering or regular column expression. */
        static boolean supports(RowFilter.Expression e)
        {
            ColumnMetadata column = e.column();
            if (e.isMapElementExpression())
                return true;
            if (e.getClass() != RowFilter.SimpleExpression.class)
                return false;
            Operator op = e.operator();
            if (!column.isComplex())
                return op.appliesToColumnValues() || op.appliesToCollectionElements() || op.appliesToMapKeys();
            // a non-frozen collection takes only the element and key operators
            return column.type.isCollection() && !op.appliesToColumnValues()
                   && (op.appliesToCollectionElements() || op.appliesToMapKeys());
        }

        static StreamedExpression of(RowFilter.Expression e)
        {
            if (!supports(e))
                throw new IllegalArgumentException("no streamed form for " + e);
            ColumnMetadata column = e.column();
            Operator op = e.operator();
            if (e.isMapElementExpression())
            {
                // getIndexValue is the composite (key, value)
                ByteBuffer key = CompositeType.extractComponent(e.getIndexValue(), 0);
                ByteBuffer value = CompositeType.extractComponent(e.getIndexValue(), 1);
                return column.isComplex() ? new MapElement(column, key, op, value) : new FrozenMapElement(column, key, op, value);
            }
            ByteBuffer value = e.getIndexValue();
            if (column.isComplex())
            {
                CollectionType<?> type = (CollectionType<?>) column.type;
                boolean negated = op == Operator.NOT_CONTAINS || op == Operator.NOT_CONTAINS_KEY;
                // SetType.contains and CONTAINS KEY find the element by its cell path
                // (ComplexColumnData.getCell); ListType and MapType compare each cell value
                boolean byPath = op.appliesToMapKeys() || type.kind == CollectionType.Kind.SET;
                return new CollectionContains(column, byPath, negated, value);
            }
            if (column.type.isCounter() && op.appliesToColumnValues())
                return new CounterTotal(column, op, value);
            return new ValueComparison(column, op, value);
        }

        /** Clustering or simple column: whether the live value satisfies the expression. */
        boolean test(ByteBuffer value)
        {
            throw new UnsupportedOperationException();
        }

        void startRow()
        {
        }

        /** Complex column: a cell the expression sees. */
        void cell(CellPath path, ByteBuffer value)
        {
            throw new UnsupportedOperationException();
        }

        /** Complex column: the verdict once the row's cells are in.  {@code present}: the row the
         *  expression sees holds the column. */
        boolean verdict(boolean present)
        {
            throw new UnsupportedOperationException();
        }
    }

    /** {@code operator.isSatisfiedBy(column.type, value, expressionValue)}: a simple column,
     *  including a frozen collection, or a clustering column. */
    private static final class ValueComparison extends StreamedExpression
    {
        private final Operator operator;
        private final ByteBuffer value;

        ValueComparison(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column);
            this.operator = operator;
            this.value = value;
        }

        @Override
        boolean test(ByteBuffer found)
        {
            return operator.isSatisfiedBy(column.type, found, value);
        }
    }

    /** A counter is compared by the total of its context, as a {@code bigint} (CASSANDRA-11629). */
    private static final class CounterTotal extends StreamedExpression
    {
        private final Operator operator;
        private final ByteBuffer value;
        private final ByteBuffer total = ByteBuffer.allocate(Long.BYTES);

        CounterTotal(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column);
            this.operator = operator;
            this.value = value;
        }

        @Override
        boolean test(ByteBuffer found)
        {
            total.putLong(0, CounterContext.instance().total(found, ByteBufferAccessor.instance));
            return operator.isSatisfiedBy(LongType.instance, total, value);
        }
    }

    /** {@code m[k] = v} on a frozen map: the value of {@code k} in the serialized map. */
    private static final class FrozenMapElement extends StreamedExpression
    {
        private final MapType<?, ?> type;
        private final ByteBuffer key;
        private final Operator operator;
        private final ByteBuffer value;

        FrozenMapElement(ColumnMetadata column, ByteBuffer key, Operator operator, ByteBuffer value)
        {
            super(column);
            this.type = (MapType<?, ?>) column.type;
            this.key = key;
            this.operator = operator;
            this.value = value;
        }

        @Override
        boolean test(ByteBuffer serializedMap)
        {
            ByteBuffer found = type.getSerializer().getSerializedValue(serializedMap, key, type.getKeysType());
            return found != null && operator.isSatisfiedBy(type.getValuesType(), found, value);
        }
    }

    /** CONTAINS, NOT CONTAINS, CONTAINS KEY and NOT CONTAINS KEY on a non-frozen collection. */
    private static final class CollectionContains extends StreamedExpression
    {
        private final boolean byPath;
        private final boolean negated;
        private final Comparator<CellPath> pathComparator;
        private final CellPath path;
        private final AbstractType<?> valueComparator;
        private final ByteBuffer value;
        private boolean found;

        CollectionContains(ColumnMetadata column, boolean byPath, boolean negated, ByteBuffer value)
        {
            super(column);
            this.byPath = byPath;
            this.negated = negated;
            this.pathComparator = column.cellPathComparator();
            this.path = CellPath.create(value);
            this.valueComparator = ((CollectionType<?>) column.type).valueComparator();
            this.value = value;
        }

        @Override
        void startRow()
        {
            found = false;
        }

        @Override
        void cell(CellPath cellPath, ByteBuffer cellValue)
        {
            if (!found)
                found = byPath ? pathComparator.compare(cellPath, path) == 0 : valueComparator.compare(cellValue, value) == 0;
        }

        @Override
        boolean verdict(boolean present)
        {
            // complexData != null && operator.isSatisfiedBy(type, complexData, value)
            return present && found != negated;
        }
    }

    /**
     * {@code m[k] = v} on a non-frozen map: {@code row.getCell(column, CellPath.create(k))}, with
     * no liveness check, so a deleted element is found, with its empty value, when the row is not
     * purged first.
     */
    private static final class MapElement extends StreamedExpression
    {
        private final Comparator<CellPath> pathComparator;
        private final CellPath path;
        private final AbstractType<?> valuesType;
        private final Operator operator;
        private final ByteBuffer value;
        private boolean found;
        private boolean satisfied;

        MapElement(ColumnMetadata column, ByteBuffer key, Operator operator, ByteBuffer value)
        {
            super(column);
            this.pathComparator = column.cellPathComparator();
            this.path = CellPath.create(key);
            this.valuesType = ((MapType<?, ?>) column.type).getValuesType();
            this.operator = operator;
            this.value = value;
        }

        @Override
        void startRow()
        {
            found = false;
            satisfied = false;
        }

        @Override
        void cell(CellPath cellPath, ByteBuffer cellValue)
        {
            if (!found && pathComparator.compare(cellPath, path) == 0)
            {
                found = true;
                satisfied = operator.isSatisfiedBy(valuesType, cellValue, value);
            }
        }

        @Override
        boolean verdict(boolean present)
        {
            return found && satisfied;
        }
    }
}
