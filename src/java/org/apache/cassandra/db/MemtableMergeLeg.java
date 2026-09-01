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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;

/**
 * M2.3 (CASSANDRA-20428, Phase 3): the OBJECT-BACKED merge leg — the memtable adapter of the M2
 * design's Q2. Wraps the exact {@link UnfilteredRowIterator} the memtable leg contributes to the
 * object-level merge today ({@code memtable.rowIterator(...)}, {@code RTBoundValidator}-wrapped by
 * the call site) and presents each live {@code Row}/{@code Cell}/{@code RangeTombstoneMarker} as
 * the descriptor-shaped {@link CursorReads.MergeLeg} state {@code CursorReadMerger} consumes, so
 * the memtable joins the SAME cursor-level merge as the sstable legs and the post-merge
 * object-level merge disappears for fully-supported reads.
 *
 * <b>EnsureOnHeap / off-heap safety (design Q2, verified against current source)</b>: this adapter
 * sits ABOVE the wrapping the memtable partition already applies —
 * {@code AtomicBTreePartition.unfilteredIterator} returns
 * {@code allocator.ensureOnHeap().applyToPartition(...)}, which is {@code EnsureOnHeap.NOOP} for
 * {@code heap_buffers} ({@code HeapPool}) and {@code CloneToHeap} for {@code offheap_buffers}
 * ({@code SlabAllocator} with {@code allocateOnHeapOnly == false}) and {@code offheap_objects}
 * ({@code NativeAllocator}). Every row, cell, marker, static row and partition key this adapter
 * touches has therefore already been cloned to heap exactly where today's object path clones it —
 * off-heap lifetime safety is inherited byte-for-byte under all three configs, and the adapter
 * needs no {@code CloneToHeap} handling of its own.
 *
 * <b>Zero-copy properties</b> (the reason this adapter exists at all — see FINDING #7/#11's
 * lesson that per-row/per-value copies are where read-path allocation hides):
 * <ul>
 *   <li>Clustering comparison: each unfiltered's clustering is serialized ONCE into the reusable
 *       descriptor buffer (standard {@code serializeValuesWithoutSize} wire form, the same layout
 *       sstable descriptors hold), buying descriptor-vs-descriptor comparison through the existing
 *       shared {@code ClusteringComparator.compare} with zero steady-state allocation — one small
 *       memcpy per memtable unfiltered.</li>
 *   <li>{@link #materializeClusteringPrefix()} returns the row's OWN clustering object — no
 *       decode, no copy.</li>
 *   <li>{@link #existingCell()}/{@link #consumeExistingRow()}: memtable-won cells/rows are emitted
 *       as the already-live objects, mirroring the object merge ({@code Cells.reconcile} returns
 *       the winner object; {@code Row.Merger.merge}'s single-version fast path returns the row
 *       unchanged). Without this, M2.3 would make memtable-hot reads allocate MORE than the
 *       object path it replaces.</li>
 * </ul>
 *
 * <b>Filtering</b>: none is applied here, by design. The wrapped iterator's rows are already
 * column-filtered and shadow-filtered by {@code RowAndDeletionMergeIterator}
 * ({@code row.filter(selection, activeDeletion, ...)}) and clipped to the query slices — exactly
 * the stream today's object merge consumes. Notably this preserves the iterator path's asymmetry
 * for {@code canSkipValue} columns: memtable cells keep their full values while sstable legs
 * reconcile on EMPTY values, so value tie-breaks resolve identically to the object path.
 *
 * <b>Validation</b>: {@link #validateRowHeader()}/{@link #validateMarkerHeader()} are no-ops —
 * the iterator path applies {@code UnfilteredValidation} only to sstable-attributed data
 * ({@code AbstractSSTableIterator}/{@code SSTableIdentityIterator}); memtable data is never
 * validated there, so it must not be validated here either.
 *
 * <b>Range tombstones</b>: the memtable's markers (from {@code DeletionInfo}, including the
 * artificial slice-bound markers the pre-clipped stream carries) enter the merge's cross-leg
 * open-marker set as normal, always-current contributions — a memtable leg never seeks, so
 * {@link #mergeSeekOpenMarker()} is always null and the M2.2 seed machinery never applies to it.
 */
// M3.3a-i: public (was package-private) so the mergeLegsWithSink test seam
// (CursorReads.mergeLegsWithSink, @VisibleForTesting) can be driven with real memtable-adapter
// legs from the test package (org.apache.cassandra.db.cursorreads), exactly mirroring the already
// -public CursorReads.PendingLeg for sstable legs and CursorReads.openLeg's factory pattern. Pure
// visibility change; behavior and the (still package-private) CursorReads.MergeLeg contract are
// unchanged.
public final class MemtableMergeLeg implements CursorReads.MergeLeg
{
    private final UnfilteredRowIterator iter;
    private final Slices slices;
    private final MemtableDescriptor desc;
    private final List<AbstractType<?>> clusteringTypes;
    /** reusable wire-form staging for the descriptor's clustering bytes (grow-once) */
    private final DataOutputBuffer clusteringScratch = new DataOutputBuffer(64);
    /** reusable liveness view of the parked cell, reset per park */
    private final ReusableCellLivenessInfo parkedCellLiveness = new ReusableCellLivenessInfo();

    /** {@link SSTableCursorReader.State} vocabulary, per the MergeLeg contract. */
    private int state;
    private Unfiltered current;
    private Row currentRow;

    // ---- cell-walk state (valid while state == CELL_HEADER_START) ----
    private Iterator<ColumnData> columnDataIter;
    private Iterator<Cell<?>> complexCellIter;
    private ColumnMetadata parkedColumn;
    /** null at a deletion-only complex-column position ({@link #cellProduced()} == false) */
    private Cell<?> parkedCell;
    private DeletionTime parkedComplexDeletion = DeletionTime.LIVE;
    private boolean parked;

    public MemtableMergeLeg(UnfilteredRowIterator iter, Slices slices)
    {
        this.iter = iter;
        this.slices = slices;
        this.clusteringTypes = iter.metadata().comparator.subtypes();
        this.desc = new MemtableDescriptor(clusteringTypes.toArray(new AbstractType<?>[0]));
        advance();
    }

    private void advance()
    {
        if (iter.hasNext())
        {
            current = iter.next();
            state = current.isRow() ? ROW_START : TOMBSTONE_START;
        }
        else
        {
            current = null;
            currentRow = null;
            state = PARTITION_END;
        }
    }

    // ---------------------------------------------------------------- partition-level surface

    public DeletionTime partitionLevelDeletion()
    {
        return iter.partitionLevelDeletion();
    }

    public Row staticRow()
    {
        return iter.staticRow();
    }

    public Slices legSlices()
    {
        return slices;
    }

    public EncodingStats legStats()
    {
        // the same stats the memtable iterator reports to the object merge's EncodingStats.merge
        return iter.stats();
    }

    public SSTableReader sstableOrNull()
    {
        return null;
    }

    public void enterMergeMode()
    {
        // nothing to switch: the adapter is merge-shaped from construction
    }

    public void seekForMerge()
    {
        // memtable legs never seek: the stream is already in memory and pre-clipped to the slice
    }

    public DeletionTime mergeSeekOpenMarker()
    {
        return null;
    }

    // ---------------------------------------------------------------- unfiltered walk

    public int cursorState()
    {
        return state;
    }

    public UnfilteredDescriptor unfiltered()
    {
        return desc;
    }

    public void readUnfilteredHeader()
    {
        if (state == ROW_START)
        {
            currentRow = (Row) current;
            loadClusteringIntoDescriptor(currentRow.clustering(), clusteringTypes.size());
            LivenessInfo liveness = currentRow.primaryKeyLivenessInfo();
            long timestamp = liveness.timestamp();
            if (CursorReads.TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS && !liveness.isEmpty())
                timestamp += 1;
            desc.livenessInfo().reset(timestamp, liveness.ttl(), liveness.localExpirationTime());
            Row.Deletion deletion = currentRow.deletion();
            if (deletion.isLive())
                desc.deletionTime().resetLive();
            else
                desc.deletionTime().reset(deletion.time());
            columnDataIter = currentRow.iterator();
            complexCellIter = null;
            parked = false;
            parkedCell = null;
            parkedColumn = null;
            parkedComplexDeletion = DeletionTime.LIVE;
            state = CELL_HEADER_START;
        }
        else if (state == TOMBSTONE_START)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) current;
            currentRow = null;
            ClusteringPrefix<?> clustering = marker.clustering();
            loadClusteringIntoDescriptor(clustering, clustering.size());
            if (clustering.kind().isBoundary())
            {
                RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                // loadTombstone convention: deletionTime = CLOSE side, deletionTime2 = OPEN side
                desc.deletionTime().reset(boundary.closeDeletionTime(false));
                desc.deletionTime2().reset(boundary.openDeletionTime(false));
            }
            else
            {
                desc.deletionTime().reset(((RangeTombstoneBoundMarker) marker).deletionTime());
            }
            state = UNFILTERED_END;
        }
        else
        {
            throw new IllegalStateException("unexpected adapter state before header read: " + state);
        }
    }

    private void loadClusteringIntoDescriptor(ClusteringPrefix<?> clustering, int columnsBound)
    {
        clusteringScratch.clear();
        try
        {
            ClusteringPrefix.serializer.serializeValuesWithoutSize(clustering, clusteringScratch,
                                                                   MessagingService.current_version, clusteringTypes);
        }
        catch (IOException e)
        {
            throw new RuntimeException("serializing to an in-memory buffer cannot throw", e);
        }
        desc.loadObjectClustering(clustering.kind(), columnsBound,
                                  clusteringScratch.getData(), clusteringScratch.getLength());
    }

    public void continueReading()
    {
        if (state != UNFILTERED_END)
            throw new IllegalStateException("adapter asked to continue from state " + state);
        advance();
    }

    public ClusteringPrefix<?> materializeClusteringPrefix()
    {
        // the live object itself — zero decode, zero copy (the object path emits the same object)
        return current.clustering();
    }

    public byte[][] materializeBoundValues()
    {
        ClusteringPrefix<?> clustering = current.clustering();
        int size = clustering.size();
        if (size == 0)
            return NO_VALUES;
        byte[][] values = new byte[size][];
        for (int i = 0; i < size; i++)
        {
            ByteBuffer value = clustering.bufferAt(i);
            values[i] = value == null ? null : ByteBufferUtil.getArray(value);
        }
        return values;
    }

    private static final byte[][] NO_VALUES = new byte[0][];

    public Row consumeExistingRow()
    {
        // Caller (CursorReadMerger.mergeRowGroup) has verified the Row.Merger single-version
        // fast-path conditions; the current row stands as the merged row unchanged.
        Row row = currentRow;
        if (row == null)
            throw new IllegalStateException("consumeExistingRow outside a row group");
        currentRow = null;
        columnDataIter = null;
        complexCellIter = null;
        parked = false;
        state = UNFILTERED_END;
        return row;
    }

    // ---------------------------------------------------------------- cell walk

    public boolean parkedAtCellPosition()
    {
        return parked;
    }

    public boolean needsCellAdvance()
    {
        return !parked && state == CELL_HEADER_START;
    }

    public void ensureParkedAtCell(boolean validateCells)
    {
        // validateCells ignored: the iterator path never validates memtable data (class javadoc)
        if (parked || state != CELL_HEADER_START)
            return;
        for (;;)
        {
            if (complexCellIter != null)
            {
                if (complexCellIter.hasNext())
                {
                    parkCell(complexCellIter.next());
                    return;
                }
                complexCellIter = null;
            }
            if (!columnDataIter.hasNext())
            {
                state = UNFILTERED_END;
                return;
            }
            ColumnData columnData = columnDataIter.next();
            if (columnData.column().isSimple())
            {
                parkedColumn = columnData.column();
                parkedComplexDeletion = DeletionTime.LIVE;
                parkCell((Cell<?>) columnData);
                return;
            }
            ComplexColumnData complex = (ComplexColumnData) columnData;
            parkedColumn = complex.column();
            parkedComplexDeletion = complex.complexDeletion();
            if (complex.cellsCount() == 0)
            {
                if (parkedComplexDeletion.isLive())
                    continue; // fully empty column data: nothing to contribute (defensive)
                // deletion-only position: sorts before any cell of the same column in the merge
                parkedCell = null;
                parked = true;
                return;
            }
            complexCellIter = complex.iterator();
            // loop back to park at the column's first cell; its park carries the column deletion
        }
    }

    private void parkCell(Cell<?> cell)
    {
        parkedCell = cell;
        long timestamp = cell.timestamp();
        if (CursorReads.TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS)
            timestamp += 1;
        parkedCellLiveness.reset(timestamp, cell.ttl(), cell.localDeletionTime());
        parked = true;
    }

    public void advancePastCellPosition()
    {
        parked = false;
        parkedCell = null;
    }

    public boolean cellProduced()
    {
        return parkedCell != null;
    }

    public ColumnMetadata cellColumn()
    {
        return parkedColumn;
    }

    public CellLivenessInfo cellLiveness()
    {
        return parkedCellLiveness;
    }

    public ByteBuffer cellPathWindow()
    {
        // the live path buffer itself — zero copy, matching the design's cell-path answer
        return parkedCell.path().get(0);
    }

    public DeletionTime cellComplexDeletion()
    {
        return parkedComplexDeletion;
    }

    public CellPath cellPath()
    {
        return parkedCell.path();
    }

    public Cell<?> existingCell()
    {
        return parkedCell;
    }

    public byte[] cellValue()
    {
        // Unreachable in practice — existingCell() short-circuits emission for object-backed
        // winners — but kept correct: the raw value bytes, exactly what the byte-backed legs
        // produce (one defensive copy).
        return parkedCell == null ? org.apache.cassandra.utils.ByteArrayUtil.EMPTY_BYTE_ARRAY
                                  : ByteBufferUtil.getArray(parkedCell.buffer());
    }

    public void stageCellValue(DataOutputPlus scratch) throws IOException
    {
        // Tie-break staging: the value bytes are read NON-destructively off the live object (the
        // object stays intact for existingCell() emission). Heap buffers (the only kind that can
        // appear here — EnsureOnHeap has already cloned off-heap configs) write without copying.
        ByteBuffer value = parkedCell.buffer();
        if (value.hasArray())
        {
            scratch.write(value.array(), value.arrayOffset() + value.position(), value.remaining());
        }
        else
        {
            // correctness-first fallback; not expected post-EnsureOnHeap
            byte[] copy = ByteBufferUtil.getArray(value);
            scratch.write(copy, 0, copy.length);
        }
    }

    public void discardCellValue()
    {
        // nothing staged on a cursor: the live object needs no discard
    }

    public boolean cellHasValue()
    {
        // Unreachable in practice — existingCell() short-circuits emission for object-backed
        // winners before CursorReadMerger.mergeCellGroup ever asks (see cellValue()'s own note)
        // — but kept correct: mirrors Cell.Serializer's own hasValue definition (valueSize > 0).
        return parkedCell != null && parkedCell.valueSize() > 0;
    }

    // ---------------------------------------------------------------- validation (no-ops)

    public void validateRowHeader()
    {
        // the iterator path never applies UnfilteredValidation to memtable data — parity demands
        // the cursor path does not either (see class javadoc)
    }

    public void validateMarkerHeader()
    {
        // see validateRowHeader
    }

    @Override
    public void close()
    {
        iter.close();
    }

    /**
     * The adapter's reusable descriptor: an {@link UnfilteredDescriptor} loaded from live objects
     * instead of the data file. The subclass only needs the protected clustering mutation surface
     * ({@code clusteringKind(Kind)} is public; {@code clusteringColumnsBound}/{@code overwrite}
     * are protected) plus the public reusable liveness/deletion resets — no shared-code change,
     * the same trick {@code CursorReadMerger.SliceBoundDescriptor} already established.
     */
    private static final class MemtableDescriptor extends UnfilteredDescriptor
    {
        MemtableDescriptor(AbstractType<?>[] clusteringTypes)
        {
            super(clusteringTypes);
        }

        void loadObjectClustering(ClusteringPrefix.Kind kind, int columnsBound, byte[] wireBytes, int length)
        {
            clusteringKind(kind);
            clusteringColumnsBound = columnsBound;
            overwrite(wireBytes, length);
        }
    }
}
