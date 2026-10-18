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
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.MemtablePartitionData;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.NativeCell;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;

/**
 * A memtable merge leg that reads the partition's own content ({@link BTreePartitionData}: the row
 * tree, the deletion info and the static row), read once when the leg opens.  It gives the merge
 * what {@code memtable.rowIterator(key, slices, columns, reversed)} gives the iterator path, in the
 * same order, by the same rules, without the iterator:
 * <ul>
 *   <li>the slices in query order, each slice's rows ({@code BTree.slice}) merged with its range
 *       tombstones ({@code DeletionInfo.rangeIterator}) like {@code RowAndDeletionMergeIterator}
 *       (removeShadowedData), including the dropped range tombstones the partition deletion
 *       supersedes and the swallowed boundary that changes nothing (CASSANDRA-14894);</li>
 *   <li>each row filtered like {@code BTreeRow.filter(selection, activeDeletion, false, ...)} and
 *       {@code ComplexColumnData.filter}: fetched columns, dropped columns, data the active
 *       deletion shadows, cells not queried (dropped when older than the row, else with an empty
 *       value), and a row left with nothing dropped;</li>
 *   <li>the static row filtered with the partition deletion, like
 *       {@code AbstractBTreePartition.staticRow};</li>
 *   <li>the marker checks of {@code RTBoundValidator} at {@code Stage.MEMTABLE}.</li>
 * </ul>
 *
 * <p>For a sink that streams cells ({@code emitObjects == false}) the leg reads clusterings, cell
 * values and cell paths from the memtable's own storage: heap or slab buffers, or native memory
 * read through one reused direct buffer.  No row or cell object is made for a row; the static row
 * and the range tombstone markers are objects, one per partition and one per range.  The values
 * are only valid while the read holds the table's read order group, which the read execution
 * controller holds until the response is written.
 *
 * <p>For a sink that keeps row objects ({@code emitObjects == true}) each row is the iterator
 * path's own object: the filtered row cloned to heap with the memtable's {@code EnsureOnHeap}, and
 * its cells are reused by the merge ({@link #existingCell()}, {@link #consumeExistingRow()}).
 */
public final class NativeMemtableMergeLeg implements CursorReads.MergeLeg
{
    private final TableMetadata metadata;
    private final ColumnFilter selection;
    private final boolean reversed;
    private final boolean emitObjects;
    private final Comparator<Clusterable> order;

    // ---- the partition the leg reads, set by reopen ----
    private DecoratedKey partitionKey;
    private BTreePartitionData data;
    private Slices slices;
    private EnsureOnHeap ensureOnHeap;
    private DeletionTime partitionDeletion;
    private Row staticRow;
    private final AbstractType<?>[] clusteringTypes;
    private final MemtableMergeLeg.MemtableDescriptor desc;
    private final DataOutputBuffer clusteringScratch = new DataOutputBuffer(64);
    private final ReusableCellLivenessInfo parkedCellLiveness = new ReusableCellLivenessInfo();

    // ---- BTreeRow.filter's inputs that do not change within the read ----
    /** {@code !fetchesAllColumns(false) || !allFetchedColumnsAreQueried()} */
    private final boolean mayFilterColumns;
    private final Map<ByteBuffer, DroppedColumn> droppedColumns;
    private final Columns fetchedColumns;
    private final Columns queriedColumns;

    // ---- RowAndDeletionMergeIterator over the current slice ----
    private int slicesOpened;
    private boolean sliceOpen;
    private Iterator<Row> rows;
    private Iterator<RangeTombstone> ranges;
    private Row nextRow;
    private RangeTombstone nextRange;
    private RangeTombstone openRange;
    /** {@code RTBoundValidator}: the deletion the last marker opened, or null. */
    private DeletionTime validatorOpenDeletion;

    // ---- the current unfiltered ----
    private int state;
    /** The memtable's row, or for {@link #emitObjects} the filtered row cloned to heap. */
    private Row currentRow;
    private RangeTombstoneMarker currentMarker;
    /** The current row is filtered on the fly ({@code BTreeRow.filter} changes it); false for the
     *  rows of {@link #emitObjects}, which are filtered already. */
    private boolean rowFiltered;
    private boolean rowMayHaveShadowed;
    private DeletionTime rowActiveDeletion;
    private LivenessInfo rowLiveness;
    private Row.Deletion rowDeletion;

    // ---- cell walk (valid while state == CELL_HEADER_START) ----
    private int columnIndex;
    private int columnCount;
    private ComplexColumnData complexColumn;
    private int cellIndex;
    private DeletionTime complexDeletionOut;
    private ColumnFilter.Tester cellTester;
    private boolean queriedColumn;
    private DroppedColumn droppedColumn;
    private DeletionTime complexActiveDeletion;
    private boolean complexFiltered;
    private boolean complexColumnKept;

    private ColumnMetadata parkedColumn;
    /** null at a position with a complex deletion and no cell */
    private Cell<?> parkedCell;
    private boolean parkedValueSkipped;
    private DeletionTime parkedComplexDeletion = DeletionTime.LIVE;
    private boolean parkedKeepsColumnWithNoCells;
    private boolean parked;

    /** A direct buffer re-pointed at the path of a cell in native memory. */
    private final ByteBuffer pathView = MemoryUtil.getHollowDirectByteBuffer(ByteOrder.BIG_ENDIAN);
    private final CellPath pathViewCellPath = CellPath.create(pathView);
    private long rowsCloned;

    /**
     * @param emitObjects whether the merge's sink keeps row objects: then the leg gives the iterator
     *                    path's own (cloned) objects; else it gives cells from the memtable's storage
     */
    public NativeMemtableMergeLeg(TableMetadata metadata, MemtablePartitionData partition, ColumnFilter selection,
                                  Slices slices, boolean reversed, boolean emitObjects)
    {
        this(metadata, partition, selection, slices, false, reversed, emitObjects);
    }

    /**
     * @param names whether the read's filter names its clusterings ({@code ClusteringIndexNamesFilter}),
     *              which {@code slices} then holds one each: the objects are cloned as the
     *              memtable's names iterator clones them ({@link MemtablePartitionData#ensureOnHeapForNames})
     */
    public NativeMemtableMergeLeg(TableMetadata metadata, MemtablePartitionData partition, ColumnFilter selection,
                                  Slices slices, boolean names, boolean reversed, boolean emitObjects)
    {
        this.metadata = metadata;
        this.selection = selection;
        this.reversed = reversed;
        this.emitObjects = emitObjects;
        this.order = reversed ? metadata.comparator.reversed() : metadata.comparator;
        this.mayFilterColumns = !selection.fetchesAllColumns(false) || !selection.allFetchedColumnsAreQueried();
        this.droppedColumns = metadata.droppedColumns;
        this.fetchedColumns = selection.fetchedColumns().regulars;
        this.queriedColumns = selection.queriedColumns().regulars;
        this.clusteringTypes = metadata.comparator.subtypes();
        this.desc = new MemtableMergeLeg.MemtableDescriptor(clusteringTypes);
        reopen(partition, slices, names);
    }

    /**
     * Points the leg at another partition of the same memtable read, as if it were made anew: a
     * range read keeps one leg per memtable.  The caller has closed it, or is done with it.
     */
    public void reopen(MemtablePartitionData partition, Slices slices, boolean names)
    {
        this.partitionKey = partition.key;
        this.data = partition.data;
        this.ensureOnHeap = names ? partition.ensureOnHeapForNames() : partition.ensureOnHeap;
        this.slices = slices;
        // UnfilteredRowIterators.noRowsIterator, for a read with no slice, reports the LIVE object
        DeletionTime deletion = data.deletionInfo().getPartitionDeletion();
        this.partitionDeletion = slices.isEmpty() && deletion.isLive() ? DeletionTime.LIVE : deletion;
        this.staticRow = filteredStaticRow();
        slicesOpened = 0;
        sliceOpen = false;
        rows = null;
        ranges = null;
        nextRow = null;
        nextRange = null;
        openRange = null;
        validatorOpenDeletion = null;
        currentRow = null;
        currentMarker = null;
        complexColumn = null;
        parked = false;
        parkedCell = null;
        parkedColumn = null;
        parkedComplexDeletion = DeletionTime.LIVE;
        parkedKeepsColumnWithNoCells = false;
        rowsCloned = 0;
        if (!emitObjects)
            CursorReads.countMemtableLegStreamed();
        advance();
    }

    /** {@code AbstractBTreePartition.staticRow(current, selection, false)}, as the iterator gives it. */
    private Row filteredStaticRow()
    {
        Row stored = data.staticRow();
        if (selection.fetchedColumns().statics.isEmpty() || (stored.isEmpty() && partitionDeletion.isLive()))
            return Rows.EMPTY_STATIC_ROW;
        Row filtered = stored.filter(selection, partitionDeletion, false, metadata);
        if (filtered == null || (slices.isEmpty() && filtered.isEmpty()))
            return Rows.EMPTY_STATIC_ROW; // noRowsIterator gives the EMPTY_STATIC_ROW object for an empty one
        return ensureOnHeap.applyToStatic(filtered);
    }

    /** The partition key the memtable's row iterator reports. */
    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    // ---------------------------------------------------------------- RowAndDeletionMergeIterator

    private void advance()
    {
        if (computeNext())
        {
            state = currentRow != null ? ROW_START : TOMBSTONE_START;
        }
        else
        {
            currentRow = null;
            currentMarker = null;
            state = PARTITION_END;
        }
    }

    /** The next unfiltered of the slices, in {@link #currentRow} or {@link #currentMarker}. */
    private boolean computeNext()
    {
        while (true)
        {
            if (!sliceOpen)
            {
                if (slicesOpened >= slices.size())
                    return false;
                // the SlicesIterator order: a reverse read takes the slices from the last
                int index = reversed ? slices.size() - slicesOpened - 1 : slicesOpened;
                slicesOpened++;
                openSlice(slices.get(index));
            }
            currentRow = null;
            currentMarker = null;
            if (!computeNextInSlice())
            {
                sliceOpen = false;
                continue;
            }
            if (currentMarker == null)
                return true;
            // RowAndDeletionMergeIterator.shouldSkip: a boundary that changes nothing
            if (currentMarker.isBoundary() && currentMarker.openDeletionTime(false).equals(currentMarker.closeDeletionTime(false)))
                continue;
            validateMarker(currentMarker);
            if (emitObjects)
                currentMarker = ensureOnHeap.applyToMarker(currentMarker);
            return true;
        }
    }

    private void openSlice(Slice slice)
    {
        ClusteringBound<?> start = slice.start().isBottom() ? null : slice.start();
        ClusteringBound<?> end = slice.end().isTop() ? null : slice.end();
        rows = BTree.slice(data.tree(), metadata.comparator, start, true, end, true, reversed ? BTree.Dir.DESC : BTree.Dir.ASC);
        ranges = data.deletionInfo().rangeIterator(slice, reversed);
        nextRow = null;
        nextRange = null;
        openRange = null;
        sliceOpen = true;
    }

    /** {@code RowAndDeletionMergeIterator.computeNextInternal}: false at the end of the slice. */
    private boolean computeNextInSlice()
    {
        while (true)
        {
            if (nextRow == null && rows.hasNext())
                nextRow = rows.next();
            if (nextRow == null)
            {
                if (openRange != null)
                {
                    closeOpenedRange();
                    return true;
                }
                updateNextRange();
                if (nextRange == null)
                    return false;
                openRange();
                return true;
            }

            if (openRange == null)
            {
                updateNextRange();
                if (nextRange != null && order.compare(nextRange.deletedSlice().open(reversed), nextRow.clustering()) < 0)
                {
                    openRange();
                    return true;
                }
            }
            else if (order.compare(openRange.deletedSlice().close(reversed), nextRow.clustering()) < 0)
            {
                closeOpenedRange();
                return true;
            }
            if (consumeNextRow())
                return true;
        }
    }

    private void updateNextRange()
    {
        while (nextRange == null && ranges.hasNext())
        {
            nextRange = ranges.next();
            // the partition deletion shadows a range tombstone it is at least as new as
            if (!nextRange.deletionTime().supersedes(partitionDeletion) || nextRange.deletedSlice().isEmpty(metadata.comparator))
                nextRange = null;
        }
    }

    private void openRange()
    {
        openRange = nextRange;
        nextRange = null;
        currentMarker = new RangeTombstoneBoundMarker(openRange.deletedSlice().open(reversed), openRange.deletionTime());
    }

    private void closeOpenedRange()
    {
        // the close is a boundary when the next range opens where this one closes
        updateNextRange();
        ClusteringBound<?> close = openRange.deletedSlice().close(reversed);
        if (nextRange != null && order.compare(close, nextRange.deletedSlice().open(reversed)) == 0)
        {
            currentMarker = RangeTombstoneBoundaryMarker.makeBoundary(reversed, close, nextRange.deletedSlice().open(reversed),
                                                                      openRange.deletionTime(), nextRange.deletionTime());
            openRange = nextRange;
            nextRange = null;
        }
        else
        {
            currentMarker = new RangeTombstoneBoundMarker(close, openRange.deletionTime());
            openRange = null;
        }
    }

    /** Takes {@link #nextRow}; false when the filter leaves nothing of it. */
    private boolean consumeNextRow()
    {
        Row row = nextRow;
        nextRow = null;
        DeletionTime activeDeletion = openRange == null ? partitionDeletion : openRange.deletionTime();
        if (emitObjects)
        {
            Row filtered = row.filter(selection, activeDeletion, false, metadata);
            if (filtered == null)
                return false;
            Row onHeap = ensureOnHeap.applyToRow(filtered);
            if (onHeap != filtered)
                rowsCloned++;
            currentRow = onHeap;
            rowFiltered = false;
            rowLiveness = onHeap.primaryKeyLivenessInfo();
            rowDeletion = onHeap.deletion();
            return true;
        }
        prepareRowFilter(row, activeDeletion);
        if (!rowSurvivesFilter(row))
            return false;
        currentRow = row;
        return true;
    }

    /** {@code RTBoundValidator} at {@code Stage.MEMTABLE}, without enforcing the last close. */
    private void validateMarker(RangeTombstoneMarker marker)
    {
        if (validatorOpenDeletion == null)
        {
            if (marker.isClose(reversed))
                throw invalidBounds("unexpected end bound or boundary " + marker.toString(metadata));
        }
        else
        {
            if (!marker.isClose(reversed))
                throw invalidBounds("start bound followed by another start bound " + marker.toString(metadata));
            DeletionTime close = marker.closeDeletionTime(reversed);
            if (!close.equals(validatorOpenDeletion))
                throw invalidBounds("open marker and close marker have different deletion times, close=" + close);
            validatorOpenDeletion = null;
        }
        if (marker.isOpen(reversed))
            validatorOpenDeletion = marker.openDeletionTime(reversed);
    }

    private IllegalStateException invalidBounds(String why)
    {
        return new IllegalStateException(String.format("%s UnfilteredRowIterator for %s (key: %s omdt: [%s]) has an illegal Range Tombstone bounds sequence: %s",
                                                       "MEMTABLE", metadata, metadata.partitionKeyType.getString(partitionKey.getKey()),
                                                       Objects.toString(validatorOpenDeletion, "not present"), why));
    }

    // ---------------------------------------------------------------- BTreeRow.filter

    /** The row-level part of {@code BTreeRow.filter(selection, activeDeletion, false, metadata)}. */
    private void prepareRowFilter(Row row, DeletionTime activeDeletion)
    {
        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        Row.Deletion deletion = row.deletion();
        // Row.Merger removes a row deletion that does not supersede the active deletion
        rowMayHaveShadowed = !activeDeletion.isLive() && !deletion.time().supersedes(activeDeletion);
        rowActiveDeletion = activeDeletion;
        rowFiltered = mayFilterColumns || rowMayHaveShadowed || !droppedColumns.isEmpty();
        if (rowMayHaveShadowed)
        {
            if (activeDeletion.deletes(liveness.timestamp()))
                liveness = LivenessInfo.EMPTY;
            deletion = Row.Deletion.LIVE;
        }
        rowLiveness = liveness;
        rowDeletion = deletion;
    }

    /**
     * Whether {@code BTreeRow.filter} returns a row: the row itself when nothing changes, else a
     * row with liveness, a deletion or a column left.
     */
    private boolean rowSurvivesFilter(Row row)
    {
        // BTreeRow.update returns null only for a row left with no liveness, no deletion and no column
        if (!rowFiltered || !rowLiveness.isEmpty() || !rowDeletion.isLive())
            return true;
        boolean changed = rowLiveness != row.primaryKeyLivenessInfo() || rowDeletion != row.deletion();
        boolean anyColumn = false;
        BTreeRow btreeRow = (BTreeRow) row;
        int count = btreeRow.columnCount();
        for (int i = 0; i < count; i++)
        {
            ColumnData cd = btreeRow.columnDataByIndex(i);
            int verdict = cd.column().isComplex() ? complexColumnVerdict((ComplexColumnData) cd) : simpleCellVerdict((Cell<?>) cd);
            if (verdict != DROPPED)
                anyColumn = true;
            if (verdict != UNCHANGED)
                changed = true;
            if (anyColumn && changed)
                return true;
        }
        if (!changed)
            return true;
        return !rowLiveness.isEmpty() || !rowDeletion.isLive() || anyColumn;
    }

    private static final int UNCHANGED = 0;
    private static final int DROPPED = 1;
    /** Kept with an empty value, or (a complex column) with fewer cells or another deletion. */
    private static final int CHANGED = 2;

    private DroppedColumn dropped(ColumnMetadata column)
    {
        return droppedColumns.isEmpty() ? null : droppedColumns.get(column.name.bytes);
    }

    /** {@code BTreeRow.filter}'s decision for a simple cell. */
    private int simpleCellVerdict(Cell<?> cell)
    {
        ColumnMetadata column = cell.column();
        if (!fetchedColumns.contains(column))
            return DROPPED;
        DroppedColumn dropped = dropped(column);
        boolean isForDropped = dropped != null && cell.timestamp() <= dropped.droppedTime;
        boolean isShadowed = rowMayHaveShadowed && rowActiveDeletion.deletes(cell);
        boolean isSkippable = !queriedColumns.contains(column);
        if (isForDropped || isShadowed || (isSkippable && cell.timestamp() < rowLiveness.timestamp()))
            return DROPPED;
        return isSkippable ? CHANGED : UNCHANGED;
    }

    /** {@code ComplexColumnData.filter}'s decision for a whole complex column. */
    private int complexColumnVerdict(ComplexColumnData cd)
    {
        if (!openComplexFilter(cd))
            return DROPPED;
        if (!complexFiltered)
            return UNCHANGED;
        boolean changed = complexDeletionOut != cd.complexDeletion();
        boolean anyCell = false;
        for (int i = 0; i < cd.cellsCount(); i++)
        {
            Cell<?> cell = cd.getCellByIndex(i);
            int verdict = complexCellVerdict(cell);
            if (verdict != DROPPED)
                anyCell = true;
            if (verdict != UNCHANGED)
                changed = true;
        }
        if (!changed)
            return UNCHANGED;
        // ComplexColumnData.update: no cells and the LIVE deletion leave no column
        if (complexDeletionOut == DeletionTime.LIVE && !anyCell)
            return DROPPED;
        return CHANGED;
    }

    /**
     * Sets up {@code ComplexColumnData.filter} for {@code cd}: false when the column is not fetched.
     * {@link #complexFiltered} is false when the column is kept as it is.
     */
    private boolean openComplexFilter(ComplexColumnData cd)
    {
        ColumnMetadata column = cd.column();
        if (!fetchedColumns.contains(column))
            return false;
        droppedColumn = dropped(column);
        complexActiveDeletion = rowMayHaveShadowed ? rowActiveDeletion : DeletionTime.LIVE;
        cellTester = selection.newTester(column);
        queriedColumn = selection.fetchedColumnIsQueried(column);
        complexFiltered = cellTester != null || !complexActiveDeletion.isLive() || droppedColumn != null || !queriedColumn;
        complexDeletionOut = complexFiltered && complexActiveDeletion.supersedes(cd.complexDeletion()) ? DeletionTime.LIVE : cd.complexDeletion();
        return true;
    }

    /** {@code ComplexColumnData.filter}'s decision for one cell, in path order (the tester walks the paths). */
    private int complexCellVerdict(Cell<?> cell)
    {
        CellPath path = cellPathOf(cell);
        boolean isForDropped = droppedColumn != null && cell.timestamp() <= droppedColumn.droppedTime;
        boolean isShadowed = complexActiveDeletion.deletes(cell);
        boolean isFetchedCell = cellTester == null || cellTester.fetches(path);
        boolean isQueriedCell = queriedColumn && isFetchedCell && (cellTester == null || cellTester.fetchedCellIsQueried(path));
        boolean isSkippableCell = !isFetchedCell || (!isQueriedCell && cell.timestamp() < rowLiveness.timestamp());
        if (isForDropped || isShadowed || isSkippableCell)
            return DROPPED;
        return isQueriedCell ? UNCHANGED : CHANGED;
    }

    private CellPath cellPathOf(Cell<?> cell)
    {
        if (cell instanceof NativeCell)
        {
            ((NativeCell) cell).pointAtPath(pathView);
            return pathViewCellPath;
        }
        return cell.path();
    }

    // ---------------------------------------------------------------- partition-level surface

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        return partitionDeletion;
    }

    @Override
    public Row staticRow()
    {
        return staticRow;
    }

    @Override
    public Slices legSlices()
    {
        return slices;
    }

    @Override
    public EncodingStats legStats()
    {
        // a read with no slice gets UnfilteredRowIterators.noRowsIterator, which has no stats
        return slices.isEmpty() ? EncodingStats.NO_STATS : data.stats;
    }

    @Override
    public RegularAndStaticColumns legColumns()
    {
        // the columns the memtable's row iterator reports: noRowsIterator's for a read with no slice
        if (!slices.isEmpty())
            return selection.fetchedColumns();
        return staticRow.isEmpty() ? RegularAndStaticColumns.NONE : new RegularAndStaticColumns(Columns.from(staticRow), Columns.NONE);
    }

    @Override
    public SSTableReader sstableOrNull()
    {
        return null;
    }

    @Override
    public void enterMergeMode()
    {
        // merge-shaped from construction
    }

    @Override
    public void seekForMerge(ClusteringBound<?> sliceStart)
    {
        // the leg walks its slices itself and never seeks
    }

    // ---------------------------------------------------------------- unfiltered walk

    @Override
    public int cursorState()
    {
        return state;
    }

    @Override
    public UnfilteredDescriptor unfiltered()
    {
        return desc;
    }

    @Override
    public void readUnfilteredHeader()
    {
        if (state == ROW_START)
        {
            MemtableMergeLeg.loadClustering(desc, currentRow.clustering(), clusteringTypes.length, clusteringTypes, clusteringScratch);
            long timestamp = rowLiveness.timestamp();
            if (CursorReads.TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS && !rowLiveness.isEmpty())
                timestamp += 1;
            desc.livenessInfo().reset(timestamp, rowLiveness.ttl(), rowLiveness.localExpirationTime());
            if (rowDeletion.isLive())
                desc.deletionTime().resetLive();
            else
                desc.deletionTime().reset(rowDeletion.time());
            columnIndex = 0;
            columnCount = currentRow.columnCount();
            complexColumn = null;
            parked = false;
            parkedCell = null;
            parkedColumn = null;
            parkedComplexDeletion = DeletionTime.LIVE;
            parkedKeepsColumnWithNoCells = false;
            state = CELL_HEADER_START;
        }
        else if (state == TOMBSTONE_START)
        {
            MemtableMergeLeg.loadMarker(desc, currentMarker, clusteringTypes, clusteringScratch);
            state = UNFILTERED_END;
        }
        else
        {
            throw new IllegalStateException("unexpected memtable leg state before a header read: " + state);
        }
    }

    @Override
    public void continueReading()
    {
        if (state != UNFILTERED_END)
            throw new IllegalStateException("memtable leg asked to continue from state " + state);
        advance();
    }

    @Override
    public ClusteringPrefix<?> materializeClusteringPrefix()
    {
        if (currentMarker != null)
            return currentMarker.clustering();
        // a row of the memtable's storage may be off heap
        return emitObjects ? currentRow.clustering() : HeapCloner.instance.clone(currentRow.clustering());
    }

    @Override
    public byte[][] materializeBoundValues()
    {
        return MemtableMergeLeg.boundValues(currentMarker != null ? currentMarker.clustering() : currentRow.clustering());
    }

    @Override
    public void skipRow()
    {
        endRow();
    }

    @Override
    public Row consumeExistingRow()
    {
        if (!emitObjects)
            return null;
        Row row = currentRow;
        if (row == null)
            throw new IllegalStateException("consumeExistingRow outside a row group");
        endRow();
        return row;
    }

    /** The memtable's row, when {@code BTreeRow.filter} returns it unchanged and no cell of it has been read. */
    @Override
    public Row unchangedRow()
    {
        // a leg that emits objects hands its row over through consumeExistingRow
        return !emitObjects && state == CELL_HEADER_START && !rowFiltered && !parked && columnIndex == 0 ? currentRow : null;
    }

    private void endRow()
    {
        complexColumn = null;
        parked = false;
        parkedCell = null;
        state = UNFILTERED_END;
    }

    // ---------------------------------------------------------------- cell walk

    @Override
    public boolean parkedAtCellPosition()
    {
        return parked;
    }

    @Override
    public boolean needsCellAdvance()
    {
        return !parked && state == CELL_HEADER_START;
    }

    @Override
    public void ensureParkedAtCell(boolean validateCells)
    {
        // the iterator path never validates memtable data
        if (parked || state != CELL_HEADER_START)
            return;
        BTreeRow row = (BTreeRow) currentRow;
        while (true)
        {
            if (complexColumn != null)
            {
                if (parkNextComplexCell())
                    return;
                ComplexColumnData finished = complexColumn;
                complexColumn = null;
                // ComplexColumnData.filter keeps a column with no cells when its deletion is
                // not the LIVE singleton: a column with a live deletion and no cells
                if (rowFiltered && complexFiltered && !complexColumnKept && complexDeletionOut != DeletionTime.LIVE
                    && finished.cellsCount() > 0)
                {
                    parkDeletionOnly(finished.column(), complexDeletionOut, complexDeletionOut.isLive());
                    return;
                }
            }
            if (columnIndex >= columnCount)
            {
                state = UNFILTERED_END;
                return;
            }
            ColumnData cd = row.columnDataByIndex(columnIndex++);
            ColumnMetadata column = cd.column();
            if (column.isSimple())
            {
                Cell<?> cell = (Cell<?>) cd;
                int verdict = rowFiltered ? simpleCellVerdict(cell) : UNCHANGED;
                if (verdict == DROPPED)
                    continue;
                parkedColumn = column;
                parkedComplexDeletion = DeletionTime.LIVE;
                parkCell(cell, verdict == CHANGED);
                return;
            }
            ComplexColumnData complex = (ComplexColumnData) cd;
            if (rowFiltered)
            {
                if (!openComplexFilter(complex))
                    continue;
            }
            else
            {
                complexFiltered = false;
                complexDeletionOut = complex.complexDeletion();
            }
            parkedColumn = column;
            parkedComplexDeletion = complexDeletionOut;
            if (complex.cellsCount() == 0)
            {
                if (!complexDeletionOut.isLive())
                {
                    parkDeletionOnly(column, complexDeletionOut, false);
                    return;
                }
                // A column with a live deletion and no cells: ComplexColumnData.filter returns it
                // unless it changes the deletion, which then is the LIVE object and leaves no column
                if (!complexFiltered || complexDeletionOut == complex.complexDeletion())
                {
                    parkDeletionOnly(column, complexDeletionOut, true);
                    return;
                }
                continue;
            }
            // the column deletion goes to the merge with each cell position, as cellComplexDeletion
            complexColumn = complex;
            cellIndex = 0;
            complexColumnKept = false;
        }
    }

    /** Parks at the next cell of {@link #complexColumn} the filter keeps; false when there is none. */
    private boolean parkNextComplexCell()
    {
        while (cellIndex < complexColumn.cellsCount())
        {
            Cell<?> cell = complexColumn.getCellByIndex(cellIndex++);
            int verdict = rowFiltered && complexFiltered ? complexCellVerdict(cell) : UNCHANGED;
            if (verdict == DROPPED)
                continue;
            complexColumnKept = true;
            parkCell(cell, verdict == CHANGED);
            return true;
        }
        return false;
    }

    private void parkDeletionOnly(ColumnMetadata column, DeletionTime deletion, boolean keepsColumnWithNoCells)
    {
        parkedColumn = column;
        parkedComplexDeletion = deletion;
        parkedCell = null;
        parkedValueSkipped = false;
        parkedKeepsColumnWithNoCells = keepsColumnWithNoCells;
        parked = true;
    }

    private void parkCell(Cell<?> cell, boolean valueSkipped)
    {
        parkedCell = cell;
        parkedValueSkipped = valueSkipped;
        parkedKeepsColumnWithNoCells = false;
        long timestamp = cell.timestamp();
        if (CursorReads.TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS)
            timestamp += 1;
        parkedCellLiveness.reset(timestamp, cell.ttl(), cell.localDeletionTime());
        parked = true;
    }

    @Override
    public void advancePastCellPosition()
    {
        parked = false;
        parkedCell = null;
    }

    @Override
    public boolean cellProduced()
    {
        return parkedCell != null;
    }

    @Override
    public ColumnMetadata cellColumn()
    {
        return parkedColumn;
    }

    @Override
    public CellLivenessInfo cellLiveness()
    {
        return parkedCellLiveness;
    }

    @Override
    public ByteBuffer cellPathWindow()
    {
        return cellPathOf(parkedCell).get(0);
    }

    @Override
    public DeletionTime cellComplexDeletion()
    {
        return parkedComplexDeletion;
    }

    @Override
    public boolean keepsColumnWithNoCells()
    {
        return parkedCell == null && parkedKeepsColumnWithNoCells;
    }

    @Override
    public CellPath cellPath()
    {
        return cellPathOf(parkedCell);
    }

    @Override
    public Cell<?> existingCell()
    {
        return emitObjects ? parkedCell : null;
    }

    @Override
    public byte[] cellValue()
    {
        if (parkedCell == null || parkedValueSkipped)
            return ByteArrayUtil.EMPTY_BYTE_ARRAY;
        Cell<?> cell = parkedCell;
        if (cell instanceof NativeCell)
        {
            byte[] value = new byte[cell.valueSize()];
            MemoryUtil.getBytes(((NativeCell) cell).getAddress(), value, 0, value.length);
            return value;
        }
        return ByteBufferUtil.getArray(cell.buffer());
    }

    /**
     * The value as the replica response carries it: the length vint for a variable-length type
     * (the response header's type, the column's own), then the bytes.  Nothing for an empty value.
     */
    @Override
    public void stageCellValue(DataOutputPlus scratch) throws IOException
    {
        if (parkedCell == null || parkedValueSkipped)
            return;
        Cell<?> cell = parkedCell;
        int length = cell.valueSize();
        if (length == 0)
            return;
        if (parkedColumn.type.valueLengthIfFixed() < 0)
            scratch.writeUnsignedVInt32(length);
        if (cell instanceof NativeCell)
        {
            scratch.writeMemory(((NativeCell) cell).getAddress(), length);
            return;
        }
        ByteBuffer value = cell.buffer();
        if (value.hasArray())
        {
            scratch.write(value.array(), value.arrayOffset() + value.position(), length);
        }
        else if (value.isDirect() && !value.isReadOnly())
        {
            scratch.writeMemory(MemoryUtil.getAddress(value) + value.position(), length);
        }
        else
        {
            byte[] copy = ByteBufferUtil.getArray(value);
            scratch.write(copy, 0, copy.length);
        }
    }

    @Override
    public void discardCellValue()
    {
        // nothing is consumed: the value stays in the memtable
    }

    @Override
    public boolean cellHasValue()
    {
        return parkedCell != null && !parkedValueSkipped && parkedCell.valueSize() > 0;
    }

    // ---------------------------------------------------------------- validation

    @Override
    public void validateRowHeader()
    {
        // the iterator path never applies UnfilteredValidation to memtable data
    }

    @Override
    public void validateMarkerHeader()
    {
        // see validateRowHeader
    }

    @Override
    public void close()
    {
        if (rowsCloned > 0)
            CursorReads.countMemtableRowsCloned(rowsCloned);
        rowsCloned = 0;
    }
}
