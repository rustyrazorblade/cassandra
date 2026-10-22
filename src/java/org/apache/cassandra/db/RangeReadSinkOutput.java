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
import java.util.List;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The {@link ResponseSink} output of a {@link CursorRangeScan}: gives what the sink writes to a
 * {@link RangeReadSink} as calls.  A partition is given to the sink only once it is known to be
 * kept, at its first row that ends, its first range tombstone marker, or its end: until then each
 * row's calls are held here and dropped with the row when it is abandoned, so the sink never
 * sees a partition the read drops.
 */
final class RangeReadSinkOutput implements ResponseSink.Output
{
    private final RangeReadSink sink;
    private final AbstractType<?>[] clusteringTypes;
    private final RowClustering clustering;

    // ---- the open partition ----
    private DecoratedKey key;
    private DeletionTime partitionDeletion;
    private Row staticRow;
    /** The partition was given to the sink. */
    private boolean partitionStarted;

    // ---- the open row ----
    private LivenessInfo rowLiveness;
    private DeletionTime rowDeletion;
    private int rowColumns;
    private ColumnMetadata openComplexColumn;
    /** The row's calls are held, as the partition was not given to the sink yet. */
    private boolean holding;

    // ---- the held row ----
    private byte[] heldClustering = new byte[64];
    private int heldClusteringLength;
    private final List<HeldCall> held = new ArrayList<>();
    private int heldCount;
    private final DataOutputBuffer heldValues = new DataOutputBuffer(256);
    private final HeldValue heldValue = new HeldValue();
    private final BufferValue bufferValue = new BufferValue();
    private DataOutputBuffer clusteringScratch;

    RangeReadSinkOutput(RangeReadSink sink, ClusteringComparator comparator)
    {
        this.sink = sink;
        this.clusteringTypes = comparator.subtypes().toArray(AbstractType[]::new);
        this.clustering = new RowClustering(clusteringTypes);
    }

    @Override
    public AbstractType<?>[] clusteringTypes()
    {
        return clusteringTypes;
    }

    @Override
    public List<AbstractType<?>> clusteringTypesList()
    {
        return clustering.clusteringTypesList();
    }

    // ---------------------------------------------------------------- partitions

    @Override
    public void openPartition(DecoratedKey key, boolean reversed, DeletionTime partitionDeletion, Row staticRow, ColumnFilter columnFilter)
    {
        this.key = key;
        this.partitionDeletion = partitionDeletion;
        this.staticRow = staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
        this.partitionStarted = false;
    }

    private void startPartition()
    {
        if (partitionStarted)
            return;
        partitionStarted = true;
        sink.partitionStart(key, partitionDeletion, staticRow);
    }

    @Override
    public void removePartition()
    {
        // ResponseSink drops only a partition with no surviving row or marker, which this never gave on
        if (partitionStarted)
            throw new IllegalStateException("a partition given to the range read sink was dropped: " + key);
        key = null;
    }

    @Override
    public void closeEmptyPartition(boolean reversed, DeletionTime partitionDeletion, ColumnFilter columnFilter)
    {
        closePartition();
    }

    @Override
    public void closePartition()
    {
        startPartition();
        sink.partitionEnd();
        key = null;
        partitionStarted = false;
    }

    // ---------------------------------------------------------------- rows

    @Override
    public void startRow(byte[] clusteringBytes, int clusteringLength, LivenessInfo liveness, DeletionTime rowDeletion)
    {
        this.rowLiveness = liveness;
        this.rowDeletion = rowDeletion;
        rowColumns = 0;
        openComplexColumn = null;
        holding = !partitionStarted;
        if (holding)
        {
            if (heldClustering.length < clusteringLength)
                heldClustering = new byte[Math.max(clusteringLength, heldClustering.length * 2)];
            System.arraycopy(clusteringBytes, 0, heldClustering, 0, clusteringLength);
            heldClusteringLength = clusteringLength;
            heldCount = 0;
            heldValues.clear();
            return;
        }
        clustering.load(clusteringBytes, clusteringLength);
        sink.startRow(clustering, liveness, rowDeletion);
    }

    @Override
    public void addComplexDeletion(ColumnMetadata column, DeletionTime complexDeletion)
    {
        openComplexColumn(column);
        if (holding)
            hold().complexDeletion(column, complexDeletion);
        else
            sink.complexDeletion(column, complexDeletion);
    }

    @Override
    public void addCell(Cell<?> cell) throws IOException
    {
        ColumnMetadata column = cell.column();
        bufferValue.value = cell.buffer();
        bufferValue.column = column;
        cell(column, cell.timestamp(), cell.ttl(), cell.localDeletionTime(), cell.path(), bufferValue.hasValue(), bufferValue);
    }

    @Override
    public void addCellFromWire(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime,
                                CellPath path, boolean hasValue, CellValueSource source) throws IOException
    {
        cell(column, timestamp, ttl, localDeletionTime, path, hasValue, source);
    }

    private void cell(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime,
                      CellPath path, boolean hasValue, CellValueSource source) throws IOException
    {
        // a simple cell has no path; the merge may still hand over a view, which the response ignores too
        if (!column.isComplex())
            path = null;
        if (column.isComplex())
        {
            if (openComplexColumn == null || !CursorReads.sameColumn(openComplexColumn, column))
                openComplexColumn(column);
        }
        else
        {
            closeComplexColumn();
            rowColumns++;
        }
        if (!holding)
        {
            sink.cell(column, timestamp, ttl, localDeletionTime, path, hasValue ? source : null);
            return;
        }
        HeldCall call = hold();
        call.cell(column, timestamp, ttl, localDeletionTime, path == null ? null : copyOf(path));
        call.valueStart = heldValues.getLength();
        if (hasValue)
            source.streamValue(heldValues);
        call.valueEnd = heldValues.getLength();
        call.hasValue = hasValue;
    }

    private void openComplexColumn(ColumnMetadata column)
    {
        openComplexColumn = column;
        rowColumns++;
    }

    private void closeComplexColumn()
    {
        openComplexColumn = null;
    }

    @Override
    public void abandonRow()
    {
        if (!holding)
            sink.abandonRow();
        holding = false;
        heldCount = 0;
    }

    @Override
    public boolean endRow()
    {
        closeComplexColumn();
        boolean wasHolding = holding;
        holding = false;
        // Row.isEmpty: nothing is written for it
        if (rowLiveness.isEmpty() && rowDeletion.isLive() && rowColumns == 0)
        {
            if (!wasHolding)
                sink.abandonRow();
            heldCount = 0;
            return false;
        }
        if (wasHolding)
            replayHeldRow();
        sink.endRow();
        return true;
    }

    /** The partition is kept: gives it to the sink, then the row held for it. */
    private void replayHeldRow()
    {
        startPartition();
        clustering.load(heldClustering, heldClusteringLength);
        sink.startRow(clustering, rowLiveness, rowDeletion);
        byte[] values = heldValues.getData();
        for (int i = 0; i < heldCount; i++)
        {
            HeldCall call = held.get(i);
            if (call.cell)
            {
                heldValue.set(call.column, values, call.valueStart, call.valueEnd);
                sink.cell(call.column, call.timestamp, call.ttl, call.localDeletionTime, call.path, call.hasValue ? heldValue : null);
            }
            else
            {
                sink.complexDeletion(call.column, call.deletion);
            }
        }
        heldCount = 0;
    }

    private HeldCall hold()
    {
        if (heldCount == held.size())
            held.add(new HeldCall());
        return held.get(heldCount++);
    }

    private static CellPath copyOf(CellPath path)
    {
        // a cell path may be a view over reused storage
        return CellPath.create(ByteBufferUtil.clone(path.get(0)));
    }

    @Override
    public void writeRow(Row row) throws IOException
    {
        if (clusteringScratch == null)
            clusteringScratch = new DataOutputBuffer(64);
        clusteringScratch.clear();
        Clustering.serializer.serialize(row.clustering(), clusteringScratch, MessagingService.current_version, clustering.clusteringTypesList());
        startRow(clusteringScratch.getData(), clusteringScratch.getLength(), row.primaryKeyLivenessInfo(), row.deletion().time());
        for (ColumnData data : row)
        {
            if (!data.column().isComplex())
            {
                addCell((Cell<?>) data);
                continue;
            }
            ComplexColumnData complex = (ComplexColumnData) data;
            // the MergeSink grammar: a complex deletion is given when it is not live, or for a column with no cells
            if (!complex.complexDeletion().isLive() || complex.cellsCount() == 0)
                addComplexDeletion(complex.column(), complex.complexDeletion());
            for (int i = 0; i < complex.cellsCount(); i++)
                addCell(complex.getCellByIndex(i));
        }
        endRow();
    }

    @Override
    public void writeMarker(RangeTombstoneMarker marker)
    {
        // a written marker keeps the partition
        startPartition();
        sink.marker(marker);
    }

    // ---------------------------------------------------------------- helpers

    /** A call of a held row. */
    private static final class HeldCall
    {
        boolean cell;
        ColumnMetadata column;
        // a complex deletion
        DeletionTime deletion;
        // a cell
        long timestamp;
        int ttl;
        long localDeletionTime;
        CellPath path;
        boolean hasValue;
        int valueStart;
        int valueEnd;

        void complexDeletion(ColumnMetadata column, DeletionTime deletion)
        {
            this.cell = false;
            this.column = column;
            // the merge's deletion objects may be reused after the call
            this.deletion = DeletionTime.build(deletion.markedForDeleteAt(), deletion.localDeletionTime());
        }

        void cell(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime, CellPath path)
        {
            this.cell = true;
            this.column = column;
            this.timestamp = timestamp;
            this.ttl = ttl;
            this.localDeletionTime = localDeletionTime;
            this.path = path;
        }
    }

    /** The clustering of the open row, the view {@link RangeReadSink#startRow} gets. */
    private static final class RowClustering extends ClusteringDescriptor
    {
        RowClustering(AbstractType<?>[] clusteringTypes)
        {
            super(clusteringTypes);
        }

        void load(byte[] bytes, int length)
        {
            clusteringKind(ClusteringPrefix.Kind.CLUSTERING);
            overwrite(bytes, length);
        }
    }

    /** A held cell value, in the form {@link CellValueSource#streamValue} writes it. */
    private static final class HeldValue implements CellValueSource
    {
        private ColumnMetadata column;
        private byte[] bytes;
        private int start;
        private int end;

        void set(ColumnMetadata column, byte[] bytes, int start, int end)
        {
            this.column = column;
            this.bytes = bytes;
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean hasValue()
        {
            return end > start;
        }

        @Override
        public void streamValue(DataOutputPlus dest) throws IOException
        {
            dest.write(bytes, start, end - start);
        }

        @Override
        public byte[] materialize() throws IOException
        {
            if (column.type.valueLengthIfFixed() >= 0)
                return java.util.Arrays.copyOfRange(bytes, start, end);
            try (DataInputBuffer in = new DataInputBuffer(bytes, start, end - start))
            {
                byte[] value = new byte[in.readUnsignedVInt32()];
                in.readFully(value);
                return value;
            }
        }
    }

    /** The value of a cell object, in the form {@link CellValueSource#streamValue} writes it. */
    private static final class BufferValue implements CellValueSource
    {
        private ColumnMetadata column;
        private ByteBuffer value;

        @Override
        public boolean hasValue()
        {
            return value.hasRemaining();
        }

        @Override
        public void streamValue(DataOutputPlus dest) throws IOException
        {
            int length = value.remaining();
            if (column.type.valueLengthIfFixed() < 0)
                dest.writeUnsignedVInt32(length);
            if (value.hasArray())
                dest.write(value.array(), value.arrayOffset() + value.position(), length);
            else
                dest.write(value.duplicate());
        }

        @Override
        public byte[] materialize()
        {
            return ByteBufferUtil.getArray(value);
        }
    }
}
