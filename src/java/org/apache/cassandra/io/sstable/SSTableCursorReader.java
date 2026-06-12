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

package org.apache.cassandra.io.sstable;

import java.io.IOException;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ResizableByteBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.SEEK;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

public class SSTableCursorReader implements AutoCloseable
{
    private static final ColumnMetadata[] COLUMN_METADATA_TYPE = new ColumnMetadata[0];
    private final boolean hasStaticColumns;

    public interface State
    {
        /** start of file, after partition end but before EOF */
        int PARTITION_START = 1;
        int STATIC_ROW_START = 1 << 1;
        int ROW_START = 1 << 2;
        /** common to row/static row cells */
        int CELL_HEADER_START = 1 << 3;
        int CELL_VALUE_START = 1 << 4;
        int CELL_END = 1 << 5;
        int TOMBSTONE_START = 1 << 6;

        /** common to rows/tombstones. Call continue(); for next unfiltered, or maybe partition end */
        int UNFILTERED_END = 1 << 7;
        /** at {@link UnfilteredSerializer#isEndOfPartition(int)} */
        int PARTITION_END = 1 << 8;
        /** EOF */
        int DONE = 1 << 9;

        /* Special case for seeking in file */
        int SEEK = 1 << 10;
        static boolean isState(int state, int mask) {
            return (state & mask) != 0;
        }
    }

    /**
     * Observes complex (multi-cell) column boundaries during cell iteration: invoked once per
     * complex column as its header is consumed, INCLUDING deletion-only columns with zero
     * cells (which produce no readCellHeader result of their own). Arguments are reusable
     * objects valid only within the callback.
     */
    public interface ComplexColumnListener
    {
        void onComplexColumn(ColumnMetadata column, DeletionTime complexDeletion, int cellCount);
    }

    private ComplexColumnListener complexColumnListener;
    // When set (the merge consumer), a zero-cell (deletion-only) complex column becomes a
    // stoppable position: readCellHeader returns with producedCell=false, cellColumn set and
    // cellPathLength=-1 instead of skipping on; the normal CELL_END -> readCellHeader cycle
    // resumes past it. Default off: plain consumers never see cell-less positions.
    private boolean pauseAtEmptyComplexColumns;

    public void complexColumnListener(ComplexColumnListener listener)
    {
        this.complexColumnListener = listener;
    }

    public void pauseAtEmptyComplexColumns(boolean pause)
    {
        this.pauseAtEmptyComplexColumns = pause;
    }

    public class CellCursor {
        public ReusableLivenessInfo rowLiveness;
        public Columns columns;

        public int columnsSize;
        public int cellFlags;
        public final ReusableLivenessInfo cellLiveness = new ReusableLivenessInfo();
        // Cell path of the current cell, garbage-free: raw bytes in a grow-only scratch
        // buffer (wire format: vint length + bytes, CollectionType.CollectionPathSerializer —
        // the single serializer for all complex columns incl. UDTs). length < 0 => no path.
        public byte[] cellPathBuffer = new byte[32];
        public int cellPathLength = -1;
        private java.nio.ByteBuffer cellPathWindow;

        /** Reusable ByteBuffer view of the current cell path (re-wrapped only when the
         *  scratch grows); valid until the next readCellHeader. */
        public java.nio.ByteBuffer cellPathWindow()
        {
            if (cellPathWindow == null || cellPathWindow.array() != cellPathBuffer)
                cellPathWindow = java.nio.ByteBuffer.wrap(cellPathBuffer);
            cellPathWindow.limit(cellPathLength).position(0);
            return cellPathWindow;
        }
        // Multi-cell column state: cells remaining in the current complex column's run, and
        // the column-level deletion (LIVE when none or when the row had no complex deletion).
        public int remainingCellsInColumn;
        public final DeletionTime.ReusableDeletionTime complexDeletion = DeletionTime.ReusableDeletionTime.live();
        // true when the last readCellHeader() call produced a cell; false when it only
        // consumed trailing deletion-only complex column headers (the -1 return)
        public boolean producedCell;
        private boolean rowHasComplexDeletion;
        public AbstractType<?> cellType;
        public ColumnMetadata cellColumn;
        private ColumnMetadata[] columnsArray;
        private AbstractType<?>[] cellTypeArray;

        // Remaining PRESENT columns of this row as a bitmask over columnsArray indices.
        // Garbage-free sparse-row iteration: rows that do not contain every header column
        // pass the missing-columns mask (or, for >= 64-column supersets, present-mask
        // words) instead of a freshly allocated Columns subset, so the identity cache
        // below only rebuilds on a genuine superset change (stable per reader).
        private long presentMask;
        // >= 64-column supersets: present-mask words (bit i of word i/64 = superset column
        // i present), walked word by word. Grow-once scratch, consumed destructively.
        private long[] presentWords;
        private int presentWordsCount;
        private int presentWordIndex;

        void init (Columns columns, long missingColumnsMask, long[] presentColumnsWords,
                   boolean rowHasComplexDeletion, ReusableLivenessInfo rowLiveness)
        {
            this.rowHasComplexDeletion = rowHasComplexDeletion;
            remainingCellsInColumn = 0;
            complexDeletion.resetLive();
            if (this.columns != columns)
            {
                // This will be a problem with changing columns
                this.columns = columns;
                columnsArray = columns.toArray(COLUMN_METADATA_TYPE);
                cellTypeArray = new AbstractType<?>[columnsArray.length];
                for (int i = 0; i < columnsArray.length; i++)
                {
                    ColumnMetadata cellColumn = columnsArray[i];
                    cellTypeArray[i]  = serializationHeader.getType(cellColumn);
                }
                columnsSize = columns.size();
            }
            if (columnsSize >= 64)
            {
                // word-mask walk over the superset; the descriptor decoded the large-subset
                // wire format into presentColumnsWords (null = all columns present)
                int nWords = (columnsSize + 63) >>> 6;
                if (presentWords == null || presentWords.length < nWords)
                    presentWords = new long[nWords]; // grow-once, amortized zero
                presentWordsCount = nWords;
                presentWordIndex = 0;
                if (presentColumnsWords != null)
                {
                    System.arraycopy(presentColumnsWords, 0, presentWords, 0, nWords);
                }
                else
                {
                    java.util.Arrays.fill(presentWords, 0, nWords, -1L);
                    if ((columnsSize & 63) != 0)
                        presentWords[nWords - 1] = -1L >>> (64 - (columnsSize & 63));
                }
                presentMask = 0;
            }
            else
            {
                // Build the present-columns bitmask from the wire's MISSING-columns mask:
                //   -1L >>> (64 - n)   is the "n low ones" template (e.g. n=3 -> 0b111): all 64
                //                      bits set, then shifted so exactly the n column bits remain.
                //                      n == 0 must be special-cased because Java shifts are mod 64
                //                      (>>> 64 is a no-op, NOT zero).
                //   ~missingColumnsMask flips missing->present but also sets every bit ABOVE the
                //                      column range, so it is ANDed with the template to trim them.
                presentMask = ~missingColumnsMask & (columnsSize == 0 ? 0 : (-1L >>> (64 - columnsSize)));
            }
            this.rowLiveness = rowLiveness;
            cellFlags = 0;
            cellPathLength = -1;
            cellType = null;
            producedCell = false;
        }

        public boolean hasNext()
        {
            return remainingCellsInColumn > 0 || columnsRemain();
        }

        private boolean columnsRemain()
        {
            if (columnsSize < 64)
                return presentMask != 0;
            // advance to the next non-empty word; position is retained across calls
            while (presentWordIndex < presentWordsCount)
            {
                if (presentWords[presentWordIndex] != 0)
                    return true;
                presentWordIndex++;
            }
            return false;
        }

        /**
         * For Cell deserialization see {@link Cell.Serializer#deserialize};
         * for complex (multi-cell) columns see UnfilteredSerializer.readComplexColumn:
         * per complex column the stream carries [complex DeletionTime if the row flag
         * HAS_COMPLEX_DELETION is set][cell count vint][cells...], cells path-sorted.
         *
         * @return 1 if the next cell has a value, 0 if it has none (tombstone),
         *         -1 if no cell remains in this row (any trailing deletion-only complex
         *         column headers have been consumed; their deletions were surfaced via the
         *         {@link ComplexColumnListener} if one is set)
         */
        int readCellHeader() throws IOException
        {
            if (!hasNext()) throw new IllegalStateException();

            producedCell = false;
            while (remainingCellsInColumn == 0)
            {
                if (!columnsRemain())
                    return -1; // trailing deletion-only complex column(s) consumed; no cell
                // HOTSPOT: suprisingly expensive
                int currIndex;
                if (columnsSize >= 64)
                {
                    // columnsRemain() above parked presentWordIndex on a non-empty word;
                    // same low-to-high bit walk as the single-mask path, word by word
                    long word = presentWords[presentWordIndex];
                    currIndex = (presentWordIndex << 6) + Long.numberOfTrailingZeros(word);
                    presentWords[presentWordIndex] = word & (word - 1);
                }
                else
                {
                    // Bit i of presentMask corresponds to the i-th column of the superset in
                    // its iteration order — the SAME order the serializer assigned bits and
                    // the same order cells appear on disk. Walking bits low-to-high therefore
                    // visits cells in exactly their on-disk order:
                    //   numberOfTrailingZeros = index of the lowest set bit (next present column)
                    //   x & (x - 1)           = clears that lowest set bit (subtracting 1 borrows
                    //                           through the trailing zeros; the AND kills both)
                    currIndex = Long.numberOfTrailingZeros(presentMask);
                    presentMask &= presentMask - 1;
                }
                cellColumn = columnsArray[currIndex];
                cellType = cellTypeArray[currIndex];
                if (!cellColumn.isComplex())
                {
                    complexDeletion.resetLive();
                    remainingCellsInColumn = 1;
                }
                else
                {
                    if (rowHasComplexDeletion)
                        serializationHeader.readDeletionTime(dataReader, complexDeletion);
                    else
                        complexDeletion.resetLive();
                    remainingCellsInColumn = (int) dataReader.readUnsignedVInt();
                    if (complexColumnListener != null)
                        complexColumnListener.onComplexColumn(cellColumn, complexDeletion, remainingCellsInColumn);
                    if (remainingCellsInColumn == 0 && pauseAtEmptyComplexColumns)
                    {
                        // deletion-only column surfaced as a position; cellColumn and
                        // complexDeletion describe it, no cell fields are valid
                        cellPathLength = -1;
                        return 0;
                    }
                    // a count of zero (deletion-only column) loops on to the next column
                }
            }
            remainingCellsInColumn--;
            producedCell = true;

            cellFlags = dataReader.readUnsignedByte();
            // TODO: specialize common case where flags == HAS_VALUE | USE_ROW_TS?
            boolean hasValue = Cell.Serializer.hasValue(cellFlags);
            boolean isDeleted = Cell.Serializer.isDeleted(cellFlags);
            boolean isExpiring = Cell.Serializer.isExpiring(cellFlags);
            boolean useRowTimestamp = Cell.Serializer.useRowTimestamp(cellFlags);
            boolean useRowTTL = Cell.Serializer.useRowTTL(cellFlags);

            long timestamp = useRowTimestamp ? rowLiveness.timestamp() : serializationHeader.readTimestamp(dataReader);

            long localDeletionTime = useRowTTL
                                     ? rowLiveness.localExpirationTime()
                                     : (isDeleted || isExpiring ? serializationHeader.readLocalDeletionTime(dataReader) : Cell.NO_DELETION_TIME);

            int ttl = useRowTTL ? rowLiveness.ttl() : (isExpiring ? serializationHeader.readTTL(dataReader) : Cell.NO_TTL);
            localDeletionTime = Cell.decodeLocalDeletionTime(localDeletionTime, ttl, deserializationHelper);

            cellLiveness.reset(timestamp, ttl, localDeletionTime);
            if (cellColumn.isComplex())
            {
                // CollectionType.CollectionPathSerializer wire format: vint length + bytes
                int pathLength = dataReader.readUnsignedVInt32();
                if (cellPathBuffer.length < pathLength)
                    cellPathBuffer = new byte[Math.max(pathLength, cellPathBuffer.length * 2)]; // grow-only, amortized
                dataReader.readFully(cellPathBuffer, 0, pathLength);
                cellPathLength = pathLength;
            }
            else
            {
                cellPathLength = -1;
            }
            return hasValue ? 1 : 0;
        }
    }

    private final Ref<SSTableReader> ssTableReaderRef;
    private final AbstractType<?>[] clusteringColumnTypes;
    private final DeserializationHelper deserializationHelper;
    private final SerializationHeader serializationHeader;

    // need to be closed
    private final SSTableReader ssTableReader;
    private final RandomAccessReader dataReader;
    private final DeletionTime.Serializer deletionTimeSerializer;

    private final CellCursor staticRowCellCursor = new CellCursor();
    private final CellCursor rowCellCursor = new CellCursor();


    private CellCursor cellCursor;

    // SHARED STATIC_ROW/ROW/TOMB
    private int basicUnfilteredFlags = 0;
    private int extendedFlags = 0;

    private int state = PARTITION_START;

    public static SSTableCursorReader fromDescriptor(Descriptor desc) throws IOException
    {
        TableMetadata metadata = Util.metadataFromSSTable(desc);
        SSTableReader reader = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(metadata));
        return new SSTableCursorReader(reader, metadata, reader.ref(), null);
    }

    public SSTableCursorReader(SSTableReader reader)
    {
        this(reader, reader.metadata(), null, null);
    }

    public SSTableCursorReader(SSTableReader reader, DiskAccessMode diskAccessMode)
    {
        this(reader, reader.metadata(), null, diskAccessMode);
    }

    private SSTableCursorReader(SSTableReader reader, TableMetadata metadata, Ref<SSTableReader> readerRef, DiskAccessMode diskAccessMode)
    {
        ssTableReader = reader;
        ssTableReaderRef = readerRef;
        Version version = reader.descriptor.version;
        deletionTimeSerializer = DeletionTime.getSerializer(version);
        ImmutableList<ColumnMetadata> clusteringColumns = metadata.clusteringColumns();
        int clusteringColumnCount = clusteringColumns.size();
        clusteringColumnTypes = new AbstractType<?>[clusteringColumnCount];
        for (int i = 0; i < clusteringColumnTypes.length; i++)
        {
            clusteringColumnTypes[i] = clusteringColumns.get(i).type;
        }
        deserializationHelper = new DeserializationHelper(metadata, version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL, null);
        serializationHeader = reader.header;

        dataReader = reader.openDataReaderForScan(diskAccessMode);
        hasStaticColumns = metadata.hasStaticColumns();
    }

    @Override
    public void close()
    {
        dataReader.close();
        if (ssTableReaderRef != null)
            ssTableReaderRef.close();
    }

    private void resetOnPartitionStart()
    {
        basicUnfilteredFlags = 0;
        extendedFlags = 0;
    }

    public int seekPartition(long position)
    {
        state = SEEK;
        if (position == 0)
        {
            dataReader.seek(position);
            state = PARTITION_START;
        }
        else {
            // verify partition start is after a partition end marker
            dataReader.seek(position - 1);
            try
            {
                basicUnfilteredFlags = dataReader.readUnsignedByte();
            }
            catch (Exception e)
            {
                return corruptSSTable(e);
            }
            // end of partition
            if (!UnfilteredSerializer.isEndOfPartition(basicUnfilteredFlags)) {
                throw new IllegalArgumentException("Seeking to a partition at: " + position + " did not result in a valid state");
            }
            state = dataReader.isEOF() ? DONE : PARTITION_START;
        }
        resetOnPartitionStart();
        return state;
    }

    public int seekUnfiltered(long position)
    {
        state = SEEK;
        // partition elements (Unfiltered) have flags
        dataReader.seek(position);
        int state = 0;
        try
        {
            state = checkNextFlagsAfterStaticRowOrUnfilteredStart(false);
        }
        catch (IOException e)
        {
            return corruptSSTable(e);
        }
        if (!isState(state , ROW_START | TOMBSTONE_START | DONE)) throw new IllegalStateException();
        return state;
    }

    // struct partition {
    //   struct partition_header header
    //   optional<struct row> row
    //   struct unfiltered unfiltereds[];
    //};
    public int readPartitionHeader(PartitionDescriptor header)
    {
        if (state != PARTITION_START) throw new IllegalStateException();
        resetOnPartitionStart();
        try
        {
            header.load(dataReader, deletionTimeSerializer);
            return checkNextFlagsAfterPartitionStart(false);
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    // struct static_row {
    //    byte flags;          // preloaded
    //    byte extended_flags; // preloaded
    //    varint row_body_size;
    //    varint prev_unfiltered_size; // for backward traversing, ignored
    //    optional<struct liveness_info> liveness_info;
    //    optional<struct delta_deletion_time> deletion_time;
    // ***  We read the columns in a separate method ***
    //      optional<varint[]> missing_columns;
    //      cell[] cells; // potentially only some
    //};
    public int readStaticRowHeader(UnfilteredDescriptor unfilteredDescriptor)
    {
        if (state != STATIC_ROW_START) throw new IllegalStateException();
        try
        {
            unfilteredDescriptor.loadStaticRow(dataReader, serializationHeader, deserializationHelper, basicUnfilteredFlags, extendedFlags);
        }
        catch (IOException e)
        {
            return corruptSSTable(e);
        }

        staticRowCellCursor.init(unfilteredDescriptor.rowColumns(), unfilteredDescriptor.missingColumnsMask(),
                                 unfilteredDescriptor.presentColumnsWords(),
                                 UnfilteredSerializer.hasComplexDeletion(unfilteredDescriptor.flags()),
                                 unfilteredDescriptor.livenessInfo());
        cellCursor = staticRowCellCursor;
        if (!staticRowCellCursor.hasNext())
        {
            try
            {
                return checkNextFlagsAfterStaticRowOrUnfilteredStart(false);
            }
            catch (Exception e)
            {
                return corruptSSTable(e);
            }
        }
        else
        {
            return state = State.CELL_HEADER_START;
        }
    }

    public int copyCellValue(DataOutputPlus writer, byte[] buffer) throws IOException
    {
        if (state != CELL_VALUE_START) throw new IllegalStateException();
        if (cellCursor.cellType == null) throw new IllegalStateException();
        int length = cellCursor.cellType.valueLengthIfFixed();
        copyCellContents(writer, buffer, length);

        try
        {
            if (!cellCursor.hasNext())
            {
                try
                {
                    return checkNextFlagsAfterCellValuesEnd();
                }
                catch (Exception e)
                {
                    return corruptSSTable(e);
                }
            }
            return state = State.CELL_END;
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    // TODO: move to cell cursor? maybe avoid copy through buffer?
    private void copyCellContents(DataOutputPlus writer, byte[] transferBuffer, int length) throws IOException
    {
        if (length >= 0)
        {
            try
            {
                dataReader.readFully(transferBuffer, 0, length);
            }
            catch (Exception e)
            {
                corruptSSTable(e);
            }
            writer.write(transferBuffer, 0, length);
        }
        else
        {
            try
            {
                length = dataReader.readUnsignedVInt32();
            }
            catch (IOException e)
            {
                corruptSSTable(e);
            }
            if (length < 0)
                corruptSSTable("Corrupt (negative) value length encountered");
            writer.writeUnsignedVInt32(length);
            int remaining = length;
            while (remaining > 0)
            {
                int readLength = Math.min(remaining, transferBuffer.length);
                try
                {
                    dataReader.readFully(transferBuffer, 0, readLength);
                }
                catch (Exception e)
                {
                    corruptSSTable(e);
                }
                writer.write(transferBuffer, 0, readLength);
                remaining -= readLength;
            }
        }
    }

    // struct row {
    //    byte flags;
    //    optional<struct clustering_block[]> clustering_blocks;
    //    varint row_body_size;
    //    varint prev_unfiltered_size; // for backward traversing, ignored
    //    optional<struct liveness_info> liveness_info;
    //    optional<struct delta_deletion_time> deletion_time;
    // ***  We read the columns in a separate step ***
    //    optional<varint[]> missing_columns;
    //    cell[] cells; // potentially only some
    //};
    public int readRowHeader(UnfilteredDescriptor unfilteredDescriptor)
    {
        if (state != State.ROW_START) throw new IllegalStateException();
        if (!UnfilteredSerializer.isRow(basicUnfilteredFlags)) throw new IllegalStateException();
        try
        {
            unfilteredDescriptor.loadRow(dataReader, serializationHeader, deserializationHelper, basicUnfilteredFlags);

            rowCellCursor.init(unfilteredDescriptor.rowColumns(), unfilteredDescriptor.missingColumnsMask(),
                               unfilteredDescriptor.presentColumnsWords(),
                               UnfilteredSerializer.hasComplexDeletion(unfilteredDescriptor.flags()),
                               unfilteredDescriptor.livenessInfo());
            cellCursor = rowCellCursor;
            if (!rowCellCursor.hasNext())
            {
                return checkNextFlagsAfterStaticRowOrUnfilteredStart(false);
            }
            else
            {
                return state = State.CELL_HEADER_START;
            }
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    // TODO: introduce cell header class
    public int readCellHeader()
    {
        if (state != State.CELL_HEADER_START) throw new IllegalStateException();
        try
        {
            int cell = cellCursor.readCellHeader();
            if (cell < 0)
                return checkNextFlagsAfterCellValuesEnd();
            if (cell > 0)
            {
                return state = State.CELL_VALUE_START;
            }
            if (!cellCursor.hasNext())
                return checkNextFlagsAfterCellValuesEnd();
            return state = State.CELL_END;
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    public int skipCellValue()
    {
        if (state != State.CELL_VALUE_START) throw new IllegalStateException();
        try
        {
            cellCursor.cellType.skipValue(dataReader);
            return !cellCursor.hasNext() ? checkNextFlagsAfterCellValuesEnd() : (state = State.CELL_HEADER_START);
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    /**
     * See: {@link org.apache.cassandra.db.rows.UnfilteredSerializer#serialize(RangeTombstoneMarker, SerializationHelper, DataOutputPlus, long, int)}
     * <pre>
     * struct range_tombstone_marker {
     *   byte flags = IS_MARKER;
     *   byte kind_ordinal;
     *   be16 bound_values_count;
     *   struct clustering_block[] clustering_blocks;
     *   varint marker_body_size;
     *   varint prev_unfiltered_size;
     * };
     * struct range_tombstone_bound_marker : range_tombstone_marker {
     *   struct delta_deletion_time deletion_time;
     * };
     * struct range_tombstone_boundary_marker : range_tombstone_marker {
     *   struct delta_deletion_time end_deletion_time;
 *       struct delta_deletion_time start_deletion_time;
     * };
     * </pre>
     *
     */
    public int readTombstoneMarker(UnfilteredDescriptor unfilteredDescriptor)
    {
        try
        {
            if (state != TOMBSTONE_START) throw new IllegalStateException();
            if (!UnfilteredSerializer.isTombstoneMarker(basicUnfilteredFlags)) throw new IllegalStateException();
            unfilteredDescriptor.loadTombstone(dataReader, serializationHeader, basicUnfilteredFlags);
            return checkNextFlagsAfterStaticRowOrUnfilteredStart(false);
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }


    /**
     * {@link ClusteringPrefix.Serializer#deserializeValuesWithoutSize}
     */
    static void readUnfilteredClustering(RandomAccessReader dataReader, AbstractType<?>[] types, int clusteringColumnsBound, ResizableByteBuffer clustering) throws IOException
    {
        clustering.resetBuffer();
        if (clusteringColumnsBound == 0) {
            return;
        }
        long clusteringBlockHeader = 0;
        int fixedLengthClusteringLength = 0;
        for (int clusteringIndex = 0; clusteringIndex < clusteringColumnsBound; clusteringIndex++)
        {
            // struct clustering_block {
            //    varint clustering_block_header;
            //    simple_cell[] clustering_cells;
            // };
            if (clusteringIndex % 32 == 0)
            {
                if (fixedLengthClusteringLength != 0) {
                    clustering.loadPart(dataReader, fixedLengthClusteringLength);
                    fixedLengthClusteringLength = 0;
                }
                clusteringBlockHeader = dataReader.readUnsignedVInt();
                clustering.writeUnsignedVInt(clusteringBlockHeader);
            }

            // load value if present
            if ((clusteringBlockHeader & 0b11) == 0)
            {
                AbstractType<?> type = types[clusteringIndex];
                if (type.isValueLengthFixed())
                {
                    fixedLengthClusteringLength += type.valueLengthIfFixed();
                }
                else
                {
                    if (fixedLengthClusteringLength != 0) {
                        clustering.loadPart(dataReader, fixedLengthClusteringLength);
                        fixedLengthClusteringLength = 0;
                    }
                    int varLength = dataReader.readUnsignedVInt32();
                    clustering.writeUnsignedVInt(varLength);
                    clustering.loadPart(dataReader, varLength);
                }
            }
            clusteringBlockHeader = clusteringBlockHeader >>> 2;
        }
        if (fixedLengthClusteringLength != 0) clustering.loadPart(dataReader, fixedLengthClusteringLength);
        if (clusteringBlockHeader != 0) {
            throw new IOException("Clustering block upper bits (those not associated with keys) expected to be 0:" + clusteringBlockHeader);
        }
    }

    private static void skipClustering(RandomAccessReader dataReader, AbstractType<?>[] types, int clusteringColumnsBound) throws IOException
    {
        long clusteringBlockHeader = 0;
        for (int clusteringIndex = 0; clusteringIndex < clusteringColumnsBound; clusteringIndex++)
        {
            // struct clustering_block {
            //    varint clustering_block_header;
            //    simple_cell[] clustering_cells;
            // };
            if (clusteringIndex % 32 == 0)
            {
                clusteringBlockHeader = dataReader.readUnsignedVInt();
            }
            // skip value if present
            if ((clusteringBlockHeader & 0b11) == 0)
            {
                AbstractType<?> type = types[clusteringIndex];
                int len = type.isValueLengthFixed() ? type.valueLengthIfFixed() : dataReader.readUnsignedVInt32();
                dataReader.skipBytes(len);
            }
            clusteringBlockHeader = clusteringBlockHeader >>> 2;
        }
        if (clusteringBlockHeader != 0) {
            throw new IOException("Clustering block upper bits (those not associated with keys) expected to be 0:" + clusteringBlockHeader);
        }
    }

    /**
     * {@link UnfilteredSerializer#deserializeRowBody(DataInputPlus, SerializationHeader, DeserializationHelper, int, int, Row.Builder)}
     */
    static void readLivenessInfo(RandomAccessReader dataReader, SerializationHeader serializationHeader, DeserializationHelper deserializationHelper, int flags, ReusableLivenessInfo livenessInfo) throws IOException
    {
        long timestamp = LivenessInfo.NO_TIMESTAMP;
        int ttl = LivenessInfo.NO_TTL;
        long localExpirationTime = LivenessInfo.NO_EXPIRATION_TIME;
        if (UnfilteredSerializer.hasTimestamp(flags))
        {
            // struct liveness_info {
            //    varint64 delta_timestamp;
            //    optional<varint32> delta_ttl;
            //    optional<varint64> delta_local_deletion_time;
            //};
            timestamp = serializationHeader.readTimestamp(dataReader);
            if (UnfilteredSerializer.hasTTL(flags))
            {
                ttl = serializationHeader.readTTL(dataReader);
                localExpirationTime = Cell.decodeLocalDeletionTime(serializationHeader.readLocalDeletionTime(dataReader), ttl, deserializationHelper);
            }
        }
        livenessInfo.reset(timestamp, ttl, localExpirationTime);
    }

    // SKIPPING
    public int skipPartition()
    {
        if (state == PARTITION_END)
            return continueReading();

        if (state == PARTITION_START)
        {
            try
            {
                int partitionKeyLength = dataReader.readUnsignedShort();
                dataReader.skipBytes(partitionKeyLength);

                // PARTITION DELETION TIME
                deletionTimeSerializer.skip(dataReader);
                checkNextFlagsAfterPartitionStart(true);
            }
            catch (Exception e)
            {
                return corruptSSTable(e);
            }
        }
        else if (!isState(state, STATIC_ROW_START | ROW_START | TOMBSTONE_START | PARTITION_END))
        {
            throw new IllegalStateException("Unexpected state: " + state);
        }

        while (!isState(state,PARTITION_START | DONE))
        {
            switch (state)
            {
                case STATIC_ROW_START:
                    state = skipStaticRow(true);
                    break;
                case ROW_START:
                case TOMBSTONE_START:
                    state = skipUnfiltered(true);
                    break;
            }
        }
        return state;
    }

    public int skipStaticRow(boolean autoContinue)
    {
        if (state != State.STATIC_ROW_START) throw new IllegalStateException();

        try
        {
            long rowSize = dataReader.readUnsignedVInt();
            dataReader.seek(dataReader.getPosition() + rowSize);
            return checkNextFlagsAfterStaticRowOrUnfilteredStart(autoContinue);
        }
        catch (IOException e)
        {
            return corruptSSTable(e);
        }
    }

    public int skipUnfiltered(boolean autoContinue)
    {
        if (!isState(state, ROW_START | TOMBSTONE_START))
            throw new IllegalStateException();

        AbstractType<?>[] types = clusteringColumnTypes;
        int clusteringColumnsBound = types.length;
        // tombstone markers have `kind` & `clusteringColumnsBound`
        try
        {
            if (!UnfilteredSerializer.isRow(basicUnfilteredFlags))
            {
                dataReader.readByte();// byte kind =
                clusteringColumnsBound = dataReader.readUnsignedShort();
            }
            /**
             * {@link org.apache.cassandra.db.ClusteringPrefix.Deserializer}
             */
            skipClustering(dataReader, types, clusteringColumnsBound);
            // same for row/tombstone
            long rowSize = dataReader.readUnsignedVInt();
            dataReader.seek(dataReader.getPosition() + rowSize);

            return checkNextFlagsAfterStaticRowOrUnfilteredStart(autoContinue);
        }
        catch (Exception e)
        {
            return corruptSSTable(e);
        }
    }

    public int skipRowCells(long unfilteredDataStart, long unfilteredSize, boolean autoContinue)
    {
        if (!(isState(state,CELL_HEADER_START | CELL_VALUE_START | CELL_END))) throw new IllegalStateException();

        try
        {
            dataReader.seek(unfilteredDataStart + unfilteredSize);
            return checkNextFlagsAfterStaticRowOrUnfilteredStart(autoContinue);
        }
        catch (IOException e)
        {
            return corruptSSTable(e);
        }
    }

    public int continueReading() {
        // TODO: can be optimized by pre-calculating next state when the flags are read
        switch (state)
        {
            case PARTITION_END:
                state = dataReader.isEOF() ? DONE : PARTITION_START;
                break;
            case UNFILTERED_END:
                if (UnfilteredSerializer.isEndOfPartition(basicUnfilteredFlags))
                {
                    state = PARTITION_END;
                }
                else
                {
                    state = UnfilteredSerializer.isRow(basicUnfilteredFlags) ? ROW_START : TOMBSTONE_START;
                }
                break;
            case CELL_END:
                if (cellCursor.hasNext())
                {
                    state = CELL_HEADER_START;
                }
                else
                {
                    state = UNFILTERED_END;
                }
                break;
            default:
                throw new IllegalStateException("Cannot continue reading in current state: " + state);
        }
        return state;
    }

    private int checkNextFlagsAfterPartitionStart(boolean autoContinue) throws IOException
    {
        long preFlagsPosition = dataReader.getPosition();
        basicUnfilteredFlags = dataReader.readUnsignedByte();
        if (UnfilteredSerializer.isEndOfPartition(basicUnfilteredFlags))
        {
            state = !autoContinue ? PARTITION_END :
                                    dataReader.isEOF() ? DONE : PARTITION_START;
        }
        else if (UnfilteredSerializer.isExtended(basicUnfilteredFlags))
        {
            state = STATIC_ROW_START;
            extendedFlags = dataReader.readUnsignedByte();
            validateStaticRowFlags(preFlagsPosition);
        }
        else
        {
            state = UnfilteredSerializer.isRow(basicUnfilteredFlags) ? ROW_START : TOMBSTONE_START;
        }
        return state;
    }

    private void validateStaticRowFlags(long preFlagsPosition)
    {
        if (!UnfilteredSerializer.isStatic(extendedFlags))
        {
            corruptSSTable("Row at: " + preFlagsPosition + " has extended flags but is not static, extendedFlags: " + extendedFlags);
        }
        if (!UnfilteredSerializer.isRow(basicUnfilteredFlags))
        {
            corruptSSTable("Static row at: " + preFlagsPosition + " is not a row, flags: " + basicUnfilteredFlags);
        }
        if (!hasStaticColumns)
        {
            corruptSSTable("Row at: " + preFlagsPosition + " is static, but table has no static columns " + ssTableReader.metadata());
        }
        if (UnfilteredSerializer.deletionIsShadowable(extendedFlags))
        {
            throw new UnsupportedOperationException("Static row at: " + preFlagsPosition + " has deletionIsShadowable, which is deprecated since 4.0");
        }
    }

    private int checkNextFlagsAfterStaticRowOrUnfilteredStart(boolean autoContinue) throws IOException
    {
        int flags = this.basicUnfilteredFlags = dataReader.readUnsignedByte();
        if (UnfilteredSerializer.isExtended(flags))
        {
            corruptSSTable("Unexpected static row (flags=" + flags + ") mid-partition, at position: " + (dataReader.getPosition() - 1));
        }

        if (!autoContinue) {
            return this.state = UNFILTERED_END;
        }
        else
        {
            return this.state = nextStateMidPartition(flags);
        }
    }

    private int checkNextFlagsAfterCellValuesEnd() throws IOException
    {
        int flags = this.basicUnfilteredFlags = dataReader.readUnsignedByte();
        if (UnfilteredSerializer.isExtended(flags))
        {
            corruptSSTable("Unexpected static row (flags=" + flags + ") mid-partition, at position: " + (dataReader.getPosition() - 1));
        }
        return this.state = CELL_END;
    }

    private int corruptSSTable(Exception e)
    {
        ssTableReader.markSuspect();
        if (e instanceof CorruptSSTableException)
            throw (CorruptSSTableException) e;

        throw new CorruptSSTableException(e, ssTableReader.getFilename());
    }

    protected int corruptSSTable(String message)
    {
        return corruptSSTable(new IllegalStateException(message));
    }

    private int nextStateMidPartition(int basicUnfilteredFlags)
    {
        if (UnfilteredSerializer.isEndOfPartition(basicUnfilteredFlags))
        {
            return dataReader.isEOF() ? DONE : PARTITION_START;
        }
        else if (UnfilteredSerializer.isRow(basicUnfilteredFlags))
        {
            return ROW_START;
        }
        else
        {
            return TOMBSTONE_START;
        }
    }

    public boolean isEOF() {
        return state == DONE || dataReader.isEOF();
    }

    public int state()
    {
        return state;
    }

    public long position() {
        return dataReader.getFilePointer();
    }

    public long uncompressedLength()
    {
        return ssTableReader.uncompressedLength();
    }

    public SSTableReader ssTableReader()
    {
        return ssTableReader;
    }

    public CellCursor cellCursor()
    {
        return cellCursor;
    }
}
