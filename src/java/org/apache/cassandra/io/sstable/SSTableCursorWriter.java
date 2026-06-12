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
import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

import org.agrona.collections.IntArrayList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Threshold;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.db.rows.UnfilteredSerializer.HAS_ALL_COLUMNS;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.HAS_DELETION;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.HAS_TIMESTAMP;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.HAS_TTL;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.IS_MARKER;
import static org.apache.cassandra.db.rows.UnfilteredSerializer.isExtended;

public class SSTableCursorWriter implements AutoCloseable
{
    private static final UnfilteredSerializer SERIALIZER = UnfilteredSerializer.serializer;
    private static final ColumnMetadata[] EMPTY_COL_META = new ColumnMetadata[0];
    private final SortedTableWriter<?,?> ssTableWriter;
    private final SequentialWriter dataWriter;
    private final SortedTableWriter.AbstractIndexWriter indexWriter;
    private final DeletionTime.Serializer deletionTimeSerializer;
    private final MetadataCollector metadataCollector;
    private final SerializationHeader serializationHeader;
    private final boolean hasStaticColumns;

    private long partitionStart;
    // Offset within the current partition of the previous non-static unfiltered's first byte.
    // Used to write the previousUnfilteredSize field exactly as the iterator path does
    // (see SortedTablePartitionWriter.addUnfiltered): rows/markers write the distance from the
    // previous unfiltered's start; static rows write 0 and do not advance this offset.
    private long previousRowStartOffset;
    // ROW contents, needed because of the order of writing and the var int fields
    private int rowFlags; // discovered as we go along
    private int rowExtendedFlags;
    private final DataOutputBuffer rowHeaderBuffer = new DataOutputBuffer(); // holds the contents between FLAGS and SIZE
    private final DataOutputBuffer rowBuffer = new DataOutputBuffer();
    private final ReusableDeletionTime openMarker = ReusableDeletionTime.live();

    // Complex (multi-cell) column staging for the current row: cells stream into rowBuffer
    // as usual; per complex column a marker records where its cells start in rowBuffer, the
    // merged column deletion, and the surviving cell count. writeRowEnd computes the final
    // cell-section length arithmetically and streams rowBuffer segments to the data file,
    // writing each marker's [deletion if row flag][count] header in between — one copy of
    // the cell bytes. Rows without complex columns keep the direct path untouched.
    private static final int MAX_COMPLEX_MARKERS_GROWTH = 8;
    private int complexMarkerCount;
    private int[] markerStartOffset = new int[MAX_COMPLEX_MARKERS_GROWTH];
    private int[] markerEndOffset = new int[MAX_COMPLEX_MARKERS_GROWTH];
    private int[] markerCellCount = new int[MAX_COMPLEX_MARKERS_GROWTH];
    private long[] markerDeletionMfda = new long[MAX_COMPLEX_MARKERS_GROWTH];
    private long[] markerDeletionLdt = new long[MAX_COMPLEX_MARKERS_GROWTH];
    private final DeletionTime.ReusableDeletionTime reusableMarkerDeletion = DeletionTime.ReusableDeletionTime.live();
    private ColumnMetadata lastCellColumn;

    private final ColumnMetadata[] staticColumns;
    private final ColumnMetadata[] regularColumns;
    private final IntArrayList missingColumns = new IntArrayList();
    private ColumnMetadata[] columns; // points to static/regular
    private int columnsWrittenCount = 0;
    private int nextCellIndex = 0;
    // Format-specific index production (BIG: promoted blocks + Index.db; BTI: tries)
    private final CursorIndexWriter cursorIndexWriter;

    private SSTableCursorWriter(
        Descriptor desc,
        SortedTableWriter<?,?> ssTableWriter,
        SequentialWriter dataWriter,
        SortedTableWriter.AbstractIndexWriter indexWriter,
        MetadataCollector metadataCollector,
        SerializationHeader serializationHeader)
    {
        this.ssTableWriter = ssTableWriter;
        this.dataWriter = dataWriter;
        this.indexWriter = indexWriter;
        this.deletionTimeSerializer = DeletionTime.getSerializer(desc.version);
        this.metadataCollector = metadataCollector;
        this.serializationHeader = serializationHeader;
        hasStaticColumns = serializationHeader.hasStatic();
        staticColumns = hasStaticColumns ? serializationHeader.columns(true).toArray(EMPTY_COL_META) : EMPTY_COL_META;
        regularColumns = serializationHeader.columns(false).toArray(EMPTY_COL_META);
        AbstractType<?>[] clusteringTypes = serializationHeader.clusteringTypes().toArray(AbstractType[]::new);
        if (ssTableWriter instanceof org.apache.cassandra.io.sstable.format.bti.BtiTableWriter)
            this.cursorIndexWriter = new org.apache.cassandra.io.sstable.format.bti.BtiCursorIndexWriter(
                (org.apache.cassandra.io.sstable.format.bti.BtiTableWriter) ssTableWriter,
                serializationHeader.clusteringTypes().isEmpty() ? new org.apache.cassandra.db.ClusteringComparator()
                                                                : new org.apache.cassandra.db.ClusteringComparator(serializationHeader.clusteringTypes()),
                clusteringTypes);
        else
            this.cursorIndexWriter = new BigCursorIndexWriter((BigTableWriter.IndexWriter) indexWriter,
                                                              this.deletionTimeSerializer,
                                                              new ClusteringDescriptor(clusteringTypes));
    }

    public SSTableCursorWriter(SortedTableWriter<?,?> ssTableWriter)
    {
        this(ssTableWriter.descriptor,
             ssTableWriter,
             ssTableWriter.dataWriter,
             ssTableWriter.indexWriter,
             ssTableWriter.metadataCollector,
             ssTableWriter.partitionWriter.getHeader());
    }

    @Override
    public void close()
    {
        SSTableReader finish = ssTableWriter.finish(false);
        if (finish != null) {
            Ref<SSTableReader> ref = finish.ref();
            if (ref != null) ref.close();
        }
        ssTableWriter.close();
    }

    public long getPartitionStart()
    {
        return partitionStart;
    }

    public long getPosition()
    {
        return dataWriter.position();
    }

    public int writePartitionStart(byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime) throws IOException
    {
        openMarker.resetLive();

        partitionStart = dataWriter.position();
        previousRowStartOffset = 0;
        writePartitionHeader(partitionKey, partitionKeyLength, partitionDeletionTime);
        cursorIndexWriter.startPartition(partitionStart, dataWriter.position());
        // immediately after startPartition this is the partition header length — always small
        return Math.toIntExact(cursorIndexWriter.indexBlockStartOffset());
    }

    public void writePartitionEnd(org.apache.cassandra.db.DecoratedKey decoratedKey, byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime, int headerLength) throws IOException
    {
        SERIALIZER.writeEndOfPartition(dataWriter);
        long partitionEnd = dataWriter.position();
        long partitionSize = partitionEnd - partitionStart;
        addPartitionMetadata(partitionKey, partitionKeyLength, partitionSize, partitionDeletionTime);

        /** {@link SortedTableWriter#endPartition(DecoratedKey, DeletionTime)}
         lastWrittenKey = key; // tracked for verification, see {@link SortedTableWriter#verifyPartition(DecoratedKey)}, checking the key size and sorting
         // first/last are retained for metadata {@link org.apache.cassandra.io.sstable.format.SSTableWriter#finalizeMetadata()}. They are also exposed via
         // getters from the writer, but usage is unclear.
         last = lastWrittenKey;
         if (first == null)
         first = lastWrittenKey;
         // this is implemented differently for BIG/BTI
         createRowIndexEntry(key, partitionLevelDeletion, partitionEnd - 1);
         */
        cursorIndexWriter.endPartition(decoratedKey, partitionKey, partitionKeyLength, headerLength, partitionDeletionTime, partitionEnd);
    }


    final long guardrailsPartitionSizeWarning = Guardrails.partitionSize.warnValue(null);
    final long guardrailsPartitionTombstonesWarning = Guardrails.partitionTombstones.warnValue(null);

    /**
     *  update metadata like {@link SortedTableWriter#endPartition} and {@link SortedTableWriter#startPartition}
     */
    private void addPartitionMetadata(byte[] partitionKey, int partitionKeyLength, long partitionSize, DeletionTime partitionDeletionTime)
    {
        if (partitionSize > guardrailsPartitionSizeWarning)
            guardPartitionThreshold(Guardrails.partitionSize, partitionKey, partitionKeyLength, partitionSize);

        if (metadataCollector.totalTombstones > guardrailsPartitionTombstonesWarning)
            guardPartitionThreshold(Guardrails.partitionTombstones, partitionKey, partitionKeyLength, metadataCollector.totalTombstones);

        metadataCollector.updatePartitionDeletion(partitionDeletionTime);
        metadataCollector.addPartitionSizeInBytes(partitionSize);
        metadataCollector.addKey(partitionKey, 0, partitionKeyLength);
        metadataCollector.addCellPerPartitionCount();
    }

    private void guardPartitionThreshold(Threshold guardrail, byte[] partitionKey, int partitionKeyLength, long size)
    {
        if (guardrail.triggersOn(size, null))
        {
            String message = String.format("%s.%s:%s on sstable %s",
                    ssTableWriter.metadata().keyspace,
                    ssTableWriter.metadata().name,
                    ssTableWriter.metadata().partitionKeyType.getString(ByteBuffer.wrap(partitionKey, 0, partitionKeyLength)),
                    ssTableWriter.getFilename());
            guardrail.guard(size, message, true, null);
        }
    }

    private void writePartitionHeader(byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime) throws IOException
    {
        dataWriter.writeShort(partitionKeyLength);
        dataWriter.write(partitionKey, 0, partitionKeyLength);
        deletionTimeSerializer.serialize(partitionDeletionTime, dataWriter);
    }

    public boolean writeEmptyStaticRow() throws IOException
    {
        if (!hasStaticColumns)
            return false;
        rowFlags = UnfilteredSerializer.EXTENSION_FLAG;
        rowExtendedFlags = UnfilteredSerializer.IS_STATIC;
        columns = staticColumns;
        // TOD: we should be able to skip the use of the row buffers in this special case, maybe it doesn't matter
        rowHeaderBuffer.clear();
        rowBuffer.clear();
        complexMarkerCount = 0;
        lastCellColumn = null;
        columnsWrittenCount = 0;
        missingColumns.clear();
        writeRowEnd(null, false);

        cursorIndexWriter.staticRowWritten(dataWriter.position());
        return true;
    }

    public void writeRowStart(LivenessInfo livenessInfo, DeletionTime deletionTime, boolean isStatic) throws IOException
    {
        if (isStatic) {
            rowFlags = UnfilteredSerializer.EXTENSION_FLAG;
            rowExtendedFlags = UnfilteredSerializer.IS_STATIC;
            columns = staticColumns;
        }
        else {
            rowFlags = 0;
            rowExtendedFlags = 0;
            columns = regularColumns;
        }
        // NOTE: Data after this point needs a computed ahead of write size. This, combined with the cost of rewriting
        // the size after the writing completes, means we have to buffer the row timestamps (most likely to differ in length)
        // and the row columns data (will differ if they use their own timestamps, probably). Unfortunate.
        // rest of header
        rowHeaderBuffer.clear();
        missingColumns.clear();
        rowBuffer.clear();
        columnsWrittenCount = 0;
        nextCellIndex = 0;
        complexMarkerCount = 0;
        lastCellColumn = null;

        // copy TS/TTL/deletion data
        rowFlags |= writeRowTimeData(livenessInfo, deletionTime, rowHeaderBuffer);
    }

    /**
     * See {@link UnfilteredSerializer#serialize(Row, SerializationHelper, DataOutputPlus, long, int)}
     */
    private int writeRowTimeData(LivenessInfo livenessInfo, DeletionTime deletionTime, DataOutputPlus writer) throws IOException
    {
        int flags = 0;
        boolean writtenLivenessMetadata = false;

        if (!livenessInfo.isEmpty())
        {
            flags |= HAS_TIMESTAMP;
            serializationHeader.writeTimestamp(livenessInfo.timestamp(), writer);
            metadataCollector.update(livenessInfo);
            writtenLivenessMetadata = true;
        }
        if (livenessInfo.isExpiring())
        {
            flags |= HAS_TTL;
            serializationHeader.writeTTL(livenessInfo.ttl(), writer);
            serializationHeader.writeLocalDeletionTime(livenessInfo.localExpirationTime(), writer);
            if (!writtenLivenessMetadata) metadataCollector.update(livenessInfo);
        }
        if (!deletionTime.isLive())
        {
            flags |= HAS_DELETION;
            writeDeletionTime(deletionTime, writer);
        }

        /**
         * Metadata calls matching: {@link org.apache.cassandra.db.rows.Rows#collectStats}
         * But the collection of data is conditional and the cell metadata is collected elsewhere.
         */
        return flags;
    }

    private void writeDeletionTime(DeletionTime deletionTime, DataOutputPlus writer) throws IOException
    {
        serializationHeader.writeDeletionTime(deletionTime, writer);
        metadataCollector.update(deletionTime);
    }

    /**
     * Opens a complex (multi-cell) column for the current row with its merged column-level
     * deletion. Must precede any of the column's writeCellHeader calls; may also stand alone
     * for a deletion-only column with zero surviving cells.
     */
    public void startComplexColumn(ColumnMetadata column, DeletionTime mergedDeletion) throws IOException
    {
        closeOpenComplexMarker();
        advanceColumnSubset(column);
        if (complexMarkerCount == markerStartOffset.length)
        {
            int n = complexMarkerCount + MAX_COMPLEX_MARKERS_GROWTH;
            markerStartOffset = java.util.Arrays.copyOf(markerStartOffset, n);
            markerEndOffset = java.util.Arrays.copyOf(markerEndOffset, n);
            markerCellCount = java.util.Arrays.copyOf(markerCellCount, n);
            markerDeletionMfda = java.util.Arrays.copyOf(markerDeletionMfda, n);
            markerDeletionLdt = java.util.Arrays.copyOf(markerDeletionLdt, n);
        }
        markerStartOffset[complexMarkerCount] = rowBuffer.getLength();
        markerEndOffset[complexMarkerCount] = -1;
        markerCellCount[complexMarkerCount] = 0;
        markerDeletionMfda[complexMarkerCount] = mergedDeletion.markedForDeleteAt();
        markerDeletionLdt[complexMarkerCount] = mergedDeletion.localDeletionTime();
        complexMarkerCount++;
        lastCellColumn = column;
        columnsWrittenCount++;
        // tombstone stats for the merged deletion are collected when the row assembly
        // writes it (writeDeletionTime updates the collector); counting here would double
    }

    private void closeOpenComplexMarker()
    {
        if (complexMarkerCount > 0 && markerEndOffset[complexMarkerCount - 1] < 0)
            markerEndOffset[complexMarkerCount - 1] = rowBuffer.getLength();
    }

    private void advanceColumnSubset(ColumnMetadata cellColumn)
    {
        for (; nextCellIndex < columns.length; nextCellIndex++) {
            if (columns[nextCellIndex].compareTo(cellColumn) == 0)
                break;
            missingColumns.addInt(nextCellIndex);
        }
        if (nextCellIndex == columns.length)
            throw new IllegalStateException("Column not found: " + cellColumn +" or cell writes out of order, or bug.");
        nextCellIndex++;
    }

    /** Appends the current complex cell's path (vint length + bytes) to the cell stream. */
    public void writeCellPath(byte[] pathBuffer, int pathLength) throws IOException
    {
        rowBuffer.writeUnsignedVInt32(pathLength);
        rowBuffer.write(pathBuffer, 0, pathLength);
    }

    public void writeCellHeader(int cellFlags, ReusableLivenessInfo cellLiveness, ColumnMetadata cellColumn) throws IOException
    {
        if (cellColumn.isComplex())
        {
            // subset advance + counting happened in startComplexColumn; just count the cell.
            // Compare by NAME, not identity: the winning cell may come from a source whose
            // open-time header holds a different ColumnMetadata instance for this column
            // (sstables flushed across a type-touching ALTER — CASSANDRA-13776 shape).
            if (lastCellColumn != cellColumn
                && (lastCellColumn == null || !lastCellColumn.name.equals(cellColumn.name)))
                throw new IllegalStateException("complex cell without startComplexColumn: " + cellColumn);
            markerCellCount[complexMarkerCount - 1]++;
        }
        else
        {
            closeOpenComplexMarker();
            advanceColumnSubset(cellColumn);
            lastCellColumn = cellColumn;
            columnsWrittenCount++;
        }
        writeCellHeader(cellFlags, cellLiveness, rowBuffer);
    }

    private void writeCellHeader(int cellFlags, ReusableLivenessInfo cellLiveness, DataOutputPlus writer) throws IOException
    {
        writer.writeByte(cellFlags);
        if (!Cell.Serializer.useRowTimestamp(cellFlags)) {
            long timestamp = cellLiveness.timestamp();
            serializationHeader.writeTimestamp(timestamp, writer);
        }
        if (!Cell.Serializer.useRowTTL(cellFlags)) {
            boolean isDeleted = Cell.Serializer.isDeleted(cellFlags);
            boolean isExpiring = Cell.Serializer.isExpiring(cellFlags);
            if (isDeleted || isExpiring) {
                // TODO: is this conversion from LET to LDT correct?
                serializationHeader.writeLocalDeletionTime(cellLiveness.localExpirationTime(), writer);
            }
            if (isExpiring) {
                serializationHeader.writeTTL(cellLiveness.ttl(), writer);
            }
        }
        /**
         * matching {@link org.apache.cassandra.db.rows.Cells#collectStats};
         */
        metadataCollector.updateCellLiveness(cellLiveness);
    }

    public int writeCellValue(SSTableCursorReader cursor, byte[] copyColumnValueBuffer) throws IOException
    {
        return cursor.copyCellValue(rowBuffer, copyColumnValueBuffer);
    }

    public void writeCellValue(DataOutputBuffer tempCellBuffer) throws IOException
    {
        rowBuffer.write(tempCellBuffer.getData(), 0, tempCellBuffer.getLength());
    }

    public void writeRowEnd(UnfilteredDescriptor rHeader, boolean updateClusteringMetadata) throws IOException
    {
        boolean isExtended = isExtended(rowFlags);
        boolean isStatic = isExtended && UnfilteredSerializer.isStatic(rowExtendedFlags);
        int columnsLength = columns.length;

        // Rows containing complex columns interleave their final cell section at write time:
        // the cells streamed into rowBuffer; each marker contributes [deletion if
        // HAS_COMPLEX_DELETION][count vint] ahead of its cells, written directly to the data
        // file between rowBuffer segments (no second staging copy — the row size vint needs
        // only the LENGTH, computed arithmetically below). HAS_COMPLEX_DELETION (row-level)
        // is decidable only now, and MUST be decided before the flags byte is emitted: any
        // marker with a non-LIVE deletion sets it, and then EVERY complex column in the row
        // serializes a deletion (LIVE included), matching UnfilteredSerializer.
        long cellSectionLength = rowBuffer.getLength();
        boolean hasComplexDeletion = false;
        if (complexMarkerCount > 0)
        {
            closeOpenComplexMarker();
            for (int i = 0; i < complexMarkerCount; i++)
                hasComplexDeletion |= markerDeletionMfda[i] != DeletionTime.LIVE.markedForDeleteAt()
                                      || markerDeletionLdt[i] != DeletionTime.LIVE.localDeletionTime();
            if (hasComplexDeletion)
                rowFlags |= UnfilteredSerializer.HAS_COMPLEX_DELETION;

            for (int i = 0; i < complexMarkerCount; i++)
            {
                if (hasComplexDeletion)
                {
                    reusableMarkerDeletion.reset(markerDeletionMfda[i], markerDeletionLdt[i]);
                    cellSectionLength += serializationHeader.deletionTimeSerializedSize(reusableMarkerDeletion);
                }
                cellSectionLength += TypeSizes.sizeofUnsignedVInt(markerCellCount[i]);
            }
        }

        if (columnsWrittenCount == columnsLength)
        {
            rowFlags |= HAS_ALL_COLUMNS;
        }
        else if (columnsWrittenCount == 0) {
            // Same as Columns.serializer.serializeSubset(Columns.NONE, serializationHeader.columns(isStatic), rowHeaderBuffer)
            if (columnsLength < 64) {
                // all the bits are set, because all the columns are missing, value is always positive
                rowHeaderBuffer.writeUnsignedVInt(-1L >>> (64 - columnsLength));
            }
            else {
                // no columns are present, nothing to write
                rowHeaderBuffer.writeUnsignedVInt32(columnsLength);
            }
        }
        else if (columnsWrittenCount < columnsLength)
        {
            for (; nextCellIndex < columnsLength; nextCellIndex++)
                missingColumns.addInt(nextCellIndex);

            if (columnsLength < 64) {
                // set a bit for every missing column
                long mask = 0;
                for (int missingIndex : missingColumns) {
                    mask |= (1L << missingIndex);
                }
                rowHeaderBuffer.writeUnsignedVInt(mask);
            }
            else {
                encodeLargeColumnsSubset();
            }
        }
        long unfilteredStartPosition = dataWriter.position();
        /** See: {@link UnfilteredSerializer#serialize} */
        dataWriter.writeByte(rowFlags);
        if (isExtended)
        {
            dataWriter.writeByte(rowExtendedFlags);
        }

        if (!isStatic)
        {
            byte[] clustering = rHeader.clusteringBytes();
            int clusteringLength = rHeader.clusteringLength();
            dataWriter.write(clustering, 0, clusteringLength);
        }

        // Matches UnfilteredSerializer.serialize: the row size includes the vint length of the
        // previousUnfilteredSize field, which is written between the size and the row body.
        // Static rows write 0 and do not advance the chain (UnfilteredSerializer.serializeStaticRow).
        long previousUnfilteredSize = 0;
        if (!isStatic)
        {
            long offsetInPartition = unfilteredStartPosition - partitionStart;
            previousUnfilteredSize = offsetInPartition - previousRowStartOffset;
            previousRowStartOffset = offsetInPartition;
        }


        dataWriter.writeUnsignedVInt32(Math.toIntExact(rowHeaderBuffer.getLength() + cellSectionLength
                                                       + TypeSizes.sizeofUnsignedVInt(previousUnfilteredSize)));
        dataWriter.writeUnsignedVInt(previousUnfilteredSize);

        dataWriter.write(rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());
        if (complexMarkerCount > 0)
        {
            // stream rowBuffer segments, interleaving each marker's [deletion][count] header
            int pos = 0;
            for (int i = 0; i < complexMarkerCount; i++)
            {
                int start = markerStartOffset[i];
                dataWriter.write(rowBuffer.getData(), pos, start - pos);
                if (hasComplexDeletion)
                {
                    reusableMarkerDeletion.reset(markerDeletionMfda[i], markerDeletionLdt[i]);
                    writeDeletionTime(reusableMarkerDeletion, dataWriter);
                }
                dataWriter.writeUnsignedVInt32(markerCellCount[i]);
                int end = markerEndOffset[i];
                dataWriter.write(rowBuffer.getData(), start, end - start);
                pos = end;
            }
            dataWriter.write(rowBuffer.getData(), pos, rowBuffer.getLength() - pos);
        }
        else
        {
            dataWriter.write(rowBuffer.getData(), 0, rowBuffer.getLength());
        }

        long unfilteredEndPosition = getPosition();

        /**
         * Matching the: {@link org.apache.cassandra.db.rows.Rows#collectStats} along with above cell level metadata updates.
         * The iterator path only collects row stats for non-empty rows
         * ({@link org.apache.cassandra.io.sstable.format.SortedTableWriter#addStaticRow} guards with
         * !row.isEmpty()): an empty static row is still WRITTEN for static-column tables whose
         * partition has no static values, but it must not count towards totalRows/totalColumnsSet.
         * Empty == no cells, no liveness timestamp/TTL, no row deletion.
         */
        boolean rowIsEmpty = columnsWrittenCount == 0
                             && (rowFlags & (HAS_TIMESTAMP | HAS_TTL | HAS_DELETION)) == 0;
        if (!rowIsEmpty)
        {
            // matching Rows.collectStats/StatsAccumulation.accumulateOnColumnData: a complex
            // column counts toward totalColumnsSet only if it contributed >=1 cell; a
            // deletion-only column is present in the row (subset encoding above) but uncounted
            int statsColumnCount = columnsWrittenCount;
            for (int i = 0; i < complexMarkerCount; i++)
                if (markerCellCount[i] == 0)
                    statsColumnCount--;
            metadataCollector.updateColumnSetPerRow(statsColumnCount);
        }

        if (isStatic)
        {
            cursorIndexWriter.staticRowWritten(dataWriter.position());
        }
        else
        {
            updateMetadataAndIndexBlock(rHeader, unfilteredStartPosition, unfilteredEndPosition, updateClusteringMetadata);
        }
    }

    /**
     * See: {@link org.apache.cassandra.io.sstable.format.SortedTableWriter#addRangeTomstoneMarker}
     */
    public void writeRangeTombstone(UnfilteredDescriptor rangeTombstone, boolean updateClusteringMetadata) throws IOException
    {
        int tombstoneKind = rangeTombstone.clusteringKindEncoded();
        ClusteringPrefix.Kind kind = ClusteringPrefix.Kind.ALL_KINDS[tombstoneKind];
        long unfilteredStartPosition = getPosition();
        /** See: {@link org.apache.cassandra.db.rows.UnfilteredSerializer#serialize */
        dataWriter.writeByte((byte)IS_MARKER);
        /** See: {@link org.apache.cassandra.db.ClusteringBoundOrBoundary.Serializer#serialize} */
        dataWriter.writeByte(tombstoneKind);
        dataWriter.writeShort(rangeTombstone.clusteringColumnsBound());

        int clusteringLength = rangeTombstone.clusteringLength();
        if (clusteringLength != 0)
        {
            byte[] clustering = rangeTombstone.clusteringBytes();
            dataWriter.write(clustering, 0, clusteringLength);
        }
        rowHeaderBuffer.clear();

        if (kind.isBoundary())
        {
            writeDeletionTime(rangeTombstone.deletionTime(), rowHeaderBuffer);
            writeDeletionTime(rangeTombstone.deletionTime2(), rowHeaderBuffer);
            openMarker.reset(rangeTombstone.deletionTime2());
        }
        else
        {
            writeDeletionTime(rangeTombstone.deletionTime(), rowHeaderBuffer);
            if (kind.isOpen(false))
                openMarker.reset(rangeTombstone.deletionTime());
            else
                openMarker.resetLive();
        }

        // Matches UnfilteredSerializer.serialize(RangeTombstoneMarker...): marker size includes the
        // vint length of previousUnfilteredSize, written between the size and the marker body.
        long offsetInPartition = unfilteredStartPosition - partitionStart;
        long previousUnfilteredSize = offsetInPartition - previousRowStartOffset;
        previousRowStartOffset = offsetInPartition;
        dataWriter.writeUnsignedVInt32(rowHeaderBuffer.getLength()
                                       + TypeSizes.sizeofUnsignedVInt(previousUnfilteredSize));
        dataWriter.writeUnsignedVInt(previousUnfilteredSize);
        dataWriter.write(rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());

        long unfilteredEndPosition = getPosition();

        /** {@link org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter#addUnfiltered(Unfiltered)} */
        // if we hit the index block size that we have to index after, go ahead and index it.
        updateMetadataAndIndexBlock(rangeTombstone, unfilteredStartPosition, unfilteredEndPosition, updateClusteringMetadata);
    }

    private void updateMetadataAndIndexBlock(UnfilteredDescriptor unfilteredDescriptor,
                                             long unfilteredStartPosition,
                                             long unfilteredEndPosition,
                                             boolean updateClusteringMetadata) throws IOException
    {
        if (updateClusteringMetadata) updateClusteringMetadata(unfilteredDescriptor);
        cursorIndexWriter.rowWritten(unfilteredDescriptor, unfilteredStartPosition, unfilteredEndPosition, openMarker);
    }

    public void updateClusteringMetadata(UnfilteredDescriptor unfilteredDescriptor)
    {
        metadataCollector.updateClusteringValues(unfilteredDescriptor);
    }


    private long currentOffsetInPartition(long position)
    {
        return position - partitionStart;
    }

    private void encodeLargeColumnsSubset() throws IOException
    {
        rowHeaderBuffer.writeUnsignedVInt32(missingColumns.size());
        // Mode selection must mirror Columns.Serializer.serializeLargeSubset AND its
        // deserializer exactly: present-index mode iff presentCount < supersetCount / 2.
        // The previous condition (missing > supersetCount / 2) agreed for even superset
        // sizes but flipped the mode for odd sizes at missing == supersetCount/2 + 1,
        // which the deserializer then read in the WRONG mode — corrupted output.
        int presentCount = columns.length - missingColumns.size();
        if (presentCount < columns.length / 2)
        {
            // write present column indices: the gaps between missing indices, INCLUDING the
            // tail after the last missing index — the previous tail loop's bound was the
            // last missing index itself (vacuously empty), so present columns sorting after
            // the last missing one were silently dropped from the encoding and the
            // deserializer consumed row-body bytes as column indices — corrupted output
            int presentIndex = 0;
            for (int i = 0; i < missingColumns.size(); i++)
            {
                int missingIndex = missingColumns.get(i);
                for (; presentIndex < missingIndex; presentIndex++)
                    rowHeaderBuffer.writeUnsignedVInt32(presentIndex);
                presentIndex = missingIndex + 1;
            }
            for (; presentIndex < columns.length; presentIndex++)
                rowHeaderBuffer.writeUnsignedVInt32(presentIndex);
        }
        else
        {
            // write missing columns
            for (int missingIndex : missingColumns) {
                rowHeaderBuffer.writeUnsignedVInt32(missingIndex);
            }
        }
    }

    public void setLast(ByteBuffer key)
    {
        IPartitioner partitioner = ssTableWriter.getPartitioner();
        DecoratedKey last = partitioner.decorateKey(ByteBufferUtil.clone(key));
        ssTableWriter.setLast(last);
    }

    public void setFirst(ByteBuffer key)
    {
        IPartitioner partitioner = ssTableWriter.getPartitioner();
        DecoratedKey first = partitioner.decorateKey(ByteBufferUtil.clone(key));
        ssTableWriter.setFirst(first);
        ssTableWriter.setLast(first);
    }

    public IPartitioner partitioner()
    {
        return ssTableWriter.getPartitioner();
    }

    public DeletionTime openMarker() {
        return openMarker;
    }
}
