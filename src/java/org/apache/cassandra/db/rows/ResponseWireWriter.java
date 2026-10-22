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
package org.apache.cassandra.db.rows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ResponseSink;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A read-owned, MESSAGING-flavor
 * wire writer for {@code ReadResponse} intra-node bytes — the transcode sink's low-level primitive,
 * co-located with {@link UnfilteredSerializer}/{@link UnfilteredRowIteratorSerializer} because it
 * reproduces their EXACT byte grammar (verified against both, see their javadocs) rather than
 * inventing a new one. Two kinds of API here, for the two problems this class solves:
 * <ul>
 *   <li><b>Passthrough</b> (partition header/footer, static row, whole-row escape hatch, range
 *       tombstone markers): every one of these arrives as an ALREADY MATERIALIZED object (a
 *       {@code Row}, a {@code RangeTombstoneMarker}, the query's {@code SerializationHeader}) — the
 *       real {@link UnfilteredSerializer}/{@link UnfilteredRowIteratorSerializer} can serialize them
 *       directly with zero transcoding, so this class just forwards to them. No re-implementation.</li>
 *   <li><b>Streaming row assembly</b> ({@link #startRow}/{@link #addComplexDeletion}/{@link #addCell}/
 *       {@link #endRow}): the genuinely new work — reconstructing a row's flags/extended-flags/
 *       column-subset-bitmap bookkeeping from a STREAM of merge events, without ever forming a
 *       {@code Row} object to ask {@code row.hasComplexDeletion()}/{@code row.columnCount()}. The
 *       MESSAGING flavor pays no per-row size prefix (confirmed against
 *       {@code UnfilteredRowIteratorSerializer#serializeWithoutKey}: rows/markers are written with
 *       {@code UnfilteredSerializer.serializer.serialize(unfiltered, helper, out, version)} directly,
 *       no length-prefixed staging — {@code assert !header.isForSSTable()} guards this everywhere),
 *       so unlike {@link org.apache.cassandra.io.sstable.SSTableCursorWriter} (the on-disk analog,
 *       whose per-row SIZE vint forces it to buffer the WHOLE row body before it can even know that
 *       vint's own width) this writer only needs to defer the parts whose value is discovered late:
 *       {@code HAS_COMPLEX_DELETION} (row-level, decidable only once every complex column in the row
 *       has been seen) and, for the SAME reason, each complex column's own {@code [deletion][count]}
 *       prefix (decidable only once that column closes). Two small staging buffers absorb exactly
 *       that lateness — copy-adapted from {@code SSTableCursorWriter}'s
 *       {@code rowHeaderBuffer}/{@code rowBuffer} + complex-marker-offset discipline, retargeted to a
 *       flavor with no size prefix and no interleaved-rewrite-at-a-known-file-position requirement:
 *       <ul>
 *         <li>{@code rowHeaderBuffer}: timestamp/TTL/row-deletion (known at {@link #startRow}, since
 *             the merge already resolved the row's liveness/deletion before calling it) followed by
 *             the column-subset marker, APPENDED at {@link #endRow} once {@code hasAllColumns} is
 *             finally knowable — both fields belong before any column data on the wire, and staying
 *             in one buffer that is only flushed at {@code endRow} keeps them contiguous without a
 *             third buffer.</li>
 *         <li>{@code rowBody}: column data, written as cells stream in via {@link #addCell}. Simple
 *             columns write straight through (one {@code <cell>}, no wrapper). Complex columns need
 *             a {@code [deletion if row-level HAS_COMPLEX_DELETION][count]} header BEFORE their
 *             cells, but neither is knowable until the column (or the row) closes — so each complex
 *             column's cell bytes are tracked by a [start, end) offset range into {@code rowBody}
 *             (the {@code marker*} arrays below) instead of being length-prefixed inline; instead of
 *             the RE-WRITE {@code SSTableCursorWriter} needs to fix up a length vint on a
 *             file-backed writer, this discipline is not needed once the row's markers are staged in
 *             buffers, so it is written as-is by copying the identical strategy anyway for
 *             uniformity: {@link #endRow} streams {@code rowBody} to {@code out} in offset-range
 *             segments, interleaving each marker's header immediately before its segment.</li>
 *       </ul>
 *       Two-phase discipline throughout: nothing this writer stages for a row reaches {@code out}
 *       (the final response buffer) before {@link #endRow} confirms the row is complete — a
 *       row-local scratch buffer, flush-on-{@code endRow}, never streamed byte-by-byte as events
 *       arrive.  A row the caller drops after staging it (a row filter or a limit rejects it) is
 *       discarded by {@link #abandonRow} and never reaches {@code out}.</li>
 * </ul>
 * Purge-free, exactly like the merge core it sits below: every event this class receives is
 * assumed ALREADY PURGED by the caller ({@code org.apache.cassandra.db.ResponseSink}) — this class only
 * ever encodes what it is handed.
 */
public final class ResponseWireWriter implements ResponseSink.Output
{
    /**
     * An output whose written bytes can be changed or dropped, so a row is written in place before
     * its flags are known, with no staging copy.
     */
    public interface PatchableOutput
    {
        /** The number of bytes written. */
        int getLength();

        /** Overwrites one byte already written. */
        void patchByte(int position, int value);

        /** Drops everything written past {@code length}. */
        void truncate(int length);

        /** Inserts {@code length} bytes at {@code position}, moving what follows. */
        void insert(int position, byte[] bytes, int offset, int length) throws IOException;
    }

    private final DataOutputPlus out;
    private SerializationHeader header;
    private SerializationHelper helper;
    private final int version;

    // ---- row-local staging (reset per row by startRow; flushed to `out` only by endRow) ----
    /** Made with {@link #rowBody}, or at the first in-place row that lacks some header column. */
    private DataOutputBuffer rowHeaderBuffer;
    /** Made only for a header whose rows are staged, not written in place. */
    private DataOutputBuffer rowBody;
    /** The open row's clustering in its wire form, {@code Clustering.serializer}'s, valid until endRow. */
    private byte[] rowClustering;
    private int rowClusteringLength;
    private LivenessInfo rowLiveness;
    private DeletionTime rowDeletion;
    private boolean rowOpen;
    /** distinct columns touched this row, in arrival (= header column) order — row.columns()'s
     *  streaming equivalent; also row.columnCount() via .size(). */
    private final List<ColumnMetadata> rowColumns = new ArrayList<>();
    /** the complex column the next addCell/addComplexDeletion call would extend, or null */
    private ColumnMetadata openComplexColumn;

    // Complex-column marker bookkeeping — SSTableCursorWriter's copy-adapted offset-range
    // discipline (see class javadoc): each entry is [start, end) into rowBody plus the column's
    // merged deletion, so endRow can interleave the row-level-decided [deletion][count] header
    // immediately before the already-buffered cell bytes without a second staging copy.
    private static final int MARKER_GROWTH = 8;
    private static final int[] NO_INTS = new int[0];
    private static final long[] NO_LONGS = new long[0];
    private int complexMarkerCount;
    // empty until the first complex column, which most rows never have
    private int[] markerStart = NO_INTS;
    private int[] markerEnd = NO_INTS;
    private int[] markerCellCount = NO_INTS;
    private long[] markerDeletionMfda = NO_LONGS;
    private long[] markerDeletionLdt = NO_LONGS;
    private final DeletionTime.ReusableDeletionTime reusableDeletion = DeletionTime.ReusableDeletionTime.live();

    /** Rows go straight to {@code out}: it is patchable and no complex column needs a prefix
     *  decided at the row's end.  Otherwise they are staged in {@link #rowBody}. */
    private PatchableOutput inPlace;
    /** Where the open row's cells go: {@code out} when written in place, else {@link #rowBody}. */
    private DataOutputPlus body;
    /** In place: where the open row starts, and where its cells start. */
    private int rowStart;
    private int rowCellsStart;

    /** The table has dropped columns, so a row can carry a cell of a column the header does not. */
    private final boolean tableHasDroppedColumns;

    private AbstractType<?>[] clusteringTypes;

    /**
     * @param tableHasDroppedColumns whether the table has dropped columns: only then can a row
     *                               carry a cell of a column the header does not (CASSANDRA-21607),
     *                               and only then does {@link #endRow} check the row's columns
     */
    public ResponseWireWriter(DataOutputPlus out, SerializationHeader header, int version, boolean tableHasDroppedColumns)
    {
        this.out = out;
        this.tableHasDroppedColumns = tableHasDroppedColumns;
        this.version = version;
        useHeader(header);
    }

    /**
     * Writes the next partitions with {@code header}, keeping this writer's buffers.  A range read
     * calls this when a partition's header differs from the one before.  No row may be open.
     */
    public void useHeader(SerializationHeader header)
    {
        assert !header.isForSSTable() : "ResponseWireWriter is the MESSAGING flavor only";
        assert !rowOpen : "useHeader called while a row is open";
        this.header = header;
        this.clusteringTypes = header.clusteringTypes().toArray(AbstractType[]::new);
        this.inPlace = out instanceof PatchableOutput && !header.columns(false).hasComplex() ? (PatchableOutput) out : null;
        if (inPlace == null && rowBody == null)
        {
            rowBody = new DataOutputBuffer();
            if (rowHeaderBuffer == null)
                rowHeaderBuffer = new DataOutputBuffer();
        }
        this.body = inPlace != null ? out : rowBody;
        this.helper = new SerializationHelper(header);
    }

    /** The clustering types the response header serializes clusterings with. */
    @Override
    public AbstractType<?>[] clusteringTypes()
    {
        return clusteringTypes;
    }

    @Override
    public List<AbstractType<?>> clusteringTypesList()
    {
        return header.clusteringTypes();
    }

    // ---------------------------------------------------------------- partition-level passthrough
    // Mirrors UnfilteredRowIteratorSerializer.serializeWithoutKey's grammar
    // (<flags><s_header>[<partition_deletion>][<static_row>]…<EOP>) exactly, sharing its flag bit
    // values (package-private in that class for this reuse) rather than redefining them.

    public void writeKey(ByteBuffer key) throws IOException
    {
        ByteBufferUtil.writeWithVIntLength(key, out);
    }

    // where the open partition starts, and where its header starts, in a patchable output
    private int partitionStart;
    private int headerStart;

    /** The "has next partition" flag, the key, the header and the static row.  Needs a
     *  {@link PatchableOutput}, which can drop or rewrite them. */
    @Override
    public void openPartition(DecoratedKey key, boolean reversed, DeletionTime partitionDeletion, Row staticRow,
                              ColumnFilter selection) throws IOException
    {
        PatchableOutput patchable = (PatchableOutput) out;
        partitionStart = patchable.getLength();
        out.writeBoolean(true);
        writeKey(key.getKey());
        headerStart = patchable.getLength();
        writeHeader(false, reversed, partitionDeletion, staticRow != null, selection);
        if (staticRow != null)
            writeStaticRow(staticRow);
    }

    @Override
    public void removePartition()
    {
        ((PatchableOutput) out).truncate(partitionStart);
    }

    @Override
    public void closeEmptyPartition(boolean reversed, DeletionTime partitionDeletion, ColumnFilter selection) throws IOException
    {
        ((PatchableOutput) out).truncate(headerStart);
        writeHeader(true, reversed, partitionDeletion, false, selection);
    }

    @Override
    public void closePartition() throws IOException
    {
        writeEndOfPartition();
    }

    /** {@code isEmpty} short-circuits to the single-byte IS_EMPTY form — the caller (which drives
     *  the merge before assembling the envelope) knows this only once the merge has finished, so it
     *  is a parameter rather than something this class infers. */
    public void writeHeader(boolean isEmpty, boolean isReversed, DeletionTime partitionDeletion,
                            boolean hasStaticRow, ColumnFilter selection) throws IOException
    {
        int flags = isReversed ? UnfilteredRowIteratorSerializer.IS_REVERSED : 0;
        if (isEmpty)
        {
            out.writeByte((byte) (flags | UnfilteredRowIteratorSerializer.IS_EMPTY));
            return;
        }
        if (!partitionDeletion.isLive())
            flags |= UnfilteredRowIteratorSerializer.HAS_PARTITION_DELETION;
        if (hasStaticRow)
            flags |= UnfilteredRowIteratorSerializer.HAS_STATIC_ROW;
        out.writeByte((byte) flags);
        SerializationHeader.serializer.serializeForMessaging(header, selection, out, hasStaticRow);
        if (!partitionDeletion.isLive())
            header.writeDeletionTime(partitionDeletion, out);
    }

    /** The static row is already a fully materialized {@code Row} (built directly by
     *  {@code mergeStaticRows}, outside the per-row-group merge this class otherwise serves) — no
     *  transcoding needed, straight passthrough to the real serializer. */
    public void writeStaticRow(Row staticRow) throws IOException
    {
        UnfilteredSerializer.serializer.serializeStaticRow(staticRow, helper, out, version);
    }

    /** Whole-row escape hatch passthrough: the merged row IS this already-live object (see
     *  {@code CursorReadMerger.MergeSink#addRow}) — nothing to transcode. */
    @Override
    public void writeRow(Row row) throws IOException
    {
        UnfilteredSerializer.serializer.serialize(row, helper, out, version);
    }

    /** Range tombstone marker passthrough: {@code CursorReadMerger.mergeMarkerGroup} always builds
     *  a real {@code RangeTombstoneBoundMarker}/{@code RangeTombstoneBoundaryMarker} before calling
     *  the sink — nothing to transcode. */
    @Override
    public void writeMarker(RangeTombstoneMarker marker) throws IOException
    {
        UnfilteredSerializer.serializer.serialize(marker, helper, out, version);
    }

    public void writeEndOfPartition() throws IOException
    {
        UnfilteredSerializer.serializer.writeEndOfPartition(out);
    }

    // ---------------------------------------------------------------- streaming row assembly
    // Mirrors UnfilteredSerializer.serialize(Row,...)/serializeRowBody's grammar
    // (<flags>[<extflags>]<clustering><ts><ttl><deletion>[<columns>]<columns_data>), assembled
    // incrementally from CursorReadMerger.MergeSink's row-group event stream instead of pulled from
    // a materialized Row via Row.apply(). See the class javadoc for the staging discipline.

    /**
     * Opens a row.  The arguments must stay unchanged until {@link #endRow} or {@link #abandonRow}.
     *
     * @param clustering the row's clustering serialized as {@code Clustering.serializer} writes it,
     *                   with this writer's header clustering types, in its first
     *                   {@code clusteringLength} bytes
     */
    @Override
    public void startRow(byte[] clustering, int clusteringLength, LivenessInfo liveness, DeletionTime rowDeletion) throws IOException
    {
        assert !rowOpen : "startRow called while a row is already open";
        this.rowClustering = clustering;
        this.rowClusteringLength = clusteringLength;
        this.rowLiveness = liveness;
        this.rowDeletion = rowDeletion;
        this.rowColumns.clear();
        this.openComplexColumn = null;
        this.complexMarkerCount = 0;
        if (rowHeaderBuffer != null)
            this.rowHeaderBuffer.clear();
        if (rowBody != null)
            this.rowBody.clear();
        this.rowOpen = true;
        if (inPlace != null)
        {
            // the flags byte is patched at endRow, once the row's columns are known
            rowStart = inPlace.getLength();
            out.writeByte(0);
            out.write(clustering, 0, clusteringLength);
            if (!liveness.isEmpty())
                header.writeTimestamp(liveness.timestamp(), out);
            if (liveness.isExpiring())
            {
                header.writeTTL(liveness.ttl(), out);
                header.writeLocalDeletionTime(liveness.localExpirationTime(), out);
            }
            if (!rowDeletion.isLive())
                header.writeDeletionTime(rowDeletion, out);
            rowCellsStart = inPlace.getLength();
            return;
        }

        // Timestamp/TTL/row-deletion are knowable NOW (the merge already resolved the row's final
        // liveness/deletion before calling startRow) — stage them immediately; the column-subset
        // marker is appended to this SAME buffer later, at endRow, once hasAllColumns is knowable.
        if (!liveness.isEmpty())
            header.writeTimestamp(liveness.timestamp(), rowHeaderBuffer);
        if (liveness.isExpiring())
        {
            header.writeTTL(liveness.ttl(), rowHeaderBuffer);
            header.writeLocalDeletionTime(liveness.localExpirationTime(), rowHeaderBuffer);
        }
        if (!rowDeletion.isLive())
            header.writeDeletionTime(rowDeletion, rowHeaderBuffer);
    }

    /** Throws, as the iterator path's serializer does, on the first row column the header does not carry. */
    private void checkRowColumnsInHeader()
    {
        Columns columns = header.columns(false);
        for (int i = 0; i < rowColumns.size(); i++)
        {
            ColumnMetadata column = rowColumns.get(i);
            if (!columns.contains(column))
                throw new AssertionError(column.toString());
        }
    }

    /** Explicit complex-column open — mirrors {@code CursorReadMerger.mergeCellGroup}'s contract:
     *  this is called at most once per complex column, before any of its cells, when the merged
     *  complex deletion is non-live, or with a live deletion for a column the row keeps with no
     *  cells.  Any other live-deletion complex column is announced only by its first
     *  {@link #addCell}, exactly like the {@code MergeSink} grammar it mirrors — see
     *  {@code ResponseSink}. */
    @Override
    public void addComplexDeletion(ColumnMetadata column, DeletionTime complexDeletion) throws IOException
    {
        openComplexColumn(column, complexDeletion);
    }

    @Override
    public void addCell(Cell<?> cell) throws IOException
    {
        ColumnMetadata column = cell.column();
        if (column.isComplex())
        {
            if (openComplexColumn == null || !sameColumn(openComplexColumn, column))
                // no addComplexDeletion preceded this column: its merged deletion is live (the
                // MergeSink grammar never announces a live complex deletion) — open it lazily
                openComplexColumn(column, DeletionTime.LIVE);
            markerCellCount[complexMarkerCount - 1]++;
        }
        else
        {
            closeOpenComplexMarker();
            openComplexColumn = null;
            rowColumns.add(column);
        }
        Cell.serializer.serialize(cell, column, body, rowLiveness, header);
    }

    /**
     * The streaming counterpart of {@link #addCell} — same complex-column bookkeeping, same
     * flags-byte/timestamp/TTL/deletion/path grammar as {@link Cell.Serializer#serialize}
     * (mirrored here, not reused, since there is no materialized {@code Cell} to ask), but the
     * VALUE itself is streamed from {@code source} instead of read off a {@code Cell} object —
     * the caller (only ever {@code ResponseSink}) has already resolved any
     * purge/tombstone-conversion decision, so {@code hasValue} here is authoritative and this
     * method never calls {@code source.hasValue()} itself.
     *
     * @param hasValue whether the emitted cell carries a value at all (the caller's own verdict,
     *                 not necessarily {@code source.hasValue()} — a purge-converted tombstone
     *                 passes false here even when {@code source} still has bytes)
     * @param source   the winner's raw value bytes, streamed only when {@code hasValue} is true
     */
    @Override
    public void addCellFromWire(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime,
                                CellPath path, boolean hasValue, CellValueSource source) throws IOException
    {
        if (column.isComplex())
        {
            if (openComplexColumn == null || !sameColumn(openComplexColumn, column))
                openComplexColumn(column, DeletionTime.LIVE);
            markerCellCount[complexMarkerCount - 1]++;
        }
        else
        {
            closeOpenComplexMarker();
            openComplexColumn = null;
            rowColumns.add(column);
        }

        // Exactly Cell.Serializer.serialize's flag/field computation,
        // replicated here because there is no materialized Cell object to ask: isDeleted/
        // isExpiring are AbstractCell's own definitions (localDeletionTime != NO_DELETION_TIME &&
        // ttl == NO_TTL / ttl != NO_TTL).
        boolean isDeleted = localDeletionTime != Cell.NO_DELETION_TIME && ttl == Cell.NO_TTL;
        boolean isExpiring = ttl != Cell.NO_TTL;
        boolean useRowTimestamp = !rowLiveness.isEmpty() && timestamp == rowLiveness.timestamp();
        boolean useRowTTL = isExpiring
                            && rowLiveness.isExpiring()
                            && ttl == rowLiveness.ttl()
                            && localDeletionTime == rowLiveness.localExpirationTime();

        int flags = 0;
        if (!hasValue)
            flags |= Cell.Serializer.HAS_EMPTY_VALUE_MASK;
        if (isDeleted)
            flags |= Cell.Serializer.IS_DELETED_MASK;
        else if (isExpiring)
            flags |= Cell.Serializer.IS_EXPIRING_MASK;
        if (useRowTimestamp)
            flags |= Cell.Serializer.USE_ROW_TIMESTAMP_MASK;
        if (useRowTTL)
            flags |= Cell.Serializer.USE_ROW_TTL_MASK;

        body.writeByte((byte) flags);
        if (!useRowTimestamp)
            header.writeTimestamp(timestamp, body);
        if ((isDeleted || isExpiring) && !useRowTTL)
            header.writeLocalDeletionTime(localDeletionTime, body);
        if (isExpiring && !useRowTTL)
            header.writeTTL(ttl, body);
        if (column.isComplex())
            column.cellPathSerializer().serialize(path, body);
        if (hasValue)
            source.streamValue(body);
    }

    private void openComplexColumn(ColumnMetadata column, DeletionTime deletion) throws IOException
    {
        closeOpenComplexMarker();
        if (complexMarkerCount == markerStart.length)
        {
            int n = complexMarkerCount + MARKER_GROWTH;
            markerStart = java.util.Arrays.copyOf(markerStart, n);
            markerEnd = java.util.Arrays.copyOf(markerEnd, n);
            markerCellCount = java.util.Arrays.copyOf(markerCellCount, n);
            markerDeletionMfda = java.util.Arrays.copyOf(markerDeletionMfda, n);
            markerDeletionLdt = java.util.Arrays.copyOf(markerDeletionLdt, n);
        }
        markerStart[complexMarkerCount] = rowBody.getLength();
        markerEnd[complexMarkerCount] = -1;
        markerCellCount[complexMarkerCount] = 0;
        markerDeletionMfda[complexMarkerCount] = deletion.markedForDeleteAt();
        markerDeletionLdt[complexMarkerCount] = deletion.localDeletionTime();
        complexMarkerCount++;
        openComplexColumn = column;
        rowColumns.add(column);
    }

    private void closeOpenComplexMarker()
    {
        if (complexMarkerCount > 0 && markerEnd[complexMarkerCount - 1] < 0)
            markerEnd[complexMarkerCount - 1] = rowBody.getLength();
    }

    private static boolean sameColumn(ColumnMetadata a, ColumnMetadata b)
    {
        // CursorReads.sameColumn's exact identity-then-name fallback (different sstables can carry
        // different ColumnMetadata instances for the same column across an ALTER — CASSANDRA-13776
        // shape); duplicated rather than shared since it lives in a different package and is a
        // one-line comparison, the same call SSTableCursorWriter itself makes inline.
        return a == b || (a != null && b != null && a.name.equals(b.name));
    }

    /** Whether the open row is empty so far, {@code Row.isEmpty()}'s definition: empty liveness,
     *  live deletion, no columns. */
    public boolean rowIsEmpty()
    {
        assert rowOpen : "rowIsEmpty called with no open row";
        return rowLiveness.isEmpty() && rowDeletion.isLive() && rowColumns.isEmpty();
    }

    /** Discards the open row: nothing staged for it reaches {@code out}. */
    @Override
    public void abandonRow()
    {
        assert rowOpen : "abandonRow called with no open row";
        rowOpen = false;
        if (inPlace != null)
            inPlace.truncate(rowStart);
    }

    /**
     * Flushes the staged row to {@code out}, or discards it entirely if it merged to nothing —
     * mirrors {@code MaterializingMergeSink.endRow}'s {@code !row.isEmpty()} guard
     * ({@code Row.isEmpty()}'s exact definition: empty liveness, live deletion, no columns).
     *
     * @return true if the row was flushed, false if it was empty and discarded (nothing written)
     */
    @Override
    public boolean endRow() throws IOException
    {
        assert rowOpen : "endRow called with no open row";
        closeOpenComplexMarker();
        rowOpen = false;

        boolean isEmpty = rowLiveness.isEmpty() && rowDeletion.isLive() && rowColumns.isEmpty();
        if (isEmpty)
        {
            if (inPlace != null)
                inPlace.truncate(rowStart);
            return false;
        }

        // HAS_COMPLEX_DELETION is row-level and decidable only now that every complex column in
        // the row has been seen: any marker with a non-live deletion sets it, and then EVERY
        // complex column in the row serializes a deletion field (live included) — the exact
        // UnfilteredSerializer.writeComplexColumn contract, SSTableCursorWriter's precedent.
        boolean hasComplexDeletion = false;
        for (int i = 0; i < complexMarkerCount; i++)
            hasComplexDeletion |= markerDeletionMfda[i] != DeletionTime.LIVE.markedForDeleteAt()
                                  || markerDeletionLdt[i] != DeletionTime.LIVE.localDeletionTime();

        boolean hasAllColumns = rowColumns.size() == header.columns(false).size();
        // UnfilteredSerializer.serializeColumnData fails on a column the header does not carry;
        // fail the same way, before the row reaches the response.  A row with fewer columns than
        // the header fails in Columns.serializer.serializeSubset below, as it does there.
        if (hasAllColumns && tableHasDroppedColumns)
            checkRowColumnsInHeader();

        int flags = 0;
        if (!rowLiveness.isEmpty())
            flags |= UnfilteredSerializer.HAS_TIMESTAMP;
        if (rowLiveness.isExpiring())
            flags |= UnfilteredSerializer.HAS_TTL;
        if (!rowDeletion.isLive())
            flags |= UnfilteredSerializer.HAS_DELETION;
        if (hasComplexDeletion)
            flags |= UnfilteredSerializer.HAS_COMPLEX_DELETION;
        if (hasAllColumns)
            flags |= UnfilteredSerializer.HAS_ALL_COLUMNS;
        // No EXTENSION_FLAG/extended-flags byte ever: rows reaching this class are never static
        // (the static row takes the separate writeStaticRow passthrough) and never carry a
        // shadowable deletion (the merge core only ever hands this class a bare DeletionTime, and
        // ResponseSink — like MaterializingMergeSink — treats every merged row
        // deletion as Row.Deletion.regular; hasExtendedFlags(row) == row.isStatic() ||
        // row.deletion().isShadowable() is therefore always false here).

        if (inPlace != null)
        {
            inPlace.patchByte(rowStart, flags);
            if (!hasAllColumns)
            {
                // the column subset goes before the cells: rare, so the cells move to make room
                if (rowHeaderBuffer == null)
                    rowHeaderBuffer = new DataOutputBuffer();
                Columns.serializer.serializeSubset(rowColumns, header.columns(false), rowHeaderBuffer);
                inPlace.insert(rowCellsStart, rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());
            }
            return true;
        }

        out.writeByte((byte) flags);
        out.write(rowClustering, 0, rowClusteringLength);

        if (!hasAllColumns)
            Columns.serializer.serializeSubset(rowColumns, header.columns(false), rowHeaderBuffer);
        out.write(rowHeaderBuffer.getData(), 0, rowHeaderBuffer.getLength());

        if (complexMarkerCount > 0)
        {
            int pos = 0;
            for (int i = 0; i < complexMarkerCount; i++)
            {
                int start = markerStart[i];
                out.write(rowBody.getData(), pos, start - pos);
                if (hasComplexDeletion)
                {
                    reusableDeletion.reset(markerDeletionMfda[i], markerDeletionLdt[i]);
                    header.writeDeletionTime(reusableDeletion, out);
                }
                out.writeUnsignedVInt32(markerCellCount[i]);
                int end = markerEnd[i];
                out.write(rowBody.getData(), start, end - start);
                pos = end;
            }
            out.write(rowBody.getData(), pos, rowBody.getLength() - pos);
        }
        else
        {
            out.write(rowBody.getData(), 0, rowBody.getLength());
        }
        return true;
    }
}
