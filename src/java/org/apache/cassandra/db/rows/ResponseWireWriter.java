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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.SerializationHeader.MessagingHeaderSerializer.MESSAGING;

/**
 * M3.3a-i (CASSANDRA-20428, Phase 4 seam iv, wire-format-only slice): a read-owned, MESSAGING-flavor
 * wire writer for {@code ReadResponse} intra-node bytes — the transcode sink's low-level primitive,
 * co-located with {@link UnfilteredSerializer}/{@link UnfilteredRowIteratorSerializer} because it
 * reproduces their EXACT byte grammar (verified against both, see their javadocs) rather than
 * inventing a new one. Two kinds of API here, mirroring the two problems the M3.3a plan identifies:
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
 *       arrive. Not needed for correctness in M3.3a-i (there is no mid-row abandonment path on this
 *       slice — {@code RowFilter} composition with the transcode sink is explicitly out of scope,
 *       M3.3a's own risk note #2), but it means a future increment that DOES need to abandon a
 *       partially-built row (the way {@code MaterializingMergeSink.abandonRow()}'s build-and-reset
 *       can) only has to stop calling {@link #endRow} — no rework of this class's staging shape.</li>
 * </ul>
 * Purge-free by design, exactly like the merge core it sits below (FINDING #12's "no purge
 * machinery" property): every event this class receives is assumed ALREADY PURGED by the caller
 * ({@code CursorReads.TranscodeMergeSink}'s gcable-purge twin) — this class only ever encodes what
 * it is handed.
 */
public final class ResponseWireWriter
{
    private final DataOutputPlus out;
    private final SerializationHeader header;
    private final SerializationHelper helper;
    private final int version;

    // ---- row-local staging (reset per row by startRow; flushed to `out` only by endRow) ----
    private final DataOutputBuffer rowHeaderBuffer = new DataOutputBuffer();
    private final DataOutputBuffer rowBody = new DataOutputBuffer();
    private Clustering<?> rowClustering;
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
    private int complexMarkerCount;
    private int[] markerStart = new int[MARKER_GROWTH];
    private int[] markerEnd = new int[MARKER_GROWTH];
    private int[] markerCellCount = new int[MARKER_GROWTH];
    private long[] markerDeletionMfda = new long[MARKER_GROWTH];
    private long[] markerDeletionLdt = new long[MARKER_GROWTH];
    private final DeletionTime.ReusableDeletionTime reusableDeletion = DeletionTime.ReusableDeletionTime.live();

    public ResponseWireWriter(DataOutputPlus out, SerializationHeader header, int version)
    {
        assert !header.isForSSTable() : "ResponseWireWriter is the MESSAGING flavor only";
        this.out = out;
        this.header = header;
        this.helper = new SerializationHelper(header);
        this.version = version;
    }

    // ---------------------------------------------------------------- partition-level passthrough
    // Mirrors UnfilteredRowIteratorSerializer.serializeWithoutKey's grammar
    // (<flags><s_header>[<partition_deletion>][<static_row>]…<EOP>) exactly, sharing its flag bit
    // values (package-private in that class for this reuse) rather than redefining them.

    public void writeKey(ByteBuffer key) throws IOException
    {
        ByteBufferUtil.writeWithVIntLength(key, out);
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
        MESSAGING.serialize(out, header, hasStaticRow, selection);
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

    /** M2.3 whole-row escape hatch passthrough: the merged row IS this already-live object (see
     *  {@code CursorReadMerger.MergeSink#addRow}) — nothing to transcode. */
    public void writeRow(Row row) throws IOException
    {
        UnfilteredSerializer.serializer.serialize(row, helper, out, version);
    }

    /** Range tombstone marker passthrough: {@code CursorReadMerger.mergeMarkerGroup} always builds
     *  a real {@code RangeTombstoneBoundMarker}/{@code RangeTombstoneBoundaryMarker} before calling
     *  the sink — nothing to transcode. */
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

    public void startRow(Clustering<?> clustering, LivenessInfo liveness, DeletionTime rowDeletion) throws IOException
    {
        assert !rowOpen : "startRow called while a row is already open";
        this.rowClustering = clustering;
        this.rowLiveness = liveness;
        this.rowDeletion = rowDeletion;
        this.rowColumns.clear();
        this.openComplexColumn = null;
        this.complexMarkerCount = 0;
        this.rowHeaderBuffer.clear();
        this.rowBody.clear();
        this.rowOpen = true;

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

    /** Explicit complex-column open with a KNOWN NON-LIVE merged deletion — mirrors
     *  {@code CursorReadMerger.mergeCellGroup}'s contract: this is called at most once per complex
     *  column, before any of its cells, and ONLY when the merged complex deletion is non-live (a
     *  live-deletion complex column is announced only by its first {@link #addCell}, exactly like
     *  the {@code MergeSink} grammar it mirrors — see {@code CursorReads.TranscodeMergeSink}). */
    public void addComplexDeletion(ColumnMetadata column, DeletionTime complexDeletion) throws IOException
    {
        openComplexColumn(column, complexDeletion);
    }

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
        Cell.serializer.serialize(cell, column, rowBody, rowLiveness, header);
    }

    /**
     * M3.3a-ii: the streaming twin of {@link #addCell} — same complex-column bookkeeping, same
     * flags-byte/timestamp/TTL/deletion/path grammar as {@link Cell.Serializer#serialize}
     * (mirrored here, not reused, since there is no materialized {@code Cell} to ask), but the
     * VALUE itself is streamed from {@code source} instead of read off a {@code Cell} object —
     * the caller (only ever {@code CursorReads.TranscodeMergeSink}) has already resolved any
     * purge/tombstone-conversion decision, so {@code hasValue} here is authoritative and this
     * method never calls {@code source.hasValue()} itself.
     *
     * @param hasValue whether the emitted cell carries a value at all (the caller's own verdict,
     *                 not necessarily {@code source.hasValue()} — a purge-converted tombstone
     *                 passes false here even when {@code source} still has bytes)
     * @param source   the winner's raw value bytes, streamed only when {@code hasValue} is true
     */
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

        // Exactly Cell.Serializer.serialize's flag/field computation (Cell.java:342-376),
        // replicated here because there is no materialized Cell object to ask: isDeleted/
        // isExpiring are AbstractCell's own definitions (localDeletionTime != NO_DELETION_TIME &&
        // ttl == NO_TTL / ttl != NO_TTL), verified against AbstractCell.java directly.
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

        rowBody.writeByte((byte) flags);
        if (!useRowTimestamp)
            header.writeTimestamp(timestamp, rowBody);
        if ((isDeleted || isExpiring) && !useRowTTL)
            header.writeLocalDeletionTime(localDeletionTime, rowBody);
        if (isExpiring && !useRowTTL)
            header.writeTTL(ttl, rowBody);
        if (column.isComplex())
            column.cellPathSerializer().serialize(path, rowBody);
        if (hasValue)
            source.streamValue(rowBody);
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

    /**
     * Flushes the staged row to {@code out}, or discards it entirely if it merged to nothing —
     * mirrors {@code MaterializingMergeSink.endRow}'s {@code !row.isEmpty()} guard
     * ({@code Row.isEmpty()}'s exact definition: empty liveness, live deletion, no columns).
     *
     * @return true if the row was flushed, false if it was empty and discarded (nothing written)
     */
    public boolean endRow() throws IOException
    {
        assert rowOpen : "endRow called with no open row";
        closeOpenComplexMarker();
        rowOpen = false;

        boolean isEmpty = rowLiveness.isEmpty() && rowDeletion.isLive() && rowColumns.isEmpty();
        if (isEmpty)
            return false;

        // HAS_COMPLEX_DELETION is row-level and decidable only now that every complex column in
        // the row has been seen: any marker with a non-live deletion sets it, and then EVERY
        // complex column in the row serializes a deletion field (live included) — the exact
        // UnfilteredSerializer.writeComplexColumn contract, SSTableCursorWriter's precedent.
        boolean hasComplexDeletion = false;
        for (int i = 0; i < complexMarkerCount; i++)
            hasComplexDeletion |= markerDeletionMfda[i] != DeletionTime.LIVE.markedForDeleteAt()
                                  || markerDeletionLdt[i] != DeletionTime.LIVE.localDeletionTime();

        boolean hasAllColumns = rowColumns.size() == header.columns(false).size();

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
        // CursorReads.TranscodeMergeSink — like MaterializingMergeSink — treats every merged row
        // deletion as Row.Deletion.regular; hasExtendedFlags(row) == row.isStatic() ||
        // row.deletion().isShadowable() is therefore always false here).

        out.writeByte((byte) flags);
        Clustering.serializer.serialize(rowClustering, out, version, header.clusteringTypes());

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
