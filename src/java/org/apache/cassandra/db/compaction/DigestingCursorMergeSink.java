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
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.CursorMergeSink;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * A {@link CursorMergeSink} that computes the same per-partition digest
 * {@link UnfilteredRowIterators#digest} would produce, without ever materializing the merged
 * partition as a real {@link org.apache.cassandra.db.rows.UnfilteredRowIterator} - unlike
 * {@code MaterializingCursorMergeSink}, which this replaces for repair validation once that
 * sink's own digest-parity/allocation-comparison tests (predecessors of
 * {@code DigestingCursorMergeSinkParityTest}/{@code ValidationAllocationComparisonTest}) proved
 * its output was bit-for-bit correct.
 * <p>
 * <b>What is still built vs. avoided.</b> This still constructs the same small, already-tested,
 * already-{@code digest()}-implementing value objects {@code MaterializingCursorMergeSink} does -
 * {@link Clustering}/{@link ClusteringBound}/{@link ClusteringBoundary} (via the same
 * {@link ClusteringDescriptor#toClusteringPrefix} reuse), {@link DeletionTime}, {@link LivenessInfo},
 * {@link CellPath}, and (deliberately) {@link BufferCell} per cell - reusing {@code BufferCell}'s
 * already-correct {@code isCounterCell()}/tombstone detection here is far safer than re-deriving
 * that logic by hand from raw liveness fields, and a single non-BTree cell object is a minor cost
 * next to what is actually avoided: {@link org.apache.cassandra.db.rows.Row.Builder}/
 * {@link BTreeRow}/BTree construction (sorting + tree-node allocation per row),
 * {@link org.apache.cassandra.db.rows.ComplexColumnData} objects (replaced by the two-line inline
 * sequence its own {@code digest()} already documents: deletion-if-not-live, then each cell), the
 * whole-partition {@code List<Unfiltered>} plus {@code AbstractUnfilteredRowIterator} wrapper, and
 * the subsequent full re-walk {@code UnfilteredRowIterators.digest()} would otherwise perform over
 * that materialized structure - the digest is fed once, directly, during the merge itself.
 * <p>
 * <b>Field order.</b> Must match {@link UnfilteredRowIterators#digest} exactly: partition key,
 * partition deletion, regular column names, static column names (only if a static row is actually
 * present - matching the {@code CASSANDRA-12090} reference-equality check against
 * {@code Rows.EMPTY_STATIC_ROW}), the (always-{@code false}) reverse-order flag, then the static
 * row's own digest, then each {@link Unfiltered} in order. A row's own digest
 * ({@link org.apache.cassandra.db.rows.AbstractRow#digest}) is kind byte, clustering, row deletion,
 * primary-key liveness, then each column in order; note row deletion here means
 * {@link org.apache.cassandra.db.rows.Row.Deletion#digest} - {@code DeletionTime.digest()} followed
 * by an {@code isShadowable} boolean, taken from whatever {@link CursorCompactor} merged for the
 * row (in practice always {@code false}: a shadowable deletion is MV-only data, and materialized
 * views are excluded from cursor validation entirely) - inlined directly rather than allocating a
 * {@code Row.Deletion} per row. A marker's digest
 * ({@link RangeTombstoneBoundMarker#digest}/{@link RangeTombstoneBoundaryMarker#digest}) has no
 * leading kind byte, unlike a row - bound, then deletion(s), only.
 * <p>
 * <b>Header timing.</b> Because a row's clustering is only known at {@code writeRowEnd} (the same
 * reason {@code MaterializingCursorMergeSink} defers {@code Row.Builder.newRow()}), and because
 * whether the static row was actually present (vs. the {@code Rows.EMPTY_STATIC_ROW} sentinel case)
 * can only be known once the static row question is resolved, the partition header (key, deletion,
 * column names, static-columns-if-present, reverse-order flag) AND the static row's own digest are
 * flushed together, exactly once, as the last step of {@link #ensureHeaderFlushed}: with the real
 * static row's stashed fields if one was written, or with the sentinel's fixed fields
 * ({@code Clustering.STATIC_CLUSTERING}, {@code LivenessInfo.EMPTY}, {@code DeletionTime.LIVE}, no
 * columns) otherwise - the static row's digest runs unconditionally either way, exactly like
 * {@code iterator.staticRow().digest(digest)} in {@link UnfilteredRowIterators#digest}, which does
 * not skip it even for a table with no static columns at all. {@code ensureHeaderFlushed} is called
 * from {@link #writeEmptyStaticRow}, from the static row's own {@code writeRowEnd}, and
 * (idempotently, defensively) from the first non-static row/marker or from
 * {@code writePartitionEnd} for a partition with no static row at all.
 * <p>
 * Not thread-safe; one instance drives one {@link CursorCompactor#mergeNextPartition} loop.
 */
public class DigestingCursorMergeSink implements CursorMergeSink
{
    private final TableMetadata metadata;
    private final List<AbstractType<?>> clusteringTypesList;
    private final RegularAndStaticColumns columns;

    // Partition being accumulated
    private DecoratedKey partitionKey;
    private DeletionTime partitionLevelDeletion = DeletionTime.LIVE;
    private Digest digest;
    private boolean headerFlushed;
    private boolean hasStaticRow;
    private LivenessInfo staticLiveness;
    private DeletionTime staticDeletion;
    private boolean staticShadowable;
    private List<ColumnDigestOp> staticColumnOps;
    private PrecomputedDigestPartition completedPartition;

    // Row being accumulated (between writeRowStart and writeRowEnd) - see
    // MaterializingCursorMergeSink for why this must be staged rather than digested inline.
    private LivenessInfo pendingRowLiveness;
    private DeletionTime pendingRowDeletion;
    private boolean pendingRowShadowable;
    private boolean currentRowIsStatic;
    private final List<ColumnDigestOp> pendingColumnOps = new ArrayList<>();
    private ComplexColumnDigestOp currentComplexOp;

    // A single column's contribution to a row's digest, in row-column order. A simple column is
    // added directly as its cell's own digest method reference; a complex column carries its own
    // deletion-then-cells sequence (see ComplexColumnDigestOp).
    private interface ColumnDigestOp
    {
        void digest(Digest digest);
    }

    private static final class ComplexColumnDigestOp implements ColumnDigestOp
    {
        private final DeletionTime complexDeletion;
        private final List<Cell<?>> cells = new ArrayList<>();

        ComplexColumnDigestOp(DeletionTime complexDeletion) { this.complexDeletion = complexDeletion; }

        public void digest(Digest digest)
        {
            // Matches ComplexColumnData.digest() exactly: deletion first (only if not live), then
            // each cell in order - deliberately not materializing a ComplexColumnData to call its
            // digest() unchanged, since the sequence is this simple to replicate directly.
            if (!complexDeletion.isLive())
                complexDeletion.digest(digest);
            for (Cell<?> cell : cells)
                cell.digest(digest);
        }
    }

    // Pending cell (between writeCellHeader and the next writeCellHeader/writeRowEnd/writeRangeTombstone).
    // The cell's (timestamp, ttl, localDeletionTime) triple is captured as raw primitives rather than
    // snapshotted through LivenessInfo: a counter tombstone carries ttl==NO_TTL with a set
    // localDeletionTime, which LivenessInfo.withExpirationTime would silently drop (it returns a plain
    // ImmutableLivenessInfo when ttl==NO_TTL), flipping the reconstructed cell's isTombstone()/
    // isCounterCell() and diverging the digest from the legacy path.
    private ColumnMetadata pendingCellColumn;
    private long pendingCellTimestamp;
    private int pendingCellTtl;
    private long pendingCellLocalDeletionTime;
    private CellPath pendingCellPath;
    private ByteBuffer pendingCellValue;
    private boolean hasPendingCell;

    // Scratch buffer for copyCellValue - cleared and reused per cell, never retained past the
    // point its contents are copied into an immutable ByteBuffer.
    private final DataOutputBuffer cellValueScratch = new DataOutputBuffer(128);

    public DigestingCursorMergeSink(TableMetadata metadata)
    {
        this.metadata = metadata;
        this.clusteringTypesList = Arrays.asList(metadata.comparator.subtypes().toArray(new AbstractType<?>[0]));
        this.columns = metadata.regularAndStaticColumns();
    }

    /**
     * Returns the most recently completed partition's precomputed digest (produced by the
     * {@code writePartitionEnd} call after {@link CursorCompactor#mergeNextPartition} last
     * returned {@code true}), and clears it - each partition can only be taken once.
     */
    public PrecomputedDigestPartition takePartitionDigest()
    {
        PrecomputedDigestPartition result = completedPartition;
        completedPartition = null;
        return result;
    }

    private static LivenessInfo snapshotLiveness(LivenessInfo info)
    {
        if (info.isEmpty())
            return LivenessInfo.EMPTY;
        return info.isExpiring()
             ? LivenessInfo.withExpirationTime(info.timestamp(), info.ttl(), info.localExpirationTime())
             : LivenessInfo.create(info.timestamp());
    }

    private static DeletionTime snapshotDeletion(DeletionTime deletionTime)
    {
        return deletionTime.isLive() ? DeletionTime.LIVE : DeletionTime.build(deletionTime.markedForDeleteAt(), deletionTime.localDeletionTime());
    }

    private void finalizePendingCell()
    {
        if (!hasPendingCell)
            return;
        ByteBuffer value = pendingCellValue != null ? pendingCellValue : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        Cell<?> cell = new BufferCell(pendingCellColumn,
                                      pendingCellTimestamp,
                                      pendingCellTtl,
                                      pendingCellLocalDeletionTime,
                                      value,
                                      pendingCellPath);
        if (pendingCellColumn.isComplex())
        {
            // startComplexColumn always precedes a complex column's first cell (eagerly if its
            // deletion survives, lazily just before the first surviving cell otherwise), so
            // currentComplexOp is guaranteed set here.
            currentComplexOp.cells.add(cell);
        }
        else
        {
            pendingColumnOps.add(cell::digest);
            currentComplexOp = null;
        }
        pendingCellColumn = null;
        pendingCellPath = null;
        pendingCellValue = null;
        hasPendingCell = false;
    }

    /**
     * {@link CursorCompactor} only uses this (and {@link #getPartitionStart}) to compute a
     * byte-accurate partition-header length for the real sstable writer's on-disk bookkeeping -
     * meaningless for a sink that never writes bytes, and no longer load-bearing for this class:
     * {@code CursorCompactor} tracks whether a partition has been started with its own field,
     * independent of these values (see {@code CursorCompactor.partitionStarted}). Always 0.
     */
    @Override
    public long getPosition()
    {
        return 0;
    }

    @Override
    public long getPartitionStart()
    {
        return 0;
    }

    @Override
    public int writePartitionStart(byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime) throws IOException
    {
        this.partitionKey = metadata.partitioner.decorateKey(ByteBuffer.wrap(Arrays.copyOf(partitionKey, partitionKeyLength)));
        this.partitionLevelDeletion = snapshotDeletion(partitionDeletionTime);
        this.digest = Digest.forValidator();
        this.headerFlushed = false;
        this.hasStaticRow = false;
        this.completedPartition = null;
        return 0;
    }

    /**
     * Emits, exactly once per partition, the full sequence {@code UnfilteredRowIterators.digest()}
     * writes before its row loop: partition key, partition deletion, regular column names, static
     * column names (only if {@code hasStaticRow}), the reverse-order flag (always {@code false}),
     * and - unconditionally, whether or not a static row was actually present, matching
     * {@code iterator.staticRow().digest(digest)}'s unconditional call - the static row's own
     * digest (either the real one, using the fields stashed by {@code writeRowEnd}, or the
     * {@code Rows.EMPTY_STATIC_ROW} sentinel's fixed digest sequence). Idempotent - safe to call
     * defensively from multiple call sites (see class doc "Header timing").
     */
    private void ensureHeaderFlushed()
    {
        if (headerFlushed)
            return;
        headerFlushed = true;
        digest.update(partitionKey.getKey());
        partitionLevelDeletion.digest(digest);
        columns.regulars.digest(digest);
        if (hasStaticRow)
            columns.statics.digest(digest);
        digest.updateWithBoolean(false);
        if (hasStaticRow)
            digestRow(Clustering.STATIC_CLUSTERING, staticLiveness, staticDeletion, staticShadowable, staticColumnOps);
        else
            digestRow(Clustering.STATIC_CLUSTERING, LivenessInfo.EMPTY, DeletionTime.LIVE, false, List.of());
    }

    /**
     * Digests one {@code Unfiltered.Kind.ROW} - matches {@code AbstractRow.digest()}: kind byte,
     * clustering, row deletion ({@code DeletionTime.digest()} then the {@code isShadowable} bit,
     * matching {@code Row.Deletion.digest()}), primary-key liveness, then each column. {@code
     * isShadowable} is whatever {@link CursorCompactor} merged for this row - in practice always
     * {@code false} here, since a shadowable deletion is only ever found on materialized-view data
     * (CASSANDRA-11500) and materialized views are excluded from cursor validation entirely
     * ({@code CursorCompactor.isValidationSupported}) - but threaded through rather than assumed,
     * so this stays correct if that exclusion ever narrows.
     */
    private void digestRow(Clustering<?> clustering, LivenessInfo liveness, DeletionTime rowDeletion, boolean isShadowable, List<ColumnDigestOp> columnOps)
    {
        digest.updateWithByte(Unfiltered.Kind.ROW.ordinal());
        clustering.digest(digest);
        rowDeletion.digest(digest);
        digest.updateWithBoolean(isShadowable); // Row.Deletion.isShadowable
        liveness.digest(digest);
        for (ColumnDigestOp op : columnOps)
            op.digest(digest);
    }

    @Override
    public void writePartitionEnd(DecoratedKey decoratedKey, byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime, int headerLength, ClusteringDescriptor lastName) throws IOException
    {
        ensureHeaderFlushed();
        completedPartition = new PrecomputedDigestPartition(metadata, this.partitionKey, columns, digest.digest(), digest.inputBytes());
    }

    @Override
    public boolean writeEmptyStaticRow() throws IOException
    {
        if (!columns.statics.isEmpty())
        {
            // No static row was written (hasStaticRow stays false) - matches
            // MaterializingCursorMergeSink leaving `staticRow` as the Rows.EMPTY_STATIC_ROW
            // sentinel here, which UnfilteredRowIterators.digest()'s reference-equality check
            // (CASSANDRA-12090) treats as "no static row" for the conditional statics-columns
            // digest. ensureHeaderFlushed() still digests the sentinel's own fixed content.
            ensureHeaderFlushed();
            return true;
        }
        return false;
    }

    @Override
    public void writeRowStart(LivenessInfo livenessInfo, DeletionTime deletionTime, boolean isShadowable, boolean isStatic) throws IOException
    {
        finalizePendingCell();
        currentRowIsStatic = isStatic;
        pendingRowLiveness = snapshotLiveness(livenessInfo);
        pendingRowDeletion = snapshotDeletion(deletionTime);
        pendingRowShadowable = isShadowable;
        pendingColumnOps.clear();
        currentComplexOp = null;
    }

    @Override
    public void startComplexColumn(ColumnMetadata column, DeletionTime mergedDeletion) throws IOException
    {
        finalizePendingCell();
        currentComplexOp = new ComplexColumnDigestOp(snapshotDeletion(mergedDeletion));
        pendingColumnOps.add(currentComplexOp);
    }

    @Override
    public void writeCellPath(byte[] pathBuffer, int pathLength) throws IOException
    {
        pendingCellPath = CellPath.create(ByteBuffer.wrap(Arrays.copyOf(pathBuffer, pathLength)));
    }

    @Override
    public void writeCellHeader(int cellFlags, CellLivenessInfo cellLiveness, ColumnMetadata cellColumn) throws IOException
    {
        finalizePendingCell();
        pendingCellColumn = cellColumn;
        pendingCellTimestamp = cellLiveness.timestamp();
        pendingCellTtl = cellLiveness.ttl();
        pendingCellLocalDeletionTime = cellLiveness.localDeletionTime();
        pendingCellPath = null;
        pendingCellValue = null;
        hasPendingCell = true;
    }

    @Override
    public int writeCellValue(SSTableCursorReader cursor, byte[] copyColumnValueBuffer) throws IOException
    {
        cellValueScratch.clear();
        int written = cursor.copyCellValue(cellValueScratch, copyColumnValueBuffer);
        byte[] data = cellValueScratch.getData();
        int length = cellValueScratch.getLength();
        if (pendingCellColumn.type.valueLengthIfFixed() >= 0)
        {
            pendingCellValue = ByteBuffer.wrap(Arrays.copyOf(data, length));
        }
        else
        {
            // Variable-length type: copyCellContents wrote [unsigned-vint length][value bytes] -
            // strip the vint prefix, same as MaterializingCursorMergeSink. The length==0 guard
            // mirrors the DataOutputBuffer overload below (this path is only reached when the cell
            // flags claim a value, so the vint prefix is always present, but keep the two overloads
            // symmetric to avoid a future copy-paste regression).
            int prefixLength = length == 0 ? 0 : VIntCoding.computeUnsignedVIntSize(ByteBuffer.wrap(data, 0, length), 0);
            pendingCellValue = ByteBuffer.wrap(Arrays.copyOfRange(data, prefixLength, length));
        }
        return written;
    }

    @Override
    public void writeCellValue(DataOutputBuffer tempCellBuffer) throws IOException
    {
        byte[] data = tempCellBuffer.getData();
        int length = tempCellBuffer.getLength();
        if (pendingCellColumn.type.valueLengthIfFixed() >= 0)
        {
            pendingCellValue = ByteBuffer.wrap(Arrays.copyOf(data, length));
        }
        else
        {
            int prefixLength = length == 0 ? 0 : VIntCoding.computeUnsignedVIntSize(ByteBuffer.wrap(data, 0, length), 0);
            pendingCellValue = ByteBuffer.wrap(Arrays.copyOfRange(data, prefixLength, length));
        }
    }

    @Override
    public void writeCellValue(byte[] value, int offset, int length) throws IOException
    {
        pendingCellValue = ByteBuffer.wrap(Arrays.copyOfRange(value, offset, offset + length));
    }

    @Override
    public void updateCounterShardStats(boolean hasLegacyShards)
    {
        // sstable metadata-collector bookkeeping only - nothing to digest.
    }

    @Override
    public void writeRowEnd(UnfilteredDescriptor rHeader, boolean updateClusteringMetadata) throws IOException
    {
        finalizePendingCell();

        if (currentRowIsStatic)
        {
            hasStaticRow = true;
            staticLiveness = pendingRowLiveness;
            staticDeletion = pendingRowDeletion;
            staticShadowable = pendingRowShadowable;
            staticColumnOps = new ArrayList<>(pendingColumnOps);
            ensureHeaderFlushed();
        }
        else
        {
            ensureHeaderFlushed();
            Clustering<?> clustering = (Clustering<?>) rHeader.toClusteringPrefix(clusteringTypesList);
            digestRow(clustering, pendingRowLiveness, pendingRowDeletion, pendingRowShadowable, pendingColumnOps);
        }
        pendingColumnOps.clear();
    }

    @Override
    public void writeRangeTombstone(UnfilteredDescriptor rangeTombstone, boolean updateClusteringMetadata) throws IOException
    {
        ensureHeaderFlushed();
        ClusteringPrefix<?> bound = rangeTombstone.toClusteringPrefix(clusteringTypesList);
        // Matches RangeTombstoneBoundMarker.digest()/RangeTombstoneBoundaryMarker.digest() exactly:
        // bound, then deletion(s) - no leading Unfiltered.Kind byte (unlike a row's digest).
        bound.digest(digest);
        if (rangeTombstone.isBoundary())
        {
            snapshotDeletion(rangeTombstone.deletionTime()).digest(digest);
            snapshotDeletion(rangeTombstone.deletionTime2()).digest(digest);
        }
        else
        {
            snapshotDeletion(rangeTombstone.deletionTime()).digest(digest);
        }
    }

    @Override
    public void updateClusteringMetadata(ClusteringDescriptor clusteringDescriptor)
    {
        // sstable min/max-clustering metadata bookkeeping only - nothing to digest.
    }

    @Override
    public void setLast(ByteBuffer key)
    {
        // sstable writer first/last-key bookkeeping only - nothing to digest.
    }

    @Override
    public void setFirst(ByteBuffer key)
    {
        // sstable writer first/last-key bookkeeping only - nothing to digest.
    }
}
