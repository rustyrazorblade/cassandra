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

package org.apache.cassandra.db.partitions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstoneListCursor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardedSkipListMemtable;
import org.apache.cassandra.db.memtable.SkipListMemtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorWriter;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;

/**
 * Push-driven, allocation-free flush of a live memtable partition set into an
 * {@link SSTableCursorWriter}. The memtable-flush counterpart to {@link SSTableCursorWriter}'s
 * existing consumer, {@code CursorCompactor} — but with exactly one, already-sorted source
 * (a memtable partition) instead of N sstable-backed sources to merge, so none of that class's
 * merge/purge/counter-context-folding machinery applies; this is closer to a direct
 * transliteration of {@code RowAndDeletionMergeIterator} against live BTree-backed objects
 * instead of an on-disk byte stream.
 * <p>
 * Lives in {@code db.partitions} (not {@code db.memtable}, where the caller — {@code Flushing} —
 * lives) because it needs {@link AtomicBTreePartition#holder()} (protected) and
 * {@link BTreePartitionData}'s row tree/deletion-info/static-row fields (package-private):
 * reaching those without allocating a defensive on-heap copy — the thing this whole path exists
 * to avoid — requires being in the same package, not a wider public accessor surface on a
 * memtable-internal class.
 */
public class MemtableCursorFlusher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MemtableCursorFlusher.class);

    private final SSTableCursorWriter writer;
    private final TableMetadata metadata;
    private final ClusteringComparator comparator;
    private final ColumnFilter selection;

    // Reused across every row/marker in the flush; see UnfilteredDescriptor#storeRowClustering/storeMarker.
    private final UnfilteredDescriptor descriptor;
    private final ReusableCellLivenessInfo cellLiveness = new ReusableCellLivenessInfo();
    private byte[] keyScratch = new byte[64];

    // Passed to Row.apply by applyRow(); a single reused instance since MemtableCursorFlusher
    // is itself never shared across concurrent flushes.
    private final Consumer<ColumnData> columnDataWriter = new ColumnDataWriter();

    // Set once per row, read back by ColumnDataWriter.accept() while walking that row's ColumnData.
    private LivenessInfo currentRowLiveness;

    // Counts the unfiltereds (rows and range tombstone markers, never the static row) written to
    // the partition currently being written; see writePartitionEnd's lastName argument below.
    private int unfilteredsWrittenToPartition;

    // null until the first partition is written; also doubles as the "have we set the writer's
    // first key yet" guard in writePartition, since that's exactly the same condition.
    private ByteBuffer lastKeyBuffer;

    public MemtableCursorFlusher(SSTableCursorWriter writer, TableMetadata metadata)
    {
        this.writer = writer;
        this.metadata = metadata;
        this.comparator = metadata.comparator;
        this.selection = ColumnFilter.all(metadata);
        this.descriptor = new UnfilteredDescriptor(metadata.comparator.subtypes().toArray(new AbstractType<?>[0]));
    }

    /**
     * Delegates the metadata-shape gate (Accord keyspace, partitioner, secondary indexes, and
     * whatever future schema-shape checks {@code CursorCompactor.unsupportedMetadata} grows) to
     * {@code CursorCompactor} rather than duplicating it — this path writes through the same
     * {@link SSTableCursorWriter} compaction does, so anything that makes compaction's cursor path
     * bail is equally a reason for flush's to bail. No dropped-column header-drift check applies
     * here, though: that's an on-disk-sstable-header concern with no live-memtable equivalent, and
     * {@code unsupportedMetadata} itself doesn't perform it either (that lives in the
     * per-sstable-reader gate compaction runs separately, {@code unsupportedHeaderColumns}, which
     * flush has no counterpart of). On top of that, a memtable-class and allocator-type
     * restriction: {@code TrieMemtable} and the off-heap allocator modes are out of scope for this
     * pass (their {@code ensureOnHeap()} for {@code heap_buffers} is already a no-op — see
     * {@code SlabAllocator} — so there's no correctness reason they couldn't work, just
     * unexercised so far). Unsupported combinations fall back to the existing iterator-based flush
     * path silently; this is never a hard failure.
     */
    public static boolean isSupported(TableMetadata metadata, Memtable memtable)
    {
        if (!(memtable instanceof SkipListMemtable) && !(memtable instanceof ShardedSkipListMemtable))
        {
            logDebugReason(metadata, "Unsupported memtable implementation: " + memtable.getClass().getSimpleName());
            return false;
        }

        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
            case unslabbed_heap_buffers:
            case unslabbed_heap_buffers_logged:
            case heap_buffers:
                break;
            default:
                logDebugReason(metadata, "Unsupported memtable allocation type: " + DatabaseDescriptor.getMemtableAllocationType());
                return false;
        }

        // SystemKeyspace.Batches gets Flushing.FlushRunnable's own CASSANDRA-4667 special case
        // (dropping partitions whose batch was both inserted and deleted within this memtable) -
        // rather than duplicate that logic here too, just decline it and let it fall back to the
        // existing iterator-based path, which already handles it. Batchlog flushes are low-volume
        // and not the workload this path exists to speed up.
        if (metadata == SystemKeyspace.Batches)
        {
            logDebugReason(metadata, "Batchlog table, handled by the legacy CASSANDRA-4667 special case only.");
            return false;
        }

        if (CursorCompactor.unsupportedMetadata(metadata))
        {
            // CursorCompactor.unsupportedMetadata already logs its own specific reason (at debug
            // level) for most of its checks - the one exception is the Accord-keyspace check,
            // which doesn't log, but that's an easily-recognized, single-purpose keyspace rather
            // than a case an operator would need help diagnosing.
            logDebugReason(metadata, "Rejected by CursorCompactor.unsupportedMetadata (see above for the specific reason, if logged).");
            return false;
        }

        return true;
    }

    private static void logDebugReason(TableMetadata metadata, String reason)
    {
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Cursor flush for table: {} keyspace: {} is not supported. REASON: {}",
                         metadata.name, metadata.keyspace, reason);
    }

    public void flush(Memtable.FlushablePartitionSet<?> toFlush) throws IOException
    {
        for (Partition partition : toFlush)
        {
            if (partition.isEmpty())
                continue;

            // isSupported()'s SkipListMemtable/ShardedSkipListMemtable check is what guarantees
            // this holds - both back their FlushablePartitionSet with AtomicBTreePartition
            // entries exclusively. Checked explicitly (rather than left as a plain cast) so a
            // future memtable implementation that reuses one of those classes but backs its
            // partition set with something else fails here, with a message pointing at the
            // actual mismatch, instead of an unqualified ClassCastException.
            if (!(partition instanceof AtomicBTreePartition))
                throw new IllegalStateException("MemtableCursorFlusher requires AtomicBTreePartition partitions, got " +
                                                 partition.getClass().getName() + " - isSupported() should have " +
                                                 "excluded this memtable before flush() was ever called");

            writePartition((AtomicBTreePartition) partition);
        }

        if (lastKeyBuffer != null)
            writer.setLast(lastKeyBuffer);
    }

    private void writePartition(AtomicBTreePartition partition) throws IOException
    {
        BTreePartitionData data = partition.holder();
        DeletionTime partitionDeletion = data.deletionInfo.getPartitionDeletion();
        DecoratedKey key = partition.partitionKey();
        ByteBuffer keyBuffer = key.getKey();
        int keyLength = keyBuffer.remaining();
        keyScratch = ensureCapacity(keyScratch, keyLength);
        ByteBufferUtil.copyBytes(keyBuffer, keyBuffer.position(), keyScratch, 0, keyLength);

        if (lastKeyBuffer == null)
            writer.setFirst(keyBuffer);
        lastKeyBuffer = keyBuffer;

        int headerLength = writer.writePartitionStart(keyScratch, keyLength, partitionDeletion);
        unfilteredsWrittenToPartition = 0;

        writeStaticRow(data, partitionDeletion);
        writeRowsAndDeletions(data, partitionDeletion);

        // The trailing index block's last name is the clustering of the last unfiltered written,
        // which descriptor still holds; a partition that wrote none has no trailing block to cut,
        // hence null. Mirrors CursorCompactor's own lastName derivation.
        ClusteringDescriptor lastName = unfilteredsWrittenToPartition > 0 ? descriptor : null;
        writer.writePartitionEnd(key, keyScratch, keyLength, partitionDeletion, headerLength, lastName);
    }

    private void writeStaticRow(BTreePartitionData data, DeletionTime partitionDeletion) throws IOException
    {
        // Mirrors AbstractBTreePartition.staticRow(current, ColumnFilter.all(metadata), false)'s
        // exact short-circuit and filtering, since the writer's own empty-static-row fast path
        // (writeEmptyStaticRow) must be taken in precisely the same cases the legacy path would
        // have produced an empty static row for.
        if (selection.fetchedColumns().statics.isEmpty() || (data.staticRow.isEmpty() && partitionDeletion.isLive()))
        {
            writer.writeEmptyStaticRow();
            return;
        }

        Row row = data.staticRow.filter(selection, partitionDeletion, false, metadata);
        if (row == null)
        {
            writer.writeEmptyStaticRow();
            return;
        }

        writer.writeRowStart(row.primaryKeyLivenessInfo(), row.deletion().time(), row.deletion().isShadowable(), true);
        currentRowLiveness = row.primaryKeyLivenessInfo();
        applyRow(row);
        writer.writeRowEnd(null, false);
    }

    private void writeRowsAndDeletions(BTreePartitionData data, DeletionTime partitionDeletion) throws IOException
    {
        RangeTombstoneListCursor rtCursor = data.deletionInfo.hasRanges()
                                            ? new RangeTombstoneListCursor(data.deletionInfo.rangeTombstoneList(), partitionDeletion, true)
                                            : null;

        Iterator<Row> rows = BTree.iterator(data.tree);
        Row nextRow = rows.hasNext() ? rows.next() : null;

        while (true)
        {
            // No row is treated as "sorts after everything": once nextRow is null, rtGoesFirst
            // stays true for as long as the RT cursor has anything left, draining it exactly the
            // way RowAndDeletionMergeIterator's own tail (nextRow == null) case does, without a
            // separate branch here. A SKIPPED result needs no row reconsideration in the tail
            // case either way (there are none left), matching the shouldSkip retry re-entering
            // computeNextInternal with an already-exhausted row iterator.
            ClusteringBound<?> rtPosition = rtCursor == null ? null : rtCursor.peekPosition();
            boolean rtGoesFirst = rtPosition != null && (nextRow == null || comparator.compare(rtPosition, nextRow.clustering()) < 0);

            if (rtGoesFirst)
            {
                RangeTombstoneListCursor.Result result = rtCursor.moveNext();
                if (result == RangeTombstoneListCursor.Result.SKIPPED)
                    continue; // state advanced; re-peek against the still-pending row (if any)
                writeMarker(rtCursor);
                continue;
            }

            if (nextRow == null)
                return; // rtGoesFirst would have been true above if there were still work to do

            // RangeTombstoneListCursor.openDeletion() is cached internally (refreshed only at
            // range-tombstone state transitions), so calling it per row here costs nothing extra.
            DeletionTime activeDeletion = (rtCursor != null && rtCursor.hasOpen()) ? rtCursor.openDeletion() : partitionDeletion;
            Row filtered = nextRow.filter(selection, activeDeletion, false, metadata);
            nextRow = rows.hasNext() ? rows.next() : null;
            if (filtered != null)
                writeRow(filtered);
        }
    }

    private void writeRow(Row row) throws IOException
    {
        writer.writeRowStart(row.primaryKeyLivenessInfo(), row.deletion().time(), row.deletion().isShadowable(), false);
        currentRowLiveness = row.primaryKeyLivenessInfo();
        applyRow(row);
        descriptor.storeRowClustering(row.clustering());
        writer.writeRowEnd(descriptor, true);
        unfilteredsWrittenToPartition++;
    }

    /** Shared by {@link #writeRow} and {@link #writeStaticRow}: unwraps the {@link ColumnDataWriter}'s IOException wrapper once, in one place, rather than each call site re-deriving whether it needs to. */
    private void applyRow(Row row) throws IOException
    {
        try
        {
            row.apply(columnDataWriter);
        }
        catch (UncheckedIOException e)
        {
            throw e.getCause();
        }
    }

    private void writeMarker(RangeTombstoneListCursor rtCursor) throws IOException
    {
        descriptor.storeMarker(rtCursor.kind(), rtCursor.valuesSource(), rtCursor.markerCloseDeletion(), rtCursor.markerOpenDeletion());
        writer.writeRangeTombstone(descriptor, true);
        unfilteredsWrittenToPartition++;
    }

    /**
     * Kept as a private inner class rather than {@code MemtableCursorFlusher} itself implementing
     * {@link Consumer}, so {@code Consumer<ColumnData>} — an implementation detail of how
     * {@link Row#apply} is driven — never leaks onto this class's public API.
     */
    private final class ColumnDataWriter implements Consumer<ColumnData>
    {
        @Override
        public void accept(ColumnData cd)
        {
            try
            {
                if (cd.column().isComplex())
                    writeComplexColumn((ComplexColumnData) cd);
                else
                    writeCell((Cell<?>) cd);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void writeComplexColumn(ComplexColumnData ccd) throws IOException
    {
        writer.startComplexColumn(ccd.column(), ccd.complexDeletion());
        int count = ccd.cellsCount();
        for (int i = 0; i < count; i++)
            writeCell(ccd.getCellByIndex(i));
    }

    private void writeCell(Cell<?> cell) throws IOException
    {
        ColumnMetadata column = cell.column();
        boolean hasValue = cell.valueSize() > 0;
        boolean isDeleted = cell.isTombstone();

        int flags = Cell.Serializer.encodeFlags(hasValue, isDeleted, cell.isExpiring(),
                                                cell.timestamp(), cell.ttl(), cell.localDeletionTime(),
                                                currentRowLiveness);

        cellLiveness.reset(cell.timestamp(), cell.ttl(), cell.localDeletionTime());
        writer.writeCellHeader(flags, cellLiveness, column);

        if (column.isComplex())
            writer.writeCellPath(cell.path().get(0));

        if (hasValue)
            writeCellValue(cell);

        if (column.isCounterColumn() && !isDeleted)
            writer.updateCounterShardStats(counterHasLegacyShards(cell));
    }

    private <V> void writeCellValue(Cell<V> cell) throws IOException
    {
        writer.writeCellValue(cell.value(), cell.accessor(), cell.column().type);
    }

    private static <V> boolean counterHasLegacyShards(Cell<V> cell)
    {
        return CounterContext.instance().hasLegacyShards(cell.value(), cell.accessor());
    }

    private static byte[] ensureCapacity(byte[] buffer, int requiredLength)
    {
        return buffer.length < requiredLength ? new byte[Math.max(requiredLength, buffer.length * 2)] : buffer;
    }
}
