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
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;

/**
 * Flushes a live memtable partition set into an {@link SSTableCursorWriter} without allocating a
 * copy.  It is the memtable-flush counterpart to {@code CursorCompactor}, but with one
 * already-sorted source (a memtable partition) instead of many sstables to merge.
 * <p>
 * It lives in {@code db.partitions} because it reads {@link AtomicBTreePartition#holder()} and
 * {@link BTreePartitionData}'s package-private fields directly.  Reaching those without a
 * defensive on-heap copy, which is what this path exists to avoid, needs the same package.
 */
public class MemtableCursorFlusher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MemtableCursorFlusher.class);

    /**
     * Supplies the {@link SortedTableWriter}(s) this flush writes into, and decides at each
     * partition boundary whether to roll onto the next one.  The single-output case
     * ({@code SimpleSSTableMultiWriter}) always returns null from {@link #maybeSwitchWriter}, so
     * the flush stays on one writer.  The sharded case ({@code ShardedMultiWriter}) returns that
     * multi-writer's own per-shard writers, so the cursor flush splits the output at the same
     * shard boundaries the iterator path would.
     */
    public interface OutputWriterProvider
    {
        /** The writer to begin with (the only shard, or the first). */
        SortedTableWriter<?, ?> firstWriter();

        /**
         * Advances the shard tracker to {@code key}.  Returns the next shard's writer when that
         * crosses a boundary after the current shard has data, otherwise null.  Called once per
         * partition, in token order.
         */
        SortedTableWriter<?, ?> maybeSwitchWriter(DecoratedKey key);
    }

    private final OutputWriterProvider writerProvider;
    // The cursor wrapping the shard currently being written; re-pointed at each shard rollover.
    private SSTableCursorWriter writer;
    private final TableMetadata metadata;
    private final ClusteringComparator comparator;
    private final ColumnFilter selection;

    // Reused across every row and marker in the flush.
    private final UnfilteredDescriptor descriptor;
    private final ReusableCellLivenessInfo cellLiveness = new ReusableCellLivenessInfo();
    private byte[] keyScratch = new byte[64];

    // Passed to Row.apply by applyRow(); one reused instance, this class is never shared.
    private final Consumer<ColumnData> columnDataWriter = new ColumnDataWriter();

    // Set once per row, read back by ColumnDataWriter.accept() while walking that row's ColumnData.
    private LivenessInfo currentRowLiveness;

    // Counts the rows and range tombstone markers written to the current partition; the static
    // row is not counted.
    private int unfilteredsWrittenToPartition;

    // null until the first partition is written; also serves as the "first key set yet?" guard.
    private ByteBuffer lastKeyBuffer;

    public MemtableCursorFlusher(OutputWriterProvider writerProvider, TableMetadata metadata)
    {
        this.writerProvider = writerProvider;
        this.writer = SSTableCursorWriter.forFlush(writerProvider.firstWriter());
        this.metadata = metadata;
        this.comparator = metadata.comparator;
        this.selection = ColumnFilter.all(metadata);
        this.descriptor = new UnfilteredDescriptor(metadata.comparator.subtypes().toArray(new AbstractType<?>[0]));
    }

    /**
     * Decides whether cursor flush can handle this table and memtable.  The metadata-shape gate
     * (Accord keyspace, partitioner, secondary indexes) is delegated to
     * {@code CursorCompactor.unsupportedMetadata}, since this path writes through the same
     * {@link SSTableCursorWriter}.  On top of that, only {@code SkipListMemtable} and
     * {@code ShardedSkipListMemtable} with an on-heap allocator are in scope for this pass.  An
     * unsupported combination falls back to the iterator flush path; it is never a hard failure.
     */
    public static boolean isSupported(TableMetadata metadata, Memtable memtable)
    {
        if (!(memtable instanceof SkipListMemtable) && !(memtable instanceof ShardedSkipListMemtable))
        {
            LOGGER.debug("Cursor flush is not supported for {}.{}: memtable implementation {} is not supported",
                         metadata.keyspace, metadata.name, memtable.getClass().getSimpleName());
            return false;
        }

        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
            case unslabbed_heap_buffers:
            case unslabbed_heap_buffers_logged:
            case heap_buffers:
                break;
            default:
                LOGGER.debug("Cursor flush is not supported for {}.{}: memtable allocation type {} is not supported",
                             metadata.keyspace, metadata.name, DatabaseDescriptor.getMemtableAllocationType());
                return false;
        }

        // The batchlog table needs Flushing's CASSANDRA-4667 special case.  Decline it here and
        // let the iterator path handle it, rather than duplicate that logic.
        if (metadata == SystemKeyspace.Batches)
        {
            LOGGER.debug("Cursor flush is not supported for {}.{}: the batchlog table is handled by the legacy CASSANDRA-4667 special case only",
                         metadata.keyspace, metadata.name);
            return false;
        }

        if (CursorCompactor.unsupportedMetadata(metadata))
        {
            LOGGER.debug("Cursor flush is not supported for {}.{}: rejected by CursorCompactor.unsupportedMetadata",
                         metadata.keyspace, metadata.name);
            return false;
        }

        return true;
    }

    public void flush(Memtable.FlushablePartitionSet<?> toFlush) throws IOException
    {
        try
        {
            for (Partition partition : toFlush)
            {
                if (partition.isEmpty())
                    continue;

                // isSupported() only admits memtables whose partitions are AtomicBTreePartition.
                // Check it here so a future mismatch fails with a clear message, not a bare
                // ClassCastException.
                if (!(partition instanceof AtomicBTreePartition))
                    throw new IllegalStateException("MemtableCursorFlusher requires AtomicBTreePartition partitions, got " +
                                                     partition.getClass().getName() + " - isSupported() should have " +
                                                     "excluded this memtable before flush() was ever called");

                maybeSwitchWriter(partition.partitionKey());
                writePartition((AtomicBTreePartition) partition);
            }

            if (lastKeyBuffer != null)
                writer.setLast(lastKeyBuffer);
        }
        finally
        {
            // Close the final shard's cursor, flushing its index-builder state.  The underlying
            // writer stays open; the flush transaction finishes it.  Earlier shards were already
            // closed in maybeSwitchWriter.
            writer.close();
        }
    }

    /**
     * On crossing a shard boundary, stamps the leaving shard's last key, closes its cursor, and
     * opens the next shard's writer.
     */
    private void maybeSwitchWriter(DecoratedKey key) throws IOException
    {
        SortedTableWriter<?, ?> next = writerProvider.maybeSwitchWriter(key);
        if (next == null)
            return;

        if (lastKeyBuffer != null)
            writer.setLast(lastKeyBuffer);
        writer.close();

        // Clearing lastKeyBuffer makes setFirst fire again for the new shard on the next
        // writePartition.
        writer = SSTableCursorWriter.forFlush(next);
        lastKeyBuffer = null;
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
        // which descriptor still holds.  A partition that wrote none has no trailing block, so null.
        ClusteringDescriptor lastName = unfilteredsWrittenToPartition > 0 ? descriptor : null;
        writer.writePartitionEnd(keyScratch, keyLength, partitionDeletion, headerLength, lastName);
    }

    private void writeStaticRow(BTreePartitionData data, DeletionTime partitionDeletion) throws IOException
    {
        // Match AbstractBTreePartition.staticRow's short-circuit and filtering, so the empty
        // static row is written in exactly the same cases the iterator path would.
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
            // A null row sorts after everything: once nextRow is null, the range-tombstone cursor
            // drains until it too is empty.
            ClusteringBound<?> rtPosition = rtCursor == null ? null : rtCursor.peekPosition();
            boolean rtGoesFirst = rtPosition != null && (nextRow == null || comparator.compare(rtPosition, nextRow.clustering()) < 0);

            if (rtGoesFirst)
            {
                RangeTombstoneListCursor.Result result = rtCursor.moveNext();
                if (result == RangeTombstoneListCursor.Result.SKIPPED)
                    continue; // state advanced; re-peek against the still-pending row
                writeMarker(rtCursor);
                continue;
            }

            if (nextRow == null)
                return;

            // openDeletion() is cached, so calling it per row costs nothing extra.
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

    /** Applies the row's columns, unwrapping the {@link ColumnDataWriter}'s IOException wrapper. */
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
     * A private inner class rather than having {@code MemtableCursorFlusher} implement
     * {@link Consumer} itself, so {@code Consumer<ColumnData>} does not leak onto the public API.
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
