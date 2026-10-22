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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.IteratorMergeOrder;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.MemtablePartitionData;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.ResponseWireWriter;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiCursorRangeSupport;
import org.apache.cassandra.io.sstable.format.bti.BtiCursorSeekSupport;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;
import static org.apache.cassandra.dht.AbstractBounds.maxLeft;
import static org.apache.cassandra.dht.AbstractBounds.minRight;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Range reads (partition-range and token-range reads) on the cursor path.
 *
 * <p>Each sstable has one source for the whole read: one {@link SSTableCursorReader} and one
 * {@link CursorReads.PendingLeg} that is opened again at each partition.  The partitions come in
 * order from the partition index (BTI) or the index file (BIG), with no lookup of a key, so no bloom
 * filter is probed and the key cache is not touched.  The cursor reads each partition header once,
 * where it reads the partition anyway, and the key comes from that header.  A partition the read
 * leaves early (a limit, {@code DISTINCT}, the end of a slice) is skipped with one seek to where the
 * next one starts.  The cursor starts on the chunk cache and moves to the read-ahead reader once it
 * reads a long run of data, so a short read pays no read-ahead fill and a scan makes large reads.
 *
 * <p>The sources merge in token order on a heap.  The sources on a key give its partition: one
 * sstable leg is read as stored, several legs and memtables merge in the cursor merge
 * ({@link CursorReads#mergeLegs}, {@link CursorReads#mergeLegsReversed}).
 *
 * <p>A replica writes its range response from the merge straight to the response bytes
 * ({@link #responseBytes}), so no row objects are built for sstable data.
 *
 * <p>The iterator range path is not changed: this class copies the rules it needs from it, the
 * sstable bounds of {@code SSTableScanner.makeBounds} and the read notifications of the scanners.
 */
public final class CursorRangeReads
{
    private CursorRangeReads()
    {
    }

    // ---------------------------------------------------------------- gate

    /**
     * Why a range read takes the iterator path while {@code cursor_range_reads_enabled} is on.  Each
     * read the gate leaves to the iterator path counts once, under its first reason.
     */
    public enum Unsupported
    {
        /** An Accord system table. */
        ACCORD_KEYSPACE,
        /** The partitioner gives no reusable keys (LocalPartitioner, ByteOrderedPartitioner, RandomPartitioner). */
        PARTITIONER_WITHOUT_REUSABLE_KEYS,
        /** A materialized view. */
        MATERIALIZED_VIEW,
        /** An sstable is not at the latest format version. */
        SSTABLE_NOT_LATEST_VERSION,
        /** A dropped collection column that was not added back is still in an sstable header (CASSANDRA-21607). */
        DROPPED_COLUMN_IN_SSTABLE_HEADER,
        /** A memtable does not give the content of its partitions ({@code Memtable.readsPartitionData}); none in the tree does not. */
        MEMTABLE_WITHOUT_PARTITION_DATA
    }

    private static final LongAdder SERVED = new LongAdder();
    private static final LongAdder RESPONSES_WRITTEN = new LongAdder();
    private static final LongAdder[] NOT_SERVED = new LongAdder[Unsupported.values().length];
    static
    {
        for (int i = 0; i < NOT_SERVED.length; i++)
            NOT_SERVED[i] = new LongAdder();
    }

    /**
     * Test only: told of each range read the gate counts, with its table and the reason it is left
     * to the iterator path, null when the cursor path serves it.  The counts above are of every
     * table, so a test that reads one table while other tables are read uses this.
     */
    @VisibleForTesting
    public static volatile BiConsumer<TableId, Unsupported> gateListener;

    private static void countServed(TableMetadata metadata)
    {
        SERVED.increment();
        BiConsumer<TableId, Unsupported> listener = gateListener;
        if (listener != null)
            listener.accept(metadata.id, null);
    }

    private static void countNotServed(TableMetadata metadata, Unsupported reason)
    {
        NOT_SERVED[reason.ordinal()].increment();
        BiConsumer<TableId, Unsupported> listener = gateListener;
        if (listener != null)
            listener.accept(metadata.id, reason);
    }

    private static final LongAdder ITERATOR_LEGS_OPENED = new LongAdder();

    /**
     * Partitions range reads took as objects from an iterator instead of reading them through a
     * merge leg: a memtable partition through the memtable's row iterator, the merged repaired
     * sstables of a read that tracks repaired data, a partition from the row cache.
     */
    public static long iteratorLegsOpened()
    {
        return ITERATOR_LEGS_OPENED.sum();
    }

    /** Range reads the cursor path served. */
    public static long rangeReadsServed()
    {
        return SERVED.sum();
    }

    /** Replica responses of range reads the cursor path wrote straight to the response bytes. */
    public static long rangeResponsesWritten()
    {
        return RESPONSES_WRITTEN.sum();
    }

    /** Range reads the gate left to the iterator path for {@code reason}. */
    public static long rangeReadsNotServed(Unsupported reason)
    {
        return NOT_SERVED[reason.ordinal()].sum();
    }

    /** Range reads the gate left to the iterator path, for any reason. */
    public static long rangeReadsNotServed()
    {
        long total = 0;
        for (LongAdder count : NOT_SERVED)
            total += count.sum();
        return total;
    }

    /**
     * Why the cursor path cannot serve {@code command}, or null when it can.  Does not count.
     *
     * @param view the memtables and sstables selected for the command's key range
     */
    public static Unsupported unsupported(PartitionRangeReadCommand command, ColumnFamilyStore cfs,
                                          ReadExecutionController controller, ColumnFamilyStore.ViewFragment view)
    {
        TableMetadata metadata = cfs.metadata();
        Collection<SSTableReader> sstables = view.sstables;
        if (!metadata.partitioner.supportsReusableKeys())
            return Unsupported.PARTITIONER_WITHOUT_REUSABLE_KEYS;
        if (metadata.isView())
            return Unsupported.MATERIALIZED_VIEW;
        for (SSTableReader sstable : sstables)
        {
            if (!sstable.descriptor.version.isLatestVersion())
                return Unsupported.SSTABLE_NOT_LATEST_VERSION;
            if (CursorCompactor.unsupportedHeaderColumns(metadata, sstable, false))
                return Unsupported.DROPPED_COLUMN_IN_SSTABLE_HEADER;
        }
        if (!CursorReads.memtablesGivePartitionData(view.memtables))
            return Unsupported.MEMTABLE_WITHOUT_PARTITION_DATA;
        return null;
    }

    /** {@link #unsupported}, counted: a read the cursor path serves counts as served, any other under its reason. */
    static boolean servedByCursor(PartitionRangeReadCommand command, ColumnFamilyStore cfs,
                                  ReadExecutionController controller, ColumnFamilyStore.ViewFragment view)
    {
        Unsupported reason = unsupported(command, cfs, controller, view);
        if (reason == null)
        {
            countServed(cfs.metadata());
            return true;
        }
        countNotServed(cfs.metadata(), reason);
        return false;
    }

    // ---------------------------------------------------------------- reads

    /**
     * The merged, not yet purged partitions of a range read, as {@code UnfilteredPartitionIterators.mergeLazily}
     * gives them on the iterator path.  Nothing is read until the first {@code hasNext}.
     *
     * @param sstables          the sstables the iterator path would scan, in its order
     * @param memtables         each memtable's partitions over the range ({@code Memtable.partitionData}),
     *                          in the iterator path's order
     * @param repaired          the merged repaired sstables of a read that tracks repaired data, with
     *                          the repaired data digest, read after every other source as the iterator
     *                          path reads it; null for none.  The returned iterator owns it.
     * @param listener          notified once per sstable when its scan starts
     */
    public static UnfilteredPartitionIterator rangeIterator(PartitionRangeReadCommand command,
                                                     ColumnFamilyStore cfs,
                                                     List<SSTableReader> sstables,
                                                     List<Iterator<MemtablePartitionData>> memtables,
                                                     UnfilteredPartitionIterator repaired,
                                                     SSTableReadsListener listener)
    {
        return new RangeMerge(command, cfs, sstables, memtables, repaired, listener);
    }

    /**
     * {@link #rangeIterator} for the repaired sstables of a read that tracks repaired data, merged on
     * their own.  After the read's limit, {@code RepairedDataInfo.overreadRepaired} reads on from the
     * repaired partition the read stopped in, which the read's merge has closed by then.  The iterator
     * path's sstable iterators do not own the scanner's file and still give the rest of the partition
     * after their close, so a partition of this merge closed before its end first reads its rest
     * into memory and stays readable.
     */
    public static UnfilteredPartitionIterator repairedRangeIterator(PartitionRangeReadCommand command,
                                                             ColumnFamilyStore cfs,
                                                             List<SSTableReader> sstables,
                                                             SSTableReadsListener listener)
    {
        RangeMerge merge = new RangeMerge(command, cfs, sstables, new ArrayList<>(0), null, listener);
        merge.readableAfterClose = true;
        return merge;
    }

    /**
     * The replica data response of a range read, written from the cursor merge straight to the
     * response bytes through {@link ResponseSink}, with no row or cell objects for sstable data.
     * The bytes are those of {@code executeLocally} then {@code createResponse}: the sink applies the
     * stages {@code executeLocally} applies above the merge (cancellation, purge, scan metrics and
     * tombstone thresholds, row filter, limits, closing a range tombstone the limit leaves open), and
     * one limit counter, row filter and scan guard span the whole read, as over the iterator path's
     * partitions.  The merge stops at the limit, so a limited read merges nothing past it.
     * <p>
     * Returns null, before anything is read, for a read it does not write this way; the caller then
     * serves it through {@code executeLocally}, which the gate counts.
     */
    static ReadResponse responseBytes(PartitionRangeReadCommand command, ReadExecutionController controller)
    {
        // a digest is a hash of the merged rows, not their bytes
        if (command.isDigestQuery())
            return null;
        // these stages of executeLocally walk the row objects this path never builds
        if (CursorReads.querySizeTrackingActive(command) || command.slowedDownForTesting())
            return null;
        // the repaired data digest is computed over the rows executeLocally builds
        if (controller.isTrackingRepairedStatus())
            return null;
        DataLimits limits = command.limits();
        if (limits.isGroupByLimit() || limitReachedAtRangeStart(command))
            return null;

        long startTimeNanos = nanoTime();
        TableMetadata metadata = command.metadata();
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata);
        ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(command.dataRange().keyRange()));
        if (unsupported(command, cfs, controller, view) != null)
            return null;
        // the range is formatted only for a traced read: it costs a String.format per read
        if (Tracing.isTracing())
            Tracing.trace("Executing seq scan across {} sstables for {}", view.sstables.size(), command.dataRange().keyRange().getString(metadata.partitionKeyType));
        countServed(metadata);

        PartitionRangeReadCommand.CursorRangeInputs inputs = command.cursorRangeInputs(controller, view);
        // like the iterator path, which reads an empty iterator when there is nothing to read
        RangeMerge merge = inputs.memtables.isEmpty() && inputs.sstables.isEmpty()
                           ? null
                           : new RangeMerge(command, cfs, inputs.sstables, inputs.memtables, null, inputs.listener);
        long nowInSec = command.nowInSec();
        long gcBefore = nowInSec == 0 ? Long.MIN_VALUE : cfs.gcBefore(nowInSec);
        DataLimits.Counter counter = limits.isUnlimited()
                                     ? null
                                     : limits.newCounter(nowInSec, false, command.selectsFullPartition(), metadata.enforceStrictLiveness());
        CursorReads.TombstoneScanGuard scanGuard = new CursorReads.TombstoneScanGuard(command, cfs.metric, startTimeNanos, nowInSec, null);
        ResponseSink.ResponseBuffer out = new ResponseSink.ResponseBuffer();
        // the writer and slices are set per partition
        ResponseSink sink = new ResponseSink(null, metadata.comparator, Slices.NONE, command.isReversed(), metadata.enforceStrictLiveness(),
                                             nowInSec, gcBefore, cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(),
                                             controller.oldestUnrepairedTombstone(), scanGuard, counter, false,
                                             command.rowFilter().isEmpty() ? null : new ResponseSink.ResponseFilter(command), command);
        Throwable failure = null;
        try
        {
            // the envelope: the unused isForThrift flag, the partitions, then the end of the partitions
            out.writeBoolean(false);
            if (merge != null)
                merge.writeTo(sink, out, counter, scanGuard);
            out.writeBoolean(false);
        }
        catch (IOException e)
        {
            failure = new RuntimeException("cursor range read response failed", e);
        }
        catch (RuntimeException | Error e)
        {
            failure = e;
        }
        finally
        {
            failure = finishRead(cfs, merge, inputs, scanGuard, failure);
        }
        Throwables.maybeFail(failure);
        RESPONSES_WRITTEN.increment();
        return ReadResponse.createTranscodedDataResponse(out.parts(), controller.getRepairedDataInfo());
    }

    /** What closing the iterator path's read does: the merge closes, the scan metrics and the
     *  sstables-per-range-read count are recorded.  Returns {@code failure} with any failure added. */
    static Throwable finishRead(ColumnFamilyStore cfs, RangeMerge merge, PartitionRangeReadCommand.CursorRangeInputs inputs,
                                        CursorReads.TombstoneScanGuard scanGuard, Throwable failure)
    {
        if (merge != null)
        {
            failure = RangeMerge.closeQuietly(merge, failure);
            cfs.metric.updateSSTableIteratedInRangeRead(inputs.sstables.size());
        }
        try
        {
            scanGuard.finish();
        }
        catch (Throwable t)
        {
            failure = Throwables.merge(failure, t);
        }
        return failure;
    }

    /**
     * Whether a paged read resumes in a partition it already finished: its limit is reached when that
     * partition starts.  The iterator path then still reads the partition's first element for the
     * purge, which this path does not reproduce.
     */
    private static boolean limitReachedAtRangeStart(PartitionRangeReadCommand command)
    {
        if (command.limits().kind() != DataLimits.Kind.CQL_PAGING_LIMIT || !(command.dataRange().startKey() instanceof DecoratedKey))
            return false;
        DataLimits.Counter probe = command.limits().newCounter(command.nowInSec(), false, command.selectsFullPartition(),
                                                               command.metadata().enforceStrictLiveness());
        probe.countPartition((DecoratedKey) command.dataRange().startKey(), Rows.EMPTY_STATIC_ROW);
        return probe.isDoneForPartition();
    }

    /** One input of the merge: a memtable or an sstable, at the partition with key {@link #key}. */
    private abstract static class Source
    {
        /** The key of the partition the source is at; null when it has no more. */
        DecoratedKey key;

        /** Moves to the next partition; false when there is none. */
        abstract boolean advance();

        abstract void close();
    }

    /** An sstable none of whose keys is in the read's range.  The iterator path still merges its
     *  empty scanner, which takes part in the order the merge gives partitions in. */
    private static final class EmptySource extends Source
    {
        @Override
        boolean advance()
        {
            return false;
        }

        @Override
        void close()
        {
        }
    }

    /** The order of the iterator path's partition merge: by key, sources on one key in the order
     *  {@code MergeIterator} gives them. */
    private static final IteratorMergeOrder.Sources<Source> PARTITION_ORDER = new IteratorMergeOrder.Sources<Source>()
    {
        @Override
        public int compare(Source a, Source b)
        {
            return a.key.compareTo(b.key);
        }

        @Override
        public boolean advance(Source source)
        {
            return source.advance();
        }
    };

    /**
     * A memtable: the content of each of its partitions in the range, read once when the source
     * reaches it, which the merge reads through a {@link NativeMemtableMergeLeg}.
     */
    private static final class MemtableSource extends Source
    {
        private final Iterator<MemtablePartitionData> partitions;
        /** The current partition's content; null when the source has no more. */
        MemtablePartitionData partition;
        /** The leg of the current partition, once {@link RangeMerge#openOnKey} opened it. */
        NativeMemtableMergeLeg leg;
        /** The leg the source reopens at each partition it reads; null until the first. */
        private NativeMemtableMergeLeg reused;

        MemtableSource(Iterator<MemtablePartitionData> partitions)
        {
            this.partitions = partitions;
        }

        @Override
        boolean advance()
        {
            leg = null;
            partition = partitions.hasNext() ? partitions.next() : null;
            key = partition == null ? null : partition.key;
            return partition != null;
        }

        @Override
        void close()
        {
            partition = null;
            leg = null;
        }
    }

    /**
     * The merged repaired sstables of a read that tracks repaired data, as an iterator: every row
     * the merge pulls from them feeds the repaired data digest.
     */
    private static final class RepairedSource extends Source
    {
        private final UnfilteredPartitionIterator partitions;
        private UnfilteredRowIterator partition;
        /** The partition {@link #take} handed out last, until the source advances. */
        UnfilteredRowIterator taken;

        RepairedSource(UnfilteredPartitionIterator partitions)
        {
            this.partitions = partitions;
        }

        @Override
        boolean advance()
        {
            taken = null;
            partition = partitions.hasNext() ? partitions.next() : null;
            key = partition == null ? null : partition.partitionKey();
            return partition != null;
        }

        /** Hands the current partition to the caller, which closes it. */
        UnfilteredRowIterator take()
        {
            taken = partition;
            partition = null;
            return taken;
        }

        @Override
        void close()
        {
            try
            {
                if (partition != null)
                    partition.close();
            }
            finally
            {
                partition = null;
                partitions.close();
            }
        }
    }

    /**
     * The partitions of one sstable within the read's key range.  {@link #key} is the key the cursor
     * read from the current partition header; it changes when the source advances.
     */
    private static final class SSTableSource extends Source
    {
        final SSTableReader sstable;
        final CursorReads.PendingLeg leg;
        private final SSTableCursorReader cursor;
        private final List<AbstractBounds<PartitionPosition>> pieces;
        private int nextPiece;
        private AbstractBounds<PartitionPosition> piece;
        private PartitionPositions positions;
        /** The current partition's row index entry, or null when it has none. */
        BtiCursorSeekSupport.PartitionEntry rowIndexed;
        /** Where the partition whose header the cursor read last starts. */
        private long lastHeaderPosition = -1;

        SSTableSource(SSTableReader sstable, TableMetadata metadata, ColumnFilter columnFilter,
                      List<AbstractBounds<PartitionPosition>> pieces, CursorReads.ValueTransfer transfer)
        {
            this.sstable = sstable;
            this.pieces = pieces;
            this.cursor = SSTableCursorReader.forRangeRead(sstable);
            try
            {
                this.leg = CursorReads.PendingLeg.forRangeRead(sstable, metadata, columnFilter, cursor, transfer);
            }
            catch (RuntimeException | Error e)
            {
                cursor.close();
                throw e;
            }
        }

        @Override
        boolean advance()
        {
            while (true)
            {
                if (positions == null)
                {
                    if (nextPiece >= pieces.size())
                    {
                        key = null;
                        return false;
                    }
                    piece = pieces.get(nextPiece++);
                    positions = PartitionPositions.open(sstable, piece);
                }
                if (!positions.advance())
                {
                    closePositions();
                    continue;
                }
                long position = positions.dataPosition();
                // A later range of the sstable can start at a partition the cursor has already
                // passed: the index may give one partition before the range, and the end of the
                // previous range was found by reading the header of the partition after it.
                // Partitions are in key order in the data file, so one before the last header read
                // is before this range.
                if (position < lastHeaderPosition)
                    continue;
                lastHeaderPosition = position;
                cursor.seekToPartition(position);
                DecoratedKey header = leg.readRangePartitionHeader();
                int left = header.compareTo(piece.left);
                if (left < 0 || (left == 0 && !piece.inclusiveLeft()))
                    continue; // the index gave a partition before the range
                int right = header.compareTo(piece.right);
                if (right > 0 || (right == 0 && !piece.inclusiveRight()))
                {
                    closePositions(); // past the end of the range
                    continue;
                }
                key = header;
                rowIndexed = positions.rowIndexedEntry();
                return true;
            }
        }

        private void closePositions()
        {
            PartitionPositions toClose = positions;
            positions = null;
            toClose.close();
        }

        @Override
        void close()
        {
            try
            {
                if (positions != null)
                    closePositions();
            }
            finally
            {
                leg.closeRangeCursor();
            }
        }
    }

    /** Where each partition of an sstable starts in its data file, in order, from a key on. */
    private abstract static class PartitionPositions implements AutoCloseable
    {
        abstract boolean advance();

        abstract long dataPosition();

        /** The row index entry of the current partition, or null when it has none. */
        abstract BtiCursorSeekSupport.PartitionEntry rowIndexedEntry();

        @Override
        public abstract void close();

        /** The partitions from the start of {@code range}, and maybe one before it and one past its end. */
        static PartitionPositions open(SSTableReader sstable, AbstractBounds<PartitionPosition> range)
        {
            if (sstable instanceof BtiTableReader)
                return new BtiPositions((BtiTableReader) sstable, range);
            return new IndexFilePositions(sstable, range.left);
        }
    }

    /** BTI: the partition index walk, which reads no data file byte. */
    private static final class BtiPositions extends PartitionPositions
    {
        private final BtiCursorRangeSupport.PartitionWalker walker;

        BtiPositions(BtiTableReader sstable, AbstractBounds<PartitionPosition> range)
        {
            this.walker = BtiCursorRangeSupport.PartitionWalker.open(sstable, range.left, range.right);
        }

        @Override
        boolean advance()
        {
            return walker.advance();
        }

        @Override
        long dataPosition()
        {
            return walker.dataPosition();
        }

        @Override
        BtiCursorSeekSupport.PartitionEntry rowIndexedEntry()
        {
            return walker.rowIndexedEntry();
        }

        @Override
        public void close()
        {
            walker.close();
        }
    }

    /** Other formats (BIG): the sstable's key reader over its index file, from the first key at or after {@code from}. */
    private static final class IndexFilePositions extends PartitionPositions
    {
        private final SSTableReader sstable;
        private final KeyReader keys;
        private boolean started;

        IndexFilePositions(SSTableReader sstable, PartitionPosition from)
        {
            this.sstable = sstable;
            try
            {
                this.keys = sstable.keyReader(from);
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }

        @Override
        boolean advance()
        {
            try
            {
                if (!started)
                {
                    started = true;
                    return !keys.isExhausted();
                }
                return keys.advance();
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }

        @Override
        long dataPosition()
        {
            return keys.dataPosition();
        }

        @Override
        BtiCursorSeekSupport.PartitionEntry rowIndexedEntry()
        {
            return null;
        }

        @Override
        public void close()
        {
            keys.close();
        }
    }

    /**
     * The key ranges of {@code sstable} a read of {@code requested} covers, in key order, each
     * clamped to the sstable's first and last keys, as {@code SSTableScanner.makeBounds} gives them:
     * the clamp keeps the read off the data of an early-opened sstable past its last key, and before
     * the moved start of a {@code MOVED_START} one.
     */
    static List<AbstractBounds<PartitionPosition>> sstableRanges(SSTableReader sstable, AbstractBounds<PartitionPosition> requested)
    {
        List<AbstractBounds<PartitionPosition>> ranges = new ArrayList<>(2);
        if (requested instanceof org.apache.cassandra.dht.Range && ((org.apache.cassandra.dht.Range<?>) requested).isWrapAround())
        {
            if (requested.right.compareTo(sstable.getFirst()) >= 0)
            {
                // the wrapped part from the sstable's first key up to the requested end
                AbstractBounds.Boundary<PartitionPosition> left = new AbstractBounds.Boundary<>(sstable.getFirst(), true);
                AbstractBounds.Boundary<PartitionPosition> right = minRight(requested.rightBoundary(), sstable.getLast(), true);
                if (!isEmpty(left, right))
                    ranges.add(AbstractBounds.bounds(left, right));
            }
            if (requested.left.compareTo(sstable.getLast()) <= 0)
            {
                // the part from the requested start to the sstable's last key
                AbstractBounds.Boundary<PartitionPosition> right = new AbstractBounds.Boundary<>(sstable.getLast(), true);
                AbstractBounds.Boundary<PartitionPosition> left = maxLeft(requested.leftBoundary(), sstable.getFirst(), true);
                if (!isEmpty(left, right))
                    ranges.add(AbstractBounds.bounds(left, right));
            }
        }
        else
        {
            AbstractBounds.Boundary<PartitionPosition> left = maxLeft(requested.leftBoundary(), sstable.getFirst(), true);
            // a range that ends at the minimum position runs to the end of the ring
            AbstractBounds.Boundary<PartitionPosition> right = requested.right.isMinimum()
                                                               ? new AbstractBounds.Boundary<>(sstable.getLast(), true)
                                                               : minRight(requested.rightBoundary(), sstable.getLast(), true);
            if (!isEmpty(left, right))
                ranges.add(AbstractBounds.bounds(left, right));
        }
        return ranges;
    }

    /**
     * The merge of the sources, partition by partition, in token order.  Lazy: the sources open
     * at the first {@code hasNext}, and each partition is read as its consumer pulls it.
     */
    static final class RangeMerge extends AbstractUnfilteredPartitionIterator
    {
        private final PartitionRangeReadCommand command;
        private final ColumnFamilyStore cfs;
        private final TableMetadata metadata;
        private final ColumnFilter columnFilter;
        private final boolean reversed;
        private final List<SSTableReader> sstables;
        private final List<Iterator<MemtablePartitionData>> memtables;
        /** See {@link #rangeIterator}; null for none. */
        private final UnfilteredPartitionIterator repaired;
        /** The repaired source's partition on the current key, or null. */
        private UnfilteredRowIterator repairedPartition;
        private final SSTableReadsListener listener;
        /** One value-transfer scratch for every leg of the read. */
        private final CursorReads.ValueTransfer transfer = new CursorReads.ValueTransfer();
        /**
         * The read's filter pushdown ({@link CursorReads#filterPushdownFor}), or null: a forward
         * merge reads no row of a partition whose partition-level expressions fail.  Rows are not
         * dropped in the merge: a range read has no {@code ScanStatsAccumulator}, so its rows reach
         * the scan metrics (for a response, {@code TombstoneScanGuard}, the one live count) before
         * the row filter, as on the iterator path.
         */
        private final CursorReads.FilterPushdown filterPushdown;

        /** The sources, in the iterator path's input order: memtables, sstables, then the repaired sstables. */
        private Source[] sources;
        private IteratorMergeOrder<Source> order;
        /** The sources of the partition returned last, in the order the iterator path's merge gives
         *  them; they advance before the next one is merged. */
        private Source[] onKey;
        private int onKeyCount;

        private ReturnedPartition returned;
        private UnfilteredRowIterator next;
        private boolean closed;

        RangeMerge(PartitionRangeReadCommand command, ColumnFamilyStore cfs, List<SSTableReader> sstables,
                   List<Iterator<MemtablePartitionData>> memtables, UnfilteredPartitionIterator repaired,
                   SSTableReadsListener listener)
        {
            this.repaired = repaired;
            this.command = command;
            this.cfs = cfs;
            this.metadata = cfs.metadata();
            this.columnFilter = command.columnFilter();
            this.reversed = command.isReversed();
            this.sstables = sstables;
            this.memtables = memtables;
            this.listener = listener;
            this.filterPushdown = CursorReads.filterPushdownFor(command);
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            if (next != null)
                return true;
            if (closed)
                return false;
            next = computeNext();
            return next != null;
        }

        @Override
        public UnfilteredRowIterator next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            UnfilteredRowIterator result = next;
            next = null;
            return result;
        }

        private UnfilteredRowIterator computeNext()
        {
            if (returned != null)
            {
                // the sources of the last partition share their cursors with it: it must be done
                // with them before they move on
                ReturnedPartition previous = returned;
                returned = null;
                previous.detach();
            }
            if (!nextKey())
                return null;
            returned = new ReturnedPartition(mergePartition(), readableAfterClose);
            return returned;
        }

        /**
         * Moves the sources of the last partition on, then puts every source on the least key in
         * {@link #onKey}, in the order the iterator path's merge gives them.  False when no partition is left.
         */
        private boolean nextKey()
        {
            if (sources == null)
                openSources();
            if (sources.length == 1)
            {
                // one source: its partitions in its order, as UnfilteredPartitionIterators.mergeLazily
                // returns a lone iterator
                onKeyCount = !oneSourceDone && sources[0].advance() ? 1 : 0;
                oneSourceDone = onKeyCount == 0;
                onKey[0] = sources[0];
                return onKeyCount > 0;
            }
            onKeyCount = order.next(onKey);
            return onKeyCount > 0;
        }

        /** A merge of one source has read all its partitions. */
        private boolean oneSourceDone;

        private void openSources()
        {
            sources = new Source[memtables.size() + sstables.size() + (repaired == null ? 0 : 1)];
            int count = 0;
            try
            {
                for (Iterator<MemtablePartitionData> memtable : memtables)
                    sources[count++] = new MemtableSource(memtable);
                // made before the sstable sources, so it is closed if one fails, but read last
                if (repaired != null)
                    sources[sources.length - 1] = new RepairedSource(repaired);
                AbstractBounds<PartitionPosition> requested = command.dataRange().keyRange();
                for (SSTableReader sstable : sstables)
                {
                    // like SSTableScanner.createIterator: once per sstable, when its scan starts
                    listener.onScanningStarted(sstable);
                    List<AbstractBounds<PartitionPosition>> ranges = sstableRanges(sstable, requested);
                    sources[count++] = ranges.isEmpty() ? new EmptySource()
                                                        : new SSTableSource(sstable, metadata, columnFilter, ranges, transfer);
                }
            }
            catch (RuntimeException | Error e)
            {
                // every memtable and the repaired iterator have a source by now: sstable sources are made after them
                closed = true;
                closeSources(sources.length, e);
                throw e;
            }
            onKey = new Source[sources.length];
            order = new IteratorMergeOrder<>(sources.length, PARTITION_ORDER);
            order.reset(sources, sources.length);
        }

        /** The partition of the sources in {@link #onKey}. */
        private UnfilteredRowIterator mergePartition()
        {
            DecoratedKey key = stableKey();
            ClusteringIndexFilter filter = command.clusteringIndexFilter(key);
            if (cfs.isRowCacheEnabled())
            {
                // checkCacheFilter above takes the cached partition again; this only keeps the
                // sources unread, as the iterator path's lazy merged partition is
                UnfilteredRowIterator cached = fromRowCache(key, filter);
                if (cached != null)
                    return cached;
            }
            Slices slices = filter.getSlices(metadata);
            List<CursorReads.PendingLeg> legs = new ArrayList<>(onKeyCount);
            List<NativeMemtableMergeLeg> memtableLegs = new ArrayList<>(onKeyCount);
            // executeLocally keeps the merged rows: the memtable legs give the iterator path's objects
            openOnKey(key, filter, slices, legs, memtableLegs, true);

            // a lone object source is read as is, as the iterator path reads it
            if (onKeyCount == 1 && repairedPartition != null)
                return repairedPartition;
            if (memtableLegs.isEmpty() && repairedPartition == null && legs.size() == 1)
                return reversed ? CursorReads.completeSingleLegReversed(legs.get(0)) : CursorReads.completeSingleLeg(legs.get(0));

            // memtables, a lone one too, merge in the cursor merge
            List<CursorReads.MergeLeg> mergeLegs = new ArrayList<>(onKeyCount);
            addMergeLegs(slices, mergeLegs);
            return reversed ? CursorReads.mergeLegsReversed(mergeLegs, metadata, key, slices, columnFilter)
                            : CursorReads.mergeLegs(mergeLegs, metadata, key, slices, columnFilter, filterPushdown);
        }

        /** The merge legs of the sources in {@link #onKey}, opened by {@link #openOnKey}, in the
         *  order the iterator path's merge gives its versions: it decides which source's objects
         *  the merged rows keep. */
        private void addMergeLegs(Slices slices, List<CursorReads.MergeLeg> mergeLegs)
        {
            for (int i = 0; i < onKeyCount; i++)
            {
                Source source = onKey[i];
                if (source instanceof MemtableSource)
                {
                    mergeLegs.add(((MemtableSource) source).leg);
                }
                else if (source instanceof RepairedSource)
                {
                    // pulled only as far as the merge reads it, since every pull feeds the digest
                    mergeLegs.add(new MemtableMergeLeg(((RepairedSource) source).taken, slices));
                }
                else
                {
                    CursorReads.PendingLeg leg = ((SSTableSource) source).leg;
                    mergeLegs.add(reversed ? new CursorReads.ReverseMergeLeg(leg) : leg);
                }
            }
        }

        /**
         * Opens the partition of every source in {@link #onKey}: each sstable leg into {@code legs},
         * each memtable partition as a {@link NativeMemtableMergeLeg} into {@code memtableLegs}, and
         * the repaired sstables' partition into {@link #repairedPartition}.  On failure it closes
         * what it opened.
         *
         * @param emitObjects whether the merge's sink keeps row objects (see {@link NativeMemtableMergeLeg})
         */
        private void openOnKey(DecoratedKey key, ClusteringIndexFilter filter, Slices slices, List<CursorReads.PendingLeg> legs,
                               List<NativeMemtableMergeLeg> memtableLegs, boolean emitObjects)
        {
            // the slices of a names filter hold one clustering each
            boolean names = filter instanceof ClusteringIndexNamesFilter;
            repairedPartition = null;
            try
            {
                for (int i = 0; i < onKeyCount; i++)
                {
                    Source source = onKey[i];
                    if (source instanceof MemtableSource)
                    {
                        MemtableSource memtable = (MemtableSource) source;
                        // the partition before it is closed: one leg serves every partition of the source
                        if (memtable.reused == null)
                            memtable.reused = new NativeMemtableMergeLeg(metadata, memtable.partition, columnFilter, slices, names, reversed, emitObjects);
                        else
                            memtable.reused.reopen(memtable.partition, slices, names);
                        memtable.leg = memtable.reused;
                        memtableLegs.add(memtable.leg);
                    }
                    else if (source instanceof RepairedSource)
                    {
                        repairedPartition = ((RepairedSource) source).take();
                        ITERATOR_LEGS_OPENED.increment();
                        iteratorLegs++;
                    }
                    else
                    {
                        SSTableSource sstable = (SSTableSource) source;
                        legs.add(sstable.leg);
                        sstable.leg.openRangePartition(key, slices, sstable.rowIndexed, reversed);
                    }
                }
            }
            catch (IOException e)
            {
                RuntimeException failure = new RuntimeException("cursor range read failed for " + key, e);
                closeOpened(legs, memtableLegs, failure);
                throw failure;
            }
            catch (RuntimeException | Error e)
            {
                closeOpened(legs, memtableLegs, e);
                throw e;
            }
        }

        // ---- the replica response

        /** The writer of every partition of the read; its header is the last partition's. */
        private ResponseWireWriter writer;
        private RegularAndStaticColumns writerColumns;
        private EncodingStats writerStats;
        private final List<EncodingStats> partitionStats = new ArrayList<>();

        /**
         * Writes the partitions into {@code out} through {@code sink}, in order, until the limit
         * counter is done or no partition is left.  Each partition merges as {@link #mergePartition}
         * merges it, into the sink instead of into row objects.
         *
         * @param counter the read's limit counter, or null when the read has no limit
         */
        void writeTo(ResponseSink sink, ResponseSink.ResponseBuffer out, DataLimits.Counter counter,
                     CursorReads.TombstoneScanGuard scanGuard) throws IOException
        {
            writePartitions(sink, out, null, null, counter, scanGuard);
        }

        /**
         * {@link #writeTo} for a {@link CursorRangeScan}: writes the partitions to {@code output},
         * while {@code scanSink} wants more, until it asks to yield after a partition.  The row cache
         * is not read: the sources hold what it holds.
         */
        CursorRangeScan.ScanEnd scanTo(ResponseSink sink, RangeReadSinkOutput output, RangeReadSink scanSink,
                                       DataLimits.Counter counter, CursorReads.TombstoneScanGuard scanGuard) throws IOException
        {
            return writePartitions(sink, null, output, scanSink, counter, scanGuard);
        }

        /** See {@link #repairedRangeIterator}. */
        boolean readableAfterClose;

        /** The key of the last partition {@link #writePartitions} finished, whether or not it was written. */
        DecoratedKey lastPartitionKey;
        /** {@link #iteratorLegsOpened}, of this merge. */
        long iteratorLegs;

        /**
         * @param out       the response bytes, or null when writing to {@code scanOutput}
         * @param scanSink  the sink of a {@link CursorRangeScan} behind {@code scanOutput}, or null
         */
        private CursorRangeScan.ScanEnd writePartitions(ResponseSink sink, ResponseSink.ResponseBuffer out,
                                                        RangeReadSinkOutput scanOutput, RangeReadSink scanSink,
                                                        DataLimits.Counter counter, CursorReads.TombstoneScanGuard scanGuard) throws IOException
        {
            // the response path never reads repaired data for its digest
            assert repaired == null : "a range response does not track repaired data";
            // a partition has at most one leg per source
            int sourceCount = memtables.size() + sstables.size();
            List<CursorReads.PendingLeg> legs = new ArrayList<>(sourceCount);
            List<NativeMemtableMergeLeg> memtableLegs = new ArrayList<>(sourceCount);
            List<CursorReads.MergeLeg> mergeLegs = new ArrayList<>(sourceCount);
            boolean rowCache = scanSink == null && cfs.isRowCacheEnabled();
            while (counter == null || !counter.isDone())
            {
                if (scanSink != null && !scanSink.wantsMore())
                    return CursorRangeScan.ScanEnd.STOPPED;
                if (!nextKey())
                    break;
                DecoratedKey key = stableKey();
                ClusteringIndexFilter filter = command.clusteringIndexFilter(key);
                Slices slices = filter.getSlices(metadata);
                legs.clear();
                memtableLegs.clear();
                mergeLegs.clear();
                UnfilteredRowIterator cached = rowCache ? fromRowCache(key, filter) : null;
                // the memtable legs give their cells from the memtable's own storage
                if (cached == null)
                    openOnKey(key, filter, slices, legs, memtableLegs, false);
                boolean written;
                try
                {
                    scanGuard.startPartition(key);
                    if (cached != null)
                    {
                        sink.startRangePartition(writerFor(out, cached), slices, true);
                        written = CursorReads.streamResponse(cached, columnFilter, sink, out);
                    }
                    else
                    {
                        sink.startRangePartition(scanOutput != null ? scanOutput : writerFor(out, legs, memtableLegs), slices, onKeyCount == 1);
                        written = writePartition(key, slices, legs, mergeLegs, sink, out);
                    }
                }
                catch (IOException e)
                {
                    RuntimeException failure = new RuntimeException("cursor range read failed for " + key, e);
                    closePartition(legs, memtableLegs, cached, mergeLegs, failure);
                    throw failure;
                }
                catch (RuntimeException | Error e)
                {
                    closePartition(legs, memtableLegs, cached, mergeLegs, e);
                    throw e;
                }
                closePartition(legs, memtableLegs, cached, mergeLegs, null);
                if (!sink.partitionSurvivedPurge())
                    scanGuard.partitionRemovedByPurge();
                // the iterator path's counter sees only the partitions the purge and the row filter keep
                if (written && counter != null)
                    counter.endPartition();
                lastPartitionKey = key;
                if (scanSink != null && scanSink.yieldAfterPartition())
                    return CursorRangeScan.ScanEnd.YIELDED;
            }
            return CursorRangeScan.ScanEnd.DONE;
        }

        /**
         * The cached partition of {@code key} when the row cache holds all the read needs of it, as
         * the iterator path's {@code checkCacheFilter} takes it; null otherwise.  The cached partition
         * replaces the sources on the key, which are left unread, as the iterator path leaves its
         * merged partition unread.
         */
        private UnfilteredRowIterator fromRowCache(DecoratedKey key, ClusteringIndexFilter filter)
        {
            CachedPartition cached = cfs.getRawCachedPartition(key);
            if (cached == null || !cfs.isFilterFullyCoveredBy(filter, command.limits(), cached, command.nowInSec(), metadata.enforceStrictLiveness()))
                return null;
            ITERATOR_LEGS_OPENED.increment();
            iteratorLegs++;
            return filter.getUnfilteredRowIterator(columnFilter, cached);
        }

        /** One partition, written the way {@link #mergePartition} reads it. */
        private boolean writePartition(DecoratedKey key, Slices slices, List<CursorReads.PendingLeg> legs,
                                       List<CursorReads.MergeLeg> mergeLegs, ResponseSink sink, ResponseSink.ResponseBuffer out) throws IOException
        {
            if (onKeyCount == 1 && !legs.isEmpty())
            {
                // one sstable: the iterator path serializes its partition as is
                return reversed ? CursorReads.streamReversedResponse(legs.get(0), columnFilter, sink, out)
                                : CursorReads.streamResponse(legs, metadata, key, slices, columnFilter, filterPushdown, sink, out);
            }
            // memtables, a lone one too: the merge of one memtable leg gives what its row iterator gives
            addMergeLegs(slices, mergeLegs);
            return reversed ? CursorReads.streamReversedResponse(mergeLegs, metadata, key, slices, columnFilter, sink, out)
                            : CursorReads.streamResponse(mergeLegs, metadata, key, slices, columnFilter, filterPushdown, sink, out);
        }

        /**
         * The writer for the partition the sources in {@link #onKey} give: its serialization header
         * has the columns and stats of the iterator path's partition, those of its one source, or
         * the union of the columns and the merged stats of several, like {@code UnfilteredRowIterators.merge}.
         */
        private ResponseWireWriter writerFor(ResponseSink.ResponseBuffer out, List<CursorReads.PendingLeg> legs,
                                             List<NativeMemtableMergeLeg> memtableLegs)
        {
            RegularAndStaticColumns columns = null;
            partitionStats.clear();
            // indexed loops: this runs once per partition
            for (int i = 0; i < memtableLegs.size(); i++)
            {
                // the columns and stats the memtable's row iterator reports
                NativeMemtableMergeLeg memtable = memtableLegs.get(i);
                columns = columns == null ? memtable.legColumns() : columns.mergeTo(memtable.legColumns());
                partitionStats.add(memtable.legStats());
            }
            for (int i = 0; i < legs.size(); i++)
            {
                RegularAndStaticColumns fetched = columnFilter.fetchedColumns();
                columns = columns == null ? fetched : columns.mergeTo(fetched);
                partitionStats.add(legs.get(i).sstable.stats());
            }
            EncodingStats stats = partitionStats.size() == 1 ? partitionStats.get(0)
                                                             : EncodingStats.merge(partitionStats, Function.identity());
            return writerFor(out, columns, stats);
        }

        /** The writer for a cached partition, with its columns and stats. */
        private ResponseWireWriter writerFor(ResponseSink.ResponseBuffer out, UnfilteredRowIterator cached)
        {
            return writerFor(out, cached.columns(), cached.stats());
        }

        private ResponseWireWriter writerFor(ResponseSink.ResponseBuffer out, RegularAndStaticColumns columns, EncodingStats stats)
        {
            if (writer == null || !columns.equals(writerColumns) || !stats.equals(writerStats))
            {
                SerializationHeader header = new SerializationHeader(false, metadata, columns, stats);
                // one writer for the whole read: a partition with a different header only changes its header
                if (writer == null)
                    writer = new ResponseWireWriter(out, header, MessagingService.current_version, !metadata.droppedColumns.isEmpty());
                else
                    writer.useHeader(header);
                writerColumns = columns;
                writerStats = stats;
            }
            return writer;
        }

        /** Closes what {@link #writeTo} opened for a partition.  With no {@code cause}, the first
         *  failure to close is thrown. */
        private static void closePartition(List<CursorReads.PendingLeg> legs, List<NativeMemtableMergeLeg> memtableLegs,
                                           UnfilteredRowIterator cached, List<CursorReads.MergeLeg> mergeLegs, Throwable cause)
        {
            Throwable failure = cause;
            if (cached != null)
                failure = closeQuietly(cached, failure);
            if (!mergeLegs.isEmpty())
            {
                // the merge legs wrap every leg
                for (CursorReads.MergeLeg leg : mergeLegs)
                    failure = closeQuietly(leg, failure);
            }
            else
            {
                for (CursorReads.PendingLeg leg : legs)
                    failure = closeQuietly(leg, failure);
                for (NativeMemtableMergeLeg leg : memtableLegs)
                    failure = closeQuietly(leg, failure);
            }
            if (cause == null && failure != null)
                Throwables.maybeFail(failure);
        }

        /**
         * The key object the iterator path's merge reports: that of the first source its merge gives
         * the partition from.  A memtable's own; for an sstable, a copy of the header's, made as
         * the sstable scanners make theirs, so the read size guardrail counts the same heap size.
         */
        private DecoratedKey stableKey()
        {
            Source first = onKey[0];
            if (first instanceof MemtableSource || first instanceof RepairedSource)
                return first.key;
            return metadata.partitioner.decorateKey(ByteBufferUtil.clone(first.key.getKey()));
        }

        private void closeOpened(List<CursorReads.PendingLeg> legs, List<NativeMemtableMergeLeg> memtableLegs, Throwable cause)
        {
            CursorReads.closeAll(legs, cause);
            CursorReads.closeAll(memtableLegs, cause);
            if (repairedPartition != null)
            {
                UnfilteredRowIterator partition = repairedPartition;
                repairedPartition = null;
                Throwable failure = closeQuietly(partition, null);
                if (failure != null)
                    cause.addSuppressed(failure);
            }
        }

        @Override
        public void close()
        {
            if (closed)
                return;
            closed = true;
            Throwable failure = null;
            try
            {
                if (returned != null)
                    returned.close();
            }
            catch (Throwable t)
            {
                failure = t;
            }
            returned = null;
            next = null;
            if (sources == null)
            {
                // nothing was read: the repaired iterator is still ours
                if (repaired != null)
                    failure = closeQuietly(repaired, failure);
            }
            else
            {
                failure = closeSources(sources.length, failure);
            }
            if (failure instanceof RuntimeException)
                throw (RuntimeException) failure;
            if (failure instanceof Error)
                throw (Error) failure;
        }

        /** Closes the first {@code count} sources; returns {@code failure} with any failure to close added. */
        private Throwable closeSources(int count, Throwable failure)
        {
            for (int i = 0; i < count; i++)
            {
                if (sources[i] != null)
                    failure = closeQuietly(sources[i]::close, failure);
            }
            return failure;
        }

        private static Throwable closeQuietly(AutoCloseable closeable, Throwable failure)
        {
            try
            {
                closeable.close();
            }
            catch (Throwable t)
            {
                if (failure == null)
                    return t;
                failure.addSuppressed(t);
            }
            return failure;
        }
    }

    /**
     * A partition the merge returned.  The merge closes it before its sources move on, when its
     * consumer has not.  A consumer may ask for the next partition before it reads this one, which
     * the {@code UnfilteredPartitionIterator} contract does not promise to support but the iterator
     * path does, as its partitions are read lazily from their own positions.  The rest of this one
     * is then read into memory first, so it stays readable.  A consumer that closes each partition
     * before it asks for the next never pays for this.
     */
    private static final class ReturnedPartition implements WrappingUnfilteredRowIterator
    {
        private UnfilteredRowIterator source;
        private boolean closed;
        /** See {@link CursorRangeReads#repairedRangeIterator}. */
        private final boolean readableAfterClose;

        ReturnedPartition(UnfilteredRowIterator source, boolean readableAfterClose)
        {
            this.source = source;
            this.readableAfterClose = readableAfterClose;
        }

        @Override
        public UnfilteredRowIterator wrapped()
        {
            return source;
        }

        /** The merge moves on: reads the rest of the partition into memory if it is still open. */
        void detach()
        {
            if (closed)
                return;
            UnfilteredRowIterator open = source;
            BufferedPartition buffered;
            try
            {
                buffered = new BufferedPartition(open);
            }
            finally
            {
                open.close();
            }
            source = buffered;
        }

        @Override
        public void close()
        {
            if (closed)
                return;
            if (readableAfterClose)
                detach();
            closed = true;
            source.close();
        }
    }

    /** The rest of a partition, read into memory by {@link ReturnedPartition#detach}. */
    private static final class BufferedPartition extends AbstractUnfilteredRowIterator
    {
        private final Iterator<Unfiltered> rest;

        /** Reads the rest of {@code open}; the caller closes it. */
        BufferedPartition(UnfilteredRowIterator open)
        {
            super(open.metadata(), open.partitionKey(), open.partitionLevelDeletion(), open.columns(), open.staticRow(),
                  open.isReverseOrder(), open.stats());
            List<Unfiltered> read = new ArrayList<>();
            while (open.hasNext())
                read.add(open.next());
            this.rest = read.iterator();
        }

        @Override
        protected Unfiltered computeNext()
        {
            return rest.hasNext() ? rest.next() : endOfData();
        }
    }
}
