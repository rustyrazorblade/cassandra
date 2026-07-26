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

package org.apache.cassandra.arrow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Full-table-scan orchestration for one table (task #11 - see {@code ARROW-FLIGHT.md}).
 * <p>
 * Memtable freshness strategy: flush-then-scan. A blocking flush is forced before the sstable set
 * is pinned, so the scan sees a consistent "snapshot as of scan start" that includes anything that
 * was in the memtable when the scan began - the simplest correct approach for this PoC (see the
 * design doc's Proposed Changes #3; live no-flush merge is deliberately deferred future work).
 * <p>
 * Producer selection: the cursor-merge path ({@link CursorCompactor}) is used when
 * {@link CursorCompactor#isCursorReadSupported} allows it; otherwise this falls back to the normal
 * iterator-based read path (a token-range-restricted {@link PartitionRangeReadCommand}, or
 * {@link PartitionRangeReadCommand#allDataRead} when no range was requested). Both producers feed
 * the same {@link ArrowRowAssembler}, so output is identical regardless of which one ran.
 * <p>
 * <b>Token-range bounding:</b> an optional {@code tokenRange} restricts either producer to a
 * partition-boundary-aligned {@code (start, end]} token subrange - the cursor path via
 * {@code StatefulCursor#positionAt}/{@code #setEndBound} (through the {@link CursorCompactor}
 * overload added for this), the iterator path via a {@link DataRange#forKeyRange}-restricted
 * {@link PartitionRangeReadCommand}. {@code null} (the default, via the 4-arg {@link #scan}/
 * {@link #scanViaIteratorForTesting} overloads) reproduces the original whole-local-range behavior
 * exactly on both paths.
 * <p>
 * <b>Disk access mode:</b> the cursor-merge producer always requests {@link DiskAccessMode#direct}
 * for its own reads (via a {@code CursorCompactor} overload added specifically for this - see that
 * constructor's javadoc), instead of deferring to the node-wide
 * {@code compaction_read_disk_access_mode} setting real background compaction uses. This keeps a
 * large analytical scan's I/O from polluting the page cache real background compaction/reads rely
 * on, independent of whatever that node-wide setting is. Direct I/O has two real, inherent
 * constraints (not PoC limitations - the same constraints apply to any caller of
 * {@code DiskAccessMode.direct} in this codebase): it requires a compressed sstable
 * ({@link org.apache.cassandra.io.util.FileHandle#supportsDirectIO()}) and a Linux host
 * ({@code ExtendedOpenOption.DIRECT} is a Linux-only JDK NIO extension, guarded elsewhere in this
 * codebase by {@code FBUtilities.isLinux}). Where either is false, the existing, pre-existing
 * fallback behavior in {@code SSTableReader}/{@code FileHandle} applies: the read silently proceeds
 * with whatever disk access mode was already in effect, rather than failing.
 */
public final class CassandraTableScanner
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraTableScanner.class);

    private CassandraTableScanner()
    {
    }

    /**
     * Scans {@code cfs} in full and delivers completed Arrow batches to {@code onBatch}, which may
     * be called zero or more times before this method returns (never after).
     */
    public static void scan(ColumnFamilyStore cfs, BufferAllocator allocator, long targetBatchBytes, Consumer<VectorSchemaRoot> onBatch)
    {
        scan(cfs, allocator, targetBatchBytes, onBatch, null, null, null);
    }

    /**
     * As {@link #scan(ColumnFamilyStore, BufferAllocator, long, Consumer)}, additionally supporting
     * (independently optional - any/all may be {@code null}) token-range bounding, post-merge filter
     * evaluation, and server-side aggregation - see {@code ARROW-FLIGHT.md} and
     * {@link ArrowRowAssembler}'s class javadoc for how the three compose. {@code tokenRange} is a
     * (start, end] Cassandra-convention token range (exclusive start, inclusive end - see
     * {@link org.apache.cassandra.dht.Range#makeRowRange}); {@code null} scans the table's entire
     * local range, exactly as {@link #scan(ColumnFamilyStore, BufferAllocator, long, Consumer)} does.
     */
    public static void scan(ColumnFamilyStore cfs, BufferAllocator allocator, long targetBatchBytes, Consumer<VectorSchemaRoot> onBatch,
                             Range<Token> tokenRange, FilterExpression filter, CompiledAggregation aggregation)
    {
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.ARROW_FLIGHT_SCAN);

        try (ColumnFamilyStore.RefViewFragment view = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            TableMetadata metadata = cfs.metadata();
            Range<PartitionPosition> keyRange = tokenRange == null ? null : Range.makeRowRange(tokenRange);
            List<SSTableReader> sstables = rangeRestrict(cfs, view.sstables, keyRange);
            try (ArrowRowAssembler assembler = new ArrowRowAssembler(metadata, allocator, targetBatchBytes, onBatch, filter, aggregation))
            {
                if (CursorCompactor.isCursorReadSupported(sstables, metadata))
                    scanViaCursor(cfs, sstables, assembler, keyRange);
                else
                    scanViaIterator(cfs, metadata, assembler, keyRange);
            }
        }
    }

    /**
     * Restricts {@code candidates} (already reference-held, from the canonical view) to only
     * sstables whose own key range overlaps {@code keyRange} - {@code null} means no restriction
     * (whole-table scan, every candidate kept). Unlike the iterator fallback path (which already
     * gets this for free via {@code PartitionRangeReadCommand}'s own {@code View.selectLive} use),
     * the cursor path previously passed the FULL canonical sstable set into {@link CursorCompactor}
     * regardless of {@code keyRange}, relying on each cursor's seek-to-bound to skip
     * non-overlapping sstables after already opening them and registering a
     * {@link CompactionController} reference against them - real, avoidable overhead per split,
     * multiplied by however many splits/tables scan concurrently (see the garbage-free-reads/
     * concurrency findings in {@code ARROW-FLIGHT.md}). Uses {@code View.liveSSTablesInBounds} -
     * the same interval-tree-backed selector the normal read path already relies on - rather than
     * hand-rolling first/last key comparisons.
     */
    private static List<SSTableReader> rangeRestrict(ColumnFamilyStore cfs, Collection<SSTableReader> candidates, Range<PartitionPosition> keyRange)
    {
        if (keyRange == null)
            return new ArrayList<>(candidates);
        Set<SSTableReader> overlapping = new HashSet<>();
        for (SSTableReader sstable : cfs.getTracker().getView().liveSSTablesInBounds(keyRange.left, keyRange.right))
            overlapping.add(sstable);
        List<SSTableReader> result = new ArrayList<>(candidates.size());
        for (SSTableReader sstable : candidates)
            if (overlapping.contains(sstable))
                result.add(sstable);
        return result;
    }

    private static void scanViaCursor(ColumnFamilyStore cfs, List<SSTableReader> sstables, ArrowRowAssembler assembler, Range<PartitionPosition> keyRange)
    {
        if (sstables.isEmpty())
            return;

        long nowInSec = assembler.nowInSec();
        logger.debug("Arrow Flight scan of {}.{} requesting direct I/O for {} sstable(s) (falls back silently per-sstable " +
                     "where unsupported - see the class javadoc)", cfs.keyspace.getName(), cfs.name, sstables.size());
        try (CompactionController controller = new CompactionController(cfs, ImmutableSet.copyOf(sstables), cfs.gcBefore(nowInSec)))
        {
            PartitionPosition startBound = keyRange == null ? null : keyRange.left;
            PartitionPosition endBound = keyRange == null ? null : keyRange.right;
            CursorCompactor compactor = new CursorCompactor(OperationType.VALIDATION, sstables, assembler, controller, nowInSec,
                                                             nextTimeUUID(), DiskAccessMode.direct, startBound, endBound);
            try
            {
                while (compactor.writeNextPartition())
                {
                    // driven entirely by side effects on `assembler`
                }
            }
            finally
            {
                // CursorCompactor is not itself AutoCloseable (no `implements Closeable`), so it
                // can't sit in the try-with-resources above - but close() MUST still run: it's
                // what releases every SSTableCursorReader (CursorCompactor.close() -> reader.close()
                // per cursor), which for a compressed sstable read under direct I/O owns a
                // ThreadLocalReadAheadBuffer. That class caches its read-ahead block in a STATIC,
                // per-thread map keyed only by file path (io/util/ThreadLocalReadAheadBuffer.java)
                // - leaving it unclosed here left a stale block in that map for the next scan of
                // the same file (same thread) to inherit with its own bufferSize field never
                // re-initialized (stuck at -1), corrupting that scan's read-ahead arithmetic into
                // requesting a negative buffer limit and crashing with CorruptSSTableException.
                // Confirmed via a live docker-compose smoke test against a real compressed table
                // on Linux (the only environment where direct I/O actually engages) - see
                // ARROW-FLIGHT.md / trino/docker-compose.yml.
                compactor.close();
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static void scanViaIterator(ColumnFamilyStore cfs, TableMetadata metadata, ArrowRowAssembler assembler, Range<PartitionPosition> keyRange)
    {
        long nowInSec = assembler.nowInSec();
        boolean enforceStrictLiveness = metadata.enforceStrictLiveness();
        // A bounded keyRange restricts this to exactly the token subrange a real CQL token-range
        // query would use under the hood (see DataRange#forTokenRange/PartitionRangeReadCommand)
        // instead of PartitionRangeReadCommand#allDataRead's always-whole-range DataRange.allData -
        // null reproduces that original whole-range behavior exactly.
        DataRange dataRange = keyRange == null ? DataRange.allData(metadata.partitioner) : DataRange.forKeyRange(keyRange);
        PartitionRangeReadCommand command = PartitionRangeReadCommand.create(metadata, nowInSec, ColumnFilter.all(metadata),
                                                                              RowFilter.none(), DataLimits.NONE, dataRange);
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    assembler.startPartition(partition.partitionKey().getKey());

                    Row staticRow = partition.staticRow();
                    if (!staticRow.isEmpty())
                    {
                        assembler.startRow(true);
                        writeRow(assembler, staticRow, nowInSec);
                        // no endRow(): the static row is not itself an output row (see
                        // ArrowRowAssembler's class javadoc) - its values are cached and
                        // replayed onto every real row of this partition instead.
                    }

                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (!unfiltered.isRow())
                            continue; // range tombstone marker: shadowing already applied by executeLocally

                        Row row = (Row) unfiltered;
                        // Matches the standard read path's own liveness gate
                        // (UnfilteredPartitionIterators.filter -> Filter.applyToRow -> Row#purge,
                        // which drops any row failing Row#hasLiveData): without this check, a
                        // dead row (fully tombstoned, or an expired-TTL row with no surviving
                        // primary-key liveness) would still satisfy Unfiltered#isRow() and be
                        // emitted here as a "ghost" all-null-column output row that real CQL would
                        // never return - see ARROW-FLIGHT bug tracker task #6. The cursor-merge
                        // producer already computes and applies this same drop internally
                        // (CursorCompactor's own row-liveness/purge handling).
                        if (!row.hasLiveData(nowInSec, enforceStrictLiveness))
                            continue;
                        assembler.startRow(false, row.clustering());
                        writeRow(assembler, row, nowInSec);
                        assembler.endRow();
                    }

                    assembler.endPartition();
                }
            }
        }
    }

    /**
     * Test-only seam (see ARROW-FLIGHT.md's "Test coverage gap" note): drives {@code cfs} through
     * {@link #scanViaIterator} directly, bypassing the {@link CursorCompactor#isCursorReadSupported}
     * gate that {@link #scan} uses to choose a producer. Every ordinary table on the current
     * partitioner with up-to-date sstables takes the cursor-merge path via {@link #scan}, which
     * otherwise leaves this producer with zero test coverage.
     */
    @VisibleForTesting
    public static void scanViaIteratorForTesting(ColumnFamilyStore cfs, BufferAllocator allocator, long targetBatchBytes, Consumer<VectorSchemaRoot> onBatch)
    {
        scanViaIteratorForTesting(cfs, allocator, targetBatchBytes, onBatch, null, null, null);
    }

    /** As above, additionally accepting the same optional tokenRange/filter/aggregation {@link #scan} does. */
    @VisibleForTesting
    public static void scanViaIteratorForTesting(ColumnFamilyStore cfs, BufferAllocator allocator, long targetBatchBytes, Consumer<VectorSchemaRoot> onBatch,
                                                  Range<Token> tokenRange, FilterExpression filter, CompiledAggregation aggregation)
    {
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.ARROW_FLIGHT_SCAN);
        try (ColumnFamilyStore.RefViewFragment ignored = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            TableMetadata metadata = cfs.metadata();
            Range<PartitionPosition> keyRange = tokenRange == null ? null : Range.makeRowRange(tokenRange);
            try (ArrowRowAssembler assembler = new ArrowRowAssembler(metadata, allocator, targetBatchBytes, onBatch, filter, aggregation))
            {
                scanViaIterator(cfs, metadata, assembler, keyRange);
            }
        }
    }

    private static void writeRow(ArrowRowAssembler assembler, Row row, long nowInSec)
    {
        for (ColumnData data : row)
        {
            ColumnMetadata column = data.column();
            if (data.column().isComplex())
            {
                ComplexColumnData complexData = (ComplexColumnData) data;
                assembler.beginComplexColumn(column);
                for (org.apache.cassandra.db.rows.Cell<?> cell : complexData)
                {
                    if (cell.isLive(nowInSec))
                        assembler.putComplexCell(column, cell.path().get(0), cell.buffer());
                }
                assembler.endComplexColumn(column);
            }
            else
            {
                org.apache.cassandra.db.rows.Cell<?> cell = (org.apache.cassandra.db.rows.Cell<?>) data;
                if (cell.isLive(nowInSec))
                {
                    if (column.type instanceof org.apache.cassandra.db.marshal.CounterColumnType)
                        assembler.putCounterCell(column, org.apache.cassandra.db.context.CounterContext.instance().total(cell));
                    else
                        assembler.putSimpleCell(column, cell.buffer());
                }
            }
        }
    }
}
