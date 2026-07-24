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
import java.util.List;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
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
 * iterator-based read path ({@link PartitionRangeReadCommand#allDataRead}). Both producers feed the
 * same {@link ArrowRowAssembler}, so output is identical regardless of which one ran.
 * <p>
 * <b>PoC limitation:</b> this always scans the table's entire local primary range in one pass - no
 * token-range splitting (the {@code StatefulCursor.positionAt}/{@code setEndBound} primitive this
 * would use already exists, from the prep branch, but is intentionally not wired in here) and no
 * server-side filter pushdown; see {@code ARROW-FLIGHT.md} for the full production design.
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
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.ARROW_FLIGHT_SCAN);

        try (ColumnFamilyStore.RefViewFragment view = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            TableMetadata metadata = cfs.metadata();
            try (ArrowRowAssembler assembler = new ArrowRowAssembler(metadata, allocator, targetBatchBytes, onBatch))
            {
                if (CursorCompactor.isCursorReadSupported(view.sstables, metadata))
                    scanViaCursor(cfs, view.sstables, assembler);
                else
                    scanViaIterator(cfs, metadata, assembler);
            }
        }
    }

    private static void scanViaCursor(ColumnFamilyStore cfs, List<SSTableReader> sstables, ArrowRowAssembler assembler)
    {
        if (sstables.isEmpty())
            return;

        long nowInSec = assembler.nowInSec();
        logger.debug("Arrow Flight scan of {}.{} requesting direct I/O for {} sstable(s) (falls back silently per-sstable " +
                     "where unsupported - see the class javadoc)", cfs.keyspace.getName(), cfs.name, sstables.size());
        try (CompactionController controller = new CompactionController(cfs, ImmutableSet.copyOf(sstables), cfs.gcBefore(nowInSec)))
        {
            CursorCompactor compactor = new CursorCompactor(OperationType.VALIDATION, sstables, assembler, controller, nowInSec,
                                                             nextTimeUUID(), DiskAccessMode.direct);
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

    private static void scanViaIterator(ColumnFamilyStore cfs, TableMetadata metadata, ArrowRowAssembler assembler)
    {
        long nowInSec = assembler.nowInSec();
        boolean enforceStrictLiveness = metadata.enforceStrictLiveness();
        PartitionRangeReadCommand command = PartitionRangeReadCommand.allDataRead(metadata, nowInSec);
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
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.ARROW_FLIGHT_SCAN);
        try (ColumnFamilyStore.RefViewFragment ignored = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            TableMetadata metadata = cfs.metadata();
            try (ArrowRowAssembler assembler = new ArrowRowAssembler(metadata, allocator, targetBatchBytes, onBatch))
            {
                scanViaIterator(cfs, metadata, assembler);
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
