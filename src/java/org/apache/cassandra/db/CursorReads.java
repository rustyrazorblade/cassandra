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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.ResponseWireWriter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.bti.BtiCursorSeekSupport;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Cursor-based single-partition read path, flag-gated and off by default
 * ({@code cassandra.cursor_reads_enabled} / {@code Config.cursor_reads_enabled}).
 *
 * Serves the sstable legs of a supported single-partition read through {@link SSTableCursorReader}
 * instead of {@code SSTableIterator}, materializing the result into ordinary {@code Row}/{@code Cell}
 * objects at the {@link UnfilteredRowIterator} boundary.  Memtable legs stay on the object-based
 * path unless they join a cursor-level merge.
 *
 * On BTI sstables a single-slice read seeks the cursor to the row-index block that holds the slice
 * start and stops materializing at the slice end; BIG format walks the whole partition.  Multiple
 * sstable and memtable legs can merge at the cursor level, with row- and partition-level filter
 * expressions and the query limit pushed into the merge.
 *
 * See {@link #isReadSupported} for the support gate; anything outside it takes the iterator path
 * with no behavior change.
 */
public final class CursorReads
{
    private static final Logger logger = LoggerFactory.getLogger(CursorReads.class);

    private CursorReads()
    {
    }

    /** Sstable legs served by a cursor. */
    private static final AtomicLong SSTABLE_LEGS_SERVED = new AtomicLong();
    /** Gated-in legs where the sstable turned out not to contain the partition (no cursor opened). */
    private static final AtomicLong SSTABLE_LEGS_WITHOUT_PARTITION = new AtomicLong();
    /** Sstable legs where the BTI row index produced a forward seek into the partition.  Counts
     *  merged-mode legs too: a merged read over N indexed BTI legs with a slice start advances
     *  this by N. */
    private static final AtomicLong SSTABLE_LEG_ROW_INDEX_SEEKS = new AtomicLong();
    /** Unfiltereds (rows and range tombstone markers, static row excluded) materialized by cursor
     *  legs.  For a merged read this counts the merged output, not the per-leg sum. */
    private static final AtomicLong UNFILTEREDS_MATERIALIZED = new AtomicLong();
    /** Multi-leg reads served by the cursor-level merge (>= 2 legs through {@link #mergeLegs}). */
    private static final AtomicLong CURSOR_MERGES_SERVED = new AtomicLong();
    /** Sstable legs that entered a cursor-level merge (each such leg is also counted in
     *  {@link #sstableLegsServed}). */
    private static final AtomicLong SSTABLE_LEGS_CURSOR_MERGED = new AtomicLong();
    /** Memtable legs that joined a cursor-level merge through the {@link MemtableMergeLeg} adapter. */
    private static final AtomicLong MEMTABLE_LEGS_CURSOR_MERGED = new AtomicLong();
    /** Merged rows emitted by reusing the memtable's own live {@code Row} object (single-version
     *  fast path). */
    private static final AtomicLong MEMTABLE_ROWS_REUSED = new AtomicLong();
    /** Merged cells emitted by reusing the memtable's own live {@code Cell} object. */
    private static final AtomicLong MEMTABLE_CELLS_REUSED = new AtomicLong();
    /** Cursor merges whose production was bounded by the query limit. */
    private static final AtomicLong MERGES_STOPPED_BY_LIMIT = new AtomicLong();
    /** Cursor merges that ran with a RowFilter pushdown context attached. */
    private static final AtomicLong FILTER_PUSHDOWNS_ENGAGED = new AtomicLong();
    /** Merged partitions skipped entirely because a partition-level filter expression failed. */
    private static final AtomicLong PARTITIONS_SKIPPED_BY_FILTER = new AtomicLong();
    /** Merged row groups dropped at production because a row-level filter expression failed
     *  (a clustering-column expression against the descriptor bytes, or a regular-column
     *  expression at winner resolution). */
    private static final AtomicLong ROWS_DROPPED_BY_FILTER = new AtomicLong();
    /** The subset of {@link #ROWS_DROPPED_BY_FILTER} dropped because a regular-column expression
     *  failed at winner resolution. */
    private static final AtomicLong ROWS_DROPPED_BY_REGULAR_FILTER = new AtomicLong();
    /** Sstable-winner cell values materialized into a {@code byte[]}/{@code Cell}.  A streaming
     *  sink leaves this at zero for its sstable-won cells; a non-streaming sink advances it once
     *  per sstable-won cell. */
    private static final AtomicLong SSTABLE_CELL_VALUES_MATERIALIZED = new AtomicLong();

    static void countSstableCellValueMaterialized()
    {
        SSTABLE_CELL_VALUES_MATERIALIZED.incrementAndGet();
    }

    @VisibleForTesting
    public static long sstableCellValuesMaterialized()
    {
        return SSTABLE_CELL_VALUES_MATERIALIZED.get();
    }

    /** {@code ReadResponse}s served via the transcode fast path from a replica-serving call site,
     *  counted only after the merge has run. */
    private static final AtomicLong TRANSCODE_RESPONSES_SERVED = new AtomicLong();

    static void countTranscodeResponseServed()
    {
        TRANSCODE_RESPONSES_SERVED.incrementAndGet();
    }

    @VisibleForTesting
    public static long transcodeResponsesServed()
    {
        return TRANSCODE_RESPONSES_SERVED.get();
    }

    public static long sstableLegsServed()
    {
        return SSTABLE_LEGS_SERVED.get();
    }

    public static long cursorMergesServed()
    {
        return CURSOR_MERGES_SERVED.get();
    }

    public static long sstableLegsCursorMerged()
    {
        return SSTABLE_LEGS_CURSOR_MERGED.get();
    }

    public static long memtableLegsCursorMerged()
    {
        return MEMTABLE_LEGS_CURSOR_MERGED.get();
    }

    public static long memtableRowsReused()
    {
        return MEMTABLE_ROWS_REUSED.get();
    }

    public static long memtableCellsReused()
    {
        return MEMTABLE_CELLS_REUSED.get();
    }

    public static long mergesStoppedByLimit()
    {
        return MERGES_STOPPED_BY_LIMIT.get();
    }

    public static long filterPushdownEngaged()
    {
        return FILTER_PUSHDOWNS_ENGAGED.get();
    }

    public static long partitionsSkippedByFilter()
    {
        return PARTITIONS_SKIPPED_BY_FILTER.get();
    }

    public static long rowsDroppedByFilter()
    {
        return ROWS_DROPPED_BY_FILTER.get();
    }

    public static long rowsDroppedByRegularColumnFilter()
    {
        return ROWS_DROPPED_BY_REGULAR_FILTER.get();
    }

    /** Counter hooks for {@code CursorReadMerger}'s escape-hatch emissions. */
    static void countMemtableRowReuse()
    {
        MEMTABLE_ROWS_REUSED.incrementAndGet();
    }

    static void countMemtableCellReuse()
    {
        MEMTABLE_CELLS_REUSED.incrementAndGet();
    }

    public static long sstableLegsWithoutPartition()
    {
        return SSTABLE_LEGS_WITHOUT_PARTITION.get();
    }

    public static long sstableLegRowIndexSeeks()
    {
        return SSTABLE_LEG_ROW_INDEX_SEEKS.get();
    }

    public static long unfilteredsMaterialized()
    {
        return UNFILTEREDS_MATERIALIZED.get();
    }

    /** Test only: corrupts materialized cell timestamps (+1) so the differential harness can prove
     *  it detects a broken cursor path.  Applies on both the per-leg materialization and the merge
     *  sink.  Never set outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_CORRUPT_CELL_TIMESTAMPS = false;

    /** Test only: inverts the cell reconciliation verdict inside the cursor-level merge, so the
     *  differential harness can prove it detects a wrong merge decision.  Never set outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_CORRUPT_MERGE_DECISIONS = false;

    /** Test only: drops every merged leg's row-index open-marker seed (the seek still happens), so
     *  the differential harness can prove it catches a missing seed.  Never set outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_DROP_MERGE_SEEK_OPEN_MARKER = false;

    /** Test only: skews every merged leg's open-marker seed value (+1), so the harness can prove
     *  it catches a wrong seed value.  Never set outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_SKEW_MERGE_SEEK_OPEN_MARKER = false;

    /** Test only: skews every timestamp the memtable adapter presents to the merge (+1), so the
     *  harness can prove it catches a wrong merge decision involving a memtable leg.  Never set
     *  outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS = false;

    /** Test only: forces the memtable whole-row reuse fast path even when the active deletion is
     *  not live, so the harness can prove it catches a wrong reuse.  Never set outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_FORCE_MEMTABLE_ROW_REUSE = false;

    /** Test only: skips the tombstone contributions of filter-dropped row groups in the scan-stats
     *  accumulator, so the scan-metrics parity harness can prove it catches broken dropped-row
     *  accounting.  Never set outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_SKEW_DROPPED_ROW_ACCOUNTING = false;

    /** Test only: skews the row liveness timestamp {@link TranscodeMergeSink} hands
     *  {@link ResponseWireWriter} (+1), so the byte-comparison harness can prove it catches a wrong
     *  encoded value.  Never set outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_TRANSCODE_SKEW_TIMESTAMP = false;

    /** Test only: reports a non-live row deletion as live to {@link ResponseWireWriter#startRow},
     *  dropping the {@code HAS_DELETION} bit, so the harness can prove it catches a wrong flag.
     *  Never set outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_TRANSCODE_WRONG_FLAGS = false;

    /** Test only: flips the low bit of the first byte of a sstable-winner cell value when
     *  {@link TranscodeMergeSink} streams it, so the harness can prove it catches a wrong value on
     *  the streaming path.  Never set outside tests. */
    @VisibleForTesting
    public static volatile boolean TEST_CORRUPT_STREAMED_CELL_VALUE = false;

    /**
     * Support gate, mirroring {@link CursorCompactor#isSupported}: returns true only for the queries
     * this path serves.  Everything else falls back to the iterator path before any cursor state is
     * created.
     *
     * Inherited from cursor compaction ({@link CursorCompactor#unsupportedMetadata}): no secondary
     * indexes, partitioner must support reusable keys, no Accord keyspace.  On top:
     * <ul>
     *   <li>flag off (default) → false</li>
     *   <li>single-partition slice reads only ({@link ClusteringIndexSliceFilter}); names filters
     *       are out of scope</li>
     *   <li>ascending order only (the cursor is forward-only)</li>
     *   <li>full-partition or a single slice; multi-slice filters fall back</li>
     *   <li>no row cache on the table (a cursor result cannot populate {@code CachedBTreePartition}
     *       without materializing anyway)</li>
     *   <li>no counter tables: {@code copyCellValue} streams raw context bytes and does not apply
     *       {@code DeserializationHelper.maybeClearCounterValue}'s local-shard clearing</li>
     *   <li>no materialized views: legacy shadowable row deletions are rejected mid-read by
     *       {@link SSTableCursorReader}</li>
     *   <li>all candidate sstables at the latest format version, none carrying dropped
     *       complex/counter ghost header columns</li>
     *   <li>at least one sstable leg; memtable-only reads stay on the object path</li>
     * </ul>
     */
    public static boolean isReadSupported(SinglePartitionReadCommand command,
                                          ColumnFamilyStore cfs,
                                          List<SSTableReader> sstables)
    {
        if (!DatabaseDescriptor.cursorReadsEnabled())
            return false;

        TableMetadata metadata = cfs.metadata();
        if (CursorCompactor.unsupportedMetadata(metadata))
            return false;
        if (metadata.isCounter() || metadata.isView())
            return false;
        if (cfs.isRowCacheEnabled())
            return false;

        ClusteringIndexFilter filter = command.clusteringIndexFilter();
        if (!(filter instanceof ClusteringIndexSliceFilter))
            return false;
        if (filter.isReversed())
            return false;
        Slices slices = filter.getSlices(metadata);
        if (slices.size() > 1)
            return false;

        if (sstables.isEmpty())
            return false;
        for (SSTableReader sstable : sstables)
        {
            if (!sstable.descriptor.version.isLatestVersion())
                return false;
            if (CursorCompactor.unsupportedHeaderColumns(metadata, sstable))
                return false;
        }
        return true;
    }

    /**
     * Cursor-served replacement for {@code sstable.rowIterator(...)}.  Must only be called for
     * commands that passed {@link #isReadSupported}.  This is the single-leg composition of
     * {@link #openLeg} and {@link #completeSingleLeg}; multi-leg reads route through
     * {@link #mergeLegs}.
     */
    public static UnfilteredRowIterator sstableRowIterator(SSTableReader sstable,
                                                           TableMetadata metadata,
                                                           DecoratedKey key,
                                                           Slices slices,
                                                           ColumnFilter columnFilter,
                                                           SSTableReadsListener listener)
    {
        PendingLeg leg = openLeg(sstable, metadata, key, slices, columnFilter, listener, new ValueTransfer());
        if (leg == null)
            // mirrors BigTableReader/BtiTableReader.rowIterator with a null index entry
            return absentPartitionIterator(metadata, key, sstable);
        return completeSingleLeg(leg);
    }

    /**
     * Substitute for a gated-in sstable leg whose sstable does not contain the queried partition:
     * an empty iterator that still reports the sstable's {@link EncodingStats}, matching the
     * iterator path's own substitute.  The merged response's stats header is
     * {@code EncodingStats.merge} over every leg, so an empty leg must still contribute real stats;
     * reporting {@code NO_STATS} diverges the header (timestamp deltas are header-relative) whenever
     * another source produces the partition.
     */
    public static UnfilteredRowIterator absentPartitionIterator(TableMetadata metadata, DecoratedKey key, SSTableReader sstable)
    {
        UnfilteredRowIterator empty = UnfilteredRowIterators.noRowsIterator(metadata, key, Rows.EMPTY_STATIC_ROW,
                                                                            DeletionTime.LIVE, false);
        return new WrappingUnfilteredRowIterator()
        {
            @Override
            public UnfilteredRowIterator wrapped()
            {
                return empty;
            }

            @Override
            public EncodingStats stats()
            {
                return sstable.stats();
            }
        };
    }

    /**
     * Leg-open phase: performs the partition lookup (with the same listener/metrics notifications
     * the iterator path makes), opens a cursor, reads the partition header and materializes the
     * static row, without materializing any rows.  Rows are produced later by
     * {@link #completeSingleLeg}, or by {@link #mergeLegs} for a multi-leg read.  Legs with empty
     * slices ({@code Slices.NONE}) have their cursor closed before this returns.
     *
     * @param transfer the query's shared {@link ValueTransfer} scratch, one instance per read, so
     *                 a merged query's legs share one transfer buffer instead of allocating one each
     * @return the pending leg, or null when the sstable does not contain the partition (counted in
     *         {@link #sstableLegsWithoutPartition()}; no cursor is opened)
     */
    public static PendingLeg openLeg(SSTableReader sstable,
                                     TableMetadata metadata,
                                     DecoratedKey key,
                                     Slices slices,
                                     ColumnFilter columnFilter,
                                     SSTableReadsListener listener,
                                     ValueTransfer transfer)
    {
        long position;
        BtiCursorSeekSupport.PartitionEntry btiEntry = null;
        if (sstable instanceof BtiTableReader)
        {
            BtiTableReader bti = (BtiTableReader) sstable;
            // Same lookup BtiTableReader.rowIterator performs, but keeps the index entry so the
            // row index is usable for the seek.
            btiEntry = BtiCursorSeekSupport.exactPartitionEntry(bti, key, listener);
            if (btiEntry == null)
            {
                SSTABLE_LEGS_WITHOUT_PARTITION.incrementAndGet();
                return null;
            }
            position = btiEntry.dataPosition;
        }
        else
        {
            // Same lookup the iterator path performs: notifies the listener (metrics parity) and
            // updates the key cache / bloom filter stats identically.
            position = sstable.getPosition(key, SSTableReader.Operator.EQ, listener);
            if (position < 0)
            {
                SSTABLE_LEGS_WITHOUT_PARTITION.incrementAndGet();
                return null;
            }
        }

        PendingLeg leg = new PendingLeg(sstable, metadata, key, slices, columnFilter, btiEntry, transfer);
        try
        {
            leg.open(position);
        }
        catch (RuntimeException | Error e)
        {
            leg.close();
            throw e;
        }
        catch (IOException e)
        {
            leg.close();
            // SSTableCursorReader wraps corruption in CorruptSSTableException internally; anything
            // surfacing here is unexpected IO.
            throw new RuntimeException("cursor read failed for " + sstable, e);
        }
        SSTABLE_LEGS_SERVED.incrementAndGet();
        return leg;
    }

    /**
     * Finishes a single pending leg: BTI row-index seek and slice-end stop when applicable, bounded
     * materialization, cursor closed, result wrapped in the slice-applying iterator.  Byte-identical
     * to the {@link #sstableRowIterator} output; the merge core engages only for {@code >= 2} legs.
     */
    public static UnfilteredRowIterator completeSingleLeg(PendingLeg leg)
    {
        MaterializedPartition materialized;
        try
        {
            materialized = leg.completeSingle();
        }
        catch (RuntimeException | Error e)
        {
            leg.close();
            throw e;
        }
        catch (IOException e)
        {
            leg.close();
            throw new RuntimeException("cursor read failed for " + leg.sstable, e);
        }
        leg.close();
        return new SlicedMaterializedIterator(leg.metadata, leg.key, leg.sstable, leg.sstable.stats(),
                                              leg.columnFilter, leg.legSlices, materialized,
                                              true); // single leg: this iterator is the leg's validation surface
    }

    /**
     * Engagement gate for the cursor merge's production bound: returns a fresh detached counter that
     * matches the counter {@code ReadCommand.executeLocally} applies above the merge, or null when
     * the bound must not engage and production stays unbounded.  Each decline is conservative: the
     * bound's one failure mode is under-production, so any query shape whose top-of-stack counting
     * this cannot reproduce exactly falls back to unbounded:
     * <ul>
     *   <li>only CQL_LIMIT / CQL_PAGING_LIMIT kinds; GROUP BY counters count group boundaries, which
     *       this does not reproduce;</li>
     *   <li>not for unlimited limits ({@code DataLimits.NONE} lands here);</li>
     *   <li>a non-empty {@code RowFilter} does not block outright: the filter sits below the counter
     *       and drops rows before they are counted, so a filter-blind bound would stop while the
     *       counter still wants rows, unless every dropped row is also one {@link #filterPushdownFor}
     *       pushes into the merge.  Declined whenever {@link #filterPushdownFor} would decline (any
     *       unpushable expression), since then production-side filtering does not match the top
     *       counter's population.  See {@link RowLevelFilterProbe} and {@link LimitingMergeSink} for
     *       how the two bounds compose once both engage;</li>
     *   <li>not under MV strict liveness (defensive: MV tables are gated out of cursor reads).</li>
     * </ul>
     *
     * Caller contract: pass the returned counter into {@link #mergeLegs} only for a read whose merged
     * iterator is consumed by {@code executeLocally}'s stack.  The other {@code queryMemtableAndDisk}
     * entry points return unlimited partition contents and must pass null, or a bounded merge would
     * under-produce.
     */
    static DataLimits.Counter limitBoundFor(SinglePartitionReadCommand command)
    {
        DataLimits limits = command.limits();
        if (!isBoundableLimitKind(limits.kind()))
            return null;
        if (limits.isUnlimited())
            return null;
        // A non-empty filter only blocks the bound when it is not fully pushable; filterPushdownFor
        // decides whether production-side filtering keeps the merge's row count equal to what
        // survives to the top counter.  It never calls back into limitBoundFor, so there is no
        // recursion.
        if (!command.rowFilter().isEmpty() && filterPushdownFor(command) == null)
            return null;
        if (command.metadata().enforceStrictLiveness())
            return null;
        return limits.newCounter(command.nowInSec(), false, command.selectsFullPartition(),
                                 command.metadata().enforceStrictLiveness());
    }

    /** Whether {@code kind} is a limit shape {@link #limitBoundFor} engages for (CQL_LIMIT or
     *  CQL_PAGING_LIMIT; GROUP BY counters count group boundaries and are excluded).  Shared by
     *  {@link #limitBoundFor} and {@link FilterPushdown}'s row-level eligibility so the two cannot
     *  drift. */
    private static boolean isBoundableLimitKind(DataLimits.Kind kind)
    {
        return kind == DataLimits.Kind.CQL_LIMIT || kind == DataLimits.Kind.CQL_PAGING_LIMIT;
    }

    /**
     * Engagement gate for RowFilter pushdown into the cursor merge: returns a pushdown context for
     * {@link #mergeLegs}, or null when pushdown must not engage.  When it declines, the query is
     * still served by the cursor path, with filtering staying at the top-of-stack
     * {@code rowFilter().filter}.  The top-level filter stays authoritative either way; the pushdown
     * is only a production bound, so its one failure mode is under-production, which the differential
     * harness catches as a byte divergence.
     *
     * All-or-nothing per query: if any expression is unpushable the whole gate disengages.  Engages
     * only when:
     * <ul>
     *   <li>the filter is non-empty, needs no coordinator reconciliation and is strict
     *       ({@code needsReconciliation()} changes the purge-before-evaluate semantics, and a
     *       non-strict filter means a coordinator-side intersection-to-union downgrade; neither is
     *       reproduced below the merge);</li>
     *   <li>every expression is a {@code RowFilter.SimpleExpression} on a non-complex, non-counter
     *       column;</li>
     *   <li>query-size tracking is not active for this command and purgeable-tombstone recording is
     *       disabled (both walk the object stream below the top filter);</li>
     *   <li>caller contract, as {@code limitBoundFor}'s: the context may reach {@link #mergeLegs}
     *       only for a read whose merged iterator is consumed by {@code executeLocally}'s stack; the
     *       other entry points must pass null.</li>
     * </ul>
     *
     * The partition-level short-circuit ({@link FilterPushdown#partitionLevelMatches}) is always
     * active once the gate engages.  Clustering- and regular-column expressions additionally drop
     * rows at production, cooperating with {@code limitBoundFor}'s bound rather than requiring its
     * absence; see {@link FilterPushdown#rowLevelPushdownEligible}.
     */
    static FilterPushdown filterPushdownFor(SinglePartitionReadCommand command)
    {
        RowFilter rowFilter = command.rowFilter();
        if (rowFilter.isEmpty())
            return null;
        if (rowFilter.needsReconciliation() || !rowFilter.isStrict())
            return null;
        if (command.metadata().enforceStrictLiveness())
            return null; // MV tables are outside isReadSupported entirely; purely defensive, like limitBoundFor's check
        for (RowFilter.Expression e : rowFilter.getExpressions())
        {
            // exact-class check for Kind.SIMPLE (Kind is not visible outside the filter package);
            // any other subclass, including a future one, must disengage
            if (e.getClass() != RowFilter.SimpleExpression.class)
                return null;
            ColumnMetadata column = e.column();
            if (column.isComplex() || column.type.isCounter())
                return null;
            // Clustering and regular expressions are evaluated against operator.isSatisfiedBy, so
            // only operator families that reduce to isSatisfiedBy(column.type, foundValue, value)
            // may engage: the column-value and frozen-collection CONTAINS families.  Anything else
            // disengages the whole gate.  Static and partition-key columns are exempt; partition-
            // level evaluation reuses the real isSatisfiedBy.
            if ((column.kind == ColumnMetadata.Kind.CLUSTERING || column.kind == ColumnMetadata.Kind.REGULAR)
                && !(e.operator().appliesToColumnValues()
                     || e.operator().appliesToCollectionElements()
                     || e.operator().appliesToMapKeys()))
                return null;
        }
        if (querySizeTrackingActive(command))
            return null;
        if (DatabaseDescriptor.getPurgeableTobmstonesMetricGranularity() != Config.TombstonesMetricGranularity.disabled)
            return null;
        return new FilterPushdown(command, rowFilter.getExpressions());
    }

    /** True iff the size-tracking transformation would engage for this command's
     *  {@code executeLocally} run.  Query-size tracking walks the object stream the transcode path
     *  never builds, so a command with tracking active must decline. */
    static boolean querySizeTrackingActive(SinglePartitionReadCommand command)
    {
        return command.isTrackingWarnings()
               && !SchemaConstants.isSystemKeyspace(command.metadata().keyspace)
               && (DatabaseDescriptor.getLocalReadSizeWarnThreshold() != null
                   || DatabaseDescriptor.getLocalReadSizeFailThreshold() != null);
    }

    /**
     * The per-query filter pushdown context {@link #filterPushdownFor} builds for
     * {@link #mergeLegs}.  Holds the gate-approved expressions split as {@code RowFilter.filter}
     * splits them: partition-level expressions (static or partition-key columns) are evaluated
     * before any row-group work.
     */
    static final class FilterPushdown
    {
        private final TableMetadata metadata;
        private final List<RowFilter.Expression> partitionLevelExpressions;
        /** The clustering-column expressions, evaluated at row-group formation. */
        private final List<RowFilter.Expression> clusteringExpressions;
        /** The regular-column expressions, evaluated at winner resolution inside the cell walk. */
        private final List<RowFilter.Expression> regularExpressions;
        /**
         * Whether row-level pushdown may actually drop rows for this query.  Requires row-level
         * (clustering- or regular-column) expressions and either an unlimited query or a production
         * LIMIT bound that will attach for it ({@code isBoundableLimitKind(limits().kind())}).  Both
         * arms exist for the same reason: the top-level {@code DataLimits} counter sits above
         * {@code withMetricsRecording}, so a limited query stops scanning and metric-counting once
         * the limit is satisfied.  An eager merge that dropped and accounted the whole slice would
         * over-count dropped rows the iterator path never scans, and could abort on tombstones it
         * never reaches.  {@code mergeLegs} wires {@link RowLevelFilterProbe} together with
         * {@link LimitingMergeSink} so the same bound that stops the top counter also stops the merge.
         * The "will attach" check does not re-derive filter pushability or call back into
         * {@code limitBoundFor}/{@code filterPushdownFor}: this constructor runs only after
         * {@link #filterPushdownFor}'s gate already passed for this command, so the only residual
         * question is the limit's shape.  This is not part of the gate itself: the context still
         * attaches for the partition-level short-circuit, which is consumption-independent.
         */
        private final boolean rowLevelPushdownEligible;
        private final long nowInSec;
        /** Set by {@link #activateScanAccounting}; null until then (and always null when
         *  {@link #rowLevelPushdownEligible} is false). */
        private ScanStatsAccumulator scanStats;

        private FilterPushdown(SinglePartitionReadCommand command, List<RowFilter.Expression> expressions)
        {
            this.metadata = command.metadata();
            // same split as RowFilter.filter: static or partition-key columns are partition-level,
            // clustering columns engage at group formation, regular columns at winner resolution
            List<RowFilter.Expression> partitionLevel = new ArrayList<>(expressions.size());
            List<RowFilter.Expression> clustering = new ArrayList<>(expressions.size());
            List<RowFilter.Expression> regular = new ArrayList<>(expressions.size());
            for (RowFilter.Expression e : expressions)
            {
                if (e.column().isStatic() || e.column().isPartitionKey())
                    partitionLevel.add(e);
                else if (e.column().kind == ColumnMetadata.Kind.CLUSTERING)
                    clustering.add(e);
                else
                    regular.add(e);
            }
            this.partitionLevelExpressions = partitionLevel;
            this.clusteringExpressions = clustering;
            this.regularExpressions = regular;
            // eligible when unlimited, or when a production LIMIT bound will attach
            // (enforceStrictLiveness is already known false, so isBoundableLimitKind is the only
            // residual question); see the field javadoc.
            DataLimits limits = command.limits();
            this.rowLevelPushdownEligible = (!clustering.isEmpty() || !regular.isEmpty())
                                            && (limits.isUnlimited() || isBoundableLimitKind(limits.kind()));
            this.nowInSec = command.nowInSec();
        }

        /**
         * Creates and attaches the per-execution {@link ScanStatsAccumulator} when row-level
         * pushdown is eligible.  Only merged reads reach this; single-leg reads never evaluate
         * row-level expressions.  The accumulator lands on the {@link ReadExecutionController} so
         * {@code ReadCommand.withMetricsRecording} can fold the dropped-row contributions into its
         * totals.
         */
        void activateScanAccounting(ReadExecutionController controller, ColumnFamilyStore cfs,
                                    SinglePartitionReadCommand command)
        {
            if (!rowLevelPushdownEligible)
                return;
            scanStats = new ScanStatsAccumulator(command, cfs, controller);
            controller.attachScanStats(scanStats);
        }

        ScanStatsAccumulator scanStats()
        {
            return scanStats;
        }

        List<RowFilter.Expression> clusteringExpressions()
        {
            return clusteringExpressions;
        }

        List<RowFilter.Expression> regularExpressions()
        {
            return regularExpressions;
        }

        TableMetadata tableMetadata()
        {
            return metadata;
        }

        long queryNowInSec()
        {
            return nowInSec;
        }

        /**
         * The cursor form of {@code RowFilter.filter}'s partition short-circuit: every
         * partition-level expression evaluated through its real {@code isSatisfiedBy} against the
         * partition key and the merged static row.  The pre-purge verdict cannot differ from the
         * top filter's post-purge verdict, since expression evaluation only consults values live at
         * {@code nowInSec} and the purge stage only drops what is not live then.
         *
         * @return false when the whole partition fails the top-level filter; the caller then skips
         *         the row-group merge and emits the empty-with-static-row shape
         */
        boolean partitionLevelMatches(DecoratedKey key, Row mergedStatic)
        {
            for (int i = 0; i < partitionLevelExpressions.size(); i++)
            {
                if (!partitionLevelExpressions.get(i).isSatisfiedBy(metadata, key, mergedStatic, nowInSec))
                    return false;
            }
            return true;
        }
    }

    /**
     * The per-execution scan-stats accounting for {@code ReadCommand.withMetricsRecording}, for rows
     * the cursor merge drops at production.  On the iterator path those rows flow through
     * {@code withoutPurgeableTombstones} and then {@code MetricRecording} before the top filter
     * discards them, feeding the tombstone/live-row histograms, {@code totalRowsRead}, the top-K
     * samplers, the warn threshold and the {@link TombstoneOverwhelmingException} abort.  A merge
     * that drops those rows must reproduce that accounting or it under-reports.
     *
     * Three coupled pieces, each reproducing a stage of the real stack:
     * <ul>
     *   <li>the gcable purge ({@link #shouldPurge(long, long)}/{@link #cellPurged}): classifies a
     *       dropped row's contributions on what survives the identical purge predicate (same
     *       {@code nowInSec}, {@code gcBefore}, {@code onlyPurgeRepairedTombstones} and expired-cell
     *       conversion);</li>
     *   <li>the dropped-row classification ({@link #beginDroppedRow} ... {@link #endDroppedRow}):
     *       {@code MetricRecording.applyToRow}'s three-way logic on post-purge content, keeping cell
     *       liveness ({@code Cell.isLive}) and row liveness ({@code LivenessInfo.isLive}) apart;</li>
     *   <li>the production-time abort ({@link #productionAbort}): the merge is eager, so it maintains
     *       a combined tombstone/live-row count over the surface {@code MetricRecording} would scan
     *       and aborts at the same element with the same side effects (Tracing message, metric
     *       increment, exception message, at-abort recordings).</li>
     * </ul>
     *
     * Engagement requires a fully-consumed query (see
     * {@code FilterPushdown.rowLevelPushdownEligible}) so the eager combined count equals what the
     * pull side would have scanned.
     */
    static final class ScanStatsAccumulator
    {
        private final SinglePartitionReadCommand command;
        private final TableMetrics metric;
        private final ReadExecutionController controller;
        private final long nowInSec;
        private final long gcBefore;
        private final boolean onlyPurgeRepairedTombstones;
        private final boolean purgeEnabled;
        private final int failureThreshold;
        private final int warningThreshold;
        private final boolean respectTombstoneThresholds;
        private final long queryStartNanos;

        // dropped-row totals folded into MetricRecording (pull side)
        private int droppedLiveRows;
        private int droppedTombstones;
        private int sampledDroppedLiveRows;
        private int sampledDroppedTombstones;

        // production-order combined counts driving the abort (statics + emitted surface + dropped
        // rows, counted in the order MetricRecording would scan them)
        private int combinedLiveRows;
        private int combinedTombstones;

        // streaming classification state for the dropped row currently being walked; the clustering
        // carrier is either the source leg (metadata-only walks, materialized lazily on the abort
        // path) or an already-materialized clustering (abandoned rows), never both
        private MergeLeg droppedClusteringSource;
        private Clustering<?> droppedClustering;
        private boolean droppedOnSurface;
        private boolean droppedPkLive;
        private boolean droppedHasDeletion;
        private boolean droppedAnyLiveCell;
        private int droppedDeadCells;

        ScanStatsAccumulator(SinglePartitionReadCommand command, ColumnFamilyStore cfs,
                             ReadExecutionController controller)
        {
            this.command = command;
            this.metric = cfs.metric;
            this.controller = controller;
            this.nowInSec = command.nowInSec();
            this.purgeEnabled = nowInSec != 0; // withoutPurgeableTombstones is a no-op at nowInSec == 0
            this.gcBefore = purgeEnabled ? cfs.gcBefore(nowInSec) : Long.MIN_VALUE;
            this.onlyPurgeRepairedTombstones = cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones();
            this.failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            this.warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();
            this.respectTombstoneThresholds = !SchemaConstants.isLocalSystemKeyspace(command.metadata().keyspace);
            this.queryStartNanos = nanoTime();
        }

        // ---- MetricRecording consumption (pull side) ----

        int droppedLiveRows()
        {
            return droppedLiveRows;
        }

        int droppedTombstones()
        {
            return droppedTombstones;
        }

        /** Fold-once accessor for {@code MetricRecording.onPartitionClose}'s samplers: returns the
         *  dropped live rows not yet folded into a partition sample and marks them folded. */
        int unsampledDroppedLiveRows()
        {
            int unsampled = droppedLiveRows - sampledDroppedLiveRows;
            sampledDroppedLiveRows = droppedLiveRows;
            return unsampled;
        }

        int unsampledDroppedTombstones()
        {
            int unsampled = droppedTombstones - sampledDroppedTombstones;
            sampledDroppedTombstones = droppedTombstones;
            return unsampled;
        }

        // ---- gcable purge (withoutPurgeableTombstones' predicate) ----

        private boolean shouldPurge(long timestamp, long localDeletionTime)
        {
            // PurgeFunction's purger with WithoutPurgeableTombstones' inputs: constant-true
            // evaluator, no gc-grace ignoring on the read path
            return purgeEnabled
                   && !(onlyPurgeRepairedTombstones && localDeletionTime >= controller.oldestUnrepairedTombstone())
                   && localDeletionTime < gcBefore;
        }

        private boolean shouldPurge(DeletionTime dt)
        {
            return !dt.isLive() && shouldPurge(dt.markedForDeleteAt(), dt.localDeletionTime());
        }

        /** The full cell-purge verdict, including {@code AbstractCell.purge}'s expired-cell-to-
         *  tombstone conversion: an expired expiring cell that survives the first test is converted
         *  to a tombstone at {@code localDeletionTime - ttl} and purge-tested again at that earlier
         *  time.  This matters when {@code onlyPurgeRepairedTombstones} makes the predicate
         *  non-monotonic in the deletion time. */
        private boolean cellPurged(long timestamp, long localDeletionTime, int ttl)
        {
            if (shouldPurge(timestamp, localDeletionTime))
                return true;
            return ttl != Cell.NO_TTL && shouldPurge(timestamp, localDeletionTime - ttl);
        }

        /** {@code Cell.isLive(nowInSec, localDeletionTime, ttl)}'s formula: cell liveness, distinct
         *  from {@code LivenessInfo.isLive}. */
        private boolean cellLive(long localDeletionTime, int ttl)
        {
            return localDeletionTime == Cell.NO_DELETION_TIME || (ttl != Cell.NO_TTL && nowInSec < localDeletionTime);
        }

        // ---- emitted-surface accounting (combined counts only; MetricRecording itself counts
        //      these on the pull side, so they never touch the dropped totals) ----

        /**
         * Classifies a merged, about-to-be-emitted row (or the merged static row) as
         * {@code MetricRecording.applyToRow} will after the purge stage, feeding only the combined
         * production-order counters.  Two passes: the first computes the post-purge
         * {@code hasDeletion(nowInSec)} gate, the second counts in MetricRecording's order
         * (surviving dead cells first, then the live-row / PK-deletion-only verdict).
         */
        void accountRow(Row row)
        {
            LivenessInfo pk = row.primaryKeyLivenessInfo();
            boolean pkLive = pk.isLive(nowInSec);
            boolean hasDeletion = false;
            if (!pk.isEmpty() && !pkLive
                && !shouldPurge(pk.timestamp(), pk.localExpirationTime())
                && pk.isExpiring() && nowInSec >= pk.localExpirationTime())
                hasDeletion = true;
            if (!row.deletion().isLive() && !shouldPurge(row.deletion().time()))
                hasDeletion = true;
            boolean anyLiveCell = false;
            for (Cell<?> cell : row.cells())
            {
                if (cellLive(cell.localDeletionTime(), cell.ttl()))
                    anyLiveCell = true;
                else if (!cellPurged(cell.timestamp(), cell.localDeletionTime(), cell.ttl()))
                    hasDeletion = true;
            }
            if (!hasDeletion)
            {
                for (ColumnData cd : row)
                {
                    if (cd.column().isComplex()
                        && !((ComplexColumnData) cd).complexDeletion().isLive()
                        && !shouldPurge(((ComplexColumnData) cd).complexDeletion()))
                    {
                        hasDeletion = true;
                        break;
                    }
                }
            }

            boolean hasTombstones = false;
            if (hasDeletion)
            {
                for (Cell<?> cell : row.cells())
                {
                    if (!cellLive(cell.localDeletionTime(), cell.ttl())
                        && !cellPurged(cell.timestamp(), cell.localDeletionTime(), cell.ttl()))
                    {
                        hasTombstones = true;
                        combinedTombstone(row.clustering());
                    }
                }
            }
            if (pkLive || anyLiveCell)
                ++combinedLiveRows;
            else if (!pkLive && hasDeletion && !hasTombstones)
                combinedTombstone(row.clustering());
        }

        /** An emitted range-tombstone marker, purge-tested: {@code PurgeFunction.applyToMarker}
         *  drops a bound whose deletion purges and a boundary both of whose deletions purge (one
         *  purged side degrades it to a bound: still one marker, still one tombstone). */
        void accountMarker(RangeTombstoneMarker marker)
        {
            boolean survives;
            if (marker.isBoundary())
            {
                RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                survives = !shouldPurge(boundary.closeDeletionTime(false)) || !shouldPurge(boundary.openDeletionTime(false));
            }
            else
            {
                survives = !shouldPurge(((RangeTombstoneBoundMarker) marker).deletionTime());
            }
            if (survives)
                combinedTombstone(marker.clustering());
        }

        /** An artificial slice-bound marker the slicer will synthesize (open at the slice start /
         *  close at the slice end when a range deletion covers the bound) — the iterator path's
         *  merged stream carries the identical artificial markers through the purge stage into
         *  MetricRecording. */
        void accountArtificialMarker(ClusteringBound<?> bound, DeletionTime openDeletion)
        {
            if (!shouldPurge(openDeletion))
                combinedTombstone(bound);
        }

        // ---- dropped-row streaming classification (fed by RowLevelFilterProbe) ----

        void beginDroppedRow(MergeLeg clusteringSource, boolean onEmittedSurface,
                             LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            droppedClusteringSource = clusteringSource;
            droppedClustering = null;
            beginDropped(onEmittedSurface, mergedLiveness, mergedRowDeletion);
        }

        /** Variant for abandoned rows whose clustering is already materialized (a started row, or a
         *  whole-row object): no leg descriptor needed on the abort path. */
        void beginDroppedRow(Clustering<?> clustering, boolean onEmittedSurface,
                             LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            droppedClusteringSource = null;
            droppedClustering = clustering;
            beginDropped(onEmittedSurface, mergedLiveness, mergedRowDeletion);
        }

        private void beginDropped(boolean onEmittedSurface, LivenessInfo mergedLiveness,
                                  DeletionTime mergedRowDeletion)
        {
            droppedOnSurface = onEmittedSurface;
            droppedPkLive = mergedLiveness != null && mergedLiveness.isLive(nowInSec);
            droppedHasDeletion = false;
            droppedAnyLiveCell = false;
            droppedDeadCells = 0;
            if (mergedLiveness != null && !droppedPkLive
                && !shouldPurge(mergedLiveness.timestamp(), mergedLiveness.localExpirationTime())
                && mergedLiveness.isExpiring() && nowInSec >= mergedLiveness.localExpirationTime())
                droppedHasDeletion = true;
            if (!mergedRowDeletion.isLive() && !shouldPurge(mergedRowDeletion))
                droppedHasDeletion = true;
        }

        void droppedComplexDeletion(DeletionTime complexDeletion)
        {
            if (!complexDeletion.isLive() && !shouldPurge(complexDeletion))
                droppedHasDeletion = true;
        }

        void droppedCell(CellLivenessInfo winnerLiveness)
        {
            droppedCell(winnerLiveness.timestamp(), winnerLiveness.localDeletionTime(), winnerLiveness.ttl());
        }

        /** Primitive variant, fed by the abandoned-row replay of already-materialized cells (and by
         *  the whole-row walk): the same classification as the LivenessInfo form. */
        void droppedCell(long timestamp, long localDeletionTime, int ttl)
        {
            if (cellLive(localDeletionTime, ttl))
                droppedAnyLiveCell = true;
            else if (!cellPurged(timestamp, localDeletionTime, ttl))
            {
                ++droppedDeadCells;
                droppedHasDeletion = true;
            }
        }

        void endDroppedRow()
        {
            if (!droppedOnSurface)
            {
                droppedClusteringSource = null;
                droppedClustering = null;
                return; // at-or-before the slice start: reaches the metrics stage on neither path
            }
            boolean skewAccounting = TEST_SKEW_DROPPED_ROW_ACCOUNTING;
            boolean hasTombstones = false;
            if (droppedHasDeletion && droppedDeadCells > 0)
            {
                hasTombstones = true;
                if (!skewAccounting)
                {
                    for (int i = 0; i < droppedDeadCells; i++)
                        droppedTombstone();
                }
            }
            if (droppedPkLive || droppedAnyLiveCell)
            {
                ++droppedLiveRows;
                ++combinedLiveRows;
            }
            else if (!droppedPkLive && droppedHasDeletion && !hasTombstones && !skewAccounting)
            {
                droppedTombstone(); // MetricRecording's PK-deletion-only arm
            }
            droppedClusteringSource = null;
            droppedClustering = null;
        }

        private void droppedTombstone()
        {
            ++droppedTombstones;
            ++combinedTombstones;
            if (combinedTombstones > failureThreshold && respectTombstoneThresholds)
                // clustering materialized only on this exceptional path, from the group's own
                // still-loaded descriptor (or reused when the abandoned row already materialized
                // it); the abort message needs the real values
                productionAbort(droppedClustering != null
                                ? droppedClustering
                                : droppedClusteringSource.materializeClusteringPrefix());
        }

        private void combinedTombstone(ClusteringPrefix<?> position)
        {
            ++combinedTombstones;
            if (combinedTombstones > failureThreshold && respectTombstoneThresholds)
                productionAbort(position);
        }

        // ---- the production-time abort ----

        /**
         * Aborts the query when the tombstone count passes the failure threshold.  Records the
         * same tracing, metrics, and warnings the iterator path records at abort, then throws
         * TombstoneOverwhelmingException.
         */
        private void productionAbort(ClusteringPrefix<?> position)
        {
            String query = command.toCQLString();
            Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)",
                          failureThreshold, query);
            metric.tombstoneFailures.inc();
            if (command.isTrackingWarnings())
            {
                MessageParams.remove(ParamType.TOMBSTONE_WARNING);
                MessageParams.add(ParamType.TOMBSTONE_FAIL, combinedTombstones);
            }

            // sample the partial counts scanned so far
            if (combinedLiveRows > 0)
                metric.topReadPartitionRowCount.addSample(command.partitionKey().getKey(), combinedLiveRows);
            metric.topReadPartitionTombstoneCount.addSample(command.partitionKey().getKey(), combinedTombstones);

            // at-abort recordings: latency, histograms, totals, then the warn block
            metric.readLatency.addNano(nanoTime() - queryStartNanos);
            metric.tombstoneScannedHistogram.update(combinedTombstones);
            metric.liveScannedHistogram.update(combinedLiveRows);
            metric.totalRowsRead.inc(combinedLiveRows);
            boolean warnTombstones = combinedTombstones > warningThreshold && respectTombstoneThresholds;
            if (warnTombstones)
            {
                String msg = String.format("Read %d live rows and %d tombstone cells for query %1.512s; token %s (see tombstone_warn_threshold)",
                                           combinedLiveRows, combinedTombstones, query,
                                           command.partitionKey().getToken());
                if (command.isTrackingWarnings())
                    MessageParams.add(ParamType.TOMBSTONE_WARNING, combinedTombstones);
                else
                    ClientWarn.instance.warn(msg);
                if (combinedTombstones < failureThreshold)
                    metric.tombstoneWarnings.inc();
                logger.warn(msg);
            }
            Tracing.trace("Read {} live rows and {} tombstone cells{}",
                          combinedLiveRows, combinedTombstones,
                          (warnTombstones ? " (see tombstone_warn_threshold)" : ""));

            throw new TombstoneOverwhelmingException(combinedTombstones, query, command.metadata(),
                                                     command.partitionKey(), position);
        }
    }

    /**
     * The row-level filter probe and emitted-surface accounting decorator.  One object implements
     * both the FilterProbe hooks (dropped-row accounting) and the MergeSink events (emitted-element
     * accounting), so both interleave in stream order and the accumulator's counts match what the
     * iterator path would scan.
     *
     * When a production LIMIT bound is engaged for the same query, this probe is the outer layer of
     * a two-decorator chain: {@link CursorReads#mergeLegs} points {@code next} at
     * {@link LimitingMergeSink} instead of the base materializer.  The probe forwards every event
     * to {@code next} unconditionally.  {@code CursorReadMerger} calls {@code endRow}/{@code addRow}
     * on the top sink only for a row group it did NOT abandon, so a filter-dropped row never reaches
     * {@link LimitingMergeSink}'s counter.
     *
     * The filter verdict ({@link #rowGroupMatches}) evaluates each gate-approved CLUSTERING-column
     * expression as {@code operator.isSatisfiedBy(column.type, componentWindow, value)}, matching
     * {@code SimpleExpression.isSatisfiedBy} for a non-complex, non-counter clustering column
     * ({@code getValue} returns {@code clustering.bufferAt(position)}; a null component
     * fails the expression) — over a window into the descriptor's clustering wire bytes, decoded
     * reads the clustering bytes directly without materializing any component.  The verdict cannot
     * differ from the top filter's: the purge before row-level evaluation never changes clustering
     * bytes, and the group's first-sorted leg is the evaluation source.  The per-expression
     * {@code ByteBuffer.wrap} window goes only to {@code Operator.isSatisfiedBy}, which retains
     * nothing and is discarded after the call.
     *
     * The emitted-surface simulation matches {@code SlicedMaterializedIterator}: elements at or
     * before the slice start do not count, skipped and emitted markers both update the open-marker
     * state, the artificial open marker counts when a range deletion covers it at slice-open time,
     * and the artificial close counts when one is still open at exhaustion ({@link #finishPartition}).
     *
     * Regular-column verdicts resolve at winner resolution inside the cell walk, after part of the
     * row may already be emitted.  To account a mid-walk abandonment like a group rejected before
     * any cell work, the probe tracks the candidate row's emitted content as it flows through the
     * MergeSink events and replays it into the accumulator when {@code abandonRowGroup} fires.
     * Tracking engages only when regular expressions exist.
     */
    static final class RowLevelFilterProbe implements CursorReadMerger.MergeSink, CursorReadMerger.FilterProbe
    {
        /**
         * Where this probe forwards the MergeSink events it observes: {@link #base} directly when no
         * production bound is engaged, or {@link LimitingMergeSink} wrapping {@link #base} when one
         * is.  Forwarding through {@code next} makes {@link LimitingMergeSink}'s counting see the
         * same event stream this probe does.
         */
        private final CursorReadMerger.MergeSink next;
        /** The base materializer, reached directly (never through {@link #next}) for the two calls
         *  no MergeSink method can serve: {@link MaterializingMergeSink#abandonRow()} and the
         *  materialized-row peek ({@code materializedCount()}/{@code unfiltereds()}) that
         *  {@link #accountEmittedRow} needs. */
        private final MaterializingMergeSink base;
        private final ScanStatsAccumulator acc;
        private final ClusteringComparator comparator;
        private final ClusteringBound<?> sliceStart; // null when the slice starts at BOTTOM
        private final ClusteringBound<?> sliceEnd;   // the artificial-close position at exhaustion
        private final RowFilter.Expression[] expressions; // gate-approved CLUSTERING expressions
        /** per-evaluation component windows indexed by clustering position (reusable) */
        private final int[] valueOffsets;
        private final int[] valueLengths;
        private final int maxPosition;

        // ---- regular-column state ----
        /** gate-approved REGULAR-column expressions (empty = clustering-only probe) */
        private final RowFilter.Expression[] regularExpressions;
        /** the DISTINCT filter columns behind {@link #regularExpressions} — AND semantics are
         *  tracked per column (a column's single winner evaluates every expression on it at once) */
        private final ColumnMetadata[] regularColumns;
        private final TableMetadata metadata;   // for the real isSatisfiedBy on escape-hatch rows
        private final DecoratedKey key;         // idem
        private final long nowInSec;

        // per-row-group tracking (reset in rowGroupMatches, which runs for EVERY probed row
        // group before any merge work); buffers populate only when regularExpressions exist
        private int matchedRegularColumns;
        private boolean sawStartRow;
        private Clustering<?> trackedClustering;
        private LivenessInfo trackedLiveness;
        private DeletionTime trackedRowDeletion;
        private final List<Cell<?>> trackedCells = new ArrayList<>();
        private final List<DeletionTime> trackedComplexDeletions = new ArrayList<>();

        private boolean pastSliceStart; // sticky: the merged stream is clustering-ordered
        private boolean sliceOpened;    // the artificial-open accounting fired
        private DeletionTime simOpenMarker;

        RowLevelFilterProbe(CursorReadMerger.MergeSink next, MaterializingMergeSink base,
                            FilterPushdown pushdown, DecoratedKey key,
                            ClusteringComparator comparator, Slice slice)
        {
            this.next = next;
            this.base = base;
            this.acc = pushdown.scanStats();
            this.comparator = comparator;
            this.sliceStart = slice.start().isBottom() ? null : slice.start();
            this.sliceEnd = slice.end();
            this.pastSliceStart = sliceStart == null;
            this.expressions = pushdown.clusteringExpressions().toArray(new RowFilter.Expression[0]);
            int max = 0;
            for (RowFilter.Expression e : expressions)
                max = Math.max(max, e.column().position());
            this.maxPosition = max;
            this.valueOffsets = new int[maxPosition + 1];
            this.valueLengths = new int[maxPosition + 1];
            this.regularExpressions = pushdown.regularExpressions().toArray(new RowFilter.Expression[0]);
            List<ColumnMetadata> distinct = new ArrayList<>(regularExpressions.length);
            for (RowFilter.Expression e : regularExpressions)
            {
                boolean seen = false;
                for (ColumnMetadata c : distinct)
                    seen |= sameColumn(c, e.column());
                if (!seen)
                    distinct.add(e.column());
            }
            this.regularColumns = distinct.toArray(new ColumnMetadata[0]);
            this.metadata = pushdown.tableMetadata();
            this.key = key;
            this.nowInSec = pushdown.queryNowInSec();
        }

        /** Seeds the open-marker simulation with the merged row-index seek state — the same value
         *  the slicer receives as {@code MaterializedPartition.openMarkerAtStart}. */
        void initOpenMarker(DeletionTime openMarkerAtStart)
        {
            this.simOpenMarker = openMarkerAtStart;
        }

        // ---- FilterProbe: the clustering verdict ----

        /** Component-window sentinel: null component (expression provably fails). */
        private static final int NULL_COMPONENT = -1;
        /** Component-window sentinel: empty component (evaluates against an empty buffer). */
        private static final int EMPTY_COMPONENT = -2;

        @Override
        public boolean rowGroupMatches(UnfilteredDescriptor descriptor)
        {
            // this hook runs for every probed row group before any merge work, so it doubles as
            // the per-row reset point for the regular-column tracking state
            if (regularExpressions.length > 0)
            {
                matchedRegularColumns = 0;
                sawStartRow = false;
                trackedClustering = null;
                trackedLiveness = null;
                trackedRowDeletion = null;
                trackedCells.clear();
                trackedComplexDeletions.clear();
            }
            if (expressions.length == 0)
                return true; // regular-only filter: no clustering verdict, no wire walk

            byte[] data = descriptor.clusteringBytes();
            int limit = descriptor.clusteringLength();
            AbstractType<?>[] types = descriptor.clusteringTypes();
            // single wire walk up to the highest filtered position — the same header-bit and
            // length semantics as readClusteringValues, no component materialized
            long header = 0;
            int offset = 0;
            for (int i = 0; i <= maxPosition; i++)
            {
                if ((i % 32) == 0)
                {
                    if (offset >= limit)
                        throw new IllegalStateException("truncated clustering bytes: header block at " + offset + " past limit " + limit);
                    header = VIntCoding.getUnsignedVInt(data, ByteArrayAccessor.instance, offset, limit);
                    offset += PartitionMaterializer.vintSize(data, offset);
                }
                if ((header & (1L << ((i * 2) + 1))) != 0) // null bit
                {
                    valueOffsets[i] = NULL_COMPONENT;
                    continue;
                }
                if ((header & (1L << (i * 2))) != 0) // empty bit
                {
                    valueOffsets[i] = EMPTY_COMPONENT;
                    continue;
                }
                int length = types[i].valueLengthIfFixed();
                if (length < 0)
                {
                    if (offset >= limit)
                        throw new IllegalStateException("truncated clustering bytes: length vint for component " + i + " at " + offset + " past limit " + limit);
                    length = VIntCoding.checkedCast(VIntCoding.getUnsignedVInt(data, ByteArrayAccessor.instance, offset, limit));
                    offset += PartitionMaterializer.vintSize(data, offset);
                }
                if (offset + length > limit)
                    throw new IllegalStateException("truncated clustering bytes: component " + i + " of length " + length + " at " + offset + " past limit " + limit);
                valueOffsets[i] = offset;
                valueLengths[i] = length;
                offset += length;
            }

            for (RowFilter.Expression e : expressions)
            {
                int position = e.column().position();
                int valueOffset = valueOffsets[position];
                if (valueOffset == NULL_COMPONENT)
                    return false; // getValue would return null -> the top filter drops the row
                ByteBuffer window = valueOffset == EMPTY_COMPONENT
                                    ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                                    : ByteBuffer.wrap(data, valueOffset, valueLengths[position]);
                if (!e.operator().isSatisfiedBy(e.column().type, window, e.getIndexValue()))
                    return false;
            }
            return true;
        }

        // ---- FilterProbe: dropped-group accounting hooks (delegating classification to the
        //      accumulator, in stream order) ----

        @Override
        public void beginDroppedRow(MergeLeg clusteringSource, boolean onEmittedSurface,
                                    LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            ROWS_DROPPED_BY_FILTER.incrementAndGet();
            if (onEmittedSurface)
                openSurface(); // the artificial slice-start open precedes this row's counts
            acc.beginDroppedRow(clusteringSource, onEmittedSurface, mergedLiveness, mergedRowDeletion);
        }

        @Override
        public void droppedComplexDeletion(DeletionTime mergedComplexDeletion)
        {
            acc.droppedComplexDeletion(mergedComplexDeletion);
        }

        @Override
        public void droppedCell(CellLivenessInfo winnerLiveness)
        {
            acc.droppedCell(winnerLiveness);
        }

        @Override
        public void endDroppedRow()
        {
            acc.endDroppedRow();
        }

        // ---- FilterProbe: the regular-column verdicts ----

        @Override
        public boolean filtersRegularColumn(ColumnMetadata column)
        {
            for (ColumnMetadata c : regularColumns)
            {
                if (sameColumn(c, column))
                    return true;
            }
            return false;
        }

        @Override
        public boolean cellIsLive(CellLivenessInfo winnerLiveness)
        {
            return acc.cellLive(winnerLiveness.localDeletionTime(), winnerLiveness.ttl());
        }

        @Override
        public boolean regularCellMatches(ColumnMetadata column, ByteBuffer valueWindow)
        {
            // evaluate every expression on this column against the live cell's value; the caller
            // has established liveness.  valueWindow views reusable scratch and is consumed within
            // the isSatisfiedBy calls, which retain nothing.
            for (RowFilter.Expression e : regularExpressions)
            {
                if (sameColumn(e.column(), column)
                    && !e.operator().isSatisfiedBy(e.column().type, valueWindow, e.getIndexValue()))
                    return false;
            }
            ++matchedRegularColumns;
            return true;
        }

        @Override
        public boolean rowRegularFiltersSatisfied()
        {
            return matchedRegularColumns >= regularColumns.length;
        }

        @Override
        public void abandonRowGroup(MergeLeg clusteringSource, boolean onEmittedSurface,
                                    CellLivenessInfo failedWinnerLiveness)
        {
            ROWS_DROPPED_BY_FILTER.incrementAndGet();
            ROWS_DROPPED_BY_REGULAR_FILTER.incrementAndGet();
            if (onEmittedSurface)
                openSurface(); // the artificial slice-start open precedes this row's counts
            if (sawStartRow)
            {
                // discard the partially-built output row and open the dropped accounting on the
                // already-materialized shell.  abandonRow() resets the shared row builder, so it
                // must reach base directly rather than through the forwarding chain.
                base.abandonRow();
                acc.beginDroppedRow(trackedClustering, onEmittedSurface,
                                    trackedLiveness.isEmpty() ? null : trackedLiveness,
                                    trackedRowDeletion);
            }
            else
            {
                // nothing was emitted yet: the merged shell was empty, so account a null/LIVE shell
                acc.beginDroppedRow(clusteringSource, onEmittedSurface, null, DeletionTime.LIVE);
            }
            // replay the row content emitted before the failure
            for (int i = 0; i < trackedComplexDeletions.size(); i++)
                acc.droppedComplexDeletion(trackedComplexDeletions.get(i));
            for (int i = 0; i < trackedCells.size(); i++)
            {
                Cell<?> cell = trackedCells.get(i);
                acc.droppedCell(cell.timestamp(), cell.localDeletionTime(), cell.ttl());
            }
            // the failing winner itself (null for shadowed/absent failures)
            if (failedWinnerLiveness != null)
                acc.droppedCell(failedWinnerLiveness);
            // the merge core streams the row's remaining winners and closes with endDroppedRow
        }

        @Override
        public boolean existingRowMatches(Row row)
        {
            // escape-hatch rows are live objects, so the real production evaluation applies
            for (RowFilter.Expression e : regularExpressions)
            {
                if (!e.isSatisfiedBy(metadata, key, row, nowInSec))
                    return false;
            }
            return true;
        }

        @Override
        public void abandonExistingRow(Row row, boolean onEmittedSurface)
        {
            ROWS_DROPPED_BY_FILTER.incrementAndGet();
            ROWS_DROPPED_BY_REGULAR_FILTER.incrementAndGet();
            if (onEmittedSurface)
                openSurface();
            LivenessInfo pk = row.primaryKeyLivenessInfo();
            acc.beginDroppedRow(row.clustering(), onEmittedSurface,
                                pk.isEmpty() ? null : pk, row.deletion().time());
            for (ColumnData cd : row)
            {
                if (cd.column().isComplex())
                    acc.droppedComplexDeletion(((ComplexColumnData) cd).complexDeletion());
            }
            for (Cell<?> cell : row.cells())
                acc.droppedCell(cell.timestamp(), cell.localDeletionTime(), cell.ttl());
            acc.endDroppedRow();
        }

        // ---- MergeSink: emitted-surface accounting around the materializer ----

        @Override
        public void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            if (regularExpressions.length > 0)
            {
                // immutable per-row outputs, safe to retain until the row resolves
                sawStartRow = true;
                trackedClustering = clustering;
                trackedLiveness = mergedLiveness;
                trackedRowDeletion = mergedRowDeletion;
            }
            next.startRow(clustering, mergedLiveness, mergedRowDeletion);
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion)
        {
            if (regularExpressions.length > 0)
                trackedComplexDeletions.add(mergedComplexDeletion); // immutable copy (see mergeCellGroup)
            next.addComplexDeletion(column, mergedComplexDeletion);
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            if (regularExpressions.length > 0)
                trackedCells.add(cell); // materialized (or live memtable) cell — immutable
            next.addCell(cell);
        }

        @Override
        public void endRow()
        {
            // peek/count against base directly; base.endRow() fires at most once per this call
            long before = base.materializedCount();
            next.endRow();
            if (base.materializedCount() != before)
                accountEmittedRow();
        }

        @Override
        public void addRow(Row row)
        {
            long before = base.materializedCount();
            next.addRow(row);
            if (base.materializedCount() != before)
                accountEmittedRow();
        }

        private void accountEmittedRow()
        {
            List<Unfiltered> unfiltereds = base.unfiltereds();
            Row row = (Row) unfiltereds.get(unfiltereds.size() - 1);
            if (!countsTowardSurface(row.clustering()))
                return;
            openSurface();
            acc.accountRow(row);
        }

        @Override
        public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            next.addRangeTombstoneMarker(marker);
            if (countsTowardSurface(marker.clustering()))
            {
                openSurface(); // uses the pre-update open-marker state
                acc.accountMarker(marker);
            }
            // both skipped and emitted markers drive the open-marker tracking
            simOpenMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        /**
         * Delegates to {@link #next}.  When a production bound is engaged, {@code next} is
         * {@link LimitingMergeSink}, so its bound stops the merge once satisfied.  Otherwise
         * {@code next} is {@link #base}, whose default is unbounded.
         */
        @Override
        public boolean wantsMore()
        {
            return next.wantsMore();
        }

        /** Matches {@code SlicedMaterializedIterator.computeNextInSlice}'s non-strict pre-slice
         *  skip: only elements strictly after the slice start reach the metrics stage.  Sticky,
         *  since the merged stream is clustering-ordered. */
        private boolean countsTowardSurface(ClusteringPrefix<?> clustering)
        {
            if (!pastSliceStart)
                pastSliceStart = comparator.compare(clustering, (ClusteringPrefix<?>) sliceStart) > 0;
            return pastSliceStart;
        }

        /** Fires the artificial slice-start open marker's accounting exactly once, at the moment
         *  the emitted slice opens (first on-surface element, or exhaustion). */
        private void openSurface()
        {
            if (sliceOpened)
                return;
            sliceOpened = true;
            if (sliceStart != null && simOpenMarker != null)
                acc.accountArtificialMarker(sliceStart, simOpenMarker);
        }

        /** Called after the merge loop completes normally: the slicer opens the slice even when
         *  nothing survived to be emitted, and artificially closes a still-open range deletion at
         *  the slice end — both reach MetricRecording on the iterator path. */
        void finishPartition()
        {
            openSurface();
            if (simOpenMarker != null)
                acc.accountArtificialMarker(sliceEnd, simOpenMarker);
        }
    }

    /**
     * The cursor-level k-way merge across {@code >= 2} pending sstable legs of one partition read.
     * Reconciles at the descriptor/byte level below materialization; only merge winners become
     * {@code Row}/{@code Cell} objects.  Returns one iterator whose emitted stream is byte-identical
     * to the object-level merge of the per-leg iterators.  Memtable legs, if any, still object-merge
     * with this result above.  Legs are always closed before this returns.
     *
     * Each indexed BTI leg of a single-slice read seeks to its row-index floor block for the slice
     * start before the merge runs ({@link PendingLeg#seekForMerge}), with its open-range-tombstone
     * state seeded into the merge's cross-leg open-marker set.  A narrow slice of a wide partition
     * no longer walks every leg from the partition start.  BIG legs keep the eager walk.  Combined
     * with the merge core's slice end-stop, the merge is bounded on both sides.
     *
     * This merge reconciles only.  It makes no purge decisions (reads purge above the merge via
     * {@code withoutPurgeableTombstones}), no expired-TTL-to-tombstone conversion, and no MV row
     * skipping (MV tables are gated out).  Corrupted-tombstone validation stays at the per-leg,
     * in-slice placement.
     *
     * @param productionBound a detached {@code DataLimits} counter matching the query's limit
     *                        ({@link #limitBoundFor}), or null for unbounded production.  Non-null
     *                        only when the caller has verified this merge's output is the final
     *                        merged stream the counter consumes.  The authoritative
     *                        {@code limits().filter} above stays in place either way.  May be
     *                        non-null at the same time as {@code filterPushdown}.
     * @param filterPushdown  the query's filter pushdown context ({@link #filterPushdownFor}), or
     *                        null when pushdown is disengaged.  When set, its partition-level
     *                        expressions can skip the row-group merge, and once
     *                        {@code filterPushdown.scanStats() != null} its row-level expressions
     *                        can drop individual rows.  When both this and {@code productionBound}
     *                        are set, this method wires {@link RowLevelFilterProbe} as the outermost
     *                        sink forwarding through {@link LimitingMergeSink}, so the limit counts
     *                        only rows that also survive the filter.
     */
    public static UnfilteredRowIterator mergeLegs(List<? extends MergeLeg> legs,
                                                  TableMetadata metadata,
                                                  DecoratedKey key,
                                                  Slices slices,
                                                  ColumnFilter columnFilter,
                                                  DataLimits.Counter productionBound,
                                                  FilterPushdown filterPushdown)
    {
        assert legs.size() >= 2 : "the merge core engages only for >= 2 legs";
        try
        {
            MergeContext<MaterializingMergeSink> ctx = runMergeLegs(legs, metadata, key, slices, columnFilter,
                                                                     productionBound, filterPushdown,
                                                                     MaterializingMergeSink::new);
            MaterializingMergeSink sink = ctx.sink;
            UNFILTEREDS_MATERIALIZED.addAndGet(sink.materializedCount());
            CURSOR_MERGES_SERVED.incrementAndGet();
            if (ctx.limitingSink != null && !ctx.limitingSink.wantsMore())
                MERGES_STOPPED_BY_LIMIT.incrementAndGet();
            int sstableLegCount = 0;
            SSTableReader attribution = null;
            for (MergeLeg leg : legs)
            {
                if (leg.sstableOrNull() != null)
                {
                    sstableLegCount++;
                    if (attribution == null)
                        attribution = leg.sstableOrNull();
                }
            }
            SSTABLE_LEGS_CURSOR_MERGED.addAndGet(sstableLegCount);
            MEMTABLE_LEGS_CURSOR_MERGED.addAndGet(legs.size() - sstableLegCount);
            assert attribution != null : "the gate requires at least one sstable leg";

            // min-merge over the same per-leg stats the per-leg iterators would report, so the
            // response's stats total matches the object merge's
            EncodingStats stats = EncodingStats.merge(legs, MergeLeg::legStats);
            MaterializedPartition partition = new MaterializedPartition(ctx.mergedDeletion, ctx.mergedStatic,
                                                                       sink.unfiltereds(), ctx.openMarkerAtStart);
            // validateOnEmission = false: per-leg validation inside the merge is the read's entire
            // validation, like the iterator path
            return new SlicedMaterializedIterator(metadata, key, attribution, stats,
                                                  columnFilter, ctx.emitSlices, partition, false);
        }
        finally
        {
            for (MergeLeg leg : legs)
                leg.close();
        }
    }

    /**
     * The sink-injection overload of {@link #mergeLegs}, parameterized by a {@link MergeSinkFactory}
     * instead of a hardcoded {@code MaterializingMergeSink}.  Runs the identical leg-setup, limit,
     * and filter-pushdown machinery via the shared {@link #runMergeLegs}; {@code mergeLegs} is this
     * overload called with {@code MaterializingMergeSink::new} plus its materialization tail.  This
     * overload is {@code @VisibleForTesting} and lets the {@code TranscodeMergeSink} test suite
     * drive the real merge core against a non-materializing sink.  No production call site uses it.
     *
     * @return a context carrying the populated sink plus the per-partition state outside the sink
     *         (the merged partition deletion, static row, start-of-merge open marker, and the
     *         emitted slices).
     */
    @VisibleForTesting
    public static <S extends CursorReadMerger.MergeSink> MergeContext<S> mergeLegsWithSink(
        List<? extends MergeLeg> legs,
        TableMetadata metadata,
        DecoratedKey key,
        Slices slices,
        ColumnFilter columnFilter,
        DataLimits.Counter productionBound,
        FilterPushdown filterPushdown,
        MergeSinkFactory<S> sinkFactory)
    {
        assert legs.size() >= 2 : "the merge core engages only for >= 2 legs";
        try
        {
            return runMergeLegs(legs, metadata, key, slices, columnFilter, productionBound, filterPushdown, sinkFactory);
        }
        finally
        {
            for (MergeLeg leg : legs)
                leg.close();
        }
    }

    /**
     * The shared core of {@link #mergeLegs}/{@link #mergeLegsWithSink}: everything from the
     * partition-deletion and static-row merge through {@code CursorReadMerger.mergeUnfiltereds()},
     * generic over the sink type.  Sink-specific post-processing stays in each caller.  Does not
     * close the legs; both callers wrap this in their own try/finally.
     */
    private static <S extends CursorReadMerger.MergeSink> MergeContext<S> runMergeLegs(
        List<? extends MergeLeg> legs,
        TableMetadata metadata,
        DecoratedKey key,
        Slices slices,
        ColumnFilter columnFilter,
        DataLimits.Counter productionBound,
        FilterPushdown filterPushdown,
        MergeSinkFactory<S> sinkFactory)
    {
        // supersedes-max over every leg, like UnfilteredRowIterators.merge's
        // collectPartitionLevelDeletion
        DeletionTime mergedDeletion = DeletionTime.LIVE;
        for (int i = 0; i < legs.size(); i++)
        {
            DeletionTime legDeletion = legs.get(i).partitionLevelDeletion();
            if (!mergedDeletion.supersedes(legDeletion))
                mergedDeletion = legDeletion;
        }

        Row mergedStatic = mergeStaticRows(legs, columnFilter.fetchedColumns().statics, mergedDeletion);

        // partition-level filter short-circuit, evaluated where the merged static row first
        // exists, like RowFilter.filter's applyToPartition check.  On failure the row legs never
        // join the merge: the emitted iterator is the empty-with-static-row shape a no-row-legs
        // merge produces, and the top-level filter drops it identically.
        boolean partitionSkippedByFilter = false;
        if (filterPushdown != null)
        {
            FILTER_PUSHDOWNS_ENGAGED.incrementAndGet();
            if (!filterPushdown.partitionLevelMatches(key, mergedStatic))
            {
                partitionSkippedByFilter = true;
                PARTITIONS_SKIPPED_BY_FILTER.incrementAndGet();
            }
        }

        // Only legs with a non-empty slice contribute rows; a Slices.NONE leg joins through its
        // partition deletion and static row alone.  Memtable legs always carry the query's real
        // slices.  A partition skipped by the filter short-circuit contributes no row legs; their
        // cursors are closed unread in the finally below.
        List<MergeLeg> rowLegs = new ArrayList<>(legs.size());
        if (!partitionSkippedByFilter)
        {
            for (MergeLeg leg : legs)
            {
                if (!leg.legSlices().isEmpty())
                    rowLegs.add(leg);
            }
        }

        S sink = sinkFactory.newSink();
        LimitingMergeSink limitingSink = null;
        DeletionTime openMarkerAtStart = null;
        if (!rowLegs.isEmpty())
        {
            Slice slice = rowLegs.get(0).legSlices().get(0);
            try
            {
                for (MergeLeg leg : rowLegs)
                {
                    leg.enterMergeMode();
                    // per-leg BTI row-index seek, after the elimination loop confirmed the leg
                    // participates.  No-op for memtable legs (already in memory).
                    leg.seekForMerge();
                }
                // wrap the sink in the limit counter.  Seeding it with the key and merged static
                // row here mirrors the top counter's per-partition attach.  LimitingMergeSink wraps
                // MaterializingMergeSink concretely, so a non-materializing sink cannot compose with
                // a non-null productionBound.
                CursorReadMerger.MergeSink mergeSink = sink;
                if (productionBound != null)
                {
                    productionBound.countPartition(key, mergedStatic);
                    limitingSink = new LimitingMergeSink((MaterializingMergeSink) sink, productionBound,
                                                         metadata.comparator, slice);
                    mergeSink = limitingSink;
                }
                // row-level filter pushdown and its scan-stats accounting.  When engaged,
                // RowLevelFilterProbe is the outermost sink, forwarding events through whatever
                // mergeSink already is (the base sink, or LimitingMergeSink wrapping it).  It peeks
                // and abandons against sink cast to MaterializingMergeSink directly.
                CursorReadMerger.FilterProbe probe = null;
                RowLevelFilterProbe filterProbe = null;
                if (filterPushdown != null && filterPushdown.scanStats() != null)
                {
                    filterProbe = new RowLevelFilterProbe(mergeSink, (MaterializingMergeSink) sink, filterPushdown,
                                                          key, metadata.comparator, slice);
                    mergeSink = filterProbe;
                    probe = filterProbe;
                }
                CursorReadMerger merger = new CursorReadMerger(rowLegs.toArray(new MergeLeg[0]), metadata,
                                                              mergedDeletion, slice, mergeSink, probe);
                // supersedes-max over the seeked legs' row-index open-deletion seeds; the slicer
                // synthesizes the artificial open marker from it
                openMarkerAtStart = merger.openMarkerAtMergeStart();
                if (filterProbe != null)
                {
                    filterProbe.initOpenMarker(openMarkerAtStart);
                    // the merged static row is scanned first on the pull side, so account it before
                    // any stream element
                    filterPushdown.scanStats().accountRow(mergedStatic);
                }
                // TranscodeMergeSink needs the same seek-state seed for its own open-marker tracking
                if (sink instanceof TranscodeMergeSink)
                    ((TranscodeMergeSink) sink).initOpenMarker(openMarkerAtStart);
                merger.mergeUnfiltereds();
                if (filterProbe != null)
                    filterProbe.finishPartition();
            }
            catch (IOException e)
            {
                throw new RuntimeException("cursor merge failed for " + key + " over " + legs.size() + " legs", e);
            }
        }

        Slices emitSlices = rowLegs.isEmpty() ? Slices.NONE : rowLegs.get(0).legSlices();
        return new MergeContext<>(sink, mergedDeletion, mergedStatic, openMarkerAtStart, emitSlices, limitingSink);
    }

    /**
     * @see #mergeLegsWithSink
     *
     * Deliberately unbounded.  Do not add {@code <S extends CursorReadMerger.MergeSink>}: that bound
     * references a package-private type, so a cross-package caller's lambda would make the JVM
     * verifier resolve the inaccessible erased type and fail with {@code IllegalAccessError} at
     * runtime.  {@link #mergeLegsWithSink}'s own bound enforces the real constraint at each call site.
     */
    @FunctionalInterface
    public interface MergeSinkFactory<S>
    {
        S newSink();
    }

    /**
     * @see #mergeLegsWithSink
     *
     * Also unbounded, for the same reason as {@link MergeSinkFactory}: the public {@code sink} field
     * below would otherwise erase to a package-private type a cross-package field read cannot access.
     */
    @VisibleForTesting
    public static final class MergeContext<S>
    {
        public final S sink;
        public final DeletionTime mergedDeletion;
        public final Row mergedStatic;
        public final DeletionTime openMarkerAtStart;
        public final Slices emitSlices;
        /** null unless a production bound was engaged; {@link #mergeLegs} consults it for its counter. */
        final LimitingMergeSink limitingSink;

        private MergeContext(S sink, DeletionTime mergedDeletion, Row mergedStatic,
                             DeletionTime openMarkerAtStart, Slices emitSlices, LimitingMergeSink limitingSink)
        {
            this.sink = sink;
            this.mergedDeletion = mergedDeletion;
            this.mergedStatic = mergedStatic;
            this.openMarkerAtStart = openMarkerAtStart;
            this.emitSlices = emitSlices;
            this.limitingSink = limitingSink;
        }
    }

    /**
     * Merges the legs' per-leg static rows, mirroring
     * {@code UnfilteredRowIterators.UnfilteredRowMergeIterator.mergeStaticRows}.  The merged static
     * row must equal what the object merge would produce, so the outer merge with any memtable legs
     * stays byte-identical.  Statics are one row per leg, so the object-level {@code Row.Merger} is
     * used here.
     */
    private static Row mergeStaticRows(List<? extends MergeLeg> legs, Columns statics, DeletionTime partitionDeletion)
    {
        if (statics.isEmpty())
            return Rows.EMPTY_STATIC_ROW;

        boolean allEmpty = true;
        for (MergeLeg leg : legs)
            allEmpty &= leg.staticRow().isEmpty();
        if (allEmpty)
            return Rows.EMPTY_STATIC_ROW;

        Row.Merger merger = new Row.Merger(legs.size(), statics.hasComplex());
        for (int i = 0; i < legs.size(); i++)
            merger.add(i, legs.get(i).staticRow());
        Row merged = merger.merge(partitionDeletion);
        return merged == null ? Rows.EMPTY_STATIC_ROW : merged;
    }

    /** Closes every leg, adding any secondary failure to {@code cause} as suppressed. */
    public static void closeAll(List<? extends AutoCloseable> legs, Throwable cause)
    {
        if (legs == null)
            return;
        for (AutoCloseable leg : legs)
        {
            try
            {
                leg.close();
            }
            catch (Throwable t)
            {
                cause.addSuppressed(t);
            }
        }
    }

    /**
     * The non-exceptional counterpart of {@link #closeAll}: closes every already-opened leg/iterator
     * when {@code SinglePartitionReadCommand.queryStorageToResponseBytes}'s gate declines after some
     * legs are already open (for example, the leg-count >= 2 requirement fails once the real
     * candidate count is known).  This is the normal fall-back path, not an error path, so there is
     * no in-flight exception to attach suppressed failures to.
     */
    static void closeAllQuietly(List<? extends AutoCloseable> items)
    {
        if (items == null)
            return;
        RuntimeException failure = null;
        for (AutoCloseable item : items)
        {
            try
            {
                item.close();
            }
            catch (Exception e)
            {
                if (failure == null)
                    failure = new RuntimeException("failed to close a cursor-read leg/iterator while declining the transcode path", e);
                else
                    failure.addSuppressed(e);
            }
        }
        if (failure != null)
            throw failure;
    }

    /**
     * Copies a (possibly reusable) DeletionTime into an immutable one, preserving the exact
     * unsigned-int local-deletion-time representation. NOT {@code DeletionTime.build(mfda, ldt)}:
     * build() takes the LONG domain and classifies {@code localDeletionTime() == Long.MAX_VALUE}
     * (the LIVE sentinel) as an InvalidDeletionTime — a bug the differential harness caught on its
     * very first run (live partition deletions came back as ldt=MAX_DELETION_TIME).
     */
    static DeletionTime copyOf(DeletionTime deletionTime)
    {
        return deletionTime.isLive()
               ? DeletionTime.LIVE
               : DeletionTime.buildUnsafeWithUnsignedInteger(deletionTime.markedForDeleteAt(),
                                                             deletionTime.localDeletionTimeUnsignedInteger());
    }

    /** The materialized content of one sstable's partition (whole partition, or the stretch
     *  covering the queried slice on a seeked or end-bounded BTI read), or of the merged stream of
     *  several sstables' partitions. */
    static final class MaterializedPartition
    {
        final DeletionTime partitionDeletion;
        final Row staticRow;
        final List<Unfiltered> unfiltereds;
        /** The range-tombstone deletion open at the first materialized unfiltered, per the BTI row
         *  index; non-null only when a row-index seek was issued, like
         *  {@code ForwardIndexedReader.setForSlice}.  For a merged partition, the supersedes-max
         *  over the seeked legs' seeds.  Null means track open markers from the stream alone. */
        final DeletionTime openMarkerAtStart;

        MaterializedPartition(DeletionTime partitionDeletion, Row staticRow, List<Unfiltered> unfiltereds,
                              DeletionTime openMarkerAtStart)
        {
            this.partitionDeletion = partitionDeletion;
            this.staticRow = staticRow;
            this.unfiltereds = unfiltereds;
            this.openMarkerAtStart = openMarkerAtStart;
        }
    }

    /**
     * Per-query cell-value transfer scratch, shared by every cursor-served leg of one
     * single-partition read: the 4KB bounce buffer for variable-length value chunking and the
     * one-copy {@link CellValueCapture}.  Sharing is safe because both are working storage scoped
     * to a single value-copy call, results land in per-leg/per-cell state, and the whole read runs
     * on one thread, so no two legs ever have transfer state in flight at once.
     */
    public static final class ValueTransfer
    {
        final CellValueCapture valueCapture = new CellValueCapture();
        // chunking buffer for variable-length cell values only; fixed-length values use the final
        // value array itself as the transfer buffer (see CellValueCapture)
        final byte[] transferBuffer = new byte[4096];
    }

    /**
     * Drives {@link SSTableCursorReader} over one partition and materializes rows/markers,
     * mirroring the filtering behavior of {@code UnfilteredSerializer.readSimpleColumn} /
     * {@code readComplexColumn} / {@code AbstractSSTableIterator.readStaticRow} for the query's
     * {@link ColumnFilter} so the produced objects match the iterator path's cell for cell.
     */
    private static final class PartitionMaterializer
    {
        private final SSTableCursorReader cursor;
        private final SSTableReader sstable;
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final ColumnFilter columnFilter;
        // Same helper type the iterator path drives its filtering with; using the real thing keeps
        // the includes/canSkipValue/isDropped semantics identical by construction.
        private final DeserializationHelper filterHelper;
        private final List<AbstractType<?>> clusteringTypes;
        private final PartitionDescriptor pHeader;
        private final UnfilteredDescriptor uDesc;
        // per-query shared value-transfer scratch (see ValueTransfer): one instance serves every leg
        private final ValueTransfer transfer;

        // one row builder for the whole partition, reused across the static row and every regular
        // row: build() resets it and newRow() asserts the previous build() happened, the same reuse
        // contract UnfilteredDeserializer relies on.  Also receives complex deletions as
        // materializeRowContents enters each complex column, in disk order.
        private final Row.Builder rowBuilder = BTreeRow.sortedBuilder();

        // the complex column the current row's cell walk last entered (reset per row): gates the
        // once-per-column work in materializeRowContents
        private ColumnMetadata currentComplexColumn;

        PartitionMaterializer(SSTableCursorReader cursor, SSTableReader sstable, TableMetadata metadata, DecoratedKey key, ColumnFilter columnFilter, ValueTransfer transfer)
        {
            this.cursor = cursor;
            this.sstable = sstable;
            this.metadata = metadata;
            this.key = key;
            this.columnFilter = columnFilter;
            this.transfer = transfer;
            this.filterHelper = new DeserializationHelper(metadata,
                                                          sstable.descriptor.version.correspondingMessagingVersion(),
                                                          DeserializationHelper.Flag.LOCAL,
                                                          columnFilter);
            this.clusteringTypes = sstable.header.clusteringTypes();
            this.pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            this.uDesc = new UnfilteredDescriptor(clusteringTypes.toArray(new AbstractType<?>[0]));

            // Deletion-only complex columns must surface as positions of their own
            // (compaction's pattern): the cursor holds the current column's deletion in
            // CellCursor.complexDeletion, and materializeRowContents records it on entering
            // each complex column, mirroring UnfilteredSerializer.readComplexColumn.
            cursor.pauseAtEmptyComplexColumns(true);
        }

        // set by openPartition; consumed by readRows / the merge path
        private DeletionTime partitionDeletion;
        private Row staticRow = Rows.EMPTY_STATIC_ROW;

        DeletionTime partitionDeletion()
        {
            return partitionDeletion;
        }

        Row staticRow()
        {
            return staticRow;
        }

        /**
         * The leg-open phase: seeks to the partition, reads and validates the header, materializes
         * the static row.  Returns the cursor state at the first unfiltered (or
         * {@code PARTITION_END}); rows are produced separately by {@link #readRows} or consumed
         * descriptor-level by the merge core.
         */
        int openPartition(long position) throws IOException
        {
            // The cursor was constructed over a single bound starting at this partition, so it is
            // already positioned; see PendingLeg.open.
            if (cursor.state() != PARTITION_START)
                throw new IllegalStateException("seek to partition at " + position + " yielded state " + cursor.state());
            int state = cursor.readPartitionHeader(pHeader);
            // corrupted_tombstone_strategy check on the partition-level deletion, mirroring
            // AbstractSSTableIterator's validate() -> handleInvalid and cursor compaction.
            // Validating the descriptor's deletion time range-checks the on-disk unsigned
            // representation; this is strictly more protective than the iterator path's check, which
            // is constant-true for the latest format.
            if (!pHeader.deletionTime().validate())
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "partitionLevelDeletion=" + pHeader.deletionTime());
            partitionDeletion = copyOf(pHeader.deletionTime());
            staticRow = Rows.EMPTY_STATIC_ROW;

            if (state == STATIC_ROW_START)
            {
                // mirrors AbstractSSTableIterator.readStaticRow: when no static column is fetched
                // the row is skipped wholesale and stays EMPTY_STATIC_ROW
                if (columnFilter.fetchedColumns().statics.isEmpty())
                {
                    state = cursor.skipStaticRow(false);
                }
                else
                {
                    state = cursor.readStaticRowHeader(uDesc);
                    rowBuilder.newRow(Clustering.STATIC_CLUSTERING);
                    state = materializeRowContents(state);
                    staticRow = rowBuilder.build();
                }
                if (state == UNFILTERED_END)
                    state = cursor.continueReading();
            }
            return state;
        }

        /**
         * The row phase for a single cursor-served leg.
         *
         * @param seekPoint when non-null (BTI, indexed partition, single slice with a real start
         *                  bound), the row-index floor block for the slice start: the cursor jumps
         *                  there instead of walking every row before it.  Rows between the block
         *                  start and the slice start are still materialized and skipped by
         *                  {@link SlicedMaterializedIterator}'s pre-slice logic.
         * @param endBound when non-null, materialization stops at the first unfiltered at or past
         *                 this bound, mirroring {@code ForwardReader.computeNext}'s end cutoff.
         */
        MaterializedPartition readRows(int state, boolean readRows,
                                       BtiCursorSeekSupport.SeekPoint seekPoint,
                                       ClusteringBound<?> endBound) throws IOException
        {
            List<Unfiltered> unfiltereds = new ArrayList<>();

            DeletionTime openMarkerAtStart = null;
            if (readRows && seekPoint != null && isState(state, ROW_START | TOMBSTONE_START))
            {
                // the current unfiltered's flags byte is already consumed, so its start is one byte
                // behind the cursor.  Like ForwardIndexedReader.setForSlice, only seek forward; when
                // the floor block is the one already being read, keep reading sequentially.
                long currentUnfilteredStart = cursor.position() - 1;
                if (seekPoint.dataPosition > currentUnfilteredStart)
                {
                    state = cursor.seekUnfiltered(seekPoint.dataPosition);
                    openMarkerAtStart = seekPoint.openMarker == null ? null : copyOf(seekPoint.openMarker);
                    SSTABLE_LEG_ROW_INDEX_SEEKS.incrementAndGet();
                }
            }

            long materializedCount = 0;
            readLoop:
            while (readRows && !isState(state, PARTITION_END | DONE))
            {
                switch (state)
                {
                    case ROW_START:
                    {
                        state = cursor.readRowHeader(uDesc);
                        Clustering<?> clustering = (Clustering<?>) toClusteringPrefix();
                        // end-stop: at or past the slice end, nothing further can be emitted, so
                        // stop before materializing this row's cells
                        if (endBound != null && metadata.comparator.compare(clustering, endBound) >= 0)
                            break readLoop;
                        rowBuilder.newRow(clustering);
                        state = materializeRowContents(state);
                        materializedCount++;
                        Row row = rowBuilder.build();
                        // mirrors ForwardReader.computeNext's next.isEmpty() skip
                        if (!row.isEmpty())
                            unfiltereds.add(row);
                        break;
                    }
                    case TOMBSTONE_START:
                    {
                        state = cursor.readTombstoneMarker(uDesc);
                        RangeTombstoneMarker marker = materializeMarker();
                        // a marker at or past the slice end is never emitted; the artificial close
                        // comes from the open-marker state
                        if (endBound != null && metadata.comparator.compare(marker.clustering(), endBound) >= 0)
                            break readLoop;
                        materializedCount++;
                        unfiltereds.add(marker);
                        break;
                    }
                    default:
                        throw new IllegalStateException("unexpected cursor state " + state);
                }
                if (state == UNFILTERED_END)
                    state = cursor.continueReading();
            }

            UNFILTEREDS_MATERIALIZED.addAndGet(materializedCount);
            return new MaterializedPartition(partitionDeletion, staticRow, unfiltereds, openMarkerAtStart);
        }

        /**
         * Reads liveness/deletion off the just-loaded {@link UnfilteredDescriptor} and then walks
         * the cell states, adding materialized cells to the reused {@link #rowBuilder} (the caller
         * has already called {@code newRow} on it). Returns the cursor state after the row's cells
         * are exhausted ({@code UNFILTERED_END}).
         */
        private int materializeRowContents(int state) throws IOException
        {
            // like UnfilteredSerializer.deserializeRowBody: a row without a timestamp keeps the
            // LivenessInfo.EMPTY singleton instead of allocating an equal empty instance per row
            LivenessInfo rowLiveness = uDesc.livenessInfo().isEmpty()
                                       ? LivenessInfo.EMPTY
                                       : LivenessInfo.withExpirationTime(uDesc.livenessInfo().timestamp(),
                                                                         uDesc.livenessInfo().ttl(),
                                                                         uDesc.livenessInfo().localExpirationTime());
            rowBuilder.addPrimaryKeyLivenessInfo(rowLiveness);
            DeletionTime rowDeletion = uDesc.deletionTime();
            rowBuilder.addRowDeletion(rowDeletion.isLive()
                                      ? Row.Deletion.LIVE
                                      : Row.Deletion.regular(copyOf(rowDeletion)));

            SSTableCursorReader.CellCursor cc = cursor.cellCursor();
            currentComplexColumn = null;
            while (true)
            {
                if (isState(state, UNFILTERED_END | PARTITION_END | DONE))
                    return state;
                if (state == CELL_END)
                {
                    state = cursor.continueReading();
                    continue;
                }
                if (state != CELL_HEADER_START)
                    throw new IllegalStateException("unexpected cursor state " + state);

                state = cursor.readCellHeader();
                if (!isState(state, CELL_VALUE_START | CELL_END))
                    continue; // row tail consumed by the dropped-column filter: no position surfaced

                ColumnMetadata column = cc.cellColumn;
                if (column.isComplex() && !sameColumn(column, currentComplexColumn))
                {
                    // entering a new complex column.  Like UnfilteredSerializer.readComplexColumn:
                    // prime the per-column caches and record the column's surviving deletion.  The
                    // helper's check covers the table-metadata dropped-column horizon.
                    currentComplexColumn = column;
                    filterHelper.startOfComplexColumn(column);
                    if (filterHelper.includes(column)
                        && !cc.complexDeletion.isLive()
                        && !filterHelper.isDroppedComplexDeletion(cc.complexDeletion))
                    {
                        rowBuilder.addComplexDeletion(column, copyOf(cc.complexDeletion));
                    }
                }
                if (!cc.producedCell)
                    continue; // deletion-only complex column: its deletion was recorded above

                if (!filterHelper.includes(column))
                {
                    // a non-fetched column materializes nothing, so skip the value without
                    // snapshotting cell state or copying the cell path
                    if (state == CELL_VALUE_START)
                        state = cursor.skipCellValue();
                    continue;
                }

                // snapshot reusable cell state before any further cursor advance
                AbstractType<?> cellType = cc.cellType;
                long timestamp = cc.cellLiveness.timestamp();
                int ttl = cc.cellLiveness.ttl();
                long localDeletionTime = cc.cellLiveness.localDeletionTime();
                CellPath path = cc.cellPathLength < 0
                                ? null
                                : CellPath.create(ByteBuffer.wrap(Arrays.copyOf(cc.cellPathBuffer, cc.cellPathLength)));

                byte[] value = ByteArrayAccessor.instance.empty();
                if (state == CELL_VALUE_START)
                {
                    // mirrors Cell.Serializer.deserialize: a fetched-but-not-queried column (or
                    // non-queried collection path) keeps the cell but skips its value
                    if (filterHelper.canSkipValue(column) || (path != null && filterHelper.canSkipValue(path)))
                    {
                        state = cursor.skipCellValue();
                    }
                    else
                    {
                        int fixedLength = cellType.valueLengthIfFixed();
                        if (fixedLength >= 0)
                        {
                            // the final value array IS the transfer buffer: copyCellValue's single
                            // readFully lands the bytes in place, one copy total
                            byte[] target = fixedLength == 0 ? ByteArrayAccessor.instance.empty() : new byte[fixedLength];
                            state = cursor.copyCellValue(transfer.valueCapture.prepareFixed(target), target);
                        }
                        else
                        {
                            state = cursor.copyCellValue(transfer.valueCapture.prepareVariable(), transfer.transferBuffer);
                        }
                        value = transfer.valueCapture.finish();
                    }
                }

                if (TEST_CORRUPT_CELL_TIMESTAMPS)
                    timestamp += 1;

                Cell<byte[]> cell = ByteArrayAccessor.instance.factory()
                                                     .cell(column, timestamp, ttl, localDeletionTime, value, path);
                if (filterHelper.includes(cell, rowLiveness) && !filterHelper.isDropped(cell, column.isComplex()))
                    rowBuilder.addCell(cell);
            }
        }

        /** Encoded size in bytes of the unsigned vint starting at {@code data[offset]}. */
        private static int vintSize(byte[] data, int offset)
        {
            int firstByte = data[offset];
            return firstByte >= 0 ? 1 : 1 + VIntCoding.numberOfExtraBytesToRead(firstByte);
        }

        private RangeTombstoneMarker materializeMarker()
        {
            ClusteringPrefix<?> prefix = toClusteringPrefix();
            if (uDesc.isBoundary())
            {
                // loadTombstone reads CLOSE (deletionTime) then OPEN (deletionTime2)
                return new RangeTombstoneBoundaryMarker((ClusteringBoundary<?>) prefix,
                                                        copyOf(uDesc.deletionTime()),
                                                        copyOf(uDesc.deletionTime2()));
            }
            return new RangeTombstoneBoundMarker((ClusteringBound<?>) prefix,
                                                 copyOf(uDesc.deletionTime()));
        }

        /**
         * Allocation-lean equivalent of {@link ClusteringDescriptor#toClusteringPrefix(List)}:
         * decodes the descriptor's clustering bytes straight off its backing array instead of
         * round-tripping through a fresh {@code DataInputBuffer} per row.  Implemented here rather
         * than in {@code ClusteringDescriptor}, which is shared with cursor-compaction code.
         */
        private ClusteringPrefix<?> toClusteringPrefix()
        {
            if (uDesc.clusteringKind() == ClusteringPrefix.Kind.CLUSTERING)
            {
                return clusteringTypes.isEmpty()
                       ? ByteArrayAccessor.factory.clustering()
                       : ByteArrayAccessor.factory.clustering(readClusteringValues(clusteringTypes.size()));
            }
            int bound = uDesc.clusteringColumnsBound();
            return bound == 0
                   ? ByteArrayAccessor.factory.bound(uDesc.clusteringKind())
                   : ByteArrayAccessor.factory.boundOrBoundary(uDesc.clusteringKind(), readClusteringValues(bound));
        }

        /** Zero-component bound/boundary sentinel: the factory builds these from its per-kind
         *  singletons, so no values array is ever decoded for them. */
        private static final byte[][] NO_CLUSTERING_VALUES = new byte[0][];

        /**
         * Decodes the descriptor's bound/boundary clustering values, kind-agnostic.  The merged
         * marker's kind is computed by the merge, which may need the same values under two kinds, so
         * values are decoded once here and prefixes of any kind are built over them by
         * {@code CursorReadMerger.boundOrBoundary}.
         */
        private byte[][] boundValues()
        {
            int bound = uDesc.clusteringColumnsBound();
            return bound == 0 ? NO_CLUSTERING_VALUES : readClusteringValues(bound);
        }

        /**
         * Mirrors {@code ClusteringPrefix.Serializer.deserializeValuesWithoutSize} over the
         * descriptor's raw byte array — same wire format ({@code readUnfilteredClustering} stores a
         * header vint per 32-component block, then raw bytes for fixed-length components and
         * vint-length-prefixed bytes for variable-length ones), same null/empty header-bit semantics
         * ({@code 1L << (i*2+1)} = null, {@code 1L << (i*2)} = empty; Java's mod-64 shift matches
         * the serializer's own indexing for components >= 32). The only allocations are the ones the
         * iterator path also makes: the values array and one {@code byte[]} per non-null/non-empty
         * component.
         */
        private byte[][] readClusteringValues(int size)
        {
            byte[] data = uDesc.clusteringBytes();
            int limit = uDesc.clusteringLength();
            int maxValueSize = DatabaseDescriptor.getMaxValueSize();
            byte[][] values = new byte[size][];
            long header = 0;
            int offset = 0;
            for (int i = 0; i < size; i++)
            {
                if ((i % 32) == 0)
                {
                    if (offset >= limit)
                        throw new IllegalStateException("truncated clustering bytes: header block at " + offset + " past limit " + limit);
                    header = VIntCoding.getUnsignedVInt(data, ByteArrayAccessor.instance, offset, limit);
                    offset += vintSize(data, offset);
                }
                if ((header & (1L << ((i * 2) + 1))) != 0) // null bit
                {
                    values[i] = null;
                    continue;
                }
                if ((header & (1L << (i * 2))) != 0) // empty bit
                {
                    values[i] = ByteArrayUtil.EMPTY_BYTE_ARRAY;
                    continue;
                }
                int length = clusteringTypes.get(i).valueLengthIfFixed();
                if (length < 0)
                {
                    if (offset >= limit)
                        throw new IllegalStateException("truncated clustering bytes: length vint for component " + i + " at " + offset + " past limit " + limit);
                    length = VIntCoding.checkedCast(VIntCoding.getUnsignedVInt(data, ByteArrayAccessor.instance, offset, limit));
                    if (length > maxValueSize)
                        throw new IllegalStateException(String.format("Corrupt value length %d encountered, as it exceeds the maximum of %d, " +
                                                                      "which is set via max_value_size in cassandra.yaml",
                                                                      length, maxValueSize));
                    offset += vintSize(data, offset);
                }
                if (offset + length > limit)
                    throw new IllegalStateException("truncated clustering bytes: component " + i + " of length " + length + " at " + offset + " past limit " + limit);
                values[i] = Arrays.copyOfRange(data, offset, offset + length);
                offset += length;
            }
            return values;
        }
    }

    // like AbstractCell.hasInvalidDeletions(); a copy of StatefulCursor's descriptor-level helper
    static boolean hasInvalidCellDeletion(int ttl, long localExpirationTime)
    {
        return ttl < 0
               || localExpirationTime == Cell.INVALID_DELETION_TIME
               || localExpirationTime < 0
               || (ttl != Cell.NO_TTL && localExpirationTime == Cell.NO_DELETION_TIME);
    }

    // like the primary-key liveness clause of AbstractRow.hasInvalidDeletions()
    static boolean hasInvalidRowLiveness(int ttl, long localExpirationTime)
    {
        return ttl != Cell.NO_TTL && (ttl < 0 || localExpirationTime < 0);
    }

    /**
     * Column identity across sources (copy of {@link ColumnMetadata#sameName}): different sstables
     * can carry different ColumnMetadata instances for the same column in their open-time
     * serialization headers, so reference identity alone is wrong across sources; identity stays
     * the fast path.
     */
    static boolean sameColumn(ColumnMetadata a, ColumnMetadata b)
    {
        return a == b || (a != null && b != null && a.name.equals(b.name));
    }

    /**
     * The merge-source contract {@code CursorReadMerger} (and {@link #mergeLegs}) consumes.  Two
     * implementations:
     * <ul>
     *   <li>{@link PendingLeg} — byte-backed: an sstable leg driven by {@link SSTableCursorReader}
     *       over reusable descriptors;</li>
     *   <li>{@link MemtableMergeLeg} — object-backed: wraps the memtable leg's
     *       {@code UnfilteredRowIterator}, presenting each live {@code Row}/{@code Cell}/marker as
     *       descriptor-shaped state, with {@link #existingCell()}/{@link #consumeExistingRow()}
     *       escape hatches so memtable-won data is emitted as the live object, not a rebuilt copy.</li>
     * </ul>
     *
     * Contract notes:
     * <ul>
     *   <li>{@link #cursorState()} speaks {@link SSTableCursorReader.State} regardless of backing:
     *       {@code ROW_START}/{@code TOMBSTONE_START} before {@link #readUnfilteredHeader()},
     *       {@code UNFILTERED_END} once the current unfiltered is consumed (then
     *       {@link #continueReading()} advances), {@code PARTITION_END}/{@code DONE} at exhaustion.</li>
     *   <li>{@link #unfiltered()} is a reusable descriptor valid until the next header load; its
     *       clustering bytes are always in {@code serializeValuesWithoutSize} wire form, so the
     *       shared {@code ClusteringComparator.compare} works across leg kinds.</li>
     *   <li>Cell positions surface in merge order, pre-filtered per leg as the leg's data reaches
     *       the object merge today.</li>
     *   <li>The escape hatches return null on byte-backed legs; non-null returns are immutable
     *       objects the merged output may retain.</li>
     * </ul>
     */
    interface MergeLeg extends AutoCloseable
    {
        // ---- partition-level surface (consumed by mergeLegs) ----
        DeletionTime partitionLevelDeletion();
        Row staticRow();
        Slices legSlices();
        EncodingStats legStats();
        /** The backing sstable, or null for an object-backed (memtable) leg. */
        SSTableReader sstableOrNull();
        void enterMergeMode();
        void seekForMerge() throws IOException;
        DeletionTime mergeSeekOpenMarker();

        // ---- unfiltered walk (consumed by CursorReadMerger) ----
        int cursorState();
        UnfilteredDescriptor unfiltered();
        void readUnfilteredHeader() throws IOException;
        void continueReading() throws IOException;
        ClusteringPrefix<?> materializeClusteringPrefix();
        byte[][] materializeBoundValues();
        /**
         * Whole-row escape hatch: when this leg's current row can stand as the merged row as-is
         * (the caller has verified the {@code Row.Merger} single-version fast path), returns the
         * live row object and consumes the leg's current unfiltered.  Returns null on byte-backed
         * legs, which always take the general cell-walk path.
         */
        Row consumeExistingRow();

        // ---- cell walk ----
        boolean parkedAtCellPosition();
        boolean needsCellAdvance();
        void ensureParkedAtCell(boolean validateCells) throws IOException;
        void advancePastCellPosition() throws IOException;
        boolean cellProduced();
        ColumnMetadata cellColumn();
        CellLivenessInfo cellLiveness();
        ByteBuffer cellPathWindow();
        DeletionTime cellComplexDeletion();
        CellPath cellPath();
        /** Cell escape hatch: the parked cell as a live object the merged output can reuse
         *  directly, or null on byte-backed legs. */
        Cell<?> existingCell();
        byte[] cellValue() throws IOException;
        void stageCellValue(DataOutputPlus scratch) throws IOException;
        void discardCellValue() throws IOException;
        /**
         * Whether the parked cell has any value bytes on the wire, like
         * {@code Cell.Serializer.HAS_EMPTY_VALUE_MASK}.  Decidable before consuming
         * {@link #cellValue()}/{@link #stageCellValue}, so a streaming {@code MergeSink} can learn
         * this without staging first.  Must be called before either of those consumes the value.
         */
        boolean cellHasValue();

        // ---- per-leg corrupted-tombstone validation (no-ops on memtable legs: the iterator path
        // never applies UnfilteredValidation to memtable data, only to sstable-attributed reads) ----
        void validateRowHeader();
        void validateMarkerHeader();

        @Override
        void close();
    }

    /**
     * One opened sstable leg of a cursor-served single-partition read: cursor open, partition
     * header read and validated, static row materialized, rows not yet touched.  The call site
     * collects these across the {@code mostRecentPartitionTombstone} elimination loop, then finishes
     * them through {@link CursorReads#completeSingleLeg} (1 leg) or {@link CursorReads#mergeLegs}
     * ({@code >= 2} legs).
     *
     * In merge mode this class is the byte-backed {@link MergeLeg} implementation.
     */
    public static final class PendingLeg implements MergeLeg
    {
        final SSTableReader sstable;
        final TableMetadata metadata;
        final DecoratedKey key;
        final Slices legSlices;
        final ColumnFilter columnFilter;
        private final BtiCursorSeekSupport.PartitionEntry btiEntry;
        /** the query's shared value-transfer scratch (one instance across all of a read's legs) */
        private final ValueTransfer transfer;

        private SSTableCursorReader cursor; // null once closed
        private PartitionMaterializer materializer;
        private int openState;
        /** see {@link #mergeSeekOpenMarker()} */
        private DeletionTime mergeSeekOpenMarker;

        // ---- merge-mode cell-walk state (meaningful only after enterMergeMode) ----
        /** The physically-parked cell was read off the wire but not yet filter-evaluated (set when
         *  a synthetic park interposed before it). */
        private boolean evaluatePendingCell;
        private CellPath pendingPath;
        /** Zero-copy filter-test view over the cell cursor's reusable path window (see
         *  {@link #filterPathView}); re-created only when the window is re-wrapped over a
         *  grown path buffer, never per cell. */
        private CellPath filterPathView;
        private ByteBuffer filterPathWindow;
        /** canSkipValue verdict for the parked cell: its value must reconcile as EMPTY (the
         *  iterator path never deserializes it) and must not be materialized if the cell wins. */
        private boolean pendingValueSkip;
        private byte[] pendingValue;
        private boolean pendingValueMaterialized;
        private ColumnMetadata primedComplexColumn;
        /**
         * Synthetic deletion-only park: a complex column whose deletion survives the per-leg
         * filters but whose cells were ALL filtered out per-leg. The iterator path still emits a
         * deletion-only ComplexColumnData for it, so it must enter the merge as a deletion-only
         * POSITION at its column-order place — this leg parks here synthetically (no underlying
         * cursor position) before its next real position.
         */
        private boolean syntheticPark;
        private ColumnMetadata syntheticColumn;
        private final DeletionTime.ReusableDeletionTime syntheticDeletion = DeletionTime.ReusableDeletionTime.live();
        /** syntheticColumn/Deletion captured while walking the column's cells; becomes a synthetic
         *  park if no cell of the column survives the per-leg filters. */
        private boolean stashPending;

        PendingLeg(SSTableReader sstable, TableMetadata metadata, DecoratedKey key,
                   Slices legSlices, ColumnFilter columnFilter, BtiCursorSeekSupport.PartitionEntry btiEntry,
                   ValueTransfer transfer)
        {
            this.sstable = sstable;
            this.metadata = metadata;
            this.key = key;
            this.legSlices = legSlices;
            this.columnFilter = columnFilter;
            this.btiEntry = btiEntry;
            this.transfer = transfer;
        }

        void open(long position) throws IOException
        {
            // The bounds constructor seeks to the partition and validates the end-of-partition
            // marker before it, which is the reader's own single implementation of that seek; the
            // upper bound is the file length, so reading past this partition behaves as before.
            cursor = new SSTableCursorReader(sstable,
                                             Collections.singletonList(new PartitionPositionBounds(position, sstable.uncompressedLength())),
                                             null);
            materializer = new PartitionMaterializer(cursor, sstable, metadata, key, columnFilter, transfer);
            openState = materializer.openPartition(position);
            if (legSlices.isEmpty())
            {
                // a Slices.NONE leg (partition-deletion check / statics-only shapes) never needs
                // rows — on the iterator path its Slices.NONE iterator emits no unfiltereds either
                SSTableCursorReader toClose = cursor;
                cursor = null;
                toClose.close();
            }
        }

        public DeletionTime partitionLevelDeletion()
        {
            return materializer.partitionDeletion();
        }

        public Row staticRow()
        {
            return materializer.staticRow();
        }

        public Slices legSlices()
        {
            return legSlices;
        }

        public EncodingStats legStats()
        {
            return sstable.stats();
        }

        public SSTableReader sstableOrNull()
        {
            return sstable;
        }

        /** Byte-backed legs never stand in for a whole merged row: descriptor state is not a
         *  reusable live object, so the general materializing path always runs. */
        public Row consumeExistingRow()
        {
            return null;
        }

        /** Byte-backed legs have no live cell object to reuse; winners materialize through
         *  {@link #cellValue()}/{@link #cellPath()}. */
        public Cell<?> existingCell()
        {
            return null;
        }

        /** Single-leg completion; see {@link CursorReads#completeSingleLeg}. */
        MaterializedPartition completeSingle() throws IOException
        {
            if (cursor == null) // Slices.NONE leg: no rows by construction
                return new MaterializedPartition(partitionLevelDeletion(), staticRow(), new ArrayList<>(), null);

            BtiCursorSeekSupport.SeekPoint seekPoint = null;
            ClusteringBound<?> endBound = null;
            if (btiEntry != null && btiEntry.isIndexed() && legSlices.size() == 1)
            {
                // bounded materialization, BTI only.  A full-partition slice (BOTTOM..TOP) leaves
                // both of these null and keeps the eager walk.
                Slice slice = legSlices.get(0);
                if (!slice.end().isTop())
                    endBound = slice.end();
                if (!slice.start().isBottom())
                    seekPoint = BtiCursorSeekSupport.floorBlock((BtiTableReader) sstable, btiEntry,
                                                                metadata.comparator, slice.start());
            }
            return materializer.readRows(openState, !legSlices.isEmpty(), seekPoint, endBound);
        }

        /**
         * Switches this leg's cursor to merge consumption: deletion-only complex columns stay
         * sortable positions ({@code pauseAtEmptyComplexColumns}), but merged rows are built by the
         * merge sink and complex deletions are reconciled across sources first.  The row-index seek
         * applies to merged legs via {@link #seekForMerge}, called right after this.
         */
        public void enterMergeMode()
        {
            assert cursor != null;
            cursor.pauseAtEmptyComplexColumns(true);
        }

        /**
         * The merge-mode counterpart of {@link #completeSingle}'s row-index seek: same gate (BTI,
         * row-indexed partition, single slice with a real start bound), same forward-only
         * floor-block jump.  Positions this leg's cursor at its floor block for the slice start so
         * the k-way merge never walks the partition prefix; rows between the block start and the
         * slice start still enter the merge and are dropped by the post-merge slicer's pre-slice skip.
         *
         * The range-tombstone deletion open at the seek point is the one piece of leg state a
         * mid-partition entry cannot recover from the stream, so it is retained in
         * {@link #mergeSeekOpenMarker} for {@code CursorReadMerger} to seed into its cross-leg
         * open-marker set.  Misaligned per-leg seek points are safe: a leg's seed is valid from its
         * floor block onward, and any close marker for the seeded deletion arrives in this leg's own
         * post-seek stream.
         *
         * No-op for BIG legs, unindexed partitions, full-partition slices, and legs already at or
         * past the floor block.  A merge may mix seeked BTI legs with unseeked BIG legs.
         */
        public void seekForMerge() throws IOException
        {
            if (btiEntry == null || !btiEntry.isIndexed() || legSlices.size() != 1)
                return;
            Slice slice = legSlices.get(0);
            if (slice.start().isBottom())
                return;
            // PARTITION_END/DONE: the partition has no unfiltereds to seek over
            if (!isState(cursor.state(), ROW_START | TOMBSTONE_START))
                return;
            BtiCursorSeekSupport.SeekPoint seekPoint = BtiCursorSeekSupport.floorBlock((BtiTableReader) sstable, btiEntry,
                                                                                      metadata.comparator, slice.start());
            // the current unfiltered's flags byte is already consumed, so its start is one byte
            // behind the cursor.  Like ForwardIndexedReader.setForSlice, only seek forward; when the
            // floor block is the one already being read, keep the sequential walk.
            if (seekPoint.dataPosition <= cursor.position() - 1)
                return;
            cursor.seekUnfiltered(seekPoint.dataPosition);
            SSTABLE_LEG_ROW_INDEX_SEEKS.incrementAndGet();
            if (TEST_DROP_MERGE_SEEK_OPEN_MARKER || seekPoint.openMarker == null)
                return;
            mergeSeekOpenMarker = TEST_SKEW_MERGE_SEEK_OPEN_MARKER
                                  ? DeletionTime.buildUnsafeWithUnsignedInteger(seekPoint.openMarker.markedForDeleteAt() + 1,
                                                                               seekPoint.openMarker.localDeletionTimeUnsignedInteger())
                                  : copyOf(seekPoint.openMarker);
        }

        /** The open-range-tombstone deletion at this leg's merge-mode seek point (immutable copy
         *  of the BTI row index's {@code IndexInfo.openDeletion}), or null when no merge-mode
         *  seek was issued or no deletion is open at the target block start. */
        public DeletionTime mergeSeekOpenMarker()
        {
            return mergeSeekOpenMarker;
        }

        // ---- merge-source surface (consumed by CursorReadMerger) ----

        int openStateAfterHeader()
        {
            return openState;
        }

        public int cursorState()
        {
            return cursor.state();
        }

        public UnfilteredDescriptor unfiltered()
        {
            return materializer.uDesc;
        }

        /** Loads the row/marker header at the current unfiltered start into {@link #unfiltered()}
         *  and resets the per-row cell-walk state. */
        public void readUnfilteredHeader() throws IOException
        {
            int s = cursor.state();
            if (s == ROW_START)
                cursor.readRowHeader(materializer.uDesc);
            else if (s == TOMBSTONE_START)
                cursor.readTombstoneMarker(materializer.uDesc);
            else
                throw new IllegalStateException("unexpected cursor state " + s);
            primedComplexColumn = null;
            stashPending = false;
            syntheticPark = false;
            evaluatePendingCell = false;
        }

        public void continueReading() throws IOException
        {
            cursor.continueReading();
        }

        public ClusteringPrefix<?> materializeClusteringPrefix()
        {
            return materializer.toClusteringPrefix();
        }

        /** The current bound/boundary descriptor's clustering values, decoded ONCE for reuse
         *  under different prefix kinds (see {@code CursorReadMerger.mergeMarkerGroup}). */
        public byte[][] materializeBoundValues()
        {
            return materializer.boundValues();
        }

        public boolean parkedAtCellPosition()
        {
            return syntheticPark || isState(cursor.state(), CELL_VALUE_START | CELL_END);
        }

        public boolean needsCellAdvance()
        {
            return !syntheticPark && (evaluatePendingCell || cursor.state() == CELL_HEADER_START);
        }

        /**
         * Parks this leg at its next merge-relevant cell position, applying the query's
         * {@code DeserializationHelper}/{@code ColumnFilter} rules per leg, below reconciliation.
         * Non-fetched columns and tester-excluded paths are skipped without snapshotting,
         * dropped-column cells are skipped, fetched-but-not-queried cells the row liveness already
         * covers are skipped (CASSANDRA-7085), and surviving fetched-not-queried cells are marked
         * value-skippable so reconciliation sees empty values.  Positions surfaced: real cells,
         * natural deletion-only complex columns, and synthetic deletion-only parks (see
         * {@link #syntheticPark}).
         *
         * @param validateCells when true, surviving cells and complex deletions are validated per
         *        leg, like the iterator path's per-leg {@code maybeValidateUnfiltered} coverage
         */
        public void ensureParkedAtCell(boolean validateCells) throws IOException
        {
            if (syntheticPark)
                return;
            DeserializationHelper helper = materializer.filterHelper;
            boolean evaluate = evaluatePendingCell;
            evaluatePendingCell = false;
            for (;;)
            {
                if (!evaluate)
                {
                    if (cursor.state() != CELL_HEADER_START)
                    {
                        // row exhausted (or already parked): a still-pending stashed complex
                        // deletion must surface as a deletion-only position first
                        if (stashPending && !isState(cursor.state(), CELL_VALUE_START | CELL_END))
                            issueSyntheticPark();
                        return;
                    }
                    cursor.readCellHeader();
                    if (!isState(cursor.state(), CELL_VALUE_START | CELL_END))
                    {
                        // the cursor's internal dropped-column filter consumed the row tail
                        if (stashPending)
                            issueSyntheticPark();
                        return;
                    }
                }
                evaluate = false;

                SSTableCursorReader.CellCursor cc = cursor.cellCursor();
                ColumnMetadata column = cc.cellColumn;
                if (!cc.producedCell)
                {
                    // natural deletion-only complex-column position (pauseAtEmptyComplexColumns)
                    if (stashPending && !sameColumn(syntheticColumn, column))
                    {
                        issueSyntheticPark();
                        evaluatePendingCell = true;
                        return;
                    }
                    primeComplexColumn(helper, column);
                    if (helper.includes(column))
                    {
                        if (validateCells && !cc.complexDeletion.isLive()
                            && !helper.isDroppedComplexDeletion(cc.complexDeletion)
                            && !cc.complexDeletion.validate())
                            UnfilteredValidation.handleInvalid(metadata, key, sstable,
                                                              "complexDeletion=" + cc.complexDeletion);
                        return; // parks; the contribution is folded (dropped-filtered) by the merge
                    }
                    // non-fetched column: consume the position, materializing nothing (parity with
                    // the iterator path's skipped column)
                    if (cursor.state() == CELL_END)
                        cursor.continueReading();
                    continue;
                }

                boolean isComplex = column.isComplex();
                if (isComplex && !sameColumn(column, primedComplexColumn))
                {
                    // entering a new complex column run: a stash from a previous column must park
                    // before this column's first cell to keep column order
                    if (stashPending)
                    {
                        issueSyntheticPark();
                        evaluatePendingCell = true;
                        return;
                    }
                    primeComplexColumn(helper, column);
                    if (helper.includes(column) && !cc.complexDeletion.isLive()
                        && !helper.isDroppedComplexDeletion(cc.complexDeletion))
                    {
                        stashPending = true;
                        syntheticColumn = column;
                        syntheticDeletion.reset(cc.complexDeletion);
                        if (validateCells && !cc.complexDeletion.validate())
                            UnfilteredValidation.handleInvalid(metadata, key, sstable,
                                                              "complexDeletion=" + cc.complexDeletion);
                    }
                }
                else if (!isComplex && stashPending)
                {
                    issueSyntheticPark();
                    evaluatePendingCell = true;
                    return;
                }

                pendingPath = null;
                pendingValue = null;
                pendingValueMaterialized = false;
                pendingValueSkip = false;
                if (!helper.includes(column))
                {
                    skipParkedCell();
                    continue;
                }
                CellPath path = null;
                if (isComplex)
                {
                    // filter tests only read the path during the call and retain nothing, so a
                    // zero-copy view over the reusable path window suffices; the real
                    // materialization stays lazy in cellPath(), paid only for cells that win
                    path = filterPathView(cc);
                    if (!helper.includes(path))
                    {
                        skipParkedCell();
                        continue;
                    }
                }
                if (helper.isDropped(column, cc.cellLiveness.timestamp(), isComplex))
                {
                    skipParkedCell();
                    continue;
                }
                // a fetched-but-not-queried cell is skipped when the row's own liveness covers it
                long rowTimestamp = materializer.uDesc.livenessInfo().timestamp();
                pendingValueSkip = isComplex ? helper.canSkipValue(path) : helper.canSkipValue(column);
                if (pendingValueSkip && cc.cellLiveness.timestamp() < rowTimestamp)
                {
                    skipParkedCell();
                    continue;
                }
                if (stashPending && sameColumn(syntheticColumn, column))
                    stashPending = false; // a surviving cell's park carries the column deletion itself
                if (validateCells && hasInvalidCellDeletion(cc.cellLiveness.ttl(), cc.cellLiveness.localDeletionTime()))
                    UnfilteredValidation.handleInvalid(metadata, key, sstable, "cellLiveness=" + cc.cellLiveness);
                return;
            }
        }

        private void primeComplexColumn(DeserializationHelper helper, ColumnMetadata column)
        {
            helper.startOfComplexColumn(column);
            primedComplexColumn = column;
        }

        private void issueSyntheticPark()
        {
            syntheticPark = true;
            stashPending = false;
        }

        private void skipParkedCell() throws IOException
        {
            if (cursor.state() == CELL_VALUE_START)
                cursor.skipCellValue();
            if (cursor.state() == CELL_END)
                cursor.continueReading();
        }

        /** Consumes the current cell position after its merge group was reconciled. */
        public void advancePastCellPosition() throws IOException
        {
            if (syntheticPark)
            {
                syntheticPark = false;
                // the physically-parked position behind the synthetic park was read but never
                // evaluated; the next ensureParkedAtCell resumes the filter walk from it
                if (isState(cursor.state(), CELL_VALUE_START | CELL_END))
                    evaluatePendingCell = true;
                return;
            }
            if (cursor.state() == CELL_VALUE_START)
                throw new IllegalStateException("cell value neither consumed nor skipped before advancing");
            if (cursor.state() == CELL_END)
                cursor.continueReading();
        }

        public boolean cellProduced()
        {
            return !syntheticPark && cursor.cellCursor().producedCell;
        }

        public ColumnMetadata cellColumn()
        {
            return syntheticPark ? syntheticColumn : cursor.cellCursor().cellColumn;
        }

        public CellLivenessInfo cellLiveness()
        {
            return cursor.cellCursor().cellLiveness;
        }

        public ByteBuffer cellPathWindow()
        {
            return cursor.cellCursor().cellPathWindow();
        }

        /** This leg's complex-deletion contribution at the current parked position — already
         *  per-leg filtered (LIVE when the dropped-column filter discards it, mirroring the
         *  iterator path's readComplexColumn). */
        public DeletionTime cellComplexDeletion()
        {
            if (syntheticPark)
                return syntheticDeletion;
            SSTableCursorReader.CellCursor cc = cursor.cellCursor();
            if (cc.complexDeletion.isLive() || materializer.filterHelper.isDroppedComplexDeletion(cc.complexDeletion))
                return DeletionTime.LIVE;
            return cc.complexDeletion;
        }

        /** The parked cell's path, materialized once (null for simple columns). */
        public CellPath cellPath()
        {
            if (pendingPath != null)
                return pendingPath;
            SSTableCursorReader.CellCursor cc = cursor.cellCursor();
            return cc.cellPathLength < 0 ? null : materializePendingPath(cc);
        }

        private CellPath materializePendingPath(SSTableCursorReader.CellCursor cc)
        {
            pendingPath = CellPath.create(ByteBuffer.wrap(Arrays.copyOf(cc.cellPathBuffer, cc.cellPathLength)));
            return pendingPath;
        }

        /**
         * Filter-test view of the parked cell's path: wraps the cell cursor's reusable path
         * window with no copying. The window's contents are only stable until the next cell
         * header is read, so this view must never escape the immediate filter-test call —
         * anything that outlives the park goes through {@link #cellPath()} instead.
         */
        private CellPath filterPathView(SSTableCursorReader.CellCursor cc)
        {
            ByteBuffer window = cc.cellPathWindow();
            if (window != filterPathWindow)
            {
                filterPathWindow = window;
                filterPathView = CellPath.create(window);
            }
            return filterPathView;
        }

        /**
         * The parked cell's raw value bytes, materialized once through the one-copy
         * {@link CellValueCapture} machinery.  Empty for valueless cells and for value-skippable
         * (fetched-but-not-queried) cells, which the iterator path reconciles on empty values.
         */
        public byte[] cellValue() throws IOException
        {
            if (pendingValueMaterialized)
                return pendingValue;
            byte[] value = ByteArrayAccessor.instance.empty();
            if (cursor.state() == CELL_VALUE_START)
            {
                if (pendingValueSkip)
                {
                    cursor.skipCellValue();
                }
                else
                {
                    SSTableCursorReader.CellCursor cc = cursor.cellCursor();
                    int fixedLength = cc.cellType.valueLengthIfFixed();
                    if (fixedLength >= 0)
                    {
                        byte[] target = fixedLength == 0 ? ByteArrayAccessor.instance.empty() : new byte[fixedLength];
                        cursor.copyCellValue(transfer.valueCapture.prepareFixed(target), target);
                        value = transfer.valueCapture.finish();
                    }
                    else
                    {
                        cursor.copyCellValue(transfer.valueCapture.prepareVariable(), transfer.transferBuffer);
                        value = transfer.valueCapture.finish();
                    }
                }
            }
            pendingValue = value;
            pendingValueMaterialized = true;
            return value;
        }

        /**
         * Streams the parked cell's raw value bytes into {@code scratch} without materializing a
         * final value array, in the same form {@link #cellValue()} produces.  Used by the merge's
         * tie-break comparison, where most staged values lose the tie and a fresh array would be
         * immediate garbage.  The caller promotes the winning scratch bytes to a real array; a
         * loser's bytes are overwritten by the next stage.
         */
        public void stageCellValue(DataOutputPlus scratch) throws IOException
        {
            if (pendingValueMaterialized)
            {
                // defensive: already materialized, which cannot happen mid-tie today
                scratch.write(pendingValue, 0, pendingValue.length);
                return;
            }
            if (cursor.state() == CELL_VALUE_START)
            {
                if (pendingValueSkip)
                    cursor.skipCellValue(); // reconciles as EMPTY, like cellValue()
                else
                    cursor.copyCellValue(scratch, transfer.transferBuffer);
            }
        }

        /**
         * Whether the parked cell has any value bytes from the output's point of view, gating on
         * the same conditions as {@link #cellValue()}, checked before consuming.  False for a
         * synthetic (deletion-only) park, a cell with no value section on the wire, and a
         * value-skippable cell ({@link #pendingValueSkip}), which {@link #cellValue()} exposes as
         * empty even though real bytes sit on the wire.
         */
        public boolean cellHasValue()
        {
            return !syntheticPark && cursor.state() == CELL_VALUE_START && !pendingValueSkip;
        }

        /** Discards the parked cell's pending value bytes, if any remain unconsumed. */
        public void discardCellValue() throws IOException
        {
            if (!syntheticPark && cursor.state() == CELL_VALUE_START)
                cursor.skipCellValue();
        }

        // ---- per-leg corrupted-tombstone validation (merge mode) ----

        public void validateRowHeader()
        {
            UnfilteredDescriptor uDesc = materializer.uDesc;
            if (!uDesc.deletionTime().validate())
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "rowDeletion=" + uDesc.deletionTime());
            if (hasInvalidRowLiveness(uDesc.livenessInfo().ttl(), uDesc.livenessInfo().localExpirationTime()))
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "rowLiveness=" + uDesc.livenessInfo());
        }

        public void validateMarkerHeader()
        {
            UnfilteredDescriptor uDesc = materializer.uDesc;
            if (!uDesc.deletionTime().validate())
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "rangeTombstoneDeletion=" + uDesc.deletionTime());
            if (uDesc.isBoundary() && !uDesc.deletionTime2().validate())
                UnfilteredValidation.handleInvalid(metadata, key, sstable, "rangeTombstoneDeletion2=" + uDesc.deletionTime2());
        }

        @Override
        public void close()
        {
            if (cursor != null)
            {
                SSTableCursorReader toClose = cursor;
                cursor = null;
                toClose.close();
            }
        }
    }

    /**
     * The read-side merge sink: clustering-first events (see {@code CursorReadMerger.MergeSink})
     * materialized into {@code Row}/{@code Unfiltered} objects.  One reused
     * {@code BTreeRow.sortedBuilder} serves all merged rows; rows that merge to empty are dropped
     * like {@code Row.Merger} returning null.
     */
    static final class MaterializingMergeSink implements CursorReadMerger.MergeSink
    {
        private final Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        private final List<Unfiltered> unfiltereds = new ArrayList<>();
        private long materializedCount;

        @Override
        public void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedDeletion)
        {
            rowBuilder.newRow(clustering);
            rowBuilder.addPrimaryKeyLivenessInfo(mergedLiveness);
            rowBuilder.addRowDeletion(mergedDeletion.isLive()
                                      ? Row.Deletion.LIVE
                                      : Row.Deletion.regular(mergedDeletion));
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion)
        {
            rowBuilder.addComplexDeletion(column, mergedComplexDeletion);
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            rowBuilder.addCell(cell);
        }

        @Override
        public void endRow()
        {
            Row row = rowBuilder.build();
            // mirrors Row.Merger.merge returning null for a row that merged to nothing
            if (!row.isEmpty())
            {
                unfiltereds.add(row);
                materializedCount++;
            }
        }

        /** Discards the partially-built row of an abandoned group (a regular-column filter failure
         *  found mid-cell-walk or at row end).  build() resets the shared sorted builder for the
         *  next group. */
        void abandonRow()
        {
            rowBuilder.build();
        }

        @Override
        public void addRow(Row row)
        {
            // whole-row escape hatch: the merged row is this live memtable object, like
            // Row.Merger.merge's single-version fast path.  The empty-drop is defensive only.
            if (!row.isEmpty())
            {
                unfiltereds.add(row);
                materializedCount++;
            }
        }

        @Override
        public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            unfiltereds.add(marker);
            materializedCount++;
        }

        List<Unfiltered> unfiltereds()
        {
            return unfiltereds;
        }

        long materializedCount()
        {
            return materializedCount;
        }
    }

    /**
     * The production-bound decorator around {@link MaterializingMergeSink}: a conservative bound,
     * not a counter replacement.  It drives a detached copy of the authoritative
     * {@code DataLimits} counter ({@link #limitBoundFor} builds it with the same {@code newCounter}
     * arguments {@code ReadCommand.executeLocally} uses) over every merged row the slicer will
     * emit, and flips {@link #wantsMore()} once that counter is done, so the merge stops producing
     * where the top counter stops consuming.  The authoritative counter stays in place above, so
     * over-production is invisible; under-production is the only failure mode.
     *
     * Fidelity notes, all achieved by reusing the production counter object:
     * <ul>
     *   <li><b>Liveness</b>: rows are counted via {@code Counter.countRow}, the same predicate the
     *       top counter applies, {@code nowInSec} included.  The merge core stays nowInSec-free; the
     *       liveness semantics live in this decorator, on already-materialized rows.  The
     *       {@code withoutPurgeableTombstones} stage above cannot flip a verdict.</li>
     *   <li><b>Emitted-surface filter</b>: rows at or before the slice start are materialized but
     *       discarded by {@code SlicedMaterializedIterator}'s non-strict pre-slice skip, so the top
     *       counter never sees them.  {@code countsTowardLimit} applies the slicer's predicate
     *       (count iff strictly after the slice start).</li>
     *   <li><b>Paging resume</b>: {@code CQLPagingLimits}' counter seeds its per-partition count at
     *       {@code countPartition}, called by {@code mergeLegs} before any row event.</li>
     *   <li><b>Empty-merged rows</b>: a group that merges to nothing emits no events; a row the
     *       inner sink drops as empty is never counted.</li>
     *   <li><b>Markers and statics</b> never count and never stop, so marker events pass through.</li>
     * </ul>
     *
     * This class is unchanged when row-level filter pushdown is also engaged: it always wraps the
     * base {@link MaterializingMergeSink} directly, and {@code CursorReads.mergeLegs} makes
     * {@link RowLevelFilterProbe} the outer sink with this object as its {@code next}.  Because
     * {@code CursorReadMerger} never calls the top sink's {@code endRow} for an abandoned row,
     * {@link #counter} only counts rows that also survive the filter.
     */
    static final class LimitingMergeSink implements CursorReadMerger.MergeSink
    {
        private final MaterializingMergeSink inner;
        private final DataLimits.Counter counter;
        private final ClusteringComparator comparator;
        /** The emitted slice's start bound, or null when the slice starts at BOTTOM. */
        private final ClusteringBound<?> sliceStart;
        /** Sticky in-slice latch — the merged stream is clustering-ordered, so once one row is
         *  strictly past the slice start every later row is too and the comparison is skipped. */
        private boolean pastSliceStart;
        /** Whether the row currently being streamed (startRow..endRow) is on the emitted surface. */
        private boolean currentRowCounts;

        LimitingMergeSink(MaterializingMergeSink inner, DataLimits.Counter counter,
                          ClusteringComparator comparator, Slice slice)
        {
            this.inner = inner;
            this.counter = counter;
            this.comparator = comparator;
            this.sliceStart = slice.start().isBottom() ? null : slice.start();
            this.pastSliceStart = sliceStart == null;
        }

        /** Matches the slicer: {@code SlicedMaterializedIterator.computeNextInSlice} skips
         *  pre-slice data with a non-strict comparison, so only rows strictly after the slice
         *  start reach the top counter. */
        private boolean countsTowardLimit(ClusteringPrefix<?> clustering)
        {
            if (!pastSliceStart)
                pastSliceStart = comparator.compare(clustering, (ClusteringPrefix<?>) sliceStart) > 0;
            return pastSliceStart;
        }

        @Override
        public void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            currentRowCounts = countsTowardLimit(clustering);
            inner.startRow(clustering, mergedLiveness, mergedRowDeletion);
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion)
        {
            inner.addComplexDeletion(column, mergedComplexDeletion);
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            inner.addCell(cell);
        }

        @Override
        public void endRow()
        {
            long before = inner.materializedCount();
            inner.endRow();
            if (currentRowCounts && inner.materializedCount() != before)
            {
                List<Unfiltered> unfiltereds = inner.unfiltereds();
                counter.countRow((Row) unfiltereds.get(unfiltereds.size() - 1));
            }
        }

        @Override
        public void addRow(Row row)
        {
            inner.addRow(row);
            if (countsTowardLimit(row.clustering()))
                counter.countRow(row);
        }

        @Override
        public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            inner.addRangeTombstoneMarker(marker);
        }

        @Override
        public boolean wantsMore()
        {
            // isDoneForPartition is true when the top counter would have stopped: the row limit or
            // the per-partition limit is reached
            return !counter.isDoneForPartition();
        }
    }

    /**
     * Wire-level counterpart of {@code ReadCommand.withMetricsRecording}'s tombstone/live-row
     * accounting and tombstone-overwhelming abort, hooked into {@link TranscodeMergeSink}'s
     * purge-aware row/cell/marker call sites.  The transcode response path never runs
     * {@code executeLocally}'s stack, so without this a transcode-served read would skip both the
     * {@code tombstone_failure_threshold} abort and the scan histograms/counters a materializing
     * read produces.  The predicates below come from real source:
     * <ul>
     *   <li>{@link Cell#isLive(long, long, int)} for per-cell liveness;</li>
     *   <li>{@code BTreeRow}'s {@code minDeletionTime} family for the row-level
     *       {@code hasDeletion(nowInSec)} check;</li>
     *   <li>{@code ReadCommand}'s {@code MetricRecording}/{@code countTombstone} for the
     *       threshold/count/histogram shape.</li>
     * </ul>
     * This never touches the wire; it is a count/exception-parity concern, verified by a
     * differential scenario that forces the same abort from both the gate-on and gate-off paths.
     */
    static final class TombstoneScanGuard
    {
        private final ReadCommand command;
        private final TableMetrics metric;
        private final long startTimeNanos;
        private final long nowInSec;
        private final DecoratedKey key;
        private final int failureThreshold;
        private final int warningThreshold;
        private final boolean respectTombstoneThresholds;

        private int liveRows;
        private int tombstones;

        // per-row scratch, reset by startRow / wholeRow
        private long rowMinDeletionTime;
        private boolean rowLivenessLive;
        private boolean rowHasTombstoneCell;
        private ClusteringPrefix<?> rowClustering;

        TombstoneScanGuard(ReadCommand command, TableMetrics metric, long startTimeNanos, long nowInSec, DecoratedKey key)
        {
            this.command = command;
            this.metric = metric;
            this.startTimeNanos = startTimeNanos;
            this.nowInSec = nowInSec;
            this.key = key;
            this.failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            this.warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();
            this.respectTombstoneThresholds = !SchemaConstants.isLocalSystemKeyspace(command.metadata().keyspace);
        }

        void startRow(ClusteringPrefix<?> clustering, LivenessInfo liveness, DeletionTime rowDeletion)
        {
            rowClustering = clustering;
            rowMinDeletionTime = Math.min(minDeletionTime(liveness), minDeletionTime(rowDeletion));
            rowLivenessLive = liveness.isLive(nowInSec);
            rowHasTombstoneCell = false;
        }

        /** Only called for a non-live complex deletion (see
         *  {@link TranscodeMergeSink#addComplexDeletion}'s early return); its
         *  {@code minDeletionTime} is always {@code Long.MIN_VALUE}. */
        void complexDeletion()
        {
            rowMinDeletionTime = Long.MIN_VALUE;
        }

        /** timestamp, ttl and localDeletionTime are the post-purge values
         *  {@link TranscodeMergeSink} is about to write, what a materialize-then-purge
         *  {@code Cell} would report, including the expired-but-not-gcable convert to tombstone. */
        void cell(long timestamp, int ttl, long localDeletionTime)
        {
            boolean isTombstone = localDeletionTime != Cell.NO_DELETION_TIME && ttl == Cell.NO_TTL;
            rowMinDeletionTime = Math.min(rowMinDeletionTime, isTombstone ? Long.MIN_VALUE : localDeletionTime);
            boolean isLive = localDeletionTime == Cell.NO_DELETION_TIME || (ttl != Cell.NO_TTL && nowInSec < localDeletionTime);
            if (!isLive)
            {
                rowHasTombstoneCell = true;
                countTombstone(rowClustering);
            }
        }

        void endRow()
        {
            boolean rowHasDeletion = nowInSec >= rowMinDeletionTime;
            if (rowLivenessLive)
                ++liveRows;
            else if (rowHasDeletion && !rowHasTombstoneCell)
                countTombstone(rowClustering);
        }

        /** Whole-row escape hatch: a real, already-purged {@code Row} object exists, so use the
         *  real predicates directly instead of the per-event formulas above. */
        void wholeRow(Row row)
        {
            boolean hasTombstones = false;
            boolean rowHasDeletion = row.hasDeletion(nowInSec);
            if (rowHasDeletion)
            {
                for (Cell<?> cell : row.cells())
                {
                    if (!cell.isLive(nowInSec))
                    {
                        countTombstone(row.clustering());
                        hasTombstones = true;
                    }
                }
            }
            if (row.hasLiveData(nowInSec, false))
                ++liveRows;
            else if (!row.primaryKeyLivenessInfo().isLive(nowInSec) && rowHasDeletion && !hasTombstones)
                countTombstone(row.clustering());
        }

        void marker(ClusteringPrefix<?> clustering)
        {
            countTombstone(clustering);
        }

        private static long minDeletionTime(LivenessInfo info)
        {
            return info.isExpiring() ? info.localExpirationTime() : Cell.MAX_DELETION_TIME;
        }

        private static long minDeletionTime(DeletionTime dt)
        {
            return dt.isLive() ? Cell.MAX_DELETION_TIME : Long.MIN_VALUE;
        }

        private void countTombstone(ClusteringPrefix<?> clustering)
        {
            ++tombstones;
            if (tombstones > failureThreshold && respectTombstoneThresholds)
            {
                String query = command.toCQLString();
                Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)", failureThreshold, query);
                metric.tombstoneFailures.inc();
                if (command.isTrackingWarnings())
                {
                    MessageParams.remove(ParamType.TOMBSTONE_WARNING);
                    MessageParams.add(ParamType.TOMBSTONE_FAIL, tombstones);
                }
                throw new TombstoneOverwhelmingException(tombstones, query, command.metadata(), key, clustering);
            }
        }

        /** Must be called once, after the merge this guard was attached to has completed, like
         *  {@link TranscodeMergeSink#finishPartition}.  Records latency, histograms, counters, and
         *  warn logging, like {@code MetricRecording.onClose()}. */
        void finish()
        {
            command.recordLatency(metric, nanoTime() - startTimeNanos);
            metric.tombstoneScannedHistogram.update(tombstones);
            metric.liveScannedHistogram.update(liveRows);
            metric.totalRowsRead.inc(liveRows);
            if (liveRows > 0)
                metric.topReadPartitionRowCount.addSample(key.getKey(), liveRows);
            if (tombstones > 0)
                metric.topReadPartitionTombstoneCount.addSample(key.getKey(), tombstones);

            boolean warnTombstones = tombstones > warningThreshold && respectTombstoneThresholds;
            if (warnTombstones)
            {
                String msg = String.format(
                    "Read %d live rows and %d tombstone cells for query %1.512s; token %s (see tombstone_warn_threshold)",
                    liveRows, tombstones, command.toCQLString(), key.getToken());
                if (command.isTrackingWarnings())
                    MessageParams.add(ParamType.TOMBSTONE_WARNING, tombstones);
                else
                    ClientWarn.instance.warn(msg);
                if (tombstones < failureThreshold)
                    metric.tombstoneWarnings.inc();
                logger.warn(msg);
            }
            Tracing.trace("Read {} live rows and {} tombstone cells{}",
                          liveRows, tombstones, (warnTombstones ? " (see tombstone_warn_threshold)" : ""));
        }
    }

    /**
     * Transcodes merge events directly into {@code ReadResponse} wire bytes through
     * {@link ResponseWireWriter}, instead of materializing {@code Row}/{@code Cell} objects first.
     * {@link #addCell} still receives an already-materialized {@code Cell<?>}.  {@code RowFilter}
     * composition ({@link RowLevelFilterProbe}) is out of scope: a wire writer that already flushed
     * bytes cannot un-flush them.
     *
     * <p>Purging: {@code ReadCommand.withoutPurgeableTombstones} purges gcable tombstones above the
     * merge in the iterator path.  The transcode path applies the identical purge decisions at
     * emission, reusing the real production purge primitives:
     * <ul>
     *   <li>PK liveness, row deletion, and complex deletion: {@link DeletionPurger#shouldPurge}.</li>
     *   <li>Cells: {@link Cell#purge(DeletionPurger, long)}, called on the already-materialized
     *       {@code Cell<?>} that {@link #addCell} receives.</li>
     *   <li>Range tombstone markers: the combining logic of {@code PurgeFunction.applyToMarker} is
     *       mirrored here, since that method has no standalone entry point.  A boundary drops only
     *       when both sides purge, degrades to the surviving side's bound marker when one does, and
     *       passes through otherwise; a plain bound drops when its deletion purges.  {@code reversed}
     *       is always {@code false}: cursor reads do not support reversed-order queries.</li>
     * </ul>
     * A row or marker that purges to nothing never reaches the wire, like the iterator path.
     *
     * <p>Slice-boundary trimming: the raw merge stream is not slice-trimmed.  In the iterator path
     * {@code SlicedMaterializedIterator} trims it and synthesizes the open/close range-tombstone
     * markers at the slice bounds.  {@link #admitOrSkip} and {@link #finishPartition} replicate that
     * algorithm in streaming form: a sticky {@code pastSliceStart} latch plus a running
     * {@code openMarker} state seeded through {@link #initOpenMarker}.  This class purge-tests each
     * real marker before computing slice admission from its raw shape, then purge-tests synthetic
     * markers when they are written.  Both orders produce identical bytes, because {@code shouldPurge}
     * is a pure function of a deletion's (timestamp, localDeletionTime) pair and a
     * boundary-to-bound downgrade always keeps the side that determines {@code isOpen()}.
     */
    public static final class TranscodeMergeSink implements CursorReadMerger.MergeSink
    {
        private final ResponseWireWriter writer;
        private final DeletionPurger purger;
        private final long nowInSec;
        private final boolean purgeEnabled;
        private long eventsWritten;

        // ---- slice-boundary state (see class javadoc) ----
        private final ClusteringComparator comparator;
        /** null when the slice starts at BOTTOM (no pre-slice trimming/synthesis needed). */
        private final ClusteringBound<?> sliceStart;
        private final ClusteringBound<?> sliceEnd;
        /** Sticky: once true, every remaining event this partition is in-slice. Starts true when
         *  the slice starts at BOTTOM (nothing to trim). */
        private boolean pastSliceStart;
        /** Whether a range tombstone is currently open: null means no open marker.  Seeded by
         *  {@link #initOpenMarker}, updated from every raw (pre-purge) marker this sink sees. */
        private DeletionTime openMarker;
        /** Whether the row currently being staged (between startRow/endRow) was admitted by the
         *  slice.  addComplexDeletion/addCell/endRow no-op when false. */
        private boolean currentRowAdmitted;
        /** Optional tombstone/live-row accounting, attached only by the production call site
         *  ({@link #attachScanGuard}).  Null means no accounting happens. */
        private TombstoneScanGuard scanGuard;

        /** Attaches the tombstone/live-row accounting.  Must be called before any merge event
         *  reaches this sink. */
        void attachScanGuard(TombstoneScanGuard scanGuard)
        {
            this.scanGuard = scanGuard;
        }

        /** Purge-disabled convenience constructor: {@code nowInSec == 0} disables purging, matching
         *  {@code ReadCommand}'s own gate. */
        public TranscodeMergeSink(ResponseWireWriter writer, ClusteringComparator comparator, Slice slice)
        {
            this(writer, comparator, slice, 0, Long.MIN_VALUE, false, Long.MIN_VALUE);
        }

        /**
         * @param nowInSec 0 disables purging entirely, matching {@code ReadCommand}'s own gate
         */
        public TranscodeMergeSink(ResponseWireWriter writer, ClusteringComparator comparator, Slice slice,
                                  long nowInSec, long gcBefore, boolean onlyPurgeRepairedTombstones,
                                  long oldestUnrepairedTombstone)
        {
            this.writer = writer;
            this.comparator = comparator;
            this.sliceStart = slice.start().isBottom() ? null : slice.start();
            this.sliceEnd = slice.end();
            this.pastSliceStart = (sliceStart == null);
            this.nowInSec = nowInSec;
            this.purgeEnabled = nowInSec != 0;
            // PurgeFunction's purger field, specialized to the read path's inputs.
            // ignoreGcGraceSeconds is compaction-only and the purge evaluator is constant-true, so
            // both are elided.
            this.purger = (timestamp, localDeletionTime) ->
                          purgeEnabled
                          && !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone)
                          && localDeletionTime < gcBefore;
        }

        /** Seeds the slice-start open-marker state with the merged row-index seek state.  Must be
         *  called after the {@code CursorReadMerger} is constructed but before
         *  {@code mergeUnfiltereds()} runs.  If never called, {@code openMarker} stays null, correct
         *  for the empty-legs case where no seek state exists. */
        public void initOpenMarker(DeletionTime openMarkerAtStart)
        {
            this.openMarker = openMarkerAtStart;
        }

        /** Must be called once, after the merge this sink was driving has completed.  Artificially
         *  closes an open range tombstone at the slice end.  Not part of the {@code MergeSink}
         *  contract. */
        public void finishPartition()
        {
            if (!pastSliceStart)
            {
                // No admitted row or marker arrived after the slice start, so admitOrSkip's
                // transition never fired.  Perform that transition here before the slice-end close,
                // matching computeNextInSlice, which runs this check even when the stream is empty.
                pastSliceStart = true;
                if (openMarker != null)
                    purgeAndWriteMarker(new RangeTombstoneBoundMarker(sliceStart, openMarker));
            }
            if (openMarker == null)
                return;
            DeletionTime toClose = openMarker;
            openMarker = null;
            purgeAndWriteMarker(new RangeTombstoneBoundMarker(sliceEnd, toClose));
        }

        /**
         * The slice-admission decision for a row or marker's clustering, replayed one element at a
         * time.  Returns false (drop) for anything at or before the slice start.  On the transition
         * into the slice, emits the synthetic open marker first, purge-tested like any other marker,
         * if a deletion is running open at that point.
         */
        private boolean admitOrSkip(ClusteringPrefix<?> clustering)
        {
            if (pastSliceStart)
                return true;
            if (sliceStart != null && comparator.compare(clustering, sliceStart) <= 0)
                return false;
            pastSliceStart = true;
            if (openMarker != null)
                purgeAndWriteMarker(new RangeTombstoneBoundMarker(sliceStart, openMarker));
            return true;
        }

        /** Purge-tests any {@code RangeTombstoneMarker} reaching the wire: real markers admitted by
         *  {@link #admitOrSkip}, and the synthetic slice-boundary markers it and
         *  {@link #finishPartition} construct.  Production purge-tests those synthetic markers too,
         *  since they are ordinary markers by the time {@code withoutPurgeableTombstones} sees them. */
        private void purgeAndWriteMarker(RangeTombstoneMarker marker)
        {
            try
            {
                if (marker.isBoundary())
                {
                    RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                    boolean purgeClose = purger.shouldPurge(boundary.closeDeletionTime(false));
                    boolean purgeOpen = purger.shouldPurge(boundary.openDeletionTime(false));
                    if (purgeClose && purgeOpen)
                        // both sides gcable: the whole marker drops
                        return;
                    if (purgeClose)
                        writer.writeMarker(boundary.createCorrespondingOpenMarker(false));
                    else if (purgeOpen)
                        writer.writeMarker(boundary.createCorrespondingCloseMarker(false));
                    else
                        writer.writeMarker(marker);
                }
                else
                {
                    if (purger.shouldPurge(((RangeTombstoneBoundMarker) marker).deletionTime()))
                        return;
                    writer.writeMarker(marker);
                }
                eventsWritten++;
                if (scanGuard != null)
                    scanGuard.marker(marker.clustering());
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        /** Diagnostic only: rows and markers that reached the wire, post-purge.  Used by test
         *  sanity checks. */
        long eventsWritten()
        {
            return eventsWritten;
        }

        @Override
        public void startRow(Clustering<?> clustering, LivenessInfo mergedLiveness, DeletionTime mergedRowDeletion)
        {
            currentRowAdmitted = admitOrSkip(clustering);
            if (!currentRowAdmitted)
                return;
            LivenessInfo liveness = purger.shouldPurge(mergedLiveness, nowInSec) ? LivenessInfo.EMPTY : mergedLiveness;
            DeletionTime deletion = purger.shouldPurge(mergedRowDeletion) ? DeletionTime.LIVE : mergedRowDeletion;
            if (scanGuard != null)
                scanGuard.startRow(clustering, liveness, deletion);
            if (TEST_TRANSCODE_SKEW_TIMESTAMP && !liveness.isEmpty())
                liveness = LivenessInfo.withExpirationTime(liveness.timestamp() + 1, liveness.ttl(), liveness.localExpirationTime());
            if (TEST_TRANSCODE_WRONG_FLAGS && !deletion.isLive())
                deletion = DeletionTime.LIVE;
            try
            {
                writer.startRow(clustering, liveness, deletion);
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime mergedComplexDeletion)
        {
            if (!currentRowAdmitted)
                return;
            DeletionTime deletion = purger.shouldPurge(mergedComplexDeletion) ? DeletionTime.LIVE : mergedComplexDeletion;
            if (deletion.isLive())
                // a live complex deletion is never announced; the column is opened lazily by its
                // first surviving addCell instead
                return;
            if (scanGuard != null)
                scanGuard.complexDeletion();
            try
            {
                writer.addComplexDeletion(column, deletion);
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            if (!currentRowAdmitted)
                return;
            Cell<?> purged = cell.purge(purger, nowInSec);
            if (purged == null)
                // fully purged: dropped from the wire entirely
                return;
            if (scanGuard != null)
                scanGuard.cell(purged.timestamp(), purged.ttl(), purged.localDeletionTime());
            try
            {
                writer.addCell(purged);
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public boolean wantsWireStreamedCells()
        {
            return true;
        }

        /**
         * Outcome of {@link #purgeCellLiveness}: mirrors the three branches of
         * {@code AbstractCell.purge(DeletionPurger, long)}.
         */
        private enum CellPurgeOutcome { UNCHANGED, CONVERT_TO_TOMBSTONE, DROP }

        /** Set by {@link #purgeCellLiveness} on a {@code CONVERT_TO_TOMBSTONE} outcome, read
         *  immediately after by {@link #addCellFromWire}.  Avoids a per-cell result-tuple
         *  allocation. */
        private long convertedLocalDeletionTime;

        /**
         * Mirrors {@code AbstractCell.purge(DeletionPurger, long)}'s decision tree, operating on the
         * cell's liveness metadata alone so the purge/convert/drop decision is made before the value
         * bytes are touched.  This lets streaming and purging compose: {@link #addCellFromWire} must
         * decide without materializing the value.  The tree: not live → shouldPurge(timestamp,
         * localDeletionTime) → DROP; else, if expiring, convert to a tombstone at the adjusted
         * localDeletionTime and check shouldPurge again → DROP or CONVERT; else → UNCHANGED.
         */
        private CellPurgeOutcome purgeCellLiveness(long timestamp, int ttl, long localDeletionTime)
        {
            boolean isLive = localDeletionTime == Cell.NO_DELETION_TIME
                             || (ttl != Cell.NO_TTL && nowInSec < localDeletionTime);
            if (isLive)
                return CellPurgeOutcome.UNCHANGED;
            if (purger.shouldPurge(timestamp, localDeletionTime))
                return CellPurgeOutcome.DROP;
            if (ttl != Cell.NO_TTL)
            {
                long adjustedLocalDeletionTime = localDeletionTime - ttl;
                if (purger.shouldPurge(timestamp, adjustedLocalDeletionTime))
                    return CellPurgeOutcome.DROP;
                convertedLocalDeletionTime = adjustedLocalDeletionTime;
                return CellPurgeOutcome.CONVERT_TO_TOMBSTONE;
            }
            return CellPurgeOutcome.UNCHANGED;
        }

        /**
         * The streaming form of {@link #addCell}: identical purge outcome to
         * {@code cell.purge(purger, nowInSec)}, but the purge/convert/drop decision is made before
         * the value is streamed, so a dropped or converted cell never reaches {@code source}.
         * {@code CursorReadMerger.mergeCellGroup}'s {@code discardCellValue()} reclaims the leg's
         * unconsumed bytes in that case.
         */
        @Override
        public void addCellFromWire(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime,
                                    CellPath path, CellValueSource source) throws IOException
        {
            if (!currentRowAdmitted)
                return;
            CellPurgeOutcome outcome = purgeCellLiveness(timestamp, ttl, localDeletionTime);
            if (outcome == CellPurgeOutcome.DROP)
                // fully purged: dropped from the wire entirely; the leg's unconsumed value bytes are
                // reclaimed by mergeCellGroup's discardCellValue()
                return;
            boolean forceEmpty = outcome == CellPurgeOutcome.CONVERT_TO_TOMBSTONE;
            if (forceEmpty)
            {
                // an expired-but-not-yet-gcable cell is re-emitted as a plain tombstone: no TTL,
                // value dropped, localDeletionTime moved back to the write-time value
                ttl = LivenessInfo.NO_TTL;
                localDeletionTime = convertedLocalDeletionTime;
            }
            if (scanGuard != null)
                scanGuard.cell(timestamp, ttl, localDeletionTime);
            boolean hasValue = !forceEmpty && source.hasValue();
            try
            {
                writer.addCellFromWire(column, timestamp, ttl, localDeletionTime, path, hasValue, source);
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void endRow()
        {
            if (!currentRowAdmitted)
                return;
            if (scanGuard != null)
                scanGuard.endRow();
            try
            {
                if (writer.endRow())
                    eventsWritten++;
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void addRow(Row row)
        {
            if (!admitOrSkip(row.clustering()))
                return;
            // whole-row path: purge with the real Row.purge (enforceStrictLiveness is always false,
            // since MV tables are gated out of cursor reads), then pass the result to the writer.
            Row purged = row.purge(purger, nowInSec, false);
            if (purged == null || purged.isEmpty())
                return;
            if (scanGuard != null)
                scanGuard.wholeRow(purged);
            try
            {
                writer.writeRow(purged);
                eventsWritten++;
            }
            catch (IOException e)
            {
                throw new RuntimeException("transcode wire write failed", e);
            }
        }

        @Override
        public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            boolean admitted = admitOrSkip(marker.clustering());
            // Open-marker tracking updates from the raw marker unconditionally, independent of both
            // admission and purging (see the class javadoc's order note).
            DeletionTime rawOpenAfter = marker.isOpen(false) ? CursorReads.copyOf(marker.openDeletionTime(false)) : null;
            if (!admitted)
            {
                openMarker = rawOpenAfter;
                return;
            }
            purgeAndWriteMarker(marker);
            openMarker = rawOpenAfter;
        }
    }

    /**
     * Drives the real merge core through {@link #mergeLegsWithSink} with a {@link TranscodeMergeSink},
     * producing the full {@code UnfilteredPartitionIterators.Serializer} envelope wire bytes for the
     * one partition a {@code SinglePartitionReadCommand} returns.  The caller must have verified the
     * gate and computed the purge parameters as {@code ReadCommand.withoutPurgeableTombstones} would.
     *
     * @param legs       >= 2 legs (the merge core's requirement, enforced by
     *                   {@link #mergeLegsWithSink}).
     * @param extraStats additional {@code EncodingStats} contributions the merge never sees, one per
     *                   gated-in candidate sstable whose {@code openLeg} returned null (the partition
     *                   was absent).  Reproduces the stats contribution the object path's absent
     *                   placeholder iterator would make, without an actual placeholder.
     * @param scanGuard  optional tombstone/live-row accounting (see {@link TombstoneScanGuard}); null
     *                   disables it.
     */
    static ByteBuffer buildTranscodeResponseBytes(List<? extends MergeLeg> legs,
                                                   TableMetadata metadata,
                                                   DecoratedKey key,
                                                   Slices slices,
                                                   ColumnFilter columnFilter,
                                                   long nowInSec,
                                                   long gcBefore,
                                                   boolean onlyPurgeRepairedTombstones,
                                                   long oldestUnrepairedTombstone,
                                                   List<EncodingStats> extraStats,
                                                   TombstoneScanGuard scanGuard) throws IOException
    {
        RegularAndStaticColumns cols = columnFilter.fetchedColumns();
        List<EncodingStats> allStats = new ArrayList<>(legs.size() + (extraStats == null ? 0 : extraStats.size()));
        for (MergeLeg leg : legs)
            allStats.add(leg.legStats());
        if (extraStats != null)
            allStats.addAll(extraStats);
        EncodingStats stats = EncodingStats.merge(allStats, Function.identity());
        SerializationHeader header = new SerializationHeader(false, metadata, cols, stats);

        DataOutputBuffer rowEvents = new DataOutputBuffer();
        ResponseWireWriter writer = new ResponseWireWriter(rowEvents, header, MessagingService.current_version);
        Slice slice = slices.get(0);
        TranscodeMergeSink sink = new TranscodeMergeSink(writer, metadata.comparator, slice, nowInSec, gcBefore,
                                                          onlyPurgeRepairedTombstones, oldestUnrepairedTombstone);
        if (scanGuard != null)
            sink.attachScanGuard(scanGuard);
        MergeSinkFactory<TranscodeMergeSink> factory = () -> sink;
        MergeContext<TranscodeMergeSink> ctx = mergeLegsWithSink(legs, metadata, key, slices, columnFilter, null, null, factory);
        // once the merge has completed, close any still-open range tombstone at the slice end with a
        // purge-tested synthetic marker
        ctx.sink.finishPartition();
        if (scanGuard != null)
            scanGuard.finish();

        boolean hasStatic = !ctx.mergedStatic.isEmpty();
        boolean isEmptyPartition = ctx.mergedDeletion.isLive() && !hasStatic && rowEvents.getLength() == 0;

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // UnfilteredPartitionIterators.Serializer's envelope: the legacy isForThrift
            // placeholder, then "has next partition" (always true), the partition itself, then
            // "has next partition" again (always false, since there is exactly one partition).
            out.writeBoolean(false);
            out.writeBoolean(true);
            ResponseWireWriter envelope = new ResponseWireWriter(out, header, MessagingService.current_version);
            envelope.writeKey(key.getKey());
            envelope.writeHeader(isEmptyPartition, false, ctx.mergedDeletion, hasStatic, columnFilter);
            if (!isEmptyPartition)
            {
                if (hasStatic)
                    envelope.writeStaticRow(ctx.mergedStatic);
                out.write(rowEvents.getData(), 0, rowEvents.getLength());
                envelope.writeEndOfPartition();
            }
            out.writeBoolean(false);
            return out.buffer(false);
        }
    }

    /**
     * Receives {@link SSTableCursorReader#copyCellValue}'s wire-form output directly into the final
     * cell value array, avoiding a scratch-buffer round trip.  copyCellValue makes two kinds of
     * calls on its writer: {@link #writeUnsignedVInt32} with the decoded value length
     * (variable-length types only, which the materialized value must not contain), then
     * {@link #write(byte[], int, int)} per transfer chunk.
     * <ul>
     *   <li>variable-length values ({@link #prepareVariable}): the value array is allocated when
     *       the length arrives and chunks are copied straight into it (two copies);</li>
     *   <li>fixed-length values ({@link #prepareFixed}): the length is known up front, so the final
     *       array is passed to copyCellValue as the transfer buffer and the bytes land in place
     *       (one copy).  {@link #write} only validates that the copy loop made a single full-array
     *       pass.</li>
     * </ul>
     * Every other output method throws {@link UnsupportedOperationException}: if the cursor reader's
     * copy loop ever changes shape, this fails loudly instead of corrupting values.
     */
    private static final class CellValueCapture implements DataOutputPlus
    {
        private byte[] target;
        private int written;
        private boolean fixed;

        /** Variable-length mode: the array is allocated when the wire's length vint is mirrored. */
        CellValueCapture prepareVariable()
        {
            target = null;
            written = 0;
            fixed = false;
            return this;
        }

        /** Fixed-length mode; {@code value} must ALSO be passed to copyCellValue as its transfer buffer. */
        CellValueCapture prepareFixed(byte[] value)
        {
            target = value;
            written = 0;
            fixed = true;
            return this;
        }

        /** The captured value array, fully written. */
        byte[] finish()
        {
            byte[] value = target;
            if (value == null || written != value.length)
                throw new IllegalStateException("incomplete cell value capture: " + written + " of "
                                                + (value == null ? "unknown" : String.valueOf(value.length)) + " bytes");
            target = null;
            return value;
        }

        @Override
        public void writeUnsignedVInt32(int length)
        {
            if (fixed || target != null)
                throw new IllegalStateException("unexpected value length vint");
            target = length == 0 ? ByteArrayAccessor.instance.empty() : new byte[length];
        }

        @Override
        public void write(byte[] buffer, int offset, int length)
        {
            if (fixed)
            {
                // the transfer buffer is the target and readFully already placed the bytes; only a
                // single full-array chunk at offset 0 cannot have clobbered them
                if (buffer != target || offset != 0 || written != 0 || length != target.length)
                    throw new IllegalStateException("unexpected fixed-length value chunking");
                written = length;
                return;
            }
            System.arraycopy(buffer, offset, target, written, length);
            written += length;
        }

        @Override
        public void write(int b)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(byte[] buffer)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(ByteBuffer buffer)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeByte(int v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeLong(long v)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeUTF(String s)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Applies the query's slices to the materialized partition with the same semantics as
     * {@code AbstractSSTableIterator.ForwardReader}: pre-slice skip with open-marker tracking,
     * artificial {@link RangeTombstoneBoundMarker}s at slice start/end when a range tombstone covers
     * the bound, and non-strict start / strict end comparisons.  The emitted stream is identical to
     * the iterator path's.
     */
    private static final class SlicedMaterializedIterator implements UnfilteredRowIterator
    {
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final SSTableReader sstable;
        private final EncodingStats stats;
        private final ColumnFilter columnFilter;
        private final Slices slices;
        private final MaterializedPartition partition;
        private final ClusteringComparator comparator;
        private final boolean validateOnEmission;

        private int sliceIdx;            // next slice to open
        private int listIdx;             // position in partition.unfiltereds
        private ClusteringBound<?> start; // null once past the current slice's start, or when it starts at BOTTOM
        private ClusteringBound<?> end;
        private boolean sliceOpen;
        private DeletionTime openMarker;
        private Unfiltered next;

        /**
         * @param sstable for a single leg, the leg's sstable; for a merged partition, the first
         *                leg's, used to attribute emission-time corrupted-tombstone validation
         * @param stats   the stats this iterator reports: the leg sstable's own for a single leg,
         *                {@code EncodingStats.merge} over the legs for a merged partition
         * @param validateOnEmission true for a single leg, where this iterator is the leg's emission
         *                surface and owns the per-leg in-slice validation; false for a merged
         *                partition, whose legs were each already validated inside the merge.
         *                Validating a merged partition here would duplicate the work and reject
         *                reads the iterator path serves.
         */
        SlicedMaterializedIterator(TableMetadata metadata,
                                   DecoratedKey key,
                                   SSTableReader sstable,
                                   EncodingStats stats,
                                   ColumnFilter columnFilter,
                                   Slices slices,
                                   MaterializedPartition partition,
                                   boolean validateOnEmission)
        {
            this.metadata = metadata;
            this.key = key;
            this.sstable = sstable;
            this.stats = stats;
            this.columnFilter = columnFilter;
            this.slices = slices;
            this.partition = partition;
            this.comparator = metadata.comparator;
            this.validateOnEmission = validateOnEmission;
            // Seeded from the BTI row index when the materialization seeked mid-partition: the range
            // tombstone open at the first materialized element cannot be found from the truncated
            // stream itself.  Null for unseeked materializations.
            this.openMarker = partition.openMarkerAtStart;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return false;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return columnFilter.fetchedColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return key;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return partition.partitionDeletion;
        }

        @Override
        public Row staticRow()
        {
            return partition.staticRow;
        }

        @Override
        public EncodingStats stats()
        {
            return stats;
        }

        @Override
        public boolean hasNext()
        {
            while (next == null)
            {
                if (!sliceOpen)
                {
                    if (sliceIdx >= slices.size())
                        return false;
                    setForSlice(slices.get(sliceIdx++));
                }
                // returns null only after setting sliceOpen = false, so a null just loops back
                // to open the next slice (or hit the sliceIdx exhaustion check above)
                next = computeNextInSlice();
            }
            return true;
        }

        @Override
        public Unfiltered next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            Unfiltered toReturn = next;
            next = null;
            return toReturn;
        }

        private void setForSlice(Slice slice)
        {
            start = slice.start().isBottom() ? null : slice.start();
            end = slice.end();
            sliceOpen = true;
        }

        // mirrors ForwardReader's in-slice iteration; only called with sliceOpen == true, and
        // clears sliceOpen when the slice is exhausted
        private Unfiltered computeNextInSlice()
        {
            if (start != null)
            {
                // Skip pre-slice data with a NON-strict comparison (see handlePreSliceData's
                // comment on RT start markers equal to the slice start), tracking the open marker.
                while (listIdx < partition.unfiltereds.size()
                       && comparator.compare(partition.unfiltereds.get(listIdx).clustering(), start) <= 0)
                {
                    Unfiltered skipped = partition.unfiltereds.get(listIdx++);
                    if (skipped.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                        updateOpenMarker((RangeTombstoneMarker) skipped);
                }
                ClusteringBound<?> sliceStart = start;
                start = null;
                if (openMarker != null)
                    return new RangeTombstoneBoundMarker(sliceStart, openMarker);
            }

            // in-slice: strict end comparison
            if (listIdx < partition.unfiltereds.size()
                && comparator.compare(partition.unfiltereds.get(listIdx).clustering(), end) < 0)
            {
                Unfiltered unfiltered = partition.unfiltereds.get(listIdx++);
                // Single-leg mode: corrupted_tombstone_strategy check, applied where the iterator
                // path applies it: each in-slice unfiltered as it is emitted.  Pre-slice skipped
                // data, the artificial slice-bound markers, the static row, and anything at or past
                // the slice end are not validated, matching the iterator path.  This keeps a
                // corrupted row outside the queried slice from failing a read the iterator path
                // would have served.  Merged mode skips this: each leg was already validated inside
                // the merge (see the validateOnEmission constructor javadoc).
                if (validateOnEmission)
                    UnfilteredValidation.maybeValidateUnfiltered(unfiltered, metadata, key, sstable);
                if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                    updateOpenMarker((RangeTombstoneMarker) unfiltered);
                return unfiltered;
            }

            // slice exhausted: artificially close an open range tombstone at the slice end
            sliceOpen = false;
            if (openMarker != null)
                return new RangeTombstoneBoundMarker(end, openMarker);
            return null;
        }

        private void updateOpenMarker(RangeTombstoneMarker marker)
        {
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        @Override
        public void close()
        {
            // fully materialized; the cursor was closed at creation
        }
    }
}
