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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.carrotsearch.hppc.LongStack;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.CursorCounterContexts;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
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
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
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
import org.apache.cassandra.io.sstable.format.bti.RowIndexReader;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
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
    /** Sstable legs of a read that passed {@link #isReadSupported} but were still read through the
     *  iterator path.  Counted once per leg.  Must stay zero. */
    private static final AtomicLong SSTABLE_LEGS_FELL_BACK_TO_ITERATOR = new AtomicLong();
    /** Sstable legs where the BTI row index produced a forward seek into the partition.  Counts
     *  merged-mode legs too: a merged read over N indexed BTI legs with a slice start advances
     *  this by N. */
    private static final AtomicLong SSTABLE_LEG_ROW_INDEX_SEEKS = new AtomicLong();
    /** Unfiltereds (rows and range tombstone markers, static row excluded) materialized by cursor
     *  legs.  For a merged read this counts the merged output, not the per-leg sum. */
    private static final AtomicLong UNFILTEREDS_MATERIALIZED = new AtomicLong();
    /** Multi-leg reads served by the cursor-level merge (>= 2 legs through {@link #mergeLegs}). */
    private static final AtomicLong CURSOR_MERGES_SERVED = new AtomicLong();
    /** Non-tracking NAMES reads served by the timestamp-order completeness driver
     *  ({@code SinglePartitionReadCommand.queryMemtableAndCursorsInTimestampOrder}), the cursor twin
     *  of the iterator's {@code queryMemtableAndSSTablesInTimestampOrder}.  Counts one per driver
     *  invocation, whether or not any sstable leg survives the completeness skip. */
    private static final AtomicLong NAMES_TIMESTAMP_ORDER_READS = new AtomicLong();
    /** Sstable legs that entered a cursor-level merge (each such leg is also counted in
     *  {@link #sstableLegsServed}). */
    private static final AtomicLong SSTABLE_LEGS_CURSOR_MERGED = new AtomicLong();
    /** Deferred legs force-opened at merge setup because their metadata lower bound spans the merge
     *  start (the range-tombstone-seeding open); a strict subset of {@link #sstableLegsServed}.
     *  Reachable only on BIG format with a primed key cache; on BTI this stays zero (see the
     *  force-open call site in {@link #mergeLegs}), which the b1 differential guards assert. */
    private static final AtomicLong SSTABLE_LEGS_FORCE_OPENED = new AtomicLong();
    /** Memtable legs that joined a cursor-level merge through the {@link MemtableMergeLeg} adapter. */
    private static final AtomicLong MEMTABLE_LEGS_CURSOR_MERGED = new AtomicLong();
    /** Merged rows emitted by reusing the memtable's own live {@code Row} object (single-version
     *  fast path). */
    private static final AtomicLong MEMTABLE_ROWS_REUSED = new AtomicLong();
    /** Merged cells emitted by reusing the memtable's own live {@code Cell} object. */
    private static final AtomicLong MEMTABLE_CELLS_REUSED = new AtomicLong();
    /** Cursor merges closed before they reached the end of their slices, because the reader
     *  stopped asking for rows (a limit or page was reached). */
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

    /** Replica data reads, with cursor reads enabled, that the transcode path declined: they are
     *  served by {@code executeLocally} instead. */
    private static final AtomicLong TRANSCODE_RESPONSES_DECLINED = new AtomicLong();

    static void countTranscodeResponseDeclined()
    {
        TRANSCODE_RESPONSES_DECLINED.incrementAndGet();
    }

    @VisibleForTesting
    public static long transcodeResponsesDeclined()
    {
        return TRANSCODE_RESPONSES_DECLINED.get();
    }

    public static long sstableLegsServed()
    {
        return SSTABLE_LEGS_SERVED.get();
    }

    @VisibleForTesting
    public static long sstableLegsForceOpened()
    {
        return SSTABLE_LEGS_FORCE_OPENED.get();
    }

    public static long cursorMergesServed()
    {
        return CURSOR_MERGES_SERVED.get();
    }

    static void countNamesTimestampOrderRead()
    {
        NAMES_TIMESTAMP_ORDER_READS.incrementAndGet();
    }

    @VisibleForTesting
    public static long namesTimestampOrderReads()
    {
        return NAMES_TIMESTAMP_ORDER_READS.get();
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

    static void countSstableLegFellBackToIterator()
    {
        SSTABLE_LEGS_FELL_BACK_TO_ITERATOR.incrementAndGet();
    }

    public static long sstableLegsFellBackToIterator()
    {
        return SSTABLE_LEGS_FELL_BACK_TO_ITERATOR.get();
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

    /** Test only: skews the row liveness timestamp {@link ResponseSink} hands
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
     *  {@link ResponseSink} streams it, so the harness can prove it catches a wrong value on
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
     *   <li>slice reads ({@link ClusteringIndexSliceFilter}) and names reads
     *       ({@link ClusteringIndexNamesFilter}, CASSANDRA-20428 gap #9)</li>
     *   <li>ascending order only (the cursor is forward-only)</li>
     *   <li>a slice filter may hold any number of forward slices (CASSANDRA-20428 gap #8): a
     *       full-partition read, a single slice, or several slices from a compound-clustering
     *       restriction.  A names filter selects a fixed set of clusterings and resolves to one point
     *       slice per clustering.  For both, the merge reads one slice at a time, seeking forward
     *       to each slice start where the row index allows, and skips the rows between slices
     *       without reading their cells</li>
     *   <li>no row cache on the table (a cursor result cannot populate {@code CachedBTreePartition}
     *       without materializing anyway)</li>
     *   <li>counter tables ARE served (CASSANDRA-20428 counter gap): the single-leg materializer and
     *       the cursor merge both apply the marked-local shard clear
     *       ({@code DeserializationHelper.maybeClearCounterValue} equivalent) and fold live contexts
     *       via {@link CursorCounterContexts}, byte-for-byte with the iterator path</li>
     *   <li>no materialized views: legacy shadowable row deletions are rejected mid-read by
     *       {@link SSTableCursorReader}</li>
     *   <li>all candidate sstables at the latest format version, none carrying dropped
     *       complex/counter ghost header columns</li>
     *   <li>any number of legs, down to one and including none: a single sstable leg, a
     *       memtable-only read (zero sstables), or a read whose surviving legs collapse to one
     *       after tombstone pruning are all served on the cursor path</li>
     * </ul>
     */
    public static boolean isReadSupported(SinglePartitionReadCommand command,
                                          ColumnFamilyStore cfs,
                                          List<SSTableReader> sstables)
    {
        if (!DatabaseDescriptor.cursorReadsEnabled())
            return false;

        TableMetadata metadata = cfs.metadata();
        if (CursorCompactor.unsupportedMetadata(metadata, false))
            return false;
        if (metadata.isView())
            return false;
        if (cfs.isRowCacheEnabled())
            return false;

        ClusteringIndexFilter filter = command.clusteringIndexFilter();
        boolean sliceFilter = filter instanceof ClusteringIndexSliceFilter;
        boolean namesFilter = filter instanceof ClusteringIndexNamesFilter;
        if (!sliceFilter && !namesFilter)
            return false;
        // A reverse-order read (ORDER BY ... DESC within a partition, CASSANDRA-20428 gap #5) is
        // served on the cursor path.  Each sstable leg walks its partition block by block from the
        // slice end backward (the cursor analog of BTI SSTableReversedIterator), emitting in
        // descending clustering order; the surviving legs reconcile in the shared reverse merge
        // (UnfilteredRowIterators.merge over per-leg reverse iterators), the same composition the
        // iterator path uses for a reverse read.  A reverse read stays off the transcode path
        // (queryStorageToResponseBytes declines it) and off the cursor-level merge
        // (cursorMergedLegs is forced false for a reversed read).
        // A multi-slice read is served on the cursor path (CASSANDRA-20428 gap #8 for a slice filter
        // of several ranges, gap #9 for a names filter of several clusterings).  The cursor merge
        // reads one slice at a time, seeking forward to each slice start where the row index
        // allows, and the emission slicer adds the slice-bound markers.  A multi-slice read stays
        // off the single-slice filter pushdown (filterPushdownFor declines it).

        // No sstable-count floor: a single sstable, a memtable-only read (zero sstables), and a
        // read that collapses to one surviving leg after tombstone pruning are all served. The
        // per-sstable checks below still run for every candidate; an empty list skips the loop.
        for (SSTableReader sstable : sstables)
        {
            if (!sstable.descriptor.version.isLatestVersion())
                return false;
            if (CursorCompactor.unsupportedHeaderColumns(metadata, sstable, false))
                return false;
        }
        return true;
    }

    /** Counts a deferred leg opened early because its data may start at or before a slice start. */
    static void countLegForceOpened()
    {
        SSTABLE_LEGS_FORCE_OPENED.incrementAndGet();
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
        return sstableRowIterator(sstable, metadata, key, slices, columnFilter, listener, false);
    }

    /**
     * Reverse-aware {@link #sstableRowIterator}.  When {@code reversed} is true the leg produces its
     * unfiltereds in descending clustering order (see {@link #completeSingleLegReversed}); the
     * result reports {@code isReverseOrder() == true}, so it composes with the other reverse legs in
     * the shared merge.
     */
    public static UnfilteredRowIterator sstableRowIterator(SSTableReader sstable,
                                                           TableMetadata metadata,
                                                           DecoratedKey key,
                                                           Slices slices,
                                                           ColumnFilter columnFilter,
                                                           SSTableReadsListener listener,
                                                           boolean reversed)
    {
        // Callers without a shared per-execution scratch (tests, allocation baselines) get a private
        // transfer.  The single-partition read path passes the controller's shared instance below.
        return sstableRowIterator(sstable, metadata, key, slices, columnFilter, listener, reversed, new ValueTransfer());
    }

    /**
     * {@link #sstableRowIterator} that reuses a caller-owned {@link ValueTransfer}, so the commands of
     * one query execution (an IN read, the legacy-2i base-read fan-out) and the single-leg legs of one
     * command share one 4 KB scratch instead of allocating one per leg.  The transfer is only in flight
     * during a cell copy, so a merge that interleaves legs is safe (see {@link ValueTransfer}).
     */
    public static UnfilteredRowIterator sstableRowIterator(SSTableReader sstable,
                                                           TableMetadata metadata,
                                                           DecoratedKey key,
                                                           Slices slices,
                                                           ColumnFilter columnFilter,
                                                           SSTableReadsListener listener,
                                                           boolean reversed,
                                                           ValueTransfer transfer)
    {
        PendingLeg leg = openLeg(sstable, metadata, key, slices, columnFilter, listener, transfer);
        if (leg == null)
            // mirrors BigTableReader/BtiTableReader.rowIterator with a null index entry
            return absentPartitionIterator(metadata, key, sstable, reversed);
        return reversed ? completeSingleLegReversed(leg) : completeSingleLeg(leg);
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
        return absentPartitionIterator(metadata, key, sstable, false);
    }

    /** Reverse-aware {@link #absentPartitionIterator}: the empty stand-in reports the requested
     *  clustering order so it composes with the other legs in a reverse merge. */
    public static UnfilteredRowIterator absentPartitionIterator(TableMetadata metadata, DecoratedKey key, SSTableReader sstable, boolean reversed)
    {
        UnfilteredRowIterator empty = UnfilteredRowIterators.noRowsIterator(metadata, key, Rows.EMPTY_STATIC_ROW,
                                                                            DeletionTime.LIVE, reversed);
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
     * Intersecting-leg row source for the timestamp-order NAMES driver
     * ({@code SinglePartitionReadCommand.queryMemtableAndCursorsInTimestampOrder}).  It is the cursor
     * twin of the oracle's {@code makeRowIterator}: the reduced point slices are materialized through
     * a cursor leg that folds the leg's REAL {@code sstable.stats()} (via {@link #completeSingleLeg}),
     * so the merged header matches the oracle for an opened intersecting leg.
     *
     * <p>When the sstable does not contain the partition ({@link #openLeg} returns null) this returns
     * a plain empty iterator, matching the oracle's {@code makeRowIterator} whose lazy init finds no
     * partition; the driver's own {@code isEmpty()} guard then skips it without merging.
     *
     * <p>The leg is ALWAYS materialized forward (ascending clustering order) via
     * {@link #completeSingleLeg}, even for a reversed read.  The reason is the driver's completeness
     * accounting: {@code reduceFilter} counts covered clusterings in timestamp order, which is
     * forward-only and independent of clustering direction, so the driver accumulates every leg
     * forward and reverses only at its final emit.  (The reverse block descent in
     * {@code ReverseSlicedCursorIterator} once had a &gt;64KB {@code gotoBlock} defect; that was fixed
     * under CASSANDRA-20428 (the reverse read past {@code column_index_size} fix), so it is no longer
     * a reason to avoid the reverse iterator.  The forward-only completeness accounting above is.)  The
     * absent stand-in is empty and the driver's {@code isEmpty()} guard drops it before any merge.
     *
     * @param transfer the driver's shared {@link ValueTransfer} scratch (its legs read one at a time,
     *                  so one buffer serves the whole read)
     */
    public static UnfilteredRowIterator namesLegIterator(SSTableReader sstable,
                                                         TableMetadata metadata,
                                                         DecoratedKey key,
                                                         Slices slices,
                                                         ColumnFilter columnFilter,
                                                         SSTableReadsListener listener,
                                                         ValueTransfer transfer)
    {
        PendingLeg leg = openLeg(sstable, metadata, key, slices, columnFilter, listener, transfer);
        if (leg == null)
            return EmptyIterators.unfilteredRow(metadata, key, false);
        return completeSingleLeg(leg);
    }

    /**
     * Tombstone-only-leg row source for the timestamp-order NAMES driver.  This is the cursor twin of
     * the oracle's {@code makeRowIteratorWithSkippedNonStaticContent} REWRAPPED through
     * {@code UnfilteredRowIterators.noRowsIterator} (see
     * {@code SinglePartitionReadCommand.queryMemtableAndSSTablesInTimestampOrder}): it opens the leg
     * with {@code Slices.NONE} to read the partition header (the counted read), then returns a
     * no-rows iterator carrying {@code EncodingStats.NO_STATS} and an empty static row.
     *
     * <p>It must NOT use {@link #completeSingleLeg}, which would fold the leg's real static row and so
     * diverge the merged header and digest.  It matches the oracle's two branches by the partition
     * deletion:
     * <ul>
     *   <li>a NON-LIVE partition deletion returns {@code noRowsIterator} with an empty static row and
     *       {@code NO_STATS}, carrying the real deletion (the oracle's {@code noRowsIterator} rewrap);</li>
     *   <li>a LIVE partition deletion returns an empty iterator that still reports the real
     *       {@code sstable.stats()} (via {@link #absentPartitionIterator}).  The oracle's else branch
     *       still MERGES its real-stats no-rows iterator, so the merged partition's {@code EncodingStats}
     *       absorbs this sstable's {@code minLocalDeletionTime}.  That value is the purge boundary
     *       {@code withoutPurgeableTombstones} uses AND it delta-encodes the serialized deletion times, so
     *       the driver must merge it, not discard it, to stay byte-identical (CASSANDRA-20428).  Content
     *       stays empty; only the stats header folds in.</li>
     * </ul>
     *
     * <p>In both cases the leg was opened, so the sstable is counted identically to the oracle.  When the
     * sstable does not contain the partition this returns a plain empty iterator with a live partition
     * deletion.
     */
    public static UnfilteredRowIterator namesTombstoneOnlyLegIterator(SSTableReader sstable,
                                                                      TableMetadata metadata,
                                                                      DecoratedKey key,
                                                                      ColumnFilter columnFilter,
                                                                      SSTableReadsListener listener,
                                                                      ValueTransfer transfer)
    {
        PendingLeg leg = openLeg(sstable, metadata, key, Slices.NONE, columnFilter, listener, transfer);
        if (leg == null)
            return EmptyIterators.unfilteredRow(metadata, key, false);
        try
        {
            DeletionTime deletion = leg.partitionLevelDeletion();
            if (deletion.isLive())
                return absentPartitionIterator(metadata, key, sstable, false);
            return UnfilteredRowIterators.noRowsIterator(metadata, key, Rows.EMPTY_STATIC_ROW,
                                                         deletion, false);
        }
        finally
        {
            leg.close();
        }
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
     * Deferred leg-open for the multi-leg forward merge: builds a {@link PendingLeg} that presents a
     * metadata lower bound but does NOT look up the partition yet, so the counted sstable read
     * happens only when the merge actually reaches this leg's data.  This mirrors the iterator
     * path's {@link org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound}, which defers
     * {@code sstable.rowIterator} to the merge's first pull and is why the {@code SSTablesIterated}
     * cost contract holds for limit-bounded reads.
     *
     * When no metadata lower bound is available (compact tables, or a requested slice at-or-before
     * the sstable's covered range — the same {@code canUseMetadataLowerBound} cases), there is
     * nothing to defer against, so this opens the leg eagerly through {@link #openLeg}, which may
     * return null for an absent partition.
     */
    public static PendingLeg openLegDeferred(SSTableReader sstable,
                                             TableMetadata metadata,
                                             DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter columnFilter,
                                             SSTableReadsListener listener,
                                             ValueTransfer transfer)
    {
        ClusteringBound<?> lowerBound = metadataLowerBound(sstable, metadata, key, slices);
        if (lowerBound == null)
            return openLeg(sstable, metadata, key, slices, columnFilter, listener, transfer);

        PendingLeg leg = new PendingLeg(sstable, metadata, key, slices, columnFilter, null, transfer);
        leg.deferOpen(listener, lowerBound);
        return leg;
    }

    /**
     * The forward-read lower bound {@link org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound}
     * would present, computed without opening the partition: the per-row key-cache bound if cached,
     * else the sstable's covered-clustering bound, else null when a lower bound would not help
     * (compact tables, or a slice start at-or-after the covered range — the leg is read regardless).
     */
    private static ClusteringBound<?> metadataLowerBound(SSTableReader sstable, TableMetadata metadata,
                                                         DecoratedKey key, Slices slices)
    {
        if (sstable instanceof KeyCacheSupport<?>)
        {
            ClusteringBound<?> cached = ((KeyCacheSupport<?>) sstable).getLowerBoundPrefixFromCache(key, false);
            if (cached != null)
                return cached;
        }
        if (metadata.isCompactTable())
            return null;
        if (!slices.isEmpty() && slices.hasLowerBound()
            && metadata.comparator.compare(slices.start(), sstable.getSSTableMetadata().coveredClustering.start()) >= 0)
            return null;
        ClusteringBound<?> bound = sstable.getSSTableMetadata().coveredClustering.open(false);
        return bound.artificialLowerBound(false);
    }

    /**
     * Finishes a single pending leg: the BTI row-index seek when applicable, then a lazy forward
     * iterator that reads the leg one unfiltered at a time as its reader pulls.  The leg's rows
     * are read as stored, with no merge, like the iterator path's single sstable iterator, and
     * the read stops at the slice end.  Byte-identical to the {@link #sstableRowIterator} output.
     * The returned iterator owns the leg and closes it in {@link UnfilteredRowIterator#close()}.
     * This is the per-leg path for a lone sstable leg with no memtable; {@link #mergeLegs} serves
     * every other leg count.
     */
    public static UnfilteredRowIterator completeSingleLeg(PendingLeg leg)
    {
        MaterializingMergeSink sink = new MaterializingMergeSink();
        CursorReadMerger merger = null;
        DeletionTime openMarkerAtStart = null;
        try
        {
            // A lone surviving cursor leg may still be deferred (elimination left one intersecting
            // sstable, no memtable): open it now.  A single-leg read always reads its one sstable,
            // so the count matches the iterator path either way.  An absent partition leaves the
            // cursor null, like a Slices.NONE leg: the iterator then has no rows.
            if (leg.deferred)
                leg.doDeferredOpen();
            if (leg.cursor != null)
            {
                openMarkerAtStart = leg.seekForSingleRead();
                merger = CursorReadMerger.forSingleLeg(leg, sink);
                merger.prepare();
            }
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
        // single leg: this iterator is the leg's validation surface
        return new ForwardSlicedCursorIterator(leg.metadata, leg.key, leg.sstable, leg.sstable.stats(), leg.columnFilter,
                                               leg.legSlices, leg.partitionLevelDeletion(), leg.staticRow(),
                                               openMarkerAtStart, merger, sink, null,
                                               Collections.singletonList(leg), true, false);
    }

    /**
     * Reverse counterpart of {@link #completeSingleLeg}: wraps the pending leg in a LAZY reverse
     * iterator (descending clustering order).  Laziness is required, not incidental: a reverse read with a small limit must touch only the
     * tail blocks, so the leg keeps its cursor open and reads (and counts) only the unfiltereds the
     * consumer actually pulls.  The returned iterator owns the leg and closes it in
     * {@link UnfilteredRowIterator#close()}.  For a multi-leg reverse read every leg is wrapped this
     * way and the legs reconcile in the shared {@code UnfilteredRowIterators.merge}, the same
     * composition the iterator path uses for a reverse read.
     */
    public static UnfilteredRowIterator completeSingleLegReversed(PendingLeg leg)
    {
        return new ReverseSlicedCursorIterator(leg);
    }

    /**
     * Cursor twin of {@code SinglePartitionReadCommand.makeRowIteratorWithLowerBound} for a reverse
     * read.  A reverse read reconciles its legs in the shared {@code UnfilteredRowIterators.merge},
     * not in the cursor-level merge, so it cannot use the deferred-leg gate.  Without this wrapper
     * every candidate sstable's partition header is read (and counted) up front, even one the merge
     * never has to descend into because a small limit is already satisfied by a higher leg.  That
     * over-reads sstables on a reverse limit query.
     *
     * <p>This returns the SAME lazy lower-bound iterator the iterator path uses
     * ({@link UnfilteredRowIteratorWithLowerBound}), so the merge sees the identical bound marker and
     * makes the identical skip decision.  The only difference is the data source: when the merge does
     * descend into the leg, {@link #initializeIterator()} opens the leg through {@link #openLeg} (the
     * counted read) and serves it reversed through {@link #completeSingleLegReversed}, instead of the
     * iterator-path {@code sstable.rowIterator}.  For a reverse read whose limit is met before this
     * leg's bound, the merge never initializes it, so {@link #openLeg} never runs and the sstable is
     * never counted.
     */
    public static UnfilteredRowIterator reversedLegWithLowerBound(SSTableReader sstable,
                                                                  TableMetadata metadata,
                                                                  DecoratedKey key,
                                                                  Slices slices,
                                                                  ColumnFilter columnFilter,
                                                                  SSTableReadsListener listener,
                                                                  ValueTransfer transfer)
    {
        return new ReversedCursorLegWithLowerBound(sstable, metadata, key, slices, columnFilter, listener, transfer);
    }

    /**
     * Lazy reverse leg used by {@link #reversedLegWithLowerBound}.  It inherits the entire lower-bound
     * marker computation (key cache, then metadata covered clustering) from
     * {@link UnfilteredRowIteratorWithLowerBound}; only the deferred data source is cursor-based.
     */
    private static final class ReversedCursorLegWithLowerBound extends UnfilteredRowIteratorWithLowerBound
    {
        private final SSTableReader sstable;
        private final TableMetadata metadata;
        private final Slices slices;
        private final ColumnFilter columnFilter;
        private final SSTableReadsListener listener;
        private final ValueTransfer transfer;

        ReversedCursorLegWithLowerBound(SSTableReader sstable,
                                        TableMetadata metadata,
                                        DecoratedKey key,
                                        Slices slices,
                                        ColumnFilter columnFilter,
                                        SSTableReadsListener listener,
                                        ValueTransfer transfer)
        {
            super(key, sstable, slices, true, columnFilter, listener);
            this.sstable = sstable;
            this.metadata = metadata;
            this.slices = slices;
            this.columnFilter = columnFilter;
            this.listener = listener;
            this.transfer = transfer;
        }

        @Override
        protected UnfilteredRowIterator initializeIterator()
        {
            // The counted partition-header read happens here, inside openLeg, and only when the merge
            // descends past this leg's lower bound -- exactly the deferral the iterator path gets.
            return sstableRowIterator(sstable, metadata, partitionKey(), slices, columnFilter, listener, true, transfer);
        }
    }

    /** Whether {@code kind} is CQL_LIMIT or CQL_PAGING_LIMIT, the limit shapes
     *  {@link FilterPushdown}'s row-level eligibility accepts.  GROUP BY counters count group
     *  boundaries and are excluded. */
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
     *   <li>caller contract: the context may reach {@link #mergeLegs} only for a read whose merged
     *       iterator is consumed by {@code executeLocally}'s stack; the other entry points must pass
     *       null.</li>
     * </ul>
     *
     * The partition-level short-circuit ({@link FilterPushdown#partitionLevelMatches}) is always
     * active once the gate engages.  Clustering- and regular-column expressions additionally drop
     * rows at production; see {@link FilterPushdown#rowLevelPushdownEligible}.
     */
    static FilterPushdown filterPushdownFor(SinglePartitionReadCommand command)
    {
        RowFilter rowFilter = command.rowFilter();
        if (rowFilter.isEmpty())
            return null;
        if (rowFilter.needsReconciliation() || !rowFilter.isStrict())
            return null;
        // The row-level filter probe simulates the emitted surface over a single contiguous slice;
        // a multi-slice read (a names filter of several clusterings) would mis-account across the
        // gaps between the point slices.  Decline so filtering stays at the top of stack.
        if (command.clusteringIndexFilter().getSlices(command.metadata()).size() > 1)
            return null;
        if (command.metadata().enforceStrictLiveness())
            return null; // MV tables are outside isReadSupported entirely; purely defensive
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
         * (clustering- or regular-column) expressions and either an unlimited query or a
         * CQL_LIMIT / CQL_PAGING_LIMIT limit ({@code isBoundableLimitKind(limits().kind())}).  The
         * merge is pulled by the top-level {@code DataLimits} counter, so it drops and accounts
         * rows only as far as that counter reads, the same rows the iterator path scans.  This
         * constructor runs only after {@link #filterPushdownFor}'s gate already passed for this
         * command, so the only residual question is the limit's shape.  This is not part of the
         * gate itself: the context still attaches for the partition-level short-circuit, which is
         * consumption-independent.
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
     *   <li>the production-time abort ({@link #productionAbort}): {@code MetricRecording} never sees
     *       a dropped row, so the merge keeps a combined tombstone count over the surface
     *       {@code MetricRecording} would scan.  When a dropped row's tombstone crosses the
     *       threshold, it aborts with the same side effects (Tracing message, metric increment,
     *       exception message).  An emitted tombstone that crosses it is left to
     *       {@code MetricRecording}, which sees it next.</li>
     * </ul>
     *
     * The merge runs only as far as the reader pulls, so the combined count covers the same rows
     * the iterator path scans (see {@code FilterPushdown.rowLevelPushdownEligible}).
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
        private final boolean respectTombstoneThresholds;

        // dropped-row totals folded into MetricRecording (pull side)
        private int droppedLiveRows;
        private int droppedTombstones;
        private int sampledDroppedLiveRows;
        private int sampledDroppedTombstones;

        // production-order combined tombstone count driving the abort (statics + emitted surface +
        // dropped rows, counted in the order MetricRecording would scan them)
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
            this.respectTombstoneThresholds = !SchemaConstants.isLocalSystemKeyspace(command.metadata().keyspace);
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
         * production-order tombstone count.  Two passes: the first computes the post-purge
         * {@code hasDeletion(nowInSec)} gate, the second counts in MetricRecording's order
         * (surviving dead cells first, then the PK-deletion-only verdict).
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
                        emittedTombstone();
                    }
                }
            }
            if (!pkLive && !anyLiveCell && hasDeletion && !hasTombstones)
                emittedTombstone();
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
                emittedTombstone();
        }

        /** An artificial slice-bound marker the slicer will synthesize (open at the slice start /
         *  close at the slice end when a range deletion covers the bound) — the iterator path's
         *  merged stream carries the identical artificial markers through the purge stage into
         *  MetricRecording. */
        void accountArtificialMarker(ClusteringBound<?> bound, DeletionTime openDeletion)
        {
            if (!shouldPurge(openDeletion))
                emittedTombstone();
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

        /** A tombstone on the emitted surface.  It is only counted: the reader takes it next, and
         *  {@code MetricRecording} aborts on it if it crosses the threshold, as on the iterator path. */
        private void emittedTombstone()
        {
            ++combinedTombstones;
        }

        // ---- the production-time abort ----

        /**
         * Aborts the query when the tombstone count passes the failure threshold, with what
         * {@code MetricRecording.countTombstone} does at its own abort: the trace, the failure
         * count, the warning params, and TombstoneOverwhelmingException.  The merge runs inside
         * the {@code MetricRecording} stream, so its {@code onPartitionClose}/{@code onClose}
         * record the samples, latency, histograms and totals once the read closes, folding in the
         * dropped rows, exactly as on the iterator path.
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
     * The probe forwards every event to the materializing sink unconditionally.
     * {@code CursorReadMerger} calls {@code endRow}/{@code addRow} only for a row group it did NOT
     * abandon, so a filter-dropped row never reaches the reader or its limit counter.
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
     * The emitted-surface simulation matches {@code ForwardSlicedCursorIterator}: elements at or
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
        /** The materializing sink this probe forwards every MergeSink event to.  Typed so
         *  {@link #accountEmittedRow} can look at the row it just built. */
        private final MaterializingMergeSink next;
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

        RowLevelFilterProbe(MaterializingMergeSink next, FilterPushdown pushdown, DecoratedKey key,
                            ClusteringComparator comparator, Slice slice)
        {
            this.next = next;
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
         *  the slicing iterator starts its open-marker tracking from. */
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
                // already-materialized shell
                next.abandonRow();
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
            // next.endRow() emits at most one row, and drops a row that merged to empty
            long before = next.materializedCount();
            next.endRow();
            if (next.materializedCount() != before)
                accountEmittedRow();
        }

        @Override
        public void addRow(Row row)
        {
            long before = next.materializedCount();
            next.addRow(row);
            if (next.materializedCount() != before)
                accountEmittedRow();
        }

        private void accountEmittedRow()
        {
            Row row = (Row) next.peek();
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

        /** Matches {@code ForwardSlicedCursorIterator.computeNextInSlice}'s non-strict pre-slice
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

        /** Called once the read reaches the end of its slice, never when the reader stops early:
         *  the slicer opens the slice even when nothing survived to be emitted, and artificially
         *  closes a still-open range deletion at the slice end — both reach MetricRecording on the
         *  iterator path. */
        void finishPartition()
        {
            openSurface();
            if (simOpenMarker != null)
                acc.accountArtificialMarker(sliceEnd, simOpenMarker);
        }
    }

    /**
     * The cursor-level k-way merge across one or more pending sstable legs of one partition read.
     * Reconciles at the descriptor/byte level below materialization; only merge winners become
     * {@code Row}/{@code Cell} objects.  Returns one iterator whose emitted stream is byte-identical
     * to the object-level merge of the per-leg iterators.  Memtable legs join the same merge
     * through {@link MemtableMergeLeg}.
     *
     * The merge is lazy: it merges one group each time the returned iterator needs another
     * unfiltered, so a read that stops early (a limit, a page) never merges the rest of the
     * partition.  The returned iterator owns the legs and closes them in
     * {@link UnfilteredRowIterator#close()}; if setup fails, the legs are closed before this throws.
     *
     * Each indexed BTI leg seeks to its row-index floor block for the first slice start before the
     * merge runs ({@link PendingLeg#seekForMerge}), with its open-range-tombstone state seeded into
     * the merge's cross-leg open-marker set, and again at each later slice start whose floor block
     * lies ahead of it ({@code CursorReadMerger.moveToSlice}).  BIG legs read from the partition
     * start.  The merge core stops at each slice end.
     *
     * This merge reconciles only.  It makes no purge decisions (reads purge above the merge via
     * {@code withoutPurgeableTombstones}), no expired-TTL-to-tombstone conversion, and no MV row
     * skipping (MV tables are gated out).  Corrupted-tombstone validation stays at the per-leg,
     * in-slice placement.
     *
     * @param filterPushdown  the query's filter pushdown context ({@link #filterPushdownFor}), or
     *                        null when pushdown is disengaged.  When set, its partition-level
     *                        expressions can skip the row-group merge, and once
     *                        {@code filterPushdown.scanStats() != null} its row-level expressions
     *                        can drop individual rows through {@link RowLevelFilterProbe}.
     */
    public static UnfilteredRowIterator mergeLegs(List<? extends MergeLeg> legs,
                                                  TableMetadata metadata,
                                                  DecoratedKey key,
                                                  Slices slices,
                                                  ColumnFilter columnFilter,
                                                  FilterPushdown filterPushdown)
    {
        assert !legs.isEmpty() : "the merge core needs at least one leg";
        MaterializingMergeSink sink = new MaterializingMergeSink();
        MergeContext<MaterializingMergeSink> ctx = setUpMergeOrCloseLegs(legs, metadata, key, slices, columnFilter,
                                                                          filterPushdown, sink, sink);
        SSTableReader attribution = null;
        for (MergeLeg leg : legs)
        {
            if (leg.sstableOrNull() != null)
            {
                attribution = leg.sstableOrNull();
                break;
            }
        }
        assert attribution != null : "the gate requires at least one sstable leg";

        // min-merge over the same per-leg stats the per-leg iterators would report, so the
        // response's stats total matches the object merge's. Non-tracking NAMES reads no longer
        // reach this merge (they take the timestamp-order driver
        // queryMemtableAndCursorsInTimestampOrder), so the NO_STATS tombstone-only-leg quirk is
        // gone: every leg here folds its real stats.
        EncodingStats stats = EncodingStats.merge(legs, MergeLeg::legStats);
        // validateOnEmission = false: per-leg validation inside the merge is the read's entire
        // validation, like the iterator path
        return new ForwardSlicedCursorIterator(metadata, key, attribution, stats, columnFilter, ctx.emitSlices,
                                               ctx.mergedDeletion, ctx.mergedStatic, ctx.openMarkerAtStart,
                                               ctx.merger, sink, ctx.filterProbe, legs, false, true);
    }

    /**
     * The form of {@link #mergeLegs} that merges the whole partition into a caller's sink, made by
     * a {@link MergeSinkFactory} instead of a hardcoded {@code MaterializingMergeSink}: the same leg
     * setup, then every group merged into the sink ({@code CursorReadMerger.mergeUnfiltereds}) before this returns.  The legs are
     * closed before this returns.  The transcode response path uses it, and tests drive the real
     * merge core through it against a non-materializing sink.  Row-level filter pushdown is not
     * supported here: only the partition-level short-circuit of {@code filterPushdown} applies.
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
        FilterPushdown filterPushdown,
        MergeSinkFactory<S> sinkFactory)
    {
        assert !legs.isEmpty() : "the merge core needs at least one leg";
        try
        {
            MergeContext<S> ctx = setUpMerge(legs, metadata, key, slices, columnFilter, filterPushdown,
                                             sinkFactory.newSink(), null);
            if (ctx.sink instanceof ResponseSink)
            {
                // the production path purges and counts the static row when it writes the header
                Row purgedStatic = ((ResponseSink) ctx.sink).purgeAndCountStaticRow(ctx.mergedStatic);
                ctx = new MergeContext<>(ctx.sink, ctx.mergedDeletion, purgedStatic, ctx.openMarkerAtStart,
                                         ctx.emitSlices, ctx.merger, ctx.filterProbe);
            }
            if (ctx.merger != null)
                ctx.merger.mergeUnfiltereds();
            return ctx;
        }
        catch (IOException e)
        {
            throw new RuntimeException("cursor merge failed for " + key + " over " + legs.size() + " legs", e);
        }
        finally
        {
            for (MergeLeg leg : legs)
                leg.close();
        }
    }

    /**
     * Writes one partition's replica data response into {@code out} through {@code sink}, merging
     * {@code legs}: the partition header, then each slice's merge groups while the sink wants more,
     * then the partition end.  A lone sstable leg the iterator path would read without a merge is
     * read as stored ({@link CursorReadMerger#forSingleLeg}).  Does not close the legs.
     *
     * @return whether the partition reached the response; false when it was removed
     */
    static boolean streamResponse(List<? extends MergeLeg> legs, TableMetadata metadata, DecoratedKey key, Slices slices,
                                  ColumnFilter columnFilter, ResponseSink sink, ResponseSink.ResponseBuffer out) throws IOException
    {
        if (legs.isEmpty())
        {
            // no sstable holds the partition: the iterator path merges empty sources, which give
            // an empty partition
            sink.partitionMetadataKnown(DeletionTime.LIVE, Rows.EMPTY_STATIC_ROW);
            sink.beginPartition(out, key, columnFilter, DeletionTime.LIVE, Rows.EMPTY_STATIC_ROW);
            return sink.finishPartition();
        }
        MergeContext<ResponseSink> ctx = sink.singleSource && legs.get(0) instanceof PendingLeg
                                         ? setUpSingleLeg((PendingLeg) legs.get(0), sink)
                                         : setUpMerge(legs, metadata, key, slices, columnFilter, null, sink, null);
        sink.beginPartition(out, key, columnFilter, ctx.mergedDeletion, ctx.mergedStatic);
        CursorReadMerger merger = ctx.merger;
        if (merger != null)
        {
            boolean movedToAdjacentSlice = false;
            for (int i = 0; i < ctx.emitSlices.size() && sink.wantsMore(); i++)
            {
                if (i > 0 && !movedToAdjacentSlice)
                {
                    boolean seeked = merger.moveToSlice(i);
                    sink.beginSlice(i, seeked, seeked ? merger.openDeletionAtSeek() : null);
                }
                movedToAdjacentSlice = false;
                while (sink.wantsMore() && merger.advance())
                {
                    // each advance merges one group into the sink
                }
                if (!sink.wantsMore())
                    break;
                if (merger.isAdjacentSliceInMerge(i + 1))
                {
                    List<RangeTombstoneMarker> markers = merger.moveToAdjacentSlice(i + 1);
                    sink.finishSliceBeforeAdjacent(markers, i + 1, merger.openDeletionAtSeek());
                    movedToAdjacentSlice = true;
                }
                else
                {
                    sink.finishSlice();
                }
            }
        }
        return sink.finishPartition();
    }

    /**
     * {@link #streamResponse} for a lone object-backed source (a memtable, or the merged repaired
     * sstables of a read that tracks repaired data): the iterator path serializes that iterator
     * unchanged, so its rows and markers go to the sink as they come, already sliced.  Pulls from
     * {@code source} only while the sink wants more.  Does not close it.
     */
    static boolean streamResponse(UnfilteredRowIterator source, ColumnFilter columnFilter, ResponseSink sink,
                                  ResponseSink.ResponseBuffer out) throws IOException
    {
        sink.partitionMetadataKnown(source.partitionLevelDeletion(), source.staticRow());
        sink.beginPartition(out, source.partitionKey(), columnFilter, source.partitionLevelDeletion(), source.staticRow());
        sink.inputAlreadySliced();
        while (sink.wantsMore() && source.hasNext())
        {
            Unfiltered unfiltered = source.next();
            if (unfiltered.isRow())
                sink.addRow((Row) unfiltered);
            else
                sink.addRangeTombstoneMarker((RangeTombstoneMarker) unfiltered);
        }
        return sink.finishPartition();
    }

    /**
     * {@link #streamResponse} for a reverse read of one sstable leg, which the iterator path reads
     * as stored with its reverse sstable iterator: the rows go from the leg to the sink in reverse
     * clustering order, through {@link ReverseSlicedCursorIterator}.  Does not close the leg.
     */
    static boolean streamReversedResponse(PendingLeg leg, ColumnFilter columnFilter, ResponseSink sink,
                                          ResponseSink.ResponseBuffer out) throws IOException
    {
        if (leg.deferred)
            leg.doDeferredOpen();
        DeletionTime partitionDeletion = leg.partitionLevelDeletion();
        Row staticRow = leg.staticRow();
        sink.partitionMetadataKnown(partitionDeletion, staticRow);
        sink.beginPartition(out, leg.key, columnFilter, partitionDeletion, staticRow);
        sink.inputAlreadySliced();
        if (leg.cursor != null)
        {
            ReverseSlicedCursorIterator reversed = new ReverseSlicedCursorIterator(leg);
            try
            {
                reversed.streamTo(sink);
            }
            finally
            {
                reversed.closeBlockCursor();
            }
        }
        return sink.finishPartition();
    }

    /** The single-leg setup of {@link #completeSingleLeg}, for {@link #streamResponse}. */
    private static MergeContext<ResponseSink> setUpSingleLeg(PendingLeg leg, ResponseSink sink) throws IOException
    {
        // a lone leg may still be deferred (elimination left one intersecting sstable): open it now
        if (leg.deferred)
            leg.doDeferredOpen();
        DeletionTime partitionDeletion = leg.partitionLevelDeletion();
        Row staticRow = leg.staticRow();
        sink.partitionMetadataKnown(partitionDeletion, staticRow);
        CursorReadMerger merger = null;
        DeletionTime openMarkerAtStart = null;
        if (leg.cursor != null)
        {
            openMarkerAtStart = leg.seekForSingleRead();
            merger = CursorReadMerger.forSingleLeg(leg, sink);
            merger.prepare();
            sink.initOpenMarker(openMarkerAtStart);
        }
        return new MergeContext<>(sink, partitionDeletion, staticRow, openMarkerAtStart,
                                  merger == null ? Slices.NONE : leg.legSlices, merger, null);
    }

    /** {@link #setUpMerge} for {@link #mergeLegs}: on failure the legs are closed before this throws,
     *  since no iterator exists yet to own them. */
    private static <S extends CursorReadMerger.MergeSink> MergeContext<S> setUpMergeOrCloseLegs(
        List<? extends MergeLeg> legs,
        TableMetadata metadata,
        DecoratedKey key,
        Slices slices,
        ColumnFilter columnFilter,
        FilterPushdown filterPushdown,
        S sink,
        MaterializingMergeSink probeTarget)
    {
        try
        {
            return setUpMerge(legs, metadata, key, slices, columnFilter, filterPushdown, sink, probeTarget);
        }
        catch (IOException e)
        {
            RuntimeException failure = new RuntimeException("cursor merge failed for " + key + " over " + legs.size() + " legs", e);
            closeAll(legs, failure);
            throw failure;
        }
        catch (RuntimeException | Error e)
        {
            closeAll(legs, e);
            throw e;
        }
    }

    /**
     * The setup shared by {@link #mergeLegs} and {@link #mergeLegsWithSink}: the partition
     * deletion and static row merge, the partition-level filter short-circuit, the per-leg seeks,
     * and the prepared {@code CursorReadMerger}.  No group is merged yet.  Does not close the legs.
     *
     * @param probeTarget the materializing sink a {@link RowLevelFilterProbe} forwards to, or null
     *                    for {@link #mergeLegsWithSink}, which does not support row-level filter pushdown
     */
    private static <S extends CursorReadMerger.MergeSink> MergeContext<S> setUpMerge(
        List<? extends MergeLeg> legs,
        TableMetadata metadata,
        DecoratedKey key,
        Slices slices,
        ColumnFilter columnFilter,
        FilterPushdown filterPushdown,
        S sink,
        MaterializingMergeSink probeTarget) throws IOException
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

        Row mergedStatic;
        if (sink instanceof ResponseSink)
        {
            ResponseSink response = (ResponseSink) sink;
            // UnfilteredRowIterators.merge returns a lone source unchanged, static row included
            mergedStatic = response.singleSource ? legs.get(0).staticRow()
                                                 : mergeStaticRows(legs, columnFilter.fetchedColumns().statics, mergedDeletion);
            response.partitionMetadataKnown(mergedDeletion, mergedStatic);
        }
        else
        {
            mergedStatic = mergeStaticRows(legs, columnFilter.fetchedColumns().statics, mergedDeletion);
        }

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
        // cursors are closed unread when the legs close.
        List<MergeLeg> rowLegs = new ArrayList<>(legs.size());
        if (!partitionSkippedByFilter)
        {
            for (MergeLeg leg : legs)
            {
                if (!leg.legSlices().isEmpty())
                    rowLegs.add(leg);
            }
        }

        CursorReadMerger merger = null;
        RowLevelFilterProbe filterProbe = null;
        DeletionTime openMarkerAtStart = null;
        if (!rowLegs.isEmpty())
        {
            // The full slice set drives the merge scan and per-slice validation. A slice read has
            // one slice; a names read has one point slice per requested clustering. The filter
            // probe below engages only for a single-slice read (filter pushdown declines
            // multi-slice), so it takes the representative first slice.
            Slices rowLegSlices = rowLegs.get(0).legSlices();
            Slice slice = rowLegSlices.get(0);
            for (MergeLeg leg : rowLegs)
            {
                if (leg.isDeferred())
                {
                    // A deferred leg whose data may start at or before the merge start must be
                    // opened now, so its row-index open marker seeds the merger's
                    // start-of-merge snapshot; dropping that seed would silently drop a range
                    // tombstone open across the slice start.  A leg that begins strictly after
                    // the merge start stays deferred and is opened lazily, only if the merge
                    // reaches its data (the SSTablesIterated cost win).
                    //
                    // This force-open is reachable only on BIG format with a primed key cache.
                    // A deferred leg's lower bound comes from metadataLowerBound: on BTI that is
                    // only the covered-clustering bound, and a leg is deferred against it only
                    // when the slice start is strictly below the covered start (an unbounded
                    // slice short-circuits on mergeStart.isBottom()), so a BTI leg can never be
                    // both deferred AND spanning the merge start.  Only the per-row key-cache
                    // bound -- KeyCacheSupport, which BigTableReader implements and
                    // BtiTableReader does not -- can sit at or before the slice start while the
                    // leg stays deferred.  On BTI SSTABLE_LEGS_FORCE_OPENED therefore stays zero
                    // here; a later slice start can still force a BTI leg open
                    // (CursorReadMerger.moveToSlice).
                    if (leg.deferredSpansMergeStart(slice.start(), metadata.comparator))
                    {
                        SSTABLE_LEGS_FORCE_OPENED.incrementAndGet();
                        leg.openForMerge(slice.start());
                    }
                    continue;
                }
                // A leg force-opened for its metadata -- partition-level deletion or a fetched
                // static row pulled it through the deferred gate (ensureOpenedForMetadata) before
                // this loop -- can find the partition absent: doDeferredOpen took its absent branch
                // and left the leg non-deferred, exhausted, and without a cursor.  Its cursor
                // reaches the DONE state and contributes no rows, so skip merge-mode entry, exactly
                // as openForMerge does for an exhausted leg.  This keeps enterMergeMode's
                // `assert cursor != null` intact instead of tripping it.
                //
                // Behaviour-preserving for every DONE leg, not only the exhausted-absent one: the
                // sole way a leg seeds an open range tombstone into the merge start is
                // openDeletion, which is set only by a seek, and seekPointFor returns no seek for
                // a DONE cursor (a DONE partition has no unfiltered to seek over).  A DONE leg therefore carries no open range tombstone
                // the merger would otherwise pick up, so skipping the seek drops nothing.
                if (leg.cursorState() == DONE)
                    continue;
                leg.enterMergeMode();
                // per-leg BTI row-index seek for the first slice, after the elimination loop
                // confirmed the leg participates.  No-op for memtable legs (already in memory).
                // Later slices seek in CursorReadMerger.moveToSlice.
                leg.seekForMerge(slice.start());
            }
            // Row-level filter pushdown and its scan-stats accounting.  When engaged,
            // RowLevelFilterProbe sits in front of the materializing sink.
            CursorReadMerger.MergeSink mergeSink = sink;
            if (filterPushdown != null && filterPushdown.scanStats() != null)
            {
                if (probeTarget == null)
                    throw new IllegalArgumentException("row-level filter pushdown needs a materializing sink");
                filterProbe = new RowLevelFilterProbe(probeTarget, filterPushdown, key, metadata.comparator, slice);
                mergeSink = filterProbe;
            }
            merger = new CursorReadMerger(rowLegs.toArray(new MergeLeg[0]), metadata,
                                          mergedDeletion, rowLegSlices, mergeSink, filterProbe);
            // supersedes-max over the seeked legs' row-index open-deletion seeds; the slicing
            // iterator synthesizes the artificial open marker from it
            openMarkerAtStart = merger.openMarkerAtMergeStart();
            if (filterProbe != null)
            {
                filterProbe.initOpenMarker(openMarkerAtStart);
                // the merged static row is scanned first on the pull side, so account it before
                // any stream element
                filterPushdown.scanStats().accountRow(mergedStatic);
            }
            // ResponseSink needs the same seek-state seed for its own open-marker tracking
            if (sink instanceof ResponseSink)
                ((ResponseSink) sink).initOpenMarker(openMarkerAtStart);
            merger.prepare();
        }

        Slices emitSlices = rowLegs.isEmpty() ? Slices.NONE : rowLegs.get(0).legSlices();
        return new MergeContext<>(sink, mergedDeletion, mergedStatic, openMarkerAtStart, emitSlices, merger, filterProbe);
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
        /** The prepared merge core, or null when no leg contributes rows. */
        final CursorReadMerger merger;
        /** The row-level filter probe in front of the sink, or null when not engaged. */
        final RowLevelFilterProbe filterProbe;

        private MergeContext(S sink, DeletionTime mergedDeletion, Row mergedStatic, DeletionTime openMarkerAtStart,
                             Slices emitSlices, CursorReadMerger merger, RowLevelFilterProbe filterProbe)
        {
            this.sink = sink;
            this.mergedDeletion = mergedDeletion;
            this.mergedStatic = mergedStatic;
            this.openMarkerAtStart = openMarkerAtStart;
            this.emitSlices = emitSlices;
            this.merger = merger;
            this.filterProbe = filterProbe;
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

        // Single-live guard.  One ValueTransfer is now shared across every leg AND every command of
        // one query execution (see ReadExecutionController.cursorValueTransfer), so a re-entrant or
        // concurrent cell copy that shared it would silently corrupt a value.  acquire()/release()
        // bracket each cell-value materialization; both are only exercised under assertions (-ea), so
        // production pays nothing.
        private boolean inUse;

        /** @return true if the scratch was free and is now marked in use; false if already in use. */
        boolean acquire()
        {
            if (inUse)
                return false;
            inUse = true;
            return true;
        }

        /** Clears the in-use mark; always returns true so it can run as an assert in a finally block. */
        boolean release()
        {
            inUse = false;
            return true;
        }
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
        private final AbstractType<?>[] clusteringTypes;
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

        // Reused counter-context clear buffer, lazily created only for counter tables: a live counter
        // cell read on the iterator path deserializes with Flag.LOCAL, which clears marked-local
        // shards (DeserializationHelper.maybeClearCounterValue -> CounterContext.clearAllLocal). The
        // cursor copies value bytes directly and bypasses that, so it applies the clear here. Null
        // for non-counter tables.
        private CursorCounterContexts counterContexts;

        // Reverse-collect scratch (see ReverseSlicedCursorIterator / collectStep): the clustering of
        // the unfiltered the last collectStep read, and — when it was a range-tombstone marker — the
        // marker itself (null for a row).  Reused per collect step; the collect pass never
        // materializes cells, so no per-row garbage beyond the clustering the compare needs.
        private RangeTombstoneMarker reverseMarker;

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
            this.uDesc = new UnfilteredDescriptor(clusteringTypes);

            // Deletion-only complex columns must surface as positions of their own
            // (compaction's pattern): the cursor holds the current column's deletion in
            // CellCursor.complexDeletion, and materializeRowContents records it on entering
            // each complex column, mirroring UnfilteredSerializer.readComplexColumn.
            cursor.pauseAtEmptyComplexColumns(true);
        }

        // set by openPartition
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
         * {@code PARTITION_END}); rows are then read by the merge core.
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
                        assert transfer.acquire() : "ValueTransfer single-live invariant violated (concurrent cursor cell copy)";
                        try
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
                                state = cursor.copyCellValue(transfer.valueCapture.prepareVariable(), null);
                            }
                            value = transfer.valueCapture.finish();
                        }
                        finally
                        {
                            assert transfer.release();
                        }
                    }

                    // Live counter cell: apply the marked-local shard clear the iterator path gets
                    // from Flag.LOCAL. A tombstone/expiring cell (localDeletionTime set) is not a
                    // counter cell and keeps its bytes; an empty value (fetched-but-not-queried,
                    // CASSANDRA-10657) has no context to clear.
                    if (cellType.isCounter() && localDeletionTime == Cell.NO_DELETION_TIME && value.length > 0)
                    {
                        if (counterContexts == null)
                            counterContexts = new CursorCounterContexts();
                        int cleared = counterContexts.clearMarkedLocal(value, 0, value.length);
                        if (cleared >= 0)
                            value = Arrays.copyOf(counterContexts.scratchBuffer(), cleared);
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
                return clusteringTypes.length == 0
                       ? ByteArrayAccessor.factory.clustering()
                       : ByteArrayAccessor.factory.clustering(readClusteringValues(clusteringTypes.length));
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
                int length = clusteringTypes[i].valueLengthIfFixed();
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

        // ---- reverse read support (see ReverseSlicedCursorIterator) ----

        /** The on-disk start (flags byte position) of the unfiltered the cursor is about to read.
         *  Valid at ROW_START/TOMBSTONE_START: the flags byte has already been consumed, so the
         *  unfiltered starts one byte behind the cursor. */
        long unfilteredStart()
        {
            return cursor.position() - 1;
        }

        /** Seeks the cursor to a partition-relative unfiltered start, arriving exactly as a
         *  row-by-row walk to that unfiltered would; returns the resulting cursor state. */
        int seekUnfiltered(long position)
        {
            return cursor.seekUnfiltered(position);
        }

        /**
         * Reverse collect step.  Reads the clustering of the unfiltered at the cursor's current
         * ROW_START/TOMBSTONE_START into {@link #uDesc} and, when it is a range-tombstone marker,
         * the marker itself (into {@link #reverseMarker}, else null), then advances the cursor PAST
         * that unfiltered WITHOUT reading the rest of a row.
         * Returns the cursor state at the next unfiltered (or PARTITION_END/DONE).  Does not touch
         * {@code UNFILTEREDS_MATERIALIZED}: only the pop phase counts, so a limited reverse read that
         * pops only the tail is not charged for the header pass.  Mirrors the {@code skipNext()} /
         * {@code readNext()} classification in {@code SSTableReversedIterator.fillOffsets}.
         */
        int collectStep(int state) throws IOException
        {
            if (state == ROW_START)
            {
                state = cursor.readRowClusteringAndSkip(uDesc);
                reverseMarker = null;
            }
            else if (state == TOMBSTONE_START)
            {
                state = cursor.readTombstoneMarker(uDesc);
                reverseMarker = materializeMarker();
                if (state == UNFILTERED_END)
                    state = cursor.continueReading();
            }
            else
            {
                throw new IllegalStateException("unexpected cursor state " + state);
            }
            return state;
        }

        /** The clustering of the unfiltered the last {@link #collectStep} read, in its wire form. */
        ClusteringDescriptor collectedClustering()
        {
            return uDesc;
        }

        /** The marker the last {@link #collectStep} read, or null if that unfiltered was a row. */
        RangeTombstoneMarker collectedMarker()
        {
            return reverseMarker;
        }

        /**
         * Reverse pop step.  Seeks to a collected position, materializes the single unfiltered there
         * (counting it in {@code UNFILTEREDS_MATERIALIZED} and validating it, like
         * {@code SSTableReversedIterator.ReverseReader.computeNext}), and returns it; null for an
         * empty row, which the iterator path skips too.
         */
        Unfiltered popUnfilteredAt(long position) throws IOException
        {
            int state = cursor.seekUnfiltered(position);
            Unfiltered result;
            if (state == ROW_START)
            {
                state = cursor.readRowHeader(uDesc);
                Clustering<?> clustering = (Clustering<?>) toClusteringPrefix();
                rowBuilder.newRow(clustering);
                materializeRowContents(state);
                result = rowBuilder.build();
            }
            else if (state == TOMBSTONE_START)
            {
                cursor.readTombstoneMarker(uDesc);
                result = materializeMarker();
            }
            else
            {
                throw new IllegalStateException("reverse pop seek yielded state " + state);
            }
            UNFILTEREDS_MATERIALIZED.incrementAndGet();
            UnfilteredValidation.maybeValidateUnfiltered(result, metadata, key, sstable);
            return result.isEmpty() ? null : result;
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
        /** The row-index seek for the first slice start; see {@link PendingLeg#seekForMerge}. */
        void seekForMerge(ClusteringBound<?> sliceStart) throws IOException;

        /**
         * The range deletion open in this leg's own stream at its current position, or null.  Set
         * by a row-index seek and kept current by {@link #noteMarker}.  Reused storage, valid until
         * the leg moves.  Always null for a leg that never seeks (a memtable leg).
         */
        default DeletionTime openDeletion()
        {
            return null;
        }

        /** Updates {@link #openDeletion} from the marker currently in {@link #unfiltered()}, which
         *  the merge has just applied. */
        default void noteMarker()
        {
        }

        /**
         * The row-index block to seek to for a slice starting at {@code sliceStart}: the floor
         * block, when it lies past this leg's current position.  Null when no forward seek
         * applies (a BIG or memtable leg, an unindexed partition, or a floor block already reached).
         */
        default BtiCursorSeekSupport.SeekPoint seekPointFor(ClusteringBound<?> sliceStart)
        {
            return null;
        }

        /** Seeks to a block {@link #seekPointFor} returned, and takes the block's open deletion as
         *  this leg's {@link #openDeletion}. */
        default void seekTo(BtiCursorSeekSupport.SeekPoint point) throws IOException
        {
            throw new UnsupportedOperationException("only a seekable leg can seek");
        }

        /** Consumes the row whose header is loaded without reading its cells, leaving the leg at
         *  {@code UNFILTERED_END}. */
        void skipRow() throws IOException;

        /**
         * A deferred leg: its partition header (the sstable lookup the {@code SSTablesIterated}
         * metric counts) is NOT yet read.  The leg presents a metadata lower bound through
         * {@link #unfiltered()} so the merge can sort it without touching the file, exactly like the
         * iterator path's {@code UnfilteredRowIteratorWithLowerBound}.  It is opened by
         * {@link #openForMerge} only when the merge reaches its data.  Always false for memtable
         * legs and for a leg whose header is already read.
         */
        default boolean isDeferred()
        {
            return false;
        }

        /**
         * Opens a still-{@link #isDeferred() deferred} leg for merge consumption: performs the
         * partition lookup (the counted read), enters merge mode, applies the row-index seek for
         * the current slice start, and reads the first unfiltered header.  When the partition is absent from the sstable the leg
         * becomes exhausted (its {@link #cursorState()} reports {@code DONE}, no count), matching an
         * iterator whose lazy init finds no partition.  A no-op on non-deferred legs.
         */
        default void openForMerge(ClusteringBound<?> sliceStart) throws IOException
        {
            throw new UnsupportedOperationException("only a deferred leg can be opened for merge");
        }

        /**
         * For a {@link #isDeferred() deferred} leg: whether its data can start at or before
         * {@code mergeStart}, so it might carry a range tombstone open across the merge start.  Such
         * a leg must be force-opened before the merger snapshots the start-of-merge open marker.
         * False for a leg that begins strictly after the merge start (its tombstones arrive in its
         * own stream), for a merge starting at {@code BOTTOM} (no seek, tombstones arrive in-stream),
         * and always for a memtable leg.
         */
        default boolean deferredSpansMergeStart(ClusteringBound<?> mergeStart, ClusteringComparator comparator)
        {
            return false;
        }

        // ---- unfiltered walk (consumed by CursorReadMerger) ----
        int cursorState();

        /** Skips the rows at or before {@code bound} without reading their cells, from a row
         *  whose header is not read yet; returns the cursor state after them. */
        default int skipRowsAtOrBefore(ClusteringDescriptor bound) throws IOException
        {
            return cursorState();
        }

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
     * ({@code >= 2} legs).  Either way the returned iterator owns the leg and closes it.
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
        // Non-final because a deferred leg (see deferOpen) looks up its BTI partition entry lazily
        // in openForMerge, not at construction.
        private BtiCursorSeekSupport.PartitionEntry btiEntry;
        /** the query's shared value-transfer scratch (one instance across all of a read's legs) */
        private final ValueTransfer transfer;

        private SSTableCursorReader cursor; // null once closed
        /** The partition's row index, opened at the first seek and kept for later slices. */
        private RowIndexReader rowIndex;
        private PartitionMaterializer materializer;
        private int openState;

        // ---- deferred-open state (see deferOpen / openForMerge) ----
        /** true while the partition header is NOT yet read: the leg presents deferredLowerBound in
         *  the merge and has performed no counted sstable read.  Cleared once opened or exhausted. */
        private boolean deferred;
        /** set when a deferred open finds no partition in the sstable: the leg reports DONE and
         *  contributes only its stats, exactly like an iterator whose lazy init finds nothing. */
        private boolean deferredExhausted;
        /** the listener/metrics sink the deferred partition lookup notifies, captured at deferOpen. */
        private SSTableReadsListener deferredListener;
        /** the metadata lower bound this leg sorts by while deferred (comparison-only). */
        private UnfilteredDescriptor deferredLowerBound;
        /** the same lower bound as a live bound, for the merge-start slice-span check. */
        private ClusteringBound<?> deferredLowerBoundValue;
        // ---- row-index seek state ----
        /** see {@link #openDeletion()}; meaningful only while {@link #hasOpenDeletion} */
        private final DeletionTime.ReusableDeletionTime openDeletion = DeletionTime.ReusableDeletionTime.live();
        private boolean hasOpenDeletion;
        /** The file position of the unfiltered whose header {@link #readUnfilteredHeader} loaded. */
        private long loadedUnfilteredStart;

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
            cursor = SSTableCursorReader.forRead(sstable,
                                                 Collections.singletonList(new PartitionPositionBounds(position, sstable.uncompressedLength())));
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

        /**
         * Arms this leg as a deferred leg: it presents {@code lowerBound} in the merge sort and does
         * NOT look up the partition (no counted read) until {@link #openForMerge} — or until a
         * metadata question ({@link #partitionLevelDeletion}/{@link #staticRow}) forces it, exactly
         * as the iterator path's lazy lower-bound iterator does.
         */
        void deferOpen(SSTableReadsListener listener, ClusteringBound<?> lowerBound)
        {
            this.deferred = true;
            this.deferredListener = listener;
            this.deferredLowerBoundValue = lowerBound;
            AbstractType<?>[] clusteringTypes = metadata.comparator.subtypes();
            this.deferredLowerBound = UnfilteredDescriptor.forBound(clusteringTypes, lowerBound);
        }

        /** The counted partition lookup + header read, shared by the deferred-open entry points.
         *  Sets {@link #deferredExhausted} when the partition is absent (no count). */
        private void doDeferredOpen() throws IOException
        {
            long position;
            if (sstable instanceof BtiTableReader)
            {
                btiEntry = BtiCursorSeekSupport.exactPartitionEntry((BtiTableReader) sstable, key, deferredListener);
                if (btiEntry == null)
                {
                    deferred = false;
                    deferredExhausted = true;
                    SSTABLE_LEGS_WITHOUT_PARTITION.incrementAndGet();
                    return;
                }
                position = btiEntry.dataPosition;
            }
            else
            {
                position = sstable.getPosition(key, SSTableReader.Operator.EQ, deferredListener);
                if (position < 0)
                {
                    deferred = false;
                    deferredExhausted = true;
                    SSTABLE_LEGS_WITHOUT_PARTITION.incrementAndGet();
                    return;
                }
            }
            open(position);
            SSTABLE_LEGS_SERVED.incrementAndGet();
            deferred = false;
        }

        /** Forces a deferred leg open to answer a partition-level metadata question, mirroring
         *  {@link #openLeg}'s exception handling (the interface methods cannot throw IOException). */
        private void ensureOpenedForMetadata()
        {
            if (!deferred)
                return;
            try
            {
                doDeferredOpen();
            }
            catch (RuntimeException | Error e)
            {
                close();
                throw e;
            }
            catch (IOException e)
            {
                close();
                throw new RuntimeException("cursor read failed for " + sstable, e);
            }
        }

        @Override
        public boolean isDeferred()
        {
            return deferred;
        }

        @Override
        public boolean deferredSpansMergeStart(ClusteringBound<?> mergeStart, ClusteringComparator comparator)
        {
            if (!deferred || mergeStart.isBottom())
                return false;
            // deferredLowerBoundValue sorts just before the covered start; <= mergeStart means the
            // leg's data can begin at or before the slice start
            return comparator.compare(deferredLowerBoundValue, mergeStart) <= 0;
        }

        @Override
        public void openForMerge(ClusteringBound<?> sliceStart) throws IOException
        {
            if (!deferred)
                return;
            doDeferredOpen();
            if (deferredExhausted)
                return; // absent partition: cursorState() reports DONE, no rows, no count
            enterMergeMode();
            seekForMerge(sliceStart);
            // read the first unfiltered header so unfiltered() returns real data for the re-sort;
            // an empty in-slice partition sits at PARTITION_END/DONE and sorts to the tail
            if (isState(cursor.state(), ROW_START | TOMBSTONE_START))
                readUnfilteredHeader();
        }

        public DeletionTime partitionLevelDeletion()
        {
            if (deferred)
            {
                // parity with UnfilteredRowIteratorWithLowerBound.partitionLevelDeletion: the
                // metadata flag answers this without a counted read for the common no-deletion case
                if (!sstable.getSSTableMetadata().hasPartitionLevelDeletions)
                    return DeletionTime.LIVE;
                ensureOpenedForMetadata();
            }
            if (deferredExhausted || materializer == null)
                return DeletionTime.LIVE;
            return materializer.partitionDeletion();
        }

        public Row staticRow()
        {
            if (deferred)
            {
                // parity with UnfilteredRowIteratorWithLowerBound.staticRow: no counted read when
                // the query selects no static columns
                if (columnFilter.fetchedColumns().statics.isEmpty())
                    return Rows.EMPTY_STATIC_ROW;
                ensureOpenedForMetadata();
            }
            if (deferredExhausted || materializer == null)
                return Rows.EMPTY_STATIC_ROW;
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

        /**
         * Single-leg read: materializes the unfiltered whose header {@link #readUnfilteredHeader}
         * just loaded, as stored (the iterator path's deserialization, no merge), and moves the
         * cursor to the next unfiltered.  A row may come back empty; the sink drops it, like
         * {@code ForwardReader}.
         */
        Unfiltered readStoredUnfiltered() throws IOException
        {
            Unfiltered result;
            if (materializer.uDesc.clusteringKind() == ClusteringPrefix.Kind.CLUSTERING)
            {
                Row.Builder builder = materializer.rowBuilder;
                builder.newRow((Clustering<?>) materializer.toClusteringPrefix());
                materializer.materializeRowContents(cursor.state());
                result = builder.build();
            }
            else
            {
                result = materializer.materializeMarker();
            }
            if (cursor.state() == UNFILTERED_END)
                cursor.continueReading();
            return result;
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
         * The merge-mode row-index seek for the first slice start.  Positions this leg's cursor at
         * its floor block so the k-way merge never walks the partition prefix; rows between the
         * block start and the slice start are skipped by the merge without reading their cells.
         *
         * The range-tombstone deletion open at the seek point is the one piece of leg state a
         * mid-partition entry cannot recover from the stream, so it is kept as this leg's
         * {@link #openDeletion} for {@code CursorReadMerger} to seed into its cross-leg open-marker
         * set.  Misaligned per-leg seek points are safe: a leg's seed is valid from its floor block
         * onward, and any close marker for the seeded deletion arrives in this leg's own
         * post-seek stream.
         *
         * No-op for BIG legs, unindexed partitions, slices starting at BOTTOM, and legs already at
         * or past the floor block.  A merge may mix seeked BTI legs with unseeked BIG legs.
         */
        public void seekForMerge(ClusteringBound<?> sliceStart) throws IOException
        {
            BtiCursorSeekSupport.SeekPoint point = seekPointFor(sliceStart);
            if (point != null)
                seekTo(point);
        }

        /**
         * The single-leg row-index seek for the first slice start.
         *
         * @return the range-tombstone deletion open at the seek point (an immutable copy of the BTI
         *         row index's {@code IndexInfo.openDeletion}), or null when no seek was issued or no
         *         deletion is open there, like {@code ForwardIndexedReader.setForSlice}
         */
        DeletionTime seekForSingleRead() throws IOException
        {
            BtiCursorSeekSupport.SeekPoint point = seekPointFor(legSlices.get(0).start());
            if (point == null)
                return null;
            seekTo(point);
            return point.openMarker == null ? null : copyOf(point.openMarker);
        }

        /**
         * The row-index seek every read shape shares.  Gate: BTI, row-indexed partition, a slice
         * with a real start bound, a cursor not past the partition.  Returns the floor block for
         * the slice start when it begins past the current unfiltered; when that block is the one
         * already being read, the leg keeps reading sequentially, like
         * {@code ForwardIndexedReader.setForSlice}.
         */
        @Override
        public BtiCursorSeekSupport.SeekPoint seekPointFor(ClusteringBound<?> sliceStart)
        {
            if (btiEntry == null || !btiEntry.isIndexed() || cursor == null || sliceStart.isBottom())
                return null;
            int state = cursor.state();
            long current;
            if (isState(state, ROW_START | TOMBSTONE_START))
                current = cursor.position() - 1; // the flags byte is already consumed
            else if (isState(state, PARTITION_END | DONE))
                return null; // the partition has no unfiltereds left to seek over
            else
                current = loadedUnfilteredStart; // a header is loaded and its unfiltered not consumed
            if (rowIndex == null)
                rowIndex = BtiCursorSeekSupport.openRowIndex((BtiTableReader) sstable, btiEntry);
            BtiCursorSeekSupport.SeekPoint point = BtiCursorSeekSupport.floorBlock((BtiTableReader) sstable, rowIndex, btiEntry,
                                                                                  metadata.comparator, sliceStart);
            return point.dataPosition > current ? point : null;
        }

        @Override
        public void seekTo(BtiCursorSeekSupport.SeekPoint point) throws IOException
        {
            cursor.seekUnfiltered(point.dataPosition);
            SSTABLE_LEG_ROW_INDEX_SEEKS.incrementAndGet();
            hasOpenDeletion = point.openMarker != null && !TEST_DROP_MERGE_SEEK_OPEN_MARKER;
            if (!hasOpenDeletion)
                return;
            if (TEST_SKEW_MERGE_SEEK_OPEN_MARKER)
                openDeletion.reset(point.openMarker.markedForDeleteAt() + 1, point.openMarker.localDeletionTimeUnsignedInteger());
            else
                openDeletion.reset(point.openMarker);
        }

        /** The range deletion open in this leg's stream at its current position: the one its last
         *  row-index seek found, then kept current by {@link #noteMarker}; null when none. */
        @Override
        public DeletionTime openDeletion()
        {
            return hasOpenDeletion ? openDeletion : null;
        }

        @Override
        public void noteMarker()
        {
            UnfilteredDescriptor marker = materializer.uDesc;
            if (marker.isStartBound())
            {
                openDeletion.reset(marker.deletionTime());
                hasOpenDeletion = true;
            }
            else if (marker.isBoundary())
            {
                openDeletion.reset(marker.deletionTime2());
                hasOpenDeletion = true;
            }
            else
            {
                hasOpenDeletion = false;
            }
        }

        @Override
        public int skipRowsAtOrBefore(ClusteringDescriptor bound)
        {
            return cursor.skipRowsAtOrBefore(bound, materializer.uDesc);
        }

        @Override
        public void skipRow() throws IOException
        {
            int state = cursor.state();
            if (isState(state, CELL_HEADER_START | CELL_VALUE_START | CELL_END))
                cursor.skipRowCells(materializer.uDesc.dataStart(), materializer.uDesc.size(), false);
            else if (state != UNFILTERED_END)
                throw new IllegalStateException("no row to skip in cursor state " + state);
        }

        // ---- merge-source surface (consumed by CursorReadMerger) ----

        int openStateAfterHeader()
        {
            return openState;
        }

        public int cursorState()
        {
            // 0 (no state bit) while deferred: not DONE/PARTITION_END, so the sort treats the leg
            // as live and reads its lower bound through unfiltered(); prepareAndSort skips it by
            // isDeferred() before this value could reach a header-read.
            if (deferred)
                return 0;
            if (deferredExhausted)
                return DONE;
            return cursor.state();
        }

        public UnfilteredDescriptor unfiltered()
        {
            if (deferred || deferredExhausted)
                return deferredLowerBound;
            return materializer.uDesc;
        }

        /** Loads the row/marker header at the current unfiltered start into {@link #unfiltered()}
         *  and resets the per-row cell-walk state. */
        public void readUnfilteredHeader() throws IOException
        {
            loadedUnfilteredStart = cursor.position() - 1;
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
                    assert transfer.acquire() : "ValueTransfer single-live invariant violated (concurrent cursor cell copy)";
                    try
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
                            cursor.copyCellValue(transfer.valueCapture.prepareVariable(), null);
                            value = transfer.valueCapture.finish();
                        }
                    }
                    finally
                    {
                        assert transfer.release();
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
                {
                    cursor.skipCellValue(); // reconciles as EMPTY, like cellValue()
                }
                else
                {
                    assert transfer.acquire() : "ValueTransfer single-live invariant violated (concurrent cursor cell copy)";
                    try
                    {
                        cursor.copyCellValue(scratch, null);
                    }
                    finally
                    {
                        assert transfer.release();
                    }
                }
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
            if (rowIndex != null)
            {
                RowIndexReader toClose = rowIndex;
                rowIndex = null;
                toClose.close();
            }
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
     * materialized into {@code Row}/{@code Unfiltered} objects.  Holds one unfiltered: one merge
     * group yields at most one row or marker, and the reader takes it before the next group runs.
     * One reused {@code BTreeRow.sortedBuilder} serves all merged rows; rows that merge to empty
     * are dropped like {@code Row.Merger} returning null.
     */
    static final class MaterializingMergeSink implements CursorReadMerger.MergeSink
    {
        private final Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        private Unfiltered slot;
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
                put(row);
        }

        /** Discards the partially-built row of an abandoned group (a regular-column filter failure
         *  found mid-cell-walk or at row end).  build() resets the shared sorted builder for the
         *  next group. */
        @Override
        public void abandonRow()
        {
            rowBuilder.build();
        }

        @Override
        public void addRow(Row row)
        {
            // a whole row: a memtable row standing as the merged row, or a single leg's row as
            // stored.  An empty row is dropped, like ForwardReader and Row.Merger.
            if (!row.isEmpty())
                put(row);
        }

        @Override
        public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            put(marker);
        }

        private void put(Unfiltered unfiltered)
        {
            assert slot == null : "one merge group produced two unfiltereds";
            slot = unfiltered;
            materializedCount++;
        }

        /** The held unfiltered, or null; it stays held. */
        Unfiltered peek()
        {
            return slot;
        }

        /** Removes and returns the held unfiltered, or null when the last group produced none. */
        Unfiltered take()
        {
            Unfiltered unfiltered = slot;
            slot = null;
            return unfiltered;
        }

        long materializedCount()
        {
            return materializedCount;
        }
    }

    /**
     * Wire-level counterpart of {@code ReadCommand.withMetricsRecording}'s tombstone/live-row
     * accounting and tombstone-overwhelming abort, hooked into {@link ResponseSink}'s
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
        private final boolean enforceStrictLiveness;

        private int liveRows;
        private int tombstones;

        // per-row scratch, reset by startRow / wholeRow
        private long rowMinDeletionTime;
        private boolean rowLivenessLive;
        private boolean rowHasLiveCell;
        private boolean rowHasTombstoneCell;
        /** The row's clustering, or null when {@link #rowClusteringSource} builds it on an abort. */
        private ClusteringPrefix<?> rowClustering;
        private Supplier<ClusteringPrefix<?>> rowClusteringSource;

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
            this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
        }

        /** Builds the current row's clustering for a {@link #startRow} that was given none. */
        void rowClusteringSource(Supplier<ClusteringPrefix<?>> source)
        {
            this.rowClusteringSource = source;
        }

        /** @param clustering the row's clustering, or null to build it from {@link #rowClusteringSource}
         *                   if the row aborts the read */
        void startRow(ClusteringPrefix<?> clustering, LivenessInfo liveness, DeletionTime rowDeletion)
        {
            rowClustering = clustering;
            rowMinDeletionTime = Math.min(minDeletionTime(liveness), minDeletionTime(rowDeletion));
            rowLivenessLive = liveness.isLive(nowInSec);
            rowHasLiveCell = false;
            rowHasTombstoneCell = false;
        }

        /** Only called for a non-live complex deletion (see
         *  {@link ResponseSink#addComplexDeletion}'s early return); its
         *  {@code minDeletionTime} is always {@code Long.MIN_VALUE}. */
        void complexDeletion()
        {
            rowMinDeletionTime = Long.MIN_VALUE;
        }

        /** timestamp, ttl and localDeletionTime are the post-purge values
         *  {@link ResponseSink} is about to write, what a materialize-then-purge
         *  {@code Cell} would report, including the expired-but-not-gcable convert to tombstone. */
        void cell(long timestamp, int ttl, long localDeletionTime)
        {
            boolean isTombstone = localDeletionTime != Cell.NO_DELETION_TIME && ttl == Cell.NO_TTL;
            rowMinDeletionTime = Math.min(rowMinDeletionTime, isTombstone ? Long.MIN_VALUE : localDeletionTime);
            boolean isLive = localDeletionTime == Cell.NO_DELETION_TIME || (ttl != Cell.NO_TTL && nowInSec < localDeletionTime);
            if (isLive)
            {
                rowHasLiveCell = true;
            }
            else
            {
                rowHasTombstoneCell = true;
                countTombstone(rowClustering);
            }
        }

        void endRow()
        {
            boolean rowHasDeletion = nowInSec >= rowMinDeletionTime;
            // Row.hasLiveData: live primary key liveness, or any live cell unless liveness is strict
            if (rowLivenessLive || (rowHasLiveCell && !enforceStrictLiveness))
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
            if (row.hasLiveData(nowInSec, enforceStrictLiveness))
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
                throw new TombstoneOverwhelmingException(tombstones, query, command.metadata(), key,
                                                         clustering != null ? clustering : rowClusteringSource.get());
            }
        }

        /** Must be called once, when the read ends, failed or not, like {@code MetricRecording}'s
         *  close.  Records latency, histograms, counters, and warn logging, like
         *  {@code MetricRecording.onPartitionClose()} and {@code onClose()}. */
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
    private static final class CellValueCapture implements DataOutputPlus, SSTableCursorReader.DirectValueTarget
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
        public void readValue(DataInputPlus in, int length) throws IOException
        {
            if (target == null || written + length > target.length)
                throw new IllegalStateException("cell value of " + length + " bytes does not fit the capture");
            in.readFully(target, written, length);
            written += length;
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
     * A LAZY forward iterator over one cursor read: a single leg read as stored, or a cursor-level
     * merge.  Each {@link #hasNext()} pulls merge groups ({@code CursorReadMerger.advance}) until
     * the one-slot sink holds an unfiltered, so the read touches only as much of the partition as
     * its reader consumes.  The query's slices are applied with the same semantics as
     * {@code AbstractSSTableIterator.ForwardReader}: pre-slice skip with open-marker tracking,
     * artificial {@link RangeTombstoneBoundMarker}s at slice start/end when a range tombstone
     * covers the bound, and non-strict start / strict end comparisons.  The emitted stream is
     * identical to the iterator path's.
     *
     * <p>Owns its legs: they close in {@link #close()}, like {@link ReverseSlicedCursorIterator}.
     */
    private static final class ForwardSlicedCursorIterator implements UnfilteredRowIterator
    {
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final SSTableReader sstable;
        private final EncodingStats stats;
        private final ColumnFilter columnFilter;
        private final Slices slices;
        private final DeletionTime partitionDeletion;
        private final Row staticRow;
        private final ClusteringComparator comparator;
        private final boolean validateOnEmission;
        /** The prepared merge core, or null when no leg contributes rows. */
        private final CursorReadMerger merger;
        private final MaterializingMergeSink sink;
        /** The row-level filter probe of a merged read, or null. */
        private final RowLevelFilterProbe filterProbe;
        private final List<? extends MergeLeg> legs;
        /** A cursor-level merge rather than a single leg; only merges count toward the merge counters. */
        private final boolean merged;

        // merge pull state
        private Unfiltered pending;     // pulled from the merge, not yet consumed by the slicer
        private boolean mergeDone;      // the merge has no unfiltereds left in the current slice
        private boolean probeFinished;  // filterProbe.finishPartition() has run
        private boolean exhausted;      // hasNext() returned false: the read reached its end
        private boolean failed;         // reading threw; the merge counters are not advanced
        private boolean closed;

        // slice state
        private int sliceIdx;            // next slice to open
        private ClusteringBound<?> start; // null once past the current slice's start, or when it starts at BOTTOM
        private ClusteringBound<?> end;
        private boolean sliceOpen;
        private DeletionTime openMarker;
        private Unfiltered next;
        /** The markers between the last slice and the next, when it starts where the last ended. */
        private final ArrayDeque<RangeTombstoneMarker> adjacentSliceMarkers = new ArrayDeque<>(2);
        /** The merge has already moved to the next slice, past what lies at or before its start. */
        private boolean movedToAdjacentSlice;

        /**
         * @param sstable for a single leg, the leg's sstable; for a merged partition, the first
         *                leg's, used to attribute emission-time corrupted-tombstone validation
         * @param stats   the stats this iterator reports: the leg sstable's own for a single leg,
         *                {@code EncodingStats.merge} over the legs for a merged partition
         * @param openMarkerAtStart the range-tombstone deletion open at the first unfiltered the
         *                merge reads, from the BTI row index when the read seeked mid-partition;
         *                null to track open markers from the stream alone
         * @param validateOnEmission true for a single leg, where this iterator is the leg's emission
         *                surface and owns the per-leg in-slice validation; false for a merged
         *                partition, whose legs were each already validated inside the merge.
         *                Validating a merged partition here would duplicate the work and reject
         *                reads the iterator path serves.
         */
        ForwardSlicedCursorIterator(TableMetadata metadata,
                                    DecoratedKey key,
                                    SSTableReader sstable,
                                    EncodingStats stats,
                                    ColumnFilter columnFilter,
                                    Slices slices,
                                    DeletionTime partitionDeletion,
                                    Row staticRow,
                                    DeletionTime openMarkerAtStart,
                                    CursorReadMerger merger,
                                    MaterializingMergeSink sink,
                                    RowLevelFilterProbe filterProbe,
                                    List<? extends MergeLeg> legs,
                                    boolean validateOnEmission,
                                    boolean merged)
        {
            this.metadata = metadata;
            this.key = key;
            this.sstable = sstable;
            this.stats = stats;
            this.columnFilter = columnFilter;
            this.slices = slices;
            this.partitionDeletion = partitionDeletion;
            this.staticRow = staticRow;
            this.comparator = metadata.comparator;
            this.merger = merger;
            this.sink = sink;
            this.filterProbe = filterProbe;
            this.legs = legs;
            this.validateOnEmission = validateOnEmission;
            this.merged = merged;
            this.openMarker = openMarkerAtStart;
            this.mergeDone = merger == null;
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
            return partitionDeletion;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public EncodingStats stats()
        {
            return stats;
        }

        @Override
        public boolean hasNext()
        {
            try
            {
                while (next == null)
                {
                    if (!adjacentSliceMarkers.isEmpty())
                    {
                        next = adjacentSliceMarkers.poll();
                        break;
                    }
                    if (!sliceOpen)
                    {
                        if (sliceIdx >= slices.size())
                        {
                            exhausted = true;
                            finishProbe();
                            return false;
                        }
                        setForSlice(sliceIdx++);
                    }
                    // returns null only after setting sliceOpen = false, so a null just loops back
                    // to open the next slice (or hit the sliceIdx exhaustion check above)
                    next = computeNextInSlice();
                }
                return true;
            }
            catch (IOException e)
            {
                failed = true;
                throw new RuntimeException("cursor read failed for " + key + " over " + legs.size() + " legs", e);
            }
            catch (RuntimeException | Error e)
            {
                failed = true;
                throw e;
            }
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

        private void setForSlice(int index) throws IOException
        {
            Slice slice = slices.get(index);
            start = slice.start().isBottom() ? null : slice.start();
            end = slice.end();
            sliceOpen = true;
            if (movedToAdjacentSlice)
            {
                // the merge already read past this slice's start and gave the markers there
                movedToAdjacentSlice = false;
                mergeDone = false;
                start = null;
                openMarker = merger.openDeletionAtSeek();
                return;
            }
            // The merge stops before the first group at or past a slice end, so nothing is
            // pending here.  It reads the next slice from here, seeking forward where the row
            // index allows; a seek replaces the open range deletion, as on the iterator path.
            if (index > 0 && merger != null)
            {
                assert pending == null : "the merge read past the end of slice " + (index - 1);
                mergeDone = false;
                if (merger.moveToSlice(index))
                    openMarker = merger.openDeletionAtSeek();
            }
        }

        // mirrors ForwardReader's in-slice iteration; only called with sliceOpen == true, and
        // clears sliceOpen when the slice is exhausted
        private Unfiltered computeNextInSlice() throws IOException
        {
            if (start != null)
            {
                // Skip pre-slice data with a NON-strict comparison (see handlePreSliceData's
                // comment on RT start markers equal to the slice start), tracking the open marker.
                Unfiltered skipped;
                while ((skipped = peekMerged()) != null && comparator.compare(skipped.clustering(), start) <= 0)
                {
                    takeMerged();
                    if (skipped.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                        updateOpenMarker((RangeTombstoneMarker) skipped);
                }
                ClusteringBound<?> sliceStart = start;
                start = null;
                if (openMarker != null)
                    return new RangeTombstoneBoundMarker(sliceStart, openMarker);
            }

            // in-slice: strict end comparison
            Unfiltered unfiltered = peekMerged();
            if (unfiltered != null && comparator.compare(unfiltered.clustering(), end) < 0)
            {
                takeMerged();
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
            if (merger != null && pending == null && merger.isAdjacentSliceInMerge(sliceIdx))
            {
                // the next slice starts here: the merge gives the markers the iterator path writes
                adjacentSliceMarkers.addAll(merger.moveToAdjacentSlice(sliceIdx));
                movedToAdjacentSlice = true;
                return adjacentSliceMarkers.poll();
            }
            if (openMarker != null)
                return new RangeTombstoneBoundMarker(end, openMarker);
            return null;
        }

        /** The next merged unfiltered, pulling merge groups until one yields it; null at the end. */
        private Unfiltered peekMerged() throws IOException
        {
            while (pending == null && !mergeDone)
            {
                if (merger.advance())
                {
                    pending = sink.take();
                }
                else
                {
                    mergeDone = true;
                    if (sliceIdx >= slices.size())
                        finishProbe();
                }
            }
            return pending;
        }

        private void takeMerged()
        {
            pending = null;
        }

        /** The merge counters, advanced once a merged read closes without failing. */
        private void countServedMerge()
        {
            CURSOR_MERGES_SERVED.incrementAndGet();
            int sstableLegCount = 0;
            for (MergeLeg leg : legs)
            {
                if (leg.sstableOrNull() != null)
                    sstableLegCount++;
            }
            SSTABLE_LEGS_CURSOR_MERGED.addAndGet(sstableLegCount);
            MEMTABLE_LEGS_CURSOR_MERGED.addAndGet(legs.size() - sstableLegCount);
            if (merger != null && !exhausted)
                MERGES_STOPPED_BY_LIMIT.incrementAndGet();
        }

        /** The probe's end-of-slice accounting, once, when the read reaches its end; never on an
         *  early close. */
        private void finishProbe()
        {
            if (probeFinished || filterProbe == null)
                return;
            probeFinished = true;
            filterProbe.finishPartition();
        }

        private void updateOpenMarker(RangeTombstoneMarker marker)
        {
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        @Override
        public void close()
        {
            if (closed)
                return;
            closed = true;
            UNFILTEREDS_MATERIALIZED.addAndGet(sink.materializedCount());
            if (merged && !failed)
                countServedMerge();
            RuntimeException failure = null;
            for (MergeLeg leg : legs)
            {
                try
                {
                    leg.close();
                }
                catch (RuntimeException e)
                {
                    if (failure == null)
                        failure = e;
                    else
                        failure.addSuppressed(e);
                }
            }
            if (failure != null)
                throw failure;
        }
    }

    /**
     * A LAZY reverse ({@code ORDER BY ... DESC}) iterator over one sstable leg, the cursor analog of
     * BTI {@code SSTableReversedIterator}.  It walks a block from the slice end backward: per block
     * it forward-collects the in-slice unfiltereds' file positions onto a stack ({@code fillOffsets}),
     * then pops them to re-read in reverse, synthesizing the per-block open/close range-tombstone
     * bound markers at the block edges.  For a BTI row-indexed partition a
     * {@link BtiCursorSeekSupport.ReverseBlockCursor} drives the blocks from the slice end backward,
     * so a reverse read with a small limit touches only the tail blocks.  For an unindexed partition
     * (a small partition, or a BIG-format sstable) the whole partition is one block, collected on the
     * first pull.
     *
     * <p>Descending cross-leg reconciliation is NOT done here: each surviving leg is one of these
     * iterators, and the legs merge in the shared {@code UnfilteredRowIterators.merge}
     * ({@link #isReverseOrder()} is true), exactly as the iterator path composes a reverse read.
     */
    private static final class ReverseSlicedCursorIterator implements UnfilteredRowIterator
    {
        private final PendingLeg leg;
        private final PartitionMaterializer materializer;
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final SSTableReader sstable;
        private final EncodingStats stats;
        private final ColumnFilter columnFilter;
        private final ClusteringComparator comparator;
        private final Slices slices;
        private final DeletionTime partitionDeletion;
        private final Row staticRow;
        /** BTI row-indexed partition: drive the blocks with the reverse block cursor. */
        private final boolean indexed;
        /** For an unindexed/BIG partition, the first-unfiltered start to re-read from per slice;
         *  -1 when there is nothing to read (a Slices.NONE leg, or an empty partition). */
        private final long unindexedStartPos;

        // slice iteration: slices are processed from the END (descending), like
        // SSTableReversedIterator.nextSliceIndex
        private int slicesOpened;
        private boolean sliceOpen;
        private Slice currentSlice;

        // block-collect state (mirrors SSTableReversedIterator.ReverseReader)
        private final LongStack rowOffsets = new LongStack();
        private RangeTombstoneMarker blockOpenMarker;
        private RangeTombstoneMarker blockCloseMarker;
        private DeletionTime openMarker; // the range tombstone open at the forward collect cursor
        private boolean foundLessThan;

        // block traversal (indexed only)
        private BtiCursorSeekSupport.ReverseBlockCursor blockCursor;
        private long currentBlockStart;
        private final CursorReadMerger.SliceBoundDescriptor startBound;
        private final CursorReadMerger.SliceBoundDescriptor endBound;

        // streaming (see streamTo): the sink, and the merge core that streams each row to it
        private ResponseSink streamSink;
        private CursorReadMerger streamMerger;

        private Unfiltered next;
        private boolean closed;

        ReverseSlicedCursorIterator(PendingLeg leg)
        {
            this.leg = leg;
            this.materializer = leg.materializer;
            this.metadata = leg.metadata;
            this.key = leg.key;
            this.sstable = leg.sstable;
            this.stats = leg.sstable.stats();
            this.columnFilter = leg.columnFilter;
            this.comparator = leg.metadata.comparator;
            this.slices = leg.legSlices;
            this.partitionDeletion = leg.partitionLevelDeletion();
            this.staticRow = leg.staticRow();
            boolean idx = leg.cursor != null && leg.btiEntry != null && leg.btiEntry.isIndexed();
            this.indexed = idx;
            // The unindexed reverse walk re-reads from the partition's first unfiltered per slice;
            // capture that start now, while the cursor sits there after openPartition.  A Slices.NONE
            // leg (cursor already closed) or an empty partition (openState PARTITION_END/DONE) has
            // nothing to read.
            this.unindexedStartPos = (!idx && leg.cursor != null && isState(leg.openState, ROW_START | TOMBSTONE_START))
                                     ? materializer.unfilteredStart()
                                     : -1L;
            AbstractType<?>[] clusteringTypes = leg.unfiltered().clusteringTypes();
            this.startBound = new CursorReadMerger.SliceBoundDescriptor(clusteringTypes);
            this.endBound = new CursorReadMerger.SliceBoundDescriptor(clusteringTypes);
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return true;
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
            return partitionDeletion;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public EncodingStats stats()
        {
            return stats;
        }

        @Override
        public boolean hasNext()
        {
            try
            {
                while (next == null)
                {
                    if (streamSink != null && !streamSink.wantsMore())
                        return false;
                    if (!sliceOpen)
                    {
                        if (slicesOpened >= slices.size())
                            return false;
                        // process slices from the end (descending clustering order)
                        setForSlice(slices.get(slices.size() - (slicesOpened + 1)));
                        slicesOpened++;
                        sliceOpen = true;
                    }
                    next = computeNext();
                    if (next == null)
                        sliceOpen = false; // slice exhausted; loop to open the next slice
                }
                return true;
            }
            catch (IOException e)
            {
                throw new RuntimeException("cursor reverse read failed for " + sstable, e);
            }
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

        private void setForSlice(Slice slice) throws IOException
        {
            currentSlice = slice;
            openMarker = null;
            blockOpenMarker = null;
            blockCloseMarker = null;
            rowOffsets.clear();
            foundLessThan = false;
            if (indexed)
            {
                if (blockCursor != null)
                    blockCursor.close();
                blockCursor = BtiCursorSeekSupport.reverseBlockCursor((BtiTableReader) sstable, leg.btiEntry,
                                                                      comparator, slice.end());
                gotoBlock(blockCursor.nextBlock(), true, Long.MAX_VALUE);
            }
            else if (unindexedStartPos >= 0)
            {
                int state = materializer.seekUnfiltered(unindexedStartPos);
                fillOffsets(slice, true, true, Long.MAX_VALUE, state);
            }
            // else: empty partition / Slices.NONE -- no offsets, computeNext returns null at once
        }

        /** Mirrors {@code ReverseIndexedReader.gotoBlock}: seeds the block's open marker from the row
         *  index, seeks to the block start, and collects the in-slice offsets of that block. */
        private boolean gotoBlock(BtiCursorSeekSupport.SeekPoint block, boolean filterEnd, long blockEnd) throws IOException
        {
            blockOpenMarker = null;
            blockCloseMarker = null;
            rowOffsets.clear();
            if (block == null)
                return false;
            currentBlockStart = block.dataPosition;
            openMarker = block.openMarker;
            int state = materializer.seekUnfiltered(currentBlockStart);
            fillOffsets(currentSlice, true, filterEnd, blockEnd, state);
            return !rowOffsets.isEmpty();
        }

        private boolean advanceIndexBlock() throws IOException
        {
            if (!indexed)
                return false;
            return gotoBlock(blockCursor.nextBlock(), false, currentBlockStart);
        }

        /**
         * Cursor port of {@code SSTableReversedIterator.fillOffsets}.  Reads the block forward once,
         * classifying each unfiltered by its clustering.  Because the cursor must CONSUME an
         * unfiltered's header to learn its clustering (the reference deserializer peeks), the
         * block-open bound marker is fixed at the pre-slice/in-slice boundary, capturing the open
         * marker BEFORE any in-slice marker updates it -- byte-identical to the reference, which sets
         * it at the same point.  {@code stopPosition} bounds the block (a block ends where the next
         * begins); the boundary is compared against each unfiltered's on-disk start, matching the
         * reference {@code currentPosition < stopPosition}.
         */
        private void fillOffsets(Slice slice, boolean filterStart, boolean filterEnd, long stopPosition, int state) throws IOException
        {
            filterStart &= !slice.start().isBottom();
            filterEnd &= !slice.end().isTop();
            ClusteringBound<?> start = slice.start();
            ClusteringBound<?> end = slice.end();
            // the bounds in the leg's wire form, so each unfiltered compares without a clustering object
            if (filterStart)
                startBound.load(start);
            if (filterEnd)
                endBound.load(end);
            foundLessThan = false;

            boolean beforeStart = filterStart;
            boolean openMarkerPlaced = false;
            if (!beforeStart)
            {
                // start is BOTTOM (or not filtered): the in-slice region begins immediately, so the
                // block-open marker is fixed now, from the block's seeded open marker
                if (openMarker != null)
                    blockOpenMarker = new RangeTombstoneBoundMarker(start, openMarker);
                openMarkerPlaced = true;
            }

            while (isState(state, ROW_START | TOMBSTONE_START))
            {
                long pos = materializer.unfilteredStart();
                if (pos >= stopPosition)
                    break;
                state = materializer.collectStep(state);
                ClusteringDescriptor clustering = materializer.collectedClustering();
                RangeTombstoneMarker marker = materializer.collectedMarker();

                if (beforeStart)
                {
                    // pre-slice: non-strict, so an RT bound equal to the slice start is skipped here
                    // and re-synthesized as the artificial open bound (see handlePreSliceData)
                    if (ClusteringComparator.compare(clustering, startBound) <= 0)
                    {
                        if (marker != null)
                            updateOpenMarker(marker);
                        foundLessThan = true;
                        continue;
                    }
                    // reached the in-slice region: fix the block-open marker from the open marker as
                    // it stands after pre-slice skipping, before any in-slice update
                    beforeStart = false;
                    if (openMarker != null)
                        blockOpenMarker = new RangeTombstoneBoundMarker(start, openMarker);
                    openMarkerPlaced = true;
                }

                // in-slice end cutoff: strict, like ForwardReader.computeNext
                if (filterEnd && ClusteringComparator.compare(clustering, endBound) >= 0)
                    break;

                rowOffsets.push(pos);
                if (marker != null)
                    updateOpenMarker(marker);
            }

            // the block was exhausted while still pre-slice: fix the block-open marker now
            if (!openMarkerPlaced && openMarker != null)
                blockOpenMarker = new RangeTombstoneBoundMarker(start, openMarker);

            // an open marker at the (filtered) slice end closes the deletion at the slice end
            if (openMarker != null && filterEnd)
            {
                blockCloseMarker = new RangeTombstoneBoundMarker(end, openMarker);
                openMarker = null;
            }
        }

        /** Mirrors {@code SSTableReversedIterator.ReverseReader.computeNext}. */
        private Unfiltered computeNext() throws IOException
        {
            Unfiltered toReturn;
            do
            {
                if (blockCloseMarker != null)
                {
                    toReturn = blockCloseMarker;
                    blockCloseMarker = null;
                    return toReturn;
                }
                while (!rowOffsets.isEmpty())
                {
                    if (streamMerger != null)
                    {
                        if (!streamSink.wantsMore())
                            return null;
                        // the row or marker goes straight to the sink, read as stored
                        materializer.seekUnfiltered(rowOffsets.pop());
                        streamMerger.advance();
                        continue;
                    }
                    Unfiltered unfiltered = materializer.popUnfilteredAt(rowOffsets.pop());
                    if (unfiltered != null)
                        return unfiltered;
                }
            }
            while (!foundLessThan && advanceIndexBlock());

            // open marker output only once the slice is finished
            if (blockOpenMarker != null)
            {
                toReturn = blockOpenMarker;
                blockOpenMarker = null;
                return toReturn;
            }
            return null;
        }

        /**
         * Writes the read into {@code sink}, which must be past its partition header.  Each row and
         * stored marker streams from the leg to the sink through a single-leg merge core, without a
         * row object; the block-edge markers go to the sink as objects.  Stops when the sink wants
         * no more.  Does not close this iterator.
         */
        void streamTo(ResponseSink sink) throws IOException
        {
            streamSink = sink;
            if (leg.cursor != null && isState(leg.openState, ROW_START | TOMBSTONE_START))
            {
                streamMerger = CursorReadMerger.forSingleLeg(leg, sink, Slices.ALL);
                // prepare reads the clustering types off the leg's descriptor, which the open loaded
                streamMerger.prepare();
            }
            while (hasNext())
                sink.addRangeTombstoneMarker((RangeTombstoneMarker) next());
        }

        // AbstractSSTableIterator.updateOpenMarker: the forward-collect walk uses the non-reversed
        // open side
        private void updateOpenMarker(RangeTombstoneMarker marker)
        {
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        /** Closes the row index walk; the leg stays open. */
        void closeBlockCursor()
        {
            if (blockCursor != null)
            {
                blockCursor.close();
                blockCursor = null;
            }
        }

        @Override
        public void close()
        {
            if (closed)
                return;
            closed = true;
            try
            {
                closeBlockCursor();
            }
            finally
            {
                leg.close();
            }
        }
    }
}
