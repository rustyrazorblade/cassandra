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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongPredicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellLivenessInfo;
import org.apache.cassandra.db.rows.CellLivenessInfo.Resolution;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.CursorMergeSink;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.SSTableCursorWriter;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.ClusteringPrefix.Kind.EXCL_END_BOUND;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.EXCL_START_BOUND;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.INCL_END_BOUND;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.INCL_START_BOUND;
import static org.apache.cassandra.db.rows.CellLivenessInfo.Resolution.COMPARE;
import static org.apache.cassandra.db.rows.CellLivenessInfo.Resolution.LEFT;
import static org.apache.cassandra.db.rows.CellLivenessInfo.Resolution.RIGHT;
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

/**
 * Compacts the contents of 1..n sstables into a 1..m sstables. The compaction is driven one output partition at a time
 * by the {@link CursorCompactionPipeline}.
 * <p>
 * Compaction here implies:
 * <ul>
 *   <li>Merge source sstable data, such that only latest live values, or tombstones, are present in the output.</li>
 *   <li>Purge gc-able tombstones if possible (see PurgeFunction below).</li>
 *   <li>Invalidate cached partitions that are empty post-compaction. This avoids keeping partitions with
 *       only purgable tombstones in the row cache.</li>
 *   <li>Keeps track of the compaction progress.</li>
 * </ul>
 * This compaction implementation supports BIG and BTI output formats (see the
 * CursorIndexWriter seam) and complex (collection/UDT) columns; it does not support 2ndary
 * indexes, counter columns, or a multi-cell column that the schema has dropped, and it stands
 * aside for a compaction that ignores gc grace for a key. See {@link #isSupported} and
 * {@link #unsupportedMetadata} for the full set of gates.
 * <p>
 *     This compaction implementation avoids garbage creation per partition/row/cell by utilizing reader/writer code
 *     which supports reusable copies of sstable entry components. The implementation consolidates and duplicates code
 *     from various classes to support the use of these reusable structures.
 * </p>
 */
public class CursorCompactor extends CompactionInfo.Holder
{
    public static boolean isSupported(AbstractCompactionStrategy.ScannerList scanners, AbstractCompactionController controller)
    {
        TableMetadata metadata = controller.cfs.metadata();
        if (unsupportedMetadata(metadata)) return false;

        for (ISSTableScanner scanner : scanners.scanners)
        {
            // TODO: implement partial range reader
            if (!scanner.isFullRange())
            {
                if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Partial scanners are not supported.");
                return false;
            }

            for (SSTableReader reader : scanner.getBackingSSTables()) {
                Version version = reader.descriptor.version;
                if (!version.isLatestVersion())
                {
                    if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Older sstable versions are not supported. version=" + version);
                    return false;
                }
                if (unsupportedHeaderColumns(metadata, reader))
                    return false;
            }
        }
        if (!(DatabaseDescriptor.getSelectedSSTableFormat() instanceof BigFormat
              || DatabaseDescriptor.getSelectedSSTableFormat() instanceof org.apache.cassandra.io.sstable.format.bti.BtiFormat))
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Only the BIG and BTI sstable output formats are supported. format=" + DatabaseDescriptor.getSelectedSSTableFormat());
            return false;
        }
        // TODO: Implement CompactionIterator.GarbageSkipper like functionality
        if (controller.tombstoneOption != CompactionParams.TombstoneOption.NONE)
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Garbage skipping not implemented. controller.tombstoneOption=" + controller.tombstoneOption);
            return false;
        }
        // Only ColumnFamilyStore.forceCompactionKeysIgnoringGcGrace puts a key in this set, and its
        // shipped caller is nodetool forcecompact. It is therefore the only way a purge is decided
        // with localDeletionTime >= gcBefore (see Purger.shouldPurge), and that is the case the
        // cursor cannot reproduce. BTreeRow.purge returns the row untouched whenever nowInSec is
        // below the row's minimum local deletion time. The iterator therefore settles row-level
        // purging all-or-nothing over the whole row, before it touches a cell. A streaming cursor
        // instead commits to the row's deletion and liveness before it walks the row's cells. Under
        // an ordinary gcBefore that short-circuit costs nothing, because such a row holds nothing
        // purgeable either way.
        // The set is per-table and lives for the whole force compaction, so a background compaction
        // that starts inside that window falls back as well. The gate is coarse, but it falls back
        // to the reference implementation, so it costs throughput and not correctness.
        if (controller.cfs.shouldIgnoreGcGraceForAnyKey())
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Ignoring gc_grace_seconds for a key is not supported (nodetool forcecompact).");
            return false;
        }
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Cursor compaction for table: " + metadata.name + " keyspace: " + metadata.keyspace + " is supported.");

        return true;
    }

    /**
     * Support gate for the read-only, partial-range validation path (see {@link #mergeNextPartition}),
     * as opposed to {@link #isSupported} which gates the writing compaction path. Differs from
     * {@code isSupported} in three ways, all because validation never writes output and always
     * reads partial ranges: partial-range scanners are the expected case here, not rejected;
     * the BIG/BTI output-format gate doesn't apply (nothing is written); and the
     * {@code tombstoneOption} gate - aimed at {@code CompactionIterator.GarbageSkipper}-equivalent
     * behavior - is replaced by requiring {@link AbstractCompactionController#guaranteesNoShadowSources}
     * on the controller, which {@code ValidationCompactionController} overrides to {@code true}
     * (it's always constructed with {@code compacting = null} and therefore always yields
     * null/empty {@link AbstractCompactionController#shadowSources} regardless of
     * {@code tombstoneOption} - see {@code ValidationCompactionControllerTest}, which pins that
     * invariant). Checking the capability rather than the controller's concrete type keeps this
     * class from depending on {@code db.repair}.
     */
    public static boolean isValidationSupported(Collection<SSTableReader> sstables, AbstractCompactionController controller)
    {
        TableMetadata metadata = controller.cfs.metadata();
        if (unsupportedMetadata(metadata)) return false;

        // Materialized-view sstables can carry shadowable row deletions, which SSTableCursorReader
        // rejects mid-read (see its static-row UnsupportedOperationException / non-static
        // corruptSSTable paths). Modern view maintenance hasn't produced these since CASSANDRA-13409
        // (Row.Deletion.shadowable(...) has no remaining caller in this codebase), so regular cursor
        // compaction admits views - see MaterializedViewDifferentialCompactionTest - but validation
        // stays conservative here: unlike compaction, which can retry via ordinary background
        // compaction, a repair session that hits this mid-merge fails the whole repair with a hard
        // error instead of falling back pre-construction the way the two-stage support check is
        // designed to, and repair sessions can run against sstables written by any Cassandra version
        // still in the supported upgrade path.
        if (metadata.isView())
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Materialized views are not supported for cursor validation.");
            return false;
        }

        for (SSTableReader reader : sstables)
        {
            Version version = reader.descriptor.version;
            if (!version.isLatestVersion())
            {
                if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Older sstable versions are not supported. version=" + version);
                return false;
            }
            if (unsupportedHeaderColumns(metadata, reader))
                return false;
        }

        if (!controller.guaranteesNoShadowSources())
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Cursor validation requires a controller that guarantees no shadow sources.");
            return false;
        }

        if (LOGGER.isDebugEnabled()) LOGGER.debug("Cursor validation compaction for table: " + metadata.name + " keyspace: " + metadata.keyspace + " is supported.");
        return true;
    }

    /**
     * Support gate for cursor-backed cleanup (see {@code CompactionManager#doCleanupOne}), which -
     * unlike {@link #isSupported}'s regular-compaction path - reads only the partial ranges this
     * node still owns, but - unlike {@link #isValidationSupported}'s path - does write real output
     * sstables. So this is {@code isSupported} minus the full-range-scanner requirement, keeping
     * the output-format and {@code tombstoneOption} gates, plus one cleanup-specific rejection:
     * a table with live secondary indexes selects {@code CompactionManager.CleanupStrategy.Full},
     * which drops out-of-range partitions itself and notifies {@code cfs.indexManager} of every
     * removed partition so 2i stays in sync with the base data. The cursor merge loop has no
     * equivalent notification hook, so such a table must stay on the legacy path - silently
     * skipping those notifications would leave index entries pointing at rows cleanup deleted.
     * {@code unsupportedMetadata} already rejects {@code metadata.indexes}; the
     * {@code indexManager} check below is the same condition {@code CleanupStrategy.get} itself
     * branches on, checked directly so the two can never disagree.
     * <p>
     * Materialized views are deliberately ADMITTED here, siding with {@link #isSupported} rather
     * than {@link #isValidationSupported}'s conservative {@code isView()} rejection. The underlying
     * risk is the same for all three - a legacy view sstable carrying shadowable row deletions,
     * which {@code SSTableCursorReader} rejects mid-read - but the consequence of hitting it is
     * not. Validation rejects because a mid-merge failure fails an entire repair session, which no
     * amount of retrying repairs. Cleanup, like compaction, fails only the one sstable it was
     * rewriting: the {@code SSTableRewriter} aborts, the transaction rolls back, the original is
     * left untouched and {@code nodetool cleanup} reports that sstable as failed. Nothing is lost
     * and the operator can rerun. Rejecting views here while regular cursor compaction admits them
     * would also be incoherent, since a view with shadowable deletions would already fail its next
     * background compaction. See {@code CursorCleanupSupportPostureTest}, which pins the three
     * gates' differing view postures so this stays a decision rather than an oversight.
     */
    public static boolean isCleanupSupported(Collection<SSTableReader> sstables, AbstractCompactionController controller)
    {
        TableMetadata metadata = controller.cfs.metadata();
        if (unsupportedMetadata(metadata)) return false;

        if (controller.cfs.indexManager.hasIndexes())
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Cursor cleanup cannot notify secondary indexes of removed partitions.");
            return false;
        }

        for (SSTableReader reader : sstables)
        {
            Version version = reader.descriptor.version;
            if (!version.isLatestVersion())
            {
                if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Older sstable versions are not supported. version=" + version);
                return false;
            }
        }

        if (!(DatabaseDescriptor.getSelectedSSTableFormat() instanceof BigFormat
              || DatabaseDescriptor.getSelectedSSTableFormat() instanceof org.apache.cassandra.io.sstable.format.bti.BtiFormat))
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Only the BIG and BTI sstable output formats are supported. format=" + DatabaseDescriptor.getSelectedSSTableFormat());
            return false;
        }

        // TODO: Implement CompactionIterator.GarbageSkipper like functionality
        if (controller.tombstoneOption != CompactionParams.TombstoneOption.NONE)
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Garbage skipping not implemented. controller.tombstoneOption=" + controller.tombstoneOption);
            return false;
        }

        if (LOGGER.isDebugEnabled()) LOGGER.debug("Cursor cleanup compaction for table: " + metadata.name + " keyspace: " + metadata.keyspace + " is supported.");
        return true;
    }

    public static boolean unsupportedMetadata(TableMetadata metadata)
    {
        if (metadata.keyspace.equals(SchemaConstants.ACCORD_KEYSPACE_NAME))
            return true;

        if (!metadata.partitioner.supportsReusableKeys())
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Incompatible partitioner, does not support reusable keys:" + metadata.partitioner.getClass().getSimpleName());
            return true;
        }

        if (metadata.indexes.size() != 0)
        {
            if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "Additional indexes are not supported. metadata.indexes=" + metadata.indexes);
            return true;
        }

        if (unsupportedSchema(metadata))
            return true;
        return false;
    }

    private static boolean unsupportedSchema(TableMetadata metadata)
    {
        // No remaining schema-shape gates: complex columns landed in increment 2, counters
        // in increment 5 (mergeCounterCells / CursorCounterContexts).
        return false;
    }

    /**
     * Rejects a dropped column that an sstable header still lists.
     *
     * <h2>What this gate decides</h2>
     *
     * {@link #unsupportedSchema} sees only the columns the table has now. A drop removes the column
     * from {@link TableMetadata#regularAndStaticColumns()}. Each sstable written before the drop
     * still lists that column in its own serialization header, with the original type. The reader
     * builds its column arrays from the header, not from the schema, so a dropped column still
     * reaches the cell cursor. This gate is the only screen that sees it.
     *
     * The output has a slot for such a column either way. {@code SerializationHeader.make} builds
     * the output header from the headers of the input sstables, not from the current schema.
     *
     * <h2>Support matrix</h2>
     *
     * <table>
     * <caption>Which path runs, per dropped column shape</caption>
     * <tr><th>Shape</th><th>Path</th><th>State</th></tr>
     *
     * <tr><td>Dropped SIMPLE column</td><td>cursor</td>
     * <td>SUPPORTED. The gate never fires. Covered by
     * {@code DroppedColumnDifferentialCompactionTest}.</td></tr>
     *
     * <tr><td>Dropped MULTI-CELL column, re-added to the schema</td><td>cursor</td>
     * <td>SUPPORTED. The gate tests {@code metadata.getColumn(name) == null}, and a re-added column
     * is back in the schema, so it does not fire. Covered by
     * {@code droppedThenReaddedComplexColumnDeletionNotResurrected}.</td></tr>
     *
     * <tr><td>Dropped MULTI-CELL column, still out of the schema</td><td>iterator</td>
     * <td>NOT SUPPORTED here, and BROKEN there. See below.</td></tr>
     *
     * <tr><td>Dropped COUNTER column</td><td>iterator</td>
     * <td>NOT SUPPORTED here. The cursor path has no counter merge at all: {@link #mergeCells}
     * throws for a counter cell, and {@link #unsupportedSchema} rejects a counter the schema still
     * has. The iterator path handles it.</td></tr>
     * </table>
     *
     * <h2>The dropped multi-cell case, and why the fallback target is not safe either</h2>
     *
     * The cursor reads, merges and writes complex framing correctly, so this gate is NOT about
     * parsing. It is closed because of the state of the fallback target.
     *
     * The drop filter is gated on the timestamp, not on the column, so a cell written above the
     * drop time survives the read. What happens next depends on the data:
     * <ul>
     *   <li>every cell and complex deletion at or below the drop time: the iterator compacts
     *       normally and the filter removes the data. The fallback costs throughput only;</li>
     *   <li>any cell above the drop time: the ITERATOR fails. It throws a NullPointerException,
     *       for a regular column and for a static one alike. The one exception is a table whose
     *       dropped column was its ONLY static column: {@code UnfilteredRowIterators.mergeStaticRows}
     *       returns {@code Rows.EMPTY_STATIC_ROW} on an empty column set, so it discards the whole
     *       static block before it builds a merger, and no exception is thrown. The table cannot
     *       compact on either path, and closing this gate only chooses which path fails. See
     *       CASSANDRA-21607.</li>
     * </ul>
     *
     * A surviving cell is also data that a later re-add of the column must not bring back, which is
     * the second half of CASSANDRA-21607. Note that the re-added row of the matrix above inherits
     * that: both paths carry such a cell once the column is back in the schema.
     *
     * <b>Open this gate to multi-cell columns once CASSANDRA-21607 is fixed</b>, that is, once the
     * read filter discards a dropped column's cells whatever their timestamp. Parity with the
     * iterator path needs no further work in this class. It needs a reference that does not fail.
     *
     * <h2>The fallback is permanent for this table</h2>
     *
     * A drop cannot be undone in the headers, and no compaction clears it.
     * {@code SerializationHeader.make} builds the output header as the union of the INPUT headers'
     * columns, so every compaction copies the dropped column forward into the header it writes. The
     * iterator path does this as well. One pre-drop sstable therefore sends every future compaction
     * of the table to the iterator, for the life of the table.
     *
     * See CASSANDRA-21463.
     */
    private static boolean unsupportedHeaderColumns(TableMetadata metadata, SSTableReader reader)
    {
        // RegularAndStaticColumns iterates statics then regulars, so this covers both
        for (ColumnMetadata column : reader.header.columns())
        {
            if (isDroppedMultiCellOrCounterColumn(metadata, column, reader.header.getType(column)))
            {
                if (LOGGER.isDebugEnabled()) logDebugReason(metadata, "A multi-cell or counter column dropped from the schema is still carried in the header of " + reader.descriptor + ", which the cursor path does not yet cover. column=" + column);
                return true;
            }
        }
        return false;
    }

    /**
     * True if {@code column} is a complex or counter column that the schema has dropped.
     *
     * @param diskType the type the header records for {@code column}. Test this type as well as
     *                 the schema type, because the reader decodes against the header type.
     */
    private static boolean isDroppedMultiCellOrCounterColumn(TableMetadata metadata, ColumnMetadata column, AbstractType<?> diskType)
    {
        boolean isMultiCellOrCounter = column.isComplex() || column.isCounterColumn()
                                      || (diskType != null && (diskType.isMultiCell() || diskType.isCounter()));
        return isMultiCellOrCounter && metadata.getColumn(column.name) == null;
    }

    private static void logDebugReason(TableMetadata metadata, String reason)
    {
        LOGGER.debug("Cursor compaction for table: " + metadata.name + " keyspace: " + metadata.keyspace + " is not supported. REASON: " + reason);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CursorCompactor.class.getName());

    private final OperationType type;
    private final AbstractCompactionController controller;
    private final ActiveCompactionsTracker activeCompactions;
    private final ImmutableSet<SSTableReader> sstables;
    private final long nowInSec;
    private final TimeUUID compactionId;
    private final long totalInputBytes;
    private final long totalCompressedInputBytes;
    private final StatefulCursor[] sstableCursors;
    private final boolean[] sstableCursorsEqualsNext;
    private final boolean hasStaticColumns;
    private final boolean enforceStrictLiveness;

    /**
     * Scratch for {@link #anyMergedCellDeadAtNow}, which walks a row's cells and then puts the
     * cursors back. The arrays hold the cursor ORDER and the equals-next flags that its sorts
     * overwrite, and the per-cursor state that tells it which cursors to rewind. All three are
     * null unless the table enforces strict liveness.
     */
    private final StatefulCursor[] probeCursorOrder;
    private final boolean[] probeEqualsNext;
    private final int[] probeCursorState;
    // Scratch space for the complex-deletion test in anyMergedCellDeadAtNow. Same reason as above.
    private final DeletionTime.ReusableDeletionTime probeComplexDeletion;

    // Keep targetDirectory for compactions, needed for `nodetool compactionstats`
    private volatile String targetDirectory;

    private CursorMergeSink ssTableCursorWriter;
    private boolean finished = false;

    /*
     * counters for merged partitions/rows/cells.
     * array index represents (number of merged rows - 1), so index 0 is counter for no merge (1 row),
     * index 1 is counter for 2 rows merged, and so on.
     */
    private final long[] partitionMergeCounters;
    private final long[] staticRowMergeCounters;
    private final long[] rowMergeCounters;
    private final long[] rangeTombstonesMergeCounters;
    private final long[] cellMergeCounters;

    // Progress accounting
    private long totalBytesRead = 0;
    private long totalSourceCQLRows;
    private long totalDataBytesWritten;

    // Optional top-partitions-by-tombstones tracking for the read-only validation path
    // (mergeNextPartition). Null on the writing compaction path, where every counting site below is
    // a no-op. Mirrors the legacy TopPartitionTracker.TombstoneCounter, which CompactionIterator
    // applies to the MERGED stream BEFORE the Purger runs - so tombstones are counted post-shadow
    // (a cell/deletion shadowed by a higher-level deletion is excluded, matching Row.Merger) but
    // PRE-purge (a gc-purgeable tombstone is still counted). Complex/collection deletions are not
    // counted, and static-row tombstones are excluded, matching TombstoneCounter.
    private TopPartitionTracker.Collector topPartitionCollector;
    private long partitionTombstoneCount;

    // state
    final Purger purger;

    private StatefulCursor lastSource = null;
    /**
     * The PartitionDescriptor instance holding the last WRITTEN partition's header, owned by the write side.
     * Obtained by swapping this field's previous contents into the writing cursor's prev slot, so the key is
     * never copied. Non-final: it IS the floater, and every steal exchanges it.
     */
    private PartitionDescriptor lastWrittenPartition;
    /**
     * The cursor whose partition was written most recently, and from which the steal is still owed. Cleared by
     * the steal, so a skipped partition leaves nothing to take and the held instance keeps describing the last
     * partition actually written.
     */
    private StatefulCursor lastWrittenPartitionSource = null;
    /** {@link StatefulCursor#partitionSwaps()} on that cursor at the moment the write happened. */
    private long lastWrittenPartitionSourceSwaps = 0;
    /**
     * The UnfilteredDescriptor holding the last unfiltered written to the current output partition.
     * A cursor overwrites its own descriptor on its next read, so this is the floater.
     */
    private UnfilteredDescriptor lastWrittenUnfiltered;
    /**
     * Unfiltereds written to the current output partition. At zero,
     * {@link #lastWrittenUnfiltered} still holds an earlier partition's clustering, or nothing on
     * the first partition.
     */
    private int unfilteredsWrittenToPartition = 0;

    // Partition state. Writes can be delayed if the deletion is purged, or live and partition is empty -> LIVE deletion.
    PartitionDescriptor partitionDescriptor;

    // This will be 0 if we haven't written partition header.
    int partitionHeaderLength = 0;
    // Whether startPartition() has been called for the partition currently being merged - tracked
    // independently of partitionHeaderLength (a byte count only meaningful to the real sstable
    // writer's header-length bookkeeping) so isPartitionStarted() doesn't require a non-writing
    // CursorMergeSink (see DigestingCursorMergeSink) to fabricate meaningful byte positions purely
    // to keep this control-flow check correct.
    private boolean partitionStarted = false;
    private OutputWriterProvider writerProvider;

    public CursorCompactor(OperationType type, List<ISSTableScanner> scanners, AbstractCompactionController controller, long nowInSec, TimeUUID compactionId)
    {
        this(type, scanners, controller, nowInSec, compactionId, ActiveCompactionsTracker.NOOP);
    }

    private CursorCompactor(OperationType type,
                           List<ISSTableScanner> scanners,
                           AbstractCompactionController controller,
                           long nowInSec,
                           TimeUUID compactionId,
                           ActiveCompactionsTracker activeCompactions)
    {
        this.controller = controller;
        this.type = type;
        this.nowInSec = resolveNowInSec(controller, nowInSec);
        this.compactionId = compactionId;

        long inputBytes = 0;
        long compressedInputBytes = 0;
        for (ISSTableScanner scanner : scanners)
        {
            inputBytes += scanner.getLengthInBytes();
            compressedInputBytes += scanner.getCompressedLengthInBytes();
        }
        this.totalInputBytes = inputBytes;
        this.totalCompressedInputBytes = compressedInputBytes;
        this.partitionMergeCounters = new long[scanners.size()];
        this.staticRowMergeCounters = new long[partitionMergeCounters.length];
        this.rowMergeCounters = new long[partitionMergeCounters.length];
        this.rangeTombstonesMergeCounters = new long[partitionMergeCounters.length];
        this.cellMergeCounters = new long[partitionMergeCounters.length];
        // note that we leak `this` from the constructor when calling beginCompaction below, this means we have to get the sstables before
        // calling that to avoid a NPE.
        this.sstables = scanners.stream().map(ISSTableScanner::getBackingSSTables).flatMap(Collection::stream).collect(ImmutableSet.toImmutableSet());
        // This is always NOOP, but keep it around in case we need it later to match CompactionIterator
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        this.activeCompactions.beginCompaction(this); // note that CompactionTask also calls this, but CT only creates CompactionIterator with a NOOP ActiveCompactions

        TableMetadata metadata = metadata();
        this.hasStaticColumns = anyStaticColumns(this.sstables);
        /**
         * Pipeline should end up similar to the one in {@link CompactionIterator}:
         * [MERGED -> ?TopPartitionTracker -> GarbageSkipper -> Purger -> org.apache.cassandra.db.transform.DuplicateRowChecker -> Abortable] -> next()
         * V - Merge - This is drawing on code all over the place to iterate through the data and merge partitions/rows/cells
         * * {@link org.apache.cassandra.db.transform.Transformation}s, applied to above iterator:
         *   X - Not needed for CompactionTask usage: {@link org.apache.cassandra.metrics.TopPartitionTracker.TombstoneCounter}
         *   X - Unsupported {@link CompactionIterator.GarbageSkipper} - filters out, or "skips" data shadowed by the provided "tombstone source".
         *   V - {@link CompactionIterator.Purger} - filters out, or "purges" gc-able tombstones. Also updates bytes read on every row % 100.
         *   X - Not needed for latest version tables: {@link org.apache.cassandra.db.transform.DuplicateRowChecker}
         *   V - Abortable - aborts the compaction if the user has requested it (at a certain granularity).
         * {@link CompactionIterator#CompactionIterator(OperationType, List, AbstractCompactionController, long, TimeUUID, ActiveCompactionsTracker)}
         */

        this.sstableCursors = convertScannersToCursors(scanners, sstables, DatabaseDescriptor.getCompactionReadDiskAccessMode());
        this.sstableCursorsEqualsNext = new boolean[sstables.size()];
        this.enforceStrictLiveness = controller.cfs.metadata.get().enforceStrictLiveness();
        this.probeCursorOrder = enforceStrictLiveness ? new StatefulCursor[sstableCursors.length] : null;
        this.probeEqualsNext = enforceStrictLiveness ? new boolean[sstableCursors.length] : null;
        this.probeCursorState = enforceStrictLiveness ? new int[sstableCursors.length] : null;
        this.probeComplexDeletion = enforceStrictLiveness ? DeletionTime.ReusableDeletionTime.live() : null;

        purger = new Purger(type, controller);

        lastWrittenPartition = new PartitionDescriptor(metadata.partitioner.createReusableKey(0));
        lastWrittenUnfiltered = new UnfilteredDescriptor(metadata.comparator.subtypes().toArray(AbstractType[]::new));
        // A steal moves a descriptor between cursors, and to the write-side instance built from
        // the table comparator above. Each parses a clustering with its own clusteringTypes, so
        // all of them must parse identically.
        assert clusteringParsingAgrees() : "the cursors disagree on how to parse a clustering: " + metadata;
    }

    /** @see #lastWrittenUnfiltered */
    private boolean clusteringParsingAgrees()
    {
        AbstractType<?>[] writeSide = lastWrittenUnfiltered.clusteringTypes();
        for (StatefulCursor cursor : sstableCursors)
        {
            AbstractType<?>[] readSide = cursor.unfiltered().clusteringTypes();
            if (readSide.length != writeSide.length)
                return false;
            for (int i = 0; i < readSide.length; i++)
            {
                if (readSide[i].isValueLengthFixed() != writeSide[i].isValueLengthFixed())
                    return false;
                if (readSide[i].isValueLengthFixed()
                    && readSide[i].valueLengthIfFixed() != writeSide[i].valueLengthIfFixed())
                    return false;
            }
        }
        return true;
    }

    /**
     * Builds cursors directly from {@code boundsBySSTable}'s sstables, each restricted to its
     * given partial byte ranges (via {@link StatefulCursor#positionAt}) instead of full-range
     * scanners - e.g. repair validation, which only ever reads its assigned repair ranges and
     * never writes output (pair with {@link #mergeNextPartition}, having confirmed
     * {@link #isValidationSupported}), and cleanup, which reads only the ranges this node still
     * owns and rewrites them (pair with {@link #writeNextPartition}, having confirmed
     * {@link #isCleanupSupported}).
     */
    public CursorCompactor(OperationType type,
                          Map<SSTableReader, List<PartitionPositionBounds>> boundsBySSTable,
                          AbstractCompactionController controller,
                          long nowInSec,
                          TimeUUID compactionId)
    {
        this(type, boundsBySSTable, controller, nowInSec, compactionId, ActiveCompactionsTracker.NOOP);
    }

    public CursorCompactor(OperationType type,
                          Map<SSTableReader, List<PartitionPositionBounds>> boundsBySSTable,
                          AbstractCompactionController controller,
                          long nowInSec,
                          TimeUUID compactionId,
                          ActiveCompactionsTracker activeCompactions)
    {
        this.controller = controller;
        this.type = type;
        this.nowInSec = resolveNowInSec(controller, nowInSec);
        this.compactionId = compactionId;

        long inputBytes = 0;
        long compressedInputBytes = 0;
        for (Map.Entry<SSTableReader, List<PartitionPositionBounds>> entry : boundsBySSTable.entrySet())
        {
            long entryBytes = 0;
            for (PartitionPositionBounds bounds : entry.getValue())
                entryBytes += bounds.upperPosition - bounds.lowerPosition;
            inputBytes += entryBytes;
            SSTableReader sstable = entry.getKey();
            compressedInputBytes += sstable.compression ? sstable.onDiskSizeForPartitionPositions(entry.getValue())
                                                        : entryBytes;
        }
        this.totalInputBytes = inputBytes;
        this.totalCompressedInputBytes = compressedInputBytes;
        this.sstables = ImmutableSet.copyOf(boundsBySSTable.keySet());
        this.partitionMergeCounters = new long[sstables.size()];
        this.staticRowMergeCounters = new long[partitionMergeCounters.length];
        this.rowMergeCounters = new long[partitionMergeCounters.length];
        this.rangeTombstonesMergeCounters = new long[partitionMergeCounters.length];
        this.cellMergeCounters = new long[partitionMergeCounters.length];
        // note that we leak `this` from the constructor when calling beginCompaction below, this means we have to get the sstables before
        // calling that to avoid a NPE (sstables is set above).
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        this.activeCompactions.beginCompaction(this);

        // beginCompaction above registered this compaction with the active tracker; finishCompaction
        // only runs from close(), which never runs on a constructor that throws. If anything below
        // fails (e.g. an I/O error opening the partial-range cursors), unregister here before
        // rethrowing so a failed setup doesn't leave a phantom nodetool compactionstats entry that
        // never clears.
        try
        {
            TableMetadata metadata = metadata();
            this.hasStaticColumns = anyStaticColumns(this.sstables);

            this.sstableCursors = convertSSTablesToPartialRangeCursors(boundsBySSTable, DatabaseDescriptor.getCompactionReadDiskAccessMode());
            this.sstableCursorsEqualsNext = new boolean[sstables.size()];
            this.enforceStrictLiveness = controller.cfs.metadata.get().enforceStrictLiveness();
            this.probeCursorOrder = enforceStrictLiveness ? new StatefulCursor[sstableCursors.length] : null;
            this.probeEqualsNext = enforceStrictLiveness ? new boolean[sstableCursors.length] : null;
            this.probeCursorState = enforceStrictLiveness ? new int[sstableCursors.length] : null;
            this.probeComplexDeletion = enforceStrictLiveness ? DeletionTime.ReusableDeletionTime.live() : null;

            purger = new Purger(type, controller);

            lastWrittenPartition = new PartitionDescriptor(metadata.partitioner.createReusableKey(0));
            lastWrittenUnfiltered = new UnfilteredDescriptor(metadata.comparator.subtypes().toArray(AbstractType[]::new));
            assert clusteringParsingAgrees() : "the cursors disagree on how to parse a clustering: " + metadata;
        }
        catch (Throwable t)
        {
            activeCompactions.finishCompaction(this);
            throw t;
        }
    }

    /**
     * Supplies the output sstable writer to the merge loop. Called once immediately before the
     * first unfiltered of an output partition is written, and must return a non-null
     * {@link SSTableWriter} only when output has to roll over to a new sstable (so the merge loop
     * knows to close out the previous one) - null means "keep writing to the current one".
     * {@link CompactionAwareWriter#maybeSwitchWriter} implements exactly this contract; cleanup
     * (see {@code CompactionManager#doCleanupOne}) supplies its own single-output implementation
     * over a bare {@link org.apache.cassandra.io.sstable.SSTableRewriter} rather than dragging in
     * the {@code CompactionAwareWriter} disk-boundary/size-rollover machinery it deliberately
     * does not use.
     */
    public interface OutputWriterProvider
    {
        SSTableWriter maybeSwitchWriter(DecoratedKey key);
    }

    /**
     * Mirrors {@code CompactionIterator.purger()}: accord-enabled (and accord-migrating) tables
     * purge and expire relative to {@code gcBefore} - derived from accord's durability bounds by
     * {@code CompactionTask.getCompactionController} - retaining data accord may still read at
     * earlier timestamps; every {@code nowInSec} use in this class is a purge/expiry decision.
     * Shared by both constructors: it's the same rule regardless of whether the input is
     * scanners or a partial-range bounds map.
     */
    private static long resolveNowInSec(AbstractCompactionController controller, long nowInSec)
    {
        TableMetadata tableMetadata = controller.cfs.metadata();
        return tableMetadata.isAccordEnabled() || tableMetadata.migratingFromAccord()
               ? controller.gcBefore
               : nowInSec;
    }

    /**
     * The INPUT headers decide whether static rows can occur in this merge (and the output
     * header, {@code SerializationHeader.make}, is their union): after {@code ALTER TABLE ... DROP}
     * of the last static column, current metadata has no static columns but older sstables
     * legitimately still carry static rows. Shared by both constructors: the rule doesn't depend
     * on whether the sstables were reached via scanners or a partial-range bounds map.
     */
    private static boolean anyStaticColumns(Set<SSTableReader> sstables)
    {
        boolean anyStaticColumns = false;
        for (SSTableReader sstable : sstables)
            anyStaticColumns |= sstable.header.hasStatic();
        return anyStaticColumns;
    }

    /**
     * @return false if finished, true if partition is written (which might require multiple partition reads)
     */
    public boolean writeNextPartition(OutputWriterProvider writerProvider) throws IOException {
        while (!finished) {
            if (tryWriteNextPartition(writerProvider)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Read-only counterpart to {@link #writeNextPartition}: drives the merge against {@code sink}
     * without ever touching a {@link CompactionAwareWriter} or writer-rollover machinery
     * ({@link #maybeSwitchWriter}) - e.g. repair validation, which never produces output
     * sstables. Unlike the writing path, {@code sink} is fixed for this compactor's entire
     * lifetime: there is no rollover concept for a non-writing consumer.
     */
    public boolean mergeNextPartition(CursorMergeSink sink) throws IOException {
        this.ssTableCursorWriter = sink;
        while (!finished) {
            if (tryWriteNextPartition(null)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if a partition was written
     */
    private boolean tryWriteNextPartition(OutputWriterProvider writerProvider) throws IOException
    {
        if (isStopRequested())
            throw new CompactionInterruptedException(getCompactionInfo());

        int partitionMergeLimit = prepareAndSortForPartitionMerge();
        // The round's slot advances have happened, so the steal owed by the last written partition is due. This
        // sits ahead of the finish() exit because that path consumes the value too, through writerRollover's
        // setLast; on it the advance came from resetAfterDone rather than from a read.
        takeOwedPartitionSteal();
        if (partitionMergeLimit == 0)
        {
            finish();
            return false;
        }
        // Top reader is on the current key/header
        StatefulCursor currSource = sstableCursors[0];
        partitionDescriptor = currSource.currPartition();

        // possibly reached boundary of the current writer
        try
        {
            DecoratedKey key = partitionDescriptor.key();
            // The check begins once a partition has been written, because there is nothing to check against
            // before that: no output exists to be out of order with, and no writer is created until the first
            // write, so setLast is unreachable in that window.
            if (hasWrittenPartition() && lastSource != null && currSource != lastSource && lastWrittenKey().compareTo(key) >= 0)
                throw new IllegalStateException(String.format("Last written key %s >= current key %s", lastWrittenKey(), key));

            // needed if we actually write a partition, not used otherwise
            this.writerProvider = writerProvider;

            purger.resetOnNewPartition(key);
            boolean written = mergePartitions(partitionMergeLimit);
            if (!written)
            {
                purger.onEmptyPartitionPostPurge();
            }
            else
            {
                // the steal is owed by this cursor, and lands one round later — see detachPrevPartition
                lastWrittenPartitionSource = currSource;
                lastWrittenPartitionSourceSwaps = currSource.partitionSwaps();
            }
            return written;
        }
        finally
        {
            lastSource = currSource;
            partitionDescriptor = null;
            partitionHeaderLength = 0;
            partitionStarted = false;
        }
    }

    /**
     * See {@link UnfilteredPartitionIterators#merge(List, UnfilteredPartitionIterators.MergeListener)}
     */
    private boolean mergePartitions(int partitionMergeLimit) throws IOException
    {
        partitionMergeCounters[partitionMergeLimit - 1]++;

        // Reset the per-partition pre-purge tombstone tally (validation path only).
        partitionTombstoneCount = 0;

        // Pick "max" pDeletion
        /** {@link UnfilteredRowIterators.UnfilteredRowMergeIterator#collectPartitionLevelDeletion(List, UnfilteredRowIterators.MergeListener)}*/
        final DeletionTime mergedDeletion = mergePartitionDeletions(partitionMergeLimit);

        // Pre-purge partition-level deletion count, matching TombstoneCounter.applyToPartition
        // (which counts the merged partitionLevelDeletion before the Purger runs).
        if (topPartitionCollector != null && !mergedDeletion.isLive())
            partitionTombstoneCount++;

        // maybe purge? If the partition is written out, this will be the deletion we write.
        final DeletionTime toWritePartitionDeletion = maybePurgedOutputDeletion(mergedDeletion);
        if (toWritePartitionDeletion != DeletionTime.LIVE) {
            startPartition(toWritePartitionDeletion);
        }
        // active deletion tracks the open deletion within a partition, so will change to track range tombstones
        DeletionTime activeDeletion = mergedDeletion;

        // Merge any common static rows
        if (hasStaticColumns)
        {
            int staticRowMergeLimit = prepareAndSortStaticForMerge(partitionMergeLimit);
            if (staticRowMergeLimit != 0)
            {
                // No steal here: this call's return value is not consumed.
                mergeRows(staticRowMergeLimit, activeDeletion, true, false);
                // Required. A cursor left at UNFILTERED_END still holds the static descriptor,
                // and STATIC_CLUSTERING sorts ahead of every row, so the loop below re-merges the
                // consumed position as a phantom row. The output is unaffected; rowMergeCounters
                // is not, and it feeds system.compaction_history.rows_merged.
                continueReadingAfterMerge(staticRowMergeLimit, UNFILTERED_END);
            }
            if (isPartitionStarted())
            {
                if (staticRowMergeLimit == 0) ssTableCursorWriter.writeEmptyStaticRow();
                partitionHeaderLength = (int) (ssTableCursorWriter.getPosition() - ssTableCursorWriter.getPartitionStart());
            }
        }

        // Merge any common normal rows
        int unfilteredMergeLimit = partitionMergeLimit;
        boolean isFirstUnfiltered = true;
        unfilteredsWrittenToPartition = 0;
        while (true)
        {
            unfilteredMergeLimit = prepareAndSortUnfilteredForMerge(partitionMergeLimit, unfilteredMergeLimit);
            if (unfilteredMergeLimit == 0)
                break;
            int flags = sstableCursors[0].unfiltered().flags();
            if (UnfilteredSerializer.isRow(flags))
            {
                if (mergeRows(unfilteredMergeLimit, activeDeletion, false, isFirstUnfiltered))
                {
                    // A static descriptor must never be written from this loop: its clustering has
                    // length 0.
                    assert sstableCursors[0].unfiltered().clusteringKind() != ClusteringPrefix.Kind.STATIC_CLUSTERING
                         : "a static descriptor was written from the unfiltered loop";
                    isFirstUnfiltered = false;
                    unfilteredsWrittenToPartition++;
                    detachWrittenUnfiltered();
                }
            }
            else if (UnfilteredSerializer.isTombstoneMarker(flags)) {
                // the tombstone processing *maybe* writes a marker, and *maybe* changes the `activeOpenRangeDeletion`
                if (mergeRangeTombstones(unfilteredMergeLimit, mergedDeletion, isFirstUnfiltered))
                {
                    isFirstUnfiltered = false;
                    unfilteredsWrittenToPartition++;
                    detachWrittenUnfiltered();
                }
                if (activeOpenRangeDeletion == DeletionTime.LIVE) {
                    activeDeletion = mergedDeletion;
                }
                else {
                    activeDeletion = activeOpenRangeDeletion;
                }
            }
            else {
                throw new IllegalStateException("Unexpected unfiltered type (not row or tombstone):" + flags);
            }
            // move along
            continueReadingAfterMerge(unfilteredMergeLimit, UNFILTERED_END);
        }

        boolean partitionWritten = isPartitionStarted();
        if (partitionWritten)
        {
            // The trailing index block's last name and the covered-clustering max are the same value, the
            // clustering of the last unfiltered written here; a partition that wrote none has no trailing
            // block to cut, hence null.
            ClusteringDescriptor lastName = unfilteredsWrittenToPartition > 0 ? lastWrittenClustering() : null;
            ssTableCursorWriter.writePartitionEnd(partitionDescriptor.key(), partitionDescriptor.keyBytes(), partitionDescriptor.keyLength(), toWritePartitionDeletion, partitionHeaderLength, lastName);
            // Update min/max clustering metadata. The count guard is required; see
            // unfilteredsWrittenToPartition.
            if (unfilteredsWrittenToPartition > 1) {
                ssTableCursorWriter.updateClusteringMetadata(lastName);
            }
        }
        // Report the pre-purge tombstone tally for this partition, matching legacy
        // TombstoneCounter.onPartitionClose - which fires once per merged partition regardless of
        // whether it survives purge. The collector keeps a stable key, so snapshot the reusable one.
        if (topPartitionCollector != null)
        {
            DecoratedKey stableKey = metadata().partitioner.decorateKey(ByteBufferUtil.clone(partitionDescriptor.key().getKey()));
            topPartitionCollector.trackTombstoneCount(stableKey, partitionTombstoneCount);
        }
        // move along
        continueReadingAfterMerge(partitionMergeLimit, PARTITION_END);
        return partitionWritten;
    }

    private void startPartition(DeletionTime toWritePartitionDeletion) throws IOException
    {
        // writerProvider is null for mergeNextPartition()'s read-only path: ssTableCursorWriter
        // was already fixed to the sink for this compactor's whole lifetime, no rollover applies.
        if (writerProvider != null)
            maybeSwitchWriter(writerProvider);
        partitionHeaderLength = ssTableCursorWriter.writePartitionStart(
                                    partitionDescriptor.keyBytes(),
                                    partitionDescriptor.keyLength(),
                                    toWritePartitionDeletion);
        partitionStarted = true;
    }

    private DeletionTime maybePurgedOutputDeletion(DeletionTime mergedDeletion) throws IOException
    {
        final DeletionTime toWritePartitionDeletion;

        if (!mergedDeletion.isLive() && !purger.shouldPurge(mergedDeletion))
        {
            toWritePartitionDeletion = mergedDeletion;
        }
        else
        {
            toWritePartitionDeletion = DeletionTime.LIVE;
        }
        return toWritePartitionDeletion;
    }

    private DeletionTime mergePartitionDeletions(int partitionMergeLimit)
    {
        DeletionTime mergedDeletion = partitionDescriptor.deletionTime();
        for (int i = 1; i < partitionMergeLimit; i++)
        {
            DeletionTime otherDeletionTime = sstableCursors[i].currPartition().deletionTime();
            if (!mergedDeletion.supersedes(otherDeletionTime))
                mergedDeletion = otherDeletionTime;
        }
        return mergedDeletion;
    }

    /**
     * We have a common clustering and need to merge data.
     * {@link Row.Merger#merge(DeletionTime)}
     */
    private boolean mergeRows(int rowMergeLimit, DeletionTime partitionActiveDeletion, boolean isStatic, boolean isFirstUnfiltered) throws IOException
    {
        if (isStopRequested())
            throw new CompactionInterruptedException(getCompactionInfo());

        if (isStatic)
        {
            staticRowMergeCounters[rowMergeLimit - 1]++;
        }
        else
        {
            rowMergeCounters[rowMergeLimit - 1]++;
        }

        // merge deletion/liveness
        /** {@link Row.Merger#merge(DeletionTime)}*/
        UnfilteredDescriptor row = sstableCursors[0].unfiltered();

        LivenessInfo mergedRowInfo = row.livenessInfo();
        DeletionTime mergedRowDeletion = row.deletionTime();
        // Row.Deletion.isShadowable(): deprecated (CASSANDRA-11500), reachable only on old
        // Materialized View data. Tracked alongside mergedRowDeletion because the shadowing step
        // below reads it, and because it has to survive to the output write attached to whichever
        // deletion wins.
        boolean mergedRowShadowable = row.isShadowableDeletion();

        for (int i = 1; i < rowMergeLimit; i++)
        {
            // TODO: can validate state here
            row = sstableCursors[i].unfiltered();
            // TODO: maybe flags more optimal(avoid ref loads and comaparisons etc)
            if (row.livenessInfo().supersedes(mergedRowInfo))
                mergedRowInfo = row.livenessInfo();
            if (row.deletionTime().supersedes(mergedRowDeletion))
            {
                mergedRowDeletion = row.deletionTime();
                mergedRowShadowable = row.isShadowableDeletion();
            }
        }

        /**
         * {@link Row.Deletion#isShadowedBy(LivenessInfo)}, placed as
         * {@link Row.Merger#merge(DeletionTime)} places it. No shadowed cell resurfaces:
         * {@code BTreeRow.Builder} drops those at write time.
         */
        if (mergedRowShadowable && mergedRowInfo.timestamp() > mergedRowDeletion.markedForDeleteAt())
        {
            mergedRowDeletion = DeletionTime.LIVE;
            mergedRowShadowable = false; // a live deletion is never shadowable
        }

        /**
         * See: {@link BTreeRow#purge(DeletionPurger, long, boolean)}
         */
        DeletionTime rowActiveDeletion = partitionActiveDeletion;
        // Whether BTreeRow.purge's hasDeletion(nowInSec) guard would be open for reasons OTHER than the
        // row's cells; see the strict-liveness branch below for why only the purger's two clearances
        // count here.
        boolean rowHasDeletionAtNow = false;
        if (mergedRowDeletion.supersedes(rowActiveDeletion))
        {
            // Pre-purge row-deletion count, matching TombstoneCounter.applyToRow (!row.deletion().
            // isLive()): counted only when the merged row deletion is NOT shadowed by the active
            // (partition/range) deletion - the same condition Row.Merger uses to keep it - and
            // before shouldPurge below can drop it. Static rows are excluded (TombstoneCounter runs
            // via applyToRow, not applyToStatic).
            if (topPartitionCollector != null && !isStatic)
                partitionTombstoneCount++;
            rowActiveDeletion = mergedRowDeletion; // deletion is in effect before purge takes effect
            if (purger.shouldPurge(mergedRowDeletion))
            {
                mergedRowDeletion = DeletionTime.LIVE;
                mergedRowShadowable = false; // a live deletion is never shadowable
                rowHasDeletionAtNow = true;
            }
        }
        else
        {
            // partition delete takes over
            mergedRowDeletion = DeletionTime.LIVE;
            mergedRowShadowable = false; // a live deletion is never shadowable
        }

        // Only the purger arm records a clearance: BTreeRow.purge computes minLocalDeletionTime
        // after the active deletion empties the liveness, and before the purger runs.
        if (rowActiveDeletion.deletes(mergedRowInfo))
        {
            mergedRowInfo = LivenessInfo.EMPTY;
        }
        else if (purger.shouldPurge(mergedRowInfo, nowInSec))
        {
            // shouldPurge requires localDeletionTime < gcBefore, and gcBefore <= nowInSec here, so
            // the reference term is at or below nowInSec.
            rowHasDeletionAtNow |= !mergedRowInfo.isEmpty();
            mergedRowInfo = LivenessInfo.EMPTY;
        }

        boolean isRowDropped = mergedRowDeletion.isLive() && mergedRowInfo.isEmpty();

        if (!isRowDropped)
        {
            lateStartRow(mergedRowInfo, mergedRowDeletion, mergedRowShadowable, isStatic);
        }

        /**
         * Strict liveness ({@link org.apache.cassandra.schema.TableMetadata#enforceStrictLiveness})
         * drops a row with no primary-key liveness and no row deletion. {@link BTreeRow#purge} reaches
         * that drop only past its opening {@code if (!hasDeletion(nowInSec)) return this;}, so the
         * cursor applies the same precondition. Without it the cursor deletes rows the iterator
         * returns untouched, cells included.
         */
        if (isRowDropped && enforceStrictLiveness
            && (rowHasDeletionAtNow || anyMergedCellDeadAtNow(rowMergeLimit, rowActiveDeletion, isStatic)))
        {
            skipRowsOnStrictLiveness(rowMergeLimit, isStatic);
        }
        else
        {
            int cellMergeLimit = rowMergeLimit;
            currentComplexColumn = null;
            // loop through the columns and copy/merge each cell
            while (true)
            {
                // advance cursors that need to read the cell header
                for (int i = 0; i < cellMergeLimit; i++)
                {
                    int readerState = sstableCursors[i].state();
                    if (readerState == CELL_HEADER_START)
                    {
                        sstableCursors[i].readCellHeader();
                    }
                }
                // Sort rows by cells
                cellMergeLimit = prepareAndSortCellsForMerge(rowMergeLimit, cellMergeLimit);
                if (cellMergeLimit == 0)
                    break;
                isRowDropped = mergeCells(rowMergeLimit, cellMergeLimit, rowActiveDeletion, mergedRowInfo, isRowDropped, isStatic);
                // move along
                continueReadingAfterMerge(cellMergeLimit, CELL_END);
            }
            if (!isRowDropped)
                ssTableCursorWriter.writeRowEnd(sstableCursors[0].unfiltered(), isFirstUnfiltered);
        }
        if (isRowDropped && isStatic &&
            isPartitionStarted())
            // if the partition write has not started, keep delaying it, might be an empty partition (purged+no data)
        {
            ssTableCursorWriter.writeEmptyStaticRow();
        }
        return !isRowDropped;
    }

    private void skipRowsOnStrictLiveness(int rowMergeLimit, boolean isStatic) throws IOException
    {
        for (int i = 0; i < rowMergeLimit; i++)
        {
            if (sstableCursors[i].state() != UNFILTERED_END){
                if (isStatic)
                    sstableCursors[i].skipStaticRow();
                else
                    sstableCursors[i].skipUnfiltered();
            }
        }
    }

    /**
     * True if the merged row has a cell that is not live at {@code nowInSec}. Such a cell is either
     * a cell tombstone or an expiring cell that is past its expiry time.
     *
     * This is the cells' part of the {@code hasDeletion(nowInSec)} guard in {@link BTreeRow#purge}.
     * {@code Cell.minDeletionTime()} gives {@code Long.MIN_VALUE} for a tombstone,
     * {@code Cell.NO_DELETION_TIME} for a live cell, and the local deletion time in all other
     * cases. Thus "at or below {@code nowInSec}" and "not live at {@code nowInSec}" select the same
     * cells.
     *
     * Only the merge winner of each column counts. Only the winner goes into the reference merged
     * row, and only if it survives the active deletion, which the reference applies before it
     * reconciles. This method picks the winner first and then tests the active deletion, which
     * gives the same answer: the winner has the highest timestamp, so if the active deletion
     * deletes the winner, it deletes every cell of that column.
     *
     * {@link CellLivenessInfo#resolve} alone selects the winner. Its {@code COMPARE} result never
     * leaves the two cells in disagreement about liveness. {@code resolve} returns
     * {@code COMPARE} in two cases only:
     * <ul>
     *   <li>both cells hold {@code Cell.NO_DELETION_TIME}, so both are live;</li>
     *   <li>both cells hold the same timestamp, deletion time and TTL.</li>
     * </ul>
     * Therefore the cell-value comparison that this method skips cannot change the answer.
     *
     * A complex column also has its own deletion, which is a second and independent term.
     * {@link BTreeRow#minDeletionTime(org.apache.cassandra.db.rows.ComplexColumnData)} always folds
     * a non-live complex deletion in as {@code Long.MIN_VALUE}, whatever the cells below it
     * contribute. A column can hold a dead deletion and a live cell together, and the deletion
     * alone still opens the guard. {@link #foldAndClampComplexDeletion} computes the same merged
     * deletion that {@link #mergeCells} uses to shadow older cells, before the purge.
     *
     * This method walks the row's cells and then rewinds every cursor it moved. The caller's cell
     * loop therefore runs against the state it would see if the probe had not run. The rewind also
     * restores the cursor order and the equals-next flags, which the cell sorts overwrite. That
     * restore is a safety measure: the real loop sorts the whole group again in any case.
     */
    private boolean anyMergedCellDeadAtNow(int rowMergeLimit, DeletionTime rowActiveDeletion, boolean isStatic)
    {
        System.arraycopy(sstableCursors, 0, probeCursorOrder, 0, rowMergeLimit);
        System.arraycopy(sstableCursorsEqualsNext, 0, probeEqualsNext, 0, rowMergeLimit);
        for (int i = 0; i < rowMergeLimit; i++)
            probeCursorState[i] = sstableCursors[i].state();

        boolean anyDead = false;
        int cellMergeLimit = rowMergeLimit;
        // All cells of one complex column share a single fold result. Compute it again only when
        // the lead cursor moves to a new column. mergeCells caches mergedComplexDeletion the same
        // way.
        ColumnMetadata probeComplexColumn = null;
        while (!anyDead)
        {
            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (sstableCursors[i].state() == CELL_HEADER_START)
                    sstableCursors[i].readCellHeader();
            }
            cellMergeLimit = prepareAndSortCellsForMerge(rowMergeLimit, cellMergeLimit);
            if (cellMergeLimit == 0)
                break;

            // BTreeRow.minDeletionTime(ComplexColumnData) always folds in the column's own
            // deletion, above what its cells contribute. A non-live complex deletion gives
            // Long.MIN_VALUE even when the cells below it are live. This test therefore decides
            // the group on its own, and it needs no cell state. It is also the only safe test
            // when the position produced no cell: see below.
            SSTableCursorReader.CellCursor leadCellCursor = sstableCursors[0].cellCursor();
            boolean complexDeletionDead = false;
            if (leadCellCursor.cellColumn.isComplex())
            {
                if (!ColumnMetadata.sameName(probeComplexColumn, leadCellCursor.cellColumn))
                {
                    probeComplexColumn = leadCellCursor.cellColumn;
                    foldAndClampComplexDeletion(rowMergeLimit, probeComplexColumn, rowActiveDeletion, probeComplexDeletion);
                }
                complexDeletionDead = !probeComplexDeletion.isLive();
            }
            if (complexDeletionDead)
            {
                anyDead = true;
            }
            else if (leadCellCursor.producedCell)
            {
                // The column is simple, or it is complex with a live deletion. Its cells alone
                // decide it. The producedCell test guards the read: a deletion-only position
                // (see mergeCells) has no valid cell fields, because cellLiveness still holds the
                // values of an earlier cell. A live complex deletion above zero cells has nothing
                // left to decide.
                ReusableCellLivenessInfo winner = leadCellCursor.cellLiveness;
                for (int i = 1; i < cellMergeLimit; i++)
                {
                    ReusableCellLivenessInfo challenger = sstableCursors[i].cellCursor().cellLiveness;
                    if (CellLivenessInfo.resolve(winner, challenger) == RIGHT)
                        winner = challenger;
                }
                anyDead = !rowActiveDeletion.deletesCellAt(winner.timestamp()) && !winner.isLive(nowInSec);
            }

            for (int i = 0; i < cellMergeLimit; i++)
            {
                if (sstableCursors[i].state() == CELL_VALUE_START)
                    sstableCursors[i].skipCellValue();
            }
            continueReadingAfterMerge(cellMergeLimit, CELL_END);
        }

        System.arraycopy(probeCursorOrder, 0, sstableCursors, 0, rowMergeLimit);
        System.arraycopy(probeEqualsNext, 0, sstableCursorsEqualsNext, 0, rowMergeLimit);
        for (int i = 0; i < rowMergeLimit; i++)
        {
            // rewindRowCells restores the walk to the row's FIRST cell, so a cursor recorded mid-row
            // would re-present its earlier cells to the merge.
            int recordedState = probeCursorState[i];
            assert recordedState == CELL_HEADER_START || recordedState == UNFILTERED_END
                 : "unexpected merge-group state before the strict-liveness probe: " + sstableCursors[i];
            if (recordedState == CELL_HEADER_START)
                sstableCursors[i].rewindRowCells(isStatic);
        }
        return anyDead;
    }

    // current output complex column state (reset per row)
    private ColumnMetadata currentComplexColumn;
    private boolean complexColumnStarted;
    private final DeletionTime.ReusableDeletionTime mergedComplexDeletion = DeletionTime.ReusableDeletionTime.live();
    // The same merged deletion, but for cell-drop decisions. It is separate because the two roles
    // differ: the output drops a purgeable deletion, but that deletion must still delete the older
    // cells below it. The iterator does the same. It applies the un-purged deletion at merge time
    // (Row.Merger.ColumnDataReducer) and purges it only afterwards (ComplexColumnData.purge).
    private final DeletionTime.ReusableDeletionTime shadowComplexDeletion = DeletionTime.ReusableDeletionTime.live();

    private DataOutputBuffer tempCellBuffer1 = new DataOutputBuffer();
    private DataOutputBuffer tempCellBuffer2 = new DataOutputBuffer();
    // Fallback transfer buffer for cell-content copies. The usual path reads directly into the
    // target DataOutputBuffer array: see SSTableCursorReader.copyCellContents.
    private final byte[] copyColumnValueBuffer = new byte[4096];

    // Counter merge state (increment 5): garbage-free CounterContext operations plus the
    // fold/staging buffers and the output cell's liveness. fold/temp swap on
    // RIGHT_SUPERSET, hence non-final.
    private final CursorCounterContexts counterContexts = new CursorCounterContexts();
    private DataOutputBuffer counterFoldBuffer = new DataOutputBuffer();
    private DataOutputBuffer counterTempBuffer = new DataOutputBuffer();
    private final DataOutputBuffer counterWireBuffer = new DataOutputBuffer();
    private final ReusableCellLivenessInfo counterLiveness = new ReusableCellLivenessInfo();

    /**
     * Computes the complex deletion of {@code column} across every source that holds it, and
     * clamps the result to {@code activeDeletion}. The result goes into {@code scratch}.
     *
     * {@link #mergeCells} calls this before it uses the result to shadow the column's older cells.
     * {@link #anyMergedCellDeadAtNow} calls it to ask the same question: does the merged row hold
     * a non-live complex deletion for this column? One method computes the fold for both callers,
     * so the two answers cannot disagree.
     *
     * This method does not purge the result. Both callers need the un-purged value:
     * <ul>
     *   <li>a deletion that the output drops as purged must still shadow an older cell;</li>
     *   <li>the strict-liveness guard asks about the row as {@link Row.Merger#merge} leaves it,
     *       which is after the clamp and before the purge.</li>
     * </ul>
     */
    private void foldAndClampComplexDeletion(int rowMergeLimit, ColumnMetadata column, DeletionTime activeDeletion,
                                             DeletionTime.ReusableDeletionTime scratch)
    {
        scratch.resetLive();
        for (int i = 0; i < rowMergeLimit; i++)
        {
            StatefulCursor c = sstableCursors[i];
            if (isState(c.state(), CELL_VALUE_START | CELL_END)
                && ColumnMetadata.sameName(c.cellCursor().cellColumn, column))
            {
                DeletionTime d = c.cellCursor().complexDeletion;
                if (d.supersedes(scratch))
                    scratch.reset(d);
            }
        }
        // The keep-condition of ColumnDataReducer: a deletion survives only if it supersedes the
        // active deletion. Every real deletion supersedes LIVE.
        if (!scratch.supersedes(activeDeletion))
            scratch.resetLive();
    }

    /**
     * Starts a new output complex column. This method folds the merged deletion of the column and
     * keeps the un-purged copy that shadows the older cells.
     *
     * If the merged deletion survives, this method opens the column now, because the output must
     * carry a surviving deletion even when no cell survives. If the deletion is live, the column
     * opens later, at its first surviving cell. A column with no surviving cell and no deletion
     * writes nothing, in the same way that the iterator drops an empty
     * {@code ComplexColumnData}.
     *
     * @return the new value of {@code isRowDropped}
     */
    private boolean startNewComplexColumn(int rowMergeLimit, ColumnMetadata column, DeletionTime activeDeletion,
                                          boolean isRowDropped, boolean isStatic) throws IOException
    {
        currentComplexColumn = column;
        complexColumnStarted = false;
        // The fold clamps against the active row, partition or range deletion. Equal deletions do
        // not survive that clamp: a row delete and a column delete can share a timestamp and a
        // second, and ColumnDataReducer then drops the complex deletion. Keeping it here would
        // write a spurious HAS_COMPLEX_DELETION flag and spurious deletion bytes.
        foldAndClampComplexDeletion(rowMergeLimit, currentComplexColumn, activeDeletion, mergedComplexDeletion);
        // The deletion purges like any other tombstone, but only in the output. It must still
        // shadow the older cells of this column during the merge: see shadowComplexDeletion. A
        // purge before the shadow step would bring those cells back.
        shadowComplexDeletion.reset(mergedComplexDeletion);
        if (purger.shouldPurge(mergedComplexDeletion))
            mergedComplexDeletion.resetLive();
        if (!mergedComplexDeletion.isLive())
            isRowDropped = openRowAndComplexColumn(isRowDropped, isStatic, true);
        return isRowDropped;
    }

    /**
     * Opens the output row if this is its first surviving content. Then opens the current complex
     * column if it is not open.
     *
     * @return the new value of {@code isRowDropped}
     */
    private boolean openRowAndComplexColumn(boolean isRowDropped, boolean isStatic, boolean isComplexColumn) throws IOException
    {
        if (isRowDropped)
        {
            isRowDropped = false;
            lateStartRow(isStatic);
        }
        if (isComplexColumn && !complexColumnStarted)
        {
            ssTableCursorWriter.startComplexColumn(currentComplexColumn, mergedComplexDeletion);
            complexColumnStarted = true;
        }
        return isRowDropped;
    }

    /**
     * {@link Row.Merger.ColumnDataReducer#getReduced()} <-- applied the delete before reconcile, should not make a difference?
     * {@link Cells#reconcile(Cell, Cell)}
     */
    private boolean mergeCells(int rowMergeLimit, int cellMergeLimit, DeletionTime activeDeletion, LivenessInfo rowLiveness, boolean isRowDropped, boolean isStatic) throws IOException
    {
        cellMergeCounters[cellMergeLimit - 1]++;
        // Nothing to sort, we basically need to pick the correct data to copy.
        // -> the latest data.
        StatefulCursor cellSource = sstableCursors[0];
        SSTableCursorReader.CellCursor cellCursor = cellSource.cellCursor();
        ReusableCellLivenessInfo cellLiveness = cellCursor.cellLiveness;
        DataOutputBuffer tempCellBuffer = null;

        if (cellCursor.cellColumn.isCounterColumn())
            return mergeCounterCells(cellMergeLimit, activeDeletion, rowLiveness, isRowDropped, isStatic);

        // All cells in this group have the same column, because the group is the merge minimum.
        // The winner changes below, but the column does not.
        final boolean isComplexColumn = cellCursor.cellColumn.isComplex();

        DeletionTime effectiveDeletion = activeDeletion;
        if (isComplexColumn)
        {
            // At a new complex column, every source that holds the column is positioned at it.
            // The streams are in column order, this column is the merge minimum, and a
            // deletion-only position sorts before the cells. The merged deletion is therefore
            // known before the first cell of the column is written.
            if (!ColumnMetadata.sameName(currentComplexColumn, cellCursor.cellColumn))
                isRowDropped = startNewComplexColumn(rowMergeLimit, cellCursor.cellColumn, activeDeletion, isRowDropped, isStatic);
            // The shadow deletion is non-live only if it superseded the active deletion at the fold.
            if (!shadowComplexDeletion.isLive())
                effectiveDeletion = shadowComplexDeletion;

            if (!cellCursor.producedCell)
            {
                // A deletion-only group. The fold above already used its deletion.
                return isRowDropped;
            }
        }

        /** See: {@link Cells#reconcile(Cell, Cell)} */
        // CellLivenessInfo.resolve makes the whole liveness decision; COMPARE means it defers to
        // the value comparison below. Unlike Cells.resolveRegular, this call site needs no
        // narrowing: ReusableCellLivenessInfo has no subclasses, so the liveness accessors
        // already bind from one type.
        for (int i = 1; i < cellMergeLimit; i++)
        {
            StatefulCursor oCellSource = sstableCursors[i];
            SSTableCursorReader.CellCursor oCellCursor = oCellSource.cellCursor();
            ReusableCellLivenessInfo oCellLiveness = oCellCursor.cellLiveness;

            Resolution cellResolution = CellLivenessInfo.resolve(cellLiveness, oCellLiveness);
            if (cellResolution == LEFT) {
                if (oCellSource.state() == CELL_VALUE_START) oCellSource.skipCellValue();
            }
            else if (cellResolution == RIGHT) {
                if (cellSource.state() == CELL_VALUE_START) cellSource.skipCellValue();
                cellSource = oCellSource;
                cellCursor = oCellCursor;
                cellLiveness = oCellLiveness;
                tempCellBuffer = null;
            }
            else { // COMPARE
                if (effectiveDeletion.deletesCellAt(oCellLiveness.timestamp())) {
                    if (oCellSource.state() == CELL_VALUE_START) oCellSource.skipCellValue();
                }
                else {
                    // copy out the values for comparison
                    if (cellSource.state() == CELL_VALUE_START)
                    {
                        if (tempCellBuffer != null)
                            throw new IllegalStateException("tempCellBuffer should be null if cellSource has a value to be read.");
                        tempCellBuffer1.clear();

                        cellSource.copyCellValue(tempCellBuffer1, copyColumnValueBuffer);

                        tempCellBuffer = tempCellBuffer1; // assume cell1 is going to be bigger
                    }
                    else if (tempCellBuffer == null) {
                        // potential trash value in buffer1
                        tempCellBuffer1.clear();
                    }
                    else if (tempCellBuffer != tempCellBuffer1) {
                        throw new IllegalStateException("tempCellBuffer should be tempCellBuffer1 if cellSource has been read.");
                    }
                    tempCellBuffer2.clear();
                    if (oCellSource.state() == CELL_VALUE_START)
                        oCellSource.copyCellValue(tempCellBuffer2, copyColumnValueBuffer);

                    // The current winner keeps ties, as Cells.resolveRegular does. These buffers
                    // hold the wire form: a variable-length type prefixes its value with a length
                    // vint, and the reference compares the raw value bytes. Skip the vint, or a
                    // lexicographic compare orders by length first.
                    int skip1 = 0, skip2 = 0;
                    if (cellCursor.cellType.valueLengthIfFixed() < 0)
                    {
                        skip1 = tempCellBuffer1.getLength() == 0 ? 0 : wireVintSize(tempCellBuffer1.getData()[0]);
                        skip2 = tempCellBuffer2.getLength() == 0 ? 0 : wireVintSize(tempCellBuffer2.getData()[0]);
                    }
                    int compare = Arrays.compareUnsigned(tempCellBuffer1.getData(), skip1, tempCellBuffer1.getLength(),
                                                         tempCellBuffer2.getData(), skip2, tempCellBuffer2.getLength());
                    if (compare < 0) {
                        // challenger wins: swap buffers so tempCellBuffer1 holds the winner's value
                        tempCellBuffer = tempCellBuffer1;
                        tempCellBuffer1 = tempCellBuffer2;
                        tempCellBuffer2 = tempCellBuffer;

                        // tempCellBuffer != null -> tempCellBuffer == tempCellBuffer1
                        tempCellBuffer = tempCellBuffer1;

                        cellSource = oCellSource;
                        cellCursor = oCellCursor;
                        cellLiveness = oCellLiveness;
                    }
                }
            }
        }


        /**
         * {@link Cell.Serializer#serialize}
         */
        int cellFlags = cellCursor.cellFlags;

        // Pre-purge cell-tombstone count, matching TombstoneCounter (which counts c.isTombstone()
        // over the merged row's cells before the Purger runs). Must be evaluated on the winning
        // cell's ORIGINAL liveness - before the ttl-to-tombstone conversion just below, which would
        // otherwise make an expired-but-not-yet-purged expiring cell look like a tombstone here even
        // though legacy still sees it as expiring at TombstoneCounter time. Count only cells that
        // survive shadowing by the active/complex deletion (Row.Merger drops shadowed cells before
        // TombstoneCounter), and exclude static rows.
        if (topPartitionCollector != null && !isStatic
            && !effectiveDeletion.deletesCellAt(cellLiveness.timestamp()) && cellLiveness.isTombstone())
            partitionTombstoneCount++;

        /** {@link org.apache.cassandra.db.rows.AbstractCell#purge(org.apache.cassandra.db.DeletionPurger, long)} */
        // if `isExpiring` => has ttl, and TTL has lapsed, convert the TTL to a tombstone
        if (Cell.Serializer.isExpiring(cellFlags) && cellLiveness.isExpired(nowInSec)) {
            cellLiveness.ttlToTombstone();
            // remove the value, this is a tombstone now
            if (Cell.Serializer.hasValue(cellFlags))
            {
                cellFlags = cellFlags | Cell.Serializer.HAS_EMPTY_VALUE_MASK;
                if (cellSource.state() == CELL_VALUE_START)
                {
                    if (tempCellBuffer != null) throw new IllegalStateException("Either copied buffer or ready to copy reader, not both.");
                    cellSource.skipCellValue();
                }
                else if (tempCellBuffer != null) {
                    tempCellBuffer = null;
                }
                else
                {
                    throw new IllegalStateException("Flags and state contradict");
                }
            }
        }

        if (effectiveDeletion.deletesCellAt(cellLiveness.timestamp()) || purger.shouldPurge(cellLiveness, nowInSec))
        {
            if (Cell.Serializer.hasValue(cellFlags))
            {
                // we're dropping the cell, but could do: cellFlags = cellFlags | Cell.Serializer.HAS_EMPTY_VALUE_MASK;
                if (cellSource.state() == CELL_VALUE_START)
                {
                    if (tempCellBuffer != null) throw new IllegalStateException("Either copied buffer or ready to copy reader, not both.");
                    cellSource.skipCellValue();
                }
                else if (tempCellBuffer != null) {
                    // we're dropping the cell, but could do: tempCellBuffer = null;
                }
                else
                {
                    throw new IllegalStateException("Flags and state contradict");
                }
            }
        }
        else
        {
            isRowDropped = openRowAndComplexColumn(isRowDropped, isStatic, isComplexColumn);
            /** {@link org.apache.cassandra.db.rows.Cell.Serializer#serialize(Cell, ColumnMetadata, DataOutputPlus, LivenessInfo, org.apache.cassandra.db.SerializationHeader)} */
            boolean isDeleted = cellLiveness.isTombstone();
            // Cell.Serializer treats deleted/expiring as mutually exclusive (else-if below), so a
            // tombstone must never carry IS_EXPIRING or a TTL field.
            boolean isExpiring = cellLiveness.isExpiring();
            boolean useRowTimestamp = !rowLiveness.isEmpty() && cellLiveness.timestamp() == rowLiveness.timestamp();
            boolean useRowTTL = isExpiring && rowLiveness.isExpiring() &&
                                cellLiveness.ttl() == rowLiveness.ttl() &&
                                cellLiveness.localDeletionTime() == rowLiveness.localExpirationTime();
            // Re-write cell flags to reflect resulting contents
            cellFlags &= Cell.Serializer.HAS_EMPTY_VALUE_MASK;
            if (isDeleted) cellFlags |= Cell.Serializer.IS_DELETED_MASK;
            else if (isExpiring) cellFlags |= Cell.Serializer.IS_EXPIRING_MASK;
            if (useRowTimestamp) cellFlags |= Cell.Serializer.USE_ROW_TIMESTAMP_MASK;
            if (useRowTTL) cellFlags |= Cell.Serializer.USE_ROW_TTL_MASK;
            // cellCursor always follows cellSource, so this column comes from the winning source.
            // The name test in writeCellHeader needs that instance.
            ssTableCursorWriter.writeCellHeader(cellFlags, cellLiveness, cellCursor.cellColumn);
            if (isComplexColumn)
                ssTableCursorWriter.writeCellPath(cellCursor.cellPathBuffer, cellCursor.cellPathLength);
            if (Cell.Serializer.hasValue(cellFlags)) {
                if (cellSource.state() == CELL_VALUE_START)
                {
                    if (tempCellBuffer != null) throw new IllegalStateException("Either copied buffer or ready to copy reader, not both.");
                    ssTableCursorWriter.writeCellValue(cellSource, copyColumnValueBuffer);
                }
                else if (tempCellBuffer != null)
                {
                    ssTableCursorWriter.writeCellValue(tempCellBuffer);
                }
                else
                {
                    throw new IllegalStateException("Flags and state contradict");
                }
            }

        }
        return isRowDropped;
    }

    /**
     * Counter cell merge — mirrors the iterator's decision table exactly:
     * {@link org.apache.cassandra.db.rows.Cells#reconcile} routes to resolveCounter when
     * either cell is a LIVE counter cell (counter tombstones reconcile as regular cells),
     * and resolveCounter implements:
     * <ul>
     *   <li>a tombstone beats a live counter cell REGARDLESS of timestamps
     *       (CASSANDRA-7346);</li>
     *   <li>live + live MERGE their contexts (CounterContext.merge — here the pinned
     *       garbage-free mirror {@link CursorCounterContexts}); the resulting timestamp is
     *       the max of the contributors; ttl and localDeletionTime are NONE.</li>
     * </ul>
     * Two iterator behaviors that are invisible for regular cells are load-bearing here:
     * <ul>
     *   <li>every INPUT cell is tested against the active deletion BEFORE reconciliation
     *       (Row.Merger.CellReducer) — a shadowed cell's shards must not pollute the merged
     *       context, and a shadowed tombstone must not exercise 7346 supremacy;</li>
     *   <li>counter values are deserialized with Flag.LOCAL, which clears marked local
     *       shards (DeserializationHelper.maybeClearCounterValue) — so every counter value,
     *       merged or passed through, runs the clear transform.</li>
     * </ul>
     */
    private boolean mergeCounterCells(int cellMergeLimit, DeletionTime activeDeletion, LivenessInfo rowLiveness,
                                      boolean isRowDropped, boolean isStatic) throws IOException
    {
        ColumnMetadata column = sstableCursors[0].cellCursor().cellColumn;
        // tombstone fold: the surviving-tombstone winner's liveness/flags (references into
        // the owning cursor's reusables — stable until that cursor reads its next cell)
        ReusableCellLivenessInfo tombstoneLiveness = null;
        int tombstoneFlags = 0;
        // live fold: merged raw context (no vint) accumulates in counterFoldBuffer
        boolean haveLive = false;
        long liveTimestamp = Long.MIN_VALUE;

        for (int i = 0; i < cellMergeLimit; i++)
        {
            StatefulCursor source = sstableCursors[i];
            SSTableCursorReader.CellCursor cc = source.cellCursor();
            ReusableCellLivenessInfo liveness = cc.cellLiveness;

            if (activeDeletion.deletesCellAt(liveness.timestamp()))
            {
                if (source.state() == CELL_VALUE_START) source.skipCellValue();
                continue;
            }

            if (liveness.isTombstone())
            {
                // multiple tombstones resolve like regular cells: a counter tombstone is not
                // a counter cell (AbstractCell.isCounterCell), so the iterator routes the
                // pair through Cells.resolveRegular — higher ts, the greater-ldt tie-break,
                // and on a full tie the greater RAW value bytes (compareValues tail)
                Resolution resolution = tombstoneLiveness == null
                                        ? RIGHT : CellLivenessInfo.resolve(tombstoneLiveness, liveness);
                if (resolution == RIGHT)
                {
                    tombstoneLiveness = liveness;
                    tombstoneFlags = cc.cellFlags;
                    // counter tombstones normally carry no value, but the serializers
                    // preserve one faithfully when present (hasValue is recomputed from the
                    // bytes, Cell.Serializer.serialize) — dropping it while the flags still
                    // claim it corrupts the row; keep the winner's wire value (vint + bytes)
                    tempCellBuffer1.clear();
                    if (source.state() == CELL_VALUE_START)
                        source.copyCellValue(tempCellBuffer1, copyColumnValueBuffer);
                }
                else if (resolution == COMPARE)
                {
                    // full (ts, ldt) tie: Cells.resolveRegular ends at
                    // compareValues(left, right) >= 0 ? left : right over the RAW value
                    // bytes. tempCellBuffer1 holds the current winner's WIRE value; counter
                    // values are variable-length, so skip the leading length vint on both
                    // sides (comparing it would order by length first, not content)
                    tempCellBuffer2.clear();
                    if (source.state() == CELL_VALUE_START)
                        source.copyCellValue(tempCellBuffer2, copyColumnValueBuffer);
                    int skip1 = tempCellBuffer1.getLength() == 0 ? 0 : wireVintSize(tempCellBuffer1.getData()[0]);
                    int skip2 = tempCellBuffer2.getLength() == 0 ? 0 : wireVintSize(tempCellBuffer2.getData()[0]);
                    int compare = Arrays.compareUnsigned(tempCellBuffer1.getData(), skip1, tempCellBuffer1.getLength(),
                                                         tempCellBuffer2.getData(), skip2, tempCellBuffer2.getLength());
                    if (compare < 0)
                    {
                        // challenger wins: its wire value is already in tempCellBuffer2 — swap
                        DataOutputBuffer swap = tempCellBuffer1;
                        tempCellBuffer1 = tempCellBuffer2;
                        tempCellBuffer2 = swap;
                        tombstoneLiveness = liveness;
                        tombstoneFlags = cc.cellFlags;
                    }
                }
                else if (source.state() == CELL_VALUE_START)
                {
                    source.skipCellValue();
                }
                continue;
            }

            if (!haveLive)
            {
                copyCounterContext(source, counterFoldBuffer);
                haveLive = true;
                liveTimestamp = liveness.timestamp();
            }
            else
            {
                copyCounterContext(source, counterTempBuffer);
                CursorCounterContexts.MergeResult result =
                    counterContexts.merge(counterFoldBuffer.getData(), 0, counterFoldBuffer.getLength(),
                                          counterTempBuffer.getData(), 0, counterTempBuffer.getLength());
                if (result == CursorCounterContexts.MergeResult.RIGHT_SUPERSET)
                {
                    DataOutputBuffer swap = counterFoldBuffer;
                    counterFoldBuffer = counterTempBuffer;
                    counterTempBuffer = swap;
                }
                else if (result == CursorCounterContexts.MergeResult.MERGED)
                {
                    counterFoldBuffer.clear();
                    counterFoldBuffer.write(counterContexts.scratchBuffer(), 0, counterContexts.scratchLength());
                }
                // LEFT_SUPERSET: the fold buffer already holds the result
                liveTimestamp = Math.max(liveTimestamp, liveness.timestamp());
            }
        }

        if (tombstoneLiveness != null)
        {
            // Pre-purge cell-tombstone count for a surviving counter tombstone. It is a cell
            // tombstone (AbstractCell.isTombstone) that survived shadowing by the active deletion
            // (shadowed inputs were skipped above), so TombstoneCounter would count it before the
            // Purger runs. Count before the shouldPurge drop below. Static rows excluded.
            if (topPartitionCollector != null && !isStatic)
                partitionTombstoneCount++;
            // 7346 supremacy: any surviving tombstone wins; merged live shards are discarded
            if (purger.shouldPurge(tombstoneLiveness, nowInSec))
                return isRowDropped;
            if (isRowDropped)
            {
                isRowDropped = false;
                lateStartRow(isStatic);
            }
            boolean useRowTimestamp = !rowLiveness.isEmpty() && tombstoneLiveness.timestamp() == rowLiveness.timestamp();
            int cellFlags = (tombstoneFlags & Cell.Serializer.HAS_EMPTY_VALUE_MASK) | Cell.Serializer.IS_DELETED_MASK;
            if (useRowTimestamp) cellFlags |= Cell.Serializer.USE_ROW_TIMESTAMP_MASK;
            ssTableCursorWriter.writeCellHeader(cellFlags, tombstoneLiveness, column);
            if (Cell.Serializer.hasValue(cellFlags))
                ssTableCursorWriter.writeCellValue(tempCellBuffer1);
            return isRowDropped;
        }

        if (!haveLive)
            return isRowDropped; // every input was shadowed by the active deletion

        if (isRowDropped)
        {
            isRowDropped = false;
            lateStartRow(isStatic);
        }
        // Cells.resolveCounter: ts = max of contributors, no ttl, no deletion time
        counterLiveness.reset(liveTimestamp, LivenessInfo.NO_TTL, LivenessInfo.NO_EXPIRATION_TIME);
        boolean useRowTimestamp = !rowLiveness.isEmpty() && liveTimestamp == rowLiveness.timestamp();
        int cellFlags = useRowTimestamp ? Cell.Serializer.USE_ROW_TIMESTAMP_MASK : 0;
        ssTableCursorWriter.writeCellHeader(cellFlags, counterLiveness, column);
        ssTableCursorWriter.writeCellValue(counterFoldBuffer.getData(), 0, counterFoldBuffer.getLength());
        // Cells.collectStats parity: updateHasLegacyCounterShards per live output counter cell
        ssTableCursorWriter.updateCounterShardStats(
            CursorCounterContexts.hasLegacyShards(counterFoldBuffer.getData(), 0, counterFoldBuffer.getLength()));
        return isRowDropped;
    }

    /**
     * Copies the source's counter value (wire form: vint length + context bytes) into dst
     * as RAW context bytes (no vint), applying the deserialization-time marked-local clear
     * the iterator path gets from Flag.LOCAL.
     */
    private void copyCounterContext(StatefulCursor source, DataOutputBuffer dst) throws IOException
    {
        counterWireBuffer.clear();
        source.copyCellValue(counterWireBuffer, copyColumnValueBuffer);
        byte[] wire = counterWireBuffer.getData();
        int vintSize = wireVintSize(wire[0]);
        int contextLength = counterWireBuffer.getLength() - vintSize;
        dst.clear();
        int cleared = counterContexts.clearMarkedLocal(wire, vintSize, contextLength);
        if (cleared >= 0)
            dst.write(counterContexts.scratchBuffer(), 0, cleared);
        else
            dst.write(wire, vintSize, contextLength);
    }

    /**
     * Byte length of the leading unsigned vint in a wire-form variable-length value:
     * non-negative first byte = single-byte vint (VIntCoding's own callers guard the same
     * way before consulting numberOfExtraBytesToRead, which expects the SIGNED byte).
     */
    private static int wireVintSize(byte firstByte)
    {
        return firstByte >= 0 ? 1
               : 1 + org.apache.cassandra.utils.vint.VIntCoding.numberOfExtraBytesToRead(firstByte);
    }

    DeletionTime activeOpenRangeDeletion = DeletionTime.LIVE;
    final List<ReusableDeletionTime> openMarkers = new ArrayList<>();
    final ArrayDeque<ReusableDeletionTime> reusableMarkersPool = new ArrayDeque<>();

    /**
     * We have a common clustering and need to merge tombstones. Alternatively, we have a series of range tombstones
     * whose intersections mutate from bounds into boundary (a combination of 2 bounds). We also need to purge any GC'ed
     * deletes.
     *
     * {@link RangeTombstoneMarker.Merger#merge()}
     *
     * @return true if written, false otherwise
     */
    private boolean mergeRangeTombstones(int rangeTombstoneMergeLimit, DeletionTime partitionDeletion, boolean isFirstUnfiltered) throws IOException
    {
        if (rangeTombstoneMergeLimit == 0)
        {
            throw new IllegalStateException();
        }
        rangeTombstonesMergeCounters[rangeTombstoneMergeLimit - 1]++;
        DeletionTime previousDeletionTimeInMerged = DeletionTime.LIVE;
        if (activeOpenRangeDeletion != DeletionTime.LIVE) {
            previousDeletionTimeInMerged = getDeletionTimeReusableCopy(activeOpenRangeDeletion);
        }
        try
        {
            updateOpenMarkers(rangeTombstoneMergeLimit, partitionDeletion);

            DeletionTime newDeletionTimeInMerged = activeOpenRangeDeletion;
            if (previousDeletionTimeInMerged.equals(newDeletionTimeInMerged))
                return false;

            // Past the equals() check the merge produces exactly one RangeTombstoneMarker at this
            // clustering (a bound or a single boundary object) - so count one, matching
            // TombstoneCounter.applyToMarker, which fires once per merged marker before the Purger
            // may drop or split it below.
            if (topPartitionCollector != null)
                partitionTombstoneCount++;

            // we will stomp on the unfiltered descriptor and write it out
            UnfilteredDescriptor rangeTombstone = sstableCursors[0].unfiltered();
            boolean isBeforeClustering = rangeTombstone.clusteringKind().comparedToClustering < 0;

            // Combining the merge and purge code
            if (previousDeletionTimeInMerged == DeletionTime.LIVE)
            {
                if (purger.shouldPurge(newDeletionTimeInMerged))
                {
                    return false;
                }
                else
                {
                    rangeTombstone.clusteringKind(isBeforeClustering ? INCL_START_BOUND : EXCL_START_BOUND);
                    rangeTombstone.deletionTime().reset(newDeletionTimeInMerged);
                }
            }
            else if (newDeletionTimeInMerged == DeletionTime.LIVE)
            {
                if (purger.shouldPurge(previousDeletionTimeInMerged))
                {
                    return false;
                }
                else
                {
                    rangeTombstone.clusteringKind(isBeforeClustering ? EXCL_END_BOUND : INCL_END_BOUND);
                    rangeTombstone.deletionTime().reset(previousDeletionTimeInMerged);
                }
            }
            else
            {
                boolean shouldPurgeClose = purger.shouldPurge(previousDeletionTimeInMerged);
                boolean shouldPurgeOpen = purger.shouldPurge(newDeletionTimeInMerged);

                if (shouldPurgeClose && shouldPurgeOpen)
                    return false;

                if (shouldPurgeClose)
                {
                    rangeTombstone.clusteringKind(isBeforeClustering ? INCL_START_BOUND : EXCL_START_BOUND);
                    rangeTombstone.deletionTime().reset(newDeletionTimeInMerged);
                }
                else if (shouldPurgeOpen)
                {
                    rangeTombstone.clusteringKind(isBeforeClustering ? EXCL_END_BOUND : INCL_END_BOUND);
                    rangeTombstone.deletionTime().reset(previousDeletionTimeInMerged);
                }
                else {
                    // Boundary
                    rangeTombstone.clusteringKind(isBeforeClustering ? EXCL_END_INCL_START_BOUNDARY : INCL_END_EXCL_START_BOUNDARY);
                    rangeTombstone.deletionTime().reset(previousDeletionTimeInMerged); // close
                    rangeTombstone.deletionTime2().reset(newDeletionTimeInMerged); // open
                }
            }

            if (isPartitionStartDelayed())
            {
                lateStartPartition(false);
                ssTableCursorWriter.writeRangeTombstone(rangeTombstone, true);
            }
            else {
                ssTableCursorWriter.writeRangeTombstone(rangeTombstone, isFirstUnfiltered);
            }
            return true;
        }
        finally
        {
            if (previousDeletionTimeInMerged != DeletionTime.LIVE)
            {
                reusableMarkersPool.offer((ReusableDeletionTime) previousDeletionTimeInMerged);
            }
        }
    }

    private void updateOpenMarkers(int rangeTombstoneMergeLimit, DeletionTime partitionDeletion)
    {
        /** Similar to {@link RangeTombstoneMarker.Merger#updateOpenMarkers()} but we validate a close exists for every open.*/
        for (int i = 0; i < rangeTombstoneMergeLimit; i++)
        {
            UnfilteredDescriptor rangeTombstone = sstableCursors[i].unfiltered();
            if (rangeTombstone.isStartBound())
            {
                DeletionTime openRangeDeletion = rangeTombstone.deletionTime();
                addOpenRangeDeletion(partitionDeletion, openRangeDeletion);
            }
            else if (rangeTombstone.isEndBound())
            {
                DeletionTime closeRangeDeletion = rangeTombstone.deletionTime();
                removeOpenRangeDeletion(partitionDeletion, closeRangeDeletion, rangeTombstone);
            }
            else if (rangeTombstone.isBoundary())
            {
                DeletionTime closeRangeDeletion = rangeTombstone.deletionTime();
                removeOpenRangeDeletion(partitionDeletion, closeRangeDeletion, rangeTombstone);
                DeletionTime openRangeDeletion = rangeTombstone.deletionTime2();
                addOpenRangeDeletion(partitionDeletion, openRangeDeletion);
            }
            else
                throw new IllegalStateException("Unexpected bound type:" + rangeTombstone.clusteringKind());
        }

        if (activeOpenRangeDeletion == null)
        {
            recalculateActiveOpen();
        }
    }

    private void recalculateActiveOpen()
    {
        // active open has been invalidated by a close bound matching it, need to scan the list for new max
        int size = openMarkers.size();
        if (size == 0)
        {
            activeOpenRangeDeletion = DeletionTime.LIVE;
            return;
        }
        // find max open marker
        DeletionTime maxOpenDeletion = openMarkers.get(0);
        for (int i = 1; i < size; i++)
        {
            DeletionTime openDeletionTime = openMarkers.get(i);
            if (openDeletionTime.supersedes(maxOpenDeletion))
                maxOpenDeletion = openDeletionTime;
        }
        activeOpenRangeDeletion = maxOpenDeletion;
    }

    private void removeOpenRangeDeletion(DeletionTime partitionDeletion, DeletionTime closeRangeDeletion, UnfilteredDescriptor rangeTombstone)
    {
        // filter out markers that are deleted by the `partitionDelete`
        if (partitionDeletion != DeletionTime.LIVE && !closeRangeDeletion.supersedes(partitionDeletion))
        {
            return;
        }
        // a close marker should have a matching open in the list
        int j = 0;
        int size = openMarkers.size();
        ReusableDeletionTime reusableOpenMarker = null;
        for (; j < size;j++) {
            reusableOpenMarker = openMarkers.get(j);
            if (reusableOpenMarker.equals(closeRangeDeletion))
                break;
        }
        if (j == size)
            throw new IllegalStateException("Expected an open marker for this closing marker:" + rangeTombstone);

        reusableMarkersPool.offer(reusableOpenMarker);
        if (activeOpenRangeDeletion == reusableOpenMarker) {
            // trigger recalculation
            activeOpenRangeDeletion = null;
        }
        if (size == 1) {
            openMarkers.clear();
        }
        else {
            // avoid expensive array copy, take the last element
            ReusableDeletionTime deletionTime = openMarkers.remove(size - 1);
            if (j != size - 1)
            {
                // overwrite the matched marker (if it was not the last one)
                openMarkers.set(j, deletionTime);
            }
        }
    }

    private void addOpenRangeDeletion(DeletionTime partitionDeletion, DeletionTime openRangeDeletion)
    {
        // filter out markers that are deleted by the `partitionDelete`
        if (partitionDeletion != DeletionTime.LIVE && !openRangeDeletion.supersedes(partitionDeletion))
        {
            return;
        }

        ReusableDeletionTime reusable = getDeletionTimeReusableCopy(openRangeDeletion);
        openMarkers.add(reusable);
        if (activeOpenRangeDeletion != null && // invalidated by remove, so full scan is required
            (activeOpenRangeDeletion == DeletionTime.LIVE || reusable.supersedes(activeOpenRangeDeletion))) {
            activeOpenRangeDeletion = reusable;
        }
    }

    private ReusableDeletionTime getDeletionTimeReusableCopy(DeletionTime openRangeDeletion)
    {
        ReusableDeletionTime reusable = reusableMarkersPool.pollLast();
        if (reusable == null) {
            reusable = ReusableDeletionTime.copy(openRangeDeletion);
        }
        else {
            reusable.reset(openRangeDeletion);
        }
        return reusable;
    }

    private boolean isPartitionStarted()
    {
        return partitionStarted;
    }

    private boolean isPartitionStartDelayed()
    {
        return !isPartitionStarted();
    }

    private void continueReadingAfterMerge(int mergeLimit, int endState)
    {
        for (int i = 0; i < mergeLimit; i++)
        {
            if (sstableCursors[i].state() == endState){
                sstableCursors[i].continueReading();
            }
        }
    }

    private void lateStartRow(boolean isStatic) throws IOException
    {
        lateStartRow(LivenessInfo.EMPTY, DeletionTime.LIVE, false, isStatic);
    }

    private void lateStartRow(LivenessInfo livenessInfo, DeletionTime deletionTime, boolean isShadowable, boolean isStatic) throws IOException
    {
        if (isPartitionStartDelayed())
        {
            lateStartPartition(isStatic);
        }
        ssTableCursorWriter.writeRowStart(livenessInfo, deletionTime, isShadowable, isStatic);
    }

    private void lateStartPartition(boolean isStatic) throws IOException
    {
        startPartition(DeletionTime.LIVE);
        // Did we miss writing an empty static row?
        if (!isStatic)
        {
            if(ssTableCursorWriter.writeEmptyStaticRow())
                partitionHeaderLength = (int) (ssTableCursorWriter.getPosition() - ssTableCursorWriter.getPartitionStart());
        }
    }

    private void finish()
    {
        // only finish writing once
        if (!finished)
        {
            finished = true;
            writerRollover();
        }
    }

    private void maybeSwitchWriter(OutputWriterProvider writerProvider)
    {
        assert !finished;
        // Set last key, so this is ready to be closed.
        SSTableWriter newWriter = writerProvider.maybeSwitchWriter(partitionDescriptor.key());
        if (newWriter != null)
        {
            writerRollover();

            ssTableCursorWriter = SSTableCursorWriter.forCompaction((SortedTableWriter) newWriter);
            ssTableCursorWriter.setFirst(partitionDescriptor.keyBuffer());
        }
        assert ssTableCursorWriter != null;
    }

    private void writerRollover()
    {
        if (ssTableCursorWriter != null) {
            totalDataBytesWritten += ssTableCursorWriter.getPosition();
            // lastWrittenKey() asserts hasWrittenPartition() (lastSource is still null) if no
            // partition was ever merged - reachable in practice via the read-only validation path
            // (mergeNextPartition), where a repair range can genuinely intersect zero partitions
            // even though the sstable itself is non-empty (see StatefulCursor's positionAt bounds).
            // Real (writing) compaction never hits this: an input sstable always has at least one
            // partition.
            if (lastSource != null || hasWrittenPartition())
                ssTableCursorWriter.setLast(lastWrittenKey().getKey());
        }
        ssTableCursorWriter = null;
    }

    private boolean hasWrittenPartition()
    {
        return lastWrittenPartition.keyLength() != 0;
    }

    private DecoratedKey lastWrittenKey()
    {
        assert hasWrittenPartition() : "no partition has been written yet";
        return lastWrittenPartition.key();
    }

    /**
     * Takes the steal owed to a cursor, if any. It waits a round because the descriptor only reaches
     * the prev slot on the next read. See {@link StatefulCursor#detachPrevPartition}.
     */
    private void takeOwedPartitionSteal()
    {
        if (lastWrittenPartitionSource == null)
            return;
        assert lastWrittenPartitionSource.partitionSwaps() == lastWrittenPartitionSourceSwaps + 1
             : "the partition steal is not one slot advance behind its write: now "
               + lastWrittenPartitionSource.partitionSwaps() + ", was " + lastWrittenPartitionSourceSwaps;
        lastWrittenPartition = lastWrittenPartitionSource.detachPrevPartition(lastWrittenPartition);
        lastWrittenPartitionSource = null;
    }

    /** Steals the descriptor of the unfiltered just written; see {@link StatefulCursor#detachUnfiltered}. */
    private void detachWrittenUnfiltered()
    {
        lastWrittenUnfiltered = sstableCursors[0].detachUnfiltered(lastWrittenUnfiltered);
    }

    /** The clustering of the last unfiltered written to the current output partition. */
    private ClusteringDescriptor lastWrittenClustering()
    {
        assert unfilteredsWrittenToPartition > 0 : "no unfiltered has been written to this partition";
        return lastWrittenUnfiltered;
    }

    // SORT AND COMPARE

    /**
     * Prepares the cursors array for partition merge.
     * <p>
     * The cursors are in one of 3 states:
     * <ul>
     *     <li>PARTITION_START - Partition header needs to be loaded in preparation for merge. This is the starting state of all cursors.</li>
     *     <li>STATIC_ROW_START | ROW_START | TOMBSTONE_START | PARTITION_END - header is loaded. Already sorted.</li>
     *     <li>DONE - Exhausted cursors. This is the end state of all cursors.</li>
     * </ul>
     * After each `mergePartitions` iteration, the recently progressed cursors are at the beginning of the array and are
     * either at a new PARTITION_START or DONE.
     * We prepare all the cursors in the PARTITION_START state for sorting by loading the key and delete time. We also
     * need to push all the DONE cursors to the back of the list.
     *
     * Once the bounds of the sorting are known we insert sort the freshly read/done cursors into the pre-sorted
     * remaining array. After the sort we find the next merge limit, which is to say how many of the top partition keys
     * are equal.
     *
     * @return the next merge limit, or 0 if all cursors are DONE
     */
    private int prepareAndSortForPartitionMerge() throws IOException
    {
        // start by loading in new partition keys from any readers for which we just merged partitions => are
        // on partition edge. Exhausted cursors are at the bottom. Mid-read partitions are in the middle.
        int perturbedCursors = 0;
        for (; perturbedCursors < sstableCursors.length; perturbedCursors++)
        {
            StatefulCursor sstableCursor = sstableCursors[perturbedCursors];
            int sstableCursorState = sstableCursor.state();

            if (sstableCursorState == PARTITION_START)
            {
                sstableCursor.readPartitionHeader();
                updateTotalBytesRead(sstableCursor);
            }
            else if (isState(sstableCursorState, STATIC_ROW_START | ROW_START | TOMBSTONE_START | PARTITION_END))
            {
                // The cursors after this point are sorted, and unmoved
                break;
            }
            else if (sstableCursorState == DONE)
            {
                if (sstableCursor.resetAfterDone())
                {
                    updateTotalBytesRead(sstableCursor);
                }
                else
                {
                    break;
                }
            }
            else
            {
                throw new IllegalStateException("Cursor is in an unexpected state:" + sstableCursor);
            }
        }
        // no cursors were moved => all done
        if (perturbedCursors == 0)
        {
            assert sstableCursors.length == 0 || sstableCursors[0].state() == DONE;
            return 0;
        }

        PARTITION_KEY_SORT.sortPerturbed(sstableCursors, sstableCursorsEqualsNext, perturbedCursors, sstableCursors.length);
        // top cursor is DONE -> all cursors are DONE
        int state = sstableCursors[0].state();
        if(state == DONE)
        {
            return 0;
        }
        assert isState(state, STATIC_ROW_START | ROW_START | TOMBSTONE_START | PARTITION_END);

        int partitionMergeLimit = 1;
        for (; partitionMergeLimit < sstableCursors.length; partitionMergeLimit++)
        {
            if (!sstableCursorsEqualsNext[partitionMergeLimit-1])
                break;
        }
        return partitionMergeLimit;
    }


    private int prepareAndSortUnfilteredForMerge(int partitionMergeLimit, int prevMergeLimit) throws IOException
    {
        // move cursors that need to move past the row header
        for (int i = 0; i < prevMergeLimit; i++)
        {
            StatefulCursor sstableCursor = sstableCursors[i];
            int readerState = sstableCursor.state();
            if (readerState == ROW_START)
            {
                totalSourceCQLRows++;
                sstableCursor.readRowHeader();
            }
            if (readerState == TOMBSTONE_START)
            {
                sstableCursor.readTombstoneMarker();
            }
            if (readerState == STATIC_ROW_START)
                throw new IllegalStateException("Unexpected static row after static row merge:" + sstableCursor);
        }

        // Sort rows by their clustering
        ROW_CLUSTERING_SORT.sortPerturbed(sstableCursors, sstableCursorsEqualsNext, prevMergeLimit, partitionMergeLimit);
        int state = sstableCursors[0].state();
        if (state == PARTITION_END)
        {
            return 0;
        }
        assert isState(state, UNFILTERED_END | CELL_HEADER_START);
        int unfilteredMergeLimit = 1;
        for (; unfilteredMergeLimit < partitionMergeLimit; unfilteredMergeLimit++)
        {
            if (!sstableCursorsEqualsNext[unfilteredMergeLimit-1])
                break;
        }
        return unfilteredMergeLimit;
    }

    private int prepareAndSortStaticForMerge(int partitionMergeLimit) throws IOException
    {
        STATIC_SORT.sortPerturbed(sstableCursors, sstableCursorsEqualsNext, partitionMergeLimit, partitionMergeLimit);
        int state = sstableCursors[0].state();
        if (state != STATIC_ROW_START)
        {
            assert isState(state, ROW_START|TOMBSTONE_START|PARTITION_END);
            return 0;
        }
        totalSourceCQLRows++;
        sstableCursors[0].readStaticRowHeader();
        int staticRowMergeLimit = 1;
        for (; staticRowMergeLimit < partitionMergeLimit; staticRowMergeLimit++)
        {
            if (sstableCursorsEqualsNext[staticRowMergeLimit - 1])
            {
                totalSourceCQLRows++;
                sstableCursors[staticRowMergeLimit].readStaticRowHeader();
            }
            else
                break;
        }

        return staticRowMergeLimit;
    }

    private int prepareAndSortCellsForMerge(int rowMergeLimit, int prevCellMergeLimit)
    {
        COLUMN_SORT.sortPerturbed(sstableCursors, sstableCursorsEqualsNext, prevCellMergeLimit, rowMergeLimit);
        // next row/partition/done
        if (sstableCursors[0].state() == UNFILTERED_END)
            return 0;

        int state = sstableCursors[0].state();
        if (isState(state, UNFILTERED_END | CELL_HEADER_START))
            return 0;

        int cellMergeLimit = 1;
        for (; cellMergeLimit < rowMergeLimit; cellMergeLimit++)
        {
            if (!sstableCursorsEqualsNext[cellMergeLimit - 1])
                break;
        }
        return cellMergeLimit;
    }

    // One dedicated, separately-compiled sort per comparison kind (see PreSortedBubbleInsert's
    // javadoc): each singleton is only ever constructed with its own compareByXxx reference, so
    // its copy's comparator.compare() call site stays monomorphic/inlinable instead of megamorphic
    // across all 4 comparators sharing one call site.
    private static final PartitionKeyMergeSort<StatefulCursor> PARTITION_KEY_SORT =
        new PartitionKeyMergeSort<>(CursorCompactor::compareByPartitionKey);
    private static final RowClusteringMergeSort<StatefulCursor> ROW_CLUSTERING_SORT =
        new RowClusteringMergeSort<>(CursorCompactor::compareByRowClustering);
    private static final StaticMergeSort<StatefulCursor> STATIC_SORT =
        new StaticMergeSort<>(CursorCompactor::compareByStatic);
    private static final ColumnMergeSort<StatefulCursor> COLUMN_SORT =
        new ColumnMergeSort<>(CursorCompactor::compareByColumnAndPath);

    private static int compareByPartitionKey(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tint = c1.state();
        int oint = c2.state();
        if (tint == DONE && oint == DONE) return 0;
        if (tint == DONE) return 1;
        if (oint == DONE) return -1;
        return c1.currentKey().compareTo(c2.currentKey());
    }

    private static int compareByStatic(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tState = c1.state();
        int oState = c2.state();

        if (tState == PARTITION_END && oState == PARTITION_END) return 0;
        if (tState == PARTITION_END) return 1;
        if (oState == PARTITION_END) return -1;

        return -Boolean.compare(tState == STATIC_ROW_START, oState == STATIC_ROW_START);
    }

    private static int compareByRowClustering(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tState = c1.state();
        int oState = c2.state();

        if (tState == PARTITION_END && oState == PARTITION_END) return 0;
        if (tState == PARTITION_END) return 1;
        if (oState == PARTITION_END) return -1;
        // Either have cells, or an empty row
        boolean tIsAfterHeader = isState(tState, CELL_HEADER_START | UNFILTERED_END);
        boolean oIsAfterHeader = isState(oState, CELL_HEADER_START | UNFILTERED_END);
        if (tIsAfterHeader && oIsAfterHeader)
            return ClusteringComparator.compare(c1.unfiltered(), c2.unfiltered());
        else
            throw new IllegalStateException("We only sort through rows ready to be merged/copied. c1 = " + c1 + ", c2 = " + c2);
    }

    private static int compareByColumnAndPath(StatefulCursor c1, StatefulCursor c2)
    {
        if (c1 == c2) return 0;
        int tState = c1.state();
        int oState = c2.state();
        if (tState == UNFILTERED_END && oState == UNFILTERED_END) return 0;
        if (tState == UNFILTERED_END) return 1;
        if (oState == UNFILTERED_END) return -1;

        boolean tIsAfterHeader = isState(tState, CELL_VALUE_START | CELL_END);
        boolean oIsAfterHeader = isState(oState, CELL_VALUE_START | CELL_END);
        if (!(tIsAfterHeader && oIsAfterHeader))
            throw new IllegalStateException("We only sort through cells ready to be merged/copied. c1 = " + c1 + ", c2 = " + c2);

        SSTableCursorReader.CellCursor cc1 = c1.cellCursor();
        SSTableCursorReader.CellCursor cc2 = c2.cellCursor();
        int byColumn = cc1.cellColumn.compareTo(cc2.cellColumn);
        if (byColumn != 0 || !cc1.cellColumn.isComplex())
            return byColumn;
        // The two cursors are at the same complex column. A deletion-only position has no cell,
        // and sorts before every cell, so the deletion sources of the column come first.
        if (!cc1.producedCell || !cc2.producedCell)
            return Boolean.compare(cc1.producedCell, cc2.producedCell);
        // The cell cursor resolves cellPathType once per column. Pass it here, because this
        // comparator runs once per cell per source.
        return comparePaths(cc1.cellColumn, cc1.cellPathType, cc1.cellPathWindow(), cc2.cellPathWindow());
    }

    /**
     * Compares two cell paths of one complex column. The order must agree with
     * {@link ColumnMetadata#cellPathComparator()}, which sets both the cell order that flush
     * writes to disk and the merge grouping of the iterator.
     */
    @VisibleForTesting
    static int comparePaths(ColumnMetadata column, ByteBuffer p1, ByteBuffer p2)
    {
        return comparePaths(column, ColumnMetadata.pathNameComparator(column.type), p1, p2);
    }

    /**
     * @param pathType the type that compares the path bytes: see
     *                 {@link ColumnMetadata#pathNameComparator}. Map keys, set elements and list
     *                 timeuuids compare by their type, not as raw bytes.
     *                 {@link SSTableCursorReader.CellCursor} resolves this type once per column.
     */
    private static int comparePaths(ColumnMetadata column, AbstractType<?> pathType, ByteBuffer p1, ByteBuffer p2)
    {
        // Today only CollectionType and UserType are multi-cell, and pathNameComparator handles
        // both, so no column that reaches this comparator has a null path type. If a new
        // multi-cell type is added and is not handled, fail here. Do not fall back to raw byte
        // order, because that order can differ from the reference order.
        if (pathType == null)
            throw new IllegalStateException("No cell-path comparator for multi-cell type: " + column.type + " (column " + column.name + ")");
        return pathType.compare(p1, p2);
    }

    // Purge

    /**
     * We are combining code from:
     * - {@link org.apache.cassandra.db.compaction.CompactionIterator.Purger}
     * - {@link org.apache.cassandra.db.partitions.PurgeFunction}
     * - {@link DeletionPurger}
     * The original code leans on the {@link org.apache.cassandra.db.transform.Transformation} abstraction and the
     * iterator infrastructure which is not fit for purpose here.
     */
    static class Purger implements DeletionPurger
    {
        private final long oldestUnrepairedTombstone;
        private final boolean onlyPurgeRepairedTombstones;
        private final boolean shouldIgnoreGcGraceForAnyKey;
        private final OperationType type;

        private boolean ignoreGcGraceSeconds;
        private final AbstractCompactionController controller;

        private DecoratedKey partitionKey;
        private LongPredicate purgeEvaluator;

        private long compactedUnfiltered;

        Purger(OperationType type, AbstractCompactionController controller)
        {
            oldestUnrepairedTombstone = controller.compactingRepaired() ? Long.MAX_VALUE : Integer.MIN_VALUE;
            onlyPurgeRepairedTombstones = controller.cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones();
            shouldIgnoreGcGraceForAnyKey = controller.cfs.shouldIgnoreGcGraceForAnyKey();
            this.controller = controller;
            this.type = type;
        }

        void resetOnNewPartition(DecoratedKey key)
        {
            partitionKey = key;
            purgeEvaluator = null;
            ignoreGcGraceSeconds = shouldIgnoreGcGraceForAnyKey && controller.cfs.shouldIgnoreGcGraceForKey(partitionKey);
        }

        void onEmptyPartitionPostPurge()
        {
            if (type == OperationType.COMPACTION)
                controller.cfs.invalidateCachedPartition(partitionKey);
        }

        @Override
        public boolean shouldPurge(long timestamp, long localDeletionTime)
        {
            return !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone)
                   && (localDeletionTime < controller.gcBefore || ignoreGcGraceSeconds)
                   && getPurgeEvaluator().test(timestamp);
        }

        /*
         * Evaluates whether a tombstone with the given deletion timestamp can be purged. This is the minimum
         * timestamp for any sstable containing `currentKey` outside of the set of sstables involved in this compaction.
         * This is computed lazily on demand as we only need this if there is tombstones and this a bit expensive
         * (see #8914).
         */
        private LongPredicate getPurgeEvaluator()
        {
            if (purgeEvaluator == null)
            {
                purgeEvaluator = controller.getPurgeEvaluator(partitionKey);
            }
            return purgeEvaluator;
        }
    }

    // ACCOUNTING CODE
    public TableMetadata metadata()
    {
        return controller.cfs.metadata();
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(controller.cfs.metadata(),
                                  type,
                                  getBytesRead(),
                                  totalInputBytes,
                                  totalCompressedInputBytes,
                                  compactionId,
                                  sstables,
                                  targetDirectory);
    }

    public boolean isGlobal()
    {
        return false;
    }

    public void setTargetDirectory(final String targetDirectory)
    {
        this.targetDirectory = targetDirectory;
    }

    /**
     * Enables pre-purge top-partitions-by-tombstones counting for the read-only validation path
     * ({@link #mergeNextPartition}), matching the legacy {@code TopPartitionTracker.TombstoneCounter}.
     * Only the validation entry point ({@code CursorValidationIterator}) sets this; left null the
     * writing compaction path counts nothing (every counting site is guarded).
     */
    public void setTopPartitionCollector(TopPartitionTracker.Collector topPartitionCollector)
    {
        this.topPartitionCollector = topPartitionCollector;
    }

    public long[] getMergedParitionsCounts()
    {
        return partitionMergeCounters;
    }

    public long[] getMergedRowsCounts()
    {
        return rowMergeCounters;
    }

    public long[] getMergedCellsCounts()
    {
        return cellMergeCounters;
    }

    public long getTotalSourceCQLRows()
    {
        return totalSourceCQLRows;
    }

    public long getBytesRead()
    {
        return totalBytesRead;
    }

    private void updateTotalBytesRead(StatefulCursor cursor)
    {
        totalBytesRead += cursor.bytesReadSinceSnapshot();
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }

    public long getTotalBytesScanned()
    {
        return getBytesRead();
    }

    private static boolean isPaxos(ColumnFamilyStore cfs)
    {
        return cfs.name.equals(SystemKeyspace.PAXOS) && cfs.getKeyspaceName().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }

    private long sumHistogram(long[] histogram)
    {
        long sum = 0;
        for (long count : histogram)
        {
            sum += count;
        }
        return sum;
    }

    private static String mergeHistogramToString(long[] histogram)
    {
        StringBuilder sb = new StringBuilder();
        long sum = 0;
        sb.append("[");
        for (int i = 0; i < histogram.length; i++)
        {
            if (histogram[i] != 0)
            {
                sb.append(i + 1).append(":").append(histogram[i]).append(", ");
                sum += (i + 1) * histogram[i];
            }
        }
        if (sb.length() > 1)
            sb.setLength(sb.length() - 1); //trim trailing comma
        sb.append("] = ").append(sum);
        return sb.toString();
    }

    /**
     * Closes scanner-opened readers before opening cursor-specific readers with the configured disk access mode.
     * In cursor compaction, scanners are only used for metadata; closing them avoids holding redundant file
     * descriptors and prevents conflicts when scan and non-scan readers for the same file share thread-local
     * buffer state on the same thread.
     */
    private static StatefulCursor[] convertScannersToCursors(List<ISSTableScanner> scanners, ImmutableSet<SSTableReader> sstables,
                                                             DiskAccessMode diskAccessMode)
    {
        for (ISSTableScanner scanner : scanners)
            scanner.close();

        return buildCursorsOrCloseOnFailure(sstables.size(), cursors -> {
            int i = 0;
            for (SSTableReader reader : sstables)
                cursors[i++] = new StatefulCursor(reader, diskAccessMode);
        });
    }

    private static StatefulCursor[] convertSSTablesToPartialRangeCursors(Map<SSTableReader, List<PartitionPositionBounds>> boundsBySSTable,
                                                                         DiskAccessMode diskAccessMode)
    {
        return buildCursorsOrCloseOnFailure(boundsBySSTable.size(), cursors -> {
            int i = 0;
            for (Map.Entry<SSTableReader, List<PartitionPositionBounds>> entry : boundsBySSTable.entrySet())
            {
                StatefulCursor cursor = new StatefulCursor(entry.getKey(), diskAccessMode);
                // see convertScannersToCursors: the merge consumes deletion-only complex columns
                // as positions (their column-level deletions must reach the merged output)
                cursor.pauseAtEmptyComplexColumns(true);
                cursor.positionAt(entry.getValue());
                cursors[i++] = cursor;
            }
        });
    }

    /**
     * Shared by both cursor-array factories above, which differ only in how they map their
     * respective source (a set of sstables, or a partial-range bounds map) onto each
     * {@link StatefulCursor}: allocates a {@code size}-element array, lets {@code populate} fill
     * it in place, and - if constructing a later element throws - closes every cursor already
     * opened into it before rethrowing, so a partial failure never leaks open file handles.
     */
    private static StatefulCursor[] buildCursorsOrCloseOnFailure(int size, Consumer<StatefulCursor[]> populate)
    {
        StatefulCursor[] cursors = new StatefulCursor[size];
        try
        {
            populate.accept(cursors);
            return cursors;
        }
        catch (RuntimeException | Error e)
        {
            Throwables.closeNonNullAndAddSuppressed(e, cursors);
            throw e;
        }
    }

    public void close()
    {
        try
        {
            finish();

            for (SSTableCursorReader reader : sstableCursors)
            {
                reader.close();
            }
        }
        finally
        {
            activeCompactions.finishCompaction(this);
        }

        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("Compaction ended {}: { data bytes read = {}, data bytes written = {}, " +
                        " input (keys = {}, rows = {}, cells = {}), " +
                        " output (keys = {}, rows = {}, cells = {})}",
                        this.compactionId, getTotalBytesScanned(), totalDataBytesWritten,
                        mergeHistogramToString(partitionMergeCounters), mergeHistogramToString(rowMergeCounters), mergeHistogramToString(cellMergeCounters),
                        sumHistogram(partitionMergeCounters), sumHistogram(rowMergeCounters), sumHistogram(cellMergeCounters));
        }
    }

}
