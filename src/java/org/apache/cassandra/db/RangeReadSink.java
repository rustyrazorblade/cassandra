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

import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Receives the partitions a {@link CursorRangeScan} reads: what the replica data response of the
 * same read carries (merged, purged, filtered and limited, in the same order), as calls instead
 * of bytes, with no row or cell object.
 *
 * <p>Calls per partition: {@link #partitionStart}, then for each row {@link #startRow}, its
 * complex deletions ({@link #complexDeletion}) and cells ({@link #cell}) in column order, and
 * {@link #endRow} or {@link #abandonRow}; range tombstone markers ({@link #marker}) between rows;
 * then {@link #partitionEnd}.  A partition the read drops (every row filtered out, or nothing left
 * after the purge) is not given at all: {@link #partitionStart} comes only with the first row that
 * ends, or at the partition's end when the partition is kept without one (a deleted or
 * static-only partition).  After it a row can still be abandoned (the limit is reached at it).
 *
 * <p>Every argument is a view over reused storage, valid only during the call.  The data of the
 * memtables is valid only while the scan holds the table's read order group, which it releases at
 * each yield ({@link #yieldAfterPartition}).
 */
public interface RangeReadSink
{
    /**
     * Starts a partition.
     *
     * @param partitionDeletion the partition deletion, purged
     * @param staticRow         the static row, purged; {@code Rows.EMPTY_STATIC_ROW} for none
     */
    void partitionStart(DecoratedKey key, DeletionTime partitionDeletion, Row staticRow);

    /**
     * Starts a row.
     *
     * @param clustering the row's clustering ({@code ClusteringDescriptor.toClusteringPrefix} builds it)
     * @param liveness   the row's primary key liveness, purged
     * @param deletion   the row's deletion, purged
     */
    void startRow(ClusteringDescriptor clustering, LivenessInfo liveness, DeletionTime deletion);

    /** A complex column's deletion; {@code DeletionTime.LIVE} for a column the row holds with no cells. */
    void complexDeletion(ColumnMetadata column, DeletionTime deletion);

    /**
     * A cell of the row.
     *
     * @param path  the cell path of a complex column's cell, else null
     * @param value the value as the replica response carries it (the length for a variable-length
     *              type, then the bytes), or null for a cell with an empty value.  Its
     *              {@code streamValue} writes into a {@code DataOutputBuffer}, or into a
     *              {@code DataOutputPlus} that is a {@code SSTableCursorReader.DirectValueTarget},
     *              which reads a value of an sstable straight from the data file
     */
    void cell(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime, CellPath path, CellValueSource value);

    /** Ends the row. */
    void endRow();

    /** Drops the row started last, with its cells: the read does not return it. */
    void abandonRow();

    /** A range tombstone marker, between rows. */
    void marker(RangeTombstoneMarker marker);

    /** Ends the partition. */
    void partitionEnd();

    /** Checked before each partition: false ends the scan, which can be run again after {@link CursorRangeScan#lastPartitionKey}. */
    default boolean wantsMore()
    {
        return true;
    }

    /**
     * Checked after each partition: true ends the current segment of the scan, which releases the
     * table's read order group and its sstables, and {@link CursorRangeScan#run} returns
     * {@link CursorRangeScan.ScanEnd#YIELDED}.  A scan never stops in the middle of a partition.
     */
    default boolean yieldAfterPartition()
    {
        return false;
    }
}
