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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * A row filter checked on reconciled data, as a cursor read streams it: each row after the
 * versions from every memtable and sstable are merged and the gcable tombstones purged, never on
 * one source's value.  The read's own row filter ({@code RowFilter.filter} on the iterator path) is
 * one; a caller of {@link CursorRangeScan} may add its own, which the read ANDs with it.
 *
 * <p>Calls per partition: {@link #partitionMatches}, then for each row {@link #startRow}, its
 * cells ({@link #cell}) and complex columns ({@link #complexColumnWritten}) in column order, and
 * {@link #rowMatches}.  A row the filter rejects is dropped; a partition whose rows are all
 * dropped, with no range tombstone left, is dropped too, like {@code RowFilter.filter}.  Every
 * argument is a view over reused storage, valid only during the call.
 */
public interface ReconciledRowFilter
{
    /**
     * Whether the partition can match, from its key and its purged static row.  False drops the
     * partition before its rows are read.
     */
    boolean partitionMatches(DecoratedKey key, Row purgedStaticRow);

    /** Whether {@link #rowMatches} can reject a row; when false, a partition is never dropped
     *  for having no row that matches. */
    boolean hasRowLevelExpressions();

    /** Whether {@link #startRow} needs each row's clustering. */
    boolean filtersClustering();

    /** @param clustering the row's clustering, or null when {@link #filtersClustering} is false */
    void startRow(Clustering<?> clustering);

    /** Whether {@link #cell} needs the values of {@code column}'s cells. */
    boolean filtersColumn(ColumnMetadata column);

    /**
     * A cell of the purged row.  It may be called for a column {@link #filtersColumn} does not
     * ask for, with its value; the filter ignores it.
     *
     * @param path  the cell path of a complex column's cell, else null
     * @param value the value, empty for a tombstone; read it without moving its position
     * @param live  whether the cell is live at the read's time
     */
    void cell(ColumnMetadata column, CellPath path, ByteBuffer value, boolean live);

    /** The purged row holds the complex column {@code column} with a complex deletion, or with no cells. */
    void complexColumnWritten(ColumnMetadata column);

    /**
     * Whether the current row passes.
     *
     * @param survivesPurgeAll whether {@code row.purge(DeletionPurger.PURGE_ALL)} leaves a row,
     *                         which {@code RowFilter.filter} checks unless the filter needs reconciliation
     * @param noDeletion       the purged row has no deletion
     */
    boolean rowMatches(boolean survivesPurgeAll, boolean noDeletion);

    /** Whether the current row has already failed, so its remaining cells cannot change the
     *  verdict and need not be written. */
    default boolean rowFailed()
    {
        return false;
    }

    /** A filter that passes what both {@code first} and {@code second} pass. */
    static ReconciledRowFilter and(ReconciledRowFilter first, ReconciledRowFilter second)
    {
        return new ReconciledRowFilter()
        {
            @Override
            public boolean partitionMatches(DecoratedKey key, Row purgedStaticRow)
            {
                return first.partitionMatches(key, purgedStaticRow) && second.partitionMatches(key, purgedStaticRow);
            }

            @Override
            public boolean hasRowLevelExpressions()
            {
                return first.hasRowLevelExpressions() || second.hasRowLevelExpressions();
            }

            @Override
            public boolean filtersClustering()
            {
                return first.filtersClustering() || second.filtersClustering();
            }

            @Override
            public void startRow(Clustering<?> clustering)
            {
                first.startRow(first.filtersClustering() ? clustering : null);
                second.startRow(second.filtersClustering() ? clustering : null);
            }

            @Override
            public boolean filtersColumn(ColumnMetadata column)
            {
                return first.filtersColumn(column) || second.filtersColumn(column);
            }

            @Override
            public void cell(ColumnMetadata column, CellPath path, ByteBuffer value, boolean live)
            {
                first.cell(column, path, value, live);
                second.cell(column, path, value, live);
            }

            @Override
            public void complexColumnWritten(ColumnMetadata column)
            {
                first.complexColumnWritten(column);
                second.complexColumnWritten(column);
            }

            @Override
            public boolean rowMatches(boolean survivesPurgeAll, boolean noDeletion)
            {
                return first.rowMatches(survivesPurgeAll, noDeletion) && second.rowMatches(survivesPurgeAll, noDeletion);
            }

            @Override
            public boolean rowFailed()
            {
                return first.rowFailed() || second.rowFailed();
            }
        };
    }
}
