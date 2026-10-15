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
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The scan counts {@link CursorReads.ScanStatsAccumulator} gives a row match
 * {@link ReadCommand#countScannedRow}, the iterator path's count, on a table with and without
 * strict liveness (a materialized view whose primary key holds a regular column of its base).
 */
public class ScanStatsAccumulatorLivenessTest extends CQLTester
{
    @Test
    public void countsMatchTheIteratorPath() throws Throwable
    {
        boolean viewsEnabled = DatabaseDescriptor.getMaterializedViewsEnabled();
        int failThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        try
        {
            createTable("CREATE TABLE %s (k int, c int, v int, w int, x int, PRIMARY KEY (k, c))");
            ColumnFamilyStore base = getCurrentColumnFamilyStore();
            String view = createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE k IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k, c)");
            ColumnFamilyStore viewStore = Keyspace.open(KEYSPACE).getColumnFamilyStore(view);
            assertTrue(viewStore.metadata().enforceStrictLiveness());
            for (ColumnFamilyStore cfs : new ColumnFamilyStore[]{ base, viewStore })
            {
                for (Row row : rows(cfs.metadata()))
                {
                    long[] expected = iteratorCounts(cfs, row);
                    assertEquals(cfs.name + " dropped " + row.toString(cfs.metadata(), true),
                                 expected[0] + " live, " + expected[1] + " tombstones", droppedCounts(cfs, row));
                    assertEmittedTombstones(cfs, row, (int) expected[1]);
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setMaterializedViewsEnabled(viewsEnabled);
            DatabaseDescriptor.setTombstoneFailureThreshold(failThreshold);
        }
    }

    /** Rows with an empty or live primary key liveness, with and without a row deletion, and with live and dead cells. */
    private static List<Row> rows(TableMetadata metadata)
    {
        long now = FBUtilities.nowInSeconds();
        ColumnMetadata w = metadata.getColumn(ByteBufferUtil.bytes("w"));
        ColumnMetadata x = metadata.getColumn(ByteBufferUtil.bytes("x"));
        List<Row> rows = new ArrayList<>();
        for (int shape = 0; shape < 6; shape++)
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(clustering(metadata));
            switch (shape)
            {
                case 0: // a live cell only
                    builder.addCell(BufferCell.live(w, 10, ByteBufferUtil.bytes(1)));
                    break;
                case 1: // a row deletion and a newer live cell
                    builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(5, now)));
                    builder.addCell(BufferCell.live(w, 10, ByteBufferUtil.bytes(1)));
                    break;
                case 2: // a live cell and a dead cell
                    builder.addCell(BufferCell.live(w, 10, ByteBufferUtil.bytes(1)));
                    builder.addCell(BufferCell.tombstone(x, 10, now));
                    break;
                case 3: // a dead cell only
                    builder.addCell(BufferCell.tombstone(x, 10, now));
                    break;
                case 4: // a live primary key and a dead cell
                    builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(10));
                    builder.addCell(BufferCell.tombstone(x, 10, now));
                    break;
                default: // a row deletion only
                    builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(5, now)));
                    break;
            }
            rows.add(builder.build());
        }
        return rows;
    }

    private static Clustering<?> clustering(TableMetadata metadata)
    {
        int size = metadata.clusteringColumns().size();
        ByteBuffer[] values = new java.nio.ByteBuffer[size];
        for (int i = 0; i < size; i++)
            values[i] = ByteBufferUtil.bytes(i + 1);
        return Clustering.make(values);
    }

    /** {live rows, tombstones}, as the iterator path's MetricRecording counts the row. */
    private static long[] iteratorCounts(ColumnFamilyStore cfs, Row row)
    {
        long[] counts = new long[2];
        ReadCommand.countScannedRow(row, FBUtilities.nowInSeconds(), cfs.metadata().enforceStrictLiveness(), new ReadCommand.ScannedRowCounter()
        {
            public void countLiveRow()
            {
                counts[0]++;
            }

            public void countTombstone(ClusteringPrefix<?> clustering)
            {
                counts[1]++;
            }
        });
        return counts;
    }

    private static SinglePartitionReadCommand command(ColumnFamilyStore cfs)
    {
        TableMetadata metadata = cfs.metadata();
        return SinglePartitionReadCommand.create(metadata, FBUtilities.nowInSeconds(), ColumnFilter.all(metadata), RowFilter.none(),
                                                 DataLimits.NONE, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1)),
                                                 new ClusteringIndexSliceFilter(Slices.ALL, false));
    }

    /** The counts the accumulator gives the row as a row the merge dropped. */
    private static String droppedCounts(ColumnFamilyStore cfs, Row row)
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(Integer.MAX_VALUE);
        SinglePartitionReadCommand command = command(cfs);
        try (ReadExecutionController controller = command.executionController())
        {
            CursorReads.ScanStatsAccumulator acc = new CursorReads.ScanStatsAccumulator(command, cfs, controller);
            acc.beginDroppedRow(row.clustering(), true, row.primaryKeyLivenessInfo(), row.deletion().time());
            for (Cell<?> cell : row.cells())
                acc.droppedCell(cell.timestamp(), cell.localDeletionTime(), cell.ttl());
            acc.endDroppedRow();
            return acc.droppedLiveRows() + " live, " + acc.droppedTombstones() + " tombstones";
        }
    }

    /**
     * The tombstones the accumulator counts for the row as an emitted row: with the failure
     * threshold at the iterator path's count, one more dropped tombstone must abort the read at
     * exactly one past it.
     */
    private static void assertEmittedTombstones(ColumnFamilyStore cfs, Row row, int expected)
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(expected);
        SinglePartitionReadCommand command = command(cfs);
        ColumnMetadata x = cfs.metadata().getColumn(ByteBufferUtil.bytes("x"));
        try (ReadExecutionController controller = command.executionController())
        {
            CursorReads.ScanStatsAccumulator acc = new CursorReads.ScanStatsAccumulator(command, cfs, controller);
            acc.accountRow(row);
            Cell<?> dead = BufferCell.tombstone(x, 10, FBUtilities.nowInSeconds());
            acc.beginDroppedRow(row.clustering(), true, LivenessInfo.EMPTY, DeletionTime.LIVE);
            acc.droppedCell(dead.timestamp(), dead.localDeletionTime(), dead.ttl());
            acc.endDroppedRow();
            fail(cfs.name + " emitted " + row.toString(cfs.metadata(), true) + ": counted fewer than " + expected + " tombstones");
        }
        catch (TombstoneOverwhelmingException e)
        {
            assertTrue(cfs.name + " emitted " + row.toString(cfs.metadata(), true) + ": " + e.getMessage(),
                       e.getMessage().startsWith("Scanned over " + (expected + 1) + " tombstones"));
        }
    }
}
