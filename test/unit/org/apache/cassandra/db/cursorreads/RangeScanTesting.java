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

package org.apache.cassandra.db.cursorreads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.CursorRangeScan;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.RangeReadSink;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.ReconciledRowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * Test helpers for {@link CursorRangeScan}: a sink that records what it gets as lines, and the same
 * lines for the replica data response of the iterator range path, the reference.  A line is a
 * partition (key, deletion), its static row, each row (clustering, liveness, deletion) with its
 * complex deletions and cells (timestamp, TTL, local deletion time, path, value), each range
 * tombstone marker, and the partition's end.
 */
final class RangeScanTesting
{
    private RangeScanTesting()
    {
    }

    /** Records what a scan gives; an abandoned row leaves no line. */
    static class RecordingSink implements RangeReadSink
    {
        final TableMetadata metadata;
        final List<String> lines = new ArrayList<>();
        /** The keys of the partitions given, in order. */
        final List<DecoratedKey> keys = new ArrayList<>();
        private final List<String> row = new ArrayList<>();
        private final BooleanSupplier yield;
        private boolean inPartition;
        private boolean inRow;
        int partitionsEnded;
        int rowsAbandoned;

        RecordingSink(TableMetadata metadata, BooleanSupplier yield)
        {
            this.metadata = metadata;
            this.yield = yield;
        }

        @Override
        public void partitionStart(DecoratedKey key, DeletionTime partitionDeletion, Row staticRow)
        {
            check(!inPartition, "partitionStart inside a partition");
            inPartition = true;
            keys.add(key);
            lines.add(partitionLine(key, partitionDeletion));
            if (!staticRow.isEmpty())
            {
                lines.add("STATIC");
                contentLines(staticRow, lines);
            }
        }

        @Override
        public void startRow(ClusteringDescriptor clustering, LivenessInfo liveness, DeletionTime deletion)
        {
            check(inPartition && !inRow, "startRow outside a partition or inside a row");
            inRow = true;
            row.clear();
            row.add(rowLine(metadata, (Clustering<?>) clustering.toClusteringPrefix(clustering.clusteringTypes()), liveness, deletion));
        }

        @Override
        public void complexDeletion(ColumnMetadata column, DeletionTime deletion)
        {
            check(inRow, "complexDeletion outside a row");
            row.add(complexLine(column, deletion));
        }

        @Override
        public void cell(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime, CellPath path, CellValueSource value)
        {
            check(inRow, "cell outside a row");
            byte[] bytes;
            try
            {
                bytes = value == null ? new byte[0] : value.materialize();
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
            row.add(cellLine(column, timestamp, ttl, localDeletionTime, path, bytes));
        }

        @Override
        public void endRow()
        {
            check(inRow, "endRow outside a row");
            inRow = false;
            lines.addAll(row);
            row.clear();
        }

        @Override
        public void abandonRow()
        {
            check(inRow, "abandonRow outside a row");
            inRow = false;
            rowsAbandoned++;
            row.clear();
        }

        @Override
        public void marker(RangeTombstoneMarker marker)
        {
            check(inPartition && !inRow, "marker outside a partition or inside a row");
            lines.add(markerLine(metadata, marker));
        }

        @Override
        public void partitionEnd()
        {
            check(inPartition && !inRow, "partitionEnd outside a partition or inside a row");
            inPartition = false;
            partitionsEnded++;
            lines.add("END");
        }

        @Override
        public boolean yieldAfterPartition()
        {
            return yield.getAsBoolean();
        }

        private static void check(boolean condition, String message)
        {
            if (!condition)
                throw new AssertionError("range read sink protocol: " + message);
        }
    }

    /** Runs {@code scan} to its end into {@code sink}; returns the segments it took. */
    static int runToEnd(CursorRangeScan scan, RangeReadSink sink)
    {
        int runs = 0;
        while (true)
        {
            runs++;
            CursorRangeScan.ScanEnd end = scan.run(sink);
            if (end == CursorRangeScan.ScanEnd.DONE)
                break;
            if (end == CursorRangeScan.ScanEnd.STOPPED)
                throw new AssertionError("the sink did not stop the scan");
            if (runs > 1_000_000)
                throw new AssertionError("the scan does not end");
        }
        assertEquals("partitions taken from an iterator", 0, scan.iteratorLegsOpened());
        return runs;
    }

    /** The lines of the cursor scan of {@code command}, yielding when {@code yield} says so. */
    static List<String> scanLines(PartitionRangeReadCommand command, ReconciledRowFilter filter, BooleanSupplier yield)
    {
        RecordingSink sink = new RecordingSink(command.metadata(), yield);
        runToEnd(CursorRangeScan.open(command, filter), sink);
        return sink.lines;
    }

    /** The lines of the replica data response of {@code command}, read on whichever path is on. */
    static List<String> responseLines(PartitionRangeReadCommand command)
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
        List<String> lines = new ArrayList<>();
        try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    lines.add(partitionLine(partition.partitionKey(), partition.partitionLevelDeletion()));
                    if (!partition.staticRow().isEmpty())
                    {
                        lines.add("STATIC");
                        contentLines(partition.staticRow(), lines);
                    }
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRow())
                        {
                            Row row = (Row) unfiltered;
                            lines.add(rowLine(command.metadata(), row.clustering(), row.primaryKeyLivenessInfo(), row.deletion().time()));
                            contentLines(row, lines);
                        }
                        else
                        {
                            lines.add(markerLine(command.metadata(), (RangeTombstoneMarker) unfiltered));
                        }
                    }
                    lines.add("END");
                }
            }
        }
        return lines;
    }

    static String partitionLine(DecoratedKey key, DeletionTime deletion)
    {
        return "PARTITION " + ByteBufferUtil.bytesToHex(key.getKey()) + " del=" + deletion.markedForDeleteAt() + '/' + deletion.localDeletionTime();
    }

    static String rowLine(TableMetadata metadata, Clustering<?> clustering, LivenessInfo liveness, DeletionTime deletion)
    {
        return "ROW " + clustering.toString(metadata) + " live=" + liveness.timestamp() + ',' + liveness.ttl() + ',' + liveness.localExpirationTime()
               + " del=" + deletion.markedForDeleteAt() + '/' + deletion.localDeletionTime();
    }

    static String complexLine(ColumnMetadata column, DeletionTime deletion)
    {
        return "  CPLX " + column.name + " del=" + deletion.markedForDeleteAt() + '/' + deletion.localDeletionTime();
    }

    static String cellLine(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime, CellPath path, byte[] value)
    {
        return "  CELL " + column.name + " path=" + (path == null ? "-" : ByteBufferUtil.bytesToHex(path.get(0)))
               + " ts=" + timestamp + " ttl=" + ttl + " ldt=" + localDeletionTime + " v=" + ByteBufferUtil.bytesToHex(java.nio.ByteBuffer.wrap(value));
    }

    static String markerLine(TableMetadata metadata, RangeTombstoneMarker marker)
    {
        return "MARKER " + marker.toString(metadata);
    }

    /** A row's complex deletions and cells, as the sink gets them: a complex deletion when it is
     *  not live, or for a column with no cells. */
    static void contentLines(Row row, List<String> lines)
    {
        for (ColumnData data : row)
        {
            if (!data.column().isComplex())
            {
                Cell<?> cell = (Cell<?>) data;
                lines.add(cellLine(cell.column(), cell.timestamp(), cell.ttl(), cell.localDeletionTime(), null, ByteBufferUtil.getArray(cell.buffer())));
                continue;
            }
            ComplexColumnData complex = (ComplexColumnData) data;
            if (!complex.complexDeletion().isLive() || complex.cellsCount() == 0)
                lines.add(complexLine(complex.column(), complex.complexDeletion()));
            for (int i = 0; i < complex.cellsCount(); i++)
            {
                Cell<?> cell = complex.getCellByIndex(i);
                lines.add(cellLine(cell.column(), cell.timestamp(), cell.ttl(), cell.localDeletionTime(), cell.path(), ByteBufferUtil.getArray(cell.buffer())));
            }
        }
    }

    /** The lines of the partition that holds line {@code i}. */
    private static List<String> partitionAround(List<String> lines, int i)
    {
        int start = i;
        while (start > 0 && !lines.get(start).startsWith("PARTITION"))
            start--;
        int end = i;
        while (end < lines.size() - 1 && !lines.get(end).equals("END"))
            end++;
        return lines.subList(start, end + 1);
    }

    /** Fails with the first line that differs. */
    static void assertLinesEqual(String what, List<String> expected, List<String> actual)
    {
        int n = Math.min(expected.size(), actual.size());
        for (int i = 0; i < n; i++)
        {
            if (!expected.get(i).equals(actual.get(i)))
                throw new AssertionError(String.format("%s: line %d differs%n  iterator: %s%n  cursor:   %s%n  iterator partition: %s%n  cursor partition:   %s",
                                                       what, i, expected.get(i), actual.get(i), partitionAround(expected, i), partitionAround(actual, i)));
        }
        if (expected.size() != actual.size())
            throw new AssertionError(String.format("%s: %d lines from the iterator path, %d from the scan; first extra: %s",
                                                   what, expected.size(), actual.size(),
                                                   expected.size() > n ? expected.get(n) : actual.get(n)));
    }
}
