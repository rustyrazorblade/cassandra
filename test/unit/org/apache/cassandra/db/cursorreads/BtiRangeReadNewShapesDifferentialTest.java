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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Differential coverage for the range-read shapes the range gate used to reject and now serves on the
 * cursor path: multi-slice per-partition filters, counter tables, and a memtable-only range (zero
 * candidate sstables).  For each shape the cross-partition cursor driver
 * {@link CursorRangeReads#rangeIterator} must produce exactly the multi-partition stream the legacy
 * iterator path ({@link UnfilteredPartitionIterators#mergeLazily} over each source's
 * {@code partitionIterator(columnFilter, dataRange)}) produces for the same command and sources.
 *
 * <p>The canonical record stream covers partition keys and deletions, static rows, clustering, row
 * liveness/deletion, cells (path, ttl, timestamp, local deletion time, on-disk value bytes), complex
 * deletions, and range tombstone markers.  The counter test exercises multi-shard reconciliation of
 * counter contexts, which the cursor merge must clear the marked-local shard on, byte-for-byte with the
 * iterator path.
 */
public class BtiRangeReadNewShapesDifferentialTest extends RangeReadOracle
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void selectBti()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    // ------------------------------------------------------------------ tests

    @Test
    public void multiSlicePerPartitionMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        TableMetadata metadata = cfs.metadata();

        // two disjoint clustering slices per partition: [1,3] and [6,8]
        Slices slices = new Slices.Builder(metadata.comparator)
                        .add(Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(1L)),
                                        BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(3L))))
                        .add(Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(6L)),
                                        BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(8L))))
                        .build();
        assertEquals("fixture must build a genuine multi-slice filter", 2, slices.size());
        DataRange dataRange = new DataRange(DataRange.allData(metadata.partitioner).keyRange(),
                                            new ClusteringIndexSliceFilter(slices, false));
        PartitionRangeReadCommand command = rangeCommand(cfs, dataRange);

        List<String> oracle = render(oracle(cfs, command));
        List<String> cursor = render(cursor(cfs, command));
        compareRenderedRecords(oracle, cursor);
        assertTrue("multi-slice range must return rows", countPartitions(oracle) > 0);

        assertDataRangeMatches(ReadCase.of("multi slice", 1), cfs, dataRange);
    }

    @Test
    public void counterTableFullRingMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingCounters();
        DataRange dataRange = DataRange.allData(cfs.metadata().partitioner);
        PartitionRangeReadCommand command = rangeCommand(cfs, dataRange);

        List<String> oracle = render(oracle(cfs, command));
        List<String> cursor = render(cursor(cfs, command));
        compareRenderedRecords(oracle, cursor);
        assertTrue("counter dataset must span many partitions", countPartitions(oracle) > 20);
        assertTrue("counter dataset must carry counter cells", oracle.stream().anyMatch(r -> r.startsWith("CELL c1")));

        assertDataRangeMatches(ReadCase.of("counters", 2), cfs, dataRange);
    }

    @Test
    public void memtableOnlyRangeMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildMemtableOnly();
        assertTrue("fixture must have zero sstables", cfs.getLiveSSTables().isEmpty());
        DataRange dataRange = DataRange.allData(cfs.metadata().partitioner);
        PartitionRangeReadCommand command = rangeCommand(cfs, dataRange);

        // the cursor side is handed zero sstables on purpose
        List<SSTableReader> sstables = new ArrayList<>(cfs.select(View.selectLive(command.dataRange().keyRange())).sstables);
        assertTrue("range must select no sstables", sstables.isEmpty());

        List<String> oracle = render(oracle(cfs, command));
        List<String> cursor = render(cursor(cfs, command));
        compareRenderedRecords(oracle, cursor);
        assertTrue("memtable-only range must return rows", countPartitions(oracle) > 10);

        assertDataRangeMatches(ReadCase.of("memtable only", 3), cfs, dataRange);
    }

    @Test
    public void namesQueryMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildNamesSources();
        TableMetadata metadata = cfs.metadata();

        // names 2, 5, 7 hit live rows in some sources; 100 hits nothing.  The fixture makes some
        // partitions carry only a whole-partition deletion in one sstable (no rows at these names)
        // while another source holds surviving rows at the same names: the exact non-intersecting,
        // tombstone-only leg shape the single-partition path handles with the names stats quirk.
        BTreeSet.Builder<Clustering<?>> names = BTreeSet.builder(metadata.comparator);
        for (long ck : new long[]{ 2L, 5L, 7L, 100L })
            names.add(Clustering.make(LongType.instance.decompose(ck)));
        ClusteringIndexNamesFilter namesFilter = new ClusteringIndexNamesFilter(names.build(), false);
        DataRange dataRange = DataRange.allData(metadata.partitioner, namesFilter);
        PartitionRangeReadCommand command = rangeCommand(cfs, dataRange);
        assertTrue("scenario precondition: the range must be a names query", command.dataRange().isNamesQuery());

        List<String> oracle = render(oracle(cfs, command));
        List<String> cursor = render(cursor(cfs, command));
        compareRenderedRecords(oracle, cursor);
        assertTrue("names range must return rows", countPartitions(oracle) > 10);

        assertDataRangeMatches(ReadCase.of("names", 4), cfs, dataRange);
    }

    @Test
    public void reversedRangeMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        TableMetadata metadata = cfs.metadata();

        DataRange reversed = DataRange.allData(metadata.partitioner, new ClusteringIndexSliceFilter(Slices.ALL, true));
        PartitionRangeReadCommand command = rangeCommand(cfs, reversed);
        assertTrue("scenario precondition: the range must be reversed", command.dataRange().isReversed());

        List<String> oracle = render(oracle(cfs, command));
        List<String> cursor = render(cursor(cfs, command));
        compareRenderedRecords(oracle, cursor);
        assertTrue("reversed range must return rows", countPartitions(oracle) > 0);

        assertDataRangeMatches(ReadCase.of("reversed", 5), cfs, reversed);
    }

    @Test
    public void forwardLargePartitionMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildLargePartition();
        DataRange dataRange = DataRange.allData(cfs.metadata().partitioner);
        PartitionRangeReadCommand command = rangeCommand(cfs, dataRange);

        List<String> oracle = render(oracle(cfs, command));
        List<String> cursor = render(cursor(cfs, command));
        compareRenderedRecords(oracle, cursor);
        assertTrue("forward large-partition range must return rows", countPartitions(oracle) > 0);

        assertDataRangeMatches(ReadCase.of("large partition", 6), cfs, dataRange);
    }

    @Test
    public void reversedLargePartitionMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildLargePartition();
        TableMetadata metadata = cfs.metadata();

        DataRange reversed = DataRange.allData(metadata.partitioner, new ClusteringIndexSliceFilter(Slices.ALL, true));
        PartitionRangeReadCommand command = rangeCommand(cfs, reversed);

        List<String> oracle = render(oracle(cfs, command));
        List<String> cursor = render(cursor(cfs, command));
        compareRenderedRecords(oracle, cursor);
        assertTrue("reversed large-partition range must return rows", countPartitions(oracle) > 0);

        assertDataRangeMatches(ReadCase.of("large partition reversed", 7), cfs, reversed);
    }

    /**
     * Diagnostic: the SAME large block-indexed partition, read reversed through the SINGLE-partition
     * cursor path (not the range driver).  This isolates the defect.  If the single-partition cursor
     * path also fails, the reverse-read foundation is broken over block-indexed partitions.  If it
     * passes, the fault is in how the range driver composes the reverse legs.
     */
    @Test
    public void reversedLargePartitionSinglePartitionMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildLargePartition();
        TableMetadata metadata = cfs.metadata();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(100L));
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(Slices.ALL, true);
        SinglePartitionReadCommand command =
            SinglePartitionReadCommand.create(metadata, FBUtilities.nowInSeconds(), key,
                                              ColumnFilter.all(metadata), filter);

        List<String> oracle = renderSinglePartition(cfs, command, false);
        List<String> cursor = renderSinglePartition(cfs, command, true);
        compareRenderedRecords(oracle, cursor);
        assertTrue("single-partition reversed read must return rows", !oracle.isEmpty());
    }

    private List<String> renderSinglePartition(ColumnFamilyStore cfs, SinglePartitionReadCommand command,
                                               boolean cursorReads)
    {
        boolean previous = DatabaseDescriptor.cursorRangeReadsEnabled();
        DatabaseDescriptor.setCursorRangeReadsEnabled(cursorReads);
        try (ReadExecutionController controller = command.executionController())
        {
            UnfilteredRowIterator partition = command.queryMemtableAndDisk(cfs, controller);
            return render(new SingletonUnfilteredPartitionIterator(partition));
        }
        finally
        {
            DatabaseDescriptor.setCursorRangeReadsEnabled(previous);
        }
    }

    // ------------------------------------------------------------------ fixtures

    /** Two overlapping BTI sstables plus a live memtable, with statics, deletes, and range tombstones. */
    private ColumnFamilyStore buildOverlappingSources() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v1 bigint, v2 text, " +
                    "m map<text, text>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 25; pk++)
        {
            if (pk % 4 == 0)
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 100, pk);
            for (long ck = 0; ck < 9; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 10 + ck, "A-" + pk + '-' + ck,
                        map("a" + ck, "x" + pk), 1000L);
            if (pk % 3 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck > ? AND ck < ?", 1001L, pk, 1L, 3L);
        }
        flush();

        for (long pk = 10; pk < 35; pk++)
        {
            for (long ck = 2; ck < 9; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 20 + ck, "B-" + pk + '-' + ck,
                        map("b" + ck, "z" + pk), 2000L);
        }
        flush();

        assertEquals("fixture expects exactly two sstables", 2, cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue("expected a BTI sstable", sstable instanceof BtiTableReader);

        for (long pk = 20; pk < 31; pk++)
            for (long ck = 0; ck < 9; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 30 + ck, "M-" + pk + '-' + ck, map("c" + ck, "m" + pk), 3000L);
        return cfs;
    }

    /** Two overlapping BTI sstables plus a memtable of counter updates, forcing multi-shard merges. */
    private ColumnFamilyStore buildOverlappingCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 15; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = ? AND ck = ?", pk + ck, pk * 2 + ck, pk, ck);
        flush();

        for (long pk = 8; pk < 25; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", pk * 10 + ck, pk, ck);
        flush();

        assertEquals("fixture expects exactly two sstables", 2, cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue("expected a BTI sstable", sstable instanceof BtiTableReader);

        for (long pk = 18; pk < 28; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("UPDATE %s SET c2 = c2 + ? WHERE pk = ? AND ck = ?", pk * 100 + ck, pk, ck);
        return cfs;
    }

    /** A table whose data lives entirely in the live memtable: no flush, so zero sstables. */
    private ColumnFamilyStore buildMemtableOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v1 bigint, v2 text, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 20; pk++)
        {
            if (pk % 4 == 0)
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 100, pk);
            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 10 + ck, "M-" + pk + '-' + ck, 3000L);
            if (pk % 5 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck > ? AND ck < ?", 3001L, pk, 1L, 3L);
        }
        return cfs;
    }

    /**
     * Two overlapping BTI sstables plus a memtable, built so that several partitions carry only a
     * whole-partition deletion in the first sstable (no rows at the queried names there) while later
     * sources hold surviving rows at those same names.  This forces the non-intersecting,
     * tombstone-only leg to merge against live legs, the shape that stresses stats-header parity.
     */
    private ColumnFamilyStore buildNamesSources() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v1 bigint, v2 text, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sstable A: even pk get a whole-partition delete at ts 1000; odd pk get full row sets at ts 1000
        for (long pk = 0; pk < 10; pk++)
        {
            if (pk % 2 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", 1000L, pk);
            else
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                            pk, ck, pk * 10 + ck, "A-" + pk + '-' + ck, 1000L);
            if (pk % 4 == 0)
                execute("UPDATE %s USING TIMESTAMP ? SET s1 = ? WHERE pk = ?", 1000L, pk * 100, pk);
        }
        flush();

        // sstable B: rows at the queried names, ts 2000, surviving the ts-1000 partition deletes
        for (long pk = 0; pk < 15; pk++)
            for (long ck : new long[]{ 2L, 5L, 7L })
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 20 + ck, "B-" + pk + '-' + ck, 2000L);
        flush();

        assertEquals("fixture expects exactly two sstables", 2, cfs.getLiveSSTables().size());

        // memtable: higher partitions, ts 3000
        for (long pk = 10; pk < 20; pk++)
            for (long ck : new long[]{ 2L, 5L, 7L })
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 30 + ck, "M-" + pk + '-' + ck, 3000L);
        return cfs;
    }

    /**
     * A dataset with one partition large enough to span several column-index blocks, split across two
     * overlapping BTI sstables plus a memtable, alongside a few small partitions.  A reversed read of
     * this dataset drives the block-indexed reverse walk (ReverseBlockCursor) and the multi-leg reverse
     * merge over indexed legs, not just the single-block reverse path.
     */
    private ColumnFamilyStore buildLargePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sstable A: a large partition (even ck) plus two small partitions, ts 1000
        for (long ck = 0; ck < 40000; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    100L, ck, ck, "A-" + ck, 1000L);
        for (long pk : new long[]{ 7L, 42L })
            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, ck, "A-" + pk + '-' + ck, 1000L);
        flush();

        // sstable B: interleaving odd ck of the large partition, ts 2000, so both indexed legs merge
        for (long ck = 1; ck < 40000; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    100L, ck, ck, "B-" + ck, 2000L);
        flush();

        assertEquals("fixture expects exactly two sstables", 2, cfs.getLiveSSTables().size());

        // memtable: a few rows in the large partition and a new small partition, ts 3000
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    100L, ck, ck, "M-" + ck, 3000L);
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    99L, ck, ck, "M-99-" + ck, 3000L);
        return cfs;
    }

    private static PartitionRangeReadCommand rangeCommand(ColumnFamilyStore cfs, DataRange dataRange)
    {
        TableMetadata metadata = cfs.metadata();
        return PartitionRangeReadCommand.create(metadata,
                                                FBUtilities.nowInSeconds(),
                                                ColumnFilter.all(metadata),
                                                RowFilter.none(),
                                                DataLimits.NONE,
                                                dataRange);
    }

    // ------------------------------------------------------------------ the two paths

    private UnfilteredPartitionIterator oracle(ColumnFamilyStore cfs, PartitionRangeReadCommand command)
    {
        List<UnfilteredPartitionIterator> inputs = new ArrayList<>();
        for (Memtable memtable : cfs.select(View.selectLive(command.dataRange().keyRange())).memtables)
            inputs.add(memtable.partitionIterator(command.columnFilter(), command.dataRange(), SSTableReadsListener.NOOP_LISTENER));
        for (SSTableReader sstable : cfs.select(View.selectLive(command.dataRange().keyRange())).sstables)
            inputs.add(sstable.partitionIterator(command.columnFilter(), command.dataRange(), SSTableReadsListener.NOOP_LISTENER));
        return UnfilteredPartitionIterators.mergeLazily(inputs);
    }

    private UnfilteredPartitionIterator cursor(ColumnFamilyStore cfs, PartitionRangeReadCommand command)
    {
        List<UnfilteredPartitionIterator> memtableIterators = new ArrayList<>();
        for (Memtable memtable : cfs.select(View.selectLive(command.dataRange().keyRange())).memtables)
            memtableIterators.add(memtable.partitionIterator(command.columnFilter(), command.dataRange(), SSTableReadsListener.NOOP_LISTENER));
        List<SSTableReader> sstables = new ArrayList<>(cfs.select(View.selectLive(command.dataRange().keyRange())).sstables);
        return CursorRangeReads.rangeIterator(command, cfs, sstables, memtableIterators, null, SSTableReadsListener.NOOP_LISTENER);
    }

    // ------------------------------------------------------------------ rendering

    private List<String> render(UnfilteredPartitionIterator partitions)
    {
        List<String> out = new ArrayList<>();
        try (UnfilteredPartitionIterator it = partitions)
        {
            while (it.hasNext())
            {
                try (UnfilteredRowIterator partition = it.next())
                {
                    boolean empty = partition.staticRow().isEmpty() && !partition.hasNext()
                                    && partition.partitionLevelDeletion().isLive();
                    out.add(partitionRecord(partition.partitionKey(), partition.partitionLevelDeletion(), empty));
                    if (!partition.staticRow().isEmpty())
                        appendRow("STATIC", partition.staticRow(), out);
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRow())
                            appendRow("ROW", (Row) unfiltered, out);
                        else
                            out.add(markerRecord((RangeTombstoneMarker) unfiltered));
                    }
                }
            }
        }
        return out;
    }

    private void appendRow(String label, Row row, List<String> out)
    {
        String clustering = label.equals("STATIC") ? "STATIC" : clusteringString(row.clustering());
        out.add(rowHeader(label, clustering,
                          row.primaryKeyLivenessInfo().timestamp(),
                          row.primaryKeyLivenessInfo().ttl(),
                          row.primaryKeyLivenessInfo().localExpirationTime(),
                          row.deletion().time().markedForDeleteAt(),
                          row.deletion().time().localDeletionTime(),
                          row.deletion().isShadowable()));
        for (ColumnData cd : row)
        {
            if (cd.column().isComplex())
            {
                ComplexColumnData complex = (ComplexColumnData) cd;
                out.add(complexHeader(cd.column().name.toString(),
                                      complex.complexDeletion().markedForDeleteAt(),
                                      complex.complexDeletion().localDeletionTime(),
                                      complex.cellsCount()));
                for (Cell<?> cell : complex)
                    out.add(cellRecord(cell.column().name.toString(), cell.path() == null ? null : cell.path().get(0),
                                       cell.timestamp(), cell.ttl(), cell.localDeletionTime(), cellValueBytes(cell)));
            }
            else
            {
                Cell<?> cell = (Cell<?>) cd;
                out.add(cellRecord(cell.column().name.toString(), cell.path() == null ? null : cell.path().get(0),
                                   cell.timestamp(), cell.ttl(), cell.localDeletionTime(), cellValueBytes(cell)));
            }
        }
    }

    private static byte[] cellValueBytes(Cell<?> cell)
    {
        if (cell.valueSize() <= 0)
            return new byte[0];
        try (DataOutputBuffer dob = new DataOutputBuffer())
        {
            cell.column().type.writeValue(cell.buffer(), ByteBufferAccessor.instance, dob);
            return dob.toByteArray();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    private static String partitionRecord(DecoratedKey key, DeletionTime deletion, boolean empty)
    {
        return "PARTITION key=" + ByteBufferUtil.bytesToHex(key.getKey())
               + " empty=" + empty
               + " del=" + dt(deletion.markedForDeleteAt(), deletion.localDeletionTime());
    }

    private static String rowHeader(String label, String clustering, long ts, int ttl, long localExpiration,
                                    long delMarkedForDeleteAt, long delLocalDeletionTime, boolean shadowable)
    {
        return label + " clustering=" + clustering
               + " liveness=" + ts + ',' + ttl + ',' + localExpiration
               + " del=" + dt(delMarkedForDeleteAt, delLocalDeletionTime)
               + (shadowable ? ",shadowable" : "");
    }

    private static String complexHeader(String column, long delMarkedForDeleteAt, long delLocalDeletionTime, int cellCount)
    {
        return "CPLX " + column + " del=" + dt(delMarkedForDeleteAt, delLocalDeletionTime) + " n=" + cellCount;
    }

    private static String cellRecord(String column, ByteBuffer path, long ts, int ttl, long localDeletionTime, byte[] value)
    {
        return "CELL " + column
               + " path=" + (path == null ? "-" : Hex.bytesToHex(ByteBufferUtil.getArray(path)))
               + " ts=" + ts + " ttl=" + ttl + " ldt=" + localDeletionTime
               + " v=" + Hex.bytesToHex(value);
    }

    private static String markerRecord(RangeTombstoneMarker marker)
    {
        StringBuilder sb = new StringBuilder("MARKER clustering=").append(clusteringString(marker.clustering()));
        if (marker.isClose(false))
            sb.append(" close=").append(dt(marker.closeDeletionTime(false).markedForDeleteAt(),
                                           marker.closeDeletionTime(false).localDeletionTime()))
              .append(marker.closeIsInclusive(false) ? ",incl" : ",excl");
        if (marker.isOpen(false))
            sb.append(" open=").append(dt(marker.openDeletionTime(false).markedForDeleteAt(),
                                          marker.openDeletionTime(false).localDeletionTime()))
              .append(marker.openIsInclusive(false) ? ",incl" : ",excl");
        return sb.toString();
    }

    private static String clusteringString(ClusteringPrefix<?> prefix)
    {
        StringBuilder sb = new StringBuilder(prefix.kind().toString());
        for (int i = 0; i < prefix.size(); i++)
        {
            ByteBuffer value = prefix.bufferAt(i);
            sb.append(':').append(value == null ? "null" : ByteBufferUtil.bytesToHex(value));
        }
        return sb.toString();
    }

    private static String dt(long markedForDeleteAt, long localDeletionTime)
    {
        return markedForDeleteAt + "/" + localDeletionTime;
    }

    private static int countPartitions(List<String> records)
    {
        return (int) records.stream().filter(r -> r.startsWith("PARTITION")).count();
    }

    private static void compareRenderedRecords(List<String> oracle, List<String> cursor)
    {
        int max = Math.max(oracle.size(), cursor.size());
        for (int i = 0; i < max; i++)
        {
            String expected = i < oracle.size() ? oracle.get(i) : "<missing>";
            String actual = i < cursor.size() ? cursor.get(i) : "<missing>";
            if (!expected.equals(actual))
                fail(String.format("range divergence at record %d (iterator vs cursor):%n  iterator: %s%n  cursor:   %s%n  context:%s",
                                   i, expected, actual, context(oracle, cursor, i)));
        }
        assertEquals("record counts diverged", oracle.size(), cursor.size());
    }

    private static String context(List<String> oracle, List<String> cursor, int index)
    {
        StringBuilder sb = new StringBuilder();
        for (int j = Math.max(0, index - 3); j < Math.min(oracle.size(), index + 4); j++)
            sb.append(String.format("%n    it[%d]=%s", j, oracle.get(j)));
        for (int j = Math.max(0, index - 3); j < Math.min(cursor.size(), index + 4); j++)
            sb.append(String.format("%n    cu[%d]=%s", j, cursor.get(j)));
        return sb.toString();
    }
}
