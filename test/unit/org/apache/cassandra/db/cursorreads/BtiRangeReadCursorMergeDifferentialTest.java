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
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.MemtablePartitionData;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Cross-partition merge differential for the cursor range read: prove the
 * cross-partition cursor merge driver {@link CursorRangeReads#rangeIterator} produces, for a
 * partition-range read, exactly the multi-partition stream the legacy iterator path produces for the
 * same command over the same sources.
 *
 * <p>Both sides read the same BTI table over the same {@link DataRange}, from the same inputs (two
 * sstables with overlapping partitions/rows AND the live memtable):
 * <ul>
 *   <li>the oracle is {@link UnfilteredPartitionIterators#mergeLazily} over each source's
 *       {@code partitionIterator(columnFilter, dataRange)} - the raw pre-purge merge
 *       {@code PartitionRangeReadCommand.queryStorage} feeds up to its transformations;</li>
 *   <li>the cursor side is {@link CursorRangeReads#rangeIterator} over the same sstables and the same
 *       memtable partition iterators.</li>
 * </ul>
 * Each side is rendered into an identical canonical record stream covering partition keys and
 * deletions, static rows (including the empty-static-row case that a static-declaring table with
 * sparse statics forces), clustering, row liveness/deletion, cells (path, ttl, timestamp, local
 * deletion time, on-disk value bytes), complex-column deletions, and range tombstone markers. The two
 * streams must match exactly.
 *
 * <p>Ranges exercised: the full ring; an interior subrange spanning several partition boundaries; a
 * subrange that lands between two adjacent partitions (empty result); a single-partition subrange.
 * The dataset forces every reconciliation shape the driver must get right: a partition/row present in
 * both sstables AND the memtable, a partition-level delete, range tombstones spanning rows, a
 * static-only partition, a static-declaring table with sparse (empty) statics, and cross-partition
 * interleaving in token order across all three sources.
 */
public class BtiRangeReadCursorMergeDifferentialTest extends RangeReadOracle
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

    // ------------------------------------------------------------------ fixture

    /**
     * Builds a BTI table whose data is split across two sstables and the live memtable, with real
     * cross-source overlap. Uses {@code memtable_flush_period_in_ms=0} + explicit flushes so the
     * first two batches become sstables and the third stays in the memtable.
     *
     * <p>Shapes forced into the dataset:
     * <ul>
     *   <li>a static column and a complex (map) column;</li>
     *   <li>static rows on some multi-row partitions, plus a static-only partition;</li>
     *   <li>a static-declaring table where most partitions set no static (sparse statics -> the raw
     *       cursor's empty static row, which the merge must drop);</li>
     *   <li>a partition-level delete;</li>
     *   <li>range tombstones spanning rows;</li>
     *   <li>partitions and individual rows written in all three sources with different timestamps, so
     *       the merge must reconcile a three-way overlap.</li>
     * </ul>
     */
    private ColumnFamilyStore buildOverlappingSources() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v1 bigint, v2 text, " +
                    "m map<text, text>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // ---- sstable A: pk 0..24 ----
        for (long pk = 0; pk < 25; pk++)
        {
            if (pk == 13)
            {
                execute("DELETE FROM %s WHERE pk = ?", pk); // partition-level delete
                continue;
            }
            if (pk == 7)
            {
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 100, pk); // static-only partition
                continue;
            }
            if (pk % 4 == 0)
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 100, pk);
            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 10 + ck, "A-" + pk + '-' + ck,
                        map("a" + ck, "x" + pk, "b" + ck, "y" + pk), 1000L);
            if (pk % 3 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck > ? AND ck < ?", 1001L, pk, 1L, 3L);
        }
        flush();

        // ---- sstable B: pk 10..34, overlapping A on 10..24 with newer rows ----
        for (long pk = 10; pk < 35; pk++)
        {
            if (pk % 5 == 0)
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 1000, pk); // static reconciliation
            for (long ck = 2; ck < 7; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 20 + ck, "B-" + pk + '-' + ck,
                        map("b" + ck, "z" + pk, "c" + ck, "w" + pk), 2000L);
            if (pk % 4 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck > ? AND ck < ?", 2001L, pk, 3L, 6L);
        }
        flush();

        assertEquals("fixture expects exactly two sstables", 2, cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue("expected a BTI sstable", sstable instanceof BtiTableReader);

        // ---- memtable: pk 20..30, overlapping both sstables with the newest rows ----
        for (long pk = 20; pk < 31; pk++)
        {
            if (pk % 3 == 0)
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 10000, pk);
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 30 + ck, "M-" + pk + '-' + ck,
                        map("c" + ck, "m" + pk), 3000L);
        }
        // a memtable-only fresh partition, and a memtable-only second copy of an existing one
        for (long ck = 0; ck < 3; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP ?", 100L, ck, ck, 3000L);

        assertFalse("the memtable must hold unflushed data", cfs.getLiveSSTables().size() != 2);
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

    /** Every live partition key of the table, in token order (across sstables + memtable). */
    private List<DecoratedKey> keysInTokenOrder(ColumnFamilyStore cfs)
    {
        java.util.TreeMap<DecoratedKey, Boolean> keys = new java.util.TreeMap<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                    try (UnfilteredRowIterator p = scanner.next()) { keys.put(p.partitionKey(), true); }
            }
        }
        // memtable keys: read them through the full-ring oracle
        PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner));
        for (Memtable memtable : cfs.select(View.selectLive(command.dataRange().keyRange())).memtables)
        {
            try (UnfilteredPartitionIterator iter = memtable.partitionIterator(command.columnFilter(), command.dataRange(), SSTableReadsListener.NOOP_LISTENER))
            {
                while (iter.hasNext())
                    try (UnfilteredRowIterator p = iter.next()) { keys.put(p.partitionKey(), true); }
            }
        }
        return new ArrayList<>(keys.keySet());
    }

    // ------------------------------------------------------------------ tests

    @Test
    public void fullRingMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        DataRange dataRange = DataRange.allData(cfs.metadata().partitioner);

        List<String> oracle = render(oracle(cfs, dataRange));
        List<String> cursor = render(cursor(cfs, dataRange));
        compareRenderedRecords(oracle, cursor);

        // the dataset really did exercise every partition shape through the range merge
        assertTrue("dataset must contain a range tombstone marker", oracle.stream().anyMatch(r -> r.startsWith("MARKER")));
        assertTrue("dataset must contain a static row", oracle.stream().anyMatch(r -> r.startsWith("STATIC")));
        assertTrue("dataset must contain a partition-level deletion",
                   oracle.stream().anyMatch(r -> r.startsWith("PARTITION") && !r.endsWith("del=" + liveDt())));
        assertTrue("dataset must span many partitions", countPartitions(oracle) > 30);

        assertDataRangeMatches(ReadCase.of("full ring", 1), cfs, dataRange);
    }

    @Test
    public void interiorSubrangeSpanningBoundariesMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        assertTrue("need enough partitions to carve an interior band", keys.size() > 20);

        // (keys[6], keys[keys.size()-6]] : starts and ends mid-ring, spanning both sstables + memtable
        Range<Token> range = new Range<>(keys.get(6).getToken(), keys.get(keys.size() - 6).getToken());
        DataRange dataRange = DataRange.forTokenRange(range);

        List<String> oracle = render(oracle(cfs, dataRange));
        List<String> cursor = render(cursor(cfs, dataRange));
        compareRenderedRecords(oracle, cursor);

        int inRange = countPartitions(oracle);
        assertTrue("the band must be a proper subset of the ring", inRange > 0 && inRange < keys.size());

        assertDataRangeMatches(ReadCase.of("interior subrange", 2), cfs, dataRange);
    }

    @Test
    public void subrangeBetweenAdjacentPartitionsIsEmptyOnBothSides() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        IPartitioner partitioner = cfs.metadata().partitioner;

        Token start = keys.get(8).getToken();
        Token mid = partitioner.midpoint(keys.get(8).getToken(), keys.get(9).getToken());
        DataRange dataRange = DataRange.forTokenRange(new Range<>(start, mid));

        List<String> oracle = render(oracle(cfs, dataRange));
        List<String> cursor = render(cursor(cfs, dataRange));
        assertTrue("iterator path over an empty range must yield nothing", oracle.isEmpty());
        assertTrue("cursor path over an empty range must yield nothing", cursor.isEmpty());
        compareRenderedRecords(oracle, cursor);

        assertDataRangeMatches(ReadCase.of("empty subrange", 3), cfs, dataRange);
    }

    @Test
    public void singlePartitionSubrangeMatchesIteratorPath() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        List<DecoratedKey> keys = keysInTokenOrder(cfs);

        // pick a partition that lives in all three sources (pk in 20..24 overlaps A, B and memtable)
        DecoratedKey overlap = decorate(cfs, 22L);
        int idx = keys.indexOf(overlap);
        assertTrue("the overlapping partition must be present", idx > 0);
        Range<Token> range = new Range<>(keys.get(idx - 1).getToken(), keys.get(idx).getToken());
        DataRange dataRange = DataRange.forTokenRange(range);

        List<String> oracle = render(oracle(cfs, dataRange));
        List<String> cursor = render(cursor(cfs, dataRange));
        compareRenderedRecords(oracle, cursor);
        assertEquals("a (predecessor, key] range selects exactly one partition", 1, countPartitions(oracle));

        assertDataRangeMatches(ReadCase.of("single partition subrange", 4), cfs, dataRange);
    }

    private DecoratedKey decorate(ColumnFamilyStore cfs, long pk)
    {
        return cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(pk));
    }

    // ------------------------------------------------------------------ the two paths

    /** Oracle: the raw pre-purge merge PartitionRangeReadCommand.queryStorage feeds its transforms. */
    private UnfilteredPartitionIterator oracle(ColumnFamilyStore cfs, DataRange dataRange)
    {
        PartitionRangeReadCommand command = rangeCommand(cfs, dataRange);
        List<UnfilteredPartitionIterator> inputs = new ArrayList<>();
        for (Memtable memtable : cfs.select(View.selectLive(command.dataRange().keyRange())).memtables)
            inputs.add(memtable.partitionIterator(command.columnFilter(), command.dataRange(), SSTableReadsListener.NOOP_LISTENER));
        for (SSTableReader sstable : cfs.select(View.selectLive(command.dataRange().keyRange())).sstables)
            inputs.add(sstable.partitionIterator(command.columnFilter(), command.dataRange(), SSTableReadsListener.NOOP_LISTENER));
        return UnfilteredPartitionIterators.mergeLazily(inputs);
    }

    /** Cursor path: the cross-partition merge driver over the same sources. */
    private UnfilteredPartitionIterator cursor(ColumnFamilyStore cfs, DataRange dataRange)
    {
        PartitionRangeReadCommand command = rangeCommand(cfs, dataRange);
        List<Iterator<MemtablePartitionData>> memtables = new ArrayList<>();
        for (Memtable memtable : cfs.select(View.selectLive(command.dataRange().keyRange())).memtables)
            memtables.add(memtable.partitionData(command.dataRange()));
        List<SSTableReader> sstables = new ArrayList<>(cfs.select(View.selectLive(command.dataRange().keyRange())).sstables);
        return CursorRangeReads.rangeIterator(command, cfs, sstables, memtables, null, SSTableReadsListener.NOOP_LISTENER);
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

    private static String liveDt()
    {
        return dt(DeletionTime.LIVE.markedForDeleteAt(), DeletionTime.LIVE.localDeletionTime());
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
                fail(String.format("range merge divergence at record %d (iterator vs cursor):%n  iterator: %s%n  cursor:   %s%n  context:%s",
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
