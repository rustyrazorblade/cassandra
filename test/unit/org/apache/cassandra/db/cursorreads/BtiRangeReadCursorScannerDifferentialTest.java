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
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiTableScanner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Low-level scanner differential for the cursor range read: prove the low-level
 * cursor primitive can scan a MULTI-PARTITION token range and returns exactly what the legacy
 * read-path range scanner {@link BtiTableScanner} returns for the same range.
 *
 * <p>Both sides read the same BTI sstable over the same {@code Range<Token>}:
 * <ul>
 *   <li>the cursor side opens a bounded {@link SSTableCursorReader} over
 *       {@link SSTableReader#getPositionsForRanges} and walks its state machine partition by
 *       partition (this is the primitive {@code StatefulCursor} and cursor compaction already use
 *       for multi-partition reads);</li>
 *   <li>the scanner side opens a {@link BtiTableScanner} over the same range as a
 *       {@link DataRange}.</li>
 * </ul>
 * Both derive their byte segments from {@code Range.makeRowRange}, so the ranges mean the same
 * thing on both sides. Each side is rendered into an identical canonical record stream covering
 * partition keys and deletions, static rows, clustering, row liveness/deletion, cells (path, ttl,
 * timestamp, local deletion time, on-disk value bytes), complex-column deletions, and range
 * tombstone markers (bounds, boundaries and their deletion times). The two streams must match
 * exactly.
 *
 * <p>The dataset carries every shape the scanner must handle: dozens of partitions, partition boundaries,
 * an empty-of-rows partition (partition-level delete), a static-only partition, static rows on
 * multi-row partitions, and range tombstones. Ranges exercised: the full ring, a subrange spanning
 * several partition boundaries, a subrange that lands between two adjacent partitions (empty
 * result), and a single-partition subrange.
 */
public class BtiRangeReadCursorScannerDifferentialTest extends RangeReadOracle
{
    private static final int PARTITION_COUNT = 40;
    /** pk that carries only a static value and no clustering rows. */
    private static final long STATIC_ONLY_PK = 7;
    /** pk that carries only a partition-level deletion and no rows at all. */
    private static final long PARTITION_DELETE_PK = 13;

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

    /**
     * Builds one BTI sstable holding {@link #PARTITION_COUNT} partitions with a mix of shapes: a
     * static column, a complex (map) column, static rows, range tombstones, a static-only
     * partition and a partition-level delete.
     */
    private SSTableReader buildManyPartitionSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v1 bigint, v2 text, " +
                    "m map<text, text>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < PARTITION_COUNT; pk++)
        {
            if (pk == PARTITION_DELETE_PK)
            {
                // an empty-of-rows partition: only a partition-level deletion on disk
                execute("DELETE FROM %s WHERE pk = ?", pk);
                continue;
            }
            if (pk == STATIC_ONLY_PK)
            {
                // a partition with a static value but no clustering rows
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 100, pk);
                continue;
            }

            // a static value on every fourth partition, so static rows coexist with clustering rows
            if (pk % 4 == 0)
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 100, pk);

            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?)",
                        pk, ck, pk * 10 + ck, "v-" + pk + '-' + ck,
                        map("a" + ck, "x" + pk, "b" + ck, "y" + pk));

            // a range tombstone across the middle of every third partition, between live rows
            if (pk % 3 == 0)
                execute("DELETE FROM %s WHERE pk = ? AND ck > ? AND ck < ?", pk, 1L, 3L);
        }

        flush();
        assertEquals("expected a single sstable", 1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        assertTrue("expected a BTI sstable", sstable instanceof BtiTableReader);
        return sstable;
    }

    /** The sstable's partition keys in file order, which for one sstable is token order. */
    private static List<DecoratedKey> keysInTokenOrder(SSTableReader sstable)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    keys.add(partition.partitionKey());
                }
            }
        }
        return keys;
    }

    // ------------------------------------------------------------------ tests

    @Test
    public void fullRingRangeMatchesScanner() throws Throwable
    {
        SSTableReader sstable = buildManyPartitionSSTable();
        IPartitioner partitioner = sstable.getPartitioner();
        Range<Token> full = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());

        List<String> scanner = scannerRecords(sstable, DataRange.allData(partitioner));
        List<String> cursor = cursorRecords(sstable, sstable.getPositionsForRanges(Collections.singletonList(full)));
        compareRenderedRecords(scanner, cursor);

        // the dataset really did exercise every scanner shape through a range read
        assertEquals("expected every partition in the full ring", PARTITION_COUNT, countPartitions(scanner));
        assertTrue("dataset must contain a range tombstone marker", scanner.stream().anyMatch(r -> r.startsWith("MARKER")));
        assertTrue("dataset must contain a static row", scanner.stream().anyMatch(r -> r.startsWith("STATIC")));
        assertTrue("dataset must contain a partition-level deletion",
                   scanner.stream().anyMatch(r -> r.startsWith("PARTITION") && !r.endsWith("del=" + liveDt())));
        assertTrue("dataset must contain an empty-of-rows partition", scanner.stream().anyMatch(r -> r.contains("empty=true")));

        assertDataRangeMatches(ReadCase.of("full ring", 1), getCurrentColumnFamilyStore(), DataRange.allData(partitioner));
    }

    @Test
    public void subrangeSpanningPartitionBoundariesMatchesScanner() throws Throwable
    {
        SSTableReader sstable = buildManyPartitionSSTable();
        List<DecoratedKey> keys = keysInTokenOrder(sstable);
        assertEquals(PARTITION_COUNT, keys.size());

        // (keys[5], keys[32]]: interior partitions 6..32, so the segment starts and ends mid-file
        Range<Token> range = new Range<>(keys.get(5).getToken(), keys.get(32).getToken());

        List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(Collections.singletonList(range));
        assertEquals("the segment must be a single mid-file span", 1, bounds.size());
        assertTrue("the segment must start after the file start (a partial read)", bounds.get(0).lowerPosition > 0);
        assertTrue("the segment must end before the file end (a partial read)",
                   bounds.get(0).upperPosition < sstable.uncompressedLength());

        List<String> scanner = scannerRecords(sstable, DataRange.forTokenRange(range));
        List<String> cursor = cursorRecords(sstable, bounds);
        compareRenderedRecords(scanner, cursor);

        int inRange = countPartitions(scanner);
        assertEquals("a (start, end] token range holds the partitions strictly after start through end", 27, inRange);
        assertTrue("the subrange must be a proper subset of the ring", inRange < PARTITION_COUNT);

        assertDataRangeMatches(ReadCase.of("subrange", 2), getCurrentColumnFamilyStore(), DataRange.forTokenRange(range));
    }

    @Test
    public void subrangeBetweenAdjacentPartitionsIsEmptyOnBothSides() throws Throwable
    {
        SSTableReader sstable = buildManyPartitionSSTable();
        List<DecoratedKey> keys = keysInTokenOrder(sstable);
        IPartitioner partitioner = sstable.getPartitioner();

        // a token strictly between two adjacent partitions; no partition falls in (start, mid]
        Token start = keys.get(10).getToken();
        Token mid = partitioner.midpoint(keys.get(10).getToken(), keys.get(11).getToken());
        Range<Token> emptyRange = new Range<>(start, mid);

        List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(Collections.singletonList(emptyRange));
        assertTrue("a range that covers no partition must produce no byte segment", bounds.isEmpty());

        List<String> scanner = scannerRecords(sstable, DataRange.forTokenRange(emptyRange));
        List<String> cursor = cursorRecords(sstable, bounds);
        assertTrue("scanner over an empty range must yield nothing", scanner.isEmpty());
        assertTrue("cursor over an empty range must yield nothing", cursor.isEmpty());
        compareRenderedRecords(scanner, cursor);

        assertDataRangeMatches(ReadCase.of("empty subrange", 3), getCurrentColumnFamilyStore(), DataRange.forTokenRange(emptyRange));
    }

    @Test
    public void singlePartitionSubrangeMatchesScanner() throws Throwable
    {
        SSTableReader sstable = buildManyPartitionSSTable();
        List<DecoratedKey> keys = keysInTokenOrder(sstable);

        // (keys[17], keys[18]] selects exactly the one partition at token index 18
        Range<Token> range = new Range<>(keys.get(17).getToken(), keys.get(18).getToken());

        List<String> scanner = scannerRecords(sstable, DataRange.forTokenRange(range));
        List<String> cursor = cursorRecords(sstable, sstable.getPositionsForRanges(Collections.singletonList(range)));
        compareRenderedRecords(scanner, cursor);
        assertEquals("a (predecessor, key] range must select exactly one partition", 1, countPartitions(scanner));

        assertDataRangeMatches(ReadCase.of("single partition subrange", 4), getCurrentColumnFamilyStore(), DataRange.forTokenRange(range));
    }

    // ------------------------------------------------------------------ scanner (oracle) side

    private List<String> scannerRecords(SSTableReader sstable, DataRange dataRange)
    {
        TableMetadata metadata = sstable.metadata();
        List<String> out = new ArrayList<>();
        try (BtiTableScanner scanner = BtiTableScanner.getScanner((BtiTableReader) sstable,
                                                                  ColumnFilter.all(metadata),
                                                                  dataRange,
                                                                  SSTableReadsListener.NOOP_LISTENER))
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    boolean empty = partition.staticRow().isEmpty() && !partition.hasNext();
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

    // ------------------------------------------------------------------ cursor side

    private List<String> cursorRecords(SSTableReader sstable, List<PartitionPositionBounds> bounds) throws Exception
    {
        TableMetadata metadata = sstable.metadata();
        AbstractType<?>[] clusteringTypes = sstable.header.clusteringTypes().toArray(AbstractType[]::new);
        List<String> out = new ArrayList<>();

        try (SSTableCursorReader cursor = new SSTableCursorReader(sstable, bounds, DiskAccessMode.standard))
        {
            PartitionDescriptor pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            UnfilteredDescriptor rHeader = new UnfilteredDescriptor(clusteringTypes);
            byte[] transfer = new byte[4096];

            int state = cursor.state();
            while (state != DONE)
            {
                state = cursor.readPartitionHeader(pHeader);
                boolean[] sawContent = { false };
                List<String> partitionBody = new ArrayList<>();
                while (state != PARTITION_END)
                {
                    switch (state)
                    {
                        case STATIC_ROW_START:
                            state = cursor.readStaticRowHeader(rHeader);
                            // The raw cursor surfaces the static row unconditionally when the table
                            // has static columns, even when this partition set none.  The iterator
                            // path (and the production cursor read merge, via row.isEmpty()) drops an
                            // empty static row, so mirror that here: only emit a static row that
                            // actually carries content.
                            List<String> staticCells = new ArrayList<>();
                            state = readCells(cursor, state, transfer, staticCells);
                            boolean staticEmpty = staticCells.isEmpty()
                                                  && rHeader.livenessInfo().timestamp() == LivenessInfo.NO_TIMESTAMP
                                                  && rHeader.deletionTime().isLive();
                            if (!staticEmpty)
                            {
                                partitionBody.add(rowHeaderFromCursor("STATIC", rHeader, clusteringTypes));
                                partitionBody.addAll(staticCells);
                                sawContent[0] = true;
                            }
                            break;
                        case ROW_START:
                            state = cursor.readRowHeader(rHeader);
                            partitionBody.add(rowHeaderFromCursor("ROW", rHeader, clusteringTypes));
                            state = readCells(cursor, state, transfer, partitionBody);
                            sawContent[0] = true;
                            break;
                        case TOMBSTONE_START:
                            state = cursor.readTombstoneMarker(rHeader);
                            partitionBody.add(markerRecord(markerFromCursor(rHeader, clusteringTypes)));
                            sawContent[0] = true;
                            break;
                        default:
                            throw new IllegalStateException("unexpected state " + state);
                    }
                    if (state == UNFILTERED_END)
                        state = cursor.continueReading();
                }
                out.add(partitionRecord(pHeader.key(), pHeader.deletionTime(), !sawContent[0]));
                out.addAll(partitionBody);
                state = cursor.continueReading();
            }
        }
        return out;
    }

    /**
     * Reads every cell of the current row through the cursor, emitting the same {@code CPLX} and
     * {@code CELL} records the scanner side emits. Modeled on {@code ComplexColumnCursorReadTest}.
     */
    private static int readCells(SSTableCursorReader cursor, int state, byte[] transfer, List<String> out) throws IOException
    {
        SSTableCursorReader.CellCursor cc = cursor.cellCursor();
        String lastComplex = null;
        while (true)
        {
            if (state == UNFILTERED_END)
                return state;
            if (state == CELL_END)
            {
                state = cursor.continueReading();
                continue;
            }
            if (state != CELL_HEADER_START)
                throw new IllegalStateException("unexpected state " + state);

            state = cursor.readCellHeader();
            boolean atPosition = cc.cellProduced() || state == CELL_END;
            if (atPosition && cc.cellColumn().isComplex() && !cc.cellColumn().name.toString().equals(lastComplex))
            {
                lastComplex = cc.cellColumn().name.toString();
                // readCellHeader already subtracted the cell it just surfaced from the column's count
                int cellsInColumn = cc.cellProduced() ? cc.remainingCellsInColumn + 1 : 0;
                out.add(complexHeader(cc.cellColumn().name.toString(),
                                      cc.complexDeletion.markedForDeleteAt(), cc.complexDeletion.localDeletionTime(),
                                      cellsInColumn));
            }
            if (!cc.cellProduced())
                continue;

            ByteBuffer path = cc.cellPathLength() < 0
                              ? null
                              : ByteBuffer.wrap(java.util.Arrays.copyOf(cc.cellPathBuffer(), cc.cellPathLength()));
            byte[] value = new byte[0];
            if (state == CELL_VALUE_START)
            {
                try (DataOutputBuffer dob = new DataOutputBuffer())
                {
                    state = cursor.copyCellValue(dob, transfer);
                    value = dob.toByteArray();
                }
            }
            out.add(cellRecord(cc.cellColumn().name.toString(), path,
                               cc.cellLiveness().timestamp(), cc.cellLiveness().ttl(), cc.cellLiveness().localDeletionTime(),
                               value));
        }
    }

    private static String rowHeaderFromCursor(String label, UnfilteredDescriptor rHeader, AbstractType<?>[] clusteringTypesList)
    {
        String clustering = label.equals("STATIC") ? "STATIC" : clusteringString(rHeader.toClusteringPrefix(rHeader.clusteringTypesList()));
        return rowHeader(label, clustering,
                         rHeader.livenessInfo().timestamp(),
                         rHeader.livenessInfo().ttl(),
                         rHeader.livenessInfo().localExpirationTime(),
                         rHeader.deletionTime().markedForDeleteAt(),
                         rHeader.deletionTime().localDeletionTime(),
                         rHeader.isShadowableDeletion());
    }

    /** Rebuilds a range tombstone marker from the cursor's descriptor, so the identical
     *  {@link #markerRecord} renders both sides and any rendering asymmetry is ruled out. */
    private static RangeTombstoneMarker markerFromCursor(UnfilteredDescriptor rHeader, AbstractType<?>[] clusteringTypesList)
    {
        ClusteringPrefix<?> prefix = rHeader.toClusteringPrefix(rHeader.clusteringTypesList());
        if (rHeader.isBoundary())
        {
            DeletionTime close = DeletionTime.build(rHeader.deletionTime().markedForDeleteAt(), rHeader.deletionTime().localDeletionTime());
            DeletionTime open = DeletionTime.build(rHeader.deletionTime2().markedForDeleteAt(), rHeader.deletionTime2().localDeletionTime());
            return new RangeTombstoneBoundaryMarker((ClusteringBoundary<?>) prefix, close, open);
        }
        DeletionTime del = DeletionTime.build(rHeader.deletionTime().markedForDeleteAt(), rHeader.deletionTime().localDeletionTime());
        return new RangeTombstoneBoundMarker((ClusteringBound<?>) prefix, del);
    }

    // ------------------------------------------------------------------ shared rendering

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

    // ------------------------------------------------------------------ comparison

    private static void compareRenderedRecords(List<String> scanner, List<String> cursor)
    {
        int max = Math.max(scanner.size(), cursor.size());
        for (int i = 0; i < max; i++)
        {
            String expected = i < scanner.size() ? scanner.get(i) : "<missing>";
            String actual = i < cursor.size() ? cursor.get(i) : "<missing>";
            if (!expected.equals(actual))
                fail(String.format("range read divergence at record %d (scanner vs cursor):%n  scanner: %s%n  cursor:  %s%n  context:%s",
                                   i, expected, actual, context(scanner, cursor, i)));
        }
        assertEquals("record counts diverged", scanner.size(), cursor.size());
    }

    private static String context(List<String> scanner, List<String> cursor, int index)
    {
        StringBuilder sb = new StringBuilder();
        for (int j = Math.max(0, index - 3); j < Math.min(scanner.size(), index + 4); j++)
            sb.append(String.format("%n    sc[%d]=%s", j, scanner.get(j)));
        for (int j = Math.max(0, index - 3); j < Math.min(cursor.size(), index + 4); j++)
            sb.append(String.format("%n    cu[%d]=%s", j, cursor.get(j)));
        return sb.toString();
    }
}
