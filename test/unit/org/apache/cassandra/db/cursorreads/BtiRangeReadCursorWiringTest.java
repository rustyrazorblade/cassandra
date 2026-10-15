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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end wiring test for the cursor range read: prove the command
 * wiring. A real range read must engage the cursor path end-to-end when supported, and fall back to
 * the iterator path otherwise, producing results identical to today.
 *
 * <p>The oracle is the iterator path itself: every scenario runs the SAME range read through
 * {@code PartitionRangeReadCommand.executeLocally} (which calls {@code queryStorage}, the wiring
 * point) once with {@code cursor_reads_enabled=false} and once with it on, and asserts both the
 * canonical record stream AND the intra-node {@code ReadResponse} bytes are identical. For supported
 * shapes it also asserts the cursor path actually engaged (via {@link CursorRangeReads#rangeReadsServed})
 * so a silent fallback cannot make the test pass trivially; for unsupported shapes it asserts the
 * cursor path did NOT engage (true fallback).
 *
 * <p>Shapes covered against a BTI table with data across two sstables AND the memtable, with statics
 * (sparse), a partition-level delete, and range tombstones: a full scan (full ring), a token-range
 * restriction, an empty result range, cross-source overlap, and paging (both a mid-partition
 * {@code Paging} DataRange and a multi-page network fetch, which the cursor path now serves on page 2+);
 * and, as fallbacks, a reversed range and a secondary-index query.
 */
public class BtiRangeReadCursorWiringTest extends RangeReadOracle
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void selectBti()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setCursorRangeReadsEnabled(false);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    // ------------------------------------------------------------------ fixture

    /**
     * A BTI table split across two sstables + the live memtable with real cross-source overlap, a
     * static column set on only some partitions (sparse statics -> the raw cursor's empty static row
     * that the merge must drop), a static-only partition, a partition-level delete, and range
     * tombstones spanning rows.
     */
    private ColumnFamilyStore buildOverlappingSources() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v1 bigint, v2 text, " +
                    "m map<text, text>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 25; pk++)
        {
            if (pk == 13) { execute("DELETE FROM %s WHERE pk = ?", pk); continue; }         // partition-level delete
            if (pk == 7)  { execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 100, pk); continue; } // static-only
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

        for (long pk = 10; pk < 35; pk++)
        {
            if (pk % 5 == 0)
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 1000, pk);
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

        for (long pk = 20; pk < 31; pk++)
        {
            if (pk % 3 == 0)
                execute("UPDATE %s SET s1 = ? WHERE pk = ?", pk * 10000, pk);
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 30 + ck, "M-" + pk + '-' + ck, map("c" + ck, "m" + pk), 3000L);
        }
        for (long ck = 0; ck < 3; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP ?", 100L, ck, ck, 3000L);

        assertEquals("the memtable must hold unflushed data", 2, cfs.getLiveSSTables().size());
        return cfs;
    }

    // ------------------------------------------------------------------ supported shapes

    @Test
    public void fullRingEngagesCursorAndMatches() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        DataRange dataRange = DataRange.allData(cfs.metadata().partitioner);

        List<String> iterator = assertRangeMatchesAndEngages(cfs, dataRange);

        // the dataset really did exercise the partition shapes through the wired range read
        assertTrue("dataset must contain a range tombstone marker", iterator.stream().anyMatch(r -> r.startsWith("MARKER")));
        assertTrue("dataset must contain a static row", iterator.stream().anyMatch(r -> r.startsWith("STATIC")));
        assertTrue("dataset must span many partitions", countPartitions(iterator) > 30);
    }

    @Test
    public void tokenRangeRestrictionEngagesCursorAndMatches() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        assertTrue("need enough partitions to carve an interior band", keys.size() > 20);

        Range<Token> range = new Range<>(keys.get(6).getToken(), keys.get(keys.size() - 6).getToken());
        DataRange dataRange = DataRange.forTokenRange(range);

        List<String> iterator = assertRangeMatchesAndEngages(cfs, dataRange);
        int inRange = countPartitions(iterator);
        assertTrue("the band must be a proper subset of the ring", inRange > 0 && inRange < keys.size());
    }

    @Test
    public void emptyRangeEngagesCursorAndMatches() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        IPartitioner partitioner = cfs.metadata().partitioner;

        Token start = keys.get(8).getToken();
        Token mid = partitioner.midpoint(keys.get(8).getToken(), keys.get(9).getToken());
        DataRange dataRange = DataRange.forTokenRange(new Range<>(start, mid));

        List<String> iterator = assertRangeMatchesAndEngages(cfs, dataRange);
        assertTrue("range between two adjacent partitions must be empty", iterator.isEmpty());
    }

    @Test
    public void singlePartitionSubrangeEngagesCursorAndMatches() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        List<DecoratedKey> keys = keysInTokenOrder(cfs);

        DecoratedKey overlap = cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(22L));
        int idx = keys.indexOf(overlap);
        assertTrue("the overlapping partition must be present", idx > 0);
        DataRange dataRange = DataRange.forTokenRange(new Range<>(keys.get(idx - 1).getToken(), keys.get(idx).getToken()));

        List<String> iterator = assertRangeMatchesAndEngages(cfs, dataRange);
        assertEquals("a (predecessor, key] range selects exactly one partition", 1, countPartitions(iterator));
    }

    @Test
    public void fullScanCqlRowsMatch() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();

        DatabaseDescriptor.setCursorRangeReadsEnabled(false);
        List<String> off = renderNet(executeNetWithoutPaging("SELECT * FROM %s"));

        DatabaseDescriptor.setCursorRangeReadsEnabled(true);
        long servedBefore = CursorRangeReads.rangeReadsServed();
        List<String> on = renderNet(executeNetWithoutPaging("SELECT * FROM %s"));
        assertTrue("full-scan SELECT did not engage the cursor range path",
                   CursorRangeReads.rangeReadsServed() > servedBefore);
        assertEquals("full-scan CQL rows diverged between iterator and cursor paths", off, on);

        assertCqlOnEverySurface(ReadCase.of("full scan", 101), cfs, "SELECT * FROM %s", 7);
    }

    /**
     * A {@code Paging} DataRange resuming MID-PARTITION: the start partition carries a start-bounded
     * slice ({@code ck > lastReturned}) while every later partition carries the full template filter.
     * This is the shape the per-partition clustering filter matters most for. The cursor path must serve
     * it (the driver applies {@code dataRange().clusteringIndexFilter(key)} per key), byte-identical to
     * the iterator path over the same Paging range.
     */
    @Test
    public void pagingDataRangeMidPartitionEngagesCursorAndMatches() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        // pk=22 carries rows across both sstables and the memtable, so a mid-partition slice is real.
        DecoratedKey startKey = cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(22L));
        DataRange paging = midPartitionPaging(cfs, startKey, 1L);
        assertTrue("start partition must exist in token order", keysInTokenOrder(cfs).contains(startKey));

        List<String> iterator = assertRangeMatchesAndEngages(cfs, paging);
        assertTrue("paged range must span the start partition and later ones", countPartitions(iterator) > 1);
    }

    /**
     * Full network paged fetch with a small page size, so pages land MID-partition (partitions hold
     * more rows than the page size) and there are many pages. With the cursor path on, later pages
     * (each a {@code Paging} DataRange) must engage the cursor too, not just page 1: a multi-page fetch
     * therefore serves strictly more range reads than a single-page (one big page) fetch of the same
     * ring, which is only possible if page 2+ used the cursor. Rows must match the iterator path.
     */
    @Test
    public void pagedFetchEngagesCursorAcrossPagesAndMatches() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();

        DatabaseDescriptor.setCursorRangeReadsEnabled(false);
        List<String> off = renderNet(executeNetWithPaging("SELECT * FROM %s", 7));

        DatabaseDescriptor.setCursorRangeReadsEnabled(true);
        // Baseline: a single big page. Its range reads == the ring's split count for one page.
        long beforeSingle = CursorRangeReads.rangeReadsServed();
        renderNet(executeNetWithPaging("SELECT * FROM %s", 1 << 20));
        long singlePageReads = CursorRangeReads.rangeReadsServed() - beforeSingle;

        long beforeMulti = CursorRangeReads.rangeReadsServed();
        List<String> on = renderNet(executeNetWithPaging("SELECT * FROM %s", 7));
        long multiPageReads = CursorRangeReads.rangeReadsServed() - beforeMulti;

        assertEquals("paged CQL rows diverged between iterator and cursor paths", off, on);
        assertTrue("paged fetch returned nothing (fixture drifted)", !on.isEmpty());
        assertTrue("later pages did not engage the cursor path (multi-page served " + multiPageReads
                   + " range reads vs single-page " + singlePageReads + ')',
                   multiPageReads > singlePageReads);

        assertCqlOnEverySurface(ReadCase.of("paged fetch", 102), cfs, "SELECT * FROM %s", 7);
    }

    /**
     * A LIMIT-capped full scan over a table with tombstone-shadowed rows (a partition-level delete and
     * range tombstones). The DataLimits counting sits directly below short-read protection on the
     * coordinator; this asserts the cursor path's {@code UnfilteredPartitionIterator} feeds that limiting
     * / row-counting machinery exactly as the iterator path does (same rows, same cap), the single-node
     * complement to the multi-node {@code ShortReadProtectionTest} exercise. The multi-node SRP re-query
     * itself needs replica disagreement, so it is proven by that jvm dtest with the cursor flag on.
     */
    @Test
    public void limitedFullScanOverTombstonesEngagesCursorAndMatches() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();

        DatabaseDescriptor.setCursorRangeReadsEnabled(false);
        List<String> off = renderNet(executeNetWithoutPaging("SELECT * FROM %s LIMIT 37"));

        DatabaseDescriptor.setCursorRangeReadsEnabled(true);
        long before = CursorRangeReads.rangeReadsServed();
        List<String> on = renderNet(executeNetWithoutPaging("SELECT * FROM %s LIMIT 37"));
        assertTrue("limited full scan did not engage the cursor range path",
                   CursorRangeReads.rangeReadsServed() > before);
        assertEquals("limited scan rows diverged between iterator and cursor paths", off, on);
        assertEquals("LIMIT must cap the returned row count", 37, on.size());

        assertCqlOnEverySurface(ReadCase.of("limited full scan", 103), cfs, "SELECT * FROM %s LIMIT 37", 7);
    }

    @Test
    public void reversedRangeEngagesCursorAndMatches() throws Throwable
    {
        ColumnFamilyStore cfs = buildOverlappingSources();
        DataRange reversed = DataRange.allData(cfs.metadata().partitioner,
                                               new ClusteringIndexSliceFilter(Slices.ALL, true));

        // A reversed range (ORDER BY ... DESC) now passes the cursor gate and engages the cursor
        // range path; each sstable leg finishes as a lazy reverse iterator and the legs reconcile
        // in the shared object merge, so the results match the iterator path exactly.
        assertRangeMatchesAndEngages(cfs, reversed);
    }

    // ------------------------------------------------------------------ fallbacks

    @Test
    public void secondaryIndexQueryFallsBackAndMatches() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        createIndex("CREATE INDEX ON %s (v)");
        for (long pk = 0; pk < 40; pk++)
            for (long ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, pk % 4);
        flush();
        for (long pk = 0; pk < 40; pk += 5)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, 2L);

        DatabaseDescriptor.setCursorRangeReadsEnabled(false);
        List<String> off = renderNet(executeNetWithoutPaging("SELECT * FROM %s WHERE v = 2"));

        DatabaseDescriptor.setCursorRangeReadsEnabled(true);
        long servedBefore = CursorRangeReads.rangeReadsServed();
        List<String> on = renderNet(executeNetWithoutPaging("SELECT * FROM %s WHERE v = 2"));
        assertEquals("a 2i table is outside cursor support and must stay on the iterator path",
                     servedBefore, CursorRangeReads.rangeReadsServed());
        assertEquals("2i CQL rows diverged between iterator and (fallback) paths", off, on);
        assertTrue("2i query returned nothing (fixture drifted)", !on.isEmpty());
    }

    // ------------------------------------------------------------------ differential harness

    /** Runs the range read flag-off then flag-on, asserts records + response bytes match and the cursor
     *  path engaged; returns the iterator-path canonical records for extra fixture assertions. */
    private List<String> assertRangeMatchesAndEngages(ColumnFamilyStore cfs, DataRange dataRange)
    {
        DatabaseDescriptor.setCursorRangeReadsEnabled(false);
        List<String> iterator = renderRange(cfs, dataRange);
        byte[] iteratorBytes = responseBytes(cfs, dataRange);

        DatabaseDescriptor.setCursorRangeReadsEnabled(true);
        assertTrue("scenario is not supported by the cursor range gate; this run would compare iterator vs iterator",
                   CursorRangeReads.unsupported(rangeCommand(cfs, dataRange), cfs, ReadExecutionController.empty(), liveSSTables(cfs, dataRange)) == null);
        long servedBefore = CursorRangeReads.rangeReadsServed();
        List<String> cursor = renderRange(cfs, dataRange);
        byte[] cursorBytes = responseBytes(cfs, dataRange);
        assertTrue("cursor range path did not engage (silent fallback?)",
                   CursorRangeReads.rangeReadsServed() > servedBefore);

        compareRenderedRecords(iterator, cursor);
        assertBytesEqual(iteratorBytes, cursorBytes);
        assertDataRangeMatches(ReadCase.of(dataRange.toString(), ++oracleCases), cfs, dataRange);
        return iterator;
    }

    /** The oracle cases run, which seed them. */
    private long oracleCases;

    /** For unsupported shapes: flag-on results must equal flag-off results and the cursor must not run. */
    private void assertRangeFallsBack(ColumnFamilyStore cfs, DataRange dataRange)
    {
        DatabaseDescriptor.setCursorRangeReadsEnabled(false);
        List<String> iterator = renderRange(cfs, dataRange);
        byte[] iteratorBytes = responseBytes(cfs, dataRange);

        DatabaseDescriptor.setCursorRangeReadsEnabled(true);
        long servedBefore = CursorRangeReads.rangeReadsServed();
        List<String> fallback = renderRange(cfs, dataRange);
        byte[] fallbackBytes = responseBytes(cfs, dataRange);
        assertEquals("cursor range path ran for a gated-out command", servedBefore, CursorRangeReads.rangeReadsServed());

        compareRenderedRecords(iterator, fallback);
        assertBytesEqual(iteratorBytes, fallbackBytes);
    }

    /**
     * Builds a {@code Paging} DataRange that resumes inside {@code lastReturnedKey} after clustering
     * {@code ck = lastReturnedCk} (exclusive), exactly as {@code PartitionRangeQueryPager} does for a
     * mid-partition resume: an inclusive left bound on the last key plus {@code forPaging(..., false)}.
     */
    private static DataRange midPartitionPaging(ColumnFamilyStore cfs, DecoratedKey lastReturnedKey, long lastReturnedCk)
    {
        DataRange full = DataRange.allData(cfs.metadata().partitioner);
        AbstractBounds<PartitionPosition> bounds = new Bounds<>(lastReturnedKey, full.keyRange().right);
        Clustering<?> lastReturned = Clustering.make(ByteBufferUtil.bytes(lastReturnedCk));
        return full.forPaging(bounds, cfs.metadata().comparator, lastReturned, false);
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

    private static List<SSTableReader> liveSSTables(ColumnFamilyStore cfs, DataRange dataRange)
    {
        return new ArrayList<>(cfs.select(View.selectLive(dataRange.keyRange())).sstables);
    }

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

    // ------------------------------------------------------------------ rendering (command level)

    private List<String> renderRange(ColumnFamilyStore cfs, DataRange dataRange)
    {
        PartitionRangeReadCommand command = rangeCommand(cfs, dataRange);
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            return renderStream(partitions);
        }
    }

    private byte[] responseBytes(ColumnFamilyStore cfs, DataRange dataRange)
    {
        PartitionRangeReadCommand command = rangeCommand(cfs, dataRange);
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller);
             DataOutputBuffer buffer = new DataOutputBuffer())
        {
            UnfilteredPartitionIterators.serializerForIntraNode()
                                        .serialize(partitions, command.columnFilter(), buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
        catch (Exception e)
        {
            throw new AssertionError("failed to serialize range read response", e);
        }
    }

    private static List<String> renderStream(UnfilteredPartitionIterator partitions)
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

    private static void appendRow(String label, Row row, List<String> out)
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
        catch (java.io.IOException e)
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

    // ------------------------------------------------------------------ CQL rendering

    private static List<String> renderNet(ResultSet rs)
    {
        List<String> out = new ArrayList<>();
        for (com.datastax.driver.core.Row row : rs)
        {
            int n = row.getColumnDefinitions().size();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < n; i++)
            {
                ByteBuffer bytes = row.getBytesUnsafe(i);
                sb.append(i == 0 ? "" : "|").append(bytes == null ? "null" : ByteBufferUtil.bytesToHex(bytes));
            }
            out.add(sb.toString());
        }
        return out;
    }

    // ------------------------------------------------------------------ comparison

    private static void compareRenderedRecords(List<String> iterator, List<String> cursor)
    {
        int max = Math.max(iterator.size(), cursor.size());
        for (int i = 0; i < max; i++)
        {
            String expected = i < iterator.size() ? iterator.get(i) : "<missing>";
            String actual = i < cursor.size() ? cursor.get(i) : "<missing>";
            if (!expected.equals(actual))
                fail(String.format("range read divergence at record %d (iterator vs cursor):%n  iterator: %s%n  cursor:   %s",
                                   i, expected, actual));
        }
        assertEquals("record counts diverged", iterator.size(), cursor.size());
    }

    private static void assertBytesEqual(byte[] iterator, byte[] cursor)
    {
        int mismatch = java.util.Arrays.mismatch(iterator, cursor);
        if (mismatch >= 0)
            fail(String.format("intra-node ReadResponse bytes diverged: lengths %d vs %d, first at offset %d",
                               iterator.length, cursor.length, mismatch));
    }
}
