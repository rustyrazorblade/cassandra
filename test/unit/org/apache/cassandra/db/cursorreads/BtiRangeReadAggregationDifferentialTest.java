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
import java.util.function.Supplier;

import com.datastax.driver.core.ResultSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Differential coverage for AGGREGATION over the cursor partition-range read path.
 *
 * <p>A full-table {@code SELECT count(*) FROM t} (no WHERE) is a {@code PartitionRangeReadCommand}.
 * The gate does not inspect {@code DataLimits} or {@code RowFilter}, so aggregation does not disable
 * the cursor range path; the count / GROUP BY machinery is applied ABOVE the iterator the cursor path
 * produces. This test proves that machinery sees the same rows on both paths.
 *
 * <p>Each scenario runs the SAME aggregate query once with {@code cursor_reads_enabled=false} and once
 * with it on, asserts the driver results are identical, and asserts the cursor range path actually
 * engaged (via {@link CursorRangeReads#rangeReadsServed}) so a silent fallback cannot make the test
 * pass trivially. As a negative control it also asserts the flag-OFF run does NOT touch the counter,
 * so a stuck-on flag cannot compare cursor-vs-cursor and pass. It covers {@code count(*)}, {@code
 * count(v)}, and {@code GROUP BY} aggregates across multiple partitions with tombstones, expired TTL
 * cells, null / cell-deleted columns, network paging, and an all-deleted table.
 */
public class BtiRangeReadAggregationDifferentialTest extends RangeReadOracle
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

    // ------------------------------------------------------------------ fixtures

    /**
     * A BTI table with data across two sstables plus the live memtable, spanning many partitions.
     * Some clusterings are removed by range tombstones, and one partition is deleted whole.
     * Deterministic timestamps keep the two paths comparable.
     */
    private ColumnFamilyStore buildMultiPartitionTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 20; pk++)
        {
            if (pk == 13) { execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", 1500L, pk); continue; }
            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 10 + ck, 1000L);
            if (pk % 3 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck > ? AND ck < ?", 1001L, pk, 1L, 3L);
        }
        flush();

        for (long pk = 10; pk < 30; pk++)
        {
            for (long ck = 3; ck < 8; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 20 + ck, 2000L);
            if (pk % 4 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck > ? AND ck < ?", 2001L, pk, 4L, 7L);
        }
        flush();

        assertEquals("fixture expects exactly two sstables", 2, cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue("expected a BTI sstable", sstable instanceof BtiTableReader);

        // live memtable data, overlapping the sstable key range
        long sstablesBeforeMemtableWrites = cfs.getLiveSSTables().size();
        for (long pk = 15; pk < 25; pk++)
            for (long ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, pk * 30 + ck, 3000L);

        assertEquals("memtable writes must not have flushed (sstable count unchanged)",
                     sstablesBeforeMemtableWrites, cfs.getLiveSSTables().size());
        return cfs;
    }

    /**
     * A BTI table whose every partition is deleted, but whose sstables and memtable still hold sources
     * for the range scan. The aggregate result must be zero rows counted, on both paths.
     */
    private ColumnFamilyStore buildAllDeletedTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 15; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, ck, pk + ck, 1000L);
        flush();

        // delete every partition at a strictly higher timestamp; sources remain on disk + memtable
        for (long pk = 0; pk < 15; pk++)
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", 2000L, pk);
        flush();

        assertEquals("fixture expects two sstables (data + tombstones)", 2, cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue("expected a BTI sstable", sstable instanceof BtiTableReader);
        return cfs;
    }

    /**
     * A BTI table where a band of rows carries a 1-second TTL and is already expired at read time.
     * The expired band overlaps permanent rows on the same clusterings at a higher timestamp, so the
     * expired cells shadow live ones. Both paths must reconcile and purge these cells identically.
     */
    private ColumnFamilyStore buildExpiredTtlTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // permanent rows (no TTL), lower timestamp
        for (long pk = 0; pk < 15; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, ck, pk * 10 + ck, 1000L);
        flush();

        // short-TTL rows at a higher timestamp; a band overlaps the permanent rows (pk 8..14)
        for (long pk = 8; pk < 22; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL 1",
                        pk, ck, pk * 100 + ck, 2000L);
        flush();

        assertEquals("fixture expects two sstables", 2, cfs.getLiveSSTables().size());

        // wait past the TTL so the short-TTL cells are expired (but uncollected) at read time
        Thread.sleep(2500L);
        return cfs;
    }

    /**
     * A BTI table where some rows have a null {@code v} (row exists via row liveness, cell never set)
     * and some have {@code v} explicitly cell-deleted. {@code count(*)} counts the rows; {@code
     * count(v)} counts only rows with a live {@code v} cell, so the two diverge. Both paths must agree.
     */
    private ColumnFamilyStore buildNullColumnTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 15; pk++)
        {
            for (long ck = 0; ck < 4; ck++)
            {
                if (ck == 0)
                    execute("INSERT INTO %s (pk, ck) VALUES (?, ?) USING TIMESTAMP ?", pk, ck, 1000L); // v null
                else
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                            pk, ck, pk * 10 + ck, 1000L);
            }
            // cell-delete v on one populated row per partition
            execute("DELETE v FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", 1001L, pk, 1L);
        }
        flush();

        // memtable rows: more nulls, overlapping key range
        for (long pk = 10; pk < 20; pk++)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?) USING TIMESTAMP ?", pk, 5L, 2000L);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue("expected a BTI sstable", sstable instanceof BtiTableReader);
        return cfs;
    }

    // ------------------------------------------------------------------ tests

    @Test
    public void fullScanCountStarEngagesCursorAndMatches() throws Throwable
    {
        buildMultiPartitionTable();

        List<String> off = countOnBothPaths("SELECT count(*) FROM %s");
        long count = Long.parseLong(off.get(0));
        assertTrue("count(*) over a populated table must be positive", count > 0);
    }

    @Test
    public void groupByPartitionKeyCountEngagesCursorAndMatches() throws Throwable
    {
        buildMultiPartitionTable();

        List<String> rows = countOnBothPaths("SELECT pk, count(*) FROM %s GROUP BY pk");
        assertTrue("GROUP BY pk must return several partition groups", rows.size() > 5);
    }

    @Test
    public void groupByPartitionAndClusteringEngagesCursorAndMatches() throws Throwable
    {
        buildMultiPartitionTable();

        List<String> rows = countOnBothPaths("SELECT pk, ck, count(*) FROM %s GROUP BY pk, ck");
        assertTrue("GROUP BY pk, ck must return many row groups", rows.size() > 20);
    }

    @Test
    public void groupByUnderPagingEngagesCursorAcrossPagesAndMatches() throws Throwable
    {
        buildMultiPartitionTable();

        // negative control: flag-off paged run must not engage the cursor path
        long servedAtStart = CursorRangeReads.rangeReadsServed();
        List<String> off = renderNet(runWithFlag(false, () -> executeNetWithPaging("SELECT pk, count(*) FROM %s GROUP BY pk", 2)));
        assertEquals("flag-off paged run must not engage the cursor range path",
                     servedAtStart, CursorRangeReads.rangeReadsServed());

        // one big page: the range-read count for a single page over the ring
        long beforeSingle = CursorRangeReads.rangeReadsServed();
        renderNet(runWithFlag(true, () -> executeNetWithPaging("SELECT pk, count(*) FROM %s GROUP BY pk", 1 << 20)));
        long singlePageReads = CursorRangeReads.rangeReadsServed() - beforeSingle;

        long beforeMulti = CursorRangeReads.rangeReadsServed();
        List<String> on = renderNet(runWithFlag(true, () -> executeNetWithPaging("SELECT pk, count(*) FROM %s GROUP BY pk", 2)));
        long multiPageReads = CursorRangeReads.rangeReadsServed() - beforeMulti;

        compareRows(off, on);
        assertTrue("paged GROUP BY returned nothing (fixture drifted)", !on.isEmpty());
        assertTrue("later pages did not engage the cursor path (multi-page served " + multiPageReads
                   + " range reads vs single-page " + singlePageReads + ')',
                   multiPageReads > singlePageReads);
        assertCqlOnEverySurface(ReadCase.of("paged GROUP BY", ++oracleCases), getCurrentColumnFamilyStore(), "SELECT pk, count(*) FROM %s GROUP BY pk", 2);
    }

    @Test
    public void countOverExpiredTtlEngagesCursorAndMatches() throws Throwable
    {
        buildExpiredTtlTable();

        countOnBothPaths("SELECT count(*) FROM %s");
        countOnBothPaths("SELECT count(v) FROM %s");
    }

    @Test
    public void countStarVsCountColumnOverNullsEngagesCursorAndMatches() throws Throwable
    {
        buildNullColumnTable();

        List<String> star = countOnBothPaths("SELECT count(*) FROM %s");
        List<String> col = countOnBothPaths("SELECT count(v) FROM %s");
        assertTrue("count(*) must exceed count(v) when some rows have null / deleted v",
                   Long.parseLong(star.get(0)) > Long.parseLong(col.get(0)));
    }

    @Test
    public void countStarOverAllDeletedEngagesCursorAndMatchesZero() throws Throwable
    {
        buildAllDeletedTable();

        List<String> rows = countOnBothPaths("SELECT count(*) FROM %s");
        assertEquals("count(*) over an all-deleted table must be zero", "0", rows.get(0));
    }

    @Test
    public void groupByOverAllDeletedEngagesCursorAndMatchesEmpty() throws Throwable
    {
        buildAllDeletedTable();

        List<String> rows = countOnBothPaths("SELECT pk, count(*) FROM %s GROUP BY pk");
        assertTrue("GROUP BY pk over an all-deleted table must return no groups", rows.isEmpty());
    }

    // ------------------------------------------------------------------ differential harness

    /**
     * Runs the aggregate query flag-off then flag-on, asserts the driver results match, that the
     * flag-off run did NOT engage the cursor range path, and that the flag-on run DID; returns the
     * flag-off rendered rows for extra per-scenario assertions.
     */
    private List<String> countOnBothPaths(String query)
    {
        long servedAtStart = CursorRangeReads.rangeReadsServed();
        List<String> off = renderNet(runWithFlag(false, () -> executeNetWithoutPaging(query)));
        assertEquals("flag-off run must not engage the cursor range path",
                     servedAtStart, CursorRangeReads.rangeReadsServed());

        long servedBeforeOn = CursorRangeReads.rangeReadsServed();
        List<String> on = renderNet(runWithFlag(true, () -> executeNetWithoutPaging(query)));
        assertTrue("aggregate full scan did not engage the cursor range path",
                   CursorRangeReads.rangeReadsServed() > servedBeforeOn);

        compareRows(off, on);
        assertCqlOnEverySurface(ReadCase.of(query, ++oracleCases), getCurrentColumnFamilyStore(), query, 2);
        return off;
    }

    /** The oracle cases run, which seed them. */
    private long oracleCases;

    /** Sets the cursor flag for one run and restores the previous value even if the run throws. */
    private ResultSet runWithFlag(boolean cursorReads, Supplier<ResultSet> run)
    {
        boolean previous = DatabaseDescriptor.cursorRangeReadsEnabled();
        DatabaseDescriptor.setCursorRangeReadsEnabled(cursorReads);
        try
        {
            return run.get();
        }
        finally
        {
            DatabaseDescriptor.setCursorRangeReadsEnabled(previous);
        }
    }

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
                sb.append(i == 0 ? "" : "|").append(bytes == null ? "null" : row.getLong(i));
            }
            out.add(sb.toString());
        }
        return out;
    }

    private static void compareRows(List<String> iterator, List<String> cursor)
    {
        int max = Math.max(iterator.size(), cursor.size());
        for (int i = 0; i < max; i++)
        {
            String expected = i < iterator.size() ? iterator.get(i) : "<missing>";
            String actual = i < cursor.size() ? cursor.get(i) : "<missing>";
            if (!expected.equals(actual))
                fail(String.format("aggregate divergence at row %d (iterator vs cursor):%n  iterator: %s%n  cursor:   %s",
                                   i, expected, actual));
        }
        assertEquals("aggregate row counts diverged", iterator.size(), cursor.size());
    }
}
