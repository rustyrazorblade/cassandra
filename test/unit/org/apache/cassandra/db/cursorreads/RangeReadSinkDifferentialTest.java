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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeScan;
import org.apache.cassandra.db.CursorReadUnsupportedException;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReconciledRowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.cursorreads.RangeScanTesting.assertLinesEqual;
import static org.apache.cassandra.db.cursorreads.RangeScanTesting.responseLines;
import static org.apache.cassandra.db.cursorreads.RangeScanTesting.scanLines;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link CursorRangeScan} into a {@link org.apache.cassandra.db.RangeReadSink} against the replica
 * data response of the iterator range path, the reference, on seeded random data and filters: what
 * the sink gets must be what the response carries, partition by partition, row by row, cell by cell.
 *
 * <p>The filters cover every operator the scan takes, on partition key, clustering, static and
 * regular columns: comparisons and {@code IN}, frozen collections and a frozen UDT, {@code CONTAINS},
 * {@code CONTAINS KEY} and their {@code NOT} forms on non-frozen collections, map elements, and
 * counters, alone and ANDed, with limits and token ranges.  The data has TTLs that expire between
 * the writes and the reads, row, range, cell and partition deletions (purgeable or not), static-only
 * partitions and partitions every row of which fails a filter, across several sstables and the
 * memtable.  Every read runs three ways: in one segment, with a yield after every partition, and
 * with yields at random.  A caller's {@link ReconciledRowFilter} is checked against the same
 * expression as a row filter.
 *
 * <p>Properties: {@code cassandra.test.range_sink_trials} (default {@value #DEFAULT_TRIALS}),
 * {@code cassandra.test.range_sink_seed} (default: the clock, logged).
 */
@RunWith(Parameterized.class)
public class RangeReadSinkDifferentialTest extends RangeReadOracle
{
    private static final Logger logger = LoggerFactory.getLogger(RangeReadSinkDifferentialTest.class);

    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> formats()
    {
        return Arrays.asList(new Object[][]{ { "big" }, { "bti" } });
    }

    private static final int DEFAULT_TRIALS = 6;
    private static final int TRIALS = Integer.getInteger("cassandra.test.range_sink_trials", DEFAULT_TRIALS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final long BASE_TS = 5_000_000_000_000L;
    private static final int SHORT_TTL = 50;
    private static final int COLUMN_INDEX_SIZE_KIB = 1;
    private static final String[] MEMTABLES = { null, "trie", "skiplist" };
    /**
     * With {@code offheap_objects} the reference cannot read a skiplist memtable by names:
     * its names iterator gives native clusterings, and {@code RTBoundCloser} fails on one with
     * {@code UnsupportedOperationException} ({@code NativeAccessor.factory}) when a limit stops a
     * read in a range tombstone.  That is a bug of the iterator path, not of the scan.
     */
    private static final String[] NATIVE_MEMTABLES = { null, "trie" };

    private SSTableFormat<?, ?> originalFormat;
    /** Reads compared, and of those the ones that returned a row. */
    private int reads;
    private int readsWithRows;
    private int originalColumnIndexSizeKiB;

    @Before
    public void pinFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(COLUMN_INDEX_SIZE_KIB);
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    // ---------------------------------------------------------------- regular tables

    /** The filters of the matrix on the regular table, as WHERE fragments. */
    private static final String[] REGULAR_FILTERS = {
        // partition key (part of a composite key)
        "k2 = 'p1'", "k2 > 'p0'", "k2 IN ('p0', 'p2')",
        // clustering
        "c = 3", "c > 5", "c >= 2", "c < 7", "c <= 4", "c IN (1, 4, 9)",
        // static
        "s = 2", "s > 3", "s < 5", "s IN (1, 6)",
        // regular
        "v = 4", "v > 10", "v <= 20", "v < 3", "v >= 25", "v IN (3, 5, 7)", "t = 'x3'", "t > 'x5'",
        // frozen list and frozen UDT
        "fl = [1, 2]", "fl CONTAINS 3", "u = {a: 1, b: 'x1'}",
        // non-frozen collections
        "st CONTAINS 4", "st NOT CONTAINS 4", "m CONTAINS KEY 'a'", "m NOT CONTAINS KEY 'b'",
        "m CONTAINS 7", "m NOT CONTAINS 7", "m['a'] = 3", "m['b'] = 2", "l CONTAINS 1", "l NOT CONTAINS 2",
    };

    @Test
    public void randomFiltersOnARegularTable() throws Throwable
    {
        long master = Long.getLong("cassandra.test.range_sink_seed", System.currentTimeMillis()); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        logger.info("{} format={} master seed = {}L, trials = {}", getClass().getSimpleName(), format, master, TRIALS);
        Random seeds = new Random(master);
        for (int trial = 0; trial < TRIALS; trial++)
        {
            long seed = seeds.nextLong();
            try
            {
                regularTrial(seed);
            }
            catch (Throwable t)
            {
                throw new AssertionError("trial " + trial + " failed, seed " + seed + "L (master " + master + "L)", t);
            }
            finally
            {
                dropTable("DROP TABLE IF EXISTS %s");
            }
        }
    }

    private void regularTrial(long seed) throws Throwable
    {
        Random r = new Random(seed);
        String type = createType("CREATE TYPE IF NOT EXISTS %s (a int, b text)");
        boolean purgeable = r.nextInt(3) == 0;
        String[] memtables = DatabaseDescriptor.getMemtableAllocationType() == Config.MemtableAllocationType.offheap_objects
                             ? NATIVE_MEMTABLES : MEMTABLES;
        String memtable = memtables[r.nextInt(memtables.length)];
        createTable("CREATE TABLE %s (k1 int, k2 text, c int, s int static, v int, t text, fl frozen<list<int>>, st set<int>, " +
                    "m map<text, int>, l list<int>, u frozen<" + type + ">, PRIMARY KEY ((k1, k2), c)) " +
                    "WITH gc_grace_seconds = " + (purgeable ? 0 : 864000) + (memtable == null ? "" : " AND memtable = '" + memtable + '\''));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        logger.info("trial seed {}L: gc_grace_seconds {}, memtable {}", seed, purgeable ? 0 : 864000, memtable);
        int partitions = 5 + r.nextInt(25);
        int flushes = 1 + r.nextInt(3);
        int operations = 100 + r.nextInt(400);
        long writeNow = FBUtilities.nowInSeconds();
        writeRegular(r, partitions, operations, flushes);

        reads = readsWithRows = 0;
        for (long nowInSec : new long[]{ writeNow + 1, writeNow + SHORT_TTL + 100 })
        {
            int queries = 15 + r.nextInt(10);
            for (int q = 0; q < queries; q++)
            {
                String query = randomRegularQuery(r);
                compareEveryWay(query, nowInSec, seed);
            }
            // a filter of the caller's, checked against the same row filter
            int value = r.nextInt(30);
            compareCallerFilter("SELECT * FROM %s WHERE v = " + value + " ALLOW FILTERING", "SELECT * FROM %s",
                                new IntEqualsFilter(column(cfs, "v"), value), nowInSec);
        }
        logger.info("trial seed {}L: {} reads compared, {} returned rows", seed, reads, readsWithRows);
        assertTrue("too few reads returned rows: " + readsWithRows + " of " + reads, readsWithRows * 4 >= reads);
    }

    private void writeRegular(Random r, int partitions, int operations, int flushes) throws Throwable
    {
        long ts = BASE_TS;
        int perFlush = Math.max(1, operations / (flushes + 1));
        for (int op = 0; op < operations; op++)
        {
            if (r.nextInt(6) != 0)
                ts++;
            int k1 = r.nextInt(partitions);
            String k2 = "p" + (k1 % 3);
            int c = r.nextInt(12);
            String ttl = r.nextInt(5) == 0 ? " AND TTL " + SHORT_TTL : "";
            switch (r.nextInt(14))
            {
                case 0:
                case 1:
                case 2:
                    execute("INSERT INTO %s (k1, k2, c, v, t, fl, st, m, l, u) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, {a: ?, b: ?}) USING TIMESTAMP " + ts + ttl,
                            k1, k2, c, r.nextInt(30), "x" + r.nextInt(8), list(r.nextInt(4), r.nextInt(4)), set(r.nextInt(6)),
                            map("a", r.nextInt(10), "b", r.nextInt(5)), list(r.nextInt(3)), r.nextInt(3), "x" + r.nextInt(3));
                    break;
                case 3:
                    execute("UPDATE %s USING TIMESTAMP " + ts + ttl + " SET v = ? WHERE k1 = ? AND k2 = ? AND c = ?", r.nextInt(30), k1, k2, c);
                    break;
                case 4:
                    execute("UPDATE %s USING TIMESTAMP " + ts + ttl + " SET st = st + ?, m['a'] = ? WHERE k1 = ? AND k2 = ? AND c = ?",
                            set(r.nextInt(6)), r.nextInt(10), k1, k2, c);
                    break;
                case 5:
                    execute("DELETE m['a'] FROM %s USING TIMESTAMP " + ts + " WHERE k1 = ? AND k2 = ? AND c = ?", k1, k2, c);
                    break;
                case 6:
                    execute("DELETE v FROM %s USING TIMESTAMP " + ts + " WHERE k1 = ? AND k2 = ? AND c = ?", k1, k2, c);
                    break;
                case 7:
                    execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE k1 = ? AND k2 = ? AND c = ?", k1, k2, c);
                    break;
                case 8:
                {
                    int a = r.nextInt(12), b = r.nextInt(12);
                    execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE k1 = ? AND k2 = ? AND c >= ? AND c < ?", k1, k2, Math.min(a, b), Math.max(a, b) + 1);
                    break;
                }
                case 9:
                    if (r.nextInt(4) == 0)
                        execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE k1 = ? AND k2 = ?", k1, k2);
                    break;
                case 10:
                case 11:
                    execute("UPDATE %s USING TIMESTAMP " + ts + ttl + " SET s = ? WHERE k1 = ? AND k2 = ?", r.nextInt(8), k1, k2);
                    break;
                case 12:
                    execute("UPDATE %s USING TIMESTAMP " + ts + " SET l = l + ?, t = ? WHERE k1 = ? AND k2 = ? AND c = ?", list(r.nextInt(3)), "x" + r.nextInt(8), k1, k2, c);
                    break;
                default:
                    execute("UPDATE %s USING TIMESTAMP " + ts + " SET st = ? WHERE k1 = ? AND k2 = ? AND c = ?", set(r.nextInt(6), r.nextInt(6)), k1, k2, c);
                    break;
            }
            if ((op + 1) % perFlush == 0 && flushes-- > 0)
                flush();
        }
        // static-only partitions, and partitions no row of which passes most filters
        for (int k1 = partitions; k1 < partitions + 3; k1++)
            execute("UPDATE %s USING TIMESTAMP " + (++ts) + " SET s = ? WHERE k1 = ? AND k2 = ?", r.nextInt(8), k1, "p" + (k1 % 3));
        for (int c = 0; c < 20; c++)
            execute("INSERT INTO %s (k1, k2, c, v, t) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP " + (++ts), 1000, "p0", c, 1000 + c, "none");
    }

    private static String randomRegularQuery(Random r)
    {
        StringBuilder where = new StringBuilder();
        int n = 1 + r.nextInt(3);
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < n; i++)
        {
            String filter = REGULAR_FILTERS[r.nextInt(REGULAR_FILTERS.length)];
            // one restriction per column: CQL refuses two bounds of one side on a column
            String column = filter.split("[ \\[]")[0];
            if (columns.contains(column))
                continue;
            columns.add(column);
            if (where.length() > 0)
                where.append(" AND ");
            where.append(filter);
        }
        if (r.nextInt(3) == 0)
        {
            long a = r.nextLong(), b = r.nextLong();
            where.append(" AND token(k1, k2) > ").append(Math.min(a, b)).append(" AND token(k1, k2) <= ").append(Math.max(a, b));
        }
        String select = r.nextInt(4) == 0 ? "SELECT k1, k2, c, v, m FROM %s" : "SELECT * FROM %s";
        String limit = "";
        switch (r.nextInt(5))
        {
            case 0: limit = " LIMIT " + (1 + r.nextInt(20)); break;
            case 1: limit = " PER PARTITION LIMIT " + (1 + r.nextInt(3)); break;
            default: break;
        }
        return select + " WHERE " + where + limit + " ALLOW FILTERING";
    }

    // ---------------------------------------------------------------- counters

    private static final String[] COUNTER_FILTERS = { "n > 5", "n = 3", "n <= 2", "n IN (1, 4)", "c > 3 AND n > 2", "k2 = 'p2' AND n >= 4" };

    @Test
    public void counterFilters() throws Throwable
    {
        long seed = Long.getLong("cassandra.test.range_sink_seed", System.currentTimeMillis()); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        logger.info("{} counters format={} seed = {}L", getClass().getSimpleName(), format, seed);
        Random r = new Random(seed);
        createTable("CREATE TABLE %s (k1 int, k2 text, c int, n counter, PRIMARY KEY ((k1, k2), c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        int partitions = 20;
        for (int round = 0; round < 3; round++)
        {
            for (int op = 0; op < 200; op++)
            {
                int k1 = r.nextInt(partitions);
                execute("UPDATE %s SET n = n + ? WHERE k1 = ? AND k2 = ? AND c = ?", (long) (r.nextInt(5) - 1), k1, "p" + (k1 % 3), r.nextInt(8));
            }
            if (round < 2)
                flush();
        }
        long nowInSec = FBUtilities.nowInSeconds() + 1;
        for (String filter : COUNTER_FILTERS)
            compareEveryWay("SELECT * FROM %s WHERE " + filter + " ALLOW FILTERING", nowInSec, seed);
    }

    // ---------------------------------------------------------------- tombstones of filtered rows

    /**
     * The rows a filter rejects are not written, but their tombstones are counted before the
     * filter, as the iterator path's metrics stage counts them, in one count with the rows it
     * keeps: the replica response and the scan fail at the same row with the same message, and
     * below the threshold record the same scan metrics.
     */
    @Test
    public void tombstonesOfFilteredRowsCountOnce() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, t text, u text, PRIMARY KEY (k, c)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int k = 0; k < 10; k++)
        {
            for (int c = 0; c < 30; c++)
            {
                // most rows fail v = 1 and carry two cell tombstones
                execute("INSERT INTO %s (k, c, v, t, u) VALUES (?, ?, ?, 'a', 'b') USING TIMESTAMP 1000", k, c, c % 10);
                if (c % 10 != 1)
                    execute("DELETE t, u FROM %s USING TIMESTAMP 2000 WHERE k = ? AND c = ?", k, c);
            }
            if (k % 4 == 3)
                flush();
        }
        int warnWas = DatabaseDescriptor.getTombstoneWarnThreshold();
        int failWas = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            long now = FBUtilities.nowInSeconds();
            String query = "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING";
            // under the thresholds: the same counts on every surface
            DatabaseDescriptor.setTombstoneWarnThreshold(100);
            DatabaseDescriptor.setTombstoneFailureThreshold(100_000);
            assertReplicaResponsesMatch(ReadCase.of("filtered tombstones under the threshold", seed()).at(now), cfs, at -> rangeCommand(query, at));
            // over it: the same failure on the replica response and in the scan
            DatabaseDescriptor.setTombstoneFailureThreshold(150);
            assertReplicaResponsesMatch(ReadCase.of("filtered tombstones over the threshold", seed()).at(now), cfs, at -> rangeCommand(query, at));
            setRangePath(false);
            String iteratorFailure = failure(() -> responseLines(rangeCommand(query, now)));
            String scanFailure = failure(() -> scanLines(rangeCommand(query, now), null, () -> false));
            assertTrue("the iterator path fails on the tombstones", iteratorFailure != null);
            assertEquals("the scan fails as the iterator path does", iteratorFailure, scanFailure);
        }
        finally
        {
            DatabaseDescriptor.setTombstoneWarnThreshold(warnWas);
            DatabaseDescriptor.setTombstoneFailureThreshold(failWas);
        }
    }

    private static long seed()
    {
        return System.nanoTime();
    }

    /** The class and message of what {@code read} throws, or null. */
    private static String failure(Runnable read)
    {
        try
        {
            read.run();
            return null;
        }
        catch (RuntimeException e)
        {
            Throwable cause = e;
            while (cause.getCause() != null && !(cause instanceof org.apache.cassandra.db.filter.TombstoneOverwhelmingException))
                cause = cause.getCause();
            return cause.getClass().getName() + ": " + cause.getMessage();
        }
    }

    // ---------------------------------------------------------------- what the scan refuses

    @Test
    public void scanRefusesWhatOnlyTheIteratorPathReads() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 1)");
        long now = FBUtilities.nowInSeconds();
        PartitionRangeReadCommand grouped = rangeCommand("SELECT k, count(*) FROM %s GROUP BY k", now);
        try
        {
            CursorRangeScan.open(grouped);
            fail("a GROUP BY read is refused");
        }
        catch (CursorReadUnsupportedException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("limit"));
        }
        PartitionRangeReadCommand digest = digestCopy(rangeCommand("SELECT * FROM %s", now));
        try
        {
            CursorRangeScan.open(digest);
            fail("a digest read is refused");
        }
        catch (CursorReadUnsupportedException e)
        {
            assertEquals(null, e.gateReason);
        }
    }

    // ---------------------------------------------------------------- the comparison

    /** One read, as the iterator path's response and as the scan in every yield mode. */
    private void compareEveryWay(String query, long nowInSec, long seed)
    {
        PartitionRangeReadCommand command = rangeCommand(query, nowInSec);
        setRangePath(false);
        List<String> expected = responseLines(command);
        reads++;
        if (expected.stream().anyMatch(line -> line.startsWith("ROW")))
            readsWithRows++;
        String what = query + " at " + nowInSec + " (seed " + seed + "L)";
        assertLinesEqual(what + ", one segment", expected, scanLines(rangeCommand(query, nowInSec), null, () -> false));
        assertLinesEqual(what + ", a yield after every partition", expected, scanLines(rangeCommand(query, nowInSec), null, () -> true));
        Random yields = new Random(seed ^ query.hashCode());
        BooleanSupplier random = () -> yields.nextInt(3) == 0;
        assertLinesEqual(what + ", random yields", expected, scanLines(rangeCommand(query, nowInSec), null, random));
        // the cursor path's own replica response agrees too
        setRangePath(true);
        try
        {
            assertLinesEqual(what + ", the cursor replica response", expected, responseLines(rangeCommand(query, nowInSec)));
        }
        finally
        {
            setRangePath(false);
        }
    }

    /** The scan of {@code unfiltered} with {@code filter} against the iterator path's response to {@code filtered}. */
    private void compareCallerFilter(String filtered, String unfiltered, ReconciledRowFilter filter, long nowInSec)
    {
        setRangePath(false);
        List<String> expected = responseLines(rangeCommand(filtered, nowInSec));
        assertLinesEqual(filtered + " as a caller's filter", expected, scanLines(rangeCommand(unfiltered, nowInSec), filter, () -> true));
    }

    private static ColumnMetadata column(ColumnFamilyStore cfs, String name)
    {
        return cfs.metadata().getColumn(ByteBufferUtil.bytes(name));
    }

    /**
     * A caller's filter that keeps a row whose int column holds {@code value}: what
     * {@code RowFilter.SimpleExpression} with {@code =} keeps, on the purged reconciled row.
     */
    private static final class IntEqualsFilter implements ReconciledRowFilter
    {
        private final ColumnMetadata column;
        private final ByteBuffer value;
        private boolean matched;
        private boolean failed;

        IntEqualsFilter(ColumnMetadata column, int value)
        {
            this.column = column;
            this.value = Int32Type.instance.decompose(value);
        }

        @Override
        public boolean partitionMatches(DecoratedKey key, Row purgedStaticRow)
        {
            return true;
        }

        @Override
        public boolean hasRowLevelExpressions()
        {
            return true;
        }

        @Override
        public boolean filtersClustering()
        {
            return false;
        }

        @Override
        public void startRow(Clustering<?> clustering)
        {
            matched = false;
            failed = false;
        }

        @Override
        public boolean filtersColumn(ColumnMetadata c)
        {
            return c.name.equals(column.name);
        }

        @Override
        public void cell(ColumnMetadata c, CellPath path, ByteBuffer cellValue, boolean live)
        {
            if (!filtersColumn(c) || failed)
                return;
            // a cell that is not live has no value for the filter, which fails the row
            if (live && cellValue.equals(value))
                matched = true;
            else
                failed = true;
        }

        @Override
        public void complexColumnWritten(ColumnMetadata c)
        {
        }

        @Override
        public boolean rowMatches(boolean survivesPurgeAll, boolean noDeletion)
        {
            // RowFilter.filter purges the row of every tombstone first
            return survivesPurgeAll && matched && !failed;
        }

        @Override
        public boolean rowFailed()
        {
            return failed;
        }
    }
}
