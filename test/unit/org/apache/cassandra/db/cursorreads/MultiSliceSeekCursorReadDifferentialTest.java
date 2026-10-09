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

import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Multi-slice and names reads over a wide partition with many row index blocks, so the cursor
 * path seeks forward at each slice start (CASSANDRA-20428).  Each case checks the cursor result
 * against the iterator path, record for record and byte for byte.  On BTI each case also checks
 * that the cursor path seeked; BIG has no cursor seek, so its cases check the same results with
 * the partition read from its start.
 *
 * <p>Rows are about 70 bytes and the row index block size is 1 KiB, so a block holds about 15 rows
 * and the partition has well over 100 blocks.
 *
 * <p>The base class runs BIG; {@link BtiMultiSliceSeekCursorReadDifferentialTest} pins BTI.
 */
public class MultiSliceSeekCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private static final long PK = 1L;
    private static final int ROWS = 2000;
    private static final long NOW = 1_700_000_000L;
    private static final String PADDING = "-padding-padding-padding-padding-padding-";

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;

    @Before
    public void pinFormatAndBlockSize()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(formatName());
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
    }

    @After
    public void restoreFormatAndBlockSize()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    /** The sstable format this class pins.  The BTI subclass overrides it. */
    protected String formatName()
    {
        return "big";
    }

    // ---------------------------------------------------------------- range tombstones and seeks

    /**
     * One sstable, so one leg read as stored.  A range tombstone covers most of the partition and
     * newer rows sit inside it.  Slices two to four start inside the tombstone, so each seek lands
     * in a block where it is open and the reader must take its deletion from the row index.
     */
    @Test
    public void rangeTombstoneOpenAcrossSliceSeeksInOneLeg() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 1, 10, "base");
        deleteRange(300, 1700, 20);
        insertRows(0, ROWS, 7, 30, "newer");
        flush();

        long[][] ranges = { { 100, 110 }, { 500, 510 }, { 900, 910 }, { 1600, 1610 }, { 1800, 1810 } };
        assertMatches(cfs, multiSlice(cfs, false, -1, ranges), true);
        assertMatches(cfs, multiSlice(cfs, true, -1, ranges), false);
        assertMatches(cfs, names(cfs, 105, 505, 905, 1605, 1699, 1700, 1805), true);
    }

    /**
     * The range tombstones live in a different sstable from the rows, and overlap each other, so
     * that sstable holds boundary markers.  A memtable tombstone and rows join the merge too.
     * Several slices start inside tombstones of one leg while other legs seek or read on.
     */
    @Test
    public void rangeTombstoneOpenAcrossSliceSeeksAcrossLegs() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 1, 10, "first");
        flush();
        deleteRange(300, 1700, 20);
        deleteRange(1000, 1200, 25);
        insertRows(250, 350, 3, 22, "inside");
        flush();
        insertRows(0, ROWS, 5, 30, "third");
        flush();
        deleteRange(1500, 1650, 40);
        insertRows(1550, 1600, 4, 45, "memtable");

        long[][] ranges = { { 50, 60 }, { 320, 330 }, { 990, 1010 }, { 1100, 1120 }, { 1199, 1201 },
                            { 1560, 1580 }, { 1640, 1660 }, { 1900, 1999 } };
        assertMatches(cfs, multiSlice(cfs, false, -1, ranges), true);
        assertMatches(cfs, multiSlice(cfs, true, -1, ranges), false);
        assertMatches(cfs, names(cfs, 55, 320, 999, 1000, 1110, 1200, 1201, 1570, 1650, 1950), true);
    }

    /** A tombstone opens before the first slice and closes after the last, so every slice opens
     *  and closes it artificially, and every later slice seeks into it. */
    @Test
    public void rangeTombstoneCoveringEverySlice() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 1, 10, "base");
        flush();
        deleteRange(10, 1990, 20);
        insertRows(0, ROWS, 11, 30, "newer");
        flush();

        long[][] ranges = { { 100, 120 }, { 600, 620 }, { 1100, 1120 }, { 1600, 1620 } };
        assertMatches(cfs, multiSlice(cfs, false, -1, ranges), true);
        assertMatches(cfs, multiSlice(cfs, true, -1, ranges), false);
    }

    /**
     * The second sstable's data starts after the first slice, so its leg stays deferred at merge
     * setup (the read selects no static column, which would open it for its static row).  A later
     * slice starts inside its data and inside its range tombstone, so the leg opens there, seeks,
     * and its open deletion joins the merge at that slice.
     */
    @Test
    public void deferredLegOpensAtALaterSlice() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 1, 10, "first");
        flush();
        deleteRange(900, 1600, 20);
        insertRows(1000, 1500, 2, 30, "second");
        flush();

        long forceOpenedBefore = CursorReads.sstableLegsForceOpened();
        Slices slices = slices(cfs.metadata(), new long[][]{ { 100, 110 }, { 1200, 1210 }, { 1590, 1610 }, { 1700, 1710 } });
        ColumnFilter regularOnly = ColumnFilter.selectionBuilder().add(cfs.metadata().getColumn(ByteBufferUtil.bytes("v"))).build();
        assertMatches(cfs, sliceRead(cfs, slices, regularOnly, false, -1), true);
        if ("bti".equals(formatName()))
            assertTrue("the deferred leg did not open at the later slice start",
                       CursorReads.sstableLegsForceOpened() > forceOpenedBefore);
        assertMatches(cfs, sliceRead(cfs, slices, regularOnly, false, 3), true);
        assertMatches(cfs, sliceRead(cfs, slices, regularOnly, true, -1), false);
    }

    // ---------------------------------------------------------------- names

    /**
     * Names inside a block, next to each other (same block), far apart (different blocks), absent
     * clusterings between rows, and the first and last rows of the partition.
     */
    @Test
    public void namesReadInsideAtEdgeAndBetweenBlocks() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 2, 10, "even"); // even clusterings only; odd ones are absent
        flush();

        long[] clusterings = { 0, 2, 4, 5, 37, 38, 40, 41, 400, 402, 999, 1000, 1001, 1500, 1998, 1999 };
        assertMatches(cfs, names(cfs, clusterings), true);

        insertRows(1, ROWS, 2, 20, "odd"); // a second leg fills the gaps
        flush();
        insertRows(0, ROWS, 9, 30, "overwrite");
        assertMatches(cfs, names(cfs, clusterings), true);
    }

    /** Many names spread over the whole partition, one in nearly every block. */
    @Test
    public void namesReadOfManyBlocks() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 1, 10, "base");
        flush();
        deleteRange(700, 900, 20);
        flush();

        long[] clusterings = new long[ROWS / 13];
        for (int i = 0; i < clusterings.length; i++)
            clusterings[i] = 13L * i + (i % 3);
        assertMatches(cfs, names(cfs, clusterings), true);
    }

    // ---------------------------------------------------------------- slices sharing a block

    /** Consecutive slices inside one block need no seek between them; the rows in the gaps are
     *  skipped.  Exclusive bounds are included. */
    @Test
    public void consecutiveSlicesShareABlock() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 1, 10, "base");
        flush();
        insertRows(0, ROWS, 3, 20, "second");
        flush();

        assertMatches(cfs, multiSlice(cfs, false, -1, new long[][]{ { 10, 11 }, { 13, 14 }, { 16, 17 }, { 19, 20 },
                                                                    { 1500, 1505 }, { 1507, 1509 } }), true);
        TableMetadata metadata = cfs.metadata();
        Slices.Builder builder = new Slices.Builder(metadata.comparator);
        builder.add(slice(metadata, 10, false, 14, false));
        builder.add(slice(metadata, 14, false, 18, true));
        builder.add(slice(metadata, 800, true, 820, false));
        builder.add(slice(metadata, 820, false, 840, false));
        Slices slices = builder.build();
        assertMatches(cfs, sliceRead(cfs, slices, false, -1), true);
        assertMatches(cfs, sliceRead(cfs, slices, true, -1), false);
    }

    // ---------------------------------------------------------------- deletions in other legs

    /** Partition, row and range deletions in legs other than the one holding most rows. */
    @Test
    public void slicesWithDeletionsInOtherLegs() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 1, 10, "base");
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 15 WHERE pk = ?", PK);
        flush();
        insertRows(0, ROWS, 4, 20, "after-partition-delete");
        for (long c = 0; c < ROWS; c += 23)
            execute("DELETE FROM %s USING TIMESTAMP 25 WHERE pk = ? AND c = ?", PK, c);
        flush();
        deleteRange(1200, 1400, 30);
        insertRows(1300, 1350, 2, 35, "memtable");

        long[][] ranges = { { 0, 30 }, { 460, 470 }, { 1190, 1210 }, { 1320, 1340 }, { 1390, 1410 } };
        assertMatches(cfs, multiSlice(cfs, false, -1, ranges), true);
        assertMatches(cfs, multiSlice(cfs, true, -1, ranges), false);
        assertMatches(cfs, names(cfs, 0, 4, 23, 460, 1200, 1204, 1320, 1400, 1404), true);
    }

    // ---------------------------------------------------------------- static rows

    /** Static values in several legs, with multi-slice and names reads. */
    @Test
    public void staticRowWithMultiSliceAndNames() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        execute("UPDATE %s USING TIMESTAMP 5 SET s = ? WHERE pk = ?", "static-1", PK);
        insertRows(0, ROWS, 1, 10, "base");
        flush();
        execute("UPDATE %s USING TIMESTAMP 15 SET s = ? WHERE pk = ?", "static-2", PK);
        insertRows(0, ROWS, 6, 20, "second");
        flush();
        deleteRange(400, 1600, 25);

        long[][] ranges = { { 100, 105 }, { 700, 705 }, { 1500, 1700 } };
        assertMatches(cfs, multiSlice(cfs, false, -1, ranges), true);
        assertMatches(cfs, multiSlice(cfs, true, -1, ranges), false);
        assertMatches(cfs, names(cfs, 100, 700, 1599, 1600, 1601), true);
    }

    // ---------------------------------------------------------------- limits and paging

    /** A limit stops the read part way through the slices. */
    @Test
    public void limitedMultiSliceAcrossLegs() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 1, 10, "base");
        flush();
        deleteRange(500, 900, 20);
        insertRows(0, ROWS, 3, 30, "newer");
        flush();

        long[][] ranges = { { 100, 110 }, { 495, 520 }, { 880, 905 }, { 1500, 1600 } };
        for (int limit : new int[]{ 1, 5, 15, 30, 60 })
        {
            assertMatches(cfs, multiSlice(cfs, false, limit, ranges), true);
            assertMatches(cfs, multiSlice(cfs, true, limit, ranges), false);
        }
    }

    /** Paging resumes part way through the slices, so each page starts with a seek. */
    @Test
    public void pagedMultiSliceAcrossLegs() throws Throwable
    {
        ColumnFamilyStore cfs = createWideTable();
        insertRows(0, ROWS, 1, 10, "base");
        flush();
        insertRows(0, ROWS, 2, 20, "second");
        flush();
        insertRows(0, ROWS, 7, 30, "memtable");

        long[][] ranges = { { 100, 140 }, { 600, 640 }, { 1100, 1140 }, { 1600, 1640 } };
        int rows = 4 * 41;
        int pageSize = 17;
        assertPagedReadMatchesIterator(cfs, multiSlice(cfs, false, -1, ranges), pageSize, rows / pageSize + 1);
    }

    // ---------------------------------------------------------------- data

    private ColumnFamilyStore createWideTable()
    {
        createTable("CREATE TABLE %s (pk bigint, c bigint, s text static, v text, PRIMARY KEY (pk, c)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    /** Inserts every {@code step}th clustering in [from, to) at {@code timestamp}. */
    private void insertRows(long from, long to, long step, long timestamp, String tag) throws Throwable
    {
        for (long c = from; c < to; c += step)
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + timestamp,
                    PK, c, tag + PADDING + c);
    }

    /** Deletes clusterings in [from, to) at {@code timestamp}. */
    private void deleteRange(long from, long to, long timestamp) throws Throwable
    {
        execute("DELETE FROM %s USING TIMESTAMP " + timestamp + " WHERE pk = ? AND c >= ? AND c < ?", PK, from, to);
    }

    // ---------------------------------------------------------------- commands

    private static Slice slice(TableMetadata metadata, long start, boolean startInclusive, long end, boolean endInclusive)
    {
        return Slice.make(ClusteringBound.create(metadata.comparator, true, startInclusive, start),
                          ClusteringBound.create(metadata.comparator, false, endInclusive, end));
    }

    /** Inclusive slices, one per {start, end} pair. */
    private static Slices slices(TableMetadata metadata, long[][] ranges)
    {
        Slices.Builder builder = new Slices.Builder(metadata.comparator);
        for (long[] range : ranges)
            builder.add(slice(metadata, range[0], true, range[1], true));
        return builder.build();
    }

    private static Supplier<SinglePartitionReadCommand> multiSlice(ColumnFamilyStore cfs, boolean reversed, int limit, long[][] ranges)
    {
        return sliceRead(cfs, slices(cfs.metadata(), ranges), reversed, limit);
    }

    private static Supplier<SinglePartitionReadCommand> sliceRead(ColumnFamilyStore cfs, Slices slices, boolean reversed, int limit)
    {
        return sliceRead(cfs, slices, ColumnFilter.all(cfs.metadata()), reversed, limit);
    }

    private static Supplier<SinglePartitionReadCommand> sliceRead(ColumnFamilyStore cfs, Slices slices, ColumnFilter columns,
                                                                  boolean reversed, int limit)
    {
        return () -> {
            TableMetadata metadata = cfs.metadata();
            DataLimits limits = limit < 0 ? DataLimits.NONE : DataLimits.cqlLimits(limit);
            return SinglePartitionReadCommand.create(metadata, NOW, columns, RowFilter.none(), limits,
                                                     metadata.partitioner.decorateKey(ByteBufferUtil.bytes(PK)),
                                                     new ClusteringIndexSliceFilter(slices, reversed));
        };
    }

    private static Supplier<SinglePartitionReadCommand> names(ColumnFamilyStore cfs, long... clusterings)
    {
        return () -> {
            AbstractReadCommandBuilder builder = Util.cmd(cfs, PK).withNowInSeconds(NOW);
            for (long c : clusterings)
                builder.includeRow(c);
            return (SinglePartitionReadCommand) builder.build();
        };
    }

    // ---------------------------------------------------------------- checks

    /**
     * The full differential.  For a forward read on BTI it also checks the cursor path seeked at
     * least once per cursor run, which proves the per-slice seek ran; BIG never seeks.
     */
    private void assertMatches(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command, boolean forward)
    {
        long seeksBefore = CursorReads.sstableLegRowIndexSeeks();
        assertCursorReadMatchesIterator(cfs, command);
        long seeks = CursorReads.sstableLegRowIndexSeeks() - seeksBefore;
        if (!"bti".equals(formatName()))
            assertEquals("BIG legs never seek on the cursor path", 0, seeks);
        else if (forward)
            assertTrue("a forward read over many row index blocks did not seek", seeks >= 2);
        logger.debug("{} seeks over the harness's two cursor runs", seeks);
    }
}
