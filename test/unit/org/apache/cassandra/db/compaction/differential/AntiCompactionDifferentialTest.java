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

package org.apache.cassandra.db.compaction.differential;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DirectIoTestUtils;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.fullOnly;
import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.liveTokens;
import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.range;
import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.rangeToMinimum;
import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.ranges;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Cursor anticompaction against legacy anticompaction, for full, transient and unrepaired ranges
 * in many layouts, on BIG and BTI.  Each case compares every output sstable byte for byte and
 * logically, and its repair metadata.
 */
@RunWith(Parameterized.class)
public class AntiCompactionDifferentialTest extends DifferentialCompactionTester
{
    private static final String TABLE = "CREATE TABLE %s (pk bigint, ck bigint, s text static, v text, " +
                                        "m map<text, int>, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000";

    private static final int PARTITIONS = 40;
    private static final int ROWS = 5;

    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static List<Object[]> formats()
    {
        return List.of(new Object[]{ "big" }, new Object[]{ "bti" });
    }

    @Before
    public void selectFormat()
    {
        selectSSTableFormat(format);
    }

    @After
    public void restoreFormat()
    {
        restoreSelectedFormat();
    }

    /** Three overlapping flushes with static rows, collections, and every kind of deletion. */
    private ColumnFamilyStore writeMixedTable(int flushes)
    {
        return writeMixedTable(TABLE, flushes);
    }

    private ColumnFamilyStore writeMixedTable(String table, int flushes)
    {
        createTable(table);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < flushes; round++)
        {
            for (long pk = round; pk < PARTITIONS; pk += 1 + round % 2)
            {
                execute("UPDATE %s SET s = ? WHERE pk = ?", "s" + round, pk);
                for (long ck = 0; ck < ROWS; ck++)
                    execute("INSERT INTO %s (pk, ck, v, m) VALUES (?, ?, ?, ?)", pk, ck + round, "v" + round, java.util.Map.of("k" + round, (int) ck));
                if (round > 0 && pk % 5 == 0)
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", pk, 1L, 3L);
                if (round > 0 && pk % 7 == 0)
                    execute("DELETE m['k0'] FROM %s WHERE pk = ? AND ck = ?", pk, 0L);
                if (round > 0 && pk % 11 == 0)
                    execute("DELETE v FROM %s WHERE pk = ? AND ck = ?", pk, (long) round);
            }
            if (round > 0)
                execute("DELETE FROM %s WHERE pk = ?", (long) (13 * round % PARTITIONS));
            flush();
        }
        return cfs;
    }

    private ColumnFamilyStore writeMixedTable()
    {
        return writeMixedTable(3);
    }

    @Test
    public void fullRangesOnly() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        List<Long> tokens = liveTokens(cfs);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, fullOnly(List.of(range(tokens.get(5), tokens.get(20)))));
        assertEquals("full and unrepaired outputs", 2, out.sstableCount());
    }

    @Test
    public void transientRangesOnly() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        List<Long> tokens = liveTokens(cfs);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(), List.of(range(tokens.get(3), tokens.get(30)))));
        assertEquals("transient and unrepaired outputs", 2, out.sstableCount());
    }

    @Test
    public void fullAndTransient() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        List<Long> tokens = liveTokens(cfs);
        RangesAtEndpoint ranges = ranges(List.of(range(tokens.get(2), tokens.get(10)), range(tokens.get(20), tokens.get(25))),
                                         List.of(range(tokens.get(10), tokens.get(15)), range(tokens.get(28), tokens.get(33))));
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges);
        assertEquals("full, transient and unrepaired outputs", 3, out.sstableCount());
    }

    /** Every key falls in the full ranges, so only the full output has data. */
    @Test
    public void wholeRingFull() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        List<Long> tokens = liveTokens(cfs);
        long first = tokens.get(0);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, fullOnly(List.of(range(first - 1, first - 1 - 1))));
        assertEquals("only the full output", 1, out.sstableCount());
    }

    /** No key is in a repaired range: every partition goes to the unrepaired output. */
    @Test
    public void neitherFullNorTransient() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        List<Long> tokens = liveTokens(cfs);
        long gapStart = tokens.get(10);
        assertTrue("need a gap after a token", gapStart + 1 < tokens.get(11));
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(gapStart, gapStart + 1)),
                                                                                            List.of()));
        assertEquals("only the unrepaired output", 1, out.sstableCount());
    }

    /** The full output gets no partition while the other two do: the empty writer must be dropped. */
    @Test
    public void outputWithNoData() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        List<Long> tokens = liveTokens(cfs);
        long gapStart = tokens.get(10);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(gapStart, gapStart + 1)),
                                                                                            List.of(range(tokens.get(15), tokens.get(25)))));
        assertEquals("transient and unrepaired outputs", 2, out.sstableCount());
    }

    @Test
    public void rangeWrapsTheRing() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        List<Long> tokens = liveTokens(cfs);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(30), tokens.get(5))),
                                                                                            List.of(range(tokens.get(10), tokens.get(20)))));
        assertEquals(3, out.sstableCount());
    }

    @Test
    public void rangeEndsAtMinimumToken() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        List<Long> tokens = liveTokens(cfs);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(rangeToMinimum(tokens.get(25))),
                                                                                            List.of(range(tokens.get(5), tokens.get(12)))));
        assertEquals(3, out.sstableCount());
    }

    /** A key in both a full and a transient range goes to the full output, as on the legacy path. */
    @Test
    public void fullRangeWinsOverTransientRange() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        List<Long> tokens = liveTokens(cfs);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(5), tokens.get(20))),
                                                                                            List.of(range(tokens.get(15), tokens.get(30)))));
        assertEquals(3, out.sstableCount());
    }

    /** Ten input sstables, as in AntiCompactionTest.antiCompactTenMixed. */
    @Test
    public void tenSSTablesMixed() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable(10);
        assertEquals(10, cfs.getLiveSSTables().size());
        List<Long> tokens = liveTokens(cfs);
        assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(0), tokens.get(12))),
                                                                 List.of(range(tokens.get(12), tokens.get(24)))));
    }

    /** Wide partitions that span many index blocks, each sent to a different output. */
    @Test
    public void widePartitions() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String padding = "x".repeat(200);
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 6; pk++)
            {
                for (long ck = round; ck < 1500; ck += 2)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, padding + round);
                execute("DELETE FROM %s WHERE pk = ? AND ck > ? AND ck < ?", pk, 100L + round * 300, 200L + round * 300);
            }
            flush();
        }
        List<Long> tokens = liveTokens(cfs);
        assertEquals(6, tokens.size());
        assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(0), tokens.get(1)), range(tokens.get(3), tokens.get(4))),
                                                                 List.of(range(tokens.get(1), tokens.get(2)), range(tokens.get(4), tokens.get(5)))));
    }

    @Test
    public void counters() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < PARTITIONS; pk++)
                for (long ck = 0; ck < ROWS; ck++)
                    execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", (long) round + 1, pk, ck);
            flush();
        }
        List<Long> tokens = liveTokens(cfs);
        assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(5), tokens.get(15))),
                                                                 List.of(range(tokens.get(20), tokens.get(30)))));
    }

    /** Every partition is deleted and past gc grace, so nothing is written and no output is left. */
    @Test
    public void allPartitionsPurged() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < PARTITIONS; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, "v");
        flush();
        List<Long> tokens = liveTokens(cfs);
        for (long pk = 0; pk < PARTITIONS; pk++)
            execute("DELETE FROM %s WHERE pk = ?", pk);
        flush();
        // gc_grace 0: the partition deletions are purgeable once the clock is past them
        awaitClockPast(FBUtilities.nowInSeconds() + 1);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(5), tokens.get(15))),
                                                                                            List.of(range(tokens.get(20), tokens.get(30)))));
        assertEquals("every partition is purged, so no output is left", 0, out.sstableCount());
    }

    /** Cells, rows and map elements with TTL 1 expire into tombstones, which gc grace keeps on both paths. */
    @Test
    public void expiredCellsKeptByGcGrace() throws Exception
    {
        createTable(TABLE);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < PARTITIONS; pk++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TTL 1", pk, 0L, "row expires");
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 1L, "lives");
            execute("UPDATE %s USING TTL 1 SET v = ? WHERE pk = ? AND ck = ?", "cell expires", pk, 2L);
            execute("UPDATE %s USING TTL 1 SET m['e'] = ? WHERE pk = ? AND ck = ?", 1, pk, 1L);
        }
        flush();
        for (long pk = 0; pk < PARTITIONS; pk += 2)
            execute("UPDATE %s SET m['f'] = ? WHERE pk = ? AND ck = ?", 2, pk, 1L);
        flush();
        awaitClockPast(FBUtilities.nowInSeconds() + 1);
        List<Long> tokens = liveTokens(cfs);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(5), tokens.get(15))),
                                                                                            List.of(range(tokens.get(20), tokens.get(30)))));
        assertTrue("the expired cells must stay as tombstones", allJson(out.captured()).contains(CELL_TOMBSTONE));
    }

    /** With only_purge_repaired_tombstones, the tombstones of unrepaired inputs are kept on both paths. */
    @Test
    public void onlyPurgeRepairedTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0 " +
                    "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones': 'true'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < PARTITIONS; pk++)
            for (long ck = 0; ck < ROWS; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v");
        flush();
        for (long pk = 0; pk < PARTITIONS; pk += 2)
            execute("DELETE FROM %s WHERE pk = ?", pk);
        flush();
        awaitClockPast(FBUtilities.nowInSeconds() + 1);
        List<Long> tokens = liveTokens(cfs);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(5), tokens.get(15))),
                                                                                            List.of(range(tokens.get(20), tokens.get(30)))));
        assertTrue("the partition deletions of unrepaired data must be kept", allJson(out.captured()).contains("marked_deleted"));
    }

    /** Purged partitions whose tokens are the endpoints of the ranges, and their neighbours that are kept. */
    @Test
    public void purgedPartitionsOnRangeEndpoints() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < PARTITIONS; pk++)
            for (long ck = 0; ck < ROWS; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v");
        flush();
        List<Long> tokens = liveTokens(cfs);
        // the right endpoint is inside its range, the left one is not: purge the keys on both
        List<Integer> endpoints = List.of(5, 15, 20, 30);
        for (long pk = 0; pk < PARTITIONS; pk++)
        {
            long token = cfs.getPartitioner().getToken(org.apache.cassandra.utils.ByteBufferUtil.bytes(pk)).getLongValue();
            for (int index : endpoints)
                if (tokens.get(index) == token)
                    execute("DELETE FROM %s WHERE pk = ?", pk);
        }
        flush();
        awaitClockPast(FBUtilities.nowInSeconds() + 1);
        AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(5), tokens.get(15))),
                                                                                            List.of(range(tokens.get(20), tokens.get(30)))));
        assertEquals(3, out.sstableCount());
        assertEquals("the four partitions on the endpoints are purged", PARTITIONS - endpoints.size(), outputPartitionCount(out));
    }

    /** A column added between flushes: the output header is the union of the input headers. */
    @Test
    public void columnAddedBetweenFlushes() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < PARTITIONS; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, "v");
        flush();
        execute("ALTER TABLE %s ADD w int");
        for (long pk = 0; pk < PARTITIONS; pk += 2)
            execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?)", pk, 1L, "v", (int) pk);
        flush();
        List<Long> tokens = liveTokens(cfs);
        assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(5), tokens.get(15))),
                                                                 List.of(range(tokens.get(20), tokens.get(30)))));
    }

    /** A compressed table written with direct I/O, as in AntiCompactionTest.testAntiCompactionWithCompressedTableAndDirectWrites. */
    @Test
    public void compressedTableWithDirectWrites() throws Exception
    {
        ColumnFamilyStore cfs = writeMixedTable();
        assertTrue("the table must be compressed for direct I/O to engage", cfs.metadata().params.compression.isEnabled());
        List<Long> tokens = liveTokens(cfs);
        DirectIoTestUtils.withDirectWrites(() -> assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(2), tokens.get(10))),
                                                                                                          List.of(range(tokens.get(10), tokens.get(15))))));
    }

    /**
     * The cursor path passes to the compaction rate limiter exactly the bytes that the legacy
     * scanners read, with and without compression, and its throughput meter matches those bytes
     * within the number of rate limiter calls.  The harness checks this for every case; here the inputs
     * are large enough to need many calls.
     */
    @Test
    public void throttledBytes() throws Exception
    {
        for (boolean compressed : new boolean[]{ true, false })
        {
            createTable(compressed ? TABLE : TABLE + " AND compression = {'enabled': 'false'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int round = 0; round < 3; round++)
            {
                for (long pk = 0; pk < PARTITIONS; pk++)
                    for (long ck = 0; ck < ROWS * 20; ck++)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck + round, "v" + round);
                flush();
            }
            long inputDataBytes = 0;
            for (SSTableReader sstable : cfs.getLiveSSTables())
                inputDataBytes += sstable.uncompressedLength();
            List<Long> tokens = liveTokens(cfs);
            AntiCompactionOutput legacy = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges(List.of(range(tokens.get(5), tokens.get(15))),
                                                                                                   List.of(range(tokens.get(20), tokens.get(30)))));
            String table = compressed ? "compressed" : "uncompressed";
            DifferentialCompactionTester.RateLimiterCalls cursor = legacy.cursorRun.throttled;
            assertEquals("the legacy scanners did not read every input byte: " + table, inputDataBytes, legacy.throttled.scannedBytes);
            assertEquals("the cursor path did not pass every input byte to the rate limiter: " + table, inputDataBytes, cursor.scannedBytes);
            assertTrue("too few rate limiter calls to test the meter: " + table, cursor.calls > 1);
            // Each call acquires its bytes times the compression ratio, rounded down, plus one.  One
            // more byte of slack for the order in which the ratio's sums are added.
            double scaled = cursor.scannedBytes * legacy.cursorRun.compressionRatio;
            assertTrue(String.format("the cursor path acquired %d bytes, not within %d calls above %.1f scanned bytes times the ratio: %s",
                                     cursor.permits, cursor.calls, scaled, table),
                       cursor.permits > scaled - 1 && cursor.permits <= scaled + cursor.calls + 1);
        }
    }

    /**
     * A corrupt input fails with the same exception type on both paths, so that
     * {@code disk_failure_policy} acts the same way.  Covers a damaged and a truncated data file,
     * with and without compression.
     */
    @Test
    public void corruptInputFailsTheSameWayOnBothPaths() throws Exception
    {
        for (boolean compressed : new boolean[]{ true, false })
        {
            for (boolean truncate : new boolean[]{ false, true })
            {
                String table = compressed ? TABLE : TABLE + " AND compression = {'enabled': 'false'}";
                ColumnFamilyStore cfs = writeMixedTable(table, 3);
                List<Long> tokens = liveTokens(cfs);
                RangesAtEndpoint ranges = fullOnly(List.of(range(tokens.get(5), tokens.get(20))));
                SSTableReader damaged = cfs.getLiveSSTables().stream().max(Comparator.comparingLong(SSTableReader::onDiskLength)).get();
                corrupt(damaged, truncate);
                String layout = (compressed ? "compressed" : "uncompressed") + (truncate ? ", truncated" : ", damaged");

                Throwable legacy = antiCompactionFailure(cfs, ranges, false);
                Throwable cursor = antiCompactionFailure(cfs, ranges, true);
                assertNotNull("the legacy path did not fail on a corrupt input: " + layout, legacy);
                assertNotNull("the cursor path did not fail on a corrupt input: " + layout, cursor);
                assertEquals("the two paths failed with different exception types: " + layout + "; legacy " + legacy + ", cursor " + cursor,
                             legacy.getClass(), cursor.getClass());
                assertTrue("the inputs are not all live after the failure: " + layout, cfs.getLiveSSTables().contains(damaged));
            }
        }
    }

    /** Overwrites bytes in the middle of the data file, or cuts the file in half. */
    private static void corrupt(SSTableReader sstable, boolean truncate) throws Exception
    {
        File data = sstable.descriptor.fileFor(SSTableFormat.Components.DATA);
        try (FileChannel channel = FileChannel.open(data.toPath(), StandardOpenOption.WRITE))
        {
            long middle = channel.size() / 2;
            if (truncate)
            {
                channel.truncate(middle);
            }
            else
            {
                byte[] garbage = new byte[64];
                new Random(42).nextBytes(garbage);
                channel.write(ByteBuffer.wrap(garbage), middle);
            }
        }
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFile(data.path());
    }

    /** Anticompacts the live set on one path and returns what it threw, or null. */
    private static Throwable antiCompactionFailure(ColumnFamilyStore cfs, RangesAtEndpoint ranges, boolean cursor)
    {
        long nowInSec = FBUtilities.nowInSeconds();
        long runsBefore = CompactionManager.cursorAnticompactionsRun.sum();
        try
        {
            antiCompactGroup(cfs, cfs.getLiveSSTables(), ranges, TimeUUID.Generator.nextTimeUUID(), cursor, () -> false, nowInSec, cfs.getDefaultGcBefore(nowInSec));
            return null;
        }
        catch (Throwable t)
        {
            // the failure must come from the path asked for, not from a fallback
            assertEquals(cursor ? "the cursor path did not run before the failure" : "the cursor path ran with it off",
                         cursor ? 1 : 0, CompactionManager.cursorAnticompactionsRun.sum() - runsBefore);
            return t;
        }
    }

    private static long outputPartitionCount(AntiCompactionOutput out)
    {
        return countOccurrences(allJson(out.captured()), "\"partition\":{");
    }

    /**
     * Waits until the wall clock is past {@code seconds}.  Each run reads the clock, so the cells
     * and tombstones must expire before the first run for both runs to purge the same data.
     */
    static void awaitClockPast(long seconds) throws InterruptedException
    {
        while (FBUtilities.nowInSeconds() <= seconds)
            Thread.sleep(100);
    }
}
