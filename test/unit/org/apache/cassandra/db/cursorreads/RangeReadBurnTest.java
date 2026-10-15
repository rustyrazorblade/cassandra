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
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.LocalReadSizeTooLargeException;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Burn test for cursor range reads (CASSANDRA-21707): range reads over a table of at least 100K
 * rows, compared with the iterator range path on the replica response bytes, page by page.  The
 * cursor path must serve every read.  A page that differs is reported by {@link RangeReadOracle}.
 *
 * <p>The table has many small partitions and three wide ones (a tenth of the rows each, row
 * indexed), written across four sstables and the memtable, with row deletions, range deletions,
 * cell deletions, live TTLs and a static column.  The reads are the full ring in pages, with and
 * without a row filter, then random reads over token windows: one read, pages of 100, 5000 or
 * 20000 rows, a limit, a row filter with ALLOW FILTERING, DISTINCT, and a per partition limit.
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code cassandra.test.cursor_range_burn_rows}: rows written (default {@value #DEFAULT_ROWS}, no upper cap);</li>
 *   <li>{@code cassandra.test.cursor_range_burn_reads}: random reads (default {@value #DEFAULT_READS});</li>
 *   <li>{@code cassandra.test.cursor_range_burn_seed}: the seed (default: the clock, logged).</li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class RangeReadBurnTest extends RangeReadOracle
{
    private static final Logger logger = LoggerFactory.getLogger(RangeReadBurnTest.class);

    private static final int DEFAULT_ROWS = 100_000;
    private static final int DEFAULT_READS = 40;
    private static final int ROWS = Integer.getInteger("cassandra.test.cursor_range_burn_rows", DEFAULT_ROWS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int READS = Integer.getInteger("cassandra.test.cursor_range_burn_reads", DEFAULT_READS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int FLUSHES = 4;
    private static final int WIDE_PARTITIONS = 3;
    private static final int LONG_TTL = 1_000_000;
    private static final long BASE_TS = 1_000_000L;
    /** Rows a random read's token window holds, about, at most. */
    private static final int WINDOW_ROWS = 50_000;

    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> formats()
    {
        return Arrays.asList(new Object[][]{ { "bti" }, { "big" } });
    }

    private SSTableFormat<?, ?> originalFormat;
    private int originalWarn;
    private int originalFail;

    @Before
    public void pinFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        // the full ring reads every deletion in one read
        originalWarn = DatabaseDescriptor.getTombstoneWarnThreshold();
        originalFail = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneWarnThreshold(Integer.MAX_VALUE);
        DatabaseDescriptor.setTombstoneFailureThreshold(Integer.MAX_VALUE);
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setTombstoneWarnThreshold(originalWarn);
        DatabaseDescriptor.setTombstoneFailureThreshold(originalFail);
    }

    @Test
    public void randomRangeReadsAtScale() throws Throwable
    {
        assertTrue("the burn row count must be at least 100K", ROWS >= 100_000);
        long seed = Long.getLong("cassandra.test.cursor_range_burn_seed", System.currentTimeMillis()); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        logger.info("RangeReadBurnTest format={} rows={} reads={} seed={}L", format, ROWS, READS, seed);
        Random random = new Random(seed);

        ColumnFamilyStore cfs = populate(random);
        long nowInSec = FBUtilities.nowInSeconds() + 60;

        long started = System.nanoTime();
        compareRead(cfs, "full ring", rangeCommand("SELECT * FROM %s", nowInSec), 5000, seed);
        compareRead(cfs, "full ring, v1 = 3 allow filtering", rangeCommand("SELECT * FROM %s WHERE v1 = 3 ALLOW FILTERING", nowInSec), 5000, seed);
        for (int i = 0; i < READS; i++)
        {
            long readSeed = random.nextLong();
            randomRead(cfs, random, nowInSec, readSeed);
        }
        logger.info("RangeReadBurnTest format={}: {} reads, {} pages, {} response bytes compared in {} ms",
                    format, READS + 2, pagesCompared, bytesCompared, (System.nanoTime() - started) / 1_000_000);
    }

    private long pagesCompared;
    private long bytesCompared;

    /** A random read over a token window that holds about WINDOW_ROWS rows or fewer. */
    private void randomRead(ColumnFamilyStore cfs, Random r, long nowInSec, long readSeed)
    {
        double fraction = Math.min(1.0, (double) WINDOW_ROWS / ROWS) * r.nextDouble();
        long left = r.nextLong();
        long right = left + (long) (fraction * 0x1p64);
        if (right < left)
            right = Long.MAX_VALUE;
        String window = " token " + left + ".." + right;
        int limit = 1 + r.nextInt(2000);
        int v1 = r.nextInt(10);
        int pageSize = new int[]{ 100, 5000, 20000 }[r.nextInt(3)];
        String inWindow = " WHERE token(pk) > ? AND token(pk) <= ?";
        switch (r.nextInt(6))
        {
            case 0:
                compareRead(cfs, "one read," + window, rangeCommand("SELECT * FROM %s" + inWindow, nowInSec, left, right), 0, readSeed);
                break;
            case 1:
                compareRead(cfs, "paged " + pageSize + ',' + window, rangeCommand("SELECT * FROM %s" + inWindow, nowInSec, left, right), pageSize, readSeed);
                break;
            case 2:
                compareRead(cfs, "from token " + left + " limit " + limit, rangeCommand("SELECT * FROM %s WHERE token(pk) > ? LIMIT " + limit, nowInSec, left), 0, readSeed);
                break;
            case 3:
                compareRead(cfs, "v1 = " + v1 + " allow filtering, paged " + pageSize + ',' + window,
                            rangeCommand("SELECT * FROM %s" + inWindow + " AND v1 = ? ALLOW FILTERING", nowInSec, left, right, v1), pageSize, readSeed);
                break;
            case 4:
                compareRead(cfs, "distinct," + window, rangeCommand("SELECT DISTINCT pk, s FROM %s" + inWindow, nowInSec, left, right), 0, readSeed);
                break;
            default:
                compareRead(cfs, "columns, per partition limit 5," + window,
                            rangeCommand("SELECT pk, ck, v1 FROM %s" + inWindow + " PER PARTITION LIMIT 5", nowInSec, left, right), 0, readSeed);
        }
    }

    /**
     * Compares the replica responses of {@code base} on both paths, page by page when
     * {@code pageSize} is positive: each page resumes after the last row of the one before, as the
     * range pager asks for it.  Holds one page at a time, so the row count has no memory cap.  A
     * page whose bytes differ is run again through the oracle for the report.
     */
    private void compareRead(ColumnFamilyStore cfs, String label, PartitionRangeReadCommand base, int pageSize, long readSeed)
    {
        PartitionRangeReadCommand page = pageSize > 0 ? base.withUpdatedLimit(base.limits().forPaging(pageSize)) : base;
        TableMetadata metadata = cfs.metadata();
        for (int n = 0; ; n++)
        {
            comparePage(cfs, label + " / page " + n, page, readSeed);
            if (pageSize <= 0)
                return;
            DecoratedKey lastKey = null;
            Clustering<?> lastClustering = null;
            int rowsInPage = 0;
            int rowsInLastPartition = 0;
            setRangePath(false);
            try (ReadExecutionController controller = page.executionController();
                 UnfilteredPartitionIterator partitions = page.executeLocally(controller))
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        rowsInLastPartition = 0;
                        while (partition.hasNext())
                        {
                            Unfiltered unfiltered = partition.next();
                            if (!unfiltered.isRow())
                                continue;
                            lastKey = partition.partitionKey();
                            lastClustering = ((Row) unfiltered).clustering();
                            rowsInPage++;
                            rowsInLastPartition++;
                        }
                    }
                }
            }
            if (rowsInPage < pageSize)
                return;
            AbstractBounds<PartitionPosition> bounds = new Bounds<>(lastKey, base.dataRange().keyRange().right);
            DataRange range = base.dataRange().forPaging(bounds, metadata.comparator, lastClustering, false);
            DataLimits limits = base.limits().forPaging(pageSize, lastKey.getKey(), Integer.MAX_VALUE - rowsInLastPartition);
            page = base.withUpdatedLimitsAndDataRange(limits, range);
        }
    }

    /** The three replica reads of a page compared on both paths: the data, its digest, and the
     *  data read for a coordinator that tracks warnings, which measures the rows' heap sizes. */
    private static final String[] PAGE_READS = { "data", "digest", "data tracking warnings" };

    private void comparePage(ColumnFamilyStore cfs, String label, PartitionRangeReadCommand command, long readSeed)
    {
        setRangePath(false);
        byte[][] iterator = new byte[PAGE_READS.length][];
        ScanMetricsCapture.Snapshot[] iteratorMetrics = pageReads(cfs, command, iterator);
        setRangePath(true);
        long servedBefore = CursorRangeReads.rangeReadsServed();
        long notServedBefore = CursorRangeReads.rangeReadsNotServed();
        byte[][] cursor = new byte[PAGE_READS.length][];
        ScanMetricsCapture.Snapshot[] cursorMetrics;
        try
        {
            cursorMetrics = pageReads(cfs, command, cursor);
        }
        finally
        {
            setRangePath(false);
        }
        assertEquals(label + ": the cursor path left a read to the iterator path", 0, CursorRangeReads.rangeReadsNotServed() - notServedBefore);
        assertEquals(label + ": the cursor path did not serve every read", PAGE_READS.length, CursorRangeReads.rangeReadsServed() - servedBefore);
        pagesCompared++;
        bytesCompared += iterator[0].length;
        boolean same = true;
        for (int i = 0; i < PAGE_READS.length; i++)
            same &= Arrays.equals(iterator[i], cursor[i]);
        if (same)
        {
            for (int i = 0; i < PAGE_READS.length; i++)
                ScanMetricsCapture.assertParity(label + ", " + PAGE_READS[i] + ", iterator vs cursor", iteratorMetrics[i], cursorMetrics[i]);
            return;
        }
        // the oracle reports where the responses differ
        StringBuilder differ = new StringBuilder();
        for (int i = 0; i < PAGE_READS.length; i++)
        {
            if (Arrays.equals(iterator[i], cursor[i]))
                continue;
            try
            {
                assertResponseBytesEqual(iterator[i], cursor[i]);
                differ.append(String.format("%n  %s: %d vs %d bytes", PAGE_READS[i], iterator[i].length, cursor[i].length));
            }
            catch (AssertionError e)
            {
                differ.append(String.format("%n  %s: %s", PAGE_READS[i], e.getMessage()));
            }
        }
        assertReplicaResponsesMatch(ReadCase.of(label, readSeed).at(command.nowInSec()), cfs, now -> command);
        fail(label + ": the replica responses differ, the oracle found no difference:" + differ);
    }

    /** Runs the {@link #PAGE_READS} of {@code command} on the current path into {@code into}; returns the metrics of each. */
    private static ScanMetricsCapture.Snapshot[] pageReads(ColumnFamilyStore cfs, PartitionRangeReadCommand command, byte[][] into)
    {
        ScanMetricsCapture.Snapshot[] metrics = new ScanMetricsCapture.Snapshot[PAGE_READS.length];
        metrics[0] = ScanMetricsCapture.capture(cfs, () -> into[0] = responseBytes(command));
        metrics[1] = ScanMetricsCapture.capture(cfs, () -> into[1] = responseBytes(digestCopy(command)));
        metrics[2] = ScanMetricsCapture.capture(cfs, () -> {
            PartitionRangeReadCommand tracked = command.withUpdatedLimit(command.limits());
            tracked.trackWarnings();
            try
            {
                into[2] = responseBytes(tracked);
            }
            catch (LocalReadSizeTooLargeException e)
            {
                // the message holds the size the guardrail counted
                into[2] = e.toString().getBytes(StandardCharsets.UTF_8);
            }
        });
        return metrics;
    }

    private static byte[] responseBytes(PartitionRangeReadCommand command)
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, out, MessagingService.current_version);
            return out.toByteArray();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes ROWS distinct rows over FLUSHES sstables and the memtable, three in ten in the wide
     * partitions, plus one change in ten to an earlier row: a row, range or cell deletion, a static
     * write, a TTL overwrite or a plain overwrite.
     */
    private ColumnFamilyStore populate(Random r) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s int static, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        long[] wideNext = new long[WIDE_PARTITIONS];
        long nextSmallPk = WIDE_PARTITIONS;
        long smallNextCk = 0;
        int smallRows = 1;
        long ts = BASE_TS;
        int perStage = ROWS / (FLUSHES + 1);
        int stage = 0;
        for (int written = 1; written <= ROWS; written++)
        {
            long pk;
            long ck;
            if (r.nextInt(10) < 3)
            {
                pk = r.nextInt(WIDE_PARTITIONS);
                ck = wideNext[(int) pk]++;
            }
            else
            {
                if (smallNextCk >= smallRows)
                {
                    nextSmallPk++;
                    smallNextCk = 0;
                    smallRows = 1 + r.nextInt(20);
                }
                pk = nextSmallPk;
                ck = smallNextCk++;
            }
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    pk, ck, (long) r.nextInt(10), "v-" + written, ++ts);
            if (r.nextInt(10) == 0)
                changeEarlierRow(r, r.nextInt(5) == 0 ? r.nextInt(WIDE_PARTITIONS) : WIDE_PARTITIONS + (long) r.nextInt((int) (nextSmallPk - WIDE_PARTITIONS + 1)),
                                 ++ts);
            if (written % perStage == 0 && stage < FLUSHES)
            {
                flush();
                stage++;
            }
        }
        awaitStableView(cfs);
        logger.info("RangeReadBurnTest format={}: {} rows, {} sstables and the memtable", format, ROWS, cfs.getLiveSSTables().size());
        return cfs;
    }

    /**
     * Waits until no memtable of the table is flushing and its memtables and sstables have not
     * changed for two seconds.  A large write fills the memtable, which then flushes on its own; a
     * flush that ends between the two paths' reads of a page would give them different sources.
     */
    private static void awaitStableView(ColumnFamilyStore cfs) throws InterruptedException
    {
        long stableSince = System.nanoTime();
        View last = cfs.getTracker().getView();
        while (true)
        {
            Thread.sleep(100);
            View now = cfs.getTracker().getView();
            if (!now.flushingMemtables.isEmpty() || !now.liveMemtables.equals(last.liveMemtables)
                || !now.liveSSTables().equals(last.liveSSTables()))
            {
                stableSince = System.nanoTime();
                last = now;
            }
            else if (System.nanoTime() - stableSince > TimeUnit.SECONDS.toNanos(2))
            {
                return;
            }
        }
    }

    private void changeEarlierRow(Random r, long pk, long ts) throws Throwable
    {
        long ck = r.nextInt(pk < WIDE_PARTITIONS ? 1000 : 20);
        switch (r.nextInt(6))
        {
            case 0:
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", ts, pk, ck);
                break;
            case 1:
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck >= ? AND ck < ?", ts, pk, ck, ck + 1 + r.nextInt(30));
                break;
            case 2:
                execute("DELETE v2 FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", ts, pk, ck);
                break;
            case 3:
                execute("UPDATE %s USING TIMESTAMP ? SET s = ? WHERE pk = ?", ts, r.nextInt(), pk);
                break;
            case 4:
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ? AND TTL " + LONG_TTL,
                        pk, ck, (long) r.nextInt(10), "ttl", ts);
                break;
            default:
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        pk, ck, (long) r.nextInt(10), "overwrite", ts);
        }
    }
}
