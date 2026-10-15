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

import java.lang.management.ManagementFactory;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Bounds what a cursor range read builds on the heap, on BTI.  Two measures:
 * <ul>
 *   <li>{@link CursorReads#unfilteredsMaterialized()}: the rows and markers the cursor path built as
 *       objects.  A replica response is written from the sstable bytes and builds none; a limited or
 *       paged read builds the rows it returns plus at most one row index block where it starts.</li>
 *   <li>the heap the reading thread allocates, against the iterator range path on the same reads:
 *       a replica response must allocate well below half the iterator path's bytes, and a page of a
 *       paged scan must allocate a bounded amount per row of the page.</li>
 * </ul>
 * Each case also checks that both paths return the same result.  Sizes come from
 * {@code -Dcassandra.test.cursor_range_partitions} (default {@value #DEFAULT_SMALL_PARTITIONS}) and
 * {@code -Dcassandra.test.cursor_range_rows} (rows of the wide partition, default {@value #DEFAULT_WIDE_ROWS}).
 */
public class RangeReadMaterializationBoundTest extends RangeReadOracle
{
    private static final Logger logger = LoggerFactory.getLogger(RangeReadMaterializationBoundTest.class);

    private static final int DEFAULT_SMALL_PARTITIONS = 20_000;
    private static final int SMALL_PARTITIONS = Integer.getInteger("cassandra.test.cursor_range_partitions", DEFAULT_SMALL_PARTITIONS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int DEFAULT_WIDE_ROWS = 200_000;
    private static final int WIDE_ROWS = Integer.getInteger("cassandra.test.cursor_range_rows", DEFAULT_WIDE_ROWS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int ROWS_PER_SMALL_PARTITION = 5;
    private static final int COLUMN_INDEX_SIZE_KIB = 4;
    private static final int LIMIT = 10;
    private static final int PAGE_SIZE = 5000;
    /** Heap one cursor page may allocate on the reading thread, per row of the page. */
    private static final long ALLOCATED_BYTES_PER_PAGE_ROW = 2048;

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;

    @Before
    public void pinBtiAndBlockSize()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(COLUMN_INDEX_SIZE_KIB);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    // ---------------------------------------------------------------- replica responses

    @Test
    public void replicaResponseOfOneSSTableBuildsNoRows() throws Throwable
    {
        ColumnFamilyStore cfs = loadSmallPartitions(1);
        assertReplicaResponseBuildsNoRowsAndAllocatesLess(cfs);
    }

    @Test
    public void replicaResponseOfOverlappingSSTablesBuildsNoRows() throws Throwable
    {
        ColumnFamilyStore cfs = loadSmallPartitions(4);
        assertReplicaResponseBuildsNoRowsAndAllocatesLess(cfs);
    }

    private void assertReplicaResponseBuildsNoRowsAndAllocatesLess(ColumnFamilyStore cfs)
    {
        long now = FBUtilities.nowInSeconds();
        PartitionRangeReadCommand command = rangeCommand("SELECT * FROM %s", now);
        setRangePath(false);
        // warm both paths once, so class loading and first-use buffers are not counted
        serialize(ReadCommandVerbHandler.instance.doRead(command, false));
        long allocated = threadAllocatedBytes();
        ReadResponse iteratorResponse = ReadCommandVerbHandler.instance.doRead(command, false);
        long iteratorAllocated = threadAllocatedBytes() - allocated;
        byte[] iterator = serialize(iteratorResponse);

        setRangePath(true);
        try
        {
            serialize(ReadCommandVerbHandler.instance.doRead(command, false));
            long served = CursorRangeReads.rangeReadsServed();
            long materialized = CursorReads.unfilteredsMaterialized();
            allocated = threadAllocatedBytes();
            ReadResponse cursorResponse = ReadCommandVerbHandler.instance.doRead(command, false);
            long cursorAllocated = threadAllocatedBytes() - allocated;
            byte[] cursor = serialize(cursorResponse);
            assertEquals("the cursor path served the reads", 1, CursorRangeReads.rangeReadsServed() - served);
            assertArrayEquals("the replica responses differ", iterator, cursor);
            long rows = (long) SMALL_PARTITIONS * ROWS_PER_SMALL_PARTITION;
            logger.info("replica response of {} rows over {} sstables: cursor allocated {} bytes ({} per row), iterator {} ({} per row)",
                        rows, cfs.getLiveSSTables().size(), cursorAllocated, cursorAllocated / rows, iteratorAllocated, iteratorAllocated / rows);
            assertEquals("rows built as objects for a replica response", 0, CursorReads.unfilteredsMaterialized() - materialized);
            assertTrue("the cursor path allocated " + cursorAllocated + " bytes for the replica response, the iterator path " + iteratorAllocated,
                       cursorAllocated * 2 < iteratorAllocated);
        }
        finally
        {
            setRangePath(false);
        }
        assertAllReadSurfacesMatch(ReadCase.of("full scan of " + cfs.getLiveSSTables().size() + " sstables", 1).at(now), cfs,
                                   at -> rangeCommand("SELECT * FROM %s", at));
    }

    // ---------------------------------------------------------------- wide partition

    @Test
    public void limitedReadOfAWidePartitionBuildsAtMostOneBlockPastTheLimit() throws Throwable
    {
        ColumnFamilyStore cfs = loadWidePartition();
        long now = FBUtilities.nowInSeconds();
        long built = cursorMaterialized(() -> List.of(rangeCommand("SELECT * FROM %s LIMIT " + LIMIT, now)));
        assertTrue("LIMIT " + LIMIT + " built " + built + " rows", built <= LIMIT + rowsPerBlock() + 1);
    }

    @Test
    public void distinctBuildsAtMostOneBlockPerPartition() throws Throwable
    {
        ColumnFamilyStore cfs = loadWidePartition();
        long now = FBUtilities.nowInSeconds();
        long built = cursorMaterialized(() -> List.of(rangeCommand("SELECT DISTINCT k FROM %s", now)));
        // two partitions: the wide one and a small one
        assertTrue("DISTINCT built " + built + " rows", built <= 2 * (rowsPerBlock() + 1));
    }

    @Test
    public void pagedScanOfAWidePartitionBuildsEachRowAboutOnceAndBoundsEachPage() throws Throwable
    {
        ColumnFamilyStore cfs = loadWidePartition();
        long now = FBUtilities.nowInSeconds();
        PartitionRangeReadCommand command = rangeCommand("SELECT * FROM %s", now);
        PartitionRangeQueryPager iteratorPager = (PartitionRangeQueryPager) command.getPager(null, ProtocolVersion.V5);
        PartitionRangeQueryPager cursorPager = (PartitionRangeQueryPager) command.getPager(null, ProtocolVersion.V5);
        long built = 0;
        long maxPageAllocated = 0;
        int pages = 0;
        while (!iteratorPager.isExhausted())
        {
            assertTrue("paging did not terminate", pages++ < WIDE_ROWS / PAGE_SIZE + 10);
            setRangePath(false);
            List<String> iteratorPage = page(cfs, iteratorPager);
            setRangePath(true);
            try
            {
                long served = CursorRangeReads.rangeReadsServed();
                long materialized = CursorReads.unfilteredsMaterialized();
                long allocated = threadAllocatedBytes();
                List<String> cursorPage = page(cfs, cursorPager);
                maxPageAllocated = Math.max(maxPageAllocated, threadAllocatedBytes() - allocated);
                built += CursorReads.unfilteredsMaterialized() - materialized;
                assertEquals("the cursor path served the page", 1, CursorRangeReads.rangeReadsServed() - served);
                compareRecords(iteratorPage, cursorPage);
            }
            finally
            {
                setRangePath(false);
            }
        }
        assertTrue("cursor paging continued after iterator paging ended", cursorPager.isExhausted());
        logger.info("paged scan of {} rows: {} rows built, at most {} bytes allocated by one page", WIDE_ROWS, built, maxPageAllocated);
        assertTrue("the paged scan built " + built + " rows for " + WIDE_ROWS, built <= WIDE_ROWS + pages * (rowsPerBlock() + 1));
        assertTrue("one page allocated " + maxPageAllocated + " bytes", maxPageAllocated <= PAGE_SIZE * ALLOCATED_BYTES_PER_PAGE_ROW);
    }

    // ---------------------------------------------------------------- data

    private ColumnFamilyStore loadSmallPartitions(int sstables) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v bigint, p text, PRIMARY KEY (k, c)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 16}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int s = 0; s < sstables; s++)
        {
            for (int k = 0; k < SMALL_PARTITIONS; k++)
                for (int c = 0; c < ROWS_PER_SMALL_PARTITION; c++)
                    if ((k + c) % sstables == s)
                        execute("INSERT INTO %s (k, c, v, p) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", k, c, (long) c, "p" + k * 31 + c, 1000L + s);
            flush();
        }
        return cfs;
    }

    /** One wide partition of {@link #WIDE_ROWS} rows and one small one, in one sstable. */
    private ColumnFamilyStore loadWidePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v bigint, p text, PRIMARY KEY (k, c)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 16}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int c = 0; c < WIDE_ROWS; c++)
            execute("INSERT INTO %s (k, c, v, p) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, c, (long) c, "payload-" + c);
        for (int c = 0; c < 5; c++)
            execute("INSERT INTO %s (k, c, v, p) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 2, c, (long) c, "payload-" + c);
        flush();
        return cfs;
    }

    /** Rows of about 25 bytes per row index block, generously rounded up. */
    private static long rowsPerBlock()
    {
        return COLUMN_INDEX_SIZE_KIB * 1024L / 16;
    }

    // ---------------------------------------------------------------- measurement

    @FunctionalInterface
    private interface Commands
    {
        List<PartitionRangeReadCommand> get();
    }

    /** Runs the reads on both paths, checks they match and that the cursor path served them, and
     *  returns the rows the cursor run built. */
    private long cursorMaterialized(Commands commands)
    {
        List<PartitionRangeReadCommand> reads = commands.get();
        setRangePath(false);
        List<List<String>> iterator = records(reads);
        setRangePath(true);
        try
        {
            long served = CursorRangeReads.rangeReadsServed();
            long materialized = CursorReads.unfilteredsMaterialized();
            List<List<String>> cursor = records(commands.get());
            long built = CursorReads.unfilteredsMaterialized() - materialized;
            assertEquals("the cursor path served every read", reads.size(), CursorRangeReads.rangeReadsServed() - served);
            for (int i = 0; i < reads.size(); i++)
                compareRecords(iterator.get(i), cursor.get(i));
            return built;
        }
        finally
        {
            setRangePath(false);
        }
    }

    private static List<List<String>> records(List<PartitionRangeReadCommand> reads)
    {
        List<List<String>> all = new java.util.ArrayList<>();
        for (PartitionRangeReadCommand read : reads)
        {
            try (ReadExecutionController controller = read.executionController();
                 UnfilteredPartitionIterator partitions = read.executeLocally(controller))
            {
                all.add(canonicalRecords(partitions));
            }
        }
        return all;
    }

    private static List<String> page(ColumnFamilyStore cfs, PartitionRangeQueryPager pager)
    {
        try (ReadExecutionController controller = pager.executionController();
             UnfilteredPartitionIterator partitions = pager.fetchPageUnfiltered(cfs.metadata(), PAGE_SIZE, controller))
        {
            return canonicalRecords(partitions);
        }
    }

    private static byte[] serialize(ReadResponse response)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, out, MessagingService.current_version);
            return out.toByteArray();
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    private static long threadAllocatedBytes()
    {
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        return threads.getThreadAllocatedBytes(Thread.currentThread().getId());
    }
}
