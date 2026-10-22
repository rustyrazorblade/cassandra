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
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Checks that a forward cursor read of one large BTI partition materializes only the rows it
 * returns, plus at most one row index block per sstable at each place it starts reading.
 * {@link CursorReads#unfilteredsMaterialized()} is the measure.  Every case also checks that the
 * cursor result equals the iterator result.
 *
 * <p>The read amplification cases count the bytes each read takes from the data files, with the
 * chunk cache emptied before it, and bound them at a few compressed chunks per sstable plus the
 * rows read; they also check the cursor path reads no more bytes than the iterator path.
 *
 * <p>The partition row count comes from {@code -Dcassandra.test.cursor_stream_rows=N} (default
 * {@value #DEFAULT_ROWS}).
 */
public class BtiCursorReadMaterializationBoundTest extends CursorReadDifferentialTester
{
    private static final int DEFAULT_ROWS = 200_000;
    private static final int ROWS = Integer.getInteger("cassandra.test.cursor_stream_rows", DEFAULT_ROWS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final long PK = 1L;
    /** Rows per c1 value; c2 runs 0..C2_PER_C1-1 under each c1. */
    private static final int C2_PER_C1 = 1000;
    private static final int COLUMN_INDEX_SIZE_KIB = 4;
    private static final int LIMIT = 10;
    private static final int PAGE_SIZE = 5000;
    private static final int SSTABLE_ROUNDS = 3;
    /** Every Nth row is overwritten in the memtable for the cases that read sstables plus memtable. */
    private static final int MEMTABLE_OVERWRITE_EVERY = 100;
    private static final int SLICE_C2_START = 100;
    private static final int SLICE_C2_END = 119;
    /** Heap allocated by the reading thread for one cursor page, per row of the page.  The page
     *  check below fails when a page costs more than this times the page size. */
    private static final long ALLOCATED_BYTES_PER_PAGE_ROW = 4096;

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;
    /** The most heap one cursor page allocated on the reading thread, set by {@link #pagedCursorMaterialized}. */
    private long maxCursorPageAllocatedBytes;

    @Before
    public void pinFormatAndBlockSize()
    {
        assertTrue("row count must be at least 10000 so the slices land in distinct c1 groups", ROWS >= 10_000);
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(COLUMN_INDEX_SIZE_KIB);
    }

    @After
    public void restoreFormatAndBlockSize()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    // ---------------------------------------------------------------- one sstable

    @Test
    public void limitOnOneSSTableMaterializesAtMostOneBlockPastTheLimit() throws Throwable
    {
        ColumnFamilyStore cfs = loadOneSSTable();
        long now = FBUtilities.nowInSeconds();
        long perRun = cursorMaterializedPerRun(cfs, () -> limitRead(cfs, now));
        // a read from the partition start needs no seek, so one block is a generous allowance
        assertWithinBound("LIMIT " + LIMIT + " on one sstable", perRun, LIMIT + rowsPerBlock(cfs) + 1);
    }

    @Test
    public void pagedReadOfOneSSTableMaterializesEachRowAboutOnce() throws Throwable
    {
        ColumnFamilyStore cfs = loadOneSSTable();
        long now = FBUtilities.nowInSeconds();
        long total = pagedCursorMaterialized(cfs, () -> fullRead(cfs, now));
        assertWithinBound("paged read of one sstable, page size " + PAGE_SIZE, total, 2L * ROWS);
    }

    @Test
    public void pagedReadOfOneSSTableAllocatesABoundedAmountPerPage() throws Throwable
    {
        ColumnFamilyStore cfs = loadOneSSTable();
        long now = FBUtilities.nowInSeconds();
        pagedCursorMaterialized(cfs, () -> fullRead(cfs, now));
        long bound = PAGE_SIZE * ALLOCATED_BYTES_PER_PAGE_ROW;
        logger.info("paged read of one sstable, page size {} rows: {} largest page allocated {} bytes, bound {}",
                    PAGE_SIZE, ROWS, maxCursorPageAllocatedBytes, bound);
        assertTrue("a cursor page of " + PAGE_SIZE + " rows over a " + ROWS + "-row partition allocated " +
                   maxCursorPageAllocatedBytes + " bytes, bound " + bound, maxCursorPageAllocatedBytes <= bound);
    }

    @Test
    public void namesReadOfTwoFarApartRowsOnOneSSTableMaterializesOnlyTheirBlocks() throws Throwable
    {
        ColumnFamilyStore cfs = loadOneSSTable();
        long now = FBUtilities.nowInSeconds();
        long perRun = cursorMaterializedPerRun(cfs, () -> namesRead(cfs, now));
        assertWithinBound("names read of 2 rows on one sstable", perRun, 2L * (blockAllowance(cfs) + 2));
    }

    @Test
    public void multiSliceReadOnOneSSTableMaterializesOnlyRowsNearEachSlice() throws Throwable
    {
        ColumnFamilyStore cfs = loadOneSSTable();
        long now = FBUtilities.nowInSeconds();
        long[] c1Values = sliceC1Values();
        long perRun = cursorMaterializedPerRun(cfs, () -> multiSliceRead(cfs, now, c1Values));
        assertWithinBound("multi-slice read of " + c1Values.length + " slices on one sstable", perRun,
                          multiSliceBound(cfs, c1Values.length));
    }

    // ---------------------------------------------------------------- several sstables plus memtable

    @Test
    public void limitAcrossSSTablesAndMemtableMaterializesAtMostOneBlockPastTheLimit() throws Throwable
    {
        ColumnFamilyStore cfs = loadSSTablesAndMemtable();
        long now = FBUtilities.nowInSeconds();
        long perRun = cursorMergeMaterializedPerRun(cfs, () -> limitRead(cfs, now));
        assertWithinBound("LIMIT " + LIMIT + " across sstables and memtable", perRun, LIMIT + rowsPerBlock(cfs) + 1);
    }

    @Test
    public void pagedReadAcrossSSTablesAndMemtableMaterializesEachRowAboutOnce() throws Throwable
    {
        ColumnFamilyStore cfs = loadSSTablesAndMemtable();
        long now = FBUtilities.nowInSeconds();
        long mergesBefore = CursorReads.cursorMergesServed();
        long total = pagedCursorMaterialized(cfs, () -> fullRead(cfs, now));
        assertTrue("paged read did not run the cursor merge", CursorReads.cursorMergesServed() > mergesBefore);
        assertWithinBound("paged read across sstables and memtable, page size " + PAGE_SIZE, total, 2L * ROWS);
    }

    @Test
    public void namesReadOfTwoFarApartRowsAcrossSSTablesAndMemtableMaterializesOnlyTheirBlocks() throws Throwable
    {
        ColumnFamilyStore cfs = loadSSTablesAndMemtable();
        long now = FBUtilities.nowInSeconds();
        long perRun = cursorMaterializedPerRun(cfs, () -> namesRead(cfs, now));
        assertWithinBound("names read of 2 rows across sstables and memtable", perRun, 2L * (blockAllowance(cfs) + 2));
    }

    @Test
    public void multiSliceReadAcrossSSTablesAndMemtableMaterializesOnlyRowsNearEachSlice() throws Throwable
    {
        ColumnFamilyStore cfs = loadSSTablesAndMemtable();
        long now = FBUtilities.nowInSeconds();
        long[] c1Values = sliceC1Values();
        long perRun = cursorMergeMaterializedPerRun(cfs, () -> multiSliceRead(cfs, now, c1Values));
        assertWithinBound("multi-slice read of " + c1Values.length + " slices across sstables and memtable", perRun,
                          multiSliceBound(cfs, c1Values.length));
    }

    // ---------------------------------------------------------------- read amplification

    /** Rows of the compressed table hold about this many bytes, so a chunk holds many rows. */
    private static final int AMPLIFICATION_ROW_PADDING = 80;

    @Test
    public void readAmplificationOnOneSSTable() throws Throwable
    {
        ColumnFamilyStore cfs = loadCompressed(1, false);
        assertReadAmplificationBounded("one sstable", cfs);
    }

    private static final long FIXED_TIMESTAMP_BASE = 1_700_000_000_000_000L;

    /**
     * Reverse LIMIT reads ending at each row around the middle of the partition, read cold: the
     * cursor path may read no more of the data file than the iterator path.  Where the read ends
     * just before a compression chunk boundary, the iterator path reads one chunk; the cursor path
     * once read the next chunk too.  The rows have fixed timestamps, so the chunk boundaries fall
     * at the same rows on every run.
     */
    @Test
    public void reverseReadsEndingNearChunkBoundariesOnOneSSTable() throws Throwable
    {
        ColumnFamilyStore cfs = loadCompressed(1, false, true);
        long now = FBUtilities.nowInSeconds();
        long mid = ROWS / 2;
        for (long end = mid - 250; end <= mid + 250; end++)
        {
            long last = end;
            Supplier<SinglePartitionReadCommand> command = () -> sliceOf(cfs, now, ROWS / 5, last, DataLimits.cqlLimits(LIMIT), true);
            for (boolean replica : new boolean[]{ false, true })
            {
                long iterator = dataFileBytesRead(command, false, replica);
                long cursor = dataFileBytesRead(command, true, replica);
                String surface = replica ? "replica response" : "executeLocally";
                assertTrue("reverse LIMIT " + LIMIT + " ending at row " + end + " via " + surface + ": the cursor path read " + cursor
                           + " bytes, the iterator path " + iterator, cursor <= iterator);
            }
        }
    }

    @Test
    public void readAmplificationAcrossSSTablesAndMemtable() throws Throwable
    {
        ColumnFamilyStore cfs = loadCompressed(SSTABLE_ROUNDS, true);
        assertReadAmplificationBounded("several sstables and memtable", cfs);
    }

    /** Rows in about 1 MB of row data: more than the cursor path reads through the chunk cache
     *  before it switches to the read-ahead reader. */
    private static final int LONG_SLICE_ROWS = (1 << 20) / (AMPLIFICATION_ROW_PADDING + 64);

    @Test
    public void readAmplificationOfALongSliceOnOneSSTable() throws Throwable
    {
        ColumnFamilyStore cfs = loadCompressed(1, false);
        assertLongSlicesReadNoMoreThanTheIterator("one sstable", cfs);
    }

    @Test
    public void readAmplificationOfALongSliceAcrossSSTablesAndMemtable() throws Throwable
    {
        ColumnFamilyStore cfs = loadCompressed(SSTABLE_ROUNDS, true);
        assertLongSlicesReadNoMoreThanTheIterator("several sstables and memtable", cfs);
    }

    /**
     * Slices of about 1 MB of rows, read cold: the cursor path may read no more of the data files
     * than the iterator path, give or take one compression chunk per sstable.  A slice ending in
     * the middle of the partition, one at an exact row count, one stopped by a limit, and a slice
     * that runs to the partition end.
     */
    private void assertLongSlicesReadNoMoreThanTheIterator(String layout, ColumnFamilyStore cfs) throws IOException
    {
        long now = FBUtilities.nowInSeconds();
        long mid = ROWS / 2;
        assertLongReadNoMoreThanTheIterator(layout + ", slice of " + LONG_SLICE_ROWS + " rows", cfs,
                                            () -> sliceOf(cfs, now, mid, mid + LONG_SLICE_ROWS - 1, DataLimits.NONE));
        assertLongReadNoMoreThanTheIterator(layout + ", slice of 1000 rows from the middle", cfs,
                                            () -> sliceOf(cfs, now, mid, mid + 999, DataLimits.NONE));
        assertLongReadNoMoreThanTheIterator(layout + ", LIMIT " + LONG_SLICE_ROWS + " from the middle", cfs,
                                            () -> sliceOf(cfs, now, mid, ROWS - 1, DataLimits.cqlLimits(LONG_SLICE_ROWS)));
        assertLongReadNoMoreThanTheIterator(layout + ", slice to the partition end", cfs,
                                            () -> sliceOf(cfs, now, ROWS - LONG_SLICE_ROWS, ROWS - 1, DataLimits.NONE));
    }

    private void assertLongReadNoMoreThanTheIterator(String read, ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    throws IOException
    {
        assertCursorReadMatchesIterator(cfs, command);
        long chunk = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            chunk = Math.max(chunk, sstable.compression ? sstable.getCompressionMetadata().chunkLength() : 64 << 10);
        long tolerance = cfs.getLiveSSTables().size() * chunk;
        for (boolean replica : new boolean[]{ false, true })
        {
            long iterator = dataFileBytesRead(command, false, replica);
            long cursor = dataFileBytesRead(command, true, replica);
            String surface = replica ? "replica response" : "executeLocally";
            logger.info("{} via {}: cursor read {} bytes, iterator {} bytes, tolerance {}", read, surface, cursor, iterator, tolerance);
            assertTrue(read + " via " + surface + ": the cursor path read " + cursor + " bytes from data files, the iterator path "
                       + iterator + " (tolerance " + tolerance + ')', cursor <= iterator + tolerance);
        }
    }

    private void assertReadAmplificationBounded(String layout, ColumnFamilyStore cfs) throws IOException
    {
        long now = FBUtilities.nowInSeconds();
        long mid = ROWS / 2;
        long[] three = { ROWS / 5, ROWS / 2, 4L * ROWS / 5 };
        assertReadsBounded(layout + ", names read of 1 row", cfs, 1, 1,
                           () -> namesOf(cfs, now, mid));
        assertReadsBounded(layout + ", names read of 3 rows", cfs, 3, 3,
                           () -> namesOf(cfs, now, three));
        assertReadsBounded(layout + ", point slice", cfs, 1, 1,
                           () -> sliceOf(cfs, now, mid, mid, DataLimits.NONE));
        assertReadsBounded(layout + ", LIMIT " + LIMIT + " from the partition start", cfs, 1, LIMIT,
                           () -> limitRead(cfs, now));
        assertReadsBounded(layout + ", LIMIT " + LIMIT + " from the middle", cfs, 1, LIMIT,
                           () -> sliceOf(cfs, now, mid, ROWS - 1, DataLimits.cqlLimits(LIMIT)));
        assertReadsBounded(layout + ", reverse LIMIT " + LIMIT + " from the partition end", cfs, 1, LIMIT,
                           () -> sliceOf(cfs, now, 0, ROWS - 1, DataLimits.cqlLimits(LIMIT), true));
        assertReadsBounded(layout + ", reverse LIMIT " + LIMIT + " from the middle", cfs, 1, LIMIT,
                           () -> sliceOf(cfs, now, ROWS / 5, mid, DataLimits.cqlLimits(LIMIT), true));
        int page = 100;
        assertReadsBounded(layout + ", page of " + page + " resumed in the middle", cfs, 1, page,
                           () -> (SinglePartitionReadCommand) fullRead(cfs, now).forPaging(clusteringOf(mid), DataLimits.cqlLimits(page)));
    }

    /**
     * Runs the read on both paths, through executeLocally and the replica response, each with an
     * empty chunk cache, and bounds the data file bytes the cursor path reads.  Each sstable may
     * cost its partition header chunk plus two chunks around each place the read starts, and the
     * rows read add their own chunks.
     */
    private void assertReadsBounded(String read, ColumnFamilyStore cfs, int starts, int rowsRead,
                                    Supplier<SinglePartitionReadCommand> command) throws IOException
    {
        assertCursorReadMatchesIterator(cfs, command);
        long chunk = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            chunk = Math.max(chunk, sstable.compression ? sstable.getCompressionMetadata().chunkLength() : 64 << 10);
        int sstables = cfs.getLiveSSTables().size();
        long rowBytes = (long) rowsRead * (AMPLIFICATION_ROW_PADDING + 64);
        long bound = sstables * (1L + 2L * starts) * chunk + rowBytes + chunk;
        for (boolean replica : new boolean[]{ false, true })
        {
            long iterator = dataFileBytesRead(command, false, replica);
            long cursor = dataFileBytesRead(command, true, replica);
            String surface = replica ? "replica response" : "executeLocally";
            logger.info("{} via {}: {} rows partition, {} sstables: cursor read {} bytes, iterator {} bytes, bound {}",
                        read, surface, ROWS, sstables, cursor, iterator, bound);
            assertTrue(read + " via " + surface + ": the cursor path read " + cursor + " bytes from data files, bound " + bound +
                       " (iterator " + iterator + ')', cursor <= bound);
            assertTrue(read + " via " + surface + ": the cursor path read " + cursor + " bytes, the iterator path " + iterator,
                       cursor <= iterator);
        }
    }

    /** The bytes one read takes from data files, from the JDK's file read events. */
    private static long dataFileBytesRead(Supplier<SinglePartitionReadCommand> command, boolean cursor, boolean replica) throws IOException
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
        Path dump = Files.createTempFile("cursor-read-amplification", ".jfr");
        try (Recording recording = new Recording())
        {
            recording.enable("jdk.FileRead").withThreshold(Duration.ZERO);
            if (ChunkCache.instance != null)
                ChunkCache.instance.close();
            SinglePartitionReadCommand read = command.get();
            recording.start();
            if (replica)
            {
                ReadCommandVerbHandler.instance.doRead(read, false);
            }
            else
            {
                try (ReadExecutionController controller = read.executionController();
                     UnfilteredPartitionIterator partitions = read.executeLocally(controller))
                {
                    canonicalRecords(partitions);
                }
            }
            recording.stop();
            recording.dump(dump);
            long bytes = 0;
            for (RecordedEvent event : RecordingFile.readAllEvents(dump))
            {
                String file = event.getString("path");
                if (file != null && file.endsWith("Data.db") && event.getThread() != null
                    && event.getThread().getJavaThreadId() == Thread.currentThread().getId())
                    bytes += Math.max(0, event.getLong("bytesRead"));
            }
            return bytes;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
            Files.deleteIfExists(dump);
        }
    }

    /** {@link #ROWS} rows of about {@link #AMPLIFICATION_ROW_PADDING} bytes in an LZ4 table, over
     *  {@code sstables} interleaved sstables, and memtable overwrites when asked. */
    private ColumnFamilyStore loadCompressed(int sstables, boolean memtable) throws Throwable
    {
        return loadCompressed(sstables, memtable, false);
    }

    /** @param fixedTimestamps write row i at a timestamp of its own, so the data file layout, and
     *                         where its compression chunks start, is the same on every run */
    private ColumnFamilyStore loadCompressed(int sstables, boolean memtable, boolean fixedTimestamps) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, c1 bigint, c2 bigint, v1 bigint, v2 text, PRIMARY KEY (pk, c1, c2)) " +
                    "WITH compression = {'class': 'LZ4Compressor'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String padding = "p".repeat(AMPLIFICATION_ROW_PADDING);
        for (int round = 0; round < sstables; round++)
        {
            for (long i = round; i < ROWS; i += sstables)
            {
                String timestamp = fixedTimestamps ? " USING TIMESTAMP " + (FIXED_TIMESTAMP_BASE + i) : "";
                execute("INSERT INTO %s (pk, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)" + timestamp,
                        PK, i / C2_PER_C1, i % C2_PER_C1, i, padding + i);
            }
            flush();
        }
        if (memtable)
        {
            for (long i = 0; i < ROWS; i += MEMTABLE_OVERWRITE_EVERY)
                execute("UPDATE %s SET v2 = ? WHERE pk = ? AND c1 = ? AND c2 = ?",
                        "memtable-" + i, PK, i / C2_PER_C1, i % C2_PER_C1);
        }
        // a large load can flush early: one sstable is compacted back to one, several stay several
        if (sstables == 1 && cfs.getLiveSSTables().size() > 1)
            cfs.forceMajorCompaction();
        if (sstables == 1)
            assertEquals(1, cfs.getLiveSSTables().size());
        else
            assertTrue("expected several sstables", cfs.getLiveSSTables().size() >= sstables);
        return cfs;
    }

    private static Clustering<?> clusteringOf(long i)
    {
        return Clustering.make(ByteBufferUtil.bytes(i / C2_PER_C1), ByteBufferUtil.bytes(i % C2_PER_C1));
    }

    private static SinglePartitionReadCommand namesOf(ColumnFamilyStore cfs, long now, long... rows)
    {
        var builder = Util.cmd(cfs, PK).withNowInSeconds(now);
        for (long i : rows)
            builder = builder.includeRow(i / C2_PER_C1, i % C2_PER_C1);
        return (SinglePartitionReadCommand) builder.build();
    }

    private static SinglePartitionReadCommand sliceOf(ColumnFamilyStore cfs, long now, long from, long to, DataLimits limits)
    {
        return sliceOf(cfs, now, from, to, limits, false);
    }

    private static SinglePartitionReadCommand sliceOf(ColumnFamilyStore cfs, long now, long from, long to, DataLimits limits,
                                                      boolean reversed)
    {
        TableMetadata metadata = cfs.metadata();
        Slice slice = Slice.make(ClusteringBound.create(metadata.comparator, true, true, from / C2_PER_C1, from % C2_PER_C1),
                                 ClusteringBound.create(metadata.comparator, false, true, to / C2_PER_C1, to % C2_PER_C1));
        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(), limits,
                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(PK)),
                                                 new ClusteringIndexSliceFilter(Slices.with(metadata.comparator, slice), reversed));
    }

    // ---------------------------------------------------------------- data

    private void createWideTable()
    {
        createTable("CREATE TABLE %s (pk bigint, c1 bigint, c2 bigint, v1 bigint, v2 text, " +
                    "PRIMARY KEY (pk, c1, c2)) WITH compression = {'enabled': 'false'}");
    }

    private void insertRow(long i) throws Throwable
    {
        execute("INSERT INTO %s (pk, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)",
                PK, i / C2_PER_C1, i % C2_PER_C1, i, "value-" + i);
    }

    /** One partition of {@link #ROWS} rows in exactly one sstable, empty memtable. */
    private ColumnFamilyStore loadOneSSTable() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long i = 0; i < ROWS; i++)
            insertRow(i);
        flush();
        if (cfs.getLiveSSTables().size() > 1)
            cfs.forceMajorCompaction();
        assertEquals("expected exactly one sstable", 1, cfs.getLiveSSTables().size());
        assertTrue("memtable must be empty", cfs.getTracker().getView().getCurrentMemtable().isClean());
        return cfs;
    }

    /** One partition of {@link #ROWS} rows split across several sstables, plus memtable overwrites. */
    private ColumnFamilyStore loadSSTablesAndMemtable() throws Throwable
    {
        createWideTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < SSTABLE_ROUNDS; round++)
        {
            for (long i = round; i < ROWS; i += SSTABLE_ROUNDS)
                insertRow(i);
            flush();
        }
        for (long i = 0; i < ROWS; i += MEMTABLE_OVERWRITE_EVERY)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND c1 = ? AND c2 = ?",
                    "memtable-" + i, PK, i / C2_PER_C1, i % C2_PER_C1);
        assertTrue("expected several sstables", cfs.getLiveSSTables().size() >= SSTABLE_ROUNDS);
        assertFalse("memtable must hold data", cfs.getTracker().getView().getCurrentMemtable().isClean());
        return cfs;
    }

    // ---------------------------------------------------------------- commands

    private static SinglePartitionReadCommand fullRead(ColumnFamilyStore cfs, long now)
    {
        return (SinglePartitionReadCommand) Util.cmd(cfs, PK).withNowInSeconds(now).build();
    }

    private static SinglePartitionReadCommand limitRead(ColumnFamilyStore cfs, long now)
    {
        return (SinglePartitionReadCommand) Util.cmd(cfs, PK).withNowInSeconds(now).withLimit(LIMIT).build();
    }

    /** {@code (c1, c2) IN (a, b)} with a at a quarter of the partition and b at three quarters. */
    private static SinglePartitionReadCommand namesRead(ColumnFamilyStore cfs, long now)
    {
        long a = ROWS / 4;
        long b = 3L * ROWS / 4;
        return (SinglePartitionReadCommand) Util.cmd(cfs, PK).withNowInSeconds(now)
                                                .includeRow(a / C2_PER_C1, a % C2_PER_C1)
                                                .includeRow(b / C2_PER_C1, b % C2_PER_C1)
                                                .build();
    }

    /** The c1 values of the multi-slice read: a quarter, half and three quarters into the partition. */
    private static long[] sliceC1Values()
    {
        long groups = ROWS / C2_PER_C1;
        return new long[]{ groups / 4, groups / 2, 3 * groups / 4 };
    }

    /**
     * The read for {@code c1 IN (x, y, z) AND c2 >= 100 AND c2 <= 119}: one slice per c1 value.
     */
    private static SinglePartitionReadCommand multiSliceRead(ColumnFamilyStore cfs, long now, long[] c1Values)
    {
        TableMetadata metadata = cfs.metadata();
        Slices.Builder builder = new Slices.Builder(metadata.comparator);
        for (long c1 : c1Values)
            builder.add(Slice.make(ClusteringBound.create(metadata.comparator, true, true, c1, (long) SLICE_C2_START),
                                   ClusteringBound.create(metadata.comparator, false, true, c1, (long) SLICE_C2_END)));
        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(),
                                                 DataLimits.NONE, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(PK)),
                                                 new ClusteringIndexSliceFilter(builder.build(), false));
    }

    // ---------------------------------------------------------------- bounds

    /**
     * An upper bound on the rows in one row index block.  A block closes once it passes
     * column_index_size, so it holds the rows that fit plus the one that crosses the limit; one
     * more covers rounding of the average row size.
     */
    private static long rowsPerBlock(ColumnFamilyStore cfs)
    {
        long blockBytes = DatabaseDescriptor.getColumnIndexSize(COLUMN_INDEX_SIZE_KIB * 1024);
        long smallestRowBytes = Long.MAX_VALUE;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            smallestRowBytes = Math.min(smallestRowBytes, Math.max(1, sstable.uncompressedLength() / sstable.getTotalRows()));
        return blockBytes / smallestRowBytes + 2;
    }

    /**
     * Rows that may sit in front of one read start position: one block in each sstable.  Blocks
     * of different sstables do not line up, so the rows before the start can come from all of them.
     */
    private static long blockAllowance(ColumnFamilyStore cfs)
    {
        return cfs.getLiveSSTables().size() * rowsPerBlock(cfs);
    }

    private static long multiSliceBound(ColumnFamilyStore cfs, int slices)
    {
        long rowsPerSlice = SLICE_C2_END - SLICE_C2_START + 1;
        return slices * (rowsPerSlice + blockAllowance(cfs) + 1);
    }

    private void assertWithinBound(String read, long measured, long bound)
    {
        logger.info("{} rows: {} materialized {}, bound {}", read, ROWS, measured, bound);
        assertTrue(read + " over a " + ROWS + "-row partition materialized " + measured +
                   " unfiltereds, bound " + bound, measured <= bound);
    }

    // ---------------------------------------------------------------- measurement

    /** Runs the full differential and returns the unfiltereds one cursor run materialized.  The
     *  harness runs the cursor path twice. */
    private long cursorMaterializedPerRun(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    {
        long before = CursorReads.unfilteredsMaterialized();
        assertCursorReadMatchesIterator(cfs, command);
        return (CursorReads.unfilteredsMaterialized() - before) / 2;
    }

    /** {@link #cursorMaterializedPerRun}, also checking that the cursor merge served both runs. */
    private long cursorMergeMaterializedPerRun(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    {
        long mergesBefore = CursorReads.cursorMergesServed();
        long memtableLegsBefore = CursorReads.memtableLegsCursorMerged();
        long perRun = cursorMaterializedPerRun(cfs, command);
        assertEquals("cursor merges served across the harness's two cursor runs",
                     2L, CursorReads.cursorMergesServed() - mergesBefore);
        assertTrue("memtable leg did not join the cursor merge",
                   CursorReads.memtableLegsCursorMerged() > memtableLegsBefore);
        return perRun;
    }

    /**
     * Pages through the partition on both paths side by side, one page at a time, so memory stays
     * at one page for any row count.  Each page's records and paging state must match.  Returns
     * the unfiltereds the cursor path materialized over all pages.
     */
    private long pagedCursorMaterialized(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    {
        DatabaseDescriptor.setCursorReadsEnabled(true);
        SinglePartitionReadCommand probe = command.get();
        assertTrue("paged read is not supported by the cursor read gate",
                   CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));

        SinglePartitionPager iteratorPager = (SinglePartitionPager) command.get().getPager(null, ProtocolVersion.CURRENT);
        SinglePartitionPager cursorPager = (SinglePartitionPager) command.get().getPager(null, ProtocolVersion.CURRENT);
        long materialized = 0;
        long rowsRead = 0;
        int page = 0;
        maxCursorPageAllocatedBytes = 0;
        try
        {
            while (!iteratorPager.isExhausted())
            {
                assertTrue("paged read did not terminate", page++ < ROWS / PAGE_SIZE + 10);

                DatabaseDescriptor.setCursorReadsEnabled(false);
                long servedBefore = CursorReads.sstableLegsServed();
                List<String> iteratorPage = fetchPage(cfs, iteratorPager);
                assertEquals("iterator page ran the cursor path", servedBefore, CursorReads.sstableLegsServed());

                DatabaseDescriptor.setCursorReadsEnabled(true);
                assertFalse("cursor paging ended before iterator paging", cursorPager.isExhausted());
                servedBefore = CursorReads.sstableLegsServed();
                long materializedBefore = CursorReads.unfilteredsMaterialized();
                long allocatedBefore = threadAllocatedBytes();
                List<String> cursorPage = fetchPage(cfs, cursorPager);
                maxCursorPageAllocatedBytes = Math.max(maxCursorPageAllocatedBytes, threadAllocatedBytes() - allocatedBefore);
                materialized += CursorReads.unfilteredsMaterialized() - materializedBefore;
                long servedDelta = CursorReads.sstableLegsServed() - servedBefore;

                compareRecords(iteratorPage, cursorPage);
                assertEquals("paging state after page " + page + " diverged between paths",
                             stateString(iteratorPager), stateString(cursorPager));
                long pageRows = iteratorPage.stream().filter(record -> record.startsWith("ROW ")).count();
                // the last page starts past the final row, so every sstable is skipped and no leg is served
                if (pageRows > 0)
                    assertTrue("cursor page " + page + " served no sstable leg (silent fallback?)", servedDelta > 0);
                rowsRead += pageRows;
            }
            assertTrue("cursor paging continued after iterator paging ended", cursorPager.isExhausted());
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
        assertEquals("paged read returned the wrong number of rows", ROWS, rowsRead);
        return materialized;
    }

    /** Heap allocated so far by the current thread. */
    private static long threadAllocatedBytes()
    {
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        return threads.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private static List<String> fetchPage(ColumnFamilyStore cfs, SinglePartitionPager pager)
    {
        try (ReadExecutionController controller = pager.executionController();
             UnfilteredPartitionIterator partitions = pager.fetchPageUnfiltered(cfs.metadata(), PAGE_SIZE, controller))
        {
            return canonicalRecords(partitions);
        }
    }

    private static String stateString(SinglePartitionPager pager)
    {
        PagingState state = pager.state();
        return state == null ? "null" : ByteBufferUtil.bytesToHex(state.serialize(ProtocolVersion.CURRENT));
    }
}
