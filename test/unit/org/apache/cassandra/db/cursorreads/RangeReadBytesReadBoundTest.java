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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Bounds the bytes a cursor range read takes from the data files, on BTI, against the iterator
 * range path and against the data the read covers.  The bytes are the JDK's file read events of
 * the reading thread on {@code Data.db} files, with the chunk cache emptied before each read.
 *
 * <p>A range read must read each byte of the data it covers about once: one pass over the sstable
 * for a scan, a few compressed chunks for a short read, the start of each partition for
 * {@code DISTINCT}, and no byte twice across the pages of a paged scan.  It must never read more
 * than the iterator path.
 *
 * <p>Sizes come from {@code -Dcassandra.test.cursor_range_partitions} (small partitions, default
 * {@value #DEFAULT_SMALL_PARTITIONS}) and {@code -Dcassandra.test.cursor_range_rows} (rows of the
 * wide partitions, default {@value #DEFAULT_WIDE_ROWS} in total).
 */
public class RangeReadBytesReadBoundTest extends RangeReadOracle
{
    private static final Logger logger = LoggerFactory.getLogger(RangeReadBytesReadBoundTest.class);

    private static final int DEFAULT_SMALL_PARTITIONS = 20_000;
    private static final int SMALL_PARTITIONS = Integer.getInteger("cassandra.test.cursor_range_partitions", DEFAULT_SMALL_PARTITIONS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int DEFAULT_WIDE_ROWS = 100_000;
    private static final int WIDE_ROWS = Integer.getInteger("cassandra.test.cursor_range_rows", DEFAULT_WIDE_ROWS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int ROWS_PER_SMALL_PARTITION = 5;
    private static final int WIDE_PARTITIONS = 10;
    private static final int CHUNK_KIB = 16;
    private static final int COLUMN_INDEX_SIZE_KIB = 4;
    private static final int LIMIT = 10;
    private static final int PAGE_SIZE = 5000;
    private static final int SHORT_READS = 20;

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

    // ---------------------------------------------------------------- small partitions

    @Test
    public void fullScanOfSmallPartitionsReadsTheDataFileOnce() throws Throwable
    {
        ColumnFamilyStore cfs = loadSmallPartitions(1);
        long now = FBUtilities.nowInSeconds();
        long onDisk = dataOnDisk(cfs);
        for (boolean replica : new boolean[]{ false, true })
        {
            Reads r = measure(cfs, () -> List.of(rangeCommand("SELECT * FROM %s", now)), replica);
            // one pass, plus the read-ahead buffer that runs past the end
            assertBound("full scan of one sstable", replica, r, onDisk + readAheadBytes());
        }
    }

    @Test
    public void fullScanOfOverlappingSSTablesReadsEachOnce() throws Throwable
    {
        ColumnFamilyStore cfs = loadSmallPartitions(8);
        long now = FBUtilities.nowInSeconds();
        long onDisk = dataOnDisk(cfs);
        for (boolean replica : new boolean[]{ false, true })
        {
            Reads r = measure(cfs, () -> List.of(rangeCommand("SELECT * FROM %s", now)), replica);
            assertBound("full scan of 8 overlapping sstables", replica, r, onDisk + 8L * readAheadBytes());
        }
    }

    @Test
    public void limitedReadsFromRandomTokensReadAFewChunks() throws Throwable
    {
        ColumnFamilyStore cfs = loadSmallPartitions(1);
        long now = FBUtilities.nowInSeconds();
        for (boolean replica : new boolean[]{ false, true })
        {
            Reads r = measure(cfs, () -> {
                Random random = new Random(42);
                List<PartitionRangeReadCommand> commands = new ArrayList<>();
                for (int i = 0; i < SHORT_READS; i++)
                    commands.add(rangeCommand("SELECT * FROM %s WHERE token(k) > ? LIMIT " + LIMIT, now, random.nextLong()));
                return commands;
            }, replica);
            // a read of 10 small rows starts in one chunk and may run into the next
            assertBound("LIMIT " + LIMIT + " from " + SHORT_READS + " random tokens", replica, r, SHORT_READS * 3L * chunkOnDisk(cfs));
        }
    }

    // ---------------------------------------------------------------- wide partitions

    @Test
    public void distinctReadsOnlyTheStartOfEachPartition() throws Throwable
    {
        ColumnFamilyStore cfs = loadWidePartitions();
        long now = FBUtilities.nowInSeconds();
        for (boolean replica : new boolean[]{ false, true })
        {
            Reads r = measure(cfs, () -> List.of(rangeCommand("SELECT DISTINCT k FROM %s", now)), replica);
            assertBound("DISTINCT over " + WIDE_PARTITIONS + " wide partitions", replica, r, WIDE_PARTITIONS * 3L * chunkOnDisk(cfs));
        }
    }

    @Test
    public void limitedReadOfAWidePartitionReadsAFewChunks() throws Throwable
    {
        ColumnFamilyStore cfs = loadWidePartitions();
        long now = FBUtilities.nowInSeconds();
        for (boolean replica : new boolean[]{ false, true })
        {
            Reads r = measure(cfs, () -> List.of(rangeCommand("SELECT * FROM %s LIMIT " + LIMIT, now)), replica);
            assertBound("LIMIT " + LIMIT + " over wide partitions", replica, r, 3L * chunkOnDisk(cfs));
        }
    }

    @Test
    public void pagedScanOfWidePartitionsReadsEachByteAboutOnce() throws Throwable
    {
        ColumnFamilyStore cfs = loadWidePartitions();
        long now = FBUtilities.nowInSeconds();
        long onDisk = dataOnDisk(cfs);
        long pages = WIDE_ROWS / PAGE_SIZE + 1;
        Reads r = measurePaged(cfs, () -> rangeCommand("SELECT * FROM %s", now));
        // each page may re-read the chunk it resumes in, and its read-ahead may run past its end
        assertBound("paged scan of wide partitions, page size " + PAGE_SIZE, false, r, onDisk + pages * (chunkOnDisk(cfs) + readAheadBytes()));
    }

    // ---------------------------------------------------------------- data

    /** Small partitions of five rows, over {@code sstables} sstables that each hold every partition. */
    private ColumnFamilyStore loadSmallPartitions(int sstables) throws Throwable
    {
        ColumnFamilyStore cfs = createLz4Table();
        for (int s = 0; s < sstables; s++)
        {
            for (int k = 0; k < SMALL_PARTITIONS; k++)
            {
                for (int c = 0; c < ROWS_PER_SMALL_PARTITION; c++)
                {
                    if ((k + c) % sstables == s)
                        execute("INSERT INTO %s (k, c, v, p) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", k, c, (long) c, payload(k, c), 1000L + s);
                }
            }
            flush();
        }
        assertEquals(sstables, cfs.getLiveSSTables().size());
        return cfs;
    }

    /** {@link #WIDE_PARTITIONS} partitions sharing {@link #WIDE_ROWS} rows, in one sstable. */
    private ColumnFamilyStore loadWidePartitions() throws Throwable
    {
        ColumnFamilyStore cfs = createLz4Table();
        int perPartition = WIDE_ROWS / WIDE_PARTITIONS;
        for (int k = 0; k < WIDE_PARTITIONS; k++)
            for (int c = 0; c < perPartition; c++)
                execute("INSERT INTO %s (k, c, v, p) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", k, c, (long) c, payload(k, c));
        flush();
        assertEquals(1, cfs.getLiveSSTables().size());
        return cfs;
    }

    private ColumnFamilyStore createLz4Table()
    {
        createTable("CREATE TABLE %s (k int, c int, v bigint, p text, PRIMARY KEY (k, c)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': " + CHUNK_KIB + "} " +
                    "AND caching = {'keys': 'NONE', 'rows_per_partition': 'NONE'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    /** About 60 bytes: half varies by row, half repeats, so LZ4 compresses it some. */
    private static String payload(int k, int c)
    {
        return Long.toHexString(k * 1_000_003L + c * 7919L) + "-abcdefghijklmnopqrstuvwxyz-" + (k ^ c);
    }

    private static long dataOnDisk(ColumnFamilyStore cfs)
    {
        long total = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            total += sstable.onDiskLength();
        return total;
    }

    /** The on-disk size of one compressed chunk, at most: the chunk length plus its checksum. */
    private static long chunkOnDisk(ColumnFamilyStore cfs)
    {
        return CHUNK_KIB * 1024L + 4;
    }

    private static long readAheadBytes()
    {
        return DatabaseDescriptor.getCompressedReadAheadBufferSize();
    }

    // ---------------------------------------------------------------- measurement

    private static final class Reads
    {
        long cursor;
        long iterator;
    }

    @FunctionalInterface
    private interface Commands
    {
        List<PartitionRangeReadCommand> get();
    }

    @FunctionalInterface
    private interface CommandSupplier
    {
        PartitionRangeReadCommand get();
    }

    private static void assertBound(String read, boolean replica, Reads r, long bound)
    {
        String surface = replica ? "replica response" : "executeLocally";
        logger.info("{} via {}: cursor read {} bytes, iterator {} bytes, bound {}", read, surface, r.cursor, r.iterator, bound);
        assertTrue(read + " via " + surface + ": the cursor path read " + r.cursor + " bytes from data files, bound " + bound
                   + " (iterator " + r.iterator + ')', r.cursor <= bound);
        assertTrue(read + " via " + surface + ": the cursor path read " + r.cursor + " bytes, the iterator path " + r.iterator,
                   r.cursor <= r.iterator);
    }

    /** The bytes the reads take on each path, and a check that the cursor path served them. */
    private Reads measure(ColumnFamilyStore cfs, Commands commands, boolean replica) throws IOException
    {
        Reads r = new Reads();
        r.iterator = bytesRead(() -> run(commands.get(), replica), false);
        long served = CursorRangeReads.rangeReadsServed();
        List<PartitionRangeReadCommand> reads = commands.get();
        r.cursor = bytesRead(() -> run(reads, replica), true);
        assertEquals("the cursor path served every range read", reads.size(), CursorRangeReads.rangeReadsServed() - served);
        // and the reads match on every surface, checked once per command list
        for (PartitionRangeReadCommand read : replica ? commands.get() : List.<PartitionRangeReadCommand>of())
            assertAllReadSurfacesMatch(ReadCase.of(read.toCQLString(), ++oracleCases).at(read.nowInSec()), cfs, now -> read);
        return r;
    }

    /** The oracle cases run, which seed them. */
    private long oracleCases;

    private Reads measurePaged(ColumnFamilyStore cfs, CommandSupplier command) throws IOException
    {
        Reads r = new Reads();
        r.iterator = bytesRead(() -> pageThrough(cfs, command.get()), false);
        long served = CursorRangeReads.rangeReadsServed();
        r.cursor = bytesRead(() -> pageThrough(cfs, command.get()), true);
        assertTrue("the cursor path served the pages", CursorRangeReads.rangeReadsServed() - served >= WIDE_ROWS / PAGE_SIZE);
        PartitionRangeReadCommand paged = command.get();
        assertPagedMatches(ReadCase.of("paged scan", ++oracleCases).at(paged.nowInSec()), cfs, now -> paged, PAGE_SIZE);
        return r;
    }

    private static void run(List<PartitionRangeReadCommand> commands, boolean replica)
    {
        for (PartitionRangeReadCommand command : commands)
        {
            if (replica)
            {
                ReadCommandVerbHandler.instance.doRead(command, false);
                continue;
            }
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller))
            {
                canonicalRecords(partitions);
            }
        }
    }

    private static void pageThrough(ColumnFamilyStore cfs, PartitionRangeReadCommand command)
    {
        PartitionRangeQueryPager pager = (PartitionRangeQueryPager) command.getPager(null, ProtocolVersion.V5);
        int pages = 0;
        while (!pager.isExhausted())
        {
            assertTrue("paging did not terminate", pages++ < WIDE_ROWS / PAGE_SIZE + 10);
            try (ReadExecutionController controller = pager.executionController();
                 UnfilteredPartitionIterator partitions = pager.fetchPageUnfiltered(cfs.metadata(), PAGE_SIZE, controller))
            {
                canonicalRecords(partitions);
            }
        }
    }

    /** The bytes {@code read} takes from data files on the reading thread, on one path. */
    private static long bytesRead(Runnable read, boolean cursor) throws IOException
    {
        setRangePath(cursor);
        Path dump = Files.createTempFile("range-read-bytes", ".jfr");
        try (Recording recording = new Recording())
        {
            recording.enable("jdk.FileRead").withThreshold(Duration.ZERO);
            if (ChunkCache.instance != null)
                ChunkCache.instance.close();
            recording.start();
            read.run();
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
            setRangePath(false);
            Files.deleteIfExists(dump);
        }
    }
}
