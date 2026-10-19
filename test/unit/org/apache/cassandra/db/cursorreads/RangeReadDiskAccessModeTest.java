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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * A cursor range read reads with {@code range_read_disk_access_mode}: direct I/O from its first
 * read by default; under {@code auto} the data file's own mode, moving to the read-ahead reader on a
 * long read.  Single-partition cursor reads keep the data file's own mode.  The chunk reader under
 * the cursor's data reader shows which kind it is.
 */
public class RangeReadDiskAccessModeTest extends CQLTester
{
    private static final int PARTITIONS = 2048;
    private static final int PAYLOAD_BYTES = 1024;
    private static final int CHUNK_KIB = 16;
    private static final String DIRECT_READER = "CompressedChunkReader.Direct(";
    private static final String CHUNK_READER = "CompressedChunkReader.";

    private DiskAccessMode rangeReadModeWas;
    private boolean cursorRangeReadsWas;
    private boolean cursorReadsWas;

    @Before
    public void save()
    {
        rangeReadModeWas = DatabaseDescriptor.getRangeReadDiskAccessMode();
        cursorRangeReadsWas = DatabaseDescriptor.cursorRangeReadsEnabled();
        cursorReadsWas = DatabaseDescriptor.cursorReadsEnabled();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setRangeReadDiskAccessMode(rangeReadModeWas);
        DatabaseDescriptor.setCursorRangeReadsEnabled(cursorRangeReadsWas);
        DatabaseDescriptor.setCursorReadsEnabled(cursorReadsWas);
    }

    @Test
    public void defaultIsDirect()
    {
        assertEquals(DiskAccessMode.direct, DatabaseDescriptor.getRangeReadDiskAccessMode());
    }

    @Test
    public void rangeReadIsDirectFromItsFirstRead() throws Throwable
    {
        SSTableReader sstable = writeSSTable();
        assumeTrue("direct I/O is supported for the data directory", FileUtils.isDirectIOSupported(sstable.descriptor.directory));
        DatabaseDescriptor.setRangeReadDiskAccessMode(DiskAccessMode.direct);

        try (SSTableCursorReader cursor = SSTableCursorReader.forRangeRead(sstable))
        {
            RandomAccessReader first = cursor.dataReader();
            assertTrue("the first reader of a range read is " + first, first.toString().contains(DIRECT_READER));
            cursor.seekToPartition(0);
            int partitions = 0;
            while (cursor.skipPartition() != DONE)
                partitions++;
            assertTrue("the cursor read " + partitions + " partitions", partitions >= PARTITIONS - 1);
            assertSame("a direct range read keeps its first reader", first, cursor.dataReader());
        }
    }

    /** A LIMIT 10 range read reads a chunk or two of the data file with direct I/O, not a read-ahead buffer. */
    @Test
    public void limitedRangeReadReadsAFewChunks() throws Throwable
    {
        SSTableReader sstable = writeSSTable();
        assumeTrue("direct I/O is supported for the data directory", FileUtils.isDirectIOSupported(sstable.descriptor.directory));
        DatabaseDescriptor.setRangeReadDiskAccessMode(DiskAccessMode.direct);
        DatabaseDescriptor.setCursorRangeReadsEnabled(true);
        DatabaseDescriptor.setCursorReadsEnabled(true);

        long served = CursorRangeReads.rangeReadsServed();
        long bytes = dataFileBytesRead(() -> assertEquals(10, execute("SELECT pk, v FROM %s LIMIT 10").size()));

        assertEquals("the cursor path served the read", 1, CursorRangeReads.rangeReadsServed() - served);
        long bound = 3L * CHUNK_KIB * 1024;
        assertTrue("a LIMIT 10 range read read " + bytes + " bytes of the data file, bound " + bound
                   + " (the read-ahead buffer is " + DatabaseDescriptor.getCompressedReadAheadBufferSize() + ')',
                   bytes > 0 && bytes <= bound);
    }

    /** The bytes the read takes from Data.db files on this thread, from the JFR file read events. */
    private static long dataFileBytesRead(ThrowingRunnable read) throws Throwable
    {
        Path dump = Files.createTempFile("range-read-direct", ".jfr");
        try (Recording recording = new Recording())
        {
            recording.enable("jdk.FileRead").withThreshold(Duration.ZERO);
            if (ChunkCache.instance != null)
                ChunkCache.instance.clear();
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
            Files.deleteIfExists(dump);
        }
    }

    private interface ThrowingRunnable
    {
        void run() throws Throwable;
    }

    @Test
    public void longRangeReadUnderAutoKeepsTheDataFileMode() throws Throwable
    {
        SSTableReader sstable = writeSSTable();
        DatabaseDescriptor.setRangeReadDiskAccessMode(DatabaseDescriptor.getDiskAccessMode());

        try (SSTableCursorReader cursor = SSTableCursorReader.forRangeRead(sstable))
        {
            assertFalse("auto never reads direct", cursor.dataReader().toString().contains(DIRECT_READER));
        }
        String reader = readAheadReaderOf(sstable, SSTableCursorReader::forRangeRead);

        assertTrue("the read-ahead reader of a range read under auto is " + reader, reader.contains(CHUNK_READER));
        assertFalse("the read-ahead reader of a range read under auto is " + reader, reader.contains(DIRECT_READER));
    }

    @Test
    public void singlePartitionReadKeepsTheDataFileMode() throws Throwable
    {
        SSTableReader sstable = writeSSTable();
        DatabaseDescriptor.setRangeReadDiskAccessMode(DiskAccessMode.direct);

        String reader = readAheadReaderOf(sstable, s -> SSTableCursorReader.forRead(s, Collections.singletonList(new PartitionPositionBounds(0, s.uncompressedLength()))));

        assertTrue("the read-ahead reader of a single-partition read is " + reader, reader.contains(CHUNK_READER));
        assertFalse("the read-ahead reader of a single-partition read is " + reader, reader.contains(DIRECT_READER));
    }

    /** A CQL range read returns the same rows under direct, under auto and on the iterator path. */
    @Test
    public void rangeReadRowsMatchUnderEveryMode() throws Throwable
    {
        writeSSTable();
        List<List<ByteBuffer>> iterator = rangeRead(false, DiskAccessMode.direct);
        assertEquals(PARTITIONS, iterator.size());
        long served = CursorRangeReads.rangeReadsServed();
        assertEquals(iterator, rangeRead(true, DiskAccessMode.direct));
        assertEquals(iterator, rangeRead(true, DatabaseDescriptor.getDiskAccessMode()));
        assertTrue("the cursor path served the range reads", CursorRangeReads.rangeReadsServed() - served >= 2);
    }

    private List<List<ByteBuffer>> rangeRead(boolean cursor, DiskAccessMode mode) throws Throwable
    {
        DatabaseDescriptor.setCursorRangeReadsEnabled(cursor);
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
        DatabaseDescriptor.setRangeReadDiskAccessMode(mode);
        if (ChunkCache.instance != null)
            ChunkCache.instance.clear();
        List<List<ByteBuffer>> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT pk, v FROM %s"))
            rows.add(List.of(row.getBytes("pk"), row.getBytes("v")));
        return rows;
    }

    /**
     * Reads every partition of the sstable through the cursor, with the chunk cache cold so the
     * read moves to the read-ahead reader, and returns that reader's description.
     */
    private static String readAheadReaderOf(SSTableReader sstable, Function<SSTableReader, SSTableCursorReader> open)
    {
        if (ChunkCache.instance != null)
            ChunkCache.instance.clear();
        try (SSTableCursorReader cursor = open.apply(sstable))
        {
            RandomAccessReader first = cursor.dataReader();
            if (cursor.state() != SSTableCursorReader.State.PARTITION_START)
                cursor.seekToPartition(0);
            int partitions = 0;
            while (cursor.skipPartition() != DONE)
                partitions++;
            assertTrue("the cursor read " + partitions + " partitions", partitions >= PARTITIONS - 1);
            assertNotSame("the read moved to the read-ahead reader", first, cursor.dataReader());
            return cursor.dataReader().toString();
        }
    }

    /** One compressed sstable of {@link #PARTITIONS} partitions, about 2 MiB of data. */
    private SSTableReader writeSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v blob) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '" + CHUNK_KIB + "'}");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        Random random = new Random(42);
        byte[] payload = new byte[PAYLOAD_BYTES];
        for (int pk = 0; pk < PARTITIONS; pk++)
        {
            random.nextBytes(payload);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, ByteBuffer.wrap(payload.clone()));
        }
        flush();
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        return getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
    }
}
