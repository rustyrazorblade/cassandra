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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.DirectCompressedSequentialWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Each reader and writer that takes a {@link ThroughputLimiter} charges it with exactly the bytes it moves to or
 * from disk.
 */
public class ThrottledIoTest
{
    private static final int LENGTH = 3 << 20;
    private static final int CHUNK = 16 << 10;
    /** The largest single I/O of a buffered reader or writer: a 64 KiB buffer, or a compressed chunk and its CRC. */
    private static final int LARGEST_IO = DiskOptimizationStrategy.MAX_BUFFER_SIZE + Integer.BYTES;

    /** Records the size of each charge, so a test can tell a charge per I/O from a charge per partition. */
    private static class RecordingLimiter extends ThroughputLimiter
    {
        int charges;
        long largest;
        final List<Long> priorityCharges = new ArrayList<>();

        RecordingLimiter()
        {
            // A limit, so the mmap reader spreads its charges, but one too high to ever wait.
            super(1e15);
        }

        @Override
        public void acquire(long bytes)
        {
            charges++;
            largest = Math.max(largest, bytes);
            super.acquire(bytes);
        }

        @Override
        public void acquirePriority(long bytes)
        {
            priorityCharges.add(bytes);
            super.acquirePriority(bytes);
        }

        void assertChargedEachIO(long expectedBytes, long largestIO, Object description)
        {
            assertThat(bytesCharged()).as("%s", description).isEqualTo(expectedBytes);
            assertThat(largest).as("%s", description).isLessThanOrEqualTo(largestIO);
            assertThat((long) charges).as("%s", description).isGreaterThanOrEqualTo(expectedBytes / largestIO);
        }
    }

    private File file;
    private File offsets;
    private int readAheadBefore;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws IOException
    {
        file = FileUtils.createTempFile("throttled", "data");
        offsets = new File(file.absolutePath() + ".offsets");
        readAheadBefore = DatabaseDescriptor.getCompressedReadAheadBufferSize() / 1024;
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(readAheadBefore);
        file.tryDelete();
        offsets.tryDelete();
    }

    @Test
    public void uncompressedWritesAndReads() throws IOException
    {
        RecordingLimiter writes = new RecordingLimiter();
        try (SequentialWriter writer = new SequentialWriter(file, SequentialWriterOption.DEFAULT.withLimiter(writes)))
        {
            writer.write(data());
            writer.finish();
        }
        writes.assertChargedEachIO(file.length(), LARGEST_IO, "uncompressed writes");

        for (DiskAccessMode mode : new DiskAccessMode[]{ DiskAccessMode.standard, DiskAccessMode.mmap })
        {
            try (FileHandle handle = new FileHandle.Builder(file).withDiskAccessMode(mode).complete())
            {
                assertReadsCharge(handle, mode, file.length());
            }
        }
    }

    @Test
    public void compressedWritesAndReads() throws IOException
    {
        RecordingLimiter writes = new RecordingLimiter();
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(file, offsets, null,
                                                                                SequentialWriterOption.DEFAULT.withLimiter(writes),
                                                                                CompressionParams.lz4(CHUNK), collector()))
        {
            writer.write(data());
            writer.finish();
        }
        writes.assertChargedEachIO(file.length(), LARGEST_IO, "compressed writes");
        assertCompressedReadsCharge();
    }

    @Test
    public void directCompressedWritesChargeWholeBlocks() throws IOException
    {
        Assume.assumeTrue(FileUtils.isDirectIOSupported(file));
        RecordingLimiter writes = new RecordingLimiter();
        try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(file, offsets, null,
                                                                                            SequentialWriterOption.DEFAULT.withLimiter(writes),
                                                                                            CompressionParams.lz4(CHUNK), collector()))
        {
            writer.write(data());
            writer.finish();
        }
        int block = FileUtils.getFileBlockSize(file);
        assertThat(writes.bytesCharged()).isBetween(file.length(), file.length() + block);
        assertThat(writes.charges).isGreaterThan(1);
        assertCompressedReadsCharge();
    }

    /** A writer that counts its data syncs, and the priority charges made before each one. */
    private static class SyncCountingWriter extends SequentialWriter
    {
        final RecordingLimiter limiter;
        final List<Integer> chargesAtSync = new ArrayList<>();

        SyncCountingWriter(File file, RecordingLimiter limiter)
        {
            super(file, SequentialWriterOption.DEFAULT.withFlushLimiter(limiter));
            this.limiter = limiter;
        }

        @Override
        protected void syncDataOnlyInternal()
        {
            chargesAtSync.add(limiter.priorityCharges.size());
            super.syncDataOnlyInternal();
        }
    }

    /** A paced writer syncs once per PACED_SYNC_BYTES, charges after each sync, and charges the rest at the end. */
    @Test
    public void pacedWriterSyncsThenChargesEachMiB() throws IOException
    {
        RecordingLimiter limiter = new RecordingLimiter();
        int remainder = 100 << 10;
        SyncCountingWriter writer = new SyncCountingWriter(file, limiter);
        try (writer)
        {
            writer.write(data());
            writer.write(new byte[remainder]);
            writer.finish();
        }
        assertThat(file.length()).isEqualTo(LENGTH + remainder);
        assertThat(limiter.charges).as("ordinary charges").isZero();
        assertThat(limiter.priorityCharges.stream().mapToLong(Long::longValue).sum()).isEqualTo(file.length());
        int full = LENGTH / SequentialWriter.PACED_SYNC_BYTES;
        assertThat(limiter.priorityCharges).hasSize(full + 1);
        for (int i = 0; i < full; i++)
            assertThat(limiter.priorityCharges.get(i)).isEqualTo(SequentialWriter.PACED_SYNC_BYTES);
        assertThat(limiter.priorityCharges.get(full)).isEqualTo(remainder);
        // Each charge comes after its own sync: the sync before charge n sees n earlier charges.
        for (int n = 0; n <= full; n++)
            assertThat(writer.chargesAtSync).contains(n);
        assertThat(writer.chargesAtSync).isSorted();
    }

    @Test
    public void pacedCompressedWriterChargesEachMiB() throws IOException
    {
        RecordingLimiter limiter = new RecordingLimiter();
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(file, offsets, null,
                                                                                SequentialWriterOption.DEFAULT.withFlushLimiter(limiter),
                                                                                CompressionParams.lz4(CHUNK), collector()))
        {
            writer.write(data());
            writer.finish();
        }
        assertThat(limiter.charges).as("ordinary charges").isZero();
        assertThat(limiter.priorityCharges.stream().mapToLong(Long::longValue).sum()).isEqualTo(file.length());
        assertThat(limiter.priorityCharges).hasSizeGreaterThan(1);
        // Every charge but the last waits for a whole PACED_SYNC_BYTES, and overshoots it by less than one chunk.
        List<Long> full = limiter.priorityCharges.subList(0, limiter.priorityCharges.size() - 1);
        for (long charge : full)
            assertThat(charge).isBetween((long) SequentialWriter.PACED_SYNC_BYTES, (long) SequentialWriter.PACED_SYNC_BYTES + LARGEST_IO);
    }

    @Test
    public void optionsCarryTheLimiter()
    {
        ThroughputLimiter limiter = new RecordingLimiter();
        assertThat(SequentialWriterOption.DEFAULT.limiter()).isNull();
        SequentialWriterOption limited = SequentialWriterOption.DEFAULT.withLimiter(limiter);
        assertThat(limited.limiter()).isSameAs(limiter);
        assertThat(limited.bufferSize()).isEqualTo(SequentialWriterOption.DEFAULT.bufferSize());
        assertThat(limited.bufferType()).isEqualTo(SequentialWriterOption.DEFAULT.bufferType());
        assertThat(limited.trickleFsync()).isEqualTo(SequentialWriterOption.DEFAULT.trickleFsync());
        assertThat(limited.trickleFsyncByteInterval()).isEqualTo(SequentialWriterOption.DEFAULT.trickleFsyncByteInterval());
        assertThat(limited.finishOnClose()).isEqualTo(SequentialWriterOption.DEFAULT.finishOnClose());
        assertThat(SequentialWriterOption.DEFAULT.limiter()).isNull();
        assertThat(SequentialWriterOption.DEFAULT.paced()).isFalse();
        assertThat(limited.paced()).isFalse();

        SequentialWriterOption paced = SequentialWriterOption.DEFAULT.withFlushLimiter(limiter);
        assertThat(paced.limiter()).isSameAs(limiter);
        assertThat(paced.paced()).isTrue();
        assertThat(paced.bufferSize()).isEqualTo(SequentialWriterOption.DEFAULT.bufferSize());
        assertThat(paced.withLimiter(limiter).paced()).isFalse();
    }

    private void assertCompressedReadsCharge()
    {
        for (int readAheadKiB : new int[]{ 0, 256 })
        {
            DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(readAheadKiB);
            for (DiskAccessMode mode : new DiskAccessMode[]{ DiskAccessMode.standard, DiskAccessMode.mmap, DiskAccessMode.direct })
            {
                if (mode == DiskAccessMode.direct && !FileUtils.isDirectIOSupported(file))
                    continue;
                try (CompressionMetadata metadata = CompressionMetadata.open(offsets, file.length(), true);
                     FileHandle handle = new FileHandle.Builder(file).withCompressionMetadata(metadata).withDiskAccessMode(mode).complete())
                {
                    assertReadsCharge(handle, mode + ", read-ahead " + readAheadKiB + " KiB", file.length());
                }
            }
        }
    }

    private static void assertReadsCharge(FileHandle handle, Object description, long expected)
    {
        for (boolean scan : new boolean[]{ true, false })
        {
            RecordingLimiter reads = new RecordingLimiter();
            try (RandomAccessReader reader = handle.createReader(reads, scan))
            {
                reader.readFully(new byte[LENGTH]);
                assertThat(reader.isEOF()).isTrue();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            reads.assertChargedEachIO(expected, LARGEST_IO, description + ", scan " + scan);
        }
    }

    /** Half random, half constant, so it compresses to about half. */
    private static byte[] data()
    {
        byte[] data = new byte[LENGTH];
        Random random = new Random(1);
        for (int i = 0; i < LENGTH; i += 2 * CHUNK)
            for (int j = i; j < Math.min(LENGTH, i + CHUNK); j++)
                data[j] = (byte) random.nextInt();
        return data;
    }

    private static MetadataCollector collector()
    {
        return new MetadataCollector(new ClusteringComparator(BytesType.instance));
    }
}
