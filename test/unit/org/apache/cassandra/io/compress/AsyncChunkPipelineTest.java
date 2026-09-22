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
package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;

import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The async writer swaps a fresh slot in on every flush instead of clearing one in place. Nothing
 * else about the write path changes, so the bytes it produces must be identical to the synchronous
 * writer's for the same input. These tests write one stream through both and compare the data file
 * and the compression metadata byte for byte, which is the only assertion that would catch a slot
 * being reused too early, released twice, or handed on with the wrong position.
 */
public class AsyncChunkPipelineTest
{
    private static final int SLOTS = 4;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void lz4MatchesSynchronousWriter() throws IOException
    {
        compareAcrossSizes(CompressionParams.lz4(), "lz4");
    }

    @Test
    public void deflateMatchesSynchronousWriter() throws IOException
    {
        // Deflate prefers on-heap buffers, so this covers the ON_HEAP slot pool.
        compareAcrossSizes(CompressionParams.deflate(), "deflate");
    }

    @Test
    public void snappyMatchesSynchronousWriter() throws IOException
    {
        compareAcrossSizes(CompressionParams.snappy(), "snappy");
    }

    @Test
    public void zstdMatchesSynchronousWriter() throws IOException
    {
        compareAcrossSizes(CompressionParams.zstd(), "zstd");
    }

    @Test
    public void noopMatchesSynchronousWriter() throws IOException
    {
        compareAcrossSizes(CompressionParams.noop(), "noop");
    }

    /**
     * Incompressible input drives the branch in flushData that writes the staging buffer itself
     * rather than the compressed buffer, padding it to maxCompressedLength. That branch mutates the
     * slot after compression, so it is the one most exposed to an early swap.
     */
    @Test
    public void incompressibleInputMatchesSynchronousWriter() throws IOException
    {
        compare(CompressionParams.lz4(), "incompressible", DEFAULT_CHUNK_LENGTH * 5 + 37, true);
    }

    /**
     * With trickle_fsync on, the writer thread starts kernel writeback of each interval's worth of
     * on-disk bytes. Those hints must tile the file: begin at zero, stay contiguous and monotonic,
     * and each cover at least the interval. A gap, an overlap, or a hint against the uncompressed
     * offset would mean the range accounting in maybeHintWriteback drifted from the bytes on disk.
     *
     * sync_file_range is a no-op off Linux, so this asserts the offsets the pipeline computes rather
     * than the syscall, and it runs the same on every platform.
     */
    @Test
    public void writebackHintsTileTheFileContiguously() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        long interval = 2L * params.chunkLength();
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .trickleFsync(true)
                                                              .trickleFsyncByteInterval(interval)
                                                              .build();

        List<long[]> hints = new ArrayList<>();
        File data = FileUtils.createTempFile("writeback", ".db");
        File meta = new File(data.absolutePath() + ".metadata");

        // Incompressible, so the on-disk size tracks the payload and several intervals are crossed.
        byte[] payload = payload(params.chunkLength() * 20, true);

        CompressedSequentialWriter writer =
            new CompressedSequentialWriter(data, meta, null, option, params, collector(), null, asyncBytes(params))
            {
                @Override
                void hintWriteback(long offset, long nbytes)
                {
                    hints.add(new long[]{ offset, nbytes });
                }
            };
        write(writer, payload);

        assertFalse("expected at least one writeback hint", hints.isEmpty());

        long expectedOffset = 0;
        for (long[] hint : hints)
        {
            assertEquals("hints must be contiguous with no gap or overlap", expectedOffset, hint[0]);
            assertTrue("each hint must cover at least the interval", hint[1] >= interval);
            expectedOffset += hint[1];
        }
        assertTrue("hints must not run past the data on disk", expectedOffset <= readAll(data).length);
    }

    /**
     * The hint has to stay off when the operator has not asked for it. Two ways turn it off: the
     * shipped default leaves trickle_fsync false, and a zero byte interval collapses writebackInterval
     * to 0. Both must issue no hints even across a payload that crosses several intervals; a regression
     * that hinted regardless of config would slip past every byte-identical test.
     */
    @Test
    public void noHintsWhenHintingIsDisabled() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        byte[] payload = payload(params.chunkLength() * 20, true);

        assertNoHints(params, SequentialWriterOption.newBuilder().trickleFsync(false).build(), payload);
        assertNoHints(params, SequentialWriterOption.newBuilder()
                                                    .trickleFsync(true)
                                                    .trickleFsyncByteInterval(0)
                                                    .build(), payload);
    }

    private void assertNoHints(CompressionParams params, SequentialWriterOption option, byte[] payload)
    throws IOException
    {
        List<long[]> hints = new ArrayList<>();
        File data = FileUtils.createTempFile("nohint", ".db");
        File meta = new File(data.absolutePath() + ".metadata");

        CompressedSequentialWriter writer =
            new CompressedSequentialWriter(data, meta, null, option, params, collector(), null, asyncBytes(params))
            {
                @Override
                void hintWriteback(long offset, long nbytes)
                {
                    hints.add(new long[]{ offset, nbytes });
                }
            };
        write(writer, payload);

        assertTrue("no writeback hints when hinting is disabled", hints.isEmpty());
    }

    /**
     * O_DIRECT bypasses the page cache, so DirectCompressedSequentialWriter overrides hintWriteback to
     * a no-op. The pipeline still asks for a hint every interval, so the suppression lives only in that
     * override. Removing it would fall back to the parent, which resolves the fd and calls
     * sync_file_range once per interval for nothing. No byte-identical or offset test catches that,
     * and off Linux the syscall is a no-op anyway, so guard the override's presence directly.
     */
    @Test
    public void directWriterSuppressesWritebackHint() throws NoSuchMethodException
    {
        assertEquals("O_DIRECT must override hintWriteback to a no-op",
                     DirectCompressedSequentialWriter.class,
                     DirectCompressedSequentialWriter.class
                         .getDeclaredMethod("hintWriteback", long.class, long.class)
                         .getDeclaringClass());
    }

    /**
     * The post-flush listener drives early open: a reader binds to whatever offset it reports. So the
     * reported offset must never run ahead of the bytes actually on disk, or the reader short-reads
     * live compaction output. This writes many chunks through the async writer with a small pool and,
     * at every callback, checks the reported offset is monotonic and never exceeds the current on-disk
     * data-file length. The payload is incompressible so the compressed file never falls below the
     * uncompressed offset the listener reports, which keeps the length comparison meaningful.
     */
    @Test
    public void postFlushOffsetNeverExceedsDurableBytes() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        byte[] payload = payload(params.chunkLength() * (SLOTS * 5) + 123, true);

        File data = FileUtils.createTempFile("durableInvariant", ".db");
        File meta = new File(data.absolutePath() + ".metadata");

        CompressedSequentialWriter writer = newWriter(params, data, meta, asyncBytes(params));

        long[] lastReported = { -1L };
        List<String> violations = new ArrayList<>();
        writer.setPostFlushListener(offset -> {
            if (offset < lastReported[0])
                violations.add("offset went backwards: " + offset + " < " + lastReported[0]);
            lastReported[0] = offset;

            long onDisk = data.length();
            if (offset > onDisk)
                violations.add("reported offset " + offset + " exceeds on-disk bytes " + onDisk);
        });

        // write() finishes and closes the writer, so the pipeline is fully drained on return.
        write(writer, payload);

        assertTrue("post-flush durable-offset invariant violated: " + violations, violations.isEmpty());
        assertEquals("final durable offset must equal the uncompressed size after finish",
                     payload.length, writer.getLastFlushOffset());
    }

    /**
     * The writer thread is started on the first flush, so there is a window where the pipeline is
     * marked started but the thread handle is not assigned yet. The Accord simulator drove an abort
     * through that window: doPreCleanup ran quiesce() then stillRunning() on the half-started
     * pipeline. Before the fix stillRunning did started && writer.isAlive() and threw a
     * NullPointerException on the null handle, which broke every memtable flush that aborted. Both
     * calls must treat a null handle as "no thread is running".
     *
     * The window is a scheduling accident that a byte-identical test never reaches, so pin the exact
     * state directly: mark the pipeline started with no thread, then run the two cleanup calls.
     */
    @Test
    public void cleanupToleratesStartedWithoutWriterThread() throws Exception
    {
        CompressionParams params = CompressionParams.lz4();
        File data = FileUtils.createTempFile("halfStarted", ".db");
        File meta = new File(data.absolutePath() + ".metadata");

        CompressedSequentialWriter writer = newWriter(params, data, meta, asyncBytes(params));
        try
        {
            AsyncChunkPipeline pipeline = pipelineOf(writer);
            setStarted(pipeline, true);

            // The abort path calls both of these; neither may dereference the null handle.
            pipeline.quiesce();
            assertFalse("a pipeline with no writer thread must not report itself running",
                        pipeline.stillRunning());
        }
        finally
        {
            writer.abort(null);
        }
    }

    private static AsyncChunkPipeline pipelineOf(CompressedSequentialWriter writer) throws Exception
    {
        Field f = CompressedSequentialWriter.class.getDeclaredField("pipeline");
        f.setAccessible(true);
        return (AsyncChunkPipeline) f.get(writer);
    }

    private static void setStarted(AsyncChunkPipeline pipeline, boolean value) throws Exception
    {
        Field f = AsyncChunkPipeline.class.getDeclaredField("started");
        f.setAccessible(true);
        f.setBoolean(pipeline, value);
    }

    private void compareAcrossSizes(CompressionParams params, String name) throws IOException
    {
        compare(params, name + "_tiny", 25, false);
        compare(params, name + "_aligned", DEFAULT_CHUNK_LENGTH, false);
        compare(params, name + "_manyChunks", DEFAULT_CHUNK_LENGTH * (SLOTS * 3) + 101, false);
    }

    /**
     * The same comparison against the O_DIRECT writer. Compression and CRC cost the same however the
     * bytes reach the disk, so that path takes the pipeline too, and it has to produce the same file.
     */
    @Test
    public void directIoMatchesSynchronousWriter() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        compareDirect(params, "direct_tiny", 25);
        compareDirect(params, "direct_aligned", DEFAULT_CHUNK_LENGTH);
        compareDirect(params, "direct_manyChunks", DEFAULT_CHUNK_LENGTH * (SLOTS * 3) + 101);
    }

    /**
     * mark() and resetAndTruncate() drain the pipeline, rewind the writer's offsets and republish
     * them. After the same rewind the async writer must land byte-for-byte where the synchronous
     * writer does, which is what shows the drain leaves no in-flight chunk or stale offset behind.
     */
    @Test
    public void markAndResetMatchesSynchronousWriter() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        compareWithReset(params, "reset_withinChunk", DEFAULT_CHUNK_LENGTH / 2);
        compareWithReset(params, "reset_manyChunks", DEFAULT_CHUNK_LENGTH * (SLOTS * 3) + 101);
    }

    private void compareWithReset(CompressionParams params, String name, int headBytes) throws IOException
    {
        byte[] head = payload(headBytes, false);
        // Two chunks written past the mark, then discarded by the reset.
        byte[] discarded = payload(DEFAULT_CHUNK_LENGTH * 2, false);
        byte[] tail = payload(headBytes, true);

        File syncData = FileUtils.createTempFile(name + "_sync", ".db");
        File asyncData = FileUtils.createTempFile(name + "_async", ".db");
        File syncMeta = new File(syncData.absolutePath() + ".metadata");
        File asyncMeta = new File(asyncData.absolutePath() + ".metadata");

        long syncPosition = writeWithReset(newWriter(params, syncData, syncMeta, 0), head, discarded, tail);
        long asyncPosition = writeWithReset(newWriter(params, asyncData, asyncMeta, asyncBytes(params)), head, discarded, tail);

        assertEquals(name + ": reported position differs", syncPosition, asyncPosition);
        assertArrayEquals(name + ": data file differs", readAll(syncData), readAll(asyncData));
        assertArrayEquals(name + ": compression metadata differs", readAll(syncMeta), readAll(asyncMeta));
    }

    private static long writeWithReset(CompressedSequentialWriter writer, byte[] head, byte[] discarded, byte[] tail)
    throws IOException
    {
        try (CompressedSequentialWriter w = writer)
        {
            w.write(ByteBuffer.wrap(head));
            DataPosition mark = w.mark();
            w.write(ByteBuffer.wrap(discarded));
            w.resetAndTruncate(mark);
            w.write(ByteBuffer.wrap(tail));
            long position = w.position();
            w.finish();
            return position;
        }
    }

    private CompressedSequentialWriter newWriter(CompressionParams params, File data, File meta, int asyncBytes)
    {
        return new CompressedSequentialWriter(data, meta, null, SequentialWriterOption.DEFAULT,
                                              params, collector(), null, asyncBytes);
    }

    private void compareDirect(CompressionParams params, String name, int bytes) throws IOException
    {
        compare(params, name, bytes, false,
                (data, meta, asyncBytes) -> new DirectCompressedSequentialWriter(data, meta, null,
                                                                                 SequentialWriterOption.DEFAULT,
                                                                                 params, collector(), null, asyncBytes));
    }

    private void compare(CompressionParams params, String name, int bytes, boolean incompressible)
    throws IOException
    {
        compare(params, name, bytes, incompressible,
                (data, meta, asyncBytes) -> new CompressedSequentialWriter(data, meta, null,
                                                                           SequentialWriterOption.DEFAULT,
                                                                           params, collector(), null, asyncBytes));
    }

    /**
     * Writes the same payload through the inline and the pipelined writer, then asserts that both
     * produced byte-identical files and the same reported position.
     */
    private void compare(CompressionParams params, String name, int bytes, boolean incompressible, WriterFactory factory)
    throws IOException
    {
        byte[] payload = payload(bytes, incompressible);

        File syncData = FileUtils.createTempFile(name + "_sync", ".db");
        File asyncData = FileUtils.createTempFile(name + "_async", ".db");
        File syncMeta = new File(syncData.absolutePath() + ".metadata");
        File asyncMeta = new File(asyncData.absolutePath() + ".metadata");

        long syncPosition = write(factory.open(syncData, syncMeta, 0), payload);
        long asyncPosition = write(factory.open(asyncData, asyncMeta, asyncBytes(params)), payload);

        assertEquals(name + ": reported position differs", syncPosition, asyncPosition);
        assertArrayEquals(name + ": data file differs", readAll(syncData), readAll(asyncData));
        assertArrayEquals(name + ": compression metadata differs", readAll(syncMeta), readAll(asyncMeta));
    }

    private interface WriterFactory
    {
        CompressedSequentialWriter open(File data, File meta, int asyncBytes);
    }

    /** Deliberately small, so the slot pool wraps repeatedly during a run. */
    private static int asyncBytes(CompressionParams params)
    {
        return SLOTS * params.chunkLength();
    }

    private static MetadataCollector collector()
    {
        return new MetadataCollector(new ClusteringComparator(BytesType.instance));
    }

    /**
     * Writes the payload in a mix of shapes so the flush lands at varied offsets within a chunk:
     * a byte at a time, then a bulk array, then a ByteBuffer.
     */
    private static long write(SequentialWriter writer, byte[] payload) throws IOException
    {
        try (SequentialWriter w = writer)
        {
            int third = payload.length / 3;
            for (int i = 0; i < third; i++)
                w.write(payload[i]);

            w.write(payload, third, third);

            ByteBuffer rest = ByteBuffer.wrap(payload, third * 2, payload.length - third * 2);
            w.write(rest);

            long position = w.position();
            w.finish();
            return position;
        }
    }

    private static byte[] payload(int bytes, boolean incompressible)
    {
        byte[] data = new byte[bytes];
        Random r = new Random(42);
        r.nextBytes(data);
        if (!incompressible)
        {
            // Leave the first half a repeating motif so the compressor finds matches and the
            // compressed branch of flushData is the one exercised.
            for (int i = 0; i < bytes / 2; i++)
                data[i] = (byte) (i % 8);
        }
        return data;
    }

    private static byte[] readAll(File f) throws IOException
    {
        return java.nio.file.Files.readAllBytes(f.toPath());
    }
}
