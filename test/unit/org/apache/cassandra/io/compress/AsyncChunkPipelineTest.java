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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Uninterruptibles;
import com.github.luben.zstd.ZstdDictTrainer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ForwardingExecutorFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    /** Chunks written while the writer thread is held. One fewer than SLOTS, so the producer never blocks. */
    private static final int HELD_CHUNKS = SLOTS - 1;

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
     * With trickle_fsync on, the writer thread runs an fdatasync every interval of uncompressed
     * bytes, as the synchronous writer does. The data compresses well, so a count of compressed bytes
     * would sync far less often.
     */
    @Test
    public void trickleFsyncRunsPerUncompressedInterval() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        int interval = 2 * params.chunkLength();
        int chunks = 20;
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .trickleFsync(true)
                                                              .trickleFsyncByteInterval(interval)
                                                              .build();
        byte[] payload = new byte[params.chunkLength() * chunks];

        SyncCountingWriter sync = syncCounting(params, option, 0);
        SyncCountingWriter async = syncCounting(params, option, asyncBytes(params));
        write(sync, payload);
        write(async, payload);

        // One sync per interval, plus the one at prepare.
        int expected = chunks * params.chunkLength() / interval + 1;
        assertEquals("synchronous writer", expected, sync.syncs);
        assertEquals("async writer must sync as often as the synchronous writer", expected, async.syncs);
        assertTrue("trickle syncs must run on the writer thread", async.syncThreads.stream().anyMatch(n -> n.startsWith("CompactionWriter-")));
    }

    @Test
    public void noTrickleFsyncWhenDisabled() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .trickleFsync(false)
                                                              .trickleFsyncByteInterval(params.chunkLength())
                                                              .build();
        SyncCountingWriter async = syncCounting(params, option, asyncBytes(params));
        write(async, new byte[params.chunkLength() * 20]);
        assertEquals("only the sync at prepare", 1, async.syncs);
    }

    /** O_DIRECT bypasses the page cache, so the async path syncs only at prepare. */
    @Test
    public void noTrickleFsyncUnderDirectIo() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .trickleFsync(true)
                                                              .trickleFsyncByteInterval(params.chunkLength())
                                                              .build();
        File data = FileUtils.createTempFile("trickle_direct", ".db");
        int[] syncs = { 0 };
        DirectCompressedSequentialWriter writer =
            new DirectCompressedSequentialWriter(data, new File(data.absolutePath() + ".metadata"), null, option, params,
                                                 collector(), null, asyncBytes(params))
            {
                @Override
                protected void syncDataOnlyInternal()
                {
                    syncs[0]++;
                    super.syncDataOnlyInternal();
                }
            };
        write(writer, new byte[params.chunkLength() * 20]);
        assertEquals("only the sync at prepare", 1, syncs[0]);
    }

    private static final class SyncCountingWriter extends CompressedSequentialWriter
    {
        volatile int syncs;
        final Set<String> syncThreads = ConcurrentHashMap.newKeySet();

        SyncCountingWriter(File data, SequentialWriterOption option, CompressionParams params, int asyncBytes)
        {
            super(data, new File(data.absolutePath() + ".metadata"), null, option, params, collector(), null, asyncBytes);
        }

        @Override
        protected void syncDataOnlyInternal()
        {
            syncs++;
            syncThreads.add(Thread.currentThread().getName());
            super.syncDataOnlyInternal();
        }
    }

    private static SyncCountingWriter syncCounting(CompressionParams params, SequentialWriterOption option, int asyncBytes)
    {
        return new SyncCountingWriter(FileUtils.createTempFile("trickle", ".db"), option, params, asyncBytes);
    }

    /**
     * A writer that is switched out keeps no writer thread and no slots. After stopAsyncWriterThread the
     * remaining chunks are written inline, and the files must still match the synchronous writer's.
     */
    @Test
    public void stopAsyncWriterThreadWritesTheRestInline() throws IOException
    {
        for (boolean direct : new boolean[]{ false, true })
        {
            CompressionParams params = CompressionParams.lz4();
            byte[] head = payload(params.chunkLength() * (SLOTS * 2) + 77, false);
            byte[] tail = payload(params.chunkLength() * 3 + 5, true);
            String name = direct ? "stop_direct" : "stop";

            File syncData = FileUtils.createTempFile(name + "_sync", ".db");
            File asyncData = FileUtils.createTempFile(name + "_async", ".db");
            CompressedSequentialWriter sync = open(direct, params, syncData, 0);
            CompressedSequentialWriter async = open(direct, params, asyncData, asyncBytes(params));

            for (CompressedSequentialWriter w : new CompressedSequentialWriter[]{ sync, async })
            {
                try (CompressedSequentialWriter writer = w)
                {
                    writer.write(head);
                    AsyncChunkPipeline pipeline = writer.pipeline;
                    writer.stopAsyncWriterThread();
                    if (writer == async)
                    {
                        assertFalse(name + ": writer thread outlived stopAsyncWriterThread", pipeline.stillRunning());
                        assertEquals(name + ": slots kept after stopAsyncWriterThread", 0, pipeline.freeSlotCount());
                        assertFalse(name + ": writer still uses the pipeline", writer.usesAsyncPipeline());
                    }
                    writer.write(tail);
                    writer.finish();
                }
            }

            assertSameFiles(name, syncData, asyncData);
        }
    }

    private CompressedSequentialWriter open(boolean direct, CompressionParams params, File data, int asyncBytes)
    {
        File meta = metaFor(data);
        File digest = digestFor(data);
        return direct
               ? new DirectCompressedSequentialWriter(data, meta, digest, SequentialWriterOption.DEFAULT, params, collector(), null, asyncBytes)
               : new CompressedSequentialWriter(data, meta, digest, SequentialWriterOption.DEFAULT, params, collector(), null, asyncBytes);
    }

    private static File metaFor(File data)
    {
        return new File(data.absolutePath() + ".metadata");
    }

    private static File digestFor(File data)
    {
        return new File(data.absolutePath() + ".digest");
    }

    private static void assertSameFiles(String name, File syncData, File asyncData) throws IOException
    {
        assertArrayEquals(name + ": data file differs", readAll(syncData), readAll(asyncData));
        assertArrayEquals(name + ": compression metadata differs", readAll(metaFor(syncData)), readAll(metaFor(asyncData)));
        assertTrue(name + ": no digest written", digestFor(syncData).exists());
        assertArrayEquals(name + ": digest differs", readAll(digestFor(syncData)), readAll(digestFor(asyncData)));
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
    public void postFlushOffsetNeverExceedsWrittenBytes() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        byte[] payload = payload(params.chunkLength() * (SLOTS * 5) + 123, true);

        File data = FileUtils.createTempFile("writtenInvariant", ".db");
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

        assertTrue("post-flush written-offset invariant violated: " + violations, violations.isEmpty());
        assertEquals("final written offset must equal the uncompressed size after finish",
                     payload.length, writer.getLastFlushOffset());
    }

    /**
     * Where the executor factory refuses to start a thread, the pipeline does the writer thread's work
     * inline. The files must match, and cleanup must see no thread.
     */
    @Test
    public void inlineFallbackMatchesSynchronousWriter() throws IOException
    {
        ExecutorFactory saved = ExecutorFactory.Global.executorFactory();
        ExecutorFactory.Global.unsafeSet(new ForwardingExecutorFactory(saved)
        {
            @Override
            public Thread startThread(String name, Runnable runnable)
            {
                throw new UnsupportedOperationException();
            }
        });
        try
        {
            CompressionParams params = CompressionParams.lz4();
            compare(params, "inline", DEFAULT_CHUNK_LENGTH * (SLOTS * 3) + 101, false);

            File data = FileUtils.createTempFile("inline_abort", ".db");
            CompressedSequentialWriter writer = newWriter(params, data, new File(data.absolutePath() + ".metadata"), asyncBytes(params));
            writer.write(payload(DEFAULT_CHUNK_LENGTH * 2, false));
            assertFalse("no writer thread may exist in inline mode", writer.pipeline.stillRunning());
            assertNull("abort in inline mode failed", writer.abort(null));
        }
        finally
        {
            ExecutorFactory.Global.unsafeSet(saved);
        }
    }

    /** The dictionary compressor shares one native dictionary across threads; the writer thread is one more. */
    @Test
    public void zstdDictionaryMatchesSynchronousWriter() throws IOException
    {
        ZstdDictTrainer trainer = new ZstdDictTrainer(100 * 1024, 6 * 1024, 3);
        Random random = new Random(7);
        for (int i = 0; i < 1000; i++)
        {
            byte[] sample = new byte[100];
            for (int b = 0; b < sample.length; b++)
                sample[b] = (byte) ((b % 8) + random.nextInt(4));
            trainer.addSample(sample);
        }
        ZstdCompressionDictionary dictionary = new ZstdCompressionDictionary(new DictId(Kind.ZSTD, 1), trainer.trainSamples());
        try
        {
            // The writer asks the manager only for the current dictionary.
            CompressionDictionaryManager manager = mock(CompressionDictionaryManager.class);
            when(manager.getCurrent()).thenReturn(dictionary);
            CompressionParams params = CompressionParams.zstd(DEFAULT_CHUNK_LENGTH, true);
            compare(params, "zstd_dictionary", DEFAULT_CHUNK_LENGTH * (SLOTS * 3) + 101, false,
                    (data, meta, asyncBytes) -> new CompressedSequentialWriter(data, meta, digestFor(data),
                                                                               SequentialWriterOption.DEFAULT,
                                                                               params, collector(), manager, asyncBytes));
        }
        finally
        {
            dictionary.close();
        }
    }

    /** A chunk length other than the default changes the slot size and the slot count. */
    @Test
    public void nonDefaultChunkLengthMatchesSynchronousWriter() throws IOException
    {
        CompressionParams params = CompressionParams.lz4(4096);
        compare(params, "chunk4k", 4096 * (SLOTS * 3) + 101, false);
        compareDirect(params, "chunk4k_direct", 4096 * (SLOTS * 3) + 101);
    }

    /** A budget smaller than two chunks still gives two slots, so the producer and writer can overlap. */
    @Test
    public void budgetBelowTwoChunksUsesTwoSlots() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        byte[] payload = payload(DEFAULT_CHUNK_LENGTH * 10 + 3, false);

        File syncData = FileUtils.createTempFile("twoslots_sync", ".db");
        File asyncData = FileUtils.createTempFile("twoslots_async", ".db");
        write(newWriter(params, syncData, metaFor(syncData), 0), payload);
        CompressedSequentialWriter async = newWriter(params, asyncData, metaFor(asyncData), 1);
        AsyncChunkPipeline pipeline = async.pipeline;
        write(async, payload);

        assertEquals("slots allocated for a 1-byte budget", 2, pipeline.allocatedSlots());
        assertSameFiles("twoslots", syncData, asyncData);
    }

    /**
     * Size-based SSTable switching reads getEstimatedOnDiskBytesWritten on the producer while chunks
     * are still queued for the writer thread. The estimate must count those chunks, or a switch comes
     * up to the whole buffer late. Let the first chunk through, hold the writer thread on the second,
     * then check the estimate matches the synchronous writer's. Every chunk is identical, so the
     * compression ratio of the one written chunk is exact for the rest.
     */
    @Test
    public void sizeEstimateCountsChunksNotYetWritten() throws Exception
    {
        CompressionParams params = CompressionParams.lz4();
        for (boolean incompressible : new boolean[]{ true, false })
        {
            byte[] payload = repeatedChunks(params, incompressible);
            long syncEstimate = syncEstimate(params, payload);

            for (HeldWriterFactory factory : heldWriterFactories(params))
            {
                WriteGate gate = new WriteGate(1);
                CompressedSequentialWriter writer = factory.open(gate);
                long estimate = estimateWhileHeld(writer, gate, payload, CompressedSequentialWriter::getEstimatedOnDiskBytesWritten);
                assertEquals(writer.getClass().getSuperclass().getSimpleName() + (incompressible ? ", incompressible" : ""),
                             syncEstimate, estimate, 1);
            }
        }
    }

    /**
     * Until a chunk is written there is no compression ratio to scale the queued chunks by, so the
     * estimate waits for the first chunk rather than guess. Hold the writer thread on the first chunk
     * and check the estimate does not return until it is released, then that it is exact.
     */
    @Test
    public void sizeEstimateWaitsForFirstChunk() throws Exception
    {
        CompressionParams params = CompressionParams.lz4();
        byte[] payload = repeatedChunks(params, false);
        long syncEstimate = syncEstimate(params, payload);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            for (HeldWriterFactory factory : heldWriterFactories(params))
            {
                WriteGate gate = new WriteGate(0);
                long estimate = estimateWhileHeld(factory.open(gate), gate, payload, w -> {
                    Future<Long> pending = executor.submit(w::getEstimatedOnDiskBytesWritten);
                    try
                    {
                        pending.get(200, TimeUnit.MILLISECONDS);
                        fail("estimate returned before the first chunk was written");
                    }
                    catch (TimeoutException expected)
                    {
                        // still waiting for the held chunk
                    }
                    gate.release.countDown();
                    return pending.get(1, TimeUnit.MINUTES);
                });
                assertEquals(syncEstimate, estimate, 1);
            }
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    /** HELD_CHUNKS copies of one chunk, plus a byte so the last full chunk is flushed. */
    private static byte[] repeatedChunks(CompressionParams params, boolean incompressible)
    {
        byte[] chunk = payload(params.chunkLength(), incompressible);
        byte[] payload = new byte[chunk.length * HELD_CHUNKS + 1];
        for (int i = 0; i < HELD_CHUNKS; i++)
            System.arraycopy(chunk, 0, payload, i * chunk.length, chunk.length);
        return payload;
    }

    private static long syncEstimate(CompressionParams params, byte[] payload) throws IOException
    {
        File data = FileUtils.createTempFile("estimate_sync", ".db");
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(data, new File(data.absolutePath() + ".metadata"),
                                                                                null, SequentialWriterOption.DEFAULT,
                                                                                params, collector(), null, 0))
        {
            writer.write(payload);
            long estimate = writer.getEstimatedOnDiskBytesWritten();
            writer.finish();
            return estimate;
        }
    }

    private interface Estimate
    {
        long read(CompressedSequentialWriter writer) throws Exception;
    }

    private static long estimateWhileHeld(CompressedSequentialWriter writer, WriteGate gate, byte[] payload, Estimate estimator)
    throws Exception
    {
        try (CompressedSequentialWriter w = writer)
        {
            long estimate;
            try
            {
                w.write(payload);
                // The writer thread is now parked on the held chunk, and everything before it is written.
                assertTrue("writer thread never reached the held chunk", gate.held.await(1, TimeUnit.MINUTES));
                estimate = estimator.read(w);
            }
            finally
            {
                gate.release.countDown();
            }
            w.finish();
            return estimate;
        }
    }

    private interface HeldWriterFactory
    {
        CompressedSequentialWriter open(WriteGate gate);
    }

    private List<HeldWriterFactory> heldWriterFactories(CompressionParams params)
    {
        List<HeldWriterFactory> factories = new ArrayList<>();
        factories.add(gate -> {
            File data = FileUtils.createTempFile("estimate_async", ".db");
            return new CompressedSequentialWriter(data, new File(data.absolutePath() + ".metadata"), null,
                                                  SequentialWriterOption.DEFAULT, params, collector(), null, asyncBytes(params))
            {
                @Override
                protected void flushData(ByteBuffer src)
                {
                    gate.pass();
                    super.flushData(src);
                }
            };
        });
        factories.add(gate -> {
            File data = FileUtils.createTempFile("estimate_direct", ".db");
            return new DirectCompressedSequentialWriter(data, new File(data.absolutePath() + ".metadata"), null,
                                                        SequentialWriterOption.DEFAULT, params, collector(), null, asyncBytes(params))
            {
                @Override
                protected void flushData(ByteBuffer src)
                {
                    gate.pass();
                    super.flushData(src);
                }
            };
        });
        return factories;
    }

    /** Lets the first {@code passing} chunks through, then holds the writer thread until released. */
    private static final class WriteGate
    {
        final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private final int passing;
        private int calls;

        WriteGate(int passing)
        {
            this.passing = passing;
        }

        void pass()
        {
            if (calls++ < passing)
                return;
            held.countDown();
            Uninterruptibles.awaitUninterruptibly(release);
        }
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
        assertSameFiles(name, syncData, asyncData);
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
        return new CompressedSequentialWriter(data, meta, digestFor(data), SequentialWriterOption.DEFAULT,
                                              params, collector(), null, asyncBytes);
    }

    private void compareDirect(CompressionParams params, String name, int bytes) throws IOException
    {
        compare(params, name, bytes, false,
                (data, meta, asyncBytes) -> new DirectCompressedSequentialWriter(data, meta, digestFor(data),
                                                                                 SequentialWriterOption.DEFAULT,
                                                                                 params, collector(), null, asyncBytes));
    }

    private void compare(CompressionParams params, String name, int bytes, boolean incompressible)
    throws IOException
    {
        compare(params, name, bytes, incompressible,
                (data, meta, asyncBytes) -> new CompressedSequentialWriter(data, meta, digestFor(data),
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
        assertSameFiles(name, syncData, asyncData);
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
