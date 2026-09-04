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
import java.util.Collections;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;

import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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

    private void compareDirect(CompressionParams params, String name, int bytes) throws IOException
    {
        byte[] payload = payload(bytes, false);

        File syncData = FileUtils.createTempFile(name + "_sync", ".db");
        File asyncData = FileUtils.createTempFile(name + "_async", ".db");
        File syncMeta = new File(syncData.absolutePath() + ".metadata");
        File asyncMeta = new File(asyncData.absolutePath() + ".metadata");

        long syncPosition = write(new DirectCompressedSequentialWriter(syncData, syncMeta, null,
                                                                       SequentialWriterOption.DEFAULT, params,
                                                                       collector(), null, 0), payload);
        long asyncPosition = write(new DirectCompressedSequentialWriter(asyncData, asyncMeta, null,
                                                                        SequentialWriterOption.DEFAULT, params,
                                                                        collector(), null, asyncBytes(params)), payload);

        assertEquals(name + ": reported position differs", syncPosition, asyncPosition);
        assertArrayEquals(name + ": data file differs", readAll(syncData), readAll(asyncData));
        assertArrayEquals(name + ": compression metadata differs", readAll(syncMeta), readAll(asyncMeta));
    }

    private void compare(CompressionParams params, String name, int bytes, boolean incompressible)
    throws IOException
    {
        byte[] payload = payload(bytes, incompressible);

        File syncData = FileUtils.createTempFile(name + "_sync", ".db");
        File asyncData = FileUtils.createTempFile(name + "_async", ".db");
        File syncMeta = new File(syncData.absolutePath() + ".metadata");
        File asyncMeta = new File(asyncData.absolutePath() + ".metadata");

        long syncPosition = write(newSync(syncData, syncMeta, params), payload);
        long asyncPosition = write(newAsync(asyncData, asyncMeta, params), payload);

        assertEquals(name + ": reported position differs", syncPosition, asyncPosition);
        assertArrayEquals(name + ": data file differs", readAll(syncData), readAll(asyncData));
        assertArrayEquals(name + ": compression metadata differs", readAll(syncMeta), readAll(asyncMeta));
    }

    private CompressedSequentialWriter newSync(File data, File meta, CompressionParams params)
    {
        return new CompressedSequentialWriter(data, meta, null, SequentialWriterOption.DEFAULT, params,
                                              collector());
    }

    private CompressedSequentialWriter newAsync(File data, File meta, CompressionParams params)
    {
        return new CompressedSequentialWriter(data, meta, null, SequentialWriterOption.DEFAULT, params,
                                              collector(), null, asyncBytes(params));
    }

    /** Deliberately small, so the slot pool wraps repeatedly during a run. */
    private static int asyncBytes(CompressionParams params)
    {
        return SLOTS * params.chunkLength();
    }

    private static MetadataCollector collector()
    {
        return new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
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
