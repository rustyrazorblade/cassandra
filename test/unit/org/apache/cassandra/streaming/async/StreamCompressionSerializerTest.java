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

package org.apache.cassandra.streaming.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Random;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.memory.MemoryUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class StreamCompressionSerializerTest
{
    private static final int VERSION = MessagingService.current_version;
    private static final Random random = new Random(2347623847623L);

    private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    private final StreamCompressionSerializer serializer = new StreamCompressionSerializer(allocator);
    private final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    private final LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();

    private ByteBuffer input;
    private ByteBuffer compressed;
    private ByteBuf output;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void tearDown()
    {
        if (input != null)
            MemoryUtil.clean(input);
        if (compressed != null)
            MemoryUtil.clean(compressed);
        if (output != null && output.refCnt() > 0)
            output.release(output.refCnt());
    }

    @Test
    public void roundTrip_HappyPath_NotReadabaleByteBuffer() throws IOException
    {
        populateInput();
        StreamCompressionSerializer.serialize(compressor, input, VERSION).write(size -> compressed = ByteBuffer.allocateDirect(size));
        input.flip();
        output = serializer.deserialize(decompressor, new DataInputBuffer(compressed, false), VERSION);
        validateResults();
    }

    private void populateInput()
    {
        int bufSize = 1 << 14;
        input = ByteBuffer.allocateDirect(bufSize);
        for (int i = 0; i < bufSize; i += 4)
            input.putInt(random.nextInt());
        input.flip();
    }

    private void validateResults()
    {
        Assert.assertEquals(input.remaining(), output.readableBytes());
        for (int i = 0; i < input.remaining(); i++)
            Assert.assertEquals(input.get(i), output.readByte());
    }

    @Test
    public void roundTrip_HappyPath_ReadabaleByteBuffer() throws IOException
    {
        populateInput();
        StreamCompressionSerializer.serialize(compressor, input, VERSION)
                                   .write(size -> {
                                       if (compressed != null)
                                           MemoryUtil.clean(compressed);
                                       return compressed = ByteBuffer.allocateDirect(size);
                                   });
        input.flip();
        output = serializer.deserialize(decompressor, new ByteBufRCH(Unpooled.wrappedBuffer(compressed)), VERSION);
        validateResults();
    }

    /**
     * The deserializer keeps one staging byte[] for the compressed chunk and reuses it across
     * calls, growing it when a chunk needs more room. Reuse is only safe if every read is bounded
     * by the current chunk's compressed length: a buffer left long by an earlier, larger chunk
     * still holds that chunk's bytes, and a smaller chunk that read the whole array, or wrapped
     * it without a limit, would decompress trailing garbage.
     */
    @Test
    public void reusedStagingBufferHandlesShrinkingChunks() throws IOException
    {
        StreamCompressionSerializer reused = new StreamCompressionSerializer(allocator);

        roundTripThrough(reused, 1 << 15);
        roundTripThrough(reused, 1 << 10);
        roundTripThrough(reused, 64);
    }

    /** The other direction: the staging buffer has to grow, and stay correct while doing it. */
    @Test
    public void reusedStagingBufferHandlesGrowingChunks() throws IOException
    {
        StreamCompressionSerializer reused = new StreamCompressionSerializer(allocator);

        roundTripThrough(reused, 64);
        roundTripThrough(reused, 1 << 10);
        roundTripThrough(reused, 1 << 15);
    }

    /** Sizes that alternate, so neither growth nor reuse is exercised in isolation. */
    @Test
    public void reusedStagingBufferHandlesAlternatingChunkSizes() throws IOException
    {
        StreamCompressionSerializer reused = new StreamCompressionSerializer(allocator);

        for (int i = 0; i < 6; i++)
            roundTripThrough(reused, (i % 2 == 0) ? 1 << 14 : 128);
    }

    /**
     * One round trip of {@code size} random bytes through the non-{@link ReadableByteChannel}
     * path, which is the one that stages into the reused array. Buffers are local so a failure
     * points at the serializer's state rather than at the fixture's.
     */
    private void roundTripThrough(StreamCompressionSerializer serializer, int size) throws IOException
    {
        ByteBuffer source = ByteBuffer.allocateDirect(size);
        while (source.remaining() >= 4)
            source.putInt(random.nextInt());
        while (source.hasRemaining())
            source.put((byte) random.nextInt());
        source.flip();

        ByteBuffer[] holder = new ByteBuffer[1];
        StreamCompressionSerializer.serialize(compressor, source, VERSION)
                                   .write(bytes -> holder[0] = ByteBuffer.allocateDirect(bytes));
        source.flip();

        ByteBuf result = null;
        try
        {
            result = serializer.deserialize(decompressor, new DataInputBuffer(holder[0], false), VERSION);
            Assert.assertEquals("wrong length for a " + size + " byte chunk", size, result.readableBytes());
            for (int i = 0; i < size; i++)
                Assert.assertEquals("byte " + i + " of a " + size + " byte chunk", source.get(i), result.readByte());
        }
        finally
        {
            if (result != null && result.refCnt() > 0)
                result.release(result.refCnt());
            MemoryUtil.clean(source);
            MemoryUtil.clean(holder[0]);
        }
    }

    private static class ByteBufRCH extends DataInputBuffer implements ReadableByteChannel
    {
        public ByteBufRCH(ByteBuf compressed)
        {
            super (compressed.nioBuffer(0, compressed.readableBytes()), false);
        }

        @Override
        public int read(ByteBuffer dst) throws IOException
        {
            int len = dst.remaining();
            dst.put(buffer);
            return len;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }
    }
}
