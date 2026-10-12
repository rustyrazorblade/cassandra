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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPools;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamReadAheadTest
{
    private static final int CHUNK_SIZE = 4096;

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final ExecutorPlus executor =
        ExecutorFactory.Global.executorFactory().pooled("test-stream-read-ahead", 4);

    @AfterClass
    public static void shutdownExecutor()
    {
        executor.shutdownNow();
    }

    @Test
    public void chunksComeOutInOrderAndThenNull() throws IOException
    {
        List<Long> progress = new ArrayList<>();
        try (StreamReadAhead ahead = StreamReadAhead.start(executor, 2, sink -> produce(sink, 8)))
        {
            StreamReadAhead.Chunk chunk;
            while ((chunk = ahead.take()) != null)
            {
                progress.add(chunk.progress);
                BufferPools.forNetworking().put(chunk.buffer);
            }
            assertNull("the reader is finished, so every further take is null", ahead.take());
        }

        assertEquals(8, progress.size());
        for (int i = 0; i < progress.size(); i++)
            assertEquals("chunks must arrive in the order the reader produced them", (long) i, (long) progress.get(i));
    }

    @Test
    public void theReaderStaysWithinTheDepth() throws IOException
    {
        AtomicInteger produced = new AtomicInteger();
        int depth = 2;
        try (StreamReadAhead ahead = StreamReadAhead.start(executor, depth, sink -> {
            for (int i = 0; i < 32; i++)
            {
                produced.incrementAndGet();
                sink.accept(new StreamReadAhead.Chunk(buffer(), i));
            }
        }))
        {
            // the queue holds the depth, and the reader blocks holding one more
            awaitAtLeast(produced, depth + 1);
            long deadline = nanoTime() + MILLISECONDS.toNanos(200);
            while (nanoTime() < deadline)
                assertTrue("the reader ran further than the depth allows: " + produced.get(),
                           produced.get() <= depth + 1);

            StreamReadAhead.Chunk chunk;
            while ((chunk = ahead.take()) != null)
                BufferPools.forNetworking().put(chunk.buffer);
        }

        assertEquals(32, produced.get());
    }

    @Test
    public void closeReturnsWhatTheSenderNeverTook() throws IOException
    {
        long used = BufferPools.forNetworking().usedSizeInBytes();
        AtomicInteger produced = new AtomicInteger();

        StreamReadAhead ahead = StreamReadAhead.start(executor, 2, sink -> {
            for (int i = 0; i < 1024; i++)
            {
                produced.incrementAndGet();
                sink.accept(new StreamReadAhead.Chunk(buffer(), i));
            }
        });

        awaitAtLeast(produced, 3);
        // takes one, leaving the queue full and the reader blocked with a chunk of its own
        StreamReadAhead.Chunk chunk = ahead.take();
        assertNotNull(chunk);
        BufferPools.forNetworking().put(chunk.buffer);

        ahead.close();

        assertTrue("the reader must stop rather than read the rest of the transfer", produced.get() < 1024);
        assertEquals("every buffer the sender did not take must go back to the pool",
                     used, BufferPools.forNetworking().usedSizeInBytes());
    }

    @Test
    public void theReadersFailureIsThrownBySend() throws IOException
    {
        try (StreamReadAhead ahead = StreamReadAhead.start(executor, 2, sink -> {
            sink.accept(new StreamReadAhead.Chunk(buffer(), 0));
            throw new IOException("disk went away");
        }))
        {
            StreamReadAhead.Chunk chunk = ahead.take();
            assertNotNull(chunk);
            BufferPools.forNetworking().put(chunk.buffer);

            try
            {
                ahead.take();
                fail("the reader failed, so the sender must see it");
            }
            catch (IOException e)
            {
                assertEquals("disk went away", e.getMessage());
            }
        }
    }

    @Test
    public void aRuntimeFailureIsRethrownUnchanged() throws IOException
    {
        RuntimeException boom = new RuntimeException("reader blew up");
        try (StreamReadAhead ahead = StreamReadAhead.start(executor, 2, sink -> {
            sink.accept(new StreamReadAhead.Chunk(buffer(), 0));
            throw boom;
        }))
        {
            StreamReadAhead.Chunk chunk = ahead.take();
            assertNotNull(chunk);
            BufferPools.forNetworking().put(chunk.buffer);

            try
            {
                ahead.take();
                fail("the reader failed, so the sender must see it");
            }
            catch (RuntimeException e)
            {
                assertSame("a RuntimeException must come back as the very instance the reader threw", boom, e);
            }
        }
    }

    @Test
    public void anUnexpectedFailureIsWrappedInIOException() throws IOException
    {
        // a Throwable that is neither IOException, RuntimeException nor Error takes the wrapping branch
        Throwable weird = new Throwable("reader broke oddly");
        try (StreamReadAhead ahead = StreamReadAhead.start(executor, 2, sink -> sneakyThrow(weird)))
        {
            try
            {
                ahead.take();
                fail("the reader failed, so the sender must see it");
            }
            catch (IOException e)
            {
                assertSame("the original throwable must be the cause of the wrapping IOException", weird, e.getCause());
            }
        }
    }

    @Test
    public void putReturnsTheBufferWhenTheReaderIsInterrupted() throws IOException
    {
        long used = BufferPools.forNetworking().usedSizeInBytes();
        AtomicReference<Thread> readerThread = new AtomicReference<>();
        AtomicInteger produced = new AtomicInteger();

        // depth one: the reader fills the single queue slot with the first chunk, then blocks in put holding
        // the second chunk. Interrupting it there makes put return that held buffer before it unwinds.
        StreamReadAhead ahead = StreamReadAhead.start(executor, 1, sink -> {
            readerThread.set(Thread.currentThread());
            for (int i = 0; i < 1024; i++)
            {
                produced.incrementAndGet();
                sink.accept(new StreamReadAhead.Chunk(buffer(), i));
            }
        });

        awaitAtLeast(produced, 2);
        awaitBlocked(readerThread);
        readerThread.get().interrupt();

        // close awaits the reader and drains the queued first chunk; the second chunk was returned by put
        ahead.close();

        assertTrue("the reader must stop rather than read the rest of the transfer", produced.get() < 1024);
        assertEquals("the buffer held in put must go back to the pool when the reader is interrupted",
                     used, BufferPools.forNetworking().usedSizeInBytes());
    }

    @Test
    public void randomTransfersDeliverEveryChunkInOrderWithoutLeaking() throws IOException
    {
        // a fixed seed keeps failures reproducible; the message carries the parameters that produced them
        Random rnd = new Random(20250918L);
        for (int iteration = 0; iteration < 200; iteration++)
        {
            int depth = 1 + rnd.nextInt(8);              // 1..8 chunks of read-ahead
            int chunkSize = 1 + rnd.nextInt(8192);       // 1 B .. 8 KiB per chunk
            // payload runs from a single byte up to several times the whole read-ahead window
            int totalSize = 1 + rnd.nextInt(depth * chunkSize * 4 + 1);
            String where = "iteration=" + iteration + " depth=" + depth
                           + " chunkSize=" + chunkSize + " totalSize=" + totalSize + " seed=20250918";

            byte[] payload = new byte[totalSize];
            rnd.nextBytes(payload);

            long used = BufferPools.forNetworking().usedSizeInBytes();

            byte[] delivered = new byte[totalSize];
            int offset = 0;
            long lastProgress = 0;

            StreamReadAhead ahead = StreamReadAhead.start(executor, depth, sink -> {
                int sent = 0;
                while (sent < totalSize)
                {
                    int len = Math.min(chunkSize, totalSize - sent);
                    ByteBuffer buffer = BufferPools.forNetworking().get(len, BufferType.OFF_HEAP);
                    buffer.put(payload, sent, len);
                    buffer.flip();
                    sent += len;
                    // progress is the transfer position after this chunk, so it climbs to the total
                    sink.accept(new StreamReadAhead.Chunk(buffer, sent));
                }
            });

            try
            {
                StreamReadAhead.Chunk chunk;
                while ((chunk = ahead.take()) != null)
                {
                    int len = chunk.buffer.remaining();
                    assertTrue("delivered more bytes than the payload holds; " + where, offset + len <= totalSize);
                    chunk.buffer.get(delivered, offset, len);
                    offset += len;

                    assertTrue("progress must climb; " + where, chunk.progress > lastProgress);
                    lastProgress = chunk.progress;

                    BufferPools.forNetworking().put(chunk.buffer);
                }
            }
            finally
            {
                ahead.close();
            }

            assertEquals("every byte of the payload must be delivered; " + where, totalSize, offset);
            assertArrayEquals("the delivered bytes must equal the payload, in order; " + where, payload, delivered);
            assertEquals("the final progress must equal the total transfer size; " + where, totalSize, lastProgress);
            assertEquals("no buffer may leak or be double-freed across a transfer; " + where,
                         used, BufferPools.forNetworking().usedSizeInBytes());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable t) throws T
    {
        throw (T) t;
    }

    private static void awaitBlocked(AtomicReference<Thread> threadRef)
    {
        long deadline = nanoTime() + SECONDS.toNanos(10);
        while (true)
        {
            Thread thread = threadRef.get();
            if (thread != null)
            {
                Thread.State state = thread.getState();
                if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
                    return;
            }
            if (nanoTime() > deadline)
                fail("timed out waiting for the reader to block in put");
            Thread.yield();
        }
    }

    @Test
    public void depthIsWholeChunksAndAtLeastOne()
    {
        int readAhead = DatabaseDescriptor.getStreamReadAheadInBytes();
        assertEquals(16, StreamReadAhead.depthFor(readAhead / 16));
        assertEquals("a chunk bigger than the read-ahead still gets one", 1, StreamReadAhead.depthFor(readAhead * 2));
    }

    private static void produce(StreamReadAhead.Sink sink, int chunks) throws InterruptedException
    {
        for (int i = 0; i < chunks; i++)
            sink.accept(new StreamReadAhead.Chunk(buffer(), i));
    }

    private static ByteBuffer buffer()
    {
        return BufferPools.forNetworking().get(CHUNK_SIZE, BufferType.OFF_HEAP);
    }

    private static void awaitAtLeast(AtomicInteger counter, int target)
    {
        long deadline = nanoTime() + SECONDS.toNanos(10);
        while (counter.get() < target)
        {
            if (nanoTime() > deadline)
                fail("timed out waiting for the reader to produce " + target + " chunks, saw " + counter.get());
            Thread.yield();
        }
    }
}
