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
import java.util.concurrent.atomic.AtomicInteger;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
