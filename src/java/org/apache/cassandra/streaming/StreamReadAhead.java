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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.memory.BufferPools;

/**
 * Reads a transfer ahead of the sender, so the disk keeps working while the send window drains.
 *
 * The sender blocks whenever the bytes in flight reach the send window. A reader on the sending thread stops
 * for as long as the sender is blocked, and the disk goes idle just as the network is about to want more.
 * Here the sender only hands finished chunks to the channel.
 *
 * The queue holds whole chunks, so the read-ahead stays within one chunk of {@code stream_read_ahead}.
 * Chunks come out in the order the reader produced them. {@link #take()} rethrows whatever the reader threw.
 */
public class StreamReadAhead implements Closeable
{
    /** A chunk of the transfer, ready for the wire, and the transfer progress that sending it represents. */
    public static class Chunk
    {
        public final ByteBuffer buffer;
        public final long progress;

        public Chunk(ByteBuffer buffer, long progress)
        {
            this.buffer = buffer;
            this.progress = progress;
        }
    }

    /** Takes the chunks the reader produces, and blocks it once the read-ahead bound is reached. */
    public interface Sink
    {
        void accept(Chunk chunk) throws InterruptedException;
    }

    /** Reads one transfer in order. Runs on the read-ahead thread, never on the sending thread. */
    public interface Reader
    {
        void read(Sink sink) throws IOException, InterruptedException;
    }

    private final BlockingQueue<Chunk> queue;
    private final Future<?> reader;
    private volatile Throwable failure;
    private volatile boolean finished;
    private volatile boolean closed;

    private StreamReadAhead(ExecutorPlus executor, int depth, Reader reader)
    {
        this.queue = new ArrayBlockingQueue<>(depth);
        this.reader = executor.submit(() -> run(reader));
    }

    /**
     * Start reading {@code reader} on {@code executor}, keeping at most {@code depth} chunks ahead of the sender.
     * The caller owns the returned instance and must close it, which releases any chunk the sender never took.
     */
    public static StreamReadAhead start(ExecutorPlus executor, int depth, Reader reader)
    {
        return new StreamReadAhead(executor, depth, reader);
    }

    /** How many chunks of {@code chunkSize} fit in the configured read-ahead, at least one. */
    public static int depthFor(int chunkSize)
    {
        return Math.max(1, DatabaseDescriptor.getStreamReadAheadInBytes() / chunkSize);
    }

    /**
     * The next chunk in order, or null once the reader has finished. The caller owns the buffer: writing it to
     * the channel passes ownership on, and any other use must return it to the networking {@code BufferPool}.
     */
    public Chunk take() throws IOException
    {
        Chunk chunk = null;
        try
        {
            // the reader marks itself finished only once it has enqueued everything
            while (chunk == null && !finished)
                chunk = queue.poll(50, TimeUnit.MILLISECONDS);

            if (chunk == null)
                chunk = queue.poll();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }

        if (chunk != null)
            return chunk;

        Throwable failure = this.failure;
        if (failure == null)
            return null;
        if (failure instanceof IOException)
            throw (IOException) failure;
        if (failure instanceof RuntimeException)
            throw (RuntimeException) failure;
        if (failure instanceof Error)
            throw (Error) failure;
        throw new IOException("Failed reading ahead of the stream", failure);
    }

    /**
     * A running task cannot be interrupted, so close stops the reader by draining the queue it waits on. The
     * drain frees the space the reader blocks for, and its next chunk hits the closed check and unwinds it.
     */
    @Override
    public void close()
    {
        closed = true;
        while (!reader.awaitUninterruptibly(50))
            release();
        release();
    }

    private void run(Reader reader)
    {
        try
        {
            reader.read(this::put);
        }
        catch (Aborted aborted)
        {
            // the sender is gone and has released what it left behind
        }
        catch (Throwable t)
        {
            if (!closed)
                failure = t;
        }
        finally
        {
            finished = true;
        }
    }

    private void put(Chunk chunk) throws InterruptedException
    {
        if (closed)
        {
            BufferPools.forNetworking().put(chunk.buffer);
            throw new Aborted();
        }

        try
        {
            queue.put(chunk);
        }
        catch (InterruptedException e)
        {
            BufferPools.forNetworking().put(chunk.buffer);
            throw e;
        }
    }

    /** Unwinds the reader once the sender has closed. */
    private static class Aborted extends RuntimeException
    {
        Aborted()
        {
            super(null, null, false, false);
        }
    }

    private void release()
    {
        List<Chunk> unsent = new ArrayList<>();
        queue.drainTo(unsent);
        for (Chunk chunk : unsent)
            BufferPools.forNetworking().put(chunk.buffer);
    }
}
