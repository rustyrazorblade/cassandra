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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.BufferUtil;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.metrics.ReadAheadBufferMetrics;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.memory.MemoryUtil;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import sun.nio.ch.DirectBuffer;

/**
 * A read-ahead buffer for sequential scans of a single file.
 */
@NotThreadSafe
public class ReadAheadBuffer implements Closeable
{
    protected final ChannelProxy channel;

    /**
     * Buffers a thread keeps for its next read-ahead buffers of the same size and type, so a
     * short-lived reader neither allocates nor zeroes one.  Only threads that clear their
     * {@link FastThreadLocal}s when they exit keep buffers, so the buffers are freed at once
     * when the thread exits.  The buffers kept by all threads fit in
     * {@link DatabaseDescriptor#getCompressedReadAheadBufferPoolSize()}.
     */
    private static final int BUFFERS_KEPT_PER_THREAD = 4;
    private static final FastThreadLocal<ArrayDeque<ByteBuffer>> KEPT = new FastThreadLocal<>()
    {
        @Override
        protected ArrayDeque<ByteBuffer> initialValue()
        {
            return new ArrayDeque<>(BUFFERS_KEPT_PER_THREAD);
        }

        @Override
        protected void onRemoval(ArrayDeque<ByteBuffer> kept)
        {
            ByteBuffer buffer;
            while ((buffer = kept.poll()) != null)
                freeKept(buffer);
        }
    };
    /** Bytes and number of the buffers kept by all threads. */
    private static final AtomicLong keptBytes = new AtomicLong();
    private static final AtomicInteger keptCount = new AtomicInteger();
    public static final ReadAheadBufferMetrics metrics = new ReadAheadBufferMetrics(keptBytes::get, keptCount::get);

    private final Supplier<ByteBuffer> bufferSupplier;
    private final long channelSize;
    /** The buffer goes back to the thread's kept buffers on close, rather than being freed. */
    private final boolean keepBuffer;

    private ByteBuffer buffer;
    private int index = -1;
    private boolean released;
    /** A fill reads no further than this file offset, unless it starts at or past it; see {@link #limitTo}. */
    private long limit = Long.MAX_VALUE;
    /** The file range in the buffer when a limit is set: [windowStart, windowEnd). */
    private long windowStart = -1;
    private long windowEnd = -1;

    public ReadAheadBuffer(ChannelProxy channel, int bufferSize, BufferType bufferType)
    {
        this(channel, () -> keptOrNew(bufferSize, bufferType), true);
    }

    public ReadAheadBuffer(ChannelProxy channel, Supplier<ByteBuffer> bufferSupplier)
    {
        this(channel, bufferSupplier, false);
    }

    /** @param keepBuffer the buffer goes back to the thread's kept buffers on close, rather than being freed */
    protected ReadAheadBuffer(ChannelProxy channel, Supplier<ByteBuffer> bufferSupplier, boolean keepBuffer)
    {
        this.channel = channel;
        this.channelSize = channel.size();
        this.bufferSupplier = bufferSupplier;
        this.keepBuffer = keepBuffer;
    }

    private static boolean keepsBuffers()
    {
        return FastThreadLocalThread.willCleanupFastThreadLocals(Thread.currentThread());
    }

    private static ByteBuffer keptOrNew(int bufferSize, BufferType bufferType)
    {
        return keptOrNew(bufferSize, bufferType == BufferType.OFF_HEAP, 1, () -> bufferType.allocate(bufferSize));
    }

    /** A kept or new direct buffer whose address is a multiple of {@code alignment}, for direct I/O. */
    protected static ByteBuffer keptOrNewAligned(int bufferSize, int alignment)
    {
        return keptOrNew(bufferSize, true, alignment, () -> BufferUtil.allocateDirectAligned(bufferSize, alignment));
    }

    private static ByteBuffer keptOrNew(int bufferSize, boolean direct, int alignment, Supplier<ByteBuffer> allocate)
    {
        if (!keepsBuffers())
            return allocate.get();

        ByteBuffer match = null;
        Iterator<ByteBuffer> kept = KEPT.get().iterator();
        while (kept.hasNext())
        {
            ByteBuffer buffer = kept.next();
            if (match == null && buffer.capacity() == bufferSize && buffer.isDirect() == direct
                && (alignment <= 1 || MemoryUtil.getAddress(buffer) % alignment == 0))
            {
                kept.remove();
                untrack(buffer);
                match = buffer;
            }
            else if (buffer.capacity() != bufferSize || buffer.isDirect() != direct)
            {
                // the size setting or the compressor changed; this buffer would only take up room
                kept.remove();
                freeKept(buffer);
            }
        }
        return match != null ? match : allocate.get();
    }

    /** Keeps {@code buffer} for this thread if the per-thread and node-wide limits allow it. */
    private static boolean tryKeep(ByteBuffer buffer)
    {
        if (!keepsBuffers())
            return false;

        ArrayDeque<ByteBuffer> kept = KEPT.get();
        if (kept.size() >= BUFFERS_KEPT_PER_THREAD)
            return false;

        int size = buffer.capacity();
        if (keptBytes.addAndGet(size) > DatabaseDescriptor.getCompressedReadAheadBufferPoolSize())
        {
            keptBytes.addAndGet(-size);
            return false;
        }
        keptCount.incrementAndGet();
        kept.addFirst(buffer);
        return true;
    }

    private static void untrack(ByteBuffer buffer)
    {
        keptBytes.addAndGet(-buffer.capacity());
        keptCount.decrementAndGet();
    }

    private static void freeKept(ByteBuffer buffer)
    {
        untrack(buffer);
        free(buffer);
    }

    /** Frees a buffer this class allocated: an aligned buffer is a slice, so its root allocation. */
    private static void free(ByteBuffer buffer)
    {
        Object root = buffer.isDirect() ? ((DirectBuffer) buffer).attachment() : null;
        FileUtils.clean(root instanceof ByteBuffer ? (ByteBuffer) root : buffer);
    }

    public boolean hasBuffer()
    {
        return buffer != null;
    }

    @VisibleForTesting
    int bufferSize()
    {
        return buffer == null ? -1 : buffer.capacity();
    }

    @VisibleForTesting
    long bufferAddress()
    {
        return buffer == null ? -1 : MemoryUtil.getAddress(buffer);
    }

    public int remaining()
    {
        return allocatedBuffer().remaining();
    }

    @VisibleForTesting
    void allocateBuffer()
    {
        getBuffer();
    }

    private ByteBuffer getBuffer()
    {
        if (released)
            throw new IllegalStateException("read-ahead buffer for " + channel.filePath() + " has been released");

        if (buffer == null)
        {
            buffer = bufferSupplier.get();
            buffer.clear();
        }
        return buffer;
    }

    private ByteBuffer allocatedBuffer()
    {
        if (buffer == null)
            throw new IllegalStateException("read-ahead buffer for " + channel.filePath() + " is not allocated");

        return buffer;
    }

    /**
     * Stops each fill at {@code limit}, the end of what the reader will need, so a read that ends
     * there does not read a whole buffer past it.  A fill then starts at the position asked for
     * rather than at a buffer-aligned block.  A fill at or past the limit reads a whole buffer.
     */
    public void limitTo(long limit)
    {
        this.limit = limit;
        windowStart = windowEnd = -1;
        index = -1;
    }

    /** Where a limited fill that must hold {@code position} starts: the position itself. */
    protected long windowStartFor(long position)
    {
        return position;
    }

    public void fill(long position) throws CorruptBlockException
    {
        ByteBuffer blockBuffer = getBuffer();
        int bufferSize = blockBuffer.capacity();
        if (position >= channelSize)
            throw new CorruptBlockException(channel.filePath(), position, bufferSize);

        if (limit != Long.MAX_VALUE)
        {
            fillLimited(blockBuffer, position, bufferSize);
            return;
        }

        int blockNo = (int) (position / bufferSize);
        long blockPosition = blockNo * (long) bufferSize;

        long remaining = channelSize - blockPosition;
        int sizeToRead = (int) Math.min(remaining, bufferSize);
        if (index != blockNo)
        {
            blockBuffer.flip();
            loadBlock(blockBuffer, blockPosition, sizeToRead);
            index = blockNo;
        }

        blockBuffer.flip();
        blockBuffer.limit(sizeToRead);
        blockBuffer.position((int) (position - blockPosition));
    }

    private void fillLimited(ByteBuffer blockBuffer, long position, int bufferSize) throws CorruptBlockException
    {
        if (position < windowStart || position >= windowEnd)
        {
            long start = windowStartFor(position);
            long end = Math.min(channelSize, start + bufferSize);
            if (limit > position)
                end = Math.min(end, limit);
            blockBuffer.clear();
            loadBlock(blockBuffer, start, (int) (end - start));
            windowStart = start;
            windowEnd = end;
        }
        blockBuffer.limit((int) (windowEnd - windowStart));
        blockBuffer.position((int) (position - windowStart));
    }

    protected void loadBlock(ByteBuffer blockBuffer, long blockPosition, int sizeToRead) throws CorruptBlockException
    {
        blockBuffer.limit(sizeToRead);
        if (channel.read(blockBuffer, blockPosition) != sizeToRead)
            throw new CorruptBlockException(channel.filePath(), blockPosition, sizeToRead);
    }

    public int read(ByteBuffer dest, int length)
    {
        ByteBuffer blockBuffer = allocatedBuffer();
        ByteBuffer tmp = blockBuffer.duplicate();
        tmp.limit(tmp.position() + length);
        dest.put(tmp);
        blockBuffer.position(blockBuffer.position() + length);

        return length;
    }

    protected void cleanBuffer(ByteBuffer buffer)
    {
        // a kept buffer may be an aligned one, whichever kind of read-ahead buffer took it
        free(buffer);
    }

    @Override
    public void close()
    {
        released = true;
        if (buffer == null)
            return;

        index = -1;
        windowStart = windowEnd = -1;
        buffer.clear();
        if (!keepBuffer || !tryKeep(buffer))
            cleanBuffer(buffer);
        buffer = null;
    }
}
