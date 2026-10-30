/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import javax.annotation.Nullable;

import org.apache.cassandra.utils.memory.BufferPools;

/**
 * Buffer manager used for reading from a ChunkReader when cache is not in use. Instances of this class are
 * reader-specific and thus do not need to be thread-safe since the reader itself isn't.
 *
 * The instances reuse themselves as the BufferHolder to avoid having to return a new object for each rebuffer call.
 */
public abstract class BufferManagingRebufferer implements Rebufferer, Rebufferer.BufferHolder
{
    protected final ChunkReader source;
    /** Charged with the bytes on disk of each chunk read, or null for none. */
    @Nullable
    private final ThroughputLimiter limiter;
    protected final ByteBuffer buffer;
    protected long offset = 0;

    abstract long alignedPosition(long position);

    protected BufferManagingRebufferer(ChunkReader wrapped)
    {
        this(wrapped, null);
    }

    protected BufferManagingRebufferer(ChunkReader wrapped, @Nullable ThroughputLimiter limiter)
    {
        this.source = wrapped;
        this.limiter = limiter;
        buffer = BufferPools.forChunkCache().get(wrapped.chunkSize(), wrapped.preferredBufferType()).order(ByteOrder.BIG_ENDIAN);
        buffer.limit(0);
    }

    @Override
    public void closeReader()
    {
        BufferPools.forChunkCache().put(buffer);
        source.releaseUnderlyingResources();
        offset = -1;
    }

    @Override
    public void close()
    {
        assert offset == -1;    // reader must be closed at this point.
        source.close();
    }

    @Override
    public ChannelProxy channel()
    {
        return source.channel();
    }

    @Override
    public long fileLength()
    {
        return source.fileLength();
    }

    @Override
    public void limitReadAhead(long position)
    {
        source.limitReadAhead(position);
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        offset = alignedPosition(position);
        source.readChunk(offset, buffer);
        if (limiter != null)
            limiter.acquire(source.diskBytes(offset, buffer));
        return this;
    }

    @Override
    public double getCrcCheckChance()
    {
        return source.getCrcCheckChance();
    }

    @Override
    public String toString()
    {
        return "BufferManagingRebufferer." + getClass().getSimpleName() + ":" + source;
    }

    // BufferHolder methods

    public ByteBuffer buffer()
    {
        return buffer;
    }

    public long offset()
    {
        return offset;
    }

    @Override
    public void release()
    {
        // nothing to do, we don't delete buffers before we're closed.
    }

    public static class Unaligned extends BufferManagingRebufferer
    {
        public Unaligned(ChunkReader wrapped)
        {
            super(wrapped);
        }

        public Unaligned(ChunkReader wrapped, @Nullable ThroughputLimiter limiter)
        {
            super(wrapped, limiter);
        }

        @Override
        long alignedPosition(long position)
        {
            return position;
        }
    }

    public static class Aligned extends BufferManagingRebufferer
    {
        public Aligned(ChunkReader wrapped)
        {
            this(wrapped, null);
        }

        public Aligned(ChunkReader wrapped, @Nullable ThroughputLimiter limiter)
        {
            super(wrapped, limiter);
            assert Integer.bitCount(wrapped.chunkSize()) == 1;
        }

        @Override
        long alignedPosition(long position)
        {
            return position & -buffer.capacity();
        }
    }
}
