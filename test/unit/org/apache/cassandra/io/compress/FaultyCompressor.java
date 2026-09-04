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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableMap;

/**
 * Delegates to LZ4 but throws on a chosen compress call, so a failure can be injected at a known
 * chunk. Used to prove that a failure raised on the async writer's own thread still unwinds the
 * producer's stack, since the try-with-resources in CompactionTask is the only thing that aborts
 * the transaction.
 *
 * The counter is static because the compressor is built by reflection from CompressionParams and
 * the test cannot reach the instance. Tests run sequentially in one JVM, so {@link #reset} between
 * them is enough.
 */
public class FaultyCompressor implements ICompressor
{
    private static final AtomicLong compressCalls = new AtomicLong();
    private static volatile long failAtCall = Long.MAX_VALUE;

    private final ICompressor delegate = LZ4Compressor.create(ImmutableMap.of());

    public static Map<String, String> params()
    {
        return ImmutableMap.of();
    }

    /** Throw on the nth compress call, counting from 1. */
    public static void failAt(long n)
    {
        compressCalls.set(0);
        failAtCall = n;
    }

    public static void reset()
    {
        compressCalls.set(0);
        failAtCall = Long.MAX_VALUE;
    }

    public static long callsSoFar()
    {
        return compressCalls.get();
    }

    @SuppressWarnings("unused")   // found by reflection from CompressionParams
    public static FaultyCompressor create(Map<String, String> opts)
    {
        return new FaultyCompressor();
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        if (compressCalls.incrementAndGet() == failAtCall)
            throw new IOException("injected compression failure at chunk " + failAtCall);

        delegate.compress(input, output);
    }

    @Override
    public int initialCompressedBufferLength(int chunkLength)
    {
        return delegate.initialCompressedBufferLength(chunkLength);
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
    throws IOException
    {
        return delegate.uncompress(input, inputOffset, inputLength, output, outputOffset);
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        delegate.uncompress(input, output);
    }

    @Override
    public BufferType preferredBufferType()
    {
        return delegate.preferredBufferType();
    }

    @Override
    public boolean supports(BufferType bufferType)
    {
        return delegate.supports(bufferType);
    }

    @Override
    public Set<String> supportedOptions()
    {
        return delegate.supportedOptions();
    }
}
