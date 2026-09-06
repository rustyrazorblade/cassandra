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
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.function.LongConsumer;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.utils.memory.BufferPools;

public class StreamingDataOutputPlusFixed extends DataOutputBufferFixed implements StreamingDataOutputPlus
{
    public StreamingDataOutputPlusFixed(ByteBuffer buffer)
    {
        super(buffer);
    }

    @Override
    public int writeToChannel(Write write, RateLimiter limiter) throws IOException
    {
        int position = buffer.position();
        write.write(size -> buffer);
        return buffer.position() - position;
    }

    @Override
    public int writeToChannel(ByteBuffer ready, RateLimiter limiter) throws IOException
    {
        try
        {
            int length = ready.remaining();
            buffer.put(ready);
            return length;
        }
        finally
        {
            BufferPools.forNetworking().put(ready);
        }
    }

    @Override
    public long writeFileToChannel(FileChannel file, RateLimiter limiter) throws IOException
    {
        long count = 0;
        long tmp;
        while (0 <= (tmp = file.read(buffer))) count += tmp;
        return count;
    }

    @Override
    public long writeFileToChannel(StreamingFileSource source, RateLimiter limiter, List<Section> sections, LongConsumer progress, ExecutorPlus readAhead) throws IOException
    {
        long count = 0;
        try
        {
            for (Section section : sections)
            {
                long position = section.start;
                long remaining = section.length();
                while (remaining > 0)
                {
                    int read = readInto(source, position, remaining);
                    position += read;
                    remaining -= read;
                    count += read;
                    progress.accept(read);
                }
            }
        }
        finally
        {
            source.close();
        }
        return count;
    }

    private int readInto(StreamingFileSource source, long position, long remaining) throws IOException
    {
        if (!buffer.hasRemaining())
            throw new IOException("Buffer is full with " + remaining + " bytes of the section still to read");

        int limit = buffer.limit();
        buffer.limit((int) Math.min(buffer.position() + remaining, limit));
        try
        {
            int read = buffer.remaining();
            source.read(buffer, position);
            return read;
        }
        finally
        {
            buffer.limit(limit);
        }
    }
}
