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
import java.nio.channels.FileChannel;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DirectThreadLocalByteBufferHolder;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * The file a streaming writer sends, and the two ways a transport reads it.
 *
 * Without encryption the bytes never enter the process. The transport hands {@link #channel()} to the kernel,
 * and the kernel sends the region. With encryption the transport reads the bytes into user space through
 * {@link #read}, which is the only method the disk access mode changes.
 *
 * Only the compressed writer sends through this interface. The uncompressed writer opens its own channel,
 * because it verifies the CRC component against the bytes it reads.
 */
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface StreamingFileSource extends Closeable
{
    /**
     * The channel to hand the kernel for a zero-copy send. The caller takes ownership and closes the channel
     * once it has written the last region that refers to it.
     */
    FileChannel channel() throws IOException;

    /** Fill the buffer from {@code position}, reading exactly as many bytes as it has remaining. */
    void read(ByteBuffer into, long position) throws IOException;

    static StreamingFileSource open(File file, DiskAccessMode mode)
    {
        return mode == DiskAccessMode.direct ? new Direct(file) : new Buffered(file);
    }

    /** Reads through the page cache, which is where the bytes usually already are. */
    class Buffered implements StreamingFileSource
    {
        private final File file;
        private FileChannel channel;

        Buffered(File file)
        {
            this.file = file;
        }

        public FileChannel channel() throws IOException
        {
            return open();
        }

        public void read(ByteBuffer into, long position) throws IOException
        {
            FileChannel channel = open();
            long at = position;
            while (into.hasRemaining())
            {
                int read = channel.read(into, at);
                if (read < 0)
                    throw new IOException("Unexpected end of " + file + " at " + at);
                at += read;
            }
        }

        private FileChannel open() throws IOException
        {
            if (channel == null)
                channel = file.newReadChannel();
            return channel;
        }

        public void close() throws IOException
        {
            // the channel is only ours until it is handed out, and whoever took it closes it
        }
    }

    /**
     * Reads with O_DIRECT, so a transfer does not evict what the page cache holds for the read path.
     * Streamed bytes are read once and never wanted again, which is the case the page cache handles worst.
     */
    class Direct implements StreamingFileSource
    {
        private final File file;
        private final int blockSize;
        private final DirectThreadLocalByteBufferHolder buffers;
        private FileChannel channel;
        private ChannelProxy direct;

        Direct(File file)
        {
            this.file = file;
            this.blockSize = FileUtils.getFileBlockSize(file);
            this.buffers = new DirectThreadLocalByteBufferHolder(blockSize);
        }

        /** A zero-copy send goes through the kernel, so this channel does not use O_DIRECT. */
        public FileChannel channel() throws IOException
        {
            if (channel == null)
                channel = file.newReadChannel();
            return channel;
        }

        /**
         * O_DIRECT requires the offset, the length and the buffer to align to the device block. A section
         * aligns to none of them, so read the aligned span that covers the request, then copy out the bytes.
         */
        public void read(ByteBuffer into, long position) throws IOException
        {
            int length = into.remaining();
            long alignedPosition = position & -blockSize;
            int delta = (int) (position - alignedPosition);

            ByteBuffer aligned = buffers.getBuffer(length + delta);
            if (proxy().read(aligned, alignedPosition) < length + delta)
                throw new IOException("Unexpected end of " + file + " at " + position);

            aligned.position(delta).limit(delta + length);
            into.put(aligned);
        }

        private ChannelProxy proxy()
        {
            if (direct == null)
                direct = new ChannelProxy(file, ChannelProxy.IOMode.DIRECT);
            return direct;
        }

        public void close()
        {
            if (direct != null)
            {
                direct.close();
                direct = null;
            }
        }
    }
}
