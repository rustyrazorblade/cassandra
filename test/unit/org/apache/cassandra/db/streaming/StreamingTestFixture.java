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
package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.utils.FBUtilities;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Shared scaffolding for the legacy (non-zero-copy) streaming tests: build a stream header, run a writer
 * against a channel that keeps whatever it emits, and work out what those bytes should have been.
 *
 * The capture channel accepts both a {@link ByteBuf} and a {@link FileRegion}, because the writer is free to
 * choose either and the tests are about the bytes, not about which one it picked.
 */
public final class StreamingTestFixture
{
    private StreamingTestFixture()
    {
    }

    /** The whole data file, as one section. */
    public static List<PartitionPositionBounds> wholeFile(SSTableReader sstable)
    {
        return Collections.singletonList(new PartitionPositionBounds(0, dataLength(sstable)));
    }

    public static long dataLength(SSTableReader sstable)
    {
        return sstable.compression
               ? sstable.getCompressionMetadata().dataLength
               : sstable.descriptor.fileFor(Components.DATA).length();
    }

    public static CassandraStreamHeader header(SSTableReader sstable, List<PartitionPositionBounds> sections)
    {
        CassandraStreamHeader.Builder builder =
            CassandraStreamHeader.builder()
                                 .withSSTableVersion(sstable.descriptor.version)
                                 .withSSTableLevel(0)
                                 .withEstimatedKeys(sstable.estimatedKeys())
                                 .withSections(sections)
                                 .withSerializationHeader(sstable.header.toComponent())
                                 .withTableId(sstable.metadata().id);

        return builder.withCompressionInfo(sstable.compression
                                           ? CompressionInfo.newLazyInstance(sstable.getCompressionMetadata(), sections)
                                           : null)
                      .build();
    }

    public static CassandraStreamWriter writer(SSTableReader sstable, List<PartitionPositionBounds> sections, StreamSession session)
    {
        CassandraStreamHeader header = header(sstable, sections);
        return sstable.compression
               ? new CassandraCompressedStreamWriter(sstable, header, session)
               : new CassandraStreamWriter(sstable, header, session);
    }

    /** Run the writer and return every byte it put on the channel. */
    public static byte[] capture(CassandraStreamWriter writer) throws IOException
    {
        return captureChannel(writer).captured();
    }

    /** As {@link #capture}, but keeps the channel so the caller can also count what was written. */
    public static CapturingChannel captureChannel(CassandraStreamWriter writer) throws IOException
    {
        CapturingChannel channel = new CapturingChannel();
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            writer.write(out);
        }
        return channel;
    }

    /**
     * What the compressed writer should send for these sections: the compressed chunks covering them, each
     * followed by its four byte CRC, straight out of the data file.
     */
    public static byte[] expectedCompressedBytes(SSTableReader sstable, List<PartitionPositionBounds> sections) throws IOException
    {
        CompressionMetadata.Chunk[] chunks = CompressionInfo.newLazyInstance(sstable.getCompressionMetadata(), sections).chunks();

        int length = 0;
        for (CompressionMetadata.Chunk chunk : chunks)
            length += chunk.length + 4;

        ByteBuffer expected = ByteBuffer.allocate(length);
        try (FileChannel data = sstable.descriptor.fileFor(Components.DATA).newReadChannel())
        {
            for (CompressionMetadata.Chunk chunk : chunks)
                read(data, expected, chunk.offset, chunk.length + 4);
        }
        return expected.array();
    }

    /** A byte range of the data file, as it sits on disk. */
    public static byte[] fileBytes(SSTableReader sstable, long start, int length) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        try (FileChannel data = sstable.descriptor.fileFor(Components.DATA).newReadChannel())
        {
            read(data, buffer, start, length);
        }
        return buffer.array();
    }

    private static void read(FileChannel channel, ByteBuffer into, long position, int length) throws IOException
    {
        int limit = into.limit();
        into.limit(into.position() + length);
        try
        {
            while (into.hasRemaining())
            {
                if (channel.read(into, position + (length - into.remaining())) < 0)
                    throw new IOException("Unexpected end of file at " + position);
            }
        }
        finally
        {
            into.limit(limit);
        }
    }

    public static StreamSession session()
    {
        StreamCoordinator coordinator = new StreamCoordinator(StreamOperation.BOOTSTRAP, 1, new NettyStreamingConnectionFactory(),
                                                              false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.BOOTSTRAP,
                                                                       Collections.<StreamEventHandler>emptyList(), coordinator);

        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        coordinator.addSessionInfo(new SessionInfo(peer, 0, peer, Collections.emptyList(), Collections.emptyList(),
                                                   StreamSession.State.INITIALIZED, null));

        StreamSession session = coordinator.getOrCreateOutboundSession(peer);
        session.init(future);
        return session;
    }

    /**
     * Keeps every byte written to it, whether the writer sent a buffer or handed the kernel a file region.
     * A region is drained here rather than by the operating system, which is the one thing a real socket does
     * differently; what the region covers is the same either way, and that is what these tests assert.
     */
    public static class CapturingChannel extends EmbeddedChannel
    {
        private final ByteBuf captured = Unpooled.buffer();
        private int messages;

        private final WritableByteChannel sink = new WritableByteChannel()
        {
            public int write(ByteBuffer src)
            {
                int remaining = src.remaining();
                captured.writeBytes(src);
                return remaining;
            }

            public boolean isOpen()
            {
                return true;
            }

            public void close()
            {
            }
        };

        public CapturingChannel()
        {
            config().setWriteBufferHighWaterMark(64 << 20); // never block the writer in a test
        }

        @Override
        protected void handleOutboundMessage(Object msg)
        {
            messages++;
            if (msg instanceof ByteBuf)
            {
                captured.writeBytes((ByteBuf) msg);
                ((ByteBuf) msg).release();
            }
            else if (msg instanceof FileRegion)
            {
                FileRegion region = (FileRegion) msg;
                try
                {
                    long transferred = 0;
                    while (transferred < region.count())
                        transferred += region.transferTo(sink, transferred);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
                region.release();
            }
            else
            {
                throw new IllegalArgumentException("Unexpected outbound message " + msg);
            }
        }

        /** One per batch the writer submitted, whichever form it took. */
        public int messageCount()
        {
            return messages;
        }

        public byte[] captured()
        {
            byte[] bytes = new byte[captured.readableBytes()];
            captured.getBytes(0, bytes);
            return bytes;
        }
    }
}
