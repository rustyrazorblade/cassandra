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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.lifecycle.StreamingLifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableTxnSingleStreamWriter;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;

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
     * Run the writer against a channel carrying an {@link SslHandler}, and keep what it submitted.
     *
     * The handler is there to select the branch, not to encrypt: the recorder sits nearer the tail, so it takes
     * each message before the handler would see it. Encrypting for real would need a completed handshake, which
     * an EmbeddedChannel with no peer on the other end cannot give us.
     *
     * What matters is the type of what the writer submits. An SslHandler cannot encrypt a FileRegion, so a
     * writer that hands one to an SSL channel fails the transfer outright.
     */
    public static SslCapture captureThroughSsl(CassandraStreamWriter writer) throws Exception
    {
        // server mode, so the handler waits for a client hello instead of starting a handshake it cannot
        // finish against an EmbeddedChannel with nothing on the other end
        SSLEngine engine = SSLContext.getDefault().createSSLEngine();
        engine.setUseClientMode(false);

        SslCapture capture = new SslCapture();
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.config().setWriteBufferHighWaterMark(64 << 20);
        channel.pipeline().addLast(new SslHandler(engine));
        channel.pipeline().addLast(capture.recorder);

        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            writer.write(out);
        }
        return capture;
    }

    /** What a writer submitted to an SSL channel: the bytes, and the type of every message. */
    public static class SslCapture
    {
        private final ByteBuf captured = Unpooled.buffer();
        private final List<Class<?>> messageTypes = new ArrayList<>();

        private final ChannelOutboundHandlerAdapter recorder = new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            {
                messageTypes.add(msg.getClass());
                if (msg instanceof ByteBuf)
                    captured.writeBytes((ByteBuf) msg);
                ReferenceCountUtil.release(msg);
                promise.setSuccess();
            }
        };

        public List<Class<?>> messageTypes()
        {
            return messageTypes;
        }

        public byte[] captured()
        {
            byte[] bytes = new byte[captured.readableBytes()];
            captured.getBytes(0, bytes);
            return bytes;
        }
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

    /**
     * The whole legacy path, writer to reader: run the writer, hand what it produced to the matching reader,
     * and return the SSTables that came out. The caller owns the returned transaction and must abort it.
     */
    public static Received roundTrip(SSTableReader sstable, List<PartitionPositionBounds> sections) throws Throwable
    {
        byte[] wire = capture(writer(sstable, sections, session()));

        StreamSession session = session();
        session.prepareReceiving(new StreamSummary(sstable.metadata().id, Collections.emptyList(), 1, wire.length));

        CassandraStreamHeader header = header(sstable, sections);
        StreamMessageHeader messageHeader = new StreamMessageHeader(sstable.metadata().id,
                                                                    FBUtilities.getBroadcastAddressAndPort(),
                                                                    session.planId(), false, 0, 0, 0, null);
        IStreamReader reader = sstable.compression
                               ? new CassandraCompressedStreamReader(messageHeader, header, session)
                               : new CassandraStreamReader(messageHeader, header, session);

        SSTableTxnSingleStreamWriter written =
            (SSTableTxnSingleStreamWriter) reader.read(new DataInputBuffer(ByteBuffer.wrap(wire), false));

        StreamingLifecycleTransaction txn = new StreamingLifecycleTransaction();
        return new Received(txn, written.transferOwnershipTo(txn));
    }

    /** The SSTables a round trip produced, and the transaction holding them. */
    public static class Received implements AutoCloseable
    {
        public final Collection<SSTableReader> sstables;
        private final StreamingLifecycleTransaction txn;

        Received(StreamingLifecycleTransaction txn, Collection<SSTableReader> sstables)
        {
            this.txn = txn;
            this.sstables = sstables;
        }

        public void close()
        {
            txn.abort();
        }
    }

    /** A content digest per partition, so two sets of SSTables can be compared without caring how they are laid out. */
    public static Map<DecoratedKey, String> digests(Collection<SSTableReader> sstables)
    {
        Map<DecoratedKey, String> digests = new LinkedHashMap<>();
        for (SSTableReader sstable : sstables)
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                digest(scanner, digests);
            }
        }
        return digests;
    }

    /** The same, for only the partitions of one SSTable that fall in the given token ranges. */
    public static Map<DecoratedKey, String> digests(SSTableReader sstable, Collection<Range<Token>> ranges)
    {
        Map<DecoratedKey, String> digests = new LinkedHashMap<>();
        try (ISSTableScanner scanner = sstable.getScanner(ranges))
        {
            digest(scanner, digests);
        }
        return digests;
    }

    private static void digest(ISSTableScanner scanner, Map<DecoratedKey, String> into)
    {
        while (scanner.hasNext())
        {
            try (UnfilteredRowIterator partition = scanner.next())
            {
                Digest digest = Digest.forValidator();
                UnfilteredRowIterators.digest(partition, digest, MessagingService.current_version);
                into.put(partition.partitionKey(), ByteBufferUtil.bytesToHex(ByteBuffer.wrap(digest.digest())));
            }
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
