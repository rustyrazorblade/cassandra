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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;

/**
 * Streams a compressed SSTable through {@link CassandraCompressedStreamWriter} over a real loopback socket,
 * and reports how long it takes and what it costs in CPU.
 *
 * The unit tests around this writer run on an EmbeddedChannel, which brings every byte into user space to
 * inspect it. That is the one thing a real socket does not have to do, so it is the one thing those tests
 * cannot measure. This test exists to measure it.
 *
 * It also asserts that what crossed the socket is what is on disk: the checksum taken on the receiving side is
 * compared against a checksum over the compressed chunks read straight out of the data file.
 *
 * Loopback has memory bandwidth to spare, so wall-clock throughput is the weaker of the numbers. Read the
 * process CPU time, the heap allocated, and the networking pool's hit count.
 *
 * The ant test JVM sets cassandra.debugrefcount, which allocates on its own account, so the heap figure is
 * good for comparing one commit against another and is not a production number.
 *
 * The knobs must be forwarded to the forked JVM; they are not read from the ant command line:
 *
 *   ant burn-testsome -Dtest.name=org.apache.cassandra.db.streaming.CompressedStreamWriterBurnTest
 *       -Dtest.jvm.args="-Dcassandra.test.streaming_burn_mib=512 -Dcassandra.test.streaming_burn_iterations=10"
 */
public class CompressedStreamWriterBurnTest
{
    private static final Logger logger = LoggerFactory.getLogger(CompressedStreamWriterBurnTest.class);

    private static final String KEYSPACE = "CompressedStreamWriterBurnTest";
    private static final String CF = "Compressed1";

    private static final int CRC_LENGTH = 4;
    private static final int VALUE_SIZE = 1024;
    private static final int DATA_MIB = Integer.getInteger("cassandra.test.streaming_burn_mib", 256);
    private static final int ITERATIONS = Integer.getInteger("cassandra.test.streaming_burn_iterations", 5);

    private static SSTableReader sstable;

    @BeforeClass
    public static void writeSSTable()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF)
                                                .compression(CompressionParams.lz4()));

        // throttling would set the throughput, not measure it
        DatabaseDescriptor.setStreamThroughputOutboundMebibytesPerSecAsInt(0);
        DatabaseDescriptor.setInterDCStreamThroughputOutboundMebibytesPerSecAsInt(0);
        StreamManager.StreamRateLimiter.updateThroughput();
        StreamManager.StreamRateLimiter.updateInterDCThroughput();

        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF);

        // incompressible values, so the file on disk is the size we asked for and the bytes we time crossing
        // the socket are the bytes we wrote
        Random random = new Random(0);
        byte[] value = new byte[VALUE_SIZE];
        int rows = (DATA_MIB << 20) / VALUE_SIZE;
        for (int i = 0; i < rows; i++)
        {
            random.nextBytes(value);
            new RowUpdateBuilder(store.metadata(), i, String.valueOf(i))
            .clustering("0")
            .add("val", ByteBuffer.wrap(value))
            .build()
            .applyUnsafe();

            if ((i & 0xFFFF) == 0xFFFF)
                Util.flush(store);
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store);

        sstable = store.getLiveSSTables().iterator().next();
        logger.info("Streaming {} of compressed data from {}",
                    FBUtilities.prettyPrintMemory(sstable.descriptor.fileFor(Components.DATA).length()),
                    sstable.descriptor);
    }

    @AfterClass
    public static void cleanup() throws IOException
    {
        SchemaLoader.cleanupAndLeaveDirs();
    }

    @Test
    public void streamCompressedSSTable() throws Exception
    {
        CassandraCompressedStreamWriter writer = compressedWriter();
        CompressionMetadata.Chunk[] chunks = compressionInfo().chunks();

        long expectedBytes = 0;
        for (CompressionMetadata.Chunk chunk : chunks)
            expectedBytes += chunk.length + CRC_LENGTH;

        try (Receiver receiver = new Receiver())
        {
            // once with a checksum, to prove what crossed the socket is what is on disk
            Result verified = run(receiver, expectedBytes, true, writer);
            assertEquals("the bytes on the wire must be the compressed chunks as they are on disk",
                         expectedChecksum(chunks), verified.checksum);

            // then time it without one: checksumming the received bytes costs more CPU than the transfer does
            Result fastest = null;
            Result leanest = null;
            for (int i = 0; i < ITERATIONS; i++)
            {
                Result result = run(receiver, expectedBytes, false, writer);
                fastest = fastest == null || result.millis < fastest.millis ? result : fastest;
                leanest = leanest == null || result.heapBytes < leanest.heapBytes ? result : leanest;
            }

            logger.info("streamed {} in {} ms, {} MiB/s, {} ms of process CPU",
                        FBUtilities.prettyPrintMemory(expectedBytes), fastest.millis, fastest.mibPerSecond(expectedBytes),
                        TimeUnit.NANOSECONDS.toMillis(fastest.cpuNanos));
            // min of the runs, the same way the allocation gates do it: a sampling profiler or a stray
            // background thread only ever adds
            logger.info("allocated {} on the heap, {} networking pool hits, {} misses",
                        FBUtilities.prettyPrintMemory(leanest.heapBytes), leanest.poolHits, leanest.poolMisses);
        }
    }

    private Result run(Receiver receiver, long expectedBytes, boolean checksum, CassandraCompressedStreamWriter writer) throws Exception
    {
        CompletableFuture<Result> received = receiver.expect(expectedBytes, checksum);
        Channel client = receiver.connect();

        BufferPool pool = BufferPools.forNetworking();
        long startHits = pool.metrics().hits.getCount();
        long startMisses = pool.metrics().misses.getCount();
        long startHeap = allocatedBytes();
        long startCpu = processCpuNanos();
        long start = System.nanoTime();
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(client))
        {
            writer.write(out);
        }
        Result result = received.get(10, TimeUnit.MINUTES);
        result.millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        result.cpuNanos = processCpuNanos() - startCpu;
        result.heapBytes = allocatedBytes() - startHeap;
        result.poolHits = pool.metrics().hits.getCount() - startHits;
        result.poolMisses = pool.metrics().misses.getCount() - startMisses;

        client.close().sync();
        assertEquals("receiver did not see every byte", expectedBytes, result.bytes);
        return result;
    }

    /** Whole-process CPU: the work spans the writing thread and Netty's event loops. */
    private static long processCpuNanos()
    {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
    }

    /**
     * Heap allocated by every thread, live and dead, for the same reason: the event loops allocate as much of
     * this as the writing thread does. The direct buffers the writer takes from the networking pool are not
     * heap and do not appear here; the pool's own hit and miss counters are what track those.
     */
    private static long allocatedBytes()
    {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        return ((com.sun.management.ThreadMXBean) threads).getTotalThreadAllocatedBytes();
    }

    /** The chunks as they sit in the data file, each followed by its CRC, in offset order. */
    private long expectedChecksum(CompressionMetadata.Chunk[] chunks) throws IOException
    {
        CRC32 crc = new CRC32();
        try (FileChannel channel = dataChannel())
        {
            for (CompressionMetadata.Chunk chunk : chunks)
            {
                ByteBuffer buffer = ByteBuffer.allocate(chunk.length + CRC_LENGTH);
                while (buffer.hasRemaining())
                {
                    if (channel.read(buffer, chunk.offset + buffer.position()) < 0)
                        throw new IOException("Unexpected end of " + sstable.descriptor);
                }
                buffer.flip();
                crc.update(buffer);
            }
        }
        return crc.getValue();
    }

    private static class Result
    {
        long bytes;
        long checksum;
        long millis;
        long cpuNanos;
        long heapBytes;
        long poolHits;
        long poolMisses;

        String mibPerSecond(long bytes)
        {
            return String.format("%.1f", (bytes / (1024.0 * 1024.0)) / (millis / 1000.0));
        }
    }

    /** A real TCP server on loopback that counts whatever arrives, then discards it. */
    private static class Receiver implements AutoCloseable
    {
        private final EventLoopGroup group = new NioEventLoopGroup(2);
        private final Channel server;
        private volatile CompletableFuture<Result> pending;
        private volatile long expected;
        private volatile boolean checksum;

        Receiver() throws InterruptedException
        {
            server = new ServerBootstrap().group(group)
                                          .channel(NioServerSocketChannel.class)
                                          .childHandler(new ChannelInitializer<NioSocketChannel>()
                                          {
                                              protected void initChannel(NioSocketChannel ch)
                                              {
                                                  ch.pipeline().addLast(new Counter());
                                              }
                                          })
                                          .bind(InetAddress.getLoopbackAddress(), 0)
                                          .sync()
                                          .channel();
        }

        CompletableFuture<Result> expect(long bytes, boolean checksum)
        {
            expected = bytes;
            this.checksum = checksum;
            pending = new CompletableFuture<>();
            return pending;
        }

        Channel connect() throws InterruptedException
        {
            return new Bootstrap().group(group)
                                  .channel(NioSocketChannel.class)
                                  .handler(new ChannelInboundHandlerAdapter())
                                  .connect(server.localAddress())
                                  .sync()
                                  .channel();
        }

        public void close() throws InterruptedException
        {
            server.close().sync();
            group.shutdownGracefully(0, 10, TimeUnit.SECONDS).sync();
        }

        private class Counter extends ChannelInboundHandlerAdapter
        {
            private final Result result = new Result();
            private final CRC32 crc = new CRC32();

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg)
            {
                ByteBuf buf = (ByteBuf) msg;
                try
                {
                    result.bytes += buf.readableBytes();
                    if (checksum)
                        crc.update(buf.nioBuffer());
                }
                finally
                {
                    buf.release();
                }

                if (result.bytes >= expected)
                {
                    result.checksum = crc.getValue();
                    pending.complete(result);
                }
            }
        }
    }

    private FileChannel dataChannel() throws IOException
    {
        return sstable.descriptor.fileFor(Components.DATA).newReadChannel();
    }

    private CompressionInfo compressionInfo()
    {
        List<SSTableReader.PartitionPositionBounds> sections =
            Collections.singletonList(new SSTableReader.PartitionPositionBounds(0, sstable.getCompressionMetadata().dataLength));
        return CompressionInfo.newLazyInstance(sstable.getCompressionMetadata(), sections);
    }

    private CassandraCompressedStreamWriter compressedWriter()
    {
        List<SSTableReader.PartitionPositionBounds> sections =
            Collections.singletonList(new SSTableReader.PartitionPositionBounds(0, sstable.getCompressionMetadata().dataLength));
        CassandraStreamHeader header =
            CassandraStreamHeader.builder()
                                 .withSSTableVersion(sstable.descriptor.version)
                                 .withSSTableLevel(0)
                                 .withEstimatedKeys(sstable.estimatedKeys())
                                 .withSections(sections)
                                 .withCompressionInfo(compressionInfo())
                                 .withSerializationHeader(sstable.header.toComponent())
                                 .withTableId(sstable.metadata().id)
                                 .build();

        return new CassandraCompressedStreamWriter(sstable, header, session());
    }

    private StreamSession session()
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
}
