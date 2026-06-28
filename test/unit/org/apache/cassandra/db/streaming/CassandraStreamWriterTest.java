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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.RateLimiter;

import io.netty.channel.Channel;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.net.TestChannel;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraStreamWriterTest
{
    public static final String KEYSPACE = "CassandraStreamWriterTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_COMPRESSED = "Compressed1";

    private static SSTableReader sstable;
    private static SSTableReader compressedSstable;

    @BeforeClass
    public static void defineSchemaAndPrepareSSTable()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    // no compression so the legacy (uncompressed) CassandraStreamWriter
                                    // streams the data file directly and sections are plain byte ranges
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD)
                                                .compression(CompressionParams.noCompression()),
                                    // compressed so the common CassandraCompressedStreamWriter path is exercised
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_COMPRESSED)
                                                .compression(CompressionParams.lz4()));

        CompactionManager.instance.disableAutoCompaction();
        sstable = writeSStable(CF_STANDARD, ByteBufferUtil.EMPTY_BYTE_BUFFER, 1000);

        // high-entropy values so the compressed data file is comfortably larger than the chunk sizes under test
        Random random = new Random(0);
        byte[] value = new byte[512];
        random.nextBytes(value);
        compressedSstable = writeSStable(CF_COMPRESSED, ByteBuffer.wrap(value), 4000);
    }

    private static SSTableReader writeSStable(String cf, ByteBuffer value, int rows)
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(cf);
        for (int j = 0; j < rows; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", value)
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store);
        return store.getLiveSSTables().iterator().next();
    }

    /**
     * The legacy streaming writer must flush once after writing all sections, not once per section.
     * A per-section flush drains the channel at every section boundary, which on a high-latency link
     * empties the send pipe and costs a full round-trip per section.
     */
    @Test
    public void testFlushesOncePerFileRegardlessOfSectionCount() throws IOException
    {
        assertFlushedOnce(buildSections(1));
        assertFlushedOnce(buildSections(2));
        assertFlushedOnce(buildSections(4));
    }

    private void assertFlushedOnce(List<SSTableReader.PartitionPositionBounds> sections) throws IOException
    {
        StreamSession session = setupStreamingSessionForTest();
        CassandraStreamHeader header =
            CassandraStreamHeader.builder()
                                 .withSSTableVersion(sstable.descriptor.version)
                                 .withSSTableLevel(0)
                                 .withEstimatedKeys(sstable.estimatedKeys())
                                 .withSections(sections)
                                 .withCompressionInfo(null)
                                 .withSerializationHeader(sstable.header.toComponent())
                                 .withTableId(sstable.metadata().id)
                                 .build();

        CountingOutputPlus out = new CountingOutputPlus(new TestChannel(Integer.MAX_VALUE));
        try
        {
            new CassandraStreamWriter(sstable, header, session).write(out);

            assertEquals("legacy writer should flush exactly once regardless of the section count (was "
                         + sections.size() + " sections)", 1, out.flushCount);
            assertTrue("data should have been streamed to the channel", out.flushedToNetwork() > 0);
        }
        finally
        {
            out.discard();
        }
    }

    /**
     * The compressed legacy writer must slice each section into stream_chunk_size network writes, so a
     * smaller configured chunk size produces strictly more writes for the same SSTable.
     */
    @Test
    public void testCompressedWriterHonorsConfiguredChunkSize() throws IOException
    {
        int original = DatabaseDescriptor.getStreamChunkSizeInBytes();
        try
        {
            long compressedLength = compressedSstable.getCompressionMetadata().compressedFileLength;

            DatabaseDescriptor.setStreamChunkSizeInBytes((int) compressedLength + (1 << 20)); // one write per section
            int writesWithLargeChunk = countCompressedWrites();

            DatabaseDescriptor.setStreamChunkSizeInBytes(4 << 10); // 4 KiB, many writes
            int writesWithSmallChunk = countCompressedWrites();

            assertTrue("a smaller chunk size must produce more network writes (large=" + writesWithLargeChunk
                       + ", small=" + writesWithSmallChunk + ")", writesWithSmallChunk > writesWithLargeChunk);
        }
        finally
        {
            DatabaseDescriptor.setStreamChunkSizeInBytes(original);
        }
    }

    private int countCompressedWrites() throws IOException
    {
        StreamSession session = setupStreamingSessionForTest();
        List<SSTableReader.PartitionPositionBounds> sections =
            Collections.singletonList(new SSTableReader.PartitionPositionBounds(0, compressedSstable.getCompressionMetadata().dataLength));
        CassandraStreamHeader header =
            CassandraStreamHeader.builder()
                                 .withSSTableVersion(compressedSstable.descriptor.version)
                                 .withSSTableLevel(0)
                                 .withEstimatedKeys(compressedSstable.estimatedKeys())
                                 .withSections(sections)
                                 .withCompressionInfo(CompressionInfo.newLazyInstance(compressedSstable.getCompressionMetadata(), sections))
                                 .withSerializationHeader(compressedSstable.header.toComponent())
                                 .withTableId(compressedSstable.metadata().id)
                                 .build();

        CountingOutputPlus out = new CountingOutputPlus(new TestChannel(Integer.MAX_VALUE));
        try
        {
            new CassandraCompressedStreamWriter(compressedSstable, header, session).write(out);
            return out.writeToChannelCount;
        }
        finally
        {
            out.discard();
        }
    }

    /** Split the whole data file contiguously into {@code count} equal byte-range sections. */
    private List<SSTableReader.PartitionPositionBounds> buildSections(int count)
    {
        long dataLength = sstable.getDataChannel().size();
        List<SSTableReader.PartitionPositionBounds> sections = new ArrayList<>(count);
        long step = dataLength / count;
        long pos = 0;
        for (int i = 0; i < count; i++)
        {
            long end = (i == count - 1) ? dataLength : pos + step;
            sections.add(new SSTableReader.PartitionPositionBounds(pos, end));
            pos = end;
        }
        return sections;
    }

    private static class CountingOutputPlus extends AsyncStreamingOutputPlus
    {
        int flushCount;
        int writeToChannelCount;

        CountingOutputPlus(Channel channel)
        {
            super(channel);
        }

        @Override
        public void flush() throws IOException
        {
            flushCount++;
            super.flush();
        }

        @Override
        public int writeToChannel(StreamingDataOutputPlus.Write write, RateLimiter limiter) throws IOException
        {
            writeToChannelCount++;
            return super.writeToChannel(write, limiter);
        }
    }

    private StreamSession setupStreamingSessionForTest()
    {
        StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.BOOTSTRAP, 1, new NettyStreamingConnectionFactory(), false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.BOOTSTRAP, Collections.<StreamEventHandler>emptyList(), streamCoordinator);

        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        streamCoordinator.addSessionInfo(new SessionInfo(peer, 0, peer, Collections.emptyList(), Collections.emptyList(), StreamSession.State.INITIALIZED, null));

        StreamSession session = streamCoordinator.getOrCreateOutboundSession(peer);
        session.init(future);
        return session;
    }
}
