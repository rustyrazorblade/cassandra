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
import java.util.function.LongConsumer;

import com.google.common.util.concurrent.RateLimiter;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.net.TestChannel;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.StreamingDataOutputPlus.Section;
import org.apache.cassandra.streaming.StreamingFileSource;
import org.apache.cassandra.utils.ByteBufferUtil;

import io.netty.channel.Channel;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;

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
                                    // no compression, so CassandraStreamWriter runs and sections are plain
                                    // byte ranges into the data file
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD)
                                                .compression(CompressionParams.noCompression()),
                                    // compressed, so CassandraCompressedStreamWriter runs
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_COMPRESSED)
                                                .compression(CompressionParams.lz4()));

        CompactionManager.instance.disableAutoCompaction();
        sstable = writeSStable(CF_STANDARD, ByteBufferUtil.EMPTY_BYTE_BUFFER, 1000);

        // 4000 rows carrying a 512-byte value, so the compressed data file is larger than the chunk sizes under test
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
     * A flush per section drains the channel at every section boundary. On a high-latency link each section
     * boundary then costs a full round trip.
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
        StreamSession session = StreamingTestFixture.session();
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
     * The compressed writer sends the on-disk chunks unchanged, so every batch must leave a plain channel as
     * a zero-copy file region. The regions must cover the fused sections with no gap and no overlap, and no
     * batch may exceed the configured chunk size.
     */
    @Test
    public void testCompressedWriterStreamsSectionsZeroCopy() throws IOException
    {
        int original = DatabaseDescriptor.getStreamChunkSizeInBytes();
        try
        {
            int chunkSize = 4 << 10;
            DatabaseDescriptor.setStreamChunkSizeInBytes(chunkSize);

            EmbeddedChannel channel = new EmbeddedChannel();
            CassandraCompressedStreamWriter writer = compressedWriter();
            CountingOutputPlus out = new CountingOutputPlus(channel);
            try
            {
                writer.write(out);
            }
            finally
            {
                out.discard();
            }

            List<long[]> regions = new ArrayList<>();
            Object msg;
            while ((msg = channel.readOutbound()) != null)
            {
                assertTrue("expected a zero-copy file region, got " + msg, msg instanceof FileRegion);
                FileRegion region = (FileRegion) msg;
                regions.add(new long[]{ region.position(), region.count() });
            }

            assertEquals("every batch should have produced one file region", out.batchCount, regions.size());
            assertCovers(writer.fuseAdjacentChunks(compressionInfo().chunks()), regions, chunkSize);
        }
        finally
        {
            DatabaseDescriptor.setStreamChunkSizeInBytes(original);
        }
    }

    /** Assert the regions walk each section start to end, in order, in batches of at most {@code maxBatch}. */
    private void assertCovers(List<Section> sections, List<long[]> regions, int maxBatch)
    {
        int i = 0;
        for (Section section : sections)
        {
            long position = section.start;
            while (position < section.end)
            {
                assertTrue("ran out of file regions while covering section [" + section.start + ", " + section.end + ')',
                           i < regions.size());
                long[] region = regions.get(i++);
                assertEquals("file region must start where the previous one ended", position, region[0]);
                assertTrue("file region of " + region[1] + " exceeds the configured chunk size", region[1] <= maxBatch);
                position += region[1];
            }
            assertEquals("file regions must end exactly on the section boundary", section.end, position);
        }
        assertEquals("no file regions may be sent beyond the sections", regions.size(), i);
    }

    private CompressionInfo compressionInfo()
    {
        List<SSTableReader.PartitionPositionBounds> sections =
            Collections.singletonList(new SSTableReader.PartitionPositionBounds(0, compressedSstable.getCompressionMetadata().dataLength));
        return CompressionInfo.newLazyInstance(compressedSstable.getCompressionMetadata(), sections);
    }

    private CassandraCompressedStreamWriter compressedWriter()
    {
        List<SSTableReader.PartitionPositionBounds> sections =
            Collections.singletonList(new SSTableReader.PartitionPositionBounds(0, compressedSstable.getCompressionMetadata().dataLength));
        CassandraStreamHeader header =
            CassandraStreamHeader.builder()
                                 .withSSTableVersion(compressedSstable.descriptor.version)
                                 .withSSTableLevel(0)
                                 .withEstimatedKeys(compressedSstable.estimatedKeys())
                                 .withSections(sections)
                                 .withCompressionInfo(compressionInfo())
                                 .withSerializationHeader(compressedSstable.header.toComponent())
                                 .withTableId(compressedSstable.metadata().id)
                                 .build();

        return new CassandraCompressedStreamWriter(compressedSstable, header, StreamingTestFixture.session());
    }

    /** Split the whole data file into {@code count} contiguous byte-range sections of near-equal length. */
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
        int batchCount;

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

        @Override
        public int writeToChannel(ByteBuffer buffer, RateLimiter limiter) throws IOException
        {
            writeToChannelCount++;
            return super.writeToChannel(buffer, limiter);
        }

        @Override
        public long writeFileToChannel(StreamingFileSource source, RateLimiter limiter, List<Section> sections, LongConsumer progress, ExecutorPlus readAhead) throws IOException
        {
            LongConsumer counting = bytes -> {
                batchCount++;
                progress.accept(bytes);
            };
            return super.writeFileToChannel(source, limiter, sections, counting, readAhead);
        }
    }

}
