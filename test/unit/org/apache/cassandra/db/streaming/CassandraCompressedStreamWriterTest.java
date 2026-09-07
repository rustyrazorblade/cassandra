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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.db.streaming.StreamingTestFixture.capture;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.expectedCompressedBytes;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.header;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.session;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.writer;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * What the compressed writer puts on the wire, for every shape of section it can be asked for.
 *
 * The writer transforms nothing: the compressed chunks covering the requested sections go out as they sit in
 * the data file, each followed by its CRC. Every case asserts the bytes, not the mechanism that sent them.
 */
public class CassandraCompressedStreamWriterTest
{
    public static final String KEYSPACE = "CassandraCompressedStreamWriterTest";
    public static final String CF = "Compressed1";

    private static final int CHUNK_LENGTH = 4096;
    private static final int CRC_LENGTH = 4;

    private static SSTableReader sstable;

    @BeforeClass
    public static void defineSchemaAndWriteSSTable()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF)
                                                .compression(CompressionParams.lz4(CHUNK_LENGTH)));

        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF);

        // incompressible, so each compressed chunk stays near CHUNK_LENGTH and section offsets map to
        // predictable chunk counts
        Random random = new Random(0);
        byte[] value = new byte[512];
        for (int i = 0; i < 4000; i++)
        {
            random.nextBytes(value);
            new RowUpdateBuilder(store.metadata(), i, String.valueOf(i))
            .clustering("0")
            .add("val", ByteBuffer.wrap(value))
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store);
        sstable = store.getLiveSSTables().iterator().next();
    }

    @Test
    public void sectionWithinASingleChunk() throws IOException
    {
        List<PartitionPositionBounds> sections = Collections.singletonList(new PartitionPositionBounds(100, 200));

        assertEquals("the section should fall inside one chunk", 1, chunkCount(sections));

        assertSendsExactly(sections);
    }

    @Test
    public void sectionOfOneWholeChunk() throws IOException
    {
        List<PartitionPositionBounds> sections = Collections.singletonList(new PartitionPositionBounds(0, CHUNK_LENGTH));

        assertEquals("a section of exactly one chunk length should need one chunk", 1, chunkCount(sections));

        assertSendsExactly(sections);
    }

    @Test
    public void sectionSpanningManyChunks() throws IOException
    {
        List<PartitionPositionBounds> sections = StreamingTestFixture.wholeFile(sstable);

        assertTrue("the whole file should span many chunks", chunkCount(sections) > 10);

        assertSendsExactly(sections);
    }

    @Test
    public void sectionStartingMidChunk() throws IOException
    {
        long start = CHUNK_LENGTH + (CHUNK_LENGTH / 2);
        List<PartitionPositionBounds> sections = Collections.singletonList(new PartitionPositionBounds(start, start + CHUNK_LENGTH));

        assertEquals("a chunk length that straddles a boundary should need two chunks", 2, chunkCount(sections));

        // the writer sends whole chunks: the receiver seeks to the offset within the first one
        assertSendsExactly(sections);
    }

    @Test
    public void severalDisjointSections() throws IOException
    {
        long far = 40 * CHUNK_LENGTH;
        List<PartitionPositionBounds> sections = Arrays.asList(new PartitionPositionBounds(0, 1000),
                                                               new PartitionPositionBounds(far, far + 1000));

        assertEquals("two sections far apart should need one chunk each", 2, chunkCount(sections));

        assertSendsExactly(sections);
    }

    @Test
    public void adjacentSectionsAreFusedButSendTheSameBytes() throws IOException
    {
        List<PartitionPositionBounds> split = Arrays.asList(new PartitionPositionBounds(0, CHUNK_LENGTH),
                                                            new PartitionPositionBounds(CHUNK_LENGTH, 2 * CHUNK_LENGTH));
        List<PartitionPositionBounds> whole = Collections.singletonList(new PartitionPositionBounds(0, 2 * CHUNK_LENGTH));

        assertEquals("both spellings should cover the same chunks", chunkCount(whole), chunkCount(split));

        assertArrayEquals("fusing adjacent sections must not change the bytes",
                          capture(writer(sstable, whole, session())),
                          capture(writer(sstable, split, session())));
    }

    @Test
    public void noSectionsSendsNothing() throws IOException
    {
        List<PartitionPositionBounds> sections = Collections.emptyList();

        assertEquals("no sections should need no chunks", 0, chunkCount(sections));

        assertEquals("a writer with nothing to send must send nothing", 0, capture(writer(sstable, sections, session())).length);
    }

    /**
     * A section goes out in batches of at most stream_chunk_size. A setting larger than the section takes one
     * write.
     */
    @Test
    public void chunkSizeDecidesHowManyWritesASectionTakes() throws IOException
    {
        int original = DatabaseDescriptor.getStreamChunkSizeInBytes();
        int originalWindow = DatabaseDescriptor.getStreamSendWindowInBytes();
        try
        {
            List<PartitionPositionBounds> sections = StreamingTestFixture.wholeFile(sstable);
            long onTheWire = header(sstable, sections).size();

            // the window has to make room first: a chunk larger than it is refused
            DatabaseDescriptor.setStreamSendWindowInBytes((int) onTheWire + (2 << 20));
            DatabaseDescriptor.setStreamChunkSizeInBytes((int) onTheWire + (1 << 20));
            StreamingTestFixture.CapturingChannel large = StreamingTestFixture.captureChannel(writer(sstable, sections, session()));

            DatabaseDescriptor.setStreamChunkSizeInBytes(CHUNK_LENGTH);
            StreamingTestFixture.CapturingChannel small = StreamingTestFixture.captureChannel(writer(sstable, sections, session()));

            assertEquals("a chunk size past the end of the section should take one write", 1, large.messageCount());
            assertEquals("a smaller chunk size should take one write per chunk of it",
                         (int) ((onTheWire + CHUNK_LENGTH - 1) / CHUNK_LENGTH), small.messageCount());
            assertArrayEquals("the chunk size must not change the bytes", large.captured(), small.captured());
        }
        finally
        {
            DatabaseDescriptor.setStreamChunkSizeInBytes(original);
            DatabaseDescriptor.setStreamSendWindowInBytes(originalWindow);
        }
    }

    /**
     * A receiver reads until it has the byte count the header declares, so the header and the bytes sent must
     * agree.
     */
    private void assertSendsExactly(List<PartitionPositionBounds> sections) throws IOException
    {
        byte[] expected = expectedCompressedBytes(sstable, sections);
        byte[] actual = capture(writer(sstable, sections, session()));

        assertArrayEquals("the writer must send the compressed chunks as they are on disk", expected, actual);
        assertEquals("the bytes sent must match the size the header declares",
                     header(sstable, sections).size(), (long) actual.length);
    }

    private int chunkCount(List<PartitionPositionBounds> sections)
    {
        return CompressionInfo.newLazyInstance(sstable.getCompressionMetadata(), sections).chunks().length;
    }
}
