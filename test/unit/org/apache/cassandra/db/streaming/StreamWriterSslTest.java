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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;

import io.netty.buffer.ByteBuf;

import static org.apache.cassandra.db.streaming.StreamingTestFixture.capture;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.captureThroughSsl;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.session;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.wholeFile;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.writer;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Streaming over an encrypted connection.
 *
 * An {@link io.netty.handler.ssl.SslHandler} encrypts in user space and can only encrypt a buffer. It fails
 * the write outright for anything else, a file region included. So a writer on an SSL channel has one
 * obligation beyond sending the right bytes: everything it submits has to be a buffer.
 *
 * Both writers are reachable over an encrypted connection, so both are covered here.
 */
public class StreamWriterSslTest
{
    public static final String KEYSPACE = "StreamWriterSslTest";
    public static final String CF_COMPRESSED = "Compressed1";
    public static final String CF_UNCOMPRESSED = "Uncompressed1";

    private static SSTableReader compressed;
    private static SSTableReader uncompressed;

    @BeforeClass
    public static void defineSchemaAndWriteSSTables()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_COMPRESSED)
                                                .compression(CompressionParams.lz4(4096)),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_UNCOMPRESSED)
                                                .compression(CompressionParams.noCompression()));

        CompactionManager.instance.disableAutoCompaction();
        compressed = write(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED));
        uncompressed = write(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_UNCOMPRESSED));
    }

    private static SSTableReader write(ColumnFamilyStore store)
    {
        Random random = new Random(0);
        byte[] value = new byte[256];
        for (int i = 0; i < 2000; i++)
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
        return store.getLiveSSTables().iterator().next();
    }

    @Test
    public void compressedWriterSubmitsOnlyBuffersOverSsl() throws Exception
    {
        assertSubmitsOnlyBuffers(compressed);
    }

    @Test
    public void uncompressedWriterSubmitsOnlyBuffersOverSsl() throws Exception
    {
        assertSubmitsOnlyBuffers(uncompressed);
    }

    @Test
    public void compressedWriterSendsTheSameBytesOverSsl() throws Exception
    {
        assertSameBytesEitherWay(compressed);
    }

    /**
     * The compressed writer reads the file itself only when encrypting, so encryption is the one case
     * stream_disk_access_mode touches. Reading with O_DIRECT must not change a byte of what goes out.
     */
    @Test
    public void compressedWriterSendsTheSameBytesOverSslWithDirectIo() throws Exception
    {
        Assume.assumeTrue("this volume does not support direct IO",
                          FileUtils.isDirectIOSupported(compressed.descriptor.fileFor(Components.DATA)));

        DiskAccessMode original = DatabaseDescriptor.getStreamDiskAccessMode();
        try
        {
            List<PartitionPositionBounds> sections = wholeFile(compressed);

            DatabaseDescriptor.setStreamDiskAccessMode(DiskAccessMode.standard);
            byte[] throughTheCache = captureThroughSsl(writer(compressed, sections, session())).captured();

            DatabaseDescriptor.setStreamDiskAccessMode(DiskAccessMode.direct);
            StreamingTestFixture.SslCapture direct = captureThroughSsl(writer(compressed, sections, session()));

            assertArrayEquals("reading with O_DIRECT must not change the bytes", throughTheCache, direct.captured());
            for (Class<?> type : direct.messageTypes())
            {
                assertTrue("direct IO must not change what is submitted either, but got a " + type.getName(),
                           ByteBuf.class.isAssignableFrom(type));
            }
        }
        finally
        {
            DatabaseDescriptor.setStreamDiskAccessMode(original);
        }
    }

    @Test
    public void uncompressedWriterSendsTheSameBytesOverSsl() throws Exception
    {
        assertSameBytesEitherWay(uncompressed);
    }

    private void assertSubmitsOnlyBuffers(SSTableReader sstable) throws Exception
    {
        List<PartitionPositionBounds> sections = wholeFile(sstable);

        List<Class<?>> submitted = captureThroughSsl(writer(sstable, sections, session())).messageTypes();

        assertFalse("the writer should have submitted something", submitted.isEmpty());
        for (Class<?> type : submitted)
        {
            assertTrue("an SSL channel can only be handed buffers, but the writer submitted a " + type.getName(),
                       ByteBuf.class.isAssignableFrom(type));
        }
    }

    private void assertSameBytesEitherWay(SSTableReader sstable) throws Exception
    {
        List<PartitionPositionBounds> sections = wholeFile(sstable);

        assertArrayEquals("encryption is the channel's business, not the writer's: the bytes handed to the "
                          + "pipeline must be the same either way",
                          capture(writer(sstable, sections, session())),
                          captureThroughSsl(writer(sstable, sections, session())).captured());
    }
}
