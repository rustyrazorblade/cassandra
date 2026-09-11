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
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.db.streaming.StreamingTestFixture.capture;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.dataFileCount;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.receive;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.session;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.wholeFile;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.writeToFailingChannel;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.writer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * What the legacy streaming path does when it goes wrong.
 *
 * Two obligations. A writer whose network gives out has to throw, because the caller aborts the transfer on
 * that exception; a silent failure leaves a session that never finishes. A reader handed truncated or corrupt
 * bytes has to fail and take its half-written SSTable with it. A partial SSTable left on disk looks complete.
 */
public class StreamFailureTest
{
    public static final String KEYSPACE = "StreamFailureTest";
    public static final String CF_COMPRESSED = "Compressed1";
    public static final String CF_UNCOMPRESSED = "Uncompressed1";

    private static SSTableReader compressed;
    private static SSTableReader uncompressed;
    private static ColumnFamilyStore compressedStore;
    private static ColumnFamilyStore uncompressedStore;

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
        compressedStore = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        uncompressedStore = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_UNCOMPRESSED);
        compressed = write(compressedStore);
        uncompressed = write(uncompressedStore);
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
    public void compressedWriterReportsAFailedWrite()
    {
        assertReportsFailedWrite(compressed);
    }

    @Test
    public void uncompressedWriterReportsAFailedWrite()
    {
        assertReportsFailedWrite(uncompressed);
    }

    @Test
    public void truncatedCompressedStreamFailsAndLeavesNothingBehind() throws Throwable
    {
        assertReceivingFails(compressed, compressedStore, truncate(capture(writer(compressed, wholeFile(compressed), session()))));
    }

    @Test
    public void truncatedUncompressedStreamFailsAndLeavesNothingBehind() throws Throwable
    {
        assertReceivingFails(uncompressed, uncompressedStore, truncate(capture(writer(uncompressed, wholeFile(uncompressed), session()))));
    }

    @Test
    public void corruptedCompressedStreamFailsAndLeavesNothingBehind() throws Throwable
    {
        byte[] wire = capture(writer(compressed, wholeFile(compressed), session()));

        wire[wire.length / 2] ^= 0x7F;

        assertReceivingFails(compressed, compressedStore, wire);
    }

    private void assertReportsFailedWrite(SSTableReader sstable)
    {
        IOException failure = new IOException("the network gave out");

        try
        {
            writeToFailingChannel(writer(sstable, wholeFile(sstable), session()), failure);
            fail("a writer whose channel fails every write must not return normally");
        }
        catch (Throwable thrown)
        {
            assertTrue("the failure the channel reported should be what reaches the caller, but got " + thrown,
                       causes(thrown).contains(failure));
        }
    }

    /**
     * A reader given bytes it cannot make sense of must throw, and must not leave its half-written SSTable on
     * disk.
     */
    private void assertReceivingFails(SSTableReader sstable, ColumnFamilyStore store, byte[] wire) throws Throwable
    {
        List<PartitionPositionBounds> sections = wholeFile(sstable);
        int before = dataFileCount(store);

        try
        {
            receive(sstable, sections, wire).close();
            fail("receiving a damaged stream must not succeed");
        }
        catch (AssertionError e)
        {
            throw e;
        }
        catch (Throwable expected)
        {
            // any failure will do
        }

        assertEquals("a failed transfer must not leave a data file behind", before, dataFileCount(store));
    }

    private static byte[] truncate(byte[] wire)
    {
        return Arrays.copyOf(wire, wire.length / 2);
    }

    private static List<Throwable> causes(Throwable thrown)
    {
        List<Throwable> causes = new java.util.ArrayList<>();
        for (Throwable t = thrown; t != null && !causes.contains(t); t = t.getCause())
            causes.add(t);
        return causes;
    }
}
