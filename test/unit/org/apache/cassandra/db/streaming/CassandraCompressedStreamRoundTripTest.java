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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.db.streaming.StreamingTestFixture.digests;
import static org.apache.cassandra.db.streaming.StreamingTestFixture.roundTrip;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The compressed half of the legacy streaming path, end to end: what
 * {@link CassandraCompressedStreamWriter} sends, {@link CassandraCompressedStreamReader} must turn back into
 * the partitions it started from. These tests fail if either side stops agreeing with the other, whatever
 * the writer does internally to get the bytes across.
 *
 * Partitions are compared by content digest, so the assertion is about the data and not about how the
 * receiving SSTable happens to be laid out.
 */
public class CassandraCompressedStreamRoundTripTest
{
    public static final String KEYSPACE = "CassandraCompressedStreamRoundTripTest";
    public static final String CF_SIMPLE = "Simple";
    public static final String CF_COMPLEX = "Complex";

    private static final int CHUNK_LENGTH = 4096;

    private static SSTableReader simple;
    private static SSTableReader complex;
    private static ColumnFamilyStore simpleStore;

    @BeforeClass
    public static void defineSchemaAndWriteSSTables()
    {
        SchemaLoader.prepareServer();

        TableMetadata.Builder complexTable =
            TableMetadata.builder(KEYSPACE, CF_COMPLEX)
                         .addPartitionKeyColumn("key", AsciiType.instance)
                         .addClusteringColumn("ck", AsciiType.instance)
                         .addStaticColumn("s", BytesType.instance)
                         .addRegularColumn("val", BytesType.instance)
                         .compression(CompressionParams.lz4(CHUNK_LENGTH));

        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_SIMPLE)
                                                .compression(CompressionParams.lz4(CHUNK_LENGTH)),
                                    complexTable);

        CompactionManager.instance.disableAutoCompaction();

        simpleStore = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_SIMPLE);
        simple = writeSimple(simpleStore);
        complex = writeComplex(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPLEX));
    }

    /** Many small partitions, incompressible, so the data file spans a good number of chunks. */
    private static SSTableReader writeSimple(ColumnFamilyStore store)
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

    /** Wide partitions with statics and range tombstones, so a partition spans several chunks on its own. */
    private static SSTableReader writeComplex(ColumnFamilyStore store)
    {
        Random random = new Random(1);
        byte[] value = new byte[512];
        for (int p = 0; p < 20; p++)
        {
            String key = "partition" + p;

            new RowUpdateBuilder(store.metadata(), 0, key).add("s", ByteBuffer.wrap(new byte[]{ (byte) p })).build().applyUnsafe();

            for (int c = 0; c < 100; c++)
            {
                random.nextBytes(value);
                new RowUpdateBuilder(store.metadata(), 1, key)
                .clustering(String.format("%04d", c))
                .add("val", ByteBuffer.wrap(value))
                .build()
                .applyUnsafe();
            }

            // a range tombstone covering part of the partition, so the stream carries markers as well as rows
            new RowUpdateBuilder(store.metadata(), 2, key)
            .addRangeTombstone(String.format("%04d", 10), String.format("%04d", 20))
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store);
        return store.getLiveSSTables().iterator().next();
    }

    @Test
    public void wholeSSTable() throws Throwable
    {
        assertRoundTrips(simple, allRanges(simple));
    }

    @Test
    public void oneSubRange() throws Throwable
    {
        List<Range<Token>> ranges = Collections.singletonList(new Range<>(simple.getFirst().getToken(),
                                                                          tokenAtIndex(simple, 500)));

        Map<DecoratedKey, String> expected = digests(simple, ranges);
        assertFalse("the sub-range should contain some partitions", expected.isEmpty());
        assertTrue("the sub-range should not be the whole SSTable", expected.size() < 2000);

        assertRoundTrips(simple, ranges);
    }

    @Test
    public void severalDisjointSubRanges() throws Throwable
    {
        List<Range<Token>> ranges = Range.normalize(Arrays.asList(new Range<>(simple.getFirst().getToken(), tokenAtIndex(simple, 200)),
                                                                  new Range<>(tokenAtIndex(simple, 800), tokenAtIndex(simple, 1200))));

        Map<DecoratedKey, String> expected = digests(simple, ranges);
        assertFalse("the sub-ranges should contain some partitions", expected.isEmpty());

        assertRoundTrips(simple, ranges);
    }

    @Test
    public void widePartitionsWithStaticsAndRangeTombstones() throws Throwable
    {
        assertRoundTrips(complex, allRanges(complex));
    }

    @Test
    public void subRangeOfWidePartitions() throws Throwable
    {
        List<Range<Token>> ranges = Collections.singletonList(new Range<>(complex.getFirst().getToken(),
                                                                          tokenAtIndex(complex, 10)));

        assertFalse("the sub-range should contain some partitions", digests(complex, ranges).isEmpty());

        assertRoundTrips(complex, ranges);
    }

    private void assertRoundTrips(SSTableReader sstable, List<Range<Token>> ranges) throws Throwable
    {
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(ranges);
        Map<DecoratedKey, String> expected = digests(sstable, ranges);

        try (StreamingTestFixture.Received received = roundTrip(sstable, sections))
        {
            Map<DecoratedKey, String> actual = digests(received.sstables);

            assertEquals("the receiver should reconstruct every partition in the streamed ranges, and no others",
                         expected.keySet(), actual.keySet());
            assertEquals("every reconstructed partition should have the content it was sent with",
                         expected, actual);
        }
    }

    private static List<Range<Token>> allRanges(SSTableReader sstable)
    {
        return Collections.singletonList(new Range<>(sstable.getPartitioner().getMinimumToken(),
                                                     sstable.getPartitioner().getMinimumToken()));
    }

    private static Token tokenAtIndex(SSTableReader sstable, int index)
    {
        int i = 0;
        try (org.apache.cassandra.io.sstable.ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (org.apache.cassandra.db.rows.UnfilteredRowIterator partition = scanner.next())
                {
                    if (i++ == index)
                        return partition.partitionKey().getToken();
                }
            }
        }
        throw new IllegalArgumentException("SSTable has fewer than " + index + " partitions");
    }
}
