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
package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.Ints;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.utils.btree.BTree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Writer-level tests for {@link BigFormatPartitionWriter}: the index-sample offsets it records, whether serialized
 * ("buffer mode") or held on-heap ("array mode"), must always agree with where the corresponding {@link IndexInfo}
 * objects actually land.
 */
public class BigFormatPartitionWriterTest extends CQLTester
{
    private static final byte[] DUMMY_8K = new byte[8 * 1024];

    private static TableMetadata metadata;
    private static ClusteringComparator comparator;
    private static Version version;

    @BeforeClass
    public static void beforeClass()
    {
        Assume.assumeTrue(BigFormat.isSelected());
        metadata = CreateTableStatement.parse("CREATE TABLE pipe.dev_null (pk bigint, ck bigint, val text, PRIMARY KEY(pk, ck))", "foo")
                                       .build();
        comparator = new ClusteringComparator(Collections.singletonList(LongType.instance));
        version = BigFormat.getInstance().getLatestVersion();
    }

    private static Clustering<?> cn(long l)
    {
        return Util.clustering(comparator, l);
    }

    private static DecoratedKey partitionKey(long l)
    {
        ByteBuffer key = LongSerializer.instance.serialize(l);
        Token token = Murmur3Partitioner.instance.getToken(key);
        return new BufferDecoratedKey(token, key);
    }

    /**
     * Wraps a single {@link BigFormatPartitionWriter} instance with explicit cache/index-block thresholds, so tests
     * never have to mutate {@code DatabaseDescriptor}.
     */
    private static final class Harness implements AutoCloseable
    {
        final IndexInfo.Serializer idxSerializer;
        final SequentialWriter writer;
        final BigFormatPartitionWriter partitionWriter;

        Harness(int cacheSizeThreshold, int indexSize) throws IOException
        {
            SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
            idxSerializer = IndexInfo.serializer(version, header);
            File f = FileUtils.createTempFile("BigFormatPartitionWriterTest-", "db");
            writer = new SequentialWriter(f, SequentialWriterOption.newBuilder().bufferSize(1024).build());
            partitionWriter = new BigFormatPartitionWriter(header, writer, version, idxSerializer, cacheSizeThreshold, indexSize);
        }

        /**
         * Writes a partition with {@code rows} rows, each carrying an 8KiB dummy cell value so the partition size
         * grows fast enough to force an index-block boundary on (or shortly after) every row.
         */
        void writePartition(DecoratedKey key, int rows) throws IOException
        {
            partitionWriter.start(key, DeletionTime.LIVE);
            for (long ck = 0; ck < rows; ck++)
                partitionWriter.addUnfiltered(buildRow(ck));
        }

        private Unfiltered buildRow(long ck)
        {
            BTree.Builder<ColumnData> builder = BTree.builder(ColumnData.comparator);
            builder.add(BufferCell.live(metadata.regularAndStaticColumns().iterator().next(), 1L, ByteBuffer.wrap(DUMMY_8K)));
            return BTreeRow.create(cn(ck), LivenessInfo.EMPTY, Row.Deletion.LIVE, builder.build());
        }

        public void close()
        {
            writer.close();
        }
    }

    /**
     * Deserializes {@code count} {@link IndexInfo} objects from {@code raw} (buffer-mode storage), recording the
     * position of each before it is read.
     */
    private int[] recordSerializedPositions(ByteBuffer raw, int count, IndexInfo.Serializer idxSerializer) throws IOException
    {
        DataInputBuffer input = new DataInputBuffer(raw, false);
        int[] recorded = new int[count];
        for (int i = 0; i < count; i++)
        {
            recorded[i] = input.buffer().position();
            idxSerializer.deserialize(input);
        }
        return recorded;
    }

    private int[] readTrailingOffsets(ByteBuffer raw, int count)
    {
        // trailing offsets follow immediately after the count serialized IndexInfo objects
        ByteBuffer buf = raw.duplicate();
        buf.position(buf.limit() - count * Integer.BYTES);
        int[] offsets = new int[count];
        for (int i = 0; i < count; i++)
            offsets[i] = buf.getInt();
        return offsets;
    }

    private void checkOffsetsMatchSerializedIndexInfoPositions(int indexSize) throws IOException
    {
        try (Harness h = new Harness(0, indexSize))
        {
            h.writePartition(partitionKey(1), 300);
            h.partitionWriter.finish();

            int count = h.partitionWriter.getColumnIndexCount();
            assertTrue("expected multiple blocks, got " + count, count > 1);

            ByteBuffer raw = h.partitionWriter.buffer();
            assertNotNull("cacheSize=0 must switch to buffer mode", raw);

            int[] recorded = recordSerializedPositions(raw.duplicate(), count, h.idxSerializer);
            int[] trailing = readTrailingOffsets(raw.duplicate(), count);

            assertArrayEquals(recorded, trailing);
        }
    }

    @Test
    public void offsetsMatchSerializedIndexInfoPositionsInBufferMode1KiB() throws IOException
    {
        checkOffsetsMatchSerializedIndexInfoPositions(1024);
    }

    @Test
    public void offsetsMatchSerializedIndexInfoPositionsInBufferMode64KiB() throws IOException
    {
        checkOffsetsMatchSerializedIndexInfoPositions(65536);
    }

    private void checkOffsetsMatchCumulativeSampleSizesInArrayMode(int indexSize) throws IOException
    {
        try (Harness h = new Harness(1 << 26, indexSize))
        {
            h.writePartition(partitionKey(2), 300);
            h.partitionWriter.finish();

            int count = h.partitionWriter.getColumnIndexCount();
            assertTrue("expected multiple blocks, got " + count, count > 1);
            assertNull("cache threshold large enough to never switch to buffer mode", h.partitionWriter.buffer());

            List<IndexInfo> samples = h.partitionWriter.indexSamples();
            assertNotNull(samples);
            assertEquals(count, samples.size());

            int[] expected = new int[count];
            for (int i = 1; i < count; i++)
                expected[i] = expected[i - 1] + Ints.checkedCast(h.idxSerializer.serializedSize(samples.get(i - 1)));

            assertArrayEquals(expected, h.partitionWriter.offsets());
        }
    }

    @Test
    public void offsetsMatchCumulativeSampleSizesInArrayMode1KiB() throws IOException
    {
        checkOffsetsMatchCumulativeSampleSizesInArrayMode(1024);
    }

    @Test
    public void offsetsMatchCumulativeSampleSizesInArrayMode64KiB() throws IOException
    {
        checkOffsetsMatchCumulativeSampleSizesInArrayMode(65536);
    }

    @Test
    public void writerReusedAcrossPartitions() throws IOException
    {
        try (Harness h = new Harness(0, 4096))
        {
            // Partition A: 200 blocks, same assertion as offsetsMatchSerializedIndexInfoPositionsInBufferMode.
            h.writePartition(partitionKey(1), 200);
            h.partitionWriter.finish();
            int countA = h.partitionWriter.getColumnIndexCount();
            assertTrue("expected at least 200 blocks, got " + countA, countA >= 200);
            ByteBuffer rawA = h.partitionWriter.buffer();
            assertNotNull(rawA);
            assertArrayEquals(recordSerializedPositions(rawA.duplicate(), countA, h.idxSerializer),
                              readTrailingOffsets(rawA.duplicate(), countA));

            h.partitionWriter.reset();

            // Partition B: exactly 3 blocks (each row's dummy write alone exceeds the 4096 threshold), offsets
            // freshly computed from scratch, not carried over from partition A.
            h.writePartition(partitionKey(2), 3);
            h.partitionWriter.finish();
            int countB = h.partitionWriter.getColumnIndexCount();
            assertEquals(3, countB);
            ByteBuffer rawB = h.partitionWriter.buffer();
            assertNotNull(rawB);

            IndexInfo[] infosB = new IndexInfo[countB];
            DataInputBuffer inputB = new DataInputBuffer(rawB.duplicate(), false);
            for (int i = 0; i < countB; i++)
                infosB[i] = h.idxSerializer.deserialize(inputB);

            int s0 = Ints.checkedCast(h.idxSerializer.serializedSize(infosB[0]));
            int s1 = Ints.checkedCast(h.idxSerializer.serializedSize(infosB[1]));
            int[] expectedB = new int[]{ 0, s0, s0 + s1 };
            assertArrayEquals(expectedB, readTrailingOffsets(rawB.duplicate(), countB));

            h.partitionWriter.reset();

            // Partition C: start()+finish() only, no rows -- must be a non-indexed entry, not just an empty array.
            h.partitionWriter.start(partitionKey(3), DeletionTime.LIVE);
            h.partitionWriter.finish();
            assertEquals(0, h.partitionWriter.getColumnIndexCount());

            RowIndexEntry rie = RowIndexEntry.create(0L, 0L, DeletionTime.LIVE,
                                                      h.partitionWriter.getHeaderLength(), h.partitionWriter.getColumnIndexCount(),
                                                      h.partitionWriter.indexInfoSerializedSize(),
                                                      h.partitionWriter.indexSamples(), h.partitionWriter.offsets(),
                                                      h.idxSerializer, version);
            assertEquals(0, rie.blockCount());
        }
    }
}
