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
package org.apache.cassandra.test.microbench.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Measures the allocation and time cost of building an on-disk incremental trie over sorted keys.  The change under test
 * removes the repeated key decode in {@link org.apache.cassandra.io.tries.IncrementalTrieWriterBase#add}, so the metric
 * to watch is {@code gc.alloc.rate.norm} (bytes per operation) under {@code -prof gc}.
 * <p>
 * This bench exercises the trie writer directly, one layer below {@code PartitionIndexBuilder}.  It reflects the trie
 * base change for every caller of the writer: the BTI partition index, the BTI row index, and SAI's terms dictionary.
 * <p>
 * The single-run norm also includes trie-node allocation, which this change does not touch, so read the before/after
 * <em>delta</em> as the decode saving, not one run's absolute number.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G" })
@Threads(1)
@State(Scope.Thread)
public class IncrementalTrieWriteBench
{
    @Param({ "1000", "100000", "1000000" })
    int keyCount;

    @Param({ "16" })
    int keyLength;

    // FIXED wraps raw bytes in a cheap fixedLength source; DECORATED feeds real DecoratedKey values, whose
    // asComparableBytes builds a multi-component token+key source.  The trie base change removes one such decode of
    // the previous key per add, so DECORATED shows the saving that FIXED cannot.
    @Param({ "FIXED", "DECORATED" })
    String keyType;

    private ByteComparable[] keys;

    @Setup(Level.Trial)
    public void setup()
    {
        if (keyType.equals("DECORATED"))
            setupDecorated();
        else
            setupFixed();
    }

    private void setupFixed()
    {
        Random random = new Random(1);
        byte[][] raw = new byte[keyCount][];
        for (int i = 0; i < keyCount; i++)
        {
            byte[] b = new byte[keyLength];
            random.nextBytes(b);
            raw[i] = b;
        }
        // fixedLength byte-comparable order equals unsigned lexicographic order of the raw bytes, so sort the raw
        // arrays and then wrap.  The writer requires strictly sorted, unique keys.
        Arrays.sort(raw, (a, b) -> Arrays.compareUnsigned(a, b));
        keys = new ByteComparable[keyCount];
        for (int i = 0; i < keyCount; i++)
            keys[i] = ByteComparable.fixedLength(raw[i]);
    }

    private void setupDecorated()
    {
        DatabaseDescriptor.daemonInitialization();
        Murmur3Partitioner partitioner = Murmur3Partitioner.instance;
        Random random = new Random(1);
        DecoratedKey[] dks = new DecoratedKey[keyCount];
        for (int i = 0; i < keyCount; i++)
        {
            ByteBuffer bb = ByteBufferUtil.bytes(new UUID(random.nextLong(), random.nextLong()));
            dks[i] = partitioner.decorateKey(bb);
        }
        Arrays.sort(dks);
        keys = dks;   // DecoratedKey is a ByteComparable with a token+key source
    }

    @Benchmark
    public long buildTrie() throws IOException
    {
        try (DataOutputBuffer out = new PagedBuffer();
             IncrementalTrieWriter<Integer> writer = IncrementalTrieWriter.open(serializer, out))
        {
            for (int i = 0; i < keys.length; i++)
                writer.add(keys[i], i & 0xF);
            return writer.complete();
        }
    }

    // A four-bit payload stored fully in the trie node header, mirroring the trie unit tests.
    private static final TrieSerializer<Integer, DataOutputPlus> serializer = new TrieSerializer<Integer, DataOutputPlus>()
    {
        @Override
        public int sizeofNode(SerializationNode<Integer> node, long nodePosition)
        {
            return TrieNode.typeFor(node, nodePosition).sizeofNode(node);
        }

        @Override
        public void write(DataOutputPlus dataOutput, SerializationNode<Integer> node, long nodePosition) throws IOException
        {
            TrieNode.typeFor(node, nodePosition).serialize(dataOutput, node, node.payload() != null ? node.payload() : 0, nodePosition);
        }
    };

    // In-memory buffer with paging parameters, so the writer does the same page layout as production.
    private static class PagedBuffer extends DataOutputBuffer
    {
        @Override
        public int maxBytesInPage()
        {
            return PageAware.PAGE_SIZE;
        }

        @Override
        public void padToPageBoundary() throws IOException
        {
            PageAware.pad(this);
        }

        @Override
        public int bytesLeftInPage()
        {
            long position = position();
            long bytesLeft = PageAware.pageLimit(position) - position;
            return (int) bytesLeft;
        }

        @Override
        public long paddedPosition()
        {
            return PageAware.padded(position());
        }
    }
}
