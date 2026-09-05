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

package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.JavaRandom;

import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The serialized size a mutation reports must equal the number of bytes serializing it writes.
 *
 * Every caller that sizes a buffer from the prediction and then fills it by serializing depends on this,
 * and the commit log is where a disagreement becomes a corrupt entry rather than a wasted byte.
 *
 * The property is checked at every messaging version, because the size is memoised per version and a
 * version added later can memoise a size the serializer for that version does not write.
 *
 * What this test cannot see:
 *
 * - Multi-cell collections and UDTs. RowUpdateBuilder takes a Java collection for those, not the raw
 *   buffer the type generator produces, so the generated types are frozen here. Frozen collections,
 *   tuples, UDTs and vectors are covered. The multi-cell paths reach the same serializers through the
 *   round-trip property, which drives CQL statements instead.
 * - Composite partition keys. One partition key column per generated table; see generateUpdate.
 * - Counters. Their serialized form depends on the local counter context rather than the generated value.
 * - The cached serialization path. Below {@code cassandra.cacheable_mutation_size_limit_bytes} a mutation
 *   holds its serialized bytes as a byte[] and reports that array's length as its size, so the two agree
 *   by construction and cannot disagree. Only the size-only path can, which is why
 *   {@link #largeMutationSizeMatchesBytesWritten} pads past the limit.
 */
public class MutationSerializedSizePropertyTest
{
    private static final Logger logger = LoggerFactory.getLogger(MutationSerializedSizePropertyTest.class);

    private static final String KEYSPACE = "commitlog_size_property";
    private static final int EXAMPLES = CassandraRelevantProperties.TEST_COMMITLOG_EXAMPLES.getInt();

    /** Comfortably above cassandra.cacheable_mutation_size_limit_bytes, whose default is 1,000,000. */
    private static final int PAD_BYTES = 1_200_000;

    /** Added to the generated schema so a mutation can be grown past the cache limit deterministically. */
    private static final String PAD_COLUMN = "pad_blob";

    private static final int[] VERSIONS = { MessagingService.VERSION_40,
                                            MessagingService.VERSION_50,
                                            MessagingService.VERSION_60 };

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        logger.info("examples={} (override with -D{}), seed property is {}",
                    EXAMPLES,
                    CassandraRelevantProperties.TEST_COMMITLOG_EXAMPLES.getKey(),
                    CassandraRelevantProperties.TEST_COMMITLOG_SEED.getKey());
    }

    /**
     * Every generated partition update serializes to exactly the size it predicts. This is the
     * computation the size-only serialization performs, reached with no caching layer in the way.
     */
    @Test
    public void partitionUpdateSizeMatchesBytesWritten() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            JavaRandom random = new JavaRandom(seed);
            TableMetadata metadata = generateTable(random);
            PartitionUpdate update = generateUpdate(metadata, random, new Random(seed), false);

            for (int version : VERSIONS)
            {
                long predicted = PartitionUpdate.serializer.serializedSize(update, version);
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    PartitionUpdate.serializer.serialize(update, out, version);
                    assertEquals(describe("partition update", version, metadata),
                                 predicted, out.getLength());
                }
            }
        });
    }

    /**
     * A mutation large enough to bypass the serialization cache serializes to exactly the size it
     * predicts. This is the case a commit log slot sized from the prediction depends on, and the only one
     * where the prediction and the write are computed independently of each other.
     */
    @Test
    public void largeMutationSizeMatchesBytesWritten() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            JavaRandom random = new JavaRandom(seed);
            TableMetadata metadata = generateTable(random);
            Mutation mutation = new Mutation(generateUpdate(metadata, random, new Random(seed), true));

            for (int version : VERSIONS)
            {
                // assert the case under test was actually reached: below the cache limit this property
                // holds by construction and proves nothing
                assertTrue("padded mutation did not clear the serialization cache limit, size="
                           + mutation.serializedSize(version),
                           mutation.serializedSize(version) > PAD_BYTES);

                int predicted = mutation.serializedSize(version);
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    Mutation.serializer.serialize(mutation, out, version);
                    assertEquals(describe("mutation", version, metadata), predicted, out.getLength());
                }
            }
        });
    }

    private static String describe(String what, int version, TableMetadata metadata)
    {
        return what + " size disagrees with bytes written at messaging version " + version + ", schema:\n"
               + metadata.toCqlString(true, false, false);
    }

    /**
     * A generated schema whose values RowUpdateBuilder accepts as raw buffers: single-cell types only and
     * no counters. A blob column is appended so a mutation can be grown past the cache limit without
     * generating until a large one turns up.
     */
    private static TableMetadata generateTable(JavaRandom random)
    {
        Gen<String> udtName = Generators.unique(IDENTIFIER_GEN);
        TypeGenBuilder primary = AbstractTypeGenerators.withoutUnsafeEquality()
                                                       .withUDTNames(udtName)
                                                       .withMaxDepth(1)
                                                       .withMultiCell(false);
        TableMetadata generated =
            new TableMetadataBuilder()
            .withKeyspaceName(KEYSPACE)
            .withTableKinds(TableMetadata.Kind.REGULAR)
            .withSimpleColumnNames()
            .withDefaultTypeGen(AbstractTypeGenerators.builder()
                                                      .withoutEmpty()
                                                      .withMaxDepth(2)
                                                      .withMultiCell(false)
                                                      .withDefaultSetKey(primary)
                                                      .withoutTypeKinds(AbstractTypeGenerators.TypeKind.COUNTER)
                                                      .withUDTNames(udtName))
            .withPrimaryColumnTypeGen(primary)
            .withPartitionColumnsCount(1)
            .withClusteringColumnsBetween(0, 3)
            .withRegularColumnsBetween(1, 4)
            .withStaticColumnsBetween(0, 0)
            .build(random);

        return generated.unbuild().addRegularColumn(PAD_COLUMN, BytesType.instance).build();
    }

    /** One row of generated values, optionally carrying a blob large enough to clear the cache limit. */
    private static PartitionUpdate generateUpdate(TableMetadata metadata, JavaRandom random, Random workload, boolean pad)
    {
        ByteBuffer[] row = CassandraGenerators.data(metadata, null).generate(random);

        int partitionColumns = metadata.partitionKeyColumns().size();
        int clusteringColumns = metadata.clusteringColumns().size();

        // one partition key column, so the generated buffer is the key as-is. A composite key would need
        // RowUpdateBuilder to be handed the components rather than a composed buffer; it makes no
        // difference to this property, which only asks whether the size matches the bytes written.
        ByteBuffer key = row[0];

        RowUpdateBuilder builder = new RowUpdateBuilder(metadata, 1L, key);
        if (clusteringColumns > 0)
        {
            Object[] clustering = new Object[clusteringColumns];
            System.arraycopy(row, partitionColumns, clustering, 0, clusteringColumns);
            builder = builder.clustering(clustering);
        }

        int valueIndex = partitionColumns + clusteringColumns;
        for (ColumnMetadata column : metadata.regularColumns())
        {
            // the generator produced a value for every column in the metadata, the pad column included,
            // so the cursor advances for all of them; skipping one shifts every value after it into the
            // wrong column and the type rejects it
            ByteBuffer generated = row[valueIndex++];
            ByteBuffer value = pad && column.name.toString().equals(PAD_COLUMN) ? padding(workload) : generated;
            if (value != null)
                builder = builder.add(column, value);
        }

        Mutation mutation = builder.build();
        return mutation.getPartitionUpdate(metadata);
    }

    /** Incompressible padding, so the size holds under a compressing commit log too. */
    private static ByteBuffer padding(Random workload)
    {
        byte[] bytes = new byte[PAD_BYTES];
        workload.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }
}
