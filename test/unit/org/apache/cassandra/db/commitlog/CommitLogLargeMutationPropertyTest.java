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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.crypto.Cipher;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.security.CipherFactory;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.quicktheories.impl.JavaRandom;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_IGNORE_REPLAY_ERRORS;
import static org.apache.cassandra.db.commitlog.CommitLogSegment.ENTRY_OVERHEAD_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Mutations at and above the sizes where the write path changes behaviour.
 *
 * Two thresholds matter. Above {@code cassandra.cacheable_mutation_size_limit_bytes} a mutation stops
 * caching its serialized bytes, so its size and its serialized form are computed independently; that is
 * the only path where the two can disagree. At {@code max_mutation_size} the entry is as large as the
 * segment will take, which is where a slot sized from a prediction has the least room to be wrong.
 *
 * The existing suite writes a mutation at the limit and asserts nothing about it. testEqualRecordLimit
 * calls add and returns. Nothing reads one back, at any segment type.
 */
@RunWith(Parameterized.class)
public class CommitLogLargeMutationPropertyTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogLargeMutationPropertyTest.class);

    private static final String KEYSPACE = "commitlog_large_mutation_property";

    /** Each example writes megabytes, so it costs far more than a small-mutation one. */
    private static final int EXAMPLES =
        Math.max(1, CassandraRelevantProperties.TEST_COMMITLOG_EXAMPLES.getInt() / 20);

    /** Above the 1,000,000 byte default of the serialization cache limit. */
    private static final int OVER_CACHE_LIMIT = 1_200_000;

    private static final int TABLES = 2;
    private static final List<TableMetadata> TABLES_GENERATED = new ArrayList<>(TABLES);

    private final Class<? extends CommitLogSegment> expectedSegmentType;
    private boolean previousIgnoreReplayErrors;

    public CommitLogLargeMutationPropertyTest(ParameterizedClass commitLogCompression,
                                              EncryptionContext encryptionContext,
                                              Class<? extends CommitLogSegment> expectedSegmentType)
    {
        this.expectedSegmentType = expectedSegmentType;
        DatabaseDescriptor.setCommitLogCompression(commitLogCompression);
        DatabaseDescriptor.setEncryptionContext(encryptionContext);
        DatabaseDescriptor.initializeCommitLogDiskAccessMode();
    }

    @Parameters(name = "{2}")
    public static Collection<Object[]> generateData() throws Exception
    {
        return Arrays.asList(new Object[][]
                             {
                             { null, EncryptionContextGenerator.createDisabledContext(), MemoryMappedSegment.class },
                             { null, newEncryptionContext(), EncryptedSegment.class },
                             { new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap()),
                               EncryptionContextGenerator.createDisabledContext(), CompressedSegment.class },
                             { new ParameterizedClass(DeflateCompressor.class.getName(), Collections.emptyMap()),
                               EncryptionContextGenerator.createDisabledContext(), CompressedSegment.class },
                             { new ParameterizedClass(ZstdCompressor.class.getName(), Collections.emptyMap()),
                               EncryptionContextGenerator.createDisabledContext(), CompressedSegment.class }
                             });
    }

    private static EncryptionContext newEncryptionContext() throws Exception
    {
        EncryptionContext context = EncryptionContextGenerator.createContext(true);
        CipherFactory cipherFactory = new CipherFactory(context.getTransparentDataEncryptionOptions());
        Cipher cipher = cipherFactory.getEncryptor(context.getTransparentDataEncryptionOptions().cipher,
                                                   context.getTransparentDataEncryptionOptions().key_alias);
        return EncryptionContextGenerator.createContext(cipher.getIV(), true);
    }

    @BeforeClass
    public static void beforeClass()
    {
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        SchemaLoader.prepareServer();

        long schemaSeed = CassandraRelevantProperties.TEST_COMMITLOG_SEED.getLong(System.currentTimeMillis());
        logger.info("schema seed={}, examples={}, max_mutation_size={}",
                    schemaSeed, EXAMPLES, DatabaseDescriptor.getMaxMutationSize());
        JavaRandom random = new JavaRandom(schemaSeed);
        for (int i = 0; i < TABLES; i++)
            TABLES_GENERATED.add(CommitLogPropertyFixture.generateTable(KEYSPACE, random, i));

        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    TABLES_GENERATED.toArray(new TableMetadata[0]));
    }

    @Before
    public void before() throws IOException
    {
        previousIgnoreReplayErrors = COMMITLOG_IGNORE_REPLAY_ERRORS.getBoolean();
        CommitLog.instance.resetUnsafe(true);
    }

    /** A mutation past the serialization cache limit is written and read back unchanged. */
    @Test
    public void mutationOverTheCacheLimitRoundTrips() throws Throwable
    {
        roundTripAtSize(OVER_CACHE_LIMIT, true);
    }

    /** A mutation at exactly max_mutation_size is written and read back unchanged. */
    @Test
    public void mutationAtMaxSizeRoundTrips() throws Throwable
    {
        roundTripAtSize(DatabaseDescriptor.getMaxMutationSize() - ENTRY_OVERHEAD_SIZE, true);
    }

    private void roundTripAtSize(int serializedSize, boolean expectSizeOnlyPath) throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            CommitLog.instance.resetUnsafe(true);

            JavaRandom random = new JavaRandom(seed);
            TableMetadata metadata = TABLES_GENERATED.get(new Random(seed).nextInt(TABLES));
            int version = MessagingService.current_version;

            Mutation mutation = CommitLogPropertyFixture.mutationOfExactSize(metadata, random, serializedSize, version);
            assertEquals("the helper did not produce the requested size", serializedSize, mutation.serializedSize(version));
            if (expectSizeOnlyPath)
                assertTrue("this mutation is small enough to be cached, so it does not exercise the path under test",
                           serializedSize >= 1_000_000);

            CommitLog.instance.add(mutation);
            CommitLog.instance.sync(true);

            assertEquals("this parameterization did not run the segment type it names",
                         expectedSegmentType, CommitLog.instance.segmentManager.allocatingFrom().getClass());

            List<ByteBuffer> replayed = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE);
            assertEquals("a " + serializedSize + " byte mutation did not come back, schema:\n"
                         + metadata.toCqlString(true, false, false),
                         Collections.singletonList(CommitLogPropertyFixture.bytes(mutation)), replayed);
        });
    }

    /**
     * A mutation that under-states its serialized size must fail its own write and leave the entries
     * after it readable.
     *
     * The slot is sized from the prediction, so an under-statement means the serializer runs out of room.
     * The buffer limit is narrowed to the slot for exactly this, so the overflow lands at the slot
     * boundary rather than over the entry's own checksum, and the next entry starts where it should.
     */
    @Test
    public void understatedSizeFailsWithoutDamagingTheNextEntry() throws Throwable
    {
        COMMITLOG_IGNORE_REPLAY_ERRORS.setBoolean(true);
        try
        {
            new CommitLogSeedRunner(EXAMPLES).run(seed -> {
                CommitLog.instance.resetUnsafe(true);

                JavaRandom random = new JavaRandom(seed);
                TableMetadata metadata = TABLES_GENERATED.get(new Random(seed).nextInt(TABLES));

                Mutation good = CommitLogPropertyFixture.generateMutation(metadata, random);
                CommitLog.instance.add(good);

                Mutation understated = new UnderstatedSizeMutation(
                    CommitLogPropertyFixture.generateMutation(metadata, random).getPartitionUpdate(metadata));
                try
                {
                    CommitLog.instance.add(understated);
                    fail("a mutation that under-states its size was accepted");
                }
                catch (RuntimeException | Error expected)
                {
                    // the serializer runs out of slot; the shape of the throw is not what this asserts
                }

                Mutation after = CommitLogPropertyFixture.generateMutation(metadata, random);
                CommitLog.instance.add(after);
                CommitLog.instance.sync(true);

                List<ByteBuffer> replayed = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE);
                assertEquals("the entries either side of a failed write did not survive it, schema:\n"
                             + metadata.toCqlString(true, false, false),
                             Arrays.asList(CommitLogPropertyFixture.bytes(good),
                                           CommitLogPropertyFixture.bytes(after)),
                             replayed);
            });
        }
        finally
        {
            COMMITLOG_IGNORE_REPLAY_ERRORS.setBoolean(previousIgnoreReplayErrors);
        }
    }

    /** Claims eight bytes fewer than it serializes, so its slot cannot hold it. */
    private static class UnderstatedSizeMutation extends Mutation
    {
        UnderstatedSizeMutation(PartitionUpdate update)
        {
            super(update);
        }

        @Override
        public int serializedSize(int version)
        {
            return super.serializedSize(version) - 8;
        }
    }
}
