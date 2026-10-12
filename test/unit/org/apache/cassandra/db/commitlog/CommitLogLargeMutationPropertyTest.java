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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.quicktheories.impl.JavaRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.security.EncryptionContext;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_IGNORE_REPLAY_ERRORS;
import static org.apache.cassandra.db.commitlog.CommitLogSegment.ENTRY_OVERHEAD_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Mutations at and above the sizes where the write path changes behaviour.
 *
 * Two thresholds matter. Above {@code cassandra.cacheable_mutation_size_limit_bytes} a mutation stops
 * caching its serialized bytes, so its size and its serialized form are computed independently; that is
 * the only path where the two can disagree, so the commit log sizes its slot from the bytes written. At
 * {@code max_mutation_size} the entry is as large as the segment will take.
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

    public CommitLogLargeMutationPropertyTest(ParameterizedClass commitLogCompression,
                                              EncryptionContext encryptionContext,
                                              Config.DiskAccessMode diskAccessMode,
                                              Class<? extends CommitLogSegment> expectedSegmentType)
    {
        this.expectedSegmentType = expectedSegmentType;
        // Pin the disk access mode per parameterization. If the mode came from the ambient yaml instead,
        // a config that sets commitlog_disk_access_mode: auto (cassandra_latest.yaml) would resolve to
        // direct IO and the memory-mapped parameterization would run the wrong segment type.
        CommitLogPropertyFixture.applySegmentConfiguration(commitLogCompression, encryptionContext, diskAccessMode);
    }

    @Parameters(name = "{3}")
    public static Collection<Object[]> generateData() throws Exception
    {
        return CommitLogPropertyFixture.segmentParameterizations();
    }

    @BeforeClass
    public static void beforeClass()
    {
        TABLES_GENERATED.addAll(CommitLogPropertyFixture.prepareKeyspace(logger, KEYSPACE, TABLES));
        logger.info("examples={}, max_mutation_size={}", EXAMPLES, DatabaseDescriptor.getMaxMutationSize());
    }

    @Before
    public void before() throws IOException
    {
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
     * A mutation past the serialization cache limit that under-states its size is still written whole and
     * comes back on replay, without the replay-error override.
     *
     * Such a mutation computes its size apart from its bytes, so the commit log must size its slot from the
     * bytes it serializes, not from the size it reports. A sync after each write puts its entry in a section
     * of its own, before the last section, where a damaged entry would stop replay.
     */
    @Test
    public void understatedSizeOverTheCacheLimitRoundTrips() throws Throwable
    {
        assertFalse("this test must replay without the replay-error override", COMMITLOG_IGNORE_REPLAY_ERRORS.getBoolean());
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            CommitLog.instance.resetUnsafe(true);

            JavaRandom random = new JavaRandom(seed);
            TableMetadata metadata = TABLES_GENERATED.get(new Random(seed).nextInt(TABLES));
            int version = MessagingService.current_version;

            Mutation before = CommitLogPropertyFixture.generateMutation(metadata, random);
            CommitLog.instance.add(before);
            CommitLog.instance.sync(true);

            Mutation understated = new UnderstatedSizeMutation(
                CommitLogPropertyFixture.mutationOfExactSize(metadata, random, OVER_CACHE_LIMIT, version).getPartitionUpdate(metadata));
            assertFalse("this mutation has a cached serialized form, so it does not exercise the path under test",
                        Mutation.serializer.hasCachedSerialization(understated, version));
            CommitLog.instance.add(understated);
            CommitLog.instance.sync(true);

            Mutation after = CommitLogPropertyFixture.generateMutation(metadata, random);
            CommitLog.instance.add(after);
            CommitLog.instance.sync(true);

            assertEquals("this parameterization did not run the segment type it names",
                         expectedSegmentType, CommitLog.instance.segmentManager.allocatingFrom().getClass());

            List<ByteBuffer> replayed = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE);
            assertEquals("replay did not return all three entries, schema:\n"
                         + metadata.toCqlString(true, false, false),
                         Arrays.asList(CommitLogPropertyFixture.bytes(before),
                                       CommitLogPropertyFixture.bytes(understated),
                                       CommitLogPropertyFixture.bytes(after)),
                         replayed);
        });
    }

    /**
     * A mutation with a cached serialized form is serialized straight into its slot, so the commit log
     * checks it wrote exactly the size it reported. A mutation that reports the wrong size must fail its
     * write rather than leave an entry whose length does not match its bytes.
     */
    @Test
    public void cachedMutationWithWrongSizeFailsItsWrite() throws Throwable
    {
        TableMetadata metadata = TABLES_GENERATED.get(0);
        JavaRandom random = new JavaRandom(1);
        int version = MessagingService.current_version;
        for (Mutation wrong : Arrays.asList(new UnderstatedSizeMutation(CommitLogPropertyFixture.generateMutation(metadata, random).getPartitionUpdate(metadata)),
                                            new OverstatedSizeMutation(CommitLogPropertyFixture.generateMutation(metadata, random).getPartitionUpdate(metadata))))
        {
            assertTrue("this mutation has no cached serialized form, so it does not exercise the check under test",
                       Mutation.serializer.hasCachedSerialization(wrong, version));
            try
            {
                CommitLog.instance.add(wrong);
                fail("a " + wrong.getClass().getSimpleName() + " was accepted");
            }
            catch (IllegalStateException expected)
            {
                assertTrue(expected.getMessage(), expected.getMessage().contains("serialized to"));
            }
        }
        CommitLog.instance.resetUnsafe(true);
    }

    /** Claims eight bytes fewer than it serializes. */
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

    /** Claims eight bytes more than it serializes. */
    private static class OverstatedSizeMutation extends Mutation
    {
        OverstatedSizeMutation(PartitionUpdate update)
        {
            super(update);
        }

        @Override
        public int serializedSize(int version)
        {
            return super.serializedSize(version) + 8;
        }
    }
}
