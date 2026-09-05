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
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.schema.TableMetadata;
import org.quicktheories.impl.JavaRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Relations that must hold over the commit log whatever the bytes look like. Each test compares two ways
 * of reaching the same result, so none of them needs a model of what the log should contain.
 */
public class CommitLogMetamorphicPropertyTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogMetamorphicPropertyTest.class);

    private static final String KEYSPACE = "commitlog_metamorphic_property";
    private static final int EXAMPLES = CassandraRelevantProperties.TEST_COMMITLOG_EXAMPLES.getInt();
    private static final int MUTATIONS = CassandraRelevantProperties.TEST_COMMITLOG_MUTATIONS_PER_EXAMPLE.getInt();

    private static final int TABLES = 4;
    private static final List<TableMetadata> TABLES_GENERATED = new ArrayList<>(TABLES);

    @BeforeClass
    public static void beforeClass()
    {
        TABLES_GENERATED.addAll(CommitLogPropertyFixture.prepareKeyspace(logger, KEYSPACE, TABLES));
        logger.info("examples={}, mutations per example={}", EXAMPLES, MUTATIONS);
    }

    @Before
    public void before() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    /**
     * The same mutations replay identically whether or not a segment boundary falls in the middle of
     * them. Forcing the switch is the point: a batch small enough to fit in one segment never exercises
     * the allocation path that changes segments.
     */
    @Test
    public void segmentBoundaryDoesNotChangeWhatReplays() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            TableMetadata metadata = pickTable(seed);
            List<Mutation> mutations = generate(metadata, seed);

            List<ByteBuffer> inOneSegment = writeAndReplay(metadata, mutations, -1);
            int unsplitSegments = CommitLog.instance.getActiveSegmentNames().size();
            int splitAt = 1 + new Random(seed).nextInt(Math.max(1, mutations.size() - 1));
            List<ByteBuffer> acrossSegments = writeAndReplay(metadata, mutations, splitAt);
            int splitSegments = CommitLog.instance.getActiveSegmentNames().size();

            assertTrue("the split example did not actually change segments, split at " + splitAt
                       + ", segments " + unsplitSegments + " then " + splitSegments,
                       splitSegments > unsplitSegments);
            assertEquals("a segment boundary changed what replayed, split at " + splitAt + ", schema:\n"
                         + metadata.toCqlString(true, false, false),
                         inOneSegment, acrossSegments);
        });
    }

    /** Replay does not consume what it reads: running it twice returns the same mutations. */
    @Test
    public void replayIsIdempotent() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            TableMetadata metadata = pickTable(seed);
            List<Mutation> mutations = generate(metadata, seed);

            CommitLog.instance.resetUnsafe(true);
            for (Mutation m : mutations)
                CommitLog.instance.add(m);
            CommitLog.instance.sync(true);

            List<ByteBuffer> first = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE);
            List<ByteBuffer> second = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE);
            assertEquals("replaying twice returned different results", first, second);
        });
    }

    /**
     * Replaying from a recorded position returns a suffix of replaying from the beginning. The filter is
     * allowed to drop a prefix and nothing else.
     */
    @Test
    public void replayFromPositionIsASuffix() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            TableMetadata metadata = pickTable(seed);
            List<Mutation> mutations = generate(metadata, seed);

            CommitLog.instance.resetUnsafe(true);
            int cutAfter = 1 + new Random(seed).nextInt(Math.max(1, mutations.size() - 1));
            CommitLogPosition cut = null;
            for (int i = 0; i < mutations.size(); i++)
            {
                CommitLogPosition position = CommitLog.instance.add(mutations.get(i));
                if (i == cutAfter - 1)
                    cut = position;
            }
            CommitLog.instance.sync(true);

            List<ByteBuffer> all = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE);
            List<ByteBuffer> fromCut = CommitLogPropertyFixture.replay(metadata, cut);

            assertTrue("filtered replay returned more than the unfiltered one",
                       fromCut.size() <= all.size());
            assertEquals("filtered replay is not a suffix of the unfiltered replay, cut after " + cutAfter,
                         all.subList(all.size() - fromCut.size(), all.size()), fromCut);
        });
    }

    private static TableMetadata pickTable(long seed)
    {
        return TABLES_GENERATED.get(new Random(seed).nextInt(TABLES));
    }

    private static List<Mutation> generate(TableMetadata metadata, long seed)
    {
        JavaRandom random = new JavaRandom(seed);
        List<Mutation> mutations = new ArrayList<>(MUTATIONS);
        for (int i = 0; i < MUTATIONS; i++)
            mutations.add(CommitLogPropertyFixture.generateMutation(metadata, random));
        return mutations;
    }

    /**
     * Writes the mutations and replays them. {@code switchAfter} forces a new segment after that many
     * mutations; -1 leaves the batch in whatever segments it naturally falls into.
     */
    private static List<ByteBuffer> writeAndReplay(TableMetadata metadata, List<Mutation> mutations, int switchAfter)
    throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        for (int i = 0; i < mutations.size(); i++)
        {
            if (switchAfter >= 0 && i == switchAfter)
                CommitLog.instance.segmentManager.advanceAllocatingFrom(CommitLog.instance.segmentManager.allocatingFrom());
            CommitLog.instance.add(mutations.get(i));
        }
        CommitLog.instance.sync(true);
        return CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE);
    }
}
