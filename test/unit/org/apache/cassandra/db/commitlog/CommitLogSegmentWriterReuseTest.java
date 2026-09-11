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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.security.EncryptionContext;
import org.quicktheories.impl.JavaRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * The writer a thread caches over the segment buffer is reused within a segment and never written
 * through after its segment's buffer has gone.
 *
 * CommitLog.add keeps one writer per thread and re-points it at each allocation, rebuilding the
 * duplicate only when the segment changes. Two things follow that no other test asserts. The reuse has
 * to actually happen, or the cache is dead code that every other test passes without. And a recycled
 * segment's buffer is either cleaned or handed to the next segment, so a writer that failed to notice
 * the change would write into freed memory or into another segment's slot.
 *
 * The two segment families reach the hazard differently, so both are parameterized. A memory-mapped
 * segment's buffer is cleaned when it closes, so a stale duplicate would address freed memory. A
 * compressed, encrypted or direct IO segment returns its buffer to a pool, so the next segment can be
 * handed the same object and a stale duplicate would write at the wrong offset in a live segment.
 *
 * Writes run on one pinned CassandraThread, which is a FastThreadLocalThread as production's threads
 * are, so the FastThreadLocal takes the same storage it takes on a node.
 *
 * What this test cannot see: a rotation racing a write from another thread. That is
 * CommitLogConcurrencyPropertyTest.concurrentWritersAndRotationLoseNothing.
 */
@RunWith(Parameterized.class)
public class CommitLogSegmentWriterReuseTest extends SegmentParameterizedBase
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentWriterReuseTest.class);

    private static final String KEYSPACE = "commitlog_segment_writer_reuse";
    private static final int TABLES = 1;
    private static final int MUTATIONS = 16;

    private static final List<TableMetadata> TABLES_GENERATED = new ArrayList<>(TABLES);

    private ExecutorService pinned;

    public CommitLogSegmentWriterReuseTest(ParameterizedClass commitLogCompression,
                                           EncryptionContext encryptionContext,
                                           Config.DiskAccessMode diskAccessMode,
                                           Class<? extends CommitLogSegment> expectedSegmentType)
    {
        super(commitLogCompression, encryptionContext, diskAccessMode, expectedSegmentType);
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
    }

    @Before
    public void before() throws IOException
    {
        applySegmentConfiguration();

        pinned = Executors.newSingleThreadExecutor(new NamedThreadFactory("commitlog-writer-reuse"));
        CommitLog.instance.resetUnsafe(true);
    }

    @After
    public void after()
    {
        if (pinned != null)
            pinned.shutdownNow();
    }

    /** A second write to the same segment reuses the writer the first one built. */
    @Test
    public void theWriterIsReusedWithinASegment() throws Throwable
    {
        TableMetadata metadata = TABLES_GENERATED.get(0);
        JavaRandom random = new JavaRandom(0);

        Mutation first = CommitLogPropertyFixture.generateMutation(metadata, random);
        Mutation second = CommitLogPropertyFixture.generateMutation(metadata, random);

        onPinnedThread(() -> CommitLog.instance.add(first));
        long afterFirst = CommitLog.getSegmentWriterRebuilds();
        onPinnedThread(() -> CommitLog.instance.add(second));

        assertEquals("the writer was rebuilt for a second write to the same segment",
                     afterFirst, CommitLog.getSegmentWriterRebuilds());
        assertSegmentType();
    }

    /**
     * Entries written after the segments were recycled are intact. A writer still holding a duplicate of
     * a recycled buffer would put them into freed memory or into the reissued buffer at the wrong offset.
     */
    @Test
    public void writesAfterARecycleAreIntact() throws Throwable
    {
        TableMetadata metadata = TABLES_GENERATED.get(0);
        JavaRandom random = new JavaRandom(1);

        List<Mutation> discarded = generate(metadata, random);
        onPinnedThread(() -> {
            for (Mutation mutation : discarded)
                CommitLog.instance.add(mutation);
            return null;
        });
        CommitLog.instance.sync(true);

        // Rotate away from the segment holding the first batch, then mark the table clean up to it. The
        // segment closes, so its buffer is cleaned or handed back to the pool for the next segment.
        AbstractCommitLogSegmentManager manager = CommitLog.instance.segmentManager;
        String recycled = manager.allocatingFrom().getName();
        CommitLogPosition upTo = CommitLog.instance.getCurrentPosition();
        manager.advanceAllocatingFrom(manager.allocatingFrom());
        CommitLog.instance.discardCompletedSegments(metadata.id, CommitLogPosition.NONE, upTo);

        assertFalse("the segment holding the first batch was not recycled, so no buffer was ever released",
                    CommitLog.instance.getActiveSegmentNames().contains(recycled));

        List<Mutation> kept = generate(metadata, random);
        onPinnedThread(() -> {
            for (Mutation mutation : kept)
                CommitLog.instance.add(mutation);
            return null;
        });
        CommitLog.instance.sync(true);
        assertSegmentType();

        List<ByteBuffer> expected = new ArrayList<>(kept.size());
        for (Mutation mutation : kept)
            expected.add(CommitLogPropertyFixture.bytes(mutation));
        assertEquals("what was written after the recycle did not come back, schema:\n"
                     + metadata.toCqlString(true, false, false),
                     expected, CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE));
    }

    private static List<Mutation> generate(TableMetadata metadata, JavaRandom random)
    {
        List<Mutation> mutations = new ArrayList<>(MUTATIONS);
        for (int i = 0; i < MUTATIONS; i++)
            mutations.add(CommitLogPropertyFixture.generateMutation(metadata, random));
        return mutations;
    }

    private <T> void onPinnedThread(Callable<T> work) throws Throwable
    {
        try
        {
            pinned.submit(work).get();
        }
        catch (java.util.concurrent.ExecutionException e)
        {
            throw e.getCause();
        }
    }
}
