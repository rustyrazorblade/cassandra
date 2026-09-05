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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Concurrent writers lose nothing, duplicate nothing, and never produce a half-written entry.
 *
 * Driving add from many threads at once exercises:
 * <ul>
 * <li>the compare-and-set loop in CommitLogSegment.allocate;</li>
 * <li>the OpOrder barrier that keeps sync from reading a slot a writer is still filling;</li>
 * <li>the segment switch that happens while other threads are mid-allocation.</li>
 * </ul>
 *
 * The test compares mutations as a multiset, not a list. Interleaved writers land in whatever order the
 * allocator gives them, so order across threads carries no information; a count that does not match
 * catches both a lost entry and a duplicated one.
 */
public class CommitLogConcurrencyPropertyTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogConcurrencyPropertyTest.class);

    private static final String KEYSPACE = "commitlog_concurrency_property";

    /** A twentieth of the shared count keeps this class in the same runtime bracket as the rest of the suite. */
    private static final int EXAMPLES =
        Math.max(1, CassandraRelevantProperties.TEST_COMMITLOG_EXAMPLES.getInt() / 20);
    private static final int PER_THREAD =
        CassandraRelevantProperties.TEST_COMMITLOG_MUTATIONS_PER_EXAMPLE.getInt();
    private static final int THREADS = 8;

    private static final int TABLES = 4;
    private static final List<TableMetadata> TABLES_GENERATED = new ArrayList<>(TABLES);

    @BeforeClass
    public static void beforeClass()
    {
        TABLES_GENERATED.addAll(CommitLogPropertyFixture.prepareKeyspace(logger, KEYSPACE, TABLES));
        logger.info("examples={}, threads={}, mutations per thread={}", EXAMPLES, THREADS, PER_THREAD);
    }

    @Before
    public void before() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    /** Every entry written by every thread comes back exactly once. */
    @Test
    public void concurrentWritersLoseNothing() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> runConcurrent(seed, false, false));
    }

    /**
     * Every entry comes back exactly once while another thread syncs throughout. A sync that reads a slot
     * a writer is still filling would write a truncated entry. That entry comes back missing or damaged.
     */
    @Test
    public void concurrentWritersAndSyncLoseNothing() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> runConcurrent(seed, true, false));
    }

    /**
     * Every entry comes back exactly once while another thread rotates the segment throughout. Rotation is
     * where a writer's cached duplicate of the segment buffer is rebuilt, and the writers have to cross
     * that boundary mid-batch for the rebuild to be exercised under concurrency at all.
     */
    @Test
    public void concurrentWritersAndRotationLoseNothing() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> runConcurrent(seed, true, true));
    }

    private static void runConcurrent(long seed, boolean syncThroughout, boolean rotateThroughout) throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);

        Random workload = new Random(seed);
        TableMetadata metadata = TABLES_GENERATED.get(workload.nextInt(TABLES));

        Map<ByteBuffer, Integer> expected = new HashMap<>();
        List<List<Mutation>> perThread = buildBatches(seed, metadata, expected);

        List<Throwable> failures = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicInteger rotations = new AtomicInteger();
        List<Thread> threads = new ArrayList<>(THREADS + 2);

        for (int t = 0; t < THREADS; t++)
            threads.add(writer(perThread.get(t), t, start, done, failures));
        if (syncThroughout)
            threads.add(background("commitlog-syncer", start, writing, failures,
                                   () -> CommitLog.instance.sync(true)));
        if (rotateThroughout)
            threads.add(background("commitlog-rotator", start, writing, failures, () -> {
                AbstractCommitLogSegmentManager manager = CommitLog.instance.segmentManager;
                manager.advanceAllocatingFrom(manager.allocatingFrom());
                rotations.incrementAndGet();
            }));
        for (Thread thread : threads)
            thread.start();

        start.countDown();
        assertTrue("writers did not finish within a minute", done.await(1, TimeUnit.MINUTES));
        writing.set(false);
        for (Thread thread : threads)
            thread.join(TimeUnit.MINUTES.toMillis(1));

        if (!failures.isEmpty())
            throw failures.get(0);

        CommitLog.instance.sync(true);

        if (rotateThroughout)
        {
            assertTrue("the rotation thread never advanced the segment", rotations.get() > 0);
            assertTrue("rotating left the writers in a single segment",
                       CommitLog.instance.getActiveSegmentNames().size() > 1);
        }

        assertReplayedMatches(metadata, expected);
    }

    private static List<List<Mutation>> buildBatches(long seed, TableMetadata metadata,
                                                     Map<ByteBuffer, Integer> expected)
    {
        // Built before the threads start, so the threads run nothing but add and the expected multiset is
        // known exactly.
        List<List<Mutation>> perThread = new ArrayList<>(THREADS);
        for (int t = 0; t < THREADS; t++)
        {
            JavaRandom random = new JavaRandom(seed + t);
            List<Mutation> batch = new ArrayList<>(PER_THREAD);
            for (int i = 0; i < PER_THREAD; i++)
            {
                Mutation mutation = CommitLogPropertyFixture.generateMutation(metadata, random);
                batch.add(mutation);
                expected.merge(CommitLogPropertyFixture.bytes(mutation), 1, Integer::sum);
            }
            perThread.add(batch);
        }
        return perThread;
    }

    private static Thread writer(List<Mutation> batch, int index, CountDownLatch start, CountDownLatch done,
                                 List<Throwable> failures)
    {
        return new Thread(() -> {
            try
            {
                start.await();
                for (Mutation mutation : batch)
                    CommitLog.instance.add(mutation);
            }
            catch (Throwable e)
            {
                failures.add(e);
            }
            finally
            {
                done.countDown();
            }
        }, "commitlog-writer-" + index);
    }

    /**
     * Repeats {@code action} from the moment the writers start until they have all finished. It runs at
     * least once, so a test can assert the action happened however fast the writers were.
     */
    private static Thread background(String name, CountDownLatch start, AtomicBoolean writing,
                                     List<Throwable> failures, ThrowingRunnable action)
    {
        return new Thread(() -> {
            try
            {
                start.await();
                do
                {
                    action.run();
                }
                while (writing.get());
            }
            catch (Throwable e)
            {
                failures.add(e);
            }
        }, name);
    }

    private static void assertReplayedMatches(TableMetadata metadata, Map<ByteBuffer, Integer> expected)
    throws IOException
    {
        Map<ByteBuffer, Integer> replayed = new HashMap<>();
        for (ByteBuffer mutation : CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE))
            replayed.merge(mutation, 1, Integer::sum);

        assertEquals("the commit log returned a different number of entries than were written, schema:\n"
                     + metadata.toCqlString(true, false, false),
                     THREADS * PER_THREAD, replayed.values().stream().mapToInt(Integer::intValue).sum());
        assertEquals("the multiset of entries that came back differs from the one written, schema:\n"
                     + metadata.toCqlString(true, false, false),
                     expected, replayed);
    }

    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }
}
