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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.quicktheories.impl.JavaRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Concurrent writers lose nothing, duplicate nothing, and never produce a half-written entry.
 *
 * This is the largest hole in the commit log suite. One test in the whole package drives add from more
 * than one thread, and it is the one that fails on this machine. Everything the write path does to stay
 * correct under concurrency is therefore unexercised: the compare-and-set loop in
 * CommitLogSegment.allocate, the OpOrder barrier that keeps sync from reading a slot a writer is still
 * filling, and the segment switch that happens while other threads are mid-allocation.
 *
 * Mutations are compared as a multiset rather than a list. Interleaved writers land in whatever order the
 * allocator gives them, so order across threads carries no information; count does. A duplicated entry
 * and a lost entry both show up as a count that does not match, which is the failure worth catching.
 */
public class CommitLogConcurrencyPropertyTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogConcurrencyPropertyTest.class);

    private static final String KEYSPACE = "commitlog_concurrency_property";

    /**
     * Each example spawns threads and writes a few hundred entries, so it costs far more than a
     * single-threaded one. A twentieth of the shared example count keeps the class in the same runtime
     * bracket as the rest of the suite.
     */
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
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        SchemaLoader.prepareServer();

        long schemaSeed = CassandraRelevantProperties.TEST_COMMITLOG_SEED.getLong(System.currentTimeMillis());
        logger.info("schema seed={}, examples={}, threads={}, mutations per thread={}",
                    schemaSeed, EXAMPLES, THREADS, PER_THREAD);
        JavaRandom random = new JavaRandom(schemaSeed);
        for (int i = 0; i < TABLES; i++)
            TABLES_GENERATED.add(CommitLogPropertyFixture.generateTable(KEYSPACE, random, i));

        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    TABLES_GENERATED.toArray(new TableMetadata[0]));
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
        new CommitLogSeedRunner(EXAMPLES).run(seed -> runConcurrent(seed, false));
    }

    /**
     * The same, with a thread syncing throughout. A sync that reads a slot while its writer is still
     * filling it would write a truncated entry, and the entry would come back missing or damaged.
     */
    @Test
    public void concurrentWritersAndSyncLoseNothing() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> runConcurrent(seed, true));
    }

    private static void runConcurrent(long seed, boolean syncThroughout) throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);

        Random workload = new Random(seed);
        TableMetadata metadata = TABLES_GENERATED.get(workload.nextInt(TABLES));

        // built before the threads start, so the measured window holds nothing but add and so the
        // expected multiset is known exactly
        List<List<Mutation>> perThread = new ArrayList<>(THREADS);
        Map<ByteBuffer, Integer> expected = new HashMap<>();
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

        List<Throwable> failures = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);
        List<Thread> threads = new ArrayList<>(THREADS + 1);

        for (int t = 0; t < THREADS; t++)
        {
            List<Mutation> batch = perThread.get(t);
            Thread thread = new Thread(() -> {
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
            }, "commitlog-writer-" + t);
            threads.add(thread);
            thread.start();
        }

        AtomicBoolean syncing = new AtomicBoolean(syncThroughout);
        if (syncThroughout)
        {
            Thread syncer = new Thread(() -> {
                try
                {
                    start.await();
                    while (syncing.get())
                        CommitLog.instance.sync(true);
                }
                catch (Throwable e)
                {
                    failures.add(e);
                }
            }, "commitlog-syncer");
            threads.add(syncer);
            syncer.start();
        }

        start.countDown();
        assertTrue("writers did not finish within a minute", done.await(1, TimeUnit.MINUTES));
        syncing.set(false);
        for (Thread thread : threads)
            thread.join(TimeUnit.MINUTES.toMillis(1));

        if (!failures.isEmpty())
            throw failures.get(0);

        CommitLog.instance.sync(true);

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
}
