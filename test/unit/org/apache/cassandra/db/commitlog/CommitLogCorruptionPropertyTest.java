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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler.CommitLogReadException;
import org.apache.cassandra.db.commitlog.CommitLogReplayer.CommitLogReplayException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.quicktheories.impl.JavaRandom;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_IGNORE_REPLAY_ERRORS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A damaged commit log never hands the replayer a mutation nobody wrote.
 *
 * Replay is allowed two outcomes on a corrupt segment. It may refuse, by raising the declared read or
 * replay exception. Or it may carry on, in which case every mutation it produces must be one that was
 * actually written. What it must never do is reconstruct something plausible out of damaged bytes and
 * hand it over as real, because that is silent data invention on a restart.
 *
 * The guidance in TESTING-ADVANCED.md prefers this shape to a coverage-guided fuzzer here: a generated
 * bit flip in a real serialized artefact, asserting the reader either completes or throws the declared
 * corruption exception, reaches the same class of defect with the machinery the tree already carries.
 *
 * The flip lands anywhere in the file, header included. Damaging the header usually costs the whole
 * segment, which is a legal outcome; the property is about what comes back, not how much.
 *
 * What this test cannot see: a flip that lands inside a value and still passes both checksums. The
 * checksum is what stands between the log and that, so a test asserting it cannot happen would only be
 * asserting CRC32 works.
 */
public class CommitLogCorruptionPropertyTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogCorruptionPropertyTest.class);

    private static final String KEYSPACE = "commitlog_corruption_property";
    private static final int EXAMPLES = CassandraRelevantProperties.TEST_COMMITLOG_EXAMPLES.getInt();
    private static final int MUTATIONS = CassandraRelevantProperties.TEST_COMMITLOG_MUTATIONS_PER_EXAMPLE.getInt();

    private static final int TABLES = 4;
    private static final List<TableMetadata> TABLES_GENERATED = new ArrayList<>(TABLES);

    private boolean previousIgnoreReplayErrors;

    @BeforeClass
    public static void beforeClass()
    {
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        SchemaLoader.prepareServer();

        long schemaSeed = CassandraRelevantProperties.TEST_COMMITLOG_SEED.getLong(System.currentTimeMillis());
        logger.info("schema seed={}, examples={}, mutations per example={}", schemaSeed, EXAMPLES, MUTATIONS);
        JavaRandom random = new JavaRandom(schemaSeed);
        for (int i = 0; i < TABLES; i++)
            TABLES_GENERATED.add(CommitLogPropertyFixture.generateTable(KEYSPACE, random, i));

        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    TABLES_GENERATED.toArray(new TableMetadata[0]));
    }

    @Before
    public void before() throws IOException
    {
        // let replay carry on past a damaged entry, so the test observes what it produces rather than
        // stopping at the first refusal. Refusing is a legal outcome and is asserted for separately.
        previousIgnoreReplayErrors = COMMITLOG_IGNORE_REPLAY_ERRORS.getBoolean();
        COMMITLOG_IGNORE_REPLAY_ERRORS.setBoolean(true);
        CommitLog.instance.resetUnsafe(true);
    }

    @After
    public void after()
    {
        COMMITLOG_IGNORE_REPLAY_ERRORS.setBoolean(previousIgnoreReplayErrors);
    }

    @Test
    public void replayNeverInventsAMutation() throws Throwable
    {
        // the property is satisfied trivially by a replay that always refuses, so count what actually
        // happened and assert both outcomes were reached
        int[] refused = { 0 };
        int[] completed = { 0 };
        long[] recovered = { 0 };

        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            Random workload = new Random(seed);
            JavaRandom random = new JavaRandom(seed);
            TableMetadata metadata = TABLES_GENERATED.get(workload.nextInt(TABLES));

            CommitLog.instance.resetUnsafe(true);
            Set<ByteBuffer> written = new HashSet<>();
            for (int i = 0; i < MUTATIONS; i++)
            {
                Mutation mutation = CommitLogPropertyFixture.generateMutation(metadata, random);
                CommitLog.instance.add(mutation);
                written.add(CommitLogPropertyFixture.bytes(mutation));
            }
            CommitLog.instance.sync(true);

            File[] copies = CommitLogPropertyFixture.copyActiveSegments(scratch(seed));
            File damaged = flipOneBit(workload, copies);

            List<ByteBuffer> replayed;
            try
            {
                replayed = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE, copies);
            }
            catch (CommitLogReplayException | CommitLogReadException e)
            {
                refused[0]++;
                return; // refusing to read a damaged segment is a legal outcome
            }
            completed[0]++;
            recovered[0] += replayed.size();

            for (ByteBuffer mutation : replayed)
                assertTrue("replay produced a mutation that was never written, after damaging "
                           + damaged.name() + ", schema:\n" + metadata.toCqlString(true, false, false),
                           written.contains(mutation));
        });

        logger.info("corruption property: {} examples completed replay, {} refused, {} mutations recovered",
                    completed[0], refused[0], recovered[0]);
        assertTrue("every example refused to replay; the comparison never ran", completed[0] > 0);
        assertTrue("no mutation survived any example; the comparison never ran", recovered[0] > 0);
    }

    /**
     * A segment truncated at a generated offset replays a prefix of what was written, never a different
     * list and never an undeclared exception.
     *
     * Truncation is what a machine losing power leaves behind, and it is the shape the reader's
     * end-of-section handling exists for. The uniform bit flip above reaches those branches only by
     * chance; this reaches them every example.
     */
    @Test
    public void truncatedSegmentReplaysAPrefix() throws Throwable
    {
        int[] shortened = { 0 };

        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            Random workload = new Random(seed);
            JavaRandom random = new JavaRandom(seed);
            TableMetadata metadata = TABLES_GENERATED.get(workload.nextInt(TABLES));

            CommitLog.instance.resetUnsafe(true);
            List<ByteBuffer> written = new ArrayList<>(MUTATIONS);
            for (int i = 0; i < MUTATIONS; i++)
            {
                Mutation mutation = CommitLogPropertyFixture.generateMutation(metadata, random);
                CommitLog.instance.add(mutation);
                written.add(CommitLogPropertyFixture.bytes(mutation));
            }
            CommitLog.instance.sync(true);

            File[] copies = CommitLogPropertyFixture.copyActiveSegments(scratch(seed));
            if (!truncateOneSegment(workload, copies))
                return;
            shortened[0]++;

            List<ByteBuffer> replayed;
            try
            {
                replayed = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE, copies);
            }
            catch (CommitLogReplayException | CommitLogReadException e)
            {
                return; // refusing to read a truncated segment is a legal outcome
            }

            assertTrue("a truncated segment replayed more entries than were written",
                       replayed.size() <= written.size());
            assertEquals("a truncated segment replayed something other than a prefix of what was written,"
                         + " schema:\n" + metadata.toCqlString(true, false, false),
                         written.subList(0, replayed.size()), replayed);
        });

        assertTrue("no example actually truncated anything", shortened[0] > 0);
    }

    /**
     * Damage confined to the front of a segment: the descriptor header and the first sync marker.
     *
     * Those are the branches that decide whether a file is readable at all, and a flip drawn uniformly
     * over a multi-megabyte segment lands there about never. Sixty-four bytes covers the header and the
     * marker after it without needing to compute where the marker sits.
     */
    @Test
    public void damageToTheSegmentHeaderIsHandled() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            Random workload = new Random(seed);
            JavaRandom random = new JavaRandom(seed);
            TableMetadata metadata = TABLES_GENERATED.get(workload.nextInt(TABLES));

            CommitLog.instance.resetUnsafe(true);
            Set<ByteBuffer> written = new HashSet<>();
            for (int i = 0; i < MUTATIONS; i++)
            {
                Mutation mutation = CommitLogPropertyFixture.generateMutation(metadata, random);
                CommitLog.instance.add(mutation);
                written.add(CommitLogPropertyFixture.bytes(mutation));
            }
            CommitLog.instance.sync(true);

            File[] copies = CommitLogPropertyFixture.copyActiveSegments(scratch(seed));
            flipOneBitWithin(workload, 64, copies);

            List<ByteBuffer> replayed;
            try
            {
                replayed = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE, copies);
            }
            catch (CommitLogReplayException | CommitLogReadException e)
            {
                return;
            }
            for (ByteBuffer mutation : replayed)
                assertTrue("replay produced a mutation that was never written after damaging the header",
                           written.contains(mutation));
        });
    }

    /** Truncates one of the copies at a generated offset. Returns false if there was nothing to cut. */
    private static boolean truncateOneSegment(Random workload, File[] copies) throws IOException
    {
        File target = pick(workload, copies);
        try (RandomAccessFile raf = new RandomAccessFile(target.toJavaIOFile(), "rw"))
        {
            long length = raf.length();
            if (length < 2)
                return false;
            raf.setLength(1 + Math.floorMod(workload.nextLong(), length - 1));
        }
        return true;
    }

    /** Flips one bit within the first {@code limit} bytes of one of the copies. */
    private static void flipOneBitWithin(Random workload, int limit, File[] copies) throws IOException
    {
        File target = pick(workload, copies);
        try (RandomAccessFile raf = new RandomAccessFile(target.toJavaIOFile(), "rw"))
        {
            long offset = Math.floorMod(workload.nextLong(), Math.min(limit, raf.length()));
            raf.seek(offset);
            int before = raf.read();
            raf.seek(offset);
            raf.write(before ^ (1 << workload.nextInt(8)));
        }
    }

    private static File pick(Random workload, File[] copies)
    {
        if (copies.length == 0)
            fail("no segment files to damage");
        return copies[workload.nextInt(copies.length)];
    }

    /** A fresh directory per example, so damage never carries from one to the next. */
    private static File scratch(long seed)
    {
        return new File(CommitLog.instance.segmentManager.storageDirectory, "damaged-" + seed);
    }

    /** Flips one bit at a generated offset in one of the copies, and returns the file. */
    private static File flipOneBit(Random workload, File[] copies) throws IOException
    {
        File target = pick(workload, copies);
        try (RandomAccessFile raf = new RandomAccessFile(target.toJavaIOFile(), "rw"))
        {
            long length = raf.length();
            assertTrue("segment file is empty", length > 0);
            long offset = Math.floorMod(workload.nextLong(), length);
            raf.seek(offset);
            int before = raf.read();
            raf.seek(offset);
            raf.write(before ^ (1 << workload.nextInt(8)));
        }
        return target;
    }
}
