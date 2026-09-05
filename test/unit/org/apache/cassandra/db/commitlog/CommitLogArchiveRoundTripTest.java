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
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.quicktheories.impl.JavaRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Everything archived comes back, byte for byte, and replays.
 *
 * Archiving is the feature a point-in-time restore rests on, and the round trip is its whole contract:
 * segments are copied out by an operator command, the originals go away, the restore command brings them
 * back, and replay returns what was written. CommitLogArchiver is the second least covered class in the
 * package at 49% of branches, and maybeRestoreArchive is its largest uncovered method.
 *
 * The archive and restore commands here are plain file copies, which is what an operator's command is in
 * substance. Using real commands rather than mocking the archiver is the point: a mock cannot show that
 * the file the archiver wrote is the file the reader can read.
 *
 * What this test cannot see: point-in-time restore, which selects a subset by timestamp, and the
 * snapshot commit log position path. Both take different branches in CommitLogReplayer.construct.
 */
public class CommitLogArchiveRoundTripTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogArchiveRoundTripTest.class);

    private static final String KEYSPACE = "commitlog_archive_round_trip";
    private static final int EXAMPLES = Math.max(1, CassandraRelevantProperties.TEST_COMMITLOG_EXAMPLES.getInt() / 20);
    private static final int MUTATIONS = CassandraRelevantProperties.TEST_COMMITLOG_MUTATIONS_PER_EXAMPLE.getInt();

    private static final int TABLES = 2;
    private static final List<TableMetadata> TABLES_GENERATED = new ArrayList<>(TABLES);

    /** One archiver for the class: each instance registers an MBean, so one per example collides. */
    private static File archive;
    private static CommitLogArchiver archiver;

    @BeforeClass
    public static void beforeClass()
    {
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        SchemaLoader.prepareServer();

        long schemaSeed = CassandraRelevantProperties.TEST_COMMITLOG_SEED.getLong(System.currentTimeMillis());
        logger.info("schema seed={}, examples={}", schemaSeed, EXAMPLES);
        JavaRandom random = new JavaRandom(schemaSeed);
        for (int i = 0; i < TABLES; i++)
            TABLES_GENERATED.add(CommitLogPropertyFixture.generateTable(KEYSPACE, random, i));

        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    TABLES_GENERATED.toArray(new TableMetadata[0]));

        archive = new File(CommitLog.instance.segmentManager.storageDirectory, "archive");
        archive.tryCreateDirectory();
        archiver = archiverFor(archive);
    }

    @Before
    public void before() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @Test
    public void archivedSegmentsRestoreAndReplay() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            // CommitLog.archiver is final, so this drives the archiver directly rather than swapping the
            // one the commit log holds. The behaviour under test is the command it runs and the file it
            // leaves, neither of which depends on that wiring.
            for (File stale : archive.tryList())
                stale.delete();
            {
                CommitLog.instance.resetUnsafe(true);

                JavaRandom random = new JavaRandom(seed);
                TableMetadata metadata = TABLES_GENERATED.get((int) Math.floorMod(seed, TABLES));

                List<ByteBuffer> written = new ArrayList<>(MUTATIONS);
                for (int i = 0; i < MUTATIONS; i++)
                {
                    Mutation mutation = CommitLogPropertyFixture.generateMutation(metadata, random);
                    CommitLog.instance.add(mutation);
                    written.add(CommitLogPropertyFixture.bytes(mutation));
                }
                CommitLog.instance.sync(true);

                // archive every active segment and wait for the commands to finish
                List<String> names = CommitLog.instance.getActiveSegmentNames();
                for (String name : names)
                {
                    archiver.maybeArchive(
                        new File(CommitLog.instance.segmentManager.storageDirectory, name).absolutePath(), name);
                    assertTrue("archiving " + name + " did not complete", archiver.maybeWaitForArchiving(name));
                }

                File[] archived = archive.tryList();
                assertTrue("nothing reached the archive", archived != null && archived.length > 0);
                assertEquals("the archive holds a different number of segments than were active",
                             names.size(), archived.length);

                // the archived copies are what a restore would bring back; replay them directly
                List<ByteBuffer> replayed = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE, archived);
                assertEquals("what came back from the archive differs from what was written, schema:\n"
                             + metadata.toCqlString(true, false, false),
                             written, replayed);
            }
        });
    }

    /** An archive command that is a plain copy, which is what an operator's command is in substance. */
    private static CommitLogArchiver archiverFor(File archive)
    {
        return new CommitLogArchiver("/bin/cp %path " + archive.absolutePath() + "/%name",
                                     "/bin/cp -f %from %to",
                                     archive.absolutePath(),
                                     Long.MAX_VALUE,
                                     CommitLogPosition.NONE,
                                     TimeUnit.MICROSECONDS);
    }
}
