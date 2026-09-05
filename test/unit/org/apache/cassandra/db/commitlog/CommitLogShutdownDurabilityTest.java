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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.quicktheories.impl.JavaRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A clean shutdown loses nothing that was accepted before it.
 *
 * This is the promise an operator relies on every time they restart a node, and under periodic sync it is
 * not implied by anything else: add returns before the entry is on disk, so what makes the write durable
 * is the shutdown flushing the commit log on its way out. If it did not, a rolling restart would lose the
 * tail of every node's log, quietly.
 *
 * The test writes, shuts the commit log down, and then reads the segment files off disk with no further
 * sync of its own. Anything the shutdown failed to write would simply be missing.
 *
 * The class runs one test and then leaves the commit log stopped, which is why it does not share a JVM
 * with anything else. Periodic sync is pinned because it is the mode where the shutdown has real work to
 * do; batch and group have already synced each entry as it landed.
 */
public class CommitLogShutdownDurabilityTest
{
    private static final String KEYSPACE = "commitlog_shutdown_durability";
    private static final int MUTATIONS = CassandraRelevantProperties.TEST_COMMITLOG_MUTATIONS_PER_EXAMPLE.getInt();

    private static TableMetadata metadata;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.initializeCommitLogDiskAccessMode();
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;

        SchemaLoader.prepareServer();
        long seed = CassandraRelevantProperties.TEST_COMMITLOG_SEED.getLong(System.currentTimeMillis());
        metadata = CommitLogPropertyFixture.generateTable(KEYSPACE, new JavaRandom(seed), 0);
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), metadata);
    }

    @Test
    public void shutdownMakesEverythingAcceptedDurable() throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);

        JavaRandom random = new JavaRandom(CassandraRelevantProperties.TEST_COMMITLOG_SEED
                                           .getLong(System.currentTimeMillis()));
        List<ByteBuffer> written = new ArrayList<>(MUTATIONS);
        for (int i = 0; i < MUTATIONS; i++)
        {
            Mutation mutation = CommitLogPropertyFixture.generateMutation(metadata, random);
            CommitLog.instance.add(mutation);
            written.add(CommitLogPropertyFixture.bytes(mutation));
        }

        // no sync here on purpose: the shutdown is what has to make these durable
        CommitLog.instance.shutdownBlocking();

        File[] segments = new File(CommitLog.instance.segmentManager.storageDirectory)
                          .tryList((dir, name) -> name.startsWith("CommitLog-") && name.endsWith(".log"));
        assertTrue("the shutdown left no segment files behind", segments != null && segments.length > 0);

        List<ByteBuffer> replayed = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE, segments);
        assertEquals("a clean shutdown lost entries that had been accepted, schema:\n"
                     + metadata.toCqlString(true, false, false),
                     written, replayed);
    }
}
