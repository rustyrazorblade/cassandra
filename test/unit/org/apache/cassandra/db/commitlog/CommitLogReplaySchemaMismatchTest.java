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
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_IGNORE_REPLAY_ERRORS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Replay against a schema that no longer matches the bytes on disk.
 *
 * Both cases pass every checksum. The entry is intact; it is the schema that moved, which is what makes
 * these the two branches in CommitLogReader.readMutation that the checksum cannot protect. It is also
 * the largest uncovered method in the package.
 *
 * The property is that such an entry is never applied, and never costs the entries written around it.
 */
public class CommitLogReplaySchemaMismatchTest
{
    private static final String KEYSPACE = "commitlog_schema_mismatch";
    /** Its own keyspace, so the drop takes the whole keyspace rather than a table out from under one. */
    private static final String DOOMED_KEYSPACE = "commitlog_schema_mismatch_doomed";
    private static final String DROPPED = "dropped";
    private static final String RETYPED = "retyped";

    private boolean previousIgnoreReplayErrors;
    private static JVMStabilityInspector.Killer oldKiller;
    private static KillerForTests testKiller;

    @BeforeClass
    public static void beforeClass()
    {
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), table(KEYSPACE, RETYPED));
        SchemaLoader.createKeyspace(DOOMED_KEYSPACE, KeyspaceParams.simple(1), table(DOOMED_KEYSPACE, DROPPED));

        // without this a commit error kills the fork outright and the failure reads as "VM exited
        // abnormally" rather than as whatever went wrong
        testKiller = new KillerForTests();
        oldKiller = JVMStabilityInspector.replaceKiller(testKiller);
    }

    @AfterClass
    public static void afterClass()
    {
        JVMStabilityInspector.replaceKiller(oldKiller);
    }

    private static TableMetadata table(String keyspace, String name)
    {
        return TableMetadata.builder(keyspace, name)
                            .addPartitionKeyColumn("pk", AsciiType.instance)
                            .addClusteringColumn("ck", AsciiType.instance)
                            .addRegularColumn("val", BytesType.instance)
                            .build();
    }

    @Before
    public void before() throws IOException
    {
        previousIgnoreReplayErrors = COMMITLOG_IGNORE_REPLAY_ERRORS.getBoolean();
        CommitLog.instance.resetUnsafe(true);
    }

    /**
     * An entry whose schema is gone by the time replay runs is never applied, and never costs the entries
     * written around it.
     *
     * The entry passes both checksums; it is the schema that moved. That is the one class of damage the
     * checksum cannot protect against, and applying it would mean writing data back into a table nobody
     * can account for.
     *
     * What this does not reach: the UnknownTableException arm of readMutation specifically. Dropping the
     * keyspace makes deserialization fail before it gets far enough to raise that, so the entry lands in
     * the unrecoverable-error arm instead, saved to a file rather than skipped by table id. Reaching the
     * other arm needs a mutation for a table id that was never in this schema at all, which add will not
     * accept, so it stays uncovered.
     */
    @Test
    public void replayNeverAppliesAMutationWhoseSchemaIsGone() throws Exception
    {
        COMMITLOG_IGNORE_REPLAY_ERRORS.setBoolean(true);
        try
        {
            TableMetadata dropped = tableMetadata(DOOMED_KEYSPACE, DROPPED);
            TableMetadata kept = tableMetadata(KEYSPACE, RETYPED);

            CommitLog.instance.add(mutation(dropped, "a"));
            Mutation survivor = mutation(kept, "b");
            CommitLog.instance.add(survivor);
            CommitLog.instance.sync(true);

            File[] copies = CommitLogPropertyFixture.copyActiveSegments(scratch("dropped"));
            SchemaTestUtil.dropKeyspaceIfExist(DOOMED_KEYSPACE, true);

            List<Mutation> applied = new ArrayList<>();
            new CountingHandler(null, applied).replayFiles(copies);
            for (Mutation m : applied)
                assertTrue("replay applied a mutation for a keyspace that no longer exists",
                           m.getPartitionUpdates().stream().noneMatch(u -> DOOMED_KEYSPACE.equals(u.metadata().keyspace)));

            // the surviving entry still replays: losing a table must not cost the entries written around it
            List<ByteBuffer> replayed = CommitLogPropertyFixture.replay(kept, CommitLogPosition.NONE, copies);
            assertEquals("the entry written after one for a dropped keyspace did not replay",
                         Collections.singletonList(CommitLogPropertyFixture.bytes(survivor)), replayed);
        }
        finally
        {
            COMMITLOG_IGNORE_REPLAY_ERRORS.setBoolean(previousIgnoreReplayErrors);
        }
    }

    private static TableMetadata tableMetadata(String keyspace, String name)
    {
        return org.apache.cassandra.schema.Schema.instance.getTableMetadata(keyspace, name);
    }

    private static Mutation mutation(TableMetadata metadata, String key)
    {
        return new RowUpdateBuilder(metadata, 0, key)
               .clustering("ck")
               .add("val", ByteBufferUtil.bytes("zzzz"))
               .build();
    }

    private static File scratch(String name)
    {
        return new File(CommitLog.instance.segmentManager.storageDirectory, "mismatch-" + name);
    }

    private static class CountingHandler extends CommitLogReplayer
    {
        private final TableMetadata metadata;
        private final List<Mutation> applied;

        CountingHandler(TableMetadata metadata, List<Mutation> applied)
        {
            super(CommitLog.instance, CommitLogPosition.NONE, Collections.emptyMap(), ReplayFilter.create());
            this.metadata = metadata;
            this.applied = applied;
        }

        @Override
        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        {
            if (metadata == null || m.getPartitionUpdates().stream().anyMatch(u -> u.metadata().id.equals(metadata.id)))
                applied.add(m);
        }
    }
}
