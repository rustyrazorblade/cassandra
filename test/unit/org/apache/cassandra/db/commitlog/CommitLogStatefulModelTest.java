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

import accord.utils.Property;
import accord.utils.Property.Command;
import accord.utils.Property.UnitCommand;
import accord.utils.RandomSource;
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
 * A model of the commit log, driven by a generated sequence of operations.
 *
 * The model is the whole point and it is deliberately trivial: an ordered list of the mutations that
 * were written. Everything the commit log does to those bytes on the way to disk and back is
 * implementation, and the model asserts none of it. What it asserts is that after any interleaving of
 * writes, segment switches and syncs, replay returns exactly the list, in order.
 *
 * The single-shot properties cannot reach this. They write a batch, sync once and replay once. Real
 * nodes interleave: a segment fills mid-batch, a sync lands between two writes, a switch happens with an
 * entry half written. The generated command sequence produces those orderings without anyone having to
 * think of them, and the runner prints the failing step and the full command history when one breaks.
 *
 * Discard is modelled too, and it is the reason the model tracks a position per entry. A discard is
 * allowed to drop entries, but only whole segments and only ones it has been told are clean, so the
 * assertion is in two halves: what comes back is a suffix of what was written, and every entry written
 * after the last discard point is still in it. Segment granularity is why the first half cannot be an
 * equality; correctness of the second half is what a bug in discard would break.
 *
 * What this model does not cover: concurrent writers, since the runner drives one thread, and everything
 * the shared fixture's schema generation leaves out.
 */
public class CommitLogStatefulModelTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogStatefulModelTest.class);

    private static final String KEYSPACE = "commitlog_stateful_model";
    private static final int STEPS = CassandraRelevantProperties.TEST_COMMITLOG_STATEFUL_STEPS.getInt();

    private static final int TABLES = 4;
    private static final List<TableMetadata> TABLES_GENERATED = new ArrayList<>(TABLES);

    @BeforeClass
    public static void beforeClass()
    {
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        SchemaLoader.prepareServer();

        long schemaSeed = CassandraRelevantProperties.TEST_COMMITLOG_SEED.getLong(System.currentTimeMillis());
        logger.info("schema seed={}, steps per run={}", schemaSeed, STEPS);
        JavaRandom random = new JavaRandom(schemaSeed);
        for (int i = 0; i < TABLES; i++)
            TABLES_GENERATED.add(CommitLogPropertyFixture.generateTable(KEYSPACE, random, i));

        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    TABLES_GENERATED.toArray(new TableMetadata[0]));
    }

    @Test
    public void modelHoldsUnderAnyInterleaving()
    {
        Property.stateful()
                .withSteps(STEPS)
                .check(Property.commands(() -> (RandomSource rs) -> new Model(rs))
                               .add((rs, state) -> new Write(state.nextMutation(rs)))
                               .add((rs, state) -> new SwitchSegment())
                               .add((rs, state) -> new Sync())
                               .add((rs, state) -> new Discard(state.metadata))
                               .add((rs, state) -> new ReplayAndCompare(state.metadata))
                               .destroyState((state, cause) -> state.reset())
                               .build());
    }

    /** The model: the mutations written, in order, and nothing else. */
    static class Model
    {
        final TableMetadata metadata;
        final List<ByteBuffer> written = new ArrayList<>();
        /** Position of each entry in {@link #written}, filled in from what add returned. */
        final List<CommitLogPosition> positions = new ArrayList<>();
        /** Everything at or before this may legally have been discarded. */
        CommitLogPosition lastDiscard = CommitLogPosition.NONE;
        private final JavaRandom values;

        Model(RandomSource rs)
        {
            this.metadata = TABLES_GENERATED.get(rs.nextInt(TABLES));
            this.values = new JavaRandom(rs.nextLong());
            reset();
        }

        Mutation nextMutation(RandomSource ignored)
        {
            return CommitLogPropertyFixture.generateMutation(metadata, values);
        }

        final void reset()
        {
            try
            {
                CommitLog.instance.resetUnsafe(true);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            written.clear();
            positions.clear();
            lastDiscard = CommitLogPosition.NONE;
        }
    }

    /**
     * Writes one mutation and records it. The position comes back from add rather than being predicted,
     * so the model learns it in checkPostconditions; apply and run keep the two lists aligned because
     * process always runs them in that order.
     */
    static class Write implements Command<Model, Void, CommitLogPosition>
    {
        private final Mutation mutation;

        Write(Mutation mutation)
        {
            this.mutation = mutation;
        }

        @Override
        public CommitLogPosition apply(Model model)
        {
            model.written.add(CommitLogPropertyFixture.bytes(mutation));
            return null;
        }

        @Override
        public CommitLogPosition run(Void sut)
        {
            return CommitLog.instance.add(mutation);
        }

        @Override
        public void checkPostconditions(Model model, CommitLogPosition ignored, Void sut, CommitLogPosition actual)
        {
            model.positions.add(actual);
        }

        @Override
        public String toString()
        {
            return "Write";
        }
    }

    /**
     * Marks the table clean up to the current position and discards whatever that frees. The model
     * records the point rather than predicting which segments go, because that is segment granularity
     * and the model works in entries.
     */
    static class Discard implements Command<Model, Void, CommitLogPosition>
    {
        private final TableMetadata metadata;

        Discard(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public CommitLogPosition apply(Model model)
        {
            return null;
        }

        @Override
        public CommitLogPosition run(Void sut) throws IOException
        {
            CommitLog.instance.sync(true);
            CommitLogPosition upTo = CommitLog.instance.getCurrentPosition();
            CommitLog.instance.discardCompletedSegments(metadata.id, CommitLogPosition.NONE, upTo);
            return upTo;
        }

        @Override
        public void checkPostconditions(Model model, CommitLogPosition ignored, Void sut, CommitLogPosition actual)
        {
            model.lastDiscard = actual;
        }

        @Override
        public String toString()
        {
            return "Discard";
        }
    }

    /**
     * Forces the next write into a new segment. Invisible to the model, which is the assertion: where an
     * entry lands must not change what comes back.
     */
    static class SwitchSegment implements UnitCommand<Model, Void>
    {
        @Override
        public void applyUnit(Model model)
        {
        }

        @Override
        public void runUnit(Void sut)
        {
            CommitLog.instance.segmentManager.advanceAllocatingFrom(CommitLog.instance.segmentManager.allocatingFrom());
        }

        @Override
        public String toString()
        {
            return "SwitchSegment";
        }
    }

    /** Invisible to the model for the same reason: syncing must not change what comes back. */
    static class Sync implements UnitCommand<Model, Void>
    {
        @Override
        public void applyUnit(Model model)
        {
        }

        @Override
        public void runUnit(Void sut) throws IOException
        {
            CommitLog.instance.sync(true);
        }

        @Override
        public String toString()
        {
            return "Sync";
        }
    }

    /** The only command that asserts: replay returns the model's list, in order. */
    static class ReplayAndCompare implements Command<Model, Void, List<ByteBuffer>>
    {
        private final TableMetadata metadata;

        ReplayAndCompare(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public List<ByteBuffer> apply(Model model)
        {
            return new ArrayList<>(model.written);
        }

        @Override
        public List<ByteBuffer> run(Void sut) throws IOException
        {
            CommitLog.instance.sync(true);
            return CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE);
        }

        @Override
        public void checkPostconditions(Model model, List<ByteBuffer> expected, Void sut, List<ByteBuffer> actual)
        {
            String schema = "\nschema:\n" + metadata.toCqlString(true, false, false);
            assertTrue("replay returned more entries than were written" + schema,
                       actual.size() <= expected.size());

            int dropped = expected.size() - actual.size();
            assertEquals("what replayed is not a suffix of what was written" + schema,
                         expected.subList(dropped, expected.size()), actual);

            // a discard may drop whole segments, so it may drop entries before its own position; it may
            // never drop one after it
            for (int i = 0; i < dropped; i++)
                assertTrue("an entry written after the last discard point went missing, entry " + i
                           + " at " + model.positions.get(i) + ", last discard " + model.lastDiscard + schema,
                           model.positions.get(i).compareTo(model.lastDiscard) <= 0);
        }

        @Override
        public String toString()
        {
            return "ReplayAndCompare";
        }
    }
}
