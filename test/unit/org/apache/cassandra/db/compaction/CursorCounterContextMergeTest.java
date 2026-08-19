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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.context.CounterContext.ContextState;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins {@link CursorCounterContexts} byte-for-byte against the upstream allocating
 * implementation ({@link CounterContext}) across randomized shard shapes: global/local/
 * remote mixes, overlapping and disjoint CounterId sets, equal and differing clocks
 * (including negative clocks), superset relationships in both directions, marked-to-clear
 * headers, and the legacy-shards predicate. Single-JVM CQL can only produce single-shard
 * global contexts, so the multi-shard semantics live or die by this sweep — it is the
 * counter equivalent of CursorColumnsSubsetEncodingTest.
 */
public class CursorCounterContextMergeTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static byte[] bytes(ByteBuffer bb)
    {
        return ByteBufferUtil.getArray(bb);
    }

    /** Random context over ascending ids drawn from [0, idSpace), at least one shard. */
    private static ByteBuffer randomContext(Random random, int idSpace, boolean[] memberOut, int[] typeOut)
    {
        int globals = 0, locals = 0, remotes = 0;
        for (int id = 0; id < idSpace; id++)
        {
            memberOut[id] = random.nextInt(3) > 0; // ~2/3 membership
            if (!memberOut[id])
                continue;
            typeOut[id] = random.nextInt(3); // 0 global, 1 local, 2 remote
            if (typeOut[id] == 0) globals++;
            else if (typeOut[id] == 1) locals++;
            else remotes++;
        }
        if (globals + locals + remotes == 0)
        {
            memberOut[0] = true;
            typeOut[0] = 0;
            globals = 1;
        }
        ContextState state = ContextState.allocate(globals, locals, remotes);
        for (int id = 0; id < idSpace; id++)
        {
            if (!memberOut[id])
                continue;
            long clock = random.nextInt(9) - 3; // includes negatives and frequent collisions
            long count = random.nextInt(100) - 50;
            if (typeOut[id] == 0) state.writeGlobal(CounterId.fromInt(id), clock, count);
            else if (typeOut[id] == 1) state.writeLocal(CounterId.fromInt(id), clock, count);
            else state.writeRemote(CounterId.fromInt(id), clock, count);
        }
        return state.context;
    }

    @Test
    public void mergeMatchesUpstreamAcrossRandomShapes()
    {
        Random random = new Random(20260611);
        CursorCounterContexts cursor = new CursorCounterContexts();
        CounterContext upstream = CounterContext.instance();
        boolean[] member = new boolean[16];
        int[] type = new int[16];
        int merged = 0, leftSuper = 0, rightSuper = 0;

        for (int trial = 0; trial < 2000; trial++)
        {
            int idSpace = 1 + random.nextInt(12);
            ByteBuffer left = randomContext(random, idSpace, member, type);
            ByteBuffer right = randomContext(random, idSpace, member, type);

            ByteBuffer leftIn = left.duplicate();
            ByteBuffer rightIn = right.duplicate();
            ByteBuffer expected = upstream.merge(leftIn, rightIn);
            byte[] l = bytes(left);
            byte[] r = bytes(right);
            CursorCounterContexts.MergeResult result = cursor.merge(l, 0, l.length, r, 0, r.length);

            if (expected == leftIn)
            {
                assertEquals("trial " + trial, CursorCounterContexts.MergeResult.LEFT_SUPERSET, result);
                leftSuper++;
            }
            else if (expected == rightIn)
            {
                assertEquals("trial " + trial, CursorCounterContexts.MergeResult.RIGHT_SUPERSET, result);
                rightSuper++;
            }
            else
            {
                assertEquals("trial " + trial, CursorCounterContexts.MergeResult.MERGED, result);
                byte[] actual = new byte[cursor.scratchLength()];
                System.arraycopy(cursor.scratchBuffer(), 0, actual, 0, actual.length);
                assertArrayEquals("trial " + trial + ": merged context bytes diverge from CounterContext.merge",
                                  bytes(expected), actual);
                merged++;
            }
        }
        // non-vacuousness: all three outcomes must actually occur
        assertTrue("no real merges exercised", merged > 100);
        assertTrue("no left-superset shortcuts exercised", leftSuper > 10);
        assertTrue("no right-superset shortcuts exercised", rightSuper > 10);
    }

    @Test
    public void clearMatchesUpstreamDeserializationBehavior()
    {
        Random random = new Random(99887766);
        CursorCounterContexts cursor = new CursorCounterContexts();
        CounterContext upstream = CounterContext.instance();
        boolean[] member = new boolean[16];
        int[] type = new int[16];
        int cleared = 0, passedThrough = 0;

        for (int trial = 0; trial < 2000; trial++)
        {
            ByteBuffer ctx = randomContext(random, 1 + random.nextInt(10), member, type);
            // half the trials run the streaming mark first (negative header count)
            if (random.nextBoolean())
                ctx = upstream.markLocalToBeCleared(ctx);

            // the iterator path: DeserializationHelper.maybeClearCounterValue with Flag.LOCAL
            ByteBuffer expected = upstream.shouldClearLocal(ctx, ByteBufferAccessor.instance)
                                  ? upstream.clearAllLocal(ctx, ByteBufferAccessor.instance)
                                  : ctx;

            byte[] raw = bytes(ctx);
            int len = cursor.clearMarkedLocal(raw, 0, raw.length);
            byte[] actual;
            if (len < 0)
            {
                actual = raw;
                passedThrough++;
            }
            else
            {
                actual = new byte[len];
                System.arraycopy(cursor.scratchBuffer(), 0, actual, 0, len);
                cleared++;
            }
            assertArrayEquals("trial " + trial, bytes(expected), actual);
        }
        assertTrue("no clears exercised", cleared > 100);
        assertTrue("no passthroughs exercised", passedThrough > 100);
    }

    @Test
    public void legacyShardsPredicateMatchesUpstream()
    {
        Random random = new Random(31415926);
        CounterContext upstream = CounterContext.instance();
        boolean[] member = new boolean[16];
        int[] type = new int[16];
        int legacy = 0;

        for (int trial = 0; trial < 1000; trial++)
        {
            ByteBuffer ctx = randomContext(random, 1 + random.nextInt(10), member, type);
            byte[] raw = bytes(ctx);
            boolean expected = upstream.hasLegacyShards(ctx, ByteBufferAccessor.instance);
            assertEquals("trial " + trial, expected, CursorCounterContexts.hasLegacyShards(raw, 0, raw.length));
            if (expected)
                legacy++;
        }
        assertTrue("no legacy shapes exercised", legacy > 100);
    }
}
