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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.ByteArrayUtil;

/**
 * Garbage-free counter-context operations over raw byte windows, for the cursor compaction
 * hot path. Mirrors {@link org.apache.cassandra.db.context.CounterContext} EXACTLY — that
 * implementation allocates per element (ContextState objects, ByteBuffer duplicates, a
 * fresh output buffer) and offers no caller-supplied-buffer API, so it cannot run per cell
 * here. {@code CursorCounterContextMergeTest} pins both operations byte-for-byte against
 * the upstream implementation across randomized shard shapes.
 *
 * Context wire layout (CounterContext header comment):
 * <pre>
 *   [short n][n_abs shorts (header elts)][body: 32-byte shards (16B CounterId, 8B clock, 8B count)]
 * </pre>
 * n &lt; 0 means "local shards marked to be cleared" (streamed sstables); abs(n) is the elt
 * count. Header elt for body index i: {@code i + Short.MIN_VALUE} if the shard is GLOBAL,
 * {@code i} if LOCAL; REMOTE shards have no header elt. Body shards sort ascending by
 * CounterId bytes.
 *
 * Instances hold grow-once scratch buffers; one instance per compaction, NOT thread-safe.
 */
final class CursorCounterContexts
{
    private static final Logger logger = LoggerFactory.getLogger(CursorCounterContexts.class);

    private static final int HEADER_SIZE_LENGTH = 2;
    private static final int HEADER_ELT_LENGTH = 2;
    private static final int COUNTER_ID_LENGTH = 16;
    private static final int CLOCK_LENGTH = 8;
    private static final int STEP_LENGTH = COUNTER_ID_LENGTH + CLOCK_LENGTH + 8;

    enum MergeResult { LEFT_SUPERSET, RIGHT_SUPERSET, MERGED }

    private byte[] scratch = new byte[128];
    private int scratchLength;

    /** Valid after {@link #merge} returns MERGED, or after {@link #clearMarkedLocal} returns >= 0. */
    byte[] scratchBuffer()
    {
        return scratch;
    }

    int scratchLength()
    {
        return scratchLength;
    }

    /**
     * Walk state over one context window — the garbage-free analogue of
     * CounterContext.ContextState, as three primitive fields advanced in lockstep.
     */
    private byte[] buf1; private int off1, len1, headerLength1, headerOffset1, bodyOffset1;
    private byte[] buf2; private int off2, len2, headerLength2, headerOffset2, bodyOffset2;
    // output cursors for the merged context
    private int outHeaderOffset, outBodyOffset, outHeaderLength;

    private static int headerLength(byte[] buf, int off)
    {
        return HEADER_SIZE_LENGTH + Math.abs(ByteArrayUtil.getShort(buf, off)) * HEADER_ELT_LENGTH;
    }

    private boolean isGlobal1() { return headerOffset1 < headerLength1 && ByteArrayUtil.getShort(buf1, off1 + headerOffset1) == elementIndex1() + Short.MIN_VALUE; }
    private boolean isLocal1()  { return headerOffset1 < headerLength1 && ByteArrayUtil.getShort(buf1, off1 + headerOffset1) == elementIndex1(); }
    private boolean isGlobal2() { return headerOffset2 < headerLength2 && ByteArrayUtil.getShort(buf2, off2 + headerOffset2) == elementIndex2() + Short.MIN_VALUE; }
    private boolean isLocal2()  { return headerOffset2 < headerLength2 && ByteArrayUtil.getShort(buf2, off2 + headerOffset2) == elementIndex2(); }
    private int elementIndex1() { return (bodyOffset1 - headerLength1) / STEP_LENGTH; }
    private int elementIndex2() { return (bodyOffset2 - headerLength2) / STEP_LENGTH; }
    private boolean hasRemaining1() { return bodyOffset1 < len1; }
    private boolean hasRemaining2() { return bodyOffset2 < len2; }
    private long clock1() { return ByteArrayUtil.getLong(buf1, off1 + bodyOffset1 + COUNTER_ID_LENGTH); }
    private long count1() { return ByteArrayUtil.getLong(buf1, off1 + bodyOffset1 + COUNTER_ID_LENGTH + CLOCK_LENGTH); }
    private long clock2() { return ByteArrayUtil.getLong(buf2, off2 + bodyOffset2 + COUNTER_ID_LENGTH); }
    private long count2() { return ByteArrayUtil.getLong(buf2, off2 + bodyOffset2 + COUNTER_ID_LENGTH + CLOCK_LENGTH); }

    private void moveToNext1()
    {
        if (isGlobal1() || isLocal1())
            headerOffset1 += HEADER_ELT_LENGTH;
        bodyOffset1 += STEP_LENGTH;
    }

    private void moveToNext2()
    {
        if (isGlobal2() || isLocal2())
            headerOffset2 += HEADER_ELT_LENGTH;
        bodyOffset2 += STEP_LENGTH;
    }

    private int compareIds()
    {
        return ByteArrayUtil.compareUnsigned(buf1, off1 + bodyOffset1, COUNTER_ID_LENGTH,
                                             buf2, off2 + bodyOffset2, COUNTER_ID_LENGTH);
    }

    // CounterContext.Relationship, as ints to stay primitive
    private static final int EQUAL = 0, GREATER_THAN = 1, LESS_THAN = 2, DISJOINT = 3;

    /** Mirrors CounterContext.compare(left, right) exactly, including the self-heal warns. */
    private int compareShards()
    {
        long leftClock = clock1();
        long leftCount = count1();
        long rightClock = clock2();
        long rightCount = count2();
        boolean leftGlobal = isGlobal1(), rightGlobal = isGlobal2();
        boolean leftLocal = isLocal1(), rightLocal = isLocal2();

        if (leftGlobal || rightGlobal)
        {
            if (leftGlobal && rightGlobal)
            {
                if (leftClock == rightClock)
                {
                    if (leftCount != rightCount && CompactionManager.isCompactor(Thread.currentThread()))
                        logger.warn("invalid global counter shard detected; (clock {}, count {}) and (clock {}, count {}) differ only in "
                                    + "count; will pick highest to self-heal on compaction",
                                    leftClock, leftCount, rightClock, rightCount);
                    return leftCount > rightCount ? GREATER_THAN : leftCount == rightCount ? EQUAL : LESS_THAN;
                }
                return leftClock > rightClock ? GREATER_THAN : LESS_THAN;
            }
            return leftGlobal ? GREATER_THAN : LESS_THAN;
        }

        if (leftLocal || rightLocal)
        {
            if (leftLocal && rightLocal)
                return DISJOINT;
            return leftLocal ? GREATER_THAN : LESS_THAN;
        }

        // both remote
        if (leftClock == rightClock)
        {
            if (leftCount != rightCount && CompactionManager.isCompactor(Thread.currentThread()))
                logger.warn("invalid remote counter shard detected; (clock {}, count {}) and (clock {}, count {}) differ only in "
                            + "count; will pick highest to self-heal on compaction",
                            leftClock, leftCount, rightClock, rightCount);
            return leftCount > rightCount ? GREATER_THAN : leftCount == rightCount ? EQUAL : LESS_THAN;
        }
        return (leftClock >= 0 && rightClock > 0 && leftClock >= rightClock)
               || (leftClock < 0 && (rightClock > 0 || leftClock < rightClock))
               ? GREATER_THAN : LESS_THAN;
    }

    private void resetWalks(byte[] a, int aOff, int aLen, byte[] b, int bOff, int bLen)
    {
        buf1 = a; off1 = aOff; len1 = aLen;
        headerLength1 = headerLength(a, aOff);
        headerOffset1 = HEADER_SIZE_LENGTH;
        bodyOffset1 = headerLength1;
        buf2 = b; off2 = bOff; len2 = bLen;
        headerLength2 = headerLength(b, bOff);
        headerOffset2 = HEADER_SIZE_LENGTH;
        bodyOffset2 = headerLength2;
    }

    /**
     * Mirrors {@link org.apache.cassandra.db.context.CounterContext#merge} exactly: when
     * one context wholly contains the other the result is LEFT/RIGHT_SUPERSET and nothing
     * is written (the caller passes the input through, preserving the iterator's
     * identity-shortcut byte behavior); otherwise the merged context is written to the
     * scratch buffer and MERGED is returned.
     */
    MergeResult merge(byte[] a, int aOff, int aLen, byte[] b, int bOff, int bLen)
    {
        boolean leftIsSuperSet = true;
        boolean rightIsSuperSet = true;
        int globalCount = 0, localCount = 0, remoteCount = 0;

        resetWalks(a, aOff, aLen, b, bOff, bLen);
        while (hasRemaining1() && hasRemaining2())
        {
            int cmp = compareIds();
            if (cmp == 0)
            {
                int rel = compareShards();
                if (rel == GREATER_THAN)
                    rightIsSuperSet = false;
                else if (rel == LESS_THAN)
                    leftIsSuperSet = false;
                else if (rel == DISJOINT)
                    leftIsSuperSet = rightIsSuperSet = false;

                if (isGlobal1() || isGlobal2())
                    globalCount += 1;
                else if (isLocal1() || isLocal2())
                    localCount += 1;
                else
                    remoteCount += 1;
                moveToNext1();
                moveToNext2();
            }
            else if (cmp > 0)
            {
                leftIsSuperSet = false;
                if (isGlobal2()) globalCount += 1;
                else if (isLocal2()) localCount += 1;
                else remoteCount += 1;
                moveToNext2();
            }
            else
            {
                rightIsSuperSet = false;
                if (isGlobal1()) globalCount += 1;
                else if (isLocal1()) localCount += 1;
                else remoteCount += 1;
                moveToNext1();
            }
        }

        if (hasRemaining1())
            rightIsSuperSet = false;
        else if (hasRemaining2())
            leftIsSuperSet = false;

        if (leftIsSuperSet)
            return MergeResult.LEFT_SUPERSET;
        if (rightIsSuperSet)
            return MergeResult.RIGHT_SUPERSET;

        while (hasRemaining1())
        {
            if (isGlobal1()) globalCount += 1;
            else if (isLocal1()) localCount += 1;
            else remoteCount += 1;
            moveToNext1();
        }
        while (hasRemaining2())
        {
            if (isGlobal2()) globalCount += 1;
            else if (isLocal2()) localCount += 1;
            else remoteCount += 1;
            moveToNext2();
        }

        // second pass: write the merged context (ContextState.allocate layout: positive
        // header count = globals + locals, header elts in body order)
        outHeaderLength = HEADER_SIZE_LENGTH + (globalCount + localCount) * HEADER_ELT_LENGTH;
        scratchLength = outHeaderLength + (globalCount + localCount + remoteCount) * STEP_LENGTH;
        if (scratch.length < scratchLength)
            scratch = new byte[Math.max(scratchLength, scratch.length * 2)]; // grow-once, amortized
        ByteArrayUtil.putShort(scratch, 0, (short) (globalCount + localCount));
        outHeaderOffset = HEADER_SIZE_LENGTH;
        outBodyOffset = outHeaderLength;

        resetWalks(a, aOff, aLen, b, bOff, bLen);
        while (hasRemaining1() && hasRemaining2())
        {
            int cmp = compareIds();
            if (cmp == 0)
            {
                int rel = compareShards();
                if (rel == DISJOINT) // two local shards: sum clocks and counts
                    writeShard(buf1, off1 + bodyOffset1, clock1() + clock2(), count1() + count2(), false, true);
                else if (rel == GREATER_THAN)
                    copyShard1();
                else // EQUAL or LESS_THAN
                    copyShard2();
                moveToNext1();
                moveToNext2();
            }
            else if (cmp > 0)
            {
                copyShard2();
                moveToNext2();
            }
            else
            {
                copyShard1();
                moveToNext1();
            }
        }
        while (hasRemaining1())
        {
            copyShard1();
            moveToNext1();
        }
        while (hasRemaining2())
        {
            copyShard2();
            moveToNext2();
        }
        return MergeResult.MERGED;
    }

    private void copyShard1()
    {
        writeShard(buf1, off1 + bodyOffset1, clock1(), count1(), isGlobal1(), isLocal1());
    }

    private void copyShard2()
    {
        writeShard(buf2, off2 + bodyOffset2, clock2(), count2(), isGlobal2(), isLocal2());
    }

    private void writeShard(byte[] idBuf, int idOff, long clock, long count, boolean isGlobal, boolean isLocal)
    {
        int elementIndex = (outBodyOffset - outHeaderLength) / STEP_LENGTH;
        System.arraycopy(idBuf, idOff, scratch, outBodyOffset, COUNTER_ID_LENGTH);
        ByteArrayUtil.putLong(scratch, outBodyOffset + COUNTER_ID_LENGTH, clock);
        ByteArrayUtil.putLong(scratch, outBodyOffset + COUNTER_ID_LENGTH + CLOCK_LENGTH, count);
        if (isGlobal)
        {
            ByteArrayUtil.putShort(scratch, outHeaderOffset, (short) (elementIndex + Short.MIN_VALUE));
            outHeaderOffset += HEADER_ELT_LENGTH;
        }
        else if (isLocal)
        {
            ByteArrayUtil.putShort(scratch, outHeaderOffset, (short) elementIndex);
            outHeaderOffset += HEADER_ELT_LENGTH;
        }
        outBodyOffset += STEP_LENGTH;
    }

    /**
     * The deserialization-time local-shard clear the iterator path applies to every counter
     * cell read with Flag.LOCAL (DeserializationHelper.maybeClearCounterValue →
     * CounterContext.clearAllLocal): contexts whose header count is NEGATIVE (marked by the
     * streaming write path) drop their LOCAL header elts — the shards become remote; body
     * bytes are unchanged. Mirrors shouldClearLocal + clearAllLocal exactly.
     *
     * @return the cleared length written into the scratch buffer, or -1 when the context is
     *         not marked (or has no local elts to drop) and the raw window should be used
     */
    int clearMarkedLocal(byte[] src, int off, int len)
    {
        short n = ByteArrayUtil.getShort(src, off);
        if (n >= 0)
            return -1; // not marked

        int count = -n;
        int globalCount = 0;
        for (int i = 0; i < count; i++)
            if (ByteArrayUtil.getShort(src, off + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH) < 0)
                globalCount++;
        if (globalCount == count)
            return -1; // no local shards: clearAllLocal passes the (still marked) context through

        int srcHeaderLength = HEADER_SIZE_LENGTH + count * HEADER_ELT_LENGTH;
        int bodyLength = len - srcHeaderLength;
        scratchLength = HEADER_SIZE_LENGTH + globalCount * HEADER_ELT_LENGTH + bodyLength;
        if (scratch.length < scratchLength)
            scratch = new byte[Math.max(scratchLength, scratch.length * 2)];
        ByteArrayUtil.putShort(scratch, 0, (short) globalCount);
        int out = HEADER_SIZE_LENGTH;
        for (int i = 0; i < count; i++)
        {
            short elt = ByteArrayUtil.getShort(src, off + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH);
            if (elt < 0)
            {
                ByteArrayUtil.putShort(scratch, out, elt);
                out += HEADER_ELT_LENGTH;
            }
        }
        System.arraycopy(src, off + srcHeaderLength, scratch, out, bodyLength);
        return scratchLength;
    }

    /**
     * Mirrors CounterContext.hasLegacyShards over a raw window: legacy = any remote shard
     * (more body shards than header elts) or any local header elt (non-negative).
     */
    static boolean hasLegacyShards(byte[] src, int off, int len)
    {
        int headerLength = headerLength(src, off);
        int totalShards = (len - headerLength) / STEP_LENGTH;
        int localAndGlobal = Math.abs(ByteArrayUtil.getShort(src, off));
        if (localAndGlobal < totalShards)
            return true; // remote shard(s)
        for (int i = 0; i < localAndGlobal; i++)
            if (ByteArrayUtil.getShort(src, off + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH) >= 0)
                return true; // local shard
        return false;
    }
}
