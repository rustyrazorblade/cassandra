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

import java.io.IOException;

/**
 * The order {@link org.apache.cassandra.utils.MergeIterator}'s many-source merge gives equal items
 * in, for sources a cursor merge reads.  The iterator path keeps the clustering of the last row and
 * of the last range tombstone marker it is given, and the first of two cells that tie on
 * everything.  Values can compare equal and still differ in bytes (decimal 1.0 and 1.00), so the
 * order decides which source's bytes the output holds.  On a read it also decides which source's
 * objects the merged row holds, and the read size guardrail counts their heap sizes.
 *
 * <p>This is {@code MergeIterator.ManyToOne} with each source's current item read through
 * {@link Sources} instead of stored: the same sorted section, binary heap, equal-parent flags and
 * removal of an exhausted source, so equal items come out in the same order.  The merge reads no
 * data itself: the caller moves the sources, and {@link Sources#advance} only reports whether a
 * source has a current item.
 *
 * <p>Built with {@link LowerBoundSources}, a source may first give a lower bound, which sorts before
 * an equal item and is consumed without being reported, like {@code MergeIterator.Candidate}.
 * {@link #nextWithLowerBounds} reports the sources of the next items that are not lower bounds.
 *
 * <p>Allocates only when built.
 */
public final class IteratorMergeOrder<T>
{
    /** What the order reads from the merge. */
    public interface Sources<T>
    {
        /** Compares the current items of two sources. */
        int compare(T a, T b);

        /**
         * Called where the merge would move {@code source} to its next item, or to its first one.
         * The caller has already moved it.
         *
         * @return false when the source has no item left
         */
        boolean advance(T source);
    }

    /** What the order reads from a merge whose sources may first give a lower bound. */
    public interface LowerBoundSources<T>
    {
        /**
         * Compares the current items of two sources.  An item is the source's lower bound when its
         * flag is set.
         */
        int compare(T a, boolean aIsLowerBound, T b, boolean bIsLowerBound);

        /**
         * Moves {@code source} to its next item, after the one the merge consumed, or to its first
         * one.  {@code afterLowerBound} is set for the first item after its lower bound.
         *
         * @return false when the source has no item left
         */
        boolean advance(T source, boolean afterLowerBound) throws IOException;
    }

    /** {@code MergeIterator.ManyToOne.SORTED_SECTION_SIZE}. */
    private static final int SORTED_SECTION_SIZE = 4;

    private static final class Candidate<T>
    {
        T source;
        /** The source still gives its lower bound before its first item. */
        boolean lowerBound;
        /** The current item is the lower bound. */
        boolean atLowerBound;
        /** The source gave its lower bound, and its first item is next. */
        boolean lowerBoundConsumed;
        /** The current item was consumed, or there is none yet: the source needs to advance. */
        boolean needsAdvance;
        boolean equalParent;

        void reset(T source, boolean lowerBound)
        {
            this.source = source;
            this.lowerBound = lowerBound;
            this.atLowerBound = false;
            this.lowerBoundConsumed = false;
            this.needsAdvance = true;
            this.equalParent = false;
        }
    }

    /** Set when built with {@link Sources}; exactly one of the two is set. */
    private final Sources<T> sources;
    private final LowerBoundSources<T> lowerBoundSources;
    private final Candidate<T>[] heap;
    /** The candidates, for {@link #reset}; the heap moves them around. */
    private final Candidate<T>[] pool;
    private int size;
    private int needingAdvance;
    /** The sources of the items {@link #consume} took, in order; filled by each step. */
    private T[] out;
    private int outCount;

    /** An order for up to {@code capacity} sources, given by {@link #reset}. */
    public IteratorMergeOrder(int capacity, Sources<T> sources)
    {
        this(capacity, sources, null);
    }

    /**
     * An order of {@code inSourceOrder}, read with {@link #nextWithLowerBounds}.
     *
     * @param inSourceOrder the sources, in the order the iterator path gives them to its merge
     * @param lowerBounds   which of them first give a lower bound
     */
    public IteratorMergeOrder(T[] inSourceOrder, boolean[] lowerBounds, LowerBoundSources<T> sources)
    {
        this(inSourceOrder.length, null, sources);
        for (int i = 0; i < inSourceOrder.length; i++)
            pool[i].reset(inSourceOrder[i], lowerBounds[i]);
        restart(inSourceOrder.length);
    }

    @SuppressWarnings("unchecked")
    private IteratorMergeOrder(int capacity, Sources<T> sources, LowerBoundSources<T> lowerBoundSources)
    {
        this.sources = sources;
        this.lowerBoundSources = lowerBoundSources;
        this.heap = new Candidate[capacity];
        this.pool = new Candidate[capacity];
        for (int i = 0; i < capacity; i++)
            pool[i] = new Candidate<>();
    }

    /** Starts a new merge of the first {@code count} of {@code inSourceOrder}, in that order. */
    public void reset(T[] inSourceOrder, int count)
    {
        assert sources != null : "an order with lower bounds is not reset";
        for (int i = 0; i < count; i++)
            pool[i].reset(inSourceOrder[i], false);
        restart(count);
    }

    private void restart(int count)
    {
        for (int i = 0; i < heap.length; i++)
            heap[i] = i < count ? pool[i] : null;
        this.size = count;
        this.needingAdvance = count;
    }

    /**
     * Advances the sources the last step consumed, then takes the next items.  Writes their sources
     * to {@code into}, in the order the merge gives them.  For an order built with {@link Sources}.
     *
     * @return how many sources it wrote; 0 once every source is exhausted
     */
    public int next(T[] into)
    {
        assert sources != null : "an order with lower bounds is read with nextWithLowerBounds";
        try
        {
            return step(into);
        }
        catch (IOException e)
        {
            // only LowerBoundSources.advance throws
            throw new AssertionError(e);
        }
    }

    /**
     * {@link #next}, for an order built with {@link LowerBoundSources}: steps that only consume lower
     * bounds are skipped.
     */
    public int nextWithLowerBounds(T[] into) throws IOException
    {
        assert lowerBoundSources != null : "an order without lower bounds is read with next";
        return step(into);
    }

    private int step(T[] into) throws IOException
    {
        this.out = into;
        try
        {
            while (true)
            {
                advance();
                if (size == 0)
                    return 0;
                outCount = 0;
                consume();
                if (outCount > 0)
                    return outCount;
            }
        }
        finally
        {
            this.out = null;
        }
    }

    /** {@code MergeIterator.ManyToOne.advance}. */
    private void advance() throws IOException
    {
        for (int i = needingAdvance - 1; i >= 0; --i)
        {
            Candidate<T> candidate = heap[i];
            if (candidate.needsAdvance)
                replaceAndSink(advance(candidate) ? candidate : null, i);
        }
    }

    /** {@code MergeIterator.Candidate.advance}. */
    private boolean advance(Candidate<T> candidate) throws IOException
    {
        candidate.needsAdvance = false;
        // a null source is an empty one
        if (candidate.source == null)
            return false;
        if (sources != null)
            return sources.advance(candidate.source);
        if (candidate.lowerBound)
        {
            candidate.atLowerBound = true;
            return true;
        }
        candidate.atLowerBound = false;
        boolean afterLowerBound = candidate.lowerBoundConsumed;
        candidate.lowerBoundConsumed = false;
        return lowerBoundSources.advance(candidate.source, afterLowerBound);
    }

    /** {@code MergeIterator.Candidate.compareTo}: a lower bound sorts before an equal item. */
    private int compare(Candidate<T> a, Candidate<T> b)
    {
        if (sources != null)
            return sources.compare(a.source, b.source);
        int cmp = lowerBoundSources.compare(a.source, a.atLowerBound, b.source, b.atLowerBound);
        if (cmp == 0 && (a.atLowerBound ^ b.atLowerBound))
            return a.atLowerBound ? -1 : 1;
        return cmp;
    }

    private void consume(Candidate<T> candidate)
    {
        candidate.needsAdvance = true;
        if (candidate.atLowerBound)
        {
            candidate.lowerBound = false;
            candidate.atLowerBound = false;
            candidate.lowerBoundConsumed = true;
            return;
        }
        out[outCount++] = candidate.source;
    }

    /** {@code MergeIterator.ManyToOne.consume}. */
    private void consume()
    {
        consume(heap[0]);
        final int size = this.size;
        final int sortedSectionSize = Math.min(size, SORTED_SECTION_SIZE);
        int i;
        consume:
        {
            for (i = 1; i < sortedSectionSize; ++i)
            {
                if (!heap[i].equalParent)
                    break consume;
                consume(heap[i]);
            }
            i = Math.max(i, consumeHeap(i) + 1);
        }
        needingAdvance = i;
    }

    /** {@code MergeIterator.ManyToOne.consumeHeap}. */
    private int consumeHeap(int idx)
    {
        if (idx >= size || !heap[idx].equalParent)
            return -1;

        consume(heap[idx]);
        int nextIdx = (idx << 1) - (SORTED_SECTION_SIZE - 1);
        return Math.max(idx, Math.max(consumeHeap(nextIdx), consumeHeap(nextIdx + 1)));
    }

    /** {@code MergeIterator.ManyToOne.replaceAndSink}. */
    private void replaceAndSink(Candidate<T> candidate, int currIdx)
    {
        if (candidate == null)
        {
            // drop the source by replacing it with the last one in the heap
            candidate = heap[--size];
            heap[size] = null;
        }
        candidate.equalParent = false;

        final int size = this.size;
        final int sortedSectionSize = Math.min(size - 1, SORTED_SECTION_SIZE);

        int nextIdx;
        while ((nextIdx = currIdx + 1) <= sortedSectionSize)
        {
            if (!heap[nextIdx].equalParent)
            {
                int cmp = compare(candidate, heap[nextIdx]);
                if (cmp <= 0)
                {
                    heap[nextIdx].equalParent = cmp == 0;
                    heap[currIdx] = candidate;
                    return;
                }
            }

            heap[currIdx] = heap[nextIdx];
            currIdx = nextIdx;
        }
        nextIdx = (currIdx * 2) - (sortedSectionSize - 1);
        if (nextIdx >= size)
        {
            heap[currIdx] = candidate;
            return;
        }
        sinkInBinaryHeap(candidate, currIdx, size, sortedSectionSize);
    }

    /** {@code MergeIterator.ManyToOne.sinkInBinaryHeap}. */
    private void sinkInBinaryHeap(Candidate<T> candidate, int currIdx, final int size, final int sortedSectionSize)
    {
        int nextIdx;
        while ((nextIdx = (currIdx * 2) - (sortedSectionSize - 1)) + 1 < size)
        {
            if (!heap[nextIdx].equalParent)
            {
                if (!heap[nextIdx + 1].equalParent)
                {
                    int siblingCmp = compare(heap[nextIdx + 1], heap[nextIdx]);
                    if (siblingCmp < 0)
                        ++nextIdx;

                    int cmp = compare(candidate, heap[nextIdx]);
                    if (cmp <= 0)
                    {
                        if (cmp == 0)
                        {
                            heap[nextIdx].equalParent = true;
                            if (siblingCmp == 0)
                                heap[nextIdx + 1].equalParent = true;
                        }

                        heap[currIdx] = candidate;
                        return;
                    }

                    if (siblingCmp == 0)
                        heap[nextIdx + 1].equalParent = true;
                }
                else
                    ++nextIdx;
            }

            heap[currIdx] = heap[nextIdx];
            currIdx = nextIdx;
        }

        if (nextIdx >= size)
        {
            heap[currIdx] = candidate;
            return;
        }

        if (!heap[nextIdx].equalParent)
        {
            int cmp = compare(candidate, heap[nextIdx]);
            if (cmp <= 0)
            {
                heap[nextIdx].equalParent = cmp == 0;
                heap[currIdx] = candidate;
                return;
            }
        }

        heap[currIdx] = heap[nextIdx];
        heap[nextIdx] = candidate;
    }
}
