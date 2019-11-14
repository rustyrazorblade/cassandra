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
package org.apache.cassandra.utils;

import java.util.*;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import io.netty.util.concurrent.FastThreadLocal;

/** Merges sorted input iterators which individually contain unique items. */
public abstract class MergeIterator<In,Out> extends AbstractIterator<Out> implements IMergeIterator<In, Out>
{
    private static class MergeIteratorPool<T extends MergeIterator>
    {
        private final Supplier<T> iteratorSupplier;
        private final FastThreadLocal<Queue<T>> queues = new FastThreadLocal<Queue<T>>()
        {
            protected Queue<T> initialValue() throws Exception
            {
                return new LinkedList<>();
            }
        };

        public MergeIteratorPool(Supplier<T> iteratorSupplier)
        {
            this.iteratorSupplier = iteratorSupplier;
        }

        protected Queue<T> initialValue() throws Exception
        {
            return new LinkedList<>();
        }

        T get()
        {
            T next = queues.get().poll();
            if (next == null)
            {
                next = iteratorSupplier.get();
            }
            return next;
        }

        void put(T iter)
        {
            queues.get().add(iter);
        }

    }

    private static final MergeIteratorPool<TrivialOneToOne> trivialOneToOne = new MergeIteratorPool<>(() -> new TrivialOneToOne());
    private static final MergeIteratorPool<OneToOne> oneToOne = new MergeIteratorPool<>(() -> new OneToOne());
    private static final MergeIteratorPool<ManyToOne> manyToOne = new MergeIteratorPool<>(() -> new ManyToOne());

    protected Reducer<In,Out> reducer;
    protected List<? extends Iterator<In>> iterators;
    protected boolean active = false;


    @SuppressWarnings("resource")
    private static <In, Out> MergeIterator<In, Out> getSimple(List<? extends Iterator<In>> sources,
                                                              Comparator<? super In> comparator,
                                                              Reducer<In, Out> reducer)
    {
        Preconditions.checkArgument(sources.size() < ManyToOne.DEFAULT_SIZE);
        MergeIterator<In, Out> mi;
        if (sources.size() == 1)
        {
            mi = reducer.trivialReduceIsTrivial() ? trivialOneToOne.get()
                                                  : oneToOne.get();
        }
        else
        {
            mi = manyToOne.get();
        }

        mi.reset(sources, comparator, reducer);
        return mi;
    }


    @SuppressWarnings("resource")
    public static <T> MergeIterator<T, T> get(List<? extends Iterator<T>> sources,
                                                       Comparator<? super T> comparator,
                                                       Reducer<T, T> reducer)
    {

        if (sources.size() < ManyToOne.DEFAULT_SIZE)
        {
            return getSimple(sources, comparator, reducer);
        }
        else
        {
            // since the in and out types are the same, we can nest pooled iterators
            List<Iterator<T>> subSources = new ArrayList<>((int) Math.ceil((double) sources.size() / ManyToOne.DEFAULT_SIZE));
            for (int i=0; i<sources.size(); i+=ManyToOne.DEFAULT_SIZE)
            {
                Iterator<T> subIter = get(sources.subList(i, Math.min(i + ManyToOne.DEFAULT_SIZE, sources.size())), comparator, reducer);
                subSources.add(subIter);
            }
            return get(subSources, comparator, reducer);
        }
    }

    @SuppressWarnings("resource")
    public static <In, Out> MergeIterator<In, Out> getTransforming(List<? extends Iterator<In>> sources,
                                                                    Comparator<? super In> comparator,
                                                                    Reducer<In, Out> reducer)
    {
        if (sources.size() < ManyToOne.DEFAULT_SIZE)
        {
            return getSimple(sources, comparator, reducer);
        }
        else
        {
            // since the in and out types are not the same, we allocate a new iterator to handle the sources
            ManyToOne<In, Out> mi = new ManyToOne<>(sources.size());
            mi.reset(sources, comparator, reducer);
            return mi;
        }
    }

    protected void reset(List<? extends Iterator<In>> sources, Comparator<? super In> comparator, Reducer<In, Out> reducer)
    {
        Preconditions.checkState(!active);
        this.iterators = sources;
        this.reducer = reducer;
        active = true;
    }

    public Iterable<? extends Iterator<In>> iterators()
    {
        return iterators;
    }

    public void close()
    {
        Preconditions.checkState(active);
        for (Iterator<In> iterator : this.iterators)
        {
            try
            {
                if (iterator instanceof AutoCloseable)
                    ((AutoCloseable)iterator).close();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        reducer.close();

        iterators = null;
        reducer = null;
        active = false;
        resetState();
        returnToPool();
    }

    protected abstract void returnToPool();

    /**
     * A MergeIterator that consumes multiple input values per output value.
     *
     * The most straightforward way to implement this is to use a {@code PriorityQueue} of iterators, {@code poll} it to
     * find the next item to consume, then {@code add} the iterator back after advancing. This is not very efficient as
     * {@code poll} and {@code add} in all cases require at least {@code log(size)} comparisons (usually more than
     * {@code 2*log(size)}) per consumed item, even if the input is suitable for fast iteration.
     *
     * The implementation below makes use of the fact that replacing the top element in a binary heap can be done much
     * more efficiently than separately removing it and placing it back, especially in the cases where the top iterator
     * is to be used again very soon (e.g. when there are large sections of the output where only a limited number of
     * input iterators overlap, which is normally the case in many practically useful situations, e.g. levelled
     * compaction). To further improve this particular scenario, we also use a short sorted section at the start of the
     * queue.
     *
     * The heap is laid out as this (for {@code SORTED_SECTION_SIZE == 2}):
     *                 0
     *                 |
     *                 1
     *                 |
     *                 2
     *               /   \
     *              3     4
     *             / \   / \
     *             5 6   7 8
     *            .. .. .. ..
     * Where each line is a <= relationship.
     *
     * In the sorted section we can advance with a single comparison per level, while advancing a level within the heap
     * requires two (so that we can find the lighter element to pop up).
     * The sorted section adds a constant overhead when data is uniformly distributed among the iterators, but may up
     * to halve the iteration time when one iterator is dominant over sections of the merged data (as is the case with
     * non-overlapping iterators).
     *
     * The iterator is further complicated by the need to avoid advancing the input iterators until an output is
     * actually requested. To achieve this {@code consume} walks the heap to find equal items without advancing the
     * iterators, and {@code advance} moves them and restores the heap structure before any items can be consumed.
     * 
     * To avoid having to do additional comparisons in consume to identify the equal items, we keep track of equality
     * between children and their parents in the heap. More precisely, the lines in the diagram above define the
     * following relationship:
     *   parent <= child && (parent == child) == child.equalParent
     * We can track, make use of and update the equalParent field without any additional comparisons.
     *
     * For more formal definitions and proof of correctness, see CASSANDRA-8915.
     */
    static final class ManyToOne<In,Out> extends MergeIterator<In,Out>
    {
        static final int DEFAULT_SIZE = 32;
        protected final Candidate<In>[] candidates;
        protected final Candidate<In>[] heap;

        /** Number of non-exhausted iterators. */
        int size = -1;
        int numIters = -1;

        /**
         * Position of the deepest, right-most child that needs advancing before we can start consuming.
         * Because advancing changes the values of the items of each iterator, the parent-chain from any position
         * in this range that needs advancing is not in correct order. The trees rooted at any position that does
         * not need advancing, however, retain their prior-held binary heap property.
         */
        int needingAdvance = -1;

        /**
         * The number of elements to keep in order before the binary heap starts, exclusive of the top heap element.
         */
        static final int SORTED_SECTION_SIZE = 4;

        public ManyToOne()
        {
            this(DEFAULT_SIZE);
        }

        public ManyToOne(int size)
        {
            this.heap = new Candidate[size];
            this.candidates = new Candidate[size];
            for (int i=0; i<size; i++)
            {
                candidates[i] = new Candidate<>();
            }
        }

        protected void reset(List<? extends Iterator<In>> sources, Comparator<? super In> comparator, Reducer<In, Out> reducer)
        {
            super.reset(sources, comparator, reducer);

            size = 0;
            for (int i=0; i<sources.size(); i++)
            {
                heap[i] = candidates[i];
                heap[i].reset(i, iterators.get(i), comparator);
                size++;
            }
            needingAdvance = size;
            numIters = size;
        }

        public void close()
        {
            for (int i=0; i<numIters; i++)
            {
                heap[i] = null;
                candidates[i].close();
            }
            size = -1;
            needingAdvance = -1;
            numIters = -1;
            super.close();
        }

        protected void returnToPool()
        {
            if (heap.length == DEFAULT_SIZE)
                manyToOne.put(this);
        }

        protected final Out computeNext()
        {
            advance();
            return consume();
        }

        /**
         * Advance all iterators that need to be advanced and place them into suitable positions in the heap.
         *
         * By walking the iterators backwards we know that everything after the point being processed already forms
         * correctly ordered subheaps, thus we can build a subheap rooted at the current position by only sinking down
         * the newly advanced iterator. Because all parents of a consumed iterator are also consumed there is no way
         * that we can process one consumed iterator but skip over its parent.
         *
         * The procedure is the same as the one used for the initial building of a heap in the heapsort algorithm and
         * has a maximum number of comparisons {@code (2 * log(size) + SORTED_SECTION_SIZE / 2)} multiplied by the
         * number of iterators whose items were consumed at the previous step, but is also at most linear in the size of
         * the heap if the number of consumed elements is high (as it is in the initial heap construction). With non- or
         * lightly-overlapping iterators the procedure finishes after just one (resp. a couple of) comparisons.
         */
        private void advance()
        {
            // Turn the set of candidates into a heap.
            for (int i = needingAdvance - 1; i >= 0; --i)
            {
                Candidate<In> candidate = heap[i];
                /**
                 *  needingAdvance runs to the maximum index (and deepest-right node) that may need advancing;
                 *  since the equal items that were consumed at-once may occur in sub-heap "veins" of equality,
                 *  not all items above this deepest-right position may have been consumed; these already form
                 *  valid sub-heaps and can be skipped-over entirely
                 */
                if (candidate.needsAdvance())
                    replaceAndSink(candidate.advance(), i);
            }
        }

        /**
         * Consume all items that sort like the current top of the heap. As we cannot advance the iterators to let
         * equivalent items pop up, we walk the heap to find them and mark them as needing advance.
         *
         * This relies on the equalParent flag to avoid doing any comparisons.
         */
        private Out consume()
        {
            if (size == 0)
                return endOfData();

            reducer.onKeyChange();
            assert !heap[0].equalParent;
            heap[0].consume(reducer);
            final int size = this.size;
            final int sortedSectionSize = Math.min(size, SORTED_SECTION_SIZE);
            int i;
            consume: {
                for (i = 1; i < sortedSectionSize; ++i)
                {
                    if (!heap[i].equalParent)
                        break consume;
                    heap[i].consume(reducer);
                }
                i = Math.max(i, consumeHeap(i) + 1);
            }
            needingAdvance = i;
            return reducer.getReduced();
        }

        /**
         * Recursively consume all items equal to equalItem in the binary subheap rooted at position idx.
         *
         * @return the largest equal index found in this search.
         */
        private int consumeHeap(int idx)
        {
            if (idx >= size || !heap[idx].equalParent)
                return -1;

            heap[idx].consume(reducer);
            int nextIdx = (idx << 1) - (SORTED_SECTION_SIZE - 1);
            return Math.max(idx, Math.max(consumeHeap(nextIdx), consumeHeap(nextIdx + 1)));
        }

        /**
         * Replace an iterator in the heap with the given position and move it down the heap until it finds its proper
         * position, pulling lighter elements up the heap.
         *
         * Whenever an equality is found between two elements that form a new parent-child relationship, the child's
         * equalParent flag is set to true if the elements are equal.
         */
        private void replaceAndSink(Candidate<In> candidate, int currIdx)
        {
            if (candidate == null)
            {
                // Drop iterator by replacing it with the last one in the heap.
                candidate = heap[--size];
                heap[size] = null; // not necessary but helpful for debugging
            }
            // The new element will be top of its heap, at this point there is no parent to be equal to.
            candidate.equalParent = false;

            final int size = this.size;
            final int sortedSectionSize = Math.min(size - 1, SORTED_SECTION_SIZE);

            int nextIdx;

            // Advance within the sorted section, pulling up items lighter than candidate.
            while ((nextIdx = currIdx + 1) <= sortedSectionSize)
            {
                if (!heap[nextIdx].equalParent) // if we were greater then an (or were the) equal parent, we are >= the child
                {
                    int cmp = candidate.compareTo(heap[nextIdx]);
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
            // If size <= SORTED_SECTION_SIZE, nextIdx below will be no less than size,
            // because currIdx == sortedSectionSize == size - 1 and nextIdx becomes
            // (size - 1) * 2) - (size - 1 - 1) == size.

            // Advance in the binary heap, pulling up the lighter element from the two at each level.
            while ((nextIdx = (currIdx * 2) - (sortedSectionSize - 1)) + 1 < size)
            {
                if (!heap[nextIdx].equalParent)
                {
                    if (!heap[nextIdx + 1].equalParent)
                    {
                        // pick the smallest of the two children
                        int siblingCmp = heap[nextIdx + 1].compareTo(heap[nextIdx]);
                        if (siblingCmp < 0)
                            ++nextIdx;

                        // if we're smaller than this, we are done, and must only restore the heap and equalParent properties
                        int cmp = candidate.compareTo(heap[nextIdx]);
                        if (cmp <= 0)
                        {
                            if (cmp == 0)
                            {
                                heap[nextIdx].equalParent = true;
                                if (siblingCmp == 0) // siblingCmp == 0 => nextIdx is the left child
                                    heap[nextIdx + 1].equalParent = true;
                            }

                            heap[currIdx] = candidate;
                            return;
                        }

                        if (siblingCmp == 0)
                        {
                            // siblingCmp == 0 => nextIdx is still the left child
                            // if the two siblings were equal, and we are inserting something greater, we will
                            // pull up the left one; this means the right gets an equalParent
                            heap[nextIdx + 1].equalParent = true;
                        }
                    }
                    else
                        ++nextIdx;  // descend down the path where we found the equal child
                }

                heap[currIdx] = heap[nextIdx];
                currIdx = nextIdx;
            }

            // our loop guard ensures there are always two siblings to process; typically when we exit the loop we will
            // be well past the end of the heap and this next condition will match...
            if (nextIdx >= size)
            {
                heap[currIdx] = candidate;
                return;
            }

            // ... but sometimes we will have one last child to compare against, that has no siblings
            if (!heap[nextIdx].equalParent)
            {
                int cmp = candidate.compareTo(heap[nextIdx]);
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

    // Holds and is comparable by the head item of an iterator it owns
    protected static final class Candidate<In> implements Comparable<Candidate<In>>
    {
        private Iterator<? extends In> iter;
        private Comparator<? super In> comp;
        private int idx = -1;
        private In item;
        private In lowerBound;
        boolean equalParent;
        boolean active = false;

        protected void reset(int idx, Iterator<? extends In> iter, Comparator<? super In> comp)
        {
            Preconditions.checkState(!active);

            this.iter = iter;
            this.comp = comp;
            this.idx = idx;
            this.item = null;
            this.lowerBound = iter instanceof IteratorWithLowerBound ? ((IteratorWithLowerBound<In>)iter).lowerBound() : null;
            this.equalParent = false;

            active = true;
        }

        protected void close()
        {
            Preconditions.checkState(active);

            this.iter = null;
            this.comp = null;
            this.idx = -1;
            this.item = null;
            this.lowerBound = null;
            this.equalParent = false;

            active = false;
        }

        /** @return this if our iterator had an item, and it is now available, otherwise null */
        protected Candidate<In> advance()
        {
            if (lowerBound != null)
            {
                item = lowerBound;
                return this;
            }

            if (!iter.hasNext())
                return null;

            item = iter.next();
            return this;
        }

        public int compareTo(Candidate<In> that)
        {
            assert this.item != null && that.item != null;
            int ret = comp.compare(this.item, that.item);
            if (ret == 0 && (this.isLowerBound() ^ that.isLowerBound()))
            {   // if the items are equal and one of them is a lower bound (but not the other one)
                // then ensure the lower bound is less than the real item so we can safely
                // skip lower bounds when consuming
                return this.isLowerBound() ? -1 : 1;
            }
            return ret;
        }

        private boolean isLowerBound()
        {
            return item == lowerBound;
        }

        public void consume(Reducer reducer)
        {
            if (isLowerBound())
            {
                item = null;
                lowerBound = null;
            }
            else
            {
                reducer.reduce(idx, item);
                item = null;
            }
        }

        public boolean needsAdvance()
        {
            return item == null;
        }
    }

    /** Accumulator that collects values of type A, and outputs a value of type B. */
    public static abstract class Reducer<In,Out>
    {
        /**
         * @return true if Out is the same as In for the case of a single source iterator
         */
        public boolean trivialReduceIsTrivial()
        {
            return false;
        }

        /**
         * combine this object with the previous ones.
         * intermediate state is up to your implementation.
         */
        public abstract void reduce(int idx, In current);

        /** @return The last object computed by reduce */
        protected abstract Out getReduced();

        /**
         * Called at the beginning of each new key, before any reduce is called.
         * To be overridden by implementing classes.
         */
        protected void onKeyChange() {}

        /**
         * May be overridden by implementations that require cleaning up after use
         */
        public void close() {}
    }

    private static class OneToOne<In, Out> extends MergeIterator<In, Out>
    {
        Iterator<In> source;

        protected void reset(List<? extends Iterator<In>> sources, Comparator<? super In> comparator, Reducer<In, Out> reducer)
        {
            Preconditions.checkArgument(sources.size() == 1);
            super.reset(sources, comparator, reducer);
            source = sources.get(0);
        }

        public void close()
        {
            source = null;
            super.close();
        }

        protected void returnToPool()
        {
            oneToOne.put(this);
        }

        protected Out computeNext()
        {
            if (!source.hasNext())
                return endOfData();
            reducer.onKeyChange();
            reducer.reduce(0, source.next());
            return reducer.getReduced();
        }
    }

    private static class TrivialOneToOne<In, Out> extends MergeIterator<In, Out>
    {
        Iterator<In> source;

        protected void reset(List<? extends Iterator<In>> sources, Comparator<? super In> comparator, Reducer<In, Out> reducer)
        {
            super.reset(sources, comparator, reducer);
            source = sources.get(0);
        }

        public void close()
        {
            source = null;
            super.close();
        }

        protected void returnToPool()
        {
            trivialOneToOne.put(this);
        }

        @SuppressWarnings("unchecked")
        protected Out computeNext()
        {
            if (!source.hasNext())
                return endOfData();
            return (Out) source.next();
        }
    }
}
