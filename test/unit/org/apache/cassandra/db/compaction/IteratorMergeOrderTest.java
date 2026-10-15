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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.IteratorWithLowerBound;
import org.apache.cassandra.utils.MergeIterator;

import static org.junit.Assert.assertEquals;

/**
 * {@link IteratorMergeOrder} must give equal items in the order {@link MergeIterator} gives them
 * to its reducer.  Each source is a sorted list of ints, and many sources hold the same ints, so
 * most steps merge several sources.  Source counts go past the sorted section into the binary
 * heap, and empty or short sources exercise the removal of an exhausted source.  With lower bounds,
 * the bounds are equal to or below the first item, and empty sources come with and without one.
 */
public class IteratorMergeOrderTest
{
    private static final class Source
    {
        final int id;
        final int[] items;
        int pos = -1;

        Source(int id, int[] items)
        {
            this.id = id;
            this.items = items;
        }

        int current()
        {
            return items[pos];
        }
    }

    private static final IteratorMergeOrder.Sources<Source> SOURCES = new IteratorMergeOrder.Sources<>()
    {
        public int compare(Source a, Source b)
        {
            return Integer.compare(a.current(), b.current());
        }

        public boolean advance(Source source)
        {
            return ++source.pos < source.items.length;
        }
    };

    @Test
    public void sameOrderAsMergeIterator()
    {
        Random random = new Random(0x1EEDL);
        IteratorMergeOrder<Source> order = new IteratorMergeOrder<>(64, SOURCES);
        for (int round = 0; round < 3000; round++)
        {
            int sourceCount = 1 + random.nextInt(round % 3 == 0 ? 64 : 9);
            int range = 1 + random.nextInt(40);
            int[][] data = new int[sourceCount][];
            for (int s = 0; s < sourceCount; s++)
            {
                int density = random.nextInt(101);
                List<Integer> items = new ArrayList<>();
                for (int v = 0; v < range; v++)
                    if (random.nextInt(100) < density)
                        items.add(v);
                data[s] = items.stream().mapToInt(Integer::intValue).toArray();
            }

            List<List<Integer>> expected = mergeIteratorOrder(data);

            Source[] sources = new Source[sourceCount];
            for (int s = 0; s < sourceCount; s++)
                sources[s] = new Source(s, data[s]);
            // reuse one order across rounds, as the compaction does
            order.reset(sources, sourceCount);
            List<List<Integer>> actual = new ArrayList<>();
            Source[] into = new Source[sourceCount];
            int n;
            while ((n = order.next(into)) > 0)
            {
                List<Integer> step = new ArrayList<>();
                for (int i = 0; i < n; i++)
                    step.add(into[i].id);
                actual.add(step);
            }
            assertEquals("round " + round + ", " + sourceCount + " sources", expected, actual);
        }
    }

    /** The source index of every item the reducer gets, step by step. */
    private static List<List<Integer>> mergeIteratorOrder(int[][] data)
    {
        List<Iterator<Integer>> iterators = new ArrayList<>();
        for (int[] items : data)
        {
            List<Integer> list = new ArrayList<>();
            for (int v : items)
                list.add(v);
            iterators.add(list.iterator());
        }
        List<List<Integer>> steps = new ArrayList<>();
        MergeIterator<Integer, List<Integer>> merge = MergeIterator.get(iterators, Comparator.naturalOrder(), new MergeIterator.Reducer<>()
        {
            List<Integer> step;

            public void reduce(int idx, Integer current)
            {
                step.add(idx);
            }

            protected List<Integer> getReduced()
            {
                return step;
            }

            protected void onKeyChange()
            {
                step = new ArrayList<>();
            }
        });
        while (merge.hasNext())
            steps.add(merge.next());
        return steps;
    }

    private static final class BoundedSource extends AbstractIterator<Integer> implements IteratorWithLowerBound<Integer>
    {
        final int index;
        final List<Integer> items;
        final Integer lowerBound;
        int position = -1;

        BoundedSource(int index, List<Integer> items, Integer lowerBound)
        {
            this.index = index;
            this.items = items;
            this.lowerBound = lowerBound;
        }

        protected Integer computeNext()
        {
            return ++position < items.size() ? items.get(position) : endOfData();
        }

        public Integer lowerBound()
        {
            return lowerBound;
        }
    }

    /** A source as the order reads it: its position in its list, moved by {@link IteratorMergeOrder.LowerBoundSources#advance}. */
    private static final class BoundedCursor
    {
        final BoundedSource source;
        int position = -1;

        BoundedCursor(BoundedSource source)
        {
            this.source = source;
        }

        int item(boolean lowerBound)
        {
            return lowerBound ? source.lowerBound : source.items.get(position);
        }
    }

    @Test
    public void sameOrderAsMergeIteratorWithLowerBounds() throws Exception
    {
        long seed = System.nanoTime();
        Random random = new Random(seed);
        for (int trial = 0; trial < 20_000; trial++)
        {
            int count = 2 + random.nextInt(11);
            int range = 1 + random.nextInt(12);
            List<BoundedSource> sources = new ArrayList<>();
            for (int i = 0; i < count; i++)
            {
                List<Integer> items = new ArrayList<>();
                for (int key = 0; key < range; key++)
                {
                    if (random.nextInt(3) == 0)
                        items.add(key);
                }
                Integer lowerBound = null;
                if (random.nextBoolean())
                {
                    int first = items.isEmpty() ? range : items.get(0);
                    lowerBound = first - random.nextInt(3);
                }
                sources.add(new BoundedSource(i, items, lowerBound));
            }
            String label = "seed " + seed + " trial " + trial;
            assertEquals(label, mergeIteratorStepsWithLowerBounds(sources), orderStepsWithLowerBounds(sources));
        }
    }

    private static List<List<Integer>> mergeIteratorStepsWithLowerBounds(List<BoundedSource> sources)
    {
        List<BoundedSource> fresh = new ArrayList<>();
        for (BoundedSource s : sources)
            fresh.add(new BoundedSource(s.index, s.items, s.lowerBound));
        List<List<Integer>> steps = new ArrayList<>();
        Iterator<List<Integer>> merge = MergeIterator.get(fresh, Integer::compare, new MergeIterator.Reducer<Integer, List<Integer>>()
        {
            List<Integer> step = new ArrayList<>();

            public void reduce(int idx, Integer current)
            {
                step.add(idx);
            }

            protected List<Integer> getReduced()
            {
                return step;
            }

            protected void onKeyChange()
            {
                step = new ArrayList<>();
            }
        });
        while (merge.hasNext())
        {
            List<Integer> step = merge.next();
            if (!step.isEmpty())
                steps.add(step);
        }
        return steps;
    }

    private static List<List<Integer>> orderStepsWithLowerBounds(List<BoundedSource> sources) throws Exception
    {
        BoundedCursor[] cursors = new BoundedCursor[sources.size()];
        boolean[] lowerBounds = new boolean[sources.size()];
        for (int i = 0; i < cursors.length; i++)
        {
            cursors[i] = new BoundedCursor(sources.get(i));
            lowerBounds[i] = sources.get(i).lowerBound != null;
        }
        IteratorMergeOrder<BoundedCursor> order = new IteratorMergeOrder<>(cursors, lowerBounds, new IteratorMergeOrder.LowerBoundSources<BoundedCursor>()
        {
            public int compare(BoundedCursor a, boolean aIsLowerBound, BoundedCursor b, boolean bIsLowerBound)
            {
                return Integer.compare(a.item(aIsLowerBound), b.item(bIsLowerBound));
            }

            public boolean advance(BoundedCursor source, boolean afterLowerBound)
            {
                return ++source.position < source.source.items.size();
            }
        });
        List<List<Integer>> steps = new ArrayList<>();
        BoundedCursor[] into = new BoundedCursor[cursors.length];
        int k;
        while ((k = order.nextWithLowerBounds(into)) > 0)
        {
            List<Integer> step = new ArrayList<>();
            for (int i = 0; i < k; i++)
                step.add(into[i].source.index);
            steps.add(step);
        }
        return steps;
    }
}
