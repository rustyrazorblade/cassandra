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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins {@link RangeTombstoneListCursor} byte-for-byte (well, decision-for-decision) against
 * {@link RowAndDeletionMergeIterator}'s range-tombstone-marker output, for every RT
 * configuration this differs on: disjoint ranges, adjacent ranges with equal deletion (must
 * silently merge, no marker), adjacent ranges with differing deletion under every
 * inclusive/exclusive combination (must produce the right boundary kind), ranges shadowed by
 * the partition-level deletion, and unbounded ranges — plus a randomized sweep. This is the
 * highest-risk piece of the memtable cursor flush path: any divergence here is a silent
 * correctness bug in flushed range tombstones.
 */
public class RangeTombstoneListCursorDifferentialTest
{
    private static final ClusteringComparator cmp = new ClusteringComparator(Int32Type.instance);
    private static TableMetadata metadata;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("ks", "t1")
                                .offline()
                                .addPartitionKeyColumn("k", Int32Type.instance)
                                .addClusteringColumn("c", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .partitioner(Murmur3Partitioner.instance)
                                .build();
    }

    private static ClusteringBound<?> bound(boolean isStart, boolean isInclusive, int value)
    {
        return ClusteringBound.create(cmp, isStart, isInclusive, value);
    }

    private static ClusteringBound<?> unbounded(boolean isStart)
    {
        return isStart ? BufferClusteringBound.BOTTOM : BufferClusteringBound.TOP;
    }

    private static void add(RangeTombstoneList list, ClusteringBound<?> start, ClusteringBound<?> end, long ts, int delTime)
    {
        list.add(new RangeTombstone(Slice.make(start, end), DeletionTime.build(ts, delTime)));
    }

    private static final class MarkerEvent
    {
        final ClusteringPrefix.Kind kind;
        final List<ByteBuffer> values;
        final DeletionTime d1;
        final DeletionTime d2;

        MarkerEvent(ClusteringPrefix.Kind kind, List<ByteBuffer> values, DeletionTime d1, DeletionTime d2)
        {
            this.kind = kind;
            this.values = values;
            this.d1 = d1;
            this.d2 = d2;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof MarkerEvent))
                return false;
            MarkerEvent that = (MarkerEvent) o;
            return kind == that.kind && values.equals(that.values)
                   && d1.markedForDeleteAt() == that.d1.markedForDeleteAt()
                   && d1.localDeletionTime() == that.d1.localDeletionTime()
                   && d2.markedForDeleteAt() == that.d2.markedForDeleteAt()
                   && d2.localDeletionTime() == that.d2.localDeletionTime();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, values, d1.markedForDeleteAt(), d1.localDeletionTime(), d2.markedForDeleteAt(), d2.localDeletionTime());
        }

        @Override
        public String toString()
        {
            return kind + " " + values + " d1=" + d1 + " d2=" + d2;
        }
    }

    private static List<ByteBuffer> valuesOf(ClusteringPrefix<?> prefix)
    {
        return Arrays.asList(prefix.getBufferArray());
    }

    private static List<MarkerEvent> viaCursor(RangeTombstoneList list, DeletionTime partitionDeletion, boolean removeShadowedData)
    {
        List<MarkerEvent> events = new ArrayList<>();
        RangeTombstoneListCursor cursor = new RangeTombstoneListCursor(list, partitionDeletion, removeShadowedData);
        while (true)
        {
            // peekPosition()/hasOpen()/openDeletion() must agree with what moveNext() actually
            // does — the contract a row-interleaving caller relies on to decide, without
            // consuming, whether a row or this cursor's next marker sorts first. This loop plays
            // the role of RowAndDeletionMergeIterator.computeNext()'s shouldSkip retry: SKIPPED
            // means try again (a real caller would re-check rows here too).
            ClusteringBound<?> peeked = cursor.peekPosition();
            boolean hadOpenBeforeMove = cursor.hasOpen();
            DeletionTime openDeletionBeforeMove = hadOpenBeforeMove ? cursor.openDeletion() : null;
            RangeTombstoneListCursor.Result result = cursor.moveNext();
            assertEquals(result != RangeTombstoneListCursor.Result.NONE, peeked != null);
            if (result == RangeTombstoneListCursor.Result.NONE)
                break;
            if (result == RangeTombstoneListCursor.Result.SKIPPED)
                continue;
            assertEquals(0, cmp.compare(peeked, cursor.valuesSource()));
            if (hadOpenBeforeMove)
                assertTrue(openDeletionBeforeMove.equals(cursor.markerCloseDeletion()));
            events.add(new MarkerEvent(cursor.kind(), valuesOf(cursor.valuesSource()), cursor.markerCloseDeletion(), cursor.markerOpenDeletion()));
        }
        return events;
    }

    private static List<MarkerEvent> viaIterator(RangeTombstoneList list, DeletionTime partitionDeletion, boolean removeShadowedData)
    {
        RowAndDeletionMergeIterator iter = new RowAndDeletionMergeIterator(metadata,
                                                                           metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0)),
                                                                           partitionDeletion,
                                                                           ColumnFilter.all(metadata),
                                                                           Rows.EMPTY_STATIC_ROW,
                                                                           false,
                                                                           EncodingStats.NO_STATS,
                                                                           Collections.emptyIterator(),
                                                                           list.iterator(),
                                                                           removeShadowedData);
        List<MarkerEvent> events = new ArrayList<>();
        try
        {
            while (iter.hasNext())
            {
                Unfiltered u = iter.next();
                assertTrue("expected only range tombstone markers (no rows fed in)", u.isRangeTombstoneMarker());
                RangeTombstoneMarker marker = (RangeTombstoneMarker) u;
                DeletionTime d1;
                DeletionTime d2;
                if (marker.isBoundary())
                {
                    RangeTombstoneBoundaryMarker b = (RangeTombstoneBoundaryMarker) marker;
                    d1 = b.closeDeletionTime(false);
                    d2 = b.openDeletionTime(false);
                }
                else
                {
                    d1 = ((RangeTombstoneBoundMarker) marker).deletionTime();
                    d2 = DeletionTime.LIVE;
                }
                events.add(new MarkerEvent(marker.clustering().kind(), valuesOf(marker.clustering()), d1, d2));
            }
        }
        finally
        {
            iter.close();
        }
        return events;
    }

    private static void assertMatches(RangeTombstoneList list, DeletionTime partitionDeletion, boolean removeShadowedData)
    {
        List<MarkerEvent> expected = viaIterator(list, partitionDeletion, removeShadowedData);
        List<MarkerEvent> actual = viaCursor(list, partitionDeletion, removeShadowedData);
        assertEquals(expected, actual);
    }

    private static void assertMatchesBothShadowModes(RangeTombstoneList list, DeletionTime partitionDeletion)
    {
        assertMatches(list, partitionDeletion, true);
        assertMatches(list, partitionDeletion, false);
    }

    @Test
    public void disjointRanges()
    {
        RangeTombstoneList list = new RangeTombstoneList(cmp, 4);
        add(list, bound(true, true, 1), bound(false, true, 5), 10, 0);
        add(list, bound(true, true, 10), bound(false, false, 15), 20, 0);
        add(list, bound(true, false, 20), bound(false, true, 25), 30, 0);
        assertMatchesBothShadowModes(list, DeletionTime.LIVE);
    }

    @Test
    public void adjacentSameDeletionMergesSilently()
    {
        RangeTombstoneList list = new RangeTombstoneList(cmp, 2);
        // [1,5] and [5,10] touching at 5, same deletion — RangeTombstoneList itself would
        // normally coalesce this on insert, so force it via addAll from two separately-built
        // lists to hit the "list doesn't merge same-ts across addAll" quirk CASSANDRA-14894
        // documents.
        RangeTombstoneList a = new RangeTombstoneList(cmp, 1);
        add(a, bound(true, true, 1), bound(false, true, 5), 10, 0);
        RangeTombstoneList b = new RangeTombstoneList(cmp, 1);
        add(b, bound(true, false, 5), bound(false, true, 10), 10, 0);
        list.addAll(a);
        list.addAll(b);
        assertMatchesBothShadowModes(list, DeletionTime.LIVE);
    }

    @Test
    public void adjacentDifferentDeletionProducesBoundary()
    {
        // inclusive-end/exclusive-start touching at 5: unambiguous EXCL_END_INCL_START... wait,
        // the *close* side here is inclusive, so this is INCL_END_EXCL_START_BOUNDARY.
        RangeTombstoneList list = new RangeTombstoneList(cmp, 2);
        add(list, bound(true, true, 1), bound(false, true, 5), 10, 0);
        add(list, bound(true, false, 5), bound(false, true, 10), 20, 0);
        assertMatchesBothShadowModes(list, DeletionTime.LIVE);
    }

    @Test
    public void adjacentDifferentDeletionExclusiveCloseInclusiveOpen()
    {
        RangeTombstoneList list = new RangeTombstoneList(cmp, 2);
        add(list, bound(true, true, 1), bound(false, false, 5), 10, 0);
        add(list, bound(true, true, 5), bound(false, true, 10), 20, 0);
        assertMatchesBothShadowModes(list, DeletionTime.LIVE);
    }

    /**
     * Neither adjacent-boundary test above has BOTH sides inclusive at the shared point: one
     * side is always exclusive, so which range "owns" that point is unambiguous before the
     * tombstones are even merged. [1,5] and [5,10], both closed at 5, genuinely overlap AT 5 -
     * this is the one shape that reaches {@link RangeTombstoneListCursor#moveNext}'s
     * {@code newOpenDeletion.supersedes(closeDeletion)} tie-break (mirroring
     * {@link org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker#makeBoundary}), added in
     * both priority orders since the tie-break's result flips with it.
     */
    @Test
    public void overlappingBothInclusiveHigherTimestampWins()
    {
        RangeTombstoneList list = new RangeTombstoneList(cmp, 2);
        add(list, bound(true, true, 1), bound(false, true, 5), 10, 0);
        add(list, bound(true, true, 5), bound(false, true, 10), 20, 0);
        assertMatchesBothShadowModes(list, DeletionTime.LIVE);
    }

    @Test
    public void overlappingBothInclusiveLowerTimestampWins()
    {
        RangeTombstoneList list = new RangeTombstoneList(cmp, 2);
        add(list, bound(true, true, 1), bound(false, true, 5), 20, 0);
        add(list, bound(true, true, 5), bound(false, true, 10), 10, 0);
        assertMatchesBothShadowModes(list, DeletionTime.LIVE);
    }

    @Test
    public void unboundedRanges()
    {
        RangeTombstoneList list = new RangeTombstoneList(cmp, 2);
        add(list, unbounded(true), bound(false, true, 5), 10, 0);
        add(list, bound(true, false, 5), unbounded(false), 20, 0);
        assertMatchesBothShadowModes(list, DeletionTime.LIVE);
    }

    @Test
    public void partitionDeletionShadowsLowerTimestampRanges()
    {
        RangeTombstoneList list = new RangeTombstoneList(cmp, 3);
        add(list, bound(true, true, 1), bound(false, true, 5), 5, 0);   // shadowed: ts < partition
        add(list, bound(true, true, 10), bound(false, true, 15), 50, 0); // survives: ts > partition
        assertMatchesBothShadowModes(list, DeletionTime.build(20, 0));
    }

    @Test
    public void randomSweep()
    {
        Random random = new Random(20260807);
        for (int trial = 0; trial < 200; trial++)
        {
            int entries = 1 + random.nextInt(12);
            RangeTombstoneList list = new RangeTombstoneList(cmp, entries);
            for (int i = 0; i < entries; i++)
            {
                int a = random.nextInt(50);
                int b = a + random.nextInt(5);
                boolean startIncl = random.nextBoolean();
                boolean endIncl = random.nextBoolean();
                if (a == b && !(startIncl && endIncl))
                    continue; // degenerate empty slice, skip
                long ts = random.nextInt(30);
                add(list, bound(true, startIncl, a), bound(false, endIncl, b), ts, 0);
            }
            DeletionTime partitionDeletion = random.nextBoolean() ? DeletionTime.LIVE : DeletionTime.build(random.nextInt(20), 0);
            assertMatchesBothShadowModes(list, partitionDeletion);
        }
    }
}
