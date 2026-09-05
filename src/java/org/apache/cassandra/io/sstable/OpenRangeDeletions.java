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
package org.apache.cassandra.io.sstable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;

/**
 * The range deletions open at the merge's current position, and which of them is in effect.
 * Follows {@code RangeTombstoneMarker.Merger.updateOpenMarkers}, with one addition: a close with
 * no matching open is a hard error rather than a silent no-op.
 *
 * The set is fed one merged marker at a time through {@link #apply}, then settled with
 * {@link #settle}. A close that removes the marker currently in effect invalidates it, and the
 * replacement is found by a rescan in {@link #settle} rather than eagerly, so a run of closes
 * costs one scan instead of one per close.
 *
 * Allocation-free in steady state: every open deletion is a {@link ReusableDeletionTime} taken
 * from {@link #copyOf}'s pool and returned by {@link #recycle}. The merge loops also take pooled
 * copies for their own marker shaping, so both entry points are public.
 */
public final class OpenRangeDeletions
{
    /** Null means a close invalidated the previous winner and {@link #settle} must rescan. */
    private DeletionTime active = DeletionTime.LIVE;
    private final List<ReusableDeletionTime> open = new ArrayList<>();
    private final ArrayDeque<ReusableDeletionTime> pool = new ArrayDeque<>();

    /** The deletion in effect at the current position, or {@link DeletionTime#LIVE}. */
    public DeletionTime active()
    {
        return active;
    }

    /** Seeds the deletion already open at a mid-partition seek point. */
    public void seedActive(DeletionTime openAtSeekPoint)
    {
        active = openAtSeekPoint;
    }

    public void clear()
    {
        open.clear();
        active = DeletionTime.LIVE;
    }

    /**
     * Applies one merged marker. A marker shadowed by {@code partitionDeletion} is ignored, since
     * it can never be in effect.
     */
    public void apply(UnfilteredDescriptor marker, DeletionTime partitionDeletion)
    {
        if (marker.isStartBound())
        {
            open(marker.deletionTime(), partitionDeletion);
        }
        else if (marker.isEndBound())
        {
            removeOpen(marker.deletionTime(), partitionDeletion, marker);
        }
        else if (marker.isBoundary())
        {
            removeOpen(marker.deletionTime(), partitionDeletion, marker);
            open(marker.deletionTime2(), partitionDeletion);
        }
        else
        {
            throw new IllegalStateException("Unexpected bound type:" + marker.clusteringKind());
        }
    }

    /** Resolves the winner after a group of {@link #apply} calls. */
    public void settle()
    {
        if (active == null)
            recalculate();
    }

    /** A pooled copy, so an open deletion outlives the descriptor it was read from. */
    public ReusableDeletionTime copyOf(DeletionTime deletionTime)
    {
        ReusableDeletionTime reusable = pool.pollLast();
        if (reusable == null)
            reusable = ReusableDeletionTime.copy(deletionTime);
        else
            reusable.reset(deletionTime);
        return reusable;
    }

    /** Returns a copy taken from {@link #copyOf} once the caller is done with it. */
    public void recycle(ReusableDeletionTime reusable)
    {
        pool.offer(reusable);
    }

    /**
     * Opens a range deletion directly, rather than through a marker. The read path seeds the
     * deletion already open at each leg's mid-partition seek point this way.
     */
    public void open(DeletionTime openRangeDeletion, DeletionTime partitionDeletion)
    {
        if (!partitionDeletion.isLive() && !openRangeDeletion.supersedes(partitionDeletion))
            return;

        ReusableDeletionTime reusable = copyOf(openRangeDeletion);
        open.add(reusable);
        // A pending rescan (active == null) wins: it will pick this one up too.
        if (active != null && (active == DeletionTime.LIVE || reusable.supersedes(active)))
            active = reusable;
    }

    private void removeOpen(DeletionTime closeRangeDeletion, DeletionTime partitionDeletion, UnfilteredDescriptor marker)
    {
        if (!partitionDeletion.isLive() && !closeRangeDeletion.supersedes(partitionDeletion))
            return;

        int size = open.size();
        int j = 0;
        ReusableDeletionTime matched = null;
        for (; j < size; j++)
        {
            matched = open.get(j);
            if (matched.equals(closeRangeDeletion))
                break;
        }
        if (j == size)
            throw new IllegalStateException("Expected an open marker for this closing marker:" + marker);

        if (matched == active)
            active = null; // trigger the rescan in settle()

        recycle(matched);
        if (size == 1)
        {
            open.clear();
        }
        else
        {
            // avoid the array copy: swap in the last element
            ReusableDeletionTime last = open.remove(size - 1);
            if (j != size - 1)
                open.set(j, last);
        }
    }

    private void recalculate()
    {
        int size = open.size();
        if (size == 0)
        {
            active = DeletionTime.LIVE;
            return;
        }
        DeletionTime max = open.get(0);
        for (int i = 1; i < size; i++)
        {
            DeletionTime candidate = open.get(i);
            if (candidate.supersedes(max))
                max = candidate;
        }
        active = max;
    }
}
