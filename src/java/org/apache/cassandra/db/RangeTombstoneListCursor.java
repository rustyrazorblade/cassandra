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

/**
 * Walks a {@link RangeTombstoneList} plus a partition-level deletion, producing the same
 * bound/boundary marker sequence {@link org.apache.cassandra.db.rows.RowAndDeletionMergeIterator}
 * would for its range-tombstone side, without allocating a {@link RangeTombstone} wrapper per
 * list entry, a merge-iterator, or (for a plain bound) a boundary wrapper object.
 * <p>
 * This mirrors {@code RowAndDeletionMergeIterator}'s {@code computeNextInternal}/
 * {@code closeOpenedRange}/{@code openRange}/{@code updateNextRange} specialized to the
 * range-tombstone side, indexed directly into the list's parallel arrays instead of pulling
 * {@link RangeTombstone} objects from an {@code Iterator}. Row interleaving (deciding, at each
 * step, whether the next row or this cursor's next marker sorts first) is the caller's job —
 * mirroring how {@code RowAndDeletionMergeIterator.computeNextInternal} compares
 * {@code nextRow.clustering()} against {@code openBound(nextRange)}/{@code closeBound(openRange)}.
 * <p>
 * {@link #moveNext} advances by exactly one list entry per call — it does NOT internally retry
 * past a noop boundary (both sides carrying equal deletion times, CASSANDRA-14894) the way
 * {@code RowAndDeletionMergeIterator.computeNext}'s {@code shouldSkip} retry loop does. That
 * retry has to happen one level up, the same place {@code computeNext} does it: between two
 * merged same-deletion segments a row can still sort in — collapsing the retry into this class
 * would hide that decision point from a caller interleaving rows. See {@link Result#SKIPPED}.
 * <p>
 * {@link #openDeletion} is cached, not recomputed from {@link RangeTombstoneList#deletionAt}
 * on every call: it changes only when {@link #moveNext} actually opens a new range (including
 * merging one via a boundary), so a caller consulting it once per interleaved row — not once
 * per range-tombstone transition — still costs one allocation per transition, not per row.
 */
public class RangeTombstoneListCursor
{
    public enum Result
    {
        /** Exhausted — mirrors {@code endOfData()}. */
        NONE,
        /** A marker is ready; read it via {@link #kind()}/{@link #valuesSource()}/{@link #markerCloseDeletion()}/{@link #markerOpenDeletion()}. */
        MARKER,
        /**
         * Internal state advanced past a noop boundary (equal deletion times on both sides) —
         * no marker produced. The caller should re-check {@link #peekPosition()} against its
         * other interleaved source (mirrors {@code computeNext}'s {@code shouldSkip} retry,
         * which re-enters {@code computeNextInternal} and so re-considers rows) and call
         * {@link #moveNext} again.
         */
        SKIPPED
    }

    private final RangeTombstoneList list;
    private final DeletionTime partitionLevelDeletion;
    private final boolean removeShadowedData;
    private final ClusteringComparator comparator;

    // next not-yet-examined list index
    private int nextIndex;
    private boolean hasOpen;
    // index whose end() is the currently open range's close point, valid iff hasOpen
    private int openIndex;
    // the currently open range's deletion, cached at the transition that opened it - see class doc
    private DeletionTime openDeletion;

    // current marker, valid after moveNext() returns MARKER; see UnfilteredDescriptor#storeMarker.
    // Named for their role on THIS marker (close/open side), distinct from the openDeletion
    // field above (the currently-open RANGE's deletion, a different concept: a plain close
    // marker's markerCloseDeletion equals the range's openDeletion, but a boundary's
    // markerOpenDeletion is the NEXT range's deletion, not yet reflected in openDeletion when
    // the marker fields are set - see moveNext.
    private ClusteringPrefix.Kind kind;
    private ClusteringBound<?> valuesSource;
    private DeletionTime markerCloseDeletion;
    private DeletionTime markerOpenDeletion;

    public RangeTombstoneListCursor(RangeTombstoneList list, DeletionTime partitionLevelDeletion, boolean removeShadowedData)
    {
        this.list = list;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.removeShadowedData = removeShadowedData;
        this.comparator = list.comparator();
    }

    /** Mirrors updateNextRange's shadow/empty filtering, applied to list entry i. */
    private boolean isSurviving(int i)
    {
        return (!removeShadowedData || list.deletionAt(i).supersedes(partitionLevelDeletion))
               && !Slice.isEmpty(comparator, list.startAt(i), list.endAt(i));
    }

    private void advanceNextIndex()
    {
        while (nextIndex < list.size() && !isSurviving(nextIndex))
            nextIndex++;
    }

    /** Advances by exactly one list entry; see {@link Result}. */
    public Result moveNext()
    {
        if (hasOpen)
        {
            advanceNextIndex();
            if (nextIndex < list.size() && comparator.compare(list.endAt(openIndex), list.startAt(nextIndex)) == 0)
            {
                ClusteringBound<?> closeBound = list.endAt(openIndex);
                ClusteringBound<?> openBound = list.startAt(nextIndex);
                DeletionTime closeDeletion = openDeletion;
                DeletionTime newOpenDeletion = list.deletionAt(nextIndex);
                openIndex = nextIndex;
                openDeletion = newOpenDeletion;
                nextIndex++;
                // hasOpen stays true: the merged range is now open, closing at openIndex's end

                if (closeDeletion.equals(newOpenDeletion))
                    return Result.SKIPPED; // noop boundary: one continuous open range, no marker

                // See RangeTombstoneBoundaryMarker.makeBoundary: the tie only needs breaking
                // when both sides are inclusive (otherwise exactly one side is exclusive and
                // that alone decides it); values always come from the close side.
                boolean isExclusiveClose = closeBound.isExclusive()
                                           || (closeBound.isInclusive() && openBound.isInclusive()
                                               && newOpenDeletion.supersedes(closeDeletion));
                kind = isExclusiveClose ? ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY
                                        : ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY;
                valuesSource = closeBound;
                markerCloseDeletion = closeDeletion;
                markerOpenDeletion = newOpenDeletion;
                return Result.MARKER;
            }
            else
            {
                kind = list.endAt(openIndex).kind();
                valuesSource = list.endAt(openIndex);
                markerCloseDeletion = openDeletion;
                markerOpenDeletion = DeletionTime.LIVE;
                hasOpen = false;
                return Result.MARKER;
            }
        }
        else
        {
            advanceNextIndex();
            if (nextIndex >= list.size())
                return Result.NONE;
            kind = list.startAt(nextIndex).kind();
            valuesSource = list.startAt(nextIndex);
            markerCloseDeletion = list.deletionAt(nextIndex);
            markerOpenDeletion = DeletionTime.LIVE;
            openIndex = nextIndex;
            openDeletion = markerCloseDeletion;
            nextIndex++;
            hasOpen = true;
            return Result.MARKER;
        }
    }

    /**
     * The clustering position of whatever marker {@link #moveNext} would produce next, without
     * consuming it — null once exhausted. Lets a caller interleaving this cursor with another
     * sorted source (rows) decide, each step, which sorts first, mirroring how
     * {@code RowAndDeletionMergeIterator.computeNextInternal} compares {@code nextRow.clustering()}
     * against {@code openBound(nextRange)}/{@code closeBound(openRange)} before consuming either.
     * (Advances past shadowed/empty entries as a side effect — that skip is unconditional
     * cleanup, not a row-dependent decision, so it's safe outside of {@code moveNext}.)
     */
    public ClusteringBound<?> peekPosition()
    {
        if (hasOpen)
            return list.endAt(openIndex);
        advanceNextIndex();
        return nextIndex < list.size() ? list.startAt(nextIndex) : null;
    }

    /** True between a plain/boundary open and its eventual close — i.e. there's an active deletion. */
    public boolean hasOpen()
    {
        return hasOpen;
    }

    /**
     * The currently open range's deletion time — cached at the transition that opened it, see
     * class doc. Only valid when {@link #hasOpen()}.
     */
    public DeletionTime openDeletion()
    {
        assert hasOpen : "openDeletion() called with no range open";
        return openDeletion;
    }

    public ClusteringPrefix.Kind kind()
    {
        return kind;
    }

    public ClusteringBound<?> valuesSource()
    {
        return valuesSource;
    }

    /** Maps directly to {@link UnfilteredDescriptor#storeMarker}'s {@code close} argument. */
    public DeletionTime markerCloseDeletion()
    {
        return markerCloseDeletion;
    }

    /** Maps directly to {@link UnfilteredDescriptor#storeMarker}'s {@code open} argument; only meaningful when {@link #kind()} is a boundary. */
    public DeletionTime markerOpenDeletion()
    {
        return markerOpenDeletion;
    }
}
