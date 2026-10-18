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

import java.io.IOException;

import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A range read through cursor code only, for a reader that streams a large range to its own
 * consumer (the Arrow Flight bulk reader): the partitions the replica data response of the same
 * command carries, given to a {@link RangeReadSink} as they merge, with no response bytes and no
 * row or cell object.  It never takes the iterator path: a read the cursor path cannot serve is
 * refused when the scan opens, with {@link CursorReadUnsupportedException}.
 *
 * <p>The scan runs in segments.  Each segment holds the table's read order group and its own view
 * of the memtables and sstables, as one read does.  The sink can end a segment after any partition
 * ({@link RangeReadSink#yieldAfterPartition}); the scan then releases the read order group, so
 * flushed memtables and compacted sstables can be freed, and the next {@link #run} reads on from
 * the partition after the last one, like the range pager ({@code PartitionRangeQueryPager}).  A
 * partition is never split between segments.  {@link #lastPartitionKey} is where a scan that
 * failed can start again.
 *
 * <p>Not thread safe.
 */
public final class CursorRangeScan
{
    /** How a {@link #run} ended. */
    public enum ScanEnd
    {
        /** The range is read, or the limit is reached. */
        DONE,
        /** The sink asked to end the segment after a partition; run again to go on. */
        YIELDED,
        /** The sink wants no more ({@link RangeReadSink#wantsMore}). */
        STOPPED
    }

    private final PartitionRangeReadCommand command;
    private final ColumnFamilyStore cfs;
    private final TableMetadata metadata;
    /** The limit counter of the whole scan, across its segments; null when the read has no limit. */
    private final DataLimits.Counter counter;
    /** The caller's filter, ANDed with the command's row filter; null for none. */
    private final ReconciledRowFilter extraFilter;
    private DecoratedKey lastPartitionKey;
    private boolean done;
    private long iteratorLegsOpened;
    private long segments;

    private CursorRangeScan(PartitionRangeReadCommand command, ColumnFamilyStore cfs, ReconciledRowFilter extraFilter)
    {
        this.command = command;
        this.cfs = cfs;
        this.metadata = cfs.metadata();
        this.extraFilter = extraFilter;
        DataLimits limits = command.limits();
        this.counter = limits.isUnlimited() ? null
                                            : limits.newCounter(command.nowInSec(), false, command.selectsFullPartition(), metadata.enforceStrictLiveness());
    }

    /** {@link #open(PartitionRangeReadCommand, ReconciledRowFilter)} with no filter of the caller's. */
    public static CursorRangeScan open(PartitionRangeReadCommand command) throws CursorReadUnsupportedException
    {
        return open(command, null);
    }

    /**
     * Opens a scan of {@code command}'s range.  Nothing is read until {@link #run}.
     *
     * @param filter the caller's filter, checked on reconciled rows with the command's row filter; null for none
     * @throws CursorReadUnsupportedException when the cursor path cannot serve the read, with the reason
     */
    public static CursorRangeScan open(PartitionRangeReadCommand command, ReconciledRowFilter filter) throws CursorReadUnsupportedException
    {
        TableMetadata metadata = command.metadata();
        if (metadata.isVirtual())
            throw new CursorReadUnsupportedException("a virtual table");
        if (command.indexQueryPlan() != null)
            throw new CursorReadUnsupportedException("a read through an index");
        if (command.isDigestQuery())
            throw new CursorReadUnsupportedException("a digest read");
        DataLimits.Kind kind = command.limits().kind();
        if (kind != DataLimits.Kind.CQL_LIMIT)
            throw new CursorReadUnsupportedException("a " + kind + " limit");
        if (CursorReads.querySizeTrackingActive(command))
            throw new CursorReadUnsupportedException("a read that tracks its size");
        if (!ResponseSink.ResponseFilter.streamable(command.rowFilter()))
            throw new CursorReadUnsupportedException("a row filter expression that needs a row object: " + command.rowFilter());
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata);
        checkGate(command, cfs);
        return new CursorRangeScan(command, cfs, filter);
    }

    private static void checkGate(PartitionRangeReadCommand command, ColumnFamilyStore cfs)
    {
        ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(command.dataRange().keyRange()));
        CursorRangeReads.Unsupported reason = CursorRangeReads.unsupported(command, cfs, null, view);
        if (reason != null)
            throw new CursorReadUnsupportedException(reason, "the cursor range read gate: " + reason);
    }

    /**
     * Reads the next segment into {@code sink}: from the start of the range, or the partition after
     * {@link #lastPartitionKey}, until the range is done, the limit is reached, or the sink stops
     * or yields.  The segment's read order group and sources are released before this returns,
     * when it throws too.
     *
     * @throws CursorReadUnsupportedException when the segment's view holds what the cursor path cannot read
     */
    public ScanEnd run(RangeReadSink sink) throws CursorReadUnsupportedException
    {
        if (done)
            return ScanEnd.DONE;
        if (!sink.wantsMore())
            return ScanEnd.STOPPED;
        PartitionRangeReadCommand segment = segmentCommand();
        if (segment == null)
        {
            done = true;
            return ScanEnd.DONE;
        }
        segments++;
        ScanEnd end;
        // the current command, for the checks that look it up while reading, like executeLocally
        ReadCommand.COMMAND.set(segment);
        try (ReadExecutionController controller = ReadExecutionController.forCommand(segment, false))
        {
            end = runSegment(segment, controller, sink);
        }
        finally
        {
            ReadCommand.COMMAND.set(null);
        }
        if (end == ScanEnd.DONE)
            done = true;
        return end;
    }

    private ScanEnd runSegment(PartitionRangeReadCommand segment, ReadExecutionController controller, RangeReadSink sink)
    {
        long startTimeNanos = nanoTime();
        // rejects a read that must go through Accord, like executeLocally
        if (!segment.potentialTxnConflicts().allowed)
            ConsensusRequestRouter.validateSafeToReadNonTransactionally(segment, null);
        ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(segment.dataRange().keyRange()));
        CursorRangeReads.Unsupported reason = CursorRangeReads.unsupported(segment, cfs, controller, view);
        if (reason != null)
            throw new CursorReadUnsupportedException(reason, "the cursor range read gate: " + reason);

        PartitionRangeReadCommand.CursorRangeInputs inputs = segment.cursorRangeInputs(controller, view);
        if (inputs.memtables.isEmpty() && inputs.sstables.isEmpty())
            return ScanEnd.DONE;
        CursorRangeReads.RangeMerge merge = new CursorRangeReads.RangeMerge(segment, cfs, inputs.sstables, inputs.memtables, null, inputs.listener);
        long nowInSec = segment.nowInSec();
        long gcBefore = nowInSec == 0 ? Long.MIN_VALUE : cfs.gcBefore(nowInSec);
        CursorReads.TombstoneScanGuard scanGuard = new CursorReads.TombstoneScanGuard(segment, cfs.metric, startTimeNanos, nowInSec, null);
        ReconciledRowFilter filter = segment.rowFilter().isEmpty() ? null : new ResponseSink.ResponseFilter(segment);
        if (extraFilter != null)
            filter = filter == null ? extraFilter : ReconciledRowFilter.and(filter, extraFilter);
        RangeReadSinkOutput output = new RangeReadSinkOutput(sink, metadata.comparator);
        ResponseSink responseSink = new ResponseSink(output, metadata.comparator, Slices.NONE, segment.isReversed(), metadata.enforceStrictLiveness(),
                                                     nowInSec, gcBefore, cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(),
                                                     controller.oldestUnrepairedTombstone(), scanGuard, counter, false, filter, segment);
        ScanEnd end = ScanEnd.DONE;
        Throwable failure = null;
        try
        {
            end = merge.scanTo(responseSink, output, sink, counter, scanGuard);
        }
        catch (IOException e)
        {
            failure = new RuntimeException("cursor range scan failed", e);
        }
        catch (RuntimeException | Error e)
        {
            failure = e;
        }
        finally
        {
            if (merge.lastPartitionKey != null)
                lastPartitionKey = merge.lastPartitionKey;
            failure = CursorRangeReads.finishRead(cfs, merge, inputs, scanGuard, failure);
            iteratorLegsOpened += merge.iteratorLegs;
        }
        Throwables.maybeFail(failure);
        return end;
    }

    /**
     * The command of the next segment: the whole range for the first, else the range after
     * {@link #lastPartitionKey}, as {@code PartitionRangeQueryPager.nextPageReadQuery} makes it for
     * a page that ends with a whole partition; null when nothing is left.
     */
    private PartitionRangeReadCommand segmentCommand()
    {
        if (lastPartitionKey == null)
            return command;
        AbstractBounds<PartitionPosition> range = command.dataRange().keyRange();
        // the last partition read is the one at the end of the range: nothing is left
        if (lastPartitionKey.equals(range.right))
            return null;
        // PartitionRangeQueryPager.makeKeyBounds, without the last key
        AbstractBounds<PartitionPosition> rest = range instanceof Range || range instanceof Bounds
                                                 ? new Range<>(lastPartitionKey, range.right)
                                                 : new ExcludingBounds<>(lastPartitionKey, range.right);
        return command.forSubRange(rest, true);
    }

    /**
     * The key of the last partition the scan read, whether or not the sink got it; null before the
     * first.  A scan of {@code command.forSubRange} after this key reads what this scan has not.
     */
    public DecoratedKey lastPartitionKey()
    {
        return lastPartitionKey;
    }

    /** Whether the range is read, or the limit is reached. */
    public boolean isDone()
    {
        return done;
    }

    /** Partitions the scan took as objects from an iterator: always 0, which a caller may check. */
    public long iteratorLegsOpened()
    {
        return iteratorLegsOpened;
    }

    /** The segments the scan has run. */
    public long segments()
    {
        return segments;
    }
}
