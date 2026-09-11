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

package org.apache.cassandra.db.repair;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.DigestingCursorMergeSink;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.PrecomputedDigestPartition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.ValidationPartitionIterator;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Cursor-backed counterpart to {@link CassandraValidationIterator}: same lifecycle (snapshot
 * selection, {@link CassandraValidationIterator#getSSTablesToValidate}, memtable-range flush,
 * estimated-bytes/partitions bookkeeping), but reads sstables via partial-range
 * {@link org.apache.cassandra.db.compaction.StatefulCursor}s and merges through
 * {@link CursorCompactor#mergeNextPartition} + {@link DigestingCursorMergeSink} instead of
 * {@link org.apache.cassandra.db.compaction.CompactionIterator} - never writes output sstables
 * and never materializes real row/cell content, only a precomputed per-partition digest (see
 * {@link PrecomputedDigestPartition}) - so both {@link ValidationCompactionController} (repair
 * always purges, see {@code ValidationCompactionControllerTest} for the invariant that makes this
 * safe regardless of the table's {@code provide_overlapping_tombstones} setting) and
 * {@link CursorCompactor}'s scan-side gates apply identically to what
 * {@link CassandraValidationIterator} already uses. Overrides {@link #feed} to read the precomputed
 * digest off {@link PrecomputedDigestPartition} directly, instead of the default that would
 * re-digest {@link #next()}'s result as a real iterator - {@code ValidationManager#doValidation}'s
 * driving loop calls {@code feed} polymorphically and never needs to know this iterator produces
 * precomputed digests rather than real content.
 * <p>
 * Checks support itself, in two stages, throwing {@link CursorValidationUnsupportedException} if
 * either rejects - callers (see {@link CassandraTableRepairManager}) catch that to fall back to
 * {@link CassandraValidationIterator}. First, {@link CursorCompactor#unsupportedMetadata} - cheap,
 * metadata-only (2i index, Accord keyspace, non-reusable-key partitioner) - runs before any
 * flush/snapshot-lookup I/O, so a permanently-unsupported schema doesn't pay for a redundant
 * blocking flush and sstable-set computation on every repair before falling back. Second,
 * {@link CursorCompactor#isValidationSupported} runs against whichever sstable set this iterator
 * actually acquired (live-selected or snapshot) - deliberately not pre-checked against an
 * approximation, since that could diverge from the real set (e.g. a snapshot's older on-disk
 * sstables vs. the live ones a cheap pre-check would see).
 */
public class CursorValidationIterator extends ValidationPartitionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(CursorValidationIterator.class);

    private final ColumnFamilyStore cfs;
    private final Refs<SSTableReader> sstables;
    private final String snapshotName;
    private final boolean isGlobalSnapshotValidation;
    private final boolean isSnapshotValidation;

    private final ValidationCompactionController controller;
    private final DigestingCursorMergeSink sink;
    private final CursorCompactor compactor;

    private final long estimatedBytes;
    private final long estimatedPartitions;
    private final Map<Range<Token>, Long> rangePartitionCounts;

    private boolean nextReady = false;

    public CursorValidationIterator(ColumnFamilyStore cfs, SharedContext ctx, Collection<Range<Token>> ranges, TimeUUID parentId, TimeUUID sessionID, boolean isIncremental, long nowInSec, boolean dontPurgeTombstones, TopPartitionTracker.Collector topPartitionCollector) throws IOException, NoSuchRepairSessionException
    {
        this.cfs = cfs;

        // Cheap, metadata-only rejection (2i index, Accord keyspace, non-reusable-key
        // partitioner) before any flush/snapshot-lookup I/O - so a permanently-unsupported
        // schema fails over on every repair without paying for a redundant blocking flush and
        // sstable-set computation that CassandraValidationIterator's constructor is about to
        // repeat anyway. The sstable-set-dependent part of support (per-reader version, actual
        // controller type) still has to wait until the real set below is acquired.
        if (CursorCompactor.unsupportedMetadata(cfs.metadata()))
            throw new CursorValidationUnsupportedException(
                "CursorCompactor.unsupportedMetadata rejected " + cfs.getKeyspaceName() + '.' + cfs.getTableName());

        isGlobalSnapshotValidation = SnapshotManager.instance.exists(cfs.getKeyspaceName(), cfs.getTableName(), parentId.toString());
        snapshotName = isGlobalSnapshotValidation ? parentId.toString() : sessionID.toString();
        isSnapshotValidation = SnapshotManager.instance.exists(cfs.getKeyspaceName(), cfs.getTableName(), snapshotName);

        if (isSnapshotValidation)
        {
            // If there is a snapshot created for the session then read from there.
            // note that we populate the parent repair session when creating the snapshot, meaning the sstables in the snapshot are the ones we
            // are supposed to validate.
            sstables = TableSnapshot.getSnapshotSSTableReaders(cfs, snapshotName);
        }
        else
        {
            if (!isIncremental)
            {
                // flush first so everyone is validating data that is as similar as possible
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.VALIDATION);
                // Note: we also flush for incremental repair during the anti-compaction process.
            }
            sstables = CassandraValidationIterator.getSSTablesToValidate(cfs, ctx, ranges, parentId, isIncremental);
        }

        // Check support against the ACTUAL sstable set this iterator would validate (which for
        // isSnapshotValidation may be an older, independent set of on-disk files the live
        // sstables give no indication of) before any further side effects, so an unsupported
        // case fails narrowly here rather than partway through real work. Callers (see
        // CassandraTableRepairManager) catch CursorValidationUnsupportedException to fall back
        // to CassandraValidationIterator.
        long gcBefore = dontPurgeTombstones ? Long.MIN_VALUE : cfs.getDefaultGcBefore(nowInSec);
        controller = new ValidationCompactionController(cfs, gcBefore);
        if (!CursorCompactor.isValidationSupported(sstables, controller))
        {
            controller.close();
            sstables.release();
            throw new CursorValidationUnsupportedException(
                "CursorCompactor.isValidationSupported rejected " + cfs.getKeyspaceName() + '.' + cfs.getTableName() +
                "'s " + sstables.size() + " sstable(s) to validate");
        }

        // Everything past this point can throw after `sstables` (Refs) and `controller` have been
        // acquired - e.g. getParentRepairSession throwing NoSuchRepairSessionException if the repair
        // session is torn down mid-setup, or getPositionsForRanges / the CursorCompactor constructor
        // failing. Since close() never runs on a constructor that throws, release the refs and close
        // the controller (and a partially-built compactor) here before rethrowing, rather than
        // leaking them.
        CursorCompactor compactorLocal = null;
        try
        {
            // Persistent memtables will not flush or snapshot to sstables, make an sstable with their data.
            cfs.writeAndAddMemtableRanges(parentId,
                                          () -> Collections2.transform(Range.normalize(ranges), Range::makeRowRange),
                                          sstables);

            ActiveRepairService.ParentRepairSession prs = ctx.repair().getParentRepairSession(parentId);
            logger.info("{}, parentSessionId={}: Performing cursor-backed validation compaction on {} sstables in {}.{}",
                        prs.previewKind.logPrefix(sessionID),
                        parentId,
                        sstables.size(),
                        cfs.getKeyspaceName(),
                        cfs.getTableName());

            // An sstable can pass the coarse, span-level filter that selected `sstables` (its
            // first/last token range merely intersects the repair ranges) while having zero keys
            // actually inside them - e.g. a sparse sstable whose few partitions all fall outside this
            // particular sub-range. getPositionsForRanges(ranges) then legitimately returns an empty
            // list; skip that sstable entirely rather than passing empty bounds into
            // StatefulCursor.positionAt, which requires (and asserts) a non-empty list.
            Map<SSTableReader, List<PartitionPositionBounds>> boundsBySSTable = Maps.newHashMapWithExpectedSize(sstables.size());
            for (SSTableReader sstable : sstables)
            {
                List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(ranges);
                if (!bounds.isEmpty())
                    boundsBySSTable.put(sstable, bounds);
            }

            sink = new DigestingCursorMergeSink(cfs.metadata());
            // Register with CompactionManager.instance.active (like legacy ValidationCompactionIterator)
            // so this validation is visible to nodetool compactionstats and interruptible via
            // nodetool stop VALIDATION / DROP TABLE / decommission / drain (see the isStopRequested
            // checks in CursorCompactor).
            compactorLocal = new CursorCompactor(OperationType.VALIDATION, boundsBySSTable, controller, nowInSec, nextTimeUUID(),
                                                 CompactionManager.instance.active);
            // Pre-purge top-partitions-by-tombstones counting, matching legacy
            // TopPartitionTracker.TombstoneCounter (which CompactionIterator applies before purge).
            compactorLocal.setTopPartitionCollector(topPartitionCollector);
            compactor = compactorLocal;

            long allPartitions = 0;
            rangePartitionCounts = Maps.newHashMapWithExpectedSize(ranges.size());
            for (Range<Token> range : ranges)
            {
                long numPartitions = 0;
                for (SSTableReader sstable : sstables)
                    numPartitions += sstable.estimatedKeysForRanges(Collections.singleton(range));
                rangePartitionCounts.put(range, numPartitions);
                allPartitions += numPartitions;
            }
            estimatedPartitions = allPartitions;

            long estimatedTotalBytes = 0;
            for (List<PartitionPositionBounds> bounds : boundsBySSTable.values())
                for (PartitionPositionBounds positionsForRanges : bounds)
                    estimatedTotalBytes += positionsForRanges.upperPosition - positionsForRanges.lowerPosition;
            estimatedBytes = estimatedTotalBytes;
        }
        catch (Throwable t)
        {
            if (compactorLocal != null)
                compactorLocal.close();
            controller.close();
            sstables.release();
            throw t;
        }
    }

    @Override
    public long getBytesRead()
    {
        return compactor.getBytesRead();
    }

    @Override
    public void close()
    {
        super.close();

        if (compactor != null)
            compactor.close();

        if (controller != null)
            controller.close();

        if (isSnapshotValidation && !isGlobalSnapshotValidation)
        {
            // we can only clear the snapshot if we are not doing a global snapshot validation (we then clear it once anticompaction
            // is done).
            SnapshotManager.instance.clearSnapshot(cfs.getKeyspaceName(), cfs.getTableName(), snapshotName);
        }

        if (sstables != null)
            sstables.release();
    }

    @Override
    public TableMetadata metadata()
    {
        return cfs.metadata.get();
    }

    @Override
    public boolean hasNext()
    {
        if (!nextReady)
        {
            try
            {
                nextReady = compactor.mergeNextPartition(sink);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed reading sstables during cursor-backed validation compaction of " +
                                           cfs.getKeyspaceName() + '.' + cfs.getTableName(), e);
            }
        }
        return nextReady;
    }

    @Override
    public UnfilteredRowIterator next()
    {
        if (!hasNext())
            throw new NoSuchElementException();
        nextReady = false;
        return sink.takePartitionDigest();
    }

    @Override
    public void feed(Validator validator, UnfilteredRowIterator partition)
    {
        // Safe: this iterator's own next() is the only producer of the partitions it's ever
        // asked to feed - see PrecomputedDigestPartition, whose row-content methods throw
        // rather than return real content, so callers must never treat it as a genuine iterator.
        PrecomputedDigestPartition precomputed = (PrecomputedDigestPartition) partition;
        validator.addDigest(precomputed.partitionKey(), precomputed.digestBytes(), precomputed.digestInputBytes());
    }

    @Override
    public long getEstimatedBytes()
    {
        return estimatedBytes;
    }

    @Override
    public long estimatedPartitions()
    {
        return estimatedPartitions;
    }

    @Override
    public Map<Range<Token>, Long> getRangePartitionCounts()
    {
        return rangePartitionCounts;
    }
}
