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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import accord.primitives.Seekable;
import accord.primitives.Seekables;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cache.RowCacheSentinel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.CachedBTreePartition;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A read command that selects a (part of a) single partition.
 */
public class SinglePartitionReadCommand extends ReadCommand implements SinglePartitionReadQuery
{
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();
    protected static final Function<Seekable, SelectionDeserializer> accordSelectionDeserializer = AccordDeserializer::new;

    protected final DecoratedKey partitionKey;
    protected final ClusteringIndexFilter clusteringIndexFilter;

    @VisibleForTesting
    protected SinglePartitionReadCommand(Epoch serializedAtEpoch,
                                         boolean isDigest,
                                         int digestVersion,
                                         boolean acceptsTransient,
                                         PotentialTxnConflicts potentialTxnConflicts,
                                         TableMetadata metadata,
                                         long nowInSec,
                                         ColumnFilter columnFilter,
                                         RowFilter rowFilter,
                                         DataLimits limits,
                                         DecoratedKey partitionKey,
                                         ClusteringIndexFilter clusteringIndexFilter,
                                         Index.QueryPlan indexQueryPlan,
                                         boolean trackWarnings,
                                         DataRange dataRange)
    {
        super(serializedAtEpoch, Kind.SINGLE_PARTITION, isDigest, digestVersion, acceptsTransient, potentialTxnConflicts, metadata, nowInSec, columnFilter, rowFilter, limits, indexQueryPlan, trackWarnings, dataRange);
        assert IPartitioner.equivalent(partitionKey.getPartitioner(), metadata.partitioner) : String.format("Mismatching partitioners for key (%s) and table metadata (%s)",
                                                                                                            partitionKey.getPartitioner(), metadata.partitioner);
        this.partitionKey = partitionKey;
        this.clusteringIndexFilter = clusteringIndexFilter;
    }

    private static SinglePartitionReadCommand create(Epoch serializedAtEpoch,
                                                     boolean isDigest,
                                                     int digestVersion,
                                                     boolean acceptsTransient,
                                                     PotentialTxnConflicts potentialTxnConflicts,
                                                     TableMetadata metadata,
                                                     long nowInSec,
                                                     ColumnFilter columnFilter,
                                                     RowFilter rowFilter,
                                                     DataLimits limits,
                                                     DecoratedKey partitionKey,
                                                     ClusteringIndexFilter clusteringIndexFilter,
                                                     Index.QueryPlan indexQueryPlan,
                                                     boolean trackWarnings)
    {
        DataRange dataRange = new DataRange(new Bounds<>(partitionKey, partitionKey), clusteringIndexFilter);

        if (metadata.isVirtual())
        {
            return new VirtualTableSinglePartitionReadCommand(isDigest,
                                                              digestVersion,
                                                              acceptsTransient,
                                                              metadata,
                                                              nowInSec,
                                                              columnFilter,
                                                              rowFilter,
                                                              limits,
                                                              partitionKey,
                                                              clusteringIndexFilter,
                                                              indexQueryPlan,
                                                              trackWarnings,
                                                              dataRange);
        }

        return new SinglePartitionReadCommand(serializedAtEpoch,
                                              isDigest,
                                              digestVersion,
                                              acceptsTransient,
                                              potentialTxnConflicts,
                                              metadata,
                                              nowInSec,
                                              columnFilter,
                                              rowFilter,
                                              limits,
                                              partitionKey,
                                              clusteringIndexFilter,
                                              indexQueryPlan,
                                              trackWarnings,
                                              dataRange);
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     * @param indexQueryPlan explicitly specified index to use for the query
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    long nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter,
                                                    Index.QueryPlan indexQueryPlan)
    {
        return create(metadata.epoch,
                      false,
                      0,
                      false,
                      PotentialTxnConflicts.DISALLOW,
                      metadata,
                      nowInSec,
                      columnFilter,
                      rowFilter,
                      limits,
                      partitionKey,
                      clusteringIndexFilter,
                      indexQueryPlan,
                      false);
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     * @param potentialTxnConflicts Whether to generate an error if this read could potentially conflict with a txn
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    long nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter,
                                                    PotentialTxnConflicts potentialTxnConflicts)
    {
        return create(metadata.epoch,
                      false,
                      0,
                      false,
                      potentialTxnConflicts,
                      metadata,
                      nowInSec,
                      columnFilter,
                      rowFilter,
                      limits,
                      partitionKey,
                      clusteringIndexFilter,
                      findIndexQueryPlan(metadata, rowFilter),
                      false);
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    long nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter)
    {
        return create(metadata,
                      nowInSec,
                      columnFilter,
                      rowFilter,
                      limits,
                      partitionKey,
                      clusteringIndexFilter,
                      findIndexQueryPlan(metadata, rowFilter));
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param columnFilter the column filter to use for the query.
     * @param filter the clustering index filter to use for the query.
     *
     * @return a newly created read command. The returned command will use no row filter and have no limits.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    long nowInSec,
                                                    DecoratedKey key,
                                                    ColumnFilter columnFilter,
                                                    ClusteringIndexFilter filter)
    {
        return create(metadata, nowInSec, columnFilter, RowFilter.none(), DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new read command that queries a single partition in its entirety.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     *
     * @return a newly created read command that queries all the rows of {@code key}.
     */
    public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, long nowInSec, DecoratedKey key)
    {
        return create(metadata, nowInSec, key, Slices.ALL);
    }

    /**
     * Creates a new read command that queries a single partition in its entirety.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     *
     * @return a newly created read command that queries all the rows of {@code key}.
     */
    public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, long nowInSec, ByteBuffer key)
    {
        return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), Slices.ALL);
    }

    /**
     * Creates a new single partition slice command for the provided single slice.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slice the slice of rows to query.
     *
     * @return a newly created read command that queries {@code slice} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, DecoratedKey key, Slice slice)
    {
        return create(metadata, nowInSec, key, Slices.with(metadata.comparator, slice));
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, DecoratedKey key, Slices slices)
    {
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, false);
        return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.none(), DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, ByteBuffer key, Slices slices)
    {
        return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), slices);
    }

    /**
     * Creates a new single partition name command for the provided rows.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param names the clustering for the rows to query.
     *
     * @return a newly created read command that queries the {@code names} in {@code key}. The returned query will
     * query every columns (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, DecoratedKey key, NavigableSet<Clustering<?>> names)
    {
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(names, false);
        return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.none(), DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new single partition name command for the provided row.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param name the clustering for the row to query.
     *
     * @return a newly created read command that queries {@code name} in {@code key}. The returned query will
     * query every columns (without limit or row filtering).
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, DecoratedKey key, Clustering<?> name)
    {
        return create(metadata, nowInSec, key, FBUtilities.singleton(name, metadata.comparator));
    }

    public SinglePartitionReadCommand copy()
    {
        return create(serializedAtEpoch(),
                      isDigestQuery(),
                      digestVersion(),
                      acceptsTransient(),
                      potentialTxnConflicts(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits(),
                      partitionKey(),
                      clusteringIndexFilter(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    protected SinglePartitionReadCommand copyAsDigestQuery()
    {
        return create(serializedAtEpoch(),
                      true,
                      digestVersion(),
                      acceptsTransient(),
                      potentialTxnConflicts(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits(),
                      partitionKey(),
                      clusteringIndexFilter(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    protected SinglePartitionReadCommand copyAsTransientQuery()
    {
        return create(serializedAtEpoch(),
                      false,
                      0,
                      true,
                      potentialTxnConflicts(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits(),
                      partitionKey(),
                      clusteringIndexFilter(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    public SinglePartitionReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return create(serializedAtEpoch(),
                      isDigestQuery(),
                      digestVersion(),
                      acceptsTransient(),
                      potentialTxnConflicts(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      newLimits,
                      partitionKey(),
                      clusteringIndexFilter(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    @Override
    public ClusteringIndexFilter clusteringIndexFilter()
    {
        return clusteringIndexFilter;
    }

    public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
    {
        return clusteringIndexFilter;
    }

    public long getTimeout(TimeUnit unit)
    {
        return DatabaseDescriptor.getReadRpcTimeout(unit);
    }

    public boolean isReversed()
    {
        return clusteringIndexFilter.isReversed();
    }

    @Override
    public SinglePartitionReadCommand forPaging(Clustering<?> lastReturned, DataLimits limits)
    {
        // We shouldn't have set digest yet when reaching that point
        assert !isDigestQuery();
        SinglePartitionReadCommand cmd = create(metadata(),
                                                nowInSec(),
                                                columnFilter(),
                                                rowFilter(),
                                                limits,
                                                partitionKey(),
                                                lastReturned == null ? clusteringIndexFilter() : clusteringIndexFilter.forPaging(metadata().comparator, lastReturned, false));
        if (isTrackingWarnings())
            cmd.trackWarnings();
        return cmd;
    }

    @Override
    public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, Dispatcher.RequestTime requestTime) throws RequestExecutionException
    {
        if (clusteringIndexFilter.isEmpty(metadata().comparator))
            return EmptyIterators.partition();

        return StorageProxy.read(Group.one(this), consistency, requestTime);
    }

    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.readLatency.addNano(latencyNanos);
    }

    /**
     * Layer-1 gate — cheap, decided before any leg opens. One check specific to this
     * path (no 2i index plan — the transcode path only ever resolves ordinary memtable/sstable
     * legs). Digest queries ARE served here: {@code queryStorageToResponseBytes} computes the
     * digest over a cursor merge in iterator form (raw transcode bytes cannot be hashed) and
     * returns a {@code DigestResponse} whose bytes match the iterator path. Repaired-status
     * tracking IS served here too: {@code queryStorageToResponseBytes} computes the repaired-data
     * digest over a cursor merge of the repaired sstables (see there). Any failure here falls
     * back to the base-class default ({@code ReadCommand.createResponseLocally}, today's
     * exact {@code executeLocally}+{@code createResponse} behavior) with zero cursor state touched.
     */
    @Override
    public ReadResponse createResponseLocally(ReadExecutionController controller)
    {
        if (DatabaseDescriptor.cursorReadsEnabled()
            && indexQueryPlan() == null)
        {
            ReadResponse response = queryStorageToResponseBytes(controller);
            if (response != null)
                return response;
        }
        return super.createResponseLocally(controller);
    }

    /**
     * The parallel transcode-path entry point. It duplicates {@link #queryMemtableAndDiskInternal}'s
     * leg-resolution preamble (memtable + sstable candidate loop, {@code
     * mostRecentPartitionTombstone} elimination) rather than sharing code with it, so that untouched
     * function stays byte-for-byte as-is. {@code queryMemtableAndDiskInternal} is NOT called,
     * modified, or extended by this method.
     * <p>
     * Returns null ("not eligible, caller must fall back to the base-class default") whenever ANY
     * layer-2 condition fails. Zero-or-more sstable reference/leg opens may happen before that
     * decision is final (the real leg count cannot be known any cheaper — the same structural
     * limitation the {@code finalLimitedStream} gate already has); every such leg/iterator is
     * closed on decline via {@link CursorReads#closeAllQuietly}.
     */
    private ReadResponse queryStorageToResponseBytes(ReadExecutionController controller)
    {
        // Layer 2, part A: query-shape checks decidable without opening anything, and WITHOUT
        // relying solely on CursorReads.limitBoundFor/filterPushdownFor returning null -- those
        // two also return null for reasons OTHER than "nothing to apply" (an unpushable filter, a
        // non-boundable limit kind like CQL_GROUP_BY_LIMIT), which would silently skip the
        // top-level rowFilter().filter/limits().filter stages this response path never runs. Only
        // a genuinely EMPTY filter and a genuinely UNLIMITED limit make skipping those stages safe.
        if (!rowFilter().isEmpty() || !limits().isUnlimited())
            return null;
        // Defense in depth: explicit reuse of the established limit/filter gate predicates,
        // already implied by the check above (limitBoundFor
        // returns null whenever limits().isUnlimited(); filterPushdownFor returns null whenever
        // rowFilter().isEmpty()), kept so a future change to either predicate's OTHER disengagement
        // reasons cannot silently widen this gate without also touching this call.
        if (CursorReads.limitBoundFor(this) != null || CursorReads.filterPushdownFor(this) != null)
            return null;
        // withQuerySizeTracking would engage independently of filter/limit shape (gated only by
        // trackWarnings + configured thresholds) -- this response path never runs it, so a command
        // that would trigger it must decline.
        if (CursorReads.querySizeTrackingActive(this))
            return null;

        long startTimeNanos = nanoTime();
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata());
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));
        if (!CursorReads.isReadSupported(this, cfs, view.sstables))
            return null;

        view.sstables.sort(SSTableReader.maxTimestampDescending);
        ClusteringIndexFilter filter = clusteringIndexFilter();
        // The transcode fast path streams one contiguous slice straight to bytes, so it serves a
        // single slice only. A names read has one point slice per requested clustering, which is a
        // multi-slice read. Decline it here; the plain cursor merge path serves it instead. A
        // single-slice transcode for names is left as a future optimization.
        if (filter instanceof ClusteringIndexNamesFilter)
            return null;
        // The transcode fast path streams one contiguous slice, so a multi-slice slice filter (a
        // compound-clustering restriction expanding to several ranges, CASSANDRA-20428 gap #8) cannot
        // use it. Decline here; the plain cursor merge path serves it instead (still the cursor path,
        // never the iterator path). buildTranscodeResponseBytes assumes a single slice.
        if (filter.getSlices(metadata()).size() > 1)
            return null;
        // Counter cells reconcile by folding every leg's counter context (CounterContext.merge),
        // not by streaming one winner's raw bytes. The transcode fast path emits a single winner
        // per cell, so it cannot fold counters; the cursor merge/materialize path serves them
        // instead (still the cursor path, never the iterator path).
        if (metadata().isCounter())
            return null;
        // The transcode fast path streams forward. A reverse (ORDER BY ... DESC) read
        // (CASSANDRA-20428 gap #5) needs the backward per-block walk, so decline here; the cursor
        // merge path serves it with its lazy reverse per-leg iterators (still the cursor path, never
        // the iterator path).
        if (filter.isReversed())
            return null;
        long mostRecentPartitionTombstone = Long.MIN_VALUE;

        List<UnfilteredRowIterator> cursorMemtableIters = null;
        List<CursorReads.PendingLeg> cursorLegs = null;
        CursorReads.ValueTransfer cursorValueTransfer = null;
        List<EncodingStats> absentSSTableStats = null;
        SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();
        // Repaired-status tracking state. A break on the mostRecentPartitionTombstone elimination
        // below skips relevant sstables, so it marks the repaired digest inconclusive -- but only
        // once we have committed to serving this read on the transcode path, never on a decline
        // (the RepairedDataInfo lives on the shared controller; a decline lets the base path own it).
        boolean trackingSkipInconclusive = false;
        boolean tracking = controller.isTrackingRepairedStatus();

        try
        {
            for (Memtable memtable : view.memtables)
            {
                UnfilteredRowIterator iter = memtable.rowIterator(partitionKey(), filter.getSlices(metadata()), columnFilter(), filter.isReversed(), metricsCollector);
                if (iter == null)
                    continue;

                // Memtable data is always considered unrepaired
                controller.updateMinOldestUnrepairedTombstone(memtable.getMinLocalDeletionTime());
                UnfilteredRowIterator validated = RTBoundValidator.validate(iter, RTBoundValidator.Stage.MEMTABLE, false);
                if (cursorMemtableIters == null)
                    cursorMemtableIters = new ArrayList<>();
                cursorMemtableIters.add(validated);

                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                        iter.partitionLevelDeletion().markedForDeleteAt());
            }

            for (SSTableReader sstable : view.sstables)
            {
                if (sstable.getMaxTimestamp() < mostRecentPartitionTombstone)
                {
                    // We are skipping relevant sstables. If tracking, remember to mark the repaired
                    // digest inconclusive (deferred until we commit to the transcode path below).
                    if (tracking)
                        trackingSkipInconclusive = true;
                    break;
                }

                boolean intersects = intersects(sstable);
                boolean hasRequiredStatics = hasRequiredStatics(sstable);
                boolean hasPartitionLevelDeletions = hasPartitionLevelDeletions(sstable);

                if (!intersects && !hasRequiredStatics && !hasPartitionLevelDeletions)
                    continue;

                if (intersects || hasRequiredStatics)
                {
                    if (!sstable.isRepaired())
                        controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());

                    if (cursorValueTransfer == null)
                        cursorValueTransfer = controller.cursorValueTransfer();
                    CursorReads.PendingLeg leg = CursorReads.openLeg(sstable, metadata(), partitionKey(),
                                                                     intersects ? filter.getSlices(metadata()) : Slices.NONE,
                                                                     columnFilter(), metricsCollector, cursorValueTransfer);
                    if (leg == null)
                    {
                        // the sstable does not contain the partition: keep parity with
                        // queryMemtableAndDiskInternal's own leg == null branch, which still
                        // contributes the SSTABLE's stats to the merge via a placeholder iterator
                        // (CursorReads.absentPartitionIterator) -- the transcode path folds the
                        // same stats contribution in directly (buildTranscodeResponseBytes'
                        // extraStats) rather than needing an actual placeholder iterator, since a
                        // live-partition-deletion/empty-static-row placeholder can never contribute
                        // anything else to an outer object merge that no longer exists here.
                        if (absentSSTableStats == null)
                            absentSSTableStats = new ArrayList<>();
                        absentSSTableStats.add(sstable.stats());
                    }
                    else
                    {
                        if (cursorLegs == null)
                            cursorLegs = new ArrayList<>(view.sstables.size());
                        cursorLegs.add(leg);
                        mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                                leg.partitionLevelDeletion().markedForDeleteAt());
                    }
                }
                else
                {
                    if (cursorValueTransfer == null)
                        cursorValueTransfer = controller.cursorValueTransfer();
                    CursorReads.PendingLeg leg = CursorReads.openLeg(sstable, metadata(), partitionKey(),
                                                                     Slices.NONE, columnFilter(), metricsCollector,
                                                                     cursorValueTransfer);
                    if (leg != null)
                    {
                        if (!leg.partitionLevelDeletion().isLive())
                        {
                            if (!sstable.isRepaired())
                                controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());
                            if (cursorLegs == null)
                                cursorLegs = new ArrayList<>(view.sstables.size());
                            cursorLegs.add(leg);
                            mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                                    leg.partitionLevelDeletion().markedForDeleteAt());
                        }
                        else
                        {
                            leg.close();
                        }
                    }
                }
            }

            int sstableLegCount = cursorLegs == null ? 0 : cursorLegs.size();
            int memtableLegCount = cursorMemtableIters == null ? 0 : cursorMemtableIters.size();
            int legCount = sstableLegCount + memtableLegCount;
            if (legCount == 0)
            {
                // The zero-leg absent-partition case: no sstable held the partition and there was no
                // memtable data. There is nothing to merge, so decline and let the caller's default
                // executeLocally path own the empty result (it serves it through cursor placeholders,
                // not the iterator read path). A read that survives with ONE leg -- a single sstable,
                // the memtable alone, or a shape that collapses to one leg after tombstone/skip
                // pruning -- is now served here: the merge core no longer needs two legs.
                CursorReads.closeAllQuietly(cursorLegs);
                CursorReads.closeAllQuietly(cursorMemtableIters);
                return null;
            }

            // Digest query: the response is a hash of the merged partition, not its data bytes, so
            // the raw-byte transcode cannot feed it. Build the cursor merge in ITERATOR form over
            // ALL surviving legs -- the same merged stream the data path emits -- and hash it with
            // the exact digest routine and digest version the iterator path uses
            // (ReadResponse.createDigestResponse -> UnfilteredPartitionIterators.digest). A
            // DigestResponse never carries a repaired-data digest (mayIncludeRepairedDigest() ==
            // false), so repaired-status tracking needs no work here: the iterator path discards
            // its tracking side-computation in its own DigestResponse, and the merge over the full
            // leg set (repaired + unrepaired together) is byte-identical to the iterator path's.
            if (isDigestQuery())
            {
                StorageHook.instance.reportRead(cfs.metadata().id, partitionKey());
                if (metricsCollector.getMergedSSTables() > DatabaseDescriptor.getSSTablesPerReadLogThreshold())
                    noSpamLogger.info("The following query '{}' has read {} SSTables.", this.toCQLString(), metricsCollector.getMergedSSTables());

                UnfilteredRowIterator mergedForDigest;
                if (cursorLegs != null && !cursorLegs.isEmpty())
                {
                    List<CursorReads.PendingLeg> legs = cursorLegs;
                    List<UnfilteredRowIterator> memtableIters = cursorMemtableIters;
                    cursorLegs = null;          // ownership passes to buildTrackingCursorMerge
                    cursorMemtableIters = null; // (it closes the sstable legs and memtable adapters)
                    mergedForDigest = buildTrackingCursorMerge(legs, memtableIters, filter.getSlices(metadata()));
                }
                else
                {
                    // Memtable-only: no sstable leg. Merge the always-unrepaired memtable iterators
                    // through the object path, exactly as executeLocally's addMemtableIterator
                    // branch does (withSSTablesIterated -> UnfilteredRowIterators.merge).
                    List<UnfilteredRowIterator> memtableIters = cursorMemtableIters;
                    cursorMemtableIters = null; // ownership passes to the merge
                    mergedForDigest = UnfilteredRowIterators.merge(memtableIters);
                }
                try (UnfilteredPartitionIterator partitions = new SingletonUnfilteredPartitionIterator(mergedForDigest))
                {
                    ReadResponse response = ReadResponse.createDigestResponse(partitions, this);
                    CursorReads.countTranscodeResponseServed();
                    return response;
                }
            }

            // Committed to serving this read on the transcode path. If tracking repaired status,
            // build the InputCollector now -- its constructor selects the repaired sstables and
            // fires the pending-repair inconclusive side effects, exactly as the iterator path does
            // -- and record which surviving legs are repaired so we can reopen them for the digest
            // once the transcode has consumed the originals.
            InputCollector<UnfilteredRowIterator> trackingCollector = null;
            List<SSTableReader> repairedSurvivors = null;
            if (tracking)
            {
                trackingCollector = iteratorsForPartition(view, controller);
                if (trackingSkipInconclusive)
                    trackingCollector.markInconclusive();
                // A memtable-only read has no sstable legs (cursorLegs is null) and memtable data is
                // always unrepaired, so there is nothing to feed the repaired digest here.
                if (cursorLegs != null)
                {
                    for (CursorReads.PendingLeg leg : cursorLegs)
                    {
                        if (trackingCollector.isRepairedForTracking(leg.sstable))
                        {
                            if (repairedSurvivors == null)
                                repairedSurvivors = new ArrayList<>(cursorLegs.size());
                            repairedSurvivors.add(leg.sstable);
                        }
                    }
                }
            }

            // At least one leg survives (legCount != 0 above). Either or both of the memtable legs
            // and the sstable legs may be present; build the combined list from whichever exist,
            // memtable legs first to mirror the object merge's memtables-then-sstables input order.
            int sstableLegs = cursorLegs == null ? 0 : cursorLegs.size();
            List<CursorReads.MergeLeg> allLegs = new ArrayList<>(memtableLegCount + sstableLegs);
            if (cursorMemtableIters != null)
            {
                for (UnfilteredRowIterator memtableIter : cursorMemtableIters)
                    allLegs.add(new MemtableMergeLeg(memtableIter, filter.getSlices(metadata())));
                cursorMemtableIters = null; // ownership passes to the adapters (closed by mergeLegsWithSink)
            }
            if (cursorLegs != null)
                allLegs.addAll(cursorLegs);
            cursorLegs = null; // ownership passes to buildTranscodeResponseBytes/mergeLegsWithSink (closes legs itself)

            // Matches queryMemtableAndDiskInternal's own placement: a pluggable, off-by-default
            // notification hook (external audit/CDC-adjacent tooling via -Dcassandra.storage_hook)
            // must still see every read this response actually serves.
            StorageHook.instance.reportRead(cfs.metadata().id, partitionKey());

            if (metricsCollector.getMergedSSTables() > DatabaseDescriptor.getSSTablesPerReadLogThreshold())
                noSpamLogger.info("The following query '{}' has read {} SSTables.", this.toCQLString(), metricsCollector.getMergedSSTables());

            long nowInSec = nowInSec();
            long gcBefore = nowInSec == 0 ? Long.MIN_VALUE : cfs.gcBefore(nowInSec);
            boolean onlyPurgeRepairedTombstones = cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones();
            long oldestUnrepairedTombstone = controller.oldestUnrepairedTombstone();

            CursorReads.TombstoneScanGuard scanGuard =
                new CursorReads.TombstoneScanGuard(this, cfs.metric, startTimeNanos, nowInSec, partitionKey());

            ByteBuffer data = CursorReads.buildTranscodeResponseBytes(allLegs, metadata(), partitionKey(),
                                                                       filter.getSlices(metadata()), columnFilter(),
                                                                       nowInSec, gcBefore, onlyPurgeRepairedTombstones,
                                                                       oldestUnrepairedTombstone, absentSSTableStats,
                                                                       scanGuard);
            // The data response is now bytes and cannot feed the repaired-data digest, so compute
            // the digest over a SEPARATE cursor merge of the repaired sstables. This records the
            // digest and the conclusive flag on the controller's RepairedDataInfo, which
            // createTranscodedDataResponse reads below (eagerly).
            if (trackingCollector != null && repairedSurvivors != null)
                computeTranscodeRepairedDigest(cfs, controller, trackingCollector, repairedSurvivors, filter, nowInSec);
            ReadResponse response = ReadResponse.createTranscodedDataResponse(data, controller.getRepairedDataInfo());
            CursorReads.countTranscodeResponseServed();
            return response;
        }
        catch (IOException e)
        {
            throw new RuntimeException("transcode response construction failed for " + partitionKey(), e);
        }
        catch (RuntimeException | Error e)
        {
            CursorReads.closeAll(cursorLegs, e);
            CursorReads.closeAll(cursorMemtableIters, e);
            throw e;
        }
    }

    @VisibleForTesting
    @SuppressWarnings("resource") // we close the created iterator through closing the result of this method (and SingletonUnfilteredPartitionIterator ctor cannot fail)
    public UnfilteredPartitionIterator queryStorage(final ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        // skip the row cache and go directly to sstables/memtable if repaired status of
        // data is being tracked. This is only requested after an initial digest mismatch
        UnfilteredRowIterator partition = cfs.isRowCacheEnabled() && !executionController.isTrackingRepairedStatus()
                                        ? getThroughCache(cfs, executionController)
                                        : queryMemtableAndDiskForExecuteLocally(cfs, executionController);
        return new SingletonUnfilteredPartitionIterator(partition);
    }

    /**
     * Fetch the rows requested if in cache; if not, read it from disk and cache it.
     * <p>
     * If the partition is cached, and the filter given is within its bounds, we return
     * from cache, otherwise from disk.
     * <p>
     * If the partition is is not cached, we figure out what filter is "biggest", read
     * that from disk, then filter the result and either cache that or return it.
     */
    private UnfilteredRowIterator getThroughCache(ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        assert !cfs.isIndex(); // CASSANDRA-5732
        assert cfs.isRowCacheEnabled() : String.format("Row cache is not enabled on table [%s]", cfs.name);

        RowCacheKey key = new RowCacheKey(metadata(), partitionKey());

        // Attempt a sentinel-read-cache sequence.  if a write invalidates our sentinel, we'll return our
        // (now potentially obsolete) data, but won't cache it. see CASSANDRA-3862
        // TODO: don't evict entire partitions on writes (#2864)
        IRowCacheEntry cached = CacheService.instance.rowCache.get(key);
        if (cached != null)
        {
            if (cached instanceof RowCacheSentinel)
            {
                // Some other read is trying to cache the value, just do a normal non-caching read
                Tracing.trace("Row cache miss (race)");
                cfs.metric.rowCacheMiss.inc();
                return queryMemtableAndDisk(cfs, executionController);
            }

            CachedPartition cachedPartition = (CachedPartition)cached;
            if (cfs.isFilterFullyCoveredBy(clusteringIndexFilter(), limits(), cachedPartition, nowInSec(), metadata().enforceStrictLiveness()))
            {
                cfs.metric.rowCacheHit.inc();
                Tracing.trace("Row cache hit");
                UnfilteredRowIterator unfilteredRowIterator = clusteringIndexFilter().getUnfilteredRowIterator(columnFilter(), cachedPartition);
                cfs.metric.updateSSTableIterated(0);
                return unfilteredRowIterator;
            }

            cfs.metric.rowCacheHitOutOfRange.inc();
            Tracing.trace("Ignoring row cache as cached value could not satisfy query");
            return queryMemtableAndDisk(cfs, executionController);
        }

        cfs.metric.rowCacheMiss.inc();
        Tracing.trace("Row cache miss");

        // Note that on tables with no clustering keys, any positive value of
        // rowsToCache implies caching the full partition
        boolean cacheFullPartitions = metadata().clusteringColumns().size() > 0 ?
                                      metadata().params.caching.cacheAllRows() :
                                      metadata().params.caching.cacheRows();

        // To be able to cache what we read, what we read must at least covers what the cache holds, that
        // is the 'rowsToCache' first rows of the partition. We could read those 'rowsToCache' first rows
        // systematically, but we'd have to "extend" that to whatever is needed for the user query that the
        // 'rowsToCache' first rows don't cover and it's not trivial with our existing filters. So currently
        // we settle for caching what we read only if the user query does query the head of the partition since
        // that's the common case of when we'll be able to use the cache anyway. One exception is if we cache
        // full partitions, in which case we just always read it all and cache.
        if (cacheFullPartitions || clusteringIndexFilter().isHeadFilter())
        {
            RowCacheSentinel sentinel = new RowCacheSentinel();
            boolean sentinelSuccess = CacheService.instance.rowCache.putIfAbsent(key, sentinel);
            boolean sentinelReplaced = false;

            try
            {
                final int rowsToCache = metadata().params.caching.rowsPerPartitionToCache();
                final boolean enforceStrictLiveness = metadata().enforceStrictLiveness();

                UnfilteredRowIterator iter = fullPartitionRead(metadata(), nowInSec(), partitionKey()).queryMemtableAndDisk(cfs, executionController);
                try
                {
                    // Use a custom iterator instead of DataLimits to avoid stopping the original iterator
                    UnfilteredRowIterator toCacheIterator = new WrappingUnfilteredRowIterator()
                    {
                        private int rowsCounted = 0;

                        @Override
                        public UnfilteredRowIterator wrapped()
                        {
                            return iter;
                        }

                        @Override
                        public boolean hasNext()
                        {
                            return rowsCounted < rowsToCache && iter.hasNext();
                        }

                        @Override
                        public Unfiltered next()
                        {
                            Unfiltered unfiltered = iter.next();
                            if (unfiltered.isRow())
                            {
                                Row row = (Row) unfiltered;
                                if (row.hasLiveData(nowInSec(), enforceStrictLiveness))
                                    rowsCounted++;
                            }
                            return unfiltered;
                        }
                    };

                    // We want to cache only rowsToCache rows
                    CachedPartition toCache = CachedBTreePartition.create(toCacheIterator, nowInSec());
                    if (sentinelSuccess && !toCache.isEmpty())
                    {
                        Tracing.trace("Caching {} rows", toCache.rowCount());
                        CacheService.instance.rowCache.replace(key, sentinel, toCache);
                        // Whether or not the previous replace has worked, our sentinel is not in the cache anymore
                        sentinelReplaced = true;
                    }

                    // We then re-filter out what this query wants.
                    // Note that in the case where we don't cache full partitions, it's possible that the current query is interested in more
                    // than what we've cached, so we can't just use toCache.
                    UnfilteredRowIterator cacheIterator = clusteringIndexFilter().getUnfilteredRowIterator(columnFilter(), toCache);
                    if (cacheFullPartitions)
                    {
                        // Everything is guaranteed to be in 'toCache', we're done with 'iter'
                        assert !iter.hasNext();
                        iter.close();
                        return cacheIterator;
                    }
                    return UnfilteredRowIterators.concat(cacheIterator, clusteringIndexFilter().filterNotIndexed(columnFilter(), iter));
                }
                catch (RuntimeException | Error e)
                {
                    iter.close();
                    throw e;
                }
            }
            finally
            {
                if (sentinelSuccess && !sentinelReplaced)
                    cfs.invalidateCachedPartition(key);
            }
        }

        Tracing.trace("Fetching data but not populating cache as query does not query from the start of the partition");
        return queryMemtableAndDisk(cfs, executionController);
    }

    /**
     * Queries both memtable and sstables to fetch the result of this query.
     * <p>
     * Please note that this method:
     *   1) does not check the row cache.
     *   2) does not apply the query limit, nor the row filter (and so ignore 2ndary indexes).
     *      Those are applied in {@link ReadCommand#executeLocally}.
     *   3) does not record some of the read metrics (latency, scanned cells histograms) nor
     *      throws TombstoneOverwhelmingException.
     * It is publicly exposed because there is a few places where that is exactly what we want,
     * but it should be used only where you know you don't need thoses things.
     * <p>
     * Also note that one must have created a {@code ReadExecutionController} on the queried table and we require it as
     * a parameter to enforce that fact, even though it's not explicitlly used by the method.
     */
    public UnfilteredRowIterator queryMemtableAndDisk(ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        assert executionController != null && executionController.validForReadOn(cfs);
        Tracing.trace("Executing single-partition query on {}", cfs.name);

        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));
        return queryMemtableAndDiskInternal(cfs, view, null, executionController, false);
    }

    /**
     * The {@link #queryStorage} flavor of {@link #queryMemtableAndDisk} —
     * identical except that it marks the result as the final merged stream
     * {@code ReadCommand.executeLocally}'s {@code limits().filter} counter consumes, which is the
     * routing condition for the cursor merge's limit-driven production bound
     * ({@code CursorReads.limitBoundFor}). The PUBLIC entry points must keep passing false: their
     * external callers (counter locks, 2i searchers, cache warming) consume the un-limited
     * partition contents this method's javadoc promises, so bounding production there would
     * truncate their view of the partition.
     */
    private UnfilteredRowIterator queryMemtableAndDiskForExecuteLocally(ColumnFamilyStore cfs,
                                                                        ReadExecutionController executionController)
    {
        assert executionController != null && executionController.validForReadOn(cfs);
        Tracing.trace("Executing single-partition query on {}", cfs.name);

        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));
        return queryMemtableAndDiskInternal(cfs, view, null, executionController, true);
    }

    public UnfilteredRowIterator queryMemtableAndDisk(ColumnFamilyStore cfs,
                                                      ColumnFamilyStore.ViewFragment view,
                                                      Function<CellSourceIdentifier, Transformation<BaseRowIterator<?>>> rowTransformer,
                                                      ReadExecutionController executionController)
    {
        assert executionController != null && executionController.validForReadOn(cfs);
        Tracing.trace("Executing single-partition query on {}", cfs.name);

        return queryMemtableAndDiskInternal(cfs, view, rowTransformer, executionController, false);
    }

    /**
     * @param finalLimitedStream whether the returned iterator is the final merged stream consumed
     *                           by {@code executeLocally}'s post-merge stack (true only from
     *                           {@link #queryMemtableAndDiskForExecuteLocally}) — the routing
     *                           gate for the limit-driven production bound
     */
    private UnfilteredRowIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs,
                                                               ColumnFamilyStore.ViewFragment view,
                                                               Function<CellSourceIdentifier, Transformation<BaseRowIterator<?>>> rowTransformer,
                                                               ReadExecutionController controller,
                                                               boolean finalLimitedStream)
    {
        /*
         * We have 2 main strategies:
         *   1) We query memtables and sstables simulateneously. This is our most generic strategy and the one we use
         *      unless we have a names filter that we know we can optimize futher.
         *   2) If we have a name filter (so we query specific rows), we can make a bet: that all column for all queried row
         *      will have data in the most recent sstable(s), thus saving us from reading older ones. This does imply we
         *      have a way to guarantee we have all the data for what is queried, which is only possible for name queries
         *      and if we have neither non-frozen collections/UDTs nor counters.
         *      If a non-frozen collection or UDT is queried we can't guarantee that an older sstable won't have some
         *      elements that weren't in the most recent sstables.
         *      Counters are intrinsically a collection of shards and so have the same problem.
         *      Counter tables are also special in the sense that their rows do not have primary key liveness
         *      as INSERT statements are not supported on counter tables. Due to that even if only the primary key
         *      columns where queried, querying SSTables in timestamp order will always be less efficient for counter tables.
         *      Also, if tracking repaired data then we skip this optimization so we can collate the repaired sstables
         *      and generate a digest over their merge, which procludes an early return.
         */
        // A non-tracking names read takes a timestamp-order completeness driver: the cursor twin
        // (queryMemtableAndCursorsInTimestampOrder) when the cursor gate supports it, else the
        // iterator oracle (queryMemtableAndSSTablesInTimestampOrder). Both skip older sstables a
        // completeness check proves are unneeded (reduceFilter); the general cursor merge below has
        // no such skip, so serving names there would over-read older sstables. The cursor driver
        // reuses the oracle's reduceFilter/isRowComplete/add verbatim and swaps only the per-sstable
        // row source to a cursor leg, so no byte of row data comes from the iterator path. A tracking
        // names read is excluded here and falls through to the general cursor merge, where
        // buildTrackingCursorMerge splits the repaired/unrepaired sets for the repaired-data digest;
        // it does not get reduceFilter this phase (a naive timestamp-order stop is unsound while a
        // repaired-data digest needs every repaired sstable collated).
        if (clusteringIndexFilter() instanceof ClusteringIndexNamesFilter
            && !metadata().isCounter()
            && !queriesMulticellType()
            && !controller.isTrackingRepairedStatus())
        {
            ClusteringIndexNamesFilter names = (ClusteringIndexNamesFilter) clusteringIndexFilter();
            if (rowTransformer == null && CursorReads.isReadSupported(this, cfs, view.sstables))
                return queryMemtableAndCursorsInTimestampOrder(cfs, view, names, controller);
            return queryMemtableAndSSTablesInTimestampOrder(cfs, view, rowTransformer, names, controller);
        }

        view.sstables.sort(SSTableReader.maxTimestampDescending);
        ClusteringIndexFilter filter = clusteringIndexFilter();
        long minTimestamp = Long.MAX_VALUE;
        long mostRecentPartitionTombstone = Long.MIN_VALUE;
        // Experimental cursor read path (flag-gated off by default): when the whole query passes
        // the gate, every sstable leg below is served through SSTableCursorReader and
        // memtable legs join the same cursor-level merge through the object-backed adapter.
        // Repaired-status tracking is supported: the surviving legs are split into a repaired set
        // and an unrepaired set, and each is cursor-merged on its own so the repaired-data digest
        // can be computed (see the tracking branch below). Row transformers stay on the iterator
        // path for now (out of the verified surface).
        boolean cursorReads = rowTransformer == null
                              && CursorReads.isReadSupported(this, cfs, view.sstables);
        // Memtable legs of a gate-supported read join the SAME cursor-level merge as the
        // sstable legs (via the object-backed MemtableMergeLeg adapter), so their iterators are
        // COLLECTED here instead of being handed to the object merge — unless no sstable leg
        // survives the elimination loop below, in which case they fall back to the object path
        // (the gate requires at least one sstable leg).
        List<UnfilteredRowIterator> cursorMemtableIters = null;
        // With more than one candidate sstable, cursor-served legs are OPENED (cursor +
        // partition header + static row — enough for the mostRecentPartitionTombstone elimination
        // below) but their rows deferred, so that all surviving legs can be merged at the CURSOR
        // level after the loop instead of each leg being independently materialized. A
        // single-candidate read without memtable data keeps the per-leg call inside the
        // loop, bit for bit; with memtable data even a single sstable leg is deferred so it
        // can cursor-merge with the memtable leg(s).
        boolean cursorMergedLegs;
        List<CursorReads.PendingLeg> cursorLegs = null;
        // ONE value-transfer scratch (4KB bounce buffer + one-copy capture) shared by every
        // cursor leg of this read, created lazily with the first leg: legs consume cell values
        // strictly one at a time on this thread, so per-leg copies are duplicate allocation
        CursorReads.ValueTransfer cursorValueTransfer = null;
        InputCollector<UnfilteredRowIterator> inputCollector = iteratorsForPartition(view, controller);
        try
        {
            SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();

            for (Memtable memtable : view.memtables)
            {
                UnfilteredRowIterator iter = memtable.rowIterator(partitionKey(), filter.getSlices(metadata()), columnFilter(), filter.isReversed(), metricsCollector);
                if (iter == null)
                    continue;

                if (memtable.getMinTimestamp() != Memtable.NO_MIN_TIMESTAMP)
                    minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp());

                if (rowTransformer != null)
                    iter = Transformation.apply(iter, rowTransformer.apply(memtable));

                // Memtable data is always considered unrepaired
                controller.updateMinOldestUnrepairedTombstone(memtable.getMinLocalDeletionTime());
                // The Stage.MEMTABLE RT-bound validation wraps the iterator on BOTH routes — on
                // the cursor route the adapter consumes the validated stream, preserving the
                // same coverage the object merge had.
                UnfilteredRowIterator validated = RTBoundValidator.validate(iter, RTBoundValidator.Stage.MEMTABLE, false);
                if (cursorReads)
                {
                    if (cursorMemtableIters == null)
                        cursorMemtableIters = new ArrayList<>();
                    cursorMemtableIters.add(validated);
                }
                else
                {
                    inputCollector.addMemtableIterator(validated);
                }

                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                        iter.partitionLevelDeletion().markedForDeleteAt());
            }
            // With memtable data present, even a single-candidate sstable read defers its
            // leg so the memtable leg(s) can join one cursor-level merge with it.
            // A reverse read reconciles descending in the shared UnfilteredRowIterators.merge over
            // per-leg reverse cursor iterators, not in the cursor-level merge (which merges forward);
            // force per-leg iterators so each leg reads reversed and the shared merge composes them.
            cursorMergedLegs = cursorReads && !filter.isReversed()
                               && (view.sstables.size() > 1 || cursorMemtableIters != null);

            /*
             * We can't eliminate full sstables based on the timestamp of what we've already read like
             * in collectTimeOrderedData, but we still want to eliminate sstable whose maxTimestamp < mostRecentTombstone
             * we've read. We still rely on the sstable ordering by maxTimestamp since if
             *   maxTimestamp_s1 < maxTimestamp_s0,
             * we're guaranteed that s1 cannot have a row tombstone such that
             *   timestamp(tombstone) > maxTimestamp_s0
             * since we necessarily have
             *   timestamp(tombstone) <= maxTimestamp_s1
             * In other words, iterating in descending maxTimestamp order allow to do our mostRecentPartitionTombstone
             * elimination in one pass, and minimize the number of sstables for which we read a partition tombstone.
            */
            view.sstables.sort(SSTableReader.maxTimestampDescending);
            int nonIntersectingSSTables = 0;
            int includedDueToTombstones = 0;

            if (controller.isTrackingRepairedStatus())
                Tracing.trace("Collecting data from sstables and tracking repaired status");

            for (SSTableReader sstable : view.sstables)
            {
                // if we've already seen a partition tombstone with a timestamp greater
                // than the most recent update to this sstable, we can skip it
                // if we're tracking repaired status, we mark the repaired digest inconclusive
                // as other replicas may not have seen this partition delete and so could include
                // data from this sstable (or others) in their digests
                if (sstable.getMaxTimestamp() < mostRecentPartitionTombstone)
                {
                    inputCollector.markInconclusive();
                    break;
                }

                boolean intersects = intersects(sstable);
                boolean hasRequiredStatics = hasRequiredStatics(sstable);
                boolean hasPartitionLevelDeletions = hasPartitionLevelDeletions(sstable);

                if (!intersects && !hasRequiredStatics && !hasPartitionLevelDeletions)
                {
                    nonIntersectingSSTables++;
                    continue;
                }

                if (intersects || hasRequiredStatics)
                {
                    if (!sstable.isRepaired())
                        controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());

                    if (cursorMergedLegs)
                    {
                        if (cursorValueTransfer == null)
                            cursorValueTransfer = controller.cursorValueTransfer();
                        // Deferred open: the partition header (the counted read) is opened only when
                        // the merge reaches this leg's data, mirroring the iterator path's
                        // makeRowIteratorWithLowerBound.  Returns an already-opened leg (or null for
                        // an absent partition) only when no metadata lower bound is available.
                        CursorReads.PendingLeg leg = CursorReads.openLegDeferred(sstable, metadata(), partitionKey(),
                                                                         intersects ? filter.getSlices(metadata()) : Slices.NONE,
                                                                         columnFilter(), metricsCollector, cursorValueTransfer);
                        if (leg == null)
                        {
                            // the sstable does not contain the partition: keep parity with the
                            // iterator path, which still contributes an empty iterator carrying
                            // the SSTABLE's stats to the merge (see CursorReads.absentPartitionIterator)
                            inputCollector.addSSTableIterator(sstable, CursorReads.absentPartitionIterator(metadata(), partitionKey(), sstable));
                        }
                        else
                        {
                            if (cursorLegs == null)
                                cursorLegs = new ArrayList<>(view.sstables.size());
                            cursorLegs.add(leg);
                            mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                                    leg.partitionLevelDeletion().markedForDeleteAt());
                        }
                        continue;
                    }

                    // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                    UnfilteredRowIterator iter;
                    if (cursorReads)
                    {
                        Slices legSlices = intersects ? filter.getSlices(metadata()) : Slices.NONE;
                        // A reverse read reconciles its legs in the shared UnfilteredRowIterators.merge,
                        // not in the cursor-level merge, so it cannot use the deferred-leg gate.  Wrap
                        // each intersecting leg in the same lazy lower-bound iterator the iterator path
                        // uses (makeRowIteratorWithLowerBound), so the counted partition-header read is
                        // deferred until the merge descends past the leg's bound.  Without this a reverse
                        // limit query over-reads sstables the merge never needs.
                        if (filter.isReversed() && intersects)
                            iter = CursorReads.reversedLegWithLowerBound(sstable, metadata(), partitionKey(),
                                                                         legSlices, columnFilter(), metricsCollector,
                                                                         controller.cursorValueTransfer());
                        else
                            iter = CursorReads.sstableRowIterator(sstable, metadata(), partitionKey(),
                                                                  legSlices, columnFilter(), metricsCollector,
                                                                  filter.isReversed(), controller.cursorValueTransfer());
                    }
                    else
                        iter = intersects ? makeRowIteratorWithLowerBound(cfs, sstable, metricsCollector)
                                          : makeRowIteratorWithSkippedNonStaticContent(cfs, sstable, metricsCollector);

                    if (rowTransformer != null)
                        iter = Transformation.apply(iter, rowTransformer.apply(sstable.getId()));

                    inputCollector.addSSTableIterator(sstable, iter);
                    mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                            iter.partitionLevelDeletion().markedForDeleteAt());
                }
                else
                {
                    nonIntersectingSSTables++;

                    // if the sstable contained range or cell tombstones, it would intersect; since we are here, it means
                    // that there are no cell or range tombstones we are interested in (due to the filter)
                    // however, we know that there are partition level deletions in this sstable and we need to make
                    // an iterator figure out that (see `StatsMetadata.hasPartitionLevelDeletions`)

                    if (cursorMergedLegs)
                    {
                        if (cursorValueTransfer == null)
                            cursorValueTransfer = controller.cursorValueTransfer();
                        CursorReads.PendingLeg leg = CursorReads.openLeg(sstable, metadata(), partitionKey(),
                                                                         Slices.NONE, columnFilter(), metricsCollector,
                                                                         cursorValueTransfer);
                        // as below: a leg with a live partition deletion contributes nothing and is
                        // discarded; one with a real deletion must join the merge regardless of
                        // whether it shadows anything seen locally
                        if (leg != null)
                        {
                            if (!leg.partitionLevelDeletion().isLive())
                            {
                                if (!sstable.isRepaired())
                                    controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());
                                if (cursorLegs == null)
                                    cursorLegs = new ArrayList<>(view.sstables.size());
                                cursorLegs.add(leg);
                                includedDueToTombstones++;
                                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                                        leg.partitionLevelDeletion().markedForDeleteAt());
                            }
                            else
                            {
                                leg.close();
                            }
                        }
                        continue;
                    }

                    // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                    UnfilteredRowIterator iter = cursorReads
                                                 ? CursorReads.sstableRowIterator(sstable, metadata(), partitionKey(),
                                                                                  Slices.NONE, columnFilter(), metricsCollector,
                                                                                  filter.isReversed(), controller.cursorValueTransfer())
                                                 : makeRowIteratorWithSkippedNonStaticContent(cfs, sstable, metricsCollector);

                    // if the sstable contains a partition delete, then we must include it regardless of whether it
                    // shadows any other data seen locally as we can't guarantee that other replicas have seen it
                    if (!iter.partitionLevelDeletion().isLive())
                    {
                        if (!sstable.isRepaired())
                            controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());

                        if (rowTransformer != null)
                            iter = Transformation.apply(iter, rowTransformer.apply(sstable.getId()));

                        inputCollector.addSSTableIterator(sstable, iter);
                        includedDueToTombstones++;
                        mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                                iter.partitionLevelDeletion().markedForDeleteAt());
                    }
                    else
                    {
                        iter.close();
                    }
                }
            }

            if (cursorLegs != null && !cursorLegs.isEmpty() && controller.isTrackingRepairedStatus())
            {
                // Repaired-status tracking: split the surviving legs into a repaired set and an
                // unrepaired set, exactly as ReadCommand.InputCollector does, then cursor-merge each
                // set on its own. The repaired merge is handed back through addSSTableIterator with
                // a repaired attribution so finalizeIterators merges it through the repaired-data
                // digest generator; the unrepaired merge (plus the always-unrepaired memtable data)
                // is handed back with an unrepaired attribution. The query limit and row filter are
                // NOT pushed into either merge here: with tracking, executeLocally applies the
                // counter, the repaired over-read extend, and the row filter above the top-level
                // merge, so both sub-merges must produce unbounded and unfiltered.
                List<CursorReads.PendingLeg> repairedLegs = null;
                List<CursorReads.PendingLeg> unrepairedLegs = null;
                for (CursorReads.PendingLeg leg : cursorLegs)
                {
                    if (inputCollector.isRepairedForTracking(leg.sstable))
                    {
                        if (repairedLegs == null)
                            repairedLegs = new ArrayList<>(cursorLegs.size());
                        repairedLegs.add(leg);
                    }
                    else
                    {
                        if (unrepairedLegs == null)
                            unrepairedLegs = new ArrayList<>(cursorLegs.size());
                        unrepairedLegs.add(leg);
                    }
                }
                cursorLegs = null; // ownership passes to the two merges (each closes its own legs)
                List<UnfilteredRowIterator> memtableIters = cursorMemtableIters;
                cursorMemtableIters = null; // ownership passes to the unrepaired merge / object path
                Slices trackingSlices = filter.getSlices(metadata());

                if (unrepairedLegs != null && !unrepairedLegs.isEmpty())
                {
                    UnfilteredRowIterator unrepairedMerged = buildTrackingCursorMerge(unrepairedLegs, memtableIters, trackingSlices);
                    inputCollector.addSSTableIterator(unrepairedLegs.get(0).sstable, unrepairedMerged);
                }
                else if (memtableIters != null)
                {
                    // No unrepaired sstable leg: the always-unrepaired memtable iterators take the
                    // object path, exactly as in the non-tracking memtable-only case below.
                    for (UnfilteredRowIterator memtableIter : memtableIters)
                        inputCollector.addMemtableIterator(memtableIter);
                }

                if (repairedLegs != null && !repairedLegs.isEmpty())
                {
                    UnfilteredRowIterator repairedMerged = buildTrackingCursorMerge(repairedLegs, null, trackingSlices);
                    inputCollector.addSSTableIterator(repairedLegs.get(0).sstable, repairedMerged);
                }
            }
            else if (cursorLegs != null && !cursorLegs.isEmpty())
            {
                // One iterator for ALL surviving cursor legs. A single sstable leg with no
                // memtable data completes on the per-leg path (bit-for-bit, seek included);
                // anything else runs the cursor-level merge, materializing only merge winners.
                // Memtable legs join that SAME merge through the object-backed adapter
                // (memtable legs FIRST, mirroring the object merge's memtables-then-sstables input
                // order), so no object-level merge step remains for fully-supported reads.
                List<CursorReads.PendingLeg> legs = cursorLegs;
                cursorLegs = null; // ownership passes to completeSingleLeg/mergeLegs (both close the legs)
                SSTableReader attribution = legs.get(0).sstable;
                // The limit-driven production bound engages only when this merge's output is
                // the final stream executeLocally's own counter consumes (finalLimitedStream —
                // the public queryMemtableAndDisk callers get unbounded production, as their
                // javadoc promises) and the query shape passes CursorReads.limitBoundFor's gate.
                // The downstream object merge with any absent-partition placeholders contributes
                // no rows (they are empty stats-carriers), so it cannot need rows the bound
                // stopped producing.
                DataLimits.Counter productionBound = finalLimitedStream ? CursorReads.limitBoundFor(this) : null;
                // RowFilter pushdown rides the exact same routing gate — only the final
                // merged stream executeLocally's own rowFilter().filter consumes may skip
                // producing what that filter would drop; the public queryMemtableAndDisk callers
                // get unfiltered production, as their javadoc promises. Disengagement (null) never
                // blocks the cursor path: the query still merges here, filtered up top as today.
                CursorReads.FilterPushdown filterPushdown = finalLimitedStream ? CursorReads.filterPushdownFor(this) : null;
                // Clustering-column pushdown may DROP rows at production, so its
                // scan-stats accumulator must exist before the (eager) merge runs — attached to
                // the controller here so executeLocally's withMetricsRecording, created after the
                // merge completes, folds the dropped rows' contributions into every scan metric,
                // threshold and warning exactly as if they had flowed through it. Only the merged
                // paths activate it: a single-leg read never attaches the context
                // (the completeSingleLeg scope choice), so it must not carry an accumulator either.
                if (filterPushdown != null && (cursorMemtableIters != null || legs.size() >= 2))
                    filterPushdown.activateScanAccounting(controller, cfs, this);
                UnfilteredRowIterator merged;
                if (cursorMemtableIters != null)
                {
                    List<CursorReads.MergeLeg> allLegs = new ArrayList<>(cursorMemtableIters.size() + legs.size());
                    List<UnfilteredRowIterator> memtableIters = cursorMemtableIters;
                    cursorMemtableIters = null; // ownership passes to the adapters (closed by mergeLegs)
                    for (UnfilteredRowIterator memtableIter : memtableIters)
                        allLegs.add(new MemtableMergeLeg(memtableIter, filter.getSlices(metadata())));
                    allLegs.addAll(legs);
                    merged = CursorReads.mergeLegs(allLegs, metadata(), partitionKey(),
                                                   filter.getSlices(metadata()), columnFilter(), productionBound,
                                                   filterPushdown);
                }
                else
                {
                    merged = legs.size() == 1
                             ? CursorReads.completeSingleLeg(legs.get(0))
                             : CursorReads.mergeLegs(legs, metadata(), partitionKey(),
                                                     filter.getSlices(metadata()), columnFilter(), productionBound,
                                                     filterPushdown);
                }
                inputCollector.addSSTableIterator(attribution, merged);
            }
            else if (cursorMemtableIters != null)
            {
                // No surviving sstable leg (all eliminated, or none contained the partition):
                // the memtable iterators take the object path.
                //
                // A raw memtable rowIterator narrows its regular columns to the ones actually present
                // in the partition: for a partition that holds only static data it reports NO regular
                // columns. A cursor read of that same data once flushed reports the full queried set
                // (columnFilter().fetchedColumns()). UnfilteredRowIterators.digest hashes
                // columns().regulars, so a memtable-only NAMES read would serve a different column
                // header, and a different digest, than a flushed (sstable) read of the same data.
                // Replicas compare those responses under read-repair, so the mismatch is a spurious
                // digest divergence. Restore the queried column set here so the memtable-only NAMES
                // read matches the flushed read. This holds regardless of repaired-status tracking:
                // the tracking path reaches this same memtable-only branch when no sstable leg
                // survives, and it must serve the same column header as every other replica.
                boolean restoreNamesColumns = filter instanceof ClusteringIndexNamesFilter;
                for (UnfilteredRowIterator memtableIter : cursorMemtableIters)
                {
                    if (restoreNamesColumns && !memtableIter.columns().equals(columnFilter().fetchedColumns()))
                        memtableIter = withQueriedColumns(memtableIter, columnFilter().fetchedColumns());
                    inputCollector.addMemtableIterator(memtableIter);
                }
                cursorMemtableIters = null;
            }

            if (Tracing.isTracing())
                Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones",
                               nonIntersectingSSTables, view.sstables.size(), includedDueToTombstones);

            if (inputCollector.isEmpty())
                return EmptyIterators.unfilteredRow(cfs.metadata(), partitionKey(), filter.isReversed());

            StorageHook.instance.reportRead(cfs.metadata().id, partitionKey());

            List<UnfilteredRowIterator> iterators = inputCollector.finalizeIterators(cfs, nowInSec(), controller.oldestUnrepairedTombstone());

            UnfilteredRowIterator result = withSSTablesIterated(iterators, cfs.metric, metricsCollector);

            if (metricsCollector.getMergedSSTables() > DatabaseDescriptor.getSSTablesPerReadLogThreshold())
                noSpamLogger.info("The following query '{}' has read {} SSTables.", this.toCQLString(), metricsCollector.getMergedSSTables());

            return result;
        }
        catch (RuntimeException | Error e)
        {
            CursorReads.closeAll(cursorLegs, e);
            CursorReads.closeAll(cursorMemtableIters, e);
            try
            {
                inputCollector.close();
            }
            catch (Exception e1)
            {
                e.addSuppressed(e1);
            }
            throw e;
        }
    }

    @Override
    protected boolean intersects(SSTableReader sstable)
    {
        return clusteringIndexFilter().intersects(sstable.metadata().comparator, sstable.getSSTableMetadata().coveredClustering);
    }

    private UnfilteredRowIteratorWithLowerBound makeRowIteratorWithLowerBound(ColumnFamilyStore cfs,
                                                                              SSTableReader sstable,
                                                                              SSTableReadsListener listener)
    {
        return StorageHook.instance.makeRowIteratorWithLowerBound(cfs,
                                                                  sstable,
                                                                  partitionKey(),
                                                                  clusteringIndexFilter(),
                                                                  columnFilter(),
                                                                  listener);

    }

    private UnfilteredRowIterator makeRowIterator(ColumnFamilyStore cfs,
                                                  SSTableReader sstable,
                                                  ClusteringIndexNamesFilter clusteringIndexFilter,
                                                  SSTableReadsListener listener)
    {
        return StorageHook.instance.makeRowIterator(cfs,
                                                    sstable,
                                                    partitionKey(),
                                                    clusteringIndexFilter.getSlices(cfs.metadata()),
                                                    columnFilter(),
                                                    clusteringIndexFilter.isReversed(),
                                                    listener);
    }

    private UnfilteredRowIterator makeRowIteratorWithSkippedNonStaticContent(ColumnFamilyStore cfs,
                                                                             SSTableReader sstable,
                                                                             SSTableReadsListener listener)
    {
        return StorageHook.instance.makeRowIterator(cfs,
                                                    sstable,
                                                    partitionKey(),
                                                    Slices.NONE,
                                                    columnFilter(),
                                                    clusteringIndexFilter().isReversed(),
                                                    listener);
    }

    /**
     * Wraps {@code iter} so it reports {@code columns} instead of its own column set, delegating
     * everything else. Used to make a cursor-served memtable-only NAMES read report the queried
     * column set, so it serves the same column header as a flushed read of the same data. Replicas
     * compare those responses under read-repair, so the two must agree (see the caller).
     */
    private static UnfilteredRowIterator withQueriedColumns(UnfilteredRowIterator iter, RegularAndStaticColumns columns)
    {
        return new WrappingUnfilteredRowIterator()
        {
            @Override
            public UnfilteredRowIterator wrapped()
            {
                return iter;
            }

            @Override
            public RegularAndStaticColumns columns()
            {
                return columns;
            }
        };
    }

    /**
     * Return a wrapped iterator that when closed will update the sstables iterated and READ sample metrics.
     * Note that we cannot use the Transformations framework because they greedily get the static row, which
     * would cause all iterators to be initialized and hence all sstables to be accessed.
     */
    private UnfilteredRowIterator withSSTablesIterated(List<UnfilteredRowIterator> iterators,
                                                       TableMetrics metrics,
                                                       SSTableReadMetricsCollector metricsCollector)
    {
        UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators);

        if (!merged.isEmpty())
        {
            DecoratedKey key = merged.partitionKey();
            metrics.topReadPartitionFrequency.addSample(key.getKey(), 1);
            metrics.topReadPartitionSSTableCount.addSample(key.getKey(), metricsCollector.getMergedSSTables());
        }

        class UpdateSstablesIterated extends Transformation<UnfilteredRowIterator>
        {
           public void onPartitionClose()
           {
               int mergedSSTablesIterated = metricsCollector.getMergedSSTables();
               metrics.updateSSTableIterated(mergedSSTablesIterated);
               Tracing.trace("Merged data from memtables and {} sstables", mergedSSTablesIterated);
           }
        }
        return Transformation.apply(merged, new UpdateSstablesIterated());
    }

    /**
     * Builds one cursor-merged iterator for a subset of the surviving legs, used by the
     * repaired-status tracking split. {@code legs} must not be empty. Optional memtable iterators
     * (always unrepaired) join the merge through the object-backed adapter, memtables first, mirroring
     * the object merge's input order. A single sstable leg with no memtable data completes on the
     * per-leg path; anything else runs the cursor-level merge. Tracking never pushes the query limit
     * or row filter into the merge, so this always merges unbounded and unfiltered.
     */
    private UnfilteredRowIterator buildTrackingCursorMerge(List<CursorReads.PendingLeg> legs,
                                                           List<UnfilteredRowIterator> memtableIters,
                                                           Slices slices)
    {
        int memtableCount = memtableIters == null ? 0 : memtableIters.size();
        if (memtableCount == 0 && legs.size() == 1)
            return CursorReads.completeSingleLeg(legs.get(0));

        List<CursorReads.MergeLeg> allLegs = new ArrayList<>(memtableCount + legs.size());
        if (memtableIters != null)
            for (UnfilteredRowIterator memtableIter : memtableIters)
                allLegs.add(new MemtableMergeLeg(memtableIter, slices));
        allLegs.addAll(legs);
        return CursorReads.mergeLegs(allLegs, metadata(), partitionKey(), slices, columnFilter(), null, null);
    }

    /**
     * Computes the repaired-data digest for a tracking read served by the transcode fast path.
     * The transcode path produces the data response as raw bytes, which cannot feed a digest, so
     * the digest is computed over a SEPARATE cursor merge restricted to the repaired sstables --
     * the same subset {@link ReadCommand.InputCollector} selects on the iterator path. Fresh legs
     * are opened over those sstables (the transcode consumed the originals), merged, and the merged
     * stream is handed to {@link ReadCommand.InputCollector#finalizeIterators}, which wraps it with
     * {@code withRepairedDataInfo} and prepares/finalizes the {@code RepairedDataInfo} exactly as
     * the iterator path does. Draining the wrapped stream records the digest and the conclusive
     * flag on the controller's {@code RepairedDataInfo}. A repaired sstable whose partition is
     * absent contributes only an empty partition to the merge, which adds nothing to the digest, so
     * it is simply skipped here; that is digest-equivalent to the iterator path's empty-partition
     * contribution. The reopened legs use a throwaway metrics collector so the read is not
     * double-counted against the table metrics the data serve already recorded.
     */
    private void computeTranscodeRepairedDigest(ColumnFamilyStore cfs,
                                                ReadExecutionController controller,
                                                InputCollector<UnfilteredRowIterator> trackingCollector,
                                                List<SSTableReader> repairedSurvivors,
                                                ClusteringIndexFilter filter,
                                                long nowInSec)
    {
        SSTableReadMetricsCollector digestMetrics = new SSTableReadMetricsCollector();
        List<CursorReads.PendingLeg> repairedLegs = null;
        CursorReads.ValueTransfer valueTransfer = null;
        try
        {
            for (SSTableReader sstable : repairedSurvivors)
            {
                boolean intersects = intersects(sstable);
                if (valueTransfer == null)
                    valueTransfer = controller.cursorValueTransfer();
                CursorReads.PendingLeg leg = CursorReads.openLeg(sstable, metadata(), partitionKey(),
                                                                 intersects ? filter.getSlices(metadata()) : Slices.NONE,
                                                                 columnFilter(), digestMetrics, valueTransfer);
                if (leg == null)
                    continue; // partition absent: empty-partition contribution, no digest bytes
                if (repairedLegs == null)
                    repairedLegs = new ArrayList<>(repairedSurvivors.size());
                repairedLegs.add(leg);
            }

            if (repairedLegs == null || repairedLegs.isEmpty())
                return; // no repaired data present -> empty digest, matching the iterator path

            SSTableReader attribution = repairedLegs.get(0).sstable;
            UnfilteredRowIterator repairedMerged = buildTrackingCursorMerge(repairedLegs, null, filter.getSlices(metadata()));
            repairedLegs = null; // ownership passes to the merge (closed via the wrapped iterator below)
            trackingCollector.addSSTableIterator(attribution, repairedMerged);

            // finalizeIterators wraps the repaired merge with the digest generator and prepares the
            // RepairedDataInfo. Draining the returned iterators computes the digest onto the
            // controller. With unlimited limits (the transcode gate rejects limited reads) the
            // repaired counter never trips, so the whole repaired subset is digested.
            List<UnfilteredRowIterator> finalized = trackingCollector.finalizeIterators(cfs, nowInSec, controller.oldestUnrepairedTombstone());
            for (UnfilteredRowIterator iterator : finalized)
            {
                try (UnfilteredRowIterator toDrain = iterator)
                {
                    while (toDrain.hasNext())
                        toDrain.next();
                }
            }
        }
        catch (RuntimeException | Error e)
        {
            CursorReads.closeAll(repairedLegs, e);
            throw e;
        }
    }

    private boolean queriesMulticellType()
    {
        for (ColumnMetadata column : columnFilter().queriedColumns())
        {
            if (column.type.isMultiCell())
                return true;
        }
        return false;
    }

    /**
     * Do a read by querying the memtable(s) first, and then each relevant sstables sequentially by order of the sstable
     * max timestamp.
     *
     * This is used for names query in the hope of only having to query the 1 or 2 most recent query and then knowing nothing
     * more recent could be in the older sstables (which we can only guarantee if we know exactly which row we queries, and if
     * no collection or counters are included).
     * This method assumes the filter is a {@code ClusteringIndexNamesFilter}.
     */
    private UnfilteredRowIterator queryMemtableAndSSTablesInTimestampOrder(ColumnFamilyStore cfs, ColumnFamilyStore.ViewFragment view, Function<CellSourceIdentifier, Transformation<BaseRowIterator<?>>> rowTransformer, ClusteringIndexNamesFilter filter, ReadExecutionController controller)
    {
        ImmutableBTreePartition result = null;
        SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();

        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            try (UnfilteredRowIterator iter = memtable.rowIterator(partitionKey, filter.getSlices(metadata()), columnFilter(), isReversed(), metricsCollector))
            {
                if (iter == null)
                    continue;

                UnfilteredRowIterator wrapped = rowTransformer != null ? Transformation.apply(iter, rowTransformer.apply(memtable))
                                                                       : iter;
                result = add(RTBoundValidator.validate(wrapped, RTBoundValidator.Stage.MEMTABLE, false),
                             result,
                             filter,
                             false,
                             controller,
                             filter.isReversed());
            }
        }

        /* add the SSTables on disk */
        view.sstables.sort(SSTableReader.maxTimestampDescending);
        // read sorted sstables
        for (SSTableReader sstable : view.sstables)
        {
            // if we've already seen a partition tombstone with a timestamp greater
            // than the most recent update to this sstable, we're done, since the rest of the sstables
            // will also be older
            if (result != null && sstable.getMaxTimestamp() < result.partitionLevelDeletion().markedForDeleteAt())
                break;

            long currentMaxTs = sstable.getMaxTimestamp();
            filter = reduceFilter(filter, result, currentMaxTs);

            if (filter == null)
                break;

            boolean intersects = intersects(sstable);
            boolean hasRequiredStatics = hasRequiredStatics(sstable);
            boolean hasPartitionLevelDeletions = hasPartitionLevelDeletions(sstable);

            if (!intersects && !hasRequiredStatics)
            {
                // This mean that nothing queried by the filter can be in the sstable. One exception is the top-level partition deletion
                // however: if it is set, it impacts everything and must be included. Getting that top-level partition deletion costs us
                // some seek in general however (unless the partition is indexed and is in the key cache), so we first check if the sstable
                // has any tombstone at all as a shortcut.
                if (!hasPartitionLevelDeletions)
                    continue; // no tombstone at all, we can skip that sstable

                // We need to get the partition deletion and include it if it's live. In any case though, we're done with that sstable.
                try (UnfilteredRowIterator iter = makeRowIteratorWithSkippedNonStaticContent(cfs, sstable, metricsCollector))
                {
                    if (!iter.partitionLevelDeletion().isLive())
                    {
                        result = add(UnfilteredRowIterators.noRowsIterator(iter.metadata(),
                                                                           iter.partitionKey(),
                                                                           Rows.EMPTY_STATIC_ROW,
                                                                           iter.partitionLevelDeletion(),
                                                                           filter.isReversed()),
                                     result,
                                     filter,
                                     sstable.isRepaired(),
                                     controller,
                                     filter.isReversed());
                    }
                    else
                    {
                        UnfilteredRowIterator wrapped = rowTransformer != null ? Transformation.apply(iter, rowTransformer.apply(sstable.getId()))
                                                                               : iter;

                        result = add(RTBoundValidator.validate(wrapped, RTBoundValidator.Stage.SSTABLE, false),
                                     result,
                                     filter,
                                     sstable.isRepaired(),
                                     controller,
                                     filter.isReversed());
                    }
                }

                continue;
            }

            try (UnfilteredRowIterator iter = makeRowIterator(cfs, sstable, filter, metricsCollector))
            {
                if (iter.isEmpty())
                    continue;
                UnfilteredRowIterator wrapped = rowTransformer != null ? Transformation.apply(iter, rowTransformer.apply(sstable.getId()))
                                                                       : iter;
                result = add(RTBoundValidator.validate(wrapped, RTBoundValidator.Stage.SSTABLE, false),
                             result,
                             filter,
                             sstable.isRepaired(),
                             controller,
                             filter.isReversed());
            }
        }

        cfs.metric.updateSSTableIterated(metricsCollector.getMergedSSTables());

        if (metricsCollector.getMergedSSTables() > DatabaseDescriptor.getSSTablesPerReadLogThreshold())
            noSpamLogger.info("The following query '{}' has read {} SSTables.", this.toCQLString(), metricsCollector.getMergedSSTables());

        if (result == null || result.isEmpty())
            return EmptyIterators.unfilteredRow(metadata(), partitionKey(), false);

        DecoratedKey key = result.partitionKey();
        cfs.metric.topReadPartitionFrequency.addSample(key.getKey(), 1);
        cfs.metric.topReadPartitionSSTableCount.addSample(key.getKey(), metricsCollector.getMergedSSTables());
        StorageHook.instance.reportRead(cfs.metadata.id, partitionKey());

        return result.unfilteredIterator(columnFilter(), Slices.ALL, clusteringIndexFilter().isReversed());
    }

    /**
     * Cursor twin of {@link #queryMemtableAndSSTablesInTimestampOrder}: the timestamp-order NAMES
     * driver for a read the cursor gate supports.  It reuses that oracle's completeness logic
     * ({@link #reduceFilter}, {@link #isRowComplete}, {@link #add}) verbatim and the same
     * accumulate-then-emit shape.  It swaps only the per-sstable row source: every sstable leg is
     * read through {@link CursorReads} (a cursor over {@code SSTableCursorReader}), never through
     * {@code SSTableIterator}.  So no byte of row data comes from the iterator path.
     *
     * <p>The sstable loop skips older sstables the completeness check proves are unneeded, exactly as
     * the oracle does: the partition-tombstone timestamp break, then {@code reduceFilter}.  The
     * general cursor merge has no such skip, which is why a NAMES read must not be served there; it
     * would over-read older sstables the oracle skips (CASSANDRA-20428, SSTablesIteratedTest).
     *
     * <p>Two per-sstable cursor sources match the oracle's two branches:
     * <ul>
     *   <li>the intersecting (or required-statics) leg, through
     *       {@link CursorReads#namesLegIterator} (the twin of {@code makeRowIterator}), guarded by the
     *       same {@code isEmpty()} skip;</li>
     *   <li>the non-intersecting, partition-deletion-only leg, through
     *       {@link CursorReads#namesTombstoneOnlyLegIterator} (the twin of the oracle's
     *       {@code noRowsIterator} rewrap of {@code makeRowIteratorWithSkippedNonStaticContent}),
     *       forcing {@code NO_STATS} and an empty static row.  A LIVE partition deletion contributes
     *       nothing and is discarded; the leg was still opened, so the sstable is counted identically
     *       to the oracle.</li>
     * </ul>
     *
     * <p>Reversed reads: this driver materializes every leg forward (ascending clustering order) and
     * reverses only at its final emit, because {@code reduceFilter} completeness accounting runs in
     * timestamp order, which is forward-only and independent of clustering direction.  (The reverse
     * cursor {@code gotoBlock} descent once had a &gt;64KB foundation defect; it was fixed under
     * CASSANDRA-20428, so it is no longer a reason to route reversed NAMES around the reverse iterator.
     * The forward-only completeness accounting is.)
     */
    private UnfilteredRowIterator queryMemtableAndCursorsInTimestampOrder(ColumnFamilyStore cfs, ColumnFamilyStore.ViewFragment view, ClusteringIndexNamesFilter filter, ReadExecutionController controller)
    {
        CursorReads.countNamesTimestampOrderRead();

        ImmutableBTreePartition result = null;
        SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();
        // One value-transfer scratch for the whole read: the driver reads legs one at a time.  Sourced
        // from the controller so it is shared with the other commands of this query execution.
        CursorReads.ValueTransfer transfer = controller.cursorValueTransfer();

        // The whole driver materializes forward (ascending clustering order) and reverses only at the
        // final emit below. This keeps a reversed NAMES read off ReverseSlicedCursorIterator (helper A
        // always uses completeSingleLeg). reduceFilter completeness is forward-only in timestamp order,
        // independent of clustering direction, so forward materialization is byte-identical.
        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            try (UnfilteredRowIterator iter = memtable.rowIterator(partitionKey, filter.getSlices(metadata()), columnFilter(), false, metricsCollector))
            {
                if (iter == null)
                    continue;

                result = add(RTBoundValidator.validate(iter, RTBoundValidator.Stage.MEMTABLE, false),
                             result,
                             filter,
                             false,
                             controller,
                             false);
            }
        }

        /* add the SSTables on disk */
        view.sstables.sort(SSTableReader.maxTimestampDescending);
        // read sorted sstables
        for (SSTableReader sstable : view.sstables)
        {
            // if we've already seen a partition tombstone with a timestamp greater
            // than the most recent update to this sstable, we're done, since the rest of the sstables
            // will also be older
            if (result != null && sstable.getMaxTimestamp() < result.partitionLevelDeletion().markedForDeleteAt())
                break;

            long currentMaxTs = sstable.getMaxTimestamp();
            filter = reduceFilter(filter, result, currentMaxTs);

            if (filter == null)
                break;

            boolean intersects = intersects(sstable);
            boolean hasRequiredStatics = hasRequiredStatics(sstable);
            boolean hasPartitionLevelDeletions = hasPartitionLevelDeletions(sstable);

            if (!intersects && !hasRequiredStatics)
            {
                // Nothing the filter queries can be in this sstable, except a top-level partition
                // deletion. Skip the sstable if it has no tombstone at all (the same shortcut the
                // oracle takes to avoid a seek).
                if (!hasPartitionLevelDeletions)
                    continue; // no tombstone at all, we can skip that sstable

                // Read only the partition header through a cursor leg (Slices.NONE). The helper returns
                // the oracle's shape by the deletion: a non-live deletion as NO_STATS + empty static
                // carrying the tombstone; a live deletion as an empty iterator reporting the real
                // sstable.stats(). We merge it in BOTH cases, exactly as the oracle add()s in both of its
                // branches: the live case adds no content but folds the sstable's minLocalDeletionTime
                // into the merged partition's stats and the purge boundary, which the serialized deletion
                // times delta-encode against. In both cases the leg was opened, so the sstable is counted
                // like the oracle. The helper closes its own leg; the returned iterator holds no cursor.
                try (UnfilteredRowIterator iter = CursorReads.namesTombstoneOnlyLegIterator(sstable, metadata(), partitionKey(),
                                                                                            columnFilter(), metricsCollector,
                                                                                            transfer))
                {
                    result = add(iter,
                                 result,
                                 filter,
                                 sstable.isRepaired(),
                                 controller,
                                 false);
                }

                continue;
            }

            try (UnfilteredRowIterator iter = CursorReads.namesLegIterator(sstable, metadata(), partitionKey(),
                                                                           filter.getSlices(metadata()), columnFilter(),
                                                                           metricsCollector, transfer))
            {
                if (iter.isEmpty())
                    continue;
                result = add(RTBoundValidator.validate(iter, RTBoundValidator.Stage.SSTABLE, false),
                             result,
                             filter,
                             sstable.isRepaired(),
                             controller,
                             false);
            }
        }

        cfs.metric.updateSSTableIterated(metricsCollector.getMergedSSTables());

        if (metricsCollector.getMergedSSTables() > DatabaseDescriptor.getSSTablesPerReadLogThreshold())
            noSpamLogger.info("The following query '{}' has read {} SSTables.", this.toCQLString(), metricsCollector.getMergedSSTables());

        if (result == null || result.isEmpty())
            return EmptyIterators.unfilteredRow(metadata(), partitionKey(), false);

        DecoratedKey key = result.partitionKey();
        cfs.metric.topReadPartitionFrequency.addSample(key.getKey(), 1);
        cfs.metric.topReadPartitionSSTableCount.addSample(key.getKey(), metricsCollector.getMergedSSTables());
        StorageHook.instance.reportRead(cfs.metadata.id, partitionKey());

        return result.unfilteredIterator(columnFilter(), Slices.ALL, clusteringIndexFilter().isReversed());
    }

    /**
     * Merges one leg into the running result for a timestamp-order NAMES read.  Both timestamp-order
     * paths share this: the iterator oracle materializes legs in {@code filter.isReversed()} order and
     * passes {@code mergeReversed = filter.isReversed()}; the cursor twin materializes every leg
     * forward and passes {@code mergeReversed = false}, reversing only at its final emit.  The
     * {@code UnfilteredRowIterators.merge} below requires {@code iter} and the re-emitted result to
     * share direction, so the caller sets {@code mergeReversed} to match the direction its legs were
     * materialized in.  The stored {@code ImmutableBTreePartition} is comparator-ordered either way.
     */
    private ImmutableBTreePartition add(UnfilteredRowIterator iter, ImmutableBTreePartition result, ClusteringIndexNamesFilter filter, boolean isRepaired, ReadExecutionController controller, boolean mergeReversed)
    {
        if (!isRepaired)
            controller.updateMinOldestUnrepairedTombstone(iter.stats().minLocalDeletionTime);

        int maxRows = Math.max(filter.requestedRows().size(), 1);
        if (result == null)
            return ImmutableBTreePartition.create(iter, maxRows);

        try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(Arrays.asList(iter, result.unfilteredIterator(columnFilter(), Slices.ALL, mergeReversed))))
        {
            return ImmutableBTreePartition.create(merged, maxRows);
        }
    }

    private ClusteringIndexNamesFilter reduceFilter(ClusteringIndexNamesFilter filter, ImmutableBTreePartition result, long sstableTimestamp)
    {
        if (result == null)
            return filter;

        // According to the CQL semantics a row exists if at least one of its columns is not null (including the primary key columns).
        // Having the queried columns not null is unfortunately not enough to prove that a row exists as some column deletion
        // for the queried columns can exist on another node.
        // For CQL tables it is enough to have the primary key liveness and the queried columns as the primary key liveness prove that
        // the row exists even if all the other columns are deleted.
        // COMPACT tables do not have primary key liveness and by consequence we are forced to get  all the fetched columns to ensure that
        // we can return the correct result if the queried columns are deleted on another node but one of the non-queried columns is not.
        RegularAndStaticColumns columns = metadata().isCompactTable() ? columnFilter().fetchedColumns() : columnFilter().queriedColumns();

        NavigableSet<Clustering<?>> clusterings = filter.requestedRows();

        // We want to remove rows for which we have values for all requested columns. We have to deal with both static and regular rows.

        boolean removeStatic = false;
        if (!columns.statics.isEmpty())
        {
            Row staticRow = result.getRow(Clustering.STATIC_CLUSTERING);
            removeStatic = staticRow != null && isRowComplete(staticRow, columns.statics, sstableTimestamp);
        }

        NavigableSet<Clustering<?>> toRemove = null;

        DeletionInfo deletionInfo = result.deletionInfo();

        if (deletionInfo.hasRanges())
        {
            for (Clustering<?> clustering : clusterings)
            {
                RangeTombstone rt = deletionInfo.rangeCovering(clustering);
                if (rt != null && rt.deletionTime().deletes(sstableTimestamp))
                {
                    if (toRemove == null)
                        toRemove = new TreeSet<>(result.metadata().comparator);
                    toRemove.add(clustering);
                }
            }
        }

        try (UnfilteredRowIterator iterator = result.unfilteredIterator(columnFilter(), clusterings, false))
        {
            while (iterator.hasNext())
            {
                Unfiltered unfiltered = iterator.next();
                if (unfiltered == null || !unfiltered.isRow())
                    continue;

                Row row = (Row) unfiltered;
                if (!isRowComplete(row, columns.regulars, sstableTimestamp))
                    continue;

                if (toRemove == null)
                    toRemove = new TreeSet<>(result.metadata().comparator);
                toRemove.add(row.clustering());
            }
        }

        if (!removeStatic && toRemove == null)
            return filter;

        // Check if we have everything we need
        boolean hasNoMoreStatic = columns.statics.isEmpty() || removeStatic;
        boolean hasNoMoreClusterings = clusterings.isEmpty() || (toRemove != null && toRemove.size() == clusterings.size());
        if (hasNoMoreStatic && hasNoMoreClusterings)
            return null;

        if (toRemove != null)
        {
            BTreeSet.Builder<Clustering<?>> newClusterings = BTreeSet.builder(result.metadata().comparator);
            newClusterings.addAll(Sets.difference(clusterings, toRemove));
            clusterings = newClusterings.build();
        }
        return new ClusteringIndexNamesFilter(clusterings, filter.isReversed());
    }

    /**
     * We can stop reading row data from disk if what we've already read is more recent than the max timestamp
     * of the next newest SSTable that might have data for the query. We care about 1.) the row timestamp (since
     * every query cares if the row exists or not), 2.) the timestamps of the requested cells, and 3.) whether or
     * not any of the cells we've read have actual data.
     *
     * @param row a potentially incomplete {@link Row}
     * @param requestedColumns the columns requested by the query
     * @param sstableTimestamp the max timestamp of the next newest SSTable to read
     *
     * @return true if the supplied {@link Row} is complete and its data more recent than the supplied timestamp
     */
    private boolean isRowComplete(Row row, Columns requestedColumns, long sstableTimestamp)
    {
        // Static rows do not have row deletion or primary key liveness info
        if (!row.isStatic())
        {
            // If the row has been deleted or is part of a range deletion we know that we have enough information and can
            // stop at this point.
            // Note that deleted rows in compact tables (non static) do not have a row deletion. Single column
            // cells are deleted instead. By consequence this check will not work for those, but the row will appear as complete later on
            // in the method.
            if (!row.deletion().isLive() && row.deletion().time().deletes(sstableTimestamp))
                return true;

            // Note that compact tables will always have an empty primary key liveness info.
            if (!metadata().isCompactTable() && (row.primaryKeyLivenessInfo().isEmpty() || row.primaryKeyLivenessInfo().timestamp() <= sstableTimestamp))
                return false;
        }

        for (ColumnMetadata column : requestedColumns)
        {
            Cell<?> cell = row.getCell(column);

            if (cell == null || cell.timestamp() <= sstableTimestamp)
                return false;
        }

        return true;
    }

    @Override
    public boolean selectsFullPartition()
    {
        if (metadata().isStaticCompactTable())
            return true;

        return clusteringIndexFilter.selectsAllPartition() && !rowFilter().hasExpressionOnClusteringOrRegularColumns();
    }

    @Override
    public String toString()
    {
        return String.format("Read(%s columns=%s rowFilter=%s limits=%s key=%s filter=%s, nowInSec=%d)",
                             metadata().toString(),
                             columnFilter(),
                             rowFilter(),
                             limits(),
                             metadata().partitionKeyType.getString(partitionKey().getKey()),
                             clusteringIndexFilter.toString(metadata()),
                             nowInSec());
    }

    @Override
    public Verb verb()
    {
        return Verb.READ_REQ;
    }

    @Override
    protected void appendCQLWhereClause(CqlBuilder builder)
    {
        builder.append(" WHERE ").append(partitionKey().toCQLString(metadata()));

        String filterString = clusteringIndexFilter().toCQLString(metadata(), rowFilter());
        if (!filterString.isEmpty())
        {
            if (!clusteringIndexFilter().selectsAllPartition() || !rowFilter().isEmpty())
                builder.append(" AND ");
            builder.append(filterString);
        }
    }

    @Override
    public String loggableTokens()
    {
        return "token=" + partitionKey.getToken().toString();
    }

    protected void serializeSelection(DataOutputPlus out, int version) throws IOException
    {
        metadata().partitionKeyType.writeValue(partitionKey().getKey(), out);
        ClusteringIndexFilter.serializer.serialize(clusteringIndexFilter(), out, version);
    }

    protected void serializeSelectionWithoutKey(DataOutputPlus out, int version) throws IOException
    {
        ClusteringIndexFilter.serializer.serialize(clusteringIndexFilter(), out, version);
    }

    protected long selectionSerializedSize(int version)
    {
        return metadata().partitionKeyType.writtenLength(partitionKey().getKey())
             + ClusteringIndexFilter.serializer.serializedSize(clusteringIndexFilter(), version);
    }

    protected long selectionSerializedSize(Seekables seekables, int version)
    {
        return metadata().partitionKeyType.writtenLength(partitionKey().getKey())
             + ClusteringIndexFilter.serializer.serializedSize(clusteringIndexFilter(), version);
    }

    public boolean isLimitedToOnePartition()
    {
        return true;
    }

    public boolean isRangeRequest()
    {
        return false;
    }

    /*
     * When running transactionally we need to use the txn system nowInSeconds, and set whether reconciliation
     * should be performed based on whether it's part of a multiple replica read. We also allow potential txn conflicts
     * because we manage those conflicts from the txn system
     */
    public SinglePartitionReadCommand withTransactionalSettings(boolean withoutReconciliation, long nowInSeconds)
    {
        return create(serializedAtEpoch(),
                      isDigestQuery(),
                      digestVersion(),
                      acceptsTransient(),
                      PotentialTxnConflicts.ALLOW,
                      metadata(),
                      nowInSeconds,
                      columnFilter(),
                      withoutReconciliation ? rowFilter().withoutReconciliation() : rowFilter(),
                      limits(),
                      partitionKey(),
                      clusteringIndexFilter(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    /**
     * Groups multiple single partition read commands.
     */
    public static class Group extends SinglePartitionReadQuery.Group<SinglePartitionReadCommand>
    {
        public static Group create(TableMetadata metadata,
                                   long nowInSec,
                                   ColumnFilter columnFilter,
                                   RowFilter rowFilter,
                                   DataLimits limits,
                                   List<DecoratedKey> partitionKeys,
                                   ClusteringIndexFilter clusteringIndexFilter,
                                   PotentialTxnConflicts potentialTxnConflicts)
        {
            if (partitionKeys.size() == 1)
            {
                return one(SinglePartitionReadCommand.create(metadata,
                                                             nowInSec,
                                                             columnFilter,
                                                             rowFilter,
                                                             limits,
                                                             partitionKeys.get(0),
                                                             clusteringIndexFilter,
                                                             potentialTxnConflicts));
            }
            List<SinglePartitionReadCommand> commands = new ArrayList<>(partitionKeys.size());
            for (DecoratedKey partitionKey : partitionKeys)
            {
                commands.add(SinglePartitionReadCommand.create(metadata,
                                                               nowInSec,
                                                               columnFilter,
                                                               rowFilter,
                                                               limits,
                                                               partitionKey,
                                                               clusteringIndexFilter,
                                                               potentialTxnConflicts));
            }

            return create(commands, limits);
        }

        private Group(List<SinglePartitionReadCommand> commands, DataLimits limits)
        {
            super(commands, limits);
        }

        public static Group one(SinglePartitionReadCommand command)
        {
            return create(Collections.singletonList(command), command.limits());
        }

        public static Group create(List<SinglePartitionReadCommand> commands, DataLimits limits)
        {
            return commands.get(0).metadata().isVirtual() ?
                   new VirtualTableGroup(commands, limits) :
                   new Group(commands, limits);
        }

        public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, Dispatcher.RequestTime requestTime) throws RequestExecutionException
        {
            return StorageProxy.read(this, consistency, requestTime);
        }
    }

    public static class VirtualTableGroup extends Group
    {
        public VirtualTableGroup(List<SinglePartitionReadCommand> commands, DataLimits limits)
        {
            super(commands, limits);
        }

        @Override
        public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, Dispatcher.RequestTime requestTime) throws RequestExecutionException
        {
            if (queries.size() == 1)
                return queries.get(0).execute(consistency, state, requestTime);

            return PartitionIterators.concat(queries.stream()
                                                    .map(q -> q.execute(consistency, state, requestTime))
                                                    .collect(Collectors.toList()));
        }
    }

    private static class Deserializer extends SelectionDeserializer
    {
        public ReadCommand deserialize(DataInputPlus in,
                                       int version,
                                       Epoch serializedAtEpoch,
                                       boolean isDigest,
                                       int digestVersion,
                                       boolean acceptsTransient,
                                       PotentialTxnConflicts potentialTxnConflicts,
                                       TableMetadata metadata,
                                       long nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       Index.QueryPlan indexQueryPlan)
        throws IOException
        {
            DecoratedKey key = metadata.partitioner.decorateKey(metadata.partitionKeyType.readBuffer(in, DatabaseDescriptor.getMaxValueSize()));
            ClusteringIndexFilter filter = ClusteringIndexFilter.serializer.deserialize(in, version, metadata);
            return SinglePartitionReadCommand.create(serializedAtEpoch, isDigest, digestVersion, acceptsTransient, potentialTxnConflicts, metadata, nowInSec, columnFilter, rowFilter, limits, key, filter, indexQueryPlan, false);
        }
    }

    private static class AccordDeserializer extends SelectionDeserializer
    {
        final DecoratedKey key;

        private AccordDeserializer(Seekable seekable)
        {
            this.key = ((PartitionKey)seekable).partitionKey();
        }

        public ReadCommand deserialize(DataInputPlus in,
                                       int version,
                                       Epoch serializedAtEpoch,
                                       boolean isDigest,
                                       int digestVersion,
                                       boolean acceptsTransient,
                                       PotentialTxnConflicts potentialTxnConflicts,
                                       TableMetadata metadata,
                                       long nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       Index.QueryPlan indexQueryPlan)
        throws IOException
        {
            ClusteringIndexFilter filter = ClusteringIndexFilter.serializer.deserialize(in, version, metadata);
            return SinglePartitionReadCommand.create(serializedAtEpoch, isDigest, digestVersion, acceptsTransient, potentialTxnConflicts, metadata, nowInSec, columnFilter, rowFilter, limits, key, filter, indexQueryPlan, false);
        }
    }

    /**
     * {@code SSTableReaderListener} used to collect metrics about SSTable read access.
     */
    private static final class SSTableReadMetricsCollector implements SSTableReadsListener
    {
        /**
         * The number of SSTables that need to be merged. This counter is only updated for single partition queries
         * since this has been the behavior so far.
         */
        private int mergedSSTables;

        @Override
        public void onSSTableSelected(SSTableReader sstable, SelectionReason reason)
        {
            sstable.incrementReadCount();
            mergedSSTables++;
        }

        /**
         * Returns the number of SSTables that need to be merged.
         * @return the number of SSTables that need to be merged.
         */
        public int getMergedSSTables()
        {
            return mergedSSTables;
        }
    }

    public static class VirtualTableSinglePartitionReadCommand extends SinglePartitionReadCommand
    {
        protected VirtualTableSinglePartitionReadCommand(boolean isDigest,
                                                         int digestVersion,
                                                         boolean acceptsTransient,
                                                         TableMetadata metadata,
                                                         long nowInSec,
                                                         ColumnFilter columnFilter,
                                                         RowFilter rowFilter,
                                                         DataLimits limits,
                                                         DecoratedKey partitionKey,
                                                         ClusteringIndexFilter clusteringIndexFilter,
                                                         Index.QueryPlan indexQueryPlan,
                                                         boolean trackWarnings,
                                                         DataRange dataRange)
        {
            super(metadata.epoch, isDigest, digestVersion, acceptsTransient, PotentialTxnConflicts.ALLOW, metadata, nowInSec, columnFilter, 
                  rowFilter, limits, partitionKey, clusteringIndexFilter, indexQueryPlan, trackWarnings, dataRange);
        }

        @Override
        public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, Dispatcher.RequestTime requestTime) throws RequestExecutionException
        {
            return executeInternal(executionController());
        }

        @Override
        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            VirtualTable view = VirtualKeyspaceRegistry.instance.getTableNullable(metadata().id);
            UnfilteredPartitionIterator resultIterator = view.select(partitionKey, clusteringIndexFilter, columnFilter(), rowFilter(), limits());
            return limits().filter(rowFilter().filter(resultIterator, nowInSec()), nowInSec(), selectsFullPartition());
        }

        @Override
        public ReadExecutionController executionController()
        {
            return ReadExecutionController.empty();
        }

        @Override
        public ReadExecutionController executionController(boolean trackRepairedStatus)
        {
            return executionController();
        }
    }
}