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
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.MemtablePartitionData;
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
import org.apache.cassandra.db.rows.ResponseWireWriter;
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
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
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
     * legs). Repaired-status tracking IS served here: the repaired sstables merge on their own
     * and feed both the repaired-data digest and the response merge, in one pass, like
     * {@code ReadCommand.InputCollector}. A read this path declines takes the base-class default
     * ({@code ReadCommand.createResponseLocally}: {@code executeLocally}, still on the cursor
     * merge, then {@code createResponse}) with zero cursor state touched.
     */
    @Override
    public ReadResponse createResponseLocally(ReadExecutionController controller)
    {
        // a virtual table has no store to read: its own executeLocally answers it
        if (DatabaseDescriptor.cursorReadsEnabled() && !metadata().isVirtual())
        {
            ReadResponse response;
            // the current command, for the checks that look it up while reading, like executeLocally
            COMMAND.set(this);
            try
            {
                response = indexQueryPlan() == null ? queryStorageToResponseBytes(controller) : null;
            }
            finally
            {
                COMMAND.set(null);
            }
            if (response != null)
                return response;
            CursorReads.countTranscodeResponseDeclined();
        }
        return super.createResponseLocally(controller);
    }

    /**
     * Writes the data response of a forward slice read straight from the cursor merge to the
     * response bytes, through {@link ResponseSink}.  The leg resolution mirrors the cursor branch
     * of {@link #queryMemtableAndDiskInternal} (memtable and sstable candidates, the
     * {@code mostRecentPartitionTombstone} elimination, deferred legs when there is more than one
     * candidate), so the sstables read and counted are the same.
     * <p>
     * Returns null, before any leg opens, for the reads it declines (see the checks below); the
     * caller then serves them through {@code executeLocally}, which also reads them through the
     * cursor merge.
     */
    private ReadResponse queryStorageToResponseBytes(ReadExecutionController controller)
    {
        ClusteringIndexFilter filter = clusteringIndexFilter();
        // A digest is a hash of the merged rows, not their bytes: executeLocally computes it.
        if (isDigestQuery())
            return null;
        // A names read takes executeLocally's timestamp-order driver, which stops at the newest
        // sstables once they hold every requested row.  It is written here only when that driver
        // would read one sstable as stored (checked below, once the memtables are known).
        boolean names = filter instanceof ClusteringIndexNamesFilter;
        if (names && filter.isReversed())
            return null;
        // A user expression is checked on a row object, which this path never builds.
        if (!ResponseSink.ResponseFilter.streamable(rowFilter()))
            return null;
        // These stages of executeLocally walk the row objects this path never builds.
        if (CursorReads.querySizeTrackingActive(this)
            || DatabaseDescriptor.getPurgeableTobmstonesMetricGranularity() != Config.TombstonesMetricGranularity.disabled
            || slowedDownForTesting())
            return null;
        boolean tracking = controller.isTrackingRepairedStatus();
        // executeLocally counts with a counter whenever it tracks repaired data, and only then
        // for an unlimited read
        DataLimits.Counter counter = limits().isUnlimited() && !tracking
                                     ? null
                                     : limits().newCounter(nowInSec(), false, selectsFullPartition(), metadata().enforceStrictLiveness());
        // A limit already reached when the partition starts (a GROUP BY page that resumes in a new
        // group): the iterator stack then drops the static row and handles the element it read
        // ahead before the counter attached, which this path does not reproduce.
        if (counter != null && limitReachedAtPartitionStart())
            return null;

        long startTimeNanos = nanoTime();
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata());
        // rejects a read that must go through Accord, like executeLocally
        if (!potentialTxnConflicts().allowed)
            ConsensusRequestRouter.validateSafeToReadNonTransactionally(this, null);
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));
        // the sstable count the names gate below needs: decline before any other work
        if (names && view.sstables.size() != 1)
            return null;
        if (!CursorReads.isReadSupported(this, cfs, view.sstables) || !CursorReads.memtablesGivePartitionData(view.memtables))
            return null;

        view.sstables.sort(SSTableReader.maxTimestampDescending);
        Slices slices = filter.getSlices(metadata());
        long mostRecentPartitionTombstone = Long.MIN_VALUE;

        List<MemtablePartitionData> memtables = null;
        List<CursorReads.PendingLeg> sstableLegs = null;
        // sstables without the partition: the iterator path still merges an empty source for each
        List<SSTableReader> absent = null;
        List<SSTableReader> absentStaticRowOnly = null;
        SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();
        // Repaired-status tracking: the collector selects the repaired sstables (and marks the
        // digest inconclusive for pending repairs) exactly as on the iterator path
        InputCollector<UnfilteredRowIterator> collector = tracking ? iteratorsForPartition(view, controller) : null;
        List<MergeLegOwner> owned = new ArrayList<>();
        UnfilteredRowIterator repaired = null;

        try
        {
            for (Memtable memtable : view.memtables)
            {
                // the partition as the memtable holds it now, read once
                MemtablePartitionData partition = memtable.partitionData(partitionKey());
                if (partition == null)
                    continue;

                // Memtable data is always considered unrepaired
                controller.updateMinOldestUnrepairedTombstone(memtable.getMinLocalDeletionTime());
                if (memtables == null)
                    memtables = new ArrayList<>();
                memtables.add(partition);

                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                        partition.data.deletionInfo().getPartitionDeletion().markedForDeleteAt());
            }

            // A reverse read that tracks repaired data takes executeLocally, which merges per-leg
            // reverse iterators.  A names read is written here only when it reads one sstable its
            // names may be in.
            if ((filter.isReversed() && tracking)
                || (names && (memtables != null || view.sstables.size() != 1 || tracking || !intersects(view.sstables.get(0)))))
            {
                MergeLegOwner.closeAll(owned, null);
                return null;
            }

            // the memtable legs, which give their cells from the memtable's own storage
            List<NativeMemtableMergeLeg> memtableLegs = null;
            if (memtables != null)
            {
                memtableLegs = new ArrayList<>(memtables.size());
                for (MemtablePartitionData partition : memtables)
                {
                    NativeMemtableMergeLeg leg = new NativeMemtableMergeLeg(metadata(), partition, columnFilter(), slices, filter.isReversed(), false);
                    owned.add(new MergeLegOwner(leg));
                    memtableLegs.add(leg);
                }
            }

            // defer the legs that may merge, as queryMemtableAndDiskInternal does
            boolean deferLegs = view.sstables.size() > 1 || memtables != null;
            CursorLegSelection selection = selectCursorLegs(view, slices, mostRecentPartitionTombstone, deferLegs,
                                                            controller, metricsCollector, collector);
            sstableLegs = selection.legs;
            absent = selection.absent;
            absentStaticRowOnly = selection.absentStaticRowOnly;
            // the names reads the iterator path sends to queryMemtableAndSSTablesInTimestampOrder
            boolean namesRead = names && !metadata().isCounter() && !queriesMulticellType();
            if (sstableLegs != null)
            {
                for (CursorReads.PendingLeg leg : sstableLegs)
                {
                    owned.add(new MergeLegOwner(leg));
                    if (namesRead)
                        leg.namesRead();
                }
            }

            // The response merges memtables first, then the unrepaired sstables in order, then (when
            // tracking) the repaired sstables merged on their own: the iterator path's input order.
            List<EncodingStats> stats = new ArrayList<>();
            RegularAndStaticColumns columns = null;
            int sources = 0;
            if (memtableLegs != null)
            {
                for (NativeMemtableMergeLeg leg : memtableLegs)
                {
                    // the stats and columns the memtable's row iterator reports
                    stats.add(leg.legStats());
                    columns = columns == null ? leg.legColumns() : columns.mergeTo(leg.legColumns());
                    sources++;
                }
            }
            List<CursorReads.PendingLeg> unrepairedLegs = new ArrayList<>();
            List<CursorReads.PendingLeg> repairedLegs = new ArrayList<>();
            if (sstableLegs != null)
            {
                for (CursorReads.PendingLeg leg : sstableLegs)
                {
                    if (collector != null && collector.isRepairedForTracking(leg.sstable))
                    {
                        repairedLegs.add(leg);
                        continue;
                    }
                    unrepairedLegs.add(leg);
                    sources++;
                    // an empty source has no stats and no columns
                    if (leg.isEmptySource())
                        continue;
                    stats.add(leg.legStats());
                    columns = columnFilter().fetchedColumns();
                }
            }
            if (absent != null)
            {
                for (SSTableReader sstable : absent)
                {
                    if (collector != null && collector.isRepairedForTracking(sstable))
                    {
                        // part of the repaired merge, like the iterator path's empty source for it
                        collector.addSSTableIterator(sstable, CursorReads.absentPartitionIterator(metadata(), partitionKey(), sstable, filter.isReversed()));
                        continue;
                    }
                    stats.add(sstable.stats());
                    columns = columnFilter().fetchedColumns();
                    sources++;
                }
            }
            if (absentStaticRowOnly != null)
            {
                for (SSTableReader sstable : absentStaticRowOnly)
                {
                    if (collector != null && collector.isRepairedForTracking(sstable))
                    {
                        collector.addSSTableIterator(sstable, CursorReads.absentStaticRowIterator(metadata(), partitionKey(), filter.isReversed()));
                        continue;
                    }
                    // an empty source with no stats and no columns
                    sources++;
                }
            }
            if (collector != null && (!repairedLegs.isEmpty() || !collector.repairedIters.isEmpty()))
            {
                if (!repairedLegs.isEmpty())
                {
                    // a lone repaired leg is read lazily, as the iterator path reads that sstable
                    UnfilteredRowIterator repairedMerge = repairedLegs.size() == 1 ? CursorReads.lazySingleLeg(repairedLegs.get(0))
                                                                                   : buildCursorMerge(repairedLegs, null, slices, null);
                    owned.add(new MergeLegOwner(repairedMerge));
                    collector.addSSTableIterator(repairedLegs.get(0).sstable, repairedMerge);
                }
                // the repaired merge wrapped with the repaired-data digest, as finalizeIterators hands
                // it to the iterator path's merge
                List<UnfilteredRowIterator> finalized = collector.finalizeIterators(cfs, nowInSec(), controller.oldestUnrepairedTombstone());
                repaired = finalized.get(finalized.size() - 1);
                owned.add(new MergeLegOwner(repaired));
                stats.add(repaired.stats());
                columns = columns == null ? repaired.columns() : columns.mergeTo(repaired.columns());
                sources++;
            }
            if (columns == null)
                columns = columnFilter().fetchedColumns();

            // Matches queryMemtableAndDiskInternal's own placement: a pluggable, off-by-default
            // notification hook must still see every read this response serves.
            if (sources > 0)
                StorageHook.instance.reportRead(cfs.metadata().id, partitionKey());

            long nowInSec = nowInSec();
            long gcBefore = nowInSec == 0 ? Long.MIN_VALUE : cfs.gcBefore(nowInSec);
            SerializationHeader header = new SerializationHeader(false, metadata(), columns, EncodingStats.merge(stats, Function.identity()));
            ResponseSink.ResponseBuffer out = new ResponseSink.ResponseBuffer();
            ResponseWireWriter writer = new ResponseWireWriter(out, header, MessagingService.current_version, !metadata().droppedColumns.isEmpty());
            CursorReads.TombstoneScanGuard scanGuard = new CursorReads.TombstoneScanGuard(this, cfs.metric, startTimeNanos, nowInSec, partitionKey());
            ResponseSink sink = new ResponseSink(writer, metadata().comparator, slices, filter.isReversed(), metadata().enforceStrictLiveness(),
                                                 nowInSec, gcBefore, cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(),
                                                 controller.oldestUnrepairedTombstone(), scanGuard, counter, limits().isGroupByLimit(),
                                                 rowFilter().isEmpty() ? null : new ResponseSink.ResponseFilter(this), this);
            sink.singleSource = sources == 1;
            sink.namesRead = namesRead;
            // The iterator path's executeLocally sets the current command only while it builds its
            // iterator, which reads the merge up to its first unfiltered to see whether the partition
            // is empty; sstables opened after that do not see it (RowIndexEntry's size check)
            sink.onNonEmptyPartition(() -> {
                recordTopReadPartition(cfs.metric, true, metricsCollector, partitionKey());
                COMMAND.set(null);
            });

            boolean partitionWritten;
            try
            {
                // a lone object-backed source is serialized as is on the iterator path
                if (sources == 1 && repaired != null)
                    partitionWritten = CursorReads.streamResponse(repaired, columnFilter(), sink, out);
                else if (filter.isReversed() && sources == 1 && unrepairedLegs.size() == 1)
                    partitionWritten = CursorReads.streamReversedResponse(unrepairedLegs.get(0), columnFilter(), sink, out);
                else if (filter.isReversed() && (memtables != null || !unrepairedLegs.isEmpty()))
                {
                    // no repaired data here: the gate above sends a reverse read that tracks it elsewhere
                    List<CursorReads.MergeLeg> legs = responseMergeLegs(memtableLegs, unrepairedLegs, true);
                    partitionWritten = CursorReads.streamReversedResponse(legs, metadata(), partitionKey(), slices, columnFilter(), sink, out);
                }
                else
                {
                    // the memtable legs give their cells from the memtable's own storage, a lone
                    // memtable too: the merge of one leg gives what its row iterator gives
                    List<CursorReads.MergeLeg> legs = responseMergeLegs(memtableLegs, unrepairedLegs, false);
                    // pulled only as far as the merge reads it, since every pull feeds the digest
                    if (repaired != null)
                        legs.add(new MemtableMergeLeg(repaired, slices));
                    // the same partition-level skip as executeLocally's cursor merge (buildCursorMerge):
                    // an sstable leg merged with a memtable or another leg, without repaired-status tracking
                    boolean cursorMerge = !unrepairedLegs.isEmpty() && (memtables != null || unrepairedLegs.size() >= 2);
                    CursorReads.FilterPushdown partitionFilter = !tracking && cursorMerge ? CursorReads.filterPushdownFor(this) : null;
                    partitionWritten = CursorReads.streamResponse(legs, metadata(), partitionKey(), slices, columnFilter(),
                                                                  partitionFilter, sink, out);
                }
                // keep reading the repaired data past the limit, like RepairedDataInfo.extend
                if (repaired != null && counter != null && partitionWritten)
                    controller.getRepairedDataInfo().overreadRepaired(counter);
            }
            finally
            {
                scanGuard.finish();
                if (sources > 0)
                    recordSSTablesIterated(cfs.metric, metricsCollector);
            }
            if (metricsCollector.getMergedSSTables() > DatabaseDescriptor.getSSTablesPerReadLogThreshold())
                noSpamLogger.info("The following query '{}' has read {} SSTables.", this.toCQLString(), metricsCollector.getMergedSSTables());

            // closing the repaired merge records the repaired-data digest the response carries
            MergeLegOwner.closeAll(owned, null);
            owned = null;
            ReadResponse.recordDataResponseSize(out.getLength());
            ReadResponse response = ReadResponse.createTranscodedDataResponse(out.parts(), controller.getRepairedDataInfo());
            CursorReads.countTranscodeResponseServed();
            return response;
        }
        catch (IOException e)
        {
            RuntimeException failure = new RuntimeException("transcode response construction failed for " + partitionKey(), e);
            MergeLegOwner.closeAll(owned, failure);
            throw failure;
        }
        catch (RuntimeException | Error e)
        {
            MergeLegOwner.closeAll(owned, e);
            throw e;
        }
    }

    /** The rows this read returns from its partition, shared by its cursor legs for their read-ahead; null for no limit. */
    private SSTableCursorReader.RowBudget cursorRowBudget()
    {
        DataLimits limits = limits();
        if (limits.isGroupByLimit())
            return null;
        int rows = Math.min(limits.count(), limits.perPartitionCount());
        return rows == DataLimits.NO_LIMIT ? null : new SSTableCursorReader.RowBudget(rows);
    }

    /** Whether the limit is already reached before the first row of this partition. */
    private boolean limitReachedAtPartitionStart()
    {
        DataLimits.Counter probe = limits().newCounter(nowInSec(), false, selectsFullPartition(), metadata().enforceStrictLiveness());
        probe.countPartition(partitionKey(), Rows.EMPTY_STATIC_ROW);
        return probe.isDoneForPartition();
    }

    /** The sstable legs {@link #selectCursorLegs} opened for a cursor merge, and what it counted. */
    private static final class CursorLegSelection
    {
        /** The opened legs, newest sstable first; null when there are none. */
        List<CursorReads.PendingLeg> legs;
        /** Sstables whose filters let the partition through, but that do not contain it; null when none. */
        List<SSTableReader> absent;
        /** The same, for sstables read only for the static row (no slice intersects them); null when none.
         *  The iterator path's empty source for these has no stats (see {@link CursorReads#absentStaticRowIterator}). */
        List<SSTableReader> absentStaticRowOnly;
        int nonIntersectingSSTables;
        int includedDueToTombstones;
        /** The legs' shared row budget, made at the first leg; see {@link #cursorRowBudget}. */
        private SSTableCursorReader.RowBudget rowBudget;
        private boolean rowBudgetMade;

        SSTableCursorReader.RowBudget rowBudget(SinglePartitionReadCommand command)
        {
            if (!rowBudgetMade)
            {
                rowBudget = command.cursorRowBudget();
                rowBudgetMade = true;
            }
            return rowBudget;
        }
    }

    /**
     * Opens a cursor leg for each sstable a cursor merge of this read must include, in
     * {@code view.sstables} order (newest first).  It stops at the first sstable older than the
     * newest partition deletion seen, which marks the repaired-data digest inconclusive.  An
     * sstable that only holds a partition deletion joins with no slices, and only when that
     * deletion is not live.  On failure it closes the legs it opened.
     *
     * @param mostRecentPartitionTombstone the newest partition deletion of the memtables
     * @param deferLegs  open the legs deferred, for a merge of several sources
     * @param collector  the repaired-status input collector, or null when the read does not track it
     */
    private CursorLegSelection selectCursorLegs(ColumnFamilyStore.ViewFragment view,
                                                Slices slices,
                                                long mostRecentPartitionTombstone,
                                                boolean deferLegs,
                                                ReadExecutionController controller,
                                                SSTableReadMetricsCollector metricsCollector,
                                                InputCollector<UnfilteredRowIterator> collector)
    {
        CursorLegSelection selection = new CursorLegSelection();
        boolean reversed = clusteringIndexFilter().isReversed();
        CursorReads.ValueTransfer valueTransfer = null;
        try
        {
            for (SSTableReader sstable : view.sstables)
            {
                // see queryMemtableAndDiskInternal: relevant sstables are skipped, so the repaired
                // digest is inconclusive
                if (sstable.getMaxTimestamp() < mostRecentPartitionTombstone)
                {
                    if (collector != null)
                        collector.markInconclusive();
                    break;
                }

                boolean intersects = intersects(sstable);
                boolean hasRequiredStatics = hasRequiredStatics(sstable);
                boolean hasPartitionLevelDeletions = hasPartitionLevelDeletions(sstable);

                if (!intersects && !hasRequiredStatics && !hasPartitionLevelDeletions)
                {
                    selection.nonIntersectingSSTables++;
                    continue;
                }

                // one value-transfer scratch for every leg: legs read cell values one at a time
                if (valueTransfer == null)
                    valueTransfer = controller.cursorValueTransfer();
                CursorReads.PendingLeg leg;
                if (intersects || hasRequiredStatics)
                {
                    if (!sstable.isRepaired())
                        controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());

                    // A deferred leg reads its partition header only when the merge reaches its
                    // data, like the iterator path's makeRowIteratorWithLowerBound.  A leg read only
                    // for the static row opens now, like makeRowIteratorWithSkippedNonStaticContent.
                    Slices legSlices = intersects ? slices : Slices.NONE;
                    leg = deferLegs && intersects
                          ? CursorReads.openLegDeferred(sstable, metadata(), partitionKey(), legSlices,
                                                        columnFilter(), metricsCollector, valueTransfer, reversed)
                          : CursorReads.openLeg(sstable, metadata(), partitionKey(), legSlices,
                                                columnFilter(), metricsCollector, valueTransfer, reversed);
                    if (leg != null)
                        leg.rowBudget(selection.rowBudget(this));
                    if (leg == null && !intersects && deferLegs)
                    {
                        // the iterator path still merges an empty source for it, which takes part
                        // in the order its merge gives rows in
                        leg = CursorReads.PendingLeg.emptySource(sstable, metadata(), partitionKey(), columnFilter(), reversed);
                    }
                    else if (leg == null)
                    {
                        // the iterator path still merges an empty source for it
                        if (intersects)
                        {
                            if (selection.absent == null)
                                selection.absent = new ArrayList<>();
                            selection.absent.add(sstable);
                        }
                        else
                        {
                            if (selection.absentStaticRowOnly == null)
                                selection.absentStaticRowOnly = new ArrayList<>();
                            selection.absentStaticRowOnly.add(sstable);
                        }
                        continue;
                    }
                }
                else
                {
                    selection.nonIntersectingSSTables++;
                    // The sstable holds a partition deletion, which joins the merge whether or not
                    // it shadows anything seen locally: other replicas may not have seen it.
                    leg = CursorReads.openLeg(sstable, metadata(), partitionKey(), Slices.NONE,
                                              columnFilter(), metricsCollector, valueTransfer, reversed);
                    if (leg == null)
                        continue;
                    if (leg.partitionLevelDeletion().isLive())
                    {
                        leg.close();
                        continue;
                    }
                    if (!sstable.isRepaired())
                        controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());
                    selection.includedDueToTombstones++;
                }
                if (selection.legs == null)
                    selection.legs = new ArrayList<>(view.sstables.size());
                selection.legs.add(leg);
                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                        leg.partitionLevelDeletion().markedForDeleteAt());
            }
            return selection;
        }
        catch (RuntimeException | Error e)
        {
            CursorReads.closeAll(selection.legs, e);
            throw e;
        }
    }

    /** Something {@link #queryStorageToResponseBytes} opened and must close: a leg or an iterator. */
    private static final class MergeLegOwner
    {
        final CursorReads.MergeLeg leg;
        final UnfilteredRowIterator iterator;

        MergeLegOwner(CursorReads.MergeLeg leg)
        {
            this.leg = leg;
            this.iterator = null;
        }

        MergeLegOwner(UnfilteredRowIterator iterator)
        {
            this.leg = null;
            this.iterator = iterator;
        }

        static void closeAll(List<MergeLegOwner> owned, Throwable cause)
        {
            if (owned == null)
                return;
            RuntimeException failure = null;
            for (MergeLegOwner o : owned)
            {
                try
                {
                    if (o.leg != null)
                        o.leg.close();
                    else
                        o.iterator.close();
                }
                catch (RuntimeException e)
                {
                    if (cause != null)
                        cause.addSuppressed(e);
                    else if (failure == null)
                        failure = e;
                    else
                        failure.addSuppressed(e);
                }
            }
            if (failure != null)
                throw failure;
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
     * {@code ReadCommand.executeLocally}'s {@code limits().filter} and {@code rowFilter().filter}
     * consume, which is the routing condition for the cursor merge's filter pushdown
     * ({@code CursorReads.filterPushdownFor}). The PUBLIC entry points must keep passing false:
     * their external callers (counter locks, 2i searchers, cache warming) consume the unfiltered
     * partition contents this method's javadoc promises, so dropping rows there would truncate
     * their view of the partition.
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
     *                           gate for the cursor merge's filter pushdown
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
        // A non-tracking names read takes the timestamp-order completeness driver
        // (queryMemtableAndSSTablesInTimestampOrder), with cursor legs when the cursor gate supports
        // it. It skips older sstables a completeness check proves are unneeded (reduceFilter); the
        // general cursor merge below has no such skip, so serving names there would over-read older
        // sstables. With cursor legs only the per-sstable row source changes, so no byte of row data
        // comes from the iterator path. A tracking
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
            boolean supported = CursorReads.isReadSupported(this, cfs, view.sstables);
            return queryMemtableAndSSTablesInTimestampOrder(cfs, view, rowTransformer, names, controller, supported,
                                                            rowTransformer == null && supported);
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
        // a supported read that still takes the iterator path counts each sstable leg as fallen back
        boolean supported = CursorReads.isReadSupported(this, cfs, view.sstables);
        // a memtable that cannot give its partition data takes the iterator path, and its sstable
        // legs count as fallen back
        boolean cursorReads = rowTransformer == null && supported && CursorReads.memtablesGivePartitionData(view.memtables);
        // Memtable legs of a gate-supported read join the SAME cursor-level merge as the
        // sstable legs (NativeMemtableMergeLeg over the partition data read here), so the
        // partitions are COLLECTED here instead of being handed to the object merge as iterators,
        // unless no sstable leg survives the elimination loop below: then their row iterators,
        // over the same partition data, take the object path, like the iterator path.
        List<MemtablePartitionData> cursorMemtables = null;
        // With more than one candidate sstable, cursor-served legs are OPENED (cursor +
        // partition header + static row — enough for the mostRecentPartitionTombstone elimination
        // below) but their rows deferred, so that all surviving legs can be merged at the CURSOR
        // level after the loop instead of each leg being independently materialized. A
        // single-candidate read without memtable data keeps the per-leg call inside the
        // loop, bit for bit; with memtable data even a single sstable leg is deferred so it
        // can cursor-merge with the memtable leg(s).
        boolean cursorMergedLegs;
        List<CursorReads.PendingLeg> cursorLegs = null;
        // the sstables a cursor merge found without the partition, added after the merge
        CursorLegSelection absentSelection = null;
        InputCollector<UnfilteredRowIterator> inputCollector = iteratorsForPartition(view, controller);
        try
        {
            SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();

            for (Memtable memtable : view.memtables)
            {
                if (cursorReads)
                {
                    // the partition as the memtable holds it now, read once
                    MemtablePartitionData partition = memtable.partitionData(partitionKey());
                    if (partition == null)
                        continue;
                    if (memtable.getMinTimestamp() != Memtable.NO_MIN_TIMESTAMP)
                        minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp());
                    // Memtable data is always considered unrepaired
                    controller.updateMinOldestUnrepairedTombstone(memtable.getMinLocalDeletionTime());
                    if (cursorMemtables == null)
                        cursorMemtables = new ArrayList<>();
                    cursorMemtables.add(partition);
                    mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                            partition.data.deletionInfo().getPartitionDeletion().markedForDeleteAt());
                    continue;
                }

                UnfilteredRowIterator iter = memtable.rowIterator(partitionKey(), filter.getSlices(metadata()), columnFilter(), filter.isReversed(), metricsCollector);
                if (iter == null)
                    continue;

                if (memtable.getMinTimestamp() != Memtable.NO_MIN_TIMESTAMP)
                    minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp());

                if (rowTransformer != null)
                    iter = Transformation.apply(iter, rowTransformer.apply(memtable));

                // Memtable data is always considered unrepaired
                controller.updateMinOldestUnrepairedTombstone(memtable.getMinLocalDeletionTime());
                inputCollector.addMemtableIterator(RTBoundValidator.validate(iter, RTBoundValidator.Stage.MEMTABLE, false));

                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                        iter.partitionLevelDeletion().markedForDeleteAt());
            }
            // With memtable data present, even a single-candidate sstable read defers its
            // leg so the memtable leg(s) can join one cursor-level merge with it.  A reverse read
            // merges its legs in reverse (CursorReads.mergeLegsReversed).
            cursorMergedLegs = cursorReads && (view.sstables.size() > 1 || cursorMemtables != null);

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

            if (cursorMergedLegs)
            {
                CursorLegSelection selection = selectCursorLegs(view, filter.getSlices(metadata()), mostRecentPartitionTombstone,
                                                                true, controller, metricsCollector, inputCollector);
                cursorLegs = selection.legs;
                nonIntersectingSSTables = selection.nonIntersectingSSTables;
                includedDueToTombstones = selection.includedDueToTombstones;
                absentSelection = selection;
            }
            else
            {
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

                        // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                        UnfilteredRowIterator iter;
                        if (cursorReads)
                        {
                            Slices legSlices = intersects ? filter.getSlices(metadata()) : Slices.NONE;
                            // A lone reverse leg is wrapped in the same lazy lower-bound iterator the
                            // iterator path uses (makeRowIteratorWithLowerBound), so its counted
                            // partition-header read happens where the iterator path's does.
                            if (filter.isReversed() && intersects)
                                iter = CursorReads.reversedLegWithLowerBound(sstable, metadata(), partitionKey(),
                                                                             legSlices, columnFilter(), metricsCollector,
                                                                             controller.cursorValueTransfer());
                            else
                                iter = CursorReads.sstableRowIterator(sstable, metadata(), partitionKey(),
                                                                      legSlices, columnFilter(), metricsCollector,
                                                                      filter.isReversed(), controller.cursorValueTransfer(),
                                                                      cursorRowBudget());
                        }
                        else
                        {
                            if (supported)
                                CursorReads.countSstableLegFellBackToIterator();
                            iter = intersects ? makeRowIteratorWithLowerBound(cfs, sstable, metricsCollector)
                                              : makeRowIteratorWithSkippedNonStaticContent(cfs, sstable, metricsCollector);
                        }

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

                        // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                        if (!cursorReads && supported)
                            CursorReads.countSstableLegFellBackToIterator();
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
            }

            if (cursorLegs != null && !cursorLegs.isEmpty() && controller.isTrackingRepairedStatus())
            {
                // Repaired-status tracking: split the surviving legs into a repaired set and an
                // unrepaired set, exactly as ReadCommand.InputCollector does. The repaired legs are
                // cursor-merged and handed back through addSSTableIterator with a repaired
                // attribution, so finalizeIterators wraps the merge in the repaired-data digest
                // generator. The unrepaired legs and the memtable data are merged after that, with
                // the wrapped repaired merge as one more leg, as the replica response does
                // (queryStorageToResponseBytes). A separate unrepaired merge would be read before
                // the merge above it knows it needs it, and open sstables the iterator path's lazy
                // iterators (makeRowIteratorWithLowerBound) never read once the limit is reached.
                // The query limit and row filter are NOT pushed into either merge here: with
                // tracking, executeLocally applies the counter, the repaired over-read extend, and
                // the row filter above the top-level merge, so both merges must produce unbounded
                // and unfiltered.
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
                // the unrepaired legs and the memtable iterators stay owned here until they are merged
                cursorLegs = unrepairedLegs;
                Slices trackingSlices = filter.getSlices(metadata());

                if (unrepairedLegs == null && cursorMemtables != null)
                {
                    // No unrepaired sstable leg: the always-unrepaired memtable iterators take the
                    // object path, exactly as in the non-tracking memtable-only case below.
                    for (MemtablePartitionData partition : cursorMemtables)
                        inputCollector.addMemtableIterator(memtableIterator(partition));
                    cursorMemtables = null;
                }

                if (repairedLegs != null && !repairedLegs.isEmpty())
                {
                    // a lone repaired leg is read lazily, as the iterator path reads that sstable
                    UnfilteredRowIterator repairedMerged = repairedLegs.size() == 1 ? CursorReads.lazySingleLeg(repairedLegs.get(0))
                                                                                    : buildCursorMerge(repairedLegs, null, trackingSlices, null);
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
                cursorLegs = null; // ownership passes to the iterator completeSingleLeg/mergeLegs return
                SSTableReader attribution = legs.get(0).sstable;
                // The merge is lazy: executeLocally's own DataLimits counter stops pulling once the
                // limit is reached, and the merge stops with it, so no limit is passed down.
                // RowFilter pushdown engages only for the final merged stream executeLocally's own
                // rowFilter().filter consumes (finalLimitedStream) — only it may skip producing
                // what that filter would drop; the public queryMemtableAndDisk callers get
                // unfiltered production, as their javadoc promises. Disengagement (null) never
                // blocks the cursor path: the query still merges here, filtered up top as today.
                CursorReads.FilterPushdown filterPushdown = finalLimitedStream ? CursorReads.filterPushdownFor(this) : null;
                // Clustering-column pushdown may DROP rows at production, so its scan-stats
                // accumulator must exist before the merge produces anything — attached to the
                // controller here so executeLocally's withMetricsRecording folds the dropped rows'
                // contributions into every scan metric, threshold and warning exactly as if they
                // had flowed through it. Only the merged paths activate it: a single-leg read never
                // attaches the context (the completeSingleLeg scope choice), so it must not carry an
                // accumulator either.
                if (filterPushdown != null && (cursorMemtables != null || legs.size() >= 2))
                    filterPushdown.activateScanAccounting(controller, cfs, this);
                List<MemtablePartitionData> memtables = cursorMemtables;
                cursorMemtables = null; // the merge's memtable legs read them, and it closes its legs
                UnfilteredRowIterator merged = buildCursorMerge(legs, memtables, filter.getSlices(metadata()), filterPushdown);
                inputCollector.addSSTableIterator(attribution, merged);
            }
            else if (cursorMemtables != null)
            {
                // No surviving sstable leg (all eliminated, or none contained the partition):
                // the memtable iterators take the object path, as on the iterator path.
                for (MemtablePartitionData partition : cursorMemtables)
                    inputCollector.addMemtableIterator(memtableIterator(partition));
                cursorMemtables = null;
            }

            // The sstables without the partition: keep parity with the iterator path, which still
            // contributes an empty iterator carrying the sstable's stats to the merge.  They are
            // added after the memtable and merged data, so the merge reports the partition key
            // object of its first source, as the iterator path's does: the first memtable's.
            if (absentSelection != null)
                addAbsentPartitionIterators(absentSelection, inputCollector, filter.isReversed());

            if (Tracing.isTracing())
                Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones",
                               nonIntersectingSSTables, view.sstables.size(), includedDueToTombstones);

            if (inputCollector.isEmpty() && cursorLegs == null)
                return EmptyIterators.unfilteredRow(cfs.metadata(), partitionKey(), filter.isReversed());

            StorageHook.instance.reportRead(cfs.metadata().id, partitionKey());

            boolean hasRepaired = !inputCollector.repairedIters.isEmpty();
            List<UnfilteredRowIterator> iterators = inputCollector.finalizeIterators(cfs, nowInSec(), controller.oldestUnrepairedTombstone());
            // the cursor merge applies the row filter and counts the rows it drops
            CursorReads.ScanStatsAccumulator filterInMerge = controller.scanStats();
            // first, as the memtables it holds come first on the iterator path
            if (cursorLegs != null)
                iterators.add(0, mergeUnrepairedWithRepaired(cursorLegs, cursorMemtables, iterators, hasRepaired));
            cursorLegs = null;
            cursorMemtables = null;

            UnfilteredRowIterator result = withSSTablesIterated(iterators, cfs.metric, metricsCollector, filterInMerge);

            if (metricsCollector.getMergedSSTables() > DatabaseDescriptor.getSSTablesPerReadLogThreshold())
                noSpamLogger.info("The following query '{}' has read {} SSTables.", this.toCQLString(), metricsCollector.getMergedSSTables());

            return result;
        }
        catch (RuntimeException | Error e)
        {
            CursorReads.closeAll(cursorLegs, e);
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
     * Return a wrapped iterator that when closed will update the sstables iterated and READ sample metrics.
     * Note that we cannot use the Transformations framework because they greedily get the static row, which
     * would cause all iterators to be initialized and hence all sstables to be accessed.
     */
    private UnfilteredRowIterator withSSTablesIterated(List<UnfilteredRowIterator> iterators,
                                                       TableMetrics metrics,
                                                       SSTableReadMetricsCollector metricsCollector,
                                                       CursorReads.ScanStatsAccumulator filterInMerge)
    {
        UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators);

        // The iterator path samples the partition when its first row is read, before the row
        // filter.  A cursor merge that drops that row samples it when it drops it.
        DecoratedKey key = merged.partitionKey();
        // The iterator path reads only the first row here, while the current command is set, so
        // only the sstables opened for that row see it (RowIndexEntry's size check).  A cursor
        // merge that drops that row reads on, so it clears the command when it drops it.
        ReadCommand current = getCommand();
        if (filterInMerge != null)
            filterInMerge.runOnFirstDroppedRow(() -> {
                recordTopReadPartition(metrics, true, metricsCollector, key);
                COMMAND.set(null);
            });
        boolean empty;
        try
        {
            empty = merged.isEmpty();
        }
        catch (TombstoneOverwhelmingException e)
        {
            // A cursor merge counts the rows its row filter drops as it reads them, so the
            // tombstone threshold can fail on this first read.  The iterator path fails on the same
            // row later, in the scan metrics stage, after the read has been set up and with the
            // metrics recorded on close, so fail on the first read of the reader instead.
            if (filterInMerge == null)
                throw e;
            merged = failsOnFirstRead(merged, e);
            empty = false;
        }
        boolean sampledOnDroppedRow = filterInMerge != null && filterInMerge.stopRunningOnFirstDroppedRow();
        COMMAND.set(current);
        if (!empty && !sampledOnDroppedRow)
            recordTopReadPartition(metrics, true, metricsCollector, key);

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

    /** {@code merged} with its rows replaced by {@code failure}, thrown on the first read. */
    private static UnfilteredRowIterator failsOnFirstRead(UnfilteredRowIterator merged, TombstoneOverwhelmingException failure)
    {
        return new WrappingUnfilteredRowIterator()
        {
            public UnfilteredRowIterator wrapped()
            {
                return merged;
            }

            public boolean hasNext()
            {
                throw failure;
            }

            public Unfiltered next()
            {
                throw failure;
            }
        };
    }

    /** The top-partition samples of a read whose merged partition is not empty. */
    private static void recordTopReadPartition(TableMetrics metrics, boolean nonEmpty, SSTableReadMetricsCollector metricsCollector,
                                               DecoratedKey key)
    {
        if (!nonEmpty)
            return;
        metrics.topReadPartitionFrequency.addSample(key.getKey(), 1);
        metrics.topReadPartitionSSTableCount.addSample(key.getKey(), metricsCollector.getMergedSSTables());
    }

    /** The transcode path's twin of {@link #withSSTablesIterated}'s partition-close update. */
    private static void recordSSTablesIterated(TableMetrics metrics, SSTableReadMetricsCollector metricsCollector)
    {
        int mergedSSTablesIterated = metricsCollector.getMergedSSTables();
        metrics.updateSSTableIterated(mergedSSTablesIterated);
        Tracing.trace("Merged data from memtables and {} sstables", mergedSSTablesIterated);
    }

    /**
     * Builds one cursor-merged iterator over {@code legs}, which must not be empty: all the
     * surviving legs, or one side of the repaired-status tracking split.  Optional memtable
     * partitions (always unrepaired) join the merge as {@link NativeMemtableMergeLeg}s giving the
     * iterator path's row objects, memtables first, mirroring the object merge's input order.  A single sstable leg with no memtable data
     * completes on the per-leg path; anything else runs the cursor-level merge.  The returned
     * iterator is lazy: it merges only as far as its reader pulls (the repaired-data digest, the
     * response, or the tracking over-read), and closes the legs on close.
     *
     * @param filterPushdown the row filter pushed into a forward merge, or null; tracking never
     *                       pushes it
     */
    private UnfilteredRowIterator buildCursorMerge(List<CursorReads.PendingLeg> legs,
                                                   List<MemtablePartitionData> memtables,
                                                   Slices slices,
                                                   CursorReads.FilterPushdown filterPushdown)
    {
        if (clusteringIndexFilter().isReversed())
            return buildReversedCursorMerge(legs, memtables, slices);
        if (memtables == null && legs.size() == 1)
            return CursorReads.completeSingleLeg(legs.get(0));
        return CursorReads.mergeLegs(cursorMergeLegs(legs, memtables, slices, false), metadata(), mergedPartitionKey(memtables),
                                     slices, columnFilter(), filterPushdown);
    }

    /**
     * The key object the iterator path's merge reports: that of its first source, the first
     * memtable that holds the partition, else this read's key (an sstable iterator reports the key
     * it was asked for).  The read size guardrail counts the heap size of this object.
     */
    private DecoratedKey mergedPartitionKey(List<MemtablePartitionData> memtables)
    {
        return memtables == null || memtables.isEmpty() ? partitionKey() : memtables.get(0).key;
    }

    /** The memtable's row iterator over {@code partition}, validated like any memtable iterator. */
    private UnfilteredRowIterator memtableIterator(MemtablePartitionData partition)
    {
        ClusteringIndexFilter filter = clusteringIndexFilter();
        return RTBoundValidator.validate(partition.unfilteredIterator(columnFilter(), filter.getSlices(metadata()), filter.isReversed()),
                                         RTBoundValidator.Stage.MEMTABLE, false);
    }

    /**
     * The merge of a read that tracks repaired data: the unrepaired sstable legs and the memtable
     * iterators, with the repaired merge {@code finalizeIterators} wrapped in the repaired-data
     * digest generator as the last leg when there is one.  The repaired leg is read only as far as
     * the merge reads it, since every row it gives feeds the digest.  Takes the repaired merge out
     * of {@code iterators}.  The returned iterator owns all its legs.
     */
    private UnfilteredRowIterator mergeUnrepairedWithRepaired(List<CursorReads.PendingLeg> unrepairedLegs,
                                                              List<MemtablePartitionData> memtables,
                                                              List<UnfilteredRowIterator> iterators,
                                                              boolean hasRepaired)
    {
        Slices slices = clusteringIndexFilter().getSlices(metadata());
        if (!hasRepaired)
            return buildCursorMerge(unrepairedLegs, memtables, slices, null);
        boolean reversed = clusteringIndexFilter().isReversed();
        List<CursorReads.MergeLeg> legs = cursorMergeLegs(unrepairedLegs, memtables, slices, reversed);
        legs.add(new MemtableMergeLeg(iterators.remove(iterators.size() - 1), slices));
        DecoratedKey key = mergedPartitionKey(memtables);
        return reversed ? CursorReads.mergeLegsReversed(legs, metadata(), key, slices, columnFilter())
                        : CursorReads.mergeLegs(legs, metadata(), key, slices, columnFilter(), null);
    }

    /** Adds an empty iterator, carrying the sstable's stats, for each sstable the cursor leg
     *  selection found without the partition. */
    private void addAbsentPartitionIterators(CursorLegSelection selection, InputCollector<UnfilteredRowIterator> inputCollector,
                                             boolean reversed)
    {
        if (selection.absent != null)
        {
            for (SSTableReader sstable : selection.absent)
                inputCollector.addSSTableIterator(sstable, CursorReads.absentPartitionIterator(metadata(), partitionKey(), sstable, reversed));
        }
        if (selection.absentStaticRowOnly != null)
        {
            for (SSTableReader sstable : selection.absentStaticRowOnly)
                inputCollector.addSSTableIterator(sstable, CursorReads.absentStaticRowIterator(metadata(), partitionKey(), reversed));
        }
    }

    /** The legs of the replica response's merge: the memtable legs first, then the sstable legs,
     *  wrapped for a reverse merge when {@code reversed}. */
    private static List<CursorReads.MergeLeg> responseMergeLegs(List<NativeMemtableMergeLeg> memtableLegs,
                                                                List<CursorReads.PendingLeg> legs,
                                                                boolean reversed)
    {
        List<CursorReads.MergeLeg> allLegs = new ArrayList<>((memtableLegs == null ? 0 : memtableLegs.size()) + legs.size() + 1);
        if (memtableLegs != null)
            allLegs.addAll(memtableLegs);
        for (CursorReads.PendingLeg leg : legs)
            allLegs.add(reversed ? new CursorReads.ReverseMergeLeg(leg) : leg);
        return allLegs;
    }

    /**
     * The legs of an {@code executeLocally} cursor merge: the memtable partitions first, as
     * {@link NativeMemtableMergeLeg}s giving the iterator path's row objects, then the sstable legs,
     * wrapped for a reverse merge when {@code reversed}.  The merge closes them.
     */
    private List<CursorReads.MergeLeg> cursorMergeLegs(List<CursorReads.PendingLeg> legs,
                                                       List<MemtablePartitionData> memtables,
                                                       Slices slices,
                                                       boolean reversed)
    {
        int memtableCount = memtables == null ? 0 : memtables.size();
        List<CursorReads.MergeLeg> allLegs = new ArrayList<>(memtableCount + legs.size() + 1);
        if (memtables != null)
        {
            for (MemtablePartitionData partition : memtables)
                allLegs.add(new NativeMemtableMergeLeg(metadata(), partition, columnFilter(), slices, reversed, true));
        }
        for (CursorReads.PendingLeg leg : legs)
            allLegs.add(reversed ? new CursorReads.ReverseMergeLeg(leg) : leg);
        return allLegs;
    }

    /** The reverse read's merge of {@code legs} and {@code memtables}, memtables first; a lone
     *  sstable leg is read as stored. */
    private UnfilteredRowIterator buildReversedCursorMerge(List<CursorReads.PendingLeg> legs,
                                                           List<MemtablePartitionData> memtables,
                                                           Slices slices)
    {
        if (memtables == null && legs.size() == 1)
            return CursorReads.completeSingleLegReversed(legs.get(0));
        return CursorReads.mergeLegsReversed(cursorMergeLegs(legs, memtables, slices, true), metadata(), mergedPartitionKey(memtables),
                                             slices, columnFilter());
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
     *
     * <p>With {@code cursorLegs}, every sstable leg is read through {@link CursorReads}
     * ({@link CursorReads#namesLegIterator}, and {@link CursorReads#namesTombstoneOnlyLegIterator} for
     * a partition deletion), never through {@code SSTableIterator}; the completeness logic
     * ({@link #reduceFilter}, {@link #add}) is the same.  The general cursor merge has no such skip of
     * older sstables, which is why a names read must not be served there (CASSANDRA-20428,
     * SSTablesIteratedTest).
     *
     * @param cursorSupported the read passed the cursor support gate, so each sstable leg read here
     *                        through the iterator path is counted in
     *                        {@link CursorReads#sstableLegsFellBackToIterator()}
     * @param cursorLegs      read the sstable legs through cursors; needs no row transformer
     */
    private UnfilteredRowIterator queryMemtableAndSSTablesInTimestampOrder(ColumnFamilyStore cfs, ColumnFamilyStore.ViewFragment view, Function<CellSourceIdentifier, Transformation<BaseRowIterator<?>>> rowTransformer, ClusteringIndexNamesFilter filter, ReadExecutionController controller, boolean cursorSupported, boolean cursorLegs)
    {
        if (cursorLegs)
            CursorReads.countNamesTimestampOrderRead();
        ImmutableBTreePartition result = null;
        SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();
        // One value-transfer scratch for the whole read: the driver reads legs one at a time.  Sourced
        // from the controller so it is shared with the other commands of this query execution.
        CursorReads.ValueTransfer transfer = cursorLegs ? controller.cursorValueTransfer() : null;
        // Each source is merged into the result in the read's direction: the order the merge gives
        // equal rows in decides which source's clustering the merged row keeps.
        boolean legsReversed = filter.isReversed();

        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            try (UnfilteredRowIterator iter = memtable.rowIterator(partitionKey, filter.getSlices(metadata()), columnFilter(), legsReversed, metricsCollector))
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
                             legsReversed);
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
                result = cursorLegs
                         ? addCursorPartitionDeletion(sstable, result, filter, controller, metricsCollector, transfer)
                         : addPartitionDeletion(cfs, sstable, rowTransformer, result, filter, controller, metricsCollector, cursorSupported);
                continue;
            }

            if (!cursorLegs && cursorSupported)
                CursorReads.countSstableLegFellBackToIterator();
            try (UnfilteredRowIterator iter = cursorLegs
                                              ? CursorReads.namesLegIterator(sstable, metadata(), partitionKey(), filter.getSlices(metadata()),
                                                                             columnFilter(), metricsCollector, transfer, legsReversed)
                                              : makeRowIterator(cfs, sstable, filter, metricsCollector))
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
                             legsReversed);
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

    /** Merges the partition deletion of an sstable the names filter does not intersect, read through the iterator path. */
    private ImmutableBTreePartition addPartitionDeletion(ColumnFamilyStore cfs, SSTableReader sstable,
                                                         Function<CellSourceIdentifier, Transformation<BaseRowIterator<?>>> rowTransformer,
                                                         ImmutableBTreePartition result, ClusteringIndexNamesFilter filter,
                                                         ReadExecutionController controller, SSTableReadMetricsCollector metricsCollector,
                                                         boolean cursorSupported)
    {
        if (cursorSupported)
            CursorReads.countSstableLegFellBackToIterator();
        try (UnfilteredRowIterator iter = makeRowIteratorWithSkippedNonStaticContent(cfs, sstable, metricsCollector))
        {
            if (!iter.partitionLevelDeletion().isLive())
            {
                return add(UnfilteredRowIterators.noRowsIterator(iter.metadata(),
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
            UnfilteredRowIterator wrapped = rowTransformer != null ? Transformation.apply(iter, rowTransformer.apply(sstable.getId()))
                                                                   : iter;
            return add(RTBoundValidator.validate(wrapped, RTBoundValidator.Stage.SSTABLE, false),
                       result,
                       filter,
                       sstable.isRepaired(),
                       controller,
                       filter.isReversed());
        }
    }

    /**
     * The cursor twin of {@link #addPartitionDeletion}: reads only the partition header through a
     * cursor leg ({@code Slices.NONE}).  {@link CursorReads#namesTombstoneOnlyLegIterator} returns the
     * same shapes: a non-live deletion as {@code NO_STATS} and an empty static row carrying the
     * tombstone, a live deletion as an empty iterator with the real {@code sstable.stats()}.  Both are
     * merged, as the iterator path merges both: the live case adds no content but folds the
     * sstable's {@code minLocalDeletionTime} into the merged partition's stats and the purge boundary.
     * The sstable is counted the same either way.  The helper closes its own leg.
     */
    private ImmutableBTreePartition addCursorPartitionDeletion(SSTableReader sstable, ImmutableBTreePartition result,
                                                               ClusteringIndexNamesFilter filter, ReadExecutionController controller,
                                                               SSTableReadMetricsCollector metricsCollector, CursorReads.ValueTransfer transfer)
    {
        try (UnfilteredRowIterator iter = CursorReads.namesTombstoneOnlyLegIterator(sstable, metadata(), partitionKey(),
                                                                                    columnFilter(), metricsCollector, transfer,
                                                                                    filter.isReversed()))
        {
            return add(iter, result, filter, sstable.isRepaired(), controller, filter.isReversed());
        }
    }

    /**
     * Merges one leg into the running result for a timestamp-order NAMES read, in the direction
     * {@code mergeReversed}, which must be the direction {@code iter} was read in.
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