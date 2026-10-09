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

package org.apache.cassandra.db.cursorreads;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongFunction;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.filter.BloomFilterTracker;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Compares the cursor read path with the iterator path (the reference) on six surfaces.  Each
 * surface is its own failure category, so a divergence names what broke:
 * <ul>
 *   <li>S1: canonical records and the intra-node bytes of {@code executeLocally};</li>
 *   <li>S2: the replica response from {@code createResponseLocally} ({@code ReadCommandVerbHandler.doRead}),
 *       serialized with {@code ReadResponse.serializer} at every supported messaging version, for the
 *       data read, its digest copy, and a copy that tracks repaired status (repaired digest and
 *       conclusive flag);</li>
 *   <li>S3: the CQL {@code ResultMessage.Rows} bytes at protocol v5 and the {@code PagingState} of
 *       every page;</li>
 *   <li>S4: the scan metrics ({@link ScanMetricsCapture}), client warning text, and the sstables-per-read
 *       histogram;</li>
 *   <li>S5: failures: exception class and message, and the records emitted before the throw;</li>
 *   <li>S6: cursor engagement: the iterator run never touches the cursor path; the cursor run either
 *       serves every sstable lookup the iterator run made (served + without-partition legs equal the
 *       reference's lookups, no leg falls back to the iterator) or the support gate rejects the read
 *       for a named reason.</li>
 * </ul>
 * Every case pins one {@code nowInSec}.  The case seed's low bit picks which path runs first.  Cold mode
 * empties the chunk cache and the key cache before each path.
 */
public abstract class CursorReadOracle extends CursorReadDifferentialTester
{
    private static final Logger logger = LoggerFactory.getLogger(CursorReadOracle.class);

    public enum Surface { S1, S2, S3, S4, S5, S6 }

    /** Why {@link CursorReads#isReadSupported} rejects a read on purpose. */
    public enum UnsupportedReason
    {
        /** a dropped collection or counter column, not re-added, is still in an sstable header */
        DROPPED_COLLECTION_OR_COUNTER_IN_HEADER
    }

    /** Test only: in the cursor run of an S3 case, lowers {@code remaining} by one in every paging
     *  state before it is recorded and used to fetch the next page.  Proves S3 can fail. */
    static volatile boolean TEST_CORRUPT_CURSOR_PAGING_STATE = false;

    /** Cases compared, and cases whose cursor run served at least one sstable leg, since the last reset. */
    protected long casesRun, casesWithServedLegs;

    @After
    public void resetOracleHooks()
    {
        TEST_CORRUPT_CURSOR_PAGING_STATE = false;
        StorageProxy.localReadsAsReplicaResponses = false;
    }

    /** A divergence on one or more surfaces. */
    public static final class Divergence extends AssertionError
    {
        public final EnumSet<Surface> surfaces;

        Divergence(String message, EnumSet<Surface> surfaces)
        {
            super(message);
            this.surfaces = surfaces;
        }
    }

    /** One comparison: a label, a seed, a pinned {@code nowInSec}, which path runs first, and cold mode. */
    protected static final class ReadCase
    {
        final String label;
        final long seed;
        final long nowInSec;
        final boolean cursorFirst;
        final boolean cold;
        final UnsupportedReason unsupported;
        /** Whether the cursor run's replica reads must all be written by the transcode path
         *  ({@code CursorReads.transcodeResponsesServed}) or all declined; null to not check. */
        final Boolean transcode;

        private ReadCase(String label, long seed, long nowInSec, boolean cold, UnsupportedReason unsupported, Boolean transcode)
        {
            this.label = label;
            this.seed = seed;
            this.nowInSec = nowInSec;
            this.cursorFirst = (seed & 1) == 1;
            this.cold = cold;
            this.unsupported = unsupported;
            this.transcode = transcode;
        }

        static ReadCase of(String label, long seed)
        {
            return new ReadCase(label, seed, FBUtilities.nowInSeconds(), false, null, null);
        }

        ReadCase at(long nowInSec)
        {
            return new ReadCase(label, seed, nowInSec, cold, unsupported, transcode);
        }

        ReadCase cold()
        {
            return new ReadCase(label, seed, nowInSec, true, unsupported, transcode);
        }

        ReadCase rejectedBecause(UnsupportedReason reason)
        {
            return new ReadCase(label, seed, nowInSec, cold, reason, transcode);
        }

        ReadCase named(String newLabel)
        {
            return new ReadCase(newLabel, seed, nowInSec, cold, unsupported, transcode);
        }

        ReadCase expectTranscode(boolean served)
        {
            return new ReadCase(label, seed, nowInSec, cold, unsupported, served);
        }

        @Override
        public String toString()
        {
            return String.format("%s [seed=%dL nowInSec=%d first=%s cold=%s%s%s]", label, seed, nowInSec,
                                 cursorFirst ? "cursor" : "iterator", cold,
                                 unsupported == null ? "" : " unsupported=" + unsupported,
                                 transcode == null ? "" : " transcode=" + transcode);
        }
    }

    /** What one path produced. */
    protected static final class Observation
    {
        final boolean cursor;
        final List<String> records = new ArrayList<>();
        /** labelled byte outputs (S1 intra-node bytes, S2 responses, S3 pages) */
        final Map<String, byte[]> bytes = new LinkedHashMap<>();
        /** labelled text outputs (paging states, repaired digest + conclusive flag) */
        final Map<String, String> text = new LinkedHashMap<>();
        ScanMetricsCapture.Snapshot scan;
        long sstablesPerReadCount, sstablesPerReadMin, sstablesPerReadMax;
        String failure;
        long served, withoutPartition, fellBack, lookups;
        long transcodeServed, transcodeDeclined;
        boolean probed;
        boolean gateSupported;

        Observation(boolean cursor)
        {
            this.cursor = cursor;
        }

        String path()
        {
            return cursor ? "cursor" : "iterator";
        }
    }

    @FunctionalInterface
    protected interface PathRead
    {
        void read(Observation into) throws Throwable;
    }

    // ---------------------------------------------------------------- entry points

    /** S1, S4, S5, S6 for {@code executeLocally}. */
    protected void assertExecuteLocallyMatches(ReadCase c, ColumnFamilyStore cfs, LongFunction<SinglePartitionReadCommand> command)
    {
        compare(c, cfs, command, EnumSet.of(Surface.S1), into -> {
            SinglePartitionReadCommand cmd = command.apply(c.nowInSec);
            try (ReadExecutionController controller = cmd.executionController();
                 UnfilteredPartitionIterator partitions = cmd.executeLocally(controller))
            {
                canonicalRecordsInto(partitions, into.records);
            }
            into.bytes.put("intra-node", intraNodeBytes(command.apply(c.nowInSec)));
        });
    }

    /** S2, S4, S5, S6 for the data read, its digest copy, and a repaired-status tracking copy. */
    protected void assertReplicaResponsesMatch(ReadCase c, ColumnFamilyStore cfs, LongFunction<SinglePartitionReadCommand> command)
    {
        SinglePartitionReadCommand probe = command.apply(c.nowInSec);
        boolean eligible = c.transcode != null ? c.transcode : transcodeEligible(probe, cfs);
        compare(c.named(c.label + " / data").expectTranscode(eligible), cfs, command, EnumSet.of(Surface.S2),
                into -> replicaResponse(command.apply(c.nowInSec), false, into));
        compare(c.named(c.label + " / digest").expectTranscode(false), cfs, command, EnumSet.of(Surface.S2),
                into -> replicaResponse(digestCopy(command.apply(c.nowInSec)), false, into));
        // a reverse or names read that tracks repaired data is declined
        boolean trackingEligible = eligible && !probe.isReversed() && !(probe.clusteringIndexFilter() instanceof ClusteringIndexNamesFilter);
        compare(c.named(c.label + " / tracking repaired status").expectTranscode(trackingEligible), cfs, command,
                EnumSet.of(Surface.S2), into -> replicaResponse(command.apply(c.nowInSec), true, into));
    }

    /** S1, S2, S4, S5, S6: both of the above. */
    protected void assertAllReadSurfacesMatch(ReadCase c, ColumnFamilyStore cfs, LongFunction<SinglePartitionReadCommand> command)
    {
        assertExecuteLocallyMatches(c, cfs, command);
        assertReplicaResponsesMatch(c, cfs, command);
    }

    /**
     * S3, S4, S5, S6 for a CQL SELECT ({@code %s} is the current table) run through
     * {@code SelectStatement.execute} at CL ONE and protocol v5, page by page.
     *
     * @param pageSize 0 or less for no paging
     */
    protected void assertCqlMatches(ReadCase c, ColumnFamilyStore cfs, String query, int pageSize)
    {
        CQLStatement statement = QueryProcessor.getStatement(formatQuery(query), ClientState.forInternalCalls());
        compare(c.named(c.label + " / " + query + " pageSize=" + pageSize), cfs, null, EnumSet.of(Surface.S3),
                into -> cqlPages(statement, pageSize, c.nowInSec, into));
    }

    /**
     * {@link #assertCqlMatches}, with each read the coordinator makes served as a replica response
     * ({@code ReadCommand.createResponseLocally}), the response a remote replica sends.  On the
     * cursor run that is the transcode path, unless it declines the read.
     */
    protected void assertCqlReplicaResponsesMatch(ReadCase c, ColumnFamilyStore cfs, String query, int pageSize)
    {
        CQLStatement statement = QueryProcessor.getStatement(formatQuery(query), ClientState.forInternalCalls());
        compare(c.named(c.label + " / " + query + " pageSize=" + pageSize + " as replica responses"), cfs, null, EnumSet.of(Surface.S3),
                into -> {
                    StorageProxy.localReadsAsReplicaResponses = true;
                    try
                    {
                        cqlPages(statement, pageSize, c.nowInSec, into);
                    }
                    finally
                    {
                        StorageProxy.localReadsAsReplicaResponses = false;
                    }
                });
    }

    /** Whether the transcode path serves {@code command} instead of declining it: a forward read
     *  with a slice filter; a reverse read over at most one sstable and no memtable data; a forward
     *  names read over exactly one sstable whose clusterings it may hit, and no memtable data. */
    protected static boolean transcodeEligible(SinglePartitionReadCommand command, ColumnFamilyStore cfs)
    {
        boolean names = command.clusteringIndexFilter() instanceof ClusteringIndexNamesFilter;
        if (names && command.isReversed())
            return false;
        if (!names && !command.isReversed())
            return true;
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, command.partitionKey()));
        if (view.sstables.size() > 1)
            return false;
        if (names && (view.sstables.isEmpty()
                      || !command.clusteringIndexFilter().intersects(cfs.metadata().comparator,
                                                                     view.sstables.get(0).getSSTableMetadata().coveredClustering)))
            return false;
        for (Memtable memtable : view.memtables)
        {
            try (UnfilteredRowIterator partition = memtable.rowIterator(command.partitionKey()))
            {
                if (partition != null)
                    return false;
            }
        }
        return true;
    }

    /**
     * S1, S4, S5, S6 for an internal paging sequence ({@code SinglePartitionPager.fetchPageUnfiltered},
     * which runs each page's {@code forPaging} command through {@code executeLocally}): the records of
     * every page and the paging state after it.
     */
    protected void assertPagedMatches(ReadCase c, ColumnFamilyStore cfs, LongFunction<SinglePartitionReadCommand> command, int pageSize)
    {
        compare(c.named(c.label + " / paged " + pageSize), cfs, command, EnumSet.of(Surface.S1), into -> {
            SinglePartitionPager pager = (SinglePartitionPager) command.apply(c.nowInSec).getPager(null, ProtocolVersion.V5);
            int page = 0;
            while (!pager.isExhausted())
            {
                if (page > 100_000)
                    throw new AssertionError("paging did not terminate");
                into.records.add("PAGE " + page);
                try (ReadExecutionController controller = pager.executionController();
                     UnfilteredPartitionIterator partitions = pager.fetchPageUnfiltered(cfs.metadata(), pageSize, controller))
                {
                    canonicalRecordsInto(partitions, into.records);
                }
                PagingState state = pager.state();
                into.text.put("paging state after page " + page,
                              state == null ? "null" : ByteBufferUtil.bytesToHex(state.serialize(ProtocolVersion.V5)));
                page++;
            }
        });
    }

    /** Runs any read on both paths under S4, S5, S6, comparing {@code surfaces} too.  For harness tests. */
    protected void compareCustom(ReadCase c, ColumnFamilyStore cfs, EnumSet<Surface> surfaces, PathRead read)
    {
        compare(c, cfs, null, surfaces, read);
    }

    /** Fails unless the cursor run of every case so far served at least one sstable leg. */
    protected void assertEveryCaseServedLegs()
    {
        if (casesRun == 0 || casesWithServedLegs != casesRun)
            throw new AssertionError("only " + casesWithServedLegs + " of " + casesRun + " cases served an sstable leg on the cursor path");
    }

    // ---------------------------------------------------------------- reads

    private static byte[] intraNodeBytes(SinglePartitionReadCommand command) throws Exception
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller);
             DataOutputBuffer buffer = new DataOutputBuffer())
        {
            UnfilteredPartitionIterators.serializerForIntraNode()
                                        .serialize(partitions, command.columnFilter(), buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
    }

    protected static SinglePartitionReadCommand digestCopy(SinglePartitionReadCommand command)
    {
        return (SinglePartitionReadCommand) command.copyAsDigestQuery(ReplicaUtils.full(FBUtilities.getBroadcastAddressAndPort()));
    }

    private static void replicaResponse(SinglePartitionReadCommand command, boolean trackRepairedStatus, Observation into) throws Exception
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, trackRepairedStatus);
        if (!response.isDigestResponse())
        {
            into.text.put("repaired digest", ByteBufferUtil.bytesToHex(response.repairedDataDigest())
                                             + " conclusive=" + response.isRepairedDigestConclusive());
            // the records the coordinator reads back from the response
            try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
            {
                canonicalRecordsInto(partitions, into.records);
            }
        }
        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                ReadResponse.serializer.serialize(response, buffer, version.value);
                into.bytes.put("response@" + version, buffer.toByteArray());
            }
        }
    }

    private static void cqlPages(CQLStatement statement, int pageSize, long nowInSec, Observation into)
    {
        PagingState state = null;
        int page = 0;
        do
        {
            if (page > 100_000)
                throw new AssertionError("paging did not terminate");
            QueryOptions options = QueryOptions.create(ConsistencyLevel.ONE, Collections.emptyList(), false, pageSize,
                                                       state, ConsistencyLevel.SERIAL, ProtocolVersion.V5,
                                                       KEYSPACE, Long.MIN_VALUE, nowInSec);
            ResultMessage.Rows rows = (ResultMessage.Rows) statement.execute(QueryState.forInternalCalls(), options,
                                                                             Dispatcher.RequestTime.forImmediateExecution());
            ByteBuf buf = Unpooled.buffer(ResultMessage.codec.encodedSize(rows, ProtocolVersion.V5));
            try
            {
                ResultMessage.codec.encode(rows, buf, ProtocolVersion.V5);
                byte[] encoded = new byte[buf.readableBytes()];
                buf.readBytes(encoded);
                into.bytes.put("page " + page, encoded);
            }
            finally
            {
                buf.release();
            }
            state = rows.result.metadata.getPagingState();
            if (state != null && into.cursor && TEST_CORRUPT_CURSOR_PAGING_STATE)
                state = new PagingState(state.partitionKey, state.rowMark, state.remaining - 1, state.remainingInPartition);
            into.text.put("paging state after page " + page,
                          state == null ? "null" : ByteBufferUtil.bytesToHex(state.serialize(ProtocolVersion.V5)));
            page++;
        }
        while (state != null);
    }

    // ---------------------------------------------------------------- engine

    private void compare(ReadCase c, ColumnFamilyStore cfs, LongFunction<SinglePartitionReadCommand> gateProbe,
                         EnumSet<Surface> surfaces, PathRead read)
    {
        logger.info("oracle case {}", c);
        Observation iterator;
        Observation cursor;
        if (c.cursorFirst)
        {
            cursor = runPath(true, c, cfs, gateProbe, read);
            iterator = runPath(false, c, cfs, gateProbe, read);
        }
        else
        {
            iterator = runPath(false, c, cfs, gateProbe, read);
            cursor = runPath(true, c, cfs, gateProbe, read);
        }

        logger.debug("oracle case {}: served={} without-partition={} lookups={}", c.label, cursor.served, cursor.withoutPartition, iterator.lookups);
        casesRun++;
        if (cursor.served > 0)
            casesWithServedLegs++;

        Map<Surface, String> divergences = new LinkedHashMap<>();
        if (surfaces.contains(Surface.S1) || surfaces.contains(Surface.S2))
            put(divergences, surfaces.contains(Surface.S1) ? Surface.S1 : Surface.S2, compareRecords(iterator, cursor));
        if (surfaces.contains(Surface.S1) || surfaces.contains(Surface.S2) || surfaces.contains(Surface.S3))
        {
            Surface bytesSurface = surfaces.contains(Surface.S3) ? Surface.S3
                                 : surfaces.contains(Surface.S2) ? Surface.S2 : Surface.S1;
            put(divergences, bytesSurface, compareBytes(iterator, cursor));
            put(divergences, bytesSurface, compareText(iterator, cursor));
        }
        put(divergences, Surface.S4, compareMetrics(iterator, cursor));
        put(divergences, Surface.S5, Objects.equals(iterator.failure, cursor.failure) ? null
                                     : String.format("failure diverged%n  iterator: %s%n  cursor:   %s", iterator.failure, cursor.failure));
        put(divergences, Surface.S6, compareEngagement(c, iterator, cursor));

        if (!divergences.isEmpty())
        {
            StringBuilder sb = new StringBuilder("cursor read oracle divergence in ").append(c);
            for (Map.Entry<Surface, String> e : divergences.entrySet())
                sb.append(String.format("%n[%s] %s", e.getKey(), e.getValue()));
            throw new Divergence(sb.toString(), EnumSet.copyOf(divergences.keySet()));
        }
    }

    private static void put(Map<Surface, String> divergences, Surface surface, String message)
    {
        if (message == null)
            return;
        divergences.merge(surface, message, (a, b) -> a + System.lineSeparator() + b);
    }

    private Observation runPath(boolean cursor, ReadCase c, ColumnFamilyStore cfs,
                                LongFunction<SinglePartitionReadCommand> gateProbe, PathRead read)
    {
        Observation into = new Observation(cursor);
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
        try
        {
            if (gateProbe != null)
            {
                SinglePartitionReadCommand probe = gateProbe.apply(c.nowInSec);
                into.probed = true;
                into.gateSupported = CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe));
            }
            if (c.cold)
                emptyCaches();

            long served = CursorReads.sstableLegsServed();
            long without = CursorReads.sstableLegsWithoutPartition();
            long fellBack = CursorReads.sstableLegsFellBackToIterator();
            long transcodeServed = CursorReads.transcodeResponsesServed();
            long transcodeDeclined = CursorReads.transcodeResponsesDeclined();
            long lookups = filterLookups(cfs);
            ClearableHistogram sstablesPerRead = (ClearableHistogram) cfs.metric.sstablesPerReadHistogram.cf;
            sstablesPerRead.clear();

            into.scan = ScanMetricsCapture.capture(cfs, () -> {
                try
                {
                    read.read(into);
                }
                catch (Throwable t)
                {
                    into.failure = describeFailure(t);
                }
            });

            into.served = CursorReads.sstableLegsServed() - served;
            into.withoutPartition = CursorReads.sstableLegsWithoutPartition() - without;
            into.fellBack = CursorReads.sstableLegsFellBackToIterator() - fellBack;
            into.transcodeServed = CursorReads.transcodeResponsesServed() - transcodeServed;
            into.transcodeDeclined = CursorReads.transcodeResponsesDeclined() - transcodeDeclined;
            into.lookups = filterLookups(cfs) - lookups;
            into.sstablesPerReadCount = sstablesPerRead.getCount();
            into.sstablesPerReadMin = into.sstablesPerReadCount == 0 ? 0 : sstablesPerRead.getSnapshot().getMin();
            into.sstablesPerReadMax = into.sstablesPerReadCount == 0 ? 0 : sstablesPerRead.getSnapshot().getMax();
            return into;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static void emptyCaches()
    {
        if (ChunkCache.instance != null)
            ChunkCache.instance.clear();
        CacheService.instance.invalidateKeyCache();
    }

    /** Partition lookups made against this table's sstables, counted by their bloom filter trackers:
     *  every lookup that passes the key range check ends as a true positive, false positive or true
     *  negative.  Counted independently of the cursor code. */
    private static long filterLookups(ColumnFamilyStore cfs)
    {
        long total = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable instanceof SSTableReaderWithFilter)
            {
                BloomFilterTracker tracker = ((SSTableReaderWithFilter) sstable).getFilterTracker();
                total += tracker.getTruePositiveCount() + tracker.getFalsePositiveCount() + tracker.getTrueNegativeCount();
            }
        }
        return total;
    }

    private static String describeFailure(Throwable t)
    {
        StringBuilder sb = new StringBuilder();
        for (Throwable cause = t; cause != null; cause = cause.getCause())
        {
            if (sb.length() > 0)
                sb.append(" <- caused by ");
            sb.append(cause.getClass().getName()).append(": ").append(cause.getMessage());
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------- comparators

    private static String compareRecords(Observation iterator, Observation cursor)
    {
        try
        {
            CursorReadDifferentialTester.compareRecords(iterator.records, cursor.records);
            return null;
        }
        catch (AssertionError e)
        {
            return e.getMessage();
        }
    }

    private static String compareBytes(Observation iterator, Observation cursor)
    {
        if (!iterator.bytes.keySet().equals(cursor.bytes.keySet()))
            return "byte outputs diverged: iterator " + iterator.bytes.keySet() + " vs cursor " + cursor.bytes.keySet();
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, byte[]> e : iterator.bytes.entrySet())
        {
            byte[] cursorBytes = cursor.bytes.get(e.getKey());
            if (!Arrays.equals(e.getValue(), cursorBytes))
            {
                try
                {
                    assertResponseBytesEqual(e.getValue(), cursorBytes);
                }
                catch (AssertionError diff)
                {
                    if (sb.length() > 0)
                        sb.append(System.lineSeparator());
                    sb.append(e.getKey()).append(": ").append(diff.getMessage());
                }
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static String compareText(Observation iterator, Observation cursor)
    {
        if (iterator.text.equals(cursor.text))
            return null;
        StringBuilder sb = new StringBuilder("text outputs diverged");
        List<String> keys = new ArrayList<>(iterator.text.keySet());
        for (String key : cursor.text.keySet())
            if (!keys.contains(key))
                keys.add(key);
        for (String key : keys)
        {
            String a = iterator.text.get(key);
            String b = cursor.text.get(key);
            if (!Objects.equals(a, b))
                sb.append(String.format("%n  %s%n    iterator: %s%n    cursor:   %s", key, a, b));
        }
        return sb.toString();
    }

    private static String compareMetrics(Observation iterator, Observation cursor)
    {
        StringBuilder sb = new StringBuilder();
        try
        {
            ScanMetricsCapture.assertParity("iterator vs cursor", iterator.scan, cursor.scan);
        }
        catch (AssertionError e)
        {
            sb.append(e.getMessage());
        }
        if (iterator.sstablesPerReadCount != cursor.sstablesPerReadCount
            || iterator.sstablesPerReadMin != cursor.sstablesPerReadMin
            || iterator.sstablesPerReadMax != cursor.sstablesPerReadMax)
        {
            if (sb.length() > 0)
                sb.append(System.lineSeparator());
            sb.append(String.format("sstables-per-read histogram diverged: iterator count=%d min=%d max=%d, cursor count=%d min=%d max=%d",
                                    iterator.sstablesPerReadCount, iterator.sstablesPerReadMin, iterator.sstablesPerReadMax,
                                    cursor.sstablesPerReadCount, cursor.sstablesPerReadMin, cursor.sstablesPerReadMax));
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static String compareEngagement(ReadCase c, Observation iterator, Observation cursor)
    {
        List<String> problems = new ArrayList<>();
        if (iterator.served != 0 || iterator.withoutPartition != 0 || iterator.fellBack != 0)
            problems.add(String.format("the iterator run touched the cursor path: served=%d without-partition=%d fell-back=%d",
                                       iterator.served, iterator.withoutPartition, iterator.fellBack));
        if (cursor.fellBack != 0)
            problems.add(cursor.fellBack + " sstable leg(s) passed the support gate but were read by the iterator path");
        if (iterator.transcodeServed != 0 || iterator.transcodeDeclined != 0)
            problems.add("the iterator run reached the transcode path");
        if (c.transcode != null && c.unsupported == null)
        {
            // a read that fails is neither served nor declined
            if (c.transcode && (cursor.transcodeDeclined != 0 || (cursor.transcodeServed == 0 && cursor.failure == null)))
                problems.add(String.format("the transcode path declined a read it must serve: served=%d declined=%d",
                                           cursor.transcodeServed, cursor.transcodeDeclined));
            if (!c.transcode && cursor.transcodeServed != 0)
                problems.add(String.format("the transcode path served %d read(s) it must decline", cursor.transcodeServed));
        }

        long candidates = iterator.lookups;
        long cursorLegs = cursor.served + cursor.withoutPartition;
        if (c.unsupported != null)
        {
            if (cursor.gateSupported)
                problems.add("expected the support gate to reject the read (" + c.unsupported + ") but it accepted it");
            if (cursorLegs != 0)
                problems.add("the support gate rejected the read (" + c.unsupported + ") but the cursor path still served "
                             + cursorLegs + " leg(s)");
        }
        else
        {
            if (cursor.probed && !cursor.gateSupported)
                problems.add("the support gate rejected a read this case expects the cursor path to serve");
            if (cursorLegs != candidates)
                problems.add(String.format("served + without-partition legs (%d + %d) != sstable lookups made by the iterator run (%d)",
                                           cursor.served, cursor.withoutPartition, candidates));
            if (cursorLegs != cursor.lookups)
                problems.add(String.format("served + without-partition legs (%d) != sstable lookups made by the cursor run (%d)",
                                           cursorLegs, cursor.lookups));
        }
        return problems.isEmpty() ? null : String.join(System.lineSeparator(), problems);
    }
}
