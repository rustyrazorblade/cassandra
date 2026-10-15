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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.filter.BloomFilterTracker;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Compares cursor range reads with the iterator range path (the reference) on six surfaces, the
 * range counterpart of {@link CursorReadOracle}.  Each surface is its own failure category:
 * <ul>
 *   <li>S1: canonical records and the intra-node bytes of {@code executeLocally};</li>
 *   <li>S2: the replica response ({@code ReadCommandVerbHandler.doRead}) serialized at every
 *       supported messaging version, for the data read, its digest copy, and a copy that tracks
 *       repaired status (repaired digest and conclusive flag);</li>
 *   <li>S3: the CQL {@code ResultMessage.Rows} bytes at protocol v5 and the paging state of every page;</li>
 *   <li>S4: the scan metrics ({@link ScanMetricsCapture}), client warnings, the sstables-per-range-read
 *       histogram, the count of range reads the table recorded latency for, and the key cache
 *       entries each path adds, replaces or removes;</li>
 *   <li>S5: failures: exception class and message, and the records emitted before the throw;</li>
 *   <li>S6: what the reads did besides their result: the iterator run never reaches the cursor
 *       path; the cursor run serves every range read it makes (or the case names the reason it must
 *       not), no sstable leg falls back to the iterator, and neither run makes a partition lookup
 *       through the bloom filters or changes the key cache size.</li>
 * </ul>
 * Every case pins one {@code nowInSec}.  The case seed's low bit picks which path runs first.  Both
 * paths start from the same key cache.  Cold mode empties the chunk cache and the key cache before
 * each path.  S1 and S2 also run each read as a coordinator that tracks warnings sends it, so the
 * read size guardrail measures the rows ({@code ReadCommand.withQuerySizeTracking}).
 */
public abstract class RangeReadOracle extends CursorReadDifferentialTester
{
    private static final Logger logger = LoggerFactory.getLogger(RangeReadOracle.class);

    public enum Surface { S1, S2, S3, S4, S5, S6 }

    /** Test only: in the cursor run of an S3 case, lowers {@code remaining} by one in every paging
     *  state before it is recorded and used to fetch the next page.  Proves S3 can fail. */
    static volatile boolean TEST_CORRUPT_CURSOR_PAGING_STATE = false;

    /** Cases compared since the class started. */
    protected long casesRun;

    @After
    public void resetRangeOracleHooks()
    {
        CursorRangeReads.gateListener = null;
        TEST_CORRUPT_CURSOR_PAGING_STATE = false;
        StorageProxy.localReadsAsReplicaResponses = false;
        setRangePath(false);
    }

    /** Turns the cursor range read path on or off; off is the iterator path, the reference. */
    protected static void setRangePath(boolean cursor)
    {
        DatabaseDescriptor.setCursorRangeReadsEnabled(cursor);
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
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

    /** One comparison: a label, a seed, a pinned {@code nowInSec}, which path runs first, cold mode,
     *  and whether the cursor run must serve its reads. */
    protected static final class ReadCase
    {
        final String label;
        final long seed;
        final long nowInSec;
        final boolean cursorFirst;
        final boolean cold;
        /** Why the gate must leave every range read of the cursor run to the iterator path; null when the cursor path must serve them all. */
        final CursorRangeReads.Unsupported notServedBecause;

        private ReadCase(String label, long seed, long nowInSec, boolean cold, CursorRangeReads.Unsupported notServedBecause)
        {
            this.label = label;
            this.seed = seed;
            this.nowInSec = nowInSec;
            this.cursorFirst = (seed & 1) == 1;
            this.cold = cold;
            this.notServedBecause = notServedBecause;
        }

        static ReadCase of(String label, long seed)
        {
            return new ReadCase(label, seed, FBUtilities.nowInSeconds(), false, null);
        }

        ReadCase at(long nowInSec)
        {
            return new ReadCase(label, seed, nowInSec, cold, notServedBecause);
        }

        ReadCase cold()
        {
            return new ReadCase(label, seed, nowInSec, true, notServedBecause);
        }

        ReadCase named(String newLabel)
        {
            return new ReadCase(newLabel, seed, nowInSec, cold, notServedBecause);
        }

        /** The gate must leave the cursor run's range reads to the iterator path, for {@code reason}. */
        ReadCase notServedBecause(CursorRangeReads.Unsupported reason)
        {
            return new ReadCase(label, seed, nowInSec, cold, reason);
        }

        @Override
        public String toString()
        {
            return String.format("%s [seed=%dL nowInSec=%d first=%s cold=%s%s]", label, seed, nowInSec,
                                 cursorFirst ? "cursor" : "iterator", cold,
                                 notServedBecause == null ? "" : " not served: " + notServedBecause);
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
        long sstablesPerRangeReadCount, sstablesPerRangeReadMin, sstablesPerRangeReadMax;
        long rangeReadsRecorded;
        String failure;
        long rangeServed, rangeNotServed, rangeNotServedForReason, legsFellBack, filterLookups, keyCacheDelta;
        /** This path's changes to the key cache entries of the table's sstables. */
        List<String> keyCacheChanges = Collections.emptyList();

        Observation(boolean cursor)
        {
            this.cursor = cursor;
        }
    }

    @FunctionalInterface
    protected interface PathRead
    {
        void read(Observation into) throws Throwable;
    }

    // ---------------------------------------------------------------- entry points

    /** S1, S4, S5, S6 for {@code executeLocally}, as is and tracking warnings. */
    protected void assertExecuteLocallyMatches(ReadCase c, ColumnFamilyStore cfs, LongFunction<PartitionRangeReadCommand> command)
    {
        for (boolean trackWarnings : new boolean[]{ false, true })
        {
            LongFunction<PartitionRangeReadCommand> read = trackWarnings ? tracking(command) : command;
            compare(trackWarnings ? c.named(c.label + " / tracking warnings") : c, cfs, EnumSet.of(Surface.S1), into -> {
                PartitionRangeReadCommand first = read.apply(c.nowInSec);
                try (ReadExecutionController controller = first.executionController();
                     UnfilteredPartitionIterator partitions = first.executeLocally(controller))
                {
                    canonicalRecordsInto(partitions, into.records);
                }
                into.bytes.put("intra-node", intraNodeBytes(read.apply(c.nowInSec)));
            });
        }
    }

    /** The commands of {@code command}, as a coordinator that tracks warnings sends them. */
    private static LongFunction<PartitionRangeReadCommand> tracking(LongFunction<PartitionRangeReadCommand> command)
    {
        return now -> {
            // a copy: the supplier may hand out one command object for every read
            PartitionRangeReadCommand read = command.apply(now);
            read = read.withUpdatedLimit(read.limits());
            read.trackWarnings();
            return read;
        };
    }

    /** S2, S4, S5, S6 for the data read alone, as is. */
    protected void assertDataResponseAloneMatches(ReadCase c, ColumnFamilyStore cfs, LongFunction<PartitionRangeReadCommand> command)
    {
        compare(c.named(c.label + " / data"), cfs, EnumSet.of(Surface.S2),
                into -> replicaResponse(command.apply(c.nowInSec), false, into));
    }

    /** S2, S4, S5, S6 for the data read alone, as is and tracking warnings. */
    protected void assertDataResponseMatches(ReadCase c, ColumnFamilyStore cfs, LongFunction<PartitionRangeReadCommand> command)
    {
        assertDataResponseAloneMatches(c, cfs, command);
        compare(c.named(c.label + " / data, tracking warnings"), cfs, EnumSet.of(Surface.S2),
                into -> replicaResponse(tracking(command).apply(c.nowInSec), false, into));
    }

    /** S2, S4, S5, S6 for the data read (as is and tracking warnings), its digest copy, and a
     *  repaired-status tracking copy. */
    protected void assertReplicaResponsesMatch(ReadCase c, ColumnFamilyStore cfs, LongFunction<PartitionRangeReadCommand> command)
    {
        assertDataResponseMatches(c, cfs, command);
        compare(c.named(c.label + " / digest"), cfs, EnumSet.of(Surface.S2),
                into -> replicaResponse(digestCopy(command.apply(c.nowInSec)), false, into));
        compare(trackingCase(c.named(c.label + " / tracking repaired status")), cfs, EnumSet.of(Surface.S2),
                into -> replicaResponse(command.apply(c.nowInSec), true, into));
    }

    /** The case for a read that tracks repaired status: the cursor path serves it like any other. */
    protected ReadCase trackingCase(ReadCase c)
    {
        return c;
    }

    /** S1, S2, S4, S5, S6: both of the above. */
    protected void assertAllReadSurfacesMatch(ReadCase c, ColumnFamilyStore cfs, LongFunction<PartitionRangeReadCommand> command)
    {
        assertExecuteLocallyMatches(c, cfs, command);
        assertReplicaResponsesMatch(c, cfs, command);
    }

    /**
     * S1, S4, S5, S6 for an internal paging sequence ({@code PartitionRangeQueryPager.fetchPageUnfiltered},
     * which runs each page's command through {@code executeLocally}): the records of every page and
     * the paging state after it.
     */
    protected void assertPagedMatches(ReadCase c, ColumnFamilyStore cfs, LongFunction<PartitionRangeReadCommand> command, int pageSize)
    {
        compare(c.named(c.label + " / paged " + pageSize), cfs, EnumSet.of(Surface.S1), into -> {
            PartitionRangeQueryPager pager = (PartitionRangeQueryPager) command.apply(c.nowInSec).getPager(null, ProtocolVersion.V5);
            Set<String> positions = new HashSet<>();
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
                checkProgress(positions, state, page);
                page++;
            }
        });
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
        compare(c.named(c.label + " / " + query + " pageSize=" + pageSize), cfs, EnumSet.of(Surface.S3),
                into -> cqlPages(statement, pageSize, c.nowInSec, into));
    }

    /**
     * {@link #assertCqlMatches}, with each read the coordinator makes served as a replica response
     * ({@code ReadCommand.createResponseLocally}), the response a remote replica sends.
     */
    protected void assertCqlReplicaResponsesMatch(ReadCase c, ColumnFamilyStore cfs, String query, int pageSize)
    {
        CQLStatement statement = QueryProcessor.getStatement(formatQuery(query), ClientState.forInternalCalls());
        compare(c.named(c.label + " / " + query + " pageSize=" + pageSize + " as replica responses"), cfs, EnumSet.of(Surface.S3),
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

    /** {@link #assertAllReadSurfacesMatch} for a read of every column of the partitions of {@code dataRange}. */
    protected void assertDataRangeMatches(ReadCase c, ColumnFamilyStore cfs, DataRange dataRange)
    {
        assertAllReadSurfacesMatch(c, cfs, now -> PartitionRangeReadCommand.create(cfs.metadata(), now, ColumnFilter.all(cfs.metadata()),
                                                                                RowFilter.none(), DataLimits.NONE, dataRange));
    }

    /**
     * A CQL SELECT ({@code %s} is the current table) on every surface: {@link #assertCqlMatches} and
     * {@link #assertCqlReplicaResponsesMatch}, unpaged and in pages of {@code pageSize}.
     */
    protected void assertCqlOnEverySurface(ReadCase c, ColumnFamilyStore cfs, String query, int pageSize)
    {
        for (int size : new int[]{ 0, pageSize })
        {
            assertCqlMatches(c, cfs, query, size);
            assertCqlReplicaResponsesMatch(c, cfs, query, size);
        }
    }

    /** Runs any read on both paths under S4, S5, S6, comparing {@code surfaces} too.  For harness tests. */
    protected void compareCustom(ReadCase c, ColumnFamilyStore cfs, EnumSet<Surface> surfaces, PathRead read)
    {
        compare(c, cfs, surfaces, read);
    }

    // ---------------------------------------------------------------- reads

    /**
     * The range read command a CQL SELECT on the current table ({@code %s}) makes, as
     * {@code SelectStatement.getQuery} builds it.  {@code values} bind the markers as bigints.
     */
    protected PartitionRangeReadCommand rangeCommand(String query, long nowInSec, Object... values)
    {
        SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(formatQuery(query), ClientState.forInternalCalls());
        List<ByteBuffer> bound = new ArrayList<>(values.length);
        for (Object value : values)
            bound.add(LongType.instance.decompose(((Number) value).longValue()));
        QueryOptions options = QueryOptions.create(ConsistencyLevel.ONE, bound, false, -1, null, ConsistencyLevel.SERIAL,
                                                   ProtocolVersion.V5, KEYSPACE, Long.MIN_VALUE, nowInSec);
        return (PartitionRangeReadCommand) select.getQuery(options, nowInSec);
    }

    static byte[] intraNodeBytes(PartitionRangeReadCommand command) throws Exception
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

    static PartitionRangeReadCommand digestCopy(PartitionRangeReadCommand command)
    {
        return (PartitionRangeReadCommand) command.copyAsDigestQuery(ReplicaUtils.full(FBUtilities.getBroadcastAddressAndPort()));
    }

    private static void replicaResponse(PartitionRangeReadCommand command, boolean trackRepairedStatus, Observation into) throws Exception
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

    /** Fails when a page ends where an earlier page ended: the paging would not end. */
    private static void checkProgress(Set<String> positions, PagingState state, int page)
    {
        if (state == null)
            return;
        String position = (state.partitionKey == null ? "-" : ByteBufferUtil.bytesToHex(state.partitionKey))
                          + '/' + (state.rowMark == null ? "-" : state.rowMark.toString());
        if (!positions.add(position))
            throw new AssertionError("page " + page + " ended where an earlier page ended, " + position + ": the paging would not end");
    }

    private static void cqlPages(CQLStatement statement, int pageSize, long nowInSec, Observation into)
    {
        PagingState state = null;
        Set<String> positions = new HashSet<>();
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
            checkProgress(positions, state, page);
            page++;
        }
        while (state != null);
    }

    // ---------------------------------------------------------------- engine

    private void compare(ReadCase c, ColumnFamilyStore cfs, EnumSet<Surface> surfaces, PathRead read)
    {
        logger.info("range oracle case {}", c);
        Observation iterator;
        Observation cursor;
        // both paths start from the same key cache: the entries the first path adds or replaces are
        // put back as they were before the second path runs
        Map<KeyCacheKey, AbstractRowIndexEntry> keyCacheBefore = keyCacheEntries();
        if (c.cursorFirst)
        {
            cursor = runPath(true, c, cfs, read);
            restoreKeyCache(keyCacheBefore);
            iterator = runPath(false, c, cfs, read);
        }
        else
        {
            iterator = runPath(false, c, cfs, read);
            restoreKeyCache(keyCacheBefore);
            cursor = runPath(true, c, cfs, read);
        }
        casesRun++;

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
            StringBuilder sb = new StringBuilder("range read oracle divergence in ").append(c);
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

    private Observation runPath(boolean cursor, ReadCase c, ColumnFamilyStore cfs, PathRead read)
    {
        Observation into = new Observation(cursor);
        setRangePath(cursor);
        try
        {
            if (c.cold)
                emptyCaches();
            Map<KeyCacheKey, AbstractRowIndexEntry> keyCacheAtStart = keyCacheEntries();

            // the gate's counts are of every table: count this table's reads alone
            LongAdder served = new LongAdder();
            LongAdder notServed = new LongAdder();
            LongAdder notServedForReason = new LongAdder();
            TableId table = cfs.metadata().id;
            CursorRangeReads.gateListener = (id, reason) -> {
                if (!id.equals(table))
                    return;
                (reason == null ? served : notServed).increment();
                if (reason != null && reason == c.notServedBecause)
                    notServedForReason.increment();
            };
            long fellBack = CursorReads.sstableLegsFellBackToIterator();
            long lookups = filterLookups(cfs);
            long keyCacheSize = keyCacheSize();
            long rangeReads = cfs.metric.rangeLatency.latency.getCount();
            ClearableHistogram sstablesPerRangeRead = (ClearableHistogram) cfs.metric.sstablesPerRangeReadHistogram.cf;
            sstablesPerRangeRead.clear();

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

            CursorRangeReads.gateListener = null;
            into.rangeServed = served.sum();
            into.rangeNotServed = notServed.sum();
            into.rangeNotServedForReason = notServedForReason.sum();
            into.legsFellBack = CursorReads.sstableLegsFellBackToIterator() - fellBack;
            into.filterLookups = filterLookups(cfs) - lookups;
            into.keyCacheDelta = keyCacheSize() - keyCacheSize;
            into.keyCacheChanges = keyCacheChangesSince(keyCacheAtStart, cfs);
            into.rangeReadsRecorded = cfs.metric.rangeLatency.latency.getCount() - rangeReads;
            into.sstablesPerRangeReadCount = sstablesPerRangeRead.getCount();
            into.sstablesPerRangeReadMin = into.sstablesPerRangeReadCount == 0 ? 0 : sstablesPerRangeRead.getSnapshot().getMin();
            into.sstablesPerRangeReadMax = into.sstablesPerRangeReadCount == 0 ? 0 : sstablesPerRangeRead.getSnapshot().getMax();
            return into;
        }
        finally
        {
            CursorRangeReads.gateListener = null;
            setRangePath(false);
        }
    }

    private static long keyCacheSize()
    {
        return CacheService.instance.keyCache == null ? 0 : CacheService.instance.keyCache.size();
    }

    private static void emptyCaches()
    {
        if (ChunkCache.instance != null)
            ChunkCache.instance.clear();
        CacheService.instance.invalidateKeyCache();
    }

    /** Partition lookups made against this table's sstables, counted by their bloom filter trackers. */
    static long filterLookups(ColumnFamilyStore cfs)
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
        List<String> problems = new ArrayList<>();
        try
        {
            ScanMetricsCapture.assertParity("iterator vs cursor", iterator.scan, cursor.scan);
        }
        catch (AssertionError e)
        {
            problems.add(e.getMessage());
        }
        if (iterator.sstablesPerRangeReadCount != cursor.sstablesPerRangeReadCount
            || iterator.sstablesPerRangeReadMin != cursor.sstablesPerRangeReadMin
            || iterator.sstablesPerRangeReadMax != cursor.sstablesPerRangeReadMax)
            problems.add(String.format("sstables-per-range-read histogram diverged: iterator count=%d min=%d max=%d, cursor count=%d min=%d max=%d",
                                       iterator.sstablesPerRangeReadCount, iterator.sstablesPerRangeReadMin, iterator.sstablesPerRangeReadMax,
                                       cursor.sstablesPerRangeReadCount, cursor.sstablesPerRangeReadMin, cursor.sstablesPerRangeReadMax));
        if (iterator.rangeReadsRecorded != cursor.rangeReadsRecorded)
            problems.add(String.format("range reads recorded in the range latency metric diverged: iterator %d, cursor %d",
                                       iterator.rangeReadsRecorded, cursor.rangeReadsRecorded));
        if (!iterator.keyCacheChanges.equals(cursor.keyCacheChanges))
            problems.add(String.format("key cache changes diverged%n  iterator: %s%n  cursor:   %s",
                                       iterator.keyCacheChanges, cursor.keyCacheChanges));
        return problems.isEmpty() ? null : String.join(System.lineSeparator(), problems);
    }

    private static String compareEngagement(ReadCase c, Observation iterator, Observation cursor)
    {
        List<String> problems = new ArrayList<>();
        if (iterator.rangeServed != 0 || iterator.rangeNotServed != 0 || iterator.legsFellBack != 0)
            problems.add(String.format("the iterator run reached the cursor path: range reads served=%d, not served=%d, legs fell back=%d",
                                       iterator.rangeServed, iterator.rangeNotServed, iterator.legsFellBack));
        if (cursor.legsFellBack != 0)
            problems.add(cursor.legsFellBack + " sstable leg(s) of the cursor run were read by the iterator path");
        for (Observation o : new Observation[]{ iterator, cursor })
        {
            String path = o.cursor ? "cursor" : "iterator";
            if (o.filterLookups != 0)
                problems.add(String.format("the %s run made %d partition lookup(s) through the bloom filters", path, o.filterLookups));
            if (o.keyCacheDelta != 0)
                problems.add(String.format("the %s run changed the key cache size by %d", path, o.keyCacheDelta));
        }
        // every range read the cursor run made reached the gate once, and recorded its latency once
        if (c.notServedBecause == null)
        {
            if (cursor.rangeNotServed != 0)
                problems.add(String.format("the gate left %d range read(s) of the cursor run to the iterator path", cursor.rangeNotServed));
            if (cursor.failure == null && cursor.rangeServed != cursor.rangeReadsRecorded)
                problems.add(String.format("the cursor path served %d of the %d range reads the cursor run made",
                                           cursor.rangeServed, cursor.rangeReadsRecorded));
        }
        else
        {
            if (cursor.rangeServed != 0)
                problems.add(String.format("the cursor path served %d range read(s) the gate must leave to the iterator path (%s)",
                                           cursor.rangeServed, c.notServedBecause));
            if (cursor.failure == null && cursor.rangeNotServedForReason != cursor.rangeReadsRecorded)
                problems.add(String.format("the gate counted %d of the cursor run's %d range reads as not served for %s",
                                           cursor.rangeNotServedForReason, cursor.rangeReadsRecorded, c.notServedBecause));
        }
        return problems.isEmpty() ? null : String.join(System.lineSeparator(), problems);
    }
}
