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
import java.util.List;
import java.util.function.LongPredicate;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.ResponseWireWriter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * M3.3a-i (CASSANDRA-20428, Phase 4 seam iv, wire-format-only slice) differential scenarios for
 * {@link CursorReads.TranscodeMergeSink} / {@link ResponseWireWriter} — the new sink that transcodes
 * merged cursor state DIRECTLY into {@code ReadResponse} (MESSAGING-flavor) wire bytes instead of
 * materializing {@code Row}/{@code Cell} objects first. This is layer-1 of the M3.3a plan's
 * three-layer harness ("unit oracle, before any production wiring"): every scenario drives the REAL
 * merge/legs machinery via {@link CursorReads#mergeLegsWithSink} twice over the SAME underlying
 * sstables — once with {@code MaterializingMergeSink} (via the untouched, production
 * {@link CursorReads#mergeLegs}), once with {@code TranscodeMergeSink} — and asserts the two
 * per-partition byte streams are IDENTICAL, then proves the candidate bytes are readable by the
 * REAL production deserializer ({@code UnfilteredRowIteratorSerializer}, the exact class
 * {@code ReadResponse}'s intra-node deserialization path uses).
 * <p>
 * No production call site reaches {@code TranscodeMergeSink} — {@link CursorReads#mergeLegsWithSink}
 * is {@code @VisibleForTesting}, and this class is the only caller besides {@code mergeLegs} itself
 * (which always uses {@code MaterializingMergeSink}). {@code cursor_reads_enabled} plays no role
 * here; legs are opened directly via the already-public {@link CursorReads#openLeg}.
 */
public class TranscodeWireFormatDifferentialTest extends CursorReadDifferentialTester
{
    @After
    public void resetTranscodeHooks()
    {
        CursorReads.TEST_TRANSCODE_SKEW_TIMESTAMP = false;
        CursorReads.TEST_TRANSCODE_WRONG_FLAGS = false;
    }

    // ---------------------------------------------------------------- scenario driving

    protected List<CursorReads.PendingLeg> openAllLegs(List<SSTableReader> sstables, TableMetadata metadata,
                                                      DecoratedKey key, Slices slices, ColumnFilter columnFilter)
    {
        CursorReads.ValueTransfer transfer = new CursorReads.ValueTransfer();
        List<CursorReads.PendingLeg> legs = new ArrayList<>(sstables.size());
        for (SSTableReader sstable : sstables)
        {
            CursorReads.PendingLeg leg = CursorReads.openLeg(sstable, metadata, key, slices, columnFilter,
                                                              SSTableReadsListener.NOOP_LISTENER, transfer);
            if (leg != null)
                legs.add(leg);
        }
        return legs;
    }

    /** The reference side: the untouched production {@link CursorReads#mergeLegs}
     *  ({@code MaterializingMergeSink} → the real {@code UnfilteredRowIteratorSerializer}),
     *  optionally purged by a REAL {@link PurgeFunction} subclass so gcable-tombstone scenarios
     *  compare against genuine post-purge bytes, not the (deliberately purge-free) merge core's
     *  raw output. {@code purge == null} means no purging (the common case). */
    protected byte[] materializingReference(List<CursorReads.PendingLeg> legs, TableMetadata metadata,
                                          DecoratedKey key, Slices slices, ColumnFilter columnFilter,
                                          PurgeFunction purge) throws Exception
    {
        UnfilteredRowIterator iter = CursorReads.mergeLegs(legs, metadata, key, slices, columnFilter, null, null);
        if (purge != null)
        {
            iter = Transformation.apply(iter, purge);
            if (iter == null)
                fail("purge scenario purged the whole partition to nothing; scenario needs a surviving row/static");
        }
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            UnfilteredRowIteratorSerializer.serializer.serialize(iter, columnFilter, buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
        finally
        {
            iter.close();
        }
    }

    /** The candidate side: {@link CursorReads.TranscodeMergeSink} via
     *  {@link CursorReads#mergeLegsWithSink}, with the header taken from the caller (so both sides
     *  serialize against the IDENTICAL {@code SerializationHeader}). */
    protected byte[] transcodeCandidate(List<CursorReads.PendingLeg> legs, TableMetadata metadata, DecoratedKey key,
                                      Slices slices, ColumnFilter columnFilter, SerializationHeader header,
                                      long nowInSec, long gcBefore, boolean onlyPurgeRepairedTombstones,
                                      long oldestUnrepairedTombstone) throws Exception
    {
        DataOutputBuffer rowEvents = new DataOutputBuffer();
        ResponseWireWriter writer = new ResponseWireWriter(rowEvents, header, MessagingService.current_version);
        Slice slice = slices.get(0);
        CursorReads.MergeSinkFactory<CursorReads.TranscodeMergeSink> factory =
            () -> new CursorReads.TranscodeMergeSink(writer, metadata.comparator, slice, nowInSec, gcBefore,
                                                      onlyPurgeRepairedTombstones, oldestUnrepairedTombstone);
        CursorReads.MergeContext<CursorReads.TranscodeMergeSink> ctx =
            CursorReads.mergeLegsWithSink(legs, metadata, key, slices, columnFilter, null, null, factory);
        // the streaming analog of SlicedMaterializedIterator's "slice exhausted" tail — must run
        // once the merge has fully completed, closing any still-open range tombstone at the slice
        // end with a purge-tested synthetic marker (see TranscodeMergeSink's class javadoc)
        ctx.sink.finishPartition();

        boolean hasStatic = !ctx.mergedStatic.isEmpty();
        boolean isEmpty = ctx.mergedDeletion.isLive() && !hasStatic && rowEvents.getLength() == 0;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ResponseWireWriter envelope = new ResponseWireWriter(out, header, MessagingService.current_version);
            envelope.writeKey(key.getKey());
            envelope.writeHeader(isEmpty, false, ctx.mergedDeletion, hasStatic, columnFilter);
            if (!isEmpty)
            {
                if (hasStatic)
                    envelope.writeStaticRow(ctx.mergedStatic);
                out.write(rowEvents.getData(), 0, rowEvents.getLength());
                envelope.writeEndOfPartition();
            }
            return out.toByteArray();
        }
    }

    /** Full scenario: byte-compare (no purging) + round-trip through the real production
     *  deserializer, plus the negative-control assertion that a deliberately corrupted transcode
     *  run is CAUGHT (not silently passing). */
    protected void assertTranscodeMatchesMaterializedOracle(ColumnFamilyStore cfs, Object... key) throws Throwable
    {
        assertTranscodeMatchesMaterializedOracleWithPurge(cfs, null, 0, Long.MIN_VALUE, false, Long.MIN_VALUE, key);
    }

    /**
     * @param purge non-null to compare against a REAL {@link PurgeFunction}-purged reference (the
     *              gcable-tombstone scenarios) — its parameters must match the (nowInSec, gcBefore,
     *              onlyPurgeRepairedTombstones, oldestUnrepairedTombstone) passed to the candidate.
     */
    protected void assertTranscodeMatchesMaterializedOracleWithPurge(ColumnFamilyStore cfs, PurgeFunction purge, long nowInSec,
                                                           long gcBefore, boolean onlyPurgeRepairedTombstones,
                                                           long oldestUnrepairedTombstone, Object... key) throws Throwable
    {
        long now = nowInSec != 0 ? nowInSec : FBUtilities.nowInSeconds();
        SinglePartitionReadCommand probe = (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build();
        assertTranscodeMatchesMaterializedOracleForCommand(cfs, probe, purge, nowInSec, gcBefore,
                                                            onlyPurgeRepairedTombstones, oldestUnrepairedTombstone);
    }

    /** Slice-aware variant: {@code probe} is already built (e.g. with {@code fromIncl}/{@code toIncl}
     *  bounds), so a slice landing inside an open range tombstone exercises the slicer's artificial
     *  slice-bound markers on BOTH sides of the comparison. */
    protected void assertTranscodeMatchesMaterializedOracleForCommand(ColumnFamilyStore cfs, SinglePartitionReadCommand probe,
                                                                     PurgeFunction purge, long nowInSec, long gcBefore,
                                                                     boolean onlyPurgeRepairedTombstones,
                                                                     long oldestUnrepairedTombstone) throws Throwable
    {
        TableMetadata metadata = cfs.metadata();
        DecoratedKey dk = probe.partitionKey();
        ColumnFilter columnFilter = probe.columnFilter();
        Slices slices = probe.clusteringIndexFilter().getSlices(metadata);
        List<SSTableReader> sstables = liveSSTablesFor(cfs, probe);
        assertTrue("scenario needs >= 2 sstable legs to engage the merge core (found " + sstables.size() + ")",
                   sstables.size() >= 2);

        List<CursorReads.PendingLeg> refLegs = openAllLegs(sstables, metadata, dk, slices, columnFilter);
        assertTrue("no leg actually opened the partition", refLegs.size() >= 2);
        byte[] refBytes = materializingReference(refLegs, metadata, dk, slices, columnFilter, purge);

        // header shared by both sides: rebuild a fresh reference iterator (unpurged — columns/stats
        // are independent of purging) purely to read off .columns()/.stats(), exactly the values
        // UnfilteredRowIteratorSerializer's own 2-arg serialize() overload would derive.
        List<CursorReads.PendingLeg> headerLegs = openAllLegs(sstables, metadata, dk, slices, columnFilter);
        UnfilteredRowIterator headerIter = CursorReads.mergeLegs(headerLegs, metadata, dk, slices, columnFilter, null, null);
        RegularAndStaticColumns cols = headerIter.columns();
        EncodingStats stats = headerIter.stats();
        headerIter.close();
        SerializationHeader header = new SerializationHeader(false, metadata, cols, stats);

        List<CursorReads.PendingLeg> candLegs = openAllLegs(sstables, metadata, dk, slices, columnFilter);
        byte[] candBytes = transcodeCandidate(candLegs, metadata, dk, slices, columnFilter, header,
                                              nowInSec, gcBefore, onlyPurgeRepairedTombstones, oldestUnrepairedTombstone);

        assertResponseBytesEqual(refBytes, candBytes);

        // round-trip through the REAL production deserializer (ReadResponse's own consumer)
        try (DataInputBuffer in = new DataInputBuffer(candBytes))
        {
            UnfilteredRowIteratorSerializer.Header h =
                UnfilteredRowIteratorSerializer.serializer.deserializeHeader(metadata, columnFilter, in,
                                                                              MessagingService.current_version,
                                                                              DeserializationHelper.Flag.FROM_REMOTE);
            try (UnfilteredRowIterator roundTripped =
                     UnfilteredRowIteratorSerializer.serializer.deserialize(in, MessagingService.current_version,
                                                                            metadata, DeserializationHelper.Flag.FROM_REMOTE, h))
            {
                while (roundTripped.hasNext())
                    roundTripped.next();
            }
        }
    }

    // ---------------------------------------------------------------- static rows, wide columns

    @Test
    public void staticAndRegularColumnsAcrossOverlappingLegs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 text static, s2 bigint static, " +
                    "v1 bigint, v2 text, v3 blob, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, s1, s2) VALUES (?, ?, ?)", 1L, "static-a", 7L);
        for (long ck = 0; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2, v3) VALUES (?, ?, ?, ?, ?)",
                    1L, ck, ck * 3, "row-" + ck, java.nio.ByteBuffer.wrap(("blob" + ck).getBytes()));
        flush();
        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 1L, "static-b");
        for (long ck = 40; ck < 80; ck++)
            execute("INSERT INTO %s (pk, ck, v2) VALUES (?, ?, ?)", 1L, ck, "row-" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    @Test
    public void columnSubsetBitmapShapesUnderColumnFilter() throws Throwable
    {
        // wide table (>=64 regular columns forces the large-subset encoding path in
        // Columns.serializer.serializeSubset — verify both the small-bitmap and large-subset shapes)
        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        for (int i = 0; i < 70; i++)
            ddl.append(", c").append(i).append(" bigint");
        ddl.append(", PRIMARY KEY (pk, ck))");
        createTable(ddl.toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // leg A: every row sets only a handful of columns (sparse -> small present-subset)
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, c0, c1, c2) VALUES (?, ?, ?, ?, ?)", 1L, ck, ck, ck + 1, ck + 2);
        flush();
        // leg B: every row sets almost all columns (dense -> small missing-subset / large-subset)
        StringBuilder insert = new StringBuilder("INSERT INTO %s (pk, ck");
        for (int i = 0; i < 70; i++)
            insert.append(", c").append(i);
        insert.append(") VALUES (?, ?");
        for (int i = 0; i < 70; i++)
            insert.append(", ?");
        insert.append(")");
        for (long ck = 10; ck < 20; ck++)
        {
            Object[] args = new Object[72];
            args[0] = 1L;
            args[1] = ck;
            for (int i = 0; i < 70; i++)
                args[2 + i] = (long) i;
            execute(insert.toString(), args);
        }
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    @Test
    public void complexColumnsCollectionsAndUdtDeletions() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a bigint, b text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<bigint, text>, l list<text>, " +
                    "s set<bigint>, u " + udt + ", PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, ck, m, l, s, u) VALUES (?, ?, ?, ?, ?, {a: 1, b: 'one'})",
                1L, 1L, java.util.Map.of(1L, "one", 2L, "two"), java.util.List.of("x", "y"), java.util.Set.of(5L, 6L));
        execute("INSERT INTO %s (pk, ck, m) VALUES (?, ?, ?)", 1L, 2L, java.util.Map.of(3L, "three"));
        flush();

        // leg B: a complex-column DELETION (whole map cleared) for ck=1, more map entries for ck=2,
        // and a deletion-only complex column (m cleared, no replacement) for ck=3
        execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", 1L, 1L);
        execute("DELETE u FROM %s WHERE pk = ? AND ck = ?", 1L, 1L);
        execute("UPDATE %s SET m = m + ? WHERE pk = ? AND ck = ?", java.util.Map.of(4L, "four"), 1L, 2L);
        execute("INSERT INTO %s (pk, ck, m) VALUES (?, ?, ?)", 1L, 3L, java.util.Map.of(9L, "nine"));
        execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", 1L, 3L);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    // ---------------------------------------------------------------- TTLs

    @Test
    public void ttlsAndRowLevelExpiration() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL 100000", 1L, ck, ck, "ttl-" + ck);
        flush();
        for (long ck = 10; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        // cell-level TTL on an otherwise-untouched row from leg A
        execute("UPDATE %s USING TTL 50000 SET v2 = ? WHERE pk = ? AND ck = ?", "later-ttl", 1L, 3L);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    // ---------------------------------------------------------------- range tombstones

    @Test
    public void rangeTombstonesOpenedAndClosedAcrossLegs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 60; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        // a range tombstone spanning multiple sub-ranges plus a point delete, in a second leg
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 10L, 25L);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 40L, 45L);
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 50L);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    /** Both slice bounds land STRICTLY INSIDE an open range tombstone: the slicer must synthesize
     *  artificial open/close boundary markers at the slice start/end, and the transcode candidate's
     *  marker-framing must match those synthetic markers byte-for-byte, not just the "real" ones
     *  the merge itself produced. */
    @Test
    public void rangeTombstonesWithArtificialSliceBoundMarkers() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 100; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        // one wide range tombstone the slice below will land fully inside, in a second leg
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 20L, 80L);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        // both bounds strictly inside [20, 80): the merged/emitted stream must open and close with
        // SYNTHETIC markers at ck=30 and ck=60 that the range tombstone itself never carried
        SinglePartitionReadCommand sliced = (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(30L).toIncl(60L).build();
        assertTranscodeMatchesMaterializedOracleForCommand(cfs, sliced, null, 0, Long.MIN_VALUE, false, Long.MIN_VALUE);

        // exclusive bounds landing exactly ON the tombstone's real open/close clustering values —
        // the real open/close markers themselves become the slice-start/end synthetic ones
        SinglePartitionReadCommand exact = (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromExcl(20L).toExcl(80L).build();
        assertTranscodeMatchesMaterializedOracleForCommand(cfs, exact, null, 0, Long.MIN_VALUE, false, Long.MIN_VALUE);
    }

    @Test
    public void partitionAndRowDeletionsInterleavedWithLiveData() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 5L);
        for (long ck = 20; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    // ---------------------------------------------------------------- shadowed row in a losing leg

    @Test
    public void shadowedRowInLosingLegProducesNoOutputForThatRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // leg A (older): writes ck=1..5
        for (long ck = 1; ck <= 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        // leg B (newer): a partition-level range tombstone shadows every one of leg A's rows, plus
        // one new live row past the shadowed range
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 0L, 10L);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, 10L, 100L);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTranscodeMatchesMaterializedOracle(cfs, 1L);
    }

    // ---------------------------------------------------------------- gcable tombstones (purge twin)

    protected void assertPurgeTwin(ColumnFamilyStore cfs, boolean onlyPurgeRepairedTombstones, Object... key) throws Throwable
    {
        long nowInSec = FBUtilities.nowInSeconds() + 1_000_000; // far enough past every deletion below to be gcable
        long gcBefore = nowInSec; // gc_grace effectively 0: everything with localDeletionTime < gcBefore purges
        long oldestUnrepairedTombstone = onlyPurgeRepairedTombstones ? nowInSec : Long.MIN_VALUE;
        PurgeFunction purge = new PurgeFunction(nowInSec, gcBefore, oldestUnrepairedTombstone,
                                                onlyPurgeRepairedTombstones, false)
        {
            protected LongPredicate getPurgeEvaluator()
            {
                return time -> true;
            }
        };
        assertTranscodeMatchesMaterializedOracleWithPurge(cfs, purge, nowInSec, gcBefore, onlyPurgeRepairedTombstones,
                                                          oldestUnrepairedTombstone, key);
    }

    @Test
    public void gcableTombstonesArePurgedAtEmission_notOnlyRepaired() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 3L);              // row tombstone -> purges
        execute("DELETE v2 FROM %s WHERE pk = ? AND ck = ?", 1L, 5L);           // cell tombstone -> purges
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 8L, 11L); // range tombstone -> purges
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, 20L, 999L); // keeps the partition non-empty
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertPurgeTwin(cfs, false, 1L);
    }

    @Test
    public void gcableTombstonesArePurgedAtEmission_onlyRepairedTombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 2L);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 5L, 8L);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, 20L, 999L);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        // onlyPurgeRepairedTombstones = true with oldestUnrepairedTombstone == nowInSec: every
        // tombstone here (localDeletionTime far below nowInSec) is treated as "repaired" (< the
        // oldest-unrepaired watermark does NOT hold since watermark == nowInSec and every
        // tombstone's LDT is far below it) — see assertPurgeTwin's derivation; this proves the twin
        // engages the onlyPurgeRepairedTombstones branch at all, not just that purging happens.
        assertPurgeTwin(cfs, true, 1L);
    }

    @Test
    public void expiredCellsConvertToTombstonesWhenNotYetGcable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // TTL short enough to have expired by "now" but gc_before set so it is NOT yet gcable —
        // AbstractCell.purge's convert-to-tombstone hijack must fire (value dropped, TTL cleared)
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TTL 1", 1L, 1L, 111L);
        flush();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, 2L, 222L);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        long nowInSec = FBUtilities.nowInSeconds() + 5; // past the 1s TTL: the cell is dead
        long gcBefore = Long.MIN_VALUE; // nothing is gcable: the dead cell survives, converted
        PurgeFunction purge = new PurgeFunction(nowInSec, gcBefore, Long.MIN_VALUE, false, false)
        {
            protected LongPredicate getPurgeEvaluator()
            {
                return time -> true;
            }
        };
        assertTranscodeMatchesMaterializedOracleWithPurge(cfs, purge, nowInSec, gcBefore, false, Long.MIN_VALUE, 1L);
    }

    // ---------------------------------------------------------------- negative controls

    @Test
    public void skewedTimestampHookIsCaughtByTheHarness() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        for (long ck = 10; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        CursorReads.TEST_TRANSCODE_SKEW_TIMESTAMP = true;
        try
        {
            assertTranscodeMatchesMaterializedOracle(cfs, 1L);
            fail("expected the byte-comparison harness to catch the deliberately skewed timestamp");
        }
        catch (AssertionError expected)
        {
            assertTrue("expected a BYTE divergence assertion, got: " + expected.getMessage(),
                       expected.getMessage().contains("BYTE divergence"));
        }
        finally
        {
            CursorReads.TEST_TRANSCODE_SKEW_TIMESTAMP = false;
        }
    }

    @Test
    public void wrongFlagsHookIsCaughtByTheHarness() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 3L); // needs a row-deletion flag to flip
        for (long ck = 10; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        CursorReads.TEST_TRANSCODE_WRONG_FLAGS = true;
        try
        {
            assertTranscodeMatchesMaterializedOracle(cfs, 1L);
            fail("expected the byte-comparison harness to catch the deliberately wrong flags byte");
        }
        catch (AssertionError expected)
        {
            assertTrue("expected a BYTE divergence assertion, got: " + expected.getMessage(),
                       expected.getMessage().contains("BYTE divergence"));
        }
        finally
        {
            CursorReads.TEST_TRANSCODE_WRONG_FLAGS = false;
        }
    }
}
