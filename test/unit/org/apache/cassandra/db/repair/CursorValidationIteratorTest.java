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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.compaction.PrecomputedDigestPartition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end test of {@link CursorValidationIterator} through its real, production-shaped
 * constructor (parent repair session registration, {@code writeAndAddMemtableRanges}, estimate
 * bookkeeping) - as opposed to {@code DigestingCursorMergeSinkParityTest}, which drives
 * {@link org.apache.cassandra.db.compaction.CursorCompactor} and
 * {@link org.apache.cassandra.db.compaction.DigestingCursorMergeSink} directly. Asserts the
 * legacy ({@link CassandraValidationIterator}) and cursor-backed paths, run over the identical
 * registered repair session, see the same number of partitions and produce bit-identical
 * per-partition digests, and that {@link CursorValidationIterator}'s progress/estimate accessors
 * return sane values.
 */
public class CursorValidationIteratorTest extends CQLTester
{
    @Test
    public void cursorMatchesLegacyValidation() throws Throwable
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("10.0.0.2");
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Several overlapping sstables, same shape as the other CASSANDRA-21452 parity tests.
        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 20; pk++)
                for (long ck = 0; ck < 5; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck + "-" + round);
            flush();
        }
        assertTrue("test setup: expected overlapping sstables", cfs.getLiveSSTables().size() >= 3);

        TimeUUID parentId = nextTimeUUID();
        Range<Token> fullRange = new Range<>(minimumToken, minimumToken);
        ActiveRepairService.instance().registerParentRepairSession(parentId,
                                                                    coordinator,
                                                                    Lists.newArrayList(cfs),
                                                                    Sets.newHashSet(fullRange),
                                                                    false,
                                                                    ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                    true,
                                                                    PreviewKind.NONE);
        List<Range<Token>> ranges = Collections.singletonList(fullRange);
        long nowInSec = FBUtilities.nowInSeconds();

        List<byte[]> legacyDigests = new ArrayList<>();
        try (CassandraValidationIterator legacy = new CassandraValidationIterator(cfs, SharedContext.Global.instance, ranges, parentId,
                                                                                  nextTimeUUID(), false, nowInSec, false,
                                                                                  (TopPartitionTracker.Collector) null))
        {
            while (legacy.hasNext())
            {
                try (UnfilteredRowIterator partition = legacy.next())
                {
                    Digest digest = Digest.forValidator();
                    UnfilteredRowIterators.digest(partition, digest, MessagingService.current_version);
                    legacyDigests.add(digest.digest());
                }
            }
        }

        // Per-partition digests, matching how Validator.rowHash() actually uses them in
        // production (one fresh Digest.forValidator() per partition), rather than one combined
        // running hash across the whole table - CursorValidationIterator's next() returns
        // PrecomputedDigestPartition, whose digest was already computed directly from cursor
        // primitives (see DigestingCursorMergeSink), not by re-walking real row/cell content.
        List<byte[]> cursorDigests = new ArrayList<>();
        long cursorEstimatedBytes;
        long cursorEstimatedPartitions;
        try (CursorValidationIterator cursor = new CursorValidationIterator(cfs, SharedContext.Global.instance, ranges, parentId,
                                                                            nextTimeUUID(), false, nowInSec, false,
                                                                            (TopPartitionTracker.Collector) null))
        {
            cursorEstimatedBytes = cursor.getEstimatedBytes();
            cursorEstimatedPartitions = cursor.estimatedPartitions();
            assertEquals(1, cursor.getRangePartitionCounts().size());

            while (cursor.hasNext())
            {
                try (UnfilteredRowIterator partition = cursor.next())
                {
                    PrecomputedDigestPartition precomputed = (PrecomputedDigestPartition) partition;
                    cursorDigests.add(precomputed.digestBytes());
                }
            }

            assertTrue("getBytesRead() should reflect real progress after draining", cursor.getBytesRead() > 0);
        }

        assertEquals("both paths must see the same number of partitions", legacyDigests.size(), cursorDigests.size());
        for (int i = 0; i < legacyDigests.size(); i++)
            assertArrayEquals("per-partition digest mismatch at partition " + i, legacyDigests.get(i), cursorDigests.get(i));
        assertEquals(20, legacyDigests.size());
        assertTrue("estimatedBytes should be positive for a non-trivial table", cursorEstimatedBytes > 0);
        assertTrue("estimatedPartitions should be positive for a non-trivial table", cursorEstimatedPartitions > 0);
    }

    /**
     * Pins the exception TYPE thrown directly by {@link CursorValidationIterator}'s constructor
     * for a schema {@link org.apache.cassandra.db.compaction.CursorCompactor#isValidationSupported}
     * rejects (a 2i index), independent of {@link CassandraTableRepairManagerDispatchTest}, which
     * only proves the dispatch-level *effect* (fallback to the legacy path). A future regression
     * that reintroduces a bare {@code IllegalStateException} here would still make the dispatch
     * test fail loudly too (nothing catches it), but this test names the exact contract so the
     * failure is legible instead of a mysterious dispatch-test crash.
     */
    @Test(expected = CursorValidationUnsupportedException.class)
    public void constructorThrowsDedicatedExceptionForUnsupportedSchema() throws Throwable
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("10.0.0.4");
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        createIndex("CREATE INDEX ON %s (v)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 1L, "v");
        flush();

        TimeUUID parentId = nextTimeUUID();
        Range<Token> fullRange = new Range<>(minimumToken, minimumToken);
        ActiveRepairService.instance().registerParentRepairSession(parentId,
                                                                    coordinator,
                                                                    Lists.newArrayList(cfs),
                                                                    Sets.newHashSet(fullRange),
                                                                    false,
                                                                    ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                    true,
                                                                    PreviewKind.NONE);
        List<Range<Token>> ranges = Collections.singletonList(fullRange);

        new CursorValidationIterator(cfs, SharedContext.Global.instance, ranges, parentId, nextTimeUUID(), false,
                                      FBUtilities.nowInSeconds(), false, (TopPartitionTracker.Collector) null);
    }

    /**
     * Materialized views are excluded from cursor validation specifically (unlike regular cursor
     * compaction, which does admit them - see {@code MaterializedViewDifferentialCompactionTest})
     * because a repair session that hits a shadowable row deletion mid-merge fails the whole repair
     * with a hard error instead of falling back pre-construction. Pins the same dedicated-exception
     * contract as {@link #constructorThrowsDedicatedExceptionForUnsupportedSchema}, for the
     * validation-only gate in {@link org.apache.cassandra.db.compaction.CursorCompactor#isValidationSupported}.
     */
    @Test(expected = CursorValidationUnsupportedException.class)
    public void constructorThrowsDedicatedExceptionForMaterializedView() throws Throwable
    {
        requireNetwork();
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("10.0.0.6");
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1 FROM %s " +
                                  "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                  "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, 1L, 1L);
        flush(KEYSPACE, view);

        TimeUUID parentId = nextTimeUUID();
        Range<Token> fullRange = new Range<>(minimumToken, minimumToken);
        ActiveRepairService.instance().registerParentRepairSession(parentId,
                                                                    coordinator,
                                                                    Lists.newArrayList(viewCfs),
                                                                    Sets.newHashSet(fullRange),
                                                                    false,
                                                                    ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                    true,
                                                                    PreviewKind.NONE);
        List<Range<Token>> ranges = Collections.singletonList(fullRange);

        new CursorValidationIterator(viewCfs, SharedContext.Global.instance, ranges, parentId, nextTimeUUID(), false,
                                      FBUtilities.nowInSeconds(), false, (TopPartitionTracker.Collector) null);
    }

    /**
     * A table with zero local data (never written/flushed) has zero sstables to validate.
     * {@code CursorCompactor.mergeNextPartition}'s very first call finds no partitions to merge
     * and calls {@code finish() -> writerRollover()} immediately - which used to NPE
     * ({@code lastWrittenKey()} dereferencing a {@code lastSource} that's only ever assigned after
     * a partition is actually merged) since nothing had ever been merged. Reachable any time a
     * repair range genuinely has no local data for a table, which is routine (a brand-new table,
     * or - on any cluster with more than a handful of nodes - most repair sub-ranges for most
     * tables).
     */
    @Test
    public void emptyTableProducesNoPartitionsAndClosesCleanly() throws Throwable
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("10.0.0.5");
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        assertEquals("test setup: table should have no sstables", 0, cfs.getLiveSSTables().size());

        TimeUUID parentId = nextTimeUUID();
        Range<Token> fullRange = new Range<>(minimumToken, minimumToken);
        ActiveRepairService.instance().registerParentRepairSession(parentId,
                                                                    coordinator,
                                                                    Lists.newArrayList(cfs),
                                                                    Sets.newHashSet(fullRange),
                                                                    false,
                                                                    ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                    true,
                                                                    PreviewKind.NONE);
        List<Range<Token>> ranges = Collections.singletonList(fullRange);

        try (CursorValidationIterator cursor = new CursorValidationIterator(cfs, SharedContext.Global.instance, ranges, parentId,
                                                                            nextTimeUUID(), false, FBUtilities.nowInSeconds(), false,
                                                                            (TopPartitionTracker.Collector) null))
        {
            assertTrue("an empty table should produce no partitions to validate", !cursor.hasNext());
        }
    }

    /**
     * An sstable can pass the coarse, span-level filter that selects which sstables to validate
     * (its first/last token merely needs to intersect the repair range) while having zero keys
     * actually inside that range - a sparse sstable whose few partitions straddle a "gap" the
     * repair range falls entirely within. {@code SSTableReader#getPositionsForRanges} then
     * legitimately returns an empty list for that sstable, which used to be passed straight into
     * {@code StatefulCursor#positionAt}, crashing (assertion failure, or {@code NoSuchElementException}
     * with assertions disabled) since that method requires a non-empty list. Meanwhile another
     * sstable with a real key inside the range must still be validated correctly.
     * <p>
     * Constructs this deterministically from real token order (not hardcoded token values, which
     * would be partitioner-hash-dependent): sstable A holds two widely-spaced partitions (a wide
     * span with an internal gap); sstable B holds one partition whose token falls inside that gap.
     * The repair range is built to cover exactly the gap - coarsely intersecting sstable A's span
     * (so it's selected) while containing none of sstable A's actual keys, and containing sstable
     * B's one key.
     */
    @Test
    public void sstableWithNoKeysInRepairRangeIsSkippedRatherThanCrashing() throws Throwable
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("10.0.0.6");

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Insert enough distinct partitions across two separate flushes that, whatever order
        // Murmur3 hashes them into, some flush ends up with the token-order-first and
        // token-order-last key (a wide span) and at least one key from the OTHER flush falls
        // strictly between them in token order (the "gap").
        for (long pk = 0; pk < 8; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, "a" + pk);
        flush();
        for (long pk = 8; pk < 16; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, "b" + pk);
        flush();

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals("test setup: expected two separate sstables", 2, sstables.size());

        // All keys from both sstables, in token order, tagged with which sstable each came from.
        List<DecoratedKey> keysA = keysInTokenOrder(sstables.get(0));
        List<DecoratedKey> keysB = keysInTokenOrder(sstables.get(1));

        // Find a token-order-adjacent pair (x from one sstable, y from the other, both from the
        // SAME sstable's neighbor removed) such that some third sstable's key falls strictly
        // between two keys of the FIRST sstable. Simplest reliable construction: merge-sort both
        // key lists by token, then find any run of 3+ consecutive keys (by token) that aren't all
        // from the same sstable - the middle one's owner is the "gap-filler" (sstable G), and the
        // two ends belong to the "wide-span" sstable (sstable W), whose exact positions for the
        // range strictly between the ends must be empty.
        List<DecoratedKey> merged = new ArrayList<>();
        merged.addAll(keysA);
        merged.addAll(keysB);
        merged.sort(DecoratedKey::compareTo);

        int gapIndex = -1;
        for (int i = 1; i < merged.size() - 1; i++)
        {
            boolean prevInA = keysA.contains(merged.get(i - 1));
            boolean midInA = keysA.contains(merged.get(i));
            boolean nextInA = keysA.contains(merged.get(i + 1));
            if (prevInA == nextInA && midInA != prevInA)
            {
                gapIndex = i;
                break;
            }
        }
        assertTrue("test setup: expected to find a token-order gap between two same-sstable keys", gapIndex >= 0);

        Token exclusiveStart = merged.get(gapIndex - 1).getToken();
        Token inclusiveEnd = merged.get(gapIndex).getToken();
        Range<Token> gapRange = new Range<>(exclusiveStart, inclusiveEnd);

        TimeUUID parentId = nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(parentId,
                                                                    coordinator,
                                                                    Lists.newArrayList(cfs),
                                                                    Sets.newHashSet(gapRange),
                                                                    false,
                                                                    ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                    true,
                                                                    PreviewKind.NONE);
        List<Range<Token>> ranges = Collections.singletonList(gapRange);
        long nowInSec = FBUtilities.nowInSeconds();

        long legacyPartitions = 0;
        try (CassandraValidationIterator legacy = new CassandraValidationIterator(cfs, SharedContext.Global.instance, ranges, parentId,
                                                                                  nextTimeUUID(), false, nowInSec, false,
                                                                                  (TopPartitionTracker.Collector) null))
        {
            while (legacy.hasNext())
            {
                try (UnfilteredRowIterator partition = legacy.next())
                {
                    legacyPartitions++;
                }
            }
        }
        assertEquals("test setup: the gap range should validate exactly one partition", 1, legacyPartitions);

        long cursorPartitions = 0;
        try (CursorValidationIterator cursor = new CursorValidationIterator(cfs, SharedContext.Global.instance, ranges, parentId,
                                                                            nextTimeUUID(), false, nowInSec, false,
                                                                            (TopPartitionTracker.Collector) null))
        {
            while (cursor.hasNext())
            {
                try (UnfilteredRowIterator partition = cursor.next())
                {
                    cursorPartitions++;
                }
            }
        }
        assertEquals("cursor path must validate the same single partition without crashing " +
                     "on the sstable that has zero keys in this range", legacyPartitions, cursorPartitions);
    }

    private static List<DecoratedKey> keysInTokenOrder(SSTableReader sstable)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    keys.add(partition.partitionKey());
                }
            }
        }
        return keys;
    }
}
