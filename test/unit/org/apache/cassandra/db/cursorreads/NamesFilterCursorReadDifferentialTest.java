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

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Differential scenarios for the {@code ClusteringIndexNamesFilter} single-partition read served on
 * the cursor path (CASSANDRA-20428, gap #9). A names read selects a fixed set of clusterings (for
 * example {@code WHERE ck IN (...)} or a point read of one row); it resolves to one point slice per
 * requested clustering. Before this gap was closed the read was declined by
 * {@code CursorReads.isReadSupported} and routed to {@code queryMemtableAndSSTablesInTimestampOrder},
 * which is entirely iterator-backed, so a names query never used the cursor path.
 *
 * <p>A non-tracking names read routes to the timestamp-order cursor driver
 * ({@code SinglePartitionReadCommand.queryMemtableAndCursorsInTimestampOrder}), the cursor twin of the
 * iterator names oracle. It reuses that oracle's completeness logic verbatim and swaps only the
 * per-sstable row source to a cursor leg, so it skips the same older sstables the oracle skips. It is
 * NOT the general cursor merge, which has no such skip.
 *
 * <p>Every scenario asserts the result is byte-identical to the iterator (timestamp-order) path
 * through the base harness ({@link CursorReadDifferentialTester}). The base harness's silent-fallback
 * guard proves the cursor path actually engaged: it fails loudly if
 * {@link CursorReads#isReadSupported} rejects the command, or if no sstable leg was cursor-served. The
 * driver-specific scenarios add {@link #assertNamesDriverMatches}, which also asserts the driver ran.
 *
 * <p>The base class runs BIG; {@link BtiNamesFilterCursorReadDifferentialTest} pins BTI, the
 * priority format for this work.
 */
public class NamesFilterCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void pinSelectedFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(formatName());
    }

    @After
    public void restoreSelectedFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /** The sstable format this class pins. The BTI subclass overrides it. */
    protected String formatName()
    {
        return "big";
    }

    /**
     * Differential comparison plus the driver-engagement guard. A non-tracking NAMES read routes to
     * the timestamp-order cursor driver ({@code queryMemtableAndCursorsInTimestampOrder}), NOT the
     * general cursor merge. So this asserts the driver ran on both of the harness's cursor runs
     * (canonical records, then response bytes). The base harness's own guard already proves a cursor
     * sstable leg was served, so no leg silently fell back to the iterator path.
     */
    private void assertNamesDriverMatches(ColumnFamilyStore cfs,
                                          Supplier<SinglePartitionReadCommand> command)
    {
        long driverBefore = CursorReads.namesTimestampOrderReads();

        assertCursorReadMatchesIterator(cfs, command);

        assertEquals("timestamp-order NAMES driver ran on both of the harness's cursor runs",
                     2L, CursorReads.namesTimestampOrderReads() - driverBefore);
    }

    // ---------------------------------------------------------------- multi-leg

    /**
     * A names read of several clusterings across two overlapping sstable legs: present in both legs
     * (merged), present in one leg only, absent from both, and one clustering covered by a range
     * tombstone. The cursor merge must reconcile all legs and the slicer must re-filter to the
     * requested clusterings, byte-identical to the timestamp-order path.
     */
    @Test
    public void multiLegNamesPresentAbsentAndTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "first-" + ck);
        flush();

        // second leg: newer values on a few rows, plus a range tombstone covering ck 4 and 5
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 4L, 5L);
        for (long ck = 0; ck < 10; ck += 2)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "second-" + ck, 1L, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        // 2 present-and-merged (0,2), 4 and 5 covered by the range tombstone, 7 present in leg one
        // only, 100 absent everywhere
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(0L).includeRow(2L).includeRow(4L).includeRow(5L).includeRow(7L).includeRow(100L)
                .build());
    }

    /** A names read whose clusterings straddle a range tombstone opened in one leg and closed in
     *  another, exercising cross-leg open-marker reconciliation under multi-slice emission. */
    @Test
    public void multiLegNamesCrossLegRangeTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 6L, 14L);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(3L).includeRow(6L).includeRow(10L).includeRow(14L).includeRow(18L)
                .build());
    }

    // ---------------------------------------------------------------- single-leg

    /** A names read of present and absent clusterings against a single sstable leg. */
    @Test
    public void singleLegNamesPresentAndAbsent() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(1L).includeRow(5L).includeRow(9L).includeRow(50L)
                .build());
    }

    /** A single-clustering names read: the common point read, which resolves to exactly one slice. */
    @Test
    public void singleClusteringPointRead() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).includeRow(6L).build());
    }

    /**
     * A single-clustering names read of an absent clustering that still falls WITHIN the sstable's
     * covered range, so the cursor leg opens and produces an empty result. The rows are sparse
     * (0, 2, 4, 6, 8); the read asks for the in-range gap 5. A clustering outside the covered range
     * would instead be skipped as non-intersecting on both paths, opening no leg at all.
     */
    @Test
    public void singleClusteringAbsentPointRead() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).includeRow(5L).build());
    }

    // ---------------------------------------------------------------- memtable

    /** A names read that merges a memtable with an sstable leg. The NAMES driver reads the memtable
     *  through its normal rowIterator and reconciles it with the cursor sstable leg by the shared
     *  object merge, byte-identical to the timestamp-order path. */
    @Test
    public void memtablePlusSstableNames() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "sstable-" + ck);
        flush();
        // newer, unflushed updates land in the memtable
        for (long ck = 0; ck < 10; ck += 3)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "memtable-" + ck, 1L, ck);

        long now = FBUtilities.nowInSeconds();
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(0L).includeRow(3L).includeRow(5L).includeRow(9L)
                .build());
    }

    /**
     * A memtable-only names read (no sstable). On {@code executeLocally} there is no sstable leg to
     * cursor-serve — a memtable has no cursor representation — so the memtable takes the object path
     * and no sstable leg is counted, yet the result must stay byte-identical to the iterator oracle.
     * Modeled on {@code CursorReadGateTest.memtableOnlyReadIsGateSupported}.
     */
    @Test
    public void memtableOnlyNames() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        // no flush: the read is served from the memtable only

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(1L).includeRow(4L).includeRow(8L).includeRow(50L)
                .build();

        DatabaseDescriptor.setCursorReadsEnabled(false);
        List<String> oracleRecords = canonicalRecords(cmd.get());
        byte[] oracleBytes = responseBytes(cmd.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("memtable-only names read must pass the cursor read gate",
                       CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));

            List<String> cursorRecords = canonicalRecords(cmd.get());
            byte[] cursorBytes = responseBytes(cmd.get());

            compareRecords(oracleRecords, cursorRecords);
            assertResponseBytesEqual(oracleBytes, cursorBytes);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /**
     * A memtable-only NAMES read (empty clustering set) of a partition that holds ONLY static data,
     * fetching a static column and a regular column that the partition does not contain. A raw
     * memtable rowIterator narrows its regular columns to the ones present, so it reports NO regular
     * columns; the iterator (timestamp-order) names oracle instead reports the full queried set.
     * UnfilteredRowIterators.digest hashes columns().regulars, so the cursor path must report the
     * queried set too, or a memtable-only read serves a different response than a flushed read of the
     * same data. This is the shape behind the SSTableAndMemTableDigestMatchTest static-only failures.
     * Same memtable-only guard as {@link #memtableOnlyNames}: no sstable leg to count, so the cursor
     * read gate is the engagement guard.
     */
    @Test
    public void memtableOnlyStaticOnlyNamesWithAbsentRegular() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // only static data, no clustering rows, and no flush: the read is served from the memtable only
        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 1L, 1L);

        long now = FBUtilities.nowInSeconds();
        ColumnFilter columns = staticAndAbsentRegularSelection(cfs);
        Supplier<SinglePartitionReadCommand> cmd = () -> staticOnlyNamesCommand(cfs, now, columns);

        DatabaseDescriptor.setCursorReadsEnabled(false);
        List<String> oracleRecords = canonicalRecords(cmd.get());
        byte[] oracleBytes = responseBytes(cmd.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("static-only memtable names read must pass the cursor read gate",
                       CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));

            List<String> cursorRecords = canonicalRecords(cmd.get());
            byte[] cursorBytes = responseBytes(cmd.get());

            compareRecords(oracleRecords, cursorRecords);
            assertResponseBytesEqual(oracleBytes, cursorBytes);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** A selection that fetches the absent regular column {@code v} alongside the static {@code s1},
     *  so the queried regular set is non-empty even though the partition holds no rows. */
    private static ColumnFilter staticAndAbsentRegularSelection(ColumnFamilyStore cfs)
    {
        RegularAndStaticColumns queried =
            RegularAndStaticColumns.builder()
                                   .add(cfs.metadata().getColumn(ColumnIdentifier.getInterned("v", false)))
                                   .add(cfs.metadata().getColumn(ColumnIdentifier.getInterned("s1", false)))
                                   .build();
        return ColumnFilter.selection(cfs.metadata(), queried, false);
    }

    /** A NAMES read with an empty clustering set (fetches only the static row) over partition pk=1. */
    private static SinglePartitionReadCommand staticOnlyNamesCommand(ColumnFamilyStore cfs, long nowInSec, ColumnFilter columns)
    {
        DecoratedKey key = ((SinglePartitionReadCommand) Util.cmd(cfs, 1L).build()).partitionKey();
        NavigableSet<Clustering<?>> noClusterings = new TreeSet<>(cfs.metadata().comparator);
        return SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, key, columns,
                                                 new ClusteringIndexNamesFilter(noClusterings, false));
    }

    // ---------------------------------------------------------------- static / partition deletion

    /**
     * A names read where the newer leg carries a partition-level tombstone that shadows the whole
     * older leg. The read path's timestamp elimination drops the older leg (its max timestamp is
     * below the tombstone), so the surviving read collapses to the single newer leg. The cursor path
     * serves that lone leg and stays byte-identical to the iterator (timestamp-order) path. The driver
     * still runs (it is entered before any leg logic), so this asserts the driver ran and the response
     * is byte-identical; it is NOT the two-leg merge counter, because the partition tombstone
     * legitimately eliminates the older leg before the merge (see
     * {@link #namesPartitionTombstoneBreakCountsOne} for the count contract of that break).
     */
    @Test
    public void multiLegNamesUnderPartitionDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        execute("DELETE FROM %s WHERE pk = ?", 1L);
        for (long ck = 0; ck < 10; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck + 1000);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(0L).includeRow(1L).includeRow(4L).includeRow(7L).includeRow(8L)
                .build());
    }

    /**
     * A names read where an OLDER leg carries the partition-level tombstone and a NEWER leg re-adds
     * rows above it. The older leg's own rows are purged at flush (the same-memtable partition delete
     * shadows them), so the older sstable keeps only the partition tombstone and covers no clustering.
     * The newer leg's max timestamp is above the tombstone, so neither leg is eliminated: both are read.
     *
     * <p>For this shape the older leg is non-intersecting and tombstone-only: it contributes no rows
     * and no static, only its partition-level deletion. The timestamp-order names oracle reads such a
     * leg through {@code UnfilteredRowIterators.noRowsIterator}, which reports
     * {@code EncodingStats.NO_STATS}. So the tombstone's timestamp and local-deletion-time fold into
     * {@code partitionLevelDeletion} but NOT into the response's stats header.
     *
     * <p>The cursor driver mirrors that oracle exactly (CASSANDRA-20428): its tombstone-only leg source
     * ({@code CursorReads.namesTombstoneOnlyLegIterator}) opens the leg with {@code Slices.NONE} and
     * rewraps it through {@code noRowsIterator}, forcing {@code NO_STATS} and an empty static row. So
     * the leg contributes {@code NO_STATS} to the merged header while its partition deletion still folds
     * into {@code partitionLevelDeletion}, and the whole response stays byte-identical. This is the
     * shape that retired the old {@code namesStatsQuirk} compensating hack.
     */
    @Test
    public void multiLegNamesPartitionDeleteInOlderLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // older leg: rows, then a partition delete that shadows them all (rows purge at flush)
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        execute("DELETE FROM %s WHERE pk = ?", 1L);
        flush();
        // newer leg: re-add a few rows above the partition tombstone
        for (long ck = 0; ck < 10; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck + 1000);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(0L).includeRow(1L).includeRow(4L).includeRow(7L).includeRow(8L)
                .build());
    }

    // ---------------------------------------------------------------- static-only / reversed on the driver

    /**
     * A static-only NAMES read (empty clustering set) over two flushed sstable legs, each holding the
     * static column. This runs on the driver directly (not by inheritance): the newer static wins and
     * the merged response must be byte-identical to the timestamp-order oracle. The driver reads each
     * leg's static row through the intersecting-leg source (a leg with required statics takes the
     * {@code makeRowIterator}/{@code namesLegIterator} branch), so this exercises static merge on the
     * driver.
     */
    @Test
    public void staticOnlyNamesOnSstableLegs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, ck, s1, v) VALUES (?, ?, ?, ?)", 1L, 1L, 7L, 1L);
        flush();
        execute("UPDATE %s SET s1 = ? WHERE pk = ?", 9L, 1L); // newer static in a second leg
        flush();

        long now = FBUtilities.nowInSeconds();
        ColumnFilter columns = staticAndAbsentRegularSelection(cfs);
        assertNamesDriverMatches(cfs, () -> staticOnlyNamesCommand(cfs, now, columns));
    }

    /**
     * A reversed NAMES read over two overlapping legs of a small (single-block) partition. The driver
     * accumulates and emits in the read's reversed direction, exactly as the oracle does, so the
     * response is byte-identical. The partition is small, so this does not touch the reverse
     * block-cursor {@code gotoBlock} foundation bug (that is the reverse-read gap, a separate branch).
     */
    @Test
    public void reversedNamesOnDriver() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "first-" + ck);
        flush();
        for (long ck = 0; ck < 10; ck += 2)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "second-" + ck, 1L, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(1L).includeRow(2L).includeRow(6L).includeRow(8L)
                .reverse()
                .build());
    }

    /**
     * A reversed NAMES read over a LARGE block-indexed partition. Each leg's partition is far larger
     * than the default 64 KiB {@code column_index_size}, so BTI builds a row index with many blocks.
     * The read selects clusterings spread across those blocks and reverses, which drives the leg's
     * per-block seek repeatedly in the reversed direction.
     *
     * The forward-materialize-then-reverse-at-emit rework must return bytes identical to the oracle
     * here. The old reversed path completed each leg through {@code completeSingleLegReversed}, which
     * seeks with {@code ReverseSlicedCursorIterator.gotoBlock}; that carries the reverse block-cursor
     * foundation bug that only appears on partitions over 64 KiB (multiple index blocks). The small
     * {@link #reversedNamesOnDriver} case cannot reach it, so this large case is the one that proves
     * the rework avoids the bug.
     */
    @Test
    public void reversedNamesLargeBlockIndexedPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // ~1 KiB per row over 300 rows makes each leg's partition ~300 KiB, so it crosses the default
        // 64 KiB column_index_size and BTI writes several row-index blocks.
        String pad = "x".repeat(1024);
        for (long ck = 0; ck < 300; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, pad + "-first-" + ck);
        flush();
        for (long ck = 0; ck < 300; ck += 2)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", pad + "-second-" + ck, 1L, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        // clusterings chosen to sit in different index blocks, so the reversed seek jumps between blocks.
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(10L).includeRow(90L).includeRow(170L).includeRow(250L).includeRow(299L)
                .reverse()
                .build());
    }

    /**
     * A pk-only NAMES read on a table with NO clustering column. Such a read builds a
     * {@code ClusteringIndexNamesFilter} over the single EMPTY clustering (the partition's one row), so
     * it must route to the sequential driver, not the general cursor merge (see
     * {@code SinglePartitionReadCommand} routing gate, which keys on {@code ClusteringIndexNamesFilter}
     * alone with no clustering-count precondition). This is the shape of
     * {@code SSTablesIteratedTest.testNonCompactTableWithMultipleRegularColumnsAndColumnDeletion}. The
     * full-row read must be byte-identical to the oracle.
     */
    @Test
    public void pkOnlyNamesEmptyClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, 1L, 1L);
        flush();
        execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 2000", 1L, 2L);
        flush();
        execute("DELETE v1 FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1L);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow()
                .build());
    }

    /**
     * The pk-only completeness stop. On the same no-clustering table, selecting only {@code v1} lets the
     * primary-key liveness in the SECOND-newest leg complete the read, so the OLDEST leg is skipped: the
     * count is 2, not 3. This is the empty-clustering twin of the explicit-clustering completeness stop
     * and the exact count trap of
     * {@code SSTablesIteratedTest.testNonCompactTableWithMultipleRegularColumnsAndColumnDeletion:1410}.
     * Both paths must report the same count, and the driver must run.
     */
    @Test
    public void pkOnlyNamesCompletenessStop() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, 1L, 1L);
        flush();
        execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 2000", 1L, 2L);
        flush();
        execute("DELETE v1 FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1L);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesSSTableCountMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow()
                .columns("v1")
                .build(), 2);
    }

    // ---------------------------------------------------------------- sstable-count parity (the two off-by-one traps)

    /**
     * The absent-partition-leg trap. A candidate sstable whose key range brackets the queried
     * partition but that does NOT contain it must be probed and NOT counted, on both paths. Here the
     * NEWER leg holds pk 0 and 2 (bracketing pk 1) but not pk 1; the OLDER leg holds pk 1. A NAMES read
     * of pk 1 must report exactly ONE sstable iterated on both the iterator and the cursor driver.
     */
    @Test
    public void namesAbsentPartitionLegNotCounted() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // older leg: the queried partition pk = 1
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        // newer leg: pk 0 and pk 2 only, bracketing pk 1's key without containing it
        for (long ck = 0; ck < 10; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0L, ck, ck);
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 2L, ck, ck);
        }
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesSSTableCountMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(2L).includeRow(5L).includeRow(8L)
                .build(), 1);
    }

    /**
     * The live-partition-deletion-leg trap. An older leg that is non-intersecting for the requested
     * clusterings and carries no required statics, but whose sstable metadata reports partition-level
     * deletions (from an unrelated partition), takes the tombstone-only branch. For the queried
     * partition its partition deletion is LIVE, so the driver discards it. The leg was still opened, so
     * it must be counted identically on both paths.
     *
     * <p>The older leg holds pk 1 at high clusterings (100-109) and an unrelated deleted partition
     * pk 99 (so the sstable reports partition-level deletions). The newer leg holds pk 1 at low
     * clusterings (0-4). The read asks for clusterings {0, 2, 6}: 0 and 2 come from the newer leg, 6 is
     * absent everywhere, so the read is not complete after the newer leg and the older leg is
     * consulted. The older leg does not intersect {6} and has no static, so its live partition deletion
     * for pk 1 is opened, counted, and discarded. Both paths must report the same sstable count.
     */
    @Test
    public void namesLivePartitionDeletionLegCounted() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // older leg: pk 1 at high clusterings, plus an unrelated deleted partition to set the
        // sstable's has-partition-deletions metadata flag
        for (long ck = 100; ck < 110; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        execute("DELETE FROM %s WHERE pk = ?", 99L);
        flush();
        // newer leg: pk 1 at low clusterings
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesSSTableCountMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(0L).includeRow(2L).includeRow(6L)
                .build(), 2);
    }

    // ---------------------------------------------------------------- completeness skip fired (count drops)

    /**
     * The completeness-stop drop. Two legs both hold the queried row (pk 1, ck 5); the NEWER leg holds a
     * complete row (primary-key liveness plus every queried column). The completeness check is satisfied
     * after the newer leg, so the OLDER leg is skipped: the count is 1, not 2. The byte-identical
     * differential tests are blind to this over-read, so this asserts the exact count on both paths.
     */
    @Test
    public void namesNewerCompleteLegSkipsOlder() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1L, 5L, 1L, 1L);
        flush();
        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 2000", 1L, 5L, 2L, 2L);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesSSTableCountMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).includeRow(5L).build(), 1);
    }

    /**
     * Progressive completeness across three legs. The read asks for clusterings {1, 2}. The newest leg
     * completes ck 1, the middle leg completes ck 2, so the filter is empty after two legs and the OLDEST
     * leg is skipped: the count is 2, not 3.
     */
    @Test
    public void namesMultiClusteringProgressiveCompletenessCountsTwo() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // oldest leg holds both queried clusterings; it must never be read
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, 1L, 1L);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, 2L, 2L);
        flush();
        // middle leg completes ck 2
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1L, 2L, 20L);
        flush();
        // newest leg completes ck 1
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 3000", 1L, 1L, 10L);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesSSTableCountMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).includeRow(1L).includeRow(2L).build(), 2);
    }

    /**
     * The partition-tombstone timestamp break. The newer leg carries a partition-level deletion whose
     * timestamp is above the older leg's max timestamp, so the driver stops before the older leg: the
     * count is 1. This is the count contract of the scenario {@link #multiLegNamesUnderPartitionDeletion}
     * checks for byte parity.
     */
    @Test
    public void namesPartitionTombstoneBreakCountsOne() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, ck);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ?", 1L);
        for (long ck = 0; ck < 10; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 2001", 1L, ck, ck + 1000);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesSSTableCountMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(0L).includeRow(1L).includeRow(4L).includeRow(7L).includeRow(8L)
                .build(), 1);
    }

    // ---------------------------------------------------------------- compact-table completeness

    /**
     * Compact-table completeness through the driver. The cursor gate admits COMPACT STORAGE NAMES reads
     * ({@code CursorReads.isReadSupported} rejects only Accord keyspaces, non-reusable-key partitioners,
     * and secondary indexes, not compact tables), so this asserts the driver runs and stays count- and
     * byte-parity with the iterator on a compact table.
     *
     * <p>A dense compact table is the compact shape a NAMES read can reach: {@code WHERE pk = ? AND ck = ?}
     * builds a {@code ClusteringIndexNamesFilter} over the clustering. It carries a single value column, so
     * {@code reduceFilter}'s compact branch ({@code isCompactTable() ? fetchedColumns() : queriedColumns()})
     * has fetched equal to queried; a present row is complete, so the newer leg completes ck 5 and the
     * older leg is skipped: the count is 1. The fetched-strictly-greater-than-queried divergence needs
     * multiple value columns, which compact allows only WITHOUT a clustering (a static compact table),
     * and such a partition read is a SLICE, not a NAMES read, so it never reaches this driver.
     */
    @Test
    public void namesCompactTableCompletenessCountsOne() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, 5L, 1L);
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1L, 5L, 2L);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertNamesSSTableCountMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).includeRow(5L).build(), 1);
    }

    // ---------------------------------------------------------------- purge boundary (NAMES-DRIVER-1)

    /**
     * The purge-boundary parity that catches NAMES-DRIVER-1. With {@code only_purge_repaired_tombstones}
     * on, an unrepaired tombstone is purged only when its local-deletion-time is BELOW the controller's
     * oldest-unrepaired-tombstone boundary (see {@code PurgeFunction}). A live-partition-deletion leg
     * whose sstable carries a low real {@code minLocalDeletionTime} (from an unrelated deleted partition)
     * must lower that boundary, exactly as the iterator oracle does through {@code add}. If the cursor
     * driver discards that leg without folding its stats, the boundary stays too high and the driver
     * purges the queried partition's tombstone that the oracle keeps, diverging the emitted bytes.
     *
     * <p>Shape: the middle leg (ts 2500) is non-intersecting for the outstanding clustering 6 and carries
     * an unrelated {@code pk = 99} deletion, so it takes the tombstone-only branch with a LIVE deletion
     * for pk 1, and its low real stats must fold into the boundary; the oldest leg (ts 2000) contributes
     * pk 1's own partition tombstone through the NO_STATS tombstone-only source on BOTH paths, so it never
     * lowers the boundary; the newest leg (ts 4000) holds live rows 0-4 that survive. So only the middle
     * leg can lower the boundary, and it does so only through the fix. The read is far in the future with
     * {@code gc_grace_seconds = 0}, so pk 1's partition tombstone is gc-eligible and its survival turns on
     * the boundary alone: with the fix both paths keep it; without the fix the driver's boundary stays too
     * high and it purges the tombstone the oracle keeps.
     *
     * <p>The pk = 99 deletion is written BEFORE the pk = 1 deletion so pk 1's real local-deletion-time is
     * greater than or equal to the boundary the middle leg folds; that keeps pk 1's tombstone on the keep
     * side of the boundary on both paths under the fix, even across a one-second wall-clock boundary. The
     * {@code USING TIMESTAMP} clauses fix the sstable order independent of that write order.
     */
    @Test
    public void namesLivePartitionDeletionLegLowersPurgeBoundary() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck)) "
                    + "WITH gc_grace_seconds = 0 "
                    + "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones': 'true'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // middle leg (ts 2500): pk 1 rows out of the queried range, plus an unrelated deleted partition
        // that gives this sstable a low real minLocalDeletionTime; for pk 1 its deletion is LIVE, so it is
        // the only leg whose real stats can lower the boundary. Written first so its pk 99 deletion time
        // is the earliest wall-clock local-deletion-time.
        for (long ck = 100; ck < 110; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 2500", 1L, ck, ck);
        execute("DELETE FROM %s USING TIMESTAMP 2500 WHERE pk = ?", 99L);
        flush();
        // oldest leg (ts 2000): pk 1's own partition tombstone, read through the NO_STATS tombstone-only
        // source, so it does not lower the purge boundary itself
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ?", 1L);
        flush();
        // newest leg (ts 4000): live rows 0-4 above the partition tombstone, so they survive in the result
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 4000", 1L, ck, ck);
        flush();

        // read well past gc_grace so the partition tombstone is gc-eligible; ck 6 is absent everywhere, so
        // the read never completes and consults every leg
        long now = FBUtilities.nowInSeconds() + 100_000;
        assertNamesDriverMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now)
                .includeRow(0L).includeRow(2L).includeRow(6L)
                .build());
    }

    /**
     * Runs the command under the iterator path and the cursor driver and asserts the sstables-iterated
     * count matches (the {@code SSTablesIteratedTest} count contract, applied differentially). Also
     * asserts the expected count, and that the driver ran under the cursor path.
     */
    private void assertNamesSSTableCountMatches(ColumnFamilyStore cfs,
                                                Supplier<SinglePartitionReadCommand> command,
                                                int expectedCount)
    {
        long iteratorCount = sstablesIterated(cfs, command.get(), false);

        long driverBefore = CursorReads.namesTimestampOrderReads();
        long cursorCount = sstablesIterated(cfs, command.get(), true);

        assertEquals("timestamp-order NAMES driver ran under the cursor path",
                     1L, CursorReads.namesTimestampOrderReads() - driverBefore);
        assertEquals("sstables iterated diverged between iterator and cursor driver",
                     iteratorCount, cursorCount);
        assertEquals("unexpected sstables-iterated count", expectedCount, cursorCount);
    }

    /** Executes the command fully under the given path and returns the max sstables-iterated the
     *  {@code sstablesPerReadHistogram} recorded for it. */
    private long sstablesIterated(ColumnFamilyStore cfs, SinglePartitionReadCommand command, boolean cursor)
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
        try
        {
            ((ClearableHistogram) cfs.metric.sstablesPerReadHistogram.cf).clear();
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller))
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        while (partition.hasNext())
                            partition.next();
                    }
                }
            }
            return cfs.metric.sstablesPerReadHistogram.cf.getSnapshot().getMax();
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }
}
