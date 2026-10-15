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

package org.apache.cassandra.db.compaction.differential;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.repair.CassandraTableRepairManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.repair.LocalValidation;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Cycles of incremental repair on one node, on BIG and BTI.  Each cycle lets UCS compact what the
 * earlier cycles left, writes rows, deletions, range deletions and expiring cells, then runs the
 * session steps that read sstables: anticompaction of random full, transient and unrepaired
 * ranges, and validation of the pending data and of the repaired data.  Each cycle runs these
 * steps on the legacy path and on the cursor path from the same sstables, and compares the Merkle
 * trees and every live sstable afterwards: bytes, logical content and repair metadata.  During the
 * validations, the bytes of live objects (from a class histogram) may grow at most a fixed 16 MiB
 * more on the cursor path than on the legacy path, at any size, so heap that the cursor path keeps
 * for each row fails.  The cycle then finalizes or fails the session at random.
 *
 * <p>Run it through {@code test-isolated.sh}.  {@code -Dcassandra.test.incremental_repair_burn_rows}
 * (default 100,000), {@code -Dcassandra.test.incremental_repair_burn_cycles} (default 20),
 * {@code -Dcassandra.test.incremental_repair_burn_sstables} (flushes per cycle, default 3),
 * {@code -Dcassandra.test.incremental_repair_burn_ranges} (the most ranges a session repairs,
 * default 32) and {@code -Dcassandra.test.incremental_repair_burn_large_partition_bytes} (one
 * partition of that size, default none) set the size, and
 * {@code -Dcassandra.test.differential.seed} set to the logged run seed replays the workload and
 * the ranges.  The replay is not exact: what background UCS compaction does between cycles, and
 * the cell timestamps, depend on the clock, so the sstables a session reads can differ.
 */
@RunWith(Parameterized.class)
public class BurnIncrementalRepairCycleDifferentialTest extends DifferentialCompactionTester
{
    private static final int ROWS_PER_PARTITION = 20;
    /** The most the live heap may grow more during the cursor validations than during the legacy ones, at any size. */
    private static final long RETAINED_HEAP_SLACK_BYTES = 16L << 20;
    private static final int LARGE_VALUE_BYTES = 64 << 10;
    private static final String VALUE_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private int flushesPerCycle;
    private int rangeCuts;
    private long largestCursorValidationHeapGrowth;
    private long largestLegacyValidationHeapGrowth;
    private int cyclesWithPendingOutput;

    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static List<Object[]> formats()
    {
        return List.of(new Object[]{ "big" }, new Object[]{ "bti" });
    }

    /** Scale mode: the logical dump is streamed into a SHA-256 digest, so capture memory stays flat. */
    @Override
    protected boolean scaleCapture()
    {
        return true;
    }

    @Before
    public void selectFormat()
    {
        selectSSTableFormat(format);
    }

    @After
    public void restoreFormat()
    {
        restoreSelectedFormat();
    }

    @Test
    public void incrementalRepairCycles() throws Throwable
    {
        long rows = CassandraRelevantProperties.TEST_INCREMENTAL_REPAIR_BURN_ROWS.getLong();
        int cycles = CassandraRelevantProperties.TEST_INCREMENTAL_REPAIR_BURN_CYCLES.getInt();
        flushesPerCycle = CassandraRelevantProperties.TEST_INCREMENTAL_REPAIR_BURN_SSTABLES.getInt();
        rangeCuts = CassandraRelevantProperties.TEST_INCREMENTAL_REPAIR_BURN_RANGES.getInt();
        long largePartitionBytes = CassandraRelevantProperties.TEST_INCREMENTAL_REPAIR_BURN_LARGE_PARTITION_BYTES.getLong();
        assertTrue("burn rows, cycles, sstables and ranges must be positive", rows > 0 && cycles > 0 && flushesPerCycle > 0 && rangeCuts > 0);
        long seed = DifferentialRunSeed.SEED ^ format.hashCode();
        logger.info("incremental repair burn: format={} rows={} cycles={} sstables per cycle={} ranges={} large partition bytes={} seed={} (run seed {})",
                    format, rows, cycles, flushesPerCycle, rangeCuts, largePartitionBytes, seed, DifferentialRunSeed.SEED);
        Random random = new Random(seed);

        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v text, m map<text, int>, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0 AND compaction = {'class': 'UnifiedCompactionStrategy'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        cyclesWithPendingOutput = 0;
        largestCursorValidationHeapGrowth = 0;
        largestLegacyValidationHeapGrowth = 0;
        long written = 0;
        long partitionsPerCycle = Math.max(1, rows / cycles / ROWS_PER_PARTITION);
        if (largePartitionBytes > 0)
        {
            try
            {
                writeLargePartition(random, largePartitionBytes);
            }
            catch (Throwable t)
            {
                throw new AssertionError("failure for seed " + seed + "; " + DifferentialRunSeed.replayAdvice(DifferentialRunSeed.SEED) +
                                         "; format " + format + ", writing the large partition: " + t, t);
            }
        }
        for (int cycle = 0; cycle < cycles; cycle++)
        {
            // background compaction of what the earlier cycles left, then this cycle's writes, so
            // the session sees all of this cycle's sstables; then let the expiring cells and the
            // tombstones pass gc grace
            try
            {
                cfs.enableAutoCompaction(true);
                cfs.disableAutoCompaction();
                awaitNoCompaction(cfs);
                written += writeCycle(random, cycle, partitionsPerCycle);
                AntiCompactionDifferentialTest.awaitClockPast(FBUtilities.nowInSeconds() + 1);
                runCycle(cfs, random, cycle);
            }
            catch (Throwable t)
            {
                throw new AssertionError("failure for seed " + seed + "; " + DifferentialRunSeed.replayAdvice(DifferentialRunSeed.SEED) +
                                         "; format " + format + ", cycle " + cycle + ": " + t, t);
            }
        }
        assertTrue("the burn wrote only " + written + " rows; expected at least " + rows, written >= rows - cycles * ROWS_PER_PARTITION);
        // a cycle whose full ranges hold only purged rows leaves nothing pending; most must not
        assertTrue("only " + cyclesWithPendingOutput + " of " + cycles + " sessions left a pending sstable", cyclesWithPendingOutput * 2 > cycles);
        logger.info("incremental repair burn: {} cycles matched; the most the live heap grew during the validations was {} bytes on the cursor path and {} bytes on the legacy path",
                    cycles, largestCursorValidationHeapGrowth, largestLegacyValidationHeapGrowth);
    }

    /**
     * Waits, for at most a minute, until no compaction of the table runs.  A background compaction
     * that started before autocompaction was disabled would otherwise compact sstables that the
     * cycle reads.
     */
    private static void awaitNoCompaction(ColumnFamilyStore cfs)
    {
        CompactionManager.instance.waitForCessation(List.of(cfs), sstable -> true);
        assertFalse("a compaction of " + cfs + " is still running", CompactionManager.instance.isCompacting(List.of(cfs), sstable -> true));
    }

    /** {@link #LARGE_VALUE_BYTES} random letters and digits. */
    private static String randomValue(Random random)
    {
        byte[] bytes = new byte[LARGE_VALUE_BYTES];
        random.nextBytes(bytes);
        char[] chars = new char[LARGE_VALUE_BYTES];
        for (int i = 0; i < chars.length; i++)
            chars[i] = VALUE_CHARS.charAt((bytes[i] & 0xff) % VALUE_CHARS.length());
        return new String(chars);
    }

    /**
     * Writes one partition of about {@code bytes}, of seeded random values so that it does not
     * compress away, flushing about every 64 MiB, so it spans several sstables.
     */
    private void writeLargePartition(Random random, long bytes)
    {
        long written = 0;
        long sinceFlush = 0;
        for (long ck = 0; written < bytes; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", -1L, ck, randomValue(random));
            written += LARGE_VALUE_BYTES;
            sinceFlush += LARGE_VALUE_BYTES;
            if (sinceFlush >= 64L << 20)
            {
                flush();
                sinceFlush = 0;
            }
        }
        flush();
    }

    /** Writes one cycle of data in the configured number of flushes; returns the rows inserted. */
    private long writeCycle(Random random, int cycle, long partitions)
    {
        long inserted = 0;
        long base = cycle * partitions / 2; // half of each cycle overwrites the previous cycle's keys
        for (int flush = 0; flush < flushesPerCycle; flush++)
        {
            for (long p = flush; p < partitions; p += flushesPerCycle)
            {
                long pk = base + p;
                execute("UPDATE %s SET s = ? WHERE pk = ?", "s" + cycle, pk);
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                {
                    int kind = random.nextInt(20);
                    if (kind == 0)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TTL 1", pk, ck, "expires");
                    else if (kind == 1)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TTL 864000", pk, ck, "expires later");
                    else
                        execute("INSERT INTO %s (pk, ck, v, m) VALUES (?, ?, ?, ?)", pk, ck, "v" + cycle, Map.of("c" + cycle, (int) ck));
                    inserted++;
                }
                int deletion = random.nextInt(10);
                if (deletion == 0)
                    execute("DELETE FROM %s WHERE pk = ?", pk - partitions / 2);
                else if (deletion == 1)
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", pk, 3L, 7L);
                else if (deletion == 2)
                    execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, 1L);
            }
            flush();
        }
        return inserted;
    }

    /**
     * Runs the cycle's session steps on both paths from the same sstables and compares them, then
     * finalizes or fails the session on the result.
     */
    private void runCycle(ColumnFamilyStore cfs, Random random, int cycle) throws Exception
    {
        RangesAtEndpoint ranges = AntiCompactionRanges.randomRanges(random, AntiCompactionRanges.liveTokens(cfs), 1 + random.nextInt(rangeCuts));
        long nowInSec = FBUtilities.nowInSeconds();
        TimeUUID session = TimeUUID.Generator.nextTimeUUID();
        Path scratch = Files.createTempDirectory("incremental-repair-burn");
        try
        {
            InputCopy copy = copyLiveSet(cfs, scratch.resolve("inputs"));
            CycleResult legacy = runSessionSteps(cfs, ranges, session, nowInSec, false, scratch.resolve("legacy"));
            // same descriptors, so UCS pairs the sstables of the session the same way on both runs
            restoreLiveSet(cfs, copy, true);
            CycleResult cursor = runSessionSteps(cfs, ranges, session, nowInSec, true, scratch.resolve("cursor"));

            assertArrayEquals("the Merkle trees of the pending data differ", legacy.pendingTrees, cursor.pendingTrees);
            assertArrayEquals("the Merkle trees of the repaired data differ", legacy.repairedTrees, cursor.repairedTrees);
            assertEquivalentOutputs(legacy.liveSet, cursor.liveSet);
            assertEquals("one path left a pending sstable and the other did not", legacy.leftPending, cursor.leftPending);
            largestCursorValidationHeapGrowth = Math.max(largestCursorValidationHeapGrowth, cursor.validationHeapGrowth);
            largestLegacyValidationHeapGrowth = Math.max(largestLegacyValidationHeapGrowth, legacy.validationHeapGrowth);
            assertTrue("during the validations, the live heap grew by " + cursor.validationHeapGrowth + " bytes on the cursor path and " +
                       legacy.validationHeapGrowth + " bytes on the legacy path; at most " + RETAINED_HEAP_SLACK_BYTES + " more is allowed",
                       cursor.validationHeapGrowth <= legacy.validationHeapGrowth + RETAINED_HEAP_SLACK_BYTES);
            if (cursor.leftPending)
                cyclesWithPendingOutput++;
        }
        finally
        {
            FileUtils.deleteDirectory(scratch.toFile());
        }

        // finalize or fail the session, as the coordinator would
        List<SSTableReader> pending = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (session.equals(sstable.getPendingRepair()))
                pending.add(sstable);
        boolean finalize = random.nextInt(4) != 0;
        if (!pending.isEmpty())
            cfs.getCompactionStrategyManager().mutateRepaired(pending, finalize ? nowInSec * 1000L : UNREPAIRED_SSTABLE, NO_PENDING_REPAIR, false);
        org.apache.cassandra.Util.assertOnDiskState(cfs, cfs.getLiveSSTables().size());
        org.apache.cassandra.db.compaction.AntiCompactionAssertions.assertEveryFileBelongsToALiveSSTable(cfs);
        logger.info("incremental repair burn: cycle {} matched over {} live sstables; {} pending sstables {}",
                    cycle, cfs.getLiveSSTables().size(), pending.size(), finalize ? "finalized" : "failed");
    }

    private static boolean insideOneRange(SSTableReader sstable, Collection<Range<Token>> ranges)
    {
        for (Range<Token> range : Range.normalize(ranges))
            if (range.contains(sstable.getFirst().getToken()) && range.contains(sstable.getLast().getToken()))
                return true;
        return false;
    }

    private static final class CycleResult
    {
        /** Whether the session left a pending sstable; not when every row in its full ranges purged. */
        boolean leftPending;
        byte[] pendingTrees;
        byte[] repairedTrees;
        CapturedOutput liveSet;
        /** How much the bytes of live objects grew during the two validations. */
        long validationHeapGrowth;
    }

    private CycleResult runSessionSteps(ColumnFamilyStore cfs, RangesAtEndpoint ranges, TimeUUID session, long nowInSec,
                                        boolean cursor, Path scratch) throws Exception
    {
        CycleResult result = new CycleResult();
        // an sstable inside one full or one transient range gets new repair metadata, not a rewrite
        boolean rewrites = false;
        for (SSTableReader sstable : sessionSSTables(cfs, ranges))
            rewrites |= !insideOneRange(sstable, ranges.onlyFull().ranges()) && !insideOneRange(sstable, ranges.onlyTransient().ranges());
        long runsBefore = CompactionManager.cursorAnticompactionsRun.sum();
        long writtenBefore = CompactionManager.cursorAnticompactionPartitionsWritten.sum();
        runAntiCompactionSession(cfs, ranges, session, cursor, -1);
        long runs = CompactionManager.cursorAnticompactionsRun.sum() - runsBefore;
        long written = CompactionManager.cursorAnticompactionPartitionsWritten.sum() - writtenBefore;
        if (cursor && rewrites)
        {
            assertTrue("the cursor path ran no anticompaction", runs > 0);
            assertTrue("the cursor path wrote no partition", written > 0);
        }
        else if (cursor)
        {
            assertEquals("the cursor path ran with no sstable to rewrite", 0, runs);
        }
        else
        {
            assertEquals("the cursor path ran on the legacy run", 0, runs);
            assertEquals("the cursor path wrote partitions on the legacy run", 0, written);
        }
        result.leftPending = cfs.getLiveSSTables().stream().anyMatch(s -> session.equals(s.getPendingRepair()));
        // count only live objects, not garbage that a young collection moved to the old generation
        RetainedHeap heap = RetainedHeap.watchLive(250);
        try
        {
            result.pendingTrees = validate(cfs, ranges, session, true, PreviewKind.NONE, nowInSec, cursor);
            result.repairedTrees = validate(cfs, ranges, TimeUUID.Generator.nextTimeUUID(), false, PreviewKind.REPAIRED, nowInSec, cursor);
        }
        finally
        {
            heap.stop();
        }
        result.validationHeapGrowth = heap.growth;
        result.liveSet = captureLiveSet(cfs, scratch);
        return result;
    }

    /** One validation of a session of this kind, on the path asked for; checks with the counters that it ran there. */
    private static byte[] validate(ColumnFamilyStore cfs, RangesAtEndpoint ranges, TimeUUID parentId, boolean incremental, PreviewKind previewKind,
                                   long nowInSec, boolean cursor) throws Exception
    {
        boolean originalCursor = DatabaseDescriptor.cursorRepairEnabled();
        DatabaseDescriptor.setCursorRepairEnabled(cursor);
        ActiveRepairService.instance().registerParentRepairSession(parentId, FBUtilities.getBroadcastAddressAndPort(), List.of(cfs),
                                                                   ranges.ranges(), incremental, UNREPAIRED_SSTABLE, true, previewKind);
        long runsBefore = CassandraTableRepairManager.cursorValidationsRun.sum();
        long fallbacksBefore = CassandraTableRepairManager.cursorValidationFallbacks.sum();
        try
        {
            LocalValidation.Result result = LocalValidation.validate(cfs, parentId, TimeUUID.Generator.nextTimeUUID(), new ArrayList<>(ranges.ranges()),
                                                                     incremental, previewKind, nowInSec);
            assertEquals("cursor validation fell back", 0, CassandraTableRepairManager.cursorValidationFallbacks.sum() - fallbacksBefore);
            assertEquals(cursor ? "the cursor validation did not run" : "the cursor validation ran with it off",
                         cursor ? 1 : 0, CassandraTableRepairManager.cursorValidationsRun.sum() - runsBefore);
            return result.serializedTrees;
        }
        finally
        {
            ActiveRepairService.instance().removeParentRepairSession(parentId);
            DatabaseDescriptor.setCursorRepairEnabled(originalCursor);
        }
    }
}
