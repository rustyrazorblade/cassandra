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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;

import org.apache.commons.io.FileUtils;
import org.junit.Assume;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.AntiCompactionAssertions;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionPipelineCounts;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Runs the same input sstables through both the iterator and cursor compaction paths and asserts
 * the outputs are byte-identical and logically identical.
 */
public abstract class DifferentialCompactionTester extends DifferentialCorpusDriver
{

    /** Keeps a failed comparison's captured sstables under scratch for local post-mortem. */
    private static final boolean KEEP_SCRATCH_ON_FAILURE =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_KEEP_SCRATCH_ON_FAILURE.getBoolean();

    /** Whether {@link #capture} reads every captured output back through single-row slices. */
    private static final boolean SLICE_READBACK =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_SLICE_READBACK.getBoolean();

    /** Ceiling on how many rows of one partition {@link #assertEveryRowReadableThroughASlice} probes. */
    private static final int SLICE_READBACK_MAX_ROWS_PER_PARTITION =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_SLICE_READBACK_MAX_ROWS.getInt();

    // Matches the "expired" flag in a JSON dump, which every capture normalizes away.
    private static final Pattern EXPIRED_FLAG =
        Pattern.compile("\"expired\"\\s*:\\s*(true|false)");

    /** The rendered form of a cell tombstone, for absolute assertions over {@link #allJson}. */
    protected static final String CELL_TOMBSTONE = "\"deletion_info\":{\"local_delete_time\"";

    /**
     * Scale mode for very large scenarios: the logical dump is streamed into a SHA-256 digest
     * instead of being retained as a String, so capture memory stays flat regardless of row count.
     */
    protected boolean scaleCapture()
    {
        return false;
    }

    /**
     * Whether {@link #assertEveryRowReadableThroughASlice} also seeks each row with a reverse slice.
     * A scenario turns it off only for a read the format itself answers differently in reverse.
     */
    protected boolean reverseSliceReadback = true;

    public static final class CapturedSSTable
    {
        final Path dir;                 // copied component files, named by component
        final String json;              // logical dump, or a SHA-256 digest string in scale mode
        final String statsSummary;
        final long totalRows;           // rows the output carries, for non-triviality guards (e.g. purge)
        final SortedMap<String, Long> componentSizes = new TreeMap<>();

        CapturedSSTable(Path dir, String json, String statsSummary, long totalRows)
        {
            this.dir = dir;
            this.json = json;
            this.statsSummary = statsSummary;
            this.totalRows = totalRows;
        }
    }

    public static final class CapturedOutput
    {
        final List<CapturedSSTable> sstables = new ArrayList<>();
    }

    /** Concatenates the logical dump (or digest string in scale mode) of every captured output sstable. */
    protected static String allJson(CapturedOutput out)
    {
        StringBuilder sb = new StringBuilder();
        for (CapturedSSTable s : out.sstables)
            sb.append(s.json);
        return sb.toString();
    }

    /** Whether one compaction gives exactly one output.  Anticompaction splits its output by repair state. */
    protected boolean expectSingleOutput()
    {
        return true;
    }

    /** Sum over the captured sstables of one numeric field of their stats summary, such as {@code totalRows}. */
    protected static long sumOfStat(CapturedOutput out, String name)
    {
        long sum = 0;
        for (CapturedSSTable s : out.sstables)
        {
            Matcher matcher = Pattern.compile("(^| )" + Pattern.quote(name) + "=(-?[0-9]+) ").matcher(s.statsSummary);
            assertTrue("no " + name + " in stats summary: " + s.statsSummary, matcher.find());
            sum += Long.parseLong(matcher.group(2));
        }
        return sum;
    }

    /** Sum of the per-sstable row counts across a captured output. */
    protected static long outputRows(CapturedOutput out)
    {
        long rows = 0;
        for (CapturedSSTable s : out.sstables)
            rows += s.totalRows;
        return rows;
    }

    /** The rendered form of a text-typed cell holding {@code text}, for absolute assertions over {@link #allJson}. */
    protected static String cellValue(String text)
    {
        return "\"value\":\"" + text + '"';
    }

    /** Counts non-overlapping occurrences of {@code needle} in {@code haystack}. */
    protected static int countOccurrences(String haystack, String needle)
    {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length()))
            count++;
        return count;
    }

    /**
     * The latest local deletion time among the given sstables, for placing a {@code gcBefore} relative
     * to their tombstones. Fails if the scenario produced no deletion.
     */
    protected static long maxTombstoneLocalDeletionTime(Iterable<SSTableReader> sstables)
    {
        long max = Long.MIN_VALUE;
        for (SSTableReader sstable : sstables)
        {
            long ldt = sstable.getSSTableMetadata().maxLocalDeletionTime;
            if (ldt != Cell.NO_DELETION_TIME)
                max = Math.max(max, ldt);
        }
        assertTrue("scenario produced no tombstone deletion times", max > 0 && max < Long.MAX_VALUE);
        return max;
    }

    /** Creates the CompactionTask for one differential run. Must honor keepOriginals=true. */
    public interface TaskFactory
    {
        CompactionTask create(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore);

        /** The nowInSec the task uses, which an anticompaction run uses too. */
        default long nowInSeconds()
        {
            return FBUtilities.nowInSeconds();
        }
    }

    public static final TaskFactory DEFAULT_TASK = (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, true);

    /** A TaskFactory that uses a fixed nowInSec for TTL expiry instead of the wall clock. */
    public static TaskFactory taskWithFixedNow(long nowInSeconds)
    {
        return taskWithNow(() -> nowInSeconds);
    }

    /** A TaskFactory that takes nowInSec for TTL expiry from {@code nowInSeconds} instead of the wall clock. */
    private static TaskFactory taskWithNow(LongSupplier nowInSeconds)
    {
        return new TaskFactory()
        {
            @Override
            public CompactionTask create(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore)
            {
                return new CompactionTask(cfs, txn, gcBefore, true).setNowInSecondsSupplier(nowInSeconds);
            }

            @Override
            public long nowInSeconds()
            {
                return nowInSeconds.getAsLong();
            }
        };
    }

    /** The earliest local deletion time across the live set: below this, gcBefore purges nothing. */
    protected static long minLocalDeletionTime(ColumnFamilyStore cfs)
    {
        long min = Long.MAX_VALUE;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            min = Math.min(min, sstable.getSSTableMetadata().minLocalDeletionTime);
        return min;
    }

    /** Asserts at least one cell in the current live set has expired relative to nowInSeconds. */
    protected void assertSomethingExpiredAt(ColumnFamilyStore cfs, long nowInSeconds)
    {
        long minLocalDeletionTime = minLocalDeletionTime(cfs);
        assertTrue("no cell in the live set has expired relative to the fixed nowInSec " + nowInSeconds +
                   "; the earliest local deletion time on disk is " + minLocalDeletionTime,
                   minLocalDeletionTime <= nowInSeconds);
    }

    /**
     * How far ahead of the wall clock a purge shape's fixed nowInSec sits, so its short-TTL cells and
     * gc_grace=0 tombstones are past gc grace.
     */
    private static final long FIXED_NOW_OFFSET_SECONDS = 3600;

    /**
     * Runs the differential for one corpus shape. Most shapes run once at the default gcBefore. A shape that
     * {@link DifferentialSchema#expectsPurge() expects purge} runs twice with the SAME fixed nowInSec after gc
     * grace, varying only gcBefore: the compaction without purging keeps every expired cell and tombstone, the
     * compaction with purging drops them. Both paths agree in each run, so each run is a valid cursor-vs-iterator
     * comparison. The purged output must then carry strictly fewer rows than the retained one; the merge dedup is
     * identical across the two runs, so the row difference isolates purge, and a shape that purges nothing fails
     * here rather than passing on a trivially equal comparison.
     */
    protected void assertCursorMatchesIteratorForShape(ColumnFamilyStore cfs, DifferentialSchema schema) throws Exception
    {
        if (!schema.expectsPurge())
        {
            assertShapeLeftMergedOutput(schema, assertCursorMatchesIterator(cfs));
            return;
        }

        long now = FBUtilities.nowInSeconds() + FIXED_NOW_OFFSET_SECONDS;
        // Anchor the compaction without purging to the data on disk, not the wall clock. One below the earliest
        // local deletion time is below every deletion time by construction, so that compaction purges nothing no
        // matter how long the writes and guards took. A wall-clock baseline drifts past short-TTL cells at burn
        // scale and makes it purge them too, which would falsely fail the guard below.
        long noPurgeGcBefore = minLocalDeletionTime(cfs) - 1;

        CapturedOutput retained = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskWithFixedNow(now), noPurgeGcBefore);
        CapturedOutput purged = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskWithFixedNow(now), now);
        assertShapeLeftMergedOutput(schema, retained);
        assertTrue("purge shape '" + schema.name() + "' dropped no rows: the merge kept " + outputRows(retained) +
                   " rows with purge disabled and " + outputRows(purged) + " with purge at now=" + now +
                   "; the scenario purged nothing, so it does not exercise purge",
                   outputRows(purged) < outputRows(retained));
    }

    /**
     * Every corpus shape must leave at least one merged sstable to compare. An equal-but-empty comparison
     * proves nothing, so this guards the corpus path itself rather than any single test.
     */
    private static void assertShapeLeftMergedOutput(DifferentialSchema schema, CapturedOutput output)
    {
        assertFalse("corpus shape '" + schema.name() + "' produced no output sstable: an equal-but-empty " +
                    "comparison proves nothing; every shape must leave at least one merged sstable",
                    output.sstables.isEmpty());
    }

    /**
     * Runs both compaction paths over the current live sstables of the table and asserts
     * byte + logical equivalence of every output component.
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs) throws Exception
    {
        return assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK);
    }

    /**
     * Variant for partial-set compactions (inputs is a subset of the live sstables) and for custom
     * CompactionTask shapes. Returns the iterator-path capture for structural assertions.
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs,
                                                         Set<SSTableReader> inputs,
                                                         TaskFactory taskFactory) throws Exception
    {
        return assertCursorMatchesIterator(cfs, inputs, taskFactory,
                                           CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));
    }

    /**
     * Variant with an explicit gcBefore, so scenarios can place purge decisions exactly at the boundary.
     * <p>
     * This is the one compare method: every other assertCursorMatchesIterator variant, and the corpus
     * and generation helpers, call it.  An anticompaction subclass of a differential class overrides
     * only this method, to call {@link #assertAntiCompactionMatchesIterator}, and so runs every
     * scenario of its parent through anticompaction.
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs,
                                                         Set<SSTableReader> inputs,
                                                         TaskFactory taskFactory,
                                                         long gcBefore) throws Exception
    {
        Path scratch = Files.createTempDirectory("differential-compaction");

        // scratch holds byte-for-byte copies of every captured output sstable, for both paths.
        boolean passed = false;
        try
        {
            CapturedOutput iterator = compactPath(cfs, inputs, false, gcBefore, scratch.resolve("iterator"), taskFactory);
            // input instances were replaced during restore; re-resolve the subset by descriptor
            Set<Descriptor> inputDescs = new HashSet<>();
            for (SSTableReader in : inputs)
                inputDescs.add(in.descriptor);
            Set<SSTableReader> reResolved = new HashSet<>();
            for (SSTableReader live : cfs.getLiveSSTables())
                if (inputDescs.contains(live.descriptor))
                    reResolved.add(live);
            assertEquals("input subset lost across restore", inputs.size(), reResolved.size());
            CapturedOutput cursor = compactPath(cfs, reResolved, true, gcBefore, scratch.resolve("cursor"), taskFactory);
            assertEquivalentOutputs(iterator, cursor);
            passed = true;
            return iterator;
        }
        finally
        {
            deleteScratch(scratch, passed);
        }
    }

    private static void deleteScratch(Path scratch, boolean passed)
    {
        if (passed || !KEEP_SCRATCH_ON_FAILURE)
        {
            // never rethrown: an IOException here would replace the AssertionError carrying the
            // divergence report with an unrelated one
            try
            {
                FileUtils.deleteDirectory(scratch.toFile());
            }
            catch (IOException e)
            {
                logger.warn("could not delete differential scratch directory {}", scratch, e);
            }
        }
        else
        {
            logger.error("differential comparison failed; both paths' captured sstables kept at {}", scratch);
        }
    }

    /** The outputs of one anticompaction run, their repair metadata, and the count antiCompactGroup returned. */
    public static final class AntiCompactionOutput
    {
        final CapturedOutput captured = new CapturedOutput();
        final List<String> repairMetadata = new ArrayList<>();
        int returned;
        /** The uncompressed bytes of the inputs: what full-range scanners of them read. */
        long inputDataBytes;
        /** What this run passed to the compaction rate limiter, seen by a listener on the test thread. */
        final RateLimiterCalls throttled = new RateLimiterCalls();
        /** The compression ratio that antiCompactGroup passes to the rate limiter for these inputs. */
        double compressionRatio;
        /** How much the heap that stays live after garbage collection grew during this run. */
        long retainedHeapGrowth;
        /** On the legacy run that a comparison returns, the cursor run it was compared with. */
        AntiCompactionOutput cursorRun;

        public int sstableCount()
        {
            return captured.sstables.size();
        }

        /** The repair metadata of each output, in first-key order. */
        public List<String> repairMetadata()
        {
            return repairMetadata;
        }

        public CapturedOutput captured()
        {
            return captured;
        }
    }

    /** How the heap is watched while each path anticompacts; a burn that checks it counts only live objects. */
    protected RetainedHeap watchHeapDuringAntiCompaction()
    {
        return RetainedHeap.watch();
    }

    /** Comparisons so far in this test method.  JUnit makes a new instance for each method, so it starts at 0. */
    private int anticompactionRangesDrawn;

    /**
     * The seed of the ranges of one comparison: from the run seed, the test class and method, and
     * the index of the comparison in the method, counted from 0.  So one failing method replays
     * alone with the run seed.  A subclass whose comparisons belong to seeded examples derives it
     * from the example's seed instead, so that an example replays alone.
     */
    protected long anticompactionRangesSeed(int comparison)
    {
        String test = getClass().getName() + '.' + testName.getMethodName();
        return DifferentialRunSeed.SEED * 31 + test.hashCode() * 1_000_003L + comparison;
    }

    /** The seed that replays a failing comparison: the run seed, or the example's seed in a subclass with seeded examples. */
    protected long replaySeed()
    {
        return DifferentialRunSeed.SEED;
    }

    /**
     * What an anticompaction subclass calls from its override of
     * {@link #assertCursorMatchesIterator(ColumnFamilyStore, Set, TaskFactory, long)}: the same
     * inputs, time and gcBefore, anticompacted on both paths with random full, transient and
     * unrepaired ranges.  The other live sstables stay in place.
     */
    protected CapturedOutput assertAntiCompactionMatchesIterator(ColumnFamilyStore cfs,
                                                                 Set<SSTableReader> inputs,
                                                                 TaskFactory taskFactory,
                                                                 long gcBefore) throws Exception
    {
        int comparison = anticompactionRangesDrawn++;
        long seed = anticompactionRangesSeed(comparison);
        Random random = new Random(seed);
        RangesAtEndpoint ranges = AntiCompactionRanges.randomRanges(random, AntiCompactionRanges.tokens(inputs), 1 + random.nextInt(32));
        logger.info("anticompaction ranges seed={} (comparison {} of {}, counted from 0; {}): {}",
                    seed, comparison, testName.getMethodName(), DifferentialRunSeed.replayAdvice(replaySeed()), ranges);
        try
        {
            return assertCursorMatchesIteratorForAntiCompaction(cfs, inputs, ranges, taskFactory.nowInSeconds(), gcBefore).captured;
        }
        catch (Throwable t)
        {
            throw new AssertionError("anticompaction differential failed; " + DifferentialRunSeed.replayAdvice(replaySeed()) +
                                     "; comparison " + comparison + " of " + testName.getMethodName() + " (counted from 0), ranges seed " +
                                     seed + ", ranges " + ranges + ": " + t, t);
        }
    }

    /**
     * Anticompacts the whole live set on the legacy path and then on the cursor path, at the wall
     * clock.  See {@link #assertCursorMatchesIteratorForAntiCompaction(ColumnFamilyStore, Set, RangesAtEndpoint, long, long)}.
     */
    protected AntiCompactionOutput assertCursorMatchesIteratorForAntiCompaction(ColumnFamilyStore cfs, RangesAtEndpoint ranges) throws Exception
    {
        long nowInSec = FBUtilities.nowInSeconds();
        return assertCursorMatchesIteratorForAntiCompaction(cfs, cfs.getLiveSSTables(), ranges, nowInSec, CompactionManager.getDefaultGcBefore(cfs, nowInSec));
    }

    /**
     * Anticompacts {@code inputs} as one group on the legacy path and then on the cursor path, from
     * identical sstables, and asserts that both give the same output sstables: bytes, logical
     * content, stats, repair metadata, first and last keys, and count; and that the cursor path
     * passes every byte of the inputs to the compaction rate limiter.  Live sstables outside {@code inputs} stay in place, so they can block purging.  The
     * live set is the same afterwards, under new descriptors, so a test can run this again.
     * Returns the legacy run.
     */
    protected AntiCompactionOutput assertCursorMatchesIteratorForAntiCompaction(ColumnFamilyStore cfs, Set<SSTableReader> inputs,
                                                                                RangesAtEndpoint ranges, long nowInSec, long gcBefore) throws Exception
    {
        Path scratch = Files.createTempDirectory("differential-anticompaction");
        TimeUUID pendingRepair = TimeUUID.Generator.nextTimeUUID();
        boolean passed = false;
        try
        {
            InputCopy copy = copyLiveSet(cfs, scratch.resolve("inputs"));
            Set<Descriptor> inputDescriptors = new HashSet<>();
            for (SSTableReader input : inputs)
                inputDescriptors.add(input.descriptor);

            AntiCompactionOutput iterator = antiCompactPath(cfs, live(cfs, inputDescriptors), ranges, pendingRepair, false, nowInSec, gcBefore, scratch.resolve("iterator"));
            // same descriptors, so the strategy groups the inputs in the same hash set order and
            // both paths merge the same scanners in the same order
            Map<Descriptor, Descriptor> restored = restoreLiveSet(cfs, copy, true);
            AntiCompactionOutput cursor = antiCompactPath(cfs, live(cfs, renamed(inputDescriptors, restored)), ranges, pendingRepair, true, nowInSec, gcBefore, scratch.resolve("cursor"));
            restoreLiveSet(cfs, copy);

            assertEquals("antiCompactGroup returned a different count on the two paths", iterator.returned, cursor.returned);
            assertEquals("the repair metadata of the outputs differs between paths", iterator.repairMetadata, cursor.repairMetadata);
            assertEquivalentOutputs(iterator.captured, cursor.captured);
            assertSameThrottling(iterator, cursor);
            iterator.cursorRun = cursor;
            passed = true;
            return iterator;
        }
        finally
        {
            deleteScratch(scratch, passed);
        }
    }

    /**
     * The calls that one operation on the test thread made to the compaction rate limiter.  Calls
     * from other threads, such as background compactions, are not counted.
     */
    public static final class RateLimiterCalls implements CompactionManager.RateLimiterListener
    {
        private final Thread thread = Thread.currentThread();
        /** The scanned bytes passed to the rate limiter. */
        long scannedBytes;
        /** The bytes acquired from the rate limiter. */
        long permits;
        long calls;

        @Override
        public void acquired(long scannedBytes, long permits)
        {
            if (Thread.currentThread() != thread)
                return;
            this.scannedBytes += scannedBytes;
            this.permits += permits;
            calls++;
        }
    }

    /**
     * The cursor path passes to the rate limiter exactly the bytes that full-range scanners of the
     * inputs read.  The legacy path makes no call after its last written partition, so it can pass
     * fewer, never more.
     */
    private static void assertSameThrottling(AntiCompactionOutput iterator, AntiCompactionOutput cursor)
    {
        assertEquals("the cursor path did not pass every scanned byte to the compaction rate limiter",
                     cursor.inputDataBytes, cursor.throttled.scannedBytes);
        assertTrue("the legacy path passed more bytes to the rate limiter than its scanners read: " + iterator.throttled.scannedBytes,
                   iterator.throttled.scannedBytes <= iterator.inputDataBytes);
    }

    /** The live sstables with these descriptors; fails if one is not live. */
    private static Set<SSTableReader> live(ColumnFamilyStore cfs, Set<Descriptor> descriptors)
    {
        Set<SSTableReader> found = new HashSet<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (descriptors.contains(sstable.descriptor))
                found.add(sstable);
        assertEquals("an input is not live", descriptors.size(), found.size());
        return found;
    }

    private static Set<Descriptor> renamed(Set<Descriptor> descriptors, Map<Descriptor, Descriptor> restored)
    {
        Set<Descriptor> renamed = new HashSet<>();
        for (Descriptor descriptor : descriptors)
            renamed.add(restored.get(descriptor));
        return renamed;
    }

    /** Runs one anticompaction path over {@code inputs}, checks what it left, and captures its outputs. */
    private AntiCompactionOutput antiCompactPath(ColumnFamilyStore cfs, Set<SSTableReader> inputs, RangesAtEndpoint ranges, TimeUUID pendingRepair,
                                                 boolean cursor, long nowInSec, long gcBefore, Path scratch) throws Exception
    {
        Set<Descriptor> liveBefore = new HashSet<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            liveBefore.add(sstable.descriptor);
        long maxDataAge = -1;
        long inputBytes = 0;
        for (SSTableReader input : inputs)
        {
            maxDataAge = Math.max(maxDataAge, input.maxDataAge);
            inputBytes += input.bytesOnDisk();
        }
        long liveDiskSpaceBefore = cfs.metric.liveDiskSpaceUsed.getCount();

        AntiCompactionOutput result = new AntiCompactionOutput();
        // As ScannerList.getCompressionRatio computes it for full-range scanners.
        double compressedBytes = 0, uncompressedBytes = 0;
        for (SSTableReader input : inputs)
        {
            compressedBytes += input.onDiskLength();
            uncompressedBytes += input.uncompressedLength();
        }
        result.compressionRatio = compressedBytes == uncompressedBytes || uncompressedBytes == 0 ? 1.0 : compressedBytes / uncompressedBytes;
        result.inputDataBytes = (long) uncompressedBytes;
        AntiCompactionRun run;
        CompactionManager.rateLimiterListener = result.throttled;
        RetainedHeap heap = watchHeapDuringAntiCompaction();
        try
        {
            run = antiCompactGroup(cfs, inputs, ranges, pendingRepair, cursor, () -> false, nowInSec, gcBefore);
        }
        finally
        {
            heap.stop();
            CompactionManager.rateLimiterListener = null;
        }
        result.retainedHeapGrowth = heap.growth;
        result.returned = run.returned;

        List<SSTableReader> outputs = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (!liveBefore.contains(sstable.descriptor))
                outputs.add(sstable);
        outputs.sort(Comparator.comparing(SSTableReader::getFirst));
        assertEquals("antiCompactGroup returned a count that is not the number of new live sstables", result.returned, outputs.size());
        assertAntiCompactionLeftNoTrace(cfs, inputs, outputs);
        long outputBytes = 0;
        for (SSTableReader out : outputs)
            outputBytes += out.bytesOnDisk();
        assertEquals("live disk space used did not move from the inputs to the outputs",
                     outputBytes - inputBytes, cfs.metric.liveDiskSpaceUsed.getCount() - liveDiskSpaceBefore);

        long outputPartitions = 0;
        int seq = 0;
        for (SSTableReader out : outputs)
        {
            assertEquals("maxDataAge of " + out, maxDataAge, out.maxDataAge);
            outputPartitions += assertOutputMatchesItsRanges(out, ranges, pendingRepair);
            result.repairMetadata.add("pendingRepair=" + out.getPendingRepair() + " repairedAt=" + out.getRepairedAt() +
                                      " transient=" + out.isTransient() + " first=" + out.getFirst() + " last=" + out.getLast());
            result.captured.sstables.add(capture(cfs, out, scratch.resolve("sstable-" + seq++)));
        }
        assertEquals(cursor ? "the cursor path did not write every output partition" : "the cursor path wrote partitions on the legacy run",
                     cursor ? outputPartitions : 0, run.cursorPartitionsWritten);
        // One call for each written partition, and on the cursor path one more for the bytes read after the last one.
        if (cursor)
            assertTrue("the cursor path made " + result.throttled.calls + " rate limiter calls for " + outputPartitions + " partitions",
                       result.throttled.calls == outputPartitions || result.throttled.calls == outputPartitions + 1);
        else
            assertEquals("legacy rate limiter calls", outputPartitions, result.throttled.calls);
        return result;
    }

    /**
     * After a successful anticompaction: every output is live with one reference, nothing is
     * compacting, no anticompaction is still registered, the inputs' files are gone, and the files
     * on disk match the live set.
     */
    protected static void assertAntiCompactionLeftNoTrace(ColumnFamilyStore cfs, Set<SSTableReader> inputs, List<SSTableReader> outputs)
    {
        LifecycleTransaction.waitForDeletions();
        for (SSTableReader out : outputs)
        {
            assertFalse("an output is marked compacted: " + out, out.isMarkedCompacted());
            assertEquals("references to " + out, 1, out.selfRef().globalCount());
        }
        assertTrue("sstables are still marked compacting: " + cfs.getTracker().getCompacting(), cfs.getTracker().getCompacting().isEmpty());
        AntiCompactionAssertions.assertNoAntiCompactionRegistered();
        for (SSTableReader input : inputs)
            assertFalse("an input was not deleted: " + input, input.descriptor.fileFor(SSTableFormat.Components.DATA).exists());
        org.apache.cassandra.Util.assertOnDiskState(cfs, cfs.getLiveSSTables().size());
        AntiCompactionAssertions.assertEveryFileBelongsToALiveSSTable(cfs);
    }

    /** What one call of {@link #antiCompactGroup} returned and wrote. */
    protected static final class AntiCompactionRun
    {
        public final int returned;
        /** Partitions the cursor path wrote; 0 on the legacy path. */
        public final long cursorPartitionsWritten;

        AntiCompactionRun(int returned, long cursorPartitionsWritten)
        {
            this.returned = returned;
            this.cursorPartitionsWritten = cursorPartitionsWritten;
        }
    }

    /**
     * Anticompacts {@code inputs} in one group, on the path asked for, and checks with the counters
     * that this path ran and did not fall back.  The inputs are replaced by the outputs.
     */
    protected static AntiCompactionRun antiCompactGroup(ColumnFamilyStore cfs, Set<SSTableReader> inputs, RangesAtEndpoint ranges, TimeUUID pendingRepair,
                                                        boolean cursor, BooleanSupplier isCancelled, long nowInSec, long gcBefore)
    {
        DatabaseDescriptor.setCursorRepairEnabled(cursor);
        assertFalse("scenario produced no input sstables", inputs.isEmpty());
        long runsBefore = CompactionManager.cursorAnticompactionsRun.sum();
        long fallbacksBefore = CompactionManager.cursorAnticompactionFallbacks.sum();
        long writtenBefore = CompactionManager.cursorAnticompactionPartitionsWritten.sum();
        int returned;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.ANTICOMPACTION))
        {
            assertNotNull("unable to mark inputs compacting", txn);
            returned = CompactionManager.instance.antiCompactGroup(cfs, ranges, txn, pendingRepair, isCancelled, nowInSec, gcBefore);
        }
        assertEquals("cursor anticompaction fell back to the legacy path", 0, CompactionManager.cursorAnticompactionFallbacks.sum() - fallbacksBefore);
        assertEquals((cursor ? "the cursor path did not run" : "the cursor path ran with cursor_repair_enabled off") +
                     "; this run would not compare the two paths",
                     cursor ? 1 : 0, CompactionManager.cursorAnticompactionsRun.sum() - runsBefore);
        return new AntiCompactionRun(returned, CompactionManager.cursorAnticompactionPartitionsWritten.sum() - writtenBefore);
    }

    /** {@link #antiCompactGroup} over the whole live set at the wall clock. */
    protected static int antiCompactLiveSet(ColumnFamilyStore cfs, RangesAtEndpoint ranges, TimeUUID pendingRepair,
                                            boolean cursor, BooleanSupplier isCancelled)
    {
        long nowInSec = FBUtilities.nowInSeconds();
        return antiCompactGroup(cfs, cfs.getLiveSSTables(), ranges, pendingRepair, cursor, isCancelled, nowInSec, CompactionManager.getDefaultGcBefore(cfs, nowInSec)).returned;
    }

    /**
     * Checks, without the other path, that every partition of {@code out} belongs to the output it is in.
     *
     * @return the number of partitions in {@code out}
     */
    private static long assertOutputMatchesItsRanges(SSTableReader out, RangesAtEndpoint ranges, TimeUUID pendingRepair)
    {
        assertFalse("anticompaction output is marked repaired: " + out, out.isRepaired());
        assertEquals("pending repair of " + out, out.isPendingRepair() ? pendingRepair : null, out.getPendingRepair());
        assertFalse("an unrepaired output is marked transient: " + out, !out.isPendingRepair() && out.isTransient());
        long partitions = 0;
        try (ISSTableScanner scanner = out.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    partitions++;
                    Token token = partition.partitionKey().getToken();
                    boolean full = ranges.onlyFull().ranges().stream().anyMatch(r -> r.contains(token));
                    boolean trans = !full && ranges.onlyTransient().ranges().stream().anyMatch(r -> r.contains(token));
                    String where = partition.partitionKey() + " in " + out;
                    if (!out.isPendingRepair())
                        assertTrue("a key in a repaired range went to the unrepaired output: " + where, !full && !trans);
                    else if (out.isTransient())
                        assertTrue("a key outside the transient ranges went to the transient output: " + where, trans);
                    else
                        assertTrue("a key outside the full ranges went to the full output: " + where, full);
                }
            }
        }
        return partitions;
    }

    /** The unrepaired live sstables that intersect the session's ranges, as the repair prepare step picks them. */
    protected static Set<SSTableReader> sessionSSTables(ColumnFamilyStore cfs, RangesAtEndpoint ranges)
    {
        Set<SSTableReader> picked = new HashSet<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (!sstable.isRepaired() && !sstable.isPendingRepair() && sstable.intersects(ranges.ranges()))
                picked.add(sstable);
        return picked;
    }

    /**
     * Runs {@code performAnticompaction} for an incremental repair session over {@code ranges}, as
     * the repair prepare step does, on the path asked for, and checks with the counters that the
     * cursor path never fell back and ran once for each of {@code expectedGroups} groups.  A
     * negative {@code expectedGroups} skips the count check.
     */
    protected static void runAntiCompactionSession(ColumnFamilyStore cfs, RangesAtEndpoint ranges, TimeUUID session,
                                                   boolean cursor, int expectedGroups) throws Exception
    {
        DatabaseDescriptor.setCursorRepairEnabled(cursor);
        ActiveRepairService.instance().registerParentRepairSession(session, FBUtilities.getBroadcastAddressAndPort(), List.of(cfs),
                                                                   ImmutableSet.copyOf(ranges.ranges()), true, ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                   true, PreviewKind.NONE);
        long runsBefore = CompactionManager.cursorAnticompactionsRun.sum();
        long fallbacksBefore = CompactionManager.cursorAnticompactionFallbacks.sum();
        try
        {
            Set<SSTableReader> sstables = sessionSSTables(cfs, ranges);
            LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
            assertNotNull("unable to mark the session's sstables compacting", txn);
            CompactionManager.instance.performAnticompaction(cfs, ranges, Refs.ref(sstables), txn, session, () -> false);
        }
        finally
        {
            ActiveRepairService.instance().removeParentRepairSession(session);
        }
        long runs = CompactionManager.cursorAnticompactionsRun.sum() - runsBefore;
        assertEquals("cursor anticompaction fell back", 0, CompactionManager.cursorAnticompactionFallbacks.sum() - fallbacksBefore);
        if (!cursor)
            assertEquals("the cursor path ran with it off", 0, runs);
        else if (expectedGroups >= 0)
            assertEquals("the cursor path must run once for each group", expectedGroups, runs);
        assertTrue("sstables are still marked compacting", cfs.getTracker().getCompacting().isEmpty());
        AntiCompactionAssertions.assertNoAntiCompactionRegistered();
        org.apache.cassandra.Util.assertOnDiskState(cfs, cfs.getLiveSSTables().size());
        AntiCompactionAssertions.assertEveryFileBelongsToALiveSSTable(cfs);
    }

    /**
     * Captures every live sstable, in an order set by its key bounds, repair metadata and content,
     * so the sstables of two runs pair up even when several share their key bounds.
     */
    protected CapturedOutput captureLiveSet(ColumnFamilyStore cfs, Path scratch) throws IOException
    {
        List<String> order = new ArrayList<>();
        List<CapturedSSTable> unordered = new ArrayList<>();
        int seq = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            CapturedSSTable c = capture(cfs, sstable, scratch.resolve("sstable-" + seq++));
            unordered.add(c);
            order.add(sstable.getFirst() + "|" + sstable.getLast() + "|" + sstable.getPendingRepair() + "|" + sstable.isTransient() + "|" +
                      sstable.getRepairedAt() + "|" + c.statsSummary + "|" + c.json);
        }
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < unordered.size(); i++)
            indexes.add(i);
        indexes.sort(Comparator.comparing(order::get));
        CapturedOutput captured = new CapturedOutput();
        for (int i : indexes)
            captured.sstables.add(unordered.get(i));
        return captured;
    }

    /** See {@link InputCopy#copyLiveSet}. */
    protected static InputCopy copyLiveSet(ColumnFamilyStore cfs, Path dir) throws IOException
    {
        return InputCopy.copyLiveSet(cfs, dir);
    }

    /** See {@link InputCopy#restoreLiveSet(ColumnFamilyStore, InputCopy, boolean)}. */
    protected static Map<Descriptor, Descriptor> restoreLiveSet(ColumnFamilyStore cfs, InputCopy copy) throws IOException
    {
        return InputCopy.restoreLiveSet(cfs, copy, false);
    }

    /** See {@link InputCopy#restoreLiveSet(ColumnFamilyStore, InputCopy, boolean)}. */
    protected static Map<Descriptor, Descriptor> restoreLiveSet(ColumnFamilyStore cfs, InputCopy copy, boolean sameDescriptors) throws IOException
    {
        return InputCopy.restoreLiveSet(cfs, copy, sameDescriptors);
    }

    /**
     * Runs the differential at two generations: once over the flushed inputs (gen 1), then again over
     * the cursor-produced outputs (gen 2). Returns the gen-1 iterator capture for structural assertions.
     */
    protected CapturedOutput assertCursorMatchesIteratorAcrossGenerations(ColumnFamilyStore cfs) throws Exception
    {
        return assertCursorMatchesIteratorAcrossGenerations(cfs, FBUtilities::nowInSeconds);
    }

    /** As above, but uses nowInSecondsSupplier as the TTL-expiry nowInSec for both generations. */
    protected CapturedOutput assertCursorMatchesIteratorAcrossGenerations(ColumnFamilyStore cfs,
                                                                          LongSupplier nowInSecondsSupplier) throws Exception
    {
        TaskFactory taskFactory = taskWithNow(nowInSecondsSupplier);
        CapturedOutput gen1 = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskFactory);

        long gcBefore = CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds());
        commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore, nowInSecondsSupplier);
        if (cfs.getLiveSSTables().isEmpty())
            return gen1; // gen 1 purged everything; there are no gen-2 inputs

        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskFactory);
        return gen1;
    }

    /**
     * Commits one compaction over the whole live set without restore: the live set becomes the outputs.
     * The factory must build its writer with keepOriginals false.
     */
    protected void commitThroughFactory(ColumnFamilyStore cfs, boolean cursor, TaskFactory taskFactory) throws Exception
    {
        commitThroughFactory(cfs, cursor, taskFactory, ActiveCompactionsTracker.NOOP);
    }

    /** As above, with a tracker of the caller's choosing. */
    protected void commitThroughFactory(ColumnFamilyStore cfs, boolean cursor, TaskFactory taskFactory,
                                        ActiveCompactionsTracker tracker) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        long gcBefore = CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds());
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        assertFalse("scenario produced no input sstables", inputs.isEmpty());
        if (cursor)
            assertCursorPathWillRun(cfs, inputs, gcBefore);
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting for commit", txn);
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        taskFactory.create(cfs, txn, gcBefore).execute(tracker);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);
    }

    /** Commits one compaction over the given inputs without restore: the live set becomes the outputs. */
    protected void commitCompaction(ColumnFamilyStore cfs, Set<SSTableReader> inputs, boolean cursor, long gcBefore) throws Exception
    {
        commitCompaction(cfs, inputs, cursor, gcBefore, FBUtilities::nowInSeconds);
    }

    /** As above, but uses nowInSecondsSupplier as the TTL-expiry nowInSec. */
    protected void commitCompaction(ColumnFamilyStore cfs, Set<SSTableReader> inputs, boolean cursor, long gcBefore,
                                    LongSupplier nowInSecondsSupplier) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        if (cursor)
            assertCursorPathWillRun(cfs, inputs, gcBefore);
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting for commit", txn);
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        new CompactionTask(cfs, txn, gcBefore, false).setNowInSecondsSupplier(nowInSecondsSupplier).execute(ActiveCompactionsTracker.NOOP);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);
    }

    /**
     * Runs one compaction path over the given input subset, captures the outputs, and restores the
     * live set so the other path sees identical bytes.
     */
    protected CapturedOutput compactPath(ColumnFamilyStore cfs,
                                         Set<SSTableReader> inputs,
                                         boolean cursor,
                                         long gcBefore,
                                         Path scratch,
                                         TaskFactory taskFactory) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);

        assertFalse("scenario produced no input sstables", inputs.isEmpty());
        Set<Descriptor> liveBeforeDescs = new HashSet<>();
        int liveBeforeCount = 0;
        for (SSTableReader live : cfs.getLiveSSTables())
        {
            liveBeforeDescs.add(live.descriptor);
            liveBeforeCount++;
        }
        List<Descriptor> inputDescriptors = new ArrayList<>();
        for (SSTableReader in : inputs)
        {
            assertTrue("input is not live", liveBeforeDescs.contains(in.descriptor));
            inputDescriptors.add(in.descriptor);
        }
        Set<Descriptor> inputDescs = new HashSet<>(inputDescriptors);

        if (cursor)
            assertCursorPathWillRun(cfs, inputs, gcBefore);

        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting", txn);
        // assert the requested pipeline actually ran, so a silent fallback cannot compare a path with itself
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        taskFactory.create(cfs, txn, gcBefore).execute(ActiveCompactionsTracker.NOOP);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);

        // outputs are identified by descriptor diff against the pre-compaction live set
        List<SSTableReader> retainedInputClones = new ArrayList<>();
        List<SSTableReader> outputs = identifyOutputs(cfs, liveBeforeDescs, inputDescs, retainedInputClones);

        CapturedOutput captured = new CapturedOutput();
        int seq = 0;
        for (SSTableReader out : outputs)
            captured.sstables.add(capture(cfs, out, scratch.resolve("sstable-" + seq++)));

        restoreAfterCompaction(cfs, outputs, retainedInputClones, inputDescriptors, liveBeforeCount);

        return captured;
    }

    /**
     * Delists and releases outputs and retained input clones, deletes output files, then reopens every
     * input fresh from its descriptor so a subsequent run sees identical pristine readers.
     */
    protected void restoreAfterCompaction(ColumnFamilyStore cfs,
                                          List<SSTableReader> outputs,
                                          List<SSTableReader> retainedInputClones,
                                          List<Descriptor> inputDescriptors,
                                          int liveBeforeCount) throws Exception
    {
        List<Path> outputFiles = new ArrayList<>();
        for (SSTableReader out : outputs)
            for (Component c : out.descriptor.discoverComponents())
                outputFiles.add(out.descriptor.fileFor(c).toPath());

        Set<SSTableReader> toRemove = new HashSet<>(outputs);
        toRemove.addAll(retainedInputClones);
        cfs.getTracker().removeUnsafe(toRemove);
        for (SSTableReader reader : toRemove)
            reader.selfRef().release();
        for (Path f : outputFiles)
            Files.deleteIfExists(f);

        List<SSTableReader> reopened = new ArrayList<>();
        for (Descriptor desc : inputDescriptors)
        {
            if (!desc.fileFor(org.apache.cassandra.io.sstable.format.SSTableFormat.Components.DATA).exists())
                fail("input sstable lost during compaction (keepOriginals violated?): " + desc +
                     "\ndata dir contents:\n" + listDataDir(desc));
            reopened.add(SSTableReader.open(cfs, desc));
        }
        cfs.getTracker().addInitialSSTables(reopened);
        // 5.0's compaction strategy ignores the notification addInitialSSTables sends, so without this LCS
        // would not know the restored sstables' levels
        cfs.getCompactionStrategyManager().handleNotification(new SSTableAddedNotification(reopened, null), cfs.getTracker());
        assertEquals("restore failed: live sstable count", liveBeforeCount, cfs.getLiveSSTables().size());
    }

    /** Identifies outputs by before/after descriptor diff. */
    protected static List<SSTableReader> identifyOutputs(ColumnFamilyStore cfs,
                                                         Set<Descriptor> liveBeforeDescs,
                                                         Set<Descriptor> inputDescs,
                                                         List<SSTableReader> retainedInputClonesOut)
    {
        List<SSTableReader> outputs = new ArrayList<>();
        for (SSTableReader reader : cfs.getLiveSSTables())
        {
            if (!liveBeforeDescs.contains(reader.descriptor))
                outputs.add(reader);
            else if (inputDescs.contains(reader.descriptor))
                retainedInputClonesOut.add(reader);
        }
        outputs.sort(Comparator.comparing(SSTableReader::getFirst));
        return outputs;
    }

    /** Asserts the cursor path would actually run for this scenario, so it cannot compare iterator vs iterator. */
    protected void assertCursorPathWillRun(ColumnFamilyStore cfs, Set<SSTableReader> inputs, long gcBefore) throws Exception
    {
        // skip an unsupported format rather than fail the assertion below
        Assume.assumeTrue("cursor compaction cannot write the selected sstable format; selected=" +
                          DatabaseDescriptor.getSelectedSSTableFormat().name(),
                          DatabaseDescriptor.getSelectedSSTableFormat().cursorIndexWriterFactory() != null);
        try (CompactionController controller = new CompactionController(cfs, inputs, gcBefore);
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            assertTrue("scenario is not supported by cursor compaction; this harness run would " +
                       "silently compare iterator vs iterator. If unsupported-ness is intended, " +
                       "assert it explicitly instead.",
                       CursorCompactor.isSupported(scanners, controller));
        }
    }

    /** Skips a scenario unless the selected sstable format supports cursor compaction. */
    protected static void assumeCursorSupportedFormatSelected()
    {
        Assume.assumeTrue("cursor compaction does not support the selected sstable format; selected=" +
                          DatabaseDescriptor.getSelectedSSTableFormat().name(),
                          DatabaseDescriptor.getSelectedSSTableFormat().cursorIndexWriterFactory() != null);
    }

    private static String listDataDir(Descriptor desc)
    {
        try (java.util.stream.Stream<Path> files = Files.list(desc.directory.toPath()))
        {
            StringBuilder sb = new StringBuilder();
            files.sorted().forEach(p -> sb.append("  ").append(p.getFileName()).append('\n'));
            return sb.toString();
        }
        catch (IOException e)
        {
            return "  <failed to list: " + e + ">";
        }
    }

    /** Asserts this compaction output was written in the sstable format the JVM currently has selected. */
    protected static void assertOutputFormatIsSelected(SSTableReader sstable)
    {
        SSTableFormat<?, ?> selected = DatabaseDescriptor.getSelectedSSTableFormat();
        SSTableFormat<?, ?> written = sstable.descriptor.getFormat();
        assertEquals("compaction output " + sstable.descriptor + " was written in the '" + written.name() +
                     "' format while '" + selected.name() + "' is selected: this scenario is not testing the " +
                     "format it claims, and the byte comparison below would compare that other format's " +
                     "components and pass",
                     selected.name(), written.name());
    }

    /**
     * Opens a single-row slice for every row of every partition and asserts the row that comes back is
     * the one a plain sequential walk returns, so the index routes seeks correctly.
     *
     * @return how many partitions carried a row index
     */
    protected int assertEveryRowReadableThroughASlice(SSTableReader sstable)
    {
        TableMetadata metadata = sstable.metadata();
        // no clustering columns: one row per partition, never indexable, no seek to route
        if (metadata.comparator.size() == 0 || SLICE_READBACK_MAX_ROWS_PER_PARTITION <= 0)
            return 0;

        ColumnFilter fetchAll = ColumnFilter.all(metadata);
        // cells are only comparable when the probe's filter fetches exactly what the sequential read deserializes
        boolean cellsComparable = sstable.header.columns().equals(metadata.regularAndStaticColumns());
        int headCap = (SLICE_READBACK_MAX_ROWS_PER_PARTITION + 1) / 2;
        int tailCap = SLICE_READBACK_MAX_ROWS_PER_PARTITION / 2;
        int granularity = DatabaseDescriptor.getColumnIndexSize(-1);
        int indexedPartitions = 0;

        PendingPartition pending = new PendingPartition();

        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                List<Row> probes = new ArrayList<>();
                int unfiltereds;
                DecoratedKey key;
                DeletionTime partitionDeletion;
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    key = partition.partitionKey();
                    partitionDeletion = partition.partitionLevelDeletion();
                    unfiltereds = collectProbeRows(partition, probes, headCap, tailCap);
                }

                AbstractRowIndexEntry entry = sstable.getRowIndexEntry(key, SSTableReader.Operator.EQ);
                if (entry == null)
                    throw new AssertionError("a partition the sequential walk returned has no index entry: " +
                                             key + " in " + sstable.descriptor);
                if (entry.blockCount() > 1)
                    indexedPartitions++;

                pending.advance(sstable, key, entry, unfiltereds, granularity);

                assertPartitionDeletionReadableFromIndexEntry(sstable, key, entry, partitionDeletion);
                assertEveryProbeReturnsExactly(sstable, metadata, fetchAll, key, probes, cellsComparable, reverseSliceReadback);
            }
        }
        pending.finish(sstable, granularity);
        return indexedPartitions;
    }

    /** Checks each partition's block-count bound one iteration late, once the next partition's start is known. */
    private static final class PendingPartition
    {
        private DecoratedKey key;
        private long position = -1;
        private int blockCount;
        private int unfiltereds;

        /** Bounds the partition held here against the next one's start, then holds the next one. */
        void advance(SSTableReader sstable, DecoratedKey nextKey, AbstractRowIndexEntry nextEntry,
                     int nextUnfiltereds, int granularity)
        {
            if (key != null && nextEntry.position > position)
                assertBlockCountWithinBounds(sstable, key, blockCount, unfiltereds,
                                             nextEntry.position - position, granularity);
            key = nextKey;
            position = nextEntry.position;
            blockCount = nextEntry.blockCount();
            unfiltereds = nextUnfiltereds;
        }

        /** The last partition's bound, measured against the end of the file. */
        void finish(SSTableReader sstable, int granularity)
        {
            if (key != null && sstable.uncompressedLength() > position)
                assertBlockCountWithinBounds(sstable, key, blockCount, unfiltereds,
                                             sstable.uncompressedLength() - position, granularity);
        }
    }

    /** Every probe row of one partition, seeked forward, and in reverse when {@code alsoReversed}. */
    private static void assertEveryProbeReturnsExactly(SSTableReader sstable, TableMetadata metadata,
                                                       ColumnFilter fetchAll, DecoratedKey key,
                                                       List<Row> probes, boolean cellsComparable,
                                                       boolean alsoReversed)
    {
        for (Row expected : probes)
        {
            assertSliceReturnsExactly(sstable, metadata, fetchAll, key, expected, false, cellsComparable);
            if (alsoReversed)
                assertSliceReturnsExactly(sstable, metadata, fetchAll, key, expected, true, cellsComparable);
        }
    }

    /**
     * Collects the rows one partition is probed with: the first {@code headCap}, then the last {@code tailCap}.
     *
     * @return how many unfiltereds the partition held, markers included
     */
    private static int collectProbeRows(UnfilteredRowIterator partition, List<Row> probes,
                                        int headCap, int tailCap)
    {
        ArrayDeque<Row> tail = new ArrayDeque<>();
        int unfiltereds = 0;
        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            unfiltereds++;
            if (!unfiltered.isRow())
                continue;
            Row row = (Row) unfiltered;
            if (probes.size() < headCap)
                probes.add(row);
            else if (tailCap > 0)
            {
                tail.addLast(row);
                if (tail.size() > tailCap)
                    tail.removeFirst();
            }
        }
        probes.addAll(tail);
        return unfiltereds;
    }

    /**
     * Asserts one single-row slice, in one direction, returns exactly one row and that it is
     * {@code expected}. Range tombstone markers are skipped.
     */
    private static void assertSliceReturnsExactly(SSTableReader sstable,
                                                  TableMetadata metadata,
                                                  ColumnFilter fetchAll,
                                                  DecoratedKey key,
                                                  Row expected,
                                                  boolean reversed,
                                                  boolean cellsComparable)
    {
        Slices slices = Slices.with(metadata.comparator, Slice.make(expected.clustering()));
        try (UnfilteredRowIterator probe = sstable.rowIterator(key, slices, fetchAll, reversed,
                                                               SSTableReadsListener.NOOP_LISTENER))
        {
            Row found = null;
            int rows = 0;
            while (probe.hasNext())
            {
                Unfiltered unfiltered = probe.next();
                if (!unfiltered.isRow())
                    continue;
                rows++;
                found = (Row) unfiltered;
            }

            if (rows == 1 && (cellsComparable ? expected.equals(found) : sameRowIdentity(expected, found)))
                return;

            String where = (reversed ? "reverse" : "forward") + " slice of " +
                           expected.clustering().toString(metadata) + " in partition " + key +
                           " of " + sstable.descriptor;
            if (rows != 1)
                fail("the index routed a " + where + " to " + rows + " rows; a single-clustering slice " +
                     "must return exactly one");
            fail("the index routed a " + where + " to the wrong row" +
                 (cellsComparable ? "" : " (this sstable's header carries columns the schema does not, " +
                                         "so only clustering, liveness and row deletion are compared)") +
                 "\n  sequential walk: " + expected.toString(metadata, true) +
                 "\n  slice returned:  " + found.toString(metadata, true));
        }
    }

    /** Clustering, primary key liveness and row deletion — everything a misrouted seek would change. */
    private static boolean sameRowIdentity(Row expected, Row found)
    {
        return expected.clustering().equals(found.clustering())
               && expected.primaryKeyLivenessInfo().equals(found.primaryKeyLivenessInfo())
               && expected.deletion().equals(found.deletion());
    }

    /** Asserts the partition-level deletion the index entry carries matches the one in the data file. */
    private static void assertPartitionDeletionReadableFromIndexEntry(SSTableReader sstable,
                                                                      DecoratedKey key,
                                                                      AbstractRowIndexEntry entry,
                                                                      DeletionTime fromDataFile)
    {
        if (entry.blockCount() <= 1)
            return;
        try (UnfilteredRowIterator probe = sstable.rowIterator(key, Slices.ALL, ColumnFilter.NONE, false,
                                                               SSTableReadsListener.NOOP_LISTENER))
        {
            assertEquals("the index entry's partition-level deletion differs from the data file's for " +
                         key + " in " + sstable.descriptor,
                         fromDataFile, probe.partitionLevelDeletion());
        }
    }

    /** Bounds {@code blockCount()} from above by what the partition's own block structure implies. */
    private static void assertBlockCountWithinBounds(SSTableReader sstable,
                                                     DecoratedKey key,
                                                     int blockCount,
                                                     int unfiltereds,
                                                     long partitionLength,
                                                     int granularity)
    {
        // granularity is -1 when column_index_size is unset, in which case the length bound cannot be stated
        boolean lengthBoundHolds = granularity <= 0 || blockCount < 2
                                   || partitionLength >= (long) (blockCount - 1) * granularity;
        if (blockCount != 1 && blockCount <= unfiltereds && lengthBoundHolds)
            return;

        String where = " for partition " + key + " in " + sstable.descriptor;
        assertFalse("a row index of exactly one block cannot be written: the writer drops it" + where,
                    blockCount == 1);
        assertTrue("row index claims " + blockCount + " blocks but the partition holds only " + unfiltereds +
                   " unfiltereds, and every block begins at its own unfiltered" + where,
                   blockCount <= unfiltereds);
        assertTrue("row index claims " + blockCount + " blocks, so at least " + (blockCount - 1) +
                   " of them were cut at the " + granularity + "-byte column_index_size, but the " +
                   "partition is only " + partitionLength + " bytes long" + where,
                   lengthBoundHolds);
    }

    protected CapturedSSTable capture(ColumnFamilyStore cfs, SSTableReader sstable, Path dir) throws IOException
    {
        // 1. copy components first: verification or the dump can fail and roll back the
        // transaction, deleting the live files; the copies are the only evidence left for
        // offline decoding
        Files.createDirectories(dir);
        SortedMap<String, Long> copiedSizes = new TreeMap<>();
        for (Component c : sstable.descriptor.discoverComponents())
        {
            Path source = sstable.descriptor.fileFor(c).toPath();
            Path target = dir.resolve(c.name());
            Files.copy(source, target);
            copiedSizes.put(c.name(), Files.size(target));
        }

        // 2. the output really is in the format this scenario selected
        assertOutputFormatIsSelected(sstable);

        // 2. structural verification of the output; the debug stream is silenced to avoid OOMing the fork
        OutputHandler verifyOutput = new OutputHandler.LogOutput() { @Override public void debug(String msg) {} };
        try (IVerifier verifier = sstable.getVerifier(cfs, verifyOutput, false,
                                                      IVerifier.options().invokeDiskFailurePolicy(true)
                                                                         .extendedVerification(true).build()))
        {
            verifier.verify();
        }

        // 3. every row is retrievable through a real slice, i.e. the index routes seeks correctly; skipped in scale mode
        if (SLICE_READBACK && !scaleCapture())
            assertEveryRowReadableThroughASlice(sstable);

        // 4. canonical logical dump, with the wall-clock-derived "expired" flag normalized out
        String json;
        if (scaleCapture())
        {
            // stream into a digest: capture memory stays flat at millions of rows
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
                NormalizingDigestOutputStream out = new NormalizingDigestOutputStream(digest);
                JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, sstable.metadata(), out);
                out.flushTail();
                json = "sha256:" + org.apache.cassandra.utils.Hex.bytesToHex(digest.digest()) +
                       " (" + out.bytesSeen + " bytes)";
            }
            catch (java.security.NoSuchAlgorithmException e)
            {
                throw new AssertionError(e);
            }
        }
        else
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, sstable.metadata(), baos);
            }
            json = EXPIRED_FLAG.matcher(baos.toString(StandardCharsets.UTF_8))
                               .replaceAll("\"expired\":\"normalized\"");
        }

        // 6. stats spot-check summary; a format that does not store the token space coverage or the
        // partition level deletion marker (nb) reads them back as defaults, so a reader reopened from
        // disk differs from the one its writer opened; leave them out there
        StatsMetadata stats = sstable.getSSTableMetadata();
        Version version = sstable.descriptor.version;
        String statsSummary = "minTimestamp=" + stats.minTimestamp +
                              " maxTimestamp=" + stats.maxTimestamp +
                              " minLocalDeletionTime=" + stats.minLocalDeletionTime +
                              " maxLocalDeletionTime=" + stats.maxLocalDeletionTime +
                              " estimatedKeys=" + sstable.estimatedKeys() +
                              " totalRows=" + stats.totalRows +
                              " totalColumnsSet=" + stats.totalColumnsSet +
                              " encodingStats=" + sstable.header.stats() +
                              " metaEncodingStats=" + stats.encodingStats.minTimestamp + "/" + stats.encodingStats.minLocalDeletionTime + "/" + stats.encodingStats.minTTL +
                              " tombstoneHist=" + tombstoneHistogram(stats) +
                              " cellsPerPartition=" + stats.estimatedCellPerPartitionCount.mean() + "/" + stats.estimatedCellPerPartitionCount.count() +
                              " partitionSize=" + stats.estimatedPartitionSize.mean() + "/" + stats.estimatedPartitionSize.count() +
                              " sstableLevel=" + stats.sstableLevel +
                              " coveredClustering=" + stats.coveredClustering.toString(sstable.metadata().comparator) +
                              (version.hasTokenSpaceCoverage() ? " tokenSpaceCoverage=" + stats.tokenSpaceCoverage : "") +
                              " minTTL=" + stats.minTTL + " maxTTL=" + stats.maxTTL +
                              (version.hasPartitionLevelDeletionsPresenceMarker() ? " hasPartitionLevelDeletions=" + stats.hasPartitionLevelDeletions : "");

        CapturedSSTable captured = new CapturedSSTable(dir, json, statsSummary, stats.totalRows);
        captured.componentSizes.putAll(copiedSizes);
        return captured;
    }

    /** Renders the tombstone histogram's content, since TombstoneHistogram has no stable toString. */
    private static String tombstoneHistogram(StatsMetadata stats)
    {
        return "size=" + stats.estimatedTombstoneDropTime.size() +
               ",sum=" + stats.estimatedTombstoneDropTime.sum(Integer.MAX_VALUE);
    }

    protected void assertEquivalentOutputs(CapturedOutput iterator, CapturedOutput cursor)
    {
        assertEquals("output sstable count differs between paths", iterator.sstables.size(), cursor.sstables.size());
        for (int i = 0; i < iterator.sstables.size(); i++)
            assertEquivalentSSTable(i, iterator.sstables.get(i), cursor.sstables.get(i));
    }

    /** One output sstable of each path: logical dump, stats summary, then every component's bytes. */
    private void assertEquivalentSSTable(int i, CapturedSSTable it, CapturedSSTable cu)
    {
        // logical first: a row-level diff is more debuggable. In scale mode the dump is a digest, deferred below.
        boolean digestMode = it.json.startsWith("sha256:");
        if (!digestMode && !it.json.equals(cu.json))
            fail("LOGICAL divergence in output sstable " + i + " (iterator vs cursor):\n" + firstJsonDiff(it.json, cu.json) +
                 "\niterator stats: " + it.statsSummary + "\ncursor stats:   " + cu.statsSummary);

        assertEquals("stats summary divergence in output sstable " + i +
                     "\n  iterator: " + it.statsSummary + "\n  cursor:   " + cu.statsSummary,
                     it.statsSummary, cu.statsSummary);

        List<String> divergences = componentDivergences(it, cu);
        if (!divergences.isEmpty())
            fail("BYTE divergence in output sstable " + i + " (iterator vs cursor):\n" + String.join("\n", divergences) +
                 "\nNothing is allowed to diverge: every divergence found to date has been a bug in one of the paths");

        if (digestMode)
            assertEquals("logical dump digest divergence in output sstable " + i +
                         " (scale mode; rerun a reduced scenario without scale mode for a row-level diff)",
                         it.json, cu.json);
    }

    /** One description per component whose bytes differ, or that only one path wrote. */
    private static List<String> componentDivergences(CapturedSSTable it, CapturedSSTable cu)
    {
        SortedSet<String> components = new TreeSet<>();
        components.addAll(it.componentSizes.keySet());
        components.addAll(cu.componentSizes.keySet());
        List<String> divergences = new ArrayList<>();
        for (String comp : components)
        {
            Path a = it.dir.resolve(comp);
            Path b = cu.dir.resolve(comp);
            boolean hasA = Files.exists(a);
            boolean hasB = Files.exists(b);
            if (hasA != hasB)
            {
                divergences.add(String.format("  %s: present only in %s path", comp, hasA ? "iterator" : "cursor"));
                continue;
            }
            if (!hasA)
                continue;
            long firstDiff = firstFileDifference(a, b);
            if (firstDiff < 0)
                continue;
            divergences.add(describeFileDiff(comp, a, b, firstDiff));
        }
        return divergences;
    }

    /** Streaming comparison: -1 if byte-identical, else the offset of the first difference
     *  (the shorter length when one file is a prefix of the other). */
    private static long firstFileDifference(Path a, Path b)
    {
        try (java.io.InputStream ia = new java.io.BufferedInputStream(Files.newInputStream(a), 1 << 16);
             java.io.InputStream ib = new java.io.BufferedInputStream(Files.newInputStream(b), 1 << 16))
        {
            byte[] bufA = new byte[1 << 16];
            byte[] bufB = new byte[1 << 16];
            long offset = 0;
            while (true)
            {
                int readA = ia.readNBytes(bufA, 0, bufA.length);
                int readB = ib.readNBytes(bufB, 0, bufB.length);
                int common = Math.min(readA, readB);
                int mismatch = java.util.Arrays.mismatch(bufA, 0, common, bufB, 0, common);
                if (mismatch >= 0)
                    return offset + mismatch;
                if (readA != readB)
                    return offset + common; // same prefix, different length
                if (readA == 0)
                    return -1;
                offset += readA;
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static String describeFileDiff(String component, Path a, Path b, long firstDiff)
    {
        try
        {
            return String.format("  %s: lengths %d vs %d, first divergence at offset %d%n    iterator: %s%n    cursor:   %s",
                                 component, Files.size(a), Files.size(b), firstDiff,
                                 hexContext(a, firstDiff), hexContext(b, firstDiff));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static String hexContext(Path file, long offset) throws IOException
    {
        long size = Files.size(file);
        long from = Math.max(0, offset - 8);
        int len = (int) Math.min(size - from, 32);
        byte[] window = new byte[Math.max(len, 0)];
        try (java.io.InputStream in = Files.newInputStream(file))
        {
            ByteStreams.skipFully(in, from);
            in.readNBytes(window, 0, window.length);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < window.length; i++)
        {
            long abs = from + i;
            if (abs == offset)
                sb.append('[');
            sb.append(String.format("%02x", window[i]));
            if (abs == offset)
                sb.append(']');
            sb.append(' ');
        }
        if (from + window.length < size)
            sb.append("...");
        return sb.toString();
    }

    /**
     * Streams a JSON dump into a digest, normalizing the wall-clock-derived "expired" flag and flushing
     * oversized lines in bounded chunks so memory stays flat for multi-GB partitions.
     */
    private static final class NormalizingDigestOutputStream extends java.io.OutputStream
    {
        private static final int FLUSH_THRESHOLD = 8 << 20;
        private static final int TAIL_KEEP = 64; // > the longest normalized token

        private final java.security.MessageDigest digest;
        private final ByteArrayOutputStream line = new ByteArrayOutputStream();
        long bytesSeen;

        NormalizingDigestOutputStream(java.security.MessageDigest digest)
        {
            this.digest = digest;
        }

        @Override
        public void write(int b)
        {
            line.write(b);
            if (b == '\n')
                flushTail();
            else if (line.size() >= FLUSH_THRESHOLD)
                flushChunk();
        }

        @Override
        public void write(byte[] b, int off, int len)
        {
            for (int i = off; i < off + len; i++)
                write(b[i]);
        }

        /** Digests all buffered content, at the end of a line or of the stream. */
        void flushTail()
        {
            if (line.size() == 0)
                return;
            update(line.toByteArray(), line.size());
            line.reset();
        }

        /** Digests all but the last TAIL_KEEP buffered bytes, so a token incomplete at the buffer end survives. */
        private void flushChunk()
        {
            byte[] buffered = line.toByteArray();
            int processed = buffered.length - TAIL_KEEP;
            update(buffered, processed);
            line.reset();
            line.write(buffered, processed, TAIL_KEEP);
        }

        private void update(byte[] bytes, int length)
        {
            byte[] normalized = EXPIRED_FLAG.matcher(new String(bytes, 0, length, StandardCharsets.UTF_8))
                                            .replaceAll("\"expired\":\"normalized\"")
                                            .getBytes(StandardCharsets.UTF_8);
            digest.update(normalized);
            bytesSeen += normalized.length;
        }
    }

    private static String firstJsonDiff(String a, String b)
    {
        String[] linesA = a.split("\n", -1);
        String[] linesB = b.split("\n", -1);
        int max = Math.max(linesA.length, linesB.length);
        for (int i = 0; i < max; i++)
        {
            if (!lineAt(linesA, i).equals(lineAt(linesB, i)))
                return renderDiffContext(linesA, linesB, i, max);
        }
        return "(no line diff found despite string inequality — check line endings)";
    }

    /** Line {@code i} of one dump, or a placeholder where that dump is the shorter one. */
    private static String lineAt(String[] lines, int i)
    {
        return i < lines.length ? lines[i] : "<missing>";
    }

    /** The differing line marked, with two lines either side, both dumps interleaved. */
    private static String renderDiffContext(String[] linesA, String[] linesB, int i, int max)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("first differing line ").append(i + 1).append(" of ").append(max).append(":\n");
        for (int j = Math.max(0, i - 2); j < Math.min(max, i + 3); j++)
        {
            sb.append(j == i ? ">>" : "  ").append(" iterator: ").append(lineAt(linesA, j)).append('\n');
            sb.append(j == i ? ">>" : "  ").append(" cursor:   ").append(lineAt(linesB, j)).append('\n');
        }
        return sb.toString();
    }
}
