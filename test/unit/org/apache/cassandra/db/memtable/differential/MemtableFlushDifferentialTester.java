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

package org.apache.cassandra.db.memtable.differential;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.FlushPipelineCounts;
import org.apache.cassandra.db.partitions.MemtableCursorFlusher;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.DifferentialTestUtils;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Differential test harness for cursor-based vs iterator-based memtable flush.
 * <p>
 * A live memtable can only be flushed once, so unlike compaction's harness there is no reusable
 * input to rerun.  Instead the same deterministic sequence of CQL statements is applied to two
 * identically-schemaed tables, one flushed via the iterator path and the other via the cursor
 * path (toggling {@link DatabaseDescriptor#setCursorFlushEnabled}), and the resulting sstables
 * are compared byte-for-byte and via a canonical JSON dump.
 * <p>
 * The byte and JSON diff primitives are shared with the compaction harness via
 * {@link DifferentialTestUtils}.
 */
public abstract class MemtableFlushDifferentialTester extends CQLTester
{
    private static final long DUMP_NOW_SEC = 0;

    /**
     * The cursor flush path accepts only a heap-based memtable allocator, so the suite pins one;
     * otherwise the ambient config allocator ({@code offheap_objects} under {@code ant
     * test-latest}) would make every scenario decline the cursor path.
     */
    private Config.MemtableAllocationType originalAllocationType;

    @Before
    public void pinHeapAllocation()
    {
        originalAllocationType = DatabaseDescriptor.getMemtableAllocationType();
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = Config.MemtableAllocationType.heap_buffers;
    }

    @After
    public void restoreAllocation()
    {
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = originalAllocationType;
    }

    /** Matches a {@code memtable = ...} table option already present in a CREATE TABLE statement. */
    private static final java.util.regex.Pattern MEMTABLE_OPTION =
        java.util.regex.Pattern.compile("(?i)\\bmemtable\\s*=");

    /**
     * Pins the SkipList memtable on a scenario's {@code CREATE TABLE}, unless it already names a
     * memtable.  The cursor flush path accepts only {@code SkipListMemtable} and
     * {@code ShardedSkipListMemtable}, and a per-table CQL option is the only way to pin it, since
     * the default is frozen at class load.
     * <p>
     * The option is joined with {@code AND} when the statement already has a {@code WITH} clause,
     * else with {@code WITH}.  A trailing {@code ;} is stripped first, since Harry's generated DDL
     * ends with one and appending past it would be invalid CQL.
     */
    protected static String withSkipListMemtable(String tableCql)
    {
        if (MEMTABLE_OPTION.matcher(tableCql).find())
            return tableCql;
        String statement = tableCql.replaceFirst(";\\s*$", "");
        String joiner = java.util.regex.Pattern.compile("(?i)\\bWITH\\b").matcher(statement).find() ? " AND " : " WITH ";
        return statement + joiner + "memtable = 'skiplist'";
    }

    // A non-frozen list column's cell path is a client-generated TimeUUID assigned before either
    // flush path sees it, so it never matches between the two tables.  Both paths treat it as an
    // opaque cell path, so normalizing it out loses no signal about a flush bug.
    private static final java.util.regex.Pattern LIST_CELL_PATH =
        java.util.regex.Pattern.compile("\"path\":\\[\"[0-9a-f-]{36}\"\\]");

    /** Any tstamp field (row liveness_info or per-cell) — see {@link #assertFlushMatchesLogically}. */
    private static final java.util.regex.Pattern TSTAMP_FIELD =
        java.util.regex.Pattern.compile("\"tstamp\":\"-?\\d+\"");

    /** Raw on-disk byte offset — shifts whenever any earlier value's encoded length differs. */
    private static final java.util.regex.Pattern POSITION_FIELD =
        java.util.regex.Pattern.compile("\"position\":\\d+");

    // A deletion's local_delete_time and a TTL's expires_at are wall-clock seconds, not set by
    // USING TIMESTAMP, so a long scenario can straddle a second between the two populate() calls
    // and differ by one second with no signal about a flush bug.  See assertFlushMatchesLogically.
    private static final java.util.regex.Pattern WALL_CLOCK_DELETION_FIELD =
        java.util.regex.Pattern.compile("\"(local_delete_time|expires_at)\":\"\\d+\"");

    private static String normalize(String json)
    {
        return LIST_CELL_PATH.matcher(json).replaceAll("\"path\":\"normalized\"");
    }

    public static final class CapturedSSTable
    {
        final Path dir;
        final String json;
        final String statsSummary;
        final SortedMap<String, Long> componentSizes = new TreeMap<>();

        CapturedSSTable(Path dir, String json, String statsSummary)
        {
            this.dir = dir;
            this.json = json;
            this.statsSummary = statsSummary;
        }
    }

    public static final class CapturedOutput
    {
        final List<CapturedSSTable> sstables = new ArrayList<>();
    }

    /**
     * Applies {@code populate} to two fresh tables of schema {@code tableCql}, flushes one with
     * the iterator path and one with the cursor path, then asserts the sstables are byte-for-byte
     * and logically equal.  Fails if the cursor path would not actually run for the table.
     *
     * @param populate given (keyspace, table) for each table in turn, issue the identical
     *                 sequence of mutations against {@code keyspace + "." + table}
     * @return the iterator-path capture, so scenarios can assert structural expectations
     */
    protected CapturedOutput assertFlushMatches(String tableCql, BiConsumer<String, String> populate) throws Exception
    {
        return assertFlushMatchesImpl(tableCql, populate, both -> assertEquivalentOutputs(both[0], both[1]));
    }

    /**
     * Same as {@link #assertFlushMatches}, but compares only the logical JSON dump, with byte
     * offsets normalized and no byte-for-byte comparison.  For scenarios whose raw bytes cannot
     * match between the two tables regardless of the flush path:
     * <ul>
     *   <li>counter columns, which have no {@code USING TIMESTAMP}: pass
     *       {@code ignoreCellTimestamps=true} to also normalize {@code tstamp} fields;</li>
     *   <li>non-frozen list columns, whose TimeUUID cell path is embedded as raw bytes.</li>
     * </ul>
     * Row, key, and column structure and values still compare exactly.
     */
    protected CapturedOutput assertFlushMatchesLogically(String tableCql, BiConsumer<String, String> populate, boolean ignoreCellTimestamps) throws Exception
    {
        return assertFlushMatchesImpl(tableCql, populate, both -> assertEquivalentOutputsLogically(both[0], both[1], ignoreCellTimestamps));
    }

    /**
     * Shared by {@link #assertFlushMatches} and {@link #assertFlushMatchesLogically}, which
     * differ only in which comparison they apply to the two captures.
     */
    private CapturedOutput assertFlushMatchesImpl(String tableCql, BiConsumer<String, String> populate, Consumer<CapturedOutput[]> compare) throws Exception
    {
        tableCql = withSkipListMemtable(tableCql);
        Path scratch = Files.createTempDirectory("differential-flush");
        try
        {
            CapturedOutput[] both = captureBothPaths(tableCql, populate, scratch);
            compare.accept(both);
            return both[0];
        }
        finally
        {
            FileUtils.deleteDirectory(scratch.toFile());
        }
    }

    /**
     * The comparison half of {@link #assertFlushMatchesLogically}, split out so a subclass that
     * drives capture itself via {@link #captureAll} (e.g. {@code HarryFlushDifferentialTest}) can
     * reuse it.
     */
    protected void assertEquivalentOutputsLogically(CapturedOutput iterator, CapturedOutput cursor, boolean ignoreCellTimestamps)
    {
        String itJson = normalizeForLogicalComparison(allJson(iterator), ignoreCellTimestamps);
        String cuJson = normalizeForLogicalComparison(allJson(cursor), ignoreCellTimestamps);
        if (!itJson.equals(cuJson))
            fail("LOGICAL divergence (iterator vs cursor):\n" + DifferentialTestUtils.firstJsonDiff(itJson, cuJson));
    }

    private static String normalizeForLogicalComparison(String json, boolean ignoreCellTimestamps)
    {
        json = normalize(json);
        json = POSITION_FIELD.matcher(json).replaceAll("\"position\":\"normalized\"");
        json = WALL_CLOCK_DELETION_FIELD.matcher(json).replaceAll("\"$1\":\"normalized\"");
        if (ignoreCellTimestamps)
            json = TSTAMP_FIELD.matcher(json).replaceAll("\"tstamp\":\"normalized\"");
        return json;
    }

    private static String allJson(CapturedOutput out)
    {
        StringBuilder sb = new StringBuilder();
        for (CapturedSSTable s : out.sstables)
            sb.append(s.json);
        return sb.toString();
    }

    // A short scenario keeps the strict byte comparison; if its two captures straddle a wall-clock
    // second (which shifts local_delete_time/expires_at by one second), it retries.  The retry
    // only fires when the output actually carries such a field, so a plain-insert scenario under
    // USING TIMESTAMP is never retried needlessly.  Long scenarios use assertFlushMatchesLogically.
    private static final int WALL_CLOCK_STRADDLE_ATTEMPTS = 4;

    private CapturedOutput[] captureBothPaths(String tableCql, BiConsumer<String, String> populate, Path scratch) throws Exception
    {
        for (int attempt = 1; ; attempt++)
        {
            long startedAtSecond = Clock.Global.nowInSeconds();
            CapturedOutput[] both = captureBothPathsOnce(tableCql, populate, scratch.resolve("attempt-" + attempt));
            if (Clock.Global.nowInSeconds() == startedAtSecond
                || !(hasWallClockField(both[0]) || hasWallClockField(both[1])))
                return both;
            assertTrue("scenario straddled a wall-clock second on all " + WALL_CLOCK_STRADDLE_ATTEMPTS +
                       " attempts; it is too long for the strict comparison, use assertFlushMatchesLogically",
                       attempt < WALL_CLOCK_STRADDLE_ATTEMPTS);
        }
    }

    /** Whether any captured sstable carries a wall-clock-derived field; see {@link #captureBothPaths}. */
    private static boolean hasWallClockField(CapturedOutput out)
    {
        for (CapturedSSTable s : out.sstables)
            if (WALL_CLOCK_DELETION_FIELD.matcher(s.json).find())
                return true;
        return false;
    }

    private CapturedOutput[] captureBothPathsOnce(String tableCql, BiConsumer<String, String> populate, Path scratch) throws Exception
    {
        try
        {
            String tableA = createTable(tableCql);
            DatabaseDescriptor.setCursorFlushEnabled(false);
            populate.accept(KEYSPACE, tableA);
            flush(KEYSPACE, tableA);
            CapturedOutput iterator = captureAll(getColumnFamilyStore(KEYSPACE, tableA), scratch.resolve("iterator"));

            String tableB = createTable(tableCql);
            ColumnFamilyStore cfsB = getColumnFamilyStore(KEYSPACE, tableB);
            DatabaseDescriptor.setCursorFlushEnabled(true);
            populate.accept(KEYSPACE, tableB);
            assertTrue("scenario's table/memtable does not satisfy MemtableCursorFlusher.isSupported; " +
                       "fix the scenario, not this assertion",
                       MemtableCursorFlusher.isSupported(cfsB.metadata(), cfsB.getCurrentMemtable()));
            long cursorRunsBefore = FlushPipelineCounts.cursorFlushesRun();
            flush(KEYSPACE, tableB);
            // isSupported() only clears the memtable/allocator gate; the writer-shape gate is
            // checked separately in Flushing and silently falls back to the iterator path.  Confirm
            // the cursor path actually ran, so this differential is not iterator-vs-iterator.
            assertTrue("cursor flush path did not run for table " + tableB + "; it fell back to the " +
                       "iterator path.  Check Flushing.canUseCursorFlush's writer-shape gate.",
                       FlushPipelineCounts.cursorFlushesRun() > cursorRunsBefore);
            CapturedOutput cursor = captureAll(cfsB, scratch.resolve("cursor"));

            return new CapturedOutput[]{ iterator, cursor };
        }
        finally
        {
            DatabaseDescriptor.setCursorFlushEnabled(false);
        }
    }

    /**
     * Exposed to subclasses whose population step does not fit {@link #assertFlushMatches}'s
     * {@code BiConsumer} shape (e.g. {@code HarryFlushDifferentialTest}), so they can drive
     * {@link #captureAll} and {@link #assertEquivalentOutputs} directly.
     */
    protected CapturedOutput captureAll(ColumnFamilyStore cfs, Path dir) throws IOException
    {
        CapturedOutput out = new CapturedOutput();
        // Sort by first key so the two paths' shards pair up by token range, not by the arbitrary
        // order of getLiveSSTables().  With a single output sstable this is a no-op.
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        sstables.sort(SSTableReader.firstKeyComparator);
        int i = 0;
        for (SSTableReader sstable : sstables)
            out.sstables.add(capture(cfs, sstable, dir.resolve(Integer.toString(i++))));
        return out;
    }

    private CapturedSSTable capture(ColumnFamilyStore cfs, SSTableReader sstable, Path dir) throws IOException
    {
        Files.createDirectories(dir);
        SortedMap<String, Long> copiedSizes = new TreeMap<>();
        for (Component c : sstable.descriptor.discoverComponents())
        {
            Path source = sstable.descriptor.fileFor(c).toPath();
            Path target = dir.resolve(c.name());
            Files.copy(source, target);
            copiedSizes.put(c.name(), Files.size(target));
        }

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false,
                                                      IVerifier.options().invokeDiskFailurePolicy(true)
                                                                         .extendedVerification(true).build()))
        {
            verifier.verify();
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (org.apache.cassandra.io.sstable.ISSTableScanner scanner = sstable.getScanner())
        {
            JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, false,
                                        sstable.metadata(), DUMP_NOW_SEC, baos);
        }
        String json = baos.toString(StandardCharsets.UTF_8);

        StatsMetadata stats = sstable.getSSTableMetadata();
        String statsSummary = "minTimestamp=" + stats.minTimestamp +
                              " maxTimestamp=" + stats.maxTimestamp +
                              " minLocalDeletionTime=" + stats.minLocalDeletionTime +
                              " maxLocalDeletionTime=" + stats.maxLocalDeletionTime +
                              " estimatedKeys=" + sstable.estimatedKeys() +
                              " totalRows=" + stats.totalRows +
                              " totalColumnsSet=" + stats.totalColumnsSet +
                              " encodingStats=" + sstable.header.stats() +
                              " metaEncodingStats=" + stats.encodingStats.minTimestamp + "/" + stats.encodingStats.minLocalDeletionTime + "/" + stats.encodingStats.minTTL +
                              " tombstoneHist=" + stats.estimatedTombstoneDropTime +
                              " cellsPerPartition=" + stats.estimatedCellPerPartitionCount.mean() + "/" + stats.estimatedCellPerPartitionCount.count();

        CapturedSSTable captured = new CapturedSSTable(dir, json, statsSummary);
        captured.componentSizes.putAll(copiedSizes);
        return captured;
    }

    protected void assertEquivalentOutputs(CapturedOutput iterator, CapturedOutput cursor)
    {
        assertEquals("output sstable count differs between paths", iterator.sstables.size(), cursor.sstables.size());
        for (int i = 0; i < iterator.sstables.size(); i++)
        {
            CapturedSSTable it = iterator.sstables.get(i);
            CapturedSSTable cu = cursor.sstables.get(i);

            String itJson = normalize(it.json);
            String cuJson = normalize(cu.json);
            if (!itJson.equals(cuJson))
                fail("LOGICAL divergence in output sstable " + i + " (iterator vs cursor):\n" + DifferentialTestUtils.firstJsonDiff(itJson, cuJson) +
                     "\niterator stats: " + it.statsSummary + "\ncursor stats:   " + cu.statsSummary);

            assertEquals("stats summary divergence in output sstable " + i, it.statsSummary, cu.statsSummary);

            SortedSet<String> components = new TreeSet<>();
            components.addAll(it.componentSizes.keySet());
            components.addAll(cu.componentSizes.keySet());
            List<String> divergences = new ArrayList<>();
            for (String comp : components)
            {
                // Statistics.db carries commitLogIntervals, the commit-log byte range this
                // table's mutations landed in.  The two tables are populated sequentially, so
                // their intervals differ regardless of the flush path.  The correctness-relevant
                // fields are already compared via the logical JSON dump and statsSummary above.
                if (comp.equals("Statistics.db"))
                    continue;
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
                long firstDiff = DifferentialTestUtils.firstFileDifference(a, b);
                if (firstDiff < 0)
                    continue;
                divergences.add(DifferentialTestUtils.describeFileDiff(comp, a, b, firstDiff));
            }
            if (!divergences.isEmpty())
                fail("BYTE divergence in output sstable " + i + " (iterator vs cursor):\n" + String.join("\n", divergences));
        }
    }

}
