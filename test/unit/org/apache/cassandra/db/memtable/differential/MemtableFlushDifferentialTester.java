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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
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
 * Compaction's differential harness ({@code DifferentialCompactionTester}) reruns both paths
 * over the SAME already-flushed input sstables — flush has no such reusable input, since a
 * live memtable can only be flushed once. Instead: the same deterministic sequence of CQL
 * statements is applied to two identically-schemaed tables, one flushed via the legacy
 * iterator path and the other via the cursor path (toggling
 * {@link DatabaseDescriptor#setCursorFlushEnabled}), and the resulting sstables are compared.
 * <p>
 * The capture/compare machinery below (byte-for-byte component comparison + canonical JSON
 * logical dump + stats spot-check) is deliberately built the same way
 * {@code DifferentialCompactionTester}'s is — same bar ("nothing is allowed to diverge"), same
 * technique. The byte/JSON-diff primitives the two harnesses' compare steps bottom out in
 * ({@code firstFileDifference}/{@code describeFileDiff}/{@code hexContext}/{@code firstJsonDiff})
 * are shared via {@link DifferentialTestUtils}; the surrounding capture/compare structure stays
 * separate since it's diverged for good reason — compaction's {@code capture()} has a scale-mode
 * digest-streaming path and an "expired" JSON field normalization this harness has never needed.
 */
public abstract class MemtableFlushDifferentialTester extends CQLTester
{
    private static final long DUMP_NOW_SEC = 0;

    // Non-frozen list columns are internally keyed by a synthetic, client-generated TimeUUID
    // cell path assigned when the mutation is built - upstream of both flush paths entirely,
    // and inherently wall-clock-based, so it can never match between the two sequentially
    // (necessarily - a live memtable can only be flushed once) populated tables. Normalizing
    // it out costs nothing: neither flush path chooses or even sees this value as anything
    // but an opaque already-assigned cell path, so it carries no signal about a flush bug.
    private static final java.util.regex.Pattern LIST_CELL_PATH =
        java.util.regex.Pattern.compile("\"path\":\\[\"[0-9a-f-]{36}\"\\]");

    /** Any tstamp field (row liveness_info or per-cell) — see {@link #assertFlushMatchesLogically}. */
    private static final java.util.regex.Pattern TSTAMP_FIELD =
        java.util.regex.Pattern.compile("\"tstamp\":\"-?\\d+\"");

    /** Raw on-disk byte offset — shifts whenever any earlier value's encoded length differs. */
    private static final java.util.regex.Pattern POSITION_FIELD =
        java.util.regex.Pattern.compile("\"position\":\\d+");

    /**
     * A deletion/TTL's local_delete_time (and a TTL row's derived expires_at) is always
     * server-wall-clock-seconds at processing time — CQL's {@code USING TIMESTAMP} controls
     * only the microsecond write timestamp, not this. A scenario with many statements (e.g. a
     * randomized sweep) can genuinely straddle a wall-clock second between the two sequential
     * populate() calls, off by exactly one second in a way that carries no signal about a flush
     * bug — see {@link #assertFlushMatchesLogically}.
     */
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
     * Applies {@code populate} identically to two fresh tables of schema {@code tableCql} (a
     * CQL create-table statement using {@code %s} for the fully-qualified table name, matching
     * {@link #createTable(String)}'s convention), flushing the first with the legacy iterator
     * path and the second with the cursor path, then asserts the resulting sstables are
     * byte-for-byte and logically equivalent. Fails loudly (rather than passing vacuously) if
     * the cursor path wouldn't actually run for the resulting table/memtable — a scenario that
     * silently falls back to the iterator path proves nothing.
     *
     * @param populate given (keyspace, table) for each of the two tables in turn, issue the
     *                 identical sequence of mutations via {@code execute(...)} against the
     *                 fully-qualified {@code keyspace + "." + table}
     * @return the iterator-path capture, so scenarios can assert structural expectations
     *         (e.g. "did this actually produce a static row")
     */
    protected CapturedOutput assertFlushMatches(String tableCql, BiConsumer<String, String> populate) throws Exception
    {
        return assertFlushMatchesImpl(tableCql, populate, both -> assertEquivalentOutputs(both[0], both[1]));
    }

    /**
     * Same as {@link #assertFlushMatches}, but compares only the logical JSON dump (normalized
     * the same way, plus stripping the byte-offset {@code position} fields that shift whenever
     * any earlier value's on-disk encoded length differs) — no byte-for-byte component
     * comparison. For scenarios where the raw bytes genuinely cannot match between the two
     * sequentially-populated tables independent of which flush algorithm wrote them:
     * <ul>
     *   <li>counter columns: CQL has no {@code USING TIMESTAMP} for counter updates (each merges
     *       into the memtable's own counter context using a server-assigned timestamp) — pass
     *       {@code ignoreCellTimestamps=true} to additionally normalize {@code tstamp} fields;</li>
     *   <li>non-frozen list columns: each element's cell path is a client-generated TimeUUID
     *       assigned when the mutation is built, embedded directly as raw bytes in Data.db —
     *       {@link #normalize} already strips it from the logical dump, but the byte-for-byte
     *       component comparison has no such normalization and would always diverge.</li>
     * </ul>
     * Row/partition-key/complex-column structure and values (including, for counters, the
     * merged counter value itself) still compare exactly.
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
     * The comparison half of {@link #assertFlushMatchesLogically}, split out so a subclass whose
     * population step doesn't fit the {@code BiConsumer<String, String>} shape (e.g.
     * {@code HarryFlushDifferentialTest}, which replays a Harry operation history through its own
     * {@code CQLVisitExecutor}) can drive capture itself via {@link #captureAll} and still reuse
     * this comparison.
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

    /**
     * A DELETE's {@code local_delete_time}, and a TTL's derived {@code expires_at}, is
     * server-wall-clock seconds at processing time; no CQL construct pins it, and
     * {@code Clock.Global}'s instance is final and chosen at class-init, so a test cannot pin it
     * either. The two populate() runs below are sequential, so a scenario that happens to
     * straddle a wall-clock second yields outputs differing by exactly one second in those
     * fields: no signal about a flush bug, but fatal to the byte-for-byte comparison.
     *
     * Long scenarios that cannot avoid straddling use {@link #assertFlushMatchesLogically},
     * which normalizes those fields away. Short scenarios keep the strict comparison and retry
     * here instead, so a wall-clock boundary costs a repeat rather than byte-level coverage.
     *
     * The retry is conditional on the output actually carrying one of those fields. A scenario
     * of plain inserts under explicit {@code USING TIMESTAMP} has no wall-clock-derived value
     * at all, so crossing a second cannot make its two captures differ - and a long one (e.g.
     * a 20,000-row wide partition) crosses one every time. Retrying those would fail a
     * perfectly comparable scenario for a difference that cannot exist.
     */
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
            assertTrue("scenario writes local_delete_time/expires_at and straddled a wall-clock second on " +
                       "all " + WALL_CLOCK_STRADDLE_ATTEMPTS + " attempts, so those fields cannot be " +
                       "compared byte-for-byte. It is too long for the strict assertion: use " +
                       "assertFlushMatchesLogically instead.",
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
            assertTrue("scenario's table/memtable doesn't satisfy MemtableCursorFlusher.isSupported " +
                       "- won't actually exercise the cursor path; fix the scenario, not this assertion",
                       MemtableCursorFlusher.isSupported(cfsB.metadata(), cfsB.getCurrentMemtable()));
            flush(KEYSPACE, tableB);
            CapturedOutput cursor = captureAll(cfsB, scratch.resolve("cursor"));

            return new CapturedOutput[]{ iterator, cursor };
        }
        finally
        {
            DatabaseDescriptor.setCursorFlushEnabled(false);
        }
    }

    /**
     * Exposed to subclasses (e.g. {@code HarryFlushDifferentialTest}) whose population step
     * doesn't fit {@link #assertFlushMatches}'s {@code BiConsumer<String, String>} shape - Harry
     * replays an abstract operation history through its own {@code CQLVisitExecutor}, not a
     * simple {@code execute(...)} callback - and so need to drive {@link #captureAll} and
     * {@link #assertEquivalentOutputs} directly instead.
     */
    protected CapturedOutput captureAll(ColumnFamilyStore cfs, Path dir) throws IOException
    {
        CapturedOutput out = new CapturedOutput();
        int i = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
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
                // Statistics.db carries StatsMetadata.commitLogIntervals - the actual commit
                // log byte range this table's mutations landed in. Unlike compaction (whose
                // differential reruns both paths over the SAME pre-existing input sstables, so
                // that field is already fixed and shared), a flush computes it fresh from
                // wherever this table's writes actually fell in the one shared, sequential
                // commit log - and the two tables here are necessarily populated sequentially
                // (a live memtable can only be flushed once), so their intervals differ for
                // real, independent of which flush algorithm wrote them. The fields that
                // matter for correctness are already covered by the logical JSON dump and
                // statsSummary above, both compared before this loop runs.
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
                fail("BYTE divergence in output sstable " + i + " (iterator vs cursor):\n" + String.join("\n", divergences) +
                     "\nNothing is allowed to diverge: every divergence found to date has been a bug in one of the paths");
        }
    }

}
