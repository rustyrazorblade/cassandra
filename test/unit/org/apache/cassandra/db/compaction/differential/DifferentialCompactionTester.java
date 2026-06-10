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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Differential test harness for cursor-based vs iterator-based compaction.
 *
 * Runs the SAME input sstables through both {@code IteratorCompactionPipeline} and
 * {@code CursorCompactionPipeline} (via the full production {@link CompactionTask} path,
 * selected by {@link DatabaseDescriptor#setCursorCompactionEnabled}), captures both outputs,
 * and asserts equivalence at two levels:
 *
 *  1. BYTE level: every output component must be byte-identical, unless the component is
 *     in the explicit allowlist of known/justified divergences.
 *  2. LOGICAL level: a canonical JSON dump (sstabledump format) of every output sstable
 *     must match exactly, and key stats metadata must match.
 *
 * Correctness invariants of the harness itself:
 *  - inputs are byte-identical for both runs: the first run keeps originals on disk
 *    ({@code keepOriginals=true}) and the harness restores the live set from the original
 *    descriptors without rewriting anything.
 *  - the same gcBefore is passed to both runs, so purge decisions cannot flip between runs.
 *  - the cursor run asserts {@link CursorCompactor#isSupported} up front: a scenario that
 *    silently falls back to the iterator path is a test bug, not a pass.
 *
 * Known limitation: nowInSec for TTL expiry
 * evaluation is taken inside CompactionTask per run; scenarios must not place TTL expiry
 * boundaries within seconds of the test run.
 */
public abstract class DifferentialCompactionTester extends CQLTester
{
    /** Fixed "now" used for JSON dumps so rendering cannot depend on wall clock. */
    private static final long DUMP_NOW_SEC = 0;

    public static final class CapturedSSTable
    {
        final Path dir;                 // copied component files, named by component (e.g. "Data.db")
        final String json;              // canonical logical dump
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

    /** Creates the CompactionTask for one differential run. MUST honor keepOriginals=true. */
    public interface TaskFactory
    {
        CompactionTask create(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore);
    }

    public static final TaskFactory DEFAULT_TASK = (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, true);

    /**
     * Runs both compaction paths over the current live sstables of the table and asserts
     * byte + logical equivalence. The byteDiffAllowlist contains component names (e.g.
     * "Statistics.db") that are permitted to differ at the byte level; logical equivalence
     * is always enforced.
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs, Set<String> byteDiffAllowlist) throws Exception
    {
        return assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), byteDiffAllowlist, DEFAULT_TASK);
    }

    /**
     * Variant for partial-set compactions (inputs is a subset of the live sstables; the rest
     * stay live and participate in purge-overlap decisions) and for custom CompactionTask
     * shapes (e.g. multi-output writers via an overridden getCompactionAwareWriter).
     */
    /** Returns the iterator-path capture so scenarios can assert structural expectations
     *  (e.g. multi-output scenarios MUST verify more than one sstable was produced —
     *  a scenario that does not exercise its mechanism passes vacuously). */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs,
                                                         Set<SSTableReader> inputs,
                                                         Set<String> byteDiffAllowlist,
                                                         TaskFactory taskFactory) throws Exception
    {
        return assertCursorMatchesIterator(cfs, inputs, byteDiffAllowlist, taskFactory,
                                           cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
    }

    /**
     * Variant with an explicit gcBefore: lets scenarios place purge decisions EXACTLY at the
     * boundary (purge requires localDeletionTime < gcBefore) without controlling the wall
     * clock — read the actual deletion time from the flushed sstable's stats, then run with
     * gcBefore == ldt (retained) and gcBefore == ldt + 1 (purged).
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs,
                                                         Set<SSTableReader> inputs,
                                                         Set<String> byteDiffAllowlist,
                                                         TaskFactory taskFactory,
                                                         long gcBefore) throws Exception
    {
        Path scratch = Files.createTempDirectory("differential-compaction");

        // Early open must be off for keepOriginals to actually keep originals:
        // SSTableRewriter.moveStarts() obsoletes fully-covered originals regardless of the
        // keepOriginals flag (only the bulk obsoleteOriginals() call is guarded by it).
        // Early open affects read availability during compaction, not final output bytes,
        // so disabling it does not weaken the differential comparison.
        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        try
        {
            CapturedOutput iterator = compactPath(cfs, inputs, false, gcBefore, scratch.resolve("iterator"), taskFactory);
            // the input INSTANCES were replaced during restore; re-resolve the subset by descriptor
            Set<Descriptor> inputDescs = new HashSet<>();
            for (SSTableReader in : inputs)
                inputDescs.add(in.descriptor);
            Set<SSTableReader> reResolved = new HashSet<>();
            for (SSTableReader live : cfs.getLiveSSTables())
                if (inputDescs.contains(live.descriptor))
                    reResolved.add(live);
            assertEquals("input subset lost across restore", inputs.size(), reResolved.size());
            CapturedOutput cursor = compactPath(cfs, reResolved, true, gcBefore, scratch.resolve("cursor"), taskFactory);
            assertEquivalentOutputs(iterator, cursor, byteDiffAllowlist);
            return iterator;
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
        }
    }

    /**
     * Runs one compaction path over the given input subset (non-participating live sstables
     * stay live and feed purge-overlap decisions), captures the outputs, and restores the live
     * set so the other path sees identical bytes.
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
        taskFactory.create(cfs, txn, gcBefore).execute(ActiveCompactionsTracker.NOOP);

        // Outputs are identified by descriptor diff against the pre-compaction live set:
        // with keepOriginals=true the originals (or early-open clones with moved starts) may
        // remain live as DIFFERENT reader instances, and non-participating sstables are live
        // throughout. Instance identity is never trusted here.
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
     * Delists + releases outputs and any retained input clones, deletes output files only,
     * then reopens every input fresh from its descriptor so a subsequent run sees pristine
     * full-range readers identical to this run's. Non-participating sstables are untouched.
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
        assertEquals("restore failed: live sstable count", liveBeforeCount, cfs.getLiveSSTables().size());
    }

    /** Output identification by before/after descriptor diff; see compactPath for rationale. */
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

    /**
     * Guards against the silent-fallback trap: if the cursor path would not actually run for
     * this scenario, the test would compare iterator vs iterator and pass vacuously. Uses the
     * same isSupported check production uses, on equivalent scanners and controller.
     */
    protected void assertCursorPathWillRun(ColumnFamilyStore cfs, Set<SSTableReader> inputs, long gcBefore) throws Exception
    {
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

    private CapturedSSTable capture(ColumnFamilyStore cfs, SSTableReader sstable, Path dir) throws IOException
    {
        // 1. structural verification of the output
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false,
                                                      IVerifier.options().invokeDiskFailurePolicy(true)
                                                                         .extendedVerification(true).build()))
        {
            verifier.verify();
        }

        // 2. canonical logical dump
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, false,
                                        sstable.metadata(), DUMP_NOW_SEC, baos);
        }
        String json = baos.toString(StandardCharsets.UTF_8);

        // 3. stats spot-check summary
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

        // 4. copy components for byte comparison
        Files.createDirectories(dir);
        CapturedSSTable captured = new CapturedSSTable(dir, json, statsSummary);
        for (Component c : sstable.descriptor.discoverComponents())
        {
            Path source = sstable.descriptor.fileFor(c).toPath();
            Path target = dir.resolve(c.name());
            Files.copy(source, target);
            captured.componentSizes.put(c.name(), Files.size(target));
        }
        return captured;
    }

    protected void assertEquivalentOutputs(CapturedOutput iterator, CapturedOutput cursor, Set<String> byteDiffAllowlist)
    {
        assertEquals("output sstable count differs between paths", iterator.sstables.size(), cursor.sstables.size());
        for (int i = 0; i < iterator.sstables.size(); i++)
        {
            CapturedSSTable it = iterator.sstables.get(i);
            CapturedSSTable cu = cursor.sstables.get(i);

            // logical first: a row-level diff is far more debuggable than a stats mismatch
            if (!it.json.equals(cu.json))
                fail("LOGICAL divergence in output sstable " + i + " (iterator vs cursor):\n" + firstJsonDiff(it.json, cu.json) +
                     "\niterator stats: " + it.statsSummary + "\ncursor stats:   " + cu.statsSummary);

            assertEquals("stats summary divergence in output sstable " + i, it.statsSummary, cu.statsSummary);

            SortedSet<String> components = new TreeSet<>();
            components.addAll(it.componentSizes.keySet());
            components.addAll(cu.componentSizes.keySet());
            List<String> divergences = new ArrayList<>();
            for (String comp : components)
            {
                byte[] a = readComponent(it.dir, comp);
                byte[] b = readComponent(cu.dir, comp);
                if (java.util.Arrays.equals(a, b))
                    continue;
                if (byteDiffAllowlist.contains(comp))
                    continue; // logical equivalence already asserted above
                divergences.add(describeByteDiff(comp, a, b));
            }
            if (!divergences.isEmpty())
                fail("BYTE divergence in output sstable " + i + " (iterator vs cursor):\n" + String.join("\n", divergences) +
                     "\nIf a divergence is benign it must be added to the allowlist with a justifying comment next to the allowlist entry");
        }
    }

    private static byte[] readComponent(Path dir, String component)
    {
        try
        {
            Path p = dir.resolve(component);
            return Files.exists(p) ? Files.readAllBytes(p) : null;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static String describeByteDiff(String component, byte[] a, byte[] b)
    {
        if (a == null || b == null)
            return String.format("  %s: present only in %s path", component, a == null ? "cursor" : "iterator");
        int firstDiff = -1;
        int max = Math.min(a.length, b.length);
        for (int i = 0; i < max; i++)
        {
            if (a[i] != b[i]) { firstDiff = i; break; }
        }
        if (firstDiff == -1)
            firstDiff = max; // same prefix, different length
        return String.format("  %s: lengths %d vs %d, first divergence at offset %d%n    iterator: %s%n    cursor:   %s",
                             component, a.length, b.length, firstDiff,
                             hexContext(a, firstDiff), hexContext(b, firstDiff));
    }

    private static String hexContext(byte[] bytes, int offset)
    {
        int from = Math.max(0, offset - 8);
        int to = Math.min(bytes.length, offset + 24);
        StringBuilder sb = new StringBuilder();
        for (int i = from; i < to; i++)
        {
            if (i == offset)
                sb.append('[');
            sb.append(String.format("%02x", bytes[i]));
            if (i == offset)
                sb.append(']');
            sb.append(' ');
        }
        if (to < bytes.length)
            sb.append("...");
        return sb.toString();
    }

    private static String firstJsonDiff(String a, String b)
    {
        String[] linesA = a.split("\n", -1);
        String[] linesB = b.split("\n", -1);
        int max = Math.max(linesA.length, linesB.length);
        for (int i = 0; i < max; i++)
        {
            String la = i < linesA.length ? linesA[i] : "<missing>";
            String lb = i < linesB.length ? linesB[i] : "<missing>";
            if (!la.equals(lb))
            {
                StringBuilder sb = new StringBuilder();
                sb.append("first differing line ").append(i + 1).append(" of ").append(max).append(":\n");
                for (int j = Math.max(0, i - 2); j < Math.min(max, i + 3); j++)
                {
                    String ja = j < linesA.length ? linesA[j] : "<missing>";
                    String jb = j < linesB.length ? linesB[j] : "<missing>";
                    sb.append(j == i ? ">>" : "  ").append(" iterator: ").append(ja).append('\n');
                    sb.append(j == i ? ">>" : "  ").append(" cursor:   ").append(jb).append('\n');
                }
                return sb.toString();
            }
        }
        return "(no line diff found despite string inequality — check line endings)";
    }
}
