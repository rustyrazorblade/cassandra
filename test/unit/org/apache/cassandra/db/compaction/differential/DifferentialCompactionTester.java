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
 *  1. BYTE level: every output component must be byte-identical. There is deliberately NO
 *     exception mechanism — nothing is allowed to diverge; every divergence found to date
 *     has been a bug in one of the paths.
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
    // sstabledump's "expired" fields come from WALL CLOCK, not the fixed nowInSec above
    // (JsonTransformer), so the two paths' captures can render them differently; the flag is
    // derived from expires_at, which is still compared. Compiled once: the streaming digest
    // applies this per dump line.
    private static final java.util.regex.Pattern EXPIRED_FLAG =
        java.util.regex.Pattern.compile("\"expired\"\\s*:\\s*(true|false)");

    /**
     * Scale mode for very large scenarios (millions of rows): the logical dump is streamed
     * into a SHA-256 digest instead of being retained as a String, so capture memory stays
     * flat regardless of row count. Byte comparison always streams. On a digest mismatch
     * the byte-level comparison (which still reports exact offsets) is the debugging tool;
     * rerun a reduced scenario without scale mode for a row-level JSON diff.
     */
    protected boolean scaleCapture()
    {
        return false;
    }

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
     * byte + logical equivalence of every output component.
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs) throws Exception
    {
        return assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK);
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
                                                         TaskFactory taskFactory) throws Exception
    {
        return assertCursorMatchesIterator(cfs, inputs, taskFactory,
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
                                                         TaskFactory taskFactory,
                                                         long gcBefore) throws Exception
    {
        Path scratch = Files.createTempDirectory("differential-compaction");

        // Early open stays ENABLED here deliberately: keepOriginals=true with early open used
        // to delete the originals (SSTableRewriter.moveStarts obsoleted fully-covered inputs
        // unconditionally — now fixed and guarded by the flag), and this harness depends on
        // the originals surviving, so every differential run doubles as the regression test.
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
        assertEquivalentOutputs(iterator, cursor);
        return iterator;
    }

    /**
     * Differential at TWO generations: the normal differential first (gen 1), then the inputs
     * are genuinely compacted through the CURSOR path and the differential runs again over the
     * cursor-produced outputs (gen 2). Write-side corruption that only manifests when the next
     * merge re-reads the output — the increment-2 HAS_COMPLEX_DELETION flag bug class, which
     * desynced the FOLLOWING compaction, not its own — fails gen 2 loudly here instead of
     * surviving until production recompacts.
     *
     * Returns the GEN-1 iterator capture: scenario structural assertions target gen 1, whose
     * shape the scenario controls directly.
     */
    protected CapturedOutput assertCursorMatchesIteratorAcrossGenerations(ColumnFamilyStore cfs) throws Exception
    {
        CapturedOutput gen1 = assertCursorMatchesIterator(cfs);

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore);
        if (cfs.getLiveSSTables().isEmpty())
            return gen1; // gen 1 purged everything; there are no gen-2 inputs

        assertCursorMatchesIterator(cfs);
        return gen1;
    }

    /**
     * Commits one compaction over the given inputs through the selected path WITHOUT restore:
     * the live set genuinely becomes the outputs. Used by the cross-generation rung so the
     * second differential reads cursor-produced sstables.
     */
    protected void commitCompaction(ColumnFamilyStore cfs, Set<SSTableReader> inputs, boolean cursor, long gcBefore) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        if (cursor)
            assertCursorPathWillRun(cfs, inputs, gcBefore);
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting for commit", txn);
        new CompactionTask(cfs, txn, gcBefore, false).execute(ActiveCompactionsTracker.NOOP);
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
        // 1. copy components FIRST: when verification (or the dump) fails, the transaction
        // rolls back and deletes the live files — the captured copies are then the ONLY
        // evidence for offline byte-level decoding (the captured dirs are the established
        // debugging instrument of this harness)
        Files.createDirectories(dir);
        SortedMap<String, Long> copiedSizes = new TreeMap<>();
        for (Component c : sstable.descriptor.discoverComponents())
        {
            Path source = sstable.descriptor.fileFor(c).toPath();
            Path target = dir.resolve(c.name());
            Files.copy(source, target);
            copiedSizes.put(c.name(), Files.size(target));
        }

        // 2. structural verification of the output. In scale mode the verifier's debug
        // stream must be silenced: the extended index walk debug-logs EVERY index block
        // (~560K lines for a >2GiB partition), and ant's junit formatter buffers all test
        // output in memory — the log volume, not the verification, OOMs the fork.
        OutputHandler verifyOutput = scaleCapture()
            ? new OutputHandler.LogOutput() { @Override public void debug(String msg) {} }
            : new OutputHandler.LogOutput();
        try (IVerifier verifier = sstable.getVerifier(cfs, verifyOutput, false,
                                                      IVerifier.options().invokeDiskFailurePolicy(true)
                                                                         .extendedVerification(true).build()))
        {
            verifier.verify();
        }

        // 2. canonical logical dump
        // JsonTransformer's "expired" fields are computed from WALL CLOCK
        // (currentTimeMillis), ignoring the fixed nowInSec passed below — so byte-identical
        // outputs can render differently when a localExpirationTime falls between the two
        // paths' captures, which run seconds apart (materialized-view expired-liveness rows
        // sit permanently on that boundary: their expiration IS the write second). The flag
        // is derived from expires_at, which is still compared, so normalize it out.
        String json;
        if (scaleCapture())
        {
            // stream into a digest: capture memory stays flat at millions of rows
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
                NormalizingDigestOutputStream out = new NormalizingDigestOutputStream(digest);
                JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, false,
                                            sstable.metadata(), DUMP_NOW_SEC, out);
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
                JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, false,
                                            sstable.metadata(), DUMP_NOW_SEC, baos);
            }
            json = baos.toString(StandardCharsets.UTF_8)
                       .transform(s -> EXPIRED_FLAG.matcher(s).replaceAll("\"expired\":\"normalized\""));
        }

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

            // logical first: a row-level diff is far more debuggable than a stats mismatch.
            // In scale mode the dump is a digest — defer it below the byte comparison, which
            // still localizes divergences to exact offsets.
            boolean digestMode = it.json.startsWith("sha256:");
            if (!digestMode && !it.json.equals(cu.json))
                fail("LOGICAL divergence in output sstable " + i + " (iterator vs cursor):\n" + firstJsonDiff(it.json, cu.json) +
                     "\niterator stats: " + it.statsSummary + "\ncursor stats:   " + cu.statsSummary);

            assertEquals("stats summary divergence in output sstable " + i, it.statsSummary, cu.statsSummary);

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
            if (!divergences.isEmpty())
                fail("BYTE divergence in output sstable " + i + " (iterator vs cursor):\n" + String.join("\n", divergences) +
                     "\nNothing is allowed to diverge: every divergence found to date has been a bug in one of the paths");

            if (digestMode)
                assertEquals("logical dump digest divergence in output sstable " + i +
                             " (scale mode; rerun a reduced scenario without scale mode for a row-level diff)",
                             it.json, cu.json);
        }
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
            in.skipNBytes(from);
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
     * Streams a JSON dump into a digest, normalizing the wall-clock-derived "expired"
     * fields. Buffers up to a line (toJsonLines emits one partition per line), but flushes
     * oversized lines in bounded chunks so memory stays flat even for multi-GB partitions:
     * the chunk cut keeps a small unprocessed tail so a normalization token can never be
     * split, and the cut points are functions of CONTENT ONLY (buffer fill, not write()
     * granularity), so both captures digest identical normalized streams.
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

        /** Digest all buffered content (end of a line or of the stream). */
        void flushTail()
        {
            if (line.size() == 0)
                return;
            update(line.toByteArray(), line.size());
            line.reset();
        }

        /** Digest all but the last TAIL_KEEP buffered bytes; the tail stays buffered so the
         *  "expired":... token can never straddle a chunk cut. */
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
            byte[] normalized = new String(bytes, 0, length, StandardCharsets.UTF_8)
                                .transform(s -> EXPIRED_FLAG.matcher(s).replaceAll("\"expired\":\"normalized\""))
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
