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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Differential harness for cursor-backed vs legacy cleanup, the cleanup analogue of
 * {@link DifferentialCompactionTester}.
 *
 * Runs the same input sstable through {@code CompactionManager.performCleanupOne} twice with the
 * same owned ranges, once with cursor compaction off and once on, and asserts the outputs are
 * byte-identical.
 *
 * Cleanup rewrites with {@code keepOriginals=false}, so the first run deletes the input.  The
 * harness copies the input's components aside up front and copies them back under the same
 * descriptor between runs, so both runs read byte-identical input.
 */
public abstract class CursorCleanupDifferentialTester extends DifferentialCompactionTester
{
    private boolean cursorCompactionWasEnabled;

    /**
     * Each path flips {@code cursor_compaction_enabled}.  Snapshot and restore it so the value does
     * not leak to other tests sharing the JVM fork.
     */
    @Before
    public void snapshotCursorCompactionSetting()
    {
        cursorCompactionWasEnabled = DatabaseDescriptor.cursorCompactionEnabled();
    }

    @After
    public void restoreCursorCompactionSetting()
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursorCompactionWasEnabled);
    }

    /** What one cleanup path produced: the captured output sstables plus what survived in them. */
    public static final class CleanupOutcome
    {
        public final CapturedOutput captured = new CapturedOutput();
        /** Every surviving partition key, in token order. */
        public final List<DecoratedKey> survivingKeys = new ArrayList<>();
        /** Total surviving rows (clustering rows and static rows) across all output sstables. */
        public long survivingRows;
    }

    /**
     * Runs both cleanup paths over the current (single) live sstable of the table with
     * {@code ownedRanges} as the ranges this node still owns, and asserts byte + logical
     * equivalence of the surviving output.
     *
     * @return the legacy-path outcome, so scenarios can assert what actually survived
     */
    protected CleanupOutcome assertCursorCleanupMatchesLegacy(ColumnFamilyStore cfs,
                                                              Collection<Range<Token>> ownedRanges) throws Exception
    {
        return assertCursorCleanupMatchesLegacy(cfs, ownedRanges, Collections.emptySet());
    }

    protected CleanupOutcome assertCursorCleanupMatchesLegacy(ColumnFamilyStore cfs,
                                                              Collection<Range<Token>> ownedRanges,
                                                              Collection<Range<Token>> transientRanges) throws Exception
    {
        Path scratch = Files.createTempDirectory("differential-cleanup");

        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertFalse("scenario produced no input sstables", live.isEmpty());
        assertEquals("cleanup operates on one sstable at a time; flush/compact the scenario to a single sstable",
                     1, live.size());
        Descriptor input = live.iterator().next().descriptor;

        Path preserved = scratch.resolve("input");
        copyComponents(input, preserved);

        CleanupOutcome legacy = cleanupPath(cfs, false, ownedRanges, transientRanges, scratch.resolve("legacy"));
        restoreInput(cfs, input, preserved);
        CleanupOutcome cursor = cleanupPath(cfs, true, ownedRanges, transientRanges, scratch.resolve("cursor"));

        assertEquals("different partitions survived cleanup on the two paths",
                     legacy.survivingKeys, cursor.survivingKeys);
        assertEquals("different row counts survived cleanup on the two paths",
                     legacy.survivingRows, cursor.survivingRows);
        assertEquivalentOutputs(legacy.captured, cursor.captured);
        return legacy;
    }

    /**
     * Runs one cleanup with the cursor path enabled and reports whether production took it.
     * <p>
     * For scenarios that assert a deliberate fallback to legacy, where
     * {@link #assertCursorCleanupMatchesLegacy} does not fit.  The assertion of interest is the
     * routing decision itself, plus whatever the caller checks about the resulting live set.
     *
     * @return true if production took the cursor path, false if it fell back to legacy
     */
    protected boolean runCleanupWithCursorEnabled(ColumnFamilyStore cfs,
                                                  Collection<Range<Token>> ownedRanges) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(true);

        Set<SSTableReader> inputs = new HashSet<>(cfs.getLiveSSTables());
        assertFalse("scenario produced no input sstables", inputs.isEmpty());

        long cursorRunsBefore = CompactionManager.cursorCleanupsRun.sum();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.CLEANUP))
        {
            assertNotNull("unable to mark input compacting for cleanup", txn);
            CompactionManager.instance.performCleanupOne(cfs, txn, ownedRanges, Collections.emptySet(),
                                                         cfs.indexManager.hasIndexes());
        }
        return CompactionManager.cursorCleanupsRun.sum() != cursorRunsBefore;
    }

    /** Runs one cleanup path over the single live sstable and captures whatever survived. */
    private CleanupOutcome cleanupPath(ColumnFamilyStore cfs,
                                       boolean cursor,
                                       Collection<Range<Token>> ownedRanges,
                                       Collection<Range<Token>> transientRanges,
                                       Path scratch) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);

        Set<SSTableReader> inputs = new HashSet<>(cfs.getLiveSSTables());
        Set<Descriptor> liveBeforeDescs = new HashSet<>();
        for (SSTableReader in : inputs)
            liveBeforeDescs.add(in.descriptor);

        if (cursor)
            assertCursorCleanupPathWillRun(cfs, inputs, ownedRanges, transientRanges);

        long cursorRunsBefore = CompactionManager.cursorCleanupsRun.sum();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.CLEANUP))
        {
            assertNotNull("unable to mark input compacting for cleanup", txn);
            CompactionManager.instance.performCleanupOne(cfs, txn, ownedRanges, transientRanges,
                                                         cfs.indexManager.hasIndexes());
        }
        assertEquals("cleanup took the wrong path: cursor cleanups run during this call",
                     cursor ? 1L : 0L, CompactionManager.cursorCleanupsRun.sum() - cursorRunsBefore);

        List<SSTableReader> outputs = identifyOutputs(cfs, liveBeforeDescs, Collections.emptySet(), new ArrayList<>());
        CleanupOutcome outcome = new CleanupOutcome();
        int seq = 0;
        for (SSTableReader out : outputs)
        {
            outcome.captured.sstables.add(capture(cfs, out, scratch.resolve("sstable-" + seq++)));
            collectSurvivors(out, outcome);
        }

        // Delist and delete the outputs so the next run starts from the restored input alone.
        List<Path> outputFiles = new ArrayList<>();
        for (SSTableReader out : outputs)
            for (Component c : out.descriptor.discoverComponents())
                outputFiles.add(out.descriptor.fileFor(c).toPath());
        cfs.getTracker().removeUnsafe(new HashSet<>(outputs));
        for (SSTableReader out : outputs)
            out.selfRef().release();
        for (Path f : outputFiles)
            Files.deleteIfExists(f);
        assertTrue("live set should be empty after delisting cleanup outputs", cfs.getLiveSSTables().isEmpty());

        return outcome;
    }

    /** Records which partitions (and how many rows) survived into an output sstable. */
    private static void collectSurvivors(SSTableReader sstable, CleanupOutcome outcome)
    {
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    outcome.survivingKeys.add(partition.partitionKey());
                    if (!partition.staticRow().isEmpty())
                        outcome.survivingRows++;
                    while (partition.hasNext())
                    {
                        if (partition.next().isRow())
                            outcome.survivingRows++;
                    }
                }
            }
        }
    }

    /**
     * If the cursor path would not run, the harness compares legacy vs legacy.  Checks the same two
     * conditions production checks: the support gate, and that the owned ranges resolve to a
     * non-empty byte range in this sstable.
     */
    protected void assertCursorCleanupPathWillRun(ColumnFamilyStore cfs,
                                                  Set<SSTableReader> inputs,
                                                  Collection<Range<Token>> ownedRanges,
                                                  Collection<Range<Token>> transientRanges) throws Exception
    {
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        try (CompactionController controller = new CompactionController(cfs, inputs, gcBefore))
        {
            assertTrue("scenario is not supported by cursor cleanup; assert that explicitly instead",
                       CursorCompactor.isCleanupSupported(inputs, controller));
        }
        Collection<Range<Token>> rangesToScan = new ArrayList<>(ownedRanges);
        SSTableReader sstable = inputs.iterator().next();
        if (sstable.isRepaired())
            rangesToScan.removeAll(transientRanges);
        assertFalse("owned ranges cover no bytes of the input sstable, so cursor cleanup falls back to legacy",
                    sstable.getPositionsForRanges(rangesToScan).isEmpty());
    }

    private static void copyComponents(Descriptor descriptor, Path target) throws IOException
    {
        Files.createDirectories(target);
        for (Component c : descriptor.discoverComponents())
            Files.copy(descriptor.fileFor(c).toPath(), target.resolve(c.name()));
    }

    /**
     * Puts the input sstable back exactly as it was - same descriptor, same component bytes - and
     * relists it, so the second path reads byte-identical input to the first.
     */
    private static void restoreInput(ColumnFamilyStore cfs, Descriptor descriptor, Path preserved) throws IOException
    {
        // Cleanup rewrites with keepOriginals=false, so the previous run obsoleted the input and
        // queued its files for deletion. Copying the preserved components back under the same
        // descriptor before that queue drains races the deferred delete against the restore. Wait
        // for the deletions to run first.
        LifecycleTransaction.waitForDeletions();

        try (java.util.stream.Stream<Path> files = Files.list(preserved))
        {
            for (Path saved : files.collect(Collectors.toList()))
                Files.copy(saved, descriptor.fileFor(Component.parse(saved.getFileName().toString(),
                                                                     descriptor.getFormat())).toPath());
        }
        assertTrue("preserved input is missing its data component",
                   descriptor.fileFor(SSTableFormat.Components.DATA).exists());
        cfs.getTracker().addInitialSSTables(Collections.singleton(SSTableReader.open(cfs, descriptor)));
        assertEquals("restore failed: live sstable count", 1, cfs.getLiveSSTables().size());
    }

    /**
     * Every partition key present in the table's single live sstable, in token order. Scenarios
     * build owned ranges from these so the boundaries land exactly where they intend rather than
     * wherever the partitioner happened to hash the keys.
     */
    protected static List<DecoratedKey> partitionKeysInTokenOrder(ColumnFamilyStore cfs)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
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
        }
        keys.sort(DecoratedKey::compareTo);
        return keys;
    }

    /**
     * Ranges that own exactly {@code keep} out of {@code all}: for each kept key, the half-open
     * range ending at its token and starting at the token immediately below it in {@code all}
     * (the partitioner minimum for the lowest key). Adjacent kept keys yield adjacent ranges,
     * which {@code getPositionsForRanges} normalizes into one section.
     */
    protected static Collection<Range<Token>> rangesOwning(List<DecoratedKey> all, Collection<DecoratedKey> keep)
    {
        Set<Token> keepTokens = new HashSet<>();
        for (DecoratedKey key : keep)
            keepTokens.add(key.getToken());

        List<Range<Token>> ranges = new ArrayList<>();
        List<Token> tokens = new ArrayList<>(new TreeSet<>(all.stream().map(DecoratedKey::getToken).collect(Collectors.toList())));
        for (int i = 0; i < tokens.size(); i++)
        {
            if (!keepTokens.contains(tokens.get(i)))
                continue;
            Token left = i == 0 ? tokens.get(i).getPartitioner().getMinimumToken() : tokens.get(i - 1);
            ranges.add(new Range<>(left, tokens.get(i)));
        }
        return ranges;
    }
}
