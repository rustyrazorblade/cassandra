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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.repair.CassandraTableRepairManager;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.LocalValidation;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Validates a table through {@code ValidationManager.doValidation} as a repair session does, once
 * with cursor validation on and once with it off, and asserts that both build the same Merkle
 * trees from the same sstables.  The validation counters must show that the first run took the
 * cursor path and the second did not, so a fallback cannot make the check compare the legacy path
 * with itself.
 */
final class SessionValidationParity
{
    /** The kinds of repair session whose validation selects or reads sstables differently. */
    enum SessionKind
    {
        FULL(false, PreviewKind.NONE),
        INCREMENTAL(true, PreviewKind.NONE),
        PREVIEW_REPAIRED(false, PreviewKind.REPAIRED),
        PREVIEW_UNREPAIRED(false, PreviewKind.UNREPAIRED),
        PREVIEW_ALL(false, PreviewKind.ALL),
        SNAPSHOT(false, PreviewKind.NONE),
        GLOBAL_SNAPSHOT(false, PreviewKind.NONE);

        final boolean isIncremental;
        final PreviewKind previewKind;

        SessionKind(boolean isIncremental, PreviewKind previewKind)
        {
            this.isIncremental = isIncremental;
            this.previewKind = previewKind;
        }

        @Override
        public String toString()
        {
            return name().toLowerCase().replace('_', '-');
        }
    }

    /** The purge settings each session is validated with. */
    private enum PurgeOption
    {
        DEFAULT, DONT_PURGE_TOMBSTONES, ONLY_PURGE_REPAIRED_TOMBSTONES
    }

    private SessionValidationParity()
    {
    }

    /** The whole ring; several ranges, one of them wrapping; and sixteen ranges, as with vnodes. */
    static List<List<Range<Token>>> rangeSets()
    {
        List<List<Range<Token>>> sets = new ArrayList<>();
        sets.add(Collections.singletonList(new Range<>(Murmur3Partitioner.MINIMUM, Murmur3Partitioner.MINIMUM)));
        sets.add(List.of(range(Long.MIN_VALUE / 2, -1000), range(0, Long.MAX_VALUE / 4), range(Long.MAX_VALUE / 2, Long.MIN_VALUE / 2 - 1)));
        List<Range<Token>> vnodes = new ArrayList<>();
        long step = Long.MAX_VALUE / 8;
        for (int i = 0; i < 16; i++)
            vnodes.add(range(Long.MIN_VALUE + i * step, i == 15 ? Long.MIN_VALUE : Long.MIN_VALUE + (i + 1) * step));
        sets.add(vnodes);
        return sets;
    }

    private static Range<Token> range(long left, long right)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }

    /**
     * Gives the live sstables a mix of repair states (unrepaired, pending for the session, repaired)
     * for the session kinds that select by repair state, validates on both paths, and puts the live
     * sstables back to unrepaired.
     */
    static void assertMerkleTreesMatch(ColumnFamilyStore cfs, SessionKind kind, long nowInSec) throws Exception
    {
        // every purge option over the whole ring; the other range sets with the default option,
        // for full and incremental sessions only, which keeps the matrix inside the test timeout
        List<List<Range<Token>>> rangeSets = rangeSets();
        // over the whole ring the session selects partitions; a purge can leave nothing to
        // validate, but some purge option must leave partitions
        long validated = 0;
        for (PurgeOption purge : PurgeOption.values())
        {
            long[] counts = assertMerkleTreesMatch(cfs, kind, rangeSets.get(0), purge, nowInSec, () -> {});
            assertTrue("the session selects no partition, so the check compares nothing: " + kind + " session, " + purge, counts[0] > 0);
            validated += counts[1];
        }
        assertTrue("the cursor path validated no partition with any purge option: " + kind + " session", validated > 0);
        if (kind == SessionKind.FULL || kind == SessionKind.INCREMENTAL)
            for (int i = 1; i < rangeSets.size(); i++)
                assertMerkleTreesMatch(cfs, kind, rangeSets.get(i), PurgeOption.DEFAULT, nowInSec, () -> {});
    }

    /**
     * Like {@link #assertMerkleTreesMatch(ColumnFamilyStore, SessionKind, long)} over the whole ring
     * only, with {@code afterSnapshot} run after the snapshots are taken and before the
     * validations.  A snapshot session must still validate what the snapshot holds.
     */
    static void assertMerkleTreesMatch(ColumnFamilyStore cfs, SessionKind kind, long nowInSec, Runnable afterSnapshot) throws Exception
    {
        long[] counts = assertMerkleTreesMatch(cfs, kind, rangeSets().get(0), PurgeOption.DEFAULT, nowInSec, afterSnapshot);
        assertTrue("the session selects no partition, so the check compares nothing: " + kind + " session", counts[0] > 0);
        assertTrue("the cursor path validated no partition: " + kind + " session", counts[1] > 0);
    }

    /** @return the partitions the session selects, by the sstables' estimates, and the partitions the cursor path validated */
    private static long[] assertMerkleTreesMatch(ColumnFamilyStore cfs, SessionKind kind, List<Range<Token>> ranges,
                                               PurgeOption purge, long nowInSec, Runnable afterSnapshot) throws Exception
    {
        TimeUUID parentId = TimeUUID.Generator.nextTimeUUID();
        // a session snapshot is cleared when its validation ends, so each run has its own session
        TimeUUID cursorSessionId = TimeUUID.Generator.nextTimeUUID();
        TimeUUID legacySessionId = TimeUUID.Generator.nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(parentId, FBUtilities.getBroadcastAddressAndPort(), List.of(cfs),
                                                                   ranges, kind.isIncremental, UNREPAIRED_SSTABLE, true, kind.previewKind);
        Map<String, String> originalCompaction = cfs.getCompactionParameters();
        boolean originalCursor = DatabaseDescriptor.cursorRepairEnabled();
        List<SSTableReader> marked = markRepairStates(cfs, kind, parentId);
        try
        {
            if (purge == PurgeOption.ONLY_PURGE_REPAIRED_TOMBSTONES)
            {
                Map<String, String> options = new HashMap<>(originalCompaction);
                options.put("only_purge_repaired_tombstones", "true");
                cfs.setCompactionParameters(options);
            }
            boolean dontPurge = purge == PurgeOption.DONT_PURGE_TOMBSTONES;
            String where = kind + " session, ranges " + ranges + ", " + purge;

            if (kind == SessionKind.SNAPSHOT)
            {
                new CassandraTableRepairManager(cfs).snapshot(cursorSessionId.toString(), ranges, true);
                new CassandraTableRepairManager(cfs).snapshot(legacySessionId.toString(), ranges, true);
            }
            else if (kind == SessionKind.GLOBAL_SNAPSHOT)
            {
                new CassandraTableRepairManager(cfs).snapshot(parentId.toString(), ranges, true);
            }

            long expectedPartitions = 0;
            long expectedBytes = 0;
            for (SSTableReader sstable : expectedSSTables(cfs, kind, parentId, ranges))
            {
                for (Range<Token> range : ranges)
                    expectedPartitions += sstable.estimatedKeysForRanges(Collections.singleton(range));
                for (SSTableReader.PartitionPositionBounds bounds : sstable.getPositionsForRanges(ranges))
                    expectedBytes += bounds.upperPosition - bounds.lowerPosition;
            }

            afterSnapshot.run();

            LocalValidation.Result cursor = validate(cfs, kind, parentId, cursorSessionId, ranges, nowInSec, dontPurge, true, where);
            LocalValidation.Result legacy = validate(cfs, kind, parentId, legacySessionId, ranges, nowInSec, dontPurge, false, where);

            assertEquals("the cursor path validated other sstables than the session selects: " + where, expectedPartitions, cursor.estimatedPartitions);
            assertEquals("the cursor path validated other sstables than the session selects: " + where, expectedBytes, cursor.estimatedTotalBytes);
            assertEquals("the legacy path validated other sstables than the session selects: " + where, expectedPartitions, legacy.estimatedPartitions);
            assertEquals("the legacy path validated other sstables than the session selects: " + where, expectedBytes, legacy.estimatedTotalBytes);
            assertEquals("partitions validated differ: " + where, legacy.partitionsProcessed, cursor.partitionsProcessed);
            assertEquals("Merkle tree row counts differ: " + where, legacy.rowCount, cursor.rowCount);
            assertArrayEquals("Merkle trees differ: " + where, legacy.serializedTrees, cursor.serializedTrees);
            return new long[]{ expectedPartitions, cursor.partitionsProcessed };
        }
        finally
        {
            DatabaseDescriptor.setCursorRepairEnabled(originalCursor);
            cfs.setCompactionParameters(originalCompaction);
            if (!marked.isEmpty())
                cfs.getCompactionStrategyManager().mutateRepaired(marked, UNREPAIRED_SSTABLE, NO_PENDING_REPAIR, false);
            if (kind == SessionKind.GLOBAL_SNAPSHOT)
                SnapshotManager.instance.clearSnapshot(cfs.getKeyspaceName(), cfs.getTableName(), parentId.toString());
            ActiveRepairService.instance().removeParentRepairSession(parentId);
        }
    }

    /** One validation on the path asked for, checking with the counters that this path ran. */
    private static LocalValidation.Result validate(ColumnFamilyStore cfs, SessionKind kind, TimeUUID parentId, TimeUUID sessionId,
                                                   List<Range<Token>> ranges, long nowInSec, boolean dontPurge, boolean cursor,
                                                   String where) throws Exception
    {
        DatabaseDescriptor.setCursorRepairEnabled(cursor);
        long runsBefore = CassandraTableRepairManager.cursorValidationsRun.sum();
        long fallbacksBefore = CassandraTableRepairManager.cursorValidationFallbacks.sum();
        LocalValidation.Result result = LocalValidation.validate(cfs, parentId, sessionId, ranges, kind.isIncremental, kind.previewKind, nowInSec, dontPurge);
        assertEquals((cursor ? "the cursor path did not run: " : "the cursor path ran with cursor validation off: ") + where,
                     cursor ? 1 : 0, CassandraTableRepairManager.cursorValidationsRun.sum() - runsBefore);
        assertEquals("cursor validation fell back to the legacy path: " + where,
                     0, CassandraTableRepairManager.cursorValidationFallbacks.sum() - fallbacksBefore);
        return result;
    }

    /**
     * For the session kinds that select by repair state, makes every third live sstable pending
     * for the session and every third repaired, so the selection has something to exclude.  First
     * it flushes the live data again until there are at least two sstables, or six for these
     * kinds, so each repair state gets at least two sstables that overlap.
     */
    private static List<SSTableReader> markRepairStates(ColumnFamilyStore cfs, SessionKind kind, TimeUUID parentId) throws Exception
    {
        boolean byRepairState = kind.isIncremental || kind.previewKind.isPreview();
        // every kind merges at least two overlapping sstables; the kinds that select by repair
        // state get two in each state
        int wanted = byRepairState ? 6 : 2;
        // background compaction would merge the new sstables away
        cfs.disableAutoCompaction();
        for (int flushes = 0; cfs.getLiveSSTables().size() < wanted; flushes++)
        {
            assertTrue("could not reach " + wanted + " live sstables", flushes < 6);
            flushLiveDataAgain(cfs);
        }
        if (!byRepairState)
            return Collections.emptyList();
        List<SSTableReader> live = new ArrayList<>(cfs.getLiveSSTables());
        live.sort(Comparator.comparing(s -> s.descriptor.id.toString()));
        List<SSTableReader> pending = new ArrayList<>();
        List<SSTableReader> repaired = new ArrayList<>();
        for (int i = 0; i < live.size(); i++)
        {
            if (i % 3 == 1)
                pending.add(live.get(i));
            else if (i % 3 == 2)
                repaired.add(live.get(i));
        }
        if (!pending.isEmpty())
            cfs.getCompactionStrategyManager().mutateRepaired(pending, UNREPAIRED_SSTABLE, parentId, false);
        if (!repaired.isEmpty())
            cfs.getCompactionStrategyManager().mutateRepaired(repaired, FBUtilities.nowInSeconds() * 1000L, NO_PENDING_REPAIR, false);
        List<SSTableReader> marked = new ArrayList<>(cfs.getLiveSSTables());
        marked.removeIf(s -> !s.isRepaired() && !s.isPendingRepair());
        return marked;
    }

    /**
     * Writes every partition of the live sstables, as it is on disk, to the memtable and flushes
     * it, so the new sstable overlaps the old ones on every key.  The cells keep their timestamps
     * and deletion times, so the merged data does not change.
     */
    private static void flushLiveDataAgain(ColumnFamilyStore cfs)
    {
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        new Mutation(PartitionUpdate.fromIterator(partition, ColumnFilter.all(cfs.metadata()))).applyUnsafe();
                    }
                }
            }
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    /** The sstables the session must validate, worked out here without the repair code. */
    private static Collection<SSTableReader> expectedSSTables(ColumnFamilyStore cfs, SessionKind kind, TimeUUID parentId, List<Range<Token>> ranges)
    {
        Predicate<SSTableReader> selected;
        switch (kind)
        {
            case INCREMENTAL:
                selected = s -> parentId.equals(s.getPendingRepair());
                break;
            case PREVIEW_REPAIRED:
                // a pending sstable of a session this node does not know is left out
                selected = s -> s.isRepaired() && !s.isPendingRepair();
                break;
            case PREVIEW_UNREPAIRED:
                selected = s -> !s.isRepaired();
                break;
            default:
                // full, preview ALL, and both snapshot kinds: the snapshot holds the same sstables
                selected = s -> true;
        }
        List<SSTableReader> expected = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (selected.test(sstable) && new Bounds<>(sstable.getFirst().getToken(), sstable.getLast().getToken()).intersects(ranges))
                expected.add(sstable);
        return expected;
    }
}
