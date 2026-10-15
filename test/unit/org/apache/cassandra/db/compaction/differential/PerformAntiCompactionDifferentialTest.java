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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.range;
import static org.apache.cassandra.db.compaction.differential.AntiCompactionRanges.ranges;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@code CompactionManager.performAnticompaction}, the entry point a repair session uses, on the
 * cursor path against the legacy path, on BIG and BTI and with each compaction strategy.  The
 * session holds several groups of sstables, one sstable fully inside a repaired range (which gets
 * a metadata change only), and an already repaired sstable that stays out of the session.  Every
 * live sstable afterwards must match between the paths, and the cursor path must run once for
 * each group.
 */
@RunWith(Parameterized.class)
public class PerformAntiCompactionDifferentialTest extends DifferentialCompactionTester
{
    private static final int SSTABLES = 7;
    private static final int PARTITIONS_PER_SSTABLE = 30;

    @Parameterized.Parameter(0)
    public String format;

    @Parameterized.Parameter(1)
    public String strategy;

    @Parameterized.Parameters(name = "{0}-{1}")
    public static List<Object[]> parameters()
    {
        List<Object[]> parameters = new ArrayList<>();
        for (String format : new String[]{ "big", "bti" })
            for (String strategy : new String[]{ "UnifiedCompactionStrategy", "SizeTieredCompactionStrategy", "LeveledCompactionStrategy", "TimeWindowCompactionStrategy" })
                parameters.add(new Object[]{ format, strategy });
        return parameters;
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
    public void sessionMatchesLegacy() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, m map<text, int>, PRIMARY KEY (pk, ck)) " +
                    "WITH compaction = {'class': '" + strategy + "'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // keys in token order, so each sstable can hold its own token range
        List<Long> keys = new ArrayList<>();
        for (long pk = 0; pk < SSTABLES * PARTITIONS_PER_SSTABLE; pk++)
            keys.add(pk);
        keys.sort(Comparator.comparing(pk -> cfs.getPartitioner().getToken(ByteBufferUtil.bytes(pk))));

        List<SSTableReader> flushed = new ArrayList<>();
        for (int i = 0; i < SSTABLES; i++)
        {
            // sstable i holds its own block; the first three also overlap the next block, so they merge
            int from = i * PARTITIONS_PER_SSTABLE;
            int to = Math.min(keys.size(), from + (i < 3 ? 2 : 1) * PARTITIONS_PER_SSTABLE);
            for (int k = from; k < to; k++)
                for (long ck = 0; ck < 3; ck++)
                    execute("INSERT INTO %s (pk, ck, v, m) VALUES (?, ?, ?, ?)", keys.get(k), ck + i, "v" + i, java.util.Map.of("k" + i, (int) ck));
            if (i == 1)
                execute("DELETE FROM %s WHERE pk = ? AND ck < ?", keys.get(from + 1), 2L);
            Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());
            flush();
            List<SSTableReader> fresh = new ArrayList<>(cfs.getLiveSSTables());
            fresh.removeAll(before);
            assertEquals(1, fresh.size());
            flushed.add(fresh.get(0));
        }
        if (strategy.startsWith("Leveled"))
        {
            // levels above 0: two sstables in level 1 and two in level 2, from disjoint token ranges
            setLevel(cfs, flushed.get(3), 1);
            setLevel(cfs, flushed.get(4), 1);
            setLevel(cfs, flushed.get(5), 2);
            setLevel(cfs, flushed.get(6), 2);
        }

        // sstable 6 is already repaired and stays out of the session.  LCS pairs level 0 sstables in
        // the order of a hash set, which differs between the runs, so under LCS sstable 2 is also
        // repaired, to leave a single pair in level 0.
        List<SSTableReader> repaired = strategy.startsWith("Leveled") ? List.of(flushed.get(2), flushed.get(6)) : List.of(flushed.get(6));
        cfs.getCompactionStrategyManager().mutateRepaired(repaired, FBUtilities.nowInSeconds() * 1000L, NO_PENDING_REPAIR, false);

        // sstable 4 lies wholly inside the full range; the other ranges cut through the rest
        SSTableReader contained = flushed.get(4);
        long containedFirst = contained.getFirst().getToken().getLongValue();
        long containedLast = contained.getLast().getToken().getLongValue();
        long firstKey = flushed.get(0).getFirst().getToken().getLongValue();
        long middleOfFirst = middleToken(flushed.get(0));
        long middleOf2 = middleToken(flushed.get(2));
        long middleOf3 = middleToken(flushed.get(3));
        long middleOf5 = middleToken(flushed.get(5));
        long lastOf5 = flushed.get(5).getLast().getToken().getLongValue();
        RangesAtEndpoint ranges = ranges(List.of(range(firstKey - 1, middleOfFirst), range(containedFirst - 1, containedLast), range(middleOf5, lastOf5)),
                                         List.of(range(middleOf2, middleOf3)));

        TimeUUID session = TimeUUID.Generator.nextTimeUUID();
        Path scratch = Files.createTempDirectory("perform-anticompaction");
        boolean passed = false;
        try
        {
            InputCopy copy = copyLiveSet(cfs, scratch.resolve("inputs"));
            int expectedGroups = expectedGroups(cfs, ranges);
            CapturedOutput legacy = runSession(cfs, ranges, session, false, expectedGroups, scratch.resolve("legacy"));
            // same descriptors, so both sessions group and merge the inputs in the same order
            restoreLiveSet(cfs, copy, true);
            CapturedOutput cursor = runSession(cfs, ranges, session, true, expectedGroups, scratch.resolve("cursor"));
            restoreLiveSet(cfs, copy);
            assertEquivalentOutputs(legacy, cursor);
            passed = true;
        }
        finally
        {
            if (passed)
                FileUtils.deleteDirectory(scratch.toFile());
        }
    }

    private static long middleToken(SSTableReader sstable)
    {
        List<Long> tokens = AntiCompactionRanges.tokens(List.of(sstable));
        return tokens.get(tokens.size() / 2);
    }

    private static void setLevel(ColumnFamilyStore cfs, SSTableReader sstable, int level) throws Exception
    {
        StatsMetadata old = sstable.getSSTableMetadata();
        sstable.mutateLevelAndReload(level);
        cfs.getTracker().notifySSTableMetadataChanged(sstable, old);
    }

    /** The groups the session must anticompact: its unrepaired sstables not wholly inside one range, grouped by the strategy. */
    private static int expectedGroups(ColumnFamilyStore cfs, RangesAtEndpoint ranges)
    {
        List<Range<Token>> normalized = Range.normalize(ranges.ranges());
        List<SSTableReader> toAntiCompact = new ArrayList<>();
        for (SSTableReader sstable : sessionSSTables(cfs, ranges))
        {
            boolean contained = false;
            for (Range<Token> range : normalized)
                contained |= range.contains(sstable.getFirst().getToken()) && range.contains(sstable.getLast().getToken());
            if (!contained)
                toAntiCompact.add(sstable);
        }
        int groups = cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(toAntiCompact).size();
        assertTrue("the session must anticompact more than one group, got " + groups, groups > 1);
        assertTrue("one sstable must lie wholly inside a range", toAntiCompact.size() < sessionSSTables(cfs, ranges).size());
        return groups;
    }

    /** Runs the session on one path, checks how often the cursor ran, and captures every live sstable. */
    private CapturedOutput runSession(ColumnFamilyStore cfs, RangesAtEndpoint ranges, TimeUUID session, boolean cursor,
                                      int expectedGroups, Path scratch) throws Exception
    {
        logger.info("{} session over {}", cursor ? "cursor" : "legacy", describe(sessionSSTables(cfs, ranges)));
        runAntiCompactionSession(cfs, ranges, session, cursor, expectedGroups);
        logger.info("{} session left {}", cursor ? "cursor" : "legacy", describe(cfs.getLiveSSTables()));
        return captureLiveSet(cfs, scratch);
    }

    /** The level, repair state and key bounds of each sstable, sorted, for the log. */
    private static List<String> describe(Iterable<SSTableReader> sstables)
    {
        List<String> described = new ArrayList<>();
        for (SSTableReader sstable : sstables)
            described.add("level=" + sstable.getSSTableLevel() + " repaired=" + sstable.isRepaired() + " pending=" + sstable.isPendingRepair() +
                          " transient=" + sstable.isTransient() + " first=" + sstable.getFirst().getToken() + " last=" + sstable.getLast().getToken());
        described.sort(String::compareTo);
        return described;
    }
}
