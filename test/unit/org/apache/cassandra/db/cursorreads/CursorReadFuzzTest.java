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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Rng;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Runs {@link CursorReadFuzzRunner}.  Properties, all under {@code cassandra.test.cursor_fuzz.}:
 * <ul>
 *   <li>{@code seed}: the master seed (default: random, logged);</li>
 *   <li>{@code max_cases} (default {@value #DEFAULT_MAX_CASES}) and {@code duration} in seconds
 *       (default {@value #DEFAULT_DURATION_SECONDS}; 0 for none): the run stops at whichever comes first;</li>
 *   <li>{@code profile}: big, bti, mixed, or all (default; the cases cycle through the three);</li>
 *   <li>{@code rows} (default {@value #DEFAULT_ROWS}): the most rows a case writes per partition;</li>
 *   <li>{@code queries} (default {@value #DEFAULT_QUERIES}): reads per case;</li>
 *   <li>{@code min_cell_hits} (default 1): hits every coverage cell needs; checked only when the
 *       run reached {@code max_cases};</li>
 *   <li>{@code max_failures} (default 1): failing cases to collect before the run stops;</li>
 *   <li>{@code case_seed}: replay one case, or a comma-separated list of {@code seed:profile}, and nothing else.</li>
 * </ul>
 */
public class CursorReadFuzzTest extends CursorReadFuzzRunner
{
    private static final Logger logger = LoggerFactory.getLogger(CursorReadFuzzTest.class);

    private static final int DEFAULT_MAX_CASES = 100_000;
    private static final int DEFAULT_DURATION_SECONDS = 240;
    private static final int DEFAULT_ROWS = 200;
    private static final int DEFAULT_QUERIES = 6;
    private static final String PREFIX = "cassandra.test.cursor_fuzz.";

    /** Case seeds that once failed, with the profile they ran under.  Add one when a bug is fixed; never remove one. */
    private static final Object[][] PINNED_CASE_SEEDS = {
        // fetched-but-not-queried collection cells kept their values
        { 5833679380957638813L, "bti" },
        { 3892645080117226033L, "mixed" },
        { -8412065011434420316L, "mixed" },
        { -3230866729326374124L, "big" },
        // a names read of one BIG sstable wrote an empty static row
        { -8564670916400471027L, "mixed" },
        { 617974430352275367L, "mixed" },
        // a memtable-only names read reported every fetched column
        { -7995527694508729151L, "big" },
        { -3572946980067025172L, "mixed" },
        { -9186087665489710237L, "bti" },
        // an sstable read only for the static row merged its stats into the response header
        { -8044424495745788820L, "mixed" },
        { 8931982579565633784L, "big" },
        { -2559686159325057763L, "bti" },
        // a read aborted by the tombstone guard counted as a merge stopped by a limit
        { -1768891337553457458L, "big" },
        // a partition the filter drops read none of the rows the iterator path reads to see whether it is empty
        { 2969407545727337054L, "big" },
        { 2163628750231642383L, "bti" },
        { -4320185034446256667L, "bti" },
        { -1325528233879201249L, "bti" },
        { 1909528591915673289L, "big" },
        { 6535614974153774322L, "big" },
        // a repaired leg's first row went into the repaired-data digest of a partition the filter drops
        { 5054348937493764749L, "mixed" },
        // a reversed BIG read kept an unqueried collection cell older than the row
        { -3334440741450517216L, "big" },
        // a limited names read that tracks repaired data read an unrepaired sstable the iterator path never opened
        { 9185961531705733982L, "bti" },
        // a filtered read over the tombstone warning dropped every row, and the warning had no partition key
        { 3398697307496197883L, "big" },
        // a read whose partition the key filter drops opened an sstable the iterator path never read
        { 1616967172746670323L, "bti" },
        // the scan metrics counted a filter-dropped counter row as live although its deletion wins
        { 6685983846420660757L, "mixed" },
        // the replica response wrote a cell of a dropped column the response header does not carry
        { 3150292922389885837L, "bti" },
        // a paged read resumed in a seeked range deletion met a deletion from the partition start
        { 2359448369874533901L, "bti" },
        // a tracked read opened its lone repaired leg for a partition the key filter drops
        { 3260241938575584980L, "mixed" },
        // a BIG reversed read kept a dropped column's cell written after the drop
        { -6167232116629285161L, "mixed" },
        // a reversed tracked read gave its repaired sstable, read only for the partition deletion, in forward order
        { 6251709150715830397L, "bti" },
        { -3896690194078956364L, "bti" },
        // a tracked read whose filter drops the partition read the repaired leg past a deletion open at the slice start
        { 3020695180115019234L, "big" },
        // a tracked names read opened an sstable whose data starts after the slice end
        { 6022817748842731445L, "big" },
        // a filtered read aborted by the tombstone guard before it gave the open marker at the slice start
        { 6415220793583911390L, "big" },
        // a names read served as a replica response kept an unqueried collection cell older than the row
        { -8258155592852026587L, "bti" },
        // a tracked names read whose filter drops the partition read the repaired merge past the open marker at the slice start
        { 7457694882470460771L, "mixed" },
        // a read left an sstable whose data starts after the last slice unopened, which the iterator path opens
        { -3256148832156277723L, "big" },
        // a filtered read failed on the tombstone threshold while it was set up, before the scan metrics stage existed
        { 507948354839892433L, "mixed" },
        // the iterator run filled the key cache, so the cursor run's reverse read skipped a BIG sstable
        { -4028466236970603159L, "big" },
        // a tracked names read ended at the slice end without opening a BIG sstable whose data starts after it
        { -5106089928704663554L, "big" },
        // a row read as stored lost a collection column whose every cell the read skipped
        { -6365199718735513617L, "mixed" },
        // the scan metrics counted a filtered memtable row as live for a collection deletion the purge removes
        { 3527823157899743341L, "big" },
        // the read size guardrail counted the command's partition key object instead of the memtable's
        { 4989549375438934090L, "big" },
        // the read size guardrail counted a merged counter cell made like an sstable cell
        { -8773897658099927984L, "bti" },
        // the read size guardrail counted a row of a table with no clustering columns with an empty clustering of no size
        { 6382939220866137718L, "big" },
        // the transcode path checked the row index size of an sstable it opened after the first row
        { 3635082653552388141L, "big" },
        // a reversed names read merged the cursor legs forward, so the merged row kept another source's clustering
        { -8246439728903787617L, "bti" },
        { -5802131501641253130L, "bti" },
        // a row holding only a collection deletion older than the column's drop counted as a version of the row
        { 3284963571533047855L, "mixed" },
        { 877818483186668942L, "bti" },
        // a reversed read tracking repaired data merged a forward stand-in for a repaired sstable without the partition
        { 5111802420062607100L, "big" },
        // a merge of a memtable holding only a static row with an empty stand-in reported every fetched column
        { -7713608765787003579L, "big" },
        // a merge whose row filter dropped the first row read on with the current command set,
        // so it counted row index sizes the iterator path does not
        { -5383455239024913570L, "big" },
        { 6364048647847548743L, "big" },
    };

    private static String property(String name)
    {
        return System.getProperty(PREFIX + name); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    }

    private static long longProperty(String name, long defaultValue)
    {
        String value = property(name);
        return value == null || value.isEmpty() ? defaultValue : Long.parseLong(value);
    }

    static RunSettings settingsFromProperties(long defaultMaster)
    {
        String profile = property("profile");
        return new RunSettings(longProperty("seed", defaultMaster),
                               profile == null || profile.isEmpty() ? "all" : profile,
                               (int) longProperty("max_cases", DEFAULT_MAX_CASES),
                               longProperty("duration", DEFAULT_DURATION_SECONDS),
                               (int) longProperty("rows", DEFAULT_ROWS),
                               (int) longProperty("queries", DEFAULT_QUERIES),
                               (int) longProperty("max_failures", 1));
    }

    @Test
    public void fuzz()
    {
        Assume.assumeTrue("a single case is being replayed", property("case_seed") == null);
        RunSettings settings = settingsFromProperties(System.nanoTime());
        int cases = fuzz(settings);
        logger.info("cursor fuzz ran {} cases ({})", cases, settings);
        assertTrue("the fuzz run ran no case", cases > 0);
        if (cases >= settings.maxCases)
            assertCoverage(longProperty("min_cell_hits", 1));
        else
            logger.info("the run stopped at its duration before {} cases; coverage is logged, not checked", settings.maxCases);
    }

    /** Replays {@code case_seed}: one seed, or a comma-separated list of {@code seed:profile}. */
    @Test
    public void replayCaseSeed()
    {
        String caseSeeds = property("case_seed");
        Assume.assumeTrue("no case seed to replay", caseSeeds != null);
        String profile = property("profile");
        RunSettings settings = settingsFromProperties(0);
        List<AssertionError> failures = new ArrayList<>();
        for (String entry : caseSeeds.split(","))
        {
            String[] parts = entry.trim().split(":");
            String caseProfile = parts.length > 1 ? parts[1] : profile == null || profile.equals("all") ? "mixed" : profile;
            try
            {
                runReportingFailure(CursorReadFuzzGenerator.generate(Long.parseLong(parts[0]), caseProfile, settings.rows, settings.queries));
            }
            catch (AssertionError e)
            {
                logger.error("replayed case {} failed", entry, e);
                failures.add(e);
            }
        }
        if (!failures.isEmpty())
            throw failures.get(0);
    }

    @Test
    public void pinnedCaseSeeds()
    {
        for (Object[] pinned : PINNED_CASE_SEEDS)
            runReportingFailure(CursorReadFuzzGenerator.generate((Long) pinned[0], (String) pinned[1], DEFAULT_ROWS, DEFAULT_QUERIES));
    }

    @Test
    public void pickIndexStaysInRangeAndReachesBothEnds()
    {
        Rng r = new Rng(42);
        for (int n : new int[]{ 1, 2, 3, 7, 64 })
        {
            boolean[] seen = new boolean[n];
            for (int i = 0; i < 10_000; i++)
            {
                int x = r.pickIndex(n);
                assertTrue(x + " out of [0, " + n + ')', x >= 0 && x < n);
                seen[x] = true;
            }
            for (int i = 0; i < n; i++)
                assertTrue("pickIndex(" + n + ") never returned " + i, seen[i]);
        }
        for (int i = 0; i < 1000; i++)
        {
            int x = r.between(-1, 1);
            assertTrue(x >= -1 && x <= 1);
        }
        assertEquals(CursorReadFuzzModel.splitmix64(7), CursorReadFuzzModel.splitmix64(7));
    }
}
