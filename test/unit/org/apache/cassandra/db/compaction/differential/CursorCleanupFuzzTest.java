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
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Seeded fuzz tester for cursor-vs-legacy cleanup parity.  Samples the content cube (partition
 * width, row width, tombstone shape) and the ownership layout, then asserts byte-identity of the
 * surviving output between the legacy cleanup path and the cursor path.
 *
 * <p>Follows the {@code RandomSchemaTest.Builder} idiom shared with the other cursor branches: a
 * default seed of {@code System.currentTimeMillis()}, a fixed-seed override for regression tests,
 * and a {@code PropertyError} that prints the seed on failure.
 *
 * <p>The strategy in TESTING.md requires three things this test honours:
 * <ul>
 *   <li>Every example forces at least one deletion feature and one collection feature.  These are
 *       the paths where the two cleanup paths can diverge, so they are never absent.</li>
 *   <li>An empty, unsupported, or unengaged draw is resampled within a bounded retry count, not
 *       dropped silently, so a small example count still gives real coverage.</li>
 *   <li>A failing example is minimized by a hand-rolled greedy shrinker that runs automatically,
 *       because QuickTheories shrinking is off repo-wide (CASSANDRA-15554).</li>
 * </ul>
 *
 * <p>The clock is not injectable through {@code performCleanupOne}; it reads
 * {@code FBUtilities.nowInSeconds()} internally.  So the fuzz pins determinism a different way: it
 * uses a high {@code gc_grace_seconds} so no tombstone is purgeable, and every example carries
 * explicit write timestamps.  The output is then independent of the wall clock.  Purge-versus-clock
 * behaviour stays in the named anchors {@code partialPartitionPurge} and {@code tombstonesPurged},
 * which pin the boundary by waiting; folding those into a clock-free fuzz would need a production
 * clock seam, which is out of scope here.
 *
 * <p>This is a unit-tier smoke check with a small example count and fixed checked-in seeds.  Wide,
 * block-indexed and 100K-plus partitions belong in test/long and test/burn.
 */
public class CursorCleanupFuzzTest extends CursorCleanupDifferentialTester
{
    // Fixed regression seeds discovered by past fuzz runs.  Append a new seed here when a fuzz
    // failure is reduced and persisted as a regression test.
    private static final long[] REGRESSION_SEEDS = new long[0];

    // Small example count for the unit-tier smoke check.  test/long uses a larger count.
    private static final int EXAMPLES_UNIT = 10;

    // Bounded resamples before an example is skipped.  A draw that resolves empty, unsupported, or
    // unengaged is redrawn, not dropped.
    private static final int MAX_RESAMPLE_ATTEMPTS = 8;

    // High gc_grace so nothing is purgeable: the output does not race the wall clock.
    private static final long GC_GRACE_SECONDS = 864000;
    private static final long BASE_TIMESTAMP = 1_000_000;
    private static final long TOMBSTONE_TIMESTAMP = BASE_TIMESTAMP + 2000;

    @Test
    public void fuzzCleanupParity() throws Throwable
    {
        long seed = System.currentTimeMillis();
        logger.info("fuzzCleanupParity seed={}", seed);
        runFuzz(seed, EXAMPLES_UNIT);
    }

    @Test
    public void regressionSeeds() throws Throwable
    {
        for (long seed : REGRESSION_SEEDS)
        {
            logger.info("fuzzCleanupParity regression seed={}", seed);
            runFuzz(seed, 1);
        }
    }

    private void runFuzz(long seed, int examples) throws Throwable
    {
        // Split the seed once, so changing the ownership draw does not reshuffle the dataset.
        Random dataSeedRng = new Random(seed);
        Random querySeedRng = new Random(seed ^ 0x0C1EA0C1EA0L);

        for (int i = 0; i < examples; i++)
        {
            long dataSeed = dataSeedRng.nextLong();
            long querySeed = querySeedRng.nextLong();
            runExample(seed, dataSeed, querySeed, i);
        }
    }

    // Draw, resample, verify, and on failure minimize.  Each attempt advances the same RNG, so the
    // sequence is deterministic and a seed reproduces it exactly.
    private void runExample(long seed, long dataSeed, long querySeed, int index) throws Throwable
    {
        Random dataRng = new Random(dataSeed);
        Random queryRng = new Random(querySeed);

        for (int attempt = 0; attempt < MAX_RESAMPLE_ATTEMPTS; attempt++)
        {
            Example example = drawExample(dataRng, queryRng);
            try
            {
                verify(example);
                return;
            }
            catch (SkipExample skip)
            {
                // Empty, unsupported, or unengaged draw.  Resample from the advanced RNG.
            }
            catch (AssertionError failure)
            {
                Example minimal = shrink(example);
                logger.error("fuzzCleanupParity minimized failing example to {}", minimal);
                throw new PropertyError(seed, dataSeed, querySeed, index, minimal, failure);
            }
        }
        logger.info("Skipping example index={} dataSeed={} after {} resample attempts",
                    index, dataSeed, MAX_RESAMPLE_ATTEMPTS);
    }

    // A fully-specified example.  Every field is deterministic, so verify() and the shrinker rebuild
    // identical bytes and the identical ownership from it.
    private static final class Example
    {
        final int partitions;
        final int rowsPerPartition;
        final int tombstoneKind;      // 0 cell, 1 row, 2 range, 3 partition
        final int ownershipMode;      // 0 contiguous-lower, 1 disjoint islands, 2 everything owned
        final int ownedCount;         // how many partitions this node keeps (>= 1)

        Example(int partitions, int rowsPerPartition, int tombstoneKind, int ownershipMode, int ownedCount)
        {
            this.partitions = partitions;
            this.rowsPerPartition = rowsPerPartition;
            this.tombstoneKind = tombstoneKind;
            this.ownershipMode = ownershipMode;
            this.ownedCount = ownedCount;
        }

        Example with(int newPartitions, int newRowsPerPartition, int newTombstoneKind, int newOwnershipMode, int newOwnedCount)
        {
            return new Example(newPartitions, newRowsPerPartition, newTombstoneKind, newOwnershipMode, newOwnedCount);
        }

        @Override
        public String toString()
        {
            return String.format("Example{partitions=%d, rowsPerPartition=%d, tombstoneKind=%d, ownershipMode=%d, ownedCount=%d}",
                                 partitions, rowsPerPartition, tombstoneKind, ownershipMode, ownedCount);
        }
    }

    private Example drawExample(Random dataRng, Random queryRng)
    {
        // Floor partitions at two and rows at one so the forced features apply and there is always
        // something to own and something to consider dropping.
        int partitions = samplePartitionCount(dataRng);
        int rowsPerPartition = sampleRowCount(dataRng);
        int tombstoneKind = legalTombstoneKind(dataRng.nextInt(4), rowsPerPartition);

        int ownershipMode = queryRng.nextInt(3);
        // Everything-owned keeps all; the other modes keep between one and all partitions.
        int ownedCount = ownershipMode == 2 ? partitions : 1 + queryRng.nextInt(partitions);
        return new Example(partitions, rowsPerPartition, tombstoneKind, ownershipMode, ownedCount);
    }

    // Build the dataset for an example, guard cursor engagement, and assert byte-identity of the
    // surviving output.  Throws SkipExample if the draw resolves empty, unsupported, or unengaged;
    // throws AssertionError on a real parity mismatch.
    private void verify(Example example) throws Throwable
    {
        // Every example carries a collection column (forced) and a deletion (forced below).  High
        // gc_grace so nothing purges, making the output clock-independent.
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, m map<text, bigint>, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = " + GC_GRACE_SECONDS);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int pk = 0; pk < example.partitions; pk++)
            for (int ck = 0; ck < example.rowsPerPartition; ck++)
                execute("INSERT INTO %s (pk, ck, v, m) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        (long) pk, (long) ck, "v" + ck, Collections.singletonMap("k" + ck, (long) ck), BASE_TIMESTAMP + ck);

        // Forced deletion feature: apply the sampled tombstone kind to the even partitions, so some
        // partitions carry a deletion and some do not.
        for (int pk = 0; pk < example.partitions; pk += 2)
            applyTombstone(example.tombstoneKind, pk, example.rowsPerPartition);

        // One flush, so the harness sees exactly one input sstable.
        flush();

        List<DecoratedKey> keys = partitionKeysInTokenOrder(cfs);
        if (keys.size() != example.partitions)
            throw new SkipExample();   // an unexpected merge; resample rather than assert on it

        Collection<Range<Token>> owned = ownedRanges(example, keys);

        // Guard: the cursor must actually engage, or the harness compares legacy vs legacy.  Check
        // the same two conditions production checks; resample a draw that would not engage.
        SSTableReader input = cfs.getLiveSSTables().iterator().next();
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        try (CompactionController controller = new CompactionController(cfs, cfs.getLiveSSTables(), gcBefore))
        {
            if (!CursorCompactor.isCleanupSupported(cfs.getLiveSSTables(), controller))
                throw new SkipExample();
        }
        if (input.getPositionsForRanges(owned).isEmpty())
            throw new SkipExample();

        // A real parity mismatch surfaces here as an AssertionError from the harness.
        assertCursorCleanupMatchesLegacy(cfs, owned);
    }

    // Build the owned-range set for the drawn ownership layout, rendered through the shared
    // rangesOwning helper so the fuzz and the matrix feed one renderer.
    private Collection<Range<Token>> ownedRanges(Example example, List<DecoratedKey> keys)
    {
        int owned = Math.min(example.ownedCount, keys.size());
        List<DecoratedKey> keep = new ArrayList<>();
        switch (example.ownershipMode)
        {
            case 1:  // disjoint islands: keep every other key, so ownership breaks into segments
                for (int i = 0; i < keys.size() && keep.size() < owned; i += 2)
                    keep.add(keys.get(i));
                if (keep.isEmpty())
                    keep.add(keys.get(0));
                break;
            case 2:  // everything owned
                keep.addAll(keys);
                break;
            default: // contiguous lower fraction
                keep.addAll(keys.subList(0, Math.max(1, owned)));
                break;
        }
        return rangesOwning(keys, keep);
    }

    private void applyTombstone(int tombstoneKind, int pk, int rowsPerPartition) throws Throwable
    {
        switch (tombstoneKind)
        {
            case 0:  // cell tombstone
                execute("DELETE v FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", TOMBSTONE_TIMESTAMP, (long) pk, 0L);
                break;
            case 1:  // row tombstone
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", TOMBSTONE_TIMESTAMP, (long) pk, 0L);
                break;
            case 2:  // range tombstone
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck > 0 AND ck < ?",
                        TOMBSTONE_TIMESTAMP, (long) pk, (long) (rowsPerPartition - 1));
                break;
            default: // partition tombstone
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", TOMBSTONE_TIMESTAMP, (long) pk);
                break;
        }
    }

    // Hand-rolled greedy shrinker that runs automatically on failure.  It repeatedly tries one
    // reduction; it keeps the reduction only if the same assertion still fails.  A reduction that
    // passes, or that turns the run unengaged (SkipExample), is rejected, because a
    // legacy-vs-legacy run hides the bug.  Timestamps and gc_grace never change, so a reduction
    // stays reproducible.
    private Example shrink(Example failing)
    {
        Example best = failing;
        boolean improved = true;
        while (improved)
        {
            improved = false;
            for (Example candidate : reductions(best))
            {
                if (stillFails(candidate))
                {
                    best = candidate;
                    improved = true;
                    break;
                }
            }
        }
        return best;
    }

    private List<Example> reductions(Example best)
    {
        List<Example> candidates = new ArrayList<>();
        // Collapse disjoint islands to a contiguous owned fraction first; simpler ownership reads
        // more clearly.
        if (best.ownershipMode != 0)
            candidates.add(best.with(best.partitions, best.rowsPerPartition, best.tombstoneKind, 0,
                                     Math.min(best.ownedCount, best.partitions)));
        // Binary-search the partition count down, keeping the owned count legal for the new width.
        if (best.partitions > 2)
        {
            int fewerPartitions = Math.max(2, best.partitions / 2);
            int cappedOwned = Math.max(1, Math.min(best.ownedCount, fewerPartitions));
            candidates.add(best.with(fewerPartitions, best.rowsPerPartition, best.tombstoneKind, best.ownershipMode, cappedOwned));
        }
        // Binary-search the row count down, keeping the tombstone kind legal for the new width.
        if (best.rowsPerPartition > 1)
        {
            int fewerRows = Math.max(1, best.rowsPerPartition / 2);
            candidates.add(best.with(best.partitions, fewerRows, legalTombstoneKind(best.tombstoneKind, fewerRows),
                                     best.ownershipMode, best.ownedCount));
        }
        // Simplify the deletion feature-class toward a whole-partition tombstone.
        if (best.tombstoneKind != 3)
            candidates.add(best.with(best.partitions, best.rowsPerPartition, 3, best.ownershipMode, best.ownedCount));
        return candidates;
    }

    private boolean stillFails(Example candidate)
    {
        try
        {
            verify(candidate);
            return false;   // reduction passed: reject it
        }
        catch (SkipExample skip)
        {
            return false;   // reduction dropped below the gate: reject it
        }
        catch (AssertionError failure)
        {
            return true;    // reduction still fails the same way: keep it
        }
        catch (Throwable unexpected)
        {
            return false;   // an unrelated error is not the failure we are minimizing
        }
    }

    private int samplePartitionCount(Random rng)
    {
        // Stratified: few (2-5) or several (6-12).  Floored at two so ownership always has a choice.
        int tier = rng.nextInt(100);
        if (tier < 55)
            return 2 + rng.nextInt(4);
        return 6 + rng.nextInt(7);
    }

    private int sampleRowCount(Random rng)
    {
        // Stratified: one, few (2-5), or several (6-20).  Floored at one for forced features.  Wide,
        // block-indexed partitions belong in test/long and test/burn.
        int tier = rng.nextInt(100);
        if (tier < 20)
            return 1;
        else if (tier < 70)
            return 2 + rng.nextInt(4);
        else
            return 6 + rng.nextInt(15);
    }

    // A range tombstone needs at least three rows.  Fall back to a whole-partition tombstone when
    // the width is too small.
    private int legalTombstoneKind(int drawn, int rowsPerPartition)
    {
        if (drawn == 2 && rowsPerPartition <= 2)
            return 3;
        return drawn;
    }

    // Thrown when a draw resolves to an empty, unsupported, or unengaged dataset.  The caller
    // resamples.
    private static final class SkipExample extends RuntimeException
    {
        SkipExample()
        {
            super(null, null, false, false);
        }
    }

    /**
     * PropertyError wrapping an assertion failure, printing the seed and the minimized example.
     * Follows the {@code RandomSchemaTest.PropertyError} idiom.
     */
    public static class PropertyError extends AssertionError
    {
        public PropertyError(long seed, long dataSeed, long querySeed, int exampleIndex, Example minimal, Throwable cause)
        {
            super(String.format("Fuzz failure at example %d: seed=%d dataSeed=%d querySeed=%d minimal=%s",
                                exampleIndex, seed, dataSeed, querySeed, minimal), cause);
        }
    }
}
