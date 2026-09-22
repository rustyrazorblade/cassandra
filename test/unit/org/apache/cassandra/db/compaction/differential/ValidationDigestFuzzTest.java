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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.DigestingCursorMergeSink;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.PrecomputedDigestPartition;
import org.apache.cassandra.db.repair.ValidationCompactionController;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Seeded fuzz tester for validation digest parity.  Samples the content cube (partition width,
 * row width, tombstone shape, purge/clock mode) and asserts per-partition digest parity between
 * the legacy compaction-iterator path and the cursor path.
 *
 * <p>Follows the RandomSchemaTest.Builder idiom: a default seed from System.currentTimeMillis(),
 * a fixed-seed override for regression tests, and a PropertyError that prints the seed on failure.
 *
 * <p>The strategy in TESTING.md requires three things this test honours:
 * <ul>
 *   <li>Every example forces at least one deletion feature and one collection feature.  These are
 *       the paths where the two validation paths can diverge, so they are never absent.</li>
 *   <li>An empty or unsupported draw is resampled within a bounded retry count, not dropped
 *       silently, so a small example count still gives real coverage.</li>
 *   <li>A failing example is minimized by a hand-rolled greedy shrinker that runs automatically,
 *       because QuickTheories shrinking is off repo-wide (CASSANDRA-15554).</li>
 * </ul>
 *
 * <p>This is a unit-tier smoke check: a small example count and fixed checked-in seeds.  The empty
 * partition edge lives in the matrix, not here, because every fuzz example carries data to force
 * its features.  Counters, statics, UDTs, and tuples are covered by the matrix base.  Wide,
 * block-indexed and 100K-plus partitions belong in test/long and test/burn.
 */
public class ValidationDigestFuzzTest extends CQLTester
{
    // Fixed regression seeds discovered by past fuzz runs.  Append a new seed here when a fuzz
    // failure is reduced and persisted as a regression test.
    private static final long[] REGRESSION_SEEDS = new long[0];

    // Small example count for the unit-tier smoke check.  test/long uses a larger count.
    private static final int EXAMPLES_UNIT = 10;

    // Bounded resamples before an example is skipped.  A draw that resolves empty or unsupported
    // is redrawn, not dropped.
    private static final int MAX_RESAMPLE_ATTEMPTS = 8;

    private static final long BASE_TIMESTAMP = 1_000_000;
    private static final long TOMBSTONE_TIMESTAMP = BASE_TIMESTAMP + 1000;

    @Test
    public void fuzzValidationDigestParity() throws Throwable
    {
        long seed = System.currentTimeMillis();
        logger.info("fuzzValidationDigestParity seed={}", seed);
        runFuzz(seed, EXAMPLES_UNIT);
    }

    @Test
    public void regressionSeeds() throws Throwable
    {
        for (long seed : REGRESSION_SEEDS)
        {
            logger.info("fuzzValidationDigestParity regression seed={}", seed);
            runFuzz(seed, 1);
        }
    }

    private void runFuzz(long seed, int examples) throws Throwable
    {
        Random dataSeedRng = new Random(seed);
        Random clockSeedRng = new Random(seed ^ 0x5EED5EEDL);

        for (int i = 0; i < examples; i++)
        {
            long dataSeed = dataSeedRng.nextLong();
            long clockSeed = clockSeedRng.nextLong();
            runExample(seed, dataSeed, clockSeed, i);
        }
    }

    // Draw, resample, verify, and on failure minimize.  Each attempt advances the same RNG, so the
    // sequence is deterministic and a seed reproduces it exactly.
    private void runExample(long seed, long dataSeed, long clockSeed, int index) throws Throwable
    {
        Random dataRng = new Random(dataSeed);
        Random clockRng = new Random(clockSeed);

        for (int attempt = 0; attempt < MAX_RESAMPLE_ATTEMPTS; attempt++)
        {
            Example example = drawExample(dataRng, clockRng);
            try
            {
                verify(example);
                return;
            }
            catch (SkipExample skip)
            {
                // Empty or unsupported draw.  Resample from the advanced RNG.
            }
            catch (AssertionError failure)
            {
                Example minimal = shrink(example);
                logger.error("fuzzValidationDigestParity minimized failing example to {}", minimal);
                throw new PropertyError(seed, dataSeed, clockSeed, index, minimal, failure);
            }
        }
        logger.info("Skipping example index={} dataSeed={} after {} resample attempts",
                    index, dataSeed, MAX_RESAMPLE_ATTEMPTS);
    }

    // A fully-specified example.  Every field is deterministic, so verify() and the shrinker
    // rebuild identical bytes from it.  Features are forced on at draw time (see drawExample).
    private static final class Example
    {
        final int partitions;
        final int rowsPerPartition;
        final int tombstoneKind;      // 0 cell, 1 row, 2 range, 3 partition
        final boolean mixedPurge;     // partition-delete even partitions, so some purge and some survive
        final boolean farFuturePurge;
        final long nowInSec;

        Example(int partitions, int rowsPerPartition, int tombstoneKind, boolean mixedPurge, boolean farFuturePurge, long nowInSec)
        {
            this.partitions = partitions;
            this.rowsPerPartition = rowsPerPartition;
            this.tombstoneKind = tombstoneKind;
            this.mixedPurge = mixedPurge;
            this.farFuturePurge = farFuturePurge;
            this.nowInSec = nowInSec;
        }

        Example with(int newPartitions, int newRowsPerPartition, int newTombstoneKind, boolean newMixedPurge)
        {
            return new Example(newPartitions, newRowsPerPartition, newTombstoneKind, newMixedPurge, farFuturePurge, nowInSec);
        }

        @Override
        public String toString()
        {
            return String.format("Example{partitions=%d, rowsPerPartition=%d, tombstoneKind=%d, mixedPurge=%b, farFuturePurge=%b, nowInSec=%d}",
                                 partitions, rowsPerPartition, tombstoneKind, mixedPurge, farFuturePurge, nowInSec);
        }
    }

    private Example drawExample(Random dataRng, Random clockRng)
    {
        // Floor at one partition and one row so the forced deletion and collection features always
        // apply.  The empty case is a matrix edge, not a fuzz draw.
        int partitions = samplePartitionCount(dataRng);
        int rowsPerPartition = sampleRowCount(dataRng);
        int tombstoneKind = legalTombstoneKind(dataRng.nextInt(4), rowsPerPartition);

        // Mixed purge only bites with more than one partition; it deletes even partitions whole so
        // some drop out and some survive.  This is the alignment axis: the two paths must agree on
        // which partitions to emit, not just on the digest of a shared partition.
        boolean mixedPurge = partitions > 1 && dataRng.nextBoolean();

        boolean farFuturePurge = clockRng.nextBoolean();
        long nowInSec = farFuturePurge ? FBUtilities.nowInSeconds() + TimeUnit.DAYS.toSeconds(30)
                                       : FBUtilities.nowInSeconds();
        return new Example(partitions, rowsPerPartition, tombstoneKind, mixedPurge, farFuturePurge, nowInSec);
    }

    // Build the dataset for an example, guard cursor engagement, and assert per-partition digest
    // parity.  Throws SkipExample if the draw resolves empty or unsupported; throws AssertionError
    // on a real parity mismatch.
    private void verify(Example example) throws Throwable
    {
        // Every example carries a collection column (forced) and a deletion (forced below).
        String schema = "CREATE TABLE %s (pk bigint, ck bigint, v text, m map<text, bigint>, PRIMARY KEY (pk, ck)) ";
        schema += example.farFuturePurge ? "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 0"
                                         : "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000";
        createTable(schema);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int pk = 0; pk < example.partitions; pk++)
        {
            for (int ck = 0; ck < example.rowsPerPartition; ck++)
            {
                execute("INSERT INTO %s (pk, ck, v, m) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        (long) pk, (long) ck, "v" + ck, Collections.singletonMap("k" + ck, (long) ck), BASE_TIMESTAMP + ck);
            }
        }
        flush();

        // Forced deletion feature: apply the sampled tombstone kind to every partition.  When
        // mixedPurge is set, delete even partitions whole instead, so some purge and some survive.
        for (int pk = 0; pk < example.partitions; pk++)
        {
            if (example.mixedPurge && pk % 2 == 0)
                applyTombstone(3, pk, example.rowsPerPartition);
            else
                applyTombstone(example.tombstoneKind, pk, example.rowsPerPartition);
        }
        flush();

        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        long gcBefore = cfs.getDefaultGcBefore(example.nowInSec);

        // Guard: an unsupported draw would silently run iterator-vs-iterator.  Resample instead.
        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            if (!CursorCompactor.isValidationSupported(sstables, controller))
                throw new SkipExample();
        }

        // Guard: an empty result proves nothing.  Resample instead.
        List<byte[]> legacyDigests = perPartitionDigestsLegacy(cfs, sstables, gcBefore, example.nowInSec);
        if (legacyDigests.isEmpty())
            throw new SkipExample();

        List<byte[]> cursorDigests = perPartitionDigestsCursor(cfs, sstables, gcBefore, example.nowInSec);
        assertEquals("legacy and cursor paths must see the same number of partitions",
                     legacyDigests.size(), cursorDigests.size());
        for (int p = 0; p < legacyDigests.size(); p++)
            assertArrayEquals("per-partition digest mismatch at partition " + p, legacyDigests.get(p), cursorDigests.get(p));
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
    // passes, or that turns the run unsupported (SkipExample), is rejected, because a
    // legacy-vs-legacy run hides the bug.  The pinned clock and timestamps never change, so a
    // reduction stays reproducible.
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
        // Drop the mixed-purge axis first; a single-feature example is easier to read.
        if (best.mixedPurge)
            candidates.add(best.with(best.partitions, best.rowsPerPartition, best.tombstoneKind, false));
        // Binary-search the partition count down.  Keep mixedPurge legal for the new width.
        if (best.partitions > 1)
        {
            int fewerPartitions = Math.max(1, best.partitions / 2);
            candidates.add(best.with(fewerPartitions, best.rowsPerPartition, best.tombstoneKind, best.mixedPurge && fewerPartitions > 1));
        }
        // Binary-search the row count down, keeping the tombstone kind legal for the new width.
        if (best.rowsPerPartition > 1)
        {
            int fewerRows = Math.max(1, best.rowsPerPartition / 2);
            candidates.add(best.with(best.partitions, fewerRows, legalTombstoneKind(best.tombstoneKind, fewerRows), best.mixedPurge));
        }
        // Simplify the deletion feature-class toward a whole-partition tombstone.
        if (best.tombstoneKind != 3)
            candidates.add(best.with(best.partitions, best.rowsPerPartition, 3, best.mixedPurge));
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
        // Stratified: one, few (2-5), or several (6-12).  Floored at one for forced features.
        int tier = rng.nextInt(100);
        if (tier < 15)
            return 1;
        else if (tier < 65)
            return 2 + rng.nextInt(4);
        else
            return 6 + rng.nextInt(7);
    }

    private int sampleRowCount(Random rng)
    {
        // Stratified: one, few (2-5), or several (6-20).  Floored at one for forced features.
        // Wide, block-indexed partitions belong in test/long and test/burn.
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

    private List<byte[]> perPartitionDigestsLegacy(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, long gcBefore, long nowInSec) throws Exception
    {
        List<byte[]> digests = new ArrayList<>();
        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
            try
            {
                try (CompactionIterator ci = new CompactionIterator(OperationType.VALIDATION, scanners.scanners, controller,
                                                                    nowInSec, nextTimeUUID()))
                {
                    while (ci.hasNext())
                    {
                        try (UnfilteredRowIterator partition = ci.next())
                        {
                            Digest digest = Digest.forValidator();
                            UnfilteredRowIterators.digest(partition, digest, MessagingService.current_version);
                            digests.add(digest.digest());
                        }
                    }
                }
            }
            finally
            {
                scanners.close();
            }
        }
        return digests;
    }

    private List<byte[]> perPartitionDigestsCursor(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, long gcBefore, long nowInSec) throws Exception
    {
        List<byte[]> digests = new ArrayList<>();
        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            assertTrue("cursor validation must actually be supported for this scenario, or the parity check is vacuous",
                       CursorCompactor.isValidationSupported(sstables, controller));

            Map<SSTableReader, List<PartitionPositionBounds>> boundsBySSTable = new HashMap<>();
            for (SSTableReader sstable : sstables)
                boundsBySSTable.put(sstable, Collections.singletonList(sstable.getPositionsForFullRange()));

            DigestingCursorMergeSink sink = new DigestingCursorMergeSink(cfs.metadata());
            CursorCompactor compactor = new CursorCompactor(OperationType.VALIDATION, boundsBySSTable, controller,
                                                            nowInSec, nextTimeUUID());
            try
            {
                while (compactor.mergeNextPartition(sink))
                {
                    PrecomputedDigestPartition partition = sink.takePartitionDigest();
                    digests.add(partition.digestBytes());
                }
            }
            finally
            {
                compactor.close();
            }
        }
        return digests;
    }

    // Thrown when a draw resolves to an empty or unsupported dataset.  The caller resamples.
    private static final class SkipExample extends RuntimeException
    {
        SkipExample()
        {
            super(null, null, false, false);
        }
    }

    /**
     * PropertyError wrapping an assertion failure, printing the seed and the minimized example.
     * Follows the RandomSchemaTest.PropertyError idiom.
     */
    public static class PropertyError extends AssertionError
    {
        public PropertyError(long seed, long dataSeed, long clockSeed, int exampleIndex, Example minimal, Throwable cause)
        {
            super(String.format("Fuzz failure at example %d: seed=%d dataSeed=%d clockSeed=%d minimal=%s",
                                exampleIndex, seed, dataSeed, clockSeed, minimal), cause);
        }
    }
}
