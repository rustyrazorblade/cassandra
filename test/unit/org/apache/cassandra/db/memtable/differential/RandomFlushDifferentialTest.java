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

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntPredicate;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Randomized sweep over the row/range-tombstone interleaving in the cursor flush path, across
 * many random combinations of row, range, and partition deletions and TTLs.  Every mutation gets
 * an explicit deterministic timestamp, so both the byte-for-byte and logical comparisons apply.
 * <p>
 * The value column's type is randomized per trial from {@link #VALUE_TYPES}, so trials write
 * varied cell shapes rather than one fixed {@code text} column.
 * <p>
 * Each trial is fully determined by one {@code trialSeed}, so a single logged seed replays one
 * trial in isolation.
 */
public class RandomFlushDifferentialTest extends MemtableFlushDifferentialTester
{
    private static final Logger logger = LoggerFactory.getLogger(RandomFlushDifferentialTest.class);
    private static final int TRIALS = 40;
    private static final long BASE_TS = 3_000_000_000_000L;

    /**
     * Seeds of trials that previously failed, replayed on every run before fresh random
     * exploration.  Add an entry when a genuine bug is found and fixed; never remove one.
     */
    private static final long[] KNOWN_REGRESSION_SEEDS = {
        // Corrupt BTI partition index and empty first/last bounds on the cursor flush path, when
        // the reusable last key was populated only on compaction (see BtiRandomFlushDifferentialTest).
        1729286432583183978L,
    };

    private static final class ValueType
    {
        final String cql;
        final Function<Random, Object> generate;

        ValueType(String cql, Function<Random, Object> generate)
        {
            this.cql = cql;
            this.generate = generate;
        }
    }

    private static final List<ValueType> VALUE_TYPES = List.of(
        new ValueType("int", Random::nextInt),
        new ValueType("bigint", Random::nextLong),
        new ValueType("double", Random::nextDouble),
        new ValueType("boolean", Random::nextBoolean),
        new ValueType("text", r -> "v-" + r.nextInt(1_000_000)),
        new ValueType("ascii", r -> "a" + r.nextInt(1_000_000)),
        new ValueType("uuid", r -> new UUID(r.nextLong(), r.nextLong())),
        new ValueType("blob", r -> ByteBufferUtil.bytes(r.nextInt())),
        new ValueType("decimal", r -> BigDecimal.valueOf(r.nextDouble()))
    );

    /** Every parameter a trial needs, all derived from one {@code trialSeed}. */
    private static final class TrialParams
    {
        final long trialSeed;
        final int partitions;
        final int maxClustering;
        final int operations;
        final ValueType valueType;

        TrialParams(long trialSeed, int partitions, int maxClustering, int operations, ValueType valueType)
        {
            this.trialSeed = trialSeed;
            this.partitions = partitions;
            this.maxClustering = maxClustering;
            this.operations = operations;
            this.valueType = valueType;
        }

        static TrialParams derive(long trialSeed)
        {
            Random r = new Random(trialSeed);
            int partitions = 1 + r.nextInt(4);
            int maxClustering = 5 + r.nextInt(60);
            int operations = 10 + r.nextInt(150);
            ValueType valueType = VALUE_TYPES.get(r.nextInt(VALUE_TYPES.size()));
            return new TrialParams(trialSeed, partitions, maxClustering, operations, valueType);
        }

        TrialParams withOperations(int newOperations)
        {
            return new TrialParams(trialSeed, partitions, maxClustering, newOperations, valueType);
        }

        TrialParams withPartitions(int newPartitions)
        {
            return new TrialParams(trialSeed, newPartitions, maxClustering, operations, valueType);
        }

        TrialParams withMaxClustering(int newMaxClustering)
        {
            return new TrialParams(trialSeed, partitions, newMaxClustering, operations, valueType);
        }

        @Override
        public String toString()
        {
            return String.format("trialSeed=%dL, partitions=%d, maxClustering=%d, operations=%d, valueType=%s",
                                 trialSeed, partitions, maxClustering, operations, valueType.cql);
        }
    }

    @Test
    public void randomRowsAndDeletions() throws Exception
    {
        long seed = System.currentTimeMillis();
        logger.info("RandomFlushDifferentialTest seed = {}", seed);

        for (long regressionSeed : KNOWN_REGRESSION_SEEDS)
            runTrial(regressionSeed, "known-regression replay");

        Random seedPicker = new Random(seed);
        for (int trial = 0; trial < TRIALS; trial++)
            runTrial(seedPicker.nextLong(), "trial " + trial);
    }

    private void runTrial(long trialSeed, String label) throws Exception
    {
        TrialParams params = TrialParams.derive(trialSeed);
        try
        {
            runOnce(params);
        }
        catch (Throwable t)
        {
            String shrunkDescription;
            try
            {
                shrunkDescription = shrink(params).toString();
            }
            catch (Throwable shrinkFailure)
            {
                shrunkDescription = "(shrinking itself threw: " + shrinkFailure + ")";
            }
            throw new AssertionError(String.format(
                "%s failed.%nOriginal: %s%nShrunk (minimal reproducer): %s%n" +
                "If genuine, add trialSeed=%dL to KNOWN_REGRESSION_SEEDS once fixed.",
                label, params, shrunkDescription, trialSeed), t);
        }
    }

    private void runOnce(TrialParams p) throws Exception
    {
        // Logical-only: local_delete_time and expires_at are wall-clock seconds, and a long trial
        // can straddle a second boundary between the two populate() calls.  Cell timestamps are
        // set by USING TIMESTAMP and stay exactly verified.
        assertFlushMatchesLogically("CREATE TABLE %s (k int, c int, v " + p.valueType.cql + ", PRIMARY KEY (k, c))",
                                    (ks, tbl) -> populate(ks, tbl, p.trialSeed, p.partitions, p.maxClustering, p.operations, p.valueType),
                                    false);
    }

    private boolean reproduces(TrialParams p)
    {
        try
        {
            runOnce(p);
            return false;
        }
        catch (Throwable t)
        {
            return true;
        }
    }

    /**
     * Binary-searches each numeric knob down to the smallest value that still reproduces the
     * failure, one knob at a time: operations, then partitions, then maxClustering.  Every
     * candidate is verified via {@link #reproduces} before being accepted.  Not a global minimum,
     * since the knobs interact, but always a case that still fails.
     */
    private TrialParams shrink(TrialParams failing)
    {
        TrialParams afterOperations = failing.withOperations(
            shrinkKnob(failing.operations, n -> n >= 1 && reproduces(failing.withOperations(n))));
        TrialParams afterPartitions = afterOperations.withPartitions(
            shrinkKnob(afterOperations.partitions, n -> n >= 1 && reproduces(afterOperations.withPartitions(n))));
        TrialParams afterMaxClustering = afterPartitions.withMaxClustering(
            shrinkKnob(afterPartitions.maxClustering, n -> n >= 1 && reproduces(afterPartitions.withMaxClustering(n))));
        return afterMaxClustering;
    }

    /** Binary search for the minimal n in [1, initial] for which reproducesAt.test(n) holds. */
    private static int shrinkKnob(int initial, IntPredicate reproducesAt)
    {
        int lo = 1, hi = initial;
        while (lo < hi)
        {
            int mid = lo + (hi - lo) / 2;
            if (reproducesAt.test(mid))
                hi = mid;
            else
                lo = mid + 1;
        }
        return lo;
    }

    private void populate(String ks, String tbl, long trialSeed, int partitions, int maxClustering, int operations, ValueType valueType)
    {
        String t = ks + "." + tbl;
        Random r = new Random(trialSeed);
        AtomicLong ts = new AtomicLong(BASE_TS);

        for (int op = 0; op < operations; op++)
        {
            int k = r.nextInt(partitions);
            long opTs = ts.getAndIncrement();
            switch (r.nextInt(6))
            {
                case 0:
                case 1:
                case 2:
                    // plain insert, occasionally with a TTL
                    int c = r.nextInt(maxClustering);
                    Object value = valueType.generate.apply(r);
                    if (r.nextInt(5) == 0)
                        execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TTL 100000 AND TIMESTAMP " + opTs,
                               k, c, value);
                    else
                        execute("INSERT INTO " + t + " (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP " + opTs,
                               k, c, value);
                    break;
                case 3:
                    // single-row delete
                    execute("DELETE FROM " + t + " USING TIMESTAMP " + opTs + " WHERE k = ? AND c = ?",
                           k, r.nextInt(maxClustering));
                    break;
                case 4:
                {
                    // range delete
                    int a = r.nextInt(maxClustering);
                    int b = r.nextInt(maxClustering);
                    int lo = Math.min(a, b);
                    int hi = Math.max(a, b);
                    if (lo == hi)
                        execute("DELETE FROM " + t + " USING TIMESTAMP " + opTs + " WHERE k = ? AND c = ?", k, lo);
                    else
                        execute("DELETE FROM " + t + " USING TIMESTAMP " + opTs + " WHERE k = ? AND c >= ? AND c < ?",
                               k, lo, hi);
                    break;
                }
                case 5:
                    // partition delete
                    execute("DELETE FROM " + t + " USING TIMESTAMP " + opTs + " WHERE k = ?", k);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }
}
