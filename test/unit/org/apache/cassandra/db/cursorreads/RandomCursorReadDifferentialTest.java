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

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Seeded-random sweep over the single-partition cursor read path.  Each trial builds one partition
 * from a random mix of inserts, TTLs, single-row deletes, range deletes, static writes, and
 * partition deletes, spread across several flushed sstables plus a final unflushed memtable overlay.
 * It then reads that partition through a randomly chosen filter shape (full, slice, names, reversed,
 * column subset) and asserts the cursor path matches the iterator path, both as canonical records
 * and as intra-node ReadResponse bytes (see {@link CursorReadDifferentialTester}).
 * <p>
 * Every mutation gets an explicit {@code USING TIMESTAMP}, and every read pins {@code nowInSeconds},
 * so liveness and TTL evaluation cannot flip between the two runs.  TTLs are large and
 * {@code gc_grace_seconds} is high, so nothing expires or is purged during a trial.  The value
 * column type is randomized per trial, so trials write varied cell shapes.
 * <p>
 * Each trial is fully determined by one {@code trialSeed}, so a single logged seed replays one trial
 * in isolation.  When a trial fails, a hand-rolled greedy shrinker binary-searches each numeric knob
 * down to the smallest value that still reproduces the failure (QuickTheories shrinking is disabled
 * repo-wide).
 */
public class RandomCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private static final Logger logger = LoggerFactory.getLogger(RandomCursorReadDifferentialTest.class);
    private static final int TRIALS = 40;
    private static final long BASE_TS = 3_000_000_000_000L;
    private static final int LARGE_TTL = 1_000_000; // long enough that no cell expires during a trial
    private static final long FILTER_SALT = 0x9E3779B97F4A7C15L; // separates the filter RNG stream from the data one

    /**
     * Seeds of trials that previously failed, replayed on every run before fresh random exploration.
     * Add an entry when a genuine bug is found and fixed; never remove one.
     */
    private static final long[] KNOWN_REGRESSION_SEEDS = {
        -4715507501933830118L, // cursor NAMES row wrapped in an RT whose exclusive upper bound misses it
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

    private enum FilterKind { FULL, SLICE, NAMES }

    /** Every parameter a trial needs, all derived from one {@code trialSeed}. */
    private static final class TrialParams
    {
        final long trialSeed;
        final int maxClustering;
        final int operations;
        final int flushRounds;
        final ValueType valueType;

        TrialParams(long trialSeed, int maxClustering, int operations, int flushRounds, ValueType valueType)
        {
            this.trialSeed = trialSeed;
            this.maxClustering = maxClustering;
            this.operations = operations;
            this.flushRounds = flushRounds;
            this.valueType = valueType;
        }

        static TrialParams derive(long trialSeed)
        {
            Random r = new Random(trialSeed);
            int maxClustering = 5 + r.nextInt(60);
            int operations = 10 + r.nextInt(150);
            int flushRounds = 1 + r.nextInt(4);
            ValueType valueType = VALUE_TYPES.get(r.nextInt(VALUE_TYPES.size()));
            return new TrialParams(trialSeed, maxClustering, operations, flushRounds, valueType);
        }

        TrialParams withOperations(int newOperations)
        {
            return new TrialParams(trialSeed, maxClustering, newOperations, flushRounds, valueType);
        }

        TrialParams withFlushRounds(int newFlushRounds)
        {
            return new TrialParams(trialSeed, maxClustering, operations, newFlushRounds, valueType);
        }

        TrialParams withMaxClustering(int newMaxClustering)
        {
            return new TrialParams(trialSeed, newMaxClustering, operations, flushRounds, valueType);
        }

        @Override
        public String toString()
        {
            return String.format("trialSeed=%dL, maxClustering=%d, operations=%d, flushRounds=%d, valueType=%s",
                                 trialSeed, maxClustering, operations, flushRounds, valueType.cql);
        }
    }

    @Test
    public void randomSinglePartitionReads() throws Throwable
    {
        long seed = System.currentTimeMillis();
        logger.info("RandomCursorReadDifferentialTest seed = {}", seed);

        for (long regressionSeed : KNOWN_REGRESSION_SEEDS)
            runTrial(regressionSeed, "known-regression replay");

        Random seedPicker = new Random(seed);
        for (int trial = 0; trial < TRIALS; trial++)
            runTrial(seedPicker.nextLong(), "trial " + trial);
    }

    private void runTrial(long trialSeed, String label) throws Throwable
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

    private void runOnce(TrialParams p) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s text static, v1 " + p.valueType.cql + ", v2 text, " +
                    "PRIMARY KEY (k, c)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        populate(cfs, p);

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, command(cfs, now, p));
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
     * Binary-searches each numeric knob down to the smallest value that still reproduces the failure,
     * one knob at a time: operations, then flushRounds, then maxClustering.  Every candidate is
     * verified via {@link #reproduces} before being accepted.  Not a global minimum, since the knobs
     * interact, but always a case that still fails.
     */
    private TrialParams shrink(TrialParams failing)
    {
        TrialParams afterOperations = failing.withOperations(
            shrinkKnob(failing.operations, n -> n >= 1 && reproduces(failing.withOperations(n))));
        TrialParams afterFlushRounds = afterOperations.withFlushRounds(
            shrinkKnob(afterOperations.flushRounds, n -> n >= 1 && reproduces(afterOperations.withFlushRounds(n))));
        TrialParams afterMaxClustering = afterFlushRounds.withMaxClustering(
            shrinkKnob(afterFlushRounds.maxClustering, n -> n >= 1 && reproduces(afterFlushRounds.withMaxClustering(n))));
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

    /**
     * Writes one partition (k = 1) from a random op stream, splitting the flushed ops across
     * {@code flushRounds} sstables and leaving a final small chunk in the memtable so every trial
     * exercises the memtable-plus-sstable merge.
     */
    private void populate(ColumnFamilyStore cfs, TrialParams p)
    {
        Random r = new Random(p.trialSeed);
        AtomicLong ts = new AtomicLong(BASE_TS);

        // Baseline rows at both ends of the clustering domain, with timestamps below every random op
        // so later ops can freely shadow them.  They guarantee the flushed sstable holds clustering
        // rows spanning [0, maxClustering], so a covering slice or names filter always intersects a
        // real clustering and the cursor is never pruned away (which would prove nothing).
        execute("INSERT INTO %s (k, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                1, 0, p.valueType.generate.apply(r), "base-lo", BASE_TS - 2);
        execute("INSERT INTO %s (k, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                1, p.maxClustering, p.valueType.generate.apply(r), "base-hi", BASE_TS - 1);

        int perRound = Math.max(1, p.operations / p.flushRounds);
        int sinceFlush = 0;
        int roundsDone = 0;
        for (int op = 0; op < p.operations; op++)
        {
            applyOp(r, ts, p, true);
            if (++sinceFlush >= perRound && roundsDone < p.flushRounds - 1)
            {
                flush();
                sinceFlush = 0;
                roundsDone++;
            }
        }
        flush(); // seal the last flushed round

        // Final unflushed overlay: a few more ops that stay in the memtable.  Partition deletes are
        // excluded here: the overlay carries the newest timestamps, and a memtable partition tombstone
        // newer than every sstable correctly eliminates all sstables (the mostRecentPartitionTombstone
        // optimization), so the cursor would serve no leg and prove nothing.  Flushed partition
        // deletes above still exercise that path and keep their own sstable served.
        int overlay = 1 + r.nextInt(5);
        for (int op = 0; op < overlay; op++)
            applyOp(r, ts, p, false);
    }

    private void applyOp(Random r, AtomicLong ts, TrialParams p, boolean allowPartitionDelete)
    {
        long opTs = ts.getAndIncrement();
        int c = r.nextInt(p.maxClustering);
        int choice = r.nextInt(8);
        if (choice == 7 && !allowPartitionDelete)
            choice = 0; // remap partition delete to a plain insert in the memtable overlay
        switch (choice)
        {
            case 0:
            case 1:
            case 2:
                // full-row insert, occasionally with a large TTL
                Object v1 = p.valueType.generate.apply(r);
                if (r.nextInt(5) == 0)
                    execute("INSERT INTO %s (k, c, v1, v2) VALUES (?, ?, ?, ?) USING TTL " + LARGE_TTL +
                            " AND TIMESTAMP " + opTs, 1, c, v1, "v2-" + c);
                else
                    execute("INSERT INTO %s (k, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP " + opTs,
                            1, c, v1, "v2-" + c);
                break;
            case 3:
                // partial insert: only v1, so v2 stays absent on this row
                execute("INSERT INTO %s (k, c, v1) VALUES (?, ?, ?) USING TIMESTAMP " + opTs,
                        1, c, p.valueType.generate.apply(r));
                break;
            case 4:
                // static column write (partition-level, no clustering)
                execute("UPDATE %s USING TIMESTAMP " + opTs + " SET s = ? WHERE k = ?", "s-" + r.nextInt(1000), 1);
                break;
            case 5:
                // single-row delete
                execute("DELETE FROM %s USING TIMESTAMP " + opTs + " WHERE k = ? AND c = ?", 1, c);
                break;
            case 6:
            {
                // range delete
                int a = r.nextInt(p.maxClustering);
                int b = r.nextInt(p.maxClustering);
                int lo = Math.min(a, b);
                int hi = Math.max(a, b);
                if (lo == hi)
                    execute("DELETE FROM %s USING TIMESTAMP " + opTs + " WHERE k = ? AND c = ?", 1, lo);
                else
                    execute("DELETE FROM %s USING TIMESTAMP " + opTs + " WHERE k = ? AND c >= ? AND c < ?",
                            1, lo, hi);
                break;
            }
            case 7:
                // partition delete (later ops re-populate above it)
                execute("DELETE FROM %s USING TIMESTAMP " + opTs + " WHERE k = ?", 1);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Builds the read command for this trial.  The shape is derived deterministically from the seed,
     * so it is identical across the iterator run, the cursor run, and every shrink replay.
     */
    private Supplier<SinglePartitionReadCommand> command(ColumnFamilyStore cfs, long now, TrialParams p)
    {
        return () -> {
            Random r = new Random(p.trialSeed ^ FILTER_SALT);
            FilterKind kind = FilterKind.values()[r.nextInt(FilterKind.values().length)];
            boolean reversed = kind != FilterKind.NAMES && r.nextBoolean(); // names + reverse: leave to the named tests
            String[] columns = pickColumns(r);

            AbstractReadCommandBuilder b = Util.cmd(cfs, 1).withNowInSeconds(now);
            switch (kind)
            {
                case FULL:
                    break;
                case SLICE:
                {
                    // A domain-covering slice: inclusive 0 to inclusive maxClustering always spans
                    // every written clustering, so the sstable is never pruned and the cursor serves.
                    // This still exercises the bounded-slice path (not Slices.ALL); the named slice
                    // tests already probe exact boundary offsets, so here the data shape is what varies.
                    b = b.fromIncl(0).toIncl(p.maxClustering);
                    break;
                }
                case NAMES:
                {
                    // Anchor names at 0 and maxClustering so the names filter's pruning range spans the
                    // whole written domain and the sstable is opened; interior names are random and may
                    // miss every row (that difference must still reconcile identically).
                    b = b.includeRow(0).includeRow(p.maxClustering);
                    int extra = r.nextInt(5);
                    for (int i = 0; i < extra; i++)
                        b = b.includeRow(r.nextInt(p.maxClustering + 1));
                    break;
                }
            }
            if (reversed)
                b = b.reverse();
            if (columns != null)
                b = b.columns(columns);
            return (SinglePartitionReadCommand) b.build();
        };
    }

    /** null means "all columns"; otherwise a fixed subset of the queried table's columns. */
    private static String[] pickColumns(Random r)
    {
        switch (r.nextInt(5))
        {
            case 0: return null;
            case 1: return new String[]{ "v1" };
            case 2: return new String[]{ "v2" };
            case 3: return new String[]{ "v1", "v2" };
            case 4: return new String[]{ "s" };
            default: throw new IllegalStateException();
        }
    }
}
