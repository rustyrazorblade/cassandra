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

import java.util.Random;
import java.util.function.Supplier;

import com.google.common.base.Strings;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Seeded-random sweep of REVERSE single-partition reads over a WIDE, row-indexed BTI partition
 * (past {@code column_index_size}, so the reverse walk seeks many real index blocks).  It targets
 * the cursor reverse block descent ({@code CursorReads.ReverseSlicedCursorIterator.gotoBlock}): each
 * trial builds one padded partition from a random op mix (inserts, TTLs, single-row deletes, range
 * deletes) then issues a reversed read of a randomly shaped filter (full, one bounded slice, or two
 * slices), asserting the cursor path is byte-identical to the iterator path via
 * {@link CursorReadDifferentialTester}.
 *
 * <p>The base randomized test ({@code RandomCursorReadDifferentialTest}) keeps partitions tiny (never
 * row-indexed), so it cannot exercise the backward multi-block seek.  This class fills that gap for
 * CASSANDRA-20428.
 */
public class ReverseLargeBlockIndexedCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private static final Logger logger = LoggerFactory.getLogger(ReverseLargeBlockIndexedCursorReadDifferentialTest.class);
    private static final int TRIALS = 40;
    private static final long BASE_TS = 3_000_000_000_000L;
    private static final int LARGE_TTL = 1_000_000;
    private static final String PAD = Strings.repeat("x", 512); // wide rows: few hundred rows span many 4KiB blocks

    /** Seeds that previously reproduced a real defect; replayed on every run.  Never remove one. */
    private static final long[] KNOWN_REGRESSION_SEEDS = {};

    private String originalFormat;

    @Before
    public void pinBti()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat().name();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void randomReverseLargePartitionReads() throws Throwable
    {
        long seed = System.currentTimeMillis();
        logger.info("ReverseLargeBlockIndexedCursorReadDifferentialTest dense seed = {}", seed);

        for (long regressionSeed : KNOWN_REGRESSION_SEEDS)
            runTrial(regressionSeed, "known-regression replay", false);

        Random seedPicker = new Random(seed);
        for (int trial = 0; trial < TRIALS; trial++)
            runTrial(seedPicker.nextLong(), "dense trial " + trial, false);
    }

    /** Sparse clustering with large gaps between rows, plus read bounds that can land in the gaps or
     *  beyond the partition: the reverse block seek then lands the trie on a block whose first row is
     *  well past the requested bound, the {@code gotoBlock} edge the dense sweep never forces. */
    @Test
    public void randomReverseSparseLargePartitionReads() throws Throwable
    {
        long seed = System.currentTimeMillis() ^ 0x5DEECE66DL;
        logger.info("ReverseLargeBlockIndexedCursorReadDifferentialTest sparse seed = {}", seed);

        Random seedPicker = new Random(seed);
        for (int trial = 0; trial < TRIALS; trial++)
            runTrial(seedPicker.nextLong(), "sparse trial " + trial, true);
    }

    private void runTrial(long trialSeed, String label, boolean sparse) throws Throwable
    {
        try
        {
            runOnce(trialSeed, sparse);
        }
        catch (Throwable t)
        {
            throw new AssertionError(String.format(
                "%s failed. trialSeed=%dL sparse=%b. Add it to KNOWN_REGRESSION_SEEDS once fixed.",
                label, trialSeed, sparse), t);
        }
    }

    private void runOnce(long trialSeed, boolean sparse) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, pad text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        Random r = new Random(trialSeed);
        int rows = 300 + r.nextInt(400); // wide enough to span many blocks
        // dense: ck = i (stride 1).  sparse: ck = i * stride, so read bounds can land in the gaps.
        long stride = sparse ? 5 + r.nextInt(20) : 1;
        long domain = rows * stride; // exclusive upper end of the clustering domain
        long ts = BASE_TS;

        // padded base rows across the whole domain, so the partition is row-indexed and every reverse
        // walk crosses real block boundaries.  Split across several flushed sstables so the reverse
        // read reconciles multiple large legs in the merge (the lower-bound deferral path).
        int flushRounds = 2 + r.nextInt(3);
        int perRound = rows / flushRounds;
        for (int round = 0; round < flushRounds; round++)
        {
            int loRow = round * perRound;
            int hiRow = round == flushRounds - 1 ? rows : loRow + perRound;
            for (int i = loRow; i < hiRow; i++)
            {
                long ck = i * stride;
                execute("INSERT INTO %s (pk, ck, v1, pad) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        1L, ck, ck, PAD, ts++);
            }
            flush();
        }

        // random overlay ops with newer timestamps, spread across more flushed sstables so legs
        // overlap in clustering and the reverse merge must interleave them.  Overlay clusterings can
        // land off the stride grid, adding rows between the base rows.
        int ops = 20 + r.nextInt(120);
        for (int i = 0; i < ops; i++)
        {
            long c = Math.floorMod(r.nextLong(), domain);
            switch (r.nextInt(6))
            {
                case 0:
                case 1:
                    execute("INSERT INTO %s (pk, ck, v1, pad) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                            1L, c, c + 1000, PAD, ts++);
                    break;
                case 2:
                    execute("INSERT INTO %s (pk, ck, v1, pad) VALUES (?, ?, ?, ?) USING TTL " + LARGE_TTL +
                            " AND TIMESTAMP " + (ts++), 1L, c, c + 2000, PAD);
                    break;
                case 3:
                    execute("DELETE FROM %s USING TIMESTAMP " + (ts++) + " WHERE pk = ? AND ck = ?", 1L, c);
                    break;
                case 4:
                case 5:
                {
                    long a = Math.floorMod(r.nextLong(), domain);
                    long b = Math.floorMod(r.nextLong(), domain);
                    long lo = Math.min(a, b);
                    long hi = Math.max(a, b);
                    execute("DELETE FROM %s USING TIMESTAMP " + (ts++) + " WHERE pk = ? AND ck >= ? AND ck <= ?",
                            1L, lo, hi);
                    break;
                }
                default:
                    throw new IllegalStateException();
            }
            if (i % 40 == 39)
                flush();
        }
        flush();

        long now = FBUtilities.nowInSeconds();
        // read bounds range over [0, domain] so, when sparse, they routinely land in gaps and at/above
        // the top row -- the reverse block seek then lands on a block whose first row is past the bound.
        assertCursorReadMatchesIterator(cfs, reverseCommand(cfs, now, domain, trialSeed));
    }

    private static DecoratedKey key(ColumnFamilyStore cfs, long pk)
    {
        return cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(pk));
    }

    private static Slice inclSlice(TableMetadata metadata, long startIncl, long endIncl)
    {
        ClusteringBound<?> start = ClusteringBound.create(metadata.comparator, true, true, startIncl);
        ClusteringBound<?> end = ClusteringBound.create(metadata.comparator, false, true, endIncl);
        return Slice.make(start, end);
    }

    /** A random clustering in [0, domain]: can equal a base row, land in a gap, or hit the top edge. */
    private static long point(Random r, long domain)
    {
        return Math.floorMod(r.nextLong(), domain + 1);
    }

    /** A reversed read whose filter shape (full, one slice, two slices, or many points) is derived
     *  from the seed.  Bounds range over [0, domain], so for a sparse partition they routinely land in
     *  gaps and at the top edge. */
    private Supplier<SinglePartitionReadCommand> reverseCommand(ColumnFamilyStore cfs, long now,
                                                                long domain, long trialSeed)
    {
        return () -> {
            Random r = new Random(trialSeed ^ 0x9E3779B97F4A7C15L);
            TableMetadata metadata = cfs.metadata();
            Slices slices;
            switch (r.nextInt(4))
            {
                case 0:
                    slices = Slices.ALL;
                    break;
                case 1:
                {
                    long a = point(r, domain);
                    long b = point(r, domain);
                    slices = new Slices.Builder(metadata.comparator)
                             .add(inclSlice(metadata, Math.min(a, b), Math.max(a, b)))
                             .build();
                    break;
                }
                case 2:
                {
                    // two non-overlapping slices, low then high (Slices requires ascending, non-overlapping)
                    long q = domain / 4;
                    slices = new Slices.Builder(metadata.comparator)
                             .add(inclSlice(metadata, point(r, q), q + point(r, q)))
                             .add(inclSlice(metadata, 2 * q + point(r, q), 3 * q + point(r, q)))
                             .build();
                    break;
                }
                default:
                {
                    // many scattered point slices [x,x], the SLICE-filter analog of a NAMES read; each
                    // point slice makes the reverse iterator seek an independent block deep in the
                    // partition, exercising gotoBlock repeatedly across many blocks.  Points may miss
                    // every stored row (a gap), forcing the block seek to land past the requested bound.
                    java.util.TreeSet<Long> xs = new java.util.TreeSet<>();
                    int points = 8 + r.nextInt(12);
                    for (int i = 0; i < points; i++)
                        xs.add(point(r, domain));
                    Slices.Builder b = new Slices.Builder(metadata.comparator);
                    for (long x : xs)
                        b.add(inclSlice(metadata, x, x));
                    slices = b.build();
                }
            }
            int limit = r.nextInt(3) == 0 ? 1 + r.nextInt(20) : -1;
            DataLimits limits = limit < 0 ? DataLimits.NONE : DataLimits.cqlLimits(limit);
            return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(),
                                                     limits, key(cfs, 1L),
                                                     new ClusteringIndexSliceFilter(slices, true)); // reversed
        };
    }
}
