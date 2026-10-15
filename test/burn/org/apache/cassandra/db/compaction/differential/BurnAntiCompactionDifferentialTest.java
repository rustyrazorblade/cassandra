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

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

/**
 * Burn counterpart to {@link AntiCompactionDifferentialTest}, on BIG and BTI.  It writes at least
 * {@link CassandraRelevantProperties#TEST_ANTICOMPACTION_BURN_ROWS} rows (default 100,000) into
 * {@link CassandraRelevantProperties#TEST_ANTICOMPACTION_BURN_SSTABLES} overlapping sstables (default
 * 4), with static rows, collections, wide partitions, expiring cells and every kind of deletion,
 * and, if {@link CassandraRelevantProperties#TEST_ANTICOMPACTION_BURN_LARGE_PARTITION_BYTES} is set,
 * one partition of that size.  It then anticompacts all of them in one group, on both paths, with
 * random full, transient and unrepaired ranges cut at
 * {@link CassandraRelevantProperties#TEST_ANTICOMPACTION_BURN_RANGES} points (default 256, as with
 * that many vnodes).  Every output sstable must match: bytes, a digest of its logical content,
 * stats and repair metadata; the cursor path must pass every scanned byte to the rate limiter; and
 * while each path anticompacts, the bytes of live objects (from a class histogram) may grow at
 * most a fixed 16 MiB more on the cursor path than on the legacy path, at any size, so heap that
 * the cursor path keeps for each row fails.
 *
 * <p>Run it through {@code test-isolated.sh}, which forwards {@code -Dcassandra.*} to the forked test
 * JVM, for example {@code -Dcassandra.test.anticompaction_burn_rows=10000000
 * -Dcassandra.test.anticompaction_burn_sstables=64
 * -Dcassandra.test.anticompaction_burn_large_partition_bytes=1073741824
 * -Dcassandra.test.anticompaction_burn_ranges=16}.  {@code -Dcassandra.test.differential.seed=S}
 * with the logged run seed S replays it.
 */
@RunWith(Parameterized.class)
public class BurnAntiCompactionDifferentialTest extends DifferentialCompactionTester
{
    private static final int ROWS_PER_PARTITION = 50;
    private static final int WIDE_PARTITION_ROWS = 2000;
    private static final int RANGE_SETS = 2;
    /** The most the live heap may grow more while the cursor path anticompacts than while the legacy path does, at any size. */
    private static final long RETAINED_HEAP_SLACK_BYTES = 16L << 20;
    private static final int LARGE_VALUE_BYTES = 64 << 10;
    private static final String VALUE_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /** Counts only live objects, not garbage that a young collection moved to the old generation. */
    @Override
    protected RetainedHeap watchHeapDuringAntiCompaction()
    {
        return RetainedHeap.watchLive(250);
    }

    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static List<Object[]> formats()
    {
        return List.of(new Object[]{ "big" }, new Object[]{ "bti" });
    }

    /** Scale mode: the logical dump is streamed into a SHA-256 digest, so capture memory stays flat. */
    @Override
    protected boolean scaleCapture()
    {
        return true;
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
    public void burnAntiCompaction() throws Exception
    {
        long targetRows = CassandraRelevantProperties.TEST_ANTICOMPACTION_BURN_ROWS.getLong();
        assertTrue("burn rows must be positive", targetRows > 0);
        long seed = DifferentialRunSeed.SEED ^ format.hashCode();
        logger.info("anticompaction burn: format={} rows={} seed={} (run seed {})", format, targetRows, seed, DifferentialRunSeed.SEED);
        Random random = new Random(seed);
        // the step that is running, for the failure message; the writes draw from the seed too
        String step = "writes";
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v text, m map<text, int>, " +
                        "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            int sstables = CassandraRelevantProperties.TEST_ANTICOMPACTION_BURN_SSTABLES.getInt();
            long largePartitionBytes = CassandraRelevantProperties.TEST_ANTICOMPACTION_BURN_LARGE_PARTITION_BYTES.getLong();
            int rangeCuts = CassandraRelevantProperties.TEST_ANTICOMPACTION_BURN_RANGES.getInt();
            assertTrue("burn sstables and ranges must be positive", sstables > 0 && rangeCuts > 0);
            logger.info("anticompaction burn: sstables={} large partition bytes={} range cuts={}", sstables, largePartitionBytes, rangeCuts);
            long written = writeFirstRound(targetRows);
            if (largePartitionBytes > 0)
                writeLargePartition(random, largePartitionBytes);
            for (int round = 1; round < sstables; round++)
                writeOverlappingRound(random, round, written);
            // TTL 1 and gc_grace 0: the expiring cells and the tombstones purge on both runs alike
            AntiCompactionDifferentialTest.awaitClockPast(FBUtilities.nowInSeconds() + 1);

            long inputRows = 0;
            for (SSTableReader sstable : cfs.getLiveSSTables())
                inputRows += sstable.getTotalRows();
            logger.info("anticompaction burn: {} input sstables, {} input rows", cfs.getLiveSSTables().size(), inputRows);
            assertTrue("the burn wrote only " + inputRows + " rows; expected at least " + targetRows, inputRows >= targetRows);
            assertTrue("the burn wrote only " + cfs.getLiveSSTables().size() + " sstables; expected at least " + sstables,
                       cfs.getLiveSSTables().size() >= sstables);

            List<Long> tokens = AntiCompactionRanges.liveTokens(cfs);
            for (int i = 0; i < RANGE_SETS; i++)
            {
                step = "range set " + i;
                RangesAtEndpoint ranges = AntiCompactionRanges.randomRanges(random, tokens, rangeCuts);
                step = "range set " + i + ", ranges " + ranges;
                AntiCompactionOutput out = assertCursorMatchesIteratorForAntiCompaction(cfs, ranges);
                logger.info("anticompaction burn: range set {} matched across {} output sstables, the live heap grew by {} bytes on the cursor path and {} bytes on the legacy path: {}",
                            i, out.sstableCount(), out.cursorRun.retainedHeapGrowth, out.retainedHeapGrowth, out.repairMetadata());
                assertTrue("the live heap grew by " + out.cursorRun.retainedHeapGrowth + " bytes on the cursor path and " + out.retainedHeapGrowth +
                           " bytes on the legacy path, for " + inputRows + " input rows; at most " + RETAINED_HEAP_SLACK_BYTES + " more is allowed",
                           out.cursorRun.retainedHeapGrowth <= out.retainedHeapGrowth + RETAINED_HEAP_SLACK_BYTES);
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError("failure for seed " + seed + "; " + DifferentialRunSeed.replayAdvice(DifferentialRunSeed.SEED) +
                                     "; format " + format + ", " + step + ": " + t, t);
        }
    }

    /** Writes {@code targetRows} rows, in partitions of {@link #ROWS_PER_PARTITION} and some wide ones. */
    private long writeFirstRound(long targetRows)
    {
        long rows = 0;
        long pk = 0;
        while (rows < targetRows)
        {
            int partitionRows = pk % 97 == 0 ? WIDE_PARTITION_ROWS : ROWS_PER_PARTITION;
            execute("UPDATE %s SET s = ? WHERE pk = ?", "s0-" + pk, pk);
            for (long ck = 0; ck < partitionRows; ck++)
            {
                // one row in ten expires at once
                String ttl = ck % 10 == 3 ? " USING TTL 1" : "";
                execute("INSERT INTO %s (pk, ck, v, m) VALUES (?, ?, ?, ?)" + ttl, pk, ck, "v0-" + ck, Map.of("a", (int) ck));
            }
            rows += partitionRows;
            pk++;
        }
        flush();
        return pk;
    }

    /** {@link #LARGE_VALUE_BYTES} random letters and digits. */
    private static String randomValue(Random random)
    {
        byte[] bytes = new byte[LARGE_VALUE_BYTES];
        random.nextBytes(bytes);
        char[] chars = new char[LARGE_VALUE_BYTES];
        for (int i = 0; i < chars.length; i++)
            chars[i] = VALUE_CHARS.charAt((bytes[i] & 0xff) % VALUE_CHARS.length());
        return new String(chars);
    }

    /**
     * Writes one partition of about {@code bytes}, of seeded random values so that it does not
     * compress away, flushing about every 64 MiB, so it spans several sstables.
     */
    private void writeLargePartition(Random random, long bytes)
    {
        long written = 0;
        long sinceFlush = 0;
        for (long ck = 0; written < bytes; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", -1L, ck, randomValue(random));
            written += LARGE_VALUE_BYTES;
            sinceFlush += LARGE_VALUE_BYTES;
            if (sinceFlush >= 64L << 20)
            {
                flush();
                sinceFlush = 0;
            }
        }
        flush();
    }

    /** Overwrites, extends and deletes a random quarter of the partitions. */
    private void writeOverlappingRound(Random random, int round, long partitions)
    {
        for (long pk = 0; pk < partitions; pk++)
        {
            if (random.nextInt(4) != 0)
                continue;
            int op = random.nextInt(10);
            if (op == 0)
            {
                execute("DELETE FROM %s WHERE pk = ?", pk);
            }
            else if (op == 1)
            {
                long from = random.nextInt(ROWS_PER_PARTITION);
                execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", pk, from, from + 1 + random.nextInt(10));
            }
            else
            {
                execute("UPDATE %s SET s = ? WHERE pk = ?", "s" + round + '-' + pk, pk);
                for (int i = 0; i < 10; i++)
                {
                    long ck = random.nextInt(ROWS_PER_PARTITION + 10);
                    execute("INSERT INTO %s (pk, ck, v, m) VALUES (?, ?, ?, ?)", pk, ck, "v" + round + '-' + ck, Map.of("r" + round, i));
                }
                execute("DELETE m['a'] FROM %s WHERE pk = ? AND ck = ?", pk, (long) random.nextInt(ROWS_PER_PARTITION));
                execute("DELETE v FROM %s WHERE pk = ? AND ck = ?", pk, (long) random.nextInt(ROWS_PER_PARTITION));
            }
        }
        flush();
    }
}
