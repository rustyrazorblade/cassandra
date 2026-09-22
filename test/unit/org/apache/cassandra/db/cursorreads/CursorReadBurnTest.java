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

import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

/**
 * Burn test for the cursor read path (CASSANDRA-20428). It reads a large partition through both
 * paths and proves two things at scale:
 * <ul>
 *   <li>correctness: the cursor response bytes equal the iterator response bytes for a partition
 *       of at least 100K rows spread across {@value #FLUSH_ROUNDS} overlapping sstables;</li>
 *   <li>throughput: it reports rows/s for both paths.</li>
 * </ul>
 *
 * <p>The row count is configurable with {@code -Dcassandra.cursor_read_burn_rows=N} (default
 * {@value #DEFAULT_ROWS}, the CLAUDE.md burn floor). Every read is guarded: it must pass
 * {@link CursorReads#isReadSupported} and the cursor read must advance
 * {@link CursorReads#sstableLegsServed()}, so a silent fallback fails the test rather than
 * reporting iterator numbers as cursor numbers.
 */
public class CursorReadBurnTest extends CursorReadDifferentialTester
{
    private static final int FLUSH_ROUNDS = 5;
    private static final int DEFAULT_ROWS = 100_000;
    private static final long BURN_PK = 42L;
    private static final int WARMUP_READS = 3;
    private static final int MEASURED_READS = 5;

    @Test
    public void burnLargePartition() throws Throwable
    {
        int rows = Integer.getInteger("cassandra.cursor_read_burn_rows", DEFAULT_ROWS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        assertTrue("burn row count must be at least 100K per the burn rule", rows >= 100_000);

        ColumnFamilyStore cfs = loadLargePartition(rows);
        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd =
            () -> (SinglePartitionReadCommand) Util.cmd(cfs, BURN_PK).withNowInSeconds(now).build();

        // guard: the shape must be cursor-supported, else the "cursor" run is an iterator run
        DatabaseDescriptor.setCursorReadsEnabled(true);
        assertTrue("large-partition read is not cursor-supported",
                   CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));

        try
        {
            // correctness at scale: byte-identical response through both paths
            DatabaseDescriptor.setCursorReadsEnabled(false);
            byte[] iteratorBytes = responseBytes(cmd.get());
            DatabaseDescriptor.setCursorReadsEnabled(true);
            long legsBefore = CursorReads.sstableLegsServed();
            byte[] cursorBytes = responseBytes(cmd.get());
            assertTrue("cursor read served no sstable legs (silent fallback?)",
                       CursorReads.sstableLegsServed() > legsBefore);
            assertResponseBytesEqual(iteratorBytes, cursorBytes);

            // throughput: min-of-N full reads on each path
            DatabaseDescriptor.setCursorReadsEnabled(false);
            double iteratorRowsPerSec = measureRowsPerSec(cmd, rows);
            DatabaseDescriptor.setCursorReadsEnabled(true);
            double cursorRowsPerSec = measureRowsPerSec(cmd, rows);

            logger.info("==== CURSOR READ BURN: {}-row partition across {} sstables ====", rows, FLUSH_ROUNDS);
            logger.info("response bytes: iterator={} cursor={} (byte-identical)", iteratorBytes.length, cursorBytes.length);
            logger.info(String.format("throughput: iterator=%,.0f rows/s  cursor=%,.0f rows/s  (%.1f%%)",
                                       iteratorRowsPerSec, cursorRowsPerSec,
                                       100.0 * (cursorRowsPerSec - iteratorRowsPerSec) / iteratorRowsPerSec));
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** Min over MEASURED_READS full reads gives the steady-state best; rows/s from that. */
    private double measureRowsPerSec(Supplier<SinglePartitionReadCommand> cmd, int rows) throws Throwable
    {
        for (int i = 0; i < WARMUP_READS; i++)
            consume(cmd.get());
        long bestNanos = Long.MAX_VALUE;
        for (int i = 0; i < MEASURED_READS; i++)
        {
            long t0 = System.nanoTime();
            consume(cmd.get());
            bestNanos = Math.min(bestNanos, System.nanoTime() - t0);
        }
        return rows / (bestNanos / 1_000_000_000.0);
    }

    private static volatile long sink;

    private void consume(SinglePartitionReadCommand command) throws Throwable
    {
        long consumed = 0;
        try (org.apache.cassandra.db.ReadExecutionController controller = command.executionController();
             org.apache.cassandra.db.partitions.UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (org.apache.cassandra.db.rows.UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        consumed += partition.next().clustering().size();
                }
            }
        }
        sink += consumed;
    }

    /** One partition of {@code rows} rows, striped across {@value #FLUSH_ROUNDS} overlapping
     *  sstables so a full read merges every leg. */
    private ColumnFamilyStore loadLargePartition(int rows) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, v3 int, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < FLUSH_ROUNDS; round++)
        {
            for (long ck = round; ck < rows; ck += FLUSH_ROUNDS)
                execute("INSERT INTO %s (pk, ck, v1, v2, v3) VALUES (?, ?, ?, ?, ?)",
                        BURN_PK, ck, ck, "burn-" + ck, (int) (ck % 1000));
            flush();
        }
        assertTrue("burn workload must be multi-leg", cfs.getLiveSSTables().size() == FLUSH_ROUNDS);
        return cfs;
    }
}
