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

package org.apache.cassandra.db;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * SCRATCH / TEMPORARY MEASUREMENT HARNESS — NOT FOR COMMIT.
 *
 * Phase 0 of the cursor-read-path investigation: JFR allocation profile of a sustained,
 * mixed read workload over a realistic layout (memtable + 5 overlapping sstables, sparse
 * rows, cell/row/range tombstones, one wide partition), to measure what fraction of total
 * read-execution allocation is the per-value/clustering/row-shell deserialization inventory
 * identified in garbage-free-compaction-improvements/read-path-allocation-plan.md.
 *
 * Methodology follows CursorCompactionAllocationGateTest: warm up past JIT tiering
 * uninstrumented, then record jdk.ObjectAllocationInNewTLAB/OutsideTLAB with stacks over
 * N more workload passes and dump the .jfr for offline aggregation. Also cross-checks the
 * JFR sample totals against ThreadMXBean.getThreadAllocatedBytes on the read thread.
 */
public class ReadPathAllocationProfileTest extends CQLTester
{
    private static final Path OUT_DIR =
        Path.of("/Users/jhaddad/dev/cassandra/garbage-free-compaction-improvements/jfr-reports/read-path");

    private static final int NARROW_PARTITIONS = 200;
    private static final int ROWS_PER_NARROW = 8;
    private static final long WIDE_PK = 1_000_000L;
    private static final int WIDE_ROWS = 1000;
    private static final int FLUSH_ROUNDS = 5;
    private static final int WARMUP_PASSES = 30;
    private static final int MEASURED_PASSES = 400;

    @Test
    public void recordReadAllocationProfile() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, " +
                    "v1 bigint, v2 text, v3 int, v4 text, v5 double, v6 bigint, v7 text, v8 int, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        loadData();

        int sstables = cfs.getLiveSSTables().size();
        assertEquals("expected exactly FLUSH_ROUNDS overlapping sstables", FLUSH_ROUNDS, sstables);
        assertTrue("expected live data in the memtable",
                   cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize() > 0);

        // sanity: the workload actually reads merged data of the expected shape
        sanityCheckReads();

        com.sun.management.ThreadMXBean bean =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemoryEnabled())
            bean.setThreadAllocatedMemoryEnabled(true);
        long tid = Thread.currentThread().getId();

        // warm up to C2 steady state, uninstrumented
        for (int i = 0; i < WARMUP_PASSES; i++)
            readPass();

        Files.createDirectories(OUT_DIR);
        String config = org.apache.cassandra.config.DatabaseDescriptor.getMemtableAllocationType()
                                                                      .name().toLowerCase();
        Path jfrPath = OUT_DIR.resolve("read-path-" + config + ".jfr");

        long[] perPass = new long[MEASURED_PASSES];
        long measuredTotal;
        try (jdk.jfr.Recording recording = new jdk.jfr.Recording())
        {
            recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
            recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
            recording.start();
            long before = bean.getThreadAllocatedBytes(tid);
            for (int i = 0; i < MEASURED_PASSES; i++)
            {
                long passBefore = bean.getThreadAllocatedBytes(tid);
                readPass();
                perPass[i] = bean.getThreadAllocatedBytes(tid) - passBefore;
            }
            measuredTotal = bean.getThreadAllocatedBytes(tid) - before;
            recording.stop();
            recording.dump(jfrPath);
        }

        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (long p : perPass) { min = Math.min(min, p); max = Math.max(max, p); }

        StringBuilder meta = new StringBuilder();
        meta.append("memtable allocation type: ").append(config).append('\n');
        meta.append("read thread name: ").append(Thread.currentThread().getName()).append('\n');
        meta.append("sstables: ").append(sstables).append('\n');
        meta.append("memtable live data bytes: ")
            .append(cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize()).append('\n');
        meta.append("warmup passes: ").append(WARMUP_PASSES).append('\n');
        meta.append("measured passes: ").append(MEASURED_PASSES).append('\n');
        meta.append("threadmxbean total allocated over measured passes (read thread only): ")
            .append(measuredTotal).append('\n');
        meta.append("per-pass allocated bytes min/max: ").append(min).append('/').append(max).append('\n');
        meta.append("per-pass values: ");
        for (long p : perPass) meta.append(p).append(' ');
        meta.append('\n');
        Files.writeString(OUT_DIR.resolve("read-path-" + config + "-meta.txt"), meta.toString());

        logger.info("read-path allocation profile dumped to {}", jfrPath);
    }

    private void loadData() throws Throwable
    {
        for (int round = 0; round < FLUSH_ROUNDS; round++)
        {
            // narrow partitions: every round rewrites every (pk, ck) => 5-way overlap on merge.
            // Odd ck rows are sparse (2 of 8 regular columns), even ck rows set all 8.
            for (long pk = 0; pk < NARROW_PARTITIONS; pk++)
            {
                for (long ck = 0; ck < ROWS_PER_NARROW; ck++)
                {
                    if (ck % 2 == 1)
                        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)",
                                pk, ck, pk * 100 + ck + round, "sparse-" + round + "-" + ck);
                    else
                        execute("INSERT INTO %s (pk, ck, v1, v2, v3, v4, v5, v6, v7, v8) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                pk, ck, pk + round, "text-value-" + round + "-" + pk + "-" + ck,
                                (int) ck, "second-text-column-" + round, (double) round + 0.5,
                                pk * ck, "third-text-" + ck, round);
                }
            }
            // wide partition: interleaved clustering stripes so every sstable contributes
            // to any slice of the partition (ck % FLUSH_ROUNDS == round)
            for (long ck = round; ck < WIDE_ROWS; ck += FLUSH_ROUNDS)
                execute("INSERT INTO %s (pk, ck, v1, v2, v3, v4, v5, v6, v7, v8) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        WIDE_PK, ck, ck, "wide-value-" + ck, (int) ck, "wide-text-" + round,
                        ck * 1.5, ck * 7, "w" + ck, round);

            // tombstones written in the LAST flush round (after that round's inserts, so the
            // deletes carry later timestamps and are not shadowed by re-inserts); they overlap
            // live data present in all five sstables
            if (round == FLUSH_ROUNDS - 1)
            {
                for (long pk = 0; pk < NARROW_PARTITIONS; pk += 10)
                    execute("DELETE FROM %s WHERE pk = ? AND ck = 2", pk);          // row tombstones
                for (long pk = 0; pk < NARROW_PARTITIONS; pk += 7)
                    execute("DELETE v2 FROM %s WHERE pk = ? AND ck = 4", pk);        // cell tombstones
                execute("DELETE FROM %s WHERE pk = ? AND ck >= 100 AND ck < 140", WIDE_PK); // range tombstone
                for (long ck = 500; ck < 520; ck += 2)
                    execute("DELETE FROM %s WHERE pk = ? AND ck = ?", WIDE_PK, ck);  // wide row tombstones
            }
            flush();
        }

        // final round stays in the memtable: reads genuinely merge memtable + 5 sstables
        for (long pk = 0; pk < NARROW_PARTITIONS; pk += 3)
        {
            execute("INSERT INTO %s (pk, ck, v1, v3) VALUES (?, ?, ?, ?)", pk, 1L, pk, 42);
            execute("INSERT INTO %s (pk, ck, v1, v3) VALUES (?, ?, ?, ?)", pk, 5L, pk, 43);
        }
        for (long ck = 7; ck < WIDE_ROWS; ck += 10)
        {
            if (ck >= 100 && ck < 140)
                continue; // don't resurrect the range-tombstoned band
            execute("INSERT INTO %s (pk, ck, v2, v5) VALUES (?, ?, ?, ?)", WIDE_PK, ck, "memtable-" + ck, 9.9);
        }
    }

    /** One workload pass: 100 point reads, 20 full narrow-partition reads,
     *  5 wide-partition slices of ~200 rows, 1 partition-range scan. */
    private void readPass() throws Throwable
    {
        for (long pk = 0; pk < NARROW_PARTITIONS; pk += 2)
            execute("SELECT * FROM %s WHERE pk = ? AND ck = ?", pk, pk % ROWS_PER_NARROW);
        for (long pk = 0; pk < NARROW_PARTITIONS; pk += 10)
            execute("SELECT * FROM %s WHERE pk = ?", pk);
        for (long start = 0; start < WIDE_ROWS; start += 200)
            execute("SELECT * FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", WIDE_PK, start, start + 200);
        execute("SELECT * FROM %s LIMIT 1000");
    }

    private void sanityCheckReads() throws Throwable
    {
        // full narrow partition: 8 rows, minus the round-3 row tombstone on pk % 10 == 0
        UntypedResultSet full = execute("SELECT * FROM %s WHERE pk = ?", 0L);
        assertEquals(ROWS_PER_NARROW - 1, full.size());
        UntypedResultSet full3 = execute("SELECT * FROM %s WHERE pk = ?", 3L);
        assertEquals(ROWS_PER_NARROW, full3.size());

        // cell tombstone: pk 7, ck 4 has v2 deleted
        UntypedResultSet cell = execute("SELECT v2 FROM %s WHERE pk = ? AND ck = 4", 7L);
        assertEquals(1, cell.size());
        assertTrue("v2 should be cell-tombstoned", !cell.one().has("v2"));

        // wide slice covering the range tombstone: 200 - 40 deleted
        UntypedResultSet slice = execute("SELECT * FROM %s WHERE pk = ? AND ck >= ? AND ck < ?",
                                         WIDE_PK, 0L, 200L);
        assertEquals(160, slice.size());
        // wide slice covering the row tombstones: 200 - 10 deleted
        UntypedResultSet slice5 = execute("SELECT * FROM %s WHERE pk = ? AND ck >= ? AND ck < ?",
                                          WIDE_PK, 400L, 600L);
        assertEquals(190, slice5.size());
        // memtable overlay visible
        UntypedResultSet mem = execute("SELECT v2, v5 FROM %s WHERE pk = ? AND ck = ?", WIDE_PK, 7L);
        assertEquals("memtable-7", mem.one().getString("v2"));
    }
}
