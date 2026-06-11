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


import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * ONE giant partition, merged from many inputs — sized for the POSITIONAL boundaries that
 * row count alone never touches:
 *
 *  - intra-partition offsets crossing Integer.MAX_VALUE (2GiB) — int casts and vint widths
 *    on the partition-relative position arithmetic (previousUnfilteredSize chains, header
 *    lengths, index block offsets);
 *  - the promoted index at hundreds of thousands of index blocks in a single partition,
 *    orders of magnitude past the scale the index-promotion logic is normally exercised at;
 *  - small partitions on BOTH sides of the giant one, so per-partition state demonstrably
 *    resets after the monster.
 *
 * Parameters (defaults are CI-sane: ~1M rows, ~160MB partition, still ~40K index blocks at
 * the 4KiB test column_index_size). The MERGED partition size is what crosses boundaries:
 * with the default half-window overlap, distinct rows = (sstables-1) * ck_stride +
 * rows_per_sstable — NOT sstables * rows_per_sstable (an earlier "2.6GiB" boundary run
 * conflated pre-merge input rows with merged output rows and only reached ~1.4GiB).
 * The true 2GiB boundary run uses DISJOINT windows (ck_stride = rows_per_sstable):
 *
 *   ant testsome -Dtest.name=...LargePartitionDifferentialCompactionTest \
 *       -Dtest.timeout=14400000 \
 *       -Dtest.jvm.args="-Dcassandra.test.differential.largepartition.sstables=8
 *                        -Dcassandra.test.differential.largepartition.rows_per_sstable=1100000
 *                        -Dcassandra.test.differential.largepartition.value_padding=240
 *                        -Dcassandra.test.differential.largepartition.ck_stride=1100000"
 *
 * That is ~8.8M distinct rows at ~280B/row: a ~2.4GiB single MERGED partition. Peak disk for
 * the full two-generation differential (inputs + live output + four captured output copies)
 * is roughly 7x the partition size. The memtable may auto-flush large rounds, so the sstables
 * parameter is a minimum input count, which the differential does not care about.
 */
public class LargePartitionDifferentialCompactionTest extends DifferentialCompactionTester
{

    private static final int SSTABLES =
        Integer.getInteger("cassandra.test.differential.largepartition.sstables", 4);
    private static final int ROWS_PER_SSTABLE =
        Integer.getInteger("cassandra.test.differential.largepartition.rows_per_sstable", 250_000);
    private static final String VALUE_PADDING =
        "p".repeat(Integer.getInteger("cassandra.test.differential.largepartition.value_padding", 120));
    /**
     * Window stride between rounds. Default: half-window overlap, so every output row in the
     * overlap merges from two inputs. Set equal to rows_per_sstable for DISJOINT windows,
     * which maximizes the merged partition size (the 2GiB boundary run).
     */
    private static final long CK_STRIDE = Math.max(1,
        Integer.getInteger("cassandra.test.differential.largepartition.ck_stride", ROWS_PER_SSTABLE / 2));

    @Override
    protected boolean scaleCapture()
    {
        return true;
    }

    @Test
    public void giantPartition() throws Throwable
    {
        long distinctRows = (long) (SSTABLES - 1) * CK_STRIDE + ROWS_PER_SSTABLE;
        logger.info("large-partition parameters: sstables={} rowsPerSSTable={} ckStride={} valuePadding={}B " +
                    "-> ~{} distinct rows in one merged partition, ~{}MB serialized",
                    SSTABLES, ROWS_PER_SSTABLE, CK_STRIDE, VALUE_PADDING.length(),
                    distinctRows,
                    distinctRows * (40 + VALUE_PADDING.length()) / (1 << 20));

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String insert = "INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)";
        String insertTtl = insert + " USING TTL 86400";
        String insertTs = insert + " USING TIMESTAMP 5000";

        for (int round = 0; round < SSTABLES; round++)
        {
            long ckBase = round * CK_STRIDE;

            // small partitions on both sides of the giant one, every round
            execute(insert, 0L, (long) round, (long) round, "side" + round);
            execute(insert, 2L, (long) round, (long) round, "side" + round);

            for (int j = 0; j < ROWS_PER_SSTABLE; j++)
            {
                long ck = ckBase + j;
                long v1 = ck * 31 + round;
                String v2 = j % 31 == 30 ? null : "v" + round + "_" + ck + VALUE_PADDING;
                if (j % 7 == 3)
                    execute(insertTtl, 1L, ck, v1, v2);
                else if (j % 13 == 7)
                    execute(insertTs, 1L, ck, v1, "tie" + round + "_" + ck + VALUE_PADDING);
                else
                    execute(insert, 1L, ck, v1, v2);
            }

            // tombstones at various depths of the giant partition: bounded ranges inside
            // this round's window, an open-ended slice off its tail, scattered row deletes
            for (int i = 0; i < 10; i++)
            {
                long start = ckBase + (long) i * (ROWS_PER_SSTABLE / 12);
                execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, start, start + 7);
            }
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 1L, ckBase + ROWS_PER_SSTABLE - 11);
            for (int i = 0; i < 20; i++)
                execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, ckBase + (long) i * (ROWS_PER_SSTABLE / 21));

            flush();
            logger.info("large-partition round {}/{} flushed", round + 1, SSTABLES);
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }
}
