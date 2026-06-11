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

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Volume scenario: TWO MILLION written rows across TWENTY input sstables — 2,000 partitions
 * x 50 rows per round x 20 flush rounds, with each round's clustering window overlapping the
 * previous round's by half so most output rows genuinely merge from 2-3 inputs. The mutation
 * mix runs at scale: ~6% multi-cell map cells, ~14% TTL'd rows, ~8% explicit-timestamp ties
 * on overwritten keys, ~3% null-overwrite cell tombstones, plus per-round row deletes,
 * bounded and single-sided range deletes, and cycling partition deletes with resurrection.
 *
 * Runs in scale-capture mode (see DifferentialCompactionTester.scaleCapture): the logical
 * dump streams into a SHA-256 digest and byte comparison streams, so harness memory stays
 * flat. Two generations as everywhere; the BTI subclass covers the trie format.
 *
 * All scale parameters are property-configurable (defaults reproduce the standard 2M-row /
 * 20-sstable run). Properties must reach the forked test JVM via -Dtest.jvm.args:
 *
 *   ant testsome -Dtest.name=...BigVolumeDifferentialCompactionTest \
 *       -Dtest.timeout=14400000 \
 *       -Dtest.jvm.args="-Dcassandra.test.differential.bigvolume.rounds=40
 *                        -Dcassandra.test.differential.bigvolume.partitions=10000
 *                        -Dcassandra.test.differential.bigvolume.rows_per_round=100
 *                        -Dcassandra.test.differential.bigvolume.value_padding=200"
 *
 *  - rounds          = number of input sstables (one flush per round)
 *  - partitions      = partitions per round
 *  - rows_per_round  = rows per partition per round (total rows = rounds x partitions x this)
 *  - value_padding   = extra bytes appended to every text value (row size knob; default 0)
 *
 * Delete counts scale with the partition count; the clustering stride is derived so
 * consecutive rounds always overlap by roughly half a window.
 */
public class BigVolumeDifferentialCompactionTest extends DifferentialCompactionTester
{
    private static final Set<String> ALLOWLIST = Set.of();

    private static final int ROUNDS = Integer.getInteger("cassandra.test.differential.bigvolume.rounds", 20);
    private static final int PARTITIONS = Integer.getInteger("cassandra.test.differential.bigvolume.partitions", 2000);
    private static final int ROWS_PER_ROUND = Integer.getInteger("cassandra.test.differential.bigvolume.rows_per_round", 50);
    private static final String VALUE_PADDING =
        "p".repeat(Integer.getInteger("cassandra.test.differential.bigvolume.value_padding", 0));
    /** < ROWS_PER_ROUND so consecutive rounds overlap by roughly half a window (50 -> 23, the original). */
    private static final int CK_STRIDE = Math.max(1, ROWS_PER_ROUND / 2 - 2);

    @Override
    protected boolean scaleCapture()
    {
        return true;
    }

    @Test
    public void twoMillionRowsTwentySSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, m map<text, bigint>, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        logger.info("big-volume parameters: rounds={} partitions={} rowsPerRound={} valuePadding={}B " +
                    "ckStride={} -> {} total rows across {} input sstables",
                    ROUNDS, PARTITIONS, ROWS_PER_ROUND, VALUE_PADDING.length(), CK_STRIDE,
                    (long) ROUNDS * PARTITIONS * ROWS_PER_ROUND, ROUNDS);

        String insert = "INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)";
        String insertTtl = insert + " USING TTL 86400";
        String insertTs = insert + " USING TIMESTAMP 5000";
        String insertMap = "INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?)";

        for (int round = 0; round < ROUNDS; round++)
        {
            long ckBase = (long) round * CK_STRIDE;
            for (long pk = 0; pk < PARTITIONS; pk++)
            {
                for (int j = 0; j < ROWS_PER_ROUND; j++)
                {
                    long ck = ckBase + j;
                    long v1 = ck * 31 + round;
                    String v2 = j % 31 == 30 ? null : "v" + round + "_" + ck + VALUE_PADDING;
                    if (j % 20 == 5)
                        execute(insertMap, pk, ck, v1, v2, map("k" + (ck % 3), ck, "r", (long) round));
                    else if (j % 7 == 3)
                        execute(insertTtl, pk, ck, v1, v2);
                    else if (j % 13 == 7)
                        execute(insertTs, pk, ck, v1, "tie" + round + "_" + ck + VALUE_PADDING);
                    else
                        execute(insert, pk, ck, v1, v2);
                }
            }

            // delete counts scale with the partition count (defaults: 100 / 30 / 20)
            for (int i = 0; i < Math.max(1, PARTITIONS / 20); i++)
                execute("DELETE FROM %s WHERE pk = ? AND ck = ?",
                        (long) ((round * 97 + i * 13) % PARTITIONS), ckBase + (i % ROWS_PER_ROUND));
            for (int i = 0; i < Math.max(1, PARTITIONS / 66); i++)
            {
                long start = ckBase + (i % 40);
                execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?",
                        (long) ((round * 53 + i * 29) % PARTITIONS), start, start + 4);
            }
            for (int i = 0; i < Math.max(1, PARTITIONS / 100); i++)
            {
                long pk = (round * 41 + i * 17) % PARTITIONS;
                if (i % 2 == 0)
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", pk, ckBase + ROWS_PER_ROUND / 2);
                else
                    execute("DELETE FROM %s WHERE pk = ? AND ck <= ?", pk, ckBase + 5);
            }
            // cycling partition deletes over the same 5 partitions: tombstone + resurrection
            execute("DELETE FROM %s WHERE pk = ?", (long) (Math.max(0, PARTITIONS - 5) + round % Math.min(5, PARTITIONS)));

            flush();
            logger.info("big-volume round {}/{} flushed", round + 1, ROUNDS);
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }
}
