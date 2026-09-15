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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The other differential tests here let the ambient compaction strategy decide the flush's output
 * shape, so under a single-output strategy (STCS et al.) they only ever exercise cursor flush into
 * one sstable. This one pins {@code UnifiedCompactionStrategy} with a fixed, greater-than-one shard
 * count on the table itself, and writes partitions whose tokens span the whole ring, so the flush
 * must split its output across several sstables at shard boundaries - the case that silently fell
 * back to the iterator path before CASSANDRA-21554 (a {@code ShardedMultiWriter} failed
 * {@code Flushing.canUseCursorFlush}'s writer-shape gate).
 * <p>
 * It then holds the sharded cursor output to the same bar as every other scenario here: the flush
 * transaction's own {@link org.apache.cassandra.db.compaction.unified.ShardedMultiWriter} splits
 * the iterator-path table identically, and the two are compared byte-for-byte, shard by shard, via
 * {@link #assertFlushMatches}. The harness additionally asserts the cursor path actually ran
 * (rather than falling back), so a regression that re-broke the writer-shape gate would fail here
 * loudly instead of passing vacuously.
 */
public class MemtableFlushShardedParityTest extends MemtableFlushDifferentialTester
{
    // Fixed 4-shard flush regardless of density: sstable_growth=1 forces exactly base_shard_count
    // shards, and min_sstable_size=0 stops a small flush from collapsing back below that. Both
    // paths flush through this same strategy, so both split at the same 4 boundaries.
    private static final String SHARDED_UCS =
        "CREATE TABLE %s (k int PRIMARY KEY, v text) " +
        "WITH compaction = {'class': 'UnifiedCompactionStrategy', 'base_shard_count': '4', " +
        "'sstable_growth': '1', 'min_sstable_size': '0B'}";

    private static final int PARTITIONS = 500;

    @Test
    public void shardedFlushMatchesIteratorPathByteForByte() throws Exception
    {
        CapturedOutput iterator = assertFlushMatches(SHARDED_UCS, (ks, tbl) -> {
            String t = ks + "." + tbl;
            long ts = 5_000_000_000_000L;
            // int partition keys hash across the whole Murmur3 ring, so 500 of them populate every
            // one of the 4 shards; USING TIMESTAMP keeps the output free of wall-clock-derived
            // fields so the strict byte-for-byte comparison applies (see the harness's retry note).
            for (int k = 0; k < PARTITIONS; k++)
                execute("INSERT INTO " + t + " (k, v) VALUES (?, ?) USING TIMESTAMP " + (ts++),
                        k, "value-for-partition-" + k);
        });

        // Prove the scenario actually sharded: a single output sstable would mean the interesting
        // multi-writer rollover never happened and this test degenerated into the single-shard case
        // the other tests already cover.
        assertTrue("expected UnifiedCompactionStrategy to split the flush across multiple shards, but got "
                   + iterator.sstables.size() + " sstable(s); the sharded rollover was not exercised",
                   iterator.sstables.size() > 1);
        assertEquals("fixed 4-shard flush should produce 4 sstables", 4, iterator.sstables.size());
    }
}
