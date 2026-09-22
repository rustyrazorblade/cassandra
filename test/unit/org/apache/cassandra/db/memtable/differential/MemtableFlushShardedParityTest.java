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
 * Pins {@code UnifiedCompactionStrategy} with a fixed shard count greater than one and writes
 * partitions whose tokens span the ring, so the flush splits its output across several sstables at
 * shard boundaries.  Compares the sharded cursor output against the iterator path byte-for-byte,
 * shard by shard, via {@link #assertFlushMatches}, and asserts the cursor path actually ran.
 */
public class MemtableFlushShardedParityTest extends MemtableFlushDifferentialTester
{
    // Fixed 4-shard flush: sstable_growth=1 forces exactly base_shard_count shards, and
    // min_sstable_size=0 stops a small flush from collapsing below that.
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
            // int partition keys hash across the whole ring, so 500 of them fill all 4 shards;
            // USING TIMESTAMP keeps the output free of wall-clock fields for the byte-for-byte
            // comparison.
            for (int k = 0; k < PARTITIONS; k++)
                execute("INSERT INTO " + t + " (k, v) VALUES (?, ?) USING TIMESTAMP " + (ts++),
                        k, "value-for-partition-" + k);
        });

        // Check the scenario actually sharded: a single sstable means the multi-writer rollover
        // never happened.
        assertTrue("expected the flush to split across multiple shards, but got "
                   + iterator.sstables.size() + " sstable(s)",
                   iterator.sstables.size() > 1);
        assertEquals("fixed 4-shard flush should produce 4 sstables", 4, iterator.sstables.size());
    }
}
