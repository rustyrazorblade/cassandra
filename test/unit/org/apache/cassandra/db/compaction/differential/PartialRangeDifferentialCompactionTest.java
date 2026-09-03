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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A compaction over a token subrange, the shape of one UCS shard task. Every input straddles the
 * range, so every scanner is partial and the cursor reads a bounded segment of each sstable.
 */
public class PartialRangeDifferentialCompactionTest extends DifferentialCompactionTester
{
    private static final int PARTITIONS = 20;
    private static final int ROWS_PER_PARTITION = 10;

    /** The sstable's partition keys in file order. */
    private static List<DecoratedKey> keysInFileOrder(SSTableReader sstable)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    keys.add(partition.partitionKey());
                }
            }
        }
        return keys;
    }

    /** A CompactionTask over {@code range}, which is how a UCS shard task reaches the strategy's scanners. */
    private static TaskFactory taskOver(Range<Token> range)
    {
        return (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, true)
        {
            @Override
            protected Range<Token> tokenRange()
            {
                return range;
            }
        };
    }

    @Test
    public void tokenSubrangeOfOverlappingSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < PARTITIONS; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)",
                            pk, ck, round * 1000 + ck, "round-" + round + "-" + ck);
            flush();
        }
        assertEquals(3, cfs.getLiveSSTables().size());

        // (keys[4], keys[13]]: nine interior partitions, so every sstable straddles both ends
        List<DecoratedKey> keys = keysInFileOrder(cfs.getLiveSSTables().iterator().next());
        assertEquals(PARTITIONS, keys.size());
        Range<Token> range = new Range<>(keys.get(4).getToken(), keys.get(13).getToken());
        int partitionsInRange = 9;

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(Collections.singleton(range));
            assertEquals("expected one segment per sstable", 1, bounds.size());
            assertTrue("the segment must start after the file start, or the scanner is not partial", bounds.get(0).lowerPosition > 0);
            assertTrue("the segment must end before the file end, or the scanner is not partial",
                       bounds.get(0).upperPosition < sstable.uncompressedLength());
        }

        // compactPath asserts that the cursor pipeline actually ran, so a fallback fails here
        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskOver(range));

        assertEquals("expected a single compaction output", 1, out.sstables.size());
        String expectedRows = "totalRows=" + (partitionsInRange * ROWS_PER_PARTITION) + ' ';
        assertTrue("the output must hold exactly the partitions inside the range; got: " + out.sstables.get(0).statsSummary,
                   out.sstables.get(0).statsSummary.contains(expectedRows));
    }
}
