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
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

/** Builds the full and transient ranges that anticompaction tests pass to {@code antiCompactGroup}. */
final class AntiCompactionRanges
{
    private AntiCompactionRanges()
    {
    }

    /** The token of every partition in the live set, in ring order, without duplicates. */
    static List<Long> liveTokens(ColumnFamilyStore cfs)
    {
        assertTrue("anticompaction tests need Murmur3Partitioner, not " + cfs.getPartitioner(),
                   cfs.getPartitioner() instanceof Murmur3Partitioner);
        return tokens(cfs.getLiveSSTables());
    }

    /** The token of every partition in {@code sstables}, in ring order, without duplicates. */
    static List<Long> tokens(Collection<SSTableReader> sstables)
    {
        TreeSet<Long> tokens = new TreeSet<>();
        for (SSTableReader sstable : sstables)
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        tokens.add(partition.partitionKey().getToken().getLongValue());
                    }
                }
            }
        }
        return new ArrayList<>(tokens);
    }

    static Range<Token> range(long left, long right)
    {
        return new Range<>(new LongToken(left), new LongToken(right));
    }

    /** The range (left, minimum token], which runs to the end of the ring. */
    static Range<Token> rangeToMinimum(long left)
    {
        return new Range<>(new LongToken(left), Murmur3Partitioner.MINIMUM);
    }

    static RangesAtEndpoint ranges(List<Range<Token>> full, List<Range<Token>> trans)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(FBUtilities.getBroadcastAddressAndPort());
        for (Range<Token> range : full)
            builder.add(Replica.fullReplica(FBUtilities.getBroadcastAddressAndPort(), range));
        for (Range<Token> range : trans)
            builder.add(Replica.transientReplica(FBUtilities.getBroadcastAddressAndPort(), range));
        return builder.build();
    }

    static RangesAtEndpoint fullOnly(List<Range<Token>> full)
    {
        return ranges(full, List.of());
    }

    /**
     * Cuts the ring at {@code cuts} random tokens, and gives each piece a random status: full,
     * transient, or not repaired.  Half of the cuts fall on or next to a live token.  The pieces do
     * not overlap.  The last piece wraps around the ring.  At least one piece is full or transient.
     */
    static RangesAtEndpoint randomRanges(Random random, List<Long> tokens, int cuts)
    {
        assertTrue("need at least one live token", !tokens.isEmpty());
        assertTrue("need at least one cut", cuts > 0);
        TreeSet<Long> boundaries = new TreeSet<>();
        while (boundaries.size() < cuts)
        {
            if (random.nextBoolean())
            {
                // Hit a token exactly, or fall just before or after it.
                long near = tokens.get(random.nextInt(tokens.size()));
                boundaries.add(near + random.nextInt(3) - 1);
            }
            else
            {
                boundaries.add(random.nextLong());
            }
        }
        List<Long> sorted = new ArrayList<>(boundaries);
        List<Range<Token>> full = new ArrayList<>();
        List<Range<Token>> trans = new ArrayList<>();
        for (int i = 0; i < sorted.size(); i++)
        {
            long left = sorted.get(i);
            long right = sorted.get((i + 1) % sorted.size());
            if (left == right)
                continue;
            int status = random.nextInt(3);
            if (status == 0)
                full.add(range(left, right));
            else if (status == 1)
                trans.add(range(left, right));
        }
        if (full.isEmpty() && trans.isEmpty())
            full.add(range(sorted.get(0), sorted.get(sorted.size() > 1 ? 1 : 0) + 1));
        return ranges(full, trans);
    }
}
