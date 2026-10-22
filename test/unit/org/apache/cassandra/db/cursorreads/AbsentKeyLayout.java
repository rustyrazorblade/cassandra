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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Partition keys (bigint) chosen by token, so that sstables cover known token ranges and keys that
 * are in no sstable fall inside the range of 0, 1 or several sstables.  {@link #layer} gives the keys
 * one sstable holds; {@link #absentKeys} picks, after the flushes, one absent key per coverage count.
 */
final class AbsentKeyLayout
{
    /** Candidate keys, in token order. */
    private final long[] byToken;

    AbsentKeyLayout(IPartitioner partitioner, int candidates)
    {
        Long[] keys = new Long[candidates];
        for (int i = 0; i < candidates; i++)
            keys[i] = 1000L + i;
        Arrays.sort(keys, Comparator.comparing(k -> partitioner.decorateKey(ByteBufferUtil.bytes(k))));
        byToken = new long[candidates];
        for (int i = 0; i < candidates; i++)
            byToken[i] = keys[i];
    }

    /** Every {@code step}th key with a token rank in [from, to). */
    long[] layer(int from, int to, int step)
    {
        List<Long> keys = new ArrayList<>();
        for (int rank = from; rank < to; rank += step)
            keys.add(byToken[rank]);
        return keys.stream().mapToLong(Long::longValue).toArray();
    }

    /** The key at one token rank. */
    long at(int rank)
    {
        return byToken[rank];
    }

    /**
     * For each count of live sstables whose key range covers the key (0, 1, 2, ...), the first key
     * in token order that no sstable or memtable holds, labelled by the count.
     *
     * @param written every key written to the table, in any layer or the memtable
     */
    Map<String, Long> absentKeys(ColumnFamilyStore cfs, long[] written)
    {
        long[] sortedWritten = written.clone();
        Arrays.sort(sortedWritten);
        Map<Integer, Long> byCoverage = new TreeMap<>();
        for (long key : byToken)
        {
            if (Arrays.binarySearch(sortedWritten, key) >= 0)
                continue;
            DecoratedKey decorated = cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(key));
            int covering = 0;
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                if (sstable.getFirst().compareTo(decorated) <= 0 && sstable.getLast().compareTo(decorated) >= 0)
                    covering++;
            }
            byCoverage.putIfAbsent(covering, key);
        }
        Map<String, Long> labelled = new LinkedHashMap<>();
        for (Map.Entry<Integer, Long> e : byCoverage.entrySet())
            labelled.put("absent key in " + e.getKey() + " sstable range(s)", e.getValue());
        return labelled;
    }
}
