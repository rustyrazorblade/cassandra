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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Exercises {@code StatefulCursor.positionAt}/{@code setEndBound} (increment 4): a
 * partition-boundary-aligned token subrange, scanned directly off each sstable's
 * {@code StatefulCursor}, must contain exactly the partitions whose decorated key falls in
 * [startBound, endBound] (both inclusive) — no more, no fewer — matching what a full scan
 * filtered client-side to the same bound would show. The reference set is derived directly
 * (not via an {@code ISSTableScanner} + {@code Range<Token>}) because the scanner's Range is a
 * ring-relative (exclusive-start, inclusive-end) token range, a different — and fiddlier to
 * reproduce exactly — boundary convention than this PartitionPosition-based primitive's.
 * <p>
 * This test lives in {@code org.apache.cassandra.db.compaction} (not the {@code .differential}
 * sub-package) because {@code StatefulCursor} is package-private.
 */
public class BoundedScanStatefulCursorTest extends CQLTester
{
    @Test
    public void positionAtAndEndBoundRestrictScanToInclusiveSubrange() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        int partitionCount = 40;
        for (long pk = 0; pk < partitionCount / 2; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 1L, "v" + pk);
        flush();
        for (long pk = partitionCount / 2; pk < partitionCount; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 1L, "v" + pk);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertTrue("expected at least 2 flushed sstables (subrange should span both)", sstables.size() >= 2);
        IPartitioner partitioner = cfs.metadata().partitioner;

        // Token order, not insertion/pk order: a token subrange generally does not align with
        // either flush's pk range, so the expected slice spans both physical sstables.
        List<DecoratedKey> tokenOrdered = new ArrayList<>();
        for (long pk = 0; pk < partitionCount; pk++)
            tokenOrdered.add(partitioner.decorateKey(LongType.instance.decompose(pk)));
        tokenOrdered.sort(DecoratedKey::compareTo);

        int fromIndex = 10;
        int toIndex = 29; // inclusive
        PartitionPosition startBound = tokenOrdered.get(fromIndex);
        PartitionPosition endBound = tokenOrdered.get(toIndex);
        Set<Long> expectedPks = new HashSet<>();
        for (int i = fromIndex; i <= toIndex; i++)
            expectedPks.add(LongType.instance.compose(tokenOrdered.get(i).getKey()));
        assertEquals(toIndex - fromIndex + 1, expectedPks.size());

        Set<Long> scannedPks = new HashSet<>();
        for (SSTableReader reader : sstables)
        {
            try (StatefulCursor cursor = new StatefulCursor(reader, DatabaseDescriptor.getCompactionReadDiskAccessMode()))
            {
                cursor.setEndBound(endBound);
                int state = cursor.positionAt(startBound);
                while (state != DONE)
                {
                    state = cursor.readPartitionHeader();
                    if (state == DONE)
                        break;
                    scannedPks.add(LongType.instance.compose(cursor.currentKey().getKey()));
                    // skips every unfiltered in the partition, landing on PARTITION_START or DONE
                    state = cursor.skipPartition();
                }
            }
        }

        assertEquals(expectedPks, scannedPks);
    }

    /**
     * A bound past the end of every sstable resolves via {@code getPosition(..., GE)} returning
     * no match: the cursor must land in DONE immediately, not throw or hang.
     */
    @Test
    public void positionAtPastEndOfFileReportsDoneImmediately() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 1L, "v");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableReader reader : cfs.getLiveSSTables())
        {
            // a synthetic marker sorting just after this sstable's actual last key (same
            // token, but KeyBound(token, false) is constructed to sort after every real key
            // with that token) — deliberately per-reader, to avoid assuming a partitioner-wide
            // "maximum token" API that not every IPartitioner exposes.
            PartitionPosition pastEverything = reader.getLast().getToken().maxKeyBound();
            try (StatefulCursor cursor = new StatefulCursor(reader, DatabaseDescriptor.getCompactionReadDiskAccessMode()))
            {
                int state = cursor.positionAt(pastEverything);
                assertEquals(DONE, state);
            }
        }
    }
}
