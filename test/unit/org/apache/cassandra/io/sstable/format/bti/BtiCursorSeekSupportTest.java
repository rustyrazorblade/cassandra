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

package org.apache.cassandra.io.sstable.format.bti;

import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link BtiCursorSeekSupport#blockEndAfter}: the end of the row index block that holds a slice
 * end is the start of a later block, or the partition end after the last block.
 */
public class BtiCursorSeekSupportTest extends CQLTester
{
    private int originalColumnIndexSizeKiB;

    @Before
    public void smallBlocks()
    {
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    @Test
    public void blockEndAfterIsTheNextBlockStart() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String pad = "x".repeat(100);
        int rows = 500;
        for (long ck = 0; ck < rows; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, pad);
        flush();
        BtiTableReader sstable = (BtiTableReader) cfs.getLiveSSTables().iterator().next();
        ClusteringComparator comparator = cfs.metadata().comparator;
        DecoratedKey key = cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(1L));
        BtiCursorSeekSupport.PartitionEntry entry = BtiCursorSeekSupport.exactPartitionEntry(sstable, key, SSTableReadsListener.NOOP_LISTENER);
        assertNotNull(entry);
        assertTrue("the partition must be row-indexed", entry.isIndexed());

        try (RowIndexReader index = BtiCursorSeekSupport.openRowIndex(sstable, entry))
        {
            long[] blockOf = new long[rows];
            TreeSet<Long> blockStarts = new TreeSet<>();
            for (int ck = 0; ck < rows; ck++)
            {
                blockOf[ck] = BtiCursorSeekSupport.floorBlock(sstable, index, entry, comparator,
                                                              ClusteringBound.create(comparator, true, true, (long) ck)).dataPosition;
                blockStarts.add(blockOf[ck]);
            }
            assertTrue("expected several row index blocks", blockStarts.size() > 10);
            long lastBlock = blockStarts.last();
            for (int ck = 0; ck < rows; ck++)
            {
                long end = BtiCursorSeekSupport.blockEndAfter(sstable, index, entry, comparator,
                                                              ClusteringBound.create(comparator, false, true, (long) ck));
                Long next = blockStarts.higher(blockOf[ck]);
                Long afterNext = next == null ? null : blockStarts.higher(next);
                assertTrue("row " + ck + ": " + end + " must be after the row's block " + blockOf[ck], end > blockOf[ck]);
                if (blockStarts.contains(end))
                    assertTrue("row " + ck + ": " + end + " must be at most the second block after the row's", afterNext == null || end <= afterNext);
                else
                    // after the last block, the index points at the partition end
                    assertTrue("row " + ck + ": " + end + " must be a block start or the partition end",
                               end > lastBlock && end < sstable.uncompressedLength() && (next == null || next == lastBlock));
            }
        }
    }
}
