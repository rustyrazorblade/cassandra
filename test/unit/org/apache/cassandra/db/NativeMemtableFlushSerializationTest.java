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

import java.lang.reflect.Field;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.NativeCell;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.NativePool;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Flushing an {@code offheap_objects} memtable writes the bytes that the same rows on heap write: every
 * row and marker of the flush set, serialized as flush serializes it, matches its heap copy byte for byte.
 * The memtable's cells and clusterings are read through {@code NativeAccessor}, the heap copy's through
 * {@code ByteBufferAccessor}, so sstables flushed by this build match those of a 5.0 node without it.
 */
public class NativeMemtableFlushSerializationTest extends CQLTester
{
    // The skiplist memtable copies cells to the heap when it is read, so only the trie memtable flushes native cells.
    private static final String TRIE_MEMTABLE = "memtable = 'trie'";

    // Shadows CQLTester.setUpClass so the allocation type is set before the memtable pool exists.
    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            ((Config) confField.get(null)).memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        prePrepareServer();
        prepareServer();
    }

    @Test
    public void regularTable() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk int, c1 text, c2 blob, s text static, v1 bigint, v2 text, v3 blob, v4 decimal, " +
                    "v5 uuid, v6 frozen<tuple<int, text>>, l list<text>, st set<blob>, m map<int, text>, fm frozen<map<text, int>>, " +
                    "u " + type + ", PRIMARY KEY (pk, c1, c2)) WITH " + TRIE_MEMTABLE);
        for (int pk = 0; pk < 20; pk++)
        {
            for (int c = 0; c < 10; c++)
            {
                long ts = 1000 + pk * 10 + c;
                execute("INSERT INTO %s (pk, c1, c2, s, v1, v2, v3, v4, v5, v6, l, st, m, fm, u) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 1.5, 5c4b9a2a-6c1f-4f7e-9b0e-4a3c2d1e0f00, (?, ?), ?, ?, ?, ?, {a: ?, b: ?}) USING TIMESTAMP " + ts,
                        pk, "c-" + c, ByteBufferUtil.bytes(c), "s" + pk, (long) pk * c, "value-" + "x".repeat(c * 30), ByteBufferUtil.bytes((long) c),
                        c, "t" + c, list("a", "b" + c), set(ByteBufferUtil.bytes(c), ByteBufferUtil.bytes(c + 100)),
                        map(1, "one", c, "v" + c), map("k" + c, c), c, "u" + c);
            }
            execute("UPDATE %s USING TIMESTAMP 5000 AND TTL 100000 SET v2 = 'expiring' WHERE pk = ? AND c1 = 'c-1' AND c2 = ?", pk, ByteBufferUtil.bytes(1));
            execute("DELETE v1 FROM %s USING TIMESTAMP 5000 WHERE pk = ? AND c1 = 'c-2' AND c2 = ?", pk, ByteBufferUtil.bytes(2));
            execute("DELETE m[1] FROM %s USING TIMESTAMP 5000 WHERE pk = ? AND c1 = 'c-3' AND c2 = ?", pk, ByteBufferUtil.bytes(3));
            execute("UPDATE %s USING TIMESTAMP 5000 SET l = ['replaced'] WHERE pk = ? AND c1 = 'c-4' AND c2 = ?", pk, ByteBufferUtil.bytes(4));
            execute("DELETE FROM %s USING TIMESTAMP 5000 WHERE pk = ? AND c1 = 'c-5' AND c2 = ?", pk, ByteBufferUtil.bytes(5));
            execute("DELETE FROM %s USING TIMESTAMP 5000 WHERE pk = ? AND c1 > 'c-6' AND c1 < 'c-8'", pk);
            execute("DELETE FROM %s USING TIMESTAMP 5000 WHERE pk = ? AND c1 = 'c-9'", pk);
        }
        execute("DELETE FROM %s USING TIMESTAMP 6000 WHERE pk = 19");
        assertFlushBytesMatchHeap();
    }

    @Test
    public void counterTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck text, c1 counter, c2 counter, PRIMARY KEY (pk, ck)) WITH " + TRIE_MEMTABLE);
        for (int pk = 0; pk < 10; pk++)
            for (int ck = 0; ck < 10; ck++)
                execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 - ? WHERE pk = ? AND ck = ?", (long) ck, (long) pk, pk, "ck" + ck);
        assertFlushBytesMatchHeap();
    }

    private void assertFlushBytesMatchHeap() throws Exception
    {
        assertTrue("the offheap_objects config did not take effect", AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = cfs.metadata();
        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        // the header and version flush gives the sstable writer
        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
        SerializationHelper helper = new SerializationHelper(header);
        int version = DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion().correspondingMessagingVersion();

        int nativeCells = 0;
        int unfiltereds = 0;
        for (Partition partition : memtable.getFlushSet(null, null))
        {
            try (UnfilteredRowIterator iter = partition.unfilteredIterator())
            {
                Row staticRow = iter.staticRow();
                if (!staticRow.isEmpty())
                {
                    nativeCells += countNativeCells(staticRow);
                    assertSameBytes(staticRow, staticRow.clone(HeapCloner.instance), helper, version);
                    unfiltereds++;
                }
                while (iter.hasNext())
                {
                    Unfiltered unfiltered = iter.next();
                    Unfiltered onHeap;
                    if (unfiltered.isRow())
                    {
                        nativeCells += countNativeCells((Row) unfiltered);
                        onHeap = ((Row) unfiltered).clone(HeapCloner.instance);
                    }
                    else
                    {
                        onHeap = ((RangeTombstoneMarker) unfiltered).clone(HeapCloner.instance);
                    }
                    assertSameBytes(unfiltered, onHeap, helper, version);
                    unfiltereds++;
                }
            }
        }
        assertTrue("no unfiltered compared", unfiltereds > 0);
        assertTrue("the memtable held no native cells", nativeCells > 0);
    }

    private static int countNativeCells(Row row)
    {
        int count = 0;
        for (Cell<?> cell : row.cells())
            if (cell instanceof NativeCell)
                count++;
        return count;
    }

    private static void assertSameBytes(Unfiltered nativeUnfiltered, Unfiltered heapUnfiltered, SerializationHelper helper, int version) throws Exception
    {
        try (DataOutputBuffer nativeOut = new DataOutputBuffer(); DataOutputBuffer heapOut = new DataOutputBuffer())
        {
            UnfilteredSerializer.serializer.serialize(nativeUnfiltered, helper, nativeOut, 0, version);
            UnfilteredSerializer.serializer.serialize(heapUnfiltered, helper, heapOut, 0, version);
            assertArrayEquals(nativeUnfiltered.toString(), heapOut.toByteArray(), nativeOut.toByteArray());
        }
    }
}
