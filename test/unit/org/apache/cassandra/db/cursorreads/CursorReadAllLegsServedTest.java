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
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Every sstable leg of a read the support gate accepts must be served by a cursor, never by the
 * iterator path.  Each case runs the {@link CursorReadOracle} S1 and S2 surfaces, whose S6 check
 * requires the {@code sstableLegsFellBackToIterator} counter to stay zero and the served plus
 * without-partition legs to equal the sstable lookups the iterator run made.  Layouts: all BIG, all
 * BTI, BIG and BTI mixed in one read, with and without a memtable overlay, sstables that do not hold
 * the partition, and sstables written before a column was dropped (and re-added).
 */
public class CursorReadAllLegsServedTest extends CursorReadOracle
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void saveFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    private ColumnFamilyStore createTableWithoutCompaction()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, v2 text, l list<int>, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    /** One flushed sstable of {@code format} holding overlapping rows of partition 1 written at {@code ts}. */
    private void flushLayer(String format, int layer, long ts)
    {
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        for (long ck = layer; ck < 60; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2, l) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP " + ts,
                    1L, ck, ck * 10 + layer, "layer" + layer + "-" + ck, List.of(layer, (int) ck));
        execute("UPDATE %s USING TIMESTAMP " + ts + " SET s = ? WHERE pk = ?", "static-" + layer, 1L);
        execute("DELETE FROM %s USING TIMESTAMP " + (ts + 1) + " WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 20L + layer, 24L + layer);
        execute("DELETE FROM %s USING TIMESTAMP " + (ts + 1) + " WHERE pk = ? AND ck = ?", 1L, 40L + layer);
        flush();
    }

    private void memtableOverlay(long ts)
    {
        for (long ck = 5; ck < 60; ck += 7)
            execute("UPDATE %s USING TIMESTAMP " + ts + " SET v2 = ? WHERE pk = ? AND ck = ?", "memtable-" + ck, 1L, ck);
        execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 30L, 33L);
    }

    /** The read shapes run against each layout. */
    private List<LongFunction<SinglePartitionReadCommand>> shapes(ColumnFamilyStore cfs)
    {
        return shapes(cfs, 1L);
    }

    private List<LongFunction<SinglePartitionReadCommand>> shapes(ColumnFamilyStore cfs, long pk)
    {
        List<LongFunction<SinglePartitionReadCommand>> shapes = new ArrayList<>();
        shapes.add(now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).build());
        shapes.add(now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).fromIncl(10L).toExcl(45L).build());
        shapes.add(now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).fromIncl(10L).toExcl(45L).reverse().build());
        shapes.add(now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now)
                                                       .includeRow(3L).includeRow(22L).includeRow(41L).includeRow(59L).build());
        shapes.add(now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).withLimit(5).build());
        shapes.add(now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).reverse().withLimit(5).build());
        shapes.add(now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).columns("s").build());
        shapes.add(now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).columns("v2").fromIncl(0L).toIncl(15L).build());
        return shapes;
    }

    private void assertEveryShapeServed(String layout, ColumnFamilyStore cfs)
    {
        assertEveryShapeServed(layout, cfs, 1L);
    }

    private void assertEveryShapeServed(String layout, ColumnFamilyStore cfs, long pk)
    {
        List<LongFunction<SinglePartitionReadCommand>> shapes = shapes(cfs, pk);
        for (int i = 0; i < shapes.size(); i++)
        {
            ReadCase c = ReadCase.of(layout + " shape " + i, layout.hashCode() * 31L + i);
            assertAllReadSurfacesMatch(c, cfs, shapes.get(i));
            assertAllReadSurfacesMatch(c.cold().named(layout + " shape " + i + " cold"), cfs, shapes.get(i));
        }
    }

    @Test
    public void allBig()
    {
        ColumnFamilyStore cfs = createTableWithoutCompaction();
        for (int layer = 0; layer < 3; layer++)
            flushLayer("big", layer, 1000 + layer * 100);
        assertEveryShapeServed("all BIG", cfs);
    }

    @Test
    public void allBti()
    {
        ColumnFamilyStore cfs = createTableWithoutCompaction();
        for (int layer = 0; layer < 3; layer++)
            flushLayer("bti", layer, 1000 + layer * 100);
        assertEveryShapeServed("all BTI", cfs);
    }

    @Test
    public void bigAndBtiMixed()
    {
        ColumnFamilyStore cfs = createTableWithoutCompaction();
        flushLayer("big", 0, 1000);
        flushLayer("bti", 1, 1100);
        flushLayer("big", 2, 1200);
        flushLayer("bti", 3, 1300);
        assertEquals(2, cfs.getLiveSSTables().stream().filter(s -> s instanceof BtiTableReader).count());
        assertEveryShapeServed("BIG and BTI mixed", cfs);
    }

    @Test
    public void bigAndBtiMixedWithMemtableOverlay()
    {
        ColumnFamilyStore cfs = createTableWithoutCompaction();
        flushLayer("bti", 0, 1000);
        flushLayer("big", 1, 1100);
        memtableOverlay(5000);
        assertEveryShapeServed("BIG and BTI mixed with memtable", cfs);
    }

    /** Sstables whose key range covers the partition but which do not hold it: without-partition legs. */
    @Test
    public void sstablesWithoutThePartition()
    {
        ColumnFamilyStore cfs = createTableWithoutCompaction();
        flushLayer("bti", 0, 1000);
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            for (long pk = 2; pk < 200; pk++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", pk, 1L, pk);
            flush();
        }
        flushLayer("big", 1, 1100);
        assertEveryShapeServed("sstables without the partition", cfs);
    }

    /**
     * Keys no sstable holds, inside the key range of 0, 1 and several sstables, BIG, BTI and mixed,
     * with and without memtable data for other keys.
     */
    @Test
    public void absentKeys()
    {
        for (String profile : new String[]{ "big", "bti", "mixed" })
        {
            for (boolean memtable : new boolean[]{ false, true })
            {
                ColumnFamilyStore cfs = createTableWithoutCompaction();
                AbsentKeyLayout layout = new AbsentKeyLayout(cfs.metadata().partitioner, 2000);
                long[][] layers = { layout.layer(200, 600, 20), layout.layer(400, 1200, 40), layout.layer(1000, 1100, 10) };
                List<Long> written = new ArrayList<>();
                for (int layer = 0; layer < layers.length; layer++)
                {
                    String format = profile.equals("mixed") ? (layer % 2 == 0 ? "big" : "bti") : profile;
                    DatabaseDescriptor.setSelectedSSTableFormat(format);
                    for (long pk : layers[layer])
                    {
                        written.add(pk);
                        for (long ck = 0; ck < 5; ck++)
                            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP " + (1000 + layer), pk, ck, ck);
                    }
                    flush();
                }
                if (memtable)
                {
                    for (int rank : new int[]{ 150, 700, 1600 })
                    {
                        written.add(layout.at(rank));
                        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, 1, 1) USING TIMESTAMP 6000", layout.at(rank));
                    }
                }
                Map<String, Long> keys = layout.absentKeys(cfs, written.stream().mapToLong(Long::longValue).toArray());
                for (int covering = 0; covering <= 2; covering++)
                    assertTrue("no absent key in " + covering + " sstable range(s): " + keys,
                               keys.containsKey("absent key in " + covering + " sstable range(s)"));
                for (Map.Entry<String, Long> key : keys.entrySet())
                    assertEveryShapeServed(profile + (memtable ? " memtable " : " ") + key.getKey(), cfs, key.getValue());
            }
        }
    }

    /** A newer partition deletion stops the read before older sstables; both paths must stop at the same leg. */
    @Test
    public void partitionDeletionSkipsOlderSSTables()
    {
        ColumnFamilyStore cfs = createTableWithoutCompaction();
        flushLayer("big", 0, 1000);
        flushLayer("bti", 1, 1100);
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        execute("DELETE FROM %s USING TIMESTAMP 1500 WHERE pk = ?", 1L);
        flush();
        flushLayer("big", 2, 2000);
        assertEveryShapeServed("partition deletion", cfs);
    }

    /** A dropped simple column is still listed in old sstable headers; the cursor path serves those legs. */
    @Test
    public void droppedSimpleColumn()
    {
        ColumnFamilyStore cfs = createTableWithoutCompaction();
        flushLayer("big", 0, 1000);
        flushLayer("bti", 1, 1100);
        execute("ALTER TABLE %s DROP v1");
        flushLayer2WithoutV1("bti", 2, 3000);
        assertEveryShapeServed("dropped simple column", cfs);
    }

    /** A collection dropped then re-added is served by the cursor path. */
    @Test
    public void droppedAndReaddedCollection()
    {
        ColumnFamilyStore cfs = createTableWithoutCompaction();
        flushLayer("big", 0, 1000);
        flushLayer("bti", 1, 1100);
        execute("ALTER TABLE %s DROP l");
        execute("ALTER TABLE %s ADD l list<int>");
        flushLayer("bti", 2, 5000);
        assertEveryShapeServed("dropped and re-added collection", cfs);
    }

    /** A collection dropped and not re-added: the one read the support gate rejects on purpose. */
    @Test
    public void droppedCollectionNotReadded()
    {
        ColumnFamilyStore cfs = createTableWithoutCompaction();
        flushLayer("big", 0, 1000);
        flushLayer("bti", 1, 1100);
        execute("ALTER TABLE %s DROP l");
        List<LongFunction<SinglePartitionReadCommand>> shapes = shapes(cfs);
        for (int i = 0; i < shapes.size(); i++)
        {
            ReadCase c = ReadCase.of("dropped collection shape " + i, i)
                                 .rejectedBecause(UnsupportedReason.DROPPED_COLLECTION_OR_COUNTER_IN_HEADER);
            assertAllReadSurfacesMatch(c, cfs, shapes.get(i));
        }
    }

    private void flushLayer2WithoutV1(String format, int layer, long ts)
    {
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        for (long ck = layer; ck < 60; ck += 3)
            execute("INSERT INTO %s (pk, ck, v2) VALUES (?, ?, ?) USING TIMESTAMP " + ts, 1L, ck, "after-drop-" + ck);
        flush();
    }
}
