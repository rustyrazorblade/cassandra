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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.junit.Assert.assertEquals;

/**
 * The read keeps, skips the value of, or drops each cell by its column: whether the column filter
 * fetches the column, whether it queries it, a drop record of the column, and a read that filters
 * its rows again.  The cursor cell walk answers these once per column of an sstable's column set;
 * this compares every read surface with the iterator path, which asks for each cell.
 *
 * <p>The sstables have different column sets: a column is dropped with cells newer than its drop
 * time, a column is added between two flushes, and one is dropped and added again with cells
 * written before the drop, after the drop and after the column came back.  Each read is on one sstable (read as stored), on all of them (a merge), and
 * on all of them with a memtable, with static and regular columns, simple and collection columns,
 * and sub-selections of a collection.  Forward, reverse, slice and names reads, BIG and BTI.
 */
public class FetchedColumnsDifferentialTest extends CursorReadOracle
{
    private static final int ROWS = 12;

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

    @Test
    public void oneSSTable() throws Throwable
    {
        run(0);
    }

    @Test
    public void threeSSTables() throws Throwable
    {
        run(1);
    }

    @Test
    public void threeSSTablesAndMemtable() throws Throwable
    {
        run(2);
    }

    private void run(int layout) throws Throwable
    {
        long seed = 40_000L * (layout + 1);
        int cases = 0;
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk int, ck int, s1 int static, s2 set<int> static, a int, b text, c bigint, " +
                        "m map<int, int>, st set<int>, f frozen<list<int>>, e int, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            writeData(layout);
            if (layout == 0)
                assertEquals(1, cfs.getLiveSSTables().size());

            TableMetadata metadata = cfs.metadata();
            List<ColumnFilter> filters = columnFilters(metadata);
            for (int f = 0; f < filters.size(); f++)
            {
                ColumnFilter columns = filters.get(f);
                for (int shape = 0; shape < 5; shape++)
                {
                    int readShape = shape;
                    String label = format + " layout=" + layout + " filter=" + f + " (" + columns + ") shape=" + shape;
                    assertAllReadSurfacesMatch(ReadCase.of(label, ++seed), cfs,
                                               now -> SinglePartitionReadCommand.create(metadata, now, columns, RowFilter.none(), DataLimits.NONE,
                                                                                        key(metadata), clusteringFilter(metadata, readShape)));
                    cases++;
                }
            }
        }
        assertEveryCaseServedLegs();
        assertEquals(2 * 9 * 5, cases);
    }

    /**
     * Rows written by INSERT, cells written by UPDATE before and after the row, column e dropped
     * with cells newer than its drop time, a column added between two flushes, and column b
     * dropped and added back with cells on both sides of the drop.  {@code layout} 0 keeps one sstable; 1 flushes three; 2 adds a memtable.
     */
    private void writeData(int layout) throws Throwable
    {
        for (int ck = 0; ck < ROWS; ck++)
        {
            execute("INSERT INTO %s (pk, ck, a, b, c, m, st, f) VALUES (1, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP 100",
                    ck, ck, "b" + ck, (long) ck, map(ck, ck + 1, ck + 1, ck + 2), set(ck, ck + 3), list(ck));
            // a cell older than the row liveness, and one newer than it
            execute("UPDATE %s USING TIMESTAMP 90 SET c = ? WHERE pk = 1 AND ck = ?", (long) ck * 10, ck);
            if (ck % 3 == 0)
                execute("UPDATE %s USING TIMESTAMP 110 SET m[?] = ? WHERE pk = 1 AND ck = ?", 7, ck, ck);
        }
        execute("UPDATE %s USING TIMESTAMP 100 SET s1 = 5, s2 = {1, 2} WHERE pk = 1");
        // e is dropped and not added back, with cells written after its drop time: a forward
        // read keeps them, a read that filters its rows again does not
        for (int ck = 0; ck < ROWS; ck += 2)
            execute("UPDATE %s USING TIMESTAMP 500 SET e = ? WHERE pk = 1 AND ck = ?", ck, ck);
        execute("UPDATE %s USING TIMESTAMP 50 SET e = 1 WHERE pk = 1 AND ck = 1");
        flush();
        alterTable("ALTER TABLE %s DROP e USING TIMESTAMP 300");
        if (layout == 0)
            return;

        // a second column set: d added, then rows that hold it
        alterTable("ALTER TABLE %s ADD d int");
        for (int ck = 0; ck < ROWS; ck += 2)
            execute("UPDATE %s USING TIMESTAMP 120 SET d = ?, a = ? WHERE pk = 1 AND ck = ?", ck, ck + 100, ck);
        flush();

        // b dropped and added back: cells from before the drop are gone, cells written after it stay
        execute("UPDATE %s USING TIMESTAMP 130 SET b = 'before drop' WHERE pk = 1 AND ck = 1");
        flush();
        alterTable("ALTER TABLE %s DROP b USING TIMESTAMP 140");
        alterTable("ALTER TABLE %s ADD b text");
        for (int ck = 0; ck < ROWS; ck += 3)
            execute("UPDATE %s USING TIMESTAMP 150 SET b = ?, st = st + {99} WHERE pk = 1 AND ck = ?", "after " + ck, ck);
        if (layout == 2)
        {
            execute("UPDATE %s USING TIMESTAMP 160 SET c = 1, m[1] = 2 WHERE pk = 1 AND ck = 5");
            execute("INSERT INTO %s (pk, ck, a) VALUES (1, 6, 66) USING TIMESTAMP 160");
            return;
        }
        flush();
    }

    private static List<ColumnFilter> columnFilters(TableMetadata metadata)
    {
        ColumnMetadata a = column(metadata, "a"), b = column(metadata, "b"), c = column(metadata, "c");
        ColumnMetadata m = column(metadata, "m"), st = column(metadata, "st"), s1 = column(metadata, "s1");
        ColumnMetadata d = metadata.getColumn(ByteBufferUtil.bytes("d"));
        CellPath key1 = CellPath.create(Int32Type.instance.decompose(1));
        CellPath key7 = CellPath.create(Int32Type.instance.decompose(7));
        List<ColumnFilter> filters = new ArrayList<>();
        filters.add(ColumnFilter.all(metadata));
        // every column fetched and listed, as a bulk reader that names its projection
        filters.add(ColumnFilter.allRegularColumnsBuilder(metadata, false).addAll(metadata.regularAndStaticColumns()).build());
        // fetched but not queried: the other columns' cells keep empty values or are dropped
        filters.add(ColumnFilter.allRegularColumnsBuilder(metadata, false).add(a).build());
        filters.add(ColumnFilter.allRegularColumnsBuilder(metadata, true).add(b).add(m).add(s1).build());
        filters.add(ColumnFilter.allRegularColumnsBuilder(metadata, false).add(d == null ? c : d).add(st).build());
        // sub-selections of a collection
        filters.add(ColumnFilter.allRegularColumnsBuilder(metadata, false).add(a).select(m, key7).build());
        filters.add(ColumnFilter.allRegularColumnsBuilder(metadata, false).slice(m, key1, key7).add(c).build());
        // only the queried columns fetched
        filters.add(ColumnFilter.selectionBuilder().add(a).add(m).add(b).build());
        filters.add(ColumnFilter.selectionBuilder().add(s1).add(st).select(m, key1).build());
        return filters;
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(ByteBufferUtil.bytes(name));
    }

    private static DecoratedKey key(TableMetadata metadata)
    {
        return metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
    }

    /** 0 forward, 1 reverse, 2 a slice, 3 a reverse slice, 4 names. */
    private static ClusteringIndexFilter clusteringFilter(TableMetadata metadata, int shape)
    {
        switch (shape)
        {
            case 0:
            case 1:
                return new ClusteringIndexSliceFilter(Slices.ALL, shape == 1);
            case 2:
            case 3:
            {
                Slices.Builder slices = new Slices.Builder(metadata.comparator);
                slices.add(Slice.make(Clustering.make(Int32Type.instance.decompose(2)), Clustering.make(Int32Type.instance.decompose(8))));
                return new ClusteringIndexSliceFilter(slices.build(), shape == 3);
            }
            case 4:
            {
                BTreeSet.Builder<Clustering<?>> names = BTreeSet.builder(metadata.comparator);
                names.add(Clustering.make(Int32Type.instance.decompose(1)));
                names.add(Clustering.make(Int32Type.instance.decompose(6)));
                names.add(Clustering.make(Int32Type.instance.decompose(9)));
                return new ClusteringIndexNamesFilter(names.build(), false);
            }
            default:
                throw new IllegalArgumentException("shape " + shape);
        }
    }
}
