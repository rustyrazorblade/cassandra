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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DeletionTime;
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
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/**
 * A row read from an sstable can hold a collection column with no cells, on the iterator path.
 * When a row has a complex deletion on any column, every complex column of the row stores a
 * deletion, and {@code UnfilteredSerializer.readComplexColumn} adds it to the row even when it is
 * live.  If the read then skips every cell of that column (a cell the column filter does not
 * fetch, or an unqueried cell older than the row), {@code BTreeRow.Builder} still builds the
 * column, with no cells and a live deletion.  The column stays in the row only when one source
 * holds the row and nothing merges it: {@code Row.Merger.merge} returns a single version as is
 * when no partition or range deletion covers it, and a real merge drops the column
 * ({@code ComplexColumnData.Builder.build}).
 *
 * <p>Each scenario reads through every surface of {@link CursorReadOracle} (records, intra-node
 * bytes, replica responses, digests, repaired-data tracking), on BIG and BTI, forward and reverse,
 * with slices and names, and checks first that the iterator path gives the shape the scenario
 * is for, so no scenario passes without covering it.
 */
public class EmptyComplexColumnDifferentialTest extends CursorReadOracle
{
    private static final String TABLE = "CREATE TABLE %s (pk int, ck int, s0 int static, sm map<int, int> static, sl list<int> static, " +
                                        "v0 int, m map<int, int>, l list<int>, st set<int>, PRIMARY KEY (pk, ck))";
    /** Where the data of the scenario sits. */
    private enum Sources
    {
        ONE_SSTABLE,
        SSTABLE_AND_MEMTABLE_WITHOUT_THE_ROW,
        SSTABLE_AND_MEMTABLE_WITH_THE_ROW,
        TWO_SSTABLES_ROW_IN_ONE,
        TWO_SSTABLES_ROW_IN_BOTH
    }

    /** What covers the row with the column. */
    private enum Cover
    {
        NONE,
        PARTITION_DELETION,
        RANGE_DELETION
    }

    private static final String EMPTY_LIVE = " del=" + DeletionTime.LIVE.markedForDeleteAt() + '/' + DeletionTime.LIVE.localDeletionTime() + " n=0";

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
    public void oneSSTable()
    {
        runScenarios(Sources.ONE_SSTABLE);
    }

    @Test
    public void sstableAndMemtableWithoutTheRow()
    {
        runScenarios(Sources.SSTABLE_AND_MEMTABLE_WITHOUT_THE_ROW);
    }

    @Test
    public void sstableAndMemtableWithTheRow()
    {
        runScenarios(Sources.SSTABLE_AND_MEMTABLE_WITH_THE_ROW);
    }

    @Test
    public void twoSSTablesRowInOne()
    {
        runScenarios(Sources.TWO_SSTABLES_ROW_IN_ONE);
    }

    @Test
    public void twoSSTablesRowInBoth()
    {
        runScenarios(Sources.TWO_SSTABLES_ROW_IN_BOTH);
    }

    /**
     * A row whose newer version stores a complex deletion (a list overwrite) and a map cell the read
     * does not fetch, so the map column comes back with no cells and a live deletion, while another
     * source holds the same row with only a column the read does not fetch and no row timestamp.
     * The iterator path's sstable or memtable iterator reads that other version as an empty row and
     * drops it, so the newer version is the only one Row.Merger sees, and the row keeps the map
     * column as stored.  The other version sits in its own sstable, in two sstables, or in the
     * memtable; with a row timestamp on it (the last case) it is a real version and the row merges.
     */
    @Test
    public void otherSourceHoldsOnlyUnfetchedColumns()
    {
        long seed = 90_000L;
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean clustering : new boolean[]{ false, true })
            {
                for (int other = 0; other < 4; other++)
                {
                    DatabaseDescriptor.setSelectedSSTableFormat(format);
                    createTable("CREATE TABLE %s (k int, " + (clustering ? "c1 int, " : "") + "v boolean, w int, e1 map<int, int>, e2 list<int>, " +
                                "PRIMARY KEY ((k)" + (clustering ? ", c1" : "") + "))");
                    ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                    cfs.disableAutoCompaction();
                    String where = "k = 1" + (clustering ? " AND c1 = 0" : "");
                    if (other == 3)
                        execute("INSERT INTO %s (k" + (clustering ? ", c1" : "") + ", w) VALUES (1" + (clustering ? ", 0" : "") + ", 1) USING TIMESTAMP 50");
                    else
                        execute("UPDATE %s USING TIMESTAMP 50 SET w = 1 WHERE " + where);
                    flush();
                    if (other == 1)
                    {
                        execute("UPDATE %s USING TIMESTAMP 60 SET w = 2 WHERE " + where);
                        flush();
                    }
                    execute("UPDATE %s USING TIMESTAMP 100 SET e1 = e1 + {3: 4} WHERE " + where);
                    execute("UPDATE %s USING TIMESTAMP 101 SET e2 = [1] WHERE " + where);
                    flush();
                    if (other == 2)
                        execute("UPDATE %s USING TIMESTAMP 70 SET w = 3 WHERE " + where);
                    TableMetadata metadata = cfs.metadata();
                    ColumnMetadata v = metadata.getColumn(ByteBufferUtil.bytes("v"));
                    ColumnMetadata w = metadata.getColumn(ByteBufferUtil.bytes("w"));
                    ColumnMetadata e1 = metadata.getColumn(ByteBufferUtil.bytes("e1"));
                    CellPath absent = CellPath.create(Int32Type.instance.decompose(12));
                    ColumnFilter[] filters = {
                        ColumnFilter.selectionBuilder().add(v).select(e1, absent).build(),
                        ColumnFilter.allRegularColumnsBuilder(metadata, false).add(v).select(e1, absent).build(),
                        ColumnFilter.allRegularColumnsBuilder(metadata, false).add(w).build()
                    };
                    for (boolean reversed : new boolean[]{ false, true })
                    {
                        for (int f = 0; f < filters.length; f++)
                        {
                            ColumnFilter columns = filters[f];
                            String label = format + " clustering=" + clustering + " other=" + other + " filter=" + f + (reversed ? " reversed" : "");
                            assertAllReadSurfacesMatch(ReadCase.of(label, ++seed), cfs,
                                                       now -> SinglePartitionReadCommand.create(metadata, now, columns, RowFilter.none(), DataLimits.NONE,
                                                                                                metadata.partitioner.decorateKey(Int32Type.instance.decompose(1)),
                                                                                                new ClusteringIndexSliceFilter(Slices.ALL, reversed)));
                        }
                    }
                }
            }
        }
    }

    /**
     * A source holds the row with only a collection deletion older than the column's drop, and
     * the column was added back.  The iterator path's sstable or memtable iterator reads that
     * version as an empty row and drops it ({@code UnfilteredSerializer.readComplexColumn} skips
     * the dropped deletion), so it is no version in Row.Merger.  The other version holds a live
     * cell.  The dropped deletion is in an sstable with the other version in a second sstable or
     * in the memtable, or in the memtable with the other version in an sstable.
     */
    @Test
    public void otherSourceHoldsOnlyADroppedCollectionDeletion()
    {
        long seed = 95_000L;
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean clustering : new boolean[]{ false, true })
            {
                for (int layout = 0; layout < 3; layout++)
                {
                    DatabaseDescriptor.setSelectedSSTableFormat(format);
                    createTable("CREATE TABLE %s (k int, " + (clustering ? "c1 int, " : "") + "w int, e1 map<int, int>, e2 list<int>, " +
                                "PRIMARY KEY ((k)" + (clustering ? ", c1" : "") + "))");
                    ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                    cfs.disableAutoCompaction();
                    String where = "k = 1" + (clustering ? " AND c1 = 0" : "");
                    String deletion = "DELETE e1 FROM %s USING TIMESTAMP 40 WHERE " + where;
                    String other = "UPDATE %s USING TIMESTAMP 100 SET w = 1 WHERE " + where;
                    if (layout == 2)
                    {
                        execute(other);
                        flush();
                    }
                    else
                    {
                        execute(deletion);
                        flush();
                    }
                    alterTable("ALTER TABLE %s DROP e1 USING TIMESTAMP 45");
                    alterTable("ALTER TABLE %s ADD e1 map<int, int>");
                    if (layout == 2)
                    {
                        execute(deletion);
                    }
                    else
                    {
                        execute(other);
                        if (layout == 0)
                            flush();
                    }
                    TableMetadata metadata = cfs.metadata();
                    ColumnFilter columns = ColumnFilter.all(metadata);
                    for (boolean reversed : new boolean[]{ false, true })
                    {
                        for (boolean names : new boolean[]{ false, true })
                        {
                            ClusteringIndexFilter clusteringFilter;
                            if (names)
                            {
                                TreeSet<Clustering<?>> rows = new TreeSet<>(metadata.comparator);
                                rows.add(clustering ? Clustering.make(Int32Type.instance.decompose(0)) : Clustering.EMPTY);
                                clusteringFilter = new ClusteringIndexNamesFilter(rows, reversed);
                            }
                            else
                            {
                                clusteringFilter = new ClusteringIndexSliceFilter(Slices.ALL, reversed);
                            }
                            String label = format + " clustering=" + clustering + " layout=" + layout + (names ? " names" : "") + (reversed ? " reversed" : "");
                            assertAllReadSurfacesMatch(ReadCase.of(label, ++seed), cfs,
                                                       now -> SinglePartitionReadCommand.create(metadata, now, columns, RowFilter.none(), DataLimits.NONE,
                                                                                                metadata.partitioner.decorateKey(Int32Type.instance.decompose(1)),
                                                                                                clusteringFilter));
                        }
                    }
                }
            }
        }
    }

    private void runScenarios(Sources sources)
    {
        long seed = sources.ordinal() * 10_000L;
        for (String format : new String[]{ "big", "bti" })
        {
            for (Cover cover : Cover.values())
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable(TABLE);
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                write(sources, cover);
                String label = format + ' ' + sources + ' ' + cover;

                // the row survives as stored only when one source holds it and nothing merges it
                boolean asStored = sources == Sources.ONE_SSTABLE
                                   || ((sources == Sources.SSTABLE_AND_MEMTABLE_WITHOUT_THE_ROW || sources == Sources.TWO_SSTABLES_ROW_IN_ONE)
                                       && cover == Cover.NONE);
                checkIteratorShape(label, cfs, asStored, sources == Sources.ONE_SSTABLE);

                for (int filter = 0; filter < 3; filter++)
                {
                    for (int clustering = 0; clustering < 3; clustering++)
                    {
                        for (boolean reversed : new boolean[]{ false, true })
                        {
                            int f = filter, c = clustering;
                            String name = label + " filter=" + f + " clustering=" + c + (reversed ? " reversed" : "");
                            assertAllReadSurfacesMatch(ReadCase.of(name, ++seed), cfs, now -> command(cfs, now, f, c, reversed));
                        }
                    }
                }
                assertCqlMatches(ReadCase.of(label + " cql", ++seed), cfs, "SELECT v0, m[7], sm[7] FROM %s WHERE pk = 1", 2);
            }
        }
    }

    /**
     * Row 1: a map cell and a set cell at 100, then an insert at 200 that overwrites the list,
     * which stores a complex deletion on the row.  A read that filters its rows again (names, or a
     * reversed BIG read) drops the unqueried set cell after reading it, which leaves no column.  Row 4: the same without a row timestamp.  The static row holds
     * a map cell at 100 and a list overwrite at 200.  Row 2 and row 3 are plain rows.
     */
    private void write(Sources sources, Cover cover)
    {
        if (cover == Cover.PARTITION_DELETION)
            execute("DELETE FROM %s USING TIMESTAMP 50 WHERE pk = 1");
        else if (cover == Cover.RANGE_DELETION)
            execute("DELETE FROM %s USING TIMESTAMP 50 WHERE pk = 1 AND ck >= 0 AND ck <= 4");
        execute("UPDATE %s USING TIMESTAMP 100 SET m = m + {1: 10}, st = st + {1} WHERE pk = 1 AND ck = 1");
        execute("INSERT INTO %s (pk, ck, v0, l) VALUES (1, 1, 5, [7]) USING TIMESTAMP 200");
        execute("UPDATE %s USING TIMESTAMP 100 SET m = m + {1: 10} WHERE pk = 1 AND ck = 4");
        execute("UPDATE %s USING TIMESTAMP 200 SET l = [7] WHERE pk = 1 AND ck = 4");
        execute("UPDATE %s USING TIMESTAMP 100 SET sm = sm + {1: 10} WHERE pk = 1");
        execute("UPDATE %s USING TIMESTAMP 200 SET sl = [7], s0 = 3 WHERE pk = 1");
        execute("INSERT INTO %s (pk, ck, v0, m) VALUES (1, 2, 6, {2: 20}) USING TIMESTAMP 150");
        flush();
        switch (sources)
        {
            case ONE_SSTABLE:
                break;
            case SSTABLE_AND_MEMTABLE_WITHOUT_THE_ROW:
                execute("INSERT INTO %s (pk, ck, v0) VALUES (1, 3, 8) USING TIMESTAMP 300");
                break;
            case SSTABLE_AND_MEMTABLE_WITH_THE_ROW:
                execute("UPDATE %s USING TIMESTAMP 300 SET v0 = 9 WHERE pk = 1 AND ck = 1");
                execute("UPDATE %s USING TIMESTAMP 300 SET v0 = 9 WHERE pk = 1 AND ck = 4");
                break;
            case TWO_SSTABLES_ROW_IN_ONE:
                execute("INSERT INTO %s (pk, ck, v0) VALUES (1, 3, 8) USING TIMESTAMP 300");
                flush();
                break;
            case TWO_SSTABLES_ROW_IN_BOTH:
                execute("UPDATE %s USING TIMESTAMP 300 SET v0 = 9 WHERE pk = 1 AND ck = 1");
                execute("UPDATE %s USING TIMESTAMP 300 SET v0 = 9 WHERE pk = 1 AND ck = 4");
                flush();
                break;
            default:
                throw new AssertionError(sources);
        }
    }

    /** The iterator path must give the empty map on row 1 exactly when the row is read as stored. */
    private void checkIteratorShape(String label, ColumnFamilyStore cfs, boolean asStored, boolean staticAsStored)
    {
        long now = FBUtilities.nowInSeconds();
        DatabaseDescriptor.setCursorReadsEnabled(false);
        List<String> fetchAll = canonicalRecords(command(cfs, now, 0, 0, false));
        List<String> selection = canonicalRecords(command(cfs, now, 1, 0, false));
        assertEquals(label + ": iterator records with every column fetched " + fetchAll,
                     asStored, rowHasEmptyMap(fetchAll, 1));
        assertEquals(label + ": iterator records with only the selection fetched " + selection,
                     asStored, rowHasEmptyMap(selection, 4));
        assertEquals(label + ": iterator static row with only the selection fetched " + selection,
                     staticAsStored, selection.stream().anyMatch(r -> r.startsWith("  CPLX sm" + EMPTY_LIVE + " heap=")));
    }

    private static boolean rowHasEmptyMap(List<String> records, int ck)
    {
        String row = "ROW clustering=CLUSTERING:" + ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(ck));
        boolean inRow = false;
        for (String record : records)
        {
            if (!record.startsWith("  "))
                inRow = record.startsWith(row + ' ');
            else if (inRow && record.startsWith("  CPLX m" + EMPTY_LIVE + " heap="))
                return true;
        }
        return false;
    }

    /**
     * @param filter     0: every regular column fetched, the map's element 7 queried; 1: only
     *                   v0, s0 and element 7 of both maps fetched; 2: every regular column
     *                   fetched, elements 5 to 9 of the map queried
     * @param clustering 0: the whole partition; 1: the slice of row 1; 2: the names of rows 1, 2 and 4
     */
    private static SinglePartitionReadCommand command(ColumnFamilyStore cfs, long now, int filter, int clustering, boolean reversed)
    {
        return command(cfs, now, filter, clustering, reversed, 0, DataLimits.NONE);
    }

    /**
     * @param rowFilter 0: none; 1: v0 = 5, which only row 1 passes and row 4 fails at its end for
     *                  having no v0; 2: ck = 2, which rows 1 and 4 fail on their clustering;
     *                  3: v0 = 999, which no row passes; 4: v0 = 5 in a filter that needs
     *                  reconciliation, which the cursor path does not push down
     */
    private static SinglePartitionReadCommand command(ColumnFamilyStore cfs, long now, int filter, int clustering, boolean reversed,
                                                      int rowFilter, DataLimits limits)
    {
        TableMetadata metadata = cfs.metadata();
        ColumnMetadata v0 = metadata.getColumn(ByteBufferUtil.bytes("v0"));
        ColumnMetadata s0 = metadata.getColumn(ByteBufferUtil.bytes("s0"));
        ColumnMetadata m = metadata.getColumn(ByteBufferUtil.bytes("m"));
        ColumnMetadata sm = metadata.getColumn(ByteBufferUtil.bytes("sm"));
        ColumnFilter columns;
        switch (filter)
        {
            case 0:
                columns = ColumnFilter.allRegularColumnsBuilder(metadata, false)
                                      .add(v0).add(s0).select(m, path(7)).select(sm, path(7)).build();
                break;
            case 1:
                columns = ColumnFilter.selectionBuilder()
                                      .add(v0).add(s0).select(m, path(7)).select(sm, path(7)).build();
                break;
            case 2:
                columns = ColumnFilter.allRegularColumnsBuilder(metadata, false)
                                      .add(v0).slice(m, path(5), path(9)).build();
                break;
            default:
                throw new AssertionError(filter);
        }
        ClusteringIndexFilter clusteringFilter;
        switch (clustering)
        {
            case 0:
                clusteringFilter = new ClusteringIndexSliceFilter(Slices.ALL, reversed);
                break;
            case 1:
                clusteringFilter = new ClusteringIndexSliceFilter(Slices.with(metadata.comparator, Slice.make(metadata.comparator, 1)), reversed);
                break;
            case 2:
                TreeSet<Clustering<?>> names = new TreeSet<>(metadata.comparator);
                for (int ck : new int[]{ 1, 2, 4 })
                    names.add(Clustering.make(Int32Type.instance.decompose(ck)));
                clusteringFilter = new ClusteringIndexNamesFilter(names, reversed);
                break;
            default:
                throw new AssertionError(clustering);
        }
        RowFilter rows = RowFilter.none();
        if (rowFilter > 0)
        {
            rows = RowFilter.create(rowFilter == 4);
            if (rowFilter == 2)
                rows.add(metadata.getColumn(ByteBufferUtil.bytes("ck")), Operator.EQ, Int32Type.instance.decompose(2));
            else
                rows.add(v0, Operator.EQ, Int32Type.instance.decompose(rowFilter == 3 ? 999 : 5));
        }
        return SinglePartitionReadCommand.create(metadata, now, columns, rows, limits,
                                                 metadata.partitioner.decorateKey(Int32Type.instance.decompose(1)), clusteringFilter);
    }

    /**
     * A row that holds only the map with no cells and has no deletion has live data on the
     * iterator path ({@code BTreeRow.hasLiveData} holds for a row with no deletion and any
     * column), so the limit counts it, and the scan metrics count it as a live row even when the
     * row filter then drops it.
     */
    @Test
    public void rowFiltersAndLimits()
    {
        long seed = 50_000;
        for (Sources sources : new Sources[]{ Sources.ONE_SSTABLE, Sources.TWO_SSTABLES_ROW_IN_ONE, Sources.SSTABLE_AND_MEMTABLE_WITHOUT_THE_ROW })
        {
            for (String format : new String[]{ "big", "bti" })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable(TABLE);
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                write(sources, Cover.NONE);
                for (int filter : new int[]{ 0, 1 })
                {
                    for (int clustering : new int[]{ 0, 2 })
                    {
                        for (boolean reversed : new boolean[]{ false, true })
                        {
                            for (int rowFilter = 0; rowFilter < 5; rowFilter++)
                            {
                                for (int limit : new int[]{ 0, 1, 2 })
                                {
                                    int f = filter, c = clustering, r = rowFilter;
                                    DataLimits limits = limit == 0 ? DataLimits.NONE : DataLimits.cqlLimits(limit);
                                    String name = format + ' ' + sources + " filter=" + f + " clustering=" + c + (reversed ? " reversed" : "")
                                                  + " rowFilter=" + r + " limit=" + limit;
                                    assertAllReadSurfacesMatch(ReadCase.of(name, ++seed), cfs, now -> command(cfs, now, f, c, reversed, r, limits));
                                }
                            }
                        }
                    }
                }
                for (int pageSize : new int[]{ 1, 2 })
                    assertPagedMatches(ReadCase.of(format + ' ' + sources + " paged", ++seed), cfs, now -> command(cfs, now, 1, 0, false), pageSize);
            }
        }
    }

    private List<List<ByteBuffer>> cqlRows(String query, long nowInSec, boolean cursorReads)
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursorReads);
        try
        {
            CQLStatement statement = QueryProcessor.getStatement(formatQuery(query), ClientState.forInternalCalls());
            QueryOptions options = QueryOptions.create(ConsistencyLevel.ONE, Collections.emptyList(), false, -1, null,
                                                       ConsistencyLevel.SERIAL, ProtocolVersion.V5, KEYSPACE, Long.MIN_VALUE, nowInSec);
            ResultMessage.Rows rows = (ResultMessage.Rows) statement.execute(QueryState.forInternalCalls(), options,
                                                                             Dispatcher.RequestTime.forImmediateExecution());
            return rows.result.rows;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static CellPath path(int key)
    {
        return CellPath.create(Int32Type.instance.decompose(key));
    }

    /**
     * A memtable row that holds only a map deletion past gc grace, which the row filter drops.
     * The purge removes the deletion, which leaves no column, so the scan metrics do not count
     * the row as live.  Only a column with no cells and a live deletion stays.
     */
    @Test
    public void filteredMemtableRowWithAPurgedCollectionDeletion()
    {
        long seed = 70_000;
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable(TABLE + " WITH gc_grace_seconds = 0");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            write(Sources.ONE_SSTABLE, Cover.NONE);
            execute("DELETE m FROM %s USING TIMESTAMP 300 WHERE pk = 1 AND ck = 5");
            long later = FBUtilities.nowInSeconds() + 100_000;
            for (int filter : new int[]{ 0, 1 })
            {
                for (boolean reversed : new boolean[]{ false, true })
                {
                    for (int rowFilter : new int[]{ 1, 3, 4 })
                    {
                        int f = filter, r = rowFilter;
                        String name = format + " memtable map deletion filter=" + f + (reversed ? " reversed" : "") + " rowFilter=" + r;
                        assertAllReadSurfacesMatch(ReadCase.of(name, ++seed).at(later), cfs,
                                                   now -> command(cfs, now, f, 0, reversed, r, DataLimits.NONE));
                    }
                }
            }
        }
    }

    /**
     * A row whose timestamp and TTL have expired past gc grace, with the list deletion also past
     * gc grace, before compaction.  The purge on the read removes the row timestamp, the cells and
     * the list deletion, and on the iterator path the row then holds only the map with no cells.
     * The row is not empty, so the purge keeps it, and the response and the digest carry it.  It
     * has no live data, so CQL does not return it.
     */
    @Test
    public void expiredRowPastGcGraceKeepsTheEmptyColumn()
    {
        long seed = 90_000;
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean secondSSTable : new boolean[]{ false, true })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable(TABLE + " WITH gc_grace_seconds = 0");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                execute("UPDATE %s USING TIMESTAMP 100 SET m = m + {1: 10} WHERE pk = 1 AND ck = 1");
                execute("INSERT INTO %s (pk, ck, v0, l) VALUES (1, 1, 5, [7]) USING TIMESTAMP 200 AND TTL 1");
                flush();
                if (secondSSTable)
                {
                    execute("INSERT INTO %s (pk, ck, v0) VALUES (1, 3, 8) USING TIMESTAMP 300");
                    flush();
                }
                String label = format + " expired row" + (secondSSTable ? " and another sstable" : "");
                long later = FBUtilities.nowInSeconds() + 100_000;

                // the iterator path keeps the row with the empty map and nothing else
                DatabaseDescriptor.setCursorReadsEnabled(false);
                List<String> records = canonicalRecords(command(cfs, later, 0, 0, false));
                assertEquals(label + ": " + records, true, rowHasEmptyMap(records, 1));
                String row = "ROW clustering=CLUSTERING:" + ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(1))
                             + " liveness=" + Long.MIN_VALUE + ",0," + Long.MAX_VALUE
                             + " deletion=" + DeletionTime.LIVE.markedForDeleteAt() + '/' + DeletionTime.LIVE.localDeletionTime();
                assertEquals(label + ": " + records, true, records.stream().anyMatch(r -> r.startsWith(row + " heap=")));
                // the row has no live cell, so on 5.0 it has no live data and CQL drops it
                String query = "SELECT v0, m[7] FROM %s WHERE pk = 1 AND ck = 1";
                assertEquals(label, 0, cqlRows(query, later, false).size());
                assertEquals(label, 0, cqlRows(query, later, true).size());
                assertCqlMatches(ReadCase.of(label + " cql", ++seed).at(later), cfs, query, 0);
                assertCqlMatches(ReadCase.of(label + " cql paged", ++seed).at(later), cfs, "SELECT v0, m[7] FROM %s WHERE pk = 1", 1);

                for (int filter = 0; filter < 3; filter++)
                {
                    for (int clustering = 0; clustering < 3; clustering++)
                    {
                        for (boolean reversed : new boolean[]{ false, true })
                        {
                            int f = filter, c = clustering;
                            String name = label + " filter=" + f + " clustering=" + c + (reversed ? " reversed" : "");
                            LongFunction<SinglePartitionReadCommand> read = now -> command(cfs, now, f, c, reversed);
                            assertAllReadSurfacesMatch(ReadCase.of(name, ++seed).at(later), cfs, read);
                        }
                    }
                }
            }
        }
    }
}
