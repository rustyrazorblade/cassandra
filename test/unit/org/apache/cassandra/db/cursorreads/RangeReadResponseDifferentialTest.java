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
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The replica data response of a range read, which the cursor path writes straight to the response
 * bytes ({@code CursorRangeReads.responseBytes}), against the iterator range path, on BIG and BTI.
 * Each case checks the response bytes at every messaging version, the scan metrics, failures and
 * that the cursor path served the read, then that it wrote the response itself, and, where the
 * read builds no row objects, that it built none.  Its digest copy and a copy that tracks repaired
 * data are compared too.
 */
@RunWith(Parameterized.class)
public class RangeReadResponseDifferentialTest extends RangeReadOracle
{
    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> formats()
    {
        return Arrays.asList(new Object[][]{ { "big" }, { "bti" } });
    }

    private static final int COLUMN_INDEX_SIZE_KIB = 1;
    private static final long BASE_TS = 1_000_000L;

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;
    private int originalTombstoneFailureThreshold;
    private long seed;

    @Before
    public void pinFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(COLUMN_INDEX_SIZE_KIB);
        originalTombstoneFailureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
        DatabaseDescriptor.setTombstoneFailureThreshold(originalTombstoneFailureThreshold);
    }

    // ---------------------------------------------------------------- cases

    /** Reads whose responses build no row objects on the cursor path. */
    private static final String[] STREAMED = {
        "SELECT * FROM %s",
        "SELECT * FROM %s LIMIT 7",
        "SELECT * FROM %s LIMIT 1",
        "SELECT * FROM %s PER PARTITION LIMIT 2",
        "SELECT * FROM %s PER PARTITION LIMIT 1 LIMIT 5",
        "SELECT DISTINCT k FROM %s",
        "SELECT DISTINCT k, s FROM %s",
        "SELECT k, s FROM %s",
        "SELECT k, c, v FROM %s",
        "SELECT * FROM %s WHERE token(k) > 0",
        "SELECT * FROM %s WHERE token(k) >= -3000000000000000000 AND token(k) < 3000000000000000000 LIMIT 40",
        "SELECT * FROM %s WHERE c > 3 AND c < 9 ALLOW FILTERING",
        "SELECT * FROM %s WHERE c IN (2, 5, 40) ALLOW FILTERING",
        "SELECT * FROM %s WHERE v = 3 ALLOW FILTERING",
        "SELECT * FROM %s WHERE v > 2 LIMIT 9 ALLOW FILTERING",
        "SELECT * FROM %s WHERE s = 's1' ALLOW FILTERING",
        "SELECT * FROM %s WHERE c > 10 PER PARTITION LIMIT 3 ALLOW FILTERING",
    };

    /** Reads the cursor path writes but must build rows for: a filter on a collection. */
    private static final String[] BUILDS_ROWS = {
        "SELECT * FROM %s WHERE l CONTAINS 4 ALLOW FILTERING",
    };

    @Test
    public void overlappingSSTablesAndMemtable() throws Throwable
    {
        ColumnFamilyStore cfs = load(false);
        long now = FBUtilities.nowInSeconds() + 100;
        runAll(cfs, now);
    }

    @Test
    public void purgeableTombstones() throws Throwable
    {
        ColumnFamilyStore cfs = load(true);
        // past gc grace, so the deletions are purged from the response
        long now = FBUtilities.nowInSeconds() + 100;
        runAll(cfs, now);
    }

    @Test
    public void repairedSSTables() throws Throwable
    {
        ColumnFamilyStore cfs = load(false);
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        for (int i = 0; i < sstables.size(); i += 2)
        {
            SSTableReader sstable = sstables.get(i);
            sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, FBUtilities.nowInSeconds(), null, false);
            sstable.reloadSSTableMetadata();
        }
        long now = FBUtilities.nowInSeconds() + 100;
        runAll(cfs, now);
    }

    @Test
    public void reversedInternalReads() throws Throwable
    {
        ColumnFamilyStore cfs = load(false);
        long now = FBUtilities.nowInSeconds() + 100;
        TableMetadata metadata = cfs.metadata();
        DataRange reversed = new DataRange(DataRange.allData(metadata.partitioner).keyRange(), new ClusteringIndexSliceFilter(Slices.ALL, true));
        int[] limits = { DataLimits.NO_LIMIT, 1, 6, 30 };
        for (int limit : limits)
        {
            DataLimits dataLimits = limit == DataLimits.NO_LIMIT ? DataLimits.NONE : DataLimits.cqlLimits(limit);
            assertWritten(cfs, now, "reversed " + dataLimits, true,
                          at -> PartitionRangeReadCommand.create(metadata, at, ColumnFilter.all(metadata), RowFilter.none(), dataLimits, reversed));
        }
    }

    /**
     * Reversed reads of a column subset over collection cells older than their row.  BIG's reverse
     * reader filters the rows it builds again, which drops those cells.
     */
    @Test
    public void reversedReadsOfAColumnSubset() throws Throwable
    {
        ColumnFamilyStore cfs = loadForReversedReads(false);
        runReversedReads(cfs, "SELECT * FROM %s", "SELECT k, c, v FROM %s", "SELECT k, c, l FROM %s");
    }

    /**
     * Reversed reads of a table with a dropped column whose cells were written after the drop
     * time.  BIG's reverse reader drops them when it filters its rows again.  Other reads of these
     * cells fail on the iterator path (CASSANDRA-21607, dropped column cells above the drop time
     * survive the read filter), so only BIG reversed reads have a result to match.
     */
    @Test
    public void reversedReadsOfADroppedColumnWrittenAfterTheDrop() throws Throwable
    {
        Assume.assumeTrue("the iterator path fails on these cells on BTI (CASSANDRA-21607)", "big".equals(format));
        ColumnFamilyStore cfs = loadForReversedReads(true);
        runReversedReads(cfs, "SELECT * FROM %s", "SELECT k, c, v FROM %s");
    }

    private ColumnFamilyStore loadForReversedReads(boolean dropAColumn) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, w int, l list<int>, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        long future = (System.currentTimeMillis() + 3_600_000L) * 1000;
        for (int flush = 0; flush < 3; flush++)
        {
            for (int k = flush; k < 20; k += 2)
            {
                for (int c = 0; c < 40; c++)
                {
                    execute("UPDATE %s USING TIMESTAMP ? SET l = ? WHERE k = ? AND c = ?", BASE_TS + flush, List.of(c, flush), k, c);
                    execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?", k, c, c % 7, BASE_TS + 100 + flush);
                    if (dropAColumn)
                        execute("UPDATE %s USING TIMESTAMP ? SET w = ? WHERE k = ? AND c = ?", future + flush, c, k, c);
                }
            }
            flush();
        }
        if (dropAColumn)
            execute("ALTER TABLE %s DROP w");
        return cfs;
    }

    private void runReversedReads(ColumnFamilyStore cfs, String... queries)
    {
        long now = FBUtilities.nowInSeconds() + 100;
        TableMetadata metadata = cfs.metadata();
        DataRange reversed = new DataRange(DataRange.allData(metadata.partitioner).keyRange(), new ClusteringIndexSliceFilter(Slices.ALL, true));
        for (String query : queries)
        {
            for (int limit : new int[]{ DataLimits.NO_LIMIT, 25 })
            {
                DataLimits dataLimits = limit == DataLimits.NO_LIMIT ? DataLimits.NONE : DataLimits.cqlLimits(limit);
                assertWritten(cfs, now, "reversed " + query + ' ' + dataLimits, true,
                              at -> {
                                  PartitionRangeReadCommand forward = rangeCommand(query, at);
                                  return PartitionRangeReadCommand.create(metadata, at, forward.columnFilter(), RowFilter.none(), dataLimits, reversed);
                              });
            }
        }
    }

    @Test
    public void pagedThroughReplicaResponses() throws Throwable
    {
        ColumnFamilyStore cfs = load(false);
        long now = FBUtilities.nowInSeconds() + 100;
        String[] queries = { "SELECT * FROM %s", "SELECT * FROM %s PER PARTITION LIMIT 3", "SELECT DISTINCT k FROM %s",
                             "SELECT * FROM %s WHERE v > 1 ALLOW FILTERING" };
        int[] pageSizes = { 1, 3, 10, 100 };
        DataStorageSpec.LongBytesBound warn = DatabaseDescriptor.getLocalReadSizeWarnThreshold();
        DataStorageSpec.LongBytesBound fail = DatabaseDescriptor.getLocalReadSizeFailThreshold();
        // the read size thresholds measure row objects, so a read that tracks them is left to executeLocally
        DatabaseDescriptor.setLocalReadSizeWarnThreshold(null);
        DatabaseDescriptor.setLocalReadSizeFailThreshold(null);
        try
        {
            for (String query : queries)
            {
                for (int pageSize : pageSizes)
                {
                    long written = CursorRangeReads.rangeResponsesWritten();
                    assertCqlReplicaResponsesMatch(ReadCase.of("paged", seed++).at(now), cfs, query, pageSize);
                    assertTrue("the cursor path wrote the pages of " + query + " at page size " + pageSize,
                               CursorRangeReads.rangeResponsesWritten() > written);
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setLocalReadSizeWarnThreshold(warn);
            DatabaseDescriptor.setLocalReadSizeFailThreshold(fail);
        }
    }

    @Test
    public void pagedWithReadSizeThresholds() throws Throwable
    {
        ColumnFamilyStore cfs = load(false);
        long now = FBUtilities.nowInSeconds() + 100;
        // the thresholds of test/conf are on: executeLocally serves the pages, still on the cursor merge
        for (int pageSize : new int[]{ 3, 100 })
            assertCqlReplicaResponsesMatch(ReadCase.of("paged with read size thresholds", seed++).at(now), cfs, "SELECT * FROM %s", pageSize);
    }

    @Test
    public void tombstoneFailureThreshold() throws Throwable
    {
        ColumnFamilyStore cfs = load(false);
        long now = FBUtilities.nowInSeconds() + 100;
        DatabaseDescriptor.setTombstoneFailureThreshold(5);
        // both paths abort the read at the same tombstone
        assertWritten(cfs, now, "SELECT * FROM %s", false, false);
    }

    @Test
    public void rowCache() throws Throwable
    {
        CacheService.instance.setRowCacheCapacityInMB(4);
        try
        {
            createTable("CREATE TABLE %s (k int, c int, s text static, v int, l list<int>, PRIMARY KEY (k, c)) " +
                        "WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            write(30, false);
            assertTrue(cfs.isRowCacheEnabled());
            // fill the row cache for some partitions with single-partition reads
            for (int k = 0; k < 30; k += 3)
                execute("SELECT * FROM %s WHERE k = ?", k);
            assertTrue("the row cache holds partitions", CacheService.instance.rowCache.size() > 0);
            long now = FBUtilities.nowInSeconds() + 100;
            runAll(cfs, now);
        }
        finally
        {
            CacheService.instance.setRowCacheCapacityInMB(0);
        }
    }

    // ---------------------------------------------------------------- collection columns with no cells

    /**
     * A table for rows that hold a collection column with no cells on the iterator path.  When a
     * row has a complex deletion on any column, every complex column of the row stores a deletion,
     * and {@code UnfilteredSerializer.readComplexColumn} adds it to the row even when it is live.
     * If the read then skips every cell of that column, {@code BTreeRow.Builder} still builds the
     * column, with no cells.  The column stays only when one source holds the partition, or when
     * one source holds the row and no partition or range deletion covers it.
     */
    private static final String EMPTY_COLUMN_TABLE = "CREATE TABLE %s (pk int, ck int, s0 int static, sm map<int, int> static, " +
                                                     "sl list<int> static, v0 int, m map<int, int>, l list<int>, st set<int>, " +
                                                     "PRIMARY KEY (pk, ck))";
    private static final String EMPTY_LIVE = " del=" + DeletionTime.LIVE.markedForDeleteAt() + '/' + DeletionTime.LIVE.localDeletionTime() + " n=0";

    /** Where the data of partition 1 sits. */
    private enum Sources
    {
        ONE_SSTABLE,
        SSTABLE_AND_MEMTABLE_WITHOUT_THE_ROW,
        SSTABLE_AND_MEMTABLE_WITH_THE_ROW,
        TWO_SSTABLES_ROW_IN_ONE,
        TWO_SSTABLES_ROW_IN_BOTH
    }

    /** What covers the rows of partition 1. */
    private enum Cover
    {
        NONE,
        PARTITION_DELETION,
        RANGE_DELETION
    }

    /**
     * Every column filter, clustering filter and order, over every placement of partition 1 and
     * every deletion covering it.  Partition 2 holds the same rows in one sstable, and partition 3
     * plain rows in the other source, so the read moves between partitions that keep the column
     * and partitions that do not.
     */
    @Test
    public void collectionColumnsWithNoCells() throws Throwable
    {
        for (Sources sources : Sources.values())
        {
            for (Cover cover : Cover.values())
            {
                ColumnFamilyStore cfs = loadEmptyColumns(sources, cover, false);
                String label = sources + " " + cover;
                boolean asStored = sources == Sources.ONE_SSTABLE
                                   || ((sources == Sources.SSTABLE_AND_MEMTABLE_WITHOUT_THE_ROW || sources == Sources.TWO_SSTABLES_ROW_IN_ONE)
                                       && cover == Cover.NONE);
                checkIteratorShape(label, cfs, asStored);
                for (int filter = 0; filter < 3; filter++)
                {
                    for (int clustering = 0; clustering < 3; clustering++)
                    {
                        for (boolean reversed : new boolean[]{ false, true })
                        {
                            for (boolean onePartition : new boolean[]{ false, true })
                            {
                                int f = filter, c = clustering;
                                String name = label + " filter=" + f + " clustering=" + c + (reversed ? " reversed" : "")
                                              + (onePartition ? " partition 1" : "");
                                assertAllReadSurfacesMatch(ReadCase.of(name, seed++), cfs,
                                                           now -> emptyColumnCommand(cfs, now, f, c, reversed, onePartition, 0, DataLimits.NONE));
                            }
                        }
                    }
                }
                assertCqlMatches(ReadCase.of(label + " cql", seed++), cfs, "SELECT v0, m[7], sm[7] FROM %s", 2);
                assertCqlReplicaResponsesMatch(ReadCase.of(label + " cql", seed++), cfs, "SELECT pk, ck, v0 FROM %s", 3);
            }
        }
    }

    /**
     * A row that holds only the map with no cells and has no deletion has live data on the
     * iterator path ({@code BTreeRow.hasLiveData} holds for a row with no deletion and any
     * column), so the limits count it, and the scan metrics count it as a live row even when the
     * row filter then drops it.
     */
    @Test
    public void collectionColumnsWithNoCellsUnderRowFiltersAndLimits() throws Throwable
    {
        DataLimits[] limits = { DataLimits.NONE, DataLimits.cqlLimits(1), DataLimits.cqlLimits(2), DataLimits.cqlLimits(5, 1) };
        for (Sources sources : new Sources[]{ Sources.ONE_SSTABLE, Sources.TWO_SSTABLES_ROW_IN_ONE, Sources.SSTABLE_AND_MEMTABLE_WITHOUT_THE_ROW })
        {
            ColumnFamilyStore cfs = loadEmptyColumns(sources, Cover.NONE, false);
            for (int filter : new int[]{ 0, 1 })
            {
                for (int clustering : new int[]{ 0, 2 })
                {
                    for (boolean reversed : new boolean[]{ false, true })
                    {
                        for (int rowFilter = 0; rowFilter < 5; rowFilter++)
                        {
                            for (DataLimits limit : limits)
                            {
                                int f = filter, c = clustering, r = rowFilter;
                                String name = sources + " filter=" + f + " clustering=" + c + (reversed ? " reversed" : "")
                                              + " rowFilter=" + r + " limits=" + limit;
                                assertAllReadSurfacesMatch(ReadCase.of(name, seed++), cfs,
                                                           now -> emptyColumnCommand(cfs, now, f, c, reversed, false, r, limit));
                            }
                        }
                    }
                }
            }
            for (int pageSize : new int[]{ 1, 2 })
                assertPagedMatches(ReadCase.of(sources + " paged", seed++), cfs,
                                   now -> emptyColumnCommand(cfs, now, 1, 0, false, false, 0, DataLimits.NONE), pageSize);
        }
    }

    /**
     * A memtable row that holds only a map deletion past gc grace, which the row filter drops.
     * The purge removes the deletion, which leaves no column, so the scan metrics do not count
     * the row as live.
     */
    @Test
    public void filteredMemtableRowWithAPurgedCollectionDeletion() throws Throwable
    {
        ColumnFamilyStore cfs = loadEmptyColumns(Sources.ONE_SSTABLE, Cover.NONE, true);
        execute("DELETE m FROM %s USING TIMESTAMP 300 WHERE pk = 1 AND ck = 5");
        long later = FBUtilities.nowInSeconds() + 100_000;
        for (int filter : new int[]{ 0, 1 })
        {
            for (boolean reversed : new boolean[]{ false, true })
            {
                for (int rowFilter : new int[]{ 1, 3, 4 })
                {
                    int f = filter, r = rowFilter;
                    String name = "memtable map deletion filter=" + f + (reversed ? " reversed" : "") + " rowFilter=" + r;
                    assertAllReadSurfacesMatch(ReadCase.of(name, seed++).at(later), cfs,
                                               now -> emptyColumnCommand(cfs, now, f, 0, reversed, false, r, DataLimits.NONE));
                }
            }
        }
    }

    /**
     * A row whose timestamp and TTL have expired past gc grace, with the list deletion also past
     * gc grace.  The purge on the read removes the row timestamp, the cells and the list deletion,
     * and on the iterator path the row then holds only the map with no cells.  The row is not
     * empty, so the purge keeps it, and the responses carry it.
     */
    @Test
    public void expiredRowPastGcGraceKeepsTheEmptyColumn() throws Throwable
    {
        for (boolean secondSSTable : new boolean[]{ false, true })
        {
            createTable(EMPTY_COLUMN_TABLE + " WITH gc_grace_seconds = 0");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int pk = 1; pk <= 3; pk++)
            {
                execute("UPDATE %s USING TIMESTAMP 100 SET m = m + {1: 10} WHERE pk = ? AND ck = 1", pk);
                execute("INSERT INTO %s (pk, ck, v0, l) VALUES (?, 1, 5, [7]) USING TIMESTAMP 200 AND TTL 1", pk);
            }
            flush();
            if (secondSSTable)
            {
                execute("INSERT INTO %s (pk, ck, v0) VALUES (1, 3, 8) USING TIMESTAMP 300");
                flush();
            }
            String label = "expired row" + (secondSSTable ? " and another sstable" : "");
            long later = FBUtilities.nowInSeconds() + 100_000;

            // the iterator path keeps the row with the empty map and nothing else
            setRangePath(false);
            List<String> records = iteratorRecords(emptyColumnCommand(cfs, later, 0, 0, false, false, 0, DataLimits.NONE));
            assertTrue(label + ": " + records, rowHasEmptyMap(records, 1, 1));

            assertCqlMatches(ReadCase.of(label + " cql", seed++).at(later), cfs, "SELECT v0, m[7] FROM %s", 0);
            assertCqlMatches(ReadCase.of(label + " cql paged", seed++).at(later), cfs, "SELECT v0, m[7] FROM %s", 1);
            for (int filter = 0; filter < 3; filter++)
            {
                for (int clustering = 0; clustering < 3; clustering++)
                {
                    for (boolean reversed : new boolean[]{ false, true })
                    {
                        int f = filter, c = clustering;
                        String name = label + " filter=" + f + " clustering=" + c + (reversed ? " reversed" : "");
                        assertAllReadSurfacesMatch(ReadCase.of(name, seed++).at(later), cfs,
                                                   now -> emptyColumnCommand(cfs, now, f, c, reversed, false, 0, DataLimits.NONE));
                    }
                }
            }
        }
    }

    private ColumnFamilyStore loadEmptyColumns(Sources sources, Cover cover, boolean purgeable) throws Throwable
    {
        createTable(EMPTY_COLUMN_TABLE + (purgeable ? " WITH gc_grace_seconds = 0" : ""));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        writeEmptyColumns(sources, cover);
        return cfs;
    }

    /**
     * Rows 1 and 4 of partitions 1 and 2: a map cell (and on row 1 a set cell) at 100, then a list
     * overwrite at 200, which stores a complex deletion on the row; row 1 also has a row timestamp.
     * The static row holds a map cell at 100 and a list overwrite at 200.  Row 2 is a plain row.
     * The cover applies to partition 1 only; partition 3 holds plain rows, in the second source.
     */
    private void writeEmptyColumns(Sources sources, Cover cover) throws Throwable
    {
        if (cover == Cover.PARTITION_DELETION)
            execute("DELETE FROM %s USING TIMESTAMP 50 WHERE pk = 1");
        else if (cover == Cover.RANGE_DELETION)
            execute("DELETE FROM %s USING TIMESTAMP 50 WHERE pk = 1 AND ck >= 0 AND ck <= 4");
        for (int pk = 1; pk <= 2; pk++)
        {
            execute("UPDATE %s USING TIMESTAMP 100 SET m = m + {1: 10}, st = st + {1} WHERE pk = ? AND ck = 1", pk);
            execute("INSERT INTO %s (pk, ck, v0, l) VALUES (?, 1, 5, [7]) USING TIMESTAMP 200", pk);
            execute("UPDATE %s USING TIMESTAMP 100 SET m = m + {1: 10} WHERE pk = ? AND ck = 4", pk);
            execute("UPDATE %s USING TIMESTAMP 200 SET l = [7] WHERE pk = ? AND ck = 4", pk);
            execute("UPDATE %s USING TIMESTAMP 100 SET sm = sm + {1: 10} WHERE pk = ?", pk);
            execute("UPDATE %s USING TIMESTAMP 200 SET sl = [7], s0 = 3 WHERE pk = ?", pk);
            execute("INSERT INTO %s (pk, ck, v0, m) VALUES (?, 2, 6, {2: 20}) USING TIMESTAMP 150", pk);
        }
        flush();
        execute("INSERT INTO %s (pk, ck, v0, m) VALUES (3, 1, 7, {3: 30}) USING TIMESTAMP 250");
        switch (sources)
        {
            case ONE_SSTABLE:
                flush();
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

    /** The iterator path must give the empty map on row 1 of partition 1 exactly when the row is
     *  read as stored, and always on partition 2, which one sstable holds. */
    private void checkIteratorShape(String label, ColumnFamilyStore cfs, boolean asStored)
    {
        long now = FBUtilities.nowInSeconds();
        setRangePath(false);
        List<String> fetchAll = iteratorRecords(emptyColumnCommand(cfs, now, 0, 0, false, false, 0, DataLimits.NONE));
        List<String> selection = iteratorRecords(emptyColumnCommand(cfs, now, 1, 0, false, false, 0, DataLimits.NONE));
        assertEquals(label + ": iterator records with every column fetched " + fetchAll, asStored, rowHasEmptyMap(fetchAll, 1, 1));
        assertEquals(label + ": iterator records with only the selection fetched " + selection, asStored, rowHasEmptyMap(selection, 1, 4));
        assertTrue(label + ": iterator records of partition 2 " + fetchAll, rowHasEmptyMap(fetchAll, 2, 1));
        assertTrue(label + ": iterator records of partition 2 " + selection, rowHasEmptyMap(selection, 2, 4));
    }

    private static List<String> iteratorRecords(PartitionRangeReadCommand command)
    {
        List<String> records = new ArrayList<>();
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            canonicalRecordsInto(partitions, records);
        }
        return records;
    }

    private static boolean rowHasEmptyMap(List<String> records, int pk, int ck)
    {
        String partition = "PARTITION key=" + ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(pk)) + ' ';
        String row = "ROW clustering=CLUSTERING:" + ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(ck)) + ' ';
        boolean inPartition = false;
        boolean inRow = false;
        for (String record : records)
        {
            if (record.startsWith("PARTITION "))
                inPartition = record.startsWith(partition);
            if (!record.startsWith("  "))
                inRow = inPartition && record.startsWith(row);
            // the record ends with the column's heap size
            else if (inRow && record.startsWith("  CPLX m" + EMPTY_LIVE + " heap="))
                return true;
        }
        return false;
    }

    /**
     * @param filter       0: every regular column fetched, the map's element 7 queried; 1: only
     *                     v0, s0 and element 7 of both maps fetched; 2: every regular column
     *                     fetched, elements 5 to 9 of the map queried
     * @param clustering   0: the whole partition; 1: the slice of row 1; 2: the names of rows 1, 2 and 4
     * @param onePartition read partition 1 alone, as a key range of one key
     * @param rowFilter    0: none; 1: v0 = 5, which only row 1 passes and row 4 fails at its end for
     *                     having no v0; 2: ck = 2, which rows 1 and 4 fail on their clustering;
     *                     3: v0 = 999, which no row passes; 4: v0 = 5 in a filter that needs
     *                     reconciliation
     */
    private static PartitionRangeReadCommand emptyColumnCommand(ColumnFamilyStore cfs, long now, int filter, int clustering, boolean reversed,
                                                                boolean onePartition, int rowFilter, DataLimits limits)
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
        AbstractBounds<PartitionPosition> keys;
        if (onePartition)
        {
            DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
            keys = new Bounds<>(key, key);
        }
        else
        {
            keys = DataRange.allData(metadata.partitioner).keyRange();
        }
        return PartitionRangeReadCommand.create(metadata, now, columns, rows, limits, new DataRange(keys, clusteringFilter));
    }

    private static CellPath path(int key)
    {
        return CellPath.create(Int32Type.instance.decompose(key));
    }

    // ---------------------------------------------------------------- checks

    private void runAll(ColumnFamilyStore cfs, long now) throws Throwable
    {
        for (String query : STREAMED)
            assertWritten(cfs, now, query, true, true);
        for (String query : BUILDS_ROWS)
            assertWritten(cfs, now, query, false, true);
    }

    private void assertWritten(ColumnFamilyStore cfs, long now, String query, boolean buildsNoRows, boolean succeeds)
    {
        assertWritten(cfs, now, query, buildsNoRows, succeeds, at -> rangeCommand(query, at));
    }

    private void assertWritten(ColumnFamilyStore cfs, long now, String label, boolean buildsNoRows, LongFunction<PartitionRangeReadCommand> command)
    {
        assertWritten(cfs, now, label, buildsNoRows, true, command);
    }

    private void assertWritten(ColumnFamilyStore cfs, long now, String label, boolean buildsNoRows, boolean succeeds,
                               LongFunction<PartitionRangeReadCommand> command)
    {
        ReadCase c = ReadCase.of(label, seed++).at(now);
        long written = CursorRangeReads.rangeResponsesWritten();
        long built = CursorReads.unfilteredsMaterialized();
        // the data read alone: a read that tracks warnings builds rows for the read size guardrail
        assertDataResponseAloneMatches(c, cfs, command);
        // a read that fails does not finish its response
        if (succeeds)
            assertEquals("the cursor path wrote the response of " + label, 1, CursorRangeReads.rangeResponsesWritten() - written);
        if (buildsNoRows)
            assertEquals("rows built for the response of " + label, 0, CursorReads.unfilteredsMaterialized() - built);
        assertReplicaResponsesMatch(c, cfs, command);
    }

    // ---------------------------------------------------------------- data

    /**
     * A table with a static column and a list, written into five overlapping sstables and a
     * memtable: rows, cells, TTLs that have expired when the reads run, row, range and partition
     * deletions, static-only partitions, and one wide partition with a row index.
     */
    private ColumnFamilyStore load(boolean purgeable) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s text static, v int, l list<int>, PRIMARY KEY (k, c)) " +
                    "WITH gc_grace_seconds = " + (purgeable ? 0 : 864000));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        write(40, true);
        return cfs;
    }

    private void write(int partitions, boolean deletions) throws Throwable
    {
        long ts = BASE_TS;
        for (int flush = 0; flush < 4; flush++)
        {
            for (int k = flush; k < partitions; k += 2)
            {
                for (int c = flush; c < 12; c += 3)
                    execute("INSERT INTO %s (k, c, v, l) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", k, c, (k + c) % 5, List.of(c, c + 1, 4), ++ts);
                execute("UPDATE %s USING TIMESTAMP ? SET s = ? WHERE k = ?", ++ts, "s" + (k % 3), k);
                if (k % 7 == 0)
                    execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL 1", k, 20 + flush, 9, ++ts);
                if (deletions && k % 5 == flush)
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND c > ? AND c < ?", ++ts, k, flush, flush + 5);
                if (deletions && k % 6 == flush)
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND c = ?", ++ts, k, flush + 3);
                if (deletions && k % 11 == flush)
                    execute("DELETE v FROM %s USING TIMESTAMP ? WHERE k = ? AND c = ?", ++ts, k, flush);
            }
            if (deletions && flush == 2)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ?", ++ts, 4);
            // a partition with only a static row
            execute("UPDATE %s USING TIMESTAMP ? SET s = ? WHERE k = ?", ++ts, "static only", partitions + flush);
            flush();
        }
        // one wide partition, row indexed
        for (int c = 100; c < 600; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, c, c % 5, ++ts);
        if (deletions)
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND c > ? AND c < ?", ++ts, 1, 150, 400);
        flush();
        // the memtable overlay
        for (int k = 0; k < partitions; k += 4)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?", k, 1, 3, ++ts);
        if (deletions)
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND c >= ?", ++ts, 8, 2);
    }
}
