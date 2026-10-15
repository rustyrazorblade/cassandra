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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MessageParams;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Named repros of cursor read bugs fixed on this branch, run through every {@link CursorReadOracle}
 * read surface:
 * <ul>
 *   <li>reverse reads over a row-indexed partition, whose tail block can start on the
 *       end-of-partition marker (the cursor once threw in {@code gotoBlock}'s counterpart);</li>
 *   <li>a merge of a BIG leg and a BTI leg where a key-cache lower bound makes the merge open a
 *       deferred leg past its header ("Leg in an unexpected state before unfiltered sort: 8");</li>
 *   <li>a names read of a row next to a range tombstone whose exclusive end does not reach it (the
 *       shape of seed -4715507501933830118L in {@link RandomCursorReadDifferentialTest});</li>
 *   <li>a range deletion with no upper bound, read through a slice that ends at TOP: the deletion
 *       closes at a bound equal to TOP, and the merge once read it as part of the slice ("the
 *       merge read past the end of slice 0");</li>
 *   <li>a replica read resumed after the last row of its slices, which leaves it no slice: the
 *       replica response once threw UnsupportedOperationException;</li>
 *   <li>a read that fetches every regular column but queries only some: the cursor path once
 *       kept the values of the collection cells it does not query;</li>
 *   <li>a names read of one sstable that holds the partition without static data: the replica
 *       response once wrote an empty static row;</li>
 *   <li>a names read of a partition only in the memtable, outside the timestamp-order read: the
 *       cursor path once reported every fetched column instead of the memtable iterator's;</li>
 *   <li>a partition only in the memtable, with an sstable read only for the static row that does
 *       not hold it: the response header once had different EncodingStats.</li>
 * </ul>
 */
public class CursorReadRegressionTest extends CursorReadOracle
{
    private static final Logger REGRESSION_LOGGER = LoggerFactory.getLogger(CursorReadRegressionTest.class);

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;
    private int originalColumnIndexCacheSizeKiB;
    private long originalKeyCacheCapacity;

    @Before
    public void save()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        originalColumnIndexCacheSizeKiB = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        originalKeyCacheCapacity = CacheService.instance.keyCache.getCapacity();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
        DatabaseDescriptor.setColumnIndexCacheSize(originalColumnIndexCacheSizeKiB);
        CacheService.instance.keyCache.setCapacity(originalKeyCacheCapacity);
        CacheService.instance.invalidateKeyCache();
    }

    @Test
    public void reverseReadOverRowIndexedPartitionBig()
    {
        reverseReadOverRowIndexedPartition("big");
    }

    @Test
    public void reverseReadOverRowIndexedPartitionBti()
    {
        reverseReadOverRowIndexedPartition("bti");
    }

    private void reverseReadOverRowIndexedPartition(String format)
    {
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        DatabaseDescriptor.setColumnIndexSizeInKiB(64);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, pad text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String pad = "x".repeat(200);
        for (long ck = 0; ck < 4000; ck++)
            execute("INSERT INTO %s (pk, ck, v1, pad) VALUES (?, ?, ?, ?)", 1L, ck, ck, pad);
        flush();
        assertEquals(1, cfs.getLiveSSTables().size());

        assertAllReadSurfacesMatch(ReadCase.of(format + " reverse full", 1), cfs,
                                   now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).reverse().build());
        assertAllReadSurfacesMatch(ReadCase.of(format + " reverse slice", 2), cfs,
                                   now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                           .fromIncl(500L).toIncl(3500L).reverse().build());
        assertAllReadSurfacesMatch(ReadCase.of(format + " reverse limit", 3), cfs,
                                   now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                           .reverse().withLimit(5).build());
        assertAllReadSurfacesMatch(ReadCase.of(format + " reverse full cold", 4).cold(), cfs,
                                   now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).reverse().build());
    }

    /**
     * A partition inside the token range of the only sstable but not in it, and nothing in the
     * memtable: the replica response once threw IndexOutOfBoundsException (the absent sstable
     * counted as the one source, with no leg to read).
     */
    @Test
    public void partitionAbsentFromTheOnlySstable()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (long pk = 0; pk < 100; pk += 2)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, 1L, pk);
            flush();
            assertEquals(1, cfs.getLiveSSTables().size());

            int seed = 0;
            for (long pk = 1; pk < 20; pk += 2)
            {
                long key = pk;
                assertAllReadSurfacesMatch(ReadCase.of(format + " absent pk=" + pk, ++seed), cfs,
                                           now -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build());
                assertAllReadSurfacesMatch(ReadCase.of(format + " absent reversed pk=" + pk, ++seed), cfs,
                                           now -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).reverse().build());
            }
        }
    }

    /** BIG legs whose lower bound comes from the key cache and spans the slice start, merged with a BTI leg. */
    @Test
    public void mergeOfForceOpenedBigLegWithBtiLeg()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        DatabaseDescriptor.setColumnIndexCacheSize(100 * 1024);
        if (originalKeyCacheCapacity == 0)
            CacheService.instance.keyCache.setCapacity(1L << 20);
        CacheService.instance.invalidateKeyCache();
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // BIG leg: its first unfiltered is a row with cells
        DatabaseDescriptor.setSelectedSSTableFormat("big");
        for (long ck = 0; ck < 2000; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1L, ck, ck, "big-" + ck);
        flush();
        // second BIG leg: its first unfiltered is an open range tombstone bound
        for (long ck = 0; ck < 2000; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1500", 1L, ck, ck, "big2-" + ck);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 0L, 1900L);
        for (long ck = 0; ck < 1600; ck++)
            execute("UPDATE %s USING TIMESTAMP 3000 SET v1 = ? WHERE pk = ? AND ck = ?", -ck, 1L, ck);
        flush();
        // BTI leg
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        for (long ck = 0; ck < 2000; ck += 3)
            execute("UPDATE %s USING TIMESTAMP 2500 SET v2 = ? WHERE pk = ? AND ck = ?", "bti-" + ck, 1L, ck);
        flush();
        assertEquals(1, cfs.getLiveSSTables().stream().filter(s -> s instanceof BtiTableReader).count());

        // warm the key cache so the BIG legs carry a lower bound from it
        execute("SELECT * FROM %s WHERE pk = 1");

        LongFunction<SinglePartitionReadCommand> fromZero =
            now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(0L).toIncl(2000L).build();
        assertExecuteLocallyMatches(ReadCase.of("BIG and BTI with key cache", 6), cfs, fromZero);
        assertReplicaResponsesMatch(ReadCase.of("BIG and BTI with key cache", 7), cfs, fromZero);
        assertAllReadSurfacesMatch(ReadCase.of("BIG and BTI with key cache, narrow slice", 8), cfs,
                                   now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                           .fromIncl(1000L).toIncl(1010L).build());
    }

    /**
     * The shape of RandomMultiSliceCursorReadDifferentialTest seed 3644616953108509967L: a slice
     * ending exclusively at a clustering and the next starting inclusively there, while one leg's
     * range tombstone stays open across both and another leg's newer one opens there.  The
     * iterator path slices each leg, then merges the slice-bound markers into one boundary; the
     * cursor path wrote a separate close and open.
     */
    @Test
    public void adjacentSlicesWithRangeTombstonesOnSeveralLegs()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean newerInMemtable : new boolean[]{ false, true })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                for (long ck = 125; ck < 150; ck++)
                    execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 300", 1L, ck, ck);
                execute("DELETE FROM %s USING TIMESTAMP 150 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 130L, 146L);
                flush();
                execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 137L, 138L);
                if (!newerInMemtable)
                    flush();
                String label = format + (newerInMemtable ? " newer tombstone in memtable" : " newer tombstone in sstable");
                int seed = newerInMemtable ? 20 : 10;
                for (boolean limited : new boolean[]{ false, true })
                {
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        TableMetadata metadata = cfs.metadata();
                        ClusteringComparator comparator = metadata.comparator;
                        Slices slices = new Slices.Builder(comparator)
                                        .add(Slice.make(ClusteringBound.create(comparator, true, false, 135L), ClusteringBound.create(comparator, false, false, 137L)))
                                        .add(Slice.make(ClusteringBound.create(comparator, true, true, 137L), ClusteringBound.create(comparator, false, true, 137L)))
                                        .add(Slice.make(ClusteringBound.create(comparator, true, true, 139L), ClusteringBound.create(comparator, false, false, 141L)))
                                        .build();
                        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(),
                                                                 limited ? DataLimits.cqlLimits(14) : DataLimits.NONE,
                                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1L)),
                                                                 new ClusteringIndexSliceFilter(slices, false));
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(label + (limited ? " limit 14" : ""), seed++), cfs, read);
                }
            }
        }
    }

    /** The shape of seed -4715507501933830118L: names reads next to range tombstones. */
    @Test
    public void namesReadNextToRangeTombstoneExclusiveEnd()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (k int, c int, s text static, v1 boolean, v2 text, PRIMARY KEY (k, c))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            execute("INSERT INTO %s (k, c, v1, v2) VALUES (1, 0, true, 'lo') USING TIMESTAMP 10");
            execute("INSERT INTO %s (k, c, v1, v2) VALUES (1, 7, false, 'hi') USING TIMESTAMP 11");
            for (int c = 1; c < 7; c++)
                execute("INSERT INTO %s (k, c, v1, v2) VALUES (1, ?, true, ?) USING TIMESTAMP 20", c, "v" + c);
            execute("DELETE FROM %s USING TIMESTAMP 52 WHERE k = 1 AND c >= 1 AND c < 5");
            execute("DELETE FROM %s USING TIMESTAMP 65 WHERE k = 1 AND c >= 5 AND c < 6");
            flush();
            execute("INSERT INTO %s (k, c, v1) VALUES (1, 3, false) USING TIMESTAMP 70");
            for (int[] names : new int[][]{ { 0, 5, 7 }, { 0, 4, 5, 6, 7 }, { 0, 1, 7 }, { 5 } })
            {
                for (boolean reversed : new boolean[]{ false, true })
                {
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        var b = Util.cmd(cfs, 1).withNowInSeconds(now).columns("v1");
                        for (int c : names)
                            b = b.includeRow(c);
                        return (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(format + " names " + Arrays.toString(names)
                                                           + (reversed ? " reversed" : ""), names.length + (reversed ? 1 : 0)),
                                               cfs, read);
                }
            }
        }
    }

    /**
     * A range deletion with no upper bound in the middle or the last row index block of one
     * compacted sstable, read by one leg.  The shape of RangeTombstoneBoundaryTest.
     */
    @Test
    public void rangeDeletionToPartitionEndInOneSstable()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        for (String format : new String[]{ "big", "bti" })
        {
            for (long deleteFrom : new long[]{ 15L, 27L })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck)) " +
                            "WITH compression = {'enabled': 'false'}");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                String pad = "x".repeat(80);
                for (long ck = 0; ck < 30; ck++)
                    execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1", 1L, ck, pad + ck);
                flush();
                execute("DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck >= ?", 1L, deleteFrom);
                flush();
                cfs.forceMajorCompaction();
                assertEquals(1, cfs.getLiveSSTables().size());
                assertRangeDeletionToPartitionEndMatches(format + " delete from " + deleteFrom, cfs, deleteFrom);
            }
        }
    }

    /**
     * A range deletion with no upper bound in a newer source than the rows it covers, so the read
     * merges the legs.  The shape of DeleteTest's range deletions and of SSTablesIteratedTest's
     * DESC deletion on a row-indexed sstable.
     */
    @Test
    public void rangeDeletionToPartitionEndInNewerSource()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean deletionInMemtable : new boolean[]{ false, true })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck)) " +
                            "WITH compression = {'enabled': 'false'}");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                String pad = "x".repeat(80);
                for (long ck = 0; ck < 30; ck++)
                    execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1", 1L, ck, pad + ck);
                flush();
                execute("DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck > ?", 1L, 20L);
                if (!deletionInMemtable)
                    flush();
                String label = format + (deletionInMemtable ? " deletion in memtable" : " deletion in sstable");
                assertRangeDeletionToPartitionEndMatches(label, cfs, 21L);
            }
        }
    }

    /**
     * A range deletion that runs to the start of a DESC partition, so to the end of its storage
     * order, over a row-indexed sstable.  The shape of SSTablesIteratedTest.testDeletionOnIndexedSSTableDESC.
     */
    @Test
    public void rangeDeletionToPartitionEndWithDescendingClustering()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck)) " +
                        "WITH CLUSTERING ORDER BY (ck DESC)");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (long ck = 1; ck <= 500; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, Long.toString(ck));
            flush();
            for (long ck = 501; ck <= 1000; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, Long.toString(ck));
            execute("DELETE FROM %s WHERE pk = ? AND ck <= ?", 1L, 500L);
            flush();
            int seed = 0;
            for (String where : new String[]{ "", " AND ck > 500", " AND ck <= 1000", " AND ck <= 700" })
            {
                for (String limit : new String[]{ "", " LIMIT 1" })
                {
                    String query = "SELECT * FROM %s WHERE pk = 1" + where + limit;
                    assertCqlMatches(ReadCase.of(format + " DESC " + query, ++seed), cfs, query, 100);
                }
            }
            assertAllReadSurfacesMatch(ReadCase.of(format + " DESC full", ++seed), cfs,
                                       now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).build());
            // in DESC order a slice from a clustering runs down to the smallest one, the end of the storage order
            for (long from : new long[]{ 700L, 500L })
            {
                assertAllReadSurfacesMatch(ReadCase.of(format + " DESC from " + from, ++seed), cfs,
                                           now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                                   .fromIncl(from).build());
                assertAllReadSurfacesMatch(ReadCase.of(format + " DESC from " + from + " limit 1", ++seed), cfs,
                                           now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                                   .fromIncl(from).withLimit(1).build());
            }
        }
    }

    /**
     * A static column and a range deletion with no upper bound, in a newer sstable than the rows.
     * The shape of SSTablesIteratedTest.testNonCompactTableWithStaticColumnAndRangeDeletion.
     */
    @Test
    public void rangeDeletionToPartitionEndWithStaticColumn()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY (pk, c))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1);
            flush();
            execute("UPDATE %s USING TIMESTAMP 2000 SET v = ? WHERE pk = ? AND c = ?", 2, 1, 1);
            execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ? AND c >= ?", 3, 0);
            flush();
            execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND c > ?", 1, 0);
            execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 3002", 3, 3);
            flush();
            int seed = 0;
            for (int pk : new int[]{ 1, 3 })
            {
                for (String where : new String[]{ "", " AND c = 1", " AND c >= 1" })
                {
                    String query = "SELECT * FROM %s WHERE pk = " + pk + where;
                    assertCqlMatches(ReadCase.of(format + " static " + query, ++seed), cfs, query, 100);
                }
                assertAllReadSurfacesMatch(ReadCase.of(format + " static full pk=" + pk, ++seed), cfs,
                                           now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now).build());
                assertAllReadSurfacesMatch(ReadCase.of(format + " static from 1 pk=" + pk, ++seed), cfs,
                                           now -> (SinglePartitionReadCommand) Util.cmd(cfs, pk).withNowInSeconds(now)
                                                                                   .fromIncl(1).build());
            }
        }
    }

    /**
     * A page that ends on the last row of its slices: the next page's command resumes after that
     * row, so {@code forPaging} leaves it no slice ({@code Slices.NONE}).  The replica response
     * once threw UnsupportedOperationException there; the iterator path returns the partition
     * with its deletion and static row and no rows.  One sstable, and two sstables under a memtable, forward and reversed, one and
     * several slices, and a CQL page size that ends a page exactly on the last row.
     */
    @Test
    public void replicaReadResumedAfterTheLastRowOfItsSlices()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean merged : new boolean[]{ false, true })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                // a partition deletion older than the data: the resumed read returns it with the static row
                execute("DELETE FROM %s USING TIMESTAMP 0 WHERE pk = ?", 1L);
                execute("UPDATE %s USING TIMESTAMP 1 SET s = 'static' WHERE pk = ?", 1L);
                for (long ck = 0; ck < 20; ck++)
                    execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1", 1L, ck, ck);
                flush();
                if (merged)
                {
                    for (long ck = 0; ck < 20; ck += 2)
                        execute("UPDATE %s USING TIMESTAMP 2 SET v1 = ? WHERE pk = ? AND ck = ?", -ck, 1L, ck);
                    flush();
                    for (long ck = 1; ck < 20; ck += 3)
                        execute("UPDATE %s USING TIMESTAMP 3 SET v1 = ? WHERE pk = ? AND ck = ?", ck * 10, 1L, ck);
                }
                String label = format + (merged ? " two sstables and a memtable" : " one sstable");
                int seed = 0;
                assertResumedPastEnd(ReadCase.of(label + " slice resumed after its end", ++seed), cfs, false, 9L, 5L, 9L);
                assertResumedPastEnd(ReadCase.of(label + " reversed slice resumed after its start", ++seed), cfs, true, 5L, 5L, 9L);
                assertResumedPastEnd(ReadCase.of(label + " two slices resumed after the last end", ++seed), cfs, false, 9L, 1L, 3L, 7L, 9L);
                assertResumedPastEnd(ReadCase.of(label + " reversed two slices resumed after the first start", ++seed), cfs, true, 1L, 1L, 3L, 7L, 9L);
                for (int pageSize : new int[]{ 5, 10 })
                {
                    assertCqlReplicaResponsesMatch(ReadCase.of(label + " cql page ends on the last row", ++seed), cfs,
                                                   "SELECT * FROM %s WHERE pk = 1 AND ck >= 5 AND ck <= 9", pageSize);
                    assertCqlReplicaResponsesMatch(ReadCase.of(label + " cql reversed page ends on the last row", ++seed), cfs,
                                                   "SELECT * FROM %s WHERE pk = 1 AND ck >= 5 AND ck <= 9 ORDER BY ck DESC", pageSize);
                }
            }
        }
    }

    /**
     * A read that fetches every regular column but queries only some (the shape of
     * {@code ColumnFilter.allRegularColumnsBuilder}): the cells of a collection column it does not
     * query keep the row, but the iterator path reads them with empty values ({@code Cell.Serializer}
     * skips the value when {@code canSkipValue(column)}).  The cursor path once kept their values.
     * Collection cells as old as the row, and newer than the row, in one sstable and in two.
     */
    @Test
    public void fetchedButNotQueriedCollectionCellsHaveNoValues()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean merged : new boolean[]{ false, true })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 int, l list<text>, m map<int, text>, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                for (long ck = 0; ck < 6; ck++)
                    execute("INSERT INTO %s (pk, ck, v1, l, m) VALUES (?, ?, ?, ['a', 'b'], {1: 'one', 2: 'two'}) USING TIMESTAMP 10",
                            1L, ck, (int) ck);
                // collection cells newer than the row liveness
                execute("UPDATE %s USING TIMESTAMP 20 SET m = m + {3: 'three'}, l = l + ['c'] WHERE pk = ? AND ck = ?", 1L, 2L);
                flush();
                if (merged)
                {
                    execute("UPDATE %s USING TIMESTAMP 30 SET m = m + {4: 'four'} WHERE pk = ? AND ck = ?", 1L, 3L);
                    flush();
                }
                String label = format + (merged ? " two sstables" : " one sstable");
                int seed = merged ? 10 : 0;
                for (boolean reversed : new boolean[]{ false, true })
                {
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        TableMetadata metadata = cfs.metadata();
                        ColumnFilter columns = ColumnFilter.allRegularColumnsBuilder(metadata, false)
                                                           .add(metadata.getColumn(ByteBufferUtil.bytes("v1")))
                                                           .build();
                        return SinglePartitionReadCommand.create(metadata, now, columns, RowFilter.none(), DataLimits.NONE,
                                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1L)),
                                                                 new ClusteringIndexSliceFilter(Slices.ALL, reversed));
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(label + " fetch all, query v1" + (reversed ? " reversed" : ""), ++seed), cfs, read);
                }
            }
        }
    }

    /**
     * A names read of one sstable that holds the partition without static data, in a table with a
     * static column: the iterator path writes no static row.  The cursor replica response once
     * wrote an empty static row.  With and without another partition that has a static value, and
     * with a partition deletion, and with a set column.
     */
    @Test
    public void namesReadOfPartitionWithoutStaticData()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (int variant = 0; variant < 6; variant++)
            {
                boolean otherPartitionHasStatic = variant % 3 > 0;
                boolean partitionDeleted = variant % 3 == 2;
                // a non-frozen collection sends the iterator path's names read to the general merge
                boolean multiCell = variant >= 3;
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk text, ck int, s text static, v boolean" + (multiCell ? ", tags set<int>" : "")
                            + ", PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                // a partition deletion older than the rows: the empty static row is then kept
                if (partitionDeleted)
                    execute("DELETE FROM %s USING TIMESTAMP 5 WHERE pk = 'a'");
                execute("INSERT INTO %s (pk, ck, v) VALUES ('a', 24, true) USING TIMESTAMP 10");
                execute("INSERT INTO %s (pk, ck, v) VALUES ('a', 30, false) USING TIMESTAMP 10");
                if (otherPartitionHasStatic)
                    execute("INSERT INTO %s (pk, ck, s, v) VALUES ('b', 1, 'static', true) USING TIMESTAMP 10");
                flush();
                assertEquals(1, cfs.getLiveSSTables().size());
                String label = format + (otherPartitionHasStatic ? " another partition has a static value" : " no static value")
                               + (partitionDeleted ? ", partition deleted" : "") + (multiCell ? ", set column" : "");
                int seed = variant * 10;
                for (int[] names : new int[][]{ { 16, 18, 20, 24 }, { 24 }, { 25 }, { 24, 30 } })
                {
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        var b = Util.cmd(cfs, "a").withNowInSeconds(now);
                        for (int c : names)
                            b = b.includeRow(c);
                        return (SinglePartitionReadCommand) b.build();
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(label + " names " + Arrays.toString(names), ++seed), cfs, read);
                }
            }
        }
    }

    /**
     * A names read of a partition only in the memtable, where the iterator path does not read in
     * timestamp order (a set column is queried, or the read tracks repaired data): the memtable
     * iterator reports only the columns of its static row when the read selects no row, or the
     * partition has none.  The cursor path once reported every fetched column.  A partition
     * deletion alone, with a static value, and with a row; no names, names, and names resumed
     * after the last one; with and without an sstable that does not hold the partition.
     */
    @Test
    public void memtableOnlyNamesReadReportsTheIteratorColumns()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (int variant = 0; variant < 6; variant++)
            {
                boolean absentSSTable = variant >= 3;
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk int, ck int, s int static, v int, tags set<int>, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                if (absentSSTable)
                {
                    execute("INSERT INTO %s (pk, ck, s, v, tags) VALUES (2, 1, 1, 1, {1}) USING TIMESTAMP 1");
                    flush();
                }
                execute("DELETE FROM %s USING TIMESTAMP 5 WHERE pk = 1");
                if (variant % 3 == 1)
                    execute("UPDATE %s USING TIMESTAMP 10 SET s = 7 WHERE pk = 1");
                if (variant % 3 == 2)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 3) USING TIMESTAMP 10");
                String label = format + " variant " + variant;
                int seed = variant * 10;
                for (boolean reversed : new boolean[]{ false, true })
                {
                    // no names: a read of the static row alone
                    assertAllReadSurfacesMatch(ReadCase.of(label + " no names" + (reversed ? " reversed" : ""), ++seed), cfs, now -> {
                        TableMetadata metadata = cfs.metadata();
                        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(), DataLimits.NONE,
                                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1)),
                                                                 new ClusteringIndexNamesFilter(new TreeSet<>(metadata.comparator), reversed));
                    });
                }
                for (int[] names : new int[][]{ { 3 }, { 1, 3, 5 } })
                {
                    for (boolean reversed : new boolean[]{ false, true })
                    {
                        LongFunction<SinglePartitionReadCommand> read = now -> {
                            var b = Util.cmd(cfs, 1).withNowInSeconds(now);
                            for (int c : names)
                                b = b.includeRow(c);
                            return (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
                        };
                        assertAllReadSurfacesMatch(ReadCase.of(label + " names " + Arrays.toString(names) + (reversed ? " reversed" : ""), ++seed),
                                                   cfs, read);
                    }
                }
                // a names read resumed after its last name selects no row
                assertAllReadSurfacesMatch(ReadCase.of(label + " names resumed after the last", ++seed), cfs, now -> {
                    SinglePartitionReadCommand base = (SinglePartitionReadCommand) Util.cmd(cfs, 1).withNowInSeconds(now)
                                                                                      .includeRow(1).includeRow(3).build();
                    return base.forPaging(Clustering.make(ByteBufferUtil.bytes(3)), DataLimits.cqlLimits(5));
                });
            }
        }
    }

    /**
     * A partition only in the memtable, and an sstable that does not hold it, holds static data,
     * and has no clustering in the slices read: the iterator path reads that sstable only for the
     * static row, at once, and its empty result has no stats.  The cursor path once deferred that
     * read and merged the sstable's stats into the response header.  A range deletion alone and
     * with a row; forward, reversed and two-slice reads.
     */
    @Test
    public void memtableOnlyPartitionWithAnSstableReadForTheStaticRow()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean withRow : new boolean[]{ false, true })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v int, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                // older data, with a static value, and only clustering 1, in partitions around pk 1
                execute("INSERT INTO %s (pk, ck, s, v) VALUES (0, 1, 'a', 1) USING TIMESTAMP 1000007");
                for (long pk = 2; pk < 40; pk++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, 1, 1) USING TIMESTAMP 1000009", pk);
                flush();
                DecoratedKey key = cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(1L));
                assertEquals("pk 1 must be in the key range of the sstable", 1,
                             cfs.select(View.select(SSTableSet.LIVE, key)).sstables.size());
                execute("DELETE FROM %s USING TIMESTAMP 1000099 WHERE pk = 1 AND ck >= 4 AND ck < 8");
                if (withRow)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (1, 9, 9) USING TIMESTAMP 1000296");
                String label = format + (withRow ? " range deletion and a row" : " range deletion");
                int seed = withRow ? 10 : 0;
                for (boolean reversed : new boolean[]{ false, true })
                {
                    assertAllReadSurfacesMatch(ReadCase.of(label + " slice" + (reversed ? " reversed" : ""), ++seed), cfs,
                                               now -> {
                                                   var b = Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(3L).toIncl(10L);
                                                   return (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
                                               });
                    assertAllReadSurfacesMatch(ReadCase.of(label + " two slices" + (reversed ? " reversed" : ""), ++seed), cfs,
                                               now -> CursorReadShapes.multiSlice(cfs, 1L, now, -1, reversed, 3L, 5L, 6L, 9L));
                }
            }
        }
    }

    /**
     * A replica read of a table whose plain reads go through Accord: {@code executeLocally} rejects
     * it ({@code ConsensusRequestRouter.validateSafeToReadNonTransactionally}).  The cursor replica
     * response once served it.  An empty table, and one with a flushed sstable and memtable data.
     */
    @Test
    public void replicaReadOfATableReadThroughAccord()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean withData : new boolean[]{ false, true })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck)) WITH transactional_mode = 'full'");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                if (withData)
                {
                    for (long ck = 0; ck < 5; ck++)
                        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
                    flush();
                    execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, 9L, 9L);
                }
                String label = format + (withData ? " with data" : " empty");
                int seed = withData ? 10 : 0;
                assertReplicaResponsesMatch(ReadCase.of(label + " full", ++seed), cfs,
                                            now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).build());
                assertReplicaResponsesMatch(ReadCase.of(label + " slice", ++seed), cfs,
                                            now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                                    .fromIncl(1L).toIncl(3L).build());
            }
        }
    }

    /**
     * A replica read of a BIG partition whose row index is larger than
     * {@code row_index_read_size_fail_threshold}: the iterator path fails it while reading the row
     * index entry, which finds the read through {@code ReadCommand.getCommand()}.  The cursor
     * replica response once read with no current command, so it served the read.
     */
    @Test
    public void replicaReadOverTheRowIndexSizeThreshold()
    {
        DataStorageSpec.LongBytesBound originalFail = DatabaseDescriptor.getRowIndexReadSizeFailThreshold();
        boolean originalEnabled = DatabaseDescriptor.getReadThresholdsEnabled();
        try
        {
            DatabaseDescriptor.setSelectedSSTableFormat("big");
            DatabaseDescriptor.setColumnIndexSizeInKiB(1);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'enabled': 'false'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            // an older partition deletion: the iterator path then opens the sstable while it
            // collects its sources, inside executeLocally
            execute("DELETE FROM %s USING TIMESTAMP 0 WHERE pk = ?", 1L);
            String pad = "x".repeat(200);
            for (long ck = 0; ck < 400; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 10", 1L, ck, pad);
            flush();
            // no key cache: every read deserializes the row index entry, which runs the check
            CacheService.instance.keyCache.setCapacity(0);
            CacheService.instance.invalidateKeyCache();
            DatabaseDescriptor.setReadThresholdsEnabled(true);
            DatabaseDescriptor.setRowIndexReadSizeFailThreshold(new DataStorageSpec.LongBytesBound("1KiB"));
            assertReplicaResponsesMatch(ReadCase.of("big row index over the fail threshold", 1), cfs,
                                        now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).build());
            assertReplicaResponsesMatch(ReadCase.of("big row index over the fail threshold, slice", 2), cfs,
                                        now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                                .fromIncl(100L).toIncl(120L).build());
        }
        finally
        {
            DatabaseDescriptor.setRowIndexReadSizeFailThreshold(originalFail);
            DatabaseDescriptor.setReadThresholdsEnabled(originalEnabled);
        }
    }

    /**
     * A reversed read whose limit is reached on the last row of its last slice, while a range
     * deletion is open across that slice's exclusive start: the iterator path stops there and
     * {@code RTBoundCloser} closes the deletion at the last row.  The cursor replica response
     * once wrote the slice-start close instead.  The shape of RandomMultiSliceCursorReadDifferentialTest
     * data seed 8531316844795626442L.  One sstable and two; one slice and two; the limit reached
     * and not.
     */
    @Test
    public void reversedReadStoppedByItsLimitOnTheLastRowOfItsSlices()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean merged : new boolean[]{ false, true })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                for (long ck = 0; ck < 30; ck++)
                    execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 10", 1L, ck, ck);
                // older than the rows: it deletes nothing, but its markers reach the response
                execute("DELETE FROM %s USING TIMESTAMP 5 WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 2L, 25L);
                flush();
                if (merged)
                {
                    for (long ck = 0; ck < 30; ck += 3)
                        execute("UPDATE %s USING TIMESTAMP 20 SET v1 = ? WHERE pk = ? AND ck = ?", -ck, 1L, ck);
                    flush();
                }
                String label = format + (merged ? " two sstables" : " one sstable");
                int seed = merged ? 10 : 0;
                // (5, 12]: rows 6 to 12, so a limit of 7 stops on row 6
                for (int limit : new int[]{ 7, 8 })
                {
                    assertAllReadSurfacesMatch(ReadCase.of(label + " slice limit " + limit, ++seed), cfs, now -> {
                        TableMetadata metadata = cfs.metadata();
                        ClusteringComparator comparator = metadata.comparator;
                        Slices slices = Slices.with(comparator, Slice.make(ClusteringBound.create(comparator, true, false, 5L),
                                                                           ClusteringBound.create(comparator, false, true, 12L)));
                        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(),
                                                                 DataLimits.cqlLimits(limit),
                                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1L)),
                                                                 new ClusteringIndexSliceFilter(slices, true));
                    });
                    // (5, 8] and [15, 20]: 3 and 6 rows, so a limit of 9 stops on row 6
                    int twoSliceLimit = limit + 2;
                    assertAllReadSurfacesMatch(ReadCase.of(label + " two slices limit " + twoSliceLimit, ++seed), cfs, now -> {
                        TableMetadata metadata = cfs.metadata();
                        ClusteringComparator comparator = metadata.comparator;
                        Slices slices = new Slices.Builder(comparator)
                                        .add(Slice.make(ClusteringBound.create(comparator, true, false, 5L), ClusteringBound.create(comparator, false, true, 8L)))
                                        .add(Slice.make(ClusteringBound.create(comparator, true, true, 15L), ClusteringBound.create(comparator, false, true, 20L)))
                                        .build();
                        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(),
                                                                 DataLimits.cqlLimits(twoSliceLimit),
                                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1L)),
                                                                 new ClusteringIndexSliceFilter(slices, true));
                    });
                }
            }
        }
    }

    /**
     * A static column filter that fails for a partition with no static row and no partition
     * deletion, whose first row holds more dead cells than tombstone_failure_threshold: the
     * iterator path reads that row to see whether the partition is empty before the filter drops
     * it, so it aborts the read with TombstoneOverwhelmingException.  The cursor path must read
     * and count the same row and abort the same way, through executeLocally and the replica
     * response, and match on every oracle surface.
     */
    @Test
    public void staticFilterOnPartitionWhoseFirstRowIsOverTheTombstoneThreshold() throws Throwable
    {
        int originalFailThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        DataStorageSpec.LongBytesBound originalLocalReadSizeWarn = DatabaseDescriptor.getLocalReadSizeWarnThreshold();
        DataStorageSpec.LongBytesBound originalLocalReadSizeFail = DatabaseDescriptor.getLocalReadSizeFailThreshold();
        try
        {
            // with read size thresholds set, a read that tracks warnings walks every row on both
            // paths (query-size tracking), so the cursor path does not skip the rows
            DatabaseDescriptor.setLocalReadSizeWarnThreshold(null);
            DatabaseDescriptor.setLocalReadSizeFailThreshold(null);
            for (String format : new String[]{ "big", "bti" })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk int, ck int, s int static, v1 int, v2 int, v3 int, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                // s is never set for pk 1; row 0 keeps its row liveness and has three dead cells,
                // inside gc grace
                execute("INSERT INTO %s (pk, ck, v1, v2, v3) VALUES (1, 0, 1, 1, 1) USING TIMESTAMP 10");
                execute("DELETE v1, v2, v3 FROM %s USING TIMESTAMP 20 WHERE pk = 1 AND ck = 0");
                flush();
                // a memtable row, so both cursor entry points merge the memtable and the sstable
                execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 1, 1) USING TIMESTAMP 30");
                DatabaseDescriptor.setTombstoneFailureThreshold(2);

                // CQL, as a client sends it
                String query = "SELECT * FROM %s WHERE pk = 1 AND s = 5 ALLOW FILTERING";
                DatabaseDescriptor.setCursorReadsEnabled(false);
                try
                {
                    execute(query);
                    fail("the iterator path must abort the read on the tombstones of row 0");
                }
                catch (TombstoneOverwhelmingException e)
                {
                    REGRESSION_LOGGER.info("{} iterator path: {}", format, e.getMessage());
                }
                DatabaseDescriptor.setCursorReadsEnabled(true);
                try
                {
                    execute(query);
                    fail("the cursor path must abort the read on the tombstones of row 0");
                }
                catch (TombstoneOverwhelmingException e)
                {
                    REGRESSION_LOGGER.info("{} cursor path: {}", format, e.getMessage());
                }
                finally
                {
                    DatabaseDescriptor.setCursorReadsEnabled(false);
                }

                // the command CQL builds for that query: a strict filter that needs no reconciliation
                LongFunction<SinglePartitionReadCommand> read = now -> {
                    TableMetadata metadata = cfs.metadata();
                    RowFilter rowFilter = RowFilter.create(false);
                    rowFilter.add(metadata.getColumn(ByteBufferUtil.bytes("s")), Operator.EQ, ByteBufferUtil.bytes(5));
                    SinglePartitionReadCommand command = SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), rowFilter,
                                                                                           DataLimits.NONE, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1)),
                                                                                           new ClusteringIndexSliceFilter(Slices.ALL, false));
                    command.trackWarnings();
                    return command;
                };
                long now = FBUtilities.nowInSeconds();
                for (boolean replica : new boolean[]{ false, true })
                {
                    MessageParams.reset();
                    DatabaseDescriptor.setCursorReadsEnabled(false);
                    try
                    {
                        readPartitions(read.apply(now), replica);
                        fail("the iterator path must abort the read on the tombstones of row 0");
                    }
                    catch (TombstoneOverwhelmingException e)
                    {
                        assertTrue(e.getMessage(), e.getMessage().contains("Scanned over 3 tombstones"));
                    }
                    assertEquals(Integer.valueOf(3), MessageParams.get(ParamType.TOMBSTONE_FAIL));

                    MessageParams.reset();
                    DatabaseDescriptor.setCursorReadsEnabled(true);
                    try
                    {
                        readPartitions(read.apply(now), replica);
                        fail("the cursor path must abort the read on the tombstones of row 0");
                    }
                    catch (TombstoneOverwhelmingException e)
                    {
                        assertTrue(e.getMessage(), e.getMessage().contains("Scanned over 3 tombstones"));
                    }
                    finally
                    {
                        DatabaseDescriptor.setCursorReadsEnabled(false);
                    }
                    assertEquals(Integer.valueOf(3), MessageParams.get(ParamType.TOMBSTONE_FAIL));
                }
                assertAllReadSurfacesMatch(ReadCase.of(format + " static filter over the tombstone threshold", 1), cfs, read);
            }
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalFailThreshold);
            DatabaseDescriptor.setLocalReadSizeWarnThreshold(originalLocalReadSizeWarn);
            DatabaseDescriptor.setLocalReadSizeFailThreshold(originalLocalReadSizeFail);
            MessageParams.reset();
        }
    }

    /**
     * A read that tracks repaired data, with a static column filter the partition fails: the
     * iterator path drops the partition before it reads a row when the merged static row is
     * present, so the repaired-data digest covers the repaired sstable's partition header only.
     * The cursor replica response once started the merge first, which read the repaired leg's
     * first row into the digest.  The shape of fuzz case 5054348937493764749L.  A newer static
     * value in the memtable that fails the filter, no static value at all, and a value that
     * matches.
     */
    @Test
    public void repairedDigestOfAPartitionTheStaticFilterDrops() throws Throwable
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (int variant = 0; variant < 3; variant++)
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk text, ck bigint, s text static, v text, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                if (variant == 1)
                    execute("INSERT INTO %s (pk, ck, v) VALUES ('a', 70, 'row') USING TIMESTAMP 1000158");
                else
                    execute("INSERT INTO %s (pk, ck, s, v) VALUES ('a', 70, 'old', 'row') USING TIMESTAMP 1000158");
                flush();
                SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
                sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, FBUtilities.nowInSeconds(), null, false);
                sstable.reloadSSTableMetadata();
                if (variant != 1)
                    execute("UPDATE %s USING TIMESTAMP 1000675 SET s = 'new' WHERE pk = 'a'");
                else
                    execute("INSERT INTO %s (pk, ck, v) VALUES ('a', 80, 'memtable') USING TIMESTAMP 1000675");
                String wanted = variant == 2 ? "new" : "other";
                String label = format + " variant " + variant;
                assertReplicaResponsesMatch(ReadCase.of(label, variant + 1), cfs, now -> {
                    TableMetadata metadata = cfs.metadata();
                    RowFilter rowFilter = RowFilter.create(false);
                    rowFilter.add(metadata.getColumn(ByteBufferUtil.bytes("s")), Operator.EQ, ByteBufferUtil.bytes(wanted));
                    return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), rowFilter, DataLimits.NONE,
                                                             metadata.partitioner.decorateKey(ByteBufferUtil.bytes("a")),
                                                             new ClusteringIndexSliceFilter(Slices.ALL, false));
                });
            }
        }
    }

    /**
     * A read that fetches every regular column but queries only some, over a collection cell of a
     * column it does not query that is older than the row's liveness.  A forward iterator read keeps
     * the cell with an empty value ({@code DeserializationHelper.includes}); a reversed one drops it,
     * since the reverse reader builds the block's rows and filters them ({@code ComplexColumnData.filter}).
     * The cursor path once kept it on reversed reads.  The shape of fuzz case -3334440741450517216L.
     */
    @Test
    public void fetchedButNotQueriedCollectionCellOlderThanTheRow()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk bigint, ck int, v0 bigint, m map<int, text>, tags set<int>, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int ck = 0; ck < 4; ck++)
            {
                // the map cell is older than the row liveness the INSERT writes
                execute("UPDATE %s USING TIMESTAMP 10 SET m = m + {8: 'old'} WHERE pk = ? AND ck = ?", 1L, ck);
                execute("INSERT INTO %s (pk, ck, v0, tags) VALUES (?, ?, ?, {5, 19}) USING TIMESTAMP 20", 1L, ck, (long) ck);
            }
            flush();
            int seed = 0;
            for (boolean reversed : new boolean[]{ false, true })
            {
                assertAllReadSurfacesMatch(ReadCase.of(format + " fetch all, query v0" + (reversed ? " reversed" : ""), ++seed), cfs, now -> {
                    TableMetadata metadata = cfs.metadata();
                    ColumnFilter columns = ColumnFilter.allRegularColumnsBuilder(metadata, false)
                                                       .add(metadata.getColumn(ByteBufferUtil.bytes("v0")))
                                                       .build();
                    return SinglePartitionReadCommand.create(metadata, now, columns, RowFilter.none(), DataLimits.NONE,
                                                             metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1L)),
                                                             new ClusteringIndexSliceFilter(Slices.ALL, reversed));
                });
            }
        }
    }

    /**
     * Names and slice reads with a limit that track repaired data, over sources that each hold one
     * row: repaired sstables (r), unrepaired sstables (u) and the memtable (m).  The iterator path
     * reads each unrepaired sstable through a lazy iterator ({@code makeRowIteratorWithLowerBound}),
     * so it stops at the limit without reading the sstables of the later rows.  The cursor path
     * once read more sstables.  The first three are the shape of fuzz case 9185961531705733982L,
     * with each sstable in turn repaired.
     */
    @Test
    public void limitedReadWithTrackingReadsNoLaterSstable() throws Throwable
    {
        String[] shapes = { "r18 u20 u32", "u18 r20 u32", "u18 u20 r32", "r20 u32 u40", "r20 r32 u40",
                            "r20 u32 m40", "u20 r32 m40", "m20 u32 r40" };
        for (String format : new String[]{ "big", "bti" })
        {
            int seed = 0;
            for (String shape : shapes)
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk int, ck bigint, v text, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                for (String source : shape.split(" "))
                {
                    long ck = Long.parseLong(source.substring(1));
                    Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());
                    execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, 'v') USING TIMESTAMP " + (1000000 + ck), ck);
                    if (source.charAt(0) == 'm')
                        continue;
                    flush();
                    if (source.charAt(0) == 'u')
                        continue;
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        if (before.contains(sstable))
                            continue;
                        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, FBUtilities.nowInSeconds(), null, false);
                        sstable.reloadSSTableMetadata();
                    }
                }
                for (int limit : new int[]{ 1, 2 })
                {
                    for (int kind = 0; kind < 4; kind++)
                    {
                        boolean names = kind < 2;
                        boolean reversed = kind % 2 == 1;
                        LongFunction<SinglePartitionReadCommand> read = now -> {
                            var b = Util.cmd(cfs, 0).withNowInSeconds(now).withLimit(limit);
                            if (names)
                            {
                                for (long ck : new long[]{ 10, 40, 20, 32 })
                                    b = b.includeRow(ck);
                            }
                            return (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
                        };
                        String label = format + " " + shape + ", " + (names ? "names" : "slice") + (reversed ? " reversed" : "") + ", limit " + limit;
                        assertAllReadSurfacesMatch(ReadCase.of(label, ++seed), cfs, read);
                    }
                }
            }
        }
    }

    /**
     * A filtered read over the tombstone warning threshold whose filter drops every row of the
     * partition.  On the iterator path the rows reach the scan metrics before the filter drops
     * them, so the warning names the partition.  The cursor path drops the rows while it merges
     * them; the partition is then empty, the tombstone purge removes it before the scan metrics
     * see its key, and the warning once threw a NullPointerException.  The shape of fuzz case
     * 3398697307496197883L.
     */
    @Test
    public void filteredReadOverTheTombstoneWarningThatDropsEveryRow() throws Throwable
    {
        int originalWarnThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();
        try
        {
            DatabaseDescriptor.setTombstoneWarnThreshold(1);
            for (String format : new String[]{ "big", "bti" })
            {
                DatabaseDescriptor.setSelectedSSTableFormat(format);
                createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.disableAutoCompaction();
                // two rows whose v is deleted, inside gc grace
                for (int ck = 0; ck < 2; ck++)
                {
                    execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, 1) USING TIMESTAMP 10", ck);
                    execute("DELETE v FROM %s USING TIMESTAMP 20 WHERE pk = 1 AND ck = ?", ck);
                }
                flush();
                // a memtable row, so the cursor path merges the memtable and the sstable
                execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2) USING TIMESTAMP 30");
                int seed = 0;
                for (int limit : new int[]{ DataLimits.NO_LIMIT, 5 })
                {
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        TableMetadata metadata = cfs.metadata();
                        RowFilter rowFilter = RowFilter.create(false);
                        rowFilter.add(metadata.getColumn(ByteBufferUtil.bytes("v")), Operator.EQ, ByteBufferUtil.bytes(5));
                        DataLimits limits = limit == DataLimits.NO_LIMIT ? DataLimits.NONE : DataLimits.cqlLimits(limit);
                        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), rowFilter, limits,
                                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1)),
                                                                 new ClusteringIndexSliceFilter(Slices.ALL, false));
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(format + " limit " + limit, ++seed), cfs, read);
                    for (boolean replica : new boolean[]{ false, true })
                    {
                        assertEquals(format + " limit " + limit + (replica ? " replica" : "") + " samples",
                                     sampledPartitions(cfs, read, replica, false), sampledPartitions(cfs, read, replica, true));
                    }
                }
                // the same read from CQL, and as a range read
                assertCqlMatches(ReadCase.of(format + " CQL", ++seed), cfs, "SELECT * FROM %s WHERE pk = 1 AND v = 5 ALLOW FILTERING", 0);
                assertCqlMatches(ReadCase.of(format + " CQL range", ++seed), cfs, "SELECT * FROM %s WHERE v = 5 ALLOW FILTERING", 0);
            }
        }
        finally
        {
            DatabaseDescriptor.setTombstoneWarnThreshold(originalWarnThreshold);
        }
    }

    /**
     * A read whose partition fails a partition key filter, over a partition deletion in one
     * sstable and a row in a newer one.  The iterator path's filter drops the partition before it
     * reads a row, so it reads only the sstable whose partition deletion it needs: the newer
     * sstable's lazy iterator ({@code UnfilteredRowIteratorWithLowerBound}) is never initialized.
     * The cursor path once opened the newer sstable too.  Forward and reversed, one slice and
     * several, a slice inside the newer sstable's clustering range, and names.  The shape of fuzz case 1616967172746670323L.
     */
    @Test
    public void readOfAPartitionThePartitionKeyFilterDrops()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            execute("DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 1");
            flush();
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 40, 'v') USING TIMESTAMP 20");
            // a range deletion of another partition that reaches past the slices
            execute("DELETE FROM %s USING TIMESTAMP 30 WHERE pk = 0 AND ck >= 52 AND ck <= 88");
            flush();
            int seed = 0;
            for (String shape : new String[]{ "slice", "multi-slice", "slice inside the sstable", "names" })
            {
                for (boolean reversed : new boolean[]{ false, true })
                {
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        TableMetadata metadata = cfs.metadata();
                        ClusteringComparator comparator = metadata.comparator;
                        RowFilter rowFilter = RowFilter.create(false);
                        rowFilter.add(metadata.getColumn(ByteBufferUtil.bytes("pk")), Operator.EQ, ByteBufferUtil.bytes(2L));
                        ClusteringIndexFilter filter;
                        if (shape.equals("names"))
                        {
                            TreeSet<Clustering<?>> names = new TreeSet<>(comparator);
                            for (long ck : new long[]{ 30, 40, 60 })
                                names.add(Clustering.make(ByteBufferUtil.bytes(ck)));
                            filter = new ClusteringIndexNamesFilter(names, reversed);
                        }
                        else
                        {
                            Slices.Builder slices = new Slices.Builder(comparator);
                            long[] bounds = shape.equals("slice") ? new long[]{ 22, 74 }
                                          : shape.equals("multi-slice") ? new long[]{ 22, 51, 52, 74 }
                                          : new long[]{ 45, 60 };
                            for (int i = 0; i < bounds.length; i += 2)
                                slices.add(Slice.make(ClusteringBound.create(comparator, true, true, bounds[i]),
                                                      ClusteringBound.create(comparator, false, true, bounds[i + 1])));
                            filter = new ClusteringIndexSliceFilter(slices.build(), reversed);
                        }
                        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), rowFilter, DataLimits.NONE,
                                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1L)), filter);
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(format + " " + shape + (reversed ? " reversed" : ""), ++seed), cfs, read);
                }
            }
        }
    }

    /**
     * A merged read over an sstable that does not hold the partition, with no lower bound to defer
     * against: the slice starts inside the clustering range of each sstable.  With no bloom filter
     * the sstable is a candidate, and the cursor path finds the partition absent only when it opens
     * the leg at the merge start.  The shape of fuzz case 1616967172746670323L, unfiltered.
     */
    @Test
    public void mergedReadOverAnSstableWithoutThePartition()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) WITH bloom_filter_fp_chance = 1.0");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 40, 'a') USING TIMESTAMP 10");
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 58, 'b') USING TIMESTAMP 10");
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 88, 'c') USING TIMESTAMP 10");
            flush();
            // other partitions only, around pk 1 in token order, over a clustering range the
            // slice starts inside
            for (long pk = 2; pk < 40; pk++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, 50, 'd') USING TIMESTAMP 20", pk);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, 70, 'e') USING TIMESTAMP 20", pk);
            }
            flush();
            int seed = 0;
            for (boolean reversed : new boolean[]{ false, true })
            {
                LongFunction<SinglePartitionReadCommand> read = now -> {
                    TableMetadata metadata = cfs.metadata();
                    ClusteringComparator comparator = metadata.comparator;
                    Slices slices = Slices.with(comparator, Slice.make(ClusteringBound.create(comparator, true, true, 55L),
                                                                       ClusteringBound.create(comparator, false, true, 60L)));
                    return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(), DataLimits.NONE,
                                                             metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1L)),
                                                             new ClusteringIndexSliceFilter(slices, reversed));
                };
                assertAllReadSurfacesMatch(ReadCase.of(format + (reversed ? " reversed" : ""), ++seed), cfs, read);
            }
        }
    }

    /**
     * A filtered read of a counter table that drops a row whose counter cell was deleted and then
     * incremented again.  A counter deletion wins over any increment, whatever the timestamps, so
     * on the iterator path the scan metrics count the row's cell as a tombstone.  The cursor path
     * drops the row while it merges it and once counted it as a live row, as for a regular cell
     * where the newer increment would win.  The shape of fuzz case 6685983846420660757L.
     */
    @Test
    public void filterDroppedCounterRowWhoseDeletionWins()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk int, ck int, c counter, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            execute("UPDATE %s SET c = c + 1 WHERE pk = 1 AND ck = 0");
            execute("UPDATE %s SET c = c + 1 WHERE pk = 1 AND ck = 10");
            execute("DELETE c FROM %s WHERE pk = 1 AND ck = 0");
            flush();
            // a newer increment of the deleted cell, in the memtable, so the cursor path merges
            execute("UPDATE %s SET c = c + 2 WHERE pk = 1 AND ck = 0");
            int seed = 0;
            for (int limit : new int[]{ DataLimits.NO_LIMIT, 5 })
            {
                LongFunction<SinglePartitionReadCommand> read = now -> {
                    TableMetadata metadata = cfs.metadata();
                    RowFilter rowFilter = RowFilter.create(false);
                    rowFilter.add(metadata.getColumn(ByteBufferUtil.bytes("ck")), Operator.GT, ByteBufferUtil.bytes(5));
                    DataLimits limits = limit == DataLimits.NO_LIMIT ? DataLimits.NONE : DataLimits.cqlLimits(limit);
                    return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), rowFilter, limits,
                                                             metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1)),
                                                             new ClusteringIndexSliceFilter(Slices.ALL, false));
                };
                assertAllReadSurfacesMatch(ReadCase.of(format + " limit " + limit, ++seed), cfs, read);
            }
        }
    }

    /**
     * A read of a single-cell column dropped with a timestamp older than its cells, which then
     * survive the read (the CASSANDRA-21607 shape).  The response header does not carry the
     * column, so the iterator path's serializer fails on the cell with an AssertionError naming
     * the column.  The cursor replica response once wrote the cell anyway, and the response did
     * not decode.  A vector column and an int column, in one sstable and merged over two, in a
     * row with as many columns as the header and in one with fewer.  The shape of fuzz case
     * 3150292922389885837L.
     */
    @Test
    public void readOfADroppedColumnWhoseCellsAreNewerThanTheDrop()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (String type : new String[]{ "vector<float, 2>", "int" })
            {
                for (int variant = 0; variant < 4; variant++)
                {
                    boolean merged = variant % 2 == 1;
                    // with no extra column the row has as many columns as the header, so the
                    // writer marks it as having all of them; with one the row has fewer, and the
                    // header encodes the subset
                    boolean subset = variant >= 2;
                    DatabaseDescriptor.setSelectedSSTableFormat(format);
                    createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, x text, " + (subset ? "y text, " : "")
                                + "d " + type + ", PRIMARY KEY (pk, ck))");
                    ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                    cfs.disableAutoCompaction();
                    String value = type.equals("int") ? "7" : "[1.0, 2.0]";
                    execute("INSERT INTO %s (pk, ck, v, d) VALUES (0, 2, 'a', " + value + ") USING TIMESTAMP 1000037");
                    if (merged)
                    {
                        flush();
                        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 3, 'b') USING TIMESTAMP 1000040");
                    }
                    // the drop is older than the cells, so they survive the read
                    alterTable("ALTER TABLE %s DROP d USING TIMESTAMP 1000008");
                    flush();
                    String label = format + " " + type + (merged ? " merged" : "") + (subset ? " subset" : "");
                    assertAllReadSurfacesMatch(ReadCase.of(label, variant + 1), cfs,
                                               now -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build());
                }
            }
        }
    }

    /**
     * A paged read resumed inside a range deletion of a row-indexed sstable, which the cursor leg
     * seeks into with the deletion open, merged with a small sstable whose own range deletion
     * starts at the partition start.  The merge meets that deletion's start before the slice
     * start, with the seeked leg's deletion already open, and once tried to build a boundary at
     * the partition start, which cannot exist.  The iterator path's per-sstable readers skip what
     * lies before the slice start.  Forward and reversed.  The shape of fuzz case
     * 2359448369874533901L.
     */
    @Test
    public void pagedReadResumedInARangeDeletionOverADeletionFromThePartitionStart()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            DatabaseDescriptor.setColumnIndexSizeInKiB(1);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            String value = "x".repeat(100);
            for (long ck = 0; ck < 200; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?) USING TIMESTAMP 20", ck, value);
            execute("DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 1 AND ck >= 50 AND ck <= 150");
            flush();
            // a range deletion from the partition start, newer than the first one, and a row
            // after the resume points, so this sstable is read for the slice
            execute("DELETE FROM %s USING TIMESTAMP 15 WHERE pk = 1 AND ck <= 30");
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 180, 'y') USING TIMESTAMP 30");
            flush();
            int seed = 0;
            for (boolean reversed : new boolean[]{ false, true })
            {
                for (long lastReturned : new long[]{ 100, 40 })
                {
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        var b = Util.cmd(cfs, 1L).withNowInSeconds(now);
                        SinglePartitionReadCommand base = (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
                        return base.forPaging(Clustering.make(ByteBufferUtil.bytes(lastReturned)), DataLimits.cqlLimits(5));
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(format + " after " + lastReturned + (reversed ? " reversed" : ""), ++seed), cfs, read);
                }
            }
        }
    }

    /**
     * The partition-end twin of {@link #pagedReadResumedInARangeDeletionOverADeletionFromThePartitionStart}:
     * the small sstable's range deletion runs to the partition end, so a reversed paged read meets
     * its start at the partition end, before the slice start, with the seeked leg's deletion
     * open.  The row-indexed sstable has a deletion to the partition end too, so a forward read
     * closes both at the partition end.  Paged and unpaged, forward and reversed.
     */
    @Test
    public void pagedReadResumedInARangeDeletionOverADeletionToThePartitionEnd()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            DatabaseDescriptor.setColumnIndexSizeInKiB(1);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            String value = "x".repeat(100);
            for (long ck = 0; ck < 200; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?) USING TIMESTAMP 20", ck, value);
            execute("DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 1 AND ck >= 50 AND ck <= 150");
            execute("DELETE FROM %s USING TIMESTAMP 12 WHERE pk = 1 AND ck >= 160");
            flush();
            // a range deletion to the partition end, newer than the others, and a row before the
            // resume points, so this sstable is read for the reversed slice
            execute("DELETE FROM %s USING TIMESTAMP 15 WHERE pk = 1 AND ck >= 170");
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 20, 'y') USING TIMESTAMP 30");
            flush();
            int seed = 0;
            for (boolean reversed : new boolean[]{ false, true })
            {
                assertAllReadSurfacesMatch(ReadCase.of(format + " unpaged" + (reversed ? " reversed" : ""), ++seed), cfs, now -> {
                    var b = Util.cmd(cfs, 1L).withNowInSeconds(now);
                    return (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
                });
                for (long lastReturned : new long[]{ 100, 165 })
                {
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        var b = Util.cmd(cfs, 1L).withNowInSeconds(now);
                        SinglePartitionReadCommand base = (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
                        return base.forPaging(Clustering.make(ByteBufferUtil.bytes(lastReturned)), DataLimits.cqlLimits(5));
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(format + " after " + lastReturned + (reversed ? " reversed" : ""), ++seed), cfs, read);
                }
            }
        }
    }

    /**
     * A read that tracks repaired data, of a partition the partition key filter drops, over a
     * partition deletion in an unrepaired sstable and a row in a newer, repaired one.  The iterator
     * path reads the repaired sstable through a lazy iterator that nothing pulls, so it reads only
     * the sstable whose partition deletion it needs.  The cursor path once opened the lone
     * repaired leg as soon as it built the repaired merge.  Forward and reversed, one slice and
     * names.  The shape of fuzz case 3260241938575584980L.
     */
    @Test
    public void readWithTrackingOfAPartitionTheKeyFilterDropsOverOneRepairedSstable() throws Throwable
    {
        for (String format : new String[]{ "big", "bti" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            execute("DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 1");
            flush();
            Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 40, 'v') USING TIMESTAMP 20");
            flush();
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                if (before.contains(sstable))
                    continue;
                sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, FBUtilities.nowInSeconds(), null, false);
                sstable.reloadSSTableMetadata();
            }
            int seed = 0;
            for (boolean names : new boolean[]{ false, true })
            {
                for (boolean reversed : new boolean[]{ false, true })
                {
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        TableMetadata metadata = cfs.metadata();
                        RowFilter rowFilter = RowFilter.create(false);
                        rowFilter.add(metadata.getColumn(ByteBufferUtil.bytes("pk")), Operator.EQ, ByteBufferUtil.bytes(2L));
                        ClusteringIndexFilter filter;
                        if (names)
                        {
                            TreeSet<Clustering<?>> clusterings = new TreeSet<>(metadata.comparator);
                            clusterings.add(Clustering.make(ByteBufferUtil.bytes(40L)));
                            filter = new ClusteringIndexNamesFilter(clusterings, reversed);
                        }
                        else
                            filter = new ClusteringIndexSliceFilter(Slices.ALL, reversed);
                        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), rowFilter, DataLimits.NONE,
                                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1L)), filter);
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(format + (names ? " names" : " slice") + (reversed ? " reversed" : ""), ++seed), cfs, read);
                }
            }
        }
    }

    /**
     * The reversed twin of {@link #readOfADroppedColumnWhoseCellsAreNewerThanTheDrop}.  BIG's
     * reverse reader builds each block's rows and filters them against the table's current
     * columns, so it drops the dropped column's cell whatever its timestamp; BTI's reverse reader
     * reads each row as the forward reader does and keeps it.  The cursor path once kept the cell
     * on a BIG reversed read.  The shape of fuzz case -6167232116629285161L.
     */
    @Test
    public void reversedReadOfADroppedColumnWhoseCellsAreNewerThanTheDrop()
    {
        for (String format : new String[]{ "big", "bti" })
        {
            // a dropped collection the table does not carry is not read on the cursor path
            for (String type : new String[]{ "vector<float, 2>", "int" })
            {
                for (boolean merged : new boolean[]{ false, true })
                {
                    DatabaseDescriptor.setSelectedSSTableFormat(format);
                    createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, x text, d " + type + ", PRIMARY KEY (pk, ck))");
                    ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                    cfs.disableAutoCompaction();
                    String value = type.equals("int") ? "7" : "[1.0, 2.0]";
                    execute("INSERT INTO %s (pk, ck, v, d) VALUES (0, 2, 'a', " + value + ") USING TIMESTAMP 1000037");
                    execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1, 'c') USING TIMESTAMP 1000038");
                    // a row with no liveness and only the dropped column: empty once the cell goes
                    execute("UPDATE %s USING TIMESTAMP 1000039 SET d = " + value + " WHERE pk = 0 AND ck = 4");
                    if (merged)
                    {
                        flush();
                        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 3, 'b') USING TIMESTAMP 1000040");
                    }
                    // the drop is older than the cells, so they survive a forward read
                    alterTable("ALTER TABLE %s DROP d USING TIMESTAMP 1000008");
                    flush();
                    String label = format + " " + type + (merged ? " merged" : "") + " reversed";
                    assertAllReadSurfacesMatch(ReadCase.of(label, merged ? 2 : 1), cfs,
                                               now -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).reverse().build());
                }
            }
        }
    }

    /** The topReadPartitionRowCount and topReadPartitionTombstoneCount samples of one read. */
    private static String sampledPartitions(ColumnFamilyStore cfs, LongFunction<SinglePartitionReadCommand> read,
                                            boolean replica, boolean cursor) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
        try
        {
            cfs.metric.topReadPartitionRowCount.beginSampling(10, 600_000);
            cfs.metric.topReadPartitionTombstoneCount.beginSampling(10, 600_000);
            readPartitions(read.apply(FBUtilities.nowInSeconds()), replica);
            Sampler.samplerExecutor.submit(() -> {}).get();
            return "rows " + describeSamples(cfs.metric.topReadPartitionRowCount.finishSampling(10))
                   + " tombstones " + describeSamples(cfs.metric.topReadPartitionTombstoneCount.finishSampling(10));
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static String describeSamples(List<Sampler.Sample<ByteBuffer>> samples)
    {
        List<String> described = new ArrayList<>();
        for (Sampler.Sample<ByteBuffer> sample : samples)
            described.add(ByteBufferUtil.bytesToHex(sample.value) + "=" + sample.count);
        Collections.sort(described);
        return described.toString();
    }

    /** The partitions {@code command} returns, through executeLocally or the replica response. */
    private static int readPartitions(SinglePartitionReadCommand command, boolean replica)
    {
        int partitions = 0;
        if (replica)
        {
            ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
            try (UnfilteredPartitionIterator iterator = response.makeIterator(command))
            {
                while (iterator.hasNext())
                {
                    iterator.next().close();
                    partitions++;
                }
            }
            return partitions;
        }
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
                partitions++;
            }
        }
        return partitions;
    }

    /** S1 and S2 for a slice read of {@code bounds} (inclusive start, end pairs) resumed after {@code lastReturned}. */
    private void assertResumedPastEnd(ReadCase c, ColumnFamilyStore cfs, boolean reversed, long lastReturned, long... bounds)
    {
        LongFunction<SinglePartitionReadCommand> resumed = now -> {
            SinglePartitionReadCommand base = CursorReadShapes.multiSlice(cfs, 1L, now, -1, reversed, bounds);
            SinglePartitionReadCommand next = base.forPaging(Clustering.make(ByteBufferUtil.bytes(lastReturned)), DataLimits.cqlLimits(5));
            assertTrue("the resumed read must have no slice left", next.clusteringIndexFilter().getSlices(cfs.metadata()).isEmpty());
            return next;
        };
        assertAllReadSurfacesMatch(c, cfs, resumed);
    }

    /** S1 and S2 for reads that end at TOP over a range deletion from {@code deleteFrom} to the partition end. */
    private void assertRangeDeletionToPartitionEndMatches(String label, ColumnFamilyStore cfs, long deleteFrom)
    {
        int seed = 0;
        assertAllReadSurfacesMatch(ReadCase.of(label + " full", ++seed), cfs,
                                   now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).build());
        for (long from : new long[]{ deleteFrom - 4, deleteFrom, deleteFrom + 2 })
        {
            assertAllReadSurfacesMatch(ReadCase.of(label + " from " + from, ++seed), cfs,
                                       now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                               .fromIncl(from).build());
            assertAllReadSurfacesMatch(ReadCase.of(label + " from " + from + " limit 2", ++seed), cfs,
                                       now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                               .fromIncl(from).withLimit(2).build());
            assertAllReadSurfacesMatch(ReadCase.of(label + " from " + from + " reversed", ++seed), cfs,
                                       now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                               .fromIncl(from).reverse().build());
        }
    }
}
