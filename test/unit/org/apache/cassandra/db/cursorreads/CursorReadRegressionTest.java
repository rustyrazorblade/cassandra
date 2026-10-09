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

import java.util.Arrays;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Named repros of cursor read bugs fixed on this branch, run through every {@link CursorReadOracle}
 * read surface:
 * <ul>
 *   <li>reverse reads over a row-indexed partition, whose tail block can start on the
 *       end-of-partition marker (the cursor once threw in {@code gotoBlock}'s counterpart);</li>
 *   <li>a merge of a BIG leg and a BTI leg where a key-cache lower bound makes the merge open a
 *       deferred leg past its header ("Leg in an unexpected state before unfiltered sort: 8");</li>
 *   <li>a names read of a row next to a range tombstone whose exclusive end does not reach it (the
 *       shape of seed -4715507501933830118L in {@link RandomCursorReadDifferentialTest}).</li>
 * </ul>
 */
public class CursorReadRegressionTest extends CursorReadOracle
{
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

    /** A BIG leg opened early because its key-cache lower bound spans the slice start, merged with a BTI leg. */
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

        long forceOpenedBefore = CursorReads.sstableLegsForceOpened();
        LongFunction<SinglePartitionReadCommand> fromZero =
            now -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(0L).toIncl(2000L).build();
        assertExecuteLocallyMatches(ReadCase.of("BIG and BTI with key cache", 6), cfs, fromZero);
        assertTrue("no BIG leg was opened early; the repro no longer reaches the force-open path",
                   CursorReads.sstableLegsForceOpened() > forceOpenedBefore);
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
}
