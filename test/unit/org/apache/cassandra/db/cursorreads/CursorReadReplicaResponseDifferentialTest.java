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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.aggregation.GroupingState;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The replica response surface (S2 of {@link CursorReadOracle}) plus S1 over the read shape corpus
 * ({@link CursorReadShapes}): slices, names, multi-slice, reversed, limits, column subsets.  The data
 * holds row, cell, range, partition and collection deletions, statics, collections and frozen
 * collections, TTLs, and counters, written as all-BIG, all-BTI and mixed BIG/BTI sstables, with and
 * without a memtable and a repaired sstable.  Each case reads once at the write time and once two days
 * later, when the TTL'd cells have expired; with {@code gc_grace_seconds = 0} the expired cells and the
 * tombstones are also purgeable then.  Row filters, row limits, per-partition limits and paging
 * limits run on every layout too, as do a lone sstable holding rows its own deletions shadow, the
 * memtable alone, and the top-partition samplers.
 */
public class CursorReadReplicaResponseDifferentialTest extends CursorReadOracle
{
    private static final String[] PROFILES = { "big", "bti", "mixed" };
    private static final long TWO_DAYS = 2 * 24 * 3600;
    private static final long[] INTERESTING = { 20, 25, 35, 44, 0, 10, 31, 59, 61 };

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

    private static void selectFormat(String profile, int layer)
    {
        String format = profile.equals("mixed") ? (layer % 2 == 0 ? "big" : "bti") : profile;
        DatabaseDescriptor.setSelectedSSTableFormat(format);
    }

    @Test
    public void regularTableLongGcGrace() throws Exception
    {
        for (String profile : PROFILES)
            runRegular(profile, 864000, false, false);
    }

    @Test
    public void regularTableZeroGcGrace() throws Exception
    {
        for (String profile : PROFILES)
            runRegular(profile, 0, false, false);
    }

    @Test
    public void regularTableWithMemtable() throws Exception
    {
        for (String profile : PROFILES)
            runRegular(profile, 0, true, false);
    }

    @Test
    public void regularTableWithRepairedSSTable() throws Exception
    {
        for (String profile : PROFILES)
            runRegular(profile, 864000, true, true);
    }

    /** A repaired sstable that survives the partition deletion check, so the tracking read digests it. */
    @Test
    public void repairedSSTableReadWithTracking() throws Exception
    {
        for (String profile : PROFILES)
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            selectFormat(profile, 0);
            for (long ck = 0; ck < 60; ck += 2)
                execute("INSERT INTO %s (pk, ck, s, v1, v2) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "s0", ck, "r-" + ck);
            execute("DELETE FROM %s USING TIMESTAMP 1000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 20L, 25L);
            flush();
            markRepaired(cfs);
            selectFormat(profile, 1);
            for (long ck = 1; ck < 60; ck += 2)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 2000", 1L, ck, ck, "u-" + ck);
            execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck = ?", 1L, 10L);
            flush();
            runShapes("repaired with tracking " + profile, cfs, new String[][]{ { "v2" } }, FBUtilities.nowInSeconds());
        }
    }

    /** With {@code gc_grace_seconds = 0}, a partition holding only tombstones purges to nothing. */
    @Test
    public void partitionThatPurgesToNothing() throws Exception
    {
        for (String profile : PROFILES)
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            long writeTime = FBUtilities.nowInSeconds();
            selectFormat(profile, 0);
            execute("DELETE FROM %s USING TIMESTAMP 1000 WHERE pk = ? AND ck = ?", 1L, 5L);
            execute("DELETE FROM %s USING TIMESTAMP 1000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 20L, 30L);
            flush();
            selectFormat(profile, 1);
            execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ?", 1L);
            execute("DELETE s FROM %s USING TIMESTAMP 2000 WHERE pk = ?", 2L);
            execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck = ?", 2L, 1L);
            flush();
            for (long pk = 1; pk <= 2; pk++)
            {
                long key = pk;
                runShapes("purges to nothing " + profile + " pk " + pk, cfs, new String[][]{ { "s" } }, writeTime, key);
                runShapes("purges to nothing " + profile + " pk " + pk + " +2 days", cfs, new String[][]{ { "s" } }, writeTime + TWO_DAYS, key);
            }
        }
    }

    @Test
    public void counterTable() throws Exception
    {
        for (String profile : PROFILES)
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int layer = 0; layer < 3; layer++)
            {
                selectFormat(profile, layer);
                for (long ck = layer; ck < 60; ck += 2)
                    execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = ? AND ck = ?", ck + layer, 1L, 1L, ck);
                if (layer == 1)
                    execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, 10L);
                flush();
            }
            for (long ck = 0; ck < 60; ck += 7)
                execute("UPDATE %s SET c1 = c1 + 1000 WHERE pk = ? AND ck = ?", 1L, ck);
            runShapes("counters " + profile, cfs, new String[][]{ { "c1" } }, FBUtilities.nowInSeconds());
        }
    }

    /**
     * Keys no sstable holds, inside the key range of 0, 1 and several sstables, with and without
     * memtable data for other keys; a key whose older sstables a newer partition deletion skips; and,
     * with the memtable, a key whose memtable partition deletion skips every sstable.
     */
    @Test
    public void absentAndSkippedPartitions() throws Exception
    {
        for (String profile : PROFILES)
        {
            runAbsentAndSkipped(profile, false);
            runAbsentAndSkipped(profile, true);
        }
    }

    private void runAbsentAndSkipped(String profile, boolean memtable) throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        AbsentKeyLayout layout = new AbsentKeyLayout(cfs.metadata().partitioner, 2000);
        long[][] layers = { layout.layer(200, 600, 20), layout.layer(400, 1200, 40), layout.layer(1000, 1100, 10) };
        List<Long> written = new ArrayList<>();
        for (int layer = 0; layer < layers.length; layer++)
        {
            selectFormat(profile, layer);
            for (long pk : layers[layer])
            {
                written.add(pk);
                execute("UPDATE %s USING TIMESTAMP " + (1000 + layer) + " SET s = ? WHERE pk = ?", "s" + layer, pk);
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP " + (1000 + layer), pk, ck, ck, "l" + layer);
            }
            flush();
        }
        long deletedInNewestSSTable = layers[0][0];
        selectFormat(profile, 3);
        execute("DELETE FROM %s USING TIMESTAMP 5000 WHERE pk = ?", deletedInNewestSSTable);
        flush();
        long deletedInMemtable = layers[1][0];
        if (memtable)
        {
            for (int rank : new int[]{ 150, 700, 1600 })
            {
                written.add(layout.at(rank));
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 6000", layout.at(rank), 1L, 1L, "m");
            }
            execute("DELETE FROM %s USING TIMESTAMP 6000 WHERE pk = ?", deletedInMemtable);
        }

        Map<String, Long> keys = layout.absentKeys(cfs, written.stream().mapToLong(Long::longValue).toArray());
        for (int covering = 0; covering <= 2; covering++)
            assertTrue("no absent key in " + covering + " sstable range(s): " + keys,
                       keys.containsKey("absent key in " + covering + " sstable range(s)"));
        keys.put("partition deleted in the newest sstable", deletedInNewestSSTable);
        if (memtable)
            keys.put("partition deleted in the memtable", deletedInMemtable);
        long now = FBUtilities.nowInSeconds();
        for (Map.Entry<String, Long> key : keys.entrySet())
            runShapes(profile + (memtable ? " memtable " : " ") + key.getKey(), cfs, new String[][]{ { "v2" }, { "s" } }, now, key.getValue());
    }

    private void runRegular(String profile, int gcGrace, boolean memtable, boolean repaired) throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, s2 int static, v1 bigint, v2 text, " +
                    "l list<int>, st set<text>, m map<int, text>, f frozen<list<int>>, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = " + gcGrace);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        long writeTime = FBUtilities.nowInSeconds();

        // layer 0: even rows, statics, collections
        selectFormat(profile, 0);
        for (long ck = 0; ck < 60; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2, l, st, m, f) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP 1000",
                    1L, ck, ck, "l0-" + ck, List.of(1, 2, 3), Set.of("a", "b"), Map.of(1, "one", 2, "two"), List.of((int) ck));
        execute("UPDATE %s USING TIMESTAMP 1000 SET s = 's0', s2 = 7 WHERE pk = ?", 1L);
        flush();
        if (repaired)
            markRepaired(cfs);

        // layer 1: odd rows, row / range / cell / element deletions, complex overwrite, TTLs
        selectFormat(profile, 1);
        for (long ck = 1; ck < 60; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 2000", 1L, ck, ck * 10, "l1-" + ck);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck = ?", 1L, 10L);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 20L, 25L);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck > ? AND ck <= ?", 1L, 30L, 35L);
        execute("DELETE v1 FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck = ?", 1L, 40L);
        execute("UPDATE %s USING TIMESTAMP 2000 SET st = st - {'a'} WHERE pk = ? AND ck = ?", 1L, 42L);
        execute("DELETE m[1] FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck = ?", 1L, 44L);
        execute("UPDATE %s USING TIMESTAMP 2000 SET l = [9, 9] WHERE pk = ? AND ck = ?", 1L, 46L);
        for (long ck = 50; ck < 55; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 AND TTL 3600 SET v2 = ? WHERE pk = ? AND ck = ?", "ttl-" + ck, 1L, ck);
        execute("UPDATE %s USING TIMESTAMP 2000 AND TTL 3600 SET s2 = 8 WHERE pk = ?", 1L);
        flush();

        // layer 2: an older partition deletion (shadows layer 0 only), a range across layers, new rows
        selectFormat(profile, 2);
        execute("DELETE FROM %s USING TIMESTAMP 1500 WHERE pk = ?", 1L);
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 26L, 31L);
        for (long ck = 0; ck < 60; ck += 5)
            execute("UPDATE %s USING TIMESTAMP 3000 SET v2 = ? WHERE pk = ? AND ck = ?", "l2-" + ck, 1L, ck);
        execute("DELETE s FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1L);
        flush();

        if (memtable)
        {
            for (long ck = 3; ck < 60; ck += 9)
                execute("UPDATE %s USING TIMESTAMP 4000 SET v1 = ? WHERE pk = ? AND ck = ?", -ck, 1L, ck);
            execute("DELETE FROM %s USING TIMESTAMP 4000 WHERE pk = ? AND ck = ?", 1L, 57L);
            execute("UPDATE %s USING TIMESTAMP 4000 SET s = 'memtable' WHERE pk = ?", 1L);
        }

        String label = "regular " + profile + " gc_grace=" + gcGrace + (memtable ? " memtable" : "") + (repaired ? " repaired" : "");
        String[][] columns = { { "s" }, { "v2" }, { "l", "m" }, { "f", "s2" } };
        runShapes(label, cfs, columns, writeTime);
        runShapes(label + " +2 days", cfs, columns, writeTime + TWO_DAYS);
        runFilterAndLimitShapes(label, cfs, writeTime);
        runFilterAndLimitShapes(label + " +2 days", cfs, writeTime + TWO_DAYS);
    }

    /** A GROUP BY page that resumes in another partition with its group limit already reached
     *  when this partition starts: the transcode path declines it. */
    private static final String LIMIT_REACHED_AT_PARTITION_START = "group by ck page 1 row resume in other partition";

    /** The limits a filtered or plain read is run with: a row limit, a per-partition limit, both, and
     *  paging limits, fresh and resuming after a clustering. */
    private static final String[] LIMITS = { "none", "limit 1", "limit 4", "per partition 2", "limit 5 per partition 3",
                                             "page 3", "page 3 resume after 20", "page 2 resume after 27",
                                             "group by ck 3 groups", "group by ck page 4 rows", "group by ck 2 groups page 5 rows",
                                             "group by ck page 4 rows resume after 20", "group by ck page 3 rows resume in other partition",
                                             "group by pk page 6 rows", LIMIT_REACHED_AT_PARTITION_START };

    private static ReadCase limitCase(String label, String limit, long seed, long nowInSec)
    {
        ReadCase c = ReadCase.of(label, seed).at(nowInSec);
        return limit.equals(LIMIT_REACHED_AT_PARTITION_START) ? c.expectTranscode(false) : c;
    }

    /**
     * Row filters, evaluated on the streamed cells (clustering and simple regular columns) or on built
     * rows (collection columns), the partition-level filters on static columns, each with every
     * {@link #LIMITS} entry.
     */
    private void runFilterAndLimitShapes(String label, ColumnFamilyStore cfs, long nowInSec)
    {
        Map<String, Function<AbstractReadCommandBuilder, AbstractReadCommandBuilder>> filters = new LinkedHashMap<>();
        filters.put("no filter", b -> b);
        addFilter(filters, cfs, "v1 > 20", b -> b.filterOn("v1", Operator.GT, 20L), "v1");
        addFilter(filters, cfs, "v1 <= 330", b -> b.filterOn("v1", Operator.LTE, 330L), "v1");
        addFilter(filters, cfs, "ck < 30", b -> b.filterOn("ck", Operator.LT, 30L), "ck");
        addFilter(filters, cfs, "ck > 4 and v2 = l1-31", b -> b.filterOn("ck", Operator.GT, 4L).filterOn("v2", Operator.EQ, "l1-31"), "ck", "v2");
        addFilter(filters, cfs, "v2 > l2", b -> b.filterOn("v2", Operator.GT, "l2"), "v2");
        addFilter(filters, cfs, "s = s0", b -> b.filterOn("s", Operator.EQ, "s0"), "s");
        addFilter(filters, cfs, "s = memtable", b -> b.filterOn("s", Operator.EQ, "memtable"), "s");
        addFilter(filters, cfs, "s2 = 7", b -> b.filterOn("s2", Operator.EQ, 7), "s2");
        addFilter(filters, cfs, "st contains b", b -> b.filterOn("st", Operator.CONTAINS, "b"), "st");
        addFilter(filters, cfs, "m contains key 2", b -> b.filterOn("m", Operator.CONTAINS_KEY, 2), "m");
        addFilter(filters, cfs, "l contains 9", b -> b.filterOn("l", Operator.CONTAINS, 9), "l");
        addFilter(filters, cfs, "f contains 40", b -> b.filterOn("f", Operator.CONTAINS, 40), "f");
        addFilter(filters, cfs, "v1 > 20 and st contains a", b -> b.filterOn("v1", Operator.GT, 20L).filterOn("st", Operator.CONTAINS, "a"), "v1", "st");
        addFilter(filters, cfs, "ck >= 20 slice [10,50]", b -> b.fromIncl(10L).toIncl(50L).filterOn("ck", Operator.GTE, 20L), "ck");
        long seed = label.hashCode() * 31L;
        for (Map.Entry<String, Function<AbstractReadCommandBuilder, AbstractReadCommandBuilder>> filter : filters.entrySet())
        {
            for (String limit : LIMITS)
            {
                LongFunction<SinglePartitionReadCommand> shape = now -> {
                    SinglePartitionReadCommand base = (SinglePartitionReadCommand) filter.getValue().apply(Util.cmd(cfs, 1L).withNowInSeconds(now)).build();
                    return withLimit(base, limit);
                };
                assertAllReadSurfacesMatch(limitCase(label + " / " + filter.getKey() + " / " + limit, limit, seed++, nowInSec), cfs, shape);
            }
        }
    }

    /** Adds a filter shape when the table has every column it filters on. */
    private static void addFilter(Map<String, Function<AbstractReadCommandBuilder, AbstractReadCommandBuilder>> filters,
                                  ColumnFamilyStore cfs, String name,
                                  Function<AbstractReadCommandBuilder, AbstractReadCommandBuilder> filter, String... columns)
    {
        for (String column : columns)
        {
            if (cfs.metadata().getColumn(ByteBufferUtil.bytes(column)) == null)
                return;
        }
        filters.put(name, filter);
    }

    private static SinglePartitionReadCommand withLimit(SinglePartitionReadCommand base, String limit)
    {
        switch (limit)
        {
            case "none":
                return base;
            case "limit 1":
                return limited(base, DataLimits.cqlLimits(1));
            case "limit 4":
                return limited(base, DataLimits.cqlLimits(4));
            case "per partition 2":
                return limited(base, DataLimits.cqlLimits(DataLimits.NO_LIMIT, 2));
            case "limit 5 per partition 3":
                return limited(base, DataLimits.cqlLimits(5, 3));
            case "page 3":
                return limited(base, DataLimits.NONE.forPaging(3));
            case "page 3 resume after 20":
                return base.forPaging(Clustering.make(ByteBufferUtil.bytes(20L)),
                                      DataLimits.NONE.forPaging(3, base.partitionKey().getKey(), 7));
            case "page 2 resume after 27":
                return base.forPaging(Clustering.make(ByteBufferUtil.bytes(27L)),
                                      DataLimits.cqlLimits(10).forPaging(2, base.partitionKey().getKey(), 10));
            case "group by ck 3 groups":
                return limited(base, DataLimits.groupByLimits(3, DataLimits.NO_LIMIT, DataLimits.NO_LIMIT, groupBy(base, 1)));
            case "group by ck page 4 rows":
                return limited(base, DataLimits.groupByLimits(DataLimits.NO_LIMIT, DataLimits.NO_LIMIT, 4, groupBy(base, 1))
                                               .forGroupByInternalPaging(GroupingState.EMPTY_STATE));
            case "group by ck 2 groups page 5 rows":
                return limited(base, DataLimits.groupByLimits(2, DataLimits.NO_LIMIT, 5, groupBy(base, 1)));
            case "group by ck page 4 rows resume after 20":
            {
                Clustering<?> last = Clustering.make(ByteBufferUtil.bytes(20L));
                return base.forPaging(last, DataLimits.groupByLimits(DataLimits.NO_LIMIT, DataLimits.NO_LIMIT, 4, groupBy(base, 1))
                                                      .forGroupByInternalPaging(new GroupingState(base.partitionKey().getKey(), last)));
            }
            case "group by ck page 3 rows resume in other partition":
                return limited(base, DataLimits.groupByLimits(DataLimits.NO_LIMIT, DataLimits.NO_LIMIT, 3, groupBy(base, 1))
                                               .forGroupByInternalPaging(new GroupingState(ByteBufferUtil.bytes(999L),
                                                                                           Clustering.make(ByteBufferUtil.bytes(5L)))));
            case LIMIT_REACHED_AT_PARTITION_START:
                return limited(base, DataLimits.groupByLimits(DataLimits.NO_LIMIT, DataLimits.NO_LIMIT, 1, groupBy(base, 1))
                                               .forGroupByInternalPaging(new GroupingState(ByteBufferUtil.bytes(999L),
                                                                                           Clustering.make(ByteBufferUtil.bytes(5L)))));
            case "group by pk page 6 rows":
                return limited(base, DataLimits.groupByLimits(DataLimits.NO_LIMIT, DataLimits.NO_LIMIT, 6, groupBy(base, 0))
                                               .forGroupByInternalPaging(GroupingState.EMPTY_STATE));
            default:
                throw new AssertionError(limit);
        }
    }

    /** GROUP BY the partition key and the first {@code clusteringColumns} clustering columns. */
    private static AggregationSpecification groupBy(SinglePartitionReadCommand base, int clusteringColumns)
    {
        return AggregationSpecification.aggregatePkPrefixFactory(base.metadata().comparator, clusteringColumns)
                                       .newInstance(QueryOptions.DEFAULT);
    }

    private static SinglePartitionReadCommand limited(SinglePartitionReadCommand base, DataLimits limits)
    {
        return SinglePartitionReadCommand.create(base.metadata(), base.nowInSec(), base.columnFilter(), base.rowFilter(),
                                                 limits, base.partitionKey(), base.clusteringIndexFilter());
    }

    /**
     * One sstable holding rows shadowed by its own partition deletion, range tombstone, row deletions
     * and complex deletions, as a writer that does not reconcile its input stores them.  The iterator
     * path reads a lone sstable as stored, without merging it, so the response must carry those rows.
     */
    @Test
    public void singleSSTableHoldingRowsItsOwnDeletionsShadow() throws Exception
    {
        for (String profile : new String[]{ "big", "bti" })
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, v2 text, st set<text>, " +
                        "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            selectFormat(profile, 0);
            long now = FBUtilities.nowInSeconds();
            TableMetadata metadata = cfs.metadata();
            ColumnMetadata v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
            ColumnMetadata v2 = metadata.getColumn(ByteBufferUtil.bytes("v2"));
            ColumnMetadata st = metadata.getColumn(ByteBufferUtil.bytes("st"));
            ColumnMetadata s = metadata.getColumn(ByteBufferUtil.bytes("s"));
            DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1L));
            PartitionUpdate.Builder update = new PartitionUpdate.Builder(metadata, key, metadata.regularAndStaticColumns(), 64);
            update.addPartitionDeletion(DeletionTime.build(1500, now));
            update.add(new RangeTombstone(Slice.make(ClusteringBound.inclusiveStartOf(Clustering.make(ByteBufferUtil.bytes(20L))),
                                                     ClusteringBound.exclusiveEndOf(Clustering.make(ByteBufferUtil.bytes(30L)))),
                                          DeletionTime.build(2000, now)));
            Row.Builder staticRow = BTreeRow.sortedBuilder();
            staticRow.newRow(Clustering.STATIC_CLUSTERING);
            staticRow.addCell(BufferCell.live(s, 1000, ByteBufferUtil.bytes("old static")));
            update.add(staticRow.build());
            for (long ck = 0; ck < 40; ck++)
            {
                long ts = ck % 3 == 0 ? 3000 : 1000;
                Row.Builder row = BTreeRow.sortedBuilder();
                row.newRow(Clustering.make(ByteBufferUtil.bytes(ck)));
                row.addPrimaryKeyLivenessInfo(LivenessInfo.create(ts));
                if (ck % 7 == 0)
                    row.addRowDeletion(Row.Deletion.regular(DeletionTime.build(2500, now)));
                row.addCell(BufferCell.live(v1, ts, ByteBufferUtil.bytes(ck)));
                row.addCell(BufferCell.live(v2, ck % 5 == 0 ? 1200 : ts, ByteBufferUtil.bytes("v-" + ck)));
                if (ck % 4 == 0)
                    row.addComplexDeletion(st, DeletionTime.build(1800, now));
                row.addCell(BufferCell.live(st, 1100, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(ByteBufferUtil.bytes("a" + ck))));
                update.add(row.build());
            }
            writeSSTable(cfs, update.build());
            assertEquals(1, cfs.getLiveSSTables().size());
            runShapes("own deletions shadow " + profile, cfs, new String[][]{ { "v1" }, { "st", "s" } }, now);
            runFilterAndLimitShapes("own deletions shadow " + profile, cfs, now);
        }
    }

    private static void writeSSTable(ColumnFamilyStore cfs, PartitionUpdate update)
    {
        Descriptor desc = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables());
        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, desc, 1, 0, null, false,
                                                               new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(),
                                                                                       EncodingStats.NO_STATS)))
        {
            writer.append(update.unfilteredIterator());
            cfs.addSSTables(writer.finish(true));
        }
    }

    /** The memtable alone: one partition with rows, one holding only a static row, one only deleted. */
    @Test
    public void memtableOnly() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, v2 text, m map<int, text>, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        long writeTime = FBUtilities.nowInSeconds();
        for (long ck = 0; ck < 60; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2, m) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 1000", 1L, ck, ck, "v-" + ck, Map.of(1, "a", 2, "b"));
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 20L, 25L);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck = ?", 1L, 31L);
        execute("UPDATE %s USING TIMESTAMP 1000 AND TTL 3600 SET v2 = 'ttl' WHERE pk = ? AND ck = ?", 1L, 44L);
        execute("UPDATE %s USING TIMESTAMP 1000 SET s = 'static only' WHERE pk = ?", 2L);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ?", 3L);
        assertEquals(0, cfs.getLiveSSTables().size());
        String[][] columns = { { "s" }, { "v1" }, { "m" } };
        for (long pk = 1; pk <= 3; pk++)
        {
            runShapes("memtable only pk " + pk, cfs, columns, writeTime, pk);
            runShapes("memtable only pk " + pk + " +2 days", cfs, columns, writeTime + TWO_DAYS, pk);
        }
        runFilterAndLimitShapesOnMemtable(cfs, writeTime);
    }

    private void runFilterAndLimitShapesOnMemtable(ColumnFamilyStore cfs, long nowInSec)
    {
        long seed = 77;
        for (String limit : LIMITS)
        {
            assertAllReadSurfacesMatch(limitCase("memtable only v1 > 10 / " + limit, limit, seed++, nowInSec), cfs,
                                       now -> withLimit((SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now)
                                                                                         .filterOn("v1", Operator.GT, 10L).build(), limit));
            assertAllReadSurfacesMatch(limitCase("memtable only / " + limit, limit, seed++, nowInSec), cfs,
                                       now -> withLimit((SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(now).build(), limit));
        }
    }

    /** The top-partition samplers record the same samples on both paths' replica data reads. */
    @Test
    public void topPartitionSamplers() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int layer = 0; layer < 3; layer++)
        {
            selectFormat("mixed", layer);
            for (long ck = layer * 10; ck < layer * 10 + 15; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP " + (1000 + layer), 1L, ck, ck);
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 2L + layer, 1L, 1L);
            flush();
        }
        execute("UPDATE %s SET s = 'static' WHERE pk = ?", 3L);
        execute("DELETE FROM %s WHERE pk = ?", 9L);
        List<SinglePartitionReadCommand> reads = new ArrayList<>();
        for (long pk = 1; pk <= 9; pk++)
        {
            reads.add((SinglePartitionReadCommand) Util.cmd(cfs, pk).build());
            reads.add((SinglePartitionReadCommand) Util.cmd(cfs, pk).withLimit(2).build());
            reads.add((SinglePartitionReadCommand) Util.cmd(cfs, pk).fromIncl(12L).toIncl(14L).build());
        }
        String iterator = sampleReplicaReads(cfs, reads, false);
        String cursor = sampleReplicaReads(cfs, reads, true);
        assertEquals(iterator, cursor);
    }

    private static String sampleReplicaReads(ColumnFamilyStore cfs, List<SinglePartitionReadCommand> reads, boolean cursor) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
        try
        {
            cfs.metric.topReadPartitionFrequency.beginSampling(100, 600_000);
            cfs.metric.topReadPartitionSSTableCount.beginSampling(100, 600_000);
            for (SinglePartitionReadCommand read : reads)
                ReadCommandVerbHandler.instance.doRead(read, false);
            Sampler.samplerExecutor.submit(() -> {}).get();
            return "frequency " + describe(cfs.metric.topReadPartitionFrequency.finishSampling(100))
                   + " sstables " + describe(cfs.metric.topReadPartitionSSTableCount.finishSampling(100));
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static String describe(List<Sampler.Sample<ByteBuffer>> samples)
    {
        List<String> described = new ArrayList<>();
        for (Sampler.Sample<ByteBuffer> sample : samples)
            described.add(ByteBufferUtil.bytesToHex(sample.value) + "=" + sample.count);
        Collections.sort(described);
        return described.toString();
    }

    private void runShapes(String label, ColumnFamilyStore cfs, String[][] columns, long nowInSec)
    {
        runShapes(label, cfs, columns, nowInSec, 1L);
    }

    private void runShapes(String label, ColumnFamilyStore cfs, String[][] columns, long nowInSec, long pk)
    {
        Map<String, LongFunction<SinglePartitionReadCommand>> shapes = CursorReadShapes.forTable(cfs, pk, INTERESTING, columns);
        long seed = label.hashCode();
        for (Map.Entry<String, LongFunction<SinglePartitionReadCommand>> shape : shapes.entrySet())
        {
            ReadCase c = ReadCase.of(label + " / " + shape.getKey(), seed++).at(nowInSec);
            assertAllReadSurfacesMatch(c, cfs, shape.getValue());
        }
    }

    private static void markRepaired(ColumnFamilyStore cfs) throws Exception
    {
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        for (SSTableReader sstable : sstables)
        {
            sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, FBUtilities.nowInSeconds(), null, false);
            sstable.reloadSSTableMetadata();
        }
    }
}
