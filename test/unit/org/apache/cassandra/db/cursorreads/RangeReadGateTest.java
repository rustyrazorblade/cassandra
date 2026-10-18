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

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTreeSet;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Gate test for the cursor range read: the range-aware
 * support gate {@link CursorRangeReads#unsupported}.
 *
 * <p>Asserts the gate ACCEPTS a plain single-slice forward partition-range read on a BTI table with
 * at-latest-version sstables.  It also ACCEPTS the shapes the range driver now serves: a multi-slice
 * filter, a names filter, a counter table, a memtable-only range (zero candidate sstables), and a
 * reversed read (finished through the lazy reverse legs and the shared object merge).  It still
 * REJECTS a materialized view, and ACCEPTS a row-cache-enabled table.  It ACCEPTS both a
 * forward and a reversed paging range (the driver and memtable iterator apply the per-key clustering
 * filter, and the paged start filter keeps the template's direction).
 *
 * <p>This exercises the predicate and its counts only.
 */
public class RangeReadGateTest extends CQLTester
{
    private SSTableFormat<?, ?> originalFormat;

    @BeforeClass
    public static void startup() throws Exception
    {
        // the materialized-view scenario builds a real view, which needs the view-build machinery
        // (and thus the network stack) running, matching ViewAbstractTest
        requireNetwork();
    }

    @Before
    public void selectBtiAndEnableCursorReads()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        DatabaseDescriptor.setCursorRangeReadsEnabled(true);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setCursorRangeReadsEnabled(false);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    // ------------------------------------------------------------------ helpers

    private ColumnFamilyStore prepareSimpleTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 8; pk++)
            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, pk * 10 + ck, "v" + pk + '-' + ck);
        flush();
        assertTrue("scenario needs at least one sstable", cfs.getLiveSSTables().size() >= 1);
        return cfs;
    }

    private static PartitionRangeReadCommand rangeCommand(ColumnFamilyStore cfs, DataRange dataRange)
    {
        TableMetadata metadata = cfs.metadata();
        return PartitionRangeReadCommand.create(metadata,
                                                FBUtilities.nowInSeconds(),
                                                ColumnFilter.all(metadata),
                                                RowFilter.none(),
                                                DataLimits.NONE,
                                                dataRange);
    }

    /** A paging DataRange resuming inside {@code lastReturnedKey} after {@code ck=lastReturnedCk}, as
     *  PartitionRangeQueryPager builds one for a mid-partition resume; {@code reversed} sets the
     *  template's order so the reversed-paging rejection can be exercised. */
    private static DataRange midPartitionPaging(ColumnFamilyStore cfs, DecoratedKey lastReturnedKey,
                                                long lastReturnedCk, boolean reversed)
    {
        DataRange full = DataRange.allData(cfs.metadata().partitioner, new ClusteringIndexSliceFilter(Slices.ALL, reversed));
        AbstractBounds<PartitionPosition> bounds = new Bounds<>(lastReturnedKey, full.keyRange().right);
        Clustering<?> lastReturned = Clustering.make(org.apache.cassandra.utils.ByteBufferUtil.bytes(lastReturnedCk));
        return full.forPaging(bounds, cfs.metadata().comparator, lastReturned, false);
    }

    private static List<SSTableReader> sstablesFor(ColumnFamilyStore cfs, PartitionRangeReadCommand command)
    {
        return cfs.select(View.selectLive(command.dataRange().keyRange())).sstables;
    }

    private static ColumnFamilyStore.ViewFragment viewFor(ColumnFamilyStore cfs, PartitionRangeReadCommand command)
    {
        return cfs.select(View.selectLive(command.dataRange().keyRange()));
    }

    private static boolean gateAccepts(ColumnFamilyStore cfs, PartitionRangeReadCommand command)
    {
        return CursorRangeReads.unsupported(command, cfs, ReadExecutionController.empty(), viewFor(cfs, command)) == null;
    }

    // ------------------------------------------------------------------ accept

    @Test
    public void plainForwardRangeReadIsSupported() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        IPartitioner partitioner = cfs.metadata().partitioner;
        PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(partitioner));

        List<SSTableReader> sstables = sstablesFor(cfs, command);
        assertFalse("scenario precondition: at least one candidate sstable", sstables.isEmpty());
        assertTrue("a plain single-slice forward range read on a BTI table must be cursor-supported",
                   CursorRangeReads.unsupported(command, cfs, ReadExecutionController.empty(), viewFor(cfs, command)) == null);
    }

    @Test
    public void forwardPagingRangeReadIsSupported() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        DecoratedKey startKey = cfs.metadata().partitioner.decorateKey(org.apache.cassandra.utils.ByteBufferUtil.bytes(3L));
        DataRange paging = midPartitionPaging(cfs, startKey, 1L, false);
        assertTrue("scenario precondition: the data range must be paging", paging.isPaging());

        PartitionRangeReadCommand command = rangeCommand(cfs, paging);
        assertFalse("scenario precondition: at least one candidate sstable", sstablesFor(cfs, command).isEmpty());
        assertTrue("a forward single-slice paging range must be cursor-supported (the driver applies the "
                   + "per-key clustering filter, and forward single-slice paging stays single-slice)",
                   gateAccepts(cfs, command));
    }

    @Test
    public void reversedPagingRangeReadIsSupported() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        DecoratedKey startKey = cfs.metadata().partitioner.decorateKey(org.apache.cassandra.utils.ByteBufferUtil.bytes(3L));
        DataRange paging = midPartitionPaging(cfs, startKey, 1L, true);
        assertTrue("scenario precondition: the data range must be paging", paging.isPaging());
        assertTrue("scenario precondition: the data range must be reversed", paging.isReversed());

        PartitionRangeReadCommand command = rangeCommand(cfs, paging);
        assertTrue("a reversed paging range is cursor-supported (lazy reverse legs in the object merge)",
                   gateAccepts(cfs, command));
    }

    @Test
    public void switchOffLeavesTheGateUnconsulted() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner));

        DatabaseDescriptor.setCursorRangeReadsEnabled(false);
        long served = CursorRangeReads.rangeReadsServed();
        long notServed = CursorRangeReads.rangeReadsNotServed();
        readAll(command);
        assertEquals("with the switch off no range read is served", served, CursorRangeReads.rangeReadsServed());
        assertEquals("with the switch off the gate counts nothing", notServed, CursorRangeReads.rangeReadsNotServed());
    }

    @Test
    public void servedReadIsCounted() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner));
        long served = CursorRangeReads.rangeReadsServed();
        long notServed = CursorRangeReads.rangeReadsNotServed();
        readAll(command);
        assertEquals(served + 1, CursorRangeReads.rangeReadsServed());
        assertEquals(notServed, CursorRangeReads.rangeReadsNotServed());
    }

    @Test
    public void readTrackingRepairedDataIsServed() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner));
        long notServed = CursorRangeReads.rangeReadsNotServed();
        long served = CursorRangeReads.rangeReadsServed();
        ReadCommandVerbHandler.instance.doRead(command, true);
        assertEquals(served + 1, CursorRangeReads.rangeReadsServed());
        assertEquals(notServed, CursorRangeReads.rangeReadsNotServed());
    }

    private static void readAll(PartitionRangeReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
        }
    }

    // ------------------------------------------------------------------ reject

    @Test
    public void reversedRangeReadIsSupported() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        IPartitioner partitioner = cfs.metadata().partitioner;
        DataRange reversed = DataRange.allData(partitioner, new ClusteringIndexSliceFilter(Slices.ALL, true));
        PartitionRangeReadCommand command = rangeCommand(cfs, reversed);
        assertTrue("scenario precondition: the data range must be reversed", command.dataRange().isReversed());
        assertTrue("a reversed range read is cursor-supported (lazy reverse legs in the object merge)",
                   gateAccepts(cfs, command));
    }

    @Test
    public void namesFilterRangeReadIsSupported() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        IPartitioner partitioner = cfs.metadata().partitioner;
        BTreeSet.Builder<org.apache.cassandra.db.Clustering<?>> names = BTreeSet.builder(cfs.metadata().comparator);
        names.add(org.apache.cassandra.db.Clustering.make(org.apache.cassandra.db.marshal.LongType.instance.decompose(1L)));
        names.add(org.apache.cassandra.db.Clustering.make(org.apache.cassandra.db.marshal.LongType.instance.decompose(3L)));
        ClusteringIndexNamesFilter namesFilter = new ClusteringIndexNamesFilter(names.build(), false);
        PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(partitioner, namesFilter));
        assertTrue("scenario precondition: the data range must be a names query", command.dataRange().isNamesQuery());
        assertTrue("a names-filter range read is cursor-supported (point slices through the merge core)",
                   gateAccepts(cfs, command));
    }

    @Test
    public void multiSliceRangeReadIsSupported() throws Throwable
    {
        ColumnFamilyStore cfs = prepareSimpleTable();
        IPartitioner partitioner = cfs.metadata().partitioner;

        Slices.Builder builder = new Slices.Builder(cfs.metadata().comparator);
        builder.add(ClusteringBound.create(cfs.metadata().comparator, true, true, 1L),
                    ClusteringBound.create(cfs.metadata().comparator, false, true, 3L));
        builder.add(ClusteringBound.create(cfs.metadata().comparator, true, true, 6L),
                    ClusteringBound.create(cfs.metadata().comparator, false, true, 8L));
        Slices slices = builder.build();
        assertEquals("scenario must actually be multi-slice", 2, slices.size());

        DataRange multiSlice = DataRange.allData(partitioner, new ClusteringIndexSliceFilter(slices, false));
        PartitionRangeReadCommand command = rangeCommand(cfs, multiSlice);
        assertTrue("a multi-slice range read is cursor-supported (the merge core threads Slices per key)",
                   gateAccepts(cfs, command));
    }

    @Test
    public void counterTableRangeReadIsSupported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 3; ck++)
                execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", ck + 1, pk, ck);
        flush();

        PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner));
        assertTrue("a counter table is cursor-supported (the merge core reconciles counter shards per key)",
                   gateAccepts(cfs, command));
    }

    @Test
    public void materializedViewRangeReadFallsBack() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck))");
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v IS NOT NULL " +
                   "PRIMARY KEY (v, pk, ck)");
        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, pk * 10 + ck);
        flush();

        ColumnFamilyStore viewCfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentView());
        viewCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertTrue("scenario precondition: the queried table must be a view", viewCfs.metadata().isView());

        PartitionRangeReadCommand command = rangeCommand(viewCfs, DataRange.allData(viewCfs.metadata().partitioner));
        assertFalse("a materialized view must fall back", gateAccepts(viewCfs, command));
    }

    @Test
    public void rowCacheTableRangeReadIsSupported() throws Throwable
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck)) " +
                        "WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (long pk = 0; pk < 4; pk++)
                for (long ck = 0; ck < 3; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, pk * 10 + ck);
            flush();
            assertTrue("scenario precondition: row cache must be enabled for this table", cfs.isRowCacheEnabled());

            PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner));
            assertTrue("a row-cache-enabled table is served", gateAccepts(cfs, command));
        }
        finally
        {
            CacheService.instance.setRowCacheCapacityInMB(0);
        }
    }

    @Test
    public void emptySSTableSetIsSupported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 4; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, pk);
        // no flush: the range lives entirely in the memtable, served via the per-partition object path

        PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner));
        assertTrue("scenario precondition: there must be no candidate sstables", sstablesFor(cfs, command).isEmpty());
        assertTrue("a memtable-only range read is cursor-supported (zero scouts, memtable object path)",
                   gateAccepts(cfs, command));
    }

    // ------------------------------------------------------------------ no logging

    /** A secondary index does not change what a plain scan of the table reads, so the read is served. */
    @Test
    public void tableWithSaiIndexIsSupported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s (v1) USING 'StorageAttachedIndex'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, 1L, 1L);
        flush();
        assertTrue(gateAccepts(cfs, rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner))));
    }

    @Test
    public void tableWithLegacyIndexIsSupported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s (v1) USING 'legacy_local_table'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, 1L, 1L);
        flush();
        assertTrue(gateAccepts(cfs, rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner))));
    }

    /**
     * An sstable that still carries a dropped collection column is rejected for each range read too, so
     * that rejection must not log either.
     */
    @Test
    public void droppedCollectionRangeReadGateDoesNotLog() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, s set<int>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v1, s) VALUES (?, ?, ?, ?)", 1L, 1L, 1L, set(1));
        flush();
        execute("ALTER TABLE %s DROP s");
        assertRangeGateRejectsWithoutLogging(cfs);
    }

    private static void assertRangeGateRejectsWithoutLogging(ColumnFamilyStore cfs)
    {
        Logger logger = (Logger) LoggerFactory.getLogger(CursorCompactor.class.getName());
        Level previousLevel = logger.getLevel();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);
        try
        {
            PartitionRangeReadCommand command = rangeCommand(cfs, DataRange.allData(cfs.metadata().partitioner));
            assertFalse(gateAccepts(cfs, command));
            assertEquals("the range read gate must not log", 0, appender.list.size());
        }
        finally
        {
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
        }
    }
}
