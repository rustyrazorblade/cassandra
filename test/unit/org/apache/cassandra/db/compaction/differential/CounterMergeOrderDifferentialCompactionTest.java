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

package org.apache.cassandra.db.compaction.differential;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.compaction.differential.CounterDifferentialCompactionTest.ShardSpec;
import static org.apache.cassandra.db.compaction.differential.CounterDifferentialCompactionTest.applyCounterCell;
import static org.apache.cassandra.db.compaction.differential.CounterDifferentialCompactionTest.context;
import static org.apache.cassandra.db.compaction.differential.CounterDifferentialCompactionTest.global;
import static org.apache.cassandra.db.compaction.differential.CounterDifferentialCompactionTest.local;
import static org.apache.cassandra.db.compaction.differential.CounterDifferentialCompactionTest.remote;
import static org.junit.Assert.assertEquals;

/**
 * Counter cells of one column in several sstables, where the merged value depends on the order
 * the merge gives the cells in.  The iterator path folds the cells of a column with
 * {@code Cells.reconcile} in the order its column merge gives them, and {@code CounterContext}
 * does not always give the same result for both orders: between a remote shard with clock 0 and
 * one with a positive clock it keeps the right-hand one.  The cursor path must merge the cells
 * in the same order, so both outputs must be byte-identical.
 *
 * <p>Timestamps are fixed so that the cells tie.  Each partition is in a different set of
 * sstables, so the merge order changes from one partition to the next.
 */
@RunWith(Parameterized.class)
public class CounterMergeOrderDifferentialCompactionTest extends DifferentialCompactionTester
{
    private static final long TIMESTAMP = 1_000_000L;
    private static final int SEEDS = 6;

    @Parameterized.Parameter(0)
    public String format;

    @Parameterized.Parameter(1)
    public int sstableCount;

    @Parameterized.Parameters(name = "{0}-{1}sstables")
    public static List<Object[]> parameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (String format : new String[]{ "big", "bti" })
            for (int sstables : new int[]{ 2, 5, 9 })
                params.add(new Object[]{ format, sstables });
        return params;
    }

    @Before
    public void selectFormat()
    {
        selectSSTableFormat(format);
    }

    /**
     * CQLTester drops the last test's tables on the single-threaded optionalTasks, and a drop flushes every
     * table in the commit log, so one landing mid-test adds an sstable here.  Wait for it first.
     */
    @Before
    public void awaitLastTestCleanup() throws Exception
    {
        ScheduledExecutors.optionalTasks.submit(() -> {}).get();
    }

    @After
    public void restoreFormat()
    {
        restoreSelectedFormat();
    }

    /** The same contexts, with the same timestamp, in every sstable. */
    @Test
    public void sameContextInEverySSTable() throws Exception
    {
        createCounterTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = cfs.metadata();
        ColumnMetadata c = column(metadata, "c");
        ColumnMetadata s = column(metadata, "s");
        for (int i = 0; i < sstableCount; i++)
        {
            applyCounterCell(metadata, 1L, 0L, BufferCell.live(c, TIMESTAMP, context(global(1, 5, 100), remote(3, 4, 9))));
            applyCounterCell(metadata, 1L, 1L, BufferCell.live(c, TIMESTAMP, context(local(2, 3, 7))));
            applyCounterCell(metadata, 1L, 2L, BufferCell.live(c, TIMESTAMP, context(global(1, 5, 100), local(2, 3, 7), remote(3, 4, 9))));
            applyStaticCounterCell(metadata, 1L, BufferCell.live(s, TIMESTAMP, context(remote(4, 2, 11))));
            flush();
        }
        assertEquals(sstableCount, cfs.getLiveSSTables().size());
        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Remote shards of the same counter ids, with clock 0 in some sstables and a positive clock
     * in others, and a different count in each sstable.
     */
    @Test
    public void remoteShardWithClockZero() throws Exception
    {
        createCounterTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = cfs.metadata();
        ColumnMetadata c = column(metadata, "c");
        ColumnMetadata s = column(metadata, "s");
        int partitions = sstableCount * 4;
        for (int i = 0; i < sstableCount; i++)
        {
            for (long pk = 0; pk < partitions; pk++)
            {
                // leave each partition out of some sstables, so the partition merge order changes
                if (pk % sstableCount == i && i > 0)
                    continue;
                long count = 10L * i + pk;
                long clock1 = (i + pk) % 3 == 0 ? 0 : 1 + i % 2;
                long clock2 = (i + pk) % 2 == 0 ? 0 : 2;
                for (long ck = 0; ck < 3; ck++)
                    applyCounterCell(metadata, pk, ck, BufferCell.live(c, TIMESTAMP, context(remote(1, clock1, count + ck), remote(2, clock2, count))));
                applyStaticCounterCell(metadata, pk, BufferCell.live(s, TIMESTAMP, context(remote(5, clock2, count))));
            }
            flush();
        }
        assertEquals(sstableCount, cfs.getLiveSSTables().size());
        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Seeded mix of global, local and remote shards with small clocks, 0 included, tied and
     * near-tied timestamps, and some tombstones, over many partitions.
     */
    @Test
    public void seededMix() throws Exception
    {
        for (int seed = 0; seed < SEEDS; seed++)
        {
            long fullSeed = 0xC0_0000L + seed * 31L + sstableCount * 1009L + format.hashCode();
            logger.info("seededMix format={} sstables={} seed={}", format, sstableCount, fullSeed);
            createCounterTable();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            writeSeededMix(cfs.metadata(), new Random(fullSeed));
            assertEquals(sstableCount, cfs.getLiveSSTables().size());
            assertCursorMatchesIteratorAcrossGenerations(cfs);
        }
    }

    private void writeSeededMix(TableMetadata metadata, Random random)
    {
        ColumnMetadata c = column(metadata, "c");
        ColumnMetadata d = column(metadata, "d");
        ColumnMetadata s = column(metadata, "s");
        long ldt = FBUtilities.nowInSeconds() - 60;
        int partitions = sstableCount * 6;
        for (int i = 0; i < sstableCount; i++)
        {
            // every sstable gets at least one cell, so the count check holds
            applyCounterCell(metadata, (long) partitions, 0L, BufferCell.live(c, TIMESTAMP, randomContext(random)));
            for (long pk = 0; pk < partitions; pk++)
            {
                if (random.nextInt(3) == 0)
                    continue;
                if (random.nextInt(4) == 0)
                    applyStaticCounterCell(metadata, pk, randomCell(s, random, ldt));
                int rows = 1 + random.nextInt(3);
                for (long ck = 0; ck < rows; ck++)
                {
                    if (random.nextBoolean())
                        applyCounterCell(metadata, pk, ck, randomCell(c, random, ldt));
                    if (random.nextBoolean())
                        applyCounterCell(metadata, pk, ck, randomCell(d, random, ldt));
                }
            }
            flush();
        }
    }

    private static Cell<?> randomCell(ColumnMetadata column, Random random, long ldt)
    {
        long timestamp = TIMESTAMP + random.nextInt(2);
        if (random.nextInt(12) == 0)
            return new BufferCell(column, timestamp, Cell.NO_TTL, ldt, ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
        return BufferCell.live(column, timestamp, randomContext(random));
    }

    /** One to three shards with ascending ids, as a counter context requires. */
    private static ByteBuffer randomContext(Random random)
    {
        int shards = 1 + random.nextInt(3);
        List<ShardSpec> specs = new ArrayList<>(shards);
        int id = 0;
        for (int n = 0; n < shards; n++)
        {
            id += 1 + random.nextInt(2);
            long clock = random.nextInt(3);
            long count = random.nextInt(50) - 10;
            switch (random.nextInt(4))
            {
                case 0: specs.add(global(id, clock == 0 ? 1 : clock, count)); break;
                case 1: specs.add(local(id, clock, count)); break;
                default: specs.add(remote(id, clock, count)); break;
            }
        }
        return context(specs.toArray(new ShardSpec[0]));
    }

    private void createCounterTable()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, d counter, s counter static, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000 AND compression = {'enabled': 'false'}");
        getCurrentColumnFamilyStore().disableAutoCompaction();
    }

    private static void applyStaticCounterCell(TableMetadata metadata, long pk, Cell<?> cell)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addCell(cell);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(
            metadata, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(pk)), builder.build());
        new Mutation(update).apply();
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(ByteBufferUtil.bytes(name));
    }
}
