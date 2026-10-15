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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.compaction.differential.CounterDifferentialCompactionTest.context;
import static org.apache.cassandra.db.compaction.differential.CounterDifferentialCompactionTest.remote;
import static org.apache.cassandra.db.compaction.differential.SessionValidationParity.SessionKind;
import static org.apache.cassandra.db.compaction.differential.SessionValidationParity.assertMerkleTreesMatch;
import static org.junit.Assert.assertTrue;

/**
 * Repair validation over sources whose merged bytes depend on the order the merge gives equal
 * items in: decimal clustering, cell path and range tombstone bound values that compare equal
 * but differ in bytes, and counter cells whose merged context depends on which cell comes first.
 * The cursor path must merge the sources the iterator path merges, in the same order, so both
 * build the same Merkle trees.  The range sets include ranges some sstables have no partitions
 * in; the iterator path still merges those sstables' scanners, as empty sources.
 */
@RunWith(Parameterized.class)
public class ValidationMergeOrderParityTest extends CQLTester
{
    private static final long TIMESTAMP = 1_000_000L;
    private static final int PARTITIONS = 24;

    @Parameterized.Parameter(0)
    public String format;

    @Parameterized.Parameter(1)
    public int sstableCount;

    private SSTableFormat<?, ?> originalFormat;

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
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /** Rows, set elements and range deletions with equal values written with different bytes. */
    @Test
    public void equalValuesWithDifferentBytes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c1 decimal, c2 int, v int, s set<decimal>, " +
                    "PRIMARY KEY (pk, c1, c2)) WITH gc_grace_seconds = 864000 AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Random random = new Random(0xD1FFL + sstableCount + format.hashCode());
        for (int i = 0; i < sstableCount; i++)
        {
            writeDecimalRows(random, PARTITIONS);
            flush();
        }
        assertParity(cfs);
    }

    /** Remote counter shards with clock 0 in some sstables and a positive clock in others. */
    @Test
    public void counterCellsWithClockZero() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000 AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();
        ColumnMetadata c = metadata.getColumn(ByteBufferUtil.bytes("c"));
        for (int i = 0; i < sstableCount; i++)
        {
            for (long pk = 0; pk < PARTITIONS; pk++)
            {
                if (pk % sstableCount == i && i > 0)
                    continue;
                long count = 10L * i + pk;
                long clock1 = (i + pk) % 3 == 0 ? 0 : 1 + i % 2;
                long clock2 = (i + pk) % 2 == 0 ? 0 : 2;
                for (long ck = 0; ck < 3; ck++)
                    applyCell(metadata, pk, ck, BufferCell.live(c, TIMESTAMP, context(remote(1, clock1, count + ck), remote(2, clock2, count))));
            }
            flush();
        }
        assertParity(cfs);
    }

    /**
     * Level 1 sstables reach the iterator path's merge through one leveled scanner, a single
     * source.  Level 0 sstables on top hold the same rows with other bytes.
     */
    @Test
    public void leveledScannerAsOneSource() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c1 decimal, c2 int, v int, s set<decimal>, PRIMARY KEY (pk, c1, c2)) " +
                    "WITH compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '1'} " +
                    "AND gc_grace_seconds = 864000 AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Random random = new Random(0x1E7E1L + sstableCount + format.hashCode());
        // enough data for several level 1 sstables of 1 MiB
        String padding = "x".repeat(2000);
        for (int pk = 0; pk < 1500; pk++)
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    pk, decimal(0, random.nextInt(4)), 0, padding.length(), TIMESTAMP);
        for (int pk = 0; pk < 1500; pk++)
            execute("UPDATE %s USING TIMESTAMP ? SET s = s + ? WHERE pk = ? AND c1 = ? AND c2 = ?",
                    TIMESTAMP, Collections.singleton(padding), pk, decimal(1, random.nextInt(4)), 0);
        flush();
        CompactionManager.instance.performMaximal(cfs);
        assertLeveledScannerHoldsSeveralSSTables(cfs);

        for (int i = 0; i < sstableCount; i++)
        {
            writeDecimalRows(random, 1500);
            flush();
        }
        assertLeveledScannerHoldsSeveralSSTables(cfs);
        assertParity(cfs);
    }

    /**
     * Full sessions only.  An incremental session validates pending sstables, and
     * {@code PendingRepairManager.getScanners} returns their scanners in a {@code HashSet}, so the
     * iterator path's merge order for them changes from one validation to the next.
     */
    private void assertParity(ColumnFamilyStore cfs) throws Exception
    {
        assertMerkleTreesMatch(cfs, SessionKind.FULL, FBUtilities.nowInSeconds());
    }

    /** Some rows, set elements and range deletions in a random subset of the partitions, mostly at one timestamp. */
    private void writeDecimalRows(Random random, int partitions)
    {
        for (int pk = 0; pk < partitions; pk++)
        {
            if (random.nextInt(10) < 3)
                continue;
            for (int c = 0; c < 3; c++)
            {
                if (random.nextBoolean())
                    execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                            pk, decimal(c, random.nextInt(4)), c % 2, 5, timestamp(random));
                if (random.nextBoolean())
                    execute("UPDATE %s USING TIMESTAMP ? SET s = s + ? WHERE pk = ? AND c1 = ? AND c2 = ?",
                            timestamp(random), Collections.singleton(decimalValue(random.nextInt(3), random.nextInt(4))),
                            pk, decimal(c, random.nextInt(4)), c % 2);
            }
            if (random.nextInt(10) < 3)
            {
                int from = random.nextInt(2);
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND c1 >= ? AND c1 < ?",
                        random.nextBoolean() ? TIMESTAMP - 1 : TIMESTAMP + 1, pk,
                        decimal(from, random.nextInt(4)), decimal(from + 1 + random.nextInt(2), random.nextInt(4)));
            }
        }
    }

    private static void assertLeveledScannerHoldsSeveralSSTables(ColumnFamilyStore cfs)
    {
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(cfs.getLiveSSTables()))
        {
            int most = 0;
            for (ISSTableScanner scanner : scanners.scanners)
                most = Math.max(most, scanner.getBackingSSTables().size());
            assertTrue("no scanner reads more than one sstable", most > 1);
        }
    }

    private static void applyCell(TableMetadata metadata, long pk, long ck, BufferCell cell)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(new BufferClustering(ByteBufferUtil.bytes(ck)));
        builder.addCell(cell);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(
            metadata, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(pk)), builder.build());
        new Mutation(update).apply();
    }

    /** Mostly the same timestamp, so most rows and cells tie. */
    private static long timestamp(Random random)
    {
        return random.nextInt(10) < 7 ? TIMESTAMP : TIMESTAMP + random.nextInt(3);
    }

    /** {@code value} with {@code scale} digits after the point: 1, 1.0, 1.00 and so on. */
    private static BigDecimal decimalValue(int value, int scale)
    {
        return new BigDecimal(BigInteger.valueOf(value).multiply(BigInteger.TEN.pow(scale)), scale);
    }

    private static ByteBuffer decimal(int value, int scale)
    {
        return DecimalType.instance.decompose(decimalValue(value, scale));
    }
}
