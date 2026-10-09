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

import java.util.Random;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
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
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.junit.Assert.assertTrue;

/**
 * Seeded random multi-slice and names reads over wide partitions with many row index blocks,
 * spread across several sstables plus a memtable, with random row, range and partition deletions
 * and static writes.  Each query is checked against the iterator path, record for record and byte
 * for byte (see {@link CursorReadDifferentialTester}).
 *
 * <p>Each data set comes from one seed and each query from its own seed, both printed on failure,
 * so a failing query replays alone.  The total query count comes from
 * {@code -Dcassandra.test.cursor_random_slice_queries=N} (default {@value #DEFAULT_QUERIES}).
 *
 * <p>The base class runs BIG; {@link BtiRandomMultiSliceCursorReadDifferentialTest} pins BTI.
 */
public class RandomMultiSliceCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private static final int DEFAULT_QUERIES = 2000;
    private static final int QUERIES = Integer.getInteger("cassandra.test.cursor_random_slice_queries", DEFAULT_QUERIES); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int DATA_SETS = 4;
    private static final int PK = 1;
    private static final long NOW = 1_700_000_000L;
    private static final long BASE_TS = 1_000_000L;
    private static final String PADDING = "-padding-padding-padding-";

    /** Data set seeds that failed once, replayed first on every run.  Add an entry when a genuine
     *  bug is found and fixed; never remove one. */
    private static final long[] KNOWN_REGRESSION_SEEDS = { -781715099318208394L, 3644616953108509967L };

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;

    @Before
    public void pinFormatAndBlockSize()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(formatName());
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
    }

    @After
    public void restoreFormatAndBlockSize()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    /** The sstable format this class pins.  The BTI subclass overrides it. */
    protected String formatName()
    {
        return "big";
    }

    @Test
    public void randomMultiSliceAndNamesReads() throws Throwable
    {
        long seed = System.currentTimeMillis();
        logger.info("{} seed = {}L, {} queries over {} data sets", getClass().getSimpleName(), seed, QUERIES, DATA_SETS);

        int queriesPerSet = Math.max(1, QUERIES / DATA_SETS);
        for (long regressionSeed : KNOWN_REGRESSION_SEEDS)
            runDataSet(regressionSeed, queriesPerSet);

        Random seedPicker = new Random(seed);
        for (int set = 0; set < DATA_SETS; set++)
            runDataSet(seedPicker.nextLong(), queriesPerSet);
    }

    private void runDataSet(long dataSeed, int queries) throws Throwable
    {
        DataShape shape = DataShape.derive(dataSeed);
        ColumnFamilyStore cfs = populate(dataSeed, shape);
        Random querySeeds = new Random(dataSeed ^ 0x9E3779B97F4A7C15L);
        long seeksBefore = CursorReads.sstableLegRowIndexSeeks();
        for (int q = 0; q < queries; q++)
        {
            long querySeed = querySeeds.nextLong();
            try
            {
                assertCursorReadMatchesIterator(cfs, command(cfs, shape, querySeed));
            }
            catch (Throwable t)
            {
                throw new AssertionError(String.format("query %d failed: dataSeed=%dL querySeed=%dL format=%s %s: %s",
                                                       q, dataSeed, querySeed, formatName(), shape,
                                                       command(cfs, shape, querySeed).get().toCQLString()), t);
            }
        }
        // the random reads must reach the per-slice seek on BTI, or this proves nothing about it
        if ("bti".equals(formatName()))
            assertTrue("no BTI row index seek in " + queries + " random queries, dataSeed=" + dataSeed + 'L',
                       CursorReads.sstableLegRowIndexSeeks() > seeksBefore);
    }

    // ---------------------------------------------------------------- data

    /** The knobs of one data set, all derived from its seed. */
    private static final class DataShape
    {
        final int clusterings;
        final int sstables;
        final int memtableOps;

        DataShape(int clusterings, int sstables, int memtableOps)
        {
            this.clusterings = clusterings;
            this.sstables = sstables;
            this.memtableOps = memtableOps;
        }

        static DataShape derive(long seed)
        {
            Random r = new Random(seed);
            return new DataShape(800 + r.nextInt(2400), 2 + r.nextInt(3), r.nextInt(60));
        }

        @Override
        public String toString()
        {
            return String.format("clusterings=%d sstables=%d memtableOps=%d", clusterings, sstables, memtableOps);
        }
    }

    /**
     * One wide partition.  The first sstable holds every clustering, so every query reaches an
     * sstable.  Each later sstable overwrites a random share of rows and adds row, range and
     * partition deletions and static writes, all at increasing timestamps.  The memtable gets a
     * few more operations, without partition deletions: a memtable partition deletion newer than
     * every sstable would leave the cursor no sstable to read.
     */
    private ColumnFamilyStore populate(long dataSeed, DataShape shape) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s text static, v1 int, v2 text, PRIMARY KEY (pk, c)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        Random r = new Random(dataSeed);
        long ts = BASE_TS;
        for (int c = 0; c < shape.clusterings; c++)
            insert(r, c, ts++);
        execute("UPDATE %s USING TIMESTAMP " + (ts++) + " SET s = ? WHERE pk = ?", "static-0", PK);
        flush();

        for (int sstable = 1; sstable < shape.sstables; sstable++)
        {
            int ops = 50 + r.nextInt(shape.clusterings / 2);
            for (int op = 0; op < ops; op++)
                ts = randomOp(r, shape, ts, false);
            if (sstable == shape.sstables - 1)
                ts = insertRowIndexedRun(r, shape, ts);
            flush();
        }
        for (int op = 0; op < shape.memtableOps; op++)
            ts = randomOp(r, shape, ts, true);
        return cfs;
    }

    /**
     * Rows newer than every partition deletion, spanning several row index blocks, in the last
     * sstable.  A partition deletion can shadow every older sstable, and the reads then skip them;
     * this keeps one row-indexed sstable in every read.
     */
    private long insertRowIndexedRun(Random r, DataShape shape, long ts) throws Throwable
    {
        int run = 200 + r.nextInt(200);
        int start = r.nextInt(shape.clusterings - run);
        for (int c = start; c < start + run; c++)
            insert(r, c, ts++);
        return ts;
    }

    private void insert(Random r, int c, long ts) throws Throwable
    {
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP " + ts,
                PK, c, r.nextInt(), "v" + PADDING + c);
    }

    /** @param inMemtable whether the operation stays in the memtable: no partition deletion and
     *                    nothing written at clustering 0 (see {@link #namesFilter}) */
    private long randomOp(Random r, DataShape shape, long ts, boolean inMemtable) throws Throwable
    {
        int c = inMemtable ? 1 + r.nextInt(shape.clusterings - 1) : r.nextInt(shape.clusterings);
        int choice = r.nextInt(100);
        if (choice < 55)
        {
            // a run of overwrites, so newer rows cluster the way real writes do
            int run = 1 + r.nextInt(20);
            for (int i = 0; i < run && c + i < shape.clusterings; i++)
                insert(r, c + i, ts++);
        }
        else if (choice < 65)
        {
            execute("UPDATE %s USING TIMESTAMP " + (ts++) + " SET v1 = ? WHERE pk = ? AND c = ?", r.nextInt(), PK, c);
        }
        else if (choice < 75)
        {
            execute("DELETE FROM %s USING TIMESTAMP " + (ts++) + " WHERE pk = ? AND c = ?", PK, c);
        }
        else if (choice < 92)
        {
            // range deletion, short or long, inclusive or exclusive ends
            int length = r.nextBoolean() ? 1 + r.nextInt(30) : 1 + r.nextInt(shape.clusterings / 3);
            int end = Math.min(shape.clusterings, c + length);
            String op = r.nextBoolean() ? ">=" : ">";
            String cl = r.nextBoolean() ? "<=" : "<";
            execute("DELETE FROM %s USING TIMESTAMP " + (ts++) + " WHERE pk = ? AND c " + op + " ? AND c " + cl + " ?",
                    PK, c, end);
        }
        else if (choice < 97)
        {
            execute("UPDATE %s USING TIMESTAMP " + (ts++) + " SET s = ? WHERE pk = ?", "static-" + ts, PK);
        }
        else if (!inMemtable)
        {
            // a partition deletion below the newest rows: later operations write above it
            execute("DELETE FROM %s USING TIMESTAMP " + (ts - 1 - r.nextInt(200)) + " WHERE pk = ?", PK);
        }
        return ts;
    }

    // ---------------------------------------------------------------- queries

    /**
     * A random query from {@code querySeed}: several slices (random widths, gaps from none to
     * hundreds of rows, inclusive or exclusive ends) or several names, forward or reversed, an
     * optional limit, and an optional column subset.
     */
    private static Supplier<SinglePartitionReadCommand> command(ColumnFamilyStore cfs, DataShape shape, long querySeed)
    {
        return () -> {
            Random r = new Random(querySeed);
            TableMetadata metadata = cfs.metadata();
            boolean reversed = r.nextInt(4) == 0;
            ClusteringIndexFilter filter = r.nextInt(3) == 0 ? namesFilter(r, metadata, shape, reversed)
                                                             : sliceFilter(r, metadata, shape, reversed);
            DataLimits limits = r.nextInt(3) == 0 ? DataLimits.cqlLimits(1 + r.nextInt(60)) : DataLimits.NONE;
            return SinglePartitionReadCommand.create(metadata, NOW, columns(r, metadata), RowFilter.none(), limits,
                                                     metadata.partitioner.decorateKey(Int32Type.instance.decompose(PK)),
                                                     filter);
        };
    }

    private static ClusteringIndexFilter sliceFilter(Random r, TableMetadata metadata, DataShape shape, boolean reversed)
    {
        Slices.Builder builder = new Slices.Builder(metadata.comparator);
        int slices = 1 + r.nextInt(8);
        int position = r.nextInt(shape.clusterings / 4 + 1);
        for (int i = 0; i < slices && position < shape.clusterings; i++)
        {
            int width = r.nextInt(4) == 0 ? r.nextInt(3) : r.nextInt(60);
            int end = position + width;
            Slice slice = Slice.make(ClusteringBound.create(metadata.comparator, true, r.nextBoolean(), position),
                                     ClusteringBound.create(metadata.comparator, false, r.nextBoolean(), end));
            // a one-clustering slice with an exclusive end is empty; read that clustering instead
            if (slice.isEmpty(metadata.comparator))
                slice = Slice.make(ClusteringBound.create(metadata.comparator, true, true, position),
                                   ClusteringBound.create(metadata.comparator, false, true, end));
            builder.add(slice);
            int gap = r.nextInt(4) == 0 ? r.nextInt(3) : r.nextInt(shape.clusterings / 4 + 1);
            position = end + gap;
        }
        return new ClusteringIndexSliceFilter(builder.build(), reversed);
    }

    private static ClusteringIndexFilter namesFilter(Random r, TableMetadata metadata, DataShape shape, boolean reversed)
    {
        TreeSet<Integer> picked = new TreeSet<>();
        // clustering 0 is never written in the memtable, so the names read always reaches an
        // sstable rather than finding every name in the memtable
        picked.add(0);
        int names = 1 + r.nextInt(12);
        while (picked.size() < names)
        {
            int c = r.nextInt(shape.clusterings + 4) - 2;
            picked.add(c);
            // sometimes the next clustering too, likely in the same block
            if (r.nextInt(3) == 0)
                picked.add(c + 1);
        }
        BTreeSet.Builder<Clustering<?>> builder = BTreeSet.builder(metadata.comparator);
        for (int c : picked)
            builder.add(Clustering.make(Int32Type.instance.decompose(c)));
        return new ClusteringIndexNamesFilter(builder.build(), reversed);
    }

    private static ColumnFilter columns(Random r, TableMetadata metadata)
    {
        switch (r.nextInt(4))
        {
            case 0:
                return ColumnFilter.selectionBuilder().add(column(metadata, "v1")).add(column(metadata, "v2")).build();
            case 1:
                return ColumnFilter.selectionBuilder().add(column(metadata, "v1")).build();
            case 2:
                return ColumnFilter.selectionBuilder().add(column(metadata, "s")).add(column(metadata, "v2")).build();
            default:
                return ColumnFilter.all(metadata);
        }
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(ByteBufferUtil.bytes(name));
    }
}
