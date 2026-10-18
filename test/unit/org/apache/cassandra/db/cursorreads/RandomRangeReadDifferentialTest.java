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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
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
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Seeded random range reads, compared with the iterator range path on every surface of
 * {@link RangeReadOracle}.  Each trial builds a table from its seed (simple or composite partition
 * key, 0 to 2 clustering columns in either order, a static column or not, a collection or not),
 * writes a random history across several partitions (inserts, updates, short and long TTLs, cell,
 * row, range and partition deletions, static writes, timestamp ties), flushes it into 1 to 6
 * sstables in BIG, BTI or a mix of both, and leaves an overlay in the memtable.  From its own
 * stream of the seed a trial also picks where the memtable stands (a small overlay, the second half
 * of the writes, or every write with nothing flushed, so partitions are in the memtable alone) and
 * the memtable implementation; the {@link #KNOWN_REGRESSION_SEEDS} keep the small overlay and the
 * default memtable they were found with.  It then runs
 * random range reads: the full ring, token ranges (wrapping or not), key bounds of each kind, empty
 * ranges, limits, per-partition limits, DISTINCT, column subsets, static-only selections, names and
 * slice filters, reversed reads, row filters, internal paging and CQL paging.
 *
 * <p>Every write has an explicit timestamp and every read pins {@code nowInSec}.  Partitions are
 * small or wide; the row index block size is 1 KiB, so wide partitions are row indexed.
 *
 * <p>A trial is fully determined by its seed.  Properties:
 * <ul>
 *   <li>{@code cassandra.test.cursor_range_trials}: trials per run (default {@value #DEFAULT_TRIALS});</li>
 *   <li>{@code cassandra.test.cursor_range_seed}: the master seed (default: the clock, logged);</li>
 *   <li>{@code cassandra.test.cursor_range_trial_seed}: replays one trial ({@link #replayTrialSeed}).</li>
 * </ul>
 * A failing trial is shrunk (operations, partitions, flushes, then the one failing read) and reported
 * with its seed; add the seed to {@link #KNOWN_REGRESSION_SEEDS} once the bug is fixed.
 */
@RunWith(Parameterized.class)
public class RandomRangeReadDifferentialTest extends RangeReadOracle
{
    /** The sstable format the trials write; a trial may still mix formats. */
    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> formats()
    {
        return Arrays.asList(new Object[][]{ { "big" }, { "bti" } });
    }

    private static final Logger logger = LoggerFactory.getLogger(RandomRangeReadDifferentialTest.class);

    private static final int DEFAULT_TRIALS = 20;
    private static final int TRIALS = Integer.getInteger("cassandra.test.cursor_range_trials", DEFAULT_TRIALS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final long BASE_TS = 3_000_000_000_000L;
    private static final int SHORT_TTL = 50;
    private static final int LONG_TTL = 1_000_000;
    /** How far after the writes the reads run: past every short TTL, before every long one. */
    private static final int READ_AFTER_SECONDS = 200;
    private static final int COLUMN_INDEX_SIZE_KIB = 1;

    /** Seeds of trials that once failed, replayed first on every run.  Never remove one. */
    private static final long[] KNOWN_REGRESSION_SEEDS = {
        // a page whose range is Range(k, k], the whole ring from k: the second range of an sstable
        // started at a partition the cursor had passed
        206992469602218461L,
        // seeds that failed in CI
        -8877200426255970015L,
        5932111658273683873L,
        // a partition in a memtable and an sstable: the merge reports the key of the source the
        // iterator path's partition merge gives first, not the memtable's
        -9208867235617653717L,
        7650556690056646382L,
    };

    /** Seeds of trials that failed with the memtable layout and implementation their seed picks.  Never remove one. */
    private static final long[] KNOWN_LAYOUT_REGRESSION_SEEDS = {
        // a partition in the memtable alone, merged as one leg: a complex deletion equal to the
        // active deletion was dropped, which the iterator path, reading the lone memtable as is, keeps
        -7333784131763744167L,
        // a skiplist memtable read by names (a table with no clustering column reads by names too):
        // its names iterator gives the memtable's own objects, not ones cloned to heap, whose heap
        // size the read size guardrail measures
        -7150240821680689603L,
        -907622551635808982L,
    };

    /** The stream of a trial's seed that picks the memtable layout and implementation. */
    private static final long MEMTABLE_SALT = 0x4CF5AD432745937FL;

    /** Where the memtable stands in a trial's data. */
    enum MemtableLayout
    {
        /** a few writes after the last flush */
        OVERLAY,
        /** the second half of the writes, partition deletions included */
        LARGE_OVERLAY,
        /** nothing is flushed */
        MEMTABLE_ONLY
    }

    /** null for the default memtable of the configuration */
    private static final String[] MEMTABLES = { null, "trie", "skiplist", "skiplist_sharded" };

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;

    @Before
    public void pinFormatAndBlockSize()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(formatName());
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(COLUMN_INDEX_SIZE_KIB);
    }

    @After
    public void restoreFormatAndBlockSize()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
    }

    protected String formatName()
    {
        return format;
    }

    @Test
    public void randomRangeReads() throws Throwable
    {
        Assume.assumeTrue("a single trial is replayed instead", System.getProperty("cassandra.test.cursor_range_trial_seed") == null); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        long master = Long.getLong("cassandra.test.cursor_range_seed", System.currentTimeMillis()); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        logger.info("{} format={} master seed = {}L, trials = {}", getClass().getSimpleName(), format, master, TRIALS);

        for (long seed : KNOWN_REGRESSION_SEEDS)
            runTrial(seed, true, "known regression");
        for (long seed : KNOWN_LAYOUT_REGRESSION_SEEDS)
            runTrial(seed, false, "known regression");

        Random seeds = new Random(master);
        for (int trial = 0; trial < TRIALS; trial++)
            runTrial(seeds.nextLong(), false, "trial " + trial + " of master seed " + master + 'L');
    }

    /** Replays the trial of {@code -Dcassandra.test.cursor_range_trial_seed}. */
    @Test
    public void replayTrialSeed() throws Throwable
    {
        String seed = System.getProperty("cassandra.test.cursor_range_trial_seed"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        Assume.assumeTrue("no trial seed to replay", seed != null);
        // a seed of KNOWN_REGRESSION_SEEDS replays with the memtable it was found with
        boolean pinned = Boolean.getBoolean("cassandra.test.cursor_range_trial_pinned"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        runTrial(Long.parseLong(seed.endsWith("L") ? seed.substring(0, seed.length() - 1) : seed), pinned, "replay");
    }

    // ---------------------------------------------------------------- trials

    /** Everything a trial needs, derived from its seed; the knobs the shrinker lowers are copies. */
    static final class Trial
    {
        final long seed;
        final boolean compositeKey;
        final int clusterings;
        final boolean descending;
        final boolean statics;
        final String collection; // null, "set", "list" or "map"
        final ValueType valueType;
        final boolean purgeable;  // gc_grace_seconds = 0, so tombstones read as purgeable
        /** 0: the class format; 1: random BIG or BTI per flush */
        final int formatMode;
        final int maxClustering;
        final boolean wide;
        final int partitions;
        final int operations;
        final int flushes;
        final int queries;
        /** Run only this read when not negative; set by the shrinker. */
        final int onlyQuery;
        /** Two more collection columns, written so that rows hold a collection column with no
         *  cells on the iterator path (see {@link #writeRowsWithEmptyCollections}). */
        final boolean emptyCollections;
        final MemtableLayout layout;
        /** The table's memtable configuration, null for the default. */
        final String memtable;

        private Trial(long seed, boolean compositeKey, int clusterings, boolean descending, boolean statics, String collection,
                      ValueType valueType, boolean purgeable, int formatMode, int maxClustering, boolean wide,
                      int partitions, int operations, int flushes, int queries, int onlyQuery, boolean emptyCollections,
                      MemtableLayout layout, String memtable)
        {
            this.seed = seed;
            this.compositeKey = compositeKey;
            this.clusterings = clusterings;
            this.descending = descending;
            this.statics = statics;
            this.collection = collection;
            this.valueType = valueType;
            this.purgeable = purgeable;
            this.formatMode = formatMode;
            this.maxClustering = maxClustering;
            this.wide = wide;
            this.partitions = partitions;
            this.operations = operations;
            this.flushes = flushes;
            this.queries = queries;
            this.onlyQuery = onlyQuery;
            this.emptyCollections = emptyCollections;
            this.layout = layout;
            this.memtable = memtable;
        }

        /** @param pinned a regression seed: the small overlay and the default memtable it was found with */
        static Trial derive(long seed, boolean pinned)
        {
            Random r = new Random(seed);
            boolean compositeKey = r.nextInt(3) == 0;
            int clusterings = r.nextInt(3);
            boolean descending = clusterings > 0 && r.nextInt(3) == 0;
            boolean statics = clusterings > 0 && r.nextBoolean();
            String collection = new String[]{ null, null, "set", "list", "map" }[r.nextInt(5)];
            ValueType valueType = ValueType.ALL.get(r.nextInt(ValueType.ALL.size()));
            boolean purgeable = r.nextInt(4) == 0;
            int formatMode = r.nextInt(3) == 0 ? 1 : 0;
            boolean wide = clusterings > 0 && r.nextInt(3) == 0;
            int maxClustering = clusterings == 0 ? 1 : wide ? 300 + r.nextInt(1500) : 2 + r.nextInt(30);
            int partitions = 3 + r.nextInt(40);
            int operations = 20 + r.nextInt(wide ? 3000 : 400);
            int flushes = 1 + r.nextInt(6);
            int queries = 6 + r.nextInt(8);
            // its own random, so the other choices of a trial stay what they were before it existed
            boolean emptyCollections = new Random(seed ^ 0x632BE59BD9B4E019L).nextBoolean();
            Random m = new Random(seed ^ MEMTABLE_SALT);
            MemtableLayout layout = pinned ? MemtableLayout.OVERLAY : MemtableLayout.values()[m.nextInt(MemtableLayout.values().length)];
            String memtable = pinned ? null : MEMTABLES[m.nextInt(MEMTABLES.length)];
            return new Trial(seed, compositeKey, clusterings, descending, statics, collection, valueType, purgeable,
                             formatMode, maxClustering, wide, partitions, operations, flushes, queries, -1, emptyCollections,
                             layout, memtable);
        }

        Trial withOperations(int n) { return new Trial(seed, compositeKey, clusterings, descending, statics, collection, valueType, purgeable, formatMode, maxClustering, wide, partitions, n, flushes, queries, onlyQuery, emptyCollections, layout, memtable); }
        Trial withPartitions(int n) { return new Trial(seed, compositeKey, clusterings, descending, statics, collection, valueType, purgeable, formatMode, maxClustering, wide, n, operations, flushes, queries, onlyQuery, emptyCollections, layout, memtable); }
        Trial withFlushes(int n) { return new Trial(seed, compositeKey, clusterings, descending, statics, collection, valueType, purgeable, formatMode, maxClustering, wide, partitions, operations, n, queries, onlyQuery, emptyCollections, layout, memtable); }
        Trial withOnlyQuery(int q) { return new Trial(seed, compositeKey, clusterings, descending, statics, collection, valueType, purgeable, formatMode, maxClustering, wide, partitions, operations, flushes, queries, q, emptyCollections, layout, memtable); }

        @Override
        public String toString()
        {
            return String.format("seed=%dL compositeKey=%s clusterings=%d descending=%s statics=%s collection=%s value=%s " +
                                 "purgeable=%s formats=%s maxClustering=%d wide=%s partitions=%d operations=%d flushes=%d queries=%d onlyQuery=%d " +
                                 "emptyCollections=%s layout=%s memtable=%s",
                                 seed, compositeKey, clusterings, descending, statics, collection, valueType.cql, purgeable,
                                 formatMode == 0 ? "class" : "mixed", maxClustering, wide, partitions, operations, flushes, queries, onlyQuery,
                                 emptyCollections, layout, memtable);
        }
    }

    /** A read of a trial that diverged, with its index among the trial's reads. */
    private static final class ReadFailure extends AssertionError
    {
        final int query;

        ReadFailure(int query, String label, Throwable cause)
        {
            super("read " + query + " (" + label + ") failed: " + cause.getMessage(), cause);
            this.query = query;
        }
    }

    private void runTrial(long seed, boolean pinned, String label) throws Throwable
    {
        Trial trial = Trial.derive(seed, pinned);
        try
        {
            runOnce(trial);
        }
        catch (Throwable t)
        {
            dropTable("DROP TABLE IF EXISTS %s");
            String shrunk;
            try
            {
                shrunk = shrink(trial, t).toString();
            }
            catch (Throwable shrinkFailure)
            {
                shrunk = "(shrinking threw: " + shrinkFailure + ')';
            }
            throw new AssertionError(String.format("%s failed under %s.%nTrial:  %s%nShrunk: %s%n" +
                                                   "Replay with -Dcassandra.test.cursor_range_trial_seed=%dL -Dcassandra.test.cursor_range_trial_pinned=%s; " +
                                                   "once fixed, add it to %s.",
                                                   label, getClass().getSimpleName() + " format=" + format, trial, shrunk, seed, pinned,
                                                   pinned ? "KNOWN_REGRESSION_SEEDS" : "KNOWN_LAYOUT_REGRESSION_SEEDS"), t);
        }
        // drop each trial's table, its data and its cached statements, so many trials fit the heap
        dropTable("DROP TABLE IF EXISTS %s");
    }

    private void runOnce(Trial trial) throws Throwable
    {
        ColumnFamilyStore cfs = createTrialTable(trial);
        long writeNow = FBUtilities.nowInSeconds();
        List<Object[]> keys = partitionKeys(trial);
        populate(trial, keys);
        long nowInSec = writeNow + READ_AFTER_SECONDS;

        List<Query> queries = queries(trial, cfs, keys);
        for (int q = 0; q < queries.size(); q++)
        {
            if (trial.onlyQuery >= 0 && q != trial.onlyQuery)
                continue;
            Query query = queries.get(q);
            ReadCase c = ReadCase.of(query.label, trial.seed + q).at(nowInSec);
            try
            {
                query.run.accept(c, cfs);
            }
            catch (Throwable t)
            {
                throw new ReadFailure(q, query.label, t);
            }
        }
    }

    private boolean reproduces(Trial trial)
    {
        try
        {
            runOnce(trial);
            return false;
        }
        catch (Throwable t)
        {
            return true;
        }
        finally
        {
            // a shrink runs the trial many times: drop each table so the heap does not fill up
            dropTable("DROP TABLE IF EXISTS %s");
        }
    }

    /** Lowers each knob to the smallest value that still fails, then keeps only the failing read. */
    private Trial shrink(Trial failing, Throwable failure)
    {
        Trial t = failure instanceof ReadFailure ? failing.withOnlyQuery(((ReadFailure) failure).query) : failing;
        Trial a = t.withOperations(shrinkKnob(t.operations, n -> reproduces(t.withOperations(n))));
        Trial b = a.withPartitions(shrinkKnob(a.partitions, n -> reproduces(a.withPartitions(n))));
        return b.withFlushes(shrinkKnob(b.flushes, n -> reproduces(b.withFlushes(n))));
    }

    private static int shrinkKnob(int initial, IntPredicate reproducesAt)
    {
        int lo = 1, hi = initial;
        while (lo < hi)
        {
            int mid = lo + (hi - lo) / 2;
            if (reproducesAt.test(mid))
                hi = mid;
            else
                lo = mid + 1;
        }
        return lo;
    }

    // ---------------------------------------------------------------- schema and data

    private ColumnFamilyStore createTrialTable(Trial t)
    {
        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (k int, ");
        if (t.compositeKey)
            ddl.append("k2 text, ");
        for (int i = 1; i <= t.clusterings; i++)
            ddl.append('c').append(i).append(" int, ");
        if (t.statics)
            ddl.append("s text static, ");
        ddl.append("v ").append(t.valueType.cql).append(", w int, ");
        if (t.collection != null)
            ddl.append("coll ").append(t.collection).append(collectionType(t.collection)).append(", ");
        if (t.emptyCollections)
            ddl.append("e1 map<int, int>, e2 list<int>, ");
        ddl.append("PRIMARY KEY ((k").append(t.compositeKey ? ", k2)" : ")");
        for (int i = 1; i <= t.clusterings; i++)
            ddl.append(", c").append(i);
        ddl.append(")) WITH gc_grace_seconds = ").append(t.purgeable ? 0 : 864000);
        if (t.descending)
            ddl.append(" AND CLUSTERING ORDER BY (c1 DESC)");
        if (t.memtable != null)
            ddl.append(" AND memtable = '").append(t.memtable).append('\'');
        createTable(ddl.toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    private static String collectionType(String kind)
    {
        return "map".equals(kind) ? "<int, text>" : "<int>";
    }

    private static List<Object[]> partitionKeys(Trial t)
    {
        List<Object[]> keys = new ArrayList<>(t.partitions);
        for (int i = 0; i < t.partitions; i++)
            keys.add(t.compositeKey ? new Object[]{ i, "p" + (i % 3) } : new Object[]{ i });
        return keys;
    }

    private void populate(Trial t, List<Object[]> keys) throws Throwable
    {
        if (t.layout != MemtableLayout.OVERLAY)
        {
            populateWithLargerMemtable(t, keys);
            return;
        }
        Random r = new Random(t.seed ^ 0x5DEECE66DL);
        long ts = BASE_TS;
        int perFlush = Math.max(1, t.operations / t.flushes);
        int flushed = 0;
        int sinceFlush = 0;
        // one partition takes most of the writes in a wide trial, so it is row indexed
        int widePartition = r.nextInt(keys.size());
        for (int op = 0; op < t.operations; op++)
        {
            if (r.nextInt(8) != 0)
                ts++; // otherwise a timestamp tie with the previous write
            Object[] key = keys.get(t.wide && r.nextInt(3) != 0 ? widePartition : r.nextInt(keys.size()));
            write(t, r, key, ts);
            if (++sinceFlush >= perFlush && flushed < t.flushes)
            {
                maybePickFormat(t, r);
                flush();
                flushed++;
                sinceFlush = 0;
            }
        }
        if (flushed < t.flushes)
        {
            maybePickFormat(t, r);
            flush();
        }
        writeRowsWithEmptyCollections(t, keys, true);
        markSomeRepaired(t);
        // the memtable overlay, with the newest timestamps
        int overlay = r.nextInt(4) == 0 ? 0 : 1 + r.nextInt(Math.max(1, t.operations / 10));
        for (int op = 0; op < overlay; op++)
            write(t, r, keys.get(r.nextInt(keys.size())), ++ts);
    }

    /**
     * {@link #populate} for {@link MemtableLayout#LARGE_OVERLAY}, where the second half of the writes
     * stays in the memtable, and {@link MemtableLayout#MEMTABLE_ONLY}, where nothing is flushed.
     * Partitions written only after the last flush are in the memtable alone.
     */
    private void populateWithLargerMemtable(Trial t, List<Object[]> keys) throws Throwable
    {
        Random r = new Random(t.seed ^ 0x5DEECE66DL);
        long ts = BASE_TS;
        boolean flushes = t.layout == MemtableLayout.LARGE_OVERLAY;
        int flushedOperations = flushes ? (t.operations + 1) / 2 : 0;
        int perFlush = Math.max(1, flushedOperations / t.flushes);
        int flushed = 0;
        int sinceFlush = 0;
        int widePartition = r.nextInt(keys.size());
        for (int op = 0; op < t.operations; op++)
        {
            if (flushes && op == flushedOperations)
            {
                if (flushed < t.flushes)
                {
                    maybePickFormat(t, r);
                    flush();
                }
                writeRowsWithEmptyCollections(t, keys, true);
                markSomeRepaired(t);
            }
            if (r.nextInt(8) != 0)
                ts++;
            Object[] key = keys.get(t.wide && r.nextInt(3) != 0 ? widePartition : r.nextInt(keys.size()));
            write(t, r, key, ts);
            if (op < flushedOperations && ++sinceFlush >= perFlush && flushed < t.flushes)
            {
                maybePickFormat(t, r);
                flush();
                flushed++;
                sinceFlush = 0;
            }
        }
        if (!flushes)
            writeRowsWithEmptyCollections(t, keys, false);
    }

    /**
     * Rows that hold a collection column with no cells on the iterator path: map cells, then a
     * list overwrite with a newer timestamp, which stores a complex deletion on the row.  On such a
     * row every complex column stores a deletion, and the iterator path keeps the map, with no
     * cells, when the read skips all its cells and nothing merges the row.  The overwrite is an
     * insert (a row timestamp, which an unqueried map cell is older than) or an update, sometimes
     * with a short TTL, so the row expires.  Its own random, so the other writes of the trial stay.
     */
    private void writeRowsWithEmptyCollections(Trial t, List<Object[]> keys, boolean flushes) throws Throwable
    {
        if (!t.emptyCollections)
            return;
        Random r = new Random(t.seed ^ 0x1B873593L);
        String where = keyWhere(t);
        int rows = 1 + r.nextInt(Math.max(1, t.partitions));
        for (int i = 0; i < rows; i++)
        {
            Object[] key = keys.get(r.nextInt(keys.size()));
            Object[] clustering = clustering(t, r);
            long ts = BASE_TS + r.nextInt(t.operations + 1);
            execute("UPDATE %s USING TIMESTAMP " + ts + " SET e1 = e1 + ? WHERE " + where + rowWhere(t),
                    concat(new Object[]{ Collections.singletonMap(r.nextInt(10), r.nextInt(100)) }, key, clustering));
            if (r.nextInt(4) == 0 && flushes)
            {
                maybePickFormat(t, r);
                flush();
            }
            long overwrite = ts + 1 + r.nextInt(5);
            String ttl = r.nextInt(4) == 0 ? " AND TTL " + SHORT_TTL : "";
            List<Object> list = Collections.singletonList(r.nextInt(10));
            if (r.nextBoolean())
            {
                StringBuilder cols = new StringBuilder(keyColumns(t));
                StringBuilder marks = new StringBuilder(keyMarks(t));
                for (int c = 1; c <= t.clusterings; c++)
                {
                    cols.append(", c").append(c);
                    marks.append(", ?");
                }
                execute("INSERT INTO %s (" + cols + ", e2) VALUES (" + marks + ", ?) USING TIMESTAMP " + overwrite + ttl,
                        concat(key, clustering, new Object[]{ list }));
            }
            else
            {
                execute("UPDATE %s USING TIMESTAMP " + overwrite + ttl + " SET e2 = ? WHERE " + where + rowWhere(t),
                        concat(new Object[]{ list }, key, clustering));
            }
        }
        if (flushes)
        {
            maybePickFormat(t, r);
            flush();
        }
    }

    /** In a third of the trials, marks each sstable repaired at random, so the reads that track
     *  repaired data merge a repaired set.  Its own random, so the other choices of the trial stay. */
    private void markSomeRepaired(Trial t) throws Exception
    {
        Random r = new Random(t.seed ^ 0x2545F4914F6CDD1DL);
        if (r.nextInt(3) != 0)
            return;
        List<SSTableReader> sstables = new ArrayList<>(getCurrentColumnFamilyStore().getLiveSSTables());
        sstables.sort((a, b) -> SSTableIdFactory.COMPARATOR.compare(a.descriptor.id, b.descriptor.id));
        for (SSTableReader sstable : sstables)
        {
            if (!r.nextBoolean())
                continue;
            sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, FBUtilities.nowInSeconds(), null, false);
            sstable.reloadSSTableMetadata();
        }
    }

    private void maybePickFormat(Trial t, Random r)
    {
        if (t.formatMode == 1)
            DatabaseDescriptor.setSelectedSSTableFormat(r.nextBoolean() ? "big" : "bti");
    }

    private void write(Trial t, Random r, Object[] key, long ts) throws Throwable
    {
        String where = keyWhere(t);
        Object[] clustering = clustering(t, r);
        int choice = r.nextInt(16);
        String ttl = r.nextInt(6) == 0 ? (r.nextBoolean() ? " AND TTL " + SHORT_TTL : " AND TTL " + LONG_TTL) : "";
        if (t.clusterings == 0 && choice == 10)
            choice = 0; // no clustering column to delete a range of
        if (choice < 6)
        {
            // a whole row, sometimes with a TTL
            List<Object> values = new ArrayList<>();
            StringBuilder cols = new StringBuilder(keyColumns(t));
            StringBuilder marks = new StringBuilder(keyMarks(t));
            Collections.addAll(values, key);
            for (int i = 0; i < t.clusterings; i++)
            {
                cols.append(", c").append(i + 1);
                marks.append(", ?");
                values.add(clustering[i]);
            }
            cols.append(", v, w");
            marks.append(", ?, ?");
            values.add(t.valueType.generate(r));
            values.add(r.nextInt(100));
            execute("INSERT INTO %s (" + cols + ") VALUES (" + marks + ") USING TIMESTAMP " + ts + ttl, values.toArray());
            return;
        }
        switch (choice)
        {
            case 6:
            case 7:
                // one cell
                execute("UPDATE %s USING TIMESTAMP " + ts + ttl + " SET w = ? WHERE " + where + rowWhere(t),
                        concat(new Object[]{ r.nextInt(100) }, key, clustering));
                return;
            case 8:
                // a cell deletion
                execute("DELETE v FROM %s USING TIMESTAMP " + ts + " WHERE " + where + rowWhere(t), concat(new Object[0], key, clustering));
                return;
            case 9:
                // a row deletion
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE " + where + rowWhere(t), concat(new Object[0], key, clustering));
                return;
            case 10:
            {
                // a range deletion over c1
                int a = r.nextInt(t.maxClustering + 1), b = r.nextInt(t.maxClustering + 1);
                // exclusive bounds only on a range that is not a single value
                String lower = a != b && r.nextBoolean() ? " > ?" : " >= ?";
                String upper = a != b && r.nextBoolean() ? " < ?" : " <= ?";
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE " + where + " AND c1" + lower + " AND c1" + upper,
                        concat(new Object[0], key, new Object[]{ Math.min(a, b), Math.max(a, b) }));
                return;
            }
            case 11:
                if (r.nextInt(4) == 0)
                {
                    // a partition deletion
                    execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE " + where, key);
                    return;
                }
                break;
            case 12:
            case 13:
                if (t.statics)
                {
                    execute("UPDATE %s USING TIMESTAMP " + ts + ttl + " SET s = ? WHERE " + where,
                            concat(new Object[]{ "s" + r.nextInt(1000) }, key, new Object[0]));
                    return;
                }
                break;
            case 14:
            case 15:
                if (t.collection != null)
                {
                    String set;
                    Object value;
                    switch (t.collection)
                    {
                        case "set": set = "coll = coll + ?"; value = Collections.singleton(r.nextInt(20)); break;
                        case "list": set = "coll = coll + ?"; value = Collections.singletonList(r.nextInt(20)); break;
                        default: set = "coll = coll + ?"; value = Collections.singletonMap(r.nextInt(20), "m" + r.nextInt(100)); break;
                    }
                    if (r.nextInt(5) == 0)
                    {
                        execute("UPDATE %s USING TIMESTAMP " + ts + " SET coll = null WHERE " + where + rowWhere(t), concat(new Object[0], key, clustering));
                        return;
                    }
                    execute("UPDATE %s USING TIMESTAMP " + ts + ttl + " SET " + set + " WHERE " + where + rowWhere(t),
                            concat(new Object[]{ value }, key, clustering));
                    return;
                }
                break;
            default:
                break;
        }
        // the choice does not apply to this schema: write one cell instead
        execute("UPDATE %s USING TIMESTAMP " + ts + " SET w = ? WHERE " + where + rowWhere(t),
                concat(new Object[]{ r.nextInt(100) }, key, clustering));
    }

    private static Object[] clustering(Trial t, Random r)
    {
        Object[] values = new Object[t.clusterings];
        for (int i = 0; i < t.clusterings; i++)
            values[i] = i == 0 ? r.nextInt(t.maxClustering + 1) : r.nextInt(4);
        return values;
    }

    private static Object[] concat(Object[] first, Object[] key, Object[] clustering)
    {
        Object[] all = new Object[first.length + key.length + clustering.length];
        System.arraycopy(first, 0, all, 0, first.length);
        System.arraycopy(key, 0, all, first.length, key.length);
        System.arraycopy(clustering, 0, all, first.length + key.length, clustering.length);
        return all;
    }

    private static String keyColumns(Trial t)
    {
        return t.compositeKey ? "k, k2" : "k";
    }

    private static String keyMarks(Trial t)
    {
        return t.compositeKey ? "?, ?" : "?";
    }

    private static String keyWhere(Trial t)
    {
        return t.compositeKey ? "k = ? AND k2 = ?" : "k = ?";
    }

    private static String rowWhere(Trial t)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= t.clusterings; i++)
            sb.append(" AND c").append(i).append(" = ?");
        return sb.toString();
    }

    // ---------------------------------------------------------------- reads

    @FunctionalInterface
    private interface QueryRun
    {
        void accept(ReadCase c, ColumnFamilyStore cfs) throws Throwable;
    }

    private static final class Query
    {
        final String label;
        final QueryRun run;

        Query(String label, QueryRun run)
        {
            this.label = label;
            this.run = run;
        }
    }

    /** The trial's reads: the full ring first, then random ones from its seed. */
    private List<Query> queries(Trial t, ColumnFamilyStore cfs, List<Object[]> keys)
    {
        Random r = new Random(t.seed ^ 0x9E3779B97F4A7C15L);
        TableMetadata metadata = cfs.metadata();
        List<Query> queries = new ArrayList<>();
        queries.add(internal("full ring", cfs, now -> command(metadata, now, ColumnFilter.all(metadata), RowFilter.none(), DataLimits.NONE,
                                                              DataRange.allData(metadata.partitioner))));
        for (int i = 0; i < t.queries; i++)
            queries.add(randomQuery(t, r, cfs, keys));
        return queries;
    }

    private Query randomQuery(Trial t, Random r, ColumnFamilyStore cfs, List<Object[]> keys)
    {
        TableMetadata metadata = cfs.metadata();
        int kind = r.nextInt(10);
        if (kind < 6)
        {
            // an internal command: a key range, a clustering filter, columns, a row filter and limits
            AbstractBounds<PartitionPosition> bounds = randomBounds(r, metadata, keys);
            ClusteringIndexFilter filter = randomClusteringFilter(t, r, metadata);
            ColumnFilter columns = randomColumns(t, r, metadata);
            RowFilter rowFilter = r.nextInt(5) == 0 ? wFilter(metadata, r) : RowFilter.none();
            DataLimits limits = randomLimits(r);
            String label = String.format("bounds=%s filter=%s columns=%s rowFilter=%s limits=%s",
                                         bounds.getString(metadata.partitionKeyType), filter.toString(metadata), columns, rowFilter, limits);
            LongFunction<PartitionRangeReadCommand> command = now -> command(metadata, now, columns, rowFilter, limits, new DataRange(bounds, filter));
            if (r.nextInt(4) == 0)
            {
                int pageSize = new int[]{ 1, 2, 3, 7, 13, 50 }[r.nextInt(6)];
                return new Query("paged " + pageSize + ' ' + label, (c, store) -> assertPagedMatches(c, store, command, pageSize));
            }
            return internal(label, cfs, command);
        }
        // a CQL query, page by page
        String query = randomCql(t, r);
        int pageSize = new int[]{ -1, 1, 3, 10, 100 }[r.nextInt(5)];
        boolean asReplica = r.nextInt(3) == 0;
        return new Query("cql",
                         (c, store) -> {
                             if (asReplica)
                                 assertCqlReplicaResponsesMatch(c, store, query, pageSize);
                             else
                                 assertCqlMatches(c, store, query, pageSize);
                         });
    }

    private Query internal(String label, ColumnFamilyStore cfs, LongFunction<PartitionRangeReadCommand> command)
    {
        return new Query(label, (c, store) -> assertAllReadSurfacesMatch(c, store, command));
    }

    private static PartitionRangeReadCommand command(TableMetadata metadata, long nowInSec, ColumnFilter columns, RowFilter rowFilter,
                                                     DataLimits limits, DataRange range)
    {
        return PartitionRangeReadCommand.create(metadata, nowInSec, columns, rowFilter, limits, range);
    }

    private static AbstractBounds<PartitionPosition> randomBounds(Random r, TableMetadata metadata, List<Object[]> keys)
    {
        switch (r.nextInt(6))
        {
            case 0:
                return DataRange.allData(metadata.partitioner).keyRange();
            case 1:
            case 2:
            {
                // a token range, wrapping when the left token is past the right one
                Token left = new Murmur3Partitioner.LongToken(r.nextLong());
                Token right = new Murmur3Partitioner.LongToken(r.nextLong());
                return DataRange.forTokenRange(new Range<>(left, right)).keyRange();
            }
            default:
            {
                // bounds on partition keys: present ones, or keys of partitions never written
                DecoratedKey a = decorate(metadata, keyOf(r, keys));
                DecoratedKey b = decorate(metadata, keyOf(r, keys));
                if (a.compareTo(b) > 0)
                {
                    DecoratedKey swap = a;
                    a = b;
                    b = swap;
                }
                switch (r.nextInt(4))
                {
                    case 0: return a.equals(b) ? new Bounds<>(a, b) : new Range<>(a, b);
                    case 1: return new Bounds<>(a, b);
                    case 2: return a.equals(b) ? new Bounds<>(a, b) : new ExcludingBounds<>(a, b);
                    default: return a.equals(b) ? new Bounds<>(a, b) : new IncludingExcludingBounds<>(a, b);
                }
            }
        }
    }

    private static Object[] keyOf(Random r, List<Object[]> keys)
    {
        if (r.nextInt(5) == 0)
        {
            Object[] absent = keys.get(0).clone();
            absent[0] = 1000 + r.nextInt(1000);
            return absent;
        }
        return keys.get(r.nextInt(keys.size()));
    }

    private static DecoratedKey decorate(TableMetadata metadata, Object[] key)
    {
        ByteBuffer serialized = metadata.partitionKeyAsClusteringComparator().make(key).serializeAsPartitionKey();
        return metadata.partitioner.decorateKey(serialized);
    }

    private static ClusteringIndexFilter randomClusteringFilter(Trial t, Random r, TableMetadata metadata)
    {
        boolean reversed = r.nextInt(3) == 0;
        if (t.clusterings == 0 || r.nextInt(3) == 0)
            return new ClusteringIndexSliceFilter(Slices.ALL, reversed);
        if (r.nextInt(3) == 0)
        {
            // names on c1 (and c2 = 0 when there are two clustering columns)
            TreeSet<Clustering<?>> names = new TreeSet<>(metadata.comparator);
            int n = 1 + r.nextInt(5);
            for (int i = 0; i < n; i++)
            {
                Object[] values = t.clusterings == 1 ? new Object[]{ r.nextInt(t.maxClustering + 1) }
                                                     : new Object[]{ r.nextInt(t.maxClustering + 1), r.nextInt(4) };
                names.add(metadata.comparator.make(values));
            }
            return new ClusteringIndexNamesFilter(names, reversed);
        }
        // one or more slices on c1
        Slices.Builder slices = new Slices.Builder(metadata.comparator);
        int n = 1 + r.nextInt(3);
        for (int i = 0; i < n; i++)
        {
            int a = r.nextInt(t.maxClustering + 1), b = r.nextInt(t.maxClustering + 1);
            ClusteringBound<?> start = r.nextInt(5) == 0 ? ClusteringBound.BOTTOM
                                                          : ClusteringBound.create(metadata.comparator, true, r.nextBoolean(), Math.min(a, b));
            ClusteringBound<?> end = r.nextInt(5) == 0 ? ClusteringBound.TOP
                                                        : ClusteringBound.create(metadata.comparator, false, r.nextBoolean(), Math.max(a, b));
            Slice slice = Slice.make(start, end);
            // an empty slice (an exclusive bound on each side of one value) selects nothing
            if (!slice.isEmpty(metadata.comparator))
                slices.add(slice);
        }
        return new ClusteringIndexSliceFilter(slices.build(), reversed);
    }

    private static ColumnFilter randomColumns(Trial t, Random r, TableMetadata metadata)
    {
        if (t.emptyCollections && r.nextInt(3) == 0)
        {
            // reads that skip every cell of e1: fetched but not queried, or queried at an element
            // the rows do not hold
            ColumnMetadata e1 = column(metadata, "e1");
            CellPath absent = CellPath.create(Int32Type.instance.decompose(10 + r.nextInt(5)));
            switch (r.nextInt(3))
            {
                case 0:
                    return ColumnFilter.allRegularColumnsBuilder(metadata, false).add(column(metadata, "w")).build();
                case 1:
                    return ColumnFilter.allRegularColumnsBuilder(metadata, false).add(column(metadata, "v")).select(e1, absent).build();
                default:
                    return ColumnFilter.selectionBuilder().add(column(metadata, "v")).select(e1, absent).build();
            }
        }
        switch (r.nextInt(5))
        {
            case 0:
                return ColumnFilter.selectionBuilder().add(column(metadata, "w")).build();
            case 1:
                if (t.statics)
                    return ColumnFilter.selectionBuilder().add(column(metadata, "s")).build();
                return ColumnFilter.all(metadata);
            case 2:
                if (t.collection != null)
                    return ColumnFilter.selectionBuilder().add(column(metadata, "coll")).add(column(metadata, "v")).build();
                return ColumnFilter.all(metadata);
            default:
                return ColumnFilter.all(metadata);
        }
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(ByteBufferUtil.bytes(name));
    }

    private static RowFilter wFilter(TableMetadata metadata, Random r)
    {
        RowFilter filter = RowFilter.create(true);
        filter.add(column(metadata, "w"), r.nextBoolean() ? Operator.GT : Operator.EQ, Int32Type.instance.decompose(r.nextInt(100)));
        return filter;
    }

    private static DataLimits randomLimits(Random r)
    {
        switch (r.nextInt(5))
        {
            case 0:
                return DataLimits.cqlLimits(1 + r.nextInt(20));
            case 1:
                return DataLimits.cqlLimits(1 + r.nextInt(50), 1 + r.nextInt(3));
            default:
                return DataLimits.NONE;
        }
    }

    private static String randomCql(Trial t, Random r)
    {
        String keys = keyColumns(t);
        switch (r.nextInt(8))
        {
            case 0:
                return "SELECT DISTINCT " + keys + (t.statics ? ", s" : "") + " FROM %s";
            case 1:
                return "SELECT * FROM %s LIMIT " + (1 + r.nextInt(30));
            case 2:
                return t.clusterings > 0 ? "SELECT * FROM %s PER PARTITION LIMIT " + (1 + r.nextInt(3)) : "SELECT * FROM %s";
            case 3:
                return "SELECT * FROM %s WHERE w > " + r.nextInt(100) + " ALLOW FILTERING";
            case 4:
                return t.statics ? "SELECT " + keys + ", s FROM %s" : "SELECT " + keys + ", w FROM %s";
            case 5:
                return "SELECT * FROM %s WHERE token(" + keys + ") > " + r.nextLong();
            case 6:
                return t.clusterings > 0 ? "SELECT * FROM %s WHERE c1 > " + r.nextInt(t.maxClustering + 1) + " ALLOW FILTERING" : "SELECT * FROM %s";
            default:
                return "SELECT * FROM %s";
        }
    }

    /** The value column's type and a generator of its values. */
    static final class ValueType
    {
        static final List<ValueType> ALL = List.of(new ValueType("int"), new ValueType("bigint"), new ValueType("text"),
                                                   new ValueType("blob"), new ValueType("double"), new ValueType("boolean"));
        final String cql;

        ValueType(String cql)
        {
            this.cql = cql;
        }

        Object generate(Random r)
        {
            switch (cql)
            {
                case "int": return r.nextInt();
                case "bigint": return r.nextLong();
                case "text": return "t" + r.nextInt(1_000_000) + "-".repeat(r.nextInt(40));
                case "blob": return ByteBuffer.wrap(new byte[r.nextInt(64)]);
                case "double": return r.nextDouble();
                default: return r.nextBoolean();
            }
        }
    }
}
