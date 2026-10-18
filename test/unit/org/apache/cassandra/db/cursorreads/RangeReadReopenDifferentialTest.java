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
import java.util.Random;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeScan;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.cursorreads.RangeScanTesting.assertLinesEqual;
import static org.apache.cassandra.db.cursorreads.RangeScanTesting.responseLines;
import static org.apache.cassandra.db.cursorreads.RangeScanTesting.runToEnd;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link CursorRangeScan} across segments: a yield after every partition with a flush, a major
 * compaction or nothing between segments, and a scan started after a partition key, as the Flight
 * bulk reader retries the rest of a split.  Each must read exactly what one uninterrupted read
 * reads: no partition lost, none read twice.
 *
 * <p>Properties: {@code cassandra.test.range_reopen_trials} (default {@value #DEFAULT_TRIALS}),
 * {@code cassandra.test.range_reopen_seed} (default: the clock, logged).
 */
@RunWith(Parameterized.class)
public class RangeReadReopenDifferentialTest extends RangeReadOracle
{
    private static final Logger logger = LoggerFactory.getLogger(RangeReadReopenDifferentialTest.class);

    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> formats()
    {
        return Arrays.asList(new Object[][]{ { "big" }, { "bti" } });
    }

    private static final int DEFAULT_TRIALS = 6;
    private static final int TRIALS = Integer.getInteger("cassandra.test.range_reopen_trials", DEFAULT_TRIALS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final long BASE_TS = 7_000_000_000_000L;

    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void pinFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void reopensAtEveryPartitionWithFlushesAndCompactionsBetween() throws Throwable
    {
        long master = Long.getLong("cassandra.test.range_reopen_seed", System.currentTimeMillis()); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        logger.info("{} reopen format={} master seed = {}L", getClass().getSimpleName(), format, master);
        Random seeds = new Random(master);
        for (int trial = 0; trial < TRIALS; trial++)
        {
            long seed = seeds.nextLong();
            try
            {
                reopenTrial(seed);
            }
            catch (Throwable t)
            {
                throw new AssertionError("trial " + trial + " failed, seed " + seed + "L (master " + master + "L)", t);
            }
            finally
            {
                dropTable("DROP TABLE IF EXISTS %s");
            }
        }
    }

    private void reopenTrial(long seed) throws Throwable
    {
        Random r = new Random(seed);
        ColumnFamilyStore cfs = load(r);
        long now = FBUtilities.nowInSeconds() + 1;
        for (Query q : queries(r, cfs))
        {
            String query = q.label;
            setRangePath(false);
            List<String> expected = responseLines(q.command.apply(now));
            RangeScanTesting.RecordingSink sink = new RangeScanTesting.RecordingSink(cfs.metadata(), () -> true);
            CursorRangeScan scan = CursorRangeScan.open(q.command.apply(now));
            int segments = 0;
            while (scan.run(sink) == CursorRangeScan.ScanEnd.YIELDED)
            {
                segments++;
                // what happens between segments changes no data
                switch (r.nextInt(4))
                {
                    case 0: flush(); break;
                    case 1: cfs.forceMajorCompaction(); break;
                    default: break;
                }
            }
            assertEquals("partitions taken from an iterator", 0, scan.iteratorLegsOpened());
            assertLinesEqual(query + " with a reopen at every partition (seed " + seed + "L)", expected, sink.lines);
            assertTrue("the scan reopened: " + segments, segments > 0 || expected.isEmpty());
            // the memtable may have been flushed: write it again for the next query
            rewriteOverlay(r);
        }
    }

    @Test
    public void startAfterAPartitionKeyReadsTheRest() throws Throwable
    {
        long master = Long.getLong("cassandra.test.range_reopen_seed", System.currentTimeMillis()); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        logger.info("{} start-after format={} master seed = {}L", getClass().getSimpleName(), format, master);
        Random seeds = new Random(master ^ 0x1234567L);
        for (int trial = 0; trial < TRIALS; trial++)
        {
            long seed = seeds.nextLong();
            try
            {
                startAfterTrial(seed);
            }
            catch (Throwable t)
            {
                throw new AssertionError("trial " + trial + " failed, seed " + seed + "L (master " + master + "L)", t);
            }
            finally
            {
                dropTable("DROP TABLE IF EXISTS %s");
            }
        }
    }

    private void startAfterTrial(long seed) throws Throwable
    {
        Random r = new Random(seed);
        ColumnFamilyStore cfs = load(r);
        long now = FBUtilities.nowInSeconds() + 1;
        for (Query q : queries(r, cfs))
        {
            String query = q.label;
            PartitionRangeReadCommand command = q.command.apply(now);
            RangeScanTesting.RecordingSink whole = new RangeScanTesting.RecordingSink(cfs.metadata(), () -> false);
            runToEnd(CursorRangeScan.open(command), whole);
            setRangePath(false);
            assertLinesEqual(query + ", uninterrupted", responseLines(q.command.apply(now)), whole.lines);
            if (whole.keys.isEmpty())
                continue;
            for (int cut = 0; cut < 4; cut++)
            {
                // the last complete partition a client received, then the rest of the range after it
                int index = r.nextInt(whole.keys.size());
                DecoratedKey last = whole.keys.get(index);
                PartitionRangeReadCommand rest = afterKey(q.command.apply(now), last);
                RangeScanTesting.RecordingSink retried = new RangeScanTesting.RecordingSink(cfs.metadata(), () -> r.nextBoolean());
                runToEnd(CursorRangeScan.open(rest), retried);
                String what = query + " after partition " + index + " of " + whole.keys.size() + " (" + ByteBufferUtil.bytesToHex(last.getKey()) + ", seed " + seed + "L)";
                // a retry of a limited read has the whole limit again, which the client lowers itself
                if (command.limits().count() == DataLimits.NO_LIMIT)
                    assertLinesEqual(what, linesAfter(whole.lines, index), retried.lines);
                setRangePath(false);
                assertLinesEqual(what + ", against the iterator path", responseLines(afterKey(q.command.apply(now), last)), retried.lines);
            }
            // a sink that stops after a partition, then takes the rest on the same scan
            RangeScanTesting.RecordingSink stopping = new StoppingSink(cfs, 1 + r.nextInt(whole.keys.size()));
            CursorRangeScan scan = CursorRangeScan.open(q.command.apply(now));
            CursorRangeScan.ScanEnd end;
            do
            {
                end = scan.run(stopping);
            }
            while (end != CursorRangeScan.ScanEnd.DONE);
            assertLinesEqual(query + ", stopped and run again", whole.lines, stopping.lines);
        }
    }

    /** A sink that wants no more once it has {@code stopAfter} partitions, once. */
    private static final class StoppingSink extends RangeScanTesting.RecordingSink
    {
        private final int stopAfter;
        private boolean stopped;

        StoppingSink(ColumnFamilyStore cfs, int stopAfter)
        {
            super(cfs.metadata(), () -> false);
            this.stopAfter = stopAfter;
        }

        @Override
        public boolean wantsMore()
        {
            if (!stopped && partitionsEnded >= stopAfter)
            {
                stopped = true;
                return false;
            }
            return true;
        }
    }

    /** The command of the range after {@code last}, as the start-after-partition-key retry and the range pager make it. */
    static PartitionRangeReadCommand afterKey(PartitionRangeReadCommand command, DecoratedKey last)
    {
        AbstractBounds<PartitionPosition> range = command.dataRange().keyRange();
        AbstractBounds<PartitionPosition> rest = range instanceof Range || range instanceof Bounds
                                                 ? new Range<>(last, range.right)
                                                 : new ExcludingBounds<>(last, range.right);
        return command.forSubRange(rest, true);
    }

    /** The lines of the partitions after the {@code index}th. */
    private static List<String> linesAfter(List<String> lines, int index)
    {
        int ended = 0;
        for (int i = 0; i < lines.size(); i++)
        {
            if (lines.get(i).equals("END") && ended++ == index)
                return new ArrayList<>(lines.subList(i + 1, lines.size()));
        }
        throw new AssertionError("no partition " + index);
    }

    /** A read of the trial, by name. */
    private static final class Query
    {
        final String label;
        final LongFunction<PartitionRangeReadCommand> command;

        Query(String label, LongFunction<PartitionRangeReadCommand> command)
        {
            this.label = label;
            this.command = command;
        }
    }

    private List<Query> queries(Random r, ColumnFamilyStore cfs)
    {
        List<String> cql = new ArrayList<>();
        cql.add("SELECT * FROM %s");
        cql.add("SELECT * FROM %s WHERE v > 50 ALLOW FILTERING");
        // long stretches of partitions no row of which matches
        cql.add("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING");
        cql.add("SELECT * FROM %s WHERE s = 1 ALLOW FILTERING");
        cql.add("SELECT * FROM %s PER PARTITION LIMIT 2");
        cql.add("SELECT * FROM %s LIMIT " + (5 + r.nextInt(40)));
        long a = r.nextLong(), b = r.nextLong();
        cql.add("SELECT * FROM %s WHERE token(k) > " + Math.min(a, b) + " AND token(k) <= " + Math.max(a, b));
        List<Query> queries = new ArrayList<>();
        for (String q : cql)
            queries.add(new Query(q, now -> rangeCommand(q, now)));
        // a token range to the end of the ring, as a coordinator gives a replica the last part of a
        // range that wraps around it
        TableMetadata metadata = cfs.metadata();
        Range<Token> toEnd = new Range<>(new Murmur3Partitioner.LongToken(Math.max(a, b)), metadata.partitioner.getMinimumToken());
        queries.add(new Query("to the end of the ring " + toEnd, now -> PartitionRangeReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(),
                                                                                                       DataLimits.NONE, DataRange.forTokenRange(toEnd))));
        return queries;
    }

    private ColumnFamilyStore load(Random r) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s int static, v int, t text, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        int partitions = 20 + r.nextInt(60);
        int sstables = 1 + r.nextInt(4);
        long ts = BASE_TS;
        for (int s = 0; s < sstables; s++)
        {
            int rows = 50 + r.nextInt(200);
            for (int i = 0; i < rows; i++)
            {
                int k = r.nextInt(partitions);
                switch (r.nextInt(8))
                {
                    case 0:
                        execute("DELETE FROM %s USING TIMESTAMP " + (++ts) + " WHERE k = ? AND c = ?", k, r.nextInt(30));
                        break;
                    case 1:
                        execute("UPDATE %s USING TIMESTAMP " + (++ts) + " SET s = ? WHERE k = ?", r.nextInt(4), k);
                        break;
                    case 2:
                    {
                        int c = r.nextInt(30);
                        execute("DELETE FROM %s USING TIMESTAMP " + (++ts) + " WHERE k = ? AND c >= ? AND c < ?", k, c, c + 1 + r.nextInt(5));
                        break;
                    }
                    default:
                        execute("INSERT INTO %s (k, c, v, t) VALUES (?, ?, ?, ?) USING TIMESTAMP " + (++ts), k, r.nextInt(30), r.nextInt(100), "t" + r.nextInt(10));
                        break;
                }
            }
            flush();
        }
        rewriteOverlay(r);
        return cfs;
    }

    private void rewriteOverlay(Random r) throws Throwable
    {
        // newer writes in the memtable, the same each time: a flush between segments does not change the data
        for (int k = 0; k < 10; k++)
            execute("INSERT INTO %s (k, c, v, t) VALUES (?, ?, ?, ?) USING TIMESTAMP " + (BASE_TS + 1_000_000 + k), k, 3, 51 + k, "overlay");
    }
}
