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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/**
 * Range reads paged by bytes (CASSANDRA-11745) must stop at the same row on both paths.  The limit
 * counts each live row's {@code Row.liveDataSize} and charges a partition's static row with its
 * first row, so the cursor range path must count the same bytes from the cells it streams, both
 * when it writes the replica response and when it gives a {@code RangeReadSink} the rows.  Each
 * case pages a table of many partitions, one of them wide, through the internal pager and through
 * CQL with every read served as a replica response, at several byte page sizes, alone and with a
 * row limit.  The rows hold simple columns, a list, a map with element deletions, cells and rows
 * whose TTL has run out at the read's {@code nowInSec}, static rows, static-only partitions,
 * deleted rows, range deletions and a deleted partition, on BIG and BTI, with a memtable.
 */
@RunWith(Parameterized.class)
public class RangeReadBytePagingDifferentialTest extends RangeReadOracle
{
    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> formats()
    {
        return Arrays.asList(new Object[][]{ { "big" }, { "bti" } });
    }

    private static final long BASE_TS = 1_000_000L;
    /** Byte page sizes: below one row, around one row, a few rows, a few partitions, many partitions. */
    private static final int[] PAGE_BYTES = { 1, 60, 250, 1000, 6000, 100_000 };

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;
    private DataStorageSpec.LongBytesBound originalReadSizeWarn, originalReadSizeFail;
    private long seed;

    @Before
    public void pinFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        // the cursor path declines a read that tracks the local read size thresholds
        originalReadSizeWarn = DatabaseDescriptor.getLocalReadSizeWarnThreshold();
        originalReadSizeFail = DatabaseDescriptor.getLocalReadSizeFailThreshold();
        DatabaseDescriptor.setLocalReadSizeWarnThreshold(null);
        DatabaseDescriptor.setLocalReadSizeFailThreshold(null);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeKiB);
        DatabaseDescriptor.setLocalReadSizeWarnThreshold(originalReadSizeWarn);
        DatabaseDescriptor.setLocalReadSizeFailThreshold(originalReadSizeFail);
    }

    @Test
    public void pagedByBytes() throws Throwable
    {
        ColumnFamilyStore cfs = load();
        // after the TTLs run out, before any tombstone is purgeable
        long now = FBUtilities.nowInSeconds() + 1000;
        for (int bytes : PAGE_BYTES)
        {
            String label = format + " bytes " + bytes;
            LongFunction<PartitionRangeReadCommand> all = at -> rangeCommand("SELECT * FROM %s", at);
            assertPagedMatches(ReadCase.of(label, seed++).at(now), cfs, all, PageSize.inBytes(bytes));
            assertCqlReplicaResponsesMatch(ReadCase.of(label, seed++).at(now), cfs, "SELECT * FROM %s", PageSize.inBytes(bytes));
            assertCqlMatches(ReadCase.of(label, seed++).at(now), cfs, "SELECT * FROM %s", PageSize.inBytes(bytes));
        }
        // fewer columns, a token range, a row limit and a per-partition limit with the byte limit
        String[] queries = { "SELECT k, c, m FROM %s",
                             "SELECT * FROM %s WHERE token(k) > 0",
                             "SELECT * FROM %s LIMIT 30",
                             "SELECT * FROM %s PER PARTITION LIMIT 3",
                             "SELECT k, s FROM %s" };
        for (String query : queries)
            for (int bytes : new int[]{ 200, 3000 })
                assertCqlReplicaResponsesMatch(ReadCase.of(format + " " + query + " bytes " + bytes, seed++).at(now), cfs, query, PageSize.inBytes(bytes));
    }

    /** A replica response and a sink scan with a byte limit, without the pager. */
    @Test
    public void byteLimitedReads() throws Throwable
    {
        ColumnFamilyStore cfs = load();
        long now = FBUtilities.nowInSeconds() + 1000;
        // many byte limits, so some land exactly where a row brings the count to the limit
        for (int bytes = 1; bytes < 3000; bytes += 47)
        {
            for (int rows : new int[]{ DataLimits.NO_LIMIT, 20 })
            {
                DataLimits limits = (rows == DataLimits.NO_LIMIT ? DataLimits.NONE : DataLimits.cqlLimits(rows)).forPaging(PageSize.inBytes(bytes));
                LongFunction<PartitionRangeReadCommand> command = at -> rangeCommand("SELECT * FROM %s", at).withUpdatedLimit(limits);
                String label = format + " bytes " + bytes + " rows " + rows;
                assertAllReadSurfacesMatch(ReadCase.of(label, seed++).at(now), cfs, command);

                // the RangeReadSink scan gives the rows the replica response holds
                setRangePath(false);
                List<String> expected = RangeScanTesting.responseLines(command.apply(now));
                assertEquals(label + ", the range read sink", expected, RangeScanTesting.scanLines(command.apply(now), null, () -> false));
                assertEquals(label + ", the range read sink with a yield after every partition", expected,
                             RangeScanTesting.scanLines(command.apply(now), null, () -> true));
            }
        }
    }

    private ColumnFamilyStore load() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s text static, v int, t text, l list<int>, m map<int, text>, PRIMARY KEY (k, c)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String padding = "p".repeat(40);
        long ts = BASE_TS;
        for (int flush = 0; flush < 3; flush++)
        {
            for (int k = flush; k < 40; k += 2)
            {
                for (int c = flush; c < 12; c += 3)
                    execute("INSERT INTO %s (k, c, v, t, l, m) VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
                            k, c, k + c, padding + c, List.of(c, c + 1), Map.of(c, "m" + c, c + 1, padding), ++ts);
                if (k % 3 == 0)
                    execute("UPDATE %s USING TIMESTAMP ? SET s = ? WHERE k = ?", ++ts, "static " + k + padding, k);
                if (k % 4 == flush)
                    execute("UPDATE %s USING TIMESTAMP ? SET m = m - ? WHERE k = ? AND c = ?", ++ts, Set.of(flush), k, flush);
                if (k % 7 == 0)
                {
                    execute("UPDATE %s USING TIMESTAMP ? AND TTL 100 SET t = 'expires' WHERE k = ? AND c = ?", ++ts, k, flush);
                    execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL 100", k, 30 + flush, 9, ++ts);
                }
                if (k % 5 == flush)
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND c > ? AND c < ?", ++ts, k, flush, flush + 5);
                if (k % 6 == flush)
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND c = ?", ++ts, k, flush + 3);
            }
            if (flush == 1)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ?", ++ts, 4);
            // a partition with only a static row
            execute("UPDATE %s USING TIMESTAMP ? SET s = ? WHERE k = ?", ++ts, "static only", 100 + flush);
            flush();
        }
        // one wide partition, row indexed
        for (int c = 100; c < 500; c++)
            execute("INSERT INTO %s (k, c, v, t) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, c, c, padding, ++ts);
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND c > ? AND c < ?", ++ts, 1, 150, 300);
        flush();
        // memtable
        for (int k = 0; k < 40; k += 5)
            execute("UPDATE %s USING TIMESTAMP ? SET v = ?, m[?] = ? WHERE k = ? AND c = ?", ++ts, -k, 77, "mem", k, 1);
        execute("UPDATE %s USING TIMESTAMP ? SET s = ? WHERE k = ?", ++ts, "static in memtable " + padding, 9);
        return cfs;
    }
}
