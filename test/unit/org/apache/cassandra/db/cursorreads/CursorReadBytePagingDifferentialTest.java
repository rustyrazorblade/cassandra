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

import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Paging by bytes (CASSANDRA-11745) must stop at the same row on both paths.  The limit counts
 * each live row's {@code Row.liveDataSize} and charges the static row's with the first row, so
 * the cursor path must count the same bytes from the cells it streams.  Each case pages one wide
 * partition (small row index blocks) at several byte page sizes, through the internal pager and
 * through CQL with every read served as a replica response, forward and reversed.  It then
 * resumes {@code forPaging} commands with a byte limit, alone and with a row limit, at many
 * clusterings.  The rows hold simple columns, non-frozen list, set, map and user type columns
 * with element deletions and a deleted then re-added collection, cells and row liveness with a
 * TTL that has run out at the read's {@code nowInSec}, deleted rows and range deletions.
 * Layouts: all BIG, all BTI, mixed, each with a memtable.
 */
public class CursorReadBytePagingDifferentialTest extends CursorReadOracle
{
    private static final int ROWS = 400;
    /** Byte page sizes: below one row, around one row, a few rows, many rows. */
    private static final int[] PAGE_BYTES = { 1, 64, 200, 777, 4096, 50_000 };

    private SSTableFormat format;
    private int originalColumnIndexSizeKiB;
    private String originalFormat;
    private DataStorageSpec.LongBytesBound originalReadSizeWarn, originalReadSizeFail;

    private enum SSTableFormat { BIG, BTI, MIXED }

    @Before
    public void smallIndexBlocks()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat().name();
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        // the transcode path declines a read that tracks the local read size thresholds
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
    public void allBig()
    {
        run(SSTableFormat.BIG);
    }

    @Test
    public void allBti()
    {
        run(SSTableFormat.BTI);
    }

    @Test
    public void bigAndBtiMixed()
    {
        run(SSTableFormat.MIXED);
    }

    /** A static row larger than a page: the first row of each page carries its bytes. */
    @Test
    public void largeStaticRow()
    {
        format = SSTableFormat.BTI;
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        selectFormat(0);
        execute("UPDATE %s USING TIMESTAMP 1000 SET s = ? WHERE pk = 1", "s".repeat(5000));
        for (long ck = 0; ck < 50; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?) USING TIMESTAMP 1000", ck, "v" + ck);
        flush();
        selectFormat(1);
        for (long ck = 0; ck < 50; ck += 4)
            execute("UPDATE %s USING TIMESTAMP 2000 SET v = ? WHERE pk = 1 AND ck = ?", "w" + ck, ck);
        flush();
        execute("UPDATE %s USING TIMESTAMP 3000 SET v = 'mem' WHERE pk = 1 AND ck = 7");

        long now = FBUtilities.nowInSeconds();
        long seed = 11;
        for (boolean reversed : new boolean[]{ false, true })
        {
            LongFunction<SinglePartitionReadCommand> full = fullRead(cfs, reversed);
            for (int bytes : new int[]{ 1, 100, 4999, 5000, 5100, 20_000 })
            {
                String label = "large static" + (reversed ? " reversed" : "") + " bytes " + bytes;
                assertPagedMatches(ReadCase.of(label, seed++).at(now), cfs, full, PageSize.inBytes(bytes));
                assertCqlReplicaResponsesMatch(ReadCase.of(label, seed++).at(now).expectTranscode(true),
                                               cfs, "SELECT * FROM %s WHERE pk = 1" + (reversed ? " ORDER BY ck DESC" : ""),
                                               PageSize.inBytes(bytes));
            }
        }
        assertEveryCaseServedLegs();
    }

    /** Text and blob clustering values, some empty: the clustering size is read from its serialized form. */
    @Test
    public void variableLengthClustering()
    {
        format = SSTableFormat.MIXED;
        createTable("CREATE TABLE %s (pk bigint, c1 text, c2 int, c3 blob, v text, PRIMARY KEY (pk, c1, c2, c3))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        selectFormat(0);
        for (int i = 0; i < 120; i++)
            execute("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (1, ?, ?, ?, ?) USING TIMESTAMP 1000",
                    i % 10 == 0 ? "" : "k".repeat(i % 37) + i, i, ByteBufferUtil.bytes(i % 3 == 0 ? "" : "b" + i), "v" + i);
        flush();
        selectFormat(1);
        for (int i = 0; i < 120; i += 3)
            execute("UPDATE %s USING TIMESTAMP 2000 SET v = ? WHERE pk = 1 AND c1 = ? AND c2 = ? AND c3 = ?",
                    "w" + i, i % 10 == 0 ? "" : "k".repeat(i % 37) + i, i, ByteBufferUtil.bytes(i % 3 == 0 ? "" : "b" + i));
        flush();

        long now = FBUtilities.nowInSeconds();
        long seed = 77;
        for (boolean reversed : new boolean[]{ false, true })
        {
            for (int bytes : new int[]{ 1, 90, 400, 2000 })
            {
                String label = "variable clustering" + (reversed ? " reversed" : "") + " bytes " + bytes;
                assertPagedMatches(ReadCase.of(label, seed++).at(now), cfs, fullRead(cfs, reversed), PageSize.inBytes(bytes));
                assertCqlReplicaResponsesMatch(ReadCase.of(label, seed++).at(now).expectTranscode(true),
                                               cfs, "SELECT * FROM %s WHERE pk = 1" + (reversed ? " ORDER BY c1 DESC, c2 DESC, c3 DESC" : ""),
                                               PageSize.inBytes(bytes));
            }
        }
        assertEveryCaseServedLegs();
    }

    private void selectFormat(int layer)
    {
        switch (format)
        {
            case BIG: DatabaseDescriptor.setSelectedSSTableFormat("big"); break;
            case BTI: DatabaseDescriptor.setSelectedSSTableFormat("bti"); break;
            default: DatabaseDescriptor.setSelectedSSTableFormat(layer % 2 == 0 ? "big" : "bti");
        }
    }

    private static LongFunction<SinglePartitionReadCommand> fullRead(ColumnFamilyStore cfs, boolean reversed)
    {
        return nowInSec -> {
            var b = Util.cmd(cfs, 1L).withNowInSeconds(nowInSec);
            return (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
        };
    }

    private void run(SSTableFormat format)
    {
        this.format = format;
        String type = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, v2 text, " +
                    "l list<text>, st set<int>, m map<int, text>, u " + type + ", PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String padding = "x".repeat(60);

        selectFormat(0);
        for (long ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2, l, st, m, u) VALUES (1, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP 1000",
                    ck, ck, padding + ck, list("a" + ck, "b" + ck), set((int) ck, (int) ck + 1),
                    map((int) ck, "m" + ck, (int) ck + 1, padding), userType("a", (int) ck, "b", "u" + ck));
        execute("UPDATE %s USING TIMESTAMP 1000 SET s = 'static' WHERE pk = 1");
        flush();

        selectFormat(1);
        // range deletions and deleted rows
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck >= 40 AND ck < 70");
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck >= 300 AND ck <= 320");
        for (long ck : new long[]{ 0, 5, 100, 101, 399 })
            execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck = ?", ck);
        // rows written back over the first range deletion
        for (long ck = 50; ck < 55; ck++)
            execute("UPDATE %s USING TIMESTAMP 3000 SET v1 = ? WHERE pk = 1 AND ck = ?", -ck, ck);
        // element deletions, and a collection deleted then re-added
        for (long ck = 1; ck < ROWS; ck += 7)
            execute("UPDATE %s USING TIMESTAMP 2000 SET st = st - ?, m = m - ? WHERE pk = 1 AND ck = ?",
                    set((int) ck), set((int) ck), ck);
        for (long ck = 2; ck < ROWS; ck += 9)
            execute("DELETE l FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck = ?", ck);
        for (long ck = 2; ck < ROWS; ck += 18)
            execute("UPDATE %s USING TIMESTAMP 2500 SET l = l + ? WHERE pk = 1 AND ck = ?", list("re" + ck), ck);
        // a deleted user type column
        for (long ck = 3; ck < ROWS; ck += 13)
            execute("DELETE u FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck = ?", ck);
        flush();

        selectFormat(2);
        // cells and row liveness with a TTL; the reads below run after they expire
        for (long ck = 4; ck < ROWS; ck += 5)
            execute("UPDATE %s USING TIMESTAMP 4000 AND TTL 100 SET v2 = ? WHERE pk = 1 AND ck = ?", "ttl" + ck, ck);
        for (long ck = 6; ck < ROWS; ck += 11)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?) USING TIMESTAMP 4000 AND TTL 100", ck, ck * 3);
        for (long ck = 8; ck < ROWS; ck += 17)
            execute("UPDATE %s USING TIMESTAMP 4000 AND TTL 100 SET m[?] = ? WHERE pk = 1 AND ck = ?", 1000, "ttl", ck);
        // rows that hold only expiring data
        for (long ck = ROWS; ck < ROWS + 10; ck++)
            execute("INSERT INTO %s (pk, ck, v2) VALUES (1, ?, 'expires') USING TIMESTAMP 4000 AND TTL 100", ck);
        flush();

        // memtable
        for (long ck = 9; ck < ROWS; ck += 19)
            execute("UPDATE %s USING TIMESTAMP 5000 SET v1 = ?, m[?] = ? WHERE pk = 1 AND ck = ?", ck * 100, 7, "mem", ck);
        execute("DELETE FROM %s USING TIMESTAMP 5000 WHERE pk = 1 AND ck >= 200 AND ck <= 205");
        execute("UPDATE %s USING TIMESTAMP 5000 SET s = ? WHERE pk = 1", "static in memtable " + padding);

        // after the TTLs run out, before any tombstone is purgeable
        long now = FBUtilities.nowInSeconds() + 1000;
        long seed = format.ordinal() * 1000L;
        for (boolean reversed : new boolean[]{ false, true })
        {
            String direction = reversed ? " reversed" : "";
            LongFunction<SinglePartitionReadCommand> full = fullRead(cfs, reversed);
            for (int bytes : PAGE_BYTES)
            {
                String label = format + direction + " bytes " + bytes;
                assertPagedMatches(ReadCase.of(label, seed++).at(now), cfs, full, PageSize.inBytes(bytes));
                assertCqlReplicaResponsesMatch(ReadCase.of(label, seed++).at(now).expectTranscode(true),
                                               cfs, "SELECT * FROM %s WHERE pk = 1" + (reversed ? " ORDER BY ck DESC" : ""),
                                               PageSize.inBytes(bytes));
            }
            // fewer columns: the size counts only the fetched cells
            assertCqlReplicaResponsesMatch(ReadCase.of(format + direction + " two columns", seed++).at(now).expectTranscode(true),
                                           cfs, "SELECT ck, m FROM %s WHERE pk = 1" + (reversed ? " ORDER BY ck DESC" : ""),
                                           PageSize.inBytes(700));
            // a slice
            assertCqlReplicaResponsesMatch(ReadCase.of(format + direction + " slice", seed++).at(now).expectTranscode(true),
                                           cfs, "SELECT * FROM %s WHERE pk = 1 AND ck >= 30 AND ck < 120" + (reversed ? " ORDER BY ck DESC" : ""),
                                           PageSize.inBytes(500));

            for (long resumeAt = -1; resumeAt <= ROWS + 10; resumeAt += 23)
            {
                long at = resumeAt;
                for (int bytes : new int[]{ 1, 300, 3000 })
                {
                    LongFunction<SinglePartitionReadCommand> resumed = nowInSec -> {
                        SinglePartitionReadCommand base = full.apply(nowInSec);
                        Clustering<?> last = Clustering.make(ByteBufferUtil.bytes(at));
                        return base.forPaging(last, base.limits().forPaging(PageSize.inBytes(bytes)));
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(format + direction + " resume after " + at + " bytes " + bytes, seed++).at(now), cfs, resumed);
                }
                // a byte limit and a row limit together
                LongFunction<SinglePartitionReadCommand> both = nowInSec -> {
                    SinglePartitionReadCommand base = full.apply(nowInSec);
                    Clustering<?> last = Clustering.make(ByteBufferUtil.bytes(at));
                    return base.forPaging(last, DataLimits.cqlLimits(5).forPaging(PageSize.inBytes(1500)));
                };
                assertAllReadSurfacesMatch(ReadCase.of(format + direction + " resume after " + at + " limit 5 bytes 1500", seed++).at(now), cfs, both);
            }
        }
        assertEveryCaseServedLegs();
    }
}
