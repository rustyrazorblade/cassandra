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
import java.util.List;
import java.util.TreeSet;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Paging must resume identically on both paths from every point a page can end.  Each case pages a
 * wide partition (small row index blocks, so resume points land mid-block and the BTI legs seek)
 * through the internal pager at page size 1 (a resume at every row), 2, 7 and N-1, forward and
 * reversed.  It then builds {@code forPaging} commands directly at every returned row, at range
 * tombstone start and end bounds, at deleted rows, at absent clusterings, and at the last row; when
 * there are more than {@value #MAX_RESUME_POINTS} points it takes an evenly spaced sample.  A slice
 * with no live rows checks a first page that holds only the static row.  Layouts: all BIG, all BTI,
 * mixed, each with a memtable.  Every resumed read is also compared as a replica response, and the
 * paged scans also run through CQL with every read served as a replica response.
 */
public class CursorReadPagingResumeEveryBoundaryTest extends CursorReadOracle
{
    private static final int MAX_RESUME_POINTS = 2000;
    private static final int ROWS = 300;

    private SSTableFormat<?, ?> originalFormat;
    private int originalColumnIndexSizeKiB;

    private DataStorageSpec.LongBytesBound originalReadSizeWarn, originalReadSizeFail;

    @Before
    public void smallIndexBlocks()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        originalColumnIndexSizeKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        // production leaves the local read size thresholds unset; the transcode path declines a
        // read that tracks them (see CqlResultMessageDifferentialTest.readSizeThresholdsDecline)
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
        run("big");
    }

    @Test
    public void allBti()
    {
        run("bti");
    }

    @Test
    public void bigAndBtiMixed()
    {
        run("mixed");
    }

    private static void selectFormat(String profile, int layer)
    {
        String format = profile.equals("mixed") ? (layer % 2 == 0 ? "big" : "bti") : profile;
        DatabaseDescriptor.setSelectedSSTableFormat(format);
    }

    /** Range tombstone bounds and deleted rows written below; resume points land on all of them. */
    private static final long[][] RANGES = { { 40, 60 }, { 100, 101 }, { 150, 210 }, { 280, 299 } };
    private static final long[] DELETED_ROWS = { 0, 7, 64, 65, 120, 299 };

    private void run(String profile)
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String padding = "x".repeat(100);

        selectFormat(profile, 0);
        for (long ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1L, ck, ck, padding + ck);
        execute("UPDATE %s USING TIMESTAMP 1000 SET s = 'static' WHERE pk = ?", 1L);
        flush();

        selectFormat(profile, 1);
        for (long[] range : RANGES)
            execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck >= ? AND ck < ?", 1L, range[0], range[1]);
        for (long ck : DELETED_ROWS)
            execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck = ?", 1L, ck);
        // rows written over the range at 150 so the range is not empty of rows
        for (long ck = 170; ck < 180; ck++)
            execute("UPDATE %s USING TIMESTAMP 3000 SET v1 = ? WHERE pk = ? AND ck = ?", -ck, 1L, ck);
        flush();

        selectFormat(profile, 2);
        for (long ck = 1; ck < ROWS; ck += 3)
            execute("UPDATE %s USING TIMESTAMP 4000 SET v2 = ? WHERE pk = ? AND ck = ?", "l2-" + ck, 1L, ck);
        flush();

        // memtable
        for (long ck = 2; ck < ROWS; ck += 11)
            execute("UPDATE %s USING TIMESTAMP 5000 SET v1 = ? WHERE pk = ? AND ck = ?", ck * 100, 1L, ck);
        execute("DELETE FROM %s USING TIMESTAMP 5000 WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 230L, 235L);

        long now = FBUtilities.nowInSeconds();
        long seed = profile.hashCode();
        for (boolean reversed : new boolean[]{ false, true })
        {
            LongFunction<SinglePartitionReadCommand> full = nowInSec -> {
                var b = Util.cmd(cfs, 1L).withNowInSeconds(nowInSec);
                return (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
            };
            int live = execute("SELECT ck FROM %s WHERE pk = 1").size();
            for (int pageSize : new int[]{ 1, 2, 7, live - 1 })
            {
                assertPagedMatches(ReadCase.of(profile + (reversed ? " reversed" : ""), seed++).at(now), cfs, full, pageSize);
                assertCqlReplicaResponsesMatch(ReadCase.of(profile + (reversed ? " reversed" : ""), seed++).at(now).expectTranscode(!reversed),
                                               cfs, "SELECT * FROM %s WHERE pk = 1" + (reversed ? " ORDER BY ck DESC" : ""), pageSize);
            }

            for (long resumeAt : resumePoints())
            {
                for (int limit : new int[]{ 1, 5 })
                {
                    LongFunction<SinglePartitionReadCommand> resumed = nowInSec -> {
                        SinglePartitionReadCommand base = full.apply(nowInSec);
                        Clustering<?> last = Clustering.make(ByteBufferUtil.bytes(resumeAt));
                        return base.forPaging(last, base.limits().forPaging(limit));
                    };
                    assertAllReadSurfacesMatch(ReadCase.of(profile + (reversed ? " reversed" : "") + " resume after " + resumeAt
                                                           + " limit " + limit, seed++).at(now), cfs, resumed);
                }
            }
        }

        // a first page with only the static row: a slice holding no live rows
        for (int pageSize : new int[]{ 1, 3 })
        {
            LongFunction<SinglePartitionReadCommand> staticOnly = nowInSec -> (SinglePartitionReadCommand)
                Util.cmd(cfs, 1L).withNowInSeconds(nowInSec).fromIncl(40L).toExcl(60L).build();
            assertPagedMatches(ReadCase.of(profile + " static-only first page", seed++).at(now), cfs, staticOnly, pageSize);
            assertCqlReplicaResponsesMatch(ReadCase.of(profile + " static-only first page", seed++).at(now).expectTranscode(true),
                                           cfs, "SELECT * FROM %s WHERE pk = 1 AND ck >= 40 AND ck < 60", pageSize);
        }
        assertEveryCaseServedLegs();
    }

    /** Every clustering value from below the first row to past the last: rows, deleted rows,
     *  range bounds and absent values, sampled evenly when there are too many. */
    private static List<Long> resumePoints()
    {
        TreeSet<Long> points = new TreeSet<>();
        for (long ck = -1; ck <= ROWS + 1; ck++)
            points.add(ck);
        for (long[] range : RANGES)
        {
            points.add(range[0]);
            points.add(range[1]);
        }
        for (long ck : DELETED_ROWS)
            points.add(ck);
        List<Long> all = new ArrayList<>(points);
        if (all.size() <= MAX_RESUME_POINTS)
            return all;
        List<Long> sample = new ArrayList<>(MAX_RESUME_POINTS);
        double step = all.size() / (double) MAX_RESUME_POINTS;
        for (int i = 0; i < MAX_RESUME_POINTS; i++)
            sample.add(all.get((int) (i * step)));
        sample.add(all.get(all.size() - 1));
        return sample;
    }
}
