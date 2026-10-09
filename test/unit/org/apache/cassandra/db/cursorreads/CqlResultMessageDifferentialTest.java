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
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The CQL result surface (S3 of {@link CursorReadOracle}): the {@code ResultMessage.Rows} bytes at
 * protocol v5 and the paging state of every page, for single-partition SELECTs run through
 * {@code SelectStatement.execute} at CL ONE.  Covers LIMIT, PER PARTITION LIMIT, GROUP BY,
 * {@code count(*)}, WRITETIME and TTL, collection element and slice selection, clustering and
 * regular column filters, IN on the partition key and the clustering, multi-column slices, forward
 * and reversed order, at page sizes 1, 2, 7, 100 and unpaged.  Data is written as all-BIG, all-BTI
 * and mixed sstables plus a memtable, with deletions and TTLs.  Each query also runs with every read
 * served as a replica response, the path a remote replica takes, where the transcode path must
 * serve every read it does not decline by design.  Also: every page size, counters, and the
 * tombstone thresholds.
 */
public class CqlResultMessageDifferentialTest extends CursorReadOracle
{
    private static final String[] PROFILES = { "big", "bti", "mixed" };
    private static final int[] PAGE_SIZES = { 0, 1, 2, 7, 100 };

    private static final String[] QUERIES = {
        "SELECT * FROM %s WHERE pk = 1",
        "SELECT * FROM %s WHERE pk = 1 ORDER BY c1 DESC, c2 DESC",
        "SELECT * FROM %s WHERE pk = 1 LIMIT 5",
        "SELECT * FROM %s WHERE pk = 1 ORDER BY c1 DESC, c2 DESC LIMIT 5",
        "SELECT * FROM %s WHERE pk = 1 LIMIT 1",
        "SELECT * FROM %s WHERE pk IN (1, 2) PER PARTITION LIMIT 3",
        "SELECT * FROM %s WHERE pk IN (1, 2) PER PARTITION LIMIT 2 LIMIT 3",
        "SELECT pk, c1, count(*) FROM %s WHERE pk = 1 GROUP BY pk, c1",
        "SELECT pk, c1, max(v1) FROM %s WHERE pk = 1 GROUP BY pk, c1 LIMIT 3",
        "SELECT pk, c1, count(*) FROM %s WHERE pk = 1 GROUP BY pk, c1 ORDER BY c1 DESC",
        "SELECT count(*) FROM %s WHERE pk = 1",
        "SELECT count(*) FROM %s WHERE pk = 1 AND c1 >= 2 AND c1 < 7",
        "SELECT c1, c2, WRITETIME(v1), TTL(v2) FROM %s WHERE pk = 1",
        "SELECT c1, c2, WRITETIME(v2), TTL(v2) FROM %s WHERE pk = 1 ORDER BY c1 DESC, c2 DESC",
        "SELECT c1, c2, m[1], m[2..3], st['a'] FROM %s WHERE pk = 1",
        "SELECT c1, c2, l, WRITETIME(l), TTL(m) FROM %s WHERE pk = 1",
        "SELECT s, v1 FROM %s WHERE pk = 1",
        "SELECT DISTINCT pk, s FROM %s WHERE pk = 1",
        "SELECT * FROM %s WHERE pk = 1 AND c1 >= 2 AND c1 < 8",
        "SELECT * FROM %s WHERE pk = 1 AND c1 > 2 AND c1 <= 8 ORDER BY c1 DESC, c2 DESC",
        "SELECT * FROM %s WHERE pk = 1 AND c1 IN (1, 3, 5, 11)",
        "SELECT * FROM %s WHERE pk = 1 AND c1 = 4 AND c2 IN (0, 2, 9)",
        "SELECT * FROM %s WHERE pk = 1 AND (c1, c2) > (2, 3) AND (c1, c2) <= (6, 1)",
        "SELECT * FROM %s WHERE pk = 1 AND c2 > 3 ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND c2 = 2 LIMIT 3 ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND v1 > 30 ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND v1 > 30 LIMIT 4 ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND st CONTAINS 'a' ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND m CONTAINS KEY 2 LIMIT 2 ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND s = 's1' ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 2 AND s = 's1' ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 3 AND s = 'none' ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 PER PARTITION LIMIT 3",
        "SELECT * FROM %s WHERE pk = 1 AND c1 > 2 PER PARTITION LIMIT 5 LIMIT 3",
        "SELECT pk, c1, c2, count(*) FROM %s WHERE pk IN (1, 2) GROUP BY pk, c1 LIMIT 4",
        "SELECT pk, c1, count(*) FROM %s WHERE pk = 1 GROUP BY pk, c1 PER PARTITION LIMIT 2",
        "SELECT * FROM %s WHERE pk = 1 AND v2 = 'ttl' ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND v1 > 30 AND c2 = 1 ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND v1 > 30 AND st CONTAINS 'b1' ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND m CONTAINS 'two-2' ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 1 AND c1 >= 3 AND v1 < 50 LIMIT 2 ALLOW FILTERING",
        "SELECT * FROM %s WHERE pk = 3",
        "SELECT * FROM %s WHERE pk = 4",
    };

    /** Queries the transcode path declines: reverse order, names, and a GROUP BY page that resumes
     *  in a new partition with its limit already reached.  Every other read must be written by it. */
    private static boolean transcodeServesEveryRead(String query)
    {
        return !query.contains("DESC")
               && !query.contains("c2 IN")
               && !query.contains("pk IN (1, 2) GROUP BY");
    }

    private SSTableFormat<?, ?> originalFormat;
    private DataStorageSpec.LongBytesBound originalReadSizeWarn, originalReadSizeFail;

    @Before
    public void saveFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        // The test configuration sets local read size thresholds, which production leaves unset.
        // The transcode path declines a read that tracks them; readSizeThresholdsDecline checks that.
        originalReadSizeWarn = DatabaseDescriptor.getLocalReadSizeWarnThreshold();
        originalReadSizeFail = DatabaseDescriptor.getLocalReadSizeFailThreshold();
        DatabaseDescriptor.setLocalReadSizeWarnThreshold(null);
        DatabaseDescriptor.setLocalReadSizeFailThreshold(null);
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setLocalReadSizeWarnThreshold(originalReadSizeWarn);
        DatabaseDescriptor.setLocalReadSizeFailThreshold(originalReadSizeFail);
    }

    /** With local read size thresholds set, a read that tracks warnings measures the heap size of
     *  each row object, so the transcode path declines it; the response still matches. */
    @Test
    public void readSizeThresholdsDecline()
    {
        DatabaseDescriptor.setLocalReadSizeWarnThreshold(originalReadSizeWarn);
        DatabaseDescriptor.setLocalReadSizeFailThreshold(originalReadSizeFail);
        createTable("CREATE TABLE %s (pk bigint, c1 int, c2 int, v1 bigint, PRIMARY KEY (pk, c1, c2))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int c1 = 0; c1 < 10; c1++)
            execute("INSERT INTO %s (pk, c1, c2, v1) VALUES (1, ?, 0, ?)", c1, (long) c1);
        flush();
        long seed = 3;
        for (int pageSize : PAGE_SIZES)
            assertCqlReplicaResponsesMatch(ReadCase.of("read size thresholds", seed++).expectTranscode(false), cfs,
                                           "SELECT * FROM %s WHERE pk = 1", pageSize);
    }

    private static void selectFormat(String profile, int layer)
    {
        String format = profile.equals("mixed") ? (layer % 2 == 0 ? "big" : "bti") : profile;
        DatabaseDescriptor.setSelectedSSTableFormat(format);
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

    private void run(String profile)
    {
        createTable("CREATE TABLE %s (pk bigint, c1 int, c2 int, s text static, v1 bigint, v2 text, " +
                    "l list<int>, st set<text>, m map<int, text>, PRIMARY KEY (pk, c1, c2)) WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        long writeTime = FBUtilities.nowInSeconds();

        for (int layer = 0; layer < 3; layer++)
        {
            selectFormat(profile, layer);
            long ts = 1000 + layer * 1000;
            for (long pk = 1; pk <= 2; pk++)
            {
                for (int c1 = layer; c1 < 12; c1 += 2)
                    for (int c2 = 0; c2 < 4; c2++)
                        execute("INSERT INTO %s (pk, c1, c2, v1, v2, l, st, m) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP " + ts,
                                pk, c1, c2, (long) (c1 * 10 + c2 + layer), "v-" + layer + "-" + c1 + "-" + c2,
                                List.of(c1, c2), Set.of("a", "b" + c2), Map.of(1, "one", 2, "two-" + c2, 3, "three"));
                execute("UPDATE %s USING TIMESTAMP " + ts + " SET s = ? WHERE pk = ?", "s" + layer, pk);
            }
            if (layer == 1)
            {
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = 1 AND c1 = 3 AND c2 = 1");
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = 1 AND c1 = 5");
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = 1 AND c1 = 7 AND c2 >= 1 AND c2 < 3");
                execute("DELETE v1 FROM %s USING TIMESTAMP " + ts + " WHERE pk = 1 AND c1 = 9 AND c2 = 0");
                execute("DELETE m[1] FROM %s USING TIMESTAMP " + ts + " WHERE pk = 1 AND c1 = 1 AND c2 = 2");
                execute("UPDATE %s USING TIMESTAMP " + ts + " AND TTL 100000 SET v2 = 'ttl' WHERE pk = 1 AND c1 = 1 AND c2 = 3");
                execute("UPDATE %s USING TIMESTAMP " + ts + " AND TTL 100000 SET m = m + {4: 'four'} WHERE pk = 1 AND c1 = 3 AND c2 = 0");
            }
            flush();
        }
        // memtable
        execute("UPDATE %s USING TIMESTAMP 5000 SET v1 = 999 WHERE pk = 1 AND c1 = 2 AND c2 = 2");
        execute("DELETE FROM %s USING TIMESTAMP 5000 WHERE pk = 1 AND c1 = 10 AND c2 = 3");
        execute("UPDATE %s USING TIMESTAMP 5000 SET s = 's1' WHERE pk = 2");
        // a partition with no static row whose rows are all deleted, and one only in the memtable
        execute("INSERT INTO %s (pk, c1, c2, v1) VALUES (3, 1, 1, 1) USING TIMESTAMP 1000");
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 3 AND c1 = 1");
        execute("INSERT INTO %s (pk, c1, c2, v1, v2) VALUES (4, 0, 0, 4, 'memtable only') USING TIMESTAMP 5000");

        long seed = profile.hashCode();
        for (boolean memtableOnly : new boolean[]{ false, true })
        {
            for (long now : new long[]{ writeTime + 60, writeTime + 2 * 24 * 3600 })
            {
                for (String query : QUERIES)
                {
                    // the partition held only by the memtable has no sstable leg to serve
                    if (query.contains("pk = 4") != memtableOnly)
                        continue;
                    for (int pageSize : PAGE_SIZES)
                    {
                        assertCqlMatches(ReadCase.of(profile, seed++).at(now), cfs, query, pageSize);
                        ReadCase replica = ReadCase.of(profile, seed++).at(now);
                        if (transcodeServesEveryRead(query))
                            replica = replica.expectTranscode(true);
                        assertCqlReplicaResponsesMatch(replica, cfs, query, pageSize);
                    }
                }
            }
            if (!memtableOnly)
                assertEveryCaseServedLegs();
        }
    }

    /**
     * Keys no sstable holds, inside the key range of 0, 1 and several sstables, with and without
     * memtable data for other keys, alone and in an IN with a key that exists.
     */
    @Test
    public void absentPartitions()
    {
        for (String profile : PROFILES)
        {
            for (boolean memtable : new boolean[]{ false, true })
            {
                createTable("CREATE TABLE %s (pk bigint, c1 int, c2 int, s text static, v1 bigint, PRIMARY KEY (pk, c1, c2))");
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
                        for (int c1 = 0; c1 < 4; c1++)
                            execute("INSERT INTO %s (pk, c1, c2, v1) VALUES (?, ?, 0, ?) USING TIMESTAMP " + (1000 + layer), pk, c1, (long) c1);
                    }
                    flush();
                }
                if (memtable)
                {
                    for (int rank : new int[]{ 150, 700, 1600 })
                    {
                        written.add(layout.at(rank));
                        execute("INSERT INTO %s (pk, c1, c2, v1) VALUES (?, 1, 1, 1) USING TIMESTAMP 6000", layout.at(rank));
                    }
                }
                Map<String, Long> keys = layout.absentKeys(cfs, written.stream().mapToLong(Long::longValue).toArray());
                long present = layers[0][0];
                long seed = profile.hashCode() + (memtable ? 1 : 0);
                for (Map.Entry<String, Long> key : keys.entrySet())
                {
                    long pk = key.getValue();
                    String[] queries = {
                        "SELECT * FROM %s WHERE pk = " + pk,
                        "SELECT * FROM %s WHERE pk = " + pk + " ORDER BY c1 DESC, c2 DESC",
                        "SELECT * FROM %s WHERE pk = " + pk + " LIMIT 1",
                        "SELECT * FROM %s WHERE pk = " + pk + " AND c1 >= 1 AND c1 < 3",
                        "SELECT * FROM %s WHERE pk = " + pk + " AND c1 IN (1, 2)",
                        "SELECT count(*) FROM %s WHERE pk = " + pk,
                        "SELECT * FROM %s WHERE pk = " + pk + " AND v1 > 0 ALLOW FILTERING",
                        "SELECT * FROM %s WHERE pk IN (" + pk + ", " + present + ")",
                    };
                    String label = profile + (memtable ? " memtable " : " ") + key.getKey();
                    for (String query : queries)
                    {
                        for (int pageSize : new int[]{ 0, 1, 3 })
                        {
                            assertCqlMatches(ReadCase.of(label, seed++), cfs, query, pageSize);
                            assertCqlReplicaResponsesMatch(ReadCase.of(label, seed++), cfs, query, pageSize);
                        }
                    }
                }
            }
        }
    }

    /** Every page size from 1 to past the result size, through replica responses. */
    @Test
    public void everyPageSize()
    {
        createTable("CREATE TABLE %s (pk bigint, c1 int, c2 int, s text static, v1 bigint, v2 text, PRIMARY KEY (pk, c1, c2)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        long writeTime = FBUtilities.nowInSeconds();
        for (int layer = 0; layer < 2; layer++)
        {
            selectFormat("mixed", layer);
            for (int c1 = layer; c1 < 10; c1 += 2)
                for (int c2 = 0; c2 < 3; c2++)
                    execute("INSERT INTO %s (pk, c1, c2, v1, v2) VALUES (1, ?, ?, ?, ?) USING TIMESTAMP " + (1000 + layer),
                            c1, c2, (long) (c1 * 10 + c2), "v" + c1);
            execute("UPDATE %s USING TIMESTAMP " + (1000 + layer) + " SET s = ? WHERE pk = 1", "s" + layer);
            flush();
        }
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = 1 AND c1 >= 3 AND c1 < 5");
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = 1 AND c1 = 6 AND c2 = 1");
        String[] queries = {
            "SELECT * FROM %s WHERE pk = 1",
            "SELECT * FROM %s WHERE pk = 1 AND c1 > 1",
            "SELECT * FROM %s WHERE pk = 1 AND v1 > 20 ALLOW FILTERING",
            "SELECT pk, c1, count(*) FROM %s WHERE pk = 1 GROUP BY pk, c1",
        };
        long seed = 42;
        for (String query : queries)
            for (int pageSize = 1; pageSize <= 32; pageSize++)
                assertCqlReplicaResponsesMatch(ReadCase.of("every page size", seed++).at(writeTime + 60).expectTranscode(true),
                                               cfs, query, pageSize);
        assertEveryCaseServedLegs();
    }

    @Test
    public void counterTable()
    {
        for (String profile : PROFILES)
        {
            createTable("CREATE TABLE %s (pk bigint, ck int, c1 counter, c2 counter, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int layer = 0; layer < 3; layer++)
            {
                selectFormat(profile, layer);
                for (int ck = layer; ck < 30; ck += 2)
                    execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = 1 AND ck = ?", (long) (ck + layer), 1L, ck);
                if (layer == 1)
                    execute("DELETE FROM %s WHERE pk = 1 AND ck = 5");
                flush();
            }
            for (int ck = 0; ck < 30; ck += 7)
                execute("UPDATE %s SET c1 = c1 + 1000 WHERE pk = 1 AND ck = ?", ck);
            String[] queries = {
                "SELECT * FROM %s WHERE pk = 1",
                "SELECT * FROM %s WHERE pk = 1 LIMIT 4",
                "SELECT ck, c1 FROM %s WHERE pk = 1 AND ck > 3",
                "SELECT * FROM %s WHERE pk = 1 AND c1 > 10 ALLOW FILTERING",
                "SELECT * FROM %s WHERE pk = 1 ORDER BY ck DESC LIMIT 3",
            };
            long seed = profile.hashCode();
            for (String query : queries)
            {
                for (int pageSize : PAGE_SIZES)
                {
                    assertCqlMatches(ReadCase.of("counters " + profile, seed++), cfs, query, pageSize);
                    ReadCase replica = ReadCase.of("counters " + profile, seed++);
                    if (transcodeServesEveryRead(query))
                        replica = replica.expectTranscode(true);
                    assertCqlReplicaResponsesMatch(replica, cfs, query, pageSize);
                }
            }
            assertEveryCaseServedLegs();
        }
    }

    /** The tombstone warn and fail thresholds just below, at and just above the tombstones a read
     *  scans: the warning text and the abort must match. */
    @Test
    public void tombstoneThresholds()
    {
        int warn = DatabaseDescriptor.getTombstoneWarnThreshold();
        int fail = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            createTable("CREATE TABLE %s (pk bigint, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            selectFormat("bti", 0);
            for (int ck = 0; ck < 40; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP 1000", ck, ck, ck);
            flush();
            selectFormat("big", 1);
            for (int ck = 0; ck < 40; ck += 3)
                execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck = ?", ck);
            for (int ck = 1; ck < 40; ck += 5)
                execute("DELETE v1 FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck = ?", ck);
            execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck > 30 AND ck < 35");
            flush();
            String[] queries = {
                "SELECT * FROM %s WHERE pk = 1",
                "SELECT * FROM %s WHERE pk = 1 LIMIT 10",
                "SELECT * FROM %s WHERE pk = 1 AND v2 > 5 ALLOW FILTERING",
            };
            long seed = 7;
            for (int threshold = 1; threshold <= 26; threshold++)
            {
                DatabaseDescriptor.setTombstoneWarnThreshold(threshold);
                DatabaseDescriptor.setTombstoneFailureThreshold(threshold + 4);
                for (String query : queries)
                {
                    for (int pageSize : new int[]{ 0, 3 })
                    {
                        String label = "tombstone warn " + threshold + " fail " + (threshold + 4);
                        assertCqlMatches(ReadCase.of(label, seed++), cfs, query, pageSize);
                        assertCqlReplicaResponsesMatch(ReadCase.of(label, seed++).expectTranscode(true), cfs, query, pageSize);
                    }
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setTombstoneWarnThreshold(warn);
            DatabaseDescriptor.setTombstoneFailureThreshold(fail);
        }
    }
}
