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


import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Differential coverage for MATERIALIZED VIEW tables: view tables are admitted
 * by the cursor gate (no isView rejection) and compact through the same pipelines, but their
 * row shapes are view-specific — strict liveness (CursorCompactor.enforceStrictLiveness
 * mirrors PurgeFunction), view-generated row tombstones when a base row moves between view
 * partitions, and expired-liveness markers. Shadowable row deletions do NOT appear: modern
 * view maintenance stopped producing them (CASSANDRA-13409) and the cursor reader fail-louds
 * if legacy data ever carries the flag.
 */
public class MaterializedViewDifferentialCompactionTest extends DifferentialCompactionTester
{

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Test
    public void materializedViewTable() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1 FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();

        for (long pk = 0; pk < 6; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, pk * 100 + ck, "x" + ck);
        flush(KEYSPACE, view);

        // view-partition moves (v1 changes: old view row dies via view tombstone, new row
        // appears) and base-row deletes (view row tombstones)
        for (long pk = 0; pk < 6; pk += 2)
        {
            execute("UPDATE %s SET v1 = ? WHERE pk = ? AND ck = ?", pk * 100 + 77, pk, 0L);
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, 1L);
        }
        flush(KEYSPACE, view);

        // a second wave of moves on already-moved rows: tombstones meeting tombstones
        execute("UPDATE %s SET v1 = ? WHERE pk = ? AND ck = ?", 999L, 0L, 0L);
        flush(KEYSPACE, view);

        assertCursorMatchesIteratorAcrossGenerations(viewCfs);
    }

    /**
     * MV + TTL: the view-generated row liveness must track the base row's TTL, including
     * across a view-partition move (v1 changes -> old view row dies, new one carries its own
     * TTL). Recent bugs in this area (CASSANDRA-21152) motivate dedicated coverage rather
     * than relying on materializedViewTable's TTL-free scenario.
     */
    @Test
    public void materializedViewWithTTL() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1 FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();

        long writeTimeSec = FBUtilities.nowInSeconds();

        // short-TTL rows: base row AND its view row will have expired by fixedNow below
        for (long pk = 0; pk < 6; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL 1", pk, ck, pk * 100 + ck, "x" + ck);
        flush(KEYSPACE, view);

        // long-TTL rows: still alive at fixedNow, exercises live-TTL view rows alongside dead ones
        for (long pk = 10; pk < 16; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL 86400", pk, ck, pk * 100 + ck, "y" + ck);
        flush(KEYSPACE, view);

        // view-partition move on a short-TTL row, itself given a new (long) TTL: the old view
        // row's move-tombstone and its own now-irrelevant short TTL race the new view row's
        // fresh long TTL — the MV+TTL interaction CASSANDRA-21152-style bugs live in
        for (long pk = 0; pk < 6; pk += 2)
            execute("UPDATE %s USING TTL 86400 SET v1 = ? WHERE pk = ? AND ck = ?", pk * 100 + 77, pk, 0L);
        flush(KEYSPACE, view);

        long fixedNow = writeTimeSec + 3; // past the short TTLs' expiry; long TTLs still alive
        assertCursorMatchesIteratorAcrossGenerations(viewCfs, () -> fixedNow);
    }
}
