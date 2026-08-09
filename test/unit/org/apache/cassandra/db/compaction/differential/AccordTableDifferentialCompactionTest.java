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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Accord-enabled user tables purge and expire relative to gcBefore, not wall-clock now:
 * CompactionIterator.purger() overrides nowInSec = controller.gcBefore when
 * metadata.isAccordEnabled() or migratingFromAccord(), deliberately deferring TTL-expiry
 * conversion and liveness purging so accord can still read the data at earlier timestamps and
 * during migration. For accord tables gcBefore itself is derived from the accord node's
 * durability bounds (CompactionTask.getCompactionController); with no transaction history that
 * derivation yields NO_GC, i.e. "expire and purge nothing". The cursor path must apply the
 * same nowInSec override.
 *
 * transactional_mode = 'test_unsafe' sets accordIsEnabled without routing plain CQL
 * reads/writes through accord; a real local AccordService is started because the gcBefore
 * derivation reads the node's durableBefore/redundantBefore state.
 */
public class AccordTableDifferentialCompactionTest extends DifferentialCompactionTester
{
    @BeforeClass
    public static void startAccord()
    {
        DatabaseDescriptor.setAccordTransactionsEnabled(true);
        AccordService.localStartup(ClusterMetadata.current().myNodeId());
        AccordService.distributedStartup();
    }

    @Test
    public void expiredCellsDeferToGcBefore() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000 AND transactional_mode = 'test_unsafe'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        long writeTimeSec = FBUtilities.nowInSeconds();

        // expiring rows (row liveness + cells carry the TTL) alongside plain ones
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?) USING TTL 1", ck, ck, "ttl-" + ck);
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?)", ck, ck);
        flush();

        // overlapping second generation, plus cell-level TTLs over previously live rows
        for (long ck = 5; ck < 10; ck++)
            execute("UPDATE %s USING TTL 1 SET v2 = ? WHERE pk = 1 AND ck = ?", "cell-ttl-" + ck, ck);
        for (long ck = 10; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (1, ?, ?)", ck, ck + 100);
        flush();

        // fixed "now" past every TTL=1 cell's expiration (writeTimeSec + 2), deterministically —
        // not relative to the accord gcBefore, which is NO_GC with no transaction history
        long fixedNow = writeTimeSec + 2;

        // NO_GC mirrors what the compaction scheduler passes for accord tables
        // (CompactionTask.getCompactionController asserts gcBefore <= 0 before deriving)
        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskWithFixedNow(fixedNow), CompactionManager.NO_GC);
    }
}
