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


import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Exact purge-boundary scenarios plus configuration-axis variants (compression algorithms,
 * compaction disk access mode). Purge requires localDeletionTime < gcBefore; instead of
 * freezing the clock, scenarios read the REAL deletion time from the flushed sstable's stats
 * and pass an explicit gcBefore placed exactly at, and one past, that boundary.
 */
public class PurgeBoundaryDifferentialCompactionTest extends DifferentialCompactionTester
{

    /** gcBefore exactly AT the tombstone's deletion time: NOT purgeable (strict less-than). */
    @Test
    public void boundaryExactlyAtDeletionTime() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 100", pk, ck, "v" + ck);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 1");
        for (long ck = 0; ck < 5; ck++)
            execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 2 AND ck = ?", ck);
        flush();

        // sstables with no tombstones report the NO_DELETION sentinel (Long.MAX_VALUE) as their
        // max local deletion time — use the min across sstables that DO have deletions, which
        // for this scenario is the single deletion second of the tombstone-only sstable
        long maxLdt = Long.MIN_VALUE;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            long ldt = sstable.getSSTableMetadata().maxLocalDeletionTime;
            if (ldt != Long.MAX_VALUE)
                maxLdt = Math.max(maxLdt, ldt);
        }
        assertTrue("scenario produced no tombstone deletion times", maxLdt > 0 && maxLdt < Long.MAX_VALUE);

        // boundary: ldt == gcBefore -> NOT purgeable (purge requires ldt < gcBefore)
        CapturedOutput at = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt);
        assertTrue("tombstones at exactly gcBefore must be retained",
                   allJson(at).contains("\"marked_deleted\":\"200\""));

        // one past: ldt < gcBefore -> purgeable, shadowed data and tombstones both gone
        CapturedOutput past = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt + 1);
        assertFalse("tombstones strictly before gcBefore must purge",
                    allJson(past).contains("\"marked_deleted\":\"200\""));
    }

    /**
     * A PURGEABLE complex (collection) deletion must still shadow the cells it covers
     * before it is dropped: the iterator applies the deletion at merge time
     * (Row.Merger.ColumnDataReducer) and purges it afterwards (ComplexColumnData.purge),
     * so both the deletion AND the older cells it deletes vanish. A resolver that purges
     * the merged complex deletion BEFORE using it as the cells' shadow resurrects the
     * deleted cells. Cells and deletion live in DIFFERENT sstables so the merge, not the
     * memtable, must apply the shadow.
     */
    @Test
    public void purgeableComplexDeletionShadowsCells() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // flanking permanent data so outputs are never empty
        for (long ck = 0; ck < 4; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 100", 0L, ck, "keep" + ck);
        // the doomed cells: complex cells only, no row liveness (UPDATE, not INSERT)
        for (long ck = 0; ck < 4; ck++)
            execute("UPDATE %s USING TIMESTAMP 100 SET m['k1'] = ?, m['k2'] = ? WHERE pk = ? AND ck = ?",
                    "doomedA" + ck, "doomedB" + ck, 1L, ck);
        flush();

        // the complex deletion, alone in its own sstable, newer than the cells
        for (long ck = 0; ck < 4; ck++)
            execute("DELETE m FROM %s USING TIMESTAMP 200 WHERE pk = ? AND ck = ?", 1L, ck);
        flush();

        long maxLdt = Long.MIN_VALUE;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            long ldt = sstable.getSSTableMetadata().maxLocalDeletionTime;
            if (ldt != Long.MAX_VALUE)
                maxLdt = Math.max(maxLdt, ldt);
        }
        assertTrue("scenario produced no deletion times", maxLdt > 0 && maxLdt < Long.MAX_VALUE);

        // NOT purgeable (ldt == gcBefore): deletion retained, doomed cells shadowed — both gone
        CapturedOutput at = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt);
        assertTrue("retained complex deletion must be in the output",
                   allJson(at).contains("\"marked_deleted\":\"200\""));
        assertFalse("shadowed cells must not survive a retained complex deletion",
                    allJson(at).contains("doomed"));

        // purgeable (ldt < gcBefore): deletion purged AND the cells it shadowed stay gone
        CapturedOutput past = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt + 1);
        assertFalse("purged complex deletion must not be in the output",
                    allJson(past).contains("\"marked_deleted\":\"200\""));
        assertFalse("cells shadowed by a purged complex deletion must not be resurrected",
                    allJson(past).contains("doomed"));
    }

    /** Same differential flow under direct I/O for the compaction read path. */
    @Test
    public void directDiskAccessMode() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 10; pk++)
                for (long ck = 0; ck < 15; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r" + round + "v" + ck);
            execute("DELETE FROM %s WHERE pk = ? AND ck >= 3 AND ck < 9", (long) round);
            flush();
        }

        DiskAccessMode original = DatabaseDescriptor.getCompactionReadDiskAccessMode();
        DatabaseDescriptor.setCompactionReadDiskAccessMode(DiskAccessMode.direct);
        try
        {
            assertCursorMatchesIterator(cfs);
        }
        finally
        {
            DatabaseDescriptor.setCompactionReadDiskAccessMode(original);
        }
    }

    /** Output equivalence must hold for every compressor, not just the default LZ4. */
    @Test
    public void compressionVariants() throws Exception
    {
        for (String compression : new String[]{ "ZstdCompressor", "DeflateCompressor", "SnappyCompressor" })
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                        "WITH gc_grace_seconds = 864000 AND compression = {'class': '" + compression + "'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            for (int round = 0; round < 2; round++)
            {
                for (long pk = 0; pk < 8; pk++)
                    for (long ck = 0; ck < 10; ck++)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, compression + round + ck);
                execute("DELETE FROM %s WHERE pk = ? AND ck >= 2 AND ck < 6", (long) round);
                flush();
            }

            assertCursorMatchesIterator(cfs);
        }
    }
}
