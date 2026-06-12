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

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Differential coverage for COUNTER tables (increment 5, H9 corpus). Counter merge differs
 * from regular reconciliation in two load-bearing ways the scenarios target:
 *
 *  - live + live counter cells MERGE their contexts (CounterContext semantics) instead of
 *    one side winning; the resulting timestamp is the max of the contributors;
 *  - a counter TOMBSTONE beats a live counter cell REGARDLESS of timestamps
 *    (CASSANDRA-7346), so shadowed inputs must be excluded before any context merge.
 *
 * Single-JVM CQL produces single-CounterId global-shard contexts only; the multi-shard,
 * local/remote, and marked-to-clear shapes are pinned at the unit level by
 * CursorCounterContextMergeTest against the upstream CounterContext implementation.
 */
public class CounterDifferentialCompactionTest extends DifferentialCompactionTester
{
    /** Same counter cells incremented across many sstables: pure context-merge folds. */
    @Test
    public void shardMergesAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 4; round++)
        {
            for (long pk = 0; pk < 4; pk++)
                for (long ck = 0; ck < 10; ck++)
                {
                    execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", ck + round, pk, ck);
                    if (ck % 3 == 0)
                        execute("UPDATE %s SET c2 = c2 + ? WHERE pk = ? AND ck = ?", -1L - round, pk, ck);
                }
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Counter tombstones (DELETE c / row deletes) interleaved with increments across
     * sstables — the CASSANDRA-7346 rule: the tombstone wins against live counter cells
     * regardless of timestamp order, so increments AFTER the delete (higher ts) still lose.
     */
    @Test
    public void counterTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 12; ck++)
        {
            execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = ? AND ck = ?", ck, 100 + ck, 1L, ck);
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", ck, 2L, ck);
        }
        flush();

        // cell deletes: tombstone newer than the increments above
        for (long ck = 0; ck < 6; ck++)
            execute("DELETE c1 FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        // row deletes over counter rows
        execute("DELETE FROM %s WHERE pk = 2 AND ck = 3");
        execute("DELETE FROM %s WHERE pk = 2 AND ck = 4");
        flush();

        // increments NEWER than the tombstones: 7346 — the tombstone still wins for c1;
        // c2 (never deleted) keeps merging
        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = ? AND ck = ?", 1000 + ck, ck, 1L, ck);
        execute("UPDATE %s SET c1 = c1 + 99 WHERE pk = 2 AND ck = 3");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Static counters next to clustered counters, incl. deletes of each. */
    @Test
    public void staticCounters() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, s counter static, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 5; pk++)
            {
                execute("UPDATE %s SET s = s + ? WHERE pk = ?", pk + round + 1, pk);
                for (long ck = 0; ck < 6; ck++)
                    execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", ck + round, pk, ck);
            }
            flush();
        }
        execute("DELETE s FROM %s WHERE pk = 3");
        execute("DELETE c FROM %s WHERE pk = 4 AND ck = 2");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Counter columns in partitions wide enough to cross index blocks (4KiB test config). */
    @Test
    public void countersAcrossIndexBlocks() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, c3 counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 400; ck++)
                execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ?, c3 = c3 + ? WHERE pk = ? AND ck = ?",
                        ck, ck * 31, -ck, 1L, ck);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Purge boundary for counter tombstones: explicit gcBefore at and past the deletion second. */
    @Test
    public void counterTombstonePurge() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 8; ck++)
            execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", ck + 1, 1L, ck);
        flush();
        for (long ck = 0; ck < 4; ck++)
            execute("DELETE c FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        flush();

        long maxLdt = Long.MIN_VALUE;
        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : cfs.getLiveSSTables())
        {
            long ldt = sstable.getSSTableMetadata().maxLocalDeletionTime;
            if (ldt != Long.MAX_VALUE)
                maxLdt = Math.max(maxLdt, ldt);
        }
        org.junit.Assert.assertTrue("scenario produced no deletion times", maxLdt > 0 && maxLdt < Long.MAX_VALUE);

        // retained at the boundary, purged one past — note 7346: the purged tombstone's
        // shadowed counter cells must STAY dead (they were excluded before merge)
        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt);
        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt + 1);
    }
}
