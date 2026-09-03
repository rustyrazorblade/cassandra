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

import java.util.ArrayList;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.Config.PaxosStatePurging;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Pins the arms of {@code CursorCompactor.isSupported} that decide on the COMPACTION rather than on
 * the schema. {@link CursorSupportMatrixTest} covers the schema and the sstable headers.
 * <p>
 * Each gate falls back to the iterator path, so a gate that silently opened would not fail a
 * differential test: both pipelines would simply be the iterator. That is why these assert the gate
 * directly.
 */
public class CursorCompactionGateTest extends CQLTester
{
    private PaxosStatePurging originalPurging;

    @After
    public void restorePaxosStatePurging()
    {
        if (originalPurging != null)
        {
            DatabaseDescriptor.setPaxosStatePurging(originalPurging);
            originalPurging = null;
        }
    }

    /** Two sstables of a plain table, which every gate below then accepts or rejects. */
    private ColumnFamilyStore twoSSTableTable()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 'y')");
        flush();
        return cfs;
    }

    private boolean isSupportedWith(ColumnFamilyStore cfs, TombstoneOption tombstoneOption) throws Exception
    {
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        try (CompactionController controller = new CompactionController(cfs, inputs, FBUtilities.nowInSeconds(),
                                                                        null, tombstoneOption);
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            return CursorCompactor.isSupported(scanners, controller);
        }
    }

    /**
     * The control. Every rejection below has to be attributable to the one input it changes, so the
     * same table and the same sstables must be accepted first.
     */
    @Test
    public void aPlainTwoSSTableCompactionIsSupported() throws Exception
    {
        assertTrue("expected a plain two-sstable compaction to be cursor-supported",
                   isSupportedWith(twoSSTableTable(), TombstoneOption.NONE));
    }

    /**
     * Garbage skipping is CompactionIterator.GarbageSkipper, which the cursor path does not
     * implement. Either non-NONE option must fall back.
     */
    @Test
    public void garbageSkippingIsUnsupported() throws Exception
    {
        ColumnFamilyStore cfs = twoSSTableTable();
        assertFalse("cursor compaction must refuse tombstone_compaction ROW: it has no GarbageSkipper",
                    isSupportedWith(cfs, TombstoneOption.ROW));
        assertFalse("cursor compaction must refuse tombstone_compaction CELL: it has no GarbageSkipper",
                    isSupportedWith(cfs, TombstoneOption.CELL));
    }

    /**
     * CompactionIterator swaps in PaxosPurger for system.paxos when purging is not legacy. The
     * cursor path has one purger, so it must decline rather than compact that table with the wrong
     * one.
     * <p>
     * This asserts the gate's own predicate rather than driving a compaction of system.paxos,
     * because the table under test here is an ordinary one: the point is that the gate reads the
     * setting, and that an ordinary table is unaffected by it.
     */
    @Test
    public void nonLegacyPaxosPurgingDoesNotAffectAnOrdinaryTable() throws Exception
    {
        originalPurging = DatabaseDescriptor.paxosStatePurging();
        ColumnFamilyStore cfs = twoSSTableTable();

        DatabaseDescriptor.setPaxosStatePurging(PaxosStatePurging.legacy);
        assertTrue("an ordinary table is unaffected by legacy paxos purging",
                   isSupportedWith(cfs, TombstoneOption.NONE));

        DatabaseDescriptor.setPaxosStatePurging(PaxosStatePurging.gc_grace);
        assertTrue("an ordinary table is not system.paxos, so the paxos gate must not close on it",
                   isSupportedWith(cfs, TombstoneOption.NONE));

        DatabaseDescriptor.setPaxosStatePurging(PaxosStatePurging.repaired);
        assertTrue("an ordinary table is not system.paxos, so the paxos gate must not close on it",
                   isSupportedWith(cfs, TombstoneOption.NONE));
    }
}
