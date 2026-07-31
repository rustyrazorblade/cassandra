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

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.repair.ValidationCompactionController;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Pins where {@code CursorCompactor}'s three support gates deliberately DISAGREE, so a divergence
 * stays a recorded decision rather than decaying into an oversight nobody can date.
 * <p>
 * Materialized views are the live case: all three gates face the same underlying risk (a legacy
 * view sstable carrying shadowable row deletions, which {@code SSTableCursorReader} rejects
 * mid-read) but not the same consequence. Validation rejects views because a mid-merge failure
 * fails an entire repair session. Compaction and cleanup admit them because a mid-merge failure
 * costs one sstable's rewrite: the transaction rolls back, the original survives untouched, and
 * the operation can simply be rerun. Cleanup follows compaction because cleanup IS a compaction.
 */
public class CursorCleanupSupportPostureTest extends CQLTester
{
    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    private static CompactionController cleanupControllerFor(ColumnFamilyStore cfs)
    {
        return new CompactionController(cfs, cfs.getLiveSSTables(), cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
    }

    /**
     * {@code isValidationSupported} additionally requires a controller that guarantees no shadow
     * sources, which only {@link ValidationCompactionController} does - so the validation gate has
     * to be probed with the real thing, or every table would appear "rejected" for that reason and
     * the view-specific assertions below would prove nothing.
     */
    private static ValidationCompactionController validationControllerFor(ColumnFamilyStore cfs)
    {
        return new ValidationCompactionController(cfs, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
    }

    @Test
    public void materializedViewIsAdmittedByCleanupAndRejectedByValidation() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1 FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();

        for (long pk = 0; pk < 6; pk++)
            for (long ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, pk * 100 + ck);
        flush(KEYSPACE, view);
        assertTrue("scenario setup: the view must have flushed an sstable", !viewCfs.getLiveSSTables().isEmpty());
        assertTrue("scenario setup: this must be a view table", viewCfs.metadata().isView());

        try (CompactionController cleanup = cleanupControllerFor(viewCfs);
             ValidationCompactionController validation = validationControllerFor(viewCfs))
        {
            assertTrue("cursor cleanup deliberately admits materialized views, siding with regular " +
                       "compaction: a mid-merge failure costs one sstable's rewrite, not a repair session",
                       CursorCompactor.isCleanupSupported(viewCfs.getLiveSSTables(), cleanup));

            assertFalse("cursor validation deliberately rejects materialized views: a mid-merge failure " +
                        "there fails an entire repair session, which retrying does not fix",
                        CursorCompactor.isValidationSupported(viewCfs.getLiveSSTables(), validation));
        }
    }

    /**
     * The same two gates on a plain table must BOTH admit it - otherwise the view assertions above
     * would pass for some unrelated reason (a version gate, a controller capability) and prove
     * nothing about views at all.
     */
    @Test
    public void plainTableIsAdmittedByBothGates() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 6; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, pk);
        flush();

        try (CompactionController cleanup = cleanupControllerFor(cfs);
             ValidationCompactionController validation = validationControllerFor(cfs))
        {
            assertTrue(CursorCompactor.isCleanupSupported(cfs.getLiveSSTables(), cleanup));
            assertTrue(CursorCompactor.isValidationSupported(cfs.getLiveSSTables(), validation));
        }
    }
}
