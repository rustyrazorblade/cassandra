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

package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.db.rows.Cell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeletionTimeTest
{
    /**
     * NO_DELETION_TIME (Long.MAX_VALUE) is the canonical "no deletion" long value and must
     * round-trip through the long-based reset to a LIVE deletion time, mirroring
     * {@link Cell#deletionTimeLongToUnsignedInteger}. It used to be classified as invalid,
     * so {@code reset(LIVE.markedForDeleteAt(), LIVE.localDeletionTime())} produced a
     * NON-live deletion — which let a live marker slip past MetadataCollector's isLive
     * guard during cursor compaction and poison minTimestamp/tombstone stats.
     */
    @Test
    public void resetWithLiveLongsStaysLive()
    {
        DeletionTime.ReusableDeletionTime reusable = DeletionTime.ReusableDeletionTime.live();
        assertTrue(reusable.isLive());

        reusable.reset(DeletionTime.LIVE.markedForDeleteAt(), DeletionTime.LIVE.localDeletionTime());
        assertTrue("reset with LIVE's long values must stay live, got mfda=" + reusable.markedForDeleteAt() +
                   " ldt=" + reusable.localDeletionTime(), reusable.isLive());
        assertEquals(DeletionTime.LIVE.localDeletionTime(), reusable.localDeletionTime());
    }

    @Test
    public void resetWithRealAndInvalidValues()
    {
        DeletionTime.ReusableDeletionTime reusable = DeletionTime.ReusableDeletionTime.live();

        reusable.reset(123456789L, 1_700_000_000L);
        assertFalse(reusable.isLive());
        assertEquals(123456789L, reusable.markedForDeleteAt());
        assertEquals(1_700_000_000L, reusable.localDeletionTime());

        // negative and beyond-max (but not NO_DELETION_TIME) stay classified as invalid
        reusable.reset(1L, -5L);
        assertFalse(reusable.isLive());
        assertFalse(reusable.validate());

        reusable.reset(1L, Cell.MAX_DELETION_TIME + 1);
        assertFalse(reusable.isLive());
        assertFalse(reusable.validate());
    }
}
