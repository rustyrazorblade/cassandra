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

import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link ReusableLivenessInfo} backs both row primary-key liveness and cell liveness on the cursor
 * path, and only {@code isExpiring} answers the same way for both references. {@code isExpired}
 * mirrors {@link LivenessInfo} alone, {@code isTombstone} and {@code isLive} mirror
 * {@link org.apache.cassandra.db.rows.AbstractCell} alone.
 */
public class ReusableLivenessInfoTest
{
    private static final long TIMESTAMP = 1000L;
    private static final long NOW_IN_SEC = 1_700_000_000L;

    private static ColumnMetadata column;
    private static ByteBuffer value;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        column = ColumnMetadata.regularColumn("ks", "tbl", "v", Int32Type.instance, 0);
        value = Int32Type.instance.decompose(1);
    }

    /**
     * {@code isExpiring()} is "has a TTL" and {@code isTombstone()} is "has an expiration time and
     * no TTL", exactly as the cell reference defines them — a tombstone carries an expiration time
     * without a TTL, which is why the two cannot share a predicate.
     */
    @Test
    public void predicatesAgreeWithCellReference()
    {
        // (ttl, localExpirationTime): live, tombstone, expiring, and the corrupt TTL with no
        // expiration time. All four are representable by a Cell.
        int[] ttls = { Cell.NO_TTL, Cell.NO_TTL, 100, 100 };
        long[] ldts = { Cell.NO_DELETION_TIME, NOW_IN_SEC - 10, NOW_IN_SEC + 10, Cell.NO_DELETION_TIME };

        for (int i = 0; i < ttls.length; i++)
        {
            ReusableLivenessInfo liveness = new ReusableLivenessInfo();
            liveness.reset(TIMESTAMP, ttls[i], ldts[i]);
            Cell<?> reference = new BufferCell(column, TIMESTAMP, ttls[i], ldts[i], value, null);
            String at = " at ttl=" + ttls[i] + " localExpirationTime=" + ldts[i];

            assertEquals("isExpiring must match AbstractCell.isExpiring()" + at,
                         reference.isExpiring(), liveness.isExpiring());
            assertEquals("isTombstone must match AbstractCell.isTombstone()" + at,
                         reference.isTombstone(), liveness.isTombstone());
            assertEquals("isLive must match AbstractCell.isLive()" + at,
                         reference.isLive(NOW_IN_SEC), liveness.isLive(NOW_IN_SEC));
        }
    }

    /**
     * For row liveness the reference is {@link LivenessInfo#withExpirationTime}, which is expiring
     * precisely when it has a TTL and discards the expiration time otherwise.
     */
    @Test
    public void isExpiringAgreesWithRowReference()
    {
        for (int ttl : new int[]{ LivenessInfo.NO_TTL, 1, 100, LivenessInfo.EXPIRED_LIVENESS_TTL })
        {
            for (long ldt : new long[]{ NOW_IN_SEC - 10, NOW_IN_SEC, NOW_IN_SEC + 10 })
            {
                ReusableLivenessInfo liveness = new ReusableLivenessInfo();
                liveness.reset(TIMESTAMP, ttl, ldt);
                LivenessInfo reference = LivenessInfo.withExpirationTime(TIMESTAMP, ttl, ldt);

                assertEquals("isExpiring must match LivenessInfo.isExpiring() at ttl=" + ttl +
                             " localExpirationTime=" + ldt,
                             reference.isExpiring(), liveness.isExpiring());
                assertEquals("isExpired must match LivenessInfo.isExpired() at ttl=" + ttl +
                             " localExpirationTime=" + ldt,
                             reference.isExpired(), liveness.isExpired());
            }
        }
    }

    /**
     * Converting a lapsed TTL to a tombstone rewinds the expiration time by the TTL, to the second
     * the data was written, matching {@link org.apache.cassandra.db.rows.AbstractCell#purge}'s
     * {@code localDeletionTime() - ttl()}, and leaves liveness that is a tombstone rather than
     * expiring.
     */
    @Test
    public void ttlToTombstoneProducesATombstone()
    {
        ReusableLivenessInfo liveness = new ReusableLivenessInfo();
        liveness.reset(TIMESTAMP, 100, NOW_IN_SEC);
        assertTrue(liveness.isExpiring());
        assertFalse(liveness.isTombstone());

        liveness.ttlToTombstone();

        assertFalse("a converted TTL no longer has one", liveness.isExpiring());
        assertTrue("a converted TTL is a tombstone", liveness.isTombstone());
        assertEquals("the expiration time is rewound by the TTL",
                     NOW_IN_SEC - 100, liveness.localExpirationTime());
        assertEquals(LivenessInfo.NO_TTL, liveness.ttl());
    }

    @Test
    public void freshInstanceIsEmptyAndNeitherExpiringNorTombstone()
    {
        ReusableLivenessInfo liveness = new ReusableLivenessInfo();
        assertTrue(liveness.isEmpty());
        assertFalse(liveness.isExpiring());
        assertFalse(liveness.isTombstone());
        assertFalse(liveness.isExpired());
        // isLive() implements the cell contract, under which no expiration time means live. Row
        // callers gate on isEmpty() instead, so this differs from LivenessInfo.EMPTY by design.
        assertTrue(liveness.isLive(NOW_IN_SEC));
    }
}
