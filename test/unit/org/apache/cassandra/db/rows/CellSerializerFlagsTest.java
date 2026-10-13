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

package org.apache.cassandra.db.rows;

import org.junit.Test;

import org.apache.cassandra.db.LivenessInfo;

import static org.apache.cassandra.db.rows.Cell.NO_DELETION_TIME;
import static org.apache.cassandra.db.rows.Cell.NO_TTL;
import static org.apache.cassandra.db.rows.Cell.Serializer.HAS_EMPTY_VALUE_MASK;
import static org.apache.cassandra.db.rows.Cell.Serializer.IS_DELETED_MASK;
import static org.apache.cassandra.db.rows.Cell.Serializer.IS_EXPIRING_MASK;
import static org.apache.cassandra.db.rows.Cell.Serializer.USE_ROW_TIMESTAMP_MASK;
import static org.apache.cassandra.db.rows.Cell.Serializer.USE_ROW_TTL_MASK;
import static org.apache.cassandra.db.rows.Cell.Serializer.encodeFlags;
import static org.junit.Assert.assertEquals;

/**
 * Checks {@link Cell.Serializer#encodeFlags} against each individually-triggerable flag,
 * independent of any {@link Cell} implementation or CQL round trip.
 * <p>
 * The {@code ttl}/{@code localDeletionTime} pairs below come from
 * {@link LivenessInfo#withExpirationTime}, not {@link LivenessInfo#expiring}, to avoid depending
 * on {@code ExpirationDateOverflowHandling}.
 */
public class CellSerializerFlagsTest
{
    private static final long TS = 1000L;
    private static final int TTL = 3600;
    private static final long EXPIRES_AT = TS + TTL;

    @Test
    public void liveNonExpiringCellNoRowLiveness()
    {
        assertEquals(0, encodeFlags(true, false, false, TS, NO_TTL, NO_DELETION_TIME, LivenessInfo.EMPTY));
    }

    @Test
    public void emptyValueSetsOnlyHasEmptyValueMask()
    {
        assertEquals(HAS_EMPTY_VALUE_MASK, encodeFlags(false, false, false, TS, NO_TTL, NO_DELETION_TIME, LivenessInfo.EMPTY));
    }

    @Test
    public void tombstoneSetsIsDeletedNotIsExpiring()
    {
        // When both isDeleted and isExpiring are true, isDeleted wins.
        assertEquals(IS_DELETED_MASK, encodeFlags(true, true, false, TS, NO_TTL, NO_DELETION_TIME, LivenessInfo.EMPTY));
        assertEquals(IS_DELETED_MASK, encodeFlags(true, true, true, TS, NO_TTL, NO_DELETION_TIME, LivenessInfo.EMPTY));
    }

    @Test
    public void expiringSetsIsExpiring()
    {
        assertEquals(IS_EXPIRING_MASK, encodeFlags(true, false, true, TS, TTL, EXPIRES_AT, LivenessInfo.EMPTY));
    }

    @Test
    public void matchingRowTimestampSetsUseRowTimestampMask()
    {
        LivenessInfo rowLiveness = LivenessInfo.create(TS);
        assertEquals(USE_ROW_TIMESTAMP_MASK, encodeFlags(true, false, false, TS, NO_TTL, NO_DELETION_TIME, rowLiveness));
    }

    @Test
    public void differingRowTimestampDoesNotSetUseRowTimestampMask()
    {
        LivenessInfo rowLiveness = LivenessInfo.create(TS + 1);
        assertEquals(0, encodeFlags(true, false, false, TS, NO_TTL, NO_DELETION_TIME, rowLiveness));
    }

    /**
     * IS_EXPIRING_MASK and USE_ROW_TTL_MASK are set together when the cell is expiring and its
     * ttl and expiration match the row's.  IS_EXPIRING_MASK says the cell expires; USE_ROW_TTL_MASK
     * says the ttl and deletion-time bytes are omitted because they equal the row's.
     */
    @Test
    public void matchingRowExpirationSetsBothIsExpiringAndUseRowTtlMask()
    {
        LivenessInfo rowLiveness = LivenessInfo.withExpirationTime(TS, TTL, EXPIRES_AT);
        int flags = encodeFlags(true, false, true, TS, TTL, EXPIRES_AT, rowLiveness);
        assertEquals(USE_ROW_TIMESTAMP_MASK | IS_EXPIRING_MASK | USE_ROW_TTL_MASK, flags);
    }

    @Test
    public void differingRowExpirationSetsIsExpiringButNotUseRowTtlMask()
    {
        LivenessInfo rowLiveness = LivenessInfo.withExpirationTime(TS, TTL + 1, EXPIRES_AT + 1);
        int flags = encodeFlags(true, false, true, TS, TTL, EXPIRES_AT, rowLiveness);
        assertEquals(USE_ROW_TIMESTAMP_MASK | IS_EXPIRING_MASK, flags);
    }

    @Test
    public void nonExpiringCellNeverSetsUseRowTtlMaskEvenWithExpiringRow()
    {
        LivenessInfo rowLiveness = LivenessInfo.withExpirationTime(TS, TTL, EXPIRES_AT);
        assertEquals(USE_ROW_TIMESTAMP_MASK, encodeFlags(true, false, false, TS, NO_TTL, NO_DELETION_TIME, rowLiveness));
    }

    @Test
    public void allFlagsCanCombine()
    {
        LivenessInfo rowLiveness = LivenessInfo.withExpirationTime(TS, TTL, EXPIRES_AT);
        int flags = encodeFlags(false, false, true, TS, TTL, EXPIRES_AT, rowLiveness);
        assertEquals(HAS_EMPTY_VALUE_MASK | IS_EXPIRING_MASK | USE_ROW_TIMESTAMP_MASK | USE_ROW_TTL_MASK, flags);
    }
}
