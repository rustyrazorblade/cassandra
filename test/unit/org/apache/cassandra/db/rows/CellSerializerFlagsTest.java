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
 * Pins {@link Cell.Serializer#encodeFlags} bit-for-bit against each individually-triggerable
 * flag, independent of any particular {@link Cell} implementation or CQL round trip — the actual
 * regression surface for both of {@code encodeFlags}'s callers ({@link Cell.Serializer#serialize}
 * and {@code MemtableCursorFlusher.writeCell}), since a wrong bit here corrupts every consumer of
 * this on-disk format identically, regardless of which path wrote it.
 * <p>
 * {@code ttl}/{@code localDeletionTime} pairs below always come from
 * {@link LivenessInfo#withExpirationTime}, not {@link LivenessInfo#expiring}: the latter derives
 * {@code localExpirationTime} through {@code ExpirationDateOverflowHandling}, an implementation
 * detail this test has no need to depend on.
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
        // A tombstone cell always carries HAS_EMPTY_VALUE_MASK too in practice, but encodeFlags
        // takes hasValue as an independent input - this pins that isDeleted takes priority over
        // isExpiring (per the mask comment: "Whether the cell is a tombstone or not") when a
        // caller passes both true, which real callers never do but the method itself must still
        // resolve deterministically.
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
     * USE_ROW_TTL_MASK does not suppress IS_EXPIRING_MASK: both bits are set together whenever
     * the cell is expiring and its ttl/expiration exactly match the row's own. IS_EXPIRING_MASK
     * says "this cell expires"; USE_ROW_TTL_MASK separately says "the ttl/deletion-time bytes are
     * omitted from the wire because they equal the row's" (see Cell.Serializer#serialize's
     * {@code if (isExpiring && !useRowTTL) header.writeTTL(...)}) - two different questions.
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
