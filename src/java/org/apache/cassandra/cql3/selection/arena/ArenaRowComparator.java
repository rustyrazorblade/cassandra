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
package org.apache.cassandra.cql3.selection.arena;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Compares two directory slots by key columns using type-aware comparison.
 * Implements null-first ordering and handles REVERSED via argument swap.
 */
final class ArenaRowComparator
{
    private final ArenaRowBuffer buffer;
    private final List<Integer> keyColumnIndices;
    private final List<AbstractType<?>> keyColumnTypes;
    private final List<Boolean> keyColumnReversed;

    ArenaRowComparator(ArenaRowBuffer buffer,
                       List<Integer> keyColumnIndices,
                       List<AbstractType<?>> keyColumnTypes,
                       List<Boolean> keyColumnReversed)
    {
        this.buffer = buffer;
        this.keyColumnIndices = keyColumnIndices;
        this.keyColumnTypes = keyColumnTypes;
        this.keyColumnReversed = keyColumnReversed;
    }

    /**
     * Compare two directory slots.
     *
     * @param slotA first slot index
     * @param slotB second slot index
     * @return comparison result following standard Comparator contract
     */
    int compare(int slotA, int slotB)
    {
        for (int i = 0; i < keyColumnIndices.size(); i++)
        {
            int columnIndex = keyColumnIndices.get(i);
            AbstractType<?> type = keyColumnTypes.get(i);
            boolean reversed = keyColumnReversed.get(i);

            boolean aNull = buffer.isNull(slotA, columnIndex);
            boolean bNull = buffer.isNull(slotB, columnIndex);

            // Null-first: null < non-null
            if (aNull && bNull)
                continue;
            if (aNull)
                return -1;
            if (bNull)
                return 1;

            // Both non-null, extract byte slices and compare
            MemorySegment aSegment = buffer.getColumnSlice(slotA, columnIndex);
            MemorySegment bSegment = buffer.getColumnSlice(slotB, columnIndex);

            // Wrap as ByteBuffer for AbstractType.compare
            ByteBuffer aBuf = aSegment.asByteBuffer();
            ByteBuffer bBuf = bSegment.asByteBuffer();

            // Reverse by argument swap, not result negation
            int cmp = reversed ? type.compare(bBuf, aBuf) : type.compare(aBuf, bBuf);

            if (cmp != 0)
                return cmp;
        }

        return 0;
    }
}
