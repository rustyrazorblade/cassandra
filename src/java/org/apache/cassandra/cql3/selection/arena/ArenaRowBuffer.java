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
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.List;

/**
 * Off-heap row buffer laid out inside one lease's fixed contiguous space.
 *
 * <p>The payload region grows UP from offset 0.  The directory region grows DOWN from the top of
 * the lease.  They occupy disjoint sub-regions of the same run and grow toward each other; when
 * they would meet, {@link #appendRow} throws {@link ArenaCapacityException}.  This keeps the two
 * regions from overlapping, which would otherwise let a payload write silently corrupt directory
 * slots (and vice versa).
 *
 * <p>Each row payload = projected columns concatenated with 4-byte length prefixes.
 * Each directory slot = payloadOffset(long), payloadLen(int), groupHash(int), null-flags(longs).
 * Directory slot {@code i} lives at {@code capacity - (i + 1) * slotSize}.
 */
final class ArenaRowBuffer
{
    private static final int BYTES_PER_LENGTH_PREFIX = 4;
    private static final int SLOT_FIXED_SIZE = 8 + 4 + 4; // payloadOffset(long) + payloadLen(int) + groupHash(int)

    private final ArenaScratchPool.Lease lease;
    private final int columnCount;
    private final int nullFlagLongs;
    private final int slotSize;
    private final int maxRows;
    private final long capacity;

    private long payloadUsed;
    private int rowCount;

    ArenaRowBuffer(ArenaScratchPool.Lease lease, int columnCount, int maxRows)
    {
        this.lease = lease;
        this.columnCount = columnCount;
        this.nullFlagLongs = (columnCount + 63) / 64;
        this.slotSize = SLOT_FIXED_SIZE + (nullFlagLongs * 8);
        this.maxRows = maxRows;
        this.capacity = lease.capacity();

        this.payloadUsed = 0;
        this.rowCount = 0;
    }

    /**
     * The byte offset of directory slot {@code slot} within the lease.
     */
    private long slotOffset(int slot)
    {
        return capacity - ((long) (slot + 1) * slotSize);
    }

    /**
     * Append a row to the buffer.
     */
    void appendRow(List<byte[]> rowBytes, boolean[] nullFlags, int groupHash) throws ArenaCapacityException
    {
        if (rowCount >= maxRows)
        {
            throw new ArenaCapacityException(
                String.format("Arena row limit exceeded: %d rows (max %d, adjust cassandra.cql.arena_aggregation.max_rows)",
                              rowCount + 1, maxRows));
        }

        // Calculate payload size needed
        long payloadSize = 0;
        for (byte[] colBytes : rowBytes)
        {
            payloadSize += BYTES_PER_LENGTH_PREFIX;
            payloadSize += (colBytes == null ? 0 : colBytes.length);
        }

        // Payload grows up from 0; the new directory slot occupies [dirLow, ...) at the top.
        // If the payload would reach the directory, the lease is full.
        long dirLow = slotOffset(rowCount);
        if (payloadUsed + payloadSize > dirLow)
        {
            throw new ArenaCapacityException(
                String.format("Query arena limit exceeded: row %d needs %d payload bytes but only %d remain " +
                              "before the directory (adjust cassandra.cql.arena_aggregation.max_query_bytes)",
                              rowCount + 1, payloadSize, dirLow - payloadUsed));
        }

        // Write payload
        long rowPayloadOffset = payloadUsed;
        long writePos = payloadUsed;

        for (byte[] colBytes : rowBytes)
        {
            int len = (colBytes == null ? 0 : colBytes.length);
            MemorySegment seg = lease.slice(writePos, BYTES_PER_LENGTH_PREFIX);
            seg.set(ValueLayout.JAVA_INT_UNALIGNED, 0, len);
            writePos += BYTES_PER_LENGTH_PREFIX;

            if (colBytes != null && len > 0)
            {
                MemorySegment dataSeg = lease.slice(writePos, len);
                MemorySegment.copy(colBytes, 0, dataSeg, ValueLayout.JAVA_BYTE, 0, len);
                writePos += len;
            }
        }

        payloadUsed += payloadSize;

        // Write directory slot
        MemorySegment slotSeg = lease.slice(dirLow, slotSize);

        slotSeg.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, rowPayloadOffset);
        slotSeg.set(ValueLayout.JAVA_INT_UNALIGNED, 8, (int) payloadSize);
        slotSeg.set(ValueLayout.JAVA_INT_UNALIGNED, 12, groupHash);

        // Write null flags
        for (int i = 0; i < nullFlagLongs; i++)
        {
            long flags = 0;
            for (int bit = 0; bit < 64 && (i * 64 + bit) < columnCount; bit++)
            {
                if (nullFlags[i * 64 + bit])
                {
                    flags |= (1L << bit);
                }
            }
            slotSeg.set(ValueLayout.JAVA_LONG_UNALIGNED, 16 + (i * 8), flags);
        }

        rowCount++;
    }

    boolean isNull(int slot, int columnIndex)
    {
        int longIndex = columnIndex / 64;
        int bitIndex = columnIndex % 64;
        long offset = slotOffset(slot) + 16 + (longIndex * 8);
        MemorySegment seg = lease.slice(offset, 8);
        long flags = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        return (flags & (1L << bitIndex)) != 0;
    }

    MemorySegment getColumnSlice(int slot, int columnIndex)
    {
        if (isNull(slot, columnIndex))
        {
            return MemorySegment.NULL;
        }

        MemorySegment slotSeg = lease.slice(slotOffset(slot), slotSize);
        long rowPayloadOffset = slotSeg.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);

        // Walk through payload to find the column
        long pos = rowPayloadOffset;
        for (int i = 0; i < columnIndex; i++)
        {
            MemorySegment lenSeg = lease.slice(pos, BYTES_PER_LENGTH_PREFIX);
            int len = lenSeg.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
            pos += BYTES_PER_LENGTH_PREFIX + len;
        }

        MemorySegment lenSeg = lease.slice(pos, BYTES_PER_LENGTH_PREFIX);
        int len = lenSeg.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        pos += BYTES_PER_LENGTH_PREFIX;

        return lease.slice(pos, len);
    }

    List<byte[]> extractRow(int slot)
    {
        MemorySegment slotSeg = lease.slice(slotOffset(slot), slotSize);
        long rowPayloadOffset = slotSeg.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);

        byte[][] result = new byte[columnCount][];
        long pos = rowPayloadOffset;

        for (int i = 0; i < columnCount; i++)
        {
            if (isNull(slot, i))
            {
                result[i] = null;
                pos += BYTES_PER_LENGTH_PREFIX; // Length is 0 for null
            }
            else
            {
                MemorySegment lenSeg = lease.slice(pos, BYTES_PER_LENGTH_PREFIX);
                int len = lenSeg.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
                pos += BYTES_PER_LENGTH_PREFIX;

                if (len == 0)
                {
                    result[i] = new byte[0];
                }
                else
                {
                    result[i] = new byte[len];
                    MemorySegment dataSeg = lease.slice(pos, len);
                    MemorySegment.copy(dataSeg, ValueLayout.JAVA_BYTE, 0, result[i], 0, len);
                    pos += len;
                }
            }
        }

        // Arrays.asList (not List.of) because a projected column value may be null.
        return Arrays.asList(result);
    }

    int getRowCount()
    {
        return rowCount;
    }

    void swapSlots(int slotA, int slotB)
    {
        if (slotA == slotB)
            return;

        long offsetA = slotOffset(slotA);
        long offsetB = slotOffset(slotB);

        byte[] temp = new byte[slotSize];

        MemorySegment segA = lease.slice(offsetA, slotSize);
        MemorySegment segB = lease.slice(offsetB, slotSize);

        // Copy A to temp
        MemorySegment.copy(segA, ValueLayout.JAVA_BYTE, 0, temp, 0, slotSize);

        // Copy B to A
        MemorySegment.copy(segB, 0, segA, 0, slotSize);

        // Copy temp to B
        MemorySegment.copy(temp, 0, segB, ValueLayout.JAVA_BYTE, 0, slotSize);
    }

    int getGroupHash(int slot)
    {
        MemorySegment seg = lease.slice(slotOffset(slot) + 12, 4);
        return seg.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
    }
}
