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

package org.apache.cassandra.io.sstable.format.big;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

/**
 * A growable array of ints backed by a buffer borrowed from {@link BufferPools#forPartitionWriters()}, so that the
 * per-index-block bookkeeping of a partition writer neither allocates on heap nor is subject to garbage collection
 * while a large partition is written.
 * <p>
 * Growth doubles the capacity and is expected to be rare - callers should size the array up front via
 * {@link #ensureCapacity(int)} whenever they have an estimate of the final element count.
 * <p>
 * Instances hold a pooled buffer until {@link #release()} is called and are not thread safe.
 */
final class PooledIntArray
{
    private static final int MIN_CAPACITY = 16;

    private ByteBuffer buffer;
    private int capacity;

    int get(int index)
    {
        return buffer.getInt(index * Integer.BYTES);
    }

    void set(int index, int value)
    {
        buffer.putInt(index * Integer.BYTES, value);
    }

    void ensureCapacity(int minElements)
    {
        if (minElements <= capacity)
            return;

        int newCapacity = Math.max(Math.max(minElements, MIN_CAPACITY), capacity * 2);
        BufferPool pool = BufferPools.forPartitionWriters();
        ByteBuffer fresh = pool.get(newCapacity * Integer.BYTES, BufferType.OFF_HEAP);
        fresh.order(ByteOrder.nativeOrder());

        if (buffer != null)
        {
            buffer.position(0);
            buffer.limit(capacity * Integer.BYTES);
            fresh.put(buffer);
            pool.put(buffer);
        }

        fresh.position(0);
        fresh.limit(newCapacity * Integer.BYTES);
        buffer = fresh;
        capacity = newCapacity;
    }

    /**
     * @return an on-heap copy of the first {@code count} elements
     */
    int[] toArray(int count)
    {
        int[] copy = new int[count];
        for (int i = 0; i < count; i++)
            copy[i] = get(i);
        return copy;
    }

    void release()
    {
        if (buffer == null)
            return;

        BufferPools.forPartitionWriters().put(buffer);
        buffer = null;
        capacity = 0;
    }
}
