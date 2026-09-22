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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Direct coverage of the growable off-heap offsets buffer. The behavioural guarantees are also covered black-box by
 * {@link BigFormatPartitionWriterTest}, which asserts offset contents across many growth steps; these tests pin the
 * arithmetic and the buffer lifecycle at the boundaries that a partition writer cannot conveniently reach.
 */
public class PooledIntArrayTest
{
    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void growthPreservesContents()
    {
        PooledIntArray array = new PooledIntArray();
        try
        {
            // well past MIN_CAPACITY, so several doublings run
            for (int i = 0; i < 5000; i++)
            {
                array.ensureCapacity(i + 1);
                array.set(i, i * 7);
            }

            for (int i = 0; i < 5000; i++)
                assertEquals("element " + i + " did not survive growth", i * 7, array.get(i));
        }
        finally
        {
            array.release();
        }
    }

    @Test
    public void presizeThenUndershootDoubles()
    {
        PooledIntArray array = new PooledIntArray();
        try
        {
            array.ensureCapacity(64);
            for (int i = 0; i < 64; i++)
                array.set(i, i);

            // one past the presize, forcing the doubling path over an already-populated buffer
            array.ensureCapacity(65);
            array.set(64, 64);

            for (int i = 0; i <= 64; i++)
                assertEquals(i, array.get(i));
        }
        finally
        {
            array.release();
        }
    }

    @Test
    public void presizeOvershootIsNotShrunkByLaterRequests()
    {
        PooledIntArray array = new PooledIntArray();
        try
        {
            array.ensureCapacity(100_000);
            array.set(99_999, 42);

            // a smaller request must be a no-op, leaving the larger buffer and its contents in place
            array.ensureCapacity(16);
            assertEquals(42, array.get(99_999));
        }
        finally
        {
            array.release();
        }
    }

    @Test
    public void toArrayCopiesExactlyTheRequestedPrefix()
    {
        PooledIntArray array = new PooledIntArray();
        try
        {
            array.ensureCapacity(300);
            for (int i = 0; i < 300; i++)
                array.set(i, i * 3);

            int[] expected = new int[200];
            for (int i = 0; i < 200; i++)
                expected[i] = i * 3;

            assertArrayEquals(expected, array.toArray(200));
            assertEquals(0, array.toArray(0).length);
        }
        finally
        {
            array.release();
        }
    }

    @Test
    public void releaseIsIdempotentAndReusable()
    {
        PooledIntArray array = new PooledIntArray();
        array.ensureCapacity(64);
        array.set(0, 7);

        array.release();
        array.release();

        // a released instance starts again from nothing rather than remembering its old capacity
        array.ensureCapacity(32);
        array.set(0, 9);
        assertEquals(9, array.get(0));
        array.release();
    }

    @Test
    public void capacityBeyondTheAddressableLimitIsRejected()
    {
        PooledIntArray array = new PooledIntArray();
        try
        {
            array.ensureCapacity(PooledIntArray.MAX_CAPACITY + 1);
            fail("expected a request past MAX_CAPACITY to be rejected rather than wrapping to a negative size");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("the limit is"));
        }
        finally
        {
            array.release();
        }
    }

    @Test
    public void pooledMemoryReturnsToTheBaselineOnRelease()
    {
        BufferPool pool = BufferPools.forPartitionWriters();
        long baseline = pool.usedSizeInBytes();

        PooledIntArray array = new PooledIntArray();
        // stay under NORMAL_CHUNK_SIZE so the request is served from a pooled chunk and is visible to the pool
        array.ensureCapacity(BufferPool.NORMAL_CHUNK_SIZE / Integer.BYTES / 4);
        array.set(0, 1);
        assertTrue("the pool should have handed out a chunk", pool.usedSizeInBytes() > baseline);

        array.release();
        assertEquals("release must return the chunk to the pool", baseline, pool.usedSizeInBytes());
    }
}
