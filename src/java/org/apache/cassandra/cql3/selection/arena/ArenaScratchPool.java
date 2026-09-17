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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * Program-lifetime scratch pool for arena aggregation.
 * Holds ONE shared backing segment and sub-allocates fixed 1 MB blocks via thread-safe lease/release.
 *
 * <p>Each lease reserves a single CONTIGUOUS run of blocks up front, sized to the per-query cap.
 * A contiguous run lets the lease address any offset in its space as {@code base + offset}, which
 * is always valid off-heap memory.  A fragmented run would make {@code base + offset} walk into
 * blocks owned by other leases and corrupt or leak their data, so the pool never hands out a
 * fragmented lease.
 *
 * <p>All leases are the same size, so the pool never fragments: it holds
 * {@code floor(totalBytes / runBytes)} interchangeable slots.  Blocks are zeroed on release, so a
 * lease never observes a prior tenant's bytes.
 *
 * <p>Enforces the atomic invariant: sum(live leased bytes) &lt;= totalBytes.
 */
public final class ArenaScratchPool
{
    private static final Logger logger = LoggerFactory.getLogger(ArenaScratchPool.class);
    private static final int BLOCK_SIZE = 1024 * 1024; // 1 MB blocks

    // Lazy initialization holder idiom (matches JsonbNative)
    private static class Holder
    {
        static final ArenaScratchPool INSTANCE;

        static
        {
            try
            {
                INSTANCE = new ArenaScratchPool();
            }
            catch (Throwable t)
            {
                logger.error("Failed to initialize ArenaScratchPool", t);
                throw new ExceptionInInitializerError(t);
            }
        }
    }

    private final Arena arena;
    private final MemorySegment backingSegment;
    private final long totalBytes;
    private final int blockCount;
    private final BitSet freeBlocks;
    private final ReentrantLock lock;
    private long leasedBytes;
    private long leasedBytesHighWater;

    private ArenaScratchPool()
    {
        this.arena = Arena.ofShared(); // Never closed
        this.totalBytes = CassandraRelevantProperties.CASSANDRA_CQL_ARENA_AGGREGATION_MAX_BYTES.getLong();
        this.blockCount = (int) ((totalBytes + BLOCK_SIZE - 1) / BLOCK_SIZE);
        this.backingSegment = arena.allocate((long) blockCount * BLOCK_SIZE);
        this.freeBlocks = new BitSet(blockCount);
        this.freeBlocks.set(0, blockCount); // All blocks start free
        this.lock = new ReentrantLock();
        this.leasedBytes = 0;
        this.leasedBytesHighWater = 0;

        logger.info("ArenaScratchPool initialized: {} bytes ({} blocks of {} bytes)",
                    totalBytes, blockCount, BLOCK_SIZE);
    }

    public static ArenaScratchPool getInstance()
    {
        return Holder.INSTANCE;
    }

    /**
     * Lease a contiguous run of scratch space for one query.
     *
     * @param maxQueryBytes per-query cap; the run is rounded up to whole 1 MB blocks
     * @return lease handle
     * @throws ArenaCapacityException if no contiguous run of that size is free
     */
    public Lease lease(long maxQueryBytes) throws ArenaCapacityException
    {
        return new Lease(maxQueryBytes);
    }

    /**
     * Find the start index of the first free run of {@code runBlocks} consecutive blocks.
     * Caller must hold {@link #lock}.
     *
     * @return the start block index, or -1 if no such run is free
     */
    private int findContiguousRun(int runBlocks)
    {
        int from = freeBlocks.nextSetBit(0);
        while (from >= 0 && from + runBlocks <= blockCount)
        {
            // nextClearBit gives the first non-free block at or after 'from'.
            int firstTaken = freeBlocks.nextClearBit(from);
            if (firstTaken - from >= runBlocks)
                return from;
            from = freeBlocks.nextSetBit(firstTaken);
        }
        return -1;
    }

    /**
     * Lease handle for one query's arena scratch allocation.
     * Owns a fixed contiguous run of blocks.  AutoCloseable so the run is always released.
     */
    public final class Lease implements AutoCloseable
    {
        private final long capacityBytes;
        private final int firstBlock;
        private final int runBlocks;
        private boolean released;

        private Lease(long maxQueryBytes) throws ArenaCapacityException
        {
            this.runBlocks = (int) ((maxQueryBytes + BLOCK_SIZE - 1) / BLOCK_SIZE);
            this.capacityBytes = (long) runBlocks * BLOCK_SIZE;

            lock.lock();
            try
            {
                int start = findContiguousRun(runBlocks);
                if (start < 0)
                {
                    throw new ArenaCapacityException(
                        String.format("Arena pool exhausted: no contiguous run of %d bytes is free, " +
                                      "%d bytes in use of %d total " +
                                      "(adjust cassandra.cql.arena_aggregation.max_bytes)",
                                      capacityBytes, leasedBytes, totalBytes));
                }
                this.firstBlock = start;
                freeBlocks.clear(start, start + runBlocks);
                leasedBytes += capacityBytes;
                if (leasedBytes > leasedBytesHighWater)
                    leasedBytesHighWater = leasedBytes;
            }
            finally
            {
                lock.unlock();
            }
            this.released = false;
        }

        /**
         * The total addressable size of this lease, in bytes.
         */
        public long capacity()
        {
            return capacityBytes;
        }

        /**
         * Get a slice of this lease's contiguous space.
         *
         * @param offset offset within this lease, in bytes
         * @param size length of the slice, in bytes
         * @return memory segment slice backed by this lease's blocks
         */
        public MemorySegment slice(long offset, long size)
        {
            if (released)
                throw new IllegalStateException("Lease already released");

            if (offset < 0 || size < 0 || offset + size > capacityBytes)
                throw new IllegalArgumentException(
                    String.format("Slice [%d, %d) exceeds lease capacity %d", offset, offset + size, capacityBytes));

            long absoluteOffset = (long) firstBlock * BLOCK_SIZE + offset;
            return backingSegment.asSlice(absoluteOffset, size);
        }

        /**
         * Release the run back to the pool.  Zeroes the blocks first so no residual data can leak
         * to the next tenant.  MUST be called on every path (success and exception).
         */
        @Override
        public void close()
        {
            if (released)
                return;

            lock.lock();
            try
            {
                backingSegment.asSlice((long) firstBlock * BLOCK_SIZE, capacityBytes).fill((byte) 0);
                freeBlocks.set(firstBlock, firstBlock + runBlocks);
                leasedBytes -= capacityBytes;
                released = true;
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    /**
     * Get current pool statistics (for testing/monitoring).
     */
    public PoolStats getStats()
    {
        lock.lock();
        try
        {
            return new PoolStats(totalBytes, leasedBytes, totalBytes - leasedBytes, leasedBytesHighWater);
        }
        finally
        {
            lock.unlock();
        }
    }

    public static class PoolStats
    {
        public final long totalBytes;
        public final long leasedBytes;
        public final long freeBytes;
        public final long leasedBytesHighWater;

        PoolStats(long totalBytes, long leasedBytes, long freeBytes, long leasedBytesHighWater)
        {
            this.totalBytes = totalBytes;
            this.leasedBytes = leasedBytes;
            this.freeBytes = freeBytes;
            this.leasedBytesHighWater = leasedBytesHighWater;
        }
    }
}
