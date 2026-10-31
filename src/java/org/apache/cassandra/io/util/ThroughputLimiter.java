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

package org.apache.cassandra.io.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.Clock;

/**
 * Limits the bytes per second that a group of threads read from and write to disk.
 * <p>
 * A thread charges each I/O when it completes, then waits until the I/O charged before it is paid for.  So each
 * thread is at most one I/O ahead of the limit, and a thread that the limit does not hold back never waits.  The
 * limiter saves at most {@link #UNUSED_TIME_SAVED_NANOS} of unused time, so an idle period does not allow a burst.
 * <p>
 * A priority charge, from {@link #acquirePriority}, waits only for the priority I/O charged before it.  An ordinary
 * charge waits for both kinds, so the two together keep to the limit.
 */
@ThreadSafe
public class ThroughputLimiter
{
    private static final double NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    @VisibleForTesting
    static final long UNUSED_TIME_SAVED_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    /** A shorter wait is left to a later charge, so a small I/O does not cost a park. */
    @VisibleForTesting
    static final long MIN_WAIT_NANOS = TimeUnit.MICROSECONDS.toNanos(100);
    /** The most debt the limiter holds, so a wait is never long enough to wrap around. */
    private static final long MAX_DEBT_NANOS = Long.MAX_VALUE / 2;
    /** The longest park, so that a waiting thread soon sees a new limit. */
    @VisibleForTesting
    static final long MAX_PARK_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    private final LongSupplier nanoTime;
    private final LongConsumer parkNanos;

    /** The time at which the I/O charged so far is paid for. */
    private final AtomicLong paidUntilNanos;
    /** The time at which the priority I/O charged so far is paid for. */
    private final AtomicLong priorityPaidUntilNanos;
    private final LongAdder bytesCharged = new LongAdder();
    private final LongAdder priorityBytesCharged = new LongAdder();
    private final LongAdder priorityWaitNanos = new LongAdder();
    /** The priority wait of each thread, so a caller can tell how long its own charges waited. */
    private final ThreadLocal<long[]> threadPriorityWaitNanos = ThreadLocal.withInitial(() -> new long[1]);

    private volatile Limit limit;

    private static final class Limit
    {
        final double bytesPerSecond;
        /** Zero when there is no limit. */
        final double nanosPerByte;

        Limit(double bytesPerSecond)
        {
            this.bytesPerSecond = bytesPerSecond;
            this.nanosPerByte = bytesPerSecond > 0 && bytesPerSecond < Double.MAX_VALUE ? NANOS_PER_SECOND / bytesPerSecond : 0;
        }
    }

    /**
     * @param bytesPerSecond the limit; zero, a negative value, NaN, {@link Double#MAX_VALUE} or infinity means none
     */
    public ThroughputLimiter(double bytesPerSecond)
    {
        this(bytesPerSecond, Clock.Global::nanoTime, LockSupport::parkNanos);
    }

    @VisibleForTesting
    public ThroughputLimiter(double bytesPerSecond, LongSupplier nanoTime, LongConsumer parkNanos)
    {
        this.nanoTime = nanoTime;
        this.parkNanos = parkNanos;
        this.paidUntilNanos = new AtomicLong(nanoTime.getAsLong());
        this.priorityPaidUntilNanos = new AtomicLong(paidUntilNanos.get());
        this.limit = new Limit(bytesPerSecond);
    }

    /**
     * Changes the limit.  The next charge does not wait for the I/O charged before this call, and a thread that is
     * already waiting stops.
     *
     * @param bytesPerSecond the limit; zero, a negative value, NaN, {@link Double#MAX_VALUE} or infinity means none
     */
    public void setRate(double bytesPerSecond)
    {
        limit = new Limit(bytesPerSecond);
        long now = nanoTime.getAsLong();
        paidUntilNanos.accumulateAndGet(now, ThroughputLimiter::earlier);
        priorityPaidUntilNanos.accumulateAndGet(now, ThroughputLimiter::earlier);
    }

    /** The limit as last set, in bytes per second. */
    public double getRate()
    {
        return limit.bytesPerSecond;
    }

    /** Whether a limit is set. */
    public boolean isLimited()
    {
        return limit.nanosPerByte != 0;
    }

    /**
     * Charges {@code bytes} of completed I/O, and waits until the I/O charged before them is paid for.  An interrupt
     * ends the wait early, with the interrupt status kept.
     */
    public void acquire(long bytes)
    {
        if (bytes <= 0)
            return;
        bytesCharged.add(bytes);
        Limit limit = this.limit;
        if (limit.nanosPerByte == 0)
            return;

        long cost = cost(bytes, limit);
        long now = nanoTime.getAsLong();
        await(reserve(paidUntilNanos, cost, now), now, limit);
    }

    /**
     * Charges {@code bytes} of completed I/O, and waits until the priority I/O charged before them is paid for.  Later
     * calls to {@link #acquire} wait for these bytes too.  An interrupt ends the wait early, with the interrupt status
     * kept.
     */
    public void acquirePriority(long bytes)
    {
        if (bytes <= 0)
            return;
        bytesCharged.add(bytes);
        priorityBytesCharged.add(bytes);
        Limit limit = this.limit;
        if (limit.nanosPerByte == 0)
            return;

        long cost = cost(bytes, limit);
        long now = nanoTime.getAsLong();
        reserve(paidUntilNanos, cost, now);
        long waited = await(reserve(priorityPaidUntilNanos, cost, now), now, limit);
        if (waited > 0)
        {
            priorityWaitNanos.add(waited);
            threadPriorityWaitNanos.get()[0] += waited;
        }
    }

    private static long earlier(long a, long b)
    {
        return a - b < 0 ? a : b;
    }

    private static long cost(long bytes, Limit limit)
    {
        return (long) Math.min(Long.MAX_VALUE / 4, Math.ceil(bytes * limit.nanosPerByte));
    }

    /** Adds {@code cost} to {@code paidUntil}, and returns the time at which the I/O charged before it is paid for. */
    private static long reserve(AtomicLong paidUntil, long cost, long now)
    {
        long earlierPaidAt, paidAt;
        do
        {
            earlierPaidAt = paidUntil.get();
            // Up to UNUSED_TIME_SAVED_NANOS of unused time carries over, to make up for a thread that wakes late.
            long debt = Math.max(earlierPaidAt - now, -UNUSED_TIME_SAVED_NANOS);
            // Times are compared by difference, as nanoTime may wrap, so the debt is capped rather than the time.
            paidAt = now + Math.min(debt + cost, MAX_DEBT_NANOS);
        }
        while (!paidUntil.compareAndSet(earlierPaidAt, paidAt));
        return earlierPaidAt;
    }

    /** Waits until {@code earlierPaidAt}, a new limit, or an interrupt, and returns the nanoseconds waited. */
    private long await(long earlierPaidAt, long now, Limit limit)
    {
        long wait = earlierPaidAt - now;
        if (wait < MIN_WAIT_NANOS)
            return 0;
        Thread thread = Thread.currentThread();
        long time = now;
        while (wait > 0 && this.limit == limit && !thread.isInterrupted())
        {
            parkNanos.accept(Math.min(wait, MAX_PARK_NANOS));
            time = nanoTime.getAsLong();
            wait = earlierPaidAt - time;
        }
        return time - now;
    }

    /** The bytes charged since this limiter was created, with or without a limit. */
    public long bytesCharged()
    {
        return bytesCharged.sum();
    }

    /** The bytes charged by {@link #acquirePriority} since this limiter was created. */
    public long priorityBytesCharged()
    {
        return priorityBytesCharged.sum();
    }

    /** The time that {@link #acquirePriority} callers have waited since this limiter was created, in nanoseconds. */
    public long priorityWaitNanos()
    {
        return priorityWaitNanos.sum();
    }

    /** The time that {@link #acquirePriority} calls from the current thread have waited, in nanoseconds. */
    public long threadPriorityWaitNanos()
    {
        return threadPriorityWaitNanos.get()[0];
    }
}
