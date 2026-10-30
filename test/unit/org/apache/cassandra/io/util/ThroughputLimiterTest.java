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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

public class ThroughputLimiterTest
{
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final double MIB = 1024 * 1024;

    /** A clock that only moves when a caller parks, so a test sees exactly how long each charge waits. */
    private static class FakeClock
    {
        final AtomicLong now = new AtomicLong(1_000 * SECOND);

        long nanoTime()
        {
            return now.get();
        }

        void park(long nanos)
        {
            now.addAndGet(nanos);
        }

        ThroughputLimiter limiter(double bytesPerSecond)
        {
            return new ThroughputLimiter(bytesPerSecond, this::nanoTime, this::park);
        }
    }

    @Test
    public void noLimitNeverWaits()
    {
        for (double rate : new double[]{ 0, -1, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN })
        {
            FakeClock clock = new FakeClock();
            long start = clock.nanoTime();
            ThroughputLimiter limiter = clock.limiter(rate);
            for (int i = 0; i < 1000; i++)
                limiter.acquire(1 << 20);
            assertThat(clock.nanoTime()).as("rate %s", rate).isEqualTo(start);
            assertThat(limiter.bytesCharged()).isEqualTo(1000L << 20);
            assertThat(limiter.isLimited()).isFalse();
        }
    }

    @Test
    public void emptyChargesAreIgnored()
    {
        FakeClock clock = new FakeClock();
        long start = clock.nanoTime();
        ThroughputLimiter limiter = clock.limiter(1);
        limiter.acquire(0);
        limiter.acquire(-5);
        assertThat(clock.nanoTime()).isEqualTo(start);
        assertThat(limiter.bytesCharged()).isZero();
    }

    @Test
    public void eachChargeWaitsForTheOneBeforeIt()
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter limiter = clock.limiter(MIB);
        long start = clock.nanoTime();
        limiter.acquire((long) MIB / 4);
        assertThat(clock.nanoTime() - start).isZero();
        limiter.acquire((long) MIB / 2);
        assertThat(clock.nanoTime() - start).isEqualTo(SECOND / 4);
        limiter.acquire(1);
        assertThat(clock.nanoTime() - start).isEqualTo(SECOND / 4 + SECOND / 2);
    }

    @Test
    public void steadyLoadRunsAtTheRate()
    {
        FakeClock clock = new FakeClock();
        double rate = 64 * MIB;
        int chunk = 64 * 1024;
        ThroughputLimiter limiter = clock.limiter(rate);
        long start = clock.nanoTime();
        long total = 0;
        for (int i = 0; i < 10_000; i++)
        {
            limiter.acquire(chunk);
            total += chunk;
        }
        // The last charge is not yet paid for.
        assertThat((double) (clock.nanoTime() - start)).isCloseTo((total - chunk) / rate * SECOND, offset(10_000.0));
    }

    @Test
    public void noSecondExceedsTheRatePlusOneIO()
    {
        FakeClock clock = new FakeClock();
        long rate = 10 << 20;
        ThroughputLimiter limiter = clock.limiter(rate);
        Random random = new Random(42);
        List<long[]> completions = new ArrayList<>(); // {time the I/O completed, bytes}
        long largest = 0;
        for (int i = 0; i < 5_000; i++)
        {
            long bytes = i == 2_500 ? 5L * rate : 1 + random.nextInt(4 << 20);
            largest = Math.max(largest, bytes);
            completions.add(new long[]{ clock.nanoTime(), bytes });
            limiter.acquire(bytes);
        }

        int from = 0;
        long inWindow = 0;
        for (long[] completion : completions)
        {
            inWindow += completion[1];
            while (completions.get(from)[0] <= completion[0] - SECOND)
                inWindow -= completions.get(from++)[1];
            // A wait shorter than MIN_WAIT_NANOS is left to a later charge, and up to UNUSED_TIME_SAVED_NANOS carries over.
            long slack = rate * (ThroughputLimiter.UNUSED_TIME_SAVED_NANOS + ThroughputLimiter.MIN_WAIT_NANOS) / SECOND;
            assertThat(inWindow).isLessThanOrEqualTo(rate + largest + slack);
        }
    }

    @Test
    public void idleTimeIsNotSaved()
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter limiter = clock.limiter(MIB);
        clock.park(60 * SECOND);
        long start = clock.nanoTime();
        for (int i = 0; i < 3; i++)
            limiter.acquire((long) MIB);
        assertThat(clock.nanoTime() - start).isEqualTo(2 * SECOND - ThroughputLimiter.UNUSED_TIME_SAVED_NANOS);
    }

    /** A thread that wakes late does not lose the time it overslept, so the limit is met, not undershot. */
    @Test
    public void lateWakeUpsAreMadeUp()
    {
        FakeClock clock = new FakeClock();
        long late = ThroughputLimiter.UNUSED_TIME_SAVED_NANOS / 2;
        ThroughputLimiter limiter = new ThroughputLimiter(MIB, clock::nanoTime, nanos -> clock.park(nanos + late));
        int chunk = 16 * 1024;
        long start = clock.nanoTime();
        for (int i = 0; i <= 1024; i++)
            limiter.acquire(chunk);
        // 1024 chunks paid for, at 1 MiB/s, waking late every time.
        assertThat(clock.nanoTime() - start).isBetween(16 * SECOND, 16 * SECOND + late);
    }

    @Test
    public void raisingTheRateDropsTheDebt()
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter limiter = clock.limiter(1);
        limiter.acquire(86_400); // a day at 1 B/s, which the next charge waits for
        limiter.setRate(MIB);
        long start = clock.nanoTime();
        limiter.acquire((long) MIB);
        assertThat(clock.nanoTime() - start).isZero();
        limiter.acquire(1);
        assertThat(clock.nanoTime() - start).isEqualTo(SECOND);
    }

    /** nodetool setcompactionthroughput reaches a thread that is already waiting, within one park. */
    @Test
    public void setRateEndsAWait()
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter[] limiter = new ThroughputLimiter[1];
        limiter[0] = new ThroughputLimiter(1, clock::nanoTime, nanos -> {
            clock.park(nanos);
            limiter[0].setRate(0);
        });
        limiter[0].acquire(3_600); // an hour at 1 B/s, which the next charge waits for
        long start = clock.nanoTime();
        limiter[0].acquire(1);
        assertThat(clock.nanoTime() - start).isEqualTo(ThroughputLimiter.MAX_PARK_NANOS);
    }

    /** Racing setRate calls leave the limit that getRate reports as the one in force. */
    @Test
    public void racingSetRateCallsLeaveOneLimit() throws InterruptedException
    {
        for (int round = 0; round < 50; round++)
        {
            FakeClock clock = new FakeClock();
            ThroughputLimiter limiter = clock.limiter(MIB);
            List<Thread> setters = new ArrayList<>();
            for (int t = 0; t < 4; t++)
            {
                double rate = (t + 1) * MIB;
                Thread setter = new Thread(() -> {
                    for (int i = 0; i < 1000; i++)
                        limiter.setRate(rate);
                });
                setters.add(setter);
                setter.start();
            }
            for (Thread setter : setters)
                setter.join();
            long start = clock.nanoTime();
            limiter.acquire((long) limiter.getRate());
            limiter.acquire(1);
            assertThat(clock.nanoTime() - start).isBetween(SECOND - 1, SECOND + 1);
        }
    }

    /** A wait under MIN_WAIT_NANOS costs no park; a later charge waits for it, so the rate still holds. */
    @Test
    public void smallWaitsAreLeftToALaterCharge()
    {
        FakeClock clock = new FakeClock();
        int[] parks = { 0 };
        ThroughputLimiter limiter = new ThroughputLimiter(MIB, clock::nanoTime, nanos -> { parks[0]++; clock.park(nanos); });
        int bytes = 16; // about 15 µs at 1 MiB/s
        long start = clock.nanoTime();
        for (int i = 0; i <= 65_536; i++)
            limiter.acquire(bytes);
        assertThat((double) (clock.nanoTime() - start)).isCloseTo(SECOND, offset((double) ThroughputLimiter.MIN_WAIT_NANOS));
        assertThat(parks[0]).isLessThanOrEqualTo((int) (SECOND / ThroughputLimiter.MIN_WAIT_NANOS) + 1);
    }

    /** Charges that would wait for centuries saturate, so the time they are paid for never wraps around. */
    @Test
    public void hugeChargesSaturate()
    {
        FakeClock clock = new FakeClock();
        long[] lastWait = { 0 };
        ThroughputLimiter limiter = new ThroughputLimiter(Double.MIN_VALUE, clock::nanoTime, nanos -> {
            lastWait[0] = nanos;
            Thread.currentThread().interrupt();
        });
        for (int i = 0; i < 10; i++)
        {
            limiter.acquire(Long.MAX_VALUE);
            Thread.interrupted();
            if (i > 0)
                assertThat(lastWait[0]).isPositive();
        }
    }

    @Test
    public void setRateChangesTheCostOfLaterCharges()
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter limiter = clock.limiter(MIB);
        limiter.setRate(2 * MIB);
        assertThat(limiter.getRate()).isEqualTo(2 * MIB);
        long start = clock.nanoTime();
        limiter.acquire((long) MIB);
        limiter.acquire(1);
        assertThat(clock.nanoTime() - start).isEqualTo(SECOND / 2);

        limiter.setRate(0);
        long unlimited = clock.nanoTime();
        limiter.acquire(1L << 40);
        limiter.acquire(1L << 40);
        assertThat(clock.nanoTime()).isEqualTo(unlimited);
    }

    @Test
    public void interruptEndsTheWaitAndKeepsTheStatus() throws InterruptedException
    {
        ThroughputLimiter limiter = new ThroughputLimiter(1);
        CountDownLatch charging = new CountDownLatch(1);
        AtomicLong waitedNanos = new AtomicLong();
        AtomicLong interrupted = new AtomicLong();
        Thread thread = new Thread(() -> {
            charging.countDown();
            long start = System.nanoTime();
            limiter.acquire(3_600); // an hour at 1 B/s, which the next charge waits for
            limiter.acquire(1);
            waitedNanos.set(System.nanoTime() - start);
            interrupted.set(Thread.currentThread().isInterrupted() ? 1 : 0);
        });
        thread.start();
        charging.await();
        Thread.sleep(100);
        thread.interrupt();
        thread.join(TimeUnit.SECONDS.toMillis(30));
        assertThat(thread.isAlive()).isFalse();
        assertThat(waitedNanos.get()).isLessThan(TimeUnit.SECONDS.toNanos(30));
        assertThat(interrupted.get()).isEqualTo(1);
    }

    /** Many threads sharing one limiter get the rate between them, not each. */
    @Test
    public void threadsShareTheRate() throws InterruptedException
    {
        double rate = 32 * MIB;
        ThroughputLimiter limiter = new ThroughputLimiter(rate);
        int threads = 8;
        long perThread = 2 << 20;
        List<Thread> workers = new ArrayList<>();
        long start = System.nanoTime();
        for (int t = 0; t < threads; t++)
        {
            Thread worker = new Thread(() -> {
                for (long done = 0; done < perThread; done += 64 * 1024)
                    limiter.acquire(64 * 1024);
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers)
            worker.join();
        double seconds = (System.nanoTime() - start) / (double) SECOND;
        double expected = threads * perThread / rate;
        // Each thread may finish up to one I/O ahead of the limit.
        assertThat(seconds).isGreaterThanOrEqualTo(expected - threads * 64 * 1024 / rate);
        assertThat(limiter.bytesCharged()).isEqualTo(threads * perThread);
    }

    /** A priority charge does not wait behind ordinary charges, but an ordinary charge waits for priority charges. */
    @Test
    public void priorityChargesWaitOnlyForEarlierPriorityCharges()
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter limiter = clock.limiter(MIB);
        long start = clock.nanoTime();
        limiter.acquire(10 * (long) MIB);
        limiter.acquirePriority((long) MIB);
        assertThat(clock.nanoTime() - start).isZero();
        limiter.acquirePriority(1);
        assertThat(clock.nanoTime() - start).isEqualTo(SECOND);
        limiter.acquire(1);
        // The ordinary charge waits for the 10 MiB, the 1 MiB and the 1 byte charged before it.
        long oneByte = (long) Math.ceil(SECOND / MIB);
        assertThat(clock.nanoTime() - start).isEqualTo(11 * SECOND + oneByte);
    }

    @Test
    public void ordinaryChargesWaitForEarlierPriorityCharges()
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter limiter = clock.limiter(MIB);
        long start = clock.nanoTime();
        limiter.acquirePriority(2 * (long) MIB);
        limiter.acquire(1);
        assertThat(clock.nanoTime() - start).isEqualTo(2 * SECOND);
    }

    @Test
    public void priorityCountersMatchTheBytesAndTheWait() throws InterruptedException
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter limiter = clock.limiter(MIB);
        limiter.acquire((long) MIB);
        assertThat(limiter.priorityBytesCharged()).isZero();
        for (int i = 0; i < 3; i++)
            limiter.acquirePriority((long) MIB);
        assertThat(limiter.bytesCharged()).isEqualTo(4 * (long) MIB);
        assertThat(limiter.priorityBytesCharged()).isEqualTo(3 * (long) MIB);
        assertThat(limiter.priorityWaitNanos()).isEqualTo(2 * SECOND);
        assertThat(limiter.threadPriorityWaitNanos()).isEqualTo(2 * SECOND);

        long[] otherThread = { -1 };
        Thread thread = new Thread(() -> otherThread[0] = limiter.threadPriorityWaitNanos());
        thread.start();
        thread.join();
        assertThat(otherThread[0]).isZero();

        ThroughputLimiter unlimited = clock.limiter(0);
        unlimited.acquirePriority((long) MIB);
        unlimited.acquirePriority((long) MIB);
        assertThat(unlimited.priorityBytesCharged()).isEqualTo(2 * (long) MIB);
        assertThat(unlimited.priorityWaitNanos()).isZero();
    }

    /** nodetool setcompactionthroughput reaches a priority thread that is already waiting, within one park. */
    @Test
    public void setRateEndsAPriorityWait()
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter[] limiter = new ThroughputLimiter[1];
        limiter[0] = new ThroughputLimiter(16 * MIB, clock::nanoTime, nanos -> {
            clock.park(nanos);
            limiter[0].setRate(0);
        });
        limiter[0].acquirePriority(32 * (long) MIB); // two seconds at 16 MiB/s, which the next charge waits for
        long start = clock.nanoTime();
        limiter[0].acquirePriority(1);
        assertThat(clock.nanoTime() - start).isEqualTo(ThroughputLimiter.MAX_PARK_NANOS);
        assertThat(limiter[0].priorityWaitNanos()).isEqualTo(ThroughputLimiter.MAX_PARK_NANOS);
    }

    @Test
    public void setRateDropsThePriorityDebt()
    {
        FakeClock clock = new FakeClock();
        ThroughputLimiter limiter = clock.limiter(16 * MIB);
        limiter.acquirePriority(160 * (long) MIB); // ten seconds at 16 MiB/s
        limiter.setRate(32 * MIB);
        long start = clock.nanoTime();
        limiter.acquirePriority(32 * (long) MIB);
        limiter.acquire(1);
        assertThat(clock.nanoTime() - start).isEqualTo(SECOND);
    }

    /**
     * Four threads charge with {@link ThroughputLimiter#acquire} and two with
     * {@link ThroughputLimiter#acquirePriority}, and the priority threads are not held back by the others.  In any
     * second they move no more than the rate, plus one I/O per priority thread, plus two I/Os per ordinary thread: the
     * one it has done and the one it reserved before the priority charges.
     */
    @Test
    public void priorityAndOrdinaryThreadsShareTheRate() throws InterruptedException
    {
        long rate = 8 << 20;
        int io = 1 << 20;
        ThroughputLimiter limiter = new ThroughputLimiter(rate);
        long deadline = System.nanoTime() + 3 * SECOND;
        List<long[]> completions = new ArrayList<>(); // {time the I/O completed, bytes, 1 if priority}
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < 6; t++)
        {
            boolean priority = t < 2;
            Thread worker = new Thread(() -> {
                List<long[]> mine = new ArrayList<>();
                long now;
                while ((now = System.nanoTime()) < deadline)
                {
                    mine.add(new long[]{ now, io, priority ? 1 : 0 });
                    if (priority)
                        limiter.acquirePriority(io);
                    else
                        limiter.acquire(io);
                }
                synchronized (completions)
                {
                    completions.addAll(mine);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers)
            worker.join();

        completions.sort((a, b) -> Long.compare(a[0], b[0]));
        int from = 0;
        long inWindow = 0;
        long priorityBytes = 0;
        long total = 0;
        for (long[] completion : completions)
        {
            total += completion[1];
            priorityBytes += completion[1] * completion[2];
            inWindow += completion[1];
            while (completions.get(from)[0] <= completion[0] - SECOND)
                inWindow -= completions.get(from++)[1];
            long slack = rate * (ThroughputLimiter.UNUSED_TIME_SAVED_NANOS + ThroughputLimiter.MIN_WAIT_NANOS) / SECOND;
            assertThat(inWindow).isLessThanOrEqualTo(rate + (2 * 4 + 2) * (long) io + slack);
        }
        // Two priority threads can use the whole rate, so they move most of the bytes.
        assertThat(priorityBytes).as("%d of %d bytes", priorityBytes, total).isGreaterThan(total / 2);
        assertThat(limiter.priorityBytesCharged()).isEqualTo(priorityBytes);
    }

    /** nanoTime may be negative or wrap around; a wait is still its full length, never none. */
    @Test
    public void waitsHoldAcrossAnyClockValue()
    {
        for (long origin : new long[]{ -1_000 * SECOND, Long.MIN_VALUE + SECOND, Long.MAX_VALUE - SECOND / 2 })
        {
            AtomicLong now = new AtomicLong(origin);
            ThroughputLimiter limiter = new ThroughputLimiter(MIB, now::get, now::addAndGet);
            limiter.acquire((long) MIB);
            limiter.acquire(1);
            assertThat(now.get() - origin).as("origin %d", origin).isEqualTo(SECOND);
        }
    }
}
