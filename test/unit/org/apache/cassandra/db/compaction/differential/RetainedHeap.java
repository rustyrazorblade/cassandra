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

package org.apache.cassandra.db.compaction.differential;

/**
 * Watches the heap that stays live after garbage collection: the largest, while it runs, of the
 * heap pools' usage after their last collection, less that usage when it started.
 * <p>
 * That usage also holds garbage that a young collection moved to the old generation and that no
 * old collection has freed yet.  With a lot of allocation, that hides what is really live, so
 * {@link #watchLive} instead takes the live bytes from a class histogram, which runs a full
 * collection first.
 */
public final class RetainedHeap
{
    private final boolean live;
    private final long start;
    private volatile boolean stopped;
    private volatile long max;
    private final Thread sampler;
    long growth;

    private RetainedHeap(boolean live, long intervalMillis)
    {
        this.live = live;
        start = sample();
        max = start;
        sampler = new Thread(() -> {
            while (!stopped)
            {
                max = Math.max(max, sample());
                try
                {
                    Thread.sleep(intervalMillis);
                }
                catch (InterruptedException e)
                {
                    return;
                }
            }
        }, "retained heap sampler");
        sampler.setDaemon(true);
        sampler.start();
    }

    public static RetainedHeap watch()
    {
        return new RetainedHeap(false, 20);
    }

    /** Samples the bytes of live objects every interval; each sample runs a full collection. */
    public static RetainedHeap watchLive(long intervalMillis)
    {
        return new RetainedHeap(true, intervalMillis);
    }

    public void stop() throws InterruptedException
    {
        stopped = true;
        sampler.join();
        max = Math.max(max, sample());
        growth = max - start;
    }

    private long sample()
    {
        return live ? liveBytes() : retained();
    }

    /** The "Total" line of the HotSpot class histogram: the bytes of all live objects. */
    private static long liveBytes()
    {
        try
        {
            String histogram = (String) java.lang.management.ManagementFactory.getPlatformMBeanServer()
                                        .invoke(new javax.management.ObjectName("com.sun.management:type=DiagnosticCommand"),
                                                "gcClassHistogram", new Object[]{ new String[0] }, new String[]{ String[].class.getName() });
            for (String line : histogram.split("\n"))
            {
                String[] fields = line.trim().split("\\s+");
                if (fields.length == 3 && fields[0].equals("Total"))
                    return Long.parseLong(fields[2]);
            }
            throw new IllegalStateException("no Total line in the class histogram");
        }
        catch (javax.management.JMException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static long retained()
    {
        long used = 0;
        for (java.lang.management.MemoryPoolMXBean pool : java.lang.management.ManagementFactory.getMemoryPoolMXBeans())
            if (pool.getType() == java.lang.management.MemoryType.HEAP && pool.getCollectionUsage() != null)
                used += pool.getCollectionUsage().getUsed();
        return used;
    }
}
