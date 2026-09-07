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

package org.apache.cassandra.metrics;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A node with many tables holds tens of thousands of meters: {@code TableMetrics} creates 33
 * meters and 11 timers per table. Constructing one meter must not cost more because other meters
 * already exist, or creating a table gets slower the more tables a keyspace holds.
 */
public class ThreadLocalMeterScalingTest
{
    private static final Logger logger = LoggerFactory.getLogger(ThreadLocalMeterScalingTest.class);

    private static final int BATCH = 2000;
    private static final int FILLER = 8000;

    @Before
    public void before()
    {
        ThreadLocalMeter.disableBackgroundTicking();
    }

    /**
     * Both growable structures behind a meter, the shared rates array and the registry of live
     * meters, used to grow by one entry per meter and copy themselves each time. That made
     * construction cost O(meters already created), so a batch late in the run allocated far more
     * per meter than the same batch early on.
     *
     * The bound is a ratio rather than a byte count, so it needs no re-tuning per JDK or machine.
     */
    @Test
    public void meterConstructionDoesNotScaleWithMeterCount()
    {
        java.lang.management.ThreadMXBean raw = ManagementFactory.getThreadMXBean();
        Assume.assumeTrue(raw instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) raw;
        if (!bean.isThreadAllocatedMemoryEnabled())
            bean.setThreadAllocatedMemoryEnabled(true);

        // strong references: a collected meter frees its rate group id for reuse, which would hide
        // the growth this test measures
        List<ThreadLocalMeter> held = new ArrayList<>(BATCH * 2 + FILLER);

        long early = allocationPerMeter(bean, held, BATCH);
        build(held, FILLER);
        long late = allocationPerMeter(bean, held, BATCH);

        logger.info("meter construction: early={}B/meter late={}B/meter", early, late);
        assertTrue(String.format("meter construction scales with meter count: %,dB -> %,dB per meter",
                                 early, late),
                   late <= early * 2);
    }

    /**
     * The registry of live meters holds one entry per meter, never fewer. It is a set, so a meter
     * that compared equal to another would go missing from the background tick.
     */
    @Test
    public void everyMeterRegistersOnce()
    {
        int before = ThreadLocalMeter.getTickingMetersCount();
        List<ThreadLocalMeter> held = new ArrayList<>(BATCH);
        build(held, BATCH);
        assertEquals(before + BATCH, ThreadLocalMeter.getTickingMetersCount());
    }

    private long allocationPerMeter(com.sun.management.ThreadMXBean bean, List<ThreadLocalMeter> held, int count)
    {
        long tid = Thread.currentThread().getId();
        long start = bean.getThreadAllocatedBytes(tid);
        build(held, count);
        return (bean.getThreadAllocatedBytes(tid) - start) / count;
    }

    private void build(List<ThreadLocalMeter> held, int count)
    {
        for (int i = 0; i < count; i++)
            held.add(new ThreadLocalMeter());
    }
}
