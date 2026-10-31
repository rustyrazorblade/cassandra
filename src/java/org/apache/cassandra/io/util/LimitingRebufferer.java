/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Rebufferer wrapper that applies rate limiting.
 * <p>
 * Instantiated once per RandomAccessReader, thread-unsafe.
 * The instances reuse themselves as the BufferHolder to avoid having to return a new object for each rebuffer call.
 * Only one BufferHolder can be active at a time. Calling {@link #rebuffer(long)} before the previously obtained
 * buffer holder is released will throw {@link AssertionError}.
 */
@NotThreadSafe
public class LimitingRebufferer extends WrappingRebufferer
{
    final private Object limiter;
    final private IntConsumer acquire;
    final private BooleanSupplier isLimited;
    final private int limitQuant;

    public LimitingRebufferer(Rebufferer wrapped, RateLimiter limiter, int limitQuant)
    {
        this(wrapped, limiter, limiter::acquire, () -> true, limitQuant);
    }

    public LimitingRebufferer(Rebufferer wrapped, ThroughputLimiter limiter, int limitQuant)
    {
        this(wrapped, limiter, limiter::acquire, limiter::isLimited, limitQuant);
    }

    private LimitingRebufferer(Rebufferer wrapped, Object limiter, IntConsumer acquire, BooleanSupplier isLimited, int limitQuant)
    {
        super(wrapped);
        this.limiter = limiter;
        this.acquire = acquire;
        this.isLimited = isLimited;
        this.limitQuant = limitQuant;
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        super.rebuffer(position);
        int posInBuffer = Ints.checkedCast(position - offset);
        int remaining = buffer.limit() - posInBuffer;
        if (remaining == 0)
            return this;

        // Smaller buffers spread the charges, which only matters while a limit is set.
        if (remaining > limitQuant && isLimited.getAsBoolean())
        {
            buffer.limit(posInBuffer + limitQuant); // certainly below current limit
            remaining = limitQuant;
        }
        acquire.accept(remaining);
        return this;
    }

    @Override
    public String toString()
    {
        return "LimitingRebufferer[" + limiter + "]:" + wrapped;
    }
}