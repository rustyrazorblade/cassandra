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

import javax.annotation.Nullable;

/**
 * Interface for the classes that can be used to instantiate rebufferers over a given file.
 *
 * These are one of two types:
 *  - chunk sources (e.g. SimpleReadRebufferer) which instantiate a buffer managing rebufferer referencing
 *    themselves.
 *  - thread-safe shared rebufferers (e.g. MmapRebufferer) which directly return themselves.
 */
public interface RebuffererFactory extends ReaderFileProxy
{
    Rebufferer instantiateRebufferer(boolean isScan);

    /**
     * Instantiates a rebufferer that charges {@code limiter} with the bytes it reads from disk.
     *
     * @param limiter the limiter to charge, or null for none
     */
    default Rebufferer instantiateRebufferer(boolean isScan, @Nullable ThroughputLimiter limiter)
    {
        Rebufferer rebufferer = instantiateRebufferer(isScan);
        return limiter == null ? rebufferer : new LimitingRebufferer(rebufferer, limiter, DiskOptimizationStrategy.MAX_BUFFER_SIZE);
    }
}
