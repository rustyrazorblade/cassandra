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

package org.apache.cassandra.repair;

import java.util.Map;

import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public abstract class ValidationPartitionIterator extends AbstractUnfilteredPartitionIterator
{
    public abstract long getEstimatedBytes();
    public abstract long estimatedPartitions();
    public abstract long getBytesRead();
    public abstract Map<Range<Token>, Long> getRangePartitionCounts();

    /**
     * Feeds one partition produced by {@link #next()} into {@code validator}. The default simply
     * digests the partition and calls {@link Validator#add}; a subclass whose partitions aren't
     * real row/cell content (e.g. one that produces a digest directly from a lower-level source)
     * overrides this to call {@link Validator#addDigest} instead, so the driving loop in
     * {@code ValidationManager#doValidation} never needs to know which strategy produced the
     * partition it's holding.
     */
    public void feed(Validator validator, UnfilteredRowIterator partition)
    {
        validator.add(partition);
    }
}
