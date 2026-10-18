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

package org.apache.cassandra.db.memtable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.memory.EnsureOnHeap;

/**
 * One memtable partition's content, read once ({@link Memtable#partitionData}), with what the
 * memtable's row iterator applies to it: the key the iterator reports and the clone to heap of
 * the objects it gives ({@code EnsureOnHeap}).
 */
public final class MemtablePartitionData
{
    /** The partition key the memtable's row iterator reports, already on heap. */
    public final DecoratedKey key;
    public final BTreePartitionData data;
    /** {@code EnsureOnHeap.NOOP} when the memtable's objects are on heap. */
    public final EnsureOnHeap ensureOnHeap;
    /** The partition the memtable reads this content through. */
    private final AbstractBTreePartition partition;

    MemtablePartitionData(DecoratedKey key, BTreePartitionData data, EnsureOnHeap ensureOnHeap, AbstractBTreePartition partition)
    {
        this.key = key;
        this.data = data;
        this.ensureOnHeap = ensureOnHeap;
        this.partition = partition;
    }

    /**
     * The clone to heap the memtable's row iterator for a read of named clusterings applies
     * ({@code AbstractBTreePartition.unfilteredIterator} with a set of clusterings, as a range
     * read's names filter reads a memtable partition).  {@code AtomicBTreePartition} overrides only
     * the slices form, so its names iterator gives the memtable's own objects.
     */
    public EnsureOnHeap ensureOnHeapForNames()
    {
        return partition instanceof AtomicBTreePartition ? EnsureOnHeap.NOOP : ensureOnHeap;
    }

    /** The memtable's row iterator over this content: {@code memtable.rowIterator} of the same snapshot. */
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return partition.unfilteredIterator(data, selection, slices, reversed);
    }
}
