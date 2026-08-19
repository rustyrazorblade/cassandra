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

import org.junit.After;
import org.junit.Before;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;

/** The allocation gates under BTI: measures the BTI index path including the bounded per-block snapshot allocations (boundary prefix copies, IndexInfo, open-marker snapshots) against the same ceilings. */
public class BtiCursorCompactionAllocationGateTest extends CursorCompactionAllocationGateTest
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void selectBti()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /**
     * BTI pays ~2KB of inherent per-partition index allocation that BIG does not (partition
     * trie nodes, the key/token snapshot, PartitionIndexBuilder internals) — the iterator
     * path pays the same, so it is format cost, not a cursor regression. Measured: the
     * sparse gate's delta moved 449KB (BIG) -> 560KB (BTI) at 54 extra partitions. 768KB
     * keeps ~200KB of trip-wire for real per-row regressions (~8B/row at gate scale).
     */
    @Override
    protected long ceilingBytes()
    {
        return 768 * 1024;
    }

    /**
     * Same inherent per-partition BTI index cost, expressed per input byte for the RT-dense
     * gate: marker-dense partitions are tiny (~10KB), so ~2KB/partition of trie/key-snapshot
     * work adds ~0.2-0.3 B/B on top of the BIG residual. Measured 1.012 B/B (vs BIG's
     * 0.684); a one-small-object-per-marker leak costs >1.5 B/B extra and still trips.
     */
    @Override
    protected double rtPerInputByteCeiling()
    {
        return 1.3;
    }

    /**
     * Unlike the RT/counter gates, the complex-column gate runs at multi-MB scale, so BTI's
     * ~2KB/partition trie/key-snapshot cost is diluted across far more input bytes per
     * partition and barely moves the ratio: measured 0.511 B/B (vs BIG's ~0.5, ceiling 0.5).
     * 0.6 keeps comparable headroom to the other gates while still tripping on a real
     * per-row regression.
     */
    @Override
    protected double complexPerInputByteCeiling()
    {
        return 0.6;
    }
}
