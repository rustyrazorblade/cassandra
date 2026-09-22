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

package org.apache.cassandra.db.cursorreads;

import org.junit.After;
import org.junit.Before;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;

/**
 * {@link CursorReadAllocationGateTest} under the BTI format: the base class's mid-slice
 * measurement runs under BIG, where the cursor path still eagerly materializes whole partitions,
 * so the row-index-seek allocation win never shows up in it. Under BTI the seek and slice-end
 * stop are active, so {@code widePartitionMidSliceEagerMaterializationStaysBounded}'s logged
 * ratio IS the measured seek payoff (BIG baseline: ~4.6x; BTI expectation: near parity). The
 * inherited assertions keep the base class's documented bounds (parity margin, and the loose
 * BIG-calibrated mid-slice ceiling — a BTI-tight ceiling is later gate work).
 */
public class BtiCursorReadAllocationGateTest extends CursorReadAllocationGateTest
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
}
