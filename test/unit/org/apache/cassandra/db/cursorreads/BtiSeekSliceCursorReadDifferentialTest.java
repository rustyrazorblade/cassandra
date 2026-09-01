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
 * The {@link SeekSliceCursorReadDifferentialTest} corpus under the BTI format — the configuration
 * M1's row-index seek was built for. With {@link #seekCapableFormat()} true, every scenario
 * enforces the seek-effectiveness guard on top of the differential comparison: the expected
 * number of {@code CursorReads.sstableLegRowIndexSeeks()} actually happened (including the
 * must-NOT-seek scenarios asserting zero), and {@code CursorReads.unfilteredsMaterialized()}
 * stayed bounded by index-block granularity instead of the partition's row count. A cursor path
 * that quietly reverted to materialize-everything-then-slice would pass the differential half of
 * these tests by design; this class is what fails it.
 */
public class BtiSeekSliceCursorReadDifferentialTest extends SeekSliceCursorReadDifferentialTest
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

    @Override
    protected boolean seekCapableFormat()
    {
        return true;
    }
}
