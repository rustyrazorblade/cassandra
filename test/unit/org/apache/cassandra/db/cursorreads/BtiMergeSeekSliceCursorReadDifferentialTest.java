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
 * The {@link MergeSeekSliceCursorReadDifferentialTest} corpus under the BTI format — the
 * configuration M2.2's merged-leg seek was built for. With {@link #seekCapableFormat()} true,
 * every scenario enforces the merged-mode seek-effectiveness guard on top of the differential
 * comparison (exact per-leg seek counts via {@code CursorReads.sstableLegRowIndexSeeks()},
 * block-granular bounds on MERGED materialization via
 * {@code CursorReads.unfilteredsMaterialized()}), and the wrong-seed mutation test — skipped
 * under BIG, where no seed exists to corrupt — actually runs. A merged cursor path that quietly
 * reverted to the M2.1 eager per-leg walk would pass the differential half of these tests by
 * design; this class is what fails it.
 */
public class BtiMergeSeekSliceCursorReadDifferentialTest extends MergeSeekSliceCursorReadDifferentialTest
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
