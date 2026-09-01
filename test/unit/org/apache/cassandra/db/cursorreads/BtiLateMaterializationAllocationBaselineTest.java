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
 * The {@link LateMaterializationAllocationBaselineTest} shapes under the BTI format, so M3's
 * before/after comparisons have baselines for both formats. The paged shape is the interesting
 * one here: each page's forPaging command is a sub-slice, so BTI legs seek (M1/M2.2) to the page
 * start instead of walking from the partition head — the per-page remainder re-materialization
 * being baselined is the same, but the constant per page differs.
 */
public class BtiLateMaterializationAllocationBaselineTest extends LateMaterializationAllocationBaselineTest
{
    private SSTableFormat<?, ?> originalFormat;

    /** Under the M3.1 production bound, each BTI page seeks past its already-returned prefix
     *  (M1/M2.2) AND stops at its page budget, so the paging sequence materializes just the rows
     *  it returns (8 x 128 = 1024) plus the block-granularity seek overshoot (the seek parks at
     *  the row-index-block boundary at-or-before each page's resume point, so a few pre-slice
     *  rows per page-leg still materialize — the same 169-row overshoot the M3.0 baseline
     *  measured, unchanged by the bound since those rows precede the counted slice). MEASURED:
     *  1193 = 1024 + 169, down from the M3.0 baseline's 4777 (remainder-sum + overshoot).
     *  Layout-sensitive: re-measure and re-record if the index encoding, block size or this
     *  workload changes. */
    @Override
    protected long expectedPagedMaterialization()
    {
        return 1193;
    }

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
