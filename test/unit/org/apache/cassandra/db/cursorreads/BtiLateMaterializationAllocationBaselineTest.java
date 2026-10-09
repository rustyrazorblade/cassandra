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
 * The {@link LateMaterializationAllocationBaselineTest} shapes under the BTI format, so the
 * before/after comparisons have baselines for both formats. The paged shape is the interesting
 * one here: each page's forPaging command is a sub-slice, so BTI legs seek to the page start
 * instead of walking from the partition head.  Both formats materialize only the rows the pages
 * return.
 */
public class BtiLateMaterializationAllocationBaselineTest extends LateMaterializationAllocationBaselineTest
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
