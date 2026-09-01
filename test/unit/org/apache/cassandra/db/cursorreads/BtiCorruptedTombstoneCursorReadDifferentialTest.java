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
 * {@link CorruptedTombstoneCursorReadDifferentialTest} with the BTI format selected: the
 * {@code UnfilteredValidation} wiring on cursor-served legs (Gap C) must behave identically —
 * same refusal, same diagnosis, same mark-suspect side effect — when the invalid deletions live
 * in a BTI sstable. The corruption fixtures themselves are format-portable (both formats share
 * the Data.db unfiltered serialization, and BTI's only version, like the latest BIG version, uses
 * the uint-ldt encoding the far-future asymmetry pin depends on); what this subclass adds is the
 * BTI lookup path in front of them.
 */
public class BtiCorruptedTombstoneCursorReadDifferentialTest extends CorruptedTombstoneCursorReadDifferentialTest
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
