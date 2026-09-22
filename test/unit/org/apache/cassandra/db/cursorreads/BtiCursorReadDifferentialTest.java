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
 * The full {@link BasicCursorReadDifferentialTest} corpus with the BTI format selected — every
 * sstable the scenarios flush is written and read as BTI, so the cursor path's partition lookup
 * ({@code sstable.getPosition(key, EQ, listener)}, resolved by {@code BtiTableReader} through the
 * Partitions.db trie) and {@code SSTableCursorReader}'s walk of a BTI Data.db are exercised over
 * the whole differential surface. Same doubling pattern as the cursor-compaction precedent
 * ({@code BtiDifferentialCompactionTest extends EdgeCaseDifferentialCompactionTest}).
 */
public class BtiCursorReadDifferentialTest extends BasicCursorReadDifferentialTest
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
