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

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Every scenario of {@link MaterializedViewDifferentialCompactionTest} (materialized view tables, which cursor anticompaction accepts as regular cursor compaction does), anticompacted with random full, transient and unrepaired
 * ranges on the cursor and legacy paths.
 */
public class MaterializedViewAntiCompactionDifferentialTest extends MaterializedViewDifferentialCompactionTest
{
    @Override
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs, Set<SSTableReader> inputs,
                                                         TaskFactory taskFactory, long gcBefore) throws Exception
    {
        return assertAntiCompactionMatchesIterator(cfs, inputs, taskFactory, gcBefore);
    }
}
