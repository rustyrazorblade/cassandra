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

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;

/**
 * Every scenario of {@link CrossFormatCursorCompactionTest} (inputs in one sstable format and output in the other), anticompacted with random full, transient and unrepaired
 * ranges on the cursor and legacy paths.  It also anticompacts one group that holds a BIG and a
 * BTI sstable, as during a change of the selected format.
 */
public class CrossFormatCursorAntiCompactionTest extends CrossFormatCursorCompactionTest
{
    @Override
    protected boolean expectSingleOutput()
    {
        return false;
    }

    @Override
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs, Set<SSTableReader> inputs,
                                                         TaskFactory taskFactory, long gcBefore) throws Exception
    {
        return assertAntiCompactionMatchesIterator(cfs, inputs, taskFactory, gcBefore);
    }

    @Test
    public void mixedFormatGroupToBti() throws Exception
    {
        assertMixedFormatGroup("bti");
    }

    @Test
    public void mixedFormatGroupToBig() throws Exception
    {
        assertMixedFormatGroup("big");
    }

    private void assertMixedFormatGroup(String outputFormat) throws Exception
    {
        DatabaseDescriptor.setSelectedSSTableFormat("big");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s text static, v text, m map<text, int>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 20; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, s, v, m) VALUES (?, ?, ?, ?, ?)", pk, ck, "s" + pk, "big" + ck, java.util.Map.of("a", (int) ck));
        flush();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        for (long pk = 10; pk < 30; pk++)
        {
            execute("DELETE FROM %s WHERE pk = ? AND ck > ? AND ck < ?", pk, 2L, 5L);
            for (long ck = 5; ck < 15; ck++)
                execute("INSERT INTO %s (pk, ck, v, m) VALUES (?, ?, ?, ?)", pk, ck, "bti" + ck, java.util.Map.of("b", (int) ck));
        }
        flush();
        Set<String> formats = new HashSet<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            formats.add(sstable.descriptor.getFormat().name());
        assertEquals("the group must hold one BIG and one BTI sstable", Set.of("big", "bti"), formats);

        DatabaseDescriptor.setSelectedSSTableFormat(outputFormat);
        assertCursorMatchesIterator(cfs);
    }
}
