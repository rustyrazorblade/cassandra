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

package org.apache.cassandra.db.memtable.differential;

import java.util.List;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.partitions.MemtableCursorFlusher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Runs CQL SELECT statements (point lookups, range reads, ORDER BY, LIMIT, an aggregate, a
 * static-column projection) against a cursor-flushed table through the full read path, and
 * compares the results against an identically-populated iterator-flushed table via
 * {@link UntypedResultSet#toStringUnsafe()}.  A read-path check on top of the other tests' output
 * comparisons.
 */
public class MemtableFlushQueryParityTest extends CQLTester
{
    // The cursor flush path requires a heap-based memtable allocator, so pin it rather than let
    // the config-selected one leak in.  The skiplist memtable is pinned on the DDL below.
    private Config.MemtableAllocationType originalAllocationType;

    @Before
    public void pinHeapAllocation()
    {
        originalAllocationType = DatabaseDescriptor.getMemtableAllocationType();
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = Config.MemtableAllocationType.heap_buffers;
    }

    @After
    public void restoreAllocation()
    {
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = originalAllocationType;
    }

    @Test
    public void queriesReturnIdenticalResultsThroughCQL() throws Throwable
    {
        String ddl = "CREATE TABLE %s (k int, c int, s text STATIC, v text, v2 bigint, PRIMARY KEY (k, c)) " +
                     "WITH memtable = 'skiplist'";

        String tableA = createTable(ddl);
        DatabaseDescriptor.setCursorFlushEnabled(false);
        populate(KEYSPACE, tableA);
        flush(KEYSPACE, tableA);

        String tableB = createTable(ddl);
        ColumnFamilyStore cfsB = getColumnFamilyStore(KEYSPACE, tableB);
        DatabaseDescriptor.setCursorFlushEnabled(true);
        try
        {
            populate(KEYSPACE, tableB);
            assertTrue("scenario's table/memtable is not supported by the cursor flush path; fix the scenario",
                       MemtableCursorFlusher.isSupported(cfsB.metadata(), cfsB.getCurrentMemtable()));
            flush(KEYSPACE, tableB);
        }
        finally
        {
            DatabaseDescriptor.setCursorFlushEnabled(false);
        }

        String tA = KEYSPACE + "." + tableA;
        String tB = KEYSPACE + "." + tableB;

        List<Function<String, String>> queries = List.of(
            t -> "SELECT * FROM " + t,
            t -> "SELECT k, c, v FROM " + t + " WHERE k = 1",
            t -> "SELECT * FROM " + t + " WHERE k = 1 AND c >= 3 AND c < 8",
            t -> "SELECT * FROM " + t + " WHERE k = 1 ORDER BY c DESC",
            t -> "SELECT * FROM " + t + " WHERE k = 1 LIMIT 3",
            t -> "SELECT s FROM " + t + " WHERE k = 1",
            t -> "SELECT k, c FROM " + t + " WHERE k = 1 AND c = 5", // deleted row: must come back empty on both
            t -> "SELECT count(*) FROM " + t + " WHERE k = 1",
            t -> "SELECT * FROM " + t + " WHERE k = 2",
            t -> "SELECT * FROM " + t + " WHERE k = 999" // no such partition
        );

        for (Function<String, String> query : queries)
        {
            UntypedResultSet rsA = execute(query.apply(tA));
            UntypedResultSet rsB = execute(query.apply(tB));
            assertEquals("query result mismatch (iterator vs cursor) for: " + query.apply("<table>"),
                        rsA.toStringUnsafe(), rsB.toStringUnsafe());
        }
    }

    private void populate(String ks, String tbl)
    {
        String t = ks + "." + tbl;
        long ts = 5_000_000_000_000L;
        execute("INSERT INTO " + t + " (k, s) VALUES (1, 'static-val') USING TIMESTAMP " + (ts++));
        for (int c = 0; c < 20; c++)
            execute("INSERT INTO " + t + " (k, c, v, v2) VALUES (1, ?, ?, ?) USING TIMESTAMP " + (ts++),
                   c, "value-" + c, (long) c * 100);
        execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = 1 AND c = 5");
        execute("DELETE FROM " + t + " USING TIMESTAMP " + (ts++) + " WHERE k = 1 AND c >= 10 AND c < 14");
        execute("INSERT INTO " + t + " (k, c, v) VALUES (2, 1, 'other-partition') USING TIMESTAMP " + (ts++));
    }
}
