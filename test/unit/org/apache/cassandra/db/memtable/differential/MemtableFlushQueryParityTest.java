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

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.partitions.MemtableCursorFlusher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Every other test in this package compares flushed output via {@code ISSTableScanner} +
 * {@code JsonTransformer} - a real, thorough check, but still one single consumption path. This
 * instead runs actual CQL {@code SELECT} statements (point lookups, range/multi-row reads,
 * {@code ORDER BY}, {@code LIMIT}, an aggregate, a static-column-only projection) against a
 * cursor-flushed table through the full production read path
 * ({@code ReadCommand}/{@code SinglePartitionReadCommand}/row and cell filtering) and compares
 * the results against an identically-populated iterator-flushed table, byte-for-byte via
 * {@link UntypedResultSet#toStringUnsafe()}.
 * <p>
 * Byte-identical sstables (which every differential test here already requires) are necessarily
 * read identically by any given reader, so this is deliberately redundant with the JSON-dump
 * comparison as far as what it can catch - it exists as an independent sanity net exercising a
 * different, more directly production-relevant code path, not because the dump comparison is
 * known to have a gap.
 */
public class MemtableFlushQueryParityTest extends CQLTester
{
    @Test
    public void queriesReturnIdenticalResultsThroughCQL() throws Throwable
    {
        String ddl = "CREATE TABLE %s (k int, c int, s text STATIC, v text, v2 bigint, PRIMARY KEY (k, c))";

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
            assertTrue("scenario's table/memtable doesn't satisfy MemtableCursorFlusher.isSupported " +
                       "- won't actually exercise the cursor path; fix the scenario, not this assertion",
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
