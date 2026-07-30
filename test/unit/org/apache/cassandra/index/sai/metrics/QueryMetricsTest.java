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
package org.apache.cassandra.index.sai.metrics;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import com.datastax.driver.core.ResultSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryMetricsTest extends AbstractMetricsTest
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s.%s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS %s ON %s.%s(%s) USING 'StorageAttachedIndex'";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testSameIndexNameAcrossKeyspaces()
    {
        String table = "test_same_index_name_across_keyspaces";
        String index = "test_same_index_name_across_keyspaces_index";

        String keyspace1 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        String keyspace2 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace1, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace1, table, "v1"));

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace2, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace2, table, "v1"));

        execute("INSERT INTO " + keyspace1 + '.' + table + " (id1, v1, v2) VALUES ('0', 0, '0')");

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace1 + '.' + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        assertEquals(1L, getTableQueryMetrics(keyspace1, table, "TotalQueriesCompleted"));
        assertEquals(1L, getTableQueryMetrics(keyspace1, table, "PostFilteringReadLatency"));
        assertEquals(0L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));
        assertEquals(0L, getTableQueryMetrics(keyspace2, table, "PostFilteringReadLatency"));

        execute("INSERT INTO " + keyspace2 + '.' + table + " (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO " + keyspace2 + '.' + table + " (id1, v1, v2) VALUES ('1', 1, '1')");

        rows = executeNet("SELECT id1 FROM " + keyspace1 + '.' + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        rows = executeNet("SELECT id1 FROM " + keyspace2 + '.' + table + " WHERE v1 = 1");
        assertEquals(1, rows.all().size());

        assertEquals(2L, getTableQueryMetrics(keyspace1, table, "TotalQueriesCompleted"));
        assertEquals(1L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));
        assertEquals(2L, getTableQueryMetrics(keyspace1, table, "PostFilteringReadLatency"));
        assertEquals(1L, getTableQueryMetrics(keyspace2, table, "PostFilteringReadLatency"));
    }

    @Test
    public void testMetricRelease() throws Throwable
    {
        String table = "test_metric_release";
        String index = "test_metric_release_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        execute("INSERT INTO " + keyspace + '.' + table + " (id1, v1, v2) VALUES ('0', 0, '0')");

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + '.' + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        assertEquals(1L, getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted"));

        // If we drop the last index on the table we should no longer see the table-level state metrics:
        dropIndex(String.format("DROP INDEX %s." + index, keyspace));
        assertThatThrownBy(() -> getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted")).hasCauseInstanceOf(InstanceNotFoundException.class);
    }

    /**
     * Pins that {@code TableQueryMetrics.PerQueryMetrics#allocatedBytes} - the JMX histogram added
     * as a baseline measurement point ahead of SAI allocation-reduction work, so it can be read
     * before/after each such change without a profiler - is actually reachable over JMX and
     * actually measures something real (not always reporting 0, which would mean the underlying
     * {@code QueryContext#totalQueryAllocatedBytes} measurement silently isn't working, e.g. because
     * this JVM's {@code ThreadMXBean} doesn't support allocation tracking or the single-thread
     * assumption that measurement depends on doesn't hold here).
     */
    @Test
    public void testAllocatedBytesMetricIsRecorded() throws Throwable
    {
        String table = "test_allocated_bytes_metric";
        String index = "test_allocated_bytes_metric_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        for (int i = 0; i < 10; i++)
            execute(String.format("INSERT INTO %s.%s (id1, v1, v2) VALUES ('%d', %d, '%d')", keyspace, table, i, i, i));

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + '.' + table + " WHERE v1 >= 0");
        assertEquals(10, rows.all().size());

        ObjectName allocatedBytes = objectNameNoIndex("AllocatedBytes", keyspace, table, TableQueryMetrics.PerQueryMetrics.PER_QUERY_METRICS_TYPE);
        assertEquals("exactly one query should have been recorded", 1L, ((Number) getMBeanAttribute(allocatedBytes, "Count")).longValue());
        assertTrue("a real, positive allocation figure should have been measured for that query",
                  ((Number) getMBeanAttribute(allocatedBytes, "Max")).longValue() > 0);
    }
}
