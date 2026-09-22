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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end coverage for CASSANDRA-21452 on a real multi-node cluster with real repair and
 * inconsistent replicas.  It checks the property the feature must preserve: cursor-backed
 * validation detects the same inconsistencies the legacy path does, so repair still converges the
 * replicas.  A validator that produced wrong digests would miss real inconsistencies or manufacture
 * false ones.
 * <p>
 * It also confirms the cursor-backed path actually ran, by grepping for
 * {@link org.apache.cassandra.db.repair.CursorValidationIterator}'s log line, both with
 * {@code cursor_compaction_enabled} on (the default) and off.
 */
public class CursorValidationCompactionRepairTest extends TestBaseImpl
{
    private static final String TABLE = "cursor_validation_repair_test";
    private static final String CURSOR_LOG_LINE = "Performing cursor-backed validation compaction";

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(3)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .with(NETWORK, GOSSIP))
                              .start());
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    private void populateWithInconsistency(String table) throws Exception
    {
        cluster.schemaChange(String.format("DROP TABLE IF EXISTS %s.%s;", KEYSPACE, table));
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v text) WITH compression = {'enabled': false}", KEYSPACE, table));

        // Data every replica agrees on.
        for (int k = 0; k < 500; k++)
            for (int node = 1; node <= 3; node++)
                cluster.get(node).executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", KEYSPACE, table),
                                                  k, "agreed" + k);

        // Data only node 1 has - real inconsistency for repair to detect and fix.
        for (int k = 500; k < 1000; k++)
            cluster.get(1).executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", KEYSPACE, table),
                                           k, "onlyNode1_" + k);

        for (int node = 1; node <= 3; node++)
            cluster.get(node).runOnInstance(rethrow(() -> StorageService.instance.forceKeyspaceFlush(KEYSPACE, ColumnFamilyStore.FlushReason.UNIT_TESTS)));

        // Confirm the inconsistency actually exists before repair - otherwise this test proves nothing.
        for (int node = 2; node <= 3; node++)
        {
            Object[][] rows = cluster.get(node).executeInternal(String.format("SELECT * FROM %s.%s WHERE k = ?", KEYSPACE, table), 750);
            assertEquals("test setup: node " + node + " should NOT have the node-1-only row yet", 0, rows.length);
        }
    }

    private void verifyConverged(String table) throws Exception
    {
        for (int node = 1; node <= 3; node++)
        {
            for (int k : new int[]{ 0, 499, 500, 750, 999 })
            {
                Object[][] rows = cluster.get(node).executeInternal(String.format("SELECT k, v FROM %s.%s WHERE k = ?", KEYSPACE, table), k);
                String expected = k < 500 ? "agreed" + k : "onlyNode1_" + k;
                assertRows(rows, new Object[]{ k, expected });
            }
        }
    }

    @Test
    public void cursorBackedRepairConvergesInconsistentReplicas() throws Exception
    {
        populateWithInconsistency(TABLE);

        long mark1 = cluster.get(1).logs().mark();

        cluster.get(1).nodetoolResult("repair", "-full", KEYSPACE, TABLE).asserts().success();

        verifyConverged(TABLE);

        LogResult<List<String>> cursorLogLines = cluster.get(1).logs().grep(mark1, CURSOR_LOG_LINE);
        assertFalse("expected the cursor-backed validation path to actually run (cursor_compaction_enabled defaults to true) - " +
                   "found no '" + CURSOR_LOG_LINE + "' log line, meaning repair silently used the legacy path instead",
                   cursorLogLines.getResult().isEmpty());
    }

    @Test
    public void legacyRepairStillConvergesWithCursorValidationDisabled() throws Exception
    {
        String table = TABLE + "_legacy";
        for (int node = 1; node <= 3; node++)
            cluster.get(node).runOnInstance(() -> DatabaseDescriptor.setCursorCompactionEnabled(false));
        try
        {
            populateWithInconsistency(table);

            long mark1 = cluster.get(1).logs().mark();

            cluster.get(1).nodetoolResult("repair", "-full", KEYSPACE, table).asserts().success();

            verifyConverged(table);

            LogResult<List<String>> cursorLogLines = cluster.get(1).logs().grep(mark1, CURSOR_LOG_LINE);
            assertTrue("cursor_compaction_enabled=false must not run the cursor-backed validation path",
                      cursorLogLines.getResult().isEmpty());
        }
        finally
        {
            for (int node = 1; node <= 3; node++)
                cluster.get(node).runOnInstance(() -> DatabaseDescriptor.setCursorCompactionEnabled(true));
        }
    }
}
