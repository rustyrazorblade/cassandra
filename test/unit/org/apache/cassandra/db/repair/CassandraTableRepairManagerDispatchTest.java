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

package org.apache.cassandra.db.repair;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.ValidationPartitionIterator;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins {@link CassandraTableRepairManager#getValidationIterator}'s dispatch decision, the entry
 * point real repair goes through, across the three cases that matter: cursor validation enabled
 * with a supported table (dispatches to {@link CursorValidationIterator}), disabled via the
 * {@code cursor_repair_enabled} toggle (falls back to {@link CassandraValidationIterator}), and a
 * schema the cursor path does not support (a secondary index) even with the toggle on (also falls
 * back).
 */
public class CassandraTableRepairManagerDispatchTest extends CQLTester
{
    private ValidationPartitionIterator dispatch(ColumnFamilyStore cfs) throws Exception
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("10.0.0.3");
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        TimeUUID parentId = nextTimeUUID();
        Range<Token> fullRange = new Range<>(minimumToken, minimumToken);
        ActiveRepairService.instance().registerParentRepairSession(parentId,
                                                                    coordinator,
                                                                    Lists.newArrayList(cfs),
                                                                    Sets.newHashSet(fullRange),
                                                                    false,
                                                                    ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                    true,
                                                                    PreviewKind.NONE);
        List<Range<Token>> ranges = Collections.singletonList(fullRange);

        CassandraTableRepairManager manager = new CassandraTableRepairManager(cfs, SharedContext.Global.instance);
        return manager.getValidationIterator(ranges, parentId, nextTimeUUID(), false, FBUtilities.nowInSeconds(),
                                             (TopPartitionTracker.Collector) null);
    }

    private static void assertCounters(long runs, long fallbacks, long runsBefore, long fallbacksBefore)
    {
        assertEquals("cursor validation runs", runs, CassandraTableRepairManager.cursorValidationsRun.sum() - runsBefore);
        assertEquals("cursor validation fallbacks", fallbacks, CassandraTableRepairManager.cursorValidationFallbacks.sum() - fallbacksBefore);
    }

    @Test
    public void dispatchesToCursorWhenEnabledAndSupported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 1L, "v");
        flush();

        boolean original = DatabaseDescriptor.cursorRepairEnabled();
        DatabaseDescriptor.setCursorRepairEnabled(true);
        long runsBefore = CassandraTableRepairManager.cursorValidationsRun.sum();
        long fallbacksBefore = CassandraTableRepairManager.cursorValidationFallbacks.sum();
        try (ValidationPartitionIterator iterator = dispatch(cfs))
        {
            assertCounters(1, 0, runsBefore, fallbacksBefore);
            assertTrue("expected CursorValidationIterator, got " + iterator.getClass(),
                       iterator instanceof CursorValidationIterator);
        }
        finally
        {
            DatabaseDescriptor.setCursorRepairEnabled(original);
        }
    }

    @Test
    public void fallsBackToLegacyWhenDisabled() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 1L, "v");
        flush();

        boolean original = DatabaseDescriptor.cursorRepairEnabled();
        DatabaseDescriptor.setCursorRepairEnabled(false);
        long runsBefore = CassandraTableRepairManager.cursorValidationsRun.sum();
        long fallbacksBefore = CassandraTableRepairManager.cursorValidationFallbacks.sum();
        try (ValidationPartitionIterator iterator = dispatch(cfs))
        {
            assertCounters(0, 0, runsBefore, fallbacksBefore);
            assertTrue("expected CassandraValidationIterator, got " + iterator.getClass(),
                       iterator instanceof CassandraValidationIterator);
        }
        finally
        {
            DatabaseDescriptor.setCursorRepairEnabled(original);
        }
    }

    @Test
    public void fallsBackToLegacyForUnsupportedSchema() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        createIndex("CREATE INDEX ON %s (v)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 1L, "v");
        flush();

        boolean original = DatabaseDescriptor.cursorRepairEnabled();
        DatabaseDescriptor.setCursorRepairEnabled(true);
        long runsBefore = CassandraTableRepairManager.cursorValidationsRun.sum();
        long fallbacksBefore = CassandraTableRepairManager.cursorValidationFallbacks.sum();
        try (ValidationPartitionIterator iterator = dispatch(cfs))
        {
            assertCounters(0, 1, runsBefore, fallbacksBefore);
            assertTrue("expected CassandraValidationIterator (2i is unsupported), got " + iterator.getClass(),
                       iterator instanceof CassandraValidationIterator);
        }
        finally
        {
            DatabaseDescriptor.setCursorRepairEnabled(original);
        }
    }
}
