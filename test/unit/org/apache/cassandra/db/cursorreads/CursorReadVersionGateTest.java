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

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Direct unit test of the {@link CursorReads#isReadSupported} gate arm requiring every candidate
 * sstable to be at its format's LATEST version (pre-upgrade sstables must fall back to the
 * iterator path — {@code SSTableCursorReader} only understands the current encoding, the same
 * restriction cursor compaction enforces in {@code CursorCompactor.isSupported}).
 *
 * Deliberate shortcut, per the Phase 1 gap-closure plan: this tests the gate LOGIC against real
 * {@link SSTableReader} instances whose descriptors carry a non-latest {@link Version}
 * ({@link MockSchema#sstableWithVersion}), not an end-to-end read over genuine legacy-format
 * fixtures. Each scenario first asserts the gate is OPEN for the same command/table with a
 * latest-version sstable, so a {@code false} on the non-latest list is attributable to the
 * version arm and not to some other arm rejecting the mock setup.
 */
public class CursorReadVersionGateTest
{
    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    @After
    public void resetCursorReadsFlag()
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
    }

    @Test
    public void nonLatestBtiVersionClosesGate()
    {
        assertVersionArm(DatabaseDescriptor.getSSTableFormats().get(BtiFormat.NAME), "da");
    }

    private static void assertVersionArm(SSTableFormat<?, ?> format, String nonLatestVersionName)
    {
        Version latestVersion = format.getLatestVersion();
        Version nonLatestVersion = format.getVersion(nonLatestVersionName);
        // self-checking fixture: if a future bump ever makes this "old" version current, fail
        // loudly here instead of testing the wrong thing
        assertTrue("fixture bug: latest version is not latest", latestVersion.isLatestVersion());
        assertFalse("fixture bug: chosen non-latest version '" + nonLatestVersionName +
                    "' IS the latest version; pick an older one", nonLatestVersion.isLatestVersion());

        ColumnFamilyStore cfs = MockSchema.newCFS("cursor_read_version_gate");
        SinglePartitionReadCommand command = fullPartitionCommand(cfs);
        SSTableReader latest = MockSchema.sstableWithVersion(1, latestVersion, cfs);
        SSTableReader nonLatest = MockSchema.sstableWithVersion(2, nonLatestVersion, cfs);

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            // attribution baseline: every OTHER gate arm admits this command/table/sstable shape
            assertTrue("gate closed for a latest-version sstable; the version-arm assertions below " +
                       "would be vacuous",
                       CursorReads.isReadSupported(command, cfs, ImmutableList.of(latest)));
            // the arm under test
            assertFalse("gate open for a non-latest-version sstable",
                        CursorReads.isReadSupported(command, cfs, ImmutableList.of(nonLatest)));
            // one non-latest candidate among latest ones must close the gate for the whole read
            assertFalse("gate open for a mixed latest/non-latest candidate list",
                        CursorReads.isReadSupported(command, cfs, ImmutableList.of(latest, nonLatest)));
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static SinglePartitionReadCommand fullPartitionCommand(ColumnFamilyStore cfs)
    {
        DecoratedKey key = cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes("key"));
        return SinglePartitionReadCommand.create(cfs.metadata(), FBUtilities.nowInSeconds(), key, Slices.ALL);
    }
}
