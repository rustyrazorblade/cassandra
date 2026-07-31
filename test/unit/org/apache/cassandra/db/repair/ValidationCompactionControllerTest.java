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

import java.nio.ByteBuffer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

/**
 * Pins the invariant {@code CursorValidationCompactor}'s validation-support check depends on
 * instead of checking {@code CompactionParams.TombstoneOption} directly: {@link ValidationCompactionController}
 * is always constructed with {@code compacting = null} (via {@link org.apache.cassandra.db.compaction.CompactionController}'s
 * 2-arg constructor), so {@code refreshOverlaps()} leaves {@code overlappingSSTables} unconditionally
 * empty and {@link org.apache.cassandra.db.compaction.CompactionController#shadowSources} always
 * returns null/empty - regardless of the table's configured {@code provide_overlapping_tombstones}
 * option and regardless of how many other live, overlapping sstables exist outside whatever was
 * actually being validated. If this ever changes (e.g. {@link ValidationCompactionController}
 * starts passing a real {@code compacting} set), this test must fail loudly, since cursor-path
 * validation compaction relies on this invariant rather than re-checking tombstoneOption.
 */
public class ValidationCompactionControllerTest extends SchemaLoader
{
    private static final String KEYSPACE = "ValidationCompactionControllerTest";
    private static final String CF_NONE = "TombstoneNone";
    private static final String CF_ROW = "TombstoneRow";
    private static final String CF_CELL = "TombstoneCell";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        ServerTestUtils.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    tableWithTombstoneOption(CF_NONE, TombstoneOption.NONE),
                                    tableWithTombstoneOption(CF_ROW, TombstoneOption.ROW),
                                    tableWithTombstoneOption(CF_CELL, TombstoneOption.CELL));
    }

    private static TableMetadata.Builder tableWithTombstoneOption(String table, TombstoneOption option)
    {
        return TableMetadata.builder(KEYSPACE, table)
                             .addPartitionKeyColumn("pk", AsciiType.instance)
                             .addClusteringColumn("ck", AsciiType.instance)
                             .addRegularColumn("val", AsciiType.instance)
                             .compaction(CompactionParams.create(SizeTieredCompactionStrategy.class,
                                                                  ImmutableMap.of("provide_overlapping_tombstones", option.name())));
    }

    @Test
    public void testNeverProvidesShadowSourcesWithTombstoneOptionNone()
    {
        assertNeverProvidesShadowSources(CF_NONE);
    }

    @Test
    public void testNeverProvidesShadowSourcesWithTombstoneOptionRow()
    {
        assertNeverProvidesShadowSources(CF_ROW);
    }

    @Test
    public void testNeverProvidesShadowSourcesWithTombstoneOptionCell()
    {
        assertNeverProvidesShadowSources(CF_CELL);
    }

    private void assertNeverProvidesShadowSources(String table)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(table);
        cfs.truncateBlocking();

        DecoratedKey key = Util.dk("k1");

        // A partition-delete tombstone in its own sstable: exactly the shape that would normally
        // trigger a GarbageSkipper.shadowSources() lookup against overlapping sstables when
        // tombstoneOption != NONE and a real `compacting` set is in play.
        applyDeleteMutation(cfs.metadata(), key, FBUtilities.timestampMicros());
        Util.flush(cfs);

        // A second, live sstable overlapping the same key, outside anything the validator is
        // itself "compacting" - the exact condition that must NOT produce shadow sources here.
        applyMutation(cfs.metadata(), key, FBUtilities.timestampMicros());
        Util.flush(cfs);

        assertTrue(cfs.getLiveSSTables().size() >= 2);

        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, FBUtilities.nowInSeconds()))
        {
            Iterable<UnfilteredRowIterator> shadowSources = controller.shadowSources(key, false);
            assertTrue("ValidationCompactionController must never provide shadow sources (table=" + table + ")",
                       shadowSources == null || Iterables.isEmpty(shadowSources));
        }
    }

    private void applyMutation(TableMetadata cfm, DecoratedKey key, long timestamp)
    {
        ByteBuffer val = ByteBufferUtil.bytes(1L);

        new RowUpdateBuilder(cfm, timestamp, key)
        .clustering("ck")
        .add("val", val)
        .build()
        .applyUnsafe();
    }

    private void applyDeleteMutation(TableMetadata cfm, DecoratedKey key, long timestamp)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(cfm, key, timestamp, FBUtilities.nowInSeconds()))
        .applyUnsafe();
    }
}
