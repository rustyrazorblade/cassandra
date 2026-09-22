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

import java.util.List;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/**
 * Differential coverage for DIGEST queries on the cursor read path (CASSANDRA-20428, gap #3).
 * A digest query returns a hash of the merged partition, not its data bytes. Before this fix the
 * cursor transcode entry point declined every digest query and delegated to the base
 * {@code executeLocally}+{@code createResponse} path, so on a multi-replica read the digest replicas
 * never used the cursor transcode entry point.
 *
 * Each scenario builds the SAME digest command, runs it twice through the replica-serving entry
 * point ({@link ReadCommandVerbHandler#doRead}) -- {@code cursor_reads_enabled} off (iterator
 * oracle) then on (cursor path) -- and byte-compares the full {@code ReadResponse.serializer}
 * output. It also asserts {@link CursorReads#transcodeResponsesServed()} advanced, proving the
 * cursor path actually served the digest rather than silently falling back.
 *
 * The tests pin the BTI format, the priority format for cursor work.
 */
public class DigestCursorReadDifferentialTest extends CursorReadDifferentialTester
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

    private static SinglePartitionReadCommand digestCommand(ColumnFamilyStore cfs, long key)
    {
        return (SinglePartitionReadCommand)
               Util.cmd(cfs, key).build()
                   .copyAsDigestQuery(ReplicaUtils.full(FBUtilities.getBroadcastAddressAndPort()));
    }

    private static byte[] responseBytes(SinglePartitionReadCommand command, boolean trackRepairedData) throws Exception
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, trackRepairedData);
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
    }

    /** The cursor path must serve the digest query and produce a byte-identical digest response. */
    private void assertDigestMatchesIterator(Supplier<SinglePartitionReadCommand> command,
                                             boolean trackRepairedData) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        byte[] iteratorResponse = responseBytes(command.get(), trackRepairedData);

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long before = CursorReads.transcodeResponsesServed();
            byte[] cursorResponse = responseBytes(command.get(), trackRepairedData);
            long after = CursorReads.transcodeResponsesServed();

            assertResponseBytesEqual(iteratorResponse, cursorResponse);
            assertEquals("cursor path did not serve the digest query (silent fallback?)",
                         before + 1, after);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static void markRepaired(SSTableReader sstable) throws Exception
    {
        sstable.descriptor.getMetadataSerializer()
                          .mutateRepairMetadata(sstable.descriptor, FBUtilities.nowInSeconds(), null, false);
        sstable.reloadSSTableMetadata();
    }

    /** Two sstables -> a multi-leg cursor merge. */
    @Test
    public void multiLegDigest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        for (long ck = 20; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertDigestMatchesIterator(() -> digestCommand(cfs, 1L), false);
    }

    /** One sstable, no memtable: a single-leg digest (gap #2). */
    @Test
    public void singleLegDigest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        assertEquals(1, cfs.getLiveSSTables().size());

        assertDigestMatchesIterator(() -> digestCommand(cfs, 1L), false);
    }

    /** The memtable alone, no sstable: a memtable-only digest (gap #2). */
    @Test
    public void memtableOnlyDigest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        // no flush: served entirely from the memtable
        assertEquals(0, cfs.getLiveSSTables().size());

        assertDigestMatchesIterator(() -> digestCommand(cfs, 1L), false);
    }

    /** A multi-leg digest with repaired-status tracking active: the response is still a plain
     *  digest (a DigestResponse never carries a repaired-data digest), byte-identical to the
     *  iterator path. */
    @Test
    public void multiLegDigestWithTracking() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        markRepaired(List.copyOf(cfs.getLiveSSTables()).get(0));
        for (long ck = 20; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        // live memtable rows, always unrepaired
        for (long ck = 40; ck < 50; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);

        assertDigestMatchesIterator(() -> digestCommand(cfs, 1L), true);
    }

    /** A single repaired sstable with tracking: the read collapses to one repaired leg. */
    @Test
    public void singleLegDigestWithTracking() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            markRepaired(sstable);
        assertEquals(1, cfs.getLiveSSTables().size());

        assertDigestMatchesIterator(() -> digestCommand(cfs, 1L), true);
    }

    /** A partition delete in a newer sstable skips an older one, leaving multiple surviving legs
     *  (tombstone/skip case). The digest must still match the iterator path. */
    @Test
    public void partitionDeleteSkipDigest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1L, ck, ck, "v-" + ck);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ?", 1L);
        flush();
        for (long ck = 20; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 3000", 1L, ck, ck, "v-" + ck);
        flush();
        for (long ck = 30; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 4000", 1L, ck, ck, "v-" + ck);
        flush();
        assertEquals(4, cfs.getLiveSSTables().size());

        assertDigestMatchesIterator(() -> digestCommand(cfs, 1L), false);
    }
}
