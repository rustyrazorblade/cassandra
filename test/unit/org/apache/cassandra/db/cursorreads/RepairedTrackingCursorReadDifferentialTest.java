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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Differential coverage for repaired-status tracking on the cursor read path (CASSANDRA-20428,
 * gap #1). A read that tracks repaired status computes a repaired-data digest by splitting the
 * candidate sstables into a repaired set and an unrepaired set, merging the repaired set through a
 * digest generator. Before this fix the cursor path declined every tracking read and fell back to
 * the legacy iterator path.
 *
 * Each scenario runs the SAME command twice — {@code cursor_reads_enabled} off (iterator oracle)
 * then on (cursor path) — with repaired-status tracking active, and compares:
 *   - the full {@code ReadResponse.serializer} output of the replica-serving transcode path
 *     ({@code ReadCommandVerbHandler.doRead(command, true)}), which carries the repaired-data
 *     digest and its conclusive flag, byte-for-byte; and
 *   - the repaired-data digest and conclusive flag produced by the {@code executeLocally} cursor
 *     merge path under a tracking controller.
 *
 * The tests pin the BTI format, the priority format for cursor work.
 */
public class RepairedTrackingCursorReadDifferentialTest extends CursorReadDifferentialTester
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

    private static byte[] trackingResponseBytes(SinglePartitionReadCommand command) throws Exception
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, true);
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
    }

    private static void drain(UnfilteredPartitionIterator partitions)
    {
        while (partitions.hasNext())
        {
            try (var partition = partitions.next())
            {
                while (partition.hasNext())
                    partition.next();
            }
        }
    }

    /** Consumes the executeLocally cursor merge under a tracking controller and returns the
     *  repaired-data digest computed over the repaired subset. */
    private static ByteBuffer trackingDigest(SinglePartitionReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController(true);
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            drain(partitions);
            return controller.getRepairedDataDigest();
        }
    }

    private static boolean trackingConclusive(SinglePartitionReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController(true);
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            drain(partitions);
            return controller.isRepairedDataDigestConclusive();
        }
    }

    /**
     * The cursor path must serve a tracking read and produce the same repaired-data digest as the
     * iterator path. Verified at two levels: the full replica-serving response bytes (transcode
     * path, which must actually engage), and the executeLocally cursor-merge digest.
     */
    private void assertTrackingDigestMatchesIterator(ColumnFamilyStore cfs,
                                                     Supplier<SinglePartitionReadCommand> command) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        byte[] iteratorResponse = trackingResponseBytes(command.get());
        ByteBuffer iteratorDigest = trackingDigest(command.get());
        boolean iteratorConclusive = trackingConclusive(command.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long transcodeBefore = CursorReads.transcodeResponsesServed();
            byte[] cursorResponse = trackingResponseBytes(command.get());
            long transcodeAfter = CursorReads.transcodeResponsesServed();

            long legsBefore = CursorReads.sstableLegsServed();
            ByteBuffer cursorDigest = trackingDigest(command.get());
            boolean cursorConclusive = trackingConclusive(command.get());
            long legsAfter = CursorReads.sstableLegsServed();

            assertResponseBytesEqual(iteratorResponse, cursorResponse);
            assertEquals("repaired-data digest diverged between cursor and iterator paths",
                         ByteBufferUtil.bytesToHex(iteratorDigest), ByteBufferUtil.bytesToHex(cursorDigest));
            assertEquals("repaired-data conclusive flag diverged between cursor and iterator paths",
                         iteratorConclusive, cursorConclusive);
            assertTrue("transcode path did not serve the tracking read (silent fallback?)",
                       transcodeAfter > transcodeBefore);
            assertTrue("executeLocally cursor merge did not serve any sstable leg (silent fallback?)",
                       legsAfter > legsBefore);
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

    /**
     * A single repaired sstable, no memtable and no unrepaired data: the read collapses to ONE
     * repaired leg (CASSANDRA-20428 gap #2). The repaired-data digest must cover that lone leg and
     * match the iterator path on both the transcode response bytes and the executeLocally digest.
     */
    @Test
    public void singleRepairedSSTable() throws Throwable
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

        assertTrackingDigestMatchesIterator(cfs, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build());
    }

    /**
     * A single UNREPAIRED sstable, no memtable: the read collapses to one leg and the repaired set
     * is empty, so the repaired-data digest is the empty-set digest. Both paths must agree on that
     * digest and on the conclusive flag.
     */
    @Test
    public void singleUnrepairedSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        assertEquals(1, cfs.getLiveSSTables().size());

        assertTrackingDigestMatchesIterator(cfs, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build());
    }

    @Test
    public void repairedPlusUnrepairedMix() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        // mark the first sstable repaired
        List<SSTableReader> live = List.copyOf(cfs.getLiveSSTables());
        markRepaired(live.get(0));

        for (long ck = 20; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTrackingDigestMatchesIterator(cfs, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build());
    }

    @Test
    public void allRepaired() throws Throwable
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
        for (SSTableReader sstable : cfs.getLiveSSTables())
            markRepaired(sstable);
        assertEquals(2, cfs.getLiveSSTables().size());

        assertTrackingDigestMatchesIterator(cfs, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build());
    }

    @Test
    public void repairedUnrepairedPlusMemtable() throws Throwable
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

        assertTrackingDigestMatchesIterator(cfs, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build());
    }

    /**
     * A partition-level delete skips an OLDER repaired sstable whose max timestamp is below the
     * delete. That skip marks the repaired-data digest inconclusive. Timestamps are pinned so the
     * skip leaves two or more surviving legs above the delete (a repaired one and the delete
     * itself), keeping the read on the transcode path rather than collapsing it to a single leg.
     * Both paths must reach the same digest and the same conclusive verdict (inconclusive).
     */
    @Test
    public void partitionDeleteSkipMarksInconclusive() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Oldest sstable, repaired. Its low timestamp puts it below the delete, so it is skipped.
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1L, ck, ck, "v-" + ck);
        flush();
        markRepaired(List.copyOf(cfs.getLiveSSTables()).get(0));

        // Partition delete above the oldest sstable's timestamp, below the newer data.
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ?", 1L);
        flush();

        // A newer sstable that survives the delete, marked repaired so a repaired leg feeds the digest.
        List<SSTableReader> beforeRepairedSurvivor = List.copyOf(cfs.getLiveSSTables());
        for (long ck = 20; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 3000", 1L, ck, ck, "v-" + ck);
        flush();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (!beforeRepairedSurvivor.contains(sstable))
                markRepaired(sstable);

        // Another newer, unrepaired sstable that also survives the delete.
        for (long ck = 30; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 4000", 1L, ck, ck, "v-" + ck);
        flush();
        assertEquals(4, cfs.getLiveSSTables().size());

        assertTrackingDigestMatchesIterator(cfs, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build());
    }

    /**
     * A static-only partition read through a NAMES filter (empty clustering set) that fetches a
     * regular column, WITH repaired-status tracking on. The partition holds only static data, so a
     * raw memtable rowIterator reports no regular columns, while a cursor read of the same data once
     * flushed reports the queried column set. Two replicas serving the same static-only data, one
     * from the memtable and one from a flushed sstable, must produce byte-identical replica-serving
     * responses; otherwise read-repair flags a spurious digest mismatch between them. The tracking
     * guard used to suppress the memtable-side column restore, so the memtable response diverged
     * from the flushed response. This case is RED before the guard is dropped and GREEN after.
     */
    @Test
    public void staticOnlyNamesTrackedMatchesBeforeAndAfterFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 1L, 1L);

        long now = FBUtilities.nowInSeconds();
        ColumnFilter columns = staticAndAbsentRegularSelection(cfs);
        assertTrackedResponseMatchesBeforeAndAfterFlush(cfs, () -> staticOnlyNamesCommand(cfs, now, columns));
    }

    /** A column selection that fetches the absent regular column {@code v} alongside the static
     *  {@code s1}, so the queried regular set is non-empty even though the partition holds no rows. */
    private static ColumnFilter staticAndAbsentRegularSelection(ColumnFamilyStore cfs)
    {
        RegularAndStaticColumns queried =
            RegularAndStaticColumns.builder()
                                   .add(cfs.metadata().getColumn(ColumnIdentifier.getInterned("v", false)))
                                   .add(cfs.metadata().getColumn(ColumnIdentifier.getInterned("s1", false)))
                                   .build();
        return ColumnFilter.selection(cfs.metadata(), queried, false);
    }

    /** A NAMES read with an empty clustering set (fetches only the static row) over partition pk=1. */
    private static SinglePartitionReadCommand staticOnlyNamesCommand(ColumnFamilyStore cfs, long nowInSec, ColumnFilter columns)
    {
        DecoratedKey key = ((SinglePartitionReadCommand) Util.cmd(cfs, 1L).build()).partitionKey();
        NavigableSet<Clustering<?>> noClusterings = new TreeSet<>(cfs.metadata().comparator);
        return SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, key, columns,
                                                 new ClusteringIndexNamesFilter(noClusterings, false));
    }

    /**
     * A tracking read of the same data must produce the same read-repair digest from the memtable
     * (before flush) and from a flushed sstable (after flush), both on the cursor path. The digest is
     * what replicas compare under read-repair; it hashes the reported columns and rows but not the
     * EncodingStats header, which legitimately differs between a memtable read and an sstable read.
     * The flushed read's cursor engagement is guarded by the served-leg counter; the memtable read is
     * guarded by the cursor read gate (a memtable has no sstable leg to count).
     */
    private void assertTrackedResponseMatchesBeforeAndAfterFlush(ColumnFamilyStore cfs,
                                                                 Supplier<SinglePartitionReadCommand> command) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("memtable-only names read must pass the cursor read gate",
                       CursorReads.isReadSupported(command.get(), cfs, liveSSTablesFor(cfs, command.get())));
            String memtableDigest = trackingDigestHex(command.get());

            flush();
            assertEquals(1, cfs.getLiveSSTables().size());

            long servedBefore = CursorReads.sstableLegsServed();
            String flushedDigest = trackingDigestHex(command.get());
            assertTrue("cursor path did not serve the flushed sstable leg (silent fallback?)",
                       CursorReads.sstableLegsServed() - servedBefore > 0);

            assertEquals("read-repair digest diverged between the memtable and the flushed sstable read",
                         memtableDigest, flushedDigest);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** The read-repair digest a replica reports for {@code command} under repaired-status tracking.
     *  Computed over the tracking data response, exactly as read-repair recomputes it to compare
     *  replicas (ReadResponse.digest hashes the reported columns and rows, not the stats header). */
    private static String trackingDigestHex(SinglePartitionReadCommand command)
    {
        command.setDigestVersion(MessagingService.current_version);
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, true);
        return ByteBufferUtil.bytesToHex(response.digest(command));
    }
}
