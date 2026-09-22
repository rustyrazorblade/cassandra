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
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Differential scenarios for counter-table single-partition reads on the cursor read path
 * (CASSANDRA-20428 counter gap).  A counter cell does not reconcile to a single winner: every
 * live leg's counter context folds together ({@code CounterContext.merge}), a surviving deletion
 * always beats live shards (CASSANDRA-7346), and every sstable context has its marked-local shards
 * cleared at read (the iterator path's {@code DeserializationHelper.Flag.LOCAL} behavior).  Each
 * scenario asserts the cursor result is byte-identical to the iterator path AND that the cursor
 * path actually served the read (no silent fallback), through three surfaces:
 * <ul>
 *   <li>the {@code executeLocally} merge core (multi-leg and memtable+sstable folds), guarded by
 *       {@link CursorReads#cursorMergesServed()} / {@link CursorReads#sstableLegsCursorMerged()};</li>
 *   <li>the single-leg materializer, guarded by {@link CursorReads#sstableLegsServed()};</li>
 *   <li>the production replica response path
 *       ({@code SinglePartitionReadCommand.queryStorageToResponseBytes}): counters must DECLINE the
 *       single-winner transcode fast path and still return a byte-identical response through the
 *       merge, guarded by {@link CursorReads#transcodeResponsesServed()} not advancing.</li>
 * </ul>
 */
public class CounterCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void pinSelectedFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(formatName());
    }

    @After
    public void restoreSelectedFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /** The sstable format this class pins.  The BTI subclass overrides it. */
    protected String formatName()
    {
        return "big";
    }

    private Supplier<SinglePartitionReadCommand> fullPartition(ColumnFamilyStore cfs, long now, Object... key)
    {
        return () -> (SinglePartitionReadCommand) Util.cmd(cfs, key).withNowInSeconds(now).build();
    }

    /** Differential comparison plus the merge-effectiveness guards: the cursor merge ran and the
     *  expected sstable leg count joined it, across the harness's two cursor runs. */
    private void assertMergedCounterMatches(ColumnFamilyStore cfs,
                                            Supplier<SinglePartitionReadCommand> command,
                                            int expectedMergedLegsPerRun)
    {
        long mergesBefore = CursorReads.cursorMergesServed();
        long legsBefore = CursorReads.sstableLegsCursorMerged();

        assertCursorReadMatchesIterator(cfs, command);

        assertEquals("cursor-level merges served across the harness's two cursor runs",
                     2L, CursorReads.cursorMergesServed() - mergesBefore);
        assertEquals("sstable legs cursor-merged across the harness's two cursor runs",
                     2L * expectedMergedLegsPerRun, CursorReads.sstableLegsCursorMerged() - legsBefore);
    }

    // ---------------------------------------------------------------- response-path (transcode) guard

    private static byte[] fullResponseBytes(SinglePartitionReadCommand command) throws Exception
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
    }

    /** A counter read must DECLINE the single-winner transcode fast path (it cannot fold contexts)
     *  and still return a byte-identical response through the cursor merge. */
    private void assertCounterDeclinesTranscodeButServesIdentical(Supplier<SinglePartitionReadCommand> command) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        byte[] off = fullResponseBytes(command.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long before = CursorReads.transcodeResponsesServed();
            byte[] on = fullResponseBytes(command.get());
            long after = CursorReads.transcodeResponsesServed();

            assertResponseBytesEqual(off, on);
            assertEquals("counters must decline the single-winner transcode fast path", before, after);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    // ---------------------------------------------------------------- multi-leg folds

    /** The same (pk, ck) incremented in every leg: every leg's context folds into the merged sum. */
    @Test
    public void counterAcrossMultipleLegs() throws Throwable
    {
        int legs = 3;
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < legs; round++)
        {
            for (long ck = 0; ck < 12; ck++)
            {
                execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", (long) (round + 1), 1L, ck);
                execute("UPDATE %s SET c2 = c2 - ? WHERE pk = ? AND ck = ?", (long) (round * 2 + 1), 1L, ck);
            }
            flush();
        }
        assertEquals(legs, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCounterMatches(cfs, fullPartition(cfs, now, 1L), legs);
        assertCounterDeclinesTranscodeButServesIdentical(fullPartition(cfs, now, 1L));
    }

    /** Newer legs increment rows the older legs never touched, and vice versa: each leg contributes
     *  a different clustering set, some rows single-source, some multi-source. */
    @Test
    public void counterUpdatedInNewerLegOverOlder() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 5L, 1L, ck);
        flush();
        for (long ck = 5; ck < 15; ck++)
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 7L, 1L, ck);
        flush();
        for (long ck = 8; ck < 20; ck++)
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 11L, 1L, ck);
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCounterMatches(cfs, fullPartition(cfs, now, 1L), 3);
        assertCounterDeclinesTranscodeButServesIdentical(fullPartition(cfs, now, 1L));
    }

    /** A cell delete in a middle leg, live increments before and after: CASSANDRA-7346 tombstone
     *  supremacy folds through the merge (a surviving deletion beats all live shards). */
    @Test
    public void counterDeleteThenReAddAcrossLegs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 8; ck++)
        {
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 3L, 1L, ck);
            execute("UPDATE %s SET c2 = c2 + ? WHERE pk = ? AND ck = ?", 4L, 1L, ck);
        }
        flush();
        // delete c1 on some rows (cell tombstone) and delete whole rows on others
        execute("DELETE c1 FROM %s WHERE pk = ? AND ck = 2", 1L);
        execute("DELETE c1 FROM %s WHERE pk = ? AND ck = 4", 1L);
        execute("DELETE FROM %s WHERE pk = ? AND ck = 6", 1L);
        flush();
        // increments after the deletes
        for (long ck = 0; ck < 8; ck++)
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 9L, 1L, ck);
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCounterMatches(cfs, fullPartition(cfs, now, 1L), 3);
        assertCounterDeclinesTranscodeButServesIdentical(fullPartition(cfs, now, 1L));
    }

    // ---------------------------------------------------------------- memtable + sstable

    /** An sstable leg folded with a memtable leg: the memtable context (global shards, no clear)
     *  must fold with the sstable context (marked-local shards cleared) exactly as the iterator's
     *  object merge does. */
    @Test
    public void counterMemtablePlusSstable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 5L, 1L, ck);
        flush();
        // stays in the memtable, no flush
        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 6L, 1L, ck);
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        // one sstable leg joins the merge; the memtable leg folds too but is not an sstable leg
        assertMergedCounterMatches(cfs, fullPartition(cfs, now, 1L), 1);
        assertCounterDeclinesTranscodeButServesIdentical(fullPartition(cfs, now, 1L));
    }

    /** Memtable-only counter read (zero sstables): a single memtable leg, no clear applied. */
    @Test
    public void counterMemtableOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
        {
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 5L, 1L, ck);
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", 3L, 1L, ck);
        }
        assertTrue("expected a memtable-only read", cfs.getLiveSSTables().isEmpty());

        long now = FBUtilities.nowInSeconds();
        // no sstable legs to guard on; the response-path decline proves byte-identical merge service
        assertCounterDeclinesTranscodeButServesIdentical(fullPartition(cfs, now, 1L));
    }

    // ---------------------------------------------------------------- single leg

    /** A single sstable leg routes through the cursor single-leg materializer, which must apply the
     *  marked-local clear to every live counter cell value. */
    @Test
    public void counterSingleLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 12; ck++)
        {
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", (long) (ck + 1), 1L, ck);
            execute("UPDATE %s SET c2 = c2 + ? WHERE pk = ? AND ck = ?", 100L, 1L, ck);
        }
        flush();
        assertEquals(1, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now, 1L));
        assertCounterDeclinesTranscodeButServesIdentical(fullPartition(cfs, now, 1L));
    }

    // ---------------------------------------------------------------- slice + names

    /** A single-slice counter read across legs: the merge folds contexts within the slice. */
    @Test
    public void counterSliceRead() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long ck = 0; ck < 20; ck++)
                execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", (long) (round + 1), 1L, ck);
            flush();
        }
        assertEquals(3, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCounterMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(4L).toIncl(12L).build(), 3);
        assertCounterDeclinesTranscodeButServesIdentical(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).fromIncl(4L).toIncl(12L).build());
    }

    /** A names (point-slice) counter read across legs: the covering-range merge folds contexts and
     *  the slicer re-filters to the requested clusterings. */
    @Test
    public void counterNamesRead() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long ck = 0; ck < 20; ck++)
                execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", (long) (round * 3 + 1), 1L, ck);
            flush();
        }
        assertEquals(3, cfs.getLiveSSTables().size());

        long now = FBUtilities.nowInSeconds();
        assertMergedCounterMatches(cfs, () -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).includeRow(3L).includeRow(7L).includeRow(15L).build(), 3);
        assertCounterDeclinesTranscodeButServesIdentical(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).withNowInSeconds(now).includeRow(3L).includeRow(7L).includeRow(15L).build());
    }
}
