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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.After;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Differential test harness for the Phase 1 cursor read path, modeled on
 * {@code DifferentialCompactionTester} but comparing READ results instead of compaction output.
 *
 * For each scenario the SAME {@link SinglePartitionReadCommand} shape (rebuilt fresh per execution,
 * with a pinned nowInSeconds so TTL/liveness evaluation cannot flip between runs) is executed through:
 *   1. the iterator path (cursor_reads_enabled = false), and
 *   2. the cursor path (cursor_reads_enabled = true),
 * and the results are compared at two levels:
 *   - LOGICAL: a canonical record stream of every partition/static row/row/marker/cell, including
 *     liveness, deletion, complex deletion, cell path, ttl/localDeletionTime and value bytes;
 *   - BYTES: the intra-node {@code ReadResponse} serialization (the exact
 *     {@code ReadResponse.LocalDataResponse.build} encoding) must be byte-identical.
 *
 * Silent-fallback guard (the trap this harness style exists to catch): before the cursor run, the
 * scenario asserts {@link CursorReads#isReadSupported} holds for the command, and after it, that
 * {@link CursorReads#sstableLegsServed()} actually advanced — a scenario whose cursor run silently
 * fell back to the iterator path fails loudly rather than passing vacuously.
 */
public abstract class CursorReadDifferentialTester extends CQLTester
{
    @After
    public void resetCursorReadState()
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS = false;
        CursorReads.TEST_CORRUPT_MERGE_DECISIONS = false;
        CursorReads.TEST_SKEW_MEMTABLE_LEG_TIMESTAMPS = false;
        CursorReads.TEST_FORCE_MEMTABLE_ROW_REUSE = false;
        CursorReads.TEST_SKEW_DROPPED_ROW_ACCOUNTING = false;
    }

    protected static List<SSTableReader> liveSSTablesFor(ColumnFamilyStore cfs, SinglePartitionReadCommand command)
    {
        return cfs.select(View.select(SSTableSet.LIVE, command.partitionKey())).sstables;
    }

    /** Full differential: gate must be open, cursor must actually serve sstable legs, results identical. */
    protected void assertCursorReadMatchesIterator(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    {
        assertCursorReadMatchesIterator(cfs, command, true);
    }

    /**
     * @param expectCursorServedLegs pass false only for scenarios where no candidate sstable
     *        contains the queried partition (the cursor is then never opened; the leg is counted in
     *        {@link CursorReads#sstableLegsWithoutPartition()} instead)
     */
    protected void assertCursorReadMatchesIterator(ColumnFamilyStore cfs,
                                                   Supplier<SinglePartitionReadCommand> command,
                                                   boolean expectCursorServedLegs)
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        List<String> iteratorRecords = canonicalRecords(command.get());
        byte[] iteratorBytes = responseBytes(command.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = command.get();
            assertTrue("scenario is not supported by the cursor read gate; this harness run would " +
                       "silently compare iterator vs iterator. If unsupported-ness is intended, use " +
                       "assertFallsBackUnchanged instead.",
                       CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));

            long servedBefore = CursorReads.sstableLegsServed();
            long missBefore = CursorReads.sstableLegsWithoutPartition();

            List<String> cursorRecords = canonicalRecords(command.get());
            byte[] cursorBytes = responseBytes(command.get());

            long servedDelta = CursorReads.sstableLegsServed() - servedBefore;
            long missDelta = CursorReads.sstableLegsWithoutPartition() - missBefore;
            if (expectCursorServedLegs)
                assertTrue("cursor path did not actually serve any sstable leg (silent fallback?); " +
                           "served=" + servedDelta + " missed=" + missDelta,
                           servedDelta > 0);
            else
                assertTrue("cursor path was not consulted at all (silent fallback?); " +
                           "served=" + servedDelta + " missed=" + missDelta,
                           servedDelta + missDelta > 0);

            compareRecords(iteratorRecords, cursorRecords);
            assertResponseBytesEqual(iteratorBytes, cursorBytes);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /**
     * For shapes OUTSIDE the Phase 1 gate: with the flag ON, the gate must reject the command, the
     * cursor must never run, and the results must be identical to the flag-OFF run (i.e. the
     * fallback is a true no-op).
     */
    protected void assertFallsBackUnchanged(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        List<String> iteratorRecords = canonicalRecords(command.get());
        byte[] iteratorBytes = responseBytes(command.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = command.get();
            assertFalse("scenario unexpectedly PASSES the cursor read gate; use " +
                        "assertCursorReadMatchesIterator for supported shapes",
                        CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));

            long servedBefore = CursorReads.sstableLegsServed();
            long missBefore = CursorReads.sstableLegsWithoutPartition();
            List<String> fallbackRecords = canonicalRecords(command.get());
            byte[] fallbackBytes = responseBytes(command.get());
            assertEquals("cursor path ran for a gated-out command", servedBefore, CursorReads.sstableLegsServed());
            assertEquals("cursor path was consulted for a gated-out command", missBefore, CursorReads.sstableLegsWithoutPartition());

            compareRecords(iteratorRecords, fallbackRecords);
            assertResponseBytesEqual(iteratorBytes, fallbackBytes);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    // ---------------------------------------------------------------- paged differential (M3.0)

    /** One full paging sequence: per-page canonical records plus the serialized paging state
     *  observed AFTER each page (the state a client would resume from). */
    protected static final class PagedRun
    {
        final List<List<String>> pageRecords = new ArrayList<>();
        final List<String> pagingStates = new ArrayList<>();
    }

    /**
     * Paged differential (M3.0, for the late-materialization scenarios): drives the ENTIRE paging
     * sequence through the real {@link org.apache.cassandra.service.pager.SinglePartitionPager}
     * machinery ({@code fetchPageUnfiltered}, which routes each page's forPaging command through
     * {@code executeLocally} exactly like internal paging does) under both paths, and asserts
     * per-page canonical records AND per-page serialized {@link PagingState} equality — so a
     * production change that bounds row production can never silently shift a page boundary or a
     * resume point. Same silent-fallback guards as {@link #assertCursorReadMatchesIterator}: the
     * gate must accept the base command, the cursor sequence must actually serve sstable legs, and
     * the iterator sequence must never consult the cursor path.
     */
    protected void assertPagedReadMatchesIterator(ColumnFamilyStore cfs,
                                                  Supplier<SinglePartitionReadCommand> command,
                                                  int pageSize,
                                                  int expectedPages)
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        long servedBeforeIterator = CursorReads.sstableLegsServed();
        long missBeforeIterator = CursorReads.sstableLegsWithoutPartition();
        PagedRun iteratorRun = runPagedSequence(cfs, command.get(), pageSize);
        assertEquals("iterator-path paging unexpectedly ran the cursor path",
                     servedBeforeIterator, CursorReads.sstableLegsServed());
        assertEquals("iterator-path paging unexpectedly consulted the cursor path",
                     missBeforeIterator, CursorReads.sstableLegsWithoutPartition());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = command.get();
            assertTrue("paged scenario is not supported by the cursor read gate; this run would " +
                       "silently compare iterator vs iterator",
                       CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));

            long servedBefore = CursorReads.sstableLegsServed();
            PagedRun cursorRun = runPagedSequence(cfs, command.get(), pageSize);
            assertTrue("cursor path did not serve any sstable leg during the paged sequence (silent fallback?)",
                       CursorReads.sstableLegsServed() - servedBefore > 0);

            assertEquals("page count diverged between paths", iteratorRun.pageRecords.size(), cursorRun.pageRecords.size());
            assertEquals("paged workload did not produce the expected number of pages (workload shape drifted)",
                         expectedPages, iteratorRun.pageRecords.size());
            for (int page = 0; page < iteratorRun.pageRecords.size(); page++)
            {
                compareRecords(iteratorRun.pageRecords.get(page), cursorRun.pageRecords.get(page));
                assertEquals("paging state after page " + page + " diverged between paths",
                             iteratorRun.pagingStates.get(page), cursorRun.pagingStates.get(page));
            }
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private PagedRun runPagedSequence(ColumnFamilyStore cfs, SinglePartitionReadCommand command, int pageSize)
    {
        PagedRun run = new PagedRun();
        SinglePartitionPager pager = (SinglePartitionPager) command.getPager(null, ProtocolVersion.CURRENT);
        int guard = 0;
        while (!pager.isExhausted())
        {
            assertTrue("paging sequence did not terminate", guard++ < 10_000);
            try (ReadExecutionController controller = pager.executionController();
                 UnfilteredPartitionIterator page = pager.fetchPageUnfiltered(cfs.metadata(), pageSize, controller))
            {
                run.pageRecords.add(canonicalRecords(page));
            }
            PagingState state = pager.state();
            run.pagingStates.add(state == null ? "null" : ByteBufferUtil.bytesToHex(state.serialize(ProtocolVersion.CURRENT)));
        }
        return run;
    }

    // ---------------------------------------------------------------- capture

    protected static List<String> canonicalRecords(SinglePartitionReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            return canonicalRecords(partitions);
        }
    }

    /** Canonical record stream of an already-open partition iterator (consumes it). Used directly by
     *  scenarios whose iterator does not come from a plain {@code executeLocally} (paged reads, the
     *  response round-trip harness). */
    protected static List<String> canonicalRecords(UnfilteredPartitionIterator partitions)
    {
        List<String> out = new ArrayList<>();
        while (partitions.hasNext())
        {
            try (UnfilteredRowIterator partition = partitions.next())
            {
                TableMetadata metadata = partition.metadata();
                out.add("PARTITION key=" + ByteBufferUtil.bytesToHex(partition.partitionKey().getKey())
                        + " deletion=" + dt(partition.partitionLevelDeletion().markedForDeleteAt(),
                                            partition.partitionLevelDeletion().localDeletionTime())
                        + " columns=" + partition.columns());
                if (!partition.staticRow().isEmpty())
                    rowRecords("STATIC", partition.staticRow(), out);
                while (partition.hasNext())
                {
                    Unfiltered unfiltered = partition.next();
                    if (unfiltered.isRow())
                        rowRecords("ROW", (Row) unfiltered, out);
                    else
                        markerRecord((RangeTombstoneMarker) unfiltered, out);
                }
            }
        }
        return out;
    }

    protected static byte[] responseBytes(SinglePartitionReadCommand command)
    {
        // the exact encoding ReadResponse.LocalDataResponse.build produces for a replica-serving read
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller);
             DataOutputBuffer buffer = new DataOutputBuffer())
        {
            UnfilteredPartitionIterators.serializerForIntraNode()
                                        .serialize(partitions, command.columnFilter(), buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
        catch (Exception e)
        {
            throw new AssertionError("failed to serialize read response", e);
        }
    }

    private static void rowRecords(String label, Row row, List<String> out)
    {
        out.add(label + " clustering=" + clusteringString(row.clustering())
                + " liveness=" + row.primaryKeyLivenessInfo().timestamp()
                + "," + row.primaryKeyLivenessInfo().ttl()
                + "," + row.primaryKeyLivenessInfo().localExpirationTime()
                + " deletion=" + dt(row.deletion().time().markedForDeleteAt(), row.deletion().time().localDeletionTime())
                + (row.deletion().isShadowable() ? ",shadowable" : ""));
        for (ColumnData cd : row)
        {
            if (cd.column().isComplex())
            {
                ComplexColumnData complex = (ComplexColumnData) cd;
                out.add("  CPLX " + cd.column().name
                        + " del=" + dt(complex.complexDeletion().markedForDeleteAt(),
                                       complex.complexDeletion().localDeletionTime())
                        + " n=" + complex.cellsCount());
                for (Cell<?> cell : complex)
                    out.add(cellRecord(cell));
            }
            else
            {
                out.add(cellRecord((Cell<?>) cd));
            }
        }
    }

    private static String cellRecord(Cell<?> cell)
    {
        String path = cell.path() == null ? "-" : ByteBufferUtil.bytesToHex(cell.path().get(0));
        return "  CELL " + cell.column().name
               + " path=" + path
               + " ts=" + cell.timestamp()
               + " ttl=" + cell.ttl()
               + " ldt=" + cell.localDeletionTime()
               + " v=" + ByteBufferUtil.bytesToHex(cell.buffer());
    }

    private static void markerRecord(RangeTombstoneMarker marker, List<String> out)
    {
        StringBuilder sb = new StringBuilder("MARKER clustering=").append(clusteringString(marker.clustering()));
        if (marker.isClose(false))
            sb.append(" close=").append(dt(marker.closeDeletionTime(false).markedForDeleteAt(),
                                           marker.closeDeletionTime(false).localDeletionTime()))
              .append(marker.closeIsInclusive(false) ? ",incl" : ",excl");
        if (marker.isOpen(false))
            sb.append(" open=").append(dt(marker.openDeletionTime(false).markedForDeleteAt(),
                                          marker.openDeletionTime(false).localDeletionTime()))
              .append(marker.openIsInclusive(false) ? ",incl" : ",excl");
        out.add(sb.toString());
    }

    private static String clusteringString(ClusteringPrefix<?> prefix)
    {
        StringBuilder sb = new StringBuilder(prefix.kind().toString());
        for (int i = 0; i < prefix.size(); i++)
        {
            ByteBuffer value = prefix.bufferAt(i);
            sb.append(':').append(value == null ? "null" : ByteBufferUtil.bytesToHex(value));
        }
        return sb.toString();
    }

    private static String dt(long markedForDeleteAt, long localDeletionTime)
    {
        return markedForDeleteAt + "/" + localDeletionTime;
    }

    // ---------------------------------------------------------------- comparison

    protected static void compareRecords(List<String> iterator, List<String> cursor)
    {
        int max = Math.max(iterator.size(), cursor.size());
        for (int i = 0; i < max; i++)
        {
            String expected = i < iterator.size() ? iterator.get(i) : "<missing>";
            String actual = i < cursor.size() ? cursor.get(i) : "<missing>";
            if (!expected.equals(actual))
                fail(String.format("LOGICAL divergence at record %d (iterator vs cursor):%n  iterator: %s%n  cursor:   %s%n  context:%s",
                                   i, expected, actual, context(iterator, cursor, i)));
        }
    }

    private static String context(List<String> iterator, List<String> cursor, int index)
    {
        StringBuilder sb = new StringBuilder();
        for (int j = Math.max(0, index - 3); j < Math.min(iterator.size(), index + 4); j++)
            sb.append(String.format("%n    it[%d]=%s", j, iterator.get(j)));
        for (int j = Math.max(0, index - 3); j < Math.min(cursor.size(), index + 4); j++)
            sb.append(String.format("%n    cu[%d]=%s", j, cursor.get(j)));
        return sb.toString();
    }

    protected static void assertResponseBytesEqual(byte[] iterator, byte[] cursor)
    {
        int mismatch = java.util.Arrays.mismatch(iterator, cursor);
        if (mismatch >= 0)
            fail(String.format("BYTE divergence in intra-node ReadResponse serialization: lengths %d vs %d, " +
                               "first divergence at offset %d%n  iterator: %s%n  cursor:   %s",
                               iterator.length, cursor.length, mismatch,
                               hexContext(iterator, mismatch), hexContext(cursor, mismatch)));
    }

    private static String hexContext(byte[] bytes, int offset)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = Math.max(0, offset - 8); i < Math.min(bytes.length, offset + 24); i++)
        {
            if (i == offset)
                sb.append('[');
            sb.append(String.format("%02x", bytes[i]));
            if (i == offset)
                sb.append(']');
            sb.append(' ');
        }
        if (offset + 24 < bytes.length)
            sb.append("...");
        return sb.toString();
    }
}
