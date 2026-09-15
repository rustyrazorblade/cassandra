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

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * M3.0 (CASSANDRA-20428, Phase 4 scaffolding): demonstrates and validates the
 * {@link ScanMetricsCapture} harness — the metric-parity capability M3.2's filter-aware production
 * is REQUIRED to satisfy (scan metrics are recorded BELOW the row filter today, so filter-dropped
 * rows still count; a merge that stops producing them must account for them identically).
 *
 * Two kinds of tests here:
 * <ul>
 *   <li><b>Parity today</b> (iterator vs cursor path on identical commands): passes now because
 *       both paths feed the SAME untouched {@code withMetricsRecording} stage with byte-identical
 *       streams — recorded as the baseline claim M3.1/M3.2 must preserve. Covers the plain
 *       tombstone-heavy shape, LIMIT (metrics see only counter-passed data — recording is below
 *       the filter but ABOVE nothing; the limit stops consumption below nothing either, both
 *       paths identical), a row filter (dropped rows still counted), and the warn / abort
 *       thresholds (exact-count parity via the warning text and abort message).</li>
 *   <li><b>Harness self-tests</b>: the capture must actually SEE what it claims to (known
 *       workload => expected exact counts), and the parity assertion must actually FAIL on
 *       diverging observations (negative control) — the same silent-vacuity discipline as the
 *       differential harness's corruption tests.</li>
 * </ul>
 */
public class ScanMetricsParityTest extends CursorReadDifferentialTester
{
    private static final int ROWS = 64;
    /** every 4th row gets a row tombstone in the second source */
    private static final int TOMBSTONED = ROWS / 4;
    private static final int LIVE = ROWS - TOMBSTONED;

    // ---------------------------------------------------------------- parity today (the baseline claim)

    @Test
    public void tombstoneHeavyReadParity() throws Throwable
    {
        ColumnFamilyStore cfs = loadTombstoneWorkload();
        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd =
            () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build();
        assertScanMetricsParity(cfs, cmd);
    }

    @Test
    public void limitReadParity() throws Throwable
    {
        ColumnFamilyStore cfs = loadTombstoneWorkload();
        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd =
            () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).withLimit(10).build();
        assertScanMetricsParity(cfs, cmd);
    }

    @Test
    public void filteredReadParity() throws Throwable
    {
        ColumnFamilyStore cfs = loadTombstoneWorkload();
        long now = FBUtilities.nowInSeconds();
        // the filter drops most rows ABOVE the metric recording: dropped rows must still be
        // counted as scanned — the exact behavior M3.2's pushdown must twin
        Supplier<SinglePartitionReadCommand> cmd =
            () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now)
                                                   .filterOn("v1", Operator.EQ, 10L).build();
        ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(cfs, cmd);
        // and the recording must reflect ALL live rows scanned, not the 1 row the filter kept
        assertEquals("filter-dropped rows must still be counted as live-scanned",
                     LIVE, iterator.totalRowsRead);
    }

    @Test
    public void warnThresholdParity() throws Throwable
    {
        ColumnFamilyStore cfs = loadTombstoneWorkload();
        long now = FBUtilities.nowInSeconds();
        int originalWarn = DatabaseDescriptor.getTombstoneWarnThreshold();
        DatabaseDescriptor.setTombstoneWarnThreshold(TOMBSTONED / 2);
        try
        {
            Supplier<SinglePartitionReadCommand> cmd =
                () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build();
            ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(cfs, cmd);
            assertEquals("warn threshold must have tripped", 1, iterator.tombstoneWarnings);
            assertFalse("warning text (with exact live/tombstone counts) must be captured",
                        iterator.clientWarnings.isEmpty());
        }
        finally
        {
            DatabaseDescriptor.setTombstoneWarnThreshold(originalWarn);
        }
    }

    @Test
    public void abortThresholdParity() throws Throwable
    {
        ColumnFamilyStore cfs = loadTombstoneWorkload();
        long now = FBUtilities.nowInSeconds();
        int originalFail = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneFailureThreshold(TOMBSTONED / 2);
        try
        {
            Supplier<SinglePartitionReadCommand> cmd =
                () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build();
            ScanMetricsCapture.Snapshot iterator = assertScanMetricsParity(cfs, cmd);
            assertTrue("read must have aborted with TombstoneOverwhelmingException", iterator.aborted());
            assertEquals("abort must have been metered", 1, iterator.tombstoneFailures);
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalFail);
        }
    }

    // ---------------------------------------------------------------- harness self-tests

    @Test
    public void captureSeesExactCounts() throws Throwable
    {
        ColumnFamilyStore cfs = loadTombstoneWorkload();
        long now = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand cmd =
            (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build();

        ScanMetricsCapture.Snapshot snap = ScanMetricsCapture.capture(cfs, () -> consume(cmd));
        // the workload's inventory is known exactly: LIVE live rows, TOMBSTONED row tombstones
        assertEquals("totalRowsRead must be the exact live-row count", LIVE, snap.totalRowsRead);
        assertEquals("one read must have been recorded on the live histogram", 1, snap.liveReadsRecorded);
        assertEquals("one read must have been recorded on the tombstone histogram", 1, snap.tombstoneReadsRecorded);
        // min/max are BUCKET-QUANTIZED (EstimatedHistogram boundaries around the recorded value —
        // e.g. a single update of 48 reports min=43: the bucket floor), so the self-check is
        // bucket containment plus tightness, not exact equality; quantization is deterministic,
        // which is what parity comparison needs
        assertTrue("recorded live value must fall in [min,max]: " + snap.describe(),
                   snap.liveMin <= LIVE && LIVE <= snap.liveMax);
        assertTrue("live bucket implausibly wide: " + snap.describe(),
                   snap.liveMax <= LIVE * 1.25);
        assertTrue("recorded tombstone value must fall in [min,max]: " + snap.describe(),
                   snap.tombstonesMin <= TOMBSTONED && TOMBSTONED <= snap.tombstonesMax);
        assertTrue("tombstone bucket implausibly wide: " + snap.describe(),
                   snap.tombstonesMax <= TOMBSTONED * 1.25);
        assertEquals(0, snap.tombstoneWarnings);
        assertEquals(0, snap.tombstoneFailures);
        assertFalse(snap.aborted());
    }

    @Test
    public void parityAssertionDetectsDivergence() throws Throwable
    {
        ColumnFamilyStore cfs = loadTombstoneWorkload();
        long now = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand full =
            (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build();
        // negative control: a sliced read scans different live/tombstone counts; the parity
        // assertion MUST fail on it, or every parity test above is vacuous
        SinglePartitionReadCommand sliced =
            (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now)
                                             .fromIncl(1L).toIncl(9L).build();

        ScanMetricsCapture.Snapshot a = ScanMetricsCapture.capture(cfs, () -> consume(full));
        ScanMetricsCapture.Snapshot b = ScanMetricsCapture.capture(cfs, () -> consume(sliced));
        try
        {
            ScanMetricsCapture.assertParity("negative control", a, b);
        }
        catch (AssertionError expected)
        {
            return;
        }
        fail("ScanMetricsCapture.assertParity accepted diverging scan metrics — the parity " +
             "capability is vacuous");
    }

    // ---------------------------------------------------------------- plumbing

    /**
     * The core parity check M3 increments will reuse: capture the scan metrics of the SAME command
     * through the iterator path and the cursor path (with the standard silent-fallback guards) and
     * assert every observation identical. Returns the iterator-path snapshot for extra assertions.
     */
    private ScanMetricsCapture.Snapshot assertScanMetricsParity(ColumnFamilyStore cfs,
                                                                Supplier<SinglePartitionReadCommand> cmd)
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        ScanMetricsCapture.Snapshot iterator = ScanMetricsCapture.capture(cfs, () -> consume(cmd.get()));

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = cmd.get();
            assertTrue("scenario is not supported by the cursor read gate; this parity run would " +
                       "silently compare iterator vs iterator",
                       CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));
            long servedBefore = CursorReads.sstableLegsServed();
            ScanMetricsCapture.Snapshot cursor = ScanMetricsCapture.capture(cfs, () -> consume(cmd.get()));
            assertTrue("cursor path did not actually serve any sstable leg (silent fallback?)",
                       CursorReads.sstableLegsServed() - servedBefore > 0);

            ScanMetricsCapture.assertParity("iterator vs cursor", iterator, cursor);
            return iterator;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** Full executeLocally consumption — metric recording happens partly at iteration, partly at close. */
    private void consume(SinglePartitionReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
        }
    }

    /**
     * Two overlapping sstables over one {@value #ROWS}-row partition: source 1 writes every row
     * (v1 = ck * 10), source 2 row-tombstones every 4th row and overwrites the rest. Metric
     * inventory at read time, per {@code MetricRecording}'s classification: {@value #LIVE} live
     * rows, {@value #TOMBSTONED} tombstones (PK-deletion rows with no dead cells). Long
     * gc_grace so nothing is purgeable.
     */
    private ColumnFamilyStore loadTombstoneWorkload() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck * 10, "r0-" + ck);
        flush();
        for (long ck = 0; ck < ROWS; ck++)
        {
            if (ck % 4 == 0)
                execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", ck);
            else
                execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck * 10);
        }
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        assertEquals(LIVE, execute("SELECT ck FROM %s WHERE pk = 0").size());
        return cfs;
    }
}
