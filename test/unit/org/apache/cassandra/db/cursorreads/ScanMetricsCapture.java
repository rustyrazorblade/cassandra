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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.service.ClientWarn;

import static org.junit.Assert.fail;

/**
 * M3.0 (CASSANDRA-20428, Phase 4 scaffolding): capture-and-compare harness for the SCAN metrics
 * recorded by {@code ReadCommand.withMetricsRecording} — the piece of the object-path stack that
 * sits BELOW the row filter (ReadCommand.java, MetricRecording), so filter-dropped rows still feed
 * it today. M3.2's filter-aware production must keep every one of these observations identical
 * (the design's "scan-metrics parity" requirement, blocking-severity); this class is the test
 * capability that lets differential scenarios assert METRIC equality between two code paths, not
 * just byte equality.
 *
 * What {@code withMetricsRecording} actually records (verified against current source), and what
 * this harness therefore captures per read run:
 * <ul>
 *   <li>{@code metric.totalRowsRead.inc(liveRows)} — captured as an exact Counter delta;</li>
 *   <li>{@code metric.liveScannedHistogram.update(liveRows)} and
 *       {@code metric.tombstoneScannedHistogram.update(tombstones)} at read close — captured as
 *       the update-count delta plus the min/max recorded value since capture start. The table-level
 *       ({@code .cf}) histograms are {@link ClearableHistogram}s and are cleared at capture start,
 *       making min/max the BUCKET-QUANTIZED values recorded by THIS capture (EstimatedHistogram
 *       bucket boundaries around the recorded value — e.g. a single update of 48 reports min=43,
 *       the bucket floor; verified empirically by {@code ScanMetricsParityTest}). Count, min and
 *       max of an {@code EstimatedHistogram} snapshot are decay-independent, unlike the weighted
 *       bucket values, so equality comparison between two captures of identical recordings is
 *       deterministic — the property parity assertions need. (Tombstone counts here
 *       are MetricRecording's three-way classification: dead cells, RT markers, and PK-deletion-
 *       only rows.)</li>
 *   <li>{@code metric.tombstoneWarnings.inc()} / {@code metric.tombstoneFailures.inc()} — exact
 *       Counter deltas (the warn/fail threshold guardrails);</li>
 *   <li>the {@link ClientWarn} tombstone warning text — which embeds the EXACT live-row and
 *       tombstone counts ("Read %d live rows and %d tombstone cells..."), giving exact-value
 *       parity on warning-shaped scenarios where histograms only give bucketized values;</li>
 *   <li>{@link TombstoneOverwhelmingException} aborts — caught and recorded (message embeds the
 *       exact tombstone count at abort), so abort-at-the-same-scan-count is directly comparable.</li>
 * </ul>
 *
 * NOT captured (deliberately, documented so M3.2 doesn't assume otherwise):
 * {@code topReadPartitionRowCount}/{@code topReadPartitionTombstoneCount} samplers (no-ops unless
 * sampling is explicitly enabled), read-latency metrics (timing, not scan accounting), and the
 * keyspace/global histogram tiers (shared across tables in the test JVM; the per-table {@code .cf}
 * tier gives isolation and is updated by the same {@code TableHistogram.update} call).
 *
 * Usage: {@code Snapshot a = ScanMetricsCapture.capture(cfs, () -> runRead(...))} around each
 * path's run, then {@link #assertParity}. {@link ScanMetricsParityTest} both demonstrates the
 * harness on today's paths and proves (negative control) that it actually detects divergence.
 */
public final class ScanMetricsCapture
{
    private ScanMetricsCapture()
    {
    }

    @FunctionalInterface
    public interface ThrowingRunnable
    {
        void run() throws Throwable;
    }

    /** Deterministic per-capture scan-metric observations; all fields compared by {@link #assertParity}. */
    public static final class Snapshot
    {
        long totalRowsRead;            // exact sum of liveRows over the captured reads
        long tombstoneWarnings;        // exact warn-threshold trips
        long tombstoneFailures;        // exact fail-threshold trips
        long liveReadsRecorded;        // liveScannedHistogram update count
        long tombstoneReadsRecorded;   // tombstoneScannedHistogram update count
        long liveMin, liveMax;         // bucket-quantized min/max liveRows recorded this capture
        long tombstonesMin, tombstonesMax; // bucket-quantized min/max tombstones recorded this capture
        List<String> clientWarnings = new ArrayList<>(); // exact warning text (embeds exact counts)
        String abortMessage;           // TombstoneOverwhelmingException message, null if no abort

        public boolean aborted()
        {
            return abortMessage != null;
        }

        public String describe()
        {
            return String.format("totalRowsRead=%d liveReads=%d live[min=%d max=%d] " +
                                 "tombstoneReads=%d tombstones[min=%d max=%d] warnings=%d failures=%d " +
                                 "clientWarnings=%s abort=%s",
                                 totalRowsRead, liveReadsRecorded, liveMin, liveMax,
                                 tombstoneReadsRecorded, tombstonesMin, tombstonesMax,
                                 tombstoneWarnings, tombstoneFailures,
                                 clientWarnings, abortMessage);
        }
    }

    /**
     * Runs {@code read} and captures every scan-metric observation it produced on {@code cfs}.
     * A {@link TombstoneOverwhelmingException} thrown by the read is recorded in the snapshot
     * (not rethrown) so abort behavior is comparable like any other observation; any other
     * throwable propagates as a test error.
     */
    public static Snapshot capture(ColumnFamilyStore cfs, ThrowingRunnable read)
    {
        // clear the per-table histograms so min/max reflect exactly this capture's recordings
        ((ClearableHistogram) cfs.metric.liveScannedHistogram.cf).clear();
        ((ClearableHistogram) cfs.metric.tombstoneScannedHistogram.cf).clear();

        long rowsBefore = cfs.metric.totalRowsRead.getCount();
        long warnBefore = cfs.metric.tombstoneWarnings.getCount();
        long failBefore = cfs.metric.tombstoneFailures.getCount();

        Snapshot snapshot = new Snapshot();
        ClientWarn.instance.captureWarnings();
        try
        {
            read.run();
        }
        catch (TombstoneOverwhelmingException e)
        {
            snapshot.abortMessage = e.getMessage();
        }
        catch (Throwable t)
        {
            throw new AssertionError("captured read failed", t);
        }
        finally
        {
            List<String> warnings = ClientWarn.instance.getWarnings();
            if (warnings != null)
                snapshot.clientWarnings.addAll(warnings);
            ClientWarn.instance.resetWarnings();
        }

        snapshot.totalRowsRead = cfs.metric.totalRowsRead.getCount() - rowsBefore;
        snapshot.tombstoneWarnings = cfs.metric.tombstoneWarnings.getCount() - warnBefore;
        snapshot.tombstoneFailures = cfs.metric.tombstoneFailures.getCount() - failBefore;

        snapshot.liveReadsRecorded = cfs.metric.liveScannedHistogram.cf.getCount();
        snapshot.tombstoneReadsRecorded = cfs.metric.tombstoneScannedHistogram.cf.getCount();
        snapshot.liveMin = snapshot.liveReadsRecorded == 0 ? 0 : cfs.metric.liveScannedHistogram.cf.getSnapshot().getMin();
        snapshot.liveMax = snapshot.liveReadsRecorded == 0 ? 0 : cfs.metric.liveScannedHistogram.cf.getSnapshot().getMax();
        snapshot.tombstonesMin = snapshot.tombstoneReadsRecorded == 0 ? 0 : cfs.metric.tombstoneScannedHistogram.cf.getSnapshot().getMin();
        snapshot.tombstonesMax = snapshot.tombstoneReadsRecorded == 0 ? 0 : cfs.metric.tombstoneScannedHistogram.cf.getSnapshot().getMax();
        return snapshot;
    }

    /** Asserts every captured observation is identical between the two snapshots, with a full
     *  side-by-side dump on divergence. */
    public static void assertParity(String label, Snapshot expected, Snapshot actual)
    {
        boolean equal = expected.totalRowsRead == actual.totalRowsRead
                        && expected.tombstoneWarnings == actual.tombstoneWarnings
                        && expected.tombstoneFailures == actual.tombstoneFailures
                        && expected.liveReadsRecorded == actual.liveReadsRecorded
                        && expected.tombstoneReadsRecorded == actual.tombstoneReadsRecorded
                        && expected.liveMin == actual.liveMin
                        && expected.liveMax == actual.liveMax
                        && expected.tombstonesMin == actual.tombstonesMin
                        && expected.tombstonesMax == actual.tombstonesMax
                        && expected.clientWarnings.equals(actual.clientWarnings)
                        && Objects.equals(expected.abortMessage, actual.abortMessage);
        if (!equal)
            fail(String.format("SCAN METRIC divergence [%s]:%n  expected: %s%n  actual:   %s",
                               label, expected.describe(), actual.describe()));
    }
}
