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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.codahale.metrics.Histogram;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.fail;

/**
 * Captures the scan metrics a read records on one table, so two read paths can be compared on
 * them, not only on their bytes.  Per read run it captures:
 * <ul>
 *   <li>{@code totalRowsRead}, {@code tombstoneWarnings} and {@code tombstoneFailures}: exact counter deltas;</li>
 *   <li>the table-level live-row, tombstone, purgeable-tombstone, local read size, row index size,
 *       repaired-data overread and sstables-per-read histograms: every bucket count.  The histograms are cleared
 *       before the read, and the counts are the non-decaying ones, so two runs that record the
 *       same values have equal buckets;</li>
 *   <li>the local read size and row index size warning and abort meters: exact count deltas;</li>
 *   <li>the top-partition samplers (reads, rows, tombstones and sstables per partition), sampled
 *       during the read: every sample with its exact count;</li>
 *   <li>the client warning text, which holds the exact live-row and tombstone counts;</li>
 *   <li>the {@link TombstoneOverwhelmingException} message, which holds the exact tombstone count.</li>
 * </ul>
 * Not captured: read latency and the slowest-query sampler, which depend on timing; and the
 * keyspace and global histograms, which other tables in the test JVM also update.
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

    /** One capture's observations; all fields compared by {@link #assertParity}. */
    public static final class Snapshot
    {
        long totalRowsRead;            // exact sum of liveRows over the captured reads
        long tombstoneWarnings;        // exact warn-threshold trips
        long tombstoneFailures;        // exact fail-threshold trips
        long liveReadsRecorded;        // liveScannedHistogram update count
        long tombstoneReadsRecorded;   // tombstoneScannedHistogram update count
        long liveMin, liveMax;         // bucket min/max of the liveRows recorded this capture
        long tombstonesMin, tombstonesMax; // bucket min/max of the tombstones recorded this capture
        /** each table histogram's update count and bucket counts, and each meter's count delta, by name */
        Map<String, String> histograms = new LinkedHashMap<>();
        /** each top-partition sampler's samples, by sampler type */
        Map<String, String> samples = new LinkedHashMap<>();
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
                                 "histograms=%s samples=%s clientWarnings=%s abort=%s",
                                 totalRowsRead, liveReadsRecorded, liveMin, liveMax,
                                 tombstoneReadsRecorded, tombstonesMin, tombstonesMax,
                                 tombstoneWarnings, tombstoneFailures,
                                 histograms, samples, clientWarnings, abortMessage);
        }
    }

    private static final int SAMPLER_CAPACITY = 1024;

    private static Map<String, TableMetrics.TableHistogram> histograms(ColumnFamilyStore cfs)
    {
        Map<String, TableMetrics.TableHistogram> h = new LinkedHashMap<>();
        h.put("LiveScanned", cfs.metric.liveScannedHistogram);
        h.put("TombstoneScanned", cfs.metric.tombstoneScannedHistogram);
        h.put("PurgeableTombstoneScanned", cfs.metric.purgeableTombstoneScannedHistogram);
        h.put("LocalReadSize", cfs.metric.localReadSize);
        h.put("RowIndexSize", cfs.metric.rowIndexSize);
        h.put("RepairedDataTrackingOverreadRows", cfs.metric.repairedDataTrackingOverreadRows);
        h.put("SSTablesPerRead", cfs.metric.sstablesPerReadHistogram);
        return h;
    }

    private static Map<String, TableMetrics.TableMeter> meters(ColumnFamilyStore cfs)
    {
        Map<String, TableMetrics.TableMeter> m = new LinkedHashMap<>();
        m.put("LocalReadSizeWarnings", cfs.metric.localReadSizeWarnings);
        m.put("LocalReadSizeAborts", cfs.metric.localReadSizeAborts);
        m.put("RowIndexSizeWarnings", cfs.metric.rowIndexSizeWarnings);
        m.put("RowIndexSizeAborts", cfs.metric.rowIndexSizeAborts);
        return m;
    }

    private static Map<String, Sampler<ByteBuffer>> samplers(ColumnFamilyStore cfs)
    {
        Map<String, Sampler<ByteBuffer>> m = new LinkedHashMap<>();
        m.put("READS", cfs.metric.topReadPartitionFrequency);
        m.put("READ_ROW_COUNT", cfs.metric.topReadPartitionRowCount);
        m.put("READ_TOMBSTONE_COUNT", cfs.metric.topReadPartitionTombstoneCount);
        m.put("READ_SSTABLE_COUNT", cfs.metric.topReadPartitionSSTableCount);
        return m;
    }

    /** The update count and every non-decaying bucket count, trailing empty buckets left out. */
    static String buckets(Histogram histogram)
    {
        long[] values = histogram.getSnapshot().getValues();
        int length = values.length;
        while (length > 0 && values[length - 1] == 0)
            length--;
        return "count=" + histogram.getCount() + " buckets=" + Arrays.toString(Arrays.copyOf(values, length));
    }

    /** Waits for the samples the read queued on the sampler executor. */
    private static void awaitSamples()
    {
        try
        {
            Sampler.samplerExecutor.submit(() -> {}).get(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        catch (ExecutionException | TimeoutException e)
        {
            throw new AssertionError(e);
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
        // clear the per-table histograms so they hold exactly this capture's recordings
        for (TableMetrics.TableHistogram histogram : histograms(cfs).values())
            ((ClearableHistogram) histogram.cf).clear();
        Map<String, Long> metersBefore = new LinkedHashMap<>();
        meters(cfs).forEach((name, meter) -> metersBefore.put(name, meter.table.getCount()));
        // samples queued by earlier reads must not land in this capture
        awaitSamples();
        for (Sampler<ByteBuffer> sampler : samplers(cfs).values())
            sampler.beginSampling(SAMPLER_CAPACITY, TimeUnit.HOURS.toMillis(1));

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

        awaitSamples();
        samplers(cfs).forEach((name, sampler) -> {
            List<String> samples = new ArrayList<>();
            for (Sampler.Sample<ByteBuffer> sample : sampler.finishSampling(SAMPLER_CAPACITY))
                samples.add(ByteBufferUtil.bytesToHex(sample.value) + "=" + sample.count + "+/-" + sample.error);
            samples.sort(null);
            snapshot.samples.put(name, samples.toString());
        });
        histograms(cfs).forEach((name, histogram) -> snapshot.histograms.put(name, buckets(histogram.cf)));
        meters(cfs).forEach((name, meter) -> snapshot.histograms.put(name, "count=" + (meter.table.getCount() - metersBefore.get(name))));

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
                        && expected.histograms.equals(actual.histograms)
                        && expected.samples.equals(actual.samples)
                        && expected.clientWarnings.equals(actual.clientWarnings)
                        && Objects.equals(expected.abortMessage, actual.abortMessage);
        if (!equal)
            fail(String.format("SCAN METRIC divergence [%s]:%n  expected: %s%n  actual:   %s",
                               label, expected.describe(), actual.describe()));
    }
}
