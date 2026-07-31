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
package org.apache.cassandra.db.compaction.differential;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.DigestingCursorMergeSink;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.PrecomputedDigestPartition;
import org.apache.cassandra.db.repair.ValidationCompactionController;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Measures total thread-allocated bytes for a full-table validation compaction (the same
 * per-partition digest work {@code Validator.add()}/{@code Validator.addDigest()} does), comparing
 * the legacy {@code CompactionIterator}-based path against the cursor-backed path
 * ({@link CursorCompactor#mergeNextPartition} + {@link DigestingCursorMergeSink}).
 * <p>
 * This is a measurement tool, not a correctness gate: {@code DigestingCursorMergeSinkParityTest}
 * is the dedicated digest-parity suite; this test's own per-iteration digest comparison exists
 * only to guard against a cheaper-but-wrong measurement (see below).
 * <p>
 * Unlike the materializing sink this replaces, the cursor path here never builds a real
 * {@link UnfilteredRowIterator}/{@code Row}/{@code Cell} per partition at all - it feeds
 * {@code Digest.update(...)} directly from cursor primitives (see {@link DigestingCursorMergeSink}),
 * so the expected reduction now covers both the source-reading/merging side (which scales with how
 * much overlapping/superseded data exists across the sstables being validated) AND the
 * per-partition output materialization the legacy path always pays for.
 */
public class ValidationAllocationComparisonTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(ValidationAllocationComparisonTest.class);

    private static final int PARTITIONS = 50;
    private static final int ROWS_PER_PARTITION = 20;
    private static final int OVERLAPPING_SSTABLES = 4;
    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASURED_ITERATIONS = 5;

    private static com.sun.management.ThreadMXBean threadMXBean()
    {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof com.sun.management.ThreadMXBean))
            return null;
        com.sun.management.ThreadMXBean sunBean = (com.sun.management.ThreadMXBean) bean;
        if (!sunBean.isThreadAllocatedMemorySupported())
            return null;
        if (!sunBean.isThreadAllocatedMemoryEnabled())
            sunBean.setThreadAllocatedMemoryEnabled(true);
        return sunBean;
    }

    @Test
    public void compareLegacyAndCursorValidationAllocation() throws Throwable
    {
        com.sun.management.ThreadMXBean threadMXBean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", threadMXBean != null);

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // OVERLAPPING_SSTABLES rounds over the SAME partitions/rows: every round rewrites the
        // same primary keys, so each of the resulting sstables carries a superseded version of
        // most of the data. This is exactly the "source-side redundancy" the cursor path avoids
        // materializing and the legacy path doesn't.
        for (int round = 0; round < OVERLAPPING_SSTABLES; round++)
        {
            for (long pk = 0; pk < PARTITIONS; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck + round, "val" + ck + "-" + round);
            flush();
        }

        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        assertTrue("test setup: expected overlapping sstables", sstables.size() >= OVERLAPPING_SSTABLES);
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());

        long legacyBest = Long.MAX_VALUE;
        long cursorBest = Long.MAX_VALUE;
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++)
        {
            MeasurementResult legacy = measureLegacyValidation(threadMXBean, cfs, sstables, gcBefore);
            MeasurementResult cursor = measureCursorValidation(threadMXBean, cfs, sstables, gcBefore);

            // Sanity check every iteration, not just once: both paths must scan the same number
            // of partitions and produce IDENTICAL per-partition digests (matching how
            // Validator.rowHash() actually uses digests in production - one fresh digest per
            // partition, not a combined table-wide hash) - otherwise a cheaper-but-wrong cursor
            // path (e.g. silently dropping data) would look like a real allocation win. This is a
            // stand-in for DigestingCursorMergeSinkParityTest's dedicated coverage, not a
            // replacement for it.
            assertEquals("legacy and cursor paths must scan the same number of partitions",
                         legacy.perPartitionDigests.size(), cursor.perPartitionDigests.size());
            for (int p = 0; p < legacy.perPartitionDigests.size(); p++)
                assertArrayEquals("legacy and cursor per-partition digests must match bit-for-bit at partition " + p +
                                  " - otherwise this allocation comparison isn't measuring equivalent work",
                                  legacy.perPartitionDigests.get(p), cursor.perPartitionDigests.get(p));

            if (i >= WARMUP_ITERATIONS)
            {
                legacyBest = Math.min(legacyBest, legacy.allocatedBytes);
                cursorBest = Math.min(cursorBest, cursor.allocatedBytes);
            }
        }

        long delta = legacyBest - cursorBest;
        double reductionPct = 100.0 * delta / legacyBest;
        String summary = String.format(
            "Validation compaction allocation over %d sstables (%d partitions x %d rows, %dx overlapping writes): " +
            "legacy=%,dB cursor=%,dB delta=%,dB reduction=%.1f%%",
            sstables.size(), PARTITIONS, ROWS_PER_PARTITION, OVERLAPPING_SSTABLES,
            legacyBest, cursorBest, delta, reductionPct);
        logger.info(summary);
        System.out.println(summary);

        assertTrue("cursor validation path must not allocate MORE than the legacy path for this scenario: " + summary,
                   cursorBest <= legacyBest);
    }

    private MeasurementResult measureLegacyValidation(com.sun.management.ThreadMXBean threadMXBean, ColumnFamilyStore cfs,
                                                      Collection<SSTableReader> sstables, long gcBefore) throws Exception
    {
        long tid = Thread.currentThread().getId();
        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
            try
            {
                List<byte[]> perPartitionDigests = new ArrayList<>();
                long before = threadMXBean.getThreadAllocatedBytes(tid);
                try (CompactionIterator ci = new CompactionIterator(OperationType.VALIDATION, scanners.scanners, controller,
                                                                    FBUtilities.nowInSeconds(), nextTimeUUID()))
                {
                    while (ci.hasNext())
                    {
                        try (UnfilteredRowIterator partition = ci.next())
                        {
                            Digest digest = Digest.forValidator();
                            UnfilteredRowIterators.digest(partition, digest, MessagingService.current_version);
                            perPartitionDigests.add(digest.digest());
                        }
                    }
                }
                long allocated = threadMXBean.getThreadAllocatedBytes(tid) - before;
                return new MeasurementResult(allocated, perPartitionDigests);
            }
            finally
            {
                scanners.close();
            }
        }
    }

    private MeasurementResult measureCursorValidation(com.sun.management.ThreadMXBean threadMXBean, ColumnFamilyStore cfs,
                                                      Collection<SSTableReader> sstables, long gcBefore) throws Exception
    {
        long tid = Thread.currentThread().getId();
        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            assertTrue("cursor validation must actually be supported for this scenario, or the measurement is vacuous",
                       CursorCompactor.isValidationSupported(sstables, controller));

            Map<SSTableReader, List<PartitionPositionBounds>> boundsBySSTable = new HashMap<>();
            for (SSTableReader sstable : sstables)
                boundsBySSTable.put(sstable, Collections.singletonList(sstable.getPositionsForFullRange()));

            DigestingCursorMergeSink sink = new DigestingCursorMergeSink(cfs.metadata());
            List<byte[]> perPartitionDigests = new ArrayList<>();
            long before = threadMXBean.getThreadAllocatedBytes(tid);
            CursorCompactor compactor = new CursorCompactor(OperationType.VALIDATION, boundsBySSTable, controller,
                                                            FBUtilities.nowInSeconds(), nextTimeUUID());
            try
            {
                while (compactor.mergeNextPartition(sink))
                {
                    PrecomputedDigestPartition partition = sink.takePartitionDigest();
                    perPartitionDigests.add(partition.digestBytes());
                }
            }
            finally
            {
                compactor.close();
            }
            long allocated = threadMXBean.getThreadAllocatedBytes(tid) - before;
            return new MeasurementResult(allocated, perPartitionDigests);
        }
    }

    private static final class MeasurementResult
    {
        final long allocatedBytes;
        final List<byte[]> perPartitionDigests;

        MeasurementResult(long allocatedBytes, List<byte[]> perPartitionDigests)
        {
            this.allocatedBytes = allocatedBytes;
            this.perPartitionDigests = perPartitionDigests;
        }
    }
}
