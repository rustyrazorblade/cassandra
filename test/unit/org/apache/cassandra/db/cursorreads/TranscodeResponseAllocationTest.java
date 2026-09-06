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

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end allocation measurement of the M3.3b transcode response path (CASSANDRA-20428):
 * {@code SinglePartitionReadCommand.createResponseLocally} with the cursor read path ON, against
 * the identical call with it OFF.
 *
 * WHY THIS EXISTS. Every other allocation instrument on this ticket stops short of the response.
 * {@code CursorReadAllocationGateTest} and {@code OverlapMergeAllocationBaselineTest} measure
 * {@code executeLocally}, which has no transcode path at all.
 * {@code LateMaterializationAllocationBaselineTest} measures response serialization, but as
 * {@code serializerForIntraNode().serialize} over an already-materialized iterator — the DEFAULT
 * path's encoding, identical on both settings by construction, which is exactly why its
 * "response-serialization overhead per pass" line reports the same number for both.
 * {@code TranscodeCellStreamingDifferentialTest} drives {@code TranscodeMergeSink} and
 * {@code MaterializingMergeSink} directly over hand-opened legs, so it sees neither the response
 * envelope nor the buffers {@code CursorReads.buildTranscodeResponseBytes} allocates around it.
 *
 * The result is that the transcode path — the one designed to skip Row/Cell materialization AND
 * the serialization pass together — has never been measured as a whole against the path it
 * replaces. This test is that measurement.
 *
 * METHOD: the established one ({@code CursorReadAllocationGateTest}) — {@code ThreadMXBean}
 * thread-allocated bytes, warm both settings to JIT steady state, then min-of-N measured passes
 * each. Numbers are LOGGED, not gated: this is a recording instrument for the copy-reduction work,
 * and a ratio bound would have to be calibrated from numbers that do not exist yet.
 *
 * TWO SHAPES, because the cost this instrument is aimed at scales with response SIZE.
 * {@code buildTranscodeResponseBytes} allocates its {@code rowEvents} and envelope buffers with
 * {@code new DataOutputBuffer()} — 128 bytes, doubling, copying the whole contents at each growth
 * — while {@code ReadResponse.LocalDataResponse.build} sizes its one buffer from the
 * {@code estimatedResponseBytes} moving average. A wide response and a narrow one therefore
 * diverge, and one shape alone would hide it.
 *
 * SILENT-FALLBACK GUARD, in both directions, the discipline every counter on this ticket follows:
 * {@link CursorReads#transcodeResponsesServed()} must advance exactly once per measured pass with
 * the path on, and not at all with it off. Every layer of
 * {@code queryStorageToResponseBytes}' gate declines by returning null, so an ineligible command
 * would quietly measure the default path twice and report a perfectly healthy ratio of 1.0.
 *
 * The responses are also asserted digest-equal. Byte equivalence has its own coverage
 * ({@code TranscodeWireFormatDifferentialTest}); this is the cheap standing check that the two
 * things being compared are still the same answer.
 */
public class TranscodeResponseAllocationTest extends CursorReadDifferentialTester
{
    private static final Logger logger = LoggerFactory.getLogger(TranscodeResponseAllocationTest.class);

    protected static final int SOURCES = 3;
    protected static final int WIDE_ROWS = 1024;
    /** rows in the narrow slice shape; small enough that per-read constants dominate */
    protected static final int NARROW_ROWS = 16;
    private static final int COLUMNS = 8;
    private static final long PK = 1L;

    private static final int WARMUP_PASSES = 12;
    private static final int MEASURED_PASSES = 8;

    /** Blackhole so pass consumption cannot be dead-code-eliminated. */
    private static volatile int sink;

    private static final class ShapeResult
    {
        long defaultBest, transcodeBest;
        int responseBytes;

        double ratio()
        {
            return (double) transcodeBest / defaultBest;
        }
    }

    @Test
    public void transcodeResponseAllocationAgainstDefaultPath() throws Throwable
    {
        com.sun.management.ThreadMXBean bean = threadMXBean();
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM", bean != null);

        ColumnFamilyStore cfs = loadWorkload();
        long now = FBUtilities.nowInSeconds();

        List<String> labels = new ArrayList<>();
        List<Supplier<SinglePartitionReadCommand>> shapes = new ArrayList<>();

        labels.add("full partition (" + WIDE_ROWS + " rows, " + SOURCES + " overlapping legs)");
        shapes.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, PK).withNowInSeconds(now).build());

        labels.add("narrow slice (" + NARROW_ROWS + " rows, " + SOURCES + " overlapping legs)");
        shapes.add(() -> (SinglePartitionReadCommand) Util.cmd(cfs, PK).withNowInSeconds(now)
                                                          .fromIncl(0L).toExcl((long) NARROW_ROWS).build());

        for (int i = 0; i < shapes.size(); i++)
        {
            ShapeResult result = measureShape(bean, cfs, shapes.get(i));
            logger.info("transcode response allocation [{}]:\n" +
                        "  allocation/pass: default={}B transcode={}B ratio={}\n" +
                        "  response size: {} bytes",
                        labels.get(i), result.defaultBest, result.transcodeBest,
                        String.format("%.4f", result.ratio()), result.responseBytes);
        }
    }

    private ShapeResult measureShape(com.sun.management.ThreadMXBean bean,
                                     ColumnFamilyStore cfs,
                                     Supplier<SinglePartitionReadCommand> shape) throws Throwable
    {
        assertGateOpen(cfs, shape);
        ShapeResult result = new ShapeResult();
        result.responseBytes = assertDigestsMatchAndMeasureSize(shape);

        try
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
            for (int i = 0; i < WARMUP_PASSES; i++)
                runPass(shape);
            DatabaseDescriptor.setCursorReadsEnabled(true);
            for (int i = 0; i < WARMUP_PASSES; i++)
                runPass(shape);

            DatabaseDescriptor.setCursorReadsEnabled(false);
            long transcodedBeforeDefault = CursorReads.transcodeResponsesServed();
            result.defaultBest = measureBest(bean, shape);
            assertEquals("default-path measurement unexpectedly served a transcoded response",
                         transcodedBeforeDefault, CursorReads.transcodeResponsesServed());

            DatabaseDescriptor.setCursorReadsEnabled(true);
            long transcodedBeforeCursor = CursorReads.transcodeResponsesServed();
            result.transcodeBest = measureBest(bean, shape);
            assertEquals("transcode measurement did not serve a transcoded response per pass (silent fallback?)",
                         MEASURED_PASSES,
                         CursorReads.transcodeResponsesServed() - transcodedBeforeCursor);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
        return result;
    }

    /** Min thread-allocated bytes for one {@code createResponseLocally} call over
     *  {@link #MEASURED_PASSES} passes. */
    private long measureBest(com.sun.management.ThreadMXBean bean, Supplier<SinglePartitionReadCommand> shape)
    {
        long tid = Thread.currentThread().getId();
        long best = Long.MAX_VALUE;
        for (int i = 0; i < MEASURED_PASSES; i++)
        {
            long before = bean.getThreadAllocatedBytes(tid);
            runPass(shape);
            best = Math.min(best, bean.getThreadAllocatedBytes(tid) - before);
        }
        return best;
    }

    /** One measured pass: exactly what {@code ReadCommandVerbHandler.doRead} does to build a
     *  replica response, and nothing else. The response is consumed by identity, not by
     *  {@code digest()} — digesting re-serializes the whole response and would dominate the
     *  measurement with work a data response never does. */
    private void runPass(Supplier<SinglePartitionReadCommand> shape)
    {
        SinglePartitionReadCommand command = shape.get();
        try (ReadExecutionController controller = command.executionController())
        {
            sink += System.identityHashCode(command.createResponseLocally(controller));
        }
    }

    /** The two things being compared must still be the same answer; also returns the transcoded
     *  response's size, which is what the output buffers have to grow to reach. */
    private int assertDigestsMatchAndMeasureSize(Supplier<SinglePartitionReadCommand> shape)
    {
        try
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
            ByteBuffer defaultDigest = digestOf(shape);
            DatabaseDescriptor.setCursorReadsEnabled(true);
            ByteBuffer transcodeDigest = digestOf(shape);
            assertEquals("transcode response digest diverged from the default path's", defaultDigest, transcodeDigest);

            SinglePartitionReadCommand command = shape.get();
            try (ReadExecutionController controller = command.executionController())
            {
                ReadResponse response = command.createResponseLocally(controller);
                byte[] bytes = responseBytes(command);
                assertTrue("expected a non-empty response to measure", bytes.length > 0);
                sink += System.identityHashCode(response);
                return bytes.length;
            }
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static ByteBuffer digestOf(Supplier<SinglePartitionReadCommand> shape)
    {
        SinglePartitionReadCommand command = shape.get();
        try (ReadExecutionController controller = command.executionController())
        {
            return command.createResponseLocally(controller).digest(command);
        }
    }

    /** See the class javadoc: every gate layer declines by returning null, so an ineligible
     *  command measures the default path twice and reports a healthy-looking 1.0. */
    private void assertGateOpen(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> shape)
    {
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = shape.get();
            assertTrue("command shape is not supported by the cursor read gate: " + probe,
                       CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));
            long before = CursorReads.transcodeResponsesServed();
            try (ReadExecutionController controller = probe.executionController())
            {
                sink += System.identityHashCode(probe.createResponseLocally(controller));
            }
            assertTrue("command shape does not reach the transcode response path — this measurement " +
                       "would silently compare the default path against itself: " + probe,
                       CursorReads.transcodeResponsesServed() > before);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /**
     * One partition, {@value #SOURCES} FULLY overlapping sstables: every round rewrites every
     * (pk, ck) with a later timestamp, so the merge reconciles and discards S-1 shadowed copies —
     * the shape the cursor merge exists for. Columns alternate bigint and text so both value arms
     * run: fixed-length values land straight in the final array, variable-length ones take
     * {@code SSTableCursorReader.copyCellContents}' chunked path.
     */
    private ColumnFamilyStore loadWorkload() throws Throwable
    {
        StringBuilder create = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        StringBuilder names = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < COLUMNS; i++)
        {
            create.append(", v").append(i).append(i % 2 == 0 ? " bigint" : " text");
            names.append(", v").append(i);
            placeholders.append(", ?");
        }
        create.append(", PRIMARY KEY(pk, ck)) WITH compression = {'enabled': 'false'}");
        createTable(create.toString());

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String insert = "INSERT INTO %s (pk, ck" + names + ") VALUES (?, ?" + placeholders + ')';
        Object[] values = new Object[2 + COLUMNS];
        values[0] = PK;
        for (int round = 0; round < SOURCES; round++)
        {
            for (long ck = 0; ck < WIDE_ROWS; ck++)
            {
                values[1] = ck;
                for (int i = 0; i < COLUMNS; i++)
                    values[2 + i] = i % 2 == 0 ? (Object) (ck * 31 + round)
                                               : (Object) ("value-" + round + '-' + ck + '-' + i);
                execute(insert, values);
            }
            flush();
        }

        assertEquals("expected exactly SOURCES overlapping sstables", SOURCES, cfs.getLiveSSTables().size());
        return cfs;
    }

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
}
