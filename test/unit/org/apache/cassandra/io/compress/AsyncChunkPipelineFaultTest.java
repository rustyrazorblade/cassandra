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
package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A failure raised on the writer thread has to unwind the producer's stack. The try-with-resources
 * in CompactionTask is the only thing that aborts the transaction, so a swallowed failure would
 * commit an SSTable whose data file is short. These tests inject a compression failure at a chosen
 * chunk and assert four things: the producer throws, the throw is an FSError so the disk failure
 * policy still recognises it, no slot is leaked, and no writer thread is left behind.
 *
 * The chunk indices are chosen around the pool boundary. A slot lost on an exception path does not
 * show up until the pool drains, which is exactly at the boundary.
 */
public class AsyncChunkPipelineFaultTest
{
    private static final int CHUNK = 1 << 14;      // 16 KiB, the default chunk length
    private static final int SLOTS = 4;
    private static final int BUFFER_BYTES = SLOTS * CHUNK;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void resetInjector()
    {
        FaultyCompressor.reset();
    }

    @After
    public void clearInjector()
    {
        FaultyCompressor.reset();
    }

    @Test
    public void failureOnFirstChunkReachesTheProducer()
    {
        assertFailurePropagates(1);
    }

    @Test
    public void failureOnSecondChunkReachesTheProducer()
    {
        assertFailurePropagates(2);
    }

    @Test
    public void failureAtThePoolBoundaryReachesTheProducer()
    {
        assertFailurePropagates(SLOTS);
    }

    @Test
    public void failureJustPastThePoolBoundaryReachesTheProducer()
    {
        assertFailurePropagates(SLOTS + 1);
    }

    /**
     * With no fault injected the same shape of run must succeed, so a passing case above cannot be
     * an artefact of the harness failing for some unrelated reason.
     */
    @Test
    public void controlRunWithoutFaultSucceeds() throws IOException
    {
        File data = FileUtils.createTempFile("asyncFaultControl", ".db");
        File meta = new File(data.absolutePath() + ".metadata");

        CompressedSequentialWriter writer = newWriter(data, meta);
        try
        {
            writeChunks(writer, SLOTS * 3);
            writer.finish();
        }
        finally
        {
            settle(writer);
            assertFalse("writer thread outlived the writer", writer.pipeline.stillRunning());
        }
        assertTrue("no data written", data.length() > 0);
    }

    private void assertFailurePropagates(int failAtChunk)
    {
        FaultyCompressor.failAt(failAtChunk);

        File data = FileUtils.createTempFile("asyncFault" + failAtChunk, ".db");
        File meta = new File(data.absolutePath() + ".metadata");

        CompressedSequentialWriter writer = newWriter(data, meta);
        Throwable thrown = null;
        try
        {
            // Write well past the injected failure. The writer latches it and rethrows on the
            // producer's next interaction, so the producer must not be able to run to completion.
            writeChunks(writer, failAtChunk + SLOTS * 3);
            writer.finish();
        }
        catch (Throwable t)
        {
            thrown = t;
        }

        assertNotNull("failure at chunk " + failAtChunk + " never reached the producer", thrown);
        // A compressor fault is not a disk fault. CompressedSequentialWriter raises a plain
        // RuntimeException for it, and it has to stay one: promoting it to FSWriteError would put a
        // compressor bug through the disk failure policy, which stops transports or kills the JVM
        // depending on disk_failure_policy. The synchronous writer never does that.
        assertFalse("a compressor fault surfaced as " + thrown.getClass().getName()
                    + ", which the disk failure policy would act on",
                    isOrCauses(thrown, FSError.class));

        try
        {
            writer.abort(null);
        }
        catch (Throwable ignored)
        {
            // abort is best effort once the writer has already failed
        }

        settle(writer);
        assertFalse("writer thread outlived the writer", writer.pipeline.stillRunning());
    }

    /**
     * The other arm of the same rule: a genuine write failure must arrive as an FSError with its
     * path, because JVMStabilityInspector tests for the type and DefaultDiskErrorsHandler reads the
     * path to mark a directory unwritable.
     */
    @Test
    public void writeFailureArrivesAsAnFSError()
    {
        File data = FileUtils.createTempFile("asyncFaultChannel", ".db");
        File meta = new File(data.absolutePath() + ".metadata");

        Map<String, String> opts = ImmutableMap.of();
        CompressionParams params = new CompressionParams(FaultyCompressor.class.getName(), opts, CHUNK,
                                                         CompressionParams.DEFAULT_MIN_COMPRESS_RATIO);

        FailingWriteWriter writer = new FailingWriteWriter(data, meta, params, 2);
        Throwable thrown = null;
        try
        {
            writeChunks(writer, SLOTS * 3);
            writer.finish();
        }
        catch (Throwable t)
        {
            thrown = t;
        }

        assertNotNull("write failure never reached the producer", thrown);
        assertTrue("write failure surfaced as " + thrown.getClass().getName()
                   + "; the disk failure policy tests for FSError and reads its path",
                   isOrCauses(thrown, FSError.class));

        try { writer.abort(null); } catch (Throwable ignored) { }
        settle(writer);
    }

    /** Fails the channel write rather than the compressor, at a chosen chunk. */
    private static class FailingWriteWriter extends CompressedSequentialWriter
    {
        private final int failAt;
        private int chunk = 0;

        FailingWriteWriter(File data, File meta, CompressionParams params, int failAt)
        {
            super(data, meta, null, SequentialWriterOption.DEFAULT, params, collector(), null, BUFFER_BYTES);
            this.failAt = failAt;
        }

        @Override
        protected void writeChunk(java.nio.ByteBuffer toWrite)
        {
            failIfDue();
            super.writeChunk(toWrite);
        }

        /** The async path computes the chunk CRC on a compressor thread and calls this overload. */
        @Override
        protected void writeChunk(java.nio.ByteBuffer toWrite, int chunkCrc)
        {
            failIfDue();
            super.writeChunk(toWrite, chunkCrc);
        }

        private void failIfDue()
        {
            if (++chunk == failAt)
                throw new org.apache.cassandra.io.FSWriteError(new IOException("injected write failure"), getPath());
        }
    }

    /** Slots must all come back even on the failure path, or the producer deadlocks once the pool drains. */
    @Test
    public void failureReturnsEverySlotToThePool() throws Exception
    {
        FaultyCompressor.failAt(2);

        File data = FileUtils.createTempFile("asyncFaultSlots", ".db");
        File meta = new File(data.absolutePath() + ".metadata");

        CompressedSequentialWriter writer = newWriter(data, meta);
        try
        {
            writeChunks(writer, SLOTS * 3);
            writer.finish();
            fail("injected failure did not reach the producer");
        }
        catch (Throwable expected)
        {
            // expected
        }

        // Slots are allocated on demand, so the pool holds what was actually handed out less the
        // one still installed as the staging buffer.
        int expected = writer.pipeline.allocatedSlots() - 1;
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (writer.pipeline.freeSlotCount() < expected && System.nanoTime() < deadline)
            Thread.sleep(10);

        assertEquals("a slot leaked on the failure path", expected, writer.pipeline.freeSlotCount());
    }

    private static boolean isOrCauses(Throwable t, Class<?> type)
    {
        for (Throwable c = t; c != null; c = c.getCause())
            if (type.isInstance(c))
                return true;
        return false;
    }

    private static void settle(CompressedSequentialWriter writer)
    {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (writer.pipeline.stillRunning() && System.nanoTime() < deadline)
        {
            try { Thread.sleep(10); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
    }

    private static void writeChunks(CompressedSequentialWriter writer, int chunks) throws IOException
    {
        byte[] block = new byte[CHUNK];
        for (int i = 0; i < block.length; i++)
            block[i] = (byte) (i % 251);   // compressible enough to take the normal branch

        for (int c = 0; c < chunks; c++)
            writer.write(block, 0, block.length);
    }

    private CompressedSequentialWriter newWriter(File data, File meta)
    {
        Map<String, String> opts = ImmutableMap.of();
        // Built by class name so CompressionParams instantiates it the way production does.
        CompressionParams params = new CompressionParams(FaultyCompressor.class.getName(),
                                                         opts,
                                                         CHUNK,
                                                         CompressionParams.DEFAULT_MIN_COMPRESS_RATIO);
        return new CompressedSequentialWriter(data, meta, null, SequentialWriterOption.DEFAULT,
                                              params, collector(), null, BUFFER_BYTES);
    }

    private static MetadataCollector collector()
    {
        return new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
    }
}
