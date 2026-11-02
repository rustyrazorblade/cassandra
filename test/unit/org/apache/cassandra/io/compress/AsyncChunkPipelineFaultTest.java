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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ForwardingExecutorFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.service.DiskErrorsHandler;
import org.apache.cassandra.service.DiskErrorsHandlerService;

import static org.apache.cassandra.utils.Throwables.anyCauseMatches;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A failure raised on the writer thread has to unwind the producer's stack. The try-with-resources
 * in CompactionTask is the only thing that aborts the transaction, so a swallowed failure would
 * commit an SSTable whose data file is short. These tests inject a compression failure at a chosen
 * chunk and assert four things: the producer throws, the throw keeps its type (a compressor fault is
 * not an FSError, a write fault is), no slot is leaked, and no writer thread is left behind.
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
                    anyCauseMatches(thrown, FSError.class::isInstance));

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
     * The other case of the same rule: a genuine write failure must arrive as an FSError with its
     * path, because JVMStabilityInspector tests for the type and DefaultDiskErrorsHandler reads the
     * path to mark a directory unwritable.
     */
    @Test
    public void writeFailureArrivesAsAnFSError()
    {
        File data = FileUtils.createTempFile("asyncFaultChannel", ".db");
        File meta = new File(data.absolutePath() + ".metadata");

        FailingWriteWriter writer = new FailingWriteWriter(data, meta, faultyParams(), 2);
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
                   anyCauseMatches(thrown, FSError.class::isInstance));

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
        Util.spinUntilTrue(() -> writer.pipeline.freeSlotCount() >= expected, 10, TimeUnit.SECONDS);

        assertEquals("a slot leaked on the failure path", expected, writer.pipeline.freeSlotCount());
    }

    /**
     * An interrupted producer must still wait for the writer thread before cleanup frees anything,
     * and must keep its interrupt.
     */
    @Test
    public void interruptedAbortStillWaitsForTheWriterThread() throws Exception
    {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch arrived = new CountDownLatch(1);
        FaultyCompressor.blockAt(1, release, arrived);

        File data = FileUtils.createTempFile("asyncFaultInterrupt", ".db");
        CompressedSequentialWriter writer = newWriter(data, new File(data.absolutePath() + ".metadata"));
        writeChunks(writer, 2);
        assertTrue("writer thread never reached the blocking compress", arrived.await(10, TimeUnit.SECONDS));
        AsyncChunkPipeline pipeline = writer.pipeline;

        Thread producer = Thread.currentThread();
        Thread releaser = new Thread(() -> {
            // Release the writer only once the producer waits for it in the join.
            Util.spinUntilTrue(() -> producer.getState() == Thread.State.TIMED_WAITING, 10, TimeUnit.SECONDS);
            release.countDown();
        });
        releaser.start();

        producer.interrupt();
        Throwable result;
        try
        {
            result = writer.abort(null);
        }
        finally
        {
            assertTrue("the producer lost its interrupt", Thread.interrupted());
            release.countDown();
            releaser.join();
        }

        assertNull("abort failed: " + result, result);
        assertFalse("cleanup ran before the writer thread stopped", pipeline.stillRunning());
        assertEquals("cleanup left slots behind", 0, pipeline.freeSlotCount());
    }

    /** An interrupt while the producer waits for a free slot aborts the write, but is not a disk fault. */
    @Test
    public void interruptWhileWaitingForASlotIsNotADiskFault() throws Exception
    {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch arrived = new CountDownLatch(1);
        FaultyCompressor.blockAt(1, release, arrived);

        File data = FileUtils.createTempFile("asyncFaultWaitInterrupt", ".db");
        CompressedSequentialWriter writer = newWriter(data, new File(data.absolutePath() + ".metadata"));
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicBoolean interruptKept = new AtomicBoolean();
        Thread producer = new Thread(() -> {
            try
            {
                // More chunks than slots, so the producer waits once the writer thread is held.
                writeChunks(writer, SLOTS * 3);
            }
            catch (Throwable t)
            {
                thrown.set(t);
                interruptKept.set(Thread.currentThread().isInterrupted());
            }
        });
        try
        {
            producer.start();
            assertTrue("writer thread never reached the blocking compress", arrived.await(10, TimeUnit.SECONDS));
            Util.spinUntilTrue(() -> producer.getState() == Thread.State.TIMED_WAITING, 10, TimeUnit.SECONDS);
            producer.interrupt();
            producer.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse("producer did not stop on interrupt", producer.isAlive());
        }
        finally
        {
            release.countDown();
        }

        assertNotNull("the interrupt did not stop the producer", thrown.get());
        assertFalse("an interrupt surfaced as a disk fault: " + thrown.get(), anyCauseMatches(thrown.get(), FSError.class::isInstance));
        assertTrue("the producer lost its interrupt", interruptKept.get());
        writer.abort(null);
        settle(writer);
    }

    /**
     * A disk fault on the writer thread that the producer never saw, because it aborted first, must
     * still reach the disk failure policy.
     */
    @Test
    public void unreportedWriteFailureReachesTheDiskFailurePolicy() throws Exception
    {
        List<Throwable> inspected = installRecordingDiskErrorsHandler();
        try
        {
            File data = FileUtils.createTempFile("asyncFaultUnreported", ".db");
            FailingWriteWriter writer = new FailingWriteWriter(data, new File(data.absolutePath() + ".metadata"), faultyParams(), 1);
            // One full chunk and one byte: the chunk goes to the writer thread, and no later flush
            // gives the producer a chance to see the failure.
            writeChunks(writer, 1);
            writer.write(0);
            AsyncChunkPipeline pipeline = writer.pipeline;
            Util.spinUntilTrue(() -> pipeline.recordedFailure() != null, 10, TimeUnit.SECONDS);

            writer.abort(null);

            assertEquals("the unreported failure must reach the disk failure policy once", 1, countFSErrors(inspected));
        }
        finally
        {
            DiskErrorsHandlerService.set(DiskErrorsHandler.NoOpDiskErrorHandler.NO_OP);
        }
    }

    /** A failure the producer already rethrew reaches the policy through the compaction; cleanup must not repeat it. */
    @Test
    public void reportedWriteFailureIsNotInspectedAgainOnCleanup() throws Exception
    {
        List<Throwable> inspected = installRecordingDiskErrorsHandler();
        try
        {
            File data = FileUtils.createTempFile("asyncFaultReported", ".db");
            FailingWriteWriter writer = new FailingWriteWriter(data, new File(data.absolutePath() + ".metadata"), faultyParams(), 1);
            try
            {
                writeChunks(writer, SLOTS * 3);
                writer.finish();
                fail("injected write failure did not reach the producer");
            }
            catch (FSError expected)
            {
                // expected
            }
            writer.abort(null);

            assertEquals("cleanup inspected a failure the producer already rethrew", 0, countFSErrors(inspected));
        }
        finally
        {
            DiskErrorsHandlerService.set(DiskErrorsHandler.NoOpDiskErrorHandler.NO_OP);
        }
    }

    /**
     * A writer thread that dies is a bug, not a disk fault, so the producer gets an
     * IllegalStateException and the disk failure policy stays out of it.
     */
    @Test
    public void deadWriterThreadIsNotADiskFault() throws Exception
    {
        ExecutorFactory saved = ExecutorFactory.Global.executorFactory();
        ExecutorFactory.Global.unsafeSet(new ForwardingExecutorFactory(saved)
        {
            @Override
            public Thread startThread(String name, Runnable runnable)
            {
                // A writer thread that exits at once, without taking any chunk.
                return super.startThread(name, () -> {});
            }
        });
        Throwable thrown = null;
        CompressedSequentialWriter writer;
        try
        {
            File data = FileUtils.createTempFile("asyncFaultDeadWriter", ".db");
            writer = newWriter(data, new File(data.absolutePath() + ".metadata"));
            try
            {
                writeChunks(writer, SLOTS * 3);
                writer.finish();
            }
            catch (Throwable t)
            {
                thrown = t;
            }
        }
        finally
        {
            ExecutorFactory.Global.unsafeSet(saved);
        }

        assertNotNull("a dead writer thread never reached the producer", thrown);
        assertTrue("expected IllegalStateException, got " + thrown, thrown instanceof IllegalStateException);
        assertFalse("a dead writer thread surfaced as a disk fault", anyCauseMatches(thrown, FSError.class::isInstance));
        writer.abort(null);
    }

    private static List<Throwable> installRecordingDiskErrorsHandler()
    {
        List<Throwable> inspected = new CopyOnWriteArrayList<>();
        DiskErrorsHandlerService.set(new ForwardingDiskErrorsHandler(DiskErrorsHandler.NoOpDiskErrorHandler.NO_OP)
        {
            @Override
            public void inspectDiskError(Throwable t)
            {
                inspected.add(t);
            }
        });
        return inspected;
    }

    private static int countFSErrors(List<Throwable> inspected)
    {
        int count = 0;
        for (Throwable t : inspected)
            if (t instanceof FSError)
                count++;
        return count;
    }

    /** Passes every call to a delegate, so a test overrides only what it records. */
    private static class ForwardingDiskErrorsHandler implements DiskErrorsHandler
    {
        private final DiskErrorsHandler delegate;

        ForwardingDiskErrorsHandler(DiskErrorsHandler delegate)
        {
            this.delegate = delegate;
        }

        @Override public void init() { delegate.init(); }
        @Override public void handleCorruptSSTable(CorruptSSTableException e) { delegate.handleCorruptSSTable(e); }
        @Override public void handleFSError(FSError e) { delegate.handleFSError(e); }
        @Override public boolean handleCommitError(String message, Throwable t) { return delegate.handleCommitError(message, t); }
        @Override public void handleStartupFSError(Throwable t) { delegate.handleStartupFSError(t); }
        @Override public void inspectDiskError(Throwable t) { delegate.inspectDiskError(t); }
        @Override public void inspectCommitLogError(Throwable t) { delegate.inspectCommitLogError(t); }
        @Override public void close() throws Exception { delegate.close(); }
    }

    private static void settle(CompressedSequentialWriter writer)
    {
        Util.spinUntilTrue(() -> !writer.pipeline.stillRunning(), 10, TimeUnit.SECONDS);
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
        return new CompressedSequentialWriter(data, meta, null, SequentialWriterOption.DEFAULT,
                                              faultyParams(), collector(), null, BUFFER_BYTES);
    }

    /** Built by class name so CompressionParams instantiates the compressor the way production does. */
    private static CompressionParams faultyParams()
    {
        return new CompressionParams(FaultyCompressor.class.getName(), ImmutableMap.of(), CHUNK,
                                     CompressionParams.DEFAULT_MIN_COMPRESS_RATIO);
    }

    private static MetadataCollector collector()
    {
        return new MetadataCollector(new ClusteringComparator(BytesType.instance));
    }
}
