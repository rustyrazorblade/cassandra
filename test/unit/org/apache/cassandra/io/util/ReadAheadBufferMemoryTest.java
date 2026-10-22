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

package org.apache.cassandra.io.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;

import io.netty.util.concurrent.FastThreadLocalThread;

import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Direct memory held by the read-ahead buffers that threads keep for reuse, measured with the JDK
 * "direct" buffer pool and with {@link ReadAheadBuffer#metrics}.
 */
public class ReadAheadBufferMemoryTest
{
    private static final int KiB = 1024;
    private static final int MiB = 1024 * KiB;
    private static final int BUFFER_SIZE = 256 * KiB;
    /** Room for direct memory allocated by anything else in the JVM while a test runs. */
    private static final long SLACK = 128 * KiB;

    private static final BufferPoolMXBean DIRECT = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)
                                                                    .stream()
                                                                    .filter(pool -> pool.getName().equals("direct"))
                                                                    .findFirst()
                                                                    .orElseThrow();
    private static File file;
    private static int defaultBufferSizeInKb;

    @BeforeClass
    public static void setup() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        defaultBufferSizeInKb = DatabaseDescriptor.getCompressedReadAheadBufferSizeInKB();
        file = new File(JAVA_IO_TMPDIR.getString(), "read-ahead-memory-" + System.nanoTime() + ".bin");
        byte[] data = new byte[2 * MiB];
        new Random(42).nextBytes(data);
        try (FileOutputStream out = new FileOutputStream(file.toJavaIOFile()))
        {
            out.write(data);
        }
    }

    @AfterClass
    public static void cleanup()
    {
        file.tryDelete();
    }

    @After
    public void resetSettings()
    {
        DatabaseDescriptor.setCompressedReadAheadBufferPoolSize(null);
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(defaultBufferSizeInKb);
    }

    @Test
    public void keptBuffersOfLiveThreadsFitTheNodeWideLimit() throws Throwable
    {
        long limit = 8 * MiB;
        DatabaseDescriptor.setCompressedReadAheadBufferPoolSize(new DataStorageSpec.LongMebibytesBound("8MiB"));
        long before = DIRECT.getMemoryUsed();

        try (LiveThreads threads = new LiveThreads(64, () -> openFillAndClose(4, BUFFER_SIZE)))
        {
            long retained = DIRECT.getMemoryUsed() - before;
            assertTrue("64 live threads retain " + retained + " bytes, over the limit of " + limit, retained <= limit + SLACK);
            assertTrue("kept bytes " + ReadAheadBuffer.metrics.size.getValue() + " over the limit", ReadAheadBuffer.metrics.size.getValue() <= limit);
            assertEquals("the kept bytes metric matches the direct memory retained", retained, ReadAheadBuffer.metrics.size.getValue(), SLACK);
            assertEquals(limit, (long) ReadAheadBuffer.metrics.capacity.getValue());
        }
    }

    @Test
    public void threadExitFreesItsKeptBuffersWithoutGC() throws Throwable
    {
        DatabaseDescriptor.setCompressedReadAheadBufferPoolSize(new DataStorageSpec.LongMebibytesBound("64MiB"));
        long before = DIRECT.getMemoryUsed();

        int keptWhileAlive;
        try (LiveThreads threads = new LiveThreads(16, () -> openFillAndClose(4, BUFFER_SIZE)))
        {
            long retained = DIRECT.getMemoryUsed() - before;
            assertEquals("each live thread keeps its 4 buffers", 16 * 4 * BUFFER_SIZE, retained, SLACK);
            keptWhileAlive = ReadAheadBuffer.metrics.bufferCount.getValue();
        }

        long retained = DIRECT.getMemoryUsed() - before;
        assertTrue("exited threads still hold " + retained + " bytes of direct memory", retained <= SLACK);
        assertEquals(16 * 4, keptWhileAlive);
        assertEquals(0L, (long) ReadAheadBuffer.metrics.size.getValue());
        assertEquals(0, (int) ReadAheadBuffer.metrics.bufferCount.getValue());
    }

    @Test
    public void bufferSizeChangeFreesOldSizeBuffers() throws Throwable
    {
        DatabaseDescriptor.setCompressedReadAheadBufferPoolSize(new DataStorageSpec.LongMebibytesBound("64MiB"));
        int newSize = 512 * KiB;
        AtomicLong retainedAtOldSize = new AtomicLong();
        AtomicLong retainedAtNewSize = new AtomicLong();
        AtomicLong allocatedByNextRead = new AtomicLong();

        runOnThreadThatKeepsBuffers(() -> {
            long before = DIRECT.getMemoryUsed();
            DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(256);
            openFillAndClose(4, BUFFER_SIZE);
            retainedAtOldSize.set(DIRECT.getMemoryUsed() - before);

            DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(512);
            openFillAndClose(1, newSize);
            retainedAtNewSize.set(DIRECT.getMemoryUsed() - before);

            long beforeRead = DIRECT.getMemoryUsed();
            try (ChannelProxy channel = new ChannelProxy(file);
                 ReadAheadBuffer buffer = new ReadAheadBuffer(channel, newSize, BufferType.OFF_HEAP))
            {
                buffer.fill(0);
                allocatedByNextRead.set(DIRECT.getMemoryUsed() - beforeRead);
            }
        });

        assertEquals(4 * BUFFER_SIZE, retainedAtOldSize.get(), SLACK);
        assertEquals("the old-size buffers are freed and the new-size buffer is kept", newSize, retainedAtNewSize.get(), SLACK);
        assertTrue("the next read allocated " + allocatedByNextRead.get() + " bytes instead of reusing the kept buffer",
                   allocatedByNextRead.get() <= SLACK);
    }

    @Test
    public void repeatedReadsOnOneThreadReuseOneBuffer() throws Throwable
    {
        DatabaseDescriptor.setCompressedReadAheadBufferPoolSize(new DataStorageSpec.LongMebibytesBound("64MiB"));
        AtomicLong mostAllocated = new AtomicLong();

        runOnThreadThatKeepsBuffers(() -> {
            long before = DIRECT.getMemoryUsed();
            for (int i = 0; i < 1000; i++)
            {
                openFillAndClose(1, BUFFER_SIZE);
                mostAllocated.accumulateAndGet(DIRECT.getMemoryUsed() - before, Math::max);
            }
        });

        assertEquals("1000 reads on one thread reserve one buffer", BUFFER_SIZE, mostAllocated.get(), SLACK);
    }

    @Test
    public void repeatedDirectReadsOnOneThreadReuseOneAlignedBuffer() throws Throwable
    {
        assumeTrue("direct I/O is supported for " + file, FileUtils.isDirectIOSupported(file));
        DatabaseDescriptor.setCompressedReadAheadBufferPoolSize(new DataStorageSpec.LongMebibytesBound("64MiB"));
        int blockSize = FileUtils.getFileBlockSize(file);
        AtomicLong mostAllocated = new AtomicLong();
        long before = DIRECT.getMemoryUsed();

        runOnThreadThatKeepsBuffers(() -> {
            try (ChannelProxy channel = new ChannelProxy(file, ChannelProxy.IOMode.DIRECT))
            {
                for (int i = 0; i < 1000; i++)
                {
                    DirectReadAheadBuffer buffer = new DirectReadAheadBuffer(channel, BUFFER_SIZE, blockSize);
                    buffer.fill((i % 256) * (long) blockSize);
                    assertEquals("the buffer is block aligned", 0, buffer.bufferAddress() % blockSize);
                    buffer.close();
                    mostAllocated.accumulateAndGet(DIRECT.getMemoryUsed() - before, Math::max);
                }
            }
            assertEquals("the thread keeps its aligned buffer for the next read", 1, (int) ReadAheadBuffer.metrics.bufferCount.getValue());
        });

        assertEquals("1000 direct reads on one thread reserve one aligned buffer", BUFFER_SIZE + blockSize, mostAllocated.get(), SLACK);
        assertTrue("the thread's exit frees its kept aligned buffer", DIRECT.getMemoryUsed() - before <= SLACK);
        assertEquals(0L, (long) ReadAheadBuffer.metrics.size.getValue());
    }

    /** A kept aligned buffer can serve a buffered read-ahead buffer, which must free it when it cannot keep it. */
    @Test
    public void bufferedReadFreesAKeptAlignedBuffer() throws Throwable
    {
        assumeTrue("direct I/O is supported for " + file, FileUtils.isDirectIOSupported(file));
        DatabaseDescriptor.setCompressedReadAheadBufferPoolSize(new DataStorageSpec.LongMebibytesBound("64MiB"));
        int blockSize = FileUtils.getFileBlockSize(file);
        long before = DIRECT.getMemoryUsed();

        runOnThreadThatKeepsBuffers(() -> {
            try (ChannelProxy direct = new ChannelProxy(file, ChannelProxy.IOMode.DIRECT);
                 ChannelProxy buffered = new ChannelProxy(file))
            {
                DirectReadAheadBuffer aligned = new DirectReadAheadBuffer(direct, BUFFER_SIZE, blockSize);
                aligned.fill(0);
                aligned.close();
                // nothing more may be kept, so the buffered reader frees the aligned buffer it took
                DatabaseDescriptor.setCompressedReadAheadBufferPoolSize(new DataStorageSpec.LongMebibytesBound("0MiB"));
                ReadAheadBuffer plain = new ReadAheadBuffer(buffered, BUFFER_SIZE, BufferType.OFF_HEAP);
                plain.fill(0);
                plain.close();
            }
        });

        assertTrue("the aligned buffer is freed", DIRECT.getMemoryUsed() - before <= SLACK);
        assertEquals(0L, (long) ReadAheadBuffer.metrics.size.getValue());
    }

    @Test
    public void threadThatDoesNotClearItsThreadLocalsFreesAtClose() throws Throwable
    {
        DatabaseDescriptor.setCompressedReadAheadBufferPoolSize(new DataStorageSpec.LongMebibytesBound("64MiB"));
        long before = DIRECT.getMemoryUsed();
        openFillAndClose(4, BUFFER_SIZE);
        long retained = DIRECT.getMemoryUsed() - before;
        assertTrue("a plain thread retains " + retained + " bytes after close", retained <= SLACK);
        assertEquals(0L, (long) ReadAheadBuffer.metrics.size.getValue());
    }

    @Test
    public void defaultLimitFollowsThreadCountsAndBufferSize()
    {
        long threads = DatabaseDescriptor.getConcurrentReaders()
                       + DatabaseDescriptor.getConcurrentCompactors()
                       + Math.min(DatabaseDescriptor.getConcurrentValidations(), DatabaseDescriptor.getConcurrentCompactors());
        assertEquals(2 * threads * 256 * KiB, DatabaseDescriptor.getCompressedReadAheadBufferPoolSize());
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(512);
        assertEquals(2 * threads * 512 * KiB, DatabaseDescriptor.getCompressedReadAheadBufferPoolSize());
    }

    private interface Task
    {
        void run() throws Exception;
    }

    /** Opens {@code count} buffers at once, fills each, then closes them all. */
    private static void openFillAndClose(int count, int bufferSize) throws Exception
    {
        try (ChannelProxy channel = new ChannelProxy(file))
        {
            List<ReadAheadBuffer> buffers = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
            {
                ReadAheadBuffer buffer = new ReadAheadBuffer(channel, bufferSize, BufferType.OFF_HEAP);
                buffers.add(buffer);
                buffer.fill(i * (long) KiB);
            }
            for (ReadAheadBuffer buffer : buffers)
                buffer.close();
        }
    }

    /** Runs {@code task} on a thread that clears its thread locals on exit, like Cassandra's pool threads. */
    private static void runOnThreadThatKeepsBuffers(Task task) throws Throwable
    {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new FastThreadLocalThread(() -> {
            try
            {
                task.run();
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        });
        thread.start();
        thread.join();
        if (failure.get() != null)
            throw failure.get();
    }

    /** Threads that run a task, then stay alive until closed; close lets them exit and joins them. */
    private static class LiveThreads implements AutoCloseable
    {
        private final CountDownLatch done;
        private final CountDownLatch exit = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final List<Thread> threads = new ArrayList<>();

        LiveThreads(int count, Task task) throws Throwable
        {
            done = new CountDownLatch(count);
            for (int i = 0; i < count; i++)
            {
                Thread thread = new FastThreadLocalThread(() -> {
                    try
                    {
                        task.run();
                    }
                    catch (Throwable t)
                    {
                        failure.compareAndSet(null, t);
                    }
                    finally
                    {
                        done.countDown();
                    }
                    Uninterruptibles.awaitUninterruptibly(exit);
                });
                threads.add(thread);
                thread.start();
            }
            done.await();
            if (failure.get() != null)
            {
                close();
                throw failure.get();
            }
        }

        @Override
        public void close() throws InterruptedException
        {
            exit.countDown();
            for (Thread thread : threads)
                thread.join();
        }
    }
}
