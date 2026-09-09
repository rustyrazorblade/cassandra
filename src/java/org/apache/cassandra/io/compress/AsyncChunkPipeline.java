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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;

import com.google.common.annotations.VisibleForTesting;

import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * Runs compression, checksumming and the channel write away from the thread producing the data.
 *
 * In tests with fast disks the write side is 30-40% of a compaction thread's CPU, about 75% of it
 * compression and CRC. That cost is the same whether the bytes reach the disk through the page cache
 * or through O_DIRECT, so this lives beside {@link CompressedSequentialWriter} rather than inside a
 * subclass of it: both that class and {@link DirectCompressedSequentialWriter} own one, and neither
 * repeats any of it.
 *
 * The producer fills a chunk-sized slot, hands it on and takes a fresh one; slots rotate between
 * two single-producer, single-consumer queues, so the pool is the back-pressure. One thread per
 * writer takes the filled slots in order and does everything order-dependent: compression, the
 * offsets table, chunkOffset, the full-file checksum and the write itself.
 *
 * The writer it serves supplies five things, all of which it already had:
 * {@link CompressedSequentialWriter#flushData(java.nio.ByteBuffer)},
 * {@link CompressedSequentialWriter#getLastFlushOffset}, {@code chunkOffsetSnapshot},
 * {@code forceDataOnly} and {@code getPath}.
 */
class AsyncChunkPipeline
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncChunkPipeline.class);

    /** How long shutdown waits for the writer thread before giving up and logging. */
    private static final long QUIESCE_MILLIS = 30_000L;

    /** How long the writer parks before looking for other work, such as a force. */
    private static final long WRITER_PARK_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

    /** Polls the queue this many times before parking; a busy pipeline never reaches the park. */
    private static final int WRITER_SPINS = 128;

    /** How long the producer parks between checks. Short: this is the back-pressure stall. */
    private static final long PRODUCER_PARK_NANOS = 50_000L;

    private final CompressedSequentialWriter owner;

    /** Slots the producer may take. Offered by the writer thread, polled by the producer. */
    private final SpscArrayQueue<ByteBuffer> free;

    /** Filled chunks, in order. Offered by the producer, polled by the writer thread. */
    private final SpscArrayQueue<ByteBuffer> filled;

    /** Producer-only until the thread starts; null until then. */
    private Thread writer;

    private final BufferType bufferType;
    private final int chunkLength;
    private final int slotCount;
    private final String threadName;

    /** Slots in existence. Grown lazily, so a writer holds only what its pipeline depth needed. */
    private int allocated;

    private long submitted = 0;
    private volatile long completed = 0;

    /**
     * First failure seen off the producer thread, rethrown on the producer's next interaction.
     * Set with compareAndSet so a later failure cannot displace the one that actually caused the
     * abort.
     */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private volatile boolean shutdown = false;

    /** Producer-only: the writer thread is started on the first flush, not during construction. */
    private boolean started = false;

    /** Guards a background force against the channel being closed underneath it. */
    private final Object fsyncLock = new Object();
    private final ScheduledFuture<?> fsyncTask;
    private volatile boolean forcePending = false;

    /** Writer-only: whether anything has reached the channel since the last force. */
    private boolean unforcedWrites = false;

    /**
     * Set while the writer is parked, so the producer pays for an unpark only when it is felt. The
     * park is bounded anyway, so a missed wake-up costs latency, never progress.
     */
    private volatile boolean writerParked = false;

    /**
     * Uncompressed offset actually put on the channel. The post-flush listener is fed this rather
     * than the staged offset: reporting less than is durable only delays an early-open reader,
     * whereas reporting more makes it short-read.
     */
    private volatile long durableOffset = 0;

    /** Published copy of chunkOffset, so the producer's size estimate is not a data race. */
    private volatile long estimatedOnDisk = 0;

    private volatile LongConsumer postFlush = null;

    AsyncChunkPipeline(CompressedSequentialWriter owner,
                       CompressionParams parameters,
                       boolean trickleFsync,
                       int bufferBytes,
                       String name)
    {
        this.owner = owner;
        this.chunkLength = parameters.chunkLength();
        this.bufferType = parameters.getSstableCompressor().preferredBufferType();
        // Derive the slot count from a byte budget: the runway that matters is bytes in flight, and
        // a fixed count would scale it with the table's chunk length.
        this.slotCount = Math.max(2, bufferBytes / chunkLength);
        this.free = new SpscArrayQueue<>(slotCount);
        // +1 so the queue can never reject: the producer holds one slot while offering another.
        this.filled = new SpscArrayQueue<>(slotCount + 1);
        // The writer already allocated one chunk buffer and installed it as its staging buffer, and
        // the first swap returns it here, so it counts against the budget.
        this.allocated = 1;

        // Registered before the thread starts, so a throw here leaves nothing running behind.
        // A period rather than a byte interval: the byte interval caps how long the writing thread
        // stalls in one force, and nothing writes on that thread. What the period bounds is the tail
        // left for the blocking force in doPrepare. 0 turns the background force off entirely.
        int fsyncMillis = DatabaseDescriptor.getAsyncCompactionWriterFsyncIntervalMillis();
        this.fsyncTask = trickleFsync && fsyncMillis > 0
                         ? ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(
                               this::requestForce, fsyncMillis, fsyncMillis, TimeUnit.MILLISECONDS)
                         : null;

        this.threadName = "CompactionWriter-" + name;
    }

    /**
     * Starts the writer thread on first use rather than in the constructor.
     *
     * The pipeline is built by CompressedSequentialWriter's constructor, which runs before a
     * subclass has initialised its own fields; DirectCompressedSequentialWriter's aligned write
     * buffer is still null at that point, and the writer thread calls back through emitChunk into
     * exactly that code. No flush can happen before construction finishes, so first use is safe.
     */
    private void ensureStarted()
    {
        if (!started)
        {
            started = true;
            writer = executorFactory().startThread(threadName, this::writerLoop);
        }
    }

    // ------------------------------------------------------------------ producer side

    /**
     * The writer's superclass fires the post-flush listener from whichever thread flushed. That
     * would be the writer thread here, and the BIG-path consumer,
     * {@code IndexSummaryBuilder.markDataSynced}, walks maps the producer mutates concurrently in
     * {@code maybeAddEntry}. Keep the callback on the producer and feed it the durable offset.
     */
    void setPostFlushListener(LongConsumer runPostFlush)
    {
        assert this.postFlush == null;
        this.postFlush = runPostFlush;
    }

    /** A slot for the producer to fill next. Blocks when everything is in flight. */
    ByteBuffer nextSlot()
    {
        ensureStarted();

        while (true)
        {
            rethrowFailure();

            ByteBuffer slot = free.poll();
            if (slot != null)
            {
                slot.clear();
                return slot;
            }

            // Grow to the budget on demand, so a writer whose pipeline never fills does not hold
            // the whole allowance.
            if (allocated < slotCount)
            {
                allocated++;
                return bufferType.allocate(chunkLength);
            }

            // Everything is in flight. Waiting here is the back-pressure.
            awaitProgress();
        }
    }

    /** Hands a filled chunk to the writer thread. */
    void submit(ByteBuffer outgoing)
    {
        submitted++;
        if (!filled.offer(outgoing))
            throw new IllegalStateException("async writer queue full; slot accounting is wrong");
        if (writerParked)
            LockSupport.unpark(writer);
    }

    /** Fires the early-open callback with what the writer has actually made durable. */
    void firePostFlush()
    {
        LongConsumer listener = postFlush;
        if (listener != null)
            listener.accept(durableOffset);
    }

    /**
     * Waits for every submitted chunk to reach the channel. Everything that reads state advanced by
     * the writer thread, or touches the channel from the producer, goes through here first.
     */
    void drain()
    {
        while (completed < submitted)
        {
            rethrowFailure();
            awaitProgress();
        }
        rethrowFailure();
    }

    /** Lags by whatever is in flight, which only delays an SSTable size switch. */
    long estimatedOnDiskBytesWritten()
    {
        return estimatedOnDisk;
    }

    /** Republishes after a truncate rewound the writer's offsets. */
    void republishOffsets(long estimated, long durable)
    {
        this.estimatedOnDisk = estimated;
        this.durableOffset = durable;
    }

    /**
     * Waits briefly for the writer to make progress. Polling rather than parking on a condition
     * keeps the writer thread free of any signalling obligation on its error paths, where a missed
     * signal would hang the producer for good.
     *
     * parkNanos rather than Thread.sleep: sleep(0, nanos) rounds up to a whole millisecond on JDK 11
     * and 17, which is 20 times the intended stall.
     */
    private void awaitProgress()
    {
        if (started && !writer.isAlive() && failure.get() == null)
            failure.compareAndSet(null, new IOException("Async writer thread for " + owner.getPath() + " exited unexpectedly"));

        LockSupport.parkNanos(PRODUCER_PARK_NANOS);

        if (Thread.interrupted())
        {
            Thread.currentThread().interrupt();
            // Not a disk fault: leave the type alone so the disk failure policy is not involved.
            throw new RuntimeException("Interrupted waiting for the async compaction writer");
        }
    }

    /**
     * A failure raised off the producer thread has to unwind the producer's stack, because the
     * try-with-resources in CompactionTask is the only thing that aborts the transaction.
     *
     * The type is preserved. FSError is an Error, so an FSWriteError from the channel arrives as one
     * and the disk failure policy still sees it with its path. A RuntimeException -- what
     * {@code compressChunk} raises for a compressor fault -- stays a RuntimeException, so a
     * compressor bug aborts the compaction rather than stopping transports or killing the JVM.
     */
    void rethrowFailure()
    {
        Throwable t = failure.get();
        if (t == null)
            return;

        com.google.common.base.Throwables.throwIfUnchecked(t);
        throw new FSWriteError(t instanceof IOException ? (IOException) t : new IOException(t), owner.getPath());
    }

    // ------------------------------------------------------------------ writer thread

    /**
     * Compresses, checksums and writes each chunk in turn.
     *
     * One consumer is what keeps the file in order: chunkOffset, chunkCount, the offsets table, the
     * full-file checksum and the compressed scratch buffer are touched only here.
     */
    private void writerLoop()
    {
        int idle = 0;
        while (true)
        {
            ByteBuffer slot = filled.poll();
            if (slot != null)
            {
                idle = 0;
                try
                {
                    owner.flushData(slot);
                    unforcedWrites = true;
                    durableOffset = owner.getLastFlushOffset();
                    estimatedOnDisk = owner.chunkOffsetSnapshot();
                }
                catch (Throwable t)
                {
                    failure.compareAndSet(null, t);
                }
                finally
                {
                    slot.clear();
                    free.offer(slot);
                    completed++;
                }
            }
            else if (shutdown)
            {
                // quiesce() runs on the producer, which has stopped submitting, so an empty queue
                // here means every submitted chunk has been written.
                return;
            }

            if (forcePending)
                backgroundForce();

            if (slot == null)
                awaitWork(idle++);
        }
    }

    /**
     * Spins for the first few empty polls, then parks. A busy pipeline never reaches the park; an
     * idle one wakes five times a second, as the old timed poll did.
     */
    private void awaitWork(int idle)
    {
        if (idle < WRITER_SPINS)
        {
            Thread.onSpinWait();
            return;
        }

        writerParked = true;
        // Re-check after publishing the flag: the producer may have offered between the poll above
        // and this store, and so skipped the unpark.
        if (filled.isEmpty() && !shutdown && !forcePending)
            LockSupport.parkNanos(WRITER_PARK_NANOS);
        writerParked = false;
    }

    /**
     * Only marks work to do; the force itself runs on the writer thread.
     * {@code ScheduledExecutors.scheduledTasks} is a single thread shared with MessagingService,
     * HintsService and the disk usage monitor, and fdatasync on a loaded device takes tens of
     * milliseconds, so forcing here would delay all of them by the sum of every open writer's forces.
     */
    private void requestForce()
    {
        forcePending = true;
    }

    private void backgroundForce()
    {
        synchronized (fsyncLock)
        {
            if (shutdown)
                return;

            forcePending = false;
            if (!unforcedWrites)
                return;

            try
            {
                owner.forceDataOnly();
                unforcedWrites = false;
            }
            catch (Throwable t)
            {
                failure.compareAndSet(null, t);
            }
        }
    }

    // ------------------------------------------------------------------ lifecycle

    /** True while a writer thread may still touch a buffer, so nothing may be freed. */
    boolean stillRunning()
    {
        return started && writer.isAlive();
    }

    void quiesce()
    {
        // Take the lock so a force already running finishes before shutdown is observed; after this
        // none starts, which is what makes closing the channel safe.
        synchronized (fsyncLock)
        {
            shutdown = true;
        }
        if (fsyncTask != null)
            fsyncTask.cancel(false);

        if (!started)
            return;   // nothing was ever flushed; there is no thread to wait for

        // The writer exits once it finds the queue empty, so nothing already dispatched is dropped.
        LockSupport.unpark(writer);
        try
        {
            writer.join(QUIESCE_MILLIS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }

        if (writer.isAlive())
            logger.error("Async compaction writer thread for {} did not stop within {}ms; its buffers " +
                         "are left to the garbage collector rather than freed underneath it",
                         owner.getPath(), QUIESCE_MILLIS);
    }

    /** Frees every buffer the pipeline owns. Only safe once {@link #stillRunning} is false. */
    Throwable releaseBuffers(Throwable accumulate)
    {
        List<ByteBuffer> remaining = new ArrayList<>(slotCount);
        free.drain(remaining::add);
        filled.drain(remaining::add);
        for (ByteBuffer slot : remaining)
        {
            try
            {
                MemoryUtil.clean(slot);
            }
            catch (Throwable t) { accumulate = merge(accumulate, t); }
        }
        return accumulate;
    }

    @VisibleForTesting
    int freeSlotCount()
    {
        return free.size();
    }

    @VisibleForTesting
    int allocatedSlots()
    {
        return allocated;
    }
}
