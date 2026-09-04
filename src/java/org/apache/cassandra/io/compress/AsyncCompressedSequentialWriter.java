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
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.utils.Throwables.merge;

/**
 * A {@link CompressedSequentialWriter} that runs compress, write and checksum away from the thread
 * producing the data.
 *
 * In multiple tests, the write side is between 30-40% of a compaction thread's CPU, and 77% of that
 * is compression and CRC. The producer fills a chunk-sized slot, hands it on and takes a fresh one;
 * slots rotate between two bounded queues, so the pool is the back-pressure. When every slot is in
 * flight the producer waits, which is the correct signal that the compressor or the disk is the
 * limit.
 *
 * One thread per writer, not a shared pool. A pool caps total compression bandwidth at its thread
 * count, and the path this replaces has no such cap: the synchronous writer compresses on the
 * producer's own thread, so every writer gets a core's worth. Since
 * {@code DataComponent.buildWriter} routes memtable flushes, stream receivers, scrub and index
 * builds through here as well as compactions, a pool would also queue a flush behind compactions,
 * which never happened before.
 *
 * Ordering needs no protocol: one consumer means chunks are processed in queue order, and
 * {@code chunkOffset}, {@code chunkCount}, the compression metadata and the compressed scratch
 * buffer are touched only on that thread.
 */
public class AsyncCompressedSequentialWriter extends CompressedSequentialWriter
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncCompressedSequentialWriter.class);

    /** Sentinel that ends the writer loop. Never compressed or written. */
    private static final ByteBuffer POISON = ByteBuffer.allocate(0);

    /** How often the background force runs. Bounds only the tail doPrepare has to cover. */
    private static final long FSYNC_PERIOD_MILLIS = 1000L;

    /** How long shutdown waits for the writer thread before giving up and logging. */
    private static final long QUIESCE_MILLIS = 30_000L;

    /** How long the writer waits for a chunk before looking for other work, such as a force. */
    private static final long POLL_MILLIS = 200L;

    /** Slots the producer may take. */
    private final ArrayBlockingQueue<ByteBuffer> free;

    /** Slots handed on, in chunk order. */
    private final ArrayBlockingQueue<ByteBuffer> filled;

    private final Thread writer;

    private final BufferType bufferType;
    private final int chunkLength;
    private final int slotCount;

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

    /** Guards a background force against the channel being closed underneath it. */
    private final Object fsyncLock = new Object();
    private final ScheduledFuture<?> fsyncTask;
    private volatile boolean forcePending = false;
    private long bytesWritten = 0;
    private long bytesForced = 0;

    /**
     * Uncompressed offset actually put on the channel. The post-flush listener is fed this rather
     * than the staged offset: reporting less than is durable only delays an early-open reader,
     * whereas reporting more makes it short-read.
     */
    private volatile long durableOffset = 0;

    /** Published copy of chunkOffset, so the producer's size estimate is not a data race. */
    private volatile long estimatedOnDisk = 0;

    private volatile LongConsumer postFlush = null;

    public AsyncCompressedSequentialWriter(File file,
                                           File offsetsFile,
                                           @Nullable File digestFile,
                                           SequentialWriterOption option,
                                           CompressionParams parameters,
                                           MetadataCollector sstableMetadataCollector,
                                           @Nullable CompressionDictionaryManager compressionDictionaryManager,
                                           int bufferBytes,
                                           OpenOption... extraOpenOptions)
    {
        super(file, offsetsFile, digestFile, option, parameters, sstableMetadataCollector,
              compressionDictionaryManager, extraOpenOptions);

        this.chunkLength = parameters.chunkLength();
        this.bufferType = parameters.getSstableCompressor().preferredBufferType();
        // Derive the slot count from a byte budget: the runway that matters is bytes in flight, and
        // a fixed count would scale it with the table's chunk length.
        this.slotCount = Math.max(2, bufferBytes / chunkLength);
        this.free = new ArrayBlockingQueue<>(slotCount);
        this.filled = new ArrayBlockingQueue<>(slotCount + 1);   // +1 so the poison pill always fits
        // The superclass already allocated one and installed it as this.buffer, and the first swap
        // returns it to the pool, so it counts against the budget.
        this.allocated = 1;

        // Registered before the thread starts, so a throw here leaves nothing running behind.
        this.fsyncTask = option.trickleFsync()
                         ? ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(
                               this::requestForce, FSYNC_PERIOD_MILLIS, FSYNC_PERIOD_MILLIS, TimeUnit.MILLISECONDS)
                         : null;

        this.writer = new Thread(this::writerLoop, "CompactionWriter-" + file.name());
        this.writer.setDaemon(true);
        this.writer.start();
    }

    /**
     * The superclass fires the listener from whichever thread flushed. That would be the writer
     * thread here, and the BIG-path consumer, {@code IndexSummaryBuilder.markDataSynced}, walks maps
     * the producer mutates concurrently in {@code maybeAddEntry}. Keep the callback on the producer
     * and feed it the durable offset instead.
     */
    @Override
    public void setPostFlushListener(LongConsumer runPostFlush)
    {
        assert this.postFlush == null;
        this.postFlush = runPostFlush;
    }

    // ------------------------------------------------------------------ producer side

    /**
     * Hands the filled slot on and installs a fresh one.
     *
     * {@code bufferOffset} is advanced before the swap because {@code current()} is
     * {@code bufferOffset + buffer.position()}, and it is the outgoing slot whose position records
     * how much this flush consumed.
     */
    @Override
    protected void doFlush(int count)
    {
        rethrowFailure();

        ByteBuffer outgoing = buffer;
        bufferOffset = current();

        ByteBuffer next = takeFreeSlot();
        next.clear();
        buffer = next;

        submitted++;
        if (!filled.offer(outgoing))
            throw new IllegalStateException("async writer queue full; slot accounting is wrong");

        LongConsumer listener = postFlush;
        if (listener != null)
            listener.accept(durableOffset);
    }

    /**
     * {@inheritDoc}
     *
     * The mark records {@code chunkOffset} and {@code chunkCount}, both advanced on the writer
     * thread, so it has to be taken against a quiesced writer. Without the drain the mark names an
     * earlier chunk than the file holds, and {@code resetAndTruncate} then takes its "mark lies in
     * an earlier chunk" branch, rebuilds the buffer from the wrong chunk and truncates live data
     * away. Only scrub marks, but scrub reaches this writer.
     */
    @Override
    public DataPosition mark()
    {
        // Flush first so the drain covers it and super.mark() does not flush again behind it.
        if (!buffer.hasRemaining())
            doFlush(0);
        drain();
        return super.mark();
    }

    @Override
    public synchronized void resetAndTruncate(DataPosition mark)
    {
        drain();
        super.resetAndTruncate(mark);
        // The truncate rewound chunkOffset and lastFlushOffset; republish both so neither the size
        // estimate nor the early-open offset reports past the truncation point.
        estimatedOnDisk = super.getEstimatedOnDiskBytesWritten();
        durableOffset = getLastFlushOffset();
    }

    @Override
    public long getEstimatedOnDiskBytesWritten()
    {
        // Published by the writer thread rather than read straight from chunkOffset, which is a
        // plain long advanced off this thread. It lags by whatever is in flight, which only delays
        // an SSTable size switch; nothing reads it for correctness.
        return estimatedOnDisk;
    }

    @Override
    protected void syncInternal()
    {
        doFlush(0);
        drain();
        syncDataOnlyInternal();
    }

    @Override
    public CompressionMetadata open(long overrideLength)
    {
        drain();
        return super.open(overrideLength);
    }

    private ByteBuffer takeFreeSlot()
    {
        while (true)
        {
            rethrowFailure();

            ByteBuffer slot = free.poll();
            if (slot != null)
                return slot;

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

    /**
     * Waits for every submitted chunk to reach the channel. Everything that reads state advanced by
     * the writer thread, or touches the channel from the producer, goes through here first.
     */
    private void drain()
    {
        while (completed < submitted)
        {
            rethrowFailure();
            awaitProgress();
        }
        rethrowFailure();
    }

    /**
     * Waits briefly for the writer to make progress. Polling rather than parking on a condition
     * keeps the writer thread free of any signalling obligation on its error paths, where a missed
     * signal would hang the producer for good.
     */
    private void awaitProgress()
    {
        if (!writer.isAlive() && failure.get() == null)
            failure.compareAndSet(null, new IOException("Async writer thread for " + getPath() + " exited unexpectedly"));

        try
        {
            Thread.sleep(0, 200_000);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            // Not a disk fault: leave the type alone so the disk failure policy is not involved.
            throw new RuntimeException("Interrupted waiting for the async compaction writer", e);
        }
    }

    /**
     * A failure raised on the writer thread has to unwind the producer's stack, because the
     * try-with-resources in CompactionTask is the only thing that aborts the transaction.
     *
     * The type is preserved. FSError is an Error, so an FSWriteError from the channel arrives as one
     * and the disk failure policy still sees it with its path. A RuntimeException -- what
     * {@code flushData} raises for a compressor fault -- stays a RuntimeException, so a compressor
     * bug aborts the compaction rather than stopping transports or killing the JVM.
     */
    private void rethrowFailure()
    {
        Throwable t = failure.get();
        if (t == null)
            return;

        com.google.common.base.Throwables.throwIfUnchecked(t);
        throw new FSWriteError(t instanceof IOException ? (IOException) t : new IOException(t), getPath());
    }

    // ------------------------------------------------------------------ writer thread

    private void writerLoop()
    {
        while (true)
        {
            ByteBuffer slot;
            try
            {
                slot = filled.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e)
            {
                // Nothing interrupts this thread today, but exiting without latching would freeze
                // `completed` and leave the producer waiting for a chunk that will never land.
                failure.compareAndSet(null, e);
                return;
            }

            if (slot == POISON)
                return;

            if (slot != null)
            {
                try
                {
                    flushData(slot);
                    bytesWritten += slot.position();
                    durableOffset = getLastFlushOffset();
                    estimatedOnDisk = super.getEstimatedOnDiskBytesWritten();
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

            if (forcePending)
                backgroundForce();
        }
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
            if (bytesWritten == bytesForced)
                return;

            long written = bytesWritten;
            try
            {
                syncDataOnlyInternal();
                bytesForced = written;
            }
            catch (Throwable t)
            {
                failure.compareAndSet(null, t);
            }
        }
    }

    // ------------------------------------------------------------------ lifecycle

    private void quiesce()
    {
        // Take the lock so a force already running finishes before shutdown is observed; after this
        // none starts, which is what makes closing the channel safe.
        synchronized (fsyncLock)
        {
            shutdown = true;
        }
        if (fsyncTask != null)
            fsyncTask.cancel(false);

        filled.offer(POISON);
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
                         getPath(), QUIESCE_MILLIS);
    }

    @VisibleForTesting
    int freeSlotCount()
    {
        return free.size();
    }

    @VisibleForTesting
    int slotCount()
    {
        return slotCount;
    }

    @VisibleForTesting
    int allocatedSlots()
    {
        return allocated;
    }

    @VisibleForTesting
    boolean writerRunning()
    {
        return writer.isAlive();
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new AsyncTransactionalProxy();
    }

    protected class AsyncTransactionalProxy extends TransactionalProxy
    {
        @Override
        protected Throwable doPreCleanup(Throwable accumulate)
        {
            try
            {
                quiesce();
            }
            catch (Throwable t) { accumulate = merge(accumulate, t); }

            if (writer.isAlive())
            {
                // Free nothing. The thread may be inside compressor.compress writing into
                // `compressed`, or inside the gathering write holding the CRC trailer, and
                // MemoryUtil.clean under native code faults the JVM rather than throwing. That
                // includes the superclass's cleanup, which frees `buffer`, `compressed` and the
                // trailer -- so it is not called either. Close the channel, which is what unblocks a
                // stuck write and lets the abort finish; the buffers carry Cleaners and the
                // collector reclaims them once the thread finally exits.
                try { channel.close(); }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
                return accumulate;
            }

            // Frees the installed slot and nulls buffer; the queues hold only the others.
            accumulate = super.doPreCleanup(accumulate);

            List<ByteBuffer> remaining = new ArrayList<>(slotCount);
            free.drainTo(remaining);
            filled.drainTo(remaining);
            for (ByteBuffer slot : remaining)
            {
                if (slot == POISON)
                    continue;
                try
                {
                    MemoryUtil.clean(slot);
                }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
            }

            return accumulate;
        }
    }
}
