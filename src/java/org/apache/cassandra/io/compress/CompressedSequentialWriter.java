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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Optional;
import java.util.zip.CRC32;

import javax.annotation.Nullable;

import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChecksumWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.utils.Throwables.merge;

public class CompressedSequentialWriter extends SequentialWriter
{
    protected final ChecksumWriter crcMetadata;

    // holds offset in the file where current chunk should be written
    // changed only by flush() method where data buffer gets compressed and stored to the file
    protected long chunkOffset = 0;

    // index file writer (random I/O)
    protected final CompressionMetadata.Writer metadataWriter;
    private final ICompressor compressor;

    // used to store compressed data
    private ByteBuffer compressed;

    // holds a number of already written chunks
    protected int chunkCount = 0;

    protected long uncompressedSize = 0;
    protected long compressedSize = 0;

    protected final MetadataCollector sstableMetadataCollector;
    private final CompressionDictionaryManager compressionDictionaryManager;

    private final ByteBuffer crcCheckBuffer = ByteBuffer.allocate(4);
    protected final Optional<File> digestFile;

    // Non-null when crcMetadata is the inline-trailer writer this class installs. A subclass that
    // overrides createChecksumWriter supplies its own CRC sink and its own writeChunk.
    private final InlineTrailerChecksumWriter inlineTrailer;

    // Reused so writeChunk's gathering write allocates nothing per chunk.
    private final ByteBuffer[] chunkAndTrailer = new ByteBuffer[2];

    /**
     * Non-null when the write path runs off this thread. Both this class and the O_DIRECT subclass
     * get it from here, so neither carries a copy of the machinery.
     */
    protected final AsyncChunkPipeline pipeline;

    private final int maxCompressedLength;
    private final boolean isDictionaryEnabled;

    private static ByteBuffer allocateBuffer(CompressionParams parameters)
    {
        return parameters.getSstableCompressor().preferredBufferType().allocate(parameters.chunkLength());
    }

    private static SequentialWriterOption buildOption(SequentialWriterOption option, CompressionParams parameters)
    {
        return SequentialWriterOption.newBuilder()
                                     .bufferSize(parameters.chunkLength())
                                     .bufferType(parameters.getSstableCompressor().preferredBufferType())
                                     .trickleFsync(option.trickleFsync())
                                     .trickleFsyncByteInterval(option.trickleFsyncByteInterval())
                                     .finishOnClose(option.finishOnClose())
                                     .build();
    }

    public CompressedSequentialWriter(File file,
                                      File offsetsFile,
                                      @Nullable File digestFile,
                                      SequentialWriterOption option,
                                      CompressionParams parameters,
                                      MetadataCollector sstableMetadataCollector)
    {
        this(file, offsetsFile, digestFile, option, parameters, sstableMetadataCollector, null);
    }

    /**
     * Create CompressedSequentialWriter with optional compression dictionary and channel options.
     *
     * @param file File to write
     * @param offsetsFile File to write compression metadata
     * @param digestFile File to write digest, or null if not needed
     * @param option Write option (buffer size and type will be set the same as compression params)
     * @param parameters Compression parameters
     * @param sstableMetadataCollector Metadata collector
     * @param compressionDictionaryManager manages compression dictionary; null if absent
     * @param extraOpenOptions additional options to pass to FileChannel.open (e.g., ExtendedOpenOption.DIRECT)
     */
    public CompressedSequentialWriter(File file,
                                      File offsetsFile,
                                      @Nullable File digestFile,
                                      SequentialWriterOption option,
                                      CompressionParams parameters,
                                      MetadataCollector sstableMetadataCollector,
                                      @Nullable CompressionDictionaryManager compressionDictionaryManager,
                                      OpenOption... extraOpenOptions)
    {
        this(file, offsetsFile, digestFile, option, parameters, sstableMetadataCollector,
             compressionDictionaryManager, 0, extraOpenOptions);
    }

    /**
     * @param asyncBufferBytes bytes the write path may keep in flight off the calling thread, or 0
     *                         to compress and write inline as before
     */
    public CompressedSequentialWriter(File file,
                                      File offsetsFile,
                                      @Nullable File digestFile,
                                      SequentialWriterOption option,
                                      CompressionParams parameters,
                                      MetadataCollector sstableMetadataCollector,
                                      @Nullable CompressionDictionaryManager compressionDictionaryManager,
                                      int asyncBufferBytes,
                                      OpenOption... extraOpenOptions)
    {
        super(file, allocateBuffer(parameters), buildOption(option, parameters), true, extraOpenOptions);
        ICompressor compressor = parameters.getSstableCompressor();
        this.digestFile = Optional.ofNullable(digestFile);

        // buffer for compression should be the same size as buffer itself
        compressed = compressor.preferredBufferType().allocate(compressor.initialCompressedBufferLength(buffer.capacity()));

        maxCompressedLength = parameters.maxCompressedLength();

        // Note that we cannot rely on the compressor type to tell whether dictionary compression is enabled.
        // Because the `CompressionParams` for this method is updated at the callsite, `DataComponent.buildWriter`.
        // See CASSANDRA-15379 for details regarding the optimization.
        // Meanwhile, as long as dictionary-based compression is enabled, we want to collect samples.
        this.isDictionaryEnabled = compressionDictionaryManager != null && compressionDictionaryManager.isEnabled();

        CompressionDictionary compressionDictionary = compressionDictionaryManager == null ? null : compressionDictionaryManager.getCurrent();
        if (compressionDictionary != null && compressor instanceof IDictionaryCompressor)
        {
            compressor = ((IDictionaryCompressor) compressor).getOrCopyWithDictionary(compressionDictionary);
        }
        else
        {
            // It is likely on the sstable flushing path and LZ4 compressor or something else is picked.
            // In this case, we disable the compression dictionary, i.e. do not attach the dictionary
            // bytes to the CompressionInfo component.
            compressionDictionary = null;
        }
        this.compressor = compressor;
        this.compressionDictionaryManager = compressionDictionaryManager;
        /* Index File (-CompressionInfo.db component) and it's header */
        metadataWriter = CompressionMetadata.Writer.open(parameters, offsetsFile, compressionDictionary);

        this.sstableMetadataCollector = sstableMetadataCollector;
        crcMetadata = createChecksumWriter();
        this.pipeline = asyncBufferBytes > 0
                        ? new AsyncChunkPipeline(this, parameters, option.trickleFsync(), asyncBufferBytes, file.name())
                        : null;
        this.inlineTrailer = crcMetadata instanceof InlineTrailerChecksumWriter
                             ? (InlineTrailerChecksumWriter) crcMetadata
                             : null;
    }

    /**
     * Creates the {@link ChecksumWriter} for the chunk and full-file checksums. Invoked from the constructor,
     * so overrides must not read subclass fields.
     */
    protected ChecksumWriter createChecksumWriter()
    {
        return new InlineTrailerChecksumWriter();
    }

    @Override
    public long getOnDiskFilePointer()
    {
        try
        {
            return fchannel.position();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    /**
     * Get a quick estimation on how many bytes have been written to disk
     *
     * It should for the most part be exactly the same as getOnDiskFilePointer()
     */
    @Override
    public long getEstimatedOnDiskBytesWritten()
    {
        // chunkOffset is a plain long advanced on the pipeline's thread when one is running, so read
        // the value it publishes rather than racing on the field.
        return pipeline == null ? chunkOffset : pipeline.estimatedOnDiskBytesWritten();
    }

    /** The raw offset, for the pipeline to publish after it writes a chunk. */
    long chunkOffsetSnapshot()
    {
        return chunkOffset;
    }

    /** syncDataOnlyInternal is protected in another package, so the pipeline goes through here. */
    void forceDataOnly()
    {
        syncDataOnlyInternal();
    }

    /**
     * Hands the filled buffer to the pipeline and installs a fresh one, or falls through to the
     * inline flush when there is no pipeline.
     *
     * bufferOffset is advanced before the swap because current() is bufferOffset + buffer.position(),
     * and it is the outgoing buffer whose position records how much this flush consumed.
     */
    @Override
    protected void doFlush(int count)
    {
        if (pipeline == null)
        {
            super.doFlush(count);
            return;
        }

        pipeline.rethrowFailure();

        ByteBuffer outgoing = buffer;
        bufferOffset = current();
        buffer = pipeline.nextSlot();

        pipeline.submit(outgoing);
        pipeline.firePostFlush();
    }

    @Override
    public void setPostFlushListener(java.util.function.LongConsumer runPostFlush)
    {
        // The superclass fires this from whichever thread flushed. That would be the pipeline's
        // thread, and the BIG-path consumer, IndexSummaryBuilder.markDataSynced, walks maps the
        // producer mutates concurrently in maybeAddEntry.
        if (pipeline != null)
            pipeline.setPostFlushListener(runPostFlush);
        else
            super.setPostFlushListener(runPostFlush);
    }

    @Override
    public void flush()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void flushData()
    {
        flushData(buffer);
    }

    /**
     * Compresses, writes and checksums one filled chunk buffer.
     */
    protected void flushData(ByteBuffer src)
    {
        ChunkPrep prep = new ChunkPrep();
        compressChunk(src, compressed, prep);
        emitChunk(prep, src, null);
    }

    /** What compressing a chunk produced: which buffer to write, and the two lengths. */
    protected static final class ChunkPrep
    {
        ByteBuffer toWrite;
        int uncompressedLength;
        int compressedLength;
    }

    /**
     * Compresses one chunk into {@code out}.
     *
     * Reads no writer state beyond the compressor and the length limit, and mutates only the two
     * buffers it is handed, so it can run on any thread. Everything order-dependent is in
     * {@link #emitChunk}.
     */
    protected void compressChunk(ByteBuffer src, ByteBuffer out, ChunkPrep prep)
    {
        try
        {
            src.flip();
            out.clear();
            compressor.compress(src, out);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Compression exception", e); // shouldn't happen
        }

        int uncompressedLength = src.position();
        int compressedLength = out.position();
        ByteBuffer toWrite = out;
        if (compressedLength >= maxCompressedLength)
        {
            toWrite = src;
            if (uncompressedLength >= maxCompressedLength)
            {
                compressedLength = uncompressedLength;
            }
            else
            {
                // Pad the uncompressed data so that it reaches the max compressed length.
                // This could make the chunk appear longer, but this path is only reached at the end of the file, where
                // we use the file size to limit the buffer on reading.
                assert maxCompressedLength <= src.capacity();   // verified by CompressionParams.validate
                src.limit(maxCompressedLength);
                ByteBufferUtil.writeZeroes(src, maxCompressedLength - uncompressedLength);
                compressedLength = maxCompressedLength;
            }
        }
        toWrite.flip();

        prep.toWrite = toWrite;
        prep.uncompressedLength = uncompressedLength;
        prep.compressedLength = compressedLength;
    }

    /**
     * Writes one compressed chunk and advances the file state.
     *
     * Every field touched here is order-dependent -- chunkOffset, chunkCount, the offsets table, the
     * full-file checksum -- so this runs on one thread, in chunk order.
     *
     * @param chunkCrc the chunk CRC if it was computed elsewhere, or null to compute it here
     */
    protected void emitChunk(ChunkPrep prep, ByteBuffer src, Integer chunkCrc)
    {
        // resetAndTruncate leaves fchannel.position() past EOF after its verification reads + truncate;
        // re-seek so the next chunk lands at chunkOffset. No-op under linear writes.
        seekToChunkStart();

        uncompressedSize += prep.uncompressedLength;
        compressedSize += prep.compressedLength;

        // write an offset of the newly written chunk to the index file
        metadataWriter.addOffset(chunkOffset);
        chunkCount++;

        if (chunkCrc == null)
            writeChunk(prep.toWrite);
        else
            writeChunk(prep.toWrite, chunkCrc);

        lastFlushOffset = uncompressedSize;

        if (prep.toWrite == src)
            src.position(prep.uncompressedLength);

        // next chunk should be written right after current + length of the checksum (int)
        chunkOffset += prep.compressedLength + 4;
        if (runPostFlush != null)
            runPostFlush.accept(getLastFlushOffset());
    }

    /** As {@link #writeChunk(ByteBuffer)}, but with the chunk CRC already computed. */
    protected void writeChunk(ByteBuffer toWrite, int chunkCrc)
    {
        try
        {
            crcMetadata.appendPrecomputed(toWrite, chunkCrc);
            gatheringWrite(toWrite);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    private void gatheringWrite(ByteBuffer toWrite) throws IOException
    {
        chunkAndTrailer[0] = toWrite;
        chunkAndTrailer[1] = inlineTrailer.trailer();
        long remaining = (long) chunkAndTrailer[0].remaining() + chunkAndTrailer[1].remaining();
        while (remaining > 0)
            remaining -= fchannel.write(chunkAndTrailer);
    }

    protected void writeChunk(ByteBuffer toWrite)
    {
        try
        {
            // Checksum first. appendDirect reads a duplicate, so toWrite keeps its position, and the
            // chunk CRC lands in the trailer buffer rather than in a channel write of its own.
            crcMetadata.appendDirect(toWrite, true);
            gatheringWrite(toWrite);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public CompressionMetadata open(long overrideLength)
    {
        if (pipeline != null)
            pipeline.drain();
        if (overrideLength <= 0)
            overrideLength = uncompressedSize;
        return metadataWriter.open(overrideLength, chunkOffset);
    }

    @Override
    public DataPosition mark()
    {
        if (!buffer.hasRemaining())
            doFlush(0);
        // chunkOffset and chunkCount are advanced off this thread when a pipeline is running, so the
        // mark has to be taken against a quiesced writer. Without the drain it names an earlier chunk
        // than the file holds, and resetAndTruncate then takes its "mark lies in an earlier chunk"
        // branch, rebuilds the buffer from the wrong chunk and truncates live data away.
        if (pipeline != null)
            pipeline.drain();
        return new CompressedFileWriterMark(chunkOffset, current(), buffer.position(), chunkCount + 1);
    }

    @Override
    protected void syncInternal()
    {
        if (pipeline == null)
        {
            super.syncInternal();
            return;
        }
        doFlush(0);
        pipeline.drain();
        syncDataOnlyInternal();
    }

    @Override
    public synchronized void resetAndTruncate(DataPosition mark)
    {
        if (pipeline != null)
            pipeline.drain();
        assert mark instanceof CompressedFileWriterMark;

        CompressedFileWriterMark realMark = (CompressedFileWriterMark) mark;

        // reset position
        long truncateTarget = realMark.uncDataOffset;

        if (realMark.chunkOffset == chunkOffset)
        {
            // simply drop bytes to the right of our mark
            buffer.position(realMark.validBufferBytes);
            return;
        }

        // synchronize current buffer with disk - we don't want any data loss
        syncInternal();

        chunkOffset = realMark.chunkOffset;

        // compressed chunk size (- 4 bytes reserved for checksum)
        int chunkSize = (int) (metadataWriter.chunkOffsetBy(realMark.nextChunkIndex) - chunkOffset - 4);
        if (compressed.capacity() < chunkSize)
        {
            MemoryUtil.clean(compressed);
            compressed = compressor.preferredBufferType().allocate(chunkSize);
        }

        try
        {
            compressed.clear();
            compressed.limit(chunkSize);
            fchannel.position(chunkOffset);
            fchannel.read(compressed);

            try
            {
                // Repopulate buffer from compressed data
                buffer.clear();
                compressed.flip();
                if (chunkSize < maxCompressedLength)
                    compressor.uncompress(compressed, buffer);
                else
                    buffer.put(compressed);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunkOffset, chunkSize, e);
            }

            CRC32 checksum = new CRC32();
            compressed.rewind();
            checksum.update(compressed);

            crcCheckBuffer.clear();
            fchannel.read(crcCheckBuffer);
            crcCheckBuffer.flip();
            if (crcCheckBuffer.getInt() != (int) checksum.getValue())
                throw new CorruptBlockException(getPath(), chunkOffset, chunkSize);
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getPath());
        }
        catch (EOFException e)
        {
            throw new CorruptSSTableException(new CorruptBlockException(getPath(), chunkOffset, chunkSize), getPath());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }

        // Mark as dirty so we can guarantee the newly buffered bytes won't be lost on a rebuffer
        buffer.position(realMark.validBufferBytes);

        bufferOffset = truncateTarget - buffer.position();
        chunkCount = realMark.nextChunkIndex - 1;

        // truncate data and index file
        truncate(chunkOffset, bufferOffset);
        metadataWriter.resetAndTruncate(realMark.nextChunkIndex - 1);

        // The truncate rewound chunkOffset and lastFlushOffset; republish both so neither the size
        // estimate nor the early-open offset reports past the truncation point.
        if (pipeline != null)
            pipeline.republishOffsets(chunkOffset, getLastFlushOffset());
    }

    private void truncate(long toFileSize, long toBufferOffset)
    {
        try
        {
            fchannel.truncate(toFileSize);
            lastFlushOffset = toBufferOffset;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    protected void writeDigestFile()
    {
        digestFile.ifPresent(crcMetadata::writeFullChecksum);
    }

    /**
     * Seek to the offset where next compressed data chunk should be stored.
     * Subclasses may override if they manage their own channel.
     */
    protected void seekToChunkStart()
    {
        if (getOnDiskFilePointer() != chunkOffset)
        {
            try
            {
                fchannel.position(chunkOffset);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, getPath());
            }
        }
    }

    // Page management using chunk boundaries

    @Override
    public int maxBytesInPage()
    {
        return buffer.capacity();
    }

    @Override
    public void padToPageBoundary()
    {
        if (buffer.position() == 0)
            return;

        int padLength = bytesLeftInPage();

        // Flush as much as we have
        doFlush(0);
        // But pretend we had a whole chunk
        bufferOffset += padLength;
        lastFlushOffset += padLength;
    }

    @Override
    public int bytesLeftInPage()
    {
        return buffer.remaining();
    }

    @Override
    public long paddedPosition()
    {
        return position() + (buffer.position() == 0 ? 0 : buffer.remaining());
    }

    protected class TransactionalProxy extends SequentialWriter.TransactionalProxy
    {
        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            return super.doCommit(metadataWriter.commit(accumulate));
        }

        @Override
        protected Throwable doAbort(Throwable accumulate)
        {
            return super.doAbort(metadataWriter.abort(accumulate));
        }

        @Override
        protected void doPrepare()
        {
            syncInternal();
            writeDigestFile();
            sstableMetadataCollector.addCompressionRatio(compressedSize, uncompressedSize);
            metadataWriter.finalizeLength(current(), chunkCount).prepareToCommit();
        }

        @Override
        protected Throwable doPreCleanup(Throwable accumulate)
        {
            if (pipeline != null)
            {
                try
                {
                    pipeline.quiesce();
                }
                catch (Throwable t) { accumulate = merge(accumulate, t); }

                if (pipeline.stillRunning())
                {
                    // Free nothing. The thread may be inside compressor.compress writing into a
                    // buffer, and MemoryUtil.clean under native code faults the JVM rather than
                    // throwing. That includes the superclass's cleanup, which frees buffer, so it is
                    // not called either. Close the channel, which unblocks a stuck write and lets
                    // the abort finish; the buffers carry Cleaners and the collector reclaims them
                    // once the thread finally exits.
                    try { channel.close(); }
                    catch (Throwable t) { accumulate = merge(accumulate, t); }
                    return accumulate;
                }

                accumulate = pipeline.releaseBuffers(accumulate);
            }

            accumulate = super.doPreCleanup(accumulate);
            if (compressed != null)
            {
                try
                {
                    MemoryUtil.clean(compressed);
                }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
                compressed = null;
            }

            if (inlineTrailer != null)
            {
                try
                {
                    inlineTrailer.release();
                }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
            }

            return accumulate;
        }
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    /**
     * Parks the per-chunk CRC in a small direct buffer so {@link #writeChunk} can emit it in the same
     * gathering write as the chunk body. Pushing it through a channel-backed {@code DataOutputStream}
     * cost a second write syscall for every chunk.
     */
    private static final class InlineTrailerChecksumWriter extends ChecksumWriter
    {
        private ByteBuffer trailer = ByteBuffer.allocateDirect(4);

        @Override
        protected void writeIncrementalInt(int value)
        {
            trailer.clear();
            trailer.putInt(value);
            trailer.flip();
        }

        @Override
        public void writeChunkSize(int length)
        {
            throw new UnsupportedOperationException("writeChunkSize is unused on the compressed path");
        }

        ByteBuffer trailer()
        {
            return trailer;
        }

        void release()
        {
            if (trailer != null)
            {
                MemoryUtil.clean(trailer);
                trailer = null;
            }
        }
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class CompressedFileWriterMark implements DataPosition
    {
        // chunk offset in the compressed file
        final long chunkOffset;
        // uncompressed data offset (real data offset)
        final long uncDataOffset;

        final int validBufferBytes;
        final int nextChunkIndex;

        public CompressedFileWriterMark(long chunkOffset, long uncDataOffset, int validBufferBytes, int nextChunkIndex)
        {
            this.chunkOffset = chunkOffset;
            this.uncDataOffset = uncDataOffset;
            this.validBufferBytes = validBufferBytes;
            this.nextChunkIndex = nextChunkIndex;
        }
    }
}
