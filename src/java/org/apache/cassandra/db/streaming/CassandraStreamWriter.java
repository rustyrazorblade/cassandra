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
package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumValidator;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.streaming.StreamReadAhead;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.async.StreamCompressionSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPools;

/**
 * CassandraStreamWriter writes given section of the SSTable to given channel.
 */
public class CassandraStreamWriter
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraStreamWriter.class);

    protected final SSTableReader sstable;
    private final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    protected final Collection<SSTableReader.PartitionPositionBounds> sections;
    protected final StreamRateLimiter limiter;
    protected final StreamSession session;
    private final long totalSize;

    public CassandraStreamWriter(SSTableReader sstable, CassandraStreamHeader header, StreamSession session)
    {
        this.session = session;
        this.sstable = sstable;
        this.sections = header.sections;
        this.limiter =  StreamManager.getRateLimiter(session.peer);
        this.totalSize = header.size();
    }

    /**
     * Stream file of specified sections to given channel.
     *
     * @param out where this writes data to
     * @throws IOException on any I/O error
     */
    public void write(StreamingDataOutputPlus out) throws IOException
    {
        long totalSize = totalSize();
        logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                     sstable.getFilename(), session.peer, sstable.getSSTableMetadata().repairedAt, totalSize);

        try(ChannelProxy proxy = sstable.getDataChannel().newChannel();
            ChecksumValidator validator = sstable.maybeGetChecksumValidator())
        {
            int bufferSize = validator == null ? DatabaseDescriptor.getStreamChunkSizeInBytes() : validator.chunkSize;
            String filename = sstable.descriptor.fileFor(Components.DATA).toString();
            long progress = 0L;

            // Read, validate and compress on the read-ahead thread. Otherwise each chunk waits on the disk
            // after the send window frees up.
            try (StreamReadAhead ahead = StreamReadAhead.start(session.getChannel().readAheadExecutor(),
                                                               StreamReadAhead.depthFor(bufferSize),
                                                               sink -> read(proxy, validator, bufferSize, sink)))
            {
                StreamReadAhead.Chunk chunk;
                while ((chunk = ahead.take()) != null)
                {
                    out.writeToChannel(chunk.buffer, limiter);
                    progress += chunk.progress;
                    session.progress(filename, ProgressInfo.Direction.OUT, progress, chunk.progress, totalSize);
                }
            }

            // One flush, after every section. A flush per section blocks the sender until the channel
            // drains, which costs a round trip at each section boundary.
            out.flush();
            logger.debug("[Stream #{}] Finished streaming file {} to {}, bytesTransferred = {}, totalSize = {}",
                         session.planId(), sstable.getFilename(), session.peer, FBUtilities.prettyPrintMemory(progress), FBUtilities.prettyPrintMemory(totalSize));
        }
    }

    protected long totalSize()
    {
        return totalSize;
    }

    /** Reads every section in order on the read-ahead thread, blocking in {@code sink} once it is far enough ahead. */
    private void read(ChannelProxy proxy, ChecksumValidator validator, int bufferSize, StreamReadAhead.Sink sink)
    throws IOException, InterruptedException
    {
        long fileSize = proxy.size();
        for (SSTableReader.PartitionPositionBounds section : sections)
        {
            long start = validator == null ? section.lowerPosition : validator.chunkStart(section.lowerPosition);
            // bytes to skip when the transfer does not start on a validator chunk boundary
            int transferOffset = (int) (section.lowerPosition - start);
            if (validator != null)
                validator.seek(start);

            long length = section.upperPosition - start;
            long bytesRead = 0;
            while (bytesRead < length)
            {
                int toTransfer = (int) Math.min(bufferSize, length - bytesRead);
                sink.accept(read(proxy, validator, fileSize, start, transferOffset, toTransfer, bufferSize));
                start += toTransfer;
                bytesRead += toTransfer;
                transferOffset = 0;
            }
        }
    }

    /**
     * Read one chunk off disk, verify it and compress it into the buffer that goes on the wire.
     *
     * @param proxy The file reader to read from
     * @param validator validator to verify data integrity
     * @param fileSize The size of the {@code proxy} file, read once for the transfer.
     * @param start The read offset from the beginning of the {@code proxy} file.
     * @param transferOffset number of bytes to skip transfer, but include for validation.
     * @param toTransfer The number of bytes to be transferred.
     *
     * @return The chunk to send, and the transfer progress it represents. The caller owns the chunk buffer,
     *         which comes from the networking buffer pool and must be returned to it.
     *
     * @throws java.io.IOException on any I/O error
     */
    protected StreamReadAhead.Chunk read(ChannelProxy proxy, ChecksumValidator validator, long fileSize, long start, int transferOffset, int toTransfer, int bufferSize) throws IOException
    {
        // the count of bytes to read off disk
        int minReadable = (int) Math.min(bufferSize, fileSize - start);

        // holds the data read from disk; the compressed copy is what goes on the wire
        ByteBuffer buffer = BufferPools.forNetworking().get(minReadable, BufferType.OFF_HEAP);
        ByteBuffer[] out = new ByteBuffer[1];
        try
        {
            int readCount = proxy.read(buffer, start);
            assert readCount == minReadable : String.format("could not read required number of bytes from file to be streamed: read %d bytes, wanted %d bytes", readCount, minReadable);
            buffer.flip();

            if (validator != null)
            {
                validator.validate(buffer);
                buffer.flip();
            }

            buffer.position(transferOffset);
            buffer.limit(transferOffset + (toTransfer - transferOffset));
            ByteBuffer compressed = StreamCompressionSerializer.compress(compressor, buffer,
                                                                        size -> out[0] = BufferPools.forNetworking().get(size, BufferType.OFF_HEAP));
            return new StreamReadAhead.Chunk(compressed, toTransfer - transferOffset);
        }
        catch (Throwable t)
        {
            if (out[0] != null)
                BufferPools.forNetworking().put(out[0]);
            throw t;
        }
        finally
        {
            BufferPools.forNetworking().put(buffer);
        }
    }
}
