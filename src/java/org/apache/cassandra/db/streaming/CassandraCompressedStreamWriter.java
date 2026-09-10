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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.StreamingDataOutputPlus.Section;
import org.apache.cassandra.streaming.StreamingFileSource;
import org.apache.cassandra.utils.FBUtilities;

/**
 * CassandraStreamWriter for compressed SSTable.
 */
public class CassandraCompressedStreamWriter extends CassandraStreamWriter
{
    private static final int CRC_LENGTH = 4;

    private static final Logger logger = LoggerFactory.getLogger(CassandraCompressedStreamWriter.class);

    private final CompressionInfo compressionInfo;

    public CassandraCompressedStreamWriter(SSTableReader sstable, CassandraStreamHeader header, StreamSession session)
    {
        super(sstable, header, session);
        this.compressionInfo = header.compressionInfo;
    }

    @Override
    public void write(StreamingDataOutputPlus out) throws IOException
    {
        logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                     sstable.getFilename(), session.peer, sstable.getSSTableMetadata().repairedAt, totalSize);

        // contiguous chunks go out together, which reduces reads from disk and writes to the network
        List<Section> sections = fuseAdjacentChunks(compressionInfo.chunks());

        // the compressed chunks already have the form they go out in, so this writer sends file ranges
        // instead of reading and transforming them; only an encrypted channel reads them into the process
        File dataFile = sstable.descriptor.fileFor(Components.DATA);
        String filename = dataFile.toString();
        long[] progress = new long[1];
        StreamingFileSource source = StreamingFileSource.open(dataFile, DatabaseDescriptor.getStreamDiskAccessMode());
        long bytesTransferred = out.writeFileToChannel(source, limiter, sections, bytes -> {
            progress[0] += bytes;
            session.progress(filename, ProgressInfo.Direction.OUT, progress[0], bytes, totalSize);
        }, session.getChannel().readAheadExecutor());

        logger.debug("[Stream #{}] Finished streaming file {} to {}, bytesTransferred = {}, totalSize = {}",
                     session.planId(), sstable.getFilename(), session.peer,
                     FBUtilities.prettyPrintMemory(bytesTransferred), FBUtilities.prettyPrintMemory(totalSize));
    }

    // chunks are assumed to be sorted by offset; each section contains 1..n adjacent compressed chunks
    @VisibleForTesting
    List<Section> fuseAdjacentChunks(CompressionMetadata.Chunk[] chunks)
    {
        if (chunks.length == 0)
            return Collections.emptyList();

        long start = chunks[0].offset;
        long end = start + chunks[0].length + CRC_LENGTH;

        List<Section> sections = new ArrayList<>();

        for (int i = 1; i < chunks.length; i++)
        {
            CompressionMetadata.Chunk chunk = chunks[i];

            if (chunk.offset == end)
            {
                end += (chunk.length + CRC_LENGTH);
            }
            else
            {
                sections.add(new Section(start, end));

                start = chunk.offset;
                end = start + chunk.length + CRC_LENGTH;
            }
        }
        sections.add(new Section(start, end));

        return sections;
    }

}
