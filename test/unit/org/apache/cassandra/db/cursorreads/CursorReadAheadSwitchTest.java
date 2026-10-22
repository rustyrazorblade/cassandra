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

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * A long cursor read of one partition moves from the chunk cache to the read-ahead reader only
 * while its chunks miss the chunk cache.  The chunk cache requests of the read show which reader
 * it used: one per chunk on the chunk cache, none once it has moved.
 */
public class CursorReadAheadSwitchTest extends CQLTester
{
    private static final int ROWS = 2048;
    private static final int PAYLOAD_BYTES = 1024;
    private static final int CHUNK_KIB = 16;
    /** The chunks of the partition's data: about ROWS * PAYLOAD_BYTES / chunk length. */
    private static final int CHUNKS = ROWS * PAYLOAD_BYTES / (CHUNK_KIB * 1024);

    private boolean cursorReadsWas;

    @Before
    public void enableCursorReads()
    {
        assumeTrue("the chunk cache is enabled in the test configuration", ChunkCache.instance != null);
        cursorReadsWas = DatabaseDescriptor.cursorReadsEnabled();
        DatabaseDescriptor.setCursorReadsEnabled(true);
    }

    @After
    public void restoreCursorReads()
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursorReadsWas);
    }

    @Test
    public void coldLongReadMovesToReadAhead() throws Throwable
    {
        writePartition();
        ChunkCache.instance.close();

        long requests = chunkCacheRequestsOf(this::readPartition);

        // the read-ahead buffer is 256 KiB, so the read leaves the chunk cache after about 16 chunks
        assertTrue("a cold read used the chunk cache for " + requests + " of " + CHUNKS + " chunks",
                   requests < CHUNKS / 2);
    }

    @Test
    public void warmLongReadStaysOnChunkCache() throws Throwable
    {
        writePartition();
        ChunkCache.instance.close();
        // the iterator path reads every chunk through the chunk cache
        DatabaseDescriptor.setCursorReadsEnabled(false);
        readPartition();
        DatabaseDescriptor.setCursorReadsEnabled(true);

        long requests = chunkCacheRequestsOf(this::readPartition);

        assertTrue("a warm read used the chunk cache for only " + requests + " of " + CHUNKS + " chunks",
                   requests >= CHUNKS - 2);
    }

    private void writePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v blob, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '" + CHUNK_KIB + "'}");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        Random random = new Random(42);
        byte[] payload = new byte[PAYLOAD_BYTES];
        for (int ck = 0; ck < ROWS; ck++)
        {
            random.nextBytes(payload);
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, ByteBuffer.wrap(payload.clone()));
        }
        flush();
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    private void readPartition() throws Throwable
    {
        UntypedResultSet rows = execute("SELECT * FROM %s WHERE pk = 0");
        assertEquals(ROWS, rows.size());
    }

    private static long chunkCacheRequestsOf(ThrowingRunnable read) throws Throwable
    {
        long before = ChunkCache.instance.metrics.requests.getCount();
        read.run();
        return ChunkCache.instance.metrics.requests.getCount() - before;
    }

    private interface ThrowingRunnable
    {
        void run() throws Throwable;
    }
}
