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

package org.apache.cassandra.db.commitlog;

import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A segment header written and read back is the header that was written.
 *
 * The header carries the version, the segment id, the compression class and its parameters, and the
 * encryption context. Everything the reader does afterwards depends on getting them right, and a header
 * that round trips wrong makes a whole segment unreadable rather than one entry.
 *
 * CommitLogDescriptorTest covers this with fixed examples. This adds generated ones: arbitrary ids
 * including the negative and extreme values a long can hold, generated parameter maps, and every
 * compressor, so a change to the encoding has to survive more than the handful of shapes someone thought
 * of.
 *
 * What this test cannot see: whether the header the writer emits matches what an older version's reader
 * expects. That is a cross-version question and needs the fixture data CommitLogUpgradeTest carries.
 */
public class CommitLogDescriptorPropertyTest
{
    private static final int EXAMPLES = CassandraRelevantProperties.TEST_COMMITLOG_EXAMPLES.getInt();

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void headerRoundTrips() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            Random workload = new Random(seed);

            CommitLogDescriptor written = new CommitLogDescriptor(CommitLogDescriptor.current_version,
                                                                  workload.nextLong(),
                                                                  compression(workload),
                                                                  encryption(workload));
            Map<String, String> additional = additionalHeaders(workload);

            ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
            CommitLogDescriptor.writeHeader(buffer, written, additional);
            buffer.flip();

            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            CommitLogDescriptor read = CommitLogDescriptor.readHeader(new DataInputStream(new ByteArrayInputStream(bytes)),
                                                                     written.getEncryptionContext());

            assertEquals("the header did not round trip", written, read);
            assertEquals("the segment id did not round trip", written.id, read.id);
            assertEquals("the version did not round trip", written.version, read.version);
        });
    }

    /**
     * The id is a long, so the extremes are the cases most likely to break a parser. Drawn deliberately
     * rather than left to a generator that will never pick them.
     */
    @Test
    public void extremeSegmentIdsRoundTripThroughTheHeader() throws Throwable
    {
        for (long id : new long[]{ 0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE + 1 })
            assertEquals("id " + id + " did not round trip through the header", id, readBack(id).id);
    }

    /**
     * The file name encodes the id in decimal, and a negative one produces a name with two adjacent
     * dashes that idFromFileName rejects: CommitLog-9--1.log. Segment ids come from a counter seeded off
     * the clock and never go negative, so this is a limit of the naming rather than a defect, but it is
     * a limit worth writing down: anything that ever hands the segment manager an id of its own choosing
     * has to keep it non-negative.
     */
    @Test
    public void nonNegativeSegmentIdsRoundTripThroughTheFileName() throws Throwable
    {
        for (long id : new long[]{ 0L, 1L, 42L, Long.MAX_VALUE })
        {
            CommitLogDescriptor written = new CommitLogDescriptor(CommitLogDescriptor.current_version, id, null,
                                                                  EncryptionContextGenerator.createDisabledContext());
            assertEquals("id " + id + " did not round trip through the file name",
                         id, CommitLogDescriptor.idFromFileName(written.fileName()));
        }
    }

    private static CommitLogDescriptor readBack(long id) throws Exception
    {
        CommitLogDescriptor written = new CommitLogDescriptor(CommitLogDescriptor.current_version, id, null,
                                                              EncryptionContextGenerator.createDisabledContext());
        ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
        CommitLogDescriptor.writeHeader(buffer, written, Collections.emptyMap());
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return CommitLogDescriptor.readHeader(new DataInputStream(new ByteArrayInputStream(bytes)),
                                              written.getEncryptionContext());
    }

    private static ParameterizedClass compression(Random workload)
    {
        switch (workload.nextInt(5))
        {
            case 0: return null;
            case 1: return new ParameterizedClass(LZ4Compressor.class.getName(), parameters(workload));
            case 2: return new ParameterizedClass(DeflateCompressor.class.getName(), parameters(workload));
            case 3: return new ParameterizedClass(SnappyCompressor.class.getName(), parameters(workload));
            default: return new ParameterizedClass(ZstdCompressor.class.getName(), parameters(workload));
        }
    }

    private static Map<String, String> parameters(Random workload)
    {
        Map<String, String> parameters = new HashMap<>();
        for (int i = 0, n = workload.nextInt(3); i < n; i++)
            parameters.put("p" + i, Long.toString(workload.nextLong()));
        return parameters;
    }

    private static EncryptionContext encryption(Random workload) throws Exception
    {
        return workload.nextBoolean() ? EncryptionContextGenerator.createDisabledContext()
                                      : EncryptionContextGenerator.createContext(true);
    }

    private static Map<String, String> additionalHeaders(Random workload)
    {
        Map<String, String> headers = new HashMap<>();
        for (int i = 0, n = workload.nextInt(3); i < n; i++)
            headers.put("h" + i, Long.toString(workload.nextLong()));
        return headers;
    }
}
