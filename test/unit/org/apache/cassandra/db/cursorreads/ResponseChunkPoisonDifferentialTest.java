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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ResponseSink;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

/**
 * The replica response corpus of {@link CursorReadReplicaResponseDifferentialTest}, with every new
 * response chunk filled with a poison byte instead of left as the JVM gives it, plus responses that
 * span many chunks.  Uninitialized arrays mostly hold zeros in tests, so without the poison the
 * byte comparison could not see a byte the response never wrote.  Each cursor response must equal
 * the iterator's and hold no run of the poison.  The large responses also run with the zeroed
 * chunks the code uses when the JVM does not export the uninitialized allocation.
 */
public class ResponseChunkPoisonDifferentialTest extends CursorReadReplicaResponseDifferentialTest
{
    private static final byte POISON = (byte) 0xA5;
    private static final int LARGE_ROWS = 20_000;

    @Before
    public void poisonNewChunks()
    {
        assertTrue("the test JVM exports jdk.internal.misc, so new chunks must skip zeroing",
                   ResponseSink.uninitializedChunksAvailable());
        ResponseSink.TEST_POISON_NEW_CHUNKS = POISON;
        rejectedResponseByte = POISON;
    }

    @After
    public void stopPoisoning()
    {
        ResponseSink.TEST_POISON_NEW_CHUNKS = 0;
        ResponseSink.TEST_ZEROED_CHUNKS_ONLY = false;
        rejectedResponseByte = null;
    }

    @Test
    public void largeResponses() throws Throwable
    {
        runLargeResponses("uninitialized chunks");
    }

    @Test
    public void largeResponsesWithZeroedChunks() throws Throwable
    {
        ResponseSink.TEST_ZEROED_CHUNKS_ONLY = true;
        runLargeResponses("zeroed chunks");
    }

    /**
     * Responses of many chunks whose last chunk is partly written, with rows a filter or a limit
     * abandons after they were written in place, rows missing a column, and partitions that purge
     * to nothing; over one sstable, two, and two plus a memtable.
     */
    private void runLargeResponses(String label) throws Throwable
    {
        for (String format : new String[]{ "bti", "big" })
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            String padding = "x".repeat(100);
            for (long ck = 0; ck < LARGE_ROWS; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1L, ck, ck, padding + ck);
            // rows missing a column, and a partition that purges to nothing
            for (long ck = 0; ck < LARGE_ROWS; ck += 7)
                execute("DELETE v1 FROM %s USING TIMESTAMP 1500 WHERE pk = ? AND ck = ?", 1L, ck);
            execute("INSERT INTO %s (pk, ck, v1) VALUES (2, 1, 1) USING TIMESTAMP 1000");
            execute("DELETE FROM %s USING TIMESTAMP 1500 WHERE pk = 2");
            flush();
            runLargeShapes(label + " " + format + " one sstable", cfs);

            for (long ck = 3; ck < LARGE_ROWS; ck += 5)
                execute("UPDATE %s USING TIMESTAMP 2000 SET v2 = ? WHERE pk = ? AND ck = ?", "second-" + ck, 1L, ck);
            flush();
            runLargeShapes(label + " " + format + " two sstables", cfs);

            for (long ck = 1; ck < LARGE_ROWS; ck += 11)
                execute("UPDATE %s USING TIMESTAMP 3000 SET v1 = ? WHERE pk = ? AND ck = ?", -ck, 1L, ck);
            runLargeShapes(label + " " + format + " two sstables and memtable", cfs);
        }
    }

    private void runLargeShapes(String label, ColumnFamilyStore cfs)
    {
        long now = FBUtilities.nowInSeconds() + 10;
        long seed = label.hashCode();
        assertAllReadSurfacesMatch(ReadCase.of(label + " / full", seed++).at(now), cfs,
                                   n -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(n).build());
        assertAllReadSurfacesMatch(ReadCase.of(label + " / limit 7777", seed++).at(now), cfs,
                                   n -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(n).withLimit(7777).build());
        assertAllReadSurfacesMatch(ReadCase.of(label + " / v1 > 10000", seed++).at(now), cfs,
                                   n -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(n)
                                                                         .filterOn("v1", Operator.GT, 10_000L).build());
        assertAllReadSurfacesMatch(ReadCase.of(label + " / v1 > 10000 limit 555", seed++).at(now), cfs,
                                   n -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(n)
                                                                         .filterOn("v1", Operator.GT, 10_000L).withLimit(555).build());
        assertAllReadSurfacesMatch(ReadCase.of(label + " / reversed", seed++).at(now), cfs,
                                   n -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withNowInSeconds(n).reverse().build());
        assertAllReadSurfacesMatch(ReadCase.of(label + " / purged partition", seed++).at(now), cfs,
                                   n -> (SinglePartitionReadCommand) Util.cmd(cfs, 2L).withNowInSeconds(n).build());
    }
}
