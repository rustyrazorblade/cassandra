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

import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * M3.3b-i (CASSANDRA-20428): routing differential for {@code ReadCommand.createResponseLocally} /
 * {@code SinglePartitionReadCommand.queryStorageToResponseBytes}, driven through the ACTUAL
 * production entry point ({@link ReadCommandVerbHandler#doRead}) rather than the lower
 * {@code CursorReads.mergeLegsWithSink} seam M3.3a's own harness used. Every scenario runs the SAME
 * command twice — {@code cursor_reads_enabled} off then on — and byte-compares the FULL
 * {@code ReadResponse.serializer} output (not just the per-partition body M3.3a compared), plus
 * asserts {@link CursorReads#transcodeResponsesServed()} advanced (or did not) to prove the
 * transcode path really engaged (or really declined) rather than the comparison passing vacuously.
 * Because gate-off is exactly today's untouched {@code executeLocally}+{@code createResponse}
 * behavior (see {@code ReadCommand.createResponseLocally}'s own javadoc), a byte match against it
 * doubles as the "fallback never blocks or corrupts the query" proof for every declining scenario.
 */
public class TranscodeRoutingDifferentialTest extends CursorReadDifferentialTester
{
    private static byte[] fullResponseBytes(SinglePartitionReadCommand command, boolean trackRepairedData) throws Exception
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, trackRepairedData);
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
    }

    private void assertEngagesTranscodePath(Supplier<SinglePartitionReadCommand> command) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        byte[] off = fullResponseBytes(command.get(), false);

        DatabaseDescriptor.setCursorReadsEnabled(true);
        long before = CursorReads.transcodeResponsesServed();
        byte[] on = fullResponseBytes(command.get(), false);
        long after = CursorReads.transcodeResponsesServed();

        assertResponseBytesEqual(off, on);
        assertEquals("expected the transcode path to engage exactly once", before + 1, after);
    }

    private void assertDeclinesTranscodePath(Supplier<SinglePartitionReadCommand> command) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        byte[] off = fullResponseBytes(command.get(), false);

        DatabaseDescriptor.setCursorReadsEnabled(true);
        long before = CursorReads.transcodeResponsesServed();
        byte[] on = fullResponseBytes(command.get(), false);
        long after = CursorReads.transcodeResponsesServed();

        // the "fallback never blocks the query" property: even declining, the gate-on response is
        // still correct -- byte-identical to the untouched gate-off oracle.
        assertResponseBytesEqual(off, on);
        assertEquals("expected the transcode path to decline (fall back to the base-class default)", before, after);
    }

    private void setUpTwoLegTable()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        for (long ck = 20; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
    }

    // ---------------------------------------------------------------- the eligible shape

    @Test
    public void plainEligibleShapeEngagesTranscodePath() throws Throwable
    {
        setUpTwoLegTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertEngagesTranscodePath(() -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build());
    }

    // ---------------------------------------------------------------- declining scenarios

    @Test
    public void digestQueryDeclines() throws Throwable
    {
        setUpTwoLegTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertDeclinesTranscodePath(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).build().copyAsDigestQuery(ReplicaUtils.full(FBUtilities.getBroadcastAddressAndPort())));
    }

    @Test
    public void limitQueryDeclines() throws Throwable
    {
        setUpTwoLegTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertDeclinesTranscodePath(() -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withLimit(5).build());
    }

    @Test
    public void filteredQueryWithPushableFilterDeclines() throws Throwable
    {
        setUpTwoLegTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertDeclinesTranscodePath(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).filterOn("v1", Operator.EQ, 7L).build());
    }

    @Test
    public void singleLegReadDeclines() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        assertEquals(1, cfs.getLiveSSTables().size());

        assertDeclinesTranscodePath(() -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build());
    }

    @Test
    public void secondaryIndexedTableDeclines() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s (v1)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        for (long ck = 20; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertDeclinesTranscodePath(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).filterOn("v1", Operator.EQ, 7L).build());
    }

    // ---------------------------------------------------------------- exception parity

    /** Tombstone-overwhelming abort: the SAME exception type must be thrown from both the gate-off
     *  (real {@code MetricRecording}) and gate-on ({@link CursorReads.TombstoneScanGuard}) paths —
     *  the one exception-parity dimension M3.3a never needed (it never threw from inside a sink in
     *  a way that reached a call site). */
    @Test
    public void tombstoneThresholdAbortExceptionParity() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        // a second leg made entirely of row tombstones -- >= 2 legs, and enough tombstones to
        // cross a deliberately tiny failure threshold
        for (long ck = 0; ck < 10; ck++)
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        int originalThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneFailureThreshold(2);
        try
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
            Throwable offFailure = null;
            try
            {
                fullResponseBytes((SinglePartitionReadCommand) Util.cmd(cfs, 1L).build(), false);
                fail("expected a TombstoneOverwhelmingException from the gate-off (iterator) path");
            }
            catch (TombstoneOverwhelmingException expected)
            {
                offFailure = expected;
            }

            DatabaseDescriptor.setCursorReadsEnabled(true);
            long before = CursorReads.transcodeResponsesServed();
            Throwable onFailure = null;
            try
            {
                fullResponseBytes((SinglePartitionReadCommand) Util.cmd(cfs, 1L).build(), false);
                fail("expected a TombstoneOverwhelmingException from the gate-on (transcode) path");
            }
            catch (TombstoneOverwhelmingException expected)
            {
                onFailure = expected;
            }
            long after = CursorReads.transcodeResponsesServed();

            assertEquals("an aborted read must not count as a served transcode response", before, after);
            assertEquals(offFailure.getClass(), onFailure.getClass());
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalThreshold);
        }
    }
}
