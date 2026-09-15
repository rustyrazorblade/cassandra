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

import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * M3.0 (CASSANDRA-20428, Phase 4 scaffolding): self-tests for
 * {@link ResponseSerializationSupport}, run over a merged multi-source workload with tombstones,
 * a range tombstone and collections so the serialized grammar exercises deletions, markers,
 * complex columns and header-delta encoding — not just plain rows. Establishes, with the
 * identity candidate (today's materialize-then-serialize path), that every comparison layer the
 * M3.3a transcode-sink verification needs is in place and NON-VACUOUS:
 * byte-compare against the real {@code LocalDataResponse.build} route, unit-oracle assembly,
 * round-trip through the production deserializer, and corruption detection in both the byte and
 * record dimensions.
 */
public class ResponseSerializationSupportTest extends CursorReadDifferentialTester
{
    private static final int ROWS = 96;

    @Test
    public void harnessPayloadIsTheProductionBuildOutput() throws Throwable
    {
        Workload w = load();
        // the payload every harness comparison uses must BE the LocalDataResponse.build output,
        // proven via the real createResponse route rather than asserted by comment
        byte[] viaProduction = ResponseSerializationSupport.extractDataPayload(
            ResponseSerializationSupport.productionResponseBytes(w.command.get()));
        byte[] viaHarness = ResponseSerializationSupport.intraNodePayload(w.command.get());
        assertArrayEquals("harness payload diverges from the production LocalDataResponse.build route",
                          viaProduction, viaHarness);
        assertTrue("empty payloads would make every comparison vacuous", viaHarness.length > 0);
    }

    @Test
    public void unitOracleAssemblesIntoThePayload() throws Throwable
    {
        Workload w = load();
        // M3.3a layer 1: per-partition serialize-the-materialized-output bytes, composed with the
        // payload framing, must reproduce the full payload byte-for-byte
        List<byte[]> oracle = ResponseSerializationSupport.materializedPartitionOracle(w.command.get());
        assertEquals("single-partition command must yield one oracle partition", 1, oracle.size());
        assertResponseBytesEqual(ResponseSerializationSupport.intraNodePayload(w.command.get()),
                                 ResponseSerializationSupport.assemblePayload(oracle));
    }

    @Test
    public void roundTripThroughProductionDeserializer() throws Throwable
    {
        Workload w = load();
        // M3.3a layer 3: payload bytes must be readable by the REAL response consumer and decode
        // to exactly the records executeLocally produces
        byte[] payload = ResponseSerializationSupport.intraNodePayload(w.command.get());
        List<String> decoded = ResponseSerializationSupport.roundTripRecords(w.command.get(), payload);
        List<String> direct = canonicalRecords(w.command.get());
        assertFalse("round-trip produced no records — workload shape drifted", direct.isEmpty());
        compareRecords(direct, decoded);
    }

    @Test
    public void byteCorruptionIsDetected() throws Throwable
    {
        Workload w = load();
        byte[] payload = ResponseSerializationSupport.intraNodePayload(w.command.get());
        byte[] corrupted = payload.clone();
        corrupted[corrupted.length / 2] ^= 0x40; // flip one bit mid-payload
        try
        {
            assertResponseBytesEqual(payload, corrupted);
        }
        catch (AssertionError expected)
        {
            return;
        }
        fail("byte comparison accepted a corrupted payload — the M3.3a oracle would be vacuous");
    }

    @Test
    public void wrongContentIsDetectedByRoundTrip() throws Throwable
    {
        Workload w = load();
        // a candidate producing VALID bytes of the WRONG content (here: a LIMIT 5 subset) must be
        // caught by the record-level round-trip comparison
        SinglePartitionReadCommand limited =
            (SinglePartitionReadCommand) Util.cmd(w.cfs, 0L).withNowInSeconds(w.nowInSec).withLimit(5).build();
        byte[] wrongPayload = ResponseSerializationSupport.intraNodePayload(limited);
        List<String> decoded = ResponseSerializationSupport.roundTripRecords(w.command.get(), wrongPayload);
        try
        {
            compareRecords(canonicalRecords(w.command.get()), decoded);
        }
        catch (AssertionError expected)
        {
            return;
        }
        fail("record comparison accepted a wrong-content payload — the round-trip layer is vacuous");
    }

    @Test
    public void harnessWorksOverTheCursorPath() throws Throwable
    {
        Workload w = load();
        // the side M3.3 will actually compare: same harness, cursor path enabled, with the
        // standard silent-fallback guards; payload must match the iterator path's byte-for-byte
        DatabaseDescriptor.setCursorReadsEnabled(false);
        byte[] iteratorPayload = ResponseSerializationSupport.intraNodePayload(w.command.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = w.command.get();
            assertTrue("workload is not supported by the cursor read gate; this check would be vacuous",
                       CursorReads.isReadSupported(probe, w.cfs, liveSSTablesFor(w.cfs, probe)));
            long servedBefore = CursorReads.sstableLegsServed();
            byte[] cursorPayload = ResponseSerializationSupport.intraNodePayload(w.command.get());
            assertTrue("cursor path did not serve any sstable leg (silent fallback?)",
                       CursorReads.sstableLegsServed() - servedBefore > 0);
            assertResponseBytesEqual(iteratorPayload, cursorPayload);

            // and the production-route wrapper works over the cursor path too
            assertArrayEquals(iteratorPayload,
                              ResponseSerializationSupport.extractDataPayload(
                                  ResponseSerializationSupport.productionResponseBytes(w.command.get())));
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    // ---------------------------------------------------------------- workload

    private final class Workload
    {
        final ColumnFamilyStore cfs;
        final long nowInSec;
        final Supplier<SinglePartitionReadCommand> command;

        Workload(ColumnFamilyStore cfs, long nowInSec)
        {
            this.cfs = cfs;
            this.nowInSec = nowInSec;
            this.command = () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(nowInSec).build();
        }
    }

    /** Two overlapping sstables over one partition: full base round, then an overwrite round with
     *  a row tombstone (ck 3), a cell tombstone (v2 at ck 5), a range tombstone [40,48) and set
     *  updates on even rows — deletions, markers and complex columns all present in the payload. */
    private Workload load() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, tags set<text>, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck * 10, "r0-" + ck);
        flush();
        for (long ck = 0; ck < ROWS; ck += 2)
        {
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck * 100, "r1-" + ck);
            execute("UPDATE %s SET tags = tags + ? WHERE pk = 0 AND ck = ?", set("common", "t" + ck), ck);
        }
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 3");
        execute("DELETE v2 FROM %s WHERE pk = 0 AND ck = 5");
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 40 AND ck < 48");
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        assertEquals(ROWS - 1 - 8, execute("SELECT ck FROM %s WHERE pk = 0").size());
        return new Workload(cfs, FBUtilities.nowInSeconds());
    }
}
