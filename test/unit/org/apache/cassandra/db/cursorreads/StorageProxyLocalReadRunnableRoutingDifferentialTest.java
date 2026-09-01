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
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.transport.Dispatcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * M3.3b-ii (CASSANDRA-20428): routing differential for the SECOND replica-serving call site,
 * {@code StorageProxy.LocalReadRunnable}, which M3.3b-i deliberately left wired to the old inline
 * {@code executeLocally}+{@code createResponse} pair. This increment replaced that pair with a call
 * to the same {@code ReadCommand.createResponseLocally(controller)} entry point
 * {@link ReadCommandVerbHandler#doRead} already uses (M3.3b-i, commit {@code 6985055665}) -- per the
 * M3.3b plan's own §3, both call sites share one implementation of the gate/fallback logic, so this
 * harness exists to prove the harness-observable claim, not to re-verify the gate itself (already
 * covered by {@link TranscodeRoutingDifferentialTest}/{@link BtiTranscodeRoutingDifferentialTest}).
 *
 * <p>Three things this class proves that the sibling {@code doRead} tests cannot, by construction:
 * <ol>
 *   <li>{@code LocalReadRunnable} itself, driven through its REAL {@code run()} method (not just the
 *       {@code createResponseLocally} method it calls), byte-matches its own gate-off oracle for both
 *       an eligible and a declining shape;</li>
 *   <li>the SAME command shape, driven through BOTH real entry points, produces byte-identical
 *       {@code ReadResponse} wire encodings -- the specific cross-call-site equivalence the M3.3b plan
 *       calls for, not just each site independently matching its own oracle;</li>
 *   <li>{@code LocalReadRunnable}'s exception handling for a tombstone-threshold abort genuinely
 *       differs in SHAPE from {@code ReadCommandVerbHandler.doRead}'s (it never rethrows
 *       {@code TombstoneOverwhelmingException} out of {@code run()}; it translates it into
 *       {@code handler.onFailure(..., RequestFailure.READ_TOO_MANY_TOMBSTONES)} instead, via its own
 *       outer catch block) -- and that this call site's own translated outcome is identical whether
 *       the gate is on or off.
 * </ol>
 */
public class StorageProxyLocalReadRunnableRoutingDifferentialTest extends CursorReadDifferentialTester
{
    /**
     * Captures whatever {@link StorageProxy.LocalReadRunnable} hands its {@link ReadCallback}
     * without exercising any of {@code ReadCallback}'s own resolver/replica-plan machinery.
     * {@code LocalReadRunnable} only ever calls {@code handler.response(...)} or
     * {@code handler.onFailure(...)} (verified directly against {@code StorageProxy.java}'s current
     * source) -- both overridden here to just record what happened, so the resolver and replica plan
     * passed to the superclass constructor are never dereferenced and can safely be {@code null}. The
     * one place the superclass constructor itself would touch them -- an {@code assert} comparing
     * {@code replicaPlan().readQuorum()} against contacts, guarded by
     * {@code !(command instanceof PartitionRangeReadCommand)} -- short-circuits away for every
     * {@link SinglePartitionReadCommand} this harness drives, so it never evaluates.
     */
    private static final class CapturingReadCallback extends ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead>
    {
        volatile ReadResponse response;
        volatile RequestFailure failureReason;

        CapturingReadCallback(ReadCommand command, Dispatcher.RequestTime requestTime)
        {
            super(null, command, null, requestTime);
        }

        @Override
        public void response(ReadResponse result)
        {
            this.response = result;
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailure failure)
        {
            this.failureReason = failure;
        }
    }

    /**
     * Drives the REAL {@code StorageProxy.LocalReadRunnable.run()} synchronously. Per the M3.3b
     * plan's own §5: {@code run()} is {@code DroppableRunnable.run()}, {@code public final}, and
     * calls {@code runMayThrow()} inline after a deadline check -- no executor is required.
     */
    private static CapturingReadCallback driveLocalReadRunnable(SinglePartitionReadCommand command, boolean trackRepairedData)
    {
        Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
        CapturingReadCallback callback = new CapturingReadCallback(command, requestTime);
        StorageProxy.LocalReadRunnable runnable =
            new StorageProxy.LocalReadRunnable(command, callback, requestTime, trackRepairedData);
        runnable.run();
        return callback;
    }

    private static byte[] localReadRunnableResponseBytes(SinglePartitionReadCommand command, boolean trackRepairedData) throws Exception
    {
        CapturingReadCallback callback = driveLocalReadRunnable(command, trackRepairedData);
        assertNotNull("LocalReadRunnable did not deliver a response to its callback (onFailure instead? reason=" +
                       callback.failureReason + ")", callback.response);
        return serialize(callback.response);
    }

    private static byte[] verbHandlerResponseBytes(SinglePartitionReadCommand command, boolean trackRepairedData) throws Exception
    {
        return serialize(ReadCommandVerbHandler.instance.doRead(command, trackRepairedData));
    }

    private static byte[] serialize(ReadResponse response) throws Exception
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
    }

    private void assertEngagesTranscodePath(Supplier<SinglePartitionReadCommand> command) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        byte[] off = localReadRunnableResponseBytes(command.get(), false);

        DatabaseDescriptor.setCursorReadsEnabled(true);
        long before = CursorReads.transcodeResponsesServed();
        byte[] on = localReadRunnableResponseBytes(command.get(), false);
        long after = CursorReads.transcodeResponsesServed();

        assertResponseBytesEqual(off, on);
        assertEquals("expected the transcode path to engage exactly once via LocalReadRunnable", before + 1, after);
    }

    private void assertDeclinesTranscodePath(Supplier<SinglePartitionReadCommand> command) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        byte[] off = localReadRunnableResponseBytes(command.get(), false);

        DatabaseDescriptor.setCursorReadsEnabled(true);
        long before = CursorReads.transcodeResponsesServed();
        byte[] on = localReadRunnableResponseBytes(command.get(), false);
        long after = CursorReads.transcodeResponsesServed();

        // the "fallback never blocks the query" property, proven for LocalReadRunnable specifically.
        assertResponseBytesEqual(off, on);
        assertEquals("expected the transcode path to decline (fall back) via LocalReadRunnable too", before, after);
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

    // ---------------------------------------------------------------- LocalReadRunnable, own oracle

    @Test
    public void plainEligibleShapeEngagesTranscodePathThroughLocalReadRunnable() throws Throwable
    {
        setUpTwoLegTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(2, cfs.getLiveSSTables().size());

        assertEngagesTranscodePath(() -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build());
    }

    @Test
    public void limitQueryDeclinesThroughLocalReadRunnable() throws Throwable
    {
        setUpTwoLegTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertDeclinesTranscodePath(() -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withLimit(5).build());
    }

    @Test
    public void filteredQueryDeclinesThroughLocalReadRunnable() throws Throwable
    {
        setUpTwoLegTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertDeclinesTranscodePath(() -> (SinglePartitionReadCommand)
            Util.cmd(cfs, 1L).filterOn("v1", Operator.EQ, 7L).build());
    }

    // ---------------------------------------------------------------- cross-call-site equivalence

    /**
     * The M3.3b plan's own specific ask (§5/§6): the SAME command shape driven through BOTH real
     * replica-serving entry points must produce byte-identical {@code ReadResponse} wire encodings
     * -- the two call sites agreeing with EACH OTHER, not just each independently matching its own
     * gate-off oracle (already proven above and in {@link TranscodeRoutingDifferentialTest}).
     */
    @Test
    public void bothCallSitesAgreeWithEachOtherWhenTranscodePathEngages() throws Throwable
    {
        setUpTwoLegTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Supplier<SinglePartitionReadCommand> command = () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build();

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long before = CursorReads.transcodeResponsesServed();
            byte[] viaVerbHandler = verbHandlerResponseBytes(command.get(), false);
            byte[] viaLocalReadRunnable = localReadRunnableResponseBytes(command.get(), false);
            long after = CursorReads.transcodeResponsesServed();

            assertResponseBytesEqual(viaVerbHandler, viaLocalReadRunnable);
            assertEquals("expected the transcode path to engage once per call site", before + 2, after);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** Same cross-call-site equivalence check, for a shape both sites must decline. */
    @Test
    public void bothCallSitesAgreeWithEachOtherWhenTranscodePathDeclines() throws Throwable
    {
        setUpTwoLegTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Supplier<SinglePartitionReadCommand> command = () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withLimit(5).build();

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long before = CursorReads.transcodeResponsesServed();
            byte[] viaVerbHandler = verbHandlerResponseBytes(command.get(), false);
            byte[] viaLocalReadRunnable = localReadRunnableResponseBytes(command.get(), false);
            long after = CursorReads.transcodeResponsesServed();

            assertResponseBytesEqual(viaVerbHandler, viaLocalReadRunnable);
            assertEquals("declining scenario must not engage the transcode path at either call site", before, after);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    // ---------------------------------------------------------------- exception parity

    /**
     * {@code LocalReadRunnable}'s exception handling genuinely differs in SHAPE from
     * {@code ReadCommandVerbHandler.doRead}'s: a {@code TombstoneOverwhelmingException} (a
     * {@code RejectException}) is caught by {@code runMayThrow}'s own inner
     * {@code catch (RejectException e)}, rethrown (since {@code command.isTrackingWarnings()} is
     * false for a plain test command), and then caught again by {@code runMayThrow}'s OUTER
     * {@code catch (Throwable t)}, which recognizes it via {@code instanceof} and translates it into
     * {@code handler.onFailure(..., RequestFailure.READ_TOO_MANY_TOMBSTONES)} -- {@code run()} does
     * NOT rethrow it (unlike {@code ReadCommandVerbHandler.doRead}, which lets the same exception
     * propagate directly out, per
     * {@code TranscodeRoutingDifferentialTest#tombstoneThresholdAbortExceptionParity}). The parity
     * this test proves is specific to THIS call site: the identical translated outcome --
     * {@code onFailure(READ_TOO_MANY_TOMBSTONES)} and no response delivered, no exception thrown out
     * of {@code run()} -- whether the gate is on or off.
     */
    @Test
    public void tombstoneThresholdAbortExceptionParityThroughLocalReadRunnable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        // a second leg made entirely of row tombstones -- >= 2 legs, and enough tombstones to cross
        // a deliberately tiny failure threshold, same shape as the doRead exception-parity scenario.
        for (long ck = 0; ck < 10; ck++)
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        int originalThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneFailureThreshold(2);
        try
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
            CapturingReadCallback offCallback = driveLocalReadRunnable((SinglePartitionReadCommand) Util.cmd(cfs, 1L).build(), false);
            assertNull("expected no response delivered on a tombstone-threshold abort (gate off)", offCallback.response);
            assertEquals(RequestFailure.READ_TOO_MANY_TOMBSTONES, offCallback.failureReason);

            DatabaseDescriptor.setCursorReadsEnabled(true);
            long before = CursorReads.transcodeResponsesServed();
            CapturingReadCallback onCallback = driveLocalReadRunnable((SinglePartitionReadCommand) Util.cmd(cfs, 1L).build(), false);
            long after = CursorReads.transcodeResponsesServed();
            assertNull("expected no response delivered on a tombstone-threshold abort (gate on)", onCallback.response);
            assertEquals(RequestFailure.READ_TOO_MANY_TOMBSTONES, onCallback.failureReason);

            assertEquals("an aborted read must not count as a served transcode response", before, after);
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(originalThreshold);
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }
}
