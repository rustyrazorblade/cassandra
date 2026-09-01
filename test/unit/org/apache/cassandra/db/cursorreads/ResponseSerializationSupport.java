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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * M3.0 (CASSANDRA-20428, Phase 4 scaffolding): response-serialization measurement and comparison
 * plumbing for the eventual transcode sink (M3.3a/b, seam iv). Today's replica-serving read
 * serializes its merged, materialized result through {@code ReadResponse.LocalDataResponse.build}
 * — {@code serializerForIntraNode().serialize} into a ByteBuffer. The transcode sink will produce
 * those SAME bytes without materializing; this class provides both sides of every comparison the
 * design's three verification layers need, so the M3.3a agent only has to plug a
 * {@link ResponsePayloadProducer} in:
 * <ol>
 *   <li><b>Unit oracle</b> ({@link #materializedPartitionOracle} + {@link #assemblePayload}):
 *       per-partition {@code UnfilteredRowIteratorSerializer} bytes of the materialized merge
 *       output — "serialize-the-materialized-output", the byte-compare oracle the transcode
 *       sink's wire writer is verified against before any production wiring exists.</li>
 *   <li><b>Full payload</b> ({@link #intraNodePayload} vs a candidate producer): the exact
 *       response payload today's production route emits, compared byte-for-byte.</li>
 *   <li><b>Round-trip through the production consumer</b> ({@link #roundTripRecords}): candidate
 *       bytes deserialized via the same {@code serializerForIntraNode().deserialize} that
 *       {@code ReadResponse.DataResponse.makeIterator} uses on real remote responses, rendered as
 *       the harness's canonical records — proving candidate bytes are READABLE by the real
 *       consumer and diagnosable record-by-record, not just equal.</li>
 * </ol>
 * {@link #productionResponseBytes}/{@link #extractDataPayload} additionally route through the REAL
 * {@code createResponse} → {@code LocalDataResponse} production path (including the PROCESSED
 * {@code RTBoundValidator} and the response-buffer estimation), so the claim that
 * {@link #intraNodePayload} IS the {@code LocalDataResponse.build} encoding is itself verified by
 * test ({@link ResponseSerializationSupportTest}), not asserted by comment.
 */
public final class ResponseSerializationSupport
{
    private ResponseSerializationSupport()
    {
    }

    /** The seam a transcode sink implements to join the comparisons: produce the intra-node
     *  response payload for a command by WHATEVER means (today: materialize + serialize; M3.3a:
     *  transcode from merge state). */
    @FunctionalInterface
    public interface ResponsePayloadProducer
    {
        byte[] produce(SinglePartitionReadCommand command) throws Exception;
    }

    /** Today's payload: {@code executeLocally} + the exact {@code LocalDataResponse.build}
     *  encoding. This is both the reference side of every comparison and the "identity candidate"
     *  the harness self-tests with. */
    public static byte[] intraNodePayload(SinglePartitionReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller);
             DataOutputBuffer buffer = new DataOutputBuffer())
        {
            UnfilteredPartitionIterators.serializerForIntraNode()
                                        .serialize(partitions, command.columnFilter(), buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
        catch (Exception e)
        {
            throw new AssertionError("failed to serialize response payload", e);
        }
    }

    /** The whole {@link ReadResponse} serialized through the REAL production route:
     *  {@code executeLocally} → {@code createResponse} (PROCESSED validation +
     *  {@code LocalDataResponse.build}) → {@code ReadResponse.serializer}. */
    public static byte[] productionResponseBytes(SinglePartitionReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            // the controller's RepairedDataInfo is NO_OP for non-tracking reads (the type itself
            // is package-private, so it flows through this call without being named)
            ReadResponse response = command.createResponse(partitions, controller.getRepairedDataInfo());
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                ReadResponse.serializer.serialize(response, buffer, MessagingService.current_version);
                return buffer.toByteArray();
            }
        }
        catch (Exception e)
        {
            throw new AssertionError("failed to build production ReadResponse", e);
        }
    }

    /** Unwraps {@code ReadResponse.serializer} framing (empty digest vint, repaired-data digest
     *  vint, conclusive boolean, data vint) and returns the embedded data payload — the raw
     *  {@code LocalDataResponse.build} output. */
    public static byte[] extractDataPayload(byte[] productionResponseBytes)
    {
        try (DataInputBuffer in = new DataInputBuffer(productionResponseBytes))
        {
            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            if (digest.hasRemaining())
                throw new AssertionError("expected a data response, got a digest response");
            ByteBufferUtil.readWithVIntLength(in); // repaired-data digest
            in.readBoolean();                      // isRepairedDigestConclusive
            return ByteBufferUtil.getArray(ByteBufferUtil.readWithVIntLength(in));
        }
        catch (Exception e)
        {
            throw new AssertionError("failed to unwrap ReadResponse framing", e);
        }
    }

    /** The M3.3a unit oracle: each merged, materialized partition serialized independently via
     *  {@code UnfilteredRowIteratorSerializer} — the per-partition bytes a transcode sink must
     *  reproduce from merge state without materializing. */
    public static List<byte[]> materializedPartitionOracle(SinglePartitionReadCommand command)
    {
        List<byte[]> partitionsBytes = new ArrayList<>();
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next();
                     DataOutputBuffer buffer = new DataOutputBuffer())
                {
                    UnfilteredRowIteratorSerializer.serializer
                        .serialize(partition, command.columnFilter(), buffer, MessagingService.current_version);
                    partitionsBytes.add(buffer.toByteArray());
                }
            }
        }
        catch (Exception e)
        {
            throw new AssertionError("failed to serialize oracle partitions", e);
        }
        return partitionsBytes;
    }

    /** Assembles per-partition oracle bytes into the exact intra-node payload framing
     *  (legacy boolean, has-next boolean per partition, terminator) — proving the unit oracle's
     *  format composes byte-identically into the response payload. */
    public static byte[] assemblePayload(List<byte[]> partitionsBytes)
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            buffer.writeBoolean(false); // legacy isForThrift, kept on the wire
            for (byte[] partition : partitionsBytes)
            {
                buffer.writeBoolean(true);
                buffer.write(partition);
            }
            buffer.writeBoolean(false);
            return buffer.toByteArray();
        }
        catch (Exception e)
        {
            throw new AssertionError("failed to assemble payload from oracle partitions", e);
        }
    }

    /** Layer 3: deserializes a candidate payload through the SAME production deserializer that
     *  consumes real remote responses ({@code ReadResponse.DataResponse.makeIterator}) and renders
     *  the harness's canonical records, so byte divergence gets a record-level diagnosis and the
     *  candidate bytes are proven readable by the real consumer. */
    public static List<String> roundTripRecords(SinglePartitionReadCommand command, byte[] payload)
    {
        try (DataInputBuffer in = new DataInputBuffer(payload);
             UnfilteredPartitionIterator partitions =
                 UnfilteredPartitionIterators.serializerForIntraNode()
                                             .deserialize(in, MessagingService.current_version,
                                                          command.metadata(), command.columnFilter(),
                                                          DeserializationHelper.Flag.FROM_REMOTE))
        {
            return CursorReadDifferentialTester.canonicalRecords(partitions);
        }
        catch (Exception e)
        {
            throw new AssertionError("candidate payload is not readable by the production deserializer", e);
        }
    }
}
