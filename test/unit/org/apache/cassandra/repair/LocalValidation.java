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

package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Runs one validation on this node through {@link ValidationManager#doValidation}, as a repair
 * session does, and returns the Merkle trees it built instead of sending them.
 */
public final class LocalValidation
{
    /** What one validation built and read. */
    public static final class Result
    {
        /** The Merkle trees in serialized form, so two results compare byte for byte. */
        public final byte[] serializedTrees;
        /**
         * The root hash of the tree of each range, in range order.  It does not depend on the
         * depth the tree was built to, which follows estimated partition counts.
         */
        public final List<byte[]> rootHashes;
        public final long rowCount;
        public final long estimatedPartitions;
        public final long estimatedTotalBytes;
        public final long partitionsProcessed;

        Result(byte[] serializedTrees, List<byte[]> rootHashes, long rowCount, ValidationState state)
        {
            this.serializedTrees = serializedTrees;
            this.rootHashes = rootHashes;
            this.rowCount = rowCount;
            this.estimatedPartitions = state.estimatedPartitions;
            this.estimatedTotalBytes = state.estimatedTotalBytes;
            this.partitionsProcessed = state.partitionsProcessed;
        }
    }

    private LocalValidation()
    {
    }

    /**
     * Validates {@code ranges} of {@code cfs} for a parent repair session that the caller has
     * already registered with {@code ActiveRepairService}.
     */
    public static Result validate(ColumnFamilyStore cfs, TimeUUID parentId, TimeUUID sessionId, Collection<Range<Token>> ranges,
                                  boolean isIncremental, PreviewKind previewKind, long nowInSec) throws Exception
    {
        RepairJobDesc desc = new RepairJobDesc(parentId, sessionId, cfs.getKeyspaceName(), cfs.getTableName(), ranges);
        ValidationState state = new ValidationState(Clock.Global.clock(), desc, FBUtilities.getBroadcastAddressAndPort());
        CompletableFuture<ValidationResponse> response = new CompletableFuture<>();
        Validator validator = new Validator(SharedContext.Global.instance, state, nowInSec, false, isIncremental, previewKind)
        {
            @Override
            void respond(ValidationResponse r)
            {
                response.complete(r);
            }
        };
        ValidationManager.doValidation(cfs, validator);
        ValidationResponse r = response.get(60, TimeUnit.SECONDS);
        if (!r.success())
            throw new AssertionError("validation failed for " + desc);
        MerkleTrees trees = r.trees;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            MerkleTrees.serializer.serialize(trees, out, MessagingService.current_version);
            List<byte[]> rootHashes = new ArrayList<>();
            for (Range<Token> range : ranges)
                rootHashes.add(trees.hash(range));
            return new Result(out.toByteArray(), rootHashes, trees.rowCount(), state);
        }
        finally
        {
            trees.release();
        }
    }
}
