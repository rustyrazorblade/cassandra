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
package org.apache.cassandra.db.rows;

import java.io.IOException;

import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * M3.3a-ii (CASSANDRA-20428, Phase 4 seam iv): an abstraction over "wherever a resolved cell
 * winner's raw, wire-form value bytes currently live" — passed to
 * {@code org.apache.cassandra.db.CursorReadMerger.MergeSink#addCellFromWire} so a sink can stream
 * a sstable-won cell's value straight from its source into its own destination without
 * {@code CursorReadMerger} ever forming an intermediate {@code byte[]}/{@code Cell} object on the
 * caller's behalf — the actual allocation win this seam exists for.
 * <p>
 * Declared here (public, in {@code db.rows}) rather than nested inside the read-owned,
 * package-private {@code CursorReadMerger} (which lives in {@code org.apache.cassandra.db})
 * purely so {@link ResponseWireWriter} — which needs to call these methods directly while
 * assembling a streamed row body — can see the type across the package boundary; the interface
 * itself carries no read-merge semantics of its own, it is a pure value-source contract.
 * <p>
 * A reused, per-call view: valid only for the duration of the {@code addCellFromWire} call that
 * receives it (the same discipline {@code CursorReadMerger.FilterProbe}'s reusable
 * {@code LivenessInfo}/{@code DeletionTime} arguments already document) — implementations must
 * not retain it past that call.
 */
public interface CellValueSource
{
    /**
     * Mirrors {@link Cell.Serializer#HAS_EMPTY_VALUE_MASK}: true iff the winner's value is
     * non-empty. A valueless cell (a tombstone, or a live cell of an intentionally empty value)
     * answers false — {@link #streamValue} and {@link #materialize} must not be called then.
     */
    boolean hasValue() throws IOException;

    /**
     * Streams the raw wire-form value bytes into {@code dest}: the value-length vint first for a
     * variable-length value (mirroring the wire's own encoding), then the raw bytes — exactly
     * the two-call contract {@code CursorReads.MergeLeg#stageCellValue} documents (this is
     * usually a direct forward to that same call). Must only be called when {@link #hasValue()}
     * is true.
     */
    void streamValue(DataOutputPlus dest) throws IOException;

    /**
     * Materializes a final value array — used only by a sink that opts into streamed cells
     * (see {@code CursorReadMerger.MergeSink#wantsWireStreamedCells}) but does not override the
     * default (materializing) {@code MergeSink#addCellFromWire}. Must only be called when
     * {@link #hasValue()} is true.
     */
    byte[] materialize() throws IOException;
}
