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

package org.apache.cassandra.io.sstable;

import java.io.IOException;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Receives the merge events produced by {@link org.apache.cassandra.db.compaction.CursorCompactor}
 * as it merges cursors over one or more sstables. {@link SSTableCursorWriter} is the only
 * production implementation today (it turns these events into a new sstable), but the seam is
 * EVENT-shaped, not writer-shaped, so a non-writer consumer (e.g. a row assembler feeding some
 * other output) can drive off the exact same call sequence without any change to the merge
 * logic in {@code CursorCompactor}.
 */
public interface CursorMergeConsumer
{
    void startPartition(byte[] partitionKey, int keyLength, DeletionTime partitionDeletion) throws IOException;

    void endPartition(DecoratedKey key, byte[] partitionKey, int keyLength, DeletionTime partitionDeletion) throws IOException;

    /**
     * Writes an empty static row, if this table has static columns.
     *
     * @return true if this output/table has static columns. For {@link SSTableCursorWriter}
     *         this specifically means the OUTPUT sstable's serialization header has static
     *         columns; other implementers must return whether the TABLE (not just their own
     *         prior output) has static columns.
     */
    boolean writeEmptyStaticRow() throws IOException;

    /**
     * Starts a row. {@code clustering} is the row's clustering (null for static rows) — it is
     * additive relative to what {@link SSTableCursorWriter} strictly needs (which only reaches
     * the writer at {@link #endRow}/late-start time): a consumer that assembles rows from
     * scratch (e.g. an Arrow-style row builder) needs row identity before any cells stream in.
     * {@link SSTableCursorWriter} ignores this parameter.
     */
    void startRow(UnfilteredDescriptor clustering, LivenessInfo liveness, DeletionTime deletion, boolean isStatic) throws IOException;

    void endRow(UnfilteredDescriptor row, boolean isFirstUnfiltered) throws IOException;

    void startComplexColumn(ColumnMetadata column, DeletionTime mergedDeletion) throws IOException;

    void writeCellHeader(int cellFlags, ReusableLivenessInfo cellLiveness, ColumnMetadata column) throws IOException;

    /** Appends the current complex cell's path (vint length + bytes) to the cell stream. */
    void writeCellPath(byte[] pathBuffer, int pathLength) throws IOException;

    /** Streaming fast path: copies the cell value straight out of the source reader. */
    int writeCellValue(SSTableCursorReader source, byte[] copyBuffer) throws IOException;

    /** Staged-value variant: the value has already been assembled in a buffer. */
    void writeCellValue(DataOutputBuffer staged) throws IOException;

    /** Raw-bytes variant: a window directly into a byte[] (e.g. a folded counter context). */
    void writeCellValue(byte[] value, int offset, int length) throws IOException;

    void writeRangeTombstone(UnfilteredDescriptor marker, boolean isFirstUnfiltered) throws IOException;

    /** {@link org.apache.cassandra.db.rows.Cells#collectStats} parity hook; no-op unless overridden. */
    default void updateCounterShardStats(boolean hasLegacyShards) {}

    /** Clustering-bounds stats hook; no-op unless overridden. */
    default void updateClusteringMetadata(UnfilteredDescriptor last) {}
}
