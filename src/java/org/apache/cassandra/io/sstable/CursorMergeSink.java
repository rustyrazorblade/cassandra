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
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * The output side of {@link org.apache.cassandra.db.compaction.CursorCompactor}'s merge loop,
 * extracted from {@link SSTableCursorWriter} so the merge loop can be driven against a sink that
 * doesn't write sstable bytes at all (e.g. a read-only consumer such as repair validation).
 * <p>
 * Pure interface extraction of {@link SSTableCursorWriter}'s public API used by
 * {@code CursorCompactor} - no behavior change from this type existing on its own.
 */
public interface CursorMergeSink
{
    long getPosition();

    long getPartitionStart();

    int writePartitionStart(byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime) throws IOException;

    void writePartitionEnd(DecoratedKey decoratedKey, byte[] partitionKey, int partitionKeyLength, DeletionTime partitionDeletionTime, int headerLength) throws IOException;

    boolean writeEmptyStaticRow() throws IOException;

    void writeRowStart(LivenessInfo livenessInfo, DeletionTime deletionTime, boolean isStatic) throws IOException;

    void startComplexColumn(ColumnMetadata column, DeletionTime mergedDeletion) throws IOException;

    void writeCellPath(byte[] pathBuffer, int pathLength) throws IOException;

    void writeCellHeader(int cellFlags, ReusableLivenessInfo cellLiveness, ColumnMetadata cellColumn) throws IOException;

    int writeCellValue(SSTableCursorReader cursor, byte[] copyColumnValueBuffer) throws IOException;

    void writeCellValue(DataOutputBuffer tempCellBuffer) throws IOException;

    void writeCellValue(byte[] value, int offset, int length) throws IOException;

    void updateCounterShardStats(boolean hasLegacyShards);

    void writeRowEnd(UnfilteredDescriptor rHeader, boolean updateClusteringMetadata) throws IOException;

    void writeRangeTombstone(UnfilteredDescriptor rangeTombstone, boolean updateClusteringMetadata) throws IOException;

    void updateClusteringMetadata(ClusteringDescriptor clusteringDescriptor);

    void setLast(ByteBuffer key);

    void setFirst(ByteBuffer key);
}
