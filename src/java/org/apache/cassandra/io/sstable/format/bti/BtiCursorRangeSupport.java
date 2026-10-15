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

package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.FBUtilities.immutableListWithFilteredNulls;

/**
 * The BTI partition index for cursor range reads ({@code CursorRangeReads}): the data file position
 * of each partition in a key range, in order, without reading the data file.
 */
public final class BtiCursorRangeSupport
{
    private BtiCursorRangeSupport()
    {
    }

    /**
     * Walks the partition index over one key range, like {@link PartitionIterator}, but gives only
     * where each partition starts in the data file, and its row index entry when it has one.  The
     * partition key is not read: the caller reads it from the partition header, where its cursor
     * has to read anyway, and checks it against the range.  The index stores key prefixes, so the
     * walk can give one partition before the range start and one after its end.
     */
    public static final class PartitionWalker implements AutoCloseable
    {
        private final BtiTableReader sstable;
        private final PartitionIndex index;
        private final PartitionIndex.IndexPosIterator positions;
        /** Opened at the first partition that has a row index entry. */
        private FileDataInput rowIndexInput;
        private long dataPosition;
        private BtiCursorSeekSupport.PartitionEntry rowIndexed;

        private PartitionWalker(BtiTableReader sstable, PartitionIndex index, PartitionPosition left, PartitionPosition right)
        {
            this.sstable = sstable;
            this.index = index;
            this.positions = new PartitionIndex.IndexPosIterator(index, left, right);
        }

        /** A walk of the partitions of {@code sstable} between {@code left} and {@code right}. */
        public static PartitionWalker open(BtiTableReader sstable, PartitionPosition left, PartitionPosition right)
        {
            PartitionIndex index = sstable.partitionIndex().sharedCopy();
            try
            {
                return new PartitionWalker(sstable, index, left, right);
            }
            catch (RuntimeException | Error e)
            {
                index.close();
                throw e;
            }
        }

        /** Moves to the next partition; false when the walk is over. */
        public boolean advance()
        {
            try
            {
                long pos = positions.nextIndexPos();
                if (pos == PartitionIndex.NOT_FOUND)
                    return false;
                if (pos >= 0)
                {
                    // a partition with a row index: its entry in the row index file follows its key
                    if (rowIndexInput == null)
                        rowIndexInput = sstable.rowIndexFile().createReader(pos);
                    else
                        rowIndexInput.seek(pos);
                    ByteBufferUtil.skipShortLength(rowIndexInput);
                    TrieIndexEntry entry = TrieIndexEntry.deserialize(rowIndexInput, rowIndexInput.getFilePointer(), sstable.descriptor.version);
                    rowIndexed = BtiCursorSeekSupport.entryOf(entry);
                    dataPosition = entry.position;
                }
                else
                {
                    rowIndexed = null;
                    dataPosition = ~pos;
                }
                return true;
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.rowIndexFile().path());
            }
        }

        /** Where the current partition starts in the data file. */
        public long dataPosition()
        {
            return dataPosition;
        }

        /** The current partition's row index entry, or null when it has none. */
        public BtiCursorSeekSupport.PartitionEntry rowIndexedEntry()
        {
            return rowIndexed;
        }

        @Override
        public void close()
        {
            Throwable failure = Throwables.close(null, immutableListWithFilteredNulls(positions, rowIndexInput, index));
            Throwables.maybeFail(failure);
        }
    }
}
