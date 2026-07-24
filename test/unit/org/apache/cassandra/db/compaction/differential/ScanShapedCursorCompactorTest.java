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

package org.apache.cassandra.db.compaction.differential;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.CursorMergeConsumer;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Exercises the scan-shaped {@link CursorCompactor} entry point (increment 5): a
 * {@link CursorMergeConsumer} test double drives a full merge across multiple sstables with no
 * {@link org.apache.cassandra.db.compaction.writers.CompactionAwareWriter} and no output
 * sstable — proving the "no writer present" path (interface-typed {@code output} set, concrete
 * {@code SSTableCursorWriter} field left null) never touches writer-rollover code.
 */
public class ScanShapedCursorCompactorTest extends CQLTester
{
    /** Records partition keys and row starts seen; does not build any real output. */
    private static class RecordingConsumer implements CursorMergeConsumer
    {
        final List<byte[]> partitionKeys = new ArrayList<>();
        final DataOutputBuffer scratch = new DataOutputBuffer();
        int rowsStarted = 0;

        @Override
        public void startPartition(byte[] partitionKey, int keyLength, DeletionTime partitionDeletion)
        {
            partitionKeys.add(Arrays.copyOf(partitionKey, keyLength));
        }

        @Override
        public void endPartition(DecoratedKey key, byte[] partitionKey, int keyLength, DeletionTime partitionDeletion)
        {
        }

        @Override
        public boolean writeEmptyStaticRow()
        {
            return false;
        }

        @Override
        public void startRow(UnfilteredDescriptor clustering, LivenessInfo liveness, DeletionTime deletion, boolean isStatic)
        {
            rowsStarted++;
        }

        @Override
        public void endRow(UnfilteredDescriptor row, boolean isFirstUnfiltered)
        {
        }

        @Override
        public void startComplexColumn(ColumnMetadata column, DeletionTime mergedDeletion)
        {
        }

        @Override
        public void writeCellHeader(int cellFlags, ReusableLivenessInfo cellLiveness, ColumnMetadata column)
        {
        }

        @Override
        public void writeCellPath(byte[] pathBuffer, int pathLength)
        {
        }

        @Override
        public int writeCellValue(SSTableCursorReader source, byte[] copyBuffer) throws IOException
        {
            // must still consume the value from the reader to advance its state correctly;
            // the bytes themselves are not needed by this test double
            scratch.clear();
            return source.copyCellValue(scratch, copyBuffer);
        }

        @Override
        public void writeCellValue(DataOutputBuffer staged)
        {
        }

        @Override
        public void writeCellValue(byte[] value, int offset, int length)
        {
        }

        @Override
        public void writeRangeTombstone(UnfilteredDescriptor marker, boolean isFirstUnfiltered)
        {
        }
    }

    @Test
    public void scanShapedConstructorDrivesFullMergeWithoutWriter() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        for (long pk = 0; pk < 5; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 1L, "v" + pk);
        flush();
        for (long pk = 5; pk < 10; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 1L, "v" + pk);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertTrue("expected at least 2 flushed sstables", sstables.size() >= 2);

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        RecordingConsumer consumer = new RecordingConsumer();
        try (CompactionController controller = new CompactionController(cfs, sstables, gcBefore))
        {
            CursorCompactor compactor = new CursorCompactor(OperationType.COMPACTION,
                                                             sstables,
                                                             consumer,
                                                             controller,
                                                             FBUtilities.nowInSeconds(),
                                                             nextTimeUUID());
            try
            {
                //noinspection StatementWithEmptyBody
                while (compactor.writeNextPartition())
                {
                    // drains every partition into the consumer
                }
            }
            finally
            {
                compactor.close();
            }
        }

        assertEquals(10, consumer.partitionKeys.size());
        assertEquals(10, consumer.rowsStarted);
    }

    /**
     * The scan-shaped constructor overload that takes an explicit {@link DiskAccessMode} (added for
     * {@code org.apache.cassandra.arrow.CassandraTableScanner}, which forces {@code direct} so its
     * scans don't pollute the page cache real background compaction/reads rely on, independent of
     * the node-wide {@code compaction_read_disk_access_mode} setting). {@code direct} is requested
     * here too, on purpose: this table is uncompressed, and direct I/O requires a compressed sstable
     * ({@link org.apache.cassandra.io.util.FileHandle#supportsDirectIO()}) as well as a Linux host -
     * neither holds for this test on most dev/CI machines, so this exercises the documented
     * graceful-fallback behavior (silently keep the existing disk access mode) rather than real
     * O_DIRECT reads, and confirms requesting an unavailable mode never breaks the merge.
     */
    @Test
    public void scanShapedConstructorWithExplicitDiskAccessModeProducesSameMerge() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        for (long pk = 0; pk < 5; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 1L, "v" + pk);
        flush();
        for (long pk = 5; pk < 10; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 1L, "v" + pk);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertTrue("expected at least 2 flushed sstables", sstables.size() >= 2);

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        RecordingConsumer consumer = new RecordingConsumer();
        try (CompactionController controller = new CompactionController(cfs, sstables, gcBefore))
        {
            CursorCompactor compactor = new CursorCompactor(OperationType.COMPACTION,
                                                             sstables,
                                                             consumer,
                                                             controller,
                                                             FBUtilities.nowInSeconds(),
                                                             nextTimeUUID(),
                                                             DiskAccessMode.direct);
            try
            {
                //noinspection StatementWithEmptyBody
                while (compactor.writeNextPartition())
                {
                    // drains every partition into the consumer
                }
            }
            finally
            {
                compactor.close();
            }
        }

        assertEquals(10, consumer.partitionKeys.size());
        assertEquals(10, consumer.rowsStarted);
    }
}
