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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Cursor range reads over an sstable opened early, while it is still being written.
 *
 * <p>An early-opened BTI reader only knows the partitions in its partial partition index, but its
 * data file is readable up to the data writer's last flush offset. That offset can lie past the end
 * of the reader's last partition, inside partitions the index does not hold yet, the last of them
 * only partly written. When a range's right bound is at or past the reader's last key, the byte range
 * for the read ends at that offset. The cursor read must still stop at the reader's last key, as the
 * iterator path does ({@code SSTableScanner.makeBounds} clamps the range to {@code getFirst()} and
 * {@code getLast()}).
 *
 * <p>Each test compares the cursor path ({@link CursorRangeReads#rangeIterator}) with the iterator
 * path ({@link SSTableReader#partitionIterator}) on the same early-opened reader, for a token range
 * whose right bound is the reader's last key, for a full range read, and for a full range read in
 * reverse clustering order. It runs for BTI and for BIG.
 */
public class RangeReadEarlyOpenDifferentialTest extends CursorReadDifferentialTester
{
    /** Upper limit on partitions written; the index files must be flushed before a reader is delivered. */
    private static final int PARTITION_COUNT = 200_000;
    private static final int OPEN_INTERVAL = 1000;
    private static final int ROWS_PER_PARTITION = 2;
    /** Not a divisor of the compression chunk size, so data flushes land inside partitions. */
    private static final int VALUE_SIZE = 113;

    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void saveFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void btiEarlyOpenedReaderMatchesIteratorPath() throws Throwable
    {
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        runAgainstEarlyOpenedReader(BtiTableReader.class);
    }

    @Test
    public void bigEarlyOpenedReaderMatchesIteratorPath() throws Throwable
    {
        DatabaseDescriptor.setSelectedSSTableFormat("big");
        runAgainstEarlyOpenedReader(BigTableReader.class);
    }

    private void runAgainstEarlyOpenedReader(Class<? extends SSTableReader> expectedReaderClass) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v blob, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();

        List<DecoratedKey> keys = new ArrayList<>(PARTITION_COUNT);
        for (int pk = 0; pk < PARTITION_COUNT; pk++)
            keys.add(metadata.partitioner.decorateKey(ByteBufferUtil.bytes(pk)));
        Collections.sort(keys); // a writer takes partitions in token order

        Descriptor descriptor = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables());
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        SSTableReader[] early = new SSTableReader[1];
        int written = 0;
        try (SSTableWriter writer = newWriter(cfs, descriptor, txn))
        {
            // Ask for an early open every OPEN_INTERVAL partitions until a reader is delivered. A BTI
            // reader is delivered only once the index files are flushed, by which time the data file
            // has been flushed well past the reader's last partition.
            while (written < PARTITION_COUNT && early[0] == null)
            {
                writer.append(partition(metadata, keys.get(written++)).unfilteredIterator());
                if (written % OPEN_INTERVAL == 0)
                {
                    writer.setMaxDataAge(1000);
                    writer.openEarly(reader -> early[0] = reader);
                }
            }
            for (int i = 0; i < OPEN_INTERVAL && written < PARTITION_COUNT; i++)
                writer.append(partition(metadata, keys.get(written++)).unfilteredIterator());

            SSTableReader sstable = early[0];
            assertNotNull("the writer never delivered an early-opened reader", sstable);
            try
            {
                assertTrue("expected a " + expectedReaderClass.getSimpleName() + " but got " + sstable.getClass().getSimpleName(),
                           expectedReaderClass.isInstance(sstable));
                assertEquals(SSTableReader.OpenReason.EARLY, sstable.openReason);
                assertTrue("partitions past the reader's last key must already be written",
                           keys.indexOf(sstable.getLast()) < written - 1);
                checkReads(cfs, sstable, keys);
            }
            finally
            {
                sstable.selfRef().release();
            }
            writer.abort();
        }
        finally
        {
            txn.abort();
            LifecycleTransaction.waitForDeletions();
        }
    }

    private void checkReads(ColumnFamilyStore cfs, SSTableReader sstable, List<DecoratedKey> keys)
    {
        IPartitioner partitioner = cfs.metadata().partitioner;
        int lastIndex = keys.indexOf(sstable.getLast());
        assertTrue("the reader must hold several partitions", lastIndex > 4);

        // (a) a token range whose right bound is the reader's last key: the byte range for it ends at
        // the readable length of the data file, not at the end of the last partition
        Range<Token> toLast = new Range<>(keys.get(2).getToken(), sstable.getLast().getToken());
        DataRange toLastRange = DataRange.forTokenRange(toLast);
        PartitionPositionBounds bounds = sstable.getPositionsForBounds(toLastRange.keyRange());
        assertNotNull(bounds);
        assertEquals("the byte range must run to the readable length of the data file",
                     sstable.uncompressedLength(), bounds.upperPosition);
        compare(cfs, sstable, toLastRange, lastIndex - 2);

        // (b) a full range read
        compare(cfs, sstable, DataRange.allData(partitioner), lastIndex + 1);

        // (c) a full range read in reverse clustering order
        compare(cfs, sstable, DataRange.allData(partitioner, new ClusteringIndexSliceFilter(Slices.ALL, true)), lastIndex + 1);
    }

    private void compare(ColumnFamilyStore cfs, SSTableReader sstable, DataRange dataRange, int expectedPartitions)
    {
        PartitionRangeReadCommand command = PartitionRangeReadCommand.create(cfs.metadata(),
                                                                             FBUtilities.nowInSeconds(),
                                                                             ColumnFilter.all(cfs.metadata()),
                                                                             RowFilter.none(),
                                                                             DataLimits.NONE,
                                                                             dataRange);
        List<String> iterator = render(sstable.partitionIterator(command.columnFilter(), dataRange, SSTableReadsListener.NOOP_LISTENER),
                                       sstable.getLast());
        List<String> cursor = render(CursorRangeReads.rangeIterator(command, cfs, Collections.singletonList(sstable),
                                                                    Collections.emptyList(), null, SSTableReadsListener.NOOP_LISTENER),
                                     sstable.getLast());
        assertEquals("iterator path partition count", expectedPartitions, iterator.stream().filter(r -> r.startsWith("PARTITION")).count());
        // the full records too, with the heap sizes the read size guardrail counts
        try (UnfilteredPartitionIterator iteratorPartitions = sstable.partitionIterator(command.columnFilter(), dataRange, SSTableReadsListener.NOOP_LISTENER);
             UnfilteredPartitionIterator cursorPartitions = CursorRangeReads.rangeIterator(command, cfs, Collections.singletonList(sstable),
                                                                                          Collections.emptyList(), null, SSTableReadsListener.NOOP_LISTENER))
        {
            compareRecords(canonicalRecords(iteratorPartitions), canonicalRecords(cursorPartitions));
        }
        int max = Math.max(iterator.size(), cursor.size());
        for (int i = 0; i < max; i++)
        {
            String expected = i < iterator.size() ? iterator.get(i) : "<missing>";
            String actual = i < cursor.size() ? cursor.get(i) : "<missing>";
            if (!expected.equals(actual))
                fail(String.format("range read divergence at record %d (iterator vs cursor):%n  iterator: %s%n  cursor:   %s",
                                   i, expected, actual));
        }
    }

    /** One record per partition and per row; fails if a partition key is past the reader's last key. */
    private static List<String> render(UnfilteredPartitionIterator partitions, PartitionPosition last)
    {
        List<String> out = new ArrayList<>();
        try (UnfilteredPartitionIterator it = partitions)
        {
            while (it.hasNext())
            {
                try (UnfilteredRowIterator partition = it.next())
                {
                    DecoratedKey key = partition.partitionKey();
                    assertTrue("partition " + ByteBufferUtil.bytesToHex(key.getKey()) + " is past the reader's last key",
                               key.compareTo(last) <= 0);
                    out.add("PARTITION " + ByteBufferUtil.bytesToHex(key.getKey()) + " del=" + partition.partitionLevelDeletion());
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        assertTrue("dataset has rows only", unfiltered.isRow());
                        Row row = (Row) unfiltered;
                        out.add("ROW " + row.clustering().toString(partition.metadata()) + ' ' + row.toString(partition.metadata(), true));
                    }
                }
            }
        }
        return out;
    }

    private static PartitionUpdate partition(TableMetadata metadata, DecoratedKey key)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key).timestamp(1000L);
        for (int ck = 0; ck < ROWS_PER_PARTITION; ck++)
        {
            byte[] value = new byte[VALUE_SIZE];
            ThreadLocalRandom.current().nextBytes(value);
            builder.row(ck).add("v", ByteBuffer.wrap(value));
        }
        return builder.build();
    }

    private static SSTableWriter newWriter(ColumnFamilyStore cfs, Descriptor descriptor, LifecycleTransaction txn)
    {
        return descriptor.getFormat().getWriterFactory().builder(descriptor)
                         .setTableMetadataRef(cfs.metadata)
                         .setKeyCount(PARTITION_COUNT)
                         .setSerializationHeader(new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS))
                         .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                         .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator))
                         .addDefaultComponents(cfs.indexManager.listIndexGroups())
                         .build(txn, cfs);
    }
}
