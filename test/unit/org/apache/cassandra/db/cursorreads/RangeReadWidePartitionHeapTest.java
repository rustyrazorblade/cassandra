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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeScan;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeReadSink;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.CellValueSource;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The heap a {@link CursorRangeScan} holds while it reads a wide partition, and how long it holds
 * the table's read order group for it (a scan yields only between partitions).  One wide partition
 * of {@code -Dcassandra.test.range_scan_partition_mib} MiB (default {@value #DEFAULT_PARTITION_MIB};
 * run it at 1024 and 10240) of 1 KiB rows, in several sstables, between two small partitions, on
 * BTI.  Every {@value #SAMPLE_MIB} MiB the sink runs a full GC and samples the heap in use: it must
 * stay within {@value #HEAP_BOUND_MIB} MiB of the heap before the scan, whatever the partition size.
 */
public class RangeReadWidePartitionHeapTest extends RangeReadOracle
{
    private static final Logger logger = LoggerFactory.getLogger(RangeReadWidePartitionHeapTest.class);

    private static final int DEFAULT_PARTITION_MIB = 64;
    private static final long PARTITION_MIB = Long.getLong("cassandra.test.range_scan_partition_mib", DEFAULT_PARTITION_MIB); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int ROW_BYTES = 1024;
    private static final int ROWS_PER_BATCH = 100;
    /** Rows written between flushes: about 128 MiB per sstable. */
    private static final int ROWS_PER_SSTABLE = 128 * 1024;
    private static final int SAMPLE_MIB = 64;
    private static final long HEAP_BOUND_MIB = 256;

    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void pinBti()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void widePartitionScanHoldsBoundedHeap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v blob, PRIMARY KEY (k, c)) WITH compression = {'class': 'LZ4Compressor'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // whole batches
        long rows = (PARTITION_MIB * 1024 * 1024 / ROW_BYTES + ROWS_PER_BATCH - 1) / ROWS_PER_BATCH * ROWS_PER_BATCH;
        long writeStart = System.nanoTime();
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, ?)", value(0));
        execute("INSERT INTO %s (k, c, v) VALUES (2, 0, ?)", value(0));
        String batch = batchStatement();
        for (long written = 0; written < rows; written += ROWS_PER_BATCH)
        {
            Object[] values = new Object[ROWS_PER_BATCH * 2];
            for (int i = 0; i < ROWS_PER_BATCH; i++)
            {
                values[2 * i] = (int) (written + i);
                values[2 * i + 1] = value(written + i);
            }
            execute(batch, values);
            if ((written + ROWS_PER_BATCH) % ROWS_PER_SSTABLE < ROWS_PER_BATCH)
                flush();
        }
        flush();
        logger.info("wrote a partition of {} MiB ({} rows) into {} sstables in {} s", PARTITION_MIB, rows, cfs.getLiveSSTables().size(),
                    TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - writeStart));

        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        System.gc();
        long baseline = memory.getHeapMemoryUsage().getUsed();
        CountingSink sink = new CountingSink(memory);
        CursorRangeScan scan = CursorRangeScan.open(rangeCommand("SELECT * FROM %s", FBUtilities.nowInSeconds()));
        long longestSegmentNanos = 0;
        long scanStart = System.nanoTime();
        while (true)
        {
            long start = System.nanoTime();
            CursorRangeScan.ScanEnd end = scan.run(sink);
            longestSegmentNanos = Math.max(longestSegmentNanos, System.nanoTime() - start);
            if (end == CursorRangeScan.ScanEnd.DONE)
                break;
        }
        long scanNanos = System.nanoTime() - scanStart;
        assertEquals("partitions taken from an iterator", 0, scan.iteratorLegsOpened());
        assertEquals("partitions", 3, sink.partitions);
        assertEquals("rows of the wide partition", rows, sink.wideRows);
        long heldMib = (sink.maxHeapUsed - baseline) / (1024 * 1024);
        logger.info("scanned {} MiB ({} rows) in {} ms ({} MiB/s); the read order group was held at most {} ms (the wide partition's segment); " +
                    "heap in use after a GC at most {} MiB above the {} MiB before the scan ({} samples)",
                    PARTITION_MIB, rows, TimeUnit.NANOSECONDS.toMillis(scanNanos),
                    PARTITION_MIB * TimeUnit.SECONDS.toNanos(1) / Math.max(1, scanNanos),
                    TimeUnit.NANOSECONDS.toMillis(longestSegmentNanos), heldMib, baseline / (1024 * 1024), sink.samples);
        assertTrue("the scan held " + heldMib + " MiB of heap reading a " + PARTITION_MIB + " MiB partition", heldMib <= HEAP_BOUND_MIB);
    }

    private static String batchStatement()
    {
        StringBuilder batch = new StringBuilder("BEGIN UNLOGGED BATCH ");
        for (int i = 0; i < ROWS_PER_BATCH; i++)
            batch.append("INSERT INTO %1$s (k, c, v) VALUES (1, ?, ?); ");
        return batch.append("APPLY BATCH").toString();
    }

    private static ByteBuffer value(long row)
    {
        byte[] bytes = new byte[ROW_BYTES];
        for (int i = 0; i < bytes.length; i++)
            bytes[i] = (byte) (row * 31 + i);
        return ByteBuffer.wrap(bytes);
    }

    /** Counts what it gets, checks the rows come in order, and samples the heap; yields after each partition. */
    private static final class CountingSink implements RangeReadSink
    {
        private final MemoryMXBean memory;
        /** One value at a time; a consumer would copy it on. */
        private final DataOutputBuffer valueBuffer = new DataOutputBuffer(ROW_BYTES + 16);
        int partitions;
        long wideRows;
        private boolean wide;
        private long bytesSinceSample;
        long maxHeapUsed;
        int samples;

        CountingSink(MemoryMXBean memory)
        {
            this.memory = memory;
        }

        @Override
        public void partitionStart(DecoratedKey key, DeletionTime partitionDeletion, Row staticRow)
        {
            partitions++;
            wide = key.getKey().getInt(key.getKey().position()) == 1;
        }

        @Override
        public void startRow(ClusteringDescriptor clustering, LivenessInfo liveness, DeletionTime deletion)
        {
            if (wide)
                wideRows++;
        }

        @Override
        public void complexDeletion(ColumnMetadata column, DeletionTime deletion)
        {
        }

        @Override
        public void cell(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime, CellPath path, CellValueSource value)
        {
            if (value == null)
                return;
            try
            {
                // a consumer reads the value; this one throws it away
                valueBuffer.clear();
                value.streamValue(valueBuffer);
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
            bytesSinceSample += ROW_BYTES;
            if (bytesSinceSample >= SAMPLE_MIB * 1024L * 1024)
            {
                bytesSinceSample = 0;
                System.gc();
                maxHeapUsed = Math.max(maxHeapUsed, memory.getHeapMemoryUsage().getUsed());
                samples++;
            }
        }

        @Override
        public void endRow()
        {
        }

        @Override
        public void abandonRow()
        {
        }

        @Override
        public void marker(RangeTombstoneMarker marker)
        {
        }

        @Override
        public void partitionEnd()
        {
        }

        @Override
        public boolean yieldAfterPartition()
        {
            return true;
        }
    }
}
