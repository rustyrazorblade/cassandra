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

import java.lang.management.ManagementFactory;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.ResponseSink;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

import static org.junit.Assert.assertEquals;

/**
 * Bytes allocated per row by a replica data read ({@code ReadCommandVerbHandler.doRead}) of one wide
 * partition, on the iterator path and on the cursor path, measured with the thread allocation
 * counter.  The read shapes: the whole partition, a limit, and a row filter, over one sstable (read
 * as stored) and over two (merged), with one and with six regular columns.  The difference between
 * six and one column, per row, is what each extra cell costs.  Every response buffer starts at one
 * fixed size larger than any response here, so it never grows; that fixed size is subtracted, and the
 * response's own bytes are reported apart.  The row count comes from
 * {@code -Dcassandra.test.cursor_alloc_rows} (default 20000).
 *
 * <p>Gates: when the cursor path writes the response itself, no sstable cell value is built
 * ({@link CursorReads#sstableCellValuesMaterialized()} does not move), and neither a row nor an
 * extra simple column allocates beyond its bytes in the response.
 */
public class TranscodeResponseAllocationTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(TranscodeResponseAllocationTest.class);

    private static final int ROWS = Integer.getInteger("cassandra.test.cursor_alloc_rows", 20000); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    /** With {@code -Dcassandra.test.cursor_alloc_default_buffers=true} the response buffers keep their
     *  production sizing, so the totals include their growth and the per-cell gate does not apply. */
    private static final boolean DEFAULT_BUFFERS = Boolean.getBoolean("cassandra.test.cursor_alloc_default_buffers"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    /** Every iterator-path response buffer starts at this size, large enough for the widest
     *  response, so a read allocates exactly this much for its buffer and never grows it; it is
     *  subtracted below.  The cursor path's buffer allocation is subtracted the same way, see
     *  {@link ResponseSink#responseBufferAllocation}. */
    private static final int RESPONSE_BUFFER = DEFAULT_BUFFERS ? 0 : Math.max(8 << 20, ROWS * 256);

    static
    {
        if (!DEFAULT_BUFFERS)
        {
            CassandraRelevantProperties.DATA_RESPONSE_BUFFER_INITIAL_SIZE_MIN.setInt(RESPONSE_BUFFER);
            CassandraRelevantProperties.DATA_RESPONSE_BUFFER_INITIAL_SIZE_MAX.setInt(RESPONSE_BUFFER);
        }
    }
    private static final int WARMUP = 6;
    private static final int MEASURED = 5;

    @After
    public void cursorReadsOff()
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
    }

    @Test
    public void bytesAllocatedPerRow() throws Throwable
    {
        java.lang.management.ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        Assume.assumeTrue(threadBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) threadBean;
        Assume.assumeTrue(bean.isThreadAllocatedMemorySupported());
        bean.setThreadAllocatedMemoryEnabled(true);

        StringBuilder report = new StringBuilder();
        StringBuilder failures = new StringBuilder();
        for (int sstables : new int[]{ 1, 2 })
        {
            Measured[] narrow = measureTable(bean, sstables, 1);
            Measured[] wide = measureTable(bean, sstables, 6);
            for (int shape = 0; shape < SHAPES.length; shape++)
            {
                double extraCell = perExtraCell(narrow[shape].cursorBytes, wide[shape].cursorBytes, wide[shape].rows);
                report.append(String.format("%n  %d sstable(s), %-14s iterator %7.1f B/row  cursor %7.1f B/row  response %6.1f B/row  " +
                                            "extra cell: iterator %6.1f B  cursor %6.1f B (response %5.1f B)  cursor served=%s",
                                            sstables, SHAPES[shape],
                                            wide[shape].iteratorPerRow(), wide[shape].cursorPerRow(), wide[shape].responsePerRow(),
                                            perExtraCell(narrow[shape].iteratorBytes, wide[shape].iteratorBytes, wide[shape].rows),
                                            extraCell,
                                            perExtraCell(narrow[shape].responseBytes, wide[shape].responseBytes, wide[shape].rows),
                                            wide[shape].transcodeServed));
                String label = sstables + " sstable(s), " + SHAPES[shape] + ": ";
                if (!wide[shape].transcodeServed)
                    failures.append(label).append("the transcode path did not serve the read; ");
                if (wide[shape].cellValuesBuilt != 0)
                    failures.append(label).append("the transcode path built ").append(wide[shape].cellValuesBuilt).append(" sstable cell values; ");
                // neither a row nor an extra simple column costs an object: what is left is the
                // read's own setup, spread over its rows
                if (extraCell >= 4 && !DEFAULT_BUFFERS)
                    failures.append(label).append(String.format("an extra cell allocates %.1f B; ", extraCell));
                if (wide[shape].cursorPerRow() >= 16 && !DEFAULT_BUFFERS)
                    failures.append(label).append(String.format("a row allocates %.1f B; ", wide[shape].cursorPerRow()));
            }
        }
        logger.info("replica data read allocation, {} rows:{}", ROWS, report);
        assertEquals("", failures.toString());
    }

    private static final String[] SHAPES = { "full", "limit half", "v1 > 10" };

    private Measured[] measureTable(com.sun.management.ThreadMXBean bean, int sstables, int columns) throws Throwable
    {
        StringBuilder schema = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        for (int c = 1; c <= columns; c++)
            schema.append(", v").append(c).append(" bigint");
        createTable(schema.append(", PRIMARY KEY (pk, ck))").toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        StringBuilder insert = new StringBuilder("INSERT INTO %s (pk, ck");
        StringBuilder values = new StringBuilder(") VALUES (1, ?");
        for (int c = 1; c <= columns; c++)
        {
            insert.append(", v").append(c);
            values.append(", ?");
        }
        String statement = insert.append(values).append(")").toString();
        for (int s = 0; s < sstables; s++)
        {
            for (long ck = s; ck < ROWS; ck += sstables)
            {
                Object[] args = new Object[columns + 1];
                args[0] = ck;
                for (int c = 1; c <= columns; c++)
                    args[c] = ck * c;
                execute(statement, args);
            }
            flush();
        }

        Measured[] measured = new Measured[SHAPES.length];
        measured[0] = measure(bean, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).build(), ROWS);
        measured[1] = measure(bean, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).withLimit(ROWS / 2).build(), ROWS / 2);
        measured[2] = measure(bean, () -> (SinglePartitionReadCommand) Util.cmd(cfs, 1L).filterOn("v1", Operator.GT, 10L).build(), ROWS);
        return measured;
    }

    private static Measured measure(com.sun.management.ThreadMXBean bean, Supplier<SinglePartitionReadCommand> command, int rows) throws Exception
    {
        Measured m = new Measured(rows);
        m.iteratorBytes = allocatedBytes(bean, command, false, m);
        long served = CursorReads.transcodeResponsesServed();
        long cellValues = CursorReads.sstableCellValuesMaterialized();
        m.cursorBytes = allocatedBytes(bean, command, true, m);
        m.transcodeServed = CursorReads.transcodeResponsesServed() - served == WARMUP + MEASURED;
        m.cellValuesBuilt = CursorReads.sstableCellValuesMaterialized() - cellValues;
        return m;
    }

    /** The fewest bytes one read allocated, over the measured runs after warming up. */
    private static long allocatedBytes(com.sun.management.ThreadMXBean bean, Supplier<SinglePartitionReadCommand> command,
                                       boolean cursor, Measured into) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
        try
        {
            long tid = Thread.currentThread().getId();
            long best = Long.MAX_VALUE;
            for (int i = 0; i < WARMUP + MEASURED; i++)
            {
                SinglePartitionReadCommand read = command.get();
                long before = bean.getThreadAllocatedBytes(tid);
                ReadResponse response = ReadCommandVerbHandler.instance.doRead(read, false);
                long allocated = bean.getThreadAllocatedBytes(tid) - before;
                if (i >= WARMUP)
                {
                    long buffer = cursor && !DEFAULT_BUFFERS ? ResponseSink.responseBufferAllocation((int) responseSize(response)) : RESPONSE_BUFFER;
                    best = Math.min(best, allocated - buffer);
                }
                if (i == 0)
                    into.responseBytes = responseSize(response);
            }
            return best;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private static long responseSize(ReadResponse response) throws Exception
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, buffer, MessagingService.current_version);
            return buffer.getLength();
        }
    }

    private static double perExtraCell(long narrow, long wide, int rows)
    {
        return (wide - narrow) / (5.0 * rows);
    }

    private static final class Measured
    {
        final int rows;
        long iteratorBytes, cursorBytes, responseBytes, cellValuesBuilt;
        boolean transcodeServed;

        Measured(int rows)
        {
            this.rows = rows;
        }

        double iteratorPerRow()
        {
            return iteratorBytes / (double) rows;
        }

        double cursorPerRow()
        {
            return cursorBytes / (double) rows;
        }

        double responsePerRow()
        {
            return responseBytes / (double) rows;
        }
    }
}
