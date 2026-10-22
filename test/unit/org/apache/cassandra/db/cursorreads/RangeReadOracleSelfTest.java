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

import java.util.EnumSet;
import java.util.function.LongFunction;

import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Proves every {@link RangeReadOracle} surface can fail, on cursor range reads: a production
 * corruption flag must trip S1, the test-side paging-state hook must trip S3, a read that differs
 * only on the cursor run must trip S2, S4 and S5, and S6 must trip on a range read the cursor path
 * does not serve, on a bloom filter lookup and on a key cache change.  The class fails if a surface
 * was never tripped.
 */
public class RangeReadOracleSelfTest extends RangeReadOracle
{
    private static final EnumSet<Surface> tripped = EnumSet.noneOf(Surface.class);

    @AfterClass
    public static void everySurfaceTripped()
    {
        assertEquals("every surface must be shown to fail", EnumSet.allOf(Surface.class), tripped);
    }

    private void expectDivergence(Surface surface, Runnable comparison)
    {
        try
        {
            comparison.run();
        }
        catch (Divergence d)
        {
            assertTrue("expected " + surface + " to trip, got " + d.surfaces + ": " + d.getMessage(), d.surfaces.contains(surface));
            tripped.add(surface);
            return;
        }
        fail("expected a divergence on " + surface);
    }

    private ColumnFamilyStore table(String format) throws Throwable
    {
        SSTableFormat<?, ?> was = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        try
        {
            createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int k = 0; k < 20; k++)
                for (int c = 0; c < 5; c++)
                    execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k, c, k * c);
            flush();
            return cfs;
        }
        finally
        {
            DatabaseDescriptor.setSelectedSSTableFormat(was);
        }
    }

    private static LongFunction<PartitionRangeReadCommand> fullRing(ColumnFamilyStore cfs)
    {
        return now -> PartitionRangeReadCommand.create(cfs.metadata(), now, ColumnFilter.all(cfs.metadata()), RowFilter.none(),
                                                       DataLimits.NONE, DataRange.allData(cfs.metadata().partitioner));
    }

    @Test
    public void cleanRangeReadPasses() throws Throwable
    {
        ColumnFamilyStore cfs = table("bti");
        assertExecuteLocallyMatches(ReadCase.of("clean", 2), cfs, fullRing(cfs));
    }

    @Test
    public void corruptCellTimestampsTripsS1() throws Throwable
    {
        ColumnFamilyStore cfs = table("bti");
        CursorReads.TEST_CORRUPT_CELL_TIMESTAMPS = true;
        expectDivergence(Surface.S1, () -> assertExecuteLocallyMatches(ReadCase.of("corrupt cell timestamps", 2), cfs, fullRing(cfs)));
    }

    @Test
    public void cursorOnlyBytesTripS2() throws Throwable
    {
        ColumnFamilyStore cfs = table("bti");
        expectDivergence(Surface.S2, () -> compareCustom(ReadCase.of("cursor-only response byte", 2), cfs, EnumSet.of(Surface.S2), into -> {
            into.bytes.put("response", new byte[]{ (byte) (into.cursor ? 1 : 0) });
        }));
    }

    @Test
    public void corruptPagingStateTripsS3() throws Throwable
    {
        table("bti");
        TEST_CORRUPT_CURSOR_PAGING_STATE = true;
        expectDivergence(Surface.S3, () -> assertCqlMatches(ReadCase.of("corrupt paging state", 2), getCurrentColumnFamilyStore(),
                                                            "SELECT * FROM %s", 7));
    }

    @Test
    public void extraRangeReadTripsS4() throws Throwable
    {
        ColumnFamilyStore cfs = table("bti");
        expectDivergence(Surface.S4, () -> compareCustom(ReadCase.of("one more range read on the cursor run", 2), cfs, EnumSet.noneOf(Surface.class), into -> {
            int reads = into.cursor ? 2 : 1;
            for (int i = 0; i < reads; i++)
            {
                PartitionRangeReadCommand command = fullRing(cfs).apply(0);
                try (ReadExecutionController controller = command.executionController();
                     UnfilteredPartitionIterator partitions = command.executeLocally(controller))
                {
                    canonicalRecords(partitions);
                }
            }
        }));
    }

    @Test
    public void oneSidedFailureTripsS5() throws Throwable
    {
        ColumnFamilyStore cfs = table("bti");
        expectDivergence(Surface.S5, () -> compareCustom(ReadCase.of("cursor-only failure", 2), cfs, EnumSet.noneOf(Surface.class), into -> {
            if (into.cursor)
                throw new IllegalStateException("cursor only");
        }));
    }

    @Test
    public void unservedRangeReadTripsS6() throws Throwable
    {
        ColumnFamilyStore cfs = table("bti");
        // the cursor run reads with the cursor path off, so it serves nothing
        expectDivergence(Surface.S6, () -> compareCustom(ReadCase.of("range read left to the iterator path", 2), cfs, EnumSet.noneOf(Surface.class), into -> {
            setRangePath(false);
            PartitionRangeReadCommand command = fullRing(cfs).apply(0);
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller))
            {
                canonicalRecords(partitions);
            }
        }));
    }

    @Test
    public void servedReadExpectedNotToBeServedTripsS6() throws Throwable
    {
        ColumnFamilyStore cfs = table("bti");
        expectDivergence(Surface.S6, () -> assertExecuteLocallyMatches(ReadCase.of("served", 2).notServedBecause(CursorRangeReads.Unsupported.MATERIALIZED_VIEW), cfs, fullRing(cfs)));
    }

    @Test
    public void bloomFilterLookupTripsS6() throws Throwable
    {
        ColumnFamilyStore cfs = table("bti");
        expectDivergence(Surface.S6, () -> compareCustom(ReadCase.of("partition lookup on the cursor run", 2), cfs,
                                                         EnumSet.noneOf(Surface.class), into -> {
            if (into.cursor)
                Util.getAll(Util.cmd(cfs, 3).build());
        }));
    }

    @Test
    public void keyCacheChangeTripsS6() throws Throwable
    {
        ColumnFamilyStore cfs = table("big");
        expectDivergence(Surface.S6, () -> compareCustom(ReadCase.of("key cache fill on the cursor run", 2), cfs,
                                                         EnumSet.noneOf(Surface.class), into -> {
            if (into.cursor)
            {
                setRangePath(false);
                SinglePartitionReadCommand read = (SinglePartitionReadCommand) Util.cmd(cfs, 4).build();
                Util.getAll(read);
            }
        }));
    }
}
