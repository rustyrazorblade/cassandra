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

package org.apache.cassandra.io.sstable.format.big;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The size hint {@link BigTableWriter} derives from its input sstables. The hint is only an optimisation, so the
 * important cases are the ones where it must decline to guess: no inputs, and an input whose partition-size
 * histogram has overflowed and can only report "larger than the last bucket".
 */
public class BigTableWriterMaxPartitionSizeTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void noInputsGivesNoEstimate()
    {
        assertEquals(-1, BigTableWriter.maxEstimatedPartitionSize(txnOver()));
    }

    @Test
    public void takesTheLargestInputMax()
    {
        EstimatedHistogram small = histogramOf(500);
        EstimatedHistogram large = histogramOf(50_000_000);
        long expected = Math.max(small.max(), large.max());

        assertEquals(expected, BigTableWriter.maxEstimatedPartitionSize(
            txnOver(readerWithPartitionSize(small), readerWithPartitionSize(large))));
    }

    @Test
    public void overflowedInputGivesNoEstimate()
    {
        EstimatedHistogram overflowed = histogramOf(Long.MAX_VALUE);
        assertEquals("an overflowed histogram must report Long.MAX_VALUE", Long.MAX_VALUE, overflowed.max());

        // The small input is seen first, so a max has already accumulated when the overflowed one forces the -1.
        assertEquals(-1, BigTableWriter.maxEstimatedPartitionSize(
            txnOver(readerWithPartitionSize(histogramOf(500)), readerWithPartitionSize(overflowed))));
    }

    private static EstimatedHistogram histogramOf(long value)
    {
        EstimatedHistogram histogram = new EstimatedHistogram(155);
        histogram.add(value);
        return histogram;
    }

    private static SSTableReader readerWithPartitionSize(EstimatedHistogram partitionSize)
    {
        SSTableReader reader = mock(SSTableReader.class);
        when(reader.getSSTableMetadata()).thenReturn(statsWithPartitionSize(partitionSize));
        return reader;
    }

    private static ILifecycleTransaction txnOver(SSTableReader... readers)
    {
        ILifecycleTransaction txn = mock(ILifecycleTransaction.class);
        when(txn.originals()).thenReturn(new LinkedHashSet<>(Arrays.asList(readers)));
        return txn;
    }

    private static StatsMetadata statsWithPartitionSize(EstimatedHistogram partitionSize)
    {
        return new StatsMetadata(partitionSize,
                                 new EstimatedHistogram(),
                                 IntervalSet.empty(),
                                 Long.MIN_VALUE,
                                 Long.MAX_VALUE,
                                 Integer.MAX_VALUE,
                                 Integer.MAX_VALUE,
                                 0,
                                 Integer.MAX_VALUE,
                                 MetadataCollector.NO_COMPRESSION_RATIO,
                                 TombstoneHistogram.createDefault(),
                                 0,
                                 Collections.emptyList(),
                                 Slice.ALL,
                                 true,
                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                 -1,
                                 -1,
                                 Double.NaN,
                                 null,
                                 null,
                                 false,
                                 true,
                                 ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                 ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }
}
