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

import java.lang.reflect.Field;
import java.util.Optional;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.SSTable;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins which value {@link BigTableWriter#estimatedPartitionSize(DecoratedKey)} picks. The writer presizes its
 * index bookkeeping from two figures: the owner's known partition size and the ceiling derived from the input
 * sstables. The rule is "the smaller of the two wins, and either alone is used when the other is absent". These
 * cases lock that selection down, which the allocation-safety tests do not.
 */
public class BigTableWriterPresizeSelectionTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void knownSmallerThanInputMaxWins() throws Exception
    {
        assertEquals(1000, select(1000, 1_000_000));
    }

    @Test
    public void staleHugeKnownLosesToInputMax() throws Exception
    {
        // A past repair can record a partition far larger than the slice these inputs hold; the input ceiling wins.
        assertEquals(50_000_000, select(1L << 40, 50_000_000));
    }

    @Test
    public void unknownFallsBackToInputMax() throws Exception
    {
        // The owner reports -1 (no knowledge), so only the input ceiling is left.
        assertEquals(12_345, select(-1, 12_345));
    }

    @Test
    public void noInputMaxFallsBackToKnown() throws Exception
    {
        // No usable input ceiling (memtable flush), so the owner's figure is used as-is.
        assertEquals(5000, select(5000, 0));
    }

    /**
     * Runs the real {@link BigTableWriter#estimatedPartitionSize(DecoratedKey)} over a mock whose owner reports
     * {@code known} and whose input-derived ceiling is {@code inputMax}. Stubbing owner() and setting the one
     * field the method reads avoids the heavy writer construction and lets each case set the two inputs directly.
     */
    private static long select(long known, long inputMax) throws Exception
    {
        SSTable.Owner owner = mock(SSTable.Owner.class);
        when(owner.getKnownPartitionSize(any())).thenReturn(known);

        BigTableWriter writer = mock(BigTableWriter.class);
        when(writer.owner()).thenReturn(Optional.of(owner));
        when(writer.estimatedPartitionSize(any())).thenCallRealMethod();
        // maxInputPartitionSize is a plain field the real method reads; the mock's no-arg construction left it
        // unset, so write it directly.
        setField(writer, "maxInputPartitionSize", inputMax);

        return writer.estimatedPartitionSize(mock(DecoratedKey.class));
    }

    private static void setField(Object target, String name, long value) throws Exception
    {
        Field field = BigTableWriter.class.getDeclaredField(name);
        field.setAccessible(true);
        field.setLong(target, value);
    }
}
