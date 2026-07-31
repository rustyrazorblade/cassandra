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

package org.apache.cassandra.index.sai.disk.v1;

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.AbstractPrimaryKeyTester;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link SkinnyPrimaryKeyMap#primaryKeyFromRowId(long)} (and its inherited use by
 * {@link WidePrimaryKeyMap}) round-trips the {@link org.apache.cassandra.dht.Token} of every row exactly,
 * including via {@code rowIdToTokenArray}-derived token construction rather than re-hashing key bytes.
 */
public class PrimaryKeyMapTest extends AbstractPrimaryKeyTester
{
    @Test
    public void skinnyPrimaryKeyFromRowIdRoundTripsToken() throws Throwable
    {
        IndexDescriptor indexDescriptor = newClusteringIndexDescriptor(simplePartition);

        SSTableComponentsWriter writer = new SSTableComponentsWriter(indexDescriptor);

        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, simplePartition.comparator);

        int rows = nextInt(1000, 5000);
        PrimaryKey[] keys = new PrimaryKey[rows];
        for (int index = 0; index < rows; index++)
            keys[index] = factory.create(makeKey(simplePartition, index));

        Arrays.sort(keys);

        for (PrimaryKey primaryKey : keys)
        {
            writer.startPartition(primaryKey.partitionKey());
            writer.nextRow(primaryKey);
        }

        writer.complete();

        try (PrimaryKeyMap.Factory mapFactory = new SkinnyPrimaryKeyMap.Factory(indexDescriptor);
             PrimaryKeyMap primaryKeyMap = mapFactory.newPerSSTablePrimaryKeyMap())
        {
            for (int rowId = 0; rowId < rows; rowId++)
            {
                DecoratedKey expected = keys[rowId].partitionKey();
                PrimaryKey actual = primaryKeyMap.primaryKeyFromRowId(rowId);

                assertEquals("partition key mismatch at rowId " + rowId, expected, actual.partitionKey());
                assertEquals("token mismatch at rowId " + rowId, expected.getToken(), actual.partitionKey().getToken());
            }
        }
    }

    @Test
    public void widePrimaryKeyFromRowIdRoundTripsToken() throws Throwable
    {
        IndexDescriptor indexDescriptor = newClusteringIndexDescriptor(compositePartitionMultipleClusteringAsc);

        SSTableComponentsWriter writer = new SSTableComponentsWriter(indexDescriptor);

        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, compositePartitionMultipleClusteringAsc.comparator);

        int rows = nextInt(1000, 5000);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int partitionSize = nextInt(5, 500);
        int partitionCounter = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(compositePartitionMultipleClusteringAsc, partition, partition),
                                         makeClustering(compositePartitionMultipleClusteringAsc,
                                                        getRandom().nextTextString(10, 100),
                                                        getRandom().nextTextString(10, 100)));
            partitionCounter++;
            if (partitionCounter == partitionSize)
            {
                partition++;
                partitionCounter = 0;
                partitionSize = nextInt(5, 500);
            }
        }

        Arrays.sort(keys);

        DecoratedKey lastKey = null;
        for (PrimaryKey primaryKey : keys)
        {
            if (lastKey == null || lastKey.compareTo(primaryKey.partitionKey()) < 0)
            {
                lastKey = primaryKey.partitionKey();
                writer.startPartition(lastKey);
            }
            writer.nextRow(primaryKey);
        }

        writer.complete();

        SSTableReader sstableReader = mock(SSTableReader.class);
        when(sstableReader.metadata()).thenReturn(compositePartitionMultipleClusteringAsc);

        try (PrimaryKeyMap.Factory mapFactory = new WidePrimaryKeyMap.Factory(indexDescriptor, sstableReader);
             PrimaryKeyMap primaryKeyMap = mapFactory.newPerSSTablePrimaryKeyMap())
        {
            for (int rowId = 0; rowId < rows; rowId++)
            {
                DecoratedKey expected = keys[rowId].partitionKey();
                PrimaryKey actual = primaryKeyMap.primaryKeyFromRowId(rowId);

                assertEquals("partition key mismatch at rowId " + rowId, expected, actual.partitionKey());
                assertEquals("token mismatch at rowId " + rowId, expected.getToken(), actual.partitionKey().getToken());
            }
        }
    }
}
