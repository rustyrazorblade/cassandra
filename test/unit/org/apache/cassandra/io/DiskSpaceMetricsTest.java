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

package org.apache.cassandra.io;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.MovingAverage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.psjava.util.AssertStatus.assertTrue;

public class DiskSpaceMetricsTest extends CQLTester
{
    /**
     * This test runs the system with normal operations and makes sure the disk metrics match reality
     */
    @Test
    public void baseline() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, PRIMARY KEY (pk)) WITH min_index_interval=1");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // disable compaction so nothing changes between calculations
        cfs.disableAutoCompaction();

        // create 100 sstables
        for (int i = 0; i < 100; i++)
            insert(cfs, i);
        assertDiskSpaceEqual(cfs);
    }


    @Test
    public void testFlushSize() throws Throwable
    {
        createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk bigint, PRIMARY KEY (pk))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore(KEYSPACE_PER_TEST);
        assertTrue(Double.isNaN(cfs.metric.flushSizeOnDisk.get()));

        // disable compaction so nothing changes between calculations
        cfs.disableAutoCompaction();

        for (int i = 0; i < 3; i++)
            insertN(KEYSPACE_PER_TEST, cfs, 1000, 55);

        final List<SSTableReader> liveSSTables = cfs.getLiveSSTables().stream()
                                                    .sorted(SSTableReader.idComparator)
                                                    .collect(Collectors.toList());
        MovingAverage expectedMetrics = ExpMovingAverage.decayBy1000();
        for (SSTableReader rdr : liveSSTables)
            expectedMetrics.update(rdr.onDiskLength());
        assertThat(cfs.metric.flushSizeOnDisk.get()).isEqualTo(expectedMetrics.get());
    }

    private void insert(ColumnFamilyStore cfs, long value) throws Throwable
    {
        insertN(cfs, 1, value);
    }

    private void insertN(ColumnFamilyStore cfs, int n, long base) throws Throwable
    {
        insertN(KEYSPACE, cfs, n, base);
    }

    private void insertN(String keyspace, ColumnFamilyStore cfs, int n, long base) throws Throwable
    {
        for (int i = 0; i < n; i++)
            executeFormattedQuery(formatQuery(keyspace, "INSERT INTO %s (pk) VALUES (?)"), base + i);

        // flush to write the sstable
        Util.flush(cfs);
    }

    private void assertDiskSpaceEqual(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> liveSSTables = cfs.getTracker().getView().liveSSTables();
        long liveDiskSpaceUsed = cfs.metric.liveDiskSpaceUsed.getCount();
        long actual = liveSSTables.stream().mapToLong(SSTableReader::bytesOnDisk).sum();
        long uncompressedLiveDiskSpaceUsed = cfs.metric.uncompressedLiveDiskSpaceUsed.getCount();
        long actualUncompressed = liveSSTables.stream().mapToLong(SSTableReader::logicalBytesOnDisk).sum();

        assertEquals("bytes on disk does not match current metric LiveDiskSpaceUsed", actual, liveDiskSpaceUsed);
        assertEquals("bytes on disk does not match current metric UncompressedLiveDiskSpaceUsed", actualUncompressed, uncompressedLiveDiskSpaceUsed);

        // Keyspace-level metrics should be equivalent to table-level metrics, as there is only one table.
        assertEquals(cfs.keyspace.metric.liveDiskSpaceUsed.getValue().longValue(), liveDiskSpaceUsed);
        assertEquals(cfs.keyspace.metric.uncompressedLiveDiskSpaceUsed.getValue().longValue(), uncompressedLiveDiskSpaceUsed);
        assertEquals(cfs.keyspace.metric.unreplicatedLiveDiskSpaceUsed.getValue().longValue(), liveDiskSpaceUsed);
        assertEquals(cfs.keyspace.metric.unreplicatedUncompressedLiveDiskSpaceUsed.getValue().longValue(), uncompressedLiveDiskSpaceUsed);

        // Global load metrics should be internally consistent, given there is no replication, but slightly greater
        // than table and keyspace-level metrics, given the global versions account for non-user tables.
        long globalLoad = StorageMetrics.load.getCount();
        assertEquals(globalLoad, StorageMetrics.unreplicatedLoad.getValue().longValue());
        assertThat(globalLoad).isGreaterThan(liveDiskSpaceUsed);

        long globalUncompressedLoad = StorageMetrics.uncompressedLoad.getCount();
        assertEquals(globalUncompressedLoad, StorageMetrics.unreplicatedUncompressedLoad.getValue().longValue());
        assertThat(globalUncompressedLoad).isGreaterThan(uncompressedLiveDiskSpaceUsed);

        // totalDiskSpaceUsed is based off SStable delete, which is async: LogTransaction's tidy enqueues in ScheduledExecutors.nonPeriodicTasks
        // wait for there to be no more pending sstable releases
        LifecycleTransaction.waitForDeletions();
        long totalDiskSpaceUsed = cfs.metric.totalDiskSpaceUsed.getCount();
        assertEquals("bytes on disk does not match current metric totalDiskSpaceUsed", actual, totalDiskSpaceUsed);
    }

}
