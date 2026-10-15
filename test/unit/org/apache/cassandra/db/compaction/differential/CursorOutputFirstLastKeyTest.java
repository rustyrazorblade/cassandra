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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.compaction.differential.CursorDiskBoundaryDifferentialTest.DIRECTORIES;
import static org.apache.cassandra.db.compaction.differential.CursorDiskBoundaryDifferentialTest.PARTITIONS;
import static org.apache.cassandra.db.compaction.differential.CursorDiskBoundaryDifferentialTest.boundaryPositions;
import static org.apache.cassandra.db.compaction.differential.CursorDiskBoundaryDifferentialTest.withDirectories;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Every output sstable of a cursor compaction that writes several outputs records, as its first and
 * last key, the first and last partition actually written to it. Checked on the committed reader, in
 * the stats metadata, on a reader opened again from disk, and on the reader opened early when the
 * writer switched.
 * <p>
 * Some partitions are deleted and purged, so the last partition read is often not the last one
 * written. A last key taken from another output, or from a partition that was read but not written,
 * fails here.
 */
@RunWith(Parameterized.class)
public class CursorOutputFirstLastKeyTest extends DifferentialCompactionTester
{
    /** Small enough that the 600 partitions split into many outputs. */
    private static final long MAX_SSTABLE_BYTES = 16 * 1024;

    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> formats()
    {
        return List.of(new Object[]{ "big" }, new Object[]{ "bti" });
    }

    private int originalInterval;

    @Before
    public void selectFormatAndEnableEarlyOpen()
    {
        selectSSTableFormat(format);
        // any interval enables the early open of each finished output at a writer switch
        originalInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(1);
    }

    @After
    public void restoreFormatAndOpenInterval()
    {
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalInterval);
        restoreSelectedFormat();
    }

    /** The last partitions in token order are purged, so the last one read is never written. */
    @Test
    public void sizeSplitWithPurgedPartitionsAtTheEnd() throws Throwable
    {
        List<DecoratedKey> sorted = sortedKeys();
        Set<DecoratedKey> deleted = everySeventh(sorted);
        deleted.addAll(sorted.subList(PARTITIONS - 3, PARTITIONS));

        assertOutputsRecordTheirOwnFirstAndLastKeys(deleted, null);
    }

    /** The last partition in token order is written, so the final output's last key is that partition. */
    @Test
    public void sizeSplitWithWrittenPartitionAtTheEnd() throws Throwable
    {
        List<DecoratedKey> sorted = sortedKeys();
        Set<DecoratedKey> deleted = everySeventh(sorted);
        deleted.removeAll(sorted.subList(PARTITIONS - 3, PARTITIONS));

        assertOutputsRecordTheirOwnFirstAndLastKeys(deleted, null);
    }

    /** The partitions on either side of each disk boundary are purged. */
    @Test
    public void diskBoundarySplitWithPurgedPartitionsAtEachBoundary() throws Throwable
    {
        List<DecoratedKey> sorted = sortedKeys();
        Set<DecoratedKey> deleted = everySeventh(sorted);
        for (int boundary : new int[]{ PARTITIONS / 3, 2 * PARTITIONS / 3 })
            deleted.addAll(sorted.subList(boundary - 1, boundary + 2));
        deleted.addAll(sorted.subList(0, 2));
        deleted.addAll(sorted.subList(PARTITIONS - 2, PARTITIONS));

        ColumnFamilyStore cfs = populated(deleted);
        ColumnFamilyStore bounded = withDirectories(cfs, boundaryPositions(cfs));
        assertOutputsRecordTheirOwnFirstAndLastKeys(cfs, deleted, bounded);
    }

    private void assertOutputsRecordTheirOwnFirstAndLastKeys(Set<DecoratedKey> deleted, ColumnFamilyStore bounded) throws Throwable
    {
        assertOutputsRecordTheirOwnFirstAndLastKeys(populated(deleted), deleted, bounded);
    }

    /** Splits by size when {@code bounded} is null, otherwise by the disk boundaries of {@code bounded}. */
    private void assertOutputsRecordTheirOwnFirstAndLastKeys(ColumnFamilyStore cfs, Set<DecoratedKey> deleted,
                                                             ColumnFamilyStore bounded) throws Throwable
    {
        CursorEarlyOpenTest.EarlyOpenRecord record = new CursorEarlyOpenTest.EarlyOpenRecord();
        commitThroughFactory(cfs, true, splittingTask(record, bounded));

        List<SSTableReader> outputs = new ArrayList<>(cfs.getLiveSSTables());
        if (bounded == null)
            assertTrue("the size cap must split the output to test anything, got " + outputs.size(),
                       outputs.size() >= 5);
        else
            assertEquals("one output per directory", DIRECTORIES, outputs.size());

        Set<DecoratedKey> written = new HashSet<>();
        Map<Descriptor, List<DecoratedKey>> keysByOutput = new HashMap<>();
        for (SSTableReader output : outputs)
        {
            assertOutputFormatIsSelected(output);
            List<DecoratedKey> keys = keysIn(output);
            assertFalse("output " + output.descriptor.id + " holds no partition", keys.isEmpty());
            keysByOutput.put(output.descriptor, keys);
            written.addAll(keys);
            assertFirstAndLast(cfs, output, keys);
        }

        Set<DecoratedKey> expected = new TreeSet<>(sortedKeys());
        expected.removeAll(deleted);
        assertEquals("the outputs must hold exactly the partitions that were not deleted, " +
                     "or the purge did not happen and this test proves nothing",
                     expected, new TreeSet<>(written));

        assertEarlyOpenedReadersMatch(record, keysByOutput);
    }

    /** The output's first and last key match its first and last partition, everywhere they are recorded. */
    private static void assertFirstAndLast(ColumnFamilyStore cfs, SSTableReader output, List<DecoratedKey> keys)
    {
        DecoratedKey first = keys.get(0);
        DecoratedKey last = keys.get(keys.size() - 1);
        String id = "output " + output.descriptor.id + ": ";

        assertEquals(id + "reader first key", first, output.getFirst());
        assertEquals(id + "reader last key", last, output.getLast());

        StatsMetadata stats = output.getSSTableMetadata();
        assertEquals(id + "stats metadata first key", hex(first), ByteBufferUtil.bytesToHex(stats.firstKey));
        assertEquals(id + "stats metadata last key", hex(last), ByteBufferUtil.bytesToHex(stats.lastKey));

        // the key bounds a restarted node loads from disk
        SSTableReader reopened = SSTableReader.open(cfs, output.descriptor);
        try
        {
            assertEquals(id + "first key read from disk", first, reopened.getFirst());
            assertEquals(id + "last key read from disk", last, reopened.getLast());
        }
        finally
        {
            reopened.selfRef().release();
        }
    }

    /**
     * Each output was opened early, at its writer switch or at prepare time, before it was final. The
     * latest of those readers covers the whole output, so it must carry the output's first and last key.
     * Every earlier one must carry keys of that output.
     */
    private static void assertEarlyOpenedReadersMatch(CursorEarlyOpenTest.EarlyOpenRecord record,
                                                      Map<Descriptor, List<DecoratedKey>> keysByOutput)
    {
        Map<Descriptor, CursorEarlyOpenTest.CapturedBoundary> latest = new HashMap<>();
        for (CursorEarlyOpenTest.CapturedBoundary boundary : record.boundaries)
        {
            Descriptor desc = boundary.reader.descriptor;
            List<DecoratedKey> keys = keysByOutput.get(desc);
            assertNotNull("an early-opened reader belongs to no committed output: " + desc, keys);
            assertTrue("the early-opened reader of " + desc.id + " has a last key that is not in its output",
                       keys.contains(boundary.reader.getPartitioner().decorateKey(boundary.keyWhenOpened)));
            latest.put(desc, boundary);
        }

        assertEquals("every output must be opened early at least once; " + record,
                     keysByOutput.keySet(), latest.keySet());

        for (Map.Entry<Descriptor, CursorEarlyOpenTest.CapturedBoundary> e : latest.entrySet())
        {
            List<DecoratedKey> keys = keysByOutput.get(e.getKey());
            assertEquals("the early-opened reader of " + e.getKey().id + " has the wrong first key",
                         hex(keys.get(0)), ByteBufferUtil.bytesToHex(e.getValue().firstWhenOpened));
            assertEquals("the last early-opened reader of " + e.getKey().id + " has the wrong last key",
                         hex(keys.get(keys.size() - 1)), ByteBufferUtil.bytesToHex(e.getValue().keyWhenOpened));
        }
    }

    /** The partition keys an output holds, in the order it holds them. */
    private static List<DecoratedKey> keysIn(SSTableReader output)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (ISSTableScanner scanner = output.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    keys.add(output.getPartitioner().decorateKey(ByteBufferUtil.clone(partition.partitionKey().getKey())));
                }
            }
        }
        return keys;
    }

    /** One compaction that commits several outputs and records every early-opened reader. */
    private static TaskFactory splittingTask(CursorEarlyOpenTest.EarlyOpenRecord record, ColumnFamilyStore bounded)
    {
        // past gc grace (which is zero), so the deleted partitions are purged
        long gcBefore = FBUtilities.nowInSeconds() + 3600;
        return (cfs, txn, ignored) -> new CompactionTask(cfs, txn, gcBefore, false)
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                ILifecycleTransaction recorder = new CursorEarlyOpenTest.EarlyOpenRecorder(transaction, record);
                if (bounded == null)
                    return new MaxSSTableSizeWriter(cfs, directories, recorder, nonExpiredSSTables,
                                                    MAX_SSTABLE_BYTES, 0, false);
                return new DefaultCompactionWriter(bounded, bounded.getDirectories(), recorder,
                                                   nonExpiredSSTables, false, 0);
            }
        };
    }

    /** Two rounds of rows over every partition, then a third sstable deleting {@code deleted}. */
    private ColumnFamilyStore populated(Set<DecoratedKey> deleted) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0 AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        for (int round = 0; round < 2; round++)
        {
            for (int pk = 0; pk < PARTITIONS; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, round, padding);
            flush();
        }
        for (DecoratedKey key : deleted)
            execute("DELETE FROM %s WHERE pk = ?", ByteBufferUtil.toInt(key.getKey()));
        flush();
        return cfs;
    }

    /** Every partition key of the fixture, in token order. */
    private List<DecoratedKey> sortedKeys()
    {
        List<DecoratedKey> keys = new ArrayList<>(PARTITIONS);
        for (int pk = 0; pk < PARTITIONS; pk++)
            keys.add(DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(pk)));
        keys.sort(Comparator.naturalOrder());
        return keys;
    }

    private static String hex(DecoratedKey key)
    {
        return ByteBufferUtil.bytesToHex(key.getKey());
    }

    private static Set<DecoratedKey> everySeventh(List<DecoratedKey> sorted)
    {
        Set<DecoratedKey> keys = new HashSet<>();
        for (int i = 3; i < sorted.size(); i += 7)
            keys.add(sorted.get(i));
        return keys;
    }
}
