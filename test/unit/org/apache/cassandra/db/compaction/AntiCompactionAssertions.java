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

package org.apache.cassandra.db.compaction;

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Checks shared by the anticompaction tests that stop or fail an anticompaction. */
public final class AntiCompactionAssertions
{
    private AntiCompactionAssertions()
    {
    }

    /**
     * After an anticompaction that stopped or failed: the originals are the live set, each with
     * one reference and not marked compacted or pending; nothing is compacting; no anticompaction
     * is registered; and every file in the table directories belongs to a live sstable, with no
     * transaction log left.
     */
    public static void assertCleanAbort(ColumnFamilyStore cfs, Set<SSTableReader> originals)
    {
        LifecycleTransaction.waitForDeletions();
        Set<Descriptor> originalDescriptors = new HashSet<>();
        for (SSTableReader sstable : originals)
            originalDescriptors.add(sstable.descriptor);
        Set<Descriptor> liveDescriptors = new HashSet<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            liveDescriptors.add(sstable.descriptor);
            assertFalse("an original is marked compacted: " + sstable, sstable.isMarkedCompacted());
            assertFalse("an original was given a pending repair: " + sstable, sstable.isPendingRepair());
            assertEquals("references to " + sstable, 1, sstable.selfRef().globalCount());
        }
        assertEquals("the live set is not the originals", originalDescriptors, liveDescriptors);
        assertTrue("sstables are still marked compacting: " + cfs.getTracker().getCompacting(), cfs.getTracker().getCompacting().isEmpty());
        assertNoAntiCompactionRegistered();
        assertEveryFileBelongsToALiveSSTable(cfs);
    }

    /** Every file in the table directories belongs to a live sstable, and no transaction log is left. */
    public static void assertEveryFileBelongsToALiveSSTable(ColumnFamilyStore cfs)
    {
        LifecycleTransaction.waitForDeletions();
        Set<Descriptor> liveDescriptors = new HashSet<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            liveDescriptors.add(sstable.descriptor);
        for (File directory : cfs.getDirectories().getCFDirectories())
        {
            for (File file : directory.tryList())
            {
                if (file.isDirectory())
                    continue;
                assertFalse("a transaction log was left: " + file, file.name().contains("_txn_"));
                Descriptor descriptor = Descriptor.fromFileWithComponent(file, false).left;
                assertTrue("a file does not belong to a live sstable: " + file, liveDescriptors.contains(descriptor));
            }
        }
    }

    public static void assertNoAntiCompactionRegistered()
    {
        for (CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
            assertFalse("an anticompaction is still registered: " + holder,
                        holder.getCompactionInfo().getTaskType() == OperationType.ANTICOMPACTION);
    }
}
