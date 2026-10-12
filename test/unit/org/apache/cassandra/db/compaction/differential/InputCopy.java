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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableListChangedNotification;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Copies of the components of each sstable in a live set, so a test can run an operation twice
 * over the same inputs.  It does not depend on {@code CQLTester}, so an in-JVM dtest instance can
 * use it too.
 */
public final class InputCopy
{
    private final List<Descriptor> descriptors = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();
    private final List<Set<Component>> components = new ArrayList<>();

    private InputCopy()
    {
    }

    /** Copies every component of every live sstable under {@code dir}. */
    public static InputCopy copyLiveSet(ColumnFamilyStore cfs, Path dir) throws IOException
    {
        InputCopy copy = new InputCopy();
        int seq = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            Path target = dir.resolve("input-" + seq++);
            Files.createDirectories(target);
            Set<Component> components = sstable.descriptor.discoverComponents();
            for (Component c : components)
                Files.copy(sstable.descriptor.fileFor(c).toPath(), target.resolve(c.name()));
            copy.descriptors.add(sstable.descriptor);
            copy.dirs.add(target);
            copy.components.add(components);
        }
        return copy;
    }

    /**
     * Drops the live set and deletes its files, then adds the copied sstables back.  Without
     * {@code sameDescriptors}, under new descriptors, which avoid any clash with the obsolete
     * inputs, whose files are deleted in the background.  With it, waits for those deletions and
     * adds the copies back under their own descriptors; a run that groups sstables in hash set
     * order then groups them the same way.
     *
     * @return the new descriptor of each copied sstable, by its descriptor when copied
     */
    public static Map<Descriptor, Descriptor> restoreLiveSet(ColumnFamilyStore cfs, InputCopy copy, boolean sameDescriptors) throws IOException
    {
        Set<SSTableReader> live = new HashSet<>(cfs.getLiveSSTables());
        List<Path> files = new ArrayList<>();
        for (SSTableReader sstable : live)
            for (Component c : sstable.descriptor.discoverComponents())
                files.add(sstable.descriptor.fileFor(c).toPath());
        cfs.getTracker().removeUnsafe(live);
        // removeUnsafe does not tell the compaction strategy, which would keep the removed sstables
        // and, under LCS, send the restored ones to level 0 for overlapping them
        cfs.getCompactionStrategyManager().handleNotification(new SSTableListChangedNotification(Collections.emptySet(), live, OperationType.UNKNOWN), cfs.getTracker());
        for (SSTableReader sstable : live)
            sstable.selfRef().release();
        for (Path f : files)
            Files.deleteIfExists(f);
        if (sameDescriptors)
            LifecycleTransaction.waitForDeletions();

        Map<Descriptor, Descriptor> restored = new HashMap<>();
        List<SSTableReader> readers = new ArrayList<>();
        for (int i = 0; i < copy.descriptors.size(); i++)
        {
            Descriptor original = copy.descriptors.get(i);
            Descriptor restoredDesc = sameDescriptors ? original : cfs.newSSTableDescriptor(original.directory, original.version);
            assertFalse("the restored sstable's files still exist: " + restoredDesc,
                        restoredDesc.fileFor(SSTableFormat.Components.DATA).exists());
            for (Component c : copy.components.get(i))
                Files.copy(copy.dirs.get(i).resolve(c.name()), restoredDesc.fileFor(c).toPath());
            readers.add(SSTableReader.open(cfs, restoredDesc));
            restored.put(original, restoredDesc);
        }
        cfs.getTracker().addInitialSSTables(readers);
        assertEquals("restore failed: live sstable count", copy.descriptors.size(), cfs.getLiveSSTables().size());
        return restored;
    }
}
