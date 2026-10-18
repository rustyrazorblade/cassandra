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

import java.lang.reflect.Field;

import org.junit.Before;
import org.junit.BeforeClass;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.SlabPool;

import static org.junit.Assert.assertTrue;

/**
 * The full {@link MemtableMergeCursorReadDifferentialTest} corpus under
 * {@code memtable_allocation_type: offheap_buffers}: the memtable's cell values, cell paths and
 * clusterings are slices of off-heap slab buffers ({@code SlabAllocator}), and the iterator path
 * clones them to heap ({@code EnsureOnHeap.CloneToHeap}).  Same config override as
 * {@link OffheapObjectsMemtableMergeCursorReadDifferentialTest}.
 */
public class OffheapBuffersMemtableMergeCursorReadDifferentialTest extends MemtableMergeCursorReadDifferentialTest
{
    // Shadows CQLTester.setUpClass so it runs INSTEAD of it (JUnit does not run shadowed static
    // @BeforeClass methods), letting the config change land before the memtable pool exists.
    @BeforeClass
    public static void setUpClass()
    {
        daemonInitialization();
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            Config conf = (Config) confField.get(null);
            conf.memtable_allocation_type = Config.MemtableAllocationType.offheap_buffers;
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        prePrepareServer();
        prepareServer();
    }

    @Before
    public void assertOffheapPool()
    {
        MemtablePool pool = AbstractAllocatorMemtable.MEMORY_POOL;
        assertTrue("memtable pool is " + pool.getClass().getSimpleName() + ", not SlabPool — " +
                   "the offheap_buffers config did not take effect (JVM reuse?); this suite would " +
                   "silently re-run the heap_buffers corpus",
                   pool instanceof SlabPool && pool.offHeap.limit > 0);
    }
}
