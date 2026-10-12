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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.compaction.AntiCompactionAssertions.assertCleanAbort;
import static org.apache.cassandra.db.compaction.AntiCompactionAssertions.assertNoAntiCompactionRegistered;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Cursor anticompaction stopped in the middle of a group, in each way a stop can arrive, and with
 * a flush and a regular compaction running during the merge.  A stopped anticompaction must leave
 * the originals live and intact, no other file, and nothing registered, and a second
 * anticompaction must then succeed on the cursor path.
 */
@RunWith(Parameterized.class)
public class CursorAntiCompactionLifecycleTest extends CQLTester
{
    private static final int PARTITIONS = 200;
    private static final int ROWS = 20;
    /** isStopRequested is checked for each partition and each row, so this is about half way. */
    private static final int HALF_WAY = PARTITIONS * ROWS / 2;

    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static List<Object[]> formats()
    {
        return List.of(new Object[]{ "big" }, new Object[]{ "bti" });
    }

    private SSTableFormat<?, ?> originalFormat;
    private boolean originalEnabled;

    @Before
    public void setUp()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        originalEnabled = DatabaseDescriptor.cursorRepairEnabled();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        DatabaseDescriptor.setCursorRepairEnabled(true);
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setCursorRepairEnabled(originalEnabled);
    }

    /** {@code isCancelled} turns true part way through the merge. */
    @Test
    public void cancelledInTheMiddleOfAGroup() throws Throwable
    {
        AtomicInteger calls = new AtomicInteger();
        assertStoppedCleanly(() -> calls.incrementAndGet() > HALF_WAY);
        assertTrue("the cursor did not check isCancelled often enough to stop in the middle", calls.get() > PARTITIONS);
    }

    /** A stop request on the registered compaction, part way through the merge. */
    @Test
    public void stopRequestedInTheMiddleOfAGroup() throws Throwable
    {
        AtomicInteger calls = new AtomicInteger();
        assertStoppedCleanly(() -> {
            if (calls.incrementAndGet() == HALF_WAY)
            {
                int stopped = 0;
                for (CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
                {
                    if (holder.getCompactionInfo().getTaskType() == OperationType.ANTICOMPACTION)
                    {
                        holder.stop();
                        stopped++;
                    }
                }
                assertEquals("the cursor anticompaction is not registered as an active compaction", 1, stopped);
            }
            return false;
        });
    }

    /** {@code nodetool stop ANTICOMPACTION}, part way through the merge. */
    @Test
    public void stopCompactionByType() throws Throwable
    {
        AtomicInteger calls = new AtomicInteger();
        assertStoppedCleanly(() -> {
            if (calls.incrementAndGet() == HALF_WAY)
                CompactionManager.instance.stopCompaction("ANTICOMPACTION");
            return false;
        });
    }

    /** {@code interruptCompactionFor}, as a table drop or a truncate sends it, part way through the merge. */
    @Test
    public void interruptCompactionFor() throws Throwable
    {
        AtomicInteger calls = new AtomicInteger();
        assertStoppedCleanly(() -> {
            if (calls.incrementAndGet() == HALF_WAY)
                CompactionManager.instance.interruptCompactionFor(List.of(getCurrentColumnFamilyStore().metadata()), s -> true, false);
            return false;
        });
    }

    /**
     * A flush, and a regular compaction of sstables outside the group, while the merge runs.  The
     * anticompaction must finish, route every key to the right output, and lose no row.
     */
    @Test
    public void flushAndCompactionDuringTheMerge() throws Throwable
    {
        ColumnFamilyStore cfs = writeTable();
        Set<SSTableReader> group = new HashSet<>(cfs.getLiveSSTables());
        // two more sstables outside the group, for the regular compaction to merge
        for (int round = 0; round < 2; round++)
        {
            for (long pk = PARTITIONS; pk < PARTITIONS + 20; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, (long) round, "other");
            flush();
        }
        Set<SSTableReader> others = new HashSet<>(cfs.getLiveSSTables());
        others.removeAll(group);

        AtomicInteger calls = new AtomicInteger();
        BooleanSupplier during = () -> {
            if (calls.incrementAndGet() == HALF_WAY)
            {
                for (long pk = PARTITIONS + 20; pk < PARTITIONS + 40; pk++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, "flushed during");
                flush();
                LifecycleTransaction txn = cfs.getTracker().tryModify(others, OperationType.COMPACTION);
                assertNotNull("the sstables outside the group must be free to compact", txn);
                new CompactionTask(cfs, txn, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds())).execute(CompactionManager.instance.active);
            }
            return false;
        };
        long runsBefore = CompactionManager.cursorAnticompactionsRun.sum();
        RangesAtEndpoint ranges = everyThirdPartition(cfs);
        int outputs;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(group, OperationType.ANTICOMPACTION))
        {
            assertNotNull(txn);
            outputs = CompactionManager.instance.antiCompactGroup(cfs, ranges, txn, nextTimeUUID(), during);
        }
        assertEquals("the cursor path did not run", 1, CompactionManager.cursorAnticompactionsRun.sum() - runsBefore);
        assertTrue("the flush and the compaction did not run during the merge", calls.get() > HALF_WAY);
        assertEquals("full, transient and unrepaired outputs", 3, outputs);
        assertNoAntiCompactionRegistered();
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertEquals("rows were lost", PARTITIONS * ROWS + 2 * 20 + 20, execute("SELECT * FROM %s").size());
        int pending = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (sstable.isPendingRepair())
                pending++;
        assertEquals("the full and transient outputs are pending", 2, pending);
    }

    private void assertStoppedCleanly(BooleanSupplier isCancelled) throws Throwable
    {
        ColumnFamilyStore cfs = writeTable();
        Set<SSTableReader> originals = new HashSet<>(cfs.getLiveSSTables());
        long rowsBefore = execute("SELECT * FROM %s").size();
        RangesAtEndpoint ranges = everyThirdPartition(cfs);

        long cursorRunsBefore = CompactionManager.cursorAnticompactionsRun.sum();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(originals, OperationType.ANTICOMPACTION))
        {
            assertNotNull(txn);
            CompactionManager.instance.antiCompactGroup(cfs, ranges, txn, nextTimeUUID(), isCancelled);
            fail("the anticompaction was not stopped");
        }
        catch (CompactionInterruptedException expected)
        {
            // stopped as asked
        }
        assertEquals("the cursor path did not run", 1, CompactionManager.cursorAnticompactionsRun.sum() - cursorRunsBefore);
        assertCleanAbort(cfs, originals);
        assertEquals("rows changed", rowsBefore, execute("SELECT * FROM %s").size());

        // nothing leaked: a second anticompaction of the same sstables succeeds on the cursor path
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.ANTICOMPACTION))
        {
            assertNotNull(txn);
            assertEquals(3, CompactionManager.instance.antiCompactGroup(cfs, ranges, txn, nextTimeUUID(), () -> false));
        }
        assertEquals("the second anticompaction did not take the cursor path", 2, CompactionManager.cursorAnticompactionsRun.sum() - cursorRunsBefore);
        assertNoAntiCompactionRegistered();
        assertEquals("rows changed", rowsBefore, execute("SELECT * FROM %s").size());
    }

    private ColumnFamilyStore writeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < PARTITIONS; pk++)
                for (long ck = 0; ck < ROWS; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + round);
            flush();
        }
        return cfs;
    }

    /** One-token ranges for every third partition, full and transient in turn, so the merge moves between all three outputs. */
    private static RangesAtEndpoint everyThirdPartition(ColumnFamilyStore cfs)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(FBUtilities.getBroadcastAddressAndPort());
        List<Long> tokens = new ArrayList<>();
        for (long pk = 0; pk < PARTITIONS; pk++)
            tokens.add(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(pk)).getToken().getLongValue());
        tokens.sort(Long::compare);
        for (int i = 0; i + 1 < tokens.size(); i += 3)
        {
            Range<Token> range = new Range<>(new LongToken(tokens.get(i) - 1), new LongToken(tokens.get(i)));
            builder.add(new Replica(FBUtilities.getBroadcastAddressAndPort(), range, i % 2 == 0));
        }
        return builder.build();
    }
}
