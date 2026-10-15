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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(BMUnitRunner.class)
public class AntiCompactionBytemanTest extends CQLTester
{
    @Test
    @BMRules(rules = { @BMRule(name = "Insert delay after first prepareToCommit",
             targetClass = "CompactionManager",
             targetMethod = "antiCompactGroup",
             condition = "not flagged(\"done\")",
             targetLocation = "AFTER INVOKE prepareToCommit",
             action = "Thread.sleep(2000L);") } ) // We need to add the unit to Thread.sleep calls in ByteBuddy now
    public void testRedundantTransitions() throws Throwable
    {
        createTable("create table %s (id int primary key, i int)");
        execute("insert into %s (id, i) values (1, 1)");
        execute("insert into %s (id, i) values (2, 1)");
        execute("insert into %s (id, i) values (3, 1)");
        flush();
        UntypedResultSet res = execute("select token(id) as tok from %s");
        Iterator<UntypedResultSet.Row> it = res.iterator();
        List<Long> tokens = new ArrayList<>();
        while (it.hasNext())
        {
            UntypedResultSet.Row r = it.next();
            tokens.add(r.getLong("tok"));
        }
        tokens.sort(Long::compareTo);

        long first = tokens.get(0) - 10;
        long last = tokens.get(0) + 10;
        Range<Token> toRepair = new Range<>(new Murmur3Partitioner.LongToken(first), new Murmur3Partitioner.LongToken(last));
        first = tokens.get(1) - 10;
        last = tokens.get(1) + 10;
        Range<Token> pending = new Range<>(new Murmur3Partitioner.LongToken(first), new Murmur3Partitioner.LongToken(last));

        RangesAtEndpoint ranges = new RangesAtEndpoint.Builder(FBUtilities.getBroadcastAddressAndPort()).add(Replica.fullReplica(FBUtilities.getBroadcastAddressAndPort(), toRepair))
                                                                                                        .add(Replica.transientReplica(InetAddressAndPort.getByName("127.0.0.1"), pending)).build();

        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicBoolean finished = new AtomicBoolean(false);

        Thread t = new Thread(() -> {
            while (!finished.get())
            {
                UntypedResultSet result = null;
                try
                {
                    result = execute("select id from %s");
                }
                catch (Throwable throwable)
                {
                    failed.set(true);
                    throw new RuntimeException(throwable);
                }

                Iterator<UntypedResultSet.Row> rowIter = result.iterator();
                Set<Integer> ids = new HashSet<>();
                while (rowIter.hasNext())
                {
                    UntypedResultSet.Row r = rowIter.next();
                    ids.add(r.getInt("id"));
                }
                if (!Sets.newHashSet(1,2,3).equals(ids))
                {
                    failed.set(true);
                    return;
                }
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            }
        });
        t.start();
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        SSTableReader sstableBefore = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();

        try (LifecycleTransaction txn = getCurrentColumnFamilyStore().getTracker().tryModify(getCurrentColumnFamilyStore().getLiveSSTables(), OperationType.ANTICOMPACTION))
        {
            CompactionManager.instance.antiCompactGroup(getCurrentColumnFamilyStore(), ranges, txn, nextTimeUUID(), () -> false);
        }
        finished.set(true);
        t.join();
        assertFalse(failed.get());
        assertFalse(getCurrentColumnFamilyStore().getLiveSSTables().contains(sstableBefore));
        Util.assertOnDiskState(getCurrentColumnFamilyStore(), 3);
    }

    /**
     * A failure in the middle of the cursor merge, after the merge has moved between outputs a few
     * times, leaves the originals live and intact and no output on disk.
     */
    @Test
    @BMRule(name = "Fail the cursor merge in the middle (BIG)",
            targetClass = "CursorCompactor",
            targetMethod = "selectOutput",
            condition = "!flagged(\"merge-big\") && incrementCounter($0) == 10",
            action = "flag(\"merge-big\"); throw new java.lang.RuntimeException(\"injected cursor merge failure\");")
    public void cursorMergeFailureBig() throws Throwable
    {
        assertFailureLeavesOriginals("big", 100, 1, "injected cursor merge failure");
    }

    @Test
    @BMRule(name = "Fail the cursor merge in the middle (BTI)",
            targetClass = "CursorCompactor",
            targetMethod = "selectOutput",
            condition = "!flagged(\"merge-bti\") && incrementCounter($0) == 10",
            action = "flag(\"merge-bti\"); throw new java.lang.RuntimeException(\"injected cursor merge failure\");")
    public void cursorMergeFailureBti() throws Throwable
    {
        assertFailureLeavesOriginals("bti", 100, 1, "injected cursor merge failure");
    }

    /** prepareToCommit throws after the cursor merge has written all three outputs. */
    @Test
    @BMRule(name = "Fail prepareToCommit (BIG)",
            targetClass = "SSTableRewriter",
            targetMethod = "doPrepare",
            condition = "!flagged(\"prepare-big\")",
            action = "flag(\"prepare-big\"); throw new java.lang.RuntimeException(\"injected prepareToCommit failure\");")
    public void prepareToCommitFailureBig() throws Throwable
    {
        assertFailureLeavesOriginals("big", 100, 1, "injected prepareToCommit failure");
    }

    @Test
    @BMRule(name = "Fail prepareToCommit (BTI)",
            targetClass = "SSTableRewriter",
            targetMethod = "doPrepare",
            condition = "!flagged(\"prepare-bti\")",
            action = "flag(\"prepare-bti\"); throw new java.lang.RuntimeException(\"injected prepareToCommit failure\");")
    public void prepareToCommitFailureBti() throws Throwable
    {
        assertFailureLeavesOriginals("bti", 100, 1, "injected prepareToCommit failure");
    }

    /** The disk fills up while the cursor merge writes: the write fails with a write error. */
    @Test
    @BMRule(name = "Disk full in the cursor merge (BIG)",
            targetClass = "SSTableCursorWriter",
            targetMethod = "writePartitionEnd",
            condition = "!flagged(\"disk-big\") && incrementCounter($0) == 20",
            action = "flag(\"disk-big\"); throw new org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"No space left on device\"));")
    public void diskFullBig() throws Throwable
    {
        assertFailureLeavesOriginals("big", 100, 1, null);
    }

    @Test
    @BMRule(name = "Disk full in the cursor merge (BTI)",
            targetClass = "SSTableCursorWriter",
            targetMethod = "writePartitionEnd",
            condition = "!flagged(\"disk-bti\") && incrementCounter($0) == 20",
            action = "flag(\"disk-bti\"); throw new org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"No space left on device\"));")
    public void diskFullBti() throws Throwable
    {
        assertFailureLeavesOriginals("bti", 100, 1, null);
    }

    /** A failure inside a wide partition, with its row index half built. */
    @Test
    @BMRule(name = "Fail inside a wide partition (BIG)",
            targetClass = "SSTableCursorWriter",
            targetMethod = "writeRowStart",
            condition = "!flagged(\"wide-big\") && incrementCounter($0) == 700",
            action = "flag(\"wide-big\"); throw new java.lang.RuntimeException(\"injected failure inside a wide partition\");")
    public void failureInsideWidePartitionBig() throws Throwable
    {
        assertFailureLeavesOriginals("big", 4, 2000, "injected failure inside a wide partition");
    }

    @Test
    @BMRule(name = "Fail inside a wide partition (BTI)",
            targetClass = "SSTableCursorWriter",
            targetMethod = "writeRowStart",
            condition = "!flagged(\"wide-bti\") && incrementCounter($0) == 700",
            action = "flag(\"wide-bti\"); throw new java.lang.RuntimeException(\"injected failure inside a wide partition\");")
    public void failureInsideWidePartitionBti() throws Throwable
    {
        assertFailureLeavesOriginals("bti", 4, 2000, "injected failure inside a wide partition");
    }

    /**
     * An I/O error while writing an output fails with the same type on both paths, so that
     * {@code disk_failure_policy} acts the same way.  The legacy writer turns it into an
     * {@code FSWriteError} for its data file; the cursor path names the output sstable.
     */
    @Test
    @BMRules(rules = { @BMRule(name = "I/O error in the legacy writer (BIG)",
                               targetClass = "SortedTableWriter",
                               targetMethod = "endPartition",
                               condition = "callerEquals(\"antiCompactGroup\", 40) && !flagged(\"legacy-io-big\") && incrementCounter(\"legacy-io-big\") == 5",
                               action = "flag(\"legacy-io-big\"); throw new java.io.IOException(\"injected write failure\");"),
                       @BMRule(name = "I/O error in the cursor writer (BIG)",
                               targetClass = "SSTableCursorWriter",
                               targetMethod = "writePartitionEnd",
                               condition = "callerEquals(\"antiCompactGroup\", 40) && !flagged(\"cursor-io-big\") && incrementCounter(\"cursor-io-big\") == 5",
                               action = "flag(\"cursor-io-big\"); throw new java.io.IOException(\"injected write failure\");") })
    public void writeIOErrorTypeMatchesLegacyBig() throws Throwable
    {
        assertWriteIOErrorTypeMatchesLegacy("big");
    }

    @Test
    @BMRules(rules = { @BMRule(name = "I/O error in the legacy writer (BTI)",
                               targetClass = "SortedTableWriter",
                               targetMethod = "endPartition",
                               condition = "callerEquals(\"antiCompactGroup\", 40) && !flagged(\"legacy-io-bti\") && incrementCounter(\"legacy-io-bti\") == 5",
                               action = "flag(\"legacy-io-bti\"); throw new java.io.IOException(\"injected write failure\");"),
                       @BMRule(name = "I/O error in the cursor writer (BTI)",
                               targetClass = "SSTableCursorWriter",
                               targetMethod = "writePartitionEnd",
                               condition = "callerEquals(\"antiCompactGroup\", 40) && !flagged(\"cursor-io-bti\") && incrementCounter(\"cursor-io-bti\") == 5",
                               action = "flag(\"cursor-io-bti\"); throw new java.io.IOException(\"injected write failure\");") })
    public void writeIOErrorTypeMatchesLegacyBti() throws Throwable
    {
        assertWriteIOErrorTypeMatchesLegacy("bti");
    }

    private void assertWriteIOErrorTypeMatchesLegacy(String format) throws Throwable
    {
        boolean originalEnabled = DatabaseDescriptor.cursorRepairEnabled();
        SSTableFormat<?, ?> originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        try
        {
            createTable("create table %s (id bigint, ck bigint, i int, primary key (id, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int round = 0; round < 2; round++)
            {
                for (long id = 0; id < 50; id++)
                    execute("insert into %s (id, ck, i) values (?, ?, ?)", id, (long) round, round);
                flush();
            }
            Set<SSTableReader> originals = new HashSet<>(cfs.getLiveSSTables());
            RangesAtEndpoint ranges = RangesAtEndpoint.of(new Replica(FBUtilities.getBroadcastAddressAndPort(),
                                                                      new Range<>(new Murmur3Partitioner.LongToken(Long.MIN_VALUE),
                                                                                  new Murmur3Partitioner.LongToken(0)),
                                                                      true));
            Throwable legacy = writeFailure(cfs, ranges, false);
            AntiCompactionAssertions.assertCleanAbort(cfs, originals);
            Throwable cursor = writeFailure(cfs, ranges, true);
            AntiCompactionAssertions.assertCleanAbort(cfs, originals);

            assertEquals("the two paths failed with different exception types; legacy " + legacy + ", cursor " + cursor,
                         legacy.getClass(), cursor.getClass());
            assertEquals("injected write failure", cursor.getCause().getMessage());
            String path = ((FSWriteError) cursor).path;
            // the output sstable, not one of its component files, as any component writer may fail
            assertTrue("the write error does not name an output sstable of the table: " + path,
                       path.contains(cfs.getTableName()) && path.endsWith("-" + format));
        }
        finally
        {
            DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
            DatabaseDescriptor.setCursorRepairEnabled(originalEnabled);
        }
    }

    /** Anticompacts the live set on one path and returns what it threw; fails if it threw nothing. */
    private static Throwable writeFailure(ColumnFamilyStore cfs, RangesAtEndpoint ranges, boolean cursor)
    {
        DatabaseDescriptor.setCursorRepairEnabled(cursor);
        long cursorRunsBefore = CompactionManager.cursorAnticompactionsRun.sum();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.ANTICOMPACTION))
        {
            CompactionManager.instance.antiCompactGroup(cfs, ranges, txn, nextTimeUUID(), () -> false);
        }
        catch (Throwable t)
        {
            assertEquals(cursor ? "the cursor path did not run" : "the cursor path ran", cursor ? 1 : 0,
                         CompactionManager.cursorAnticompactionsRun.sum() - cursorRunsBefore);
            return t;
        }
        throw new AssertionError("the injected write failure did not reach the caller; cursor=" + cursor);
    }

    /**
     * Anticompacts on the cursor path a table that the rule makes fail once, checks that the
     * failure left only the originals, then anticompacts again and checks that it succeeds.
     *
     * @param message the message of the injected failure, or null to accept any
     */
    private void assertFailureLeavesOriginals(String format, int partitions, int rowsPerPartition, String message) throws Throwable
    {
        boolean originalEnabled = DatabaseDescriptor.cursorRepairEnabled();
        SSTableFormat<?, ?> originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setCursorRepairEnabled(true);
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        try
        {
            createTable("create table %s (id bigint, ck bigint, i int, primary key (id, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int round = 0; round < 2; round++)
            {
                for (long id = 0; id < partitions; id++)
                    for (long ck = round; ck < rowsPerPartition; ck += 2)
                        execute("insert into %s (id, ck, i) values (?, ?, ?)", id, ck, round);
                flush();
            }
            Set<SSTableReader> originals = new HashSet<>(cfs.getLiveSSTables());
            int rows = execute("select * from %s").size();

            // single-token ranges, full and transient in turn, so consecutive partitions change output
            List<Long> tokens = new ArrayList<>();
            for (long id = 0; id < partitions; id++)
                tokens.add(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(id)).getToken().getLongValue());
            tokens.sort(Long::compare);
            RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(FBUtilities.getBroadcastAddressAndPort());
            for (int i = 0; i < tokens.size(); i += 2)
                builder.add(new Replica(FBUtilities.getBroadcastAddressAndPort(),
                                        new Range<>(new Murmur3Partitioner.LongToken(tokens.get(i) - 1), new Murmur3Partitioner.LongToken(tokens.get(i))),
                                        i % 4 == 0));
            RangesAtEndpoint ranges = builder.build();

            long cursorRunsBefore = CompactionManager.cursorAnticompactionsRun.sum();
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(originals, OperationType.ANTICOMPACTION))
            {
                CompactionManager.instance.antiCompactGroup(cfs, ranges, txn, nextTimeUUID(), () -> false);
                fail("the injected failure did not reach the caller");
            }
            catch (RuntimeException | FSWriteError e)
            {
                if (message != null)
                    assertEquals(message, e.getMessage());
            }
            assertEquals("the cursor path did not run", 1, CompactionManager.cursorAnticompactionsRun.sum() - cursorRunsBefore);
            AntiCompactionAssertions.assertCleanAbort(cfs, originals);
            assertEquals(rows, execute("select * from %s").size());

            // nothing leaked: a second anticompaction succeeds on the cursor path
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.ANTICOMPACTION))
            {
                assertTrue(CompactionManager.instance.antiCompactGroup(cfs, ranges, txn, nextTimeUUID(), () -> false) > 0);
            }
            assertEquals("the second anticompaction did not take the cursor path", 2, CompactionManager.cursorAnticompactionsRun.sum() - cursorRunsBefore);
            AntiCompactionAssertions.assertNoAntiCompactionRegistered();
            assertEquals(rows, execute("select * from %s").size());
        }
        finally
        {
            DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
            DatabaseDescriptor.setCursorRepairEnabled(originalEnabled);
        }
    }
}
