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

package org.apache.cassandra.repair;

import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.repair.CassandraTableRepairManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.DiskErrorsHandler;
import org.apache.cassandra.service.DiskErrorsHandlerService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A read failure during repair validation must fail the validation the same way on the cursor path
 * as on the iterator path: {@link ValidationManager#submitValidation} fails with a
 * {@link CorruptSSTableException}, the sstable is marked suspect, the validator responds with a
 * failure (no Merkle trees), and the disk failure handler sees the {@link CorruptSSTableException}.
 * Each case runs with {@code cursor_repair_enabled} on and off.
 */
@RunWith(BMUnitRunner.class)
public class CursorValidationReadFailureTest extends CQLTester
{
    /** The Data.db path whose reads throw, or null to read normally. */
    private static volatile String failReadsOfDataFile;

    private final Queue<Throwable> diskErrors = new ConcurrentLinkedQueue<>();
    private boolean cursorRepairEnabledBefore;

    /** Called by the byteman rule on every byte read; true makes the read throw an IOException. */
    public static boolean failRead(Object in)
    {
        String path = failReadsOfDataFile;
        return path != null && in instanceof RandomAccessReader && path.equals(((RandomAccessReader) in).getPath());
    }

    /** Called by the byteman rule at the start of every cursor merge step. */
    public static boolean failMerge()
    {
        return failReadsOfDataFile != null;
    }

    @Before
    public void recordDiskErrors()
    {
        cursorRepairEnabledBefore = DatabaseDescriptor.cursorRepairEnabled();
        DiskErrorsHandlerService.set(new RecordingDiskErrorsHandler(diskErrors));
    }

    @After
    public void restore()
    {
        failReadsOfDataFile = null;
        DatabaseDescriptor.setCursorRepairEnabled(cursorRepairEnabledBefore);
        DiskErrorsHandlerService.configure();
    }

    @Test
    public void corruptDataFileFailsCursorValidation() throws Throwable
    {
        corruptDataFileFailsValidation(true);
    }

    @Test
    public void corruptDataFileFailsIteratorValidation() throws Throwable
    {
        corruptDataFileFailsValidation(false);
    }

    @Test
    @BMRule(name = "Throw IOException from Data.db reads",
            targetClass = "org.apache.cassandra.io.util.RebufferingInputStream",
            targetMethod = "readByte",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.repair.CursorValidationReadFailureTest.failRead($0)",
            action = "throw new java.io.IOException(\"injected read failure\")")
    public void readIOExceptionFailsCursorValidation() throws Throwable
    {
        readIOExceptionFailsValidation(true);
    }

    @Test
    @BMRule(name = "Throw IOException from Data.db reads",
            targetClass = "org.apache.cassandra.io.util.RebufferingInputStream",
            targetMethod = "readByte",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.repair.CursorValidationReadFailureTest.failRead($0)",
            action = "throw new java.io.IOException(\"injected read failure\")")
    public void readIOExceptionFailsIteratorValidation() throws Throwable
    {
        readIOExceptionFailsValidation(false);
    }

    /**
     * An IOException that the cursor reader did not already turn into a CorruptSSTableException
     * must still reach the disk failure handler as one.
     */
    @Test
    @BMRule(name = "Throw IOException from the cursor merge",
            targetClass = "org.apache.cassandra.db.compaction.CursorCompactor",
            targetMethod = "mergeNextPartition",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.repair.CursorValidationReadFailureTest.failMerge()",
            action = "throw new java.io.IOException(\"injected merge failure\")")
    public void mergeIOExceptionFailsCursorValidation() throws Throwable
    {
        DatabaseDescriptor.setCursorRepairEnabled(true);
        ColumnFamilyStore cfs = createTableWithOneSSTable();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        failReadsOfDataFile = sstable.getFilename();

        Throwable failure = validateExpectingFailure(cfs, true);
        assertEquals("the IOException is the cause", "injected merge failure", rootCause(failure).getMessage());
        assertTrue("the sstable is marked suspect", sstable.isMarkedSuspect());
    }

    private void corruptDataFileFailsValidation(boolean cursor) throws Throwable
    {
        DatabaseDescriptor.setCursorRepairEnabled(cursor);
        ColumnFamilyStore cfs = createTableWithOneSSTable();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Overwrite bytes in the middle of the compressed data, so the chunk checksum fails.
        try (RandomAccessFile file = new RandomAccessFile(sstable.getFilename(), "rw"))
        {
            file.seek(file.length() / 2);
            byte[] garbage = new byte[64];
            new Random(42).nextBytes(garbage);
            file.write(garbage);
        }

        validateExpectingFailure(cfs, cursor);
        // Not checked on the cursor path: SSTableCursorReader.copyCellContents catches only
        // IOException, so the CorruptSSTableException that the compressed chunk reader throws
        // skips markSuspect().  That code comes from cassandra-6.0.
        if (!cursor)
            assertTrue("the sstable is marked suspect", sstable.isMarkedSuspect());
    }

    private void readIOExceptionFailsValidation(boolean cursor) throws Throwable
    {
        DatabaseDescriptor.setCursorRepairEnabled(cursor);
        ColumnFamilyStore cfs = createTableWithOneSSTable();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        failReadsOfDataFile = sstable.getFilename();

        Throwable failure = validateExpectingFailure(cfs, cursor);
        assertEquals("the IOException is the cause", "injected read failure", rootCause(failure).getMessage());
        assertTrue("the sstable is marked suspect", sstable.isMarkedSuspect());
    }

    private ColumnFamilyStore createTableWithOneSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Random random = new Random(7);
        for (int pk = 0; pk < 100; pk++)
            for (int ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, randomText(random, 100));
        flush();
        assertEquals("test setup: one sstable", 1, cfs.getLiveSSTables().size());
        return cfs;
    }

    /**
     * Runs a full-range validation through {@link ValidationManager#submitValidation}, as a repair
     * session does, and checks that it failed on the expected path.
     *
     * @return the exception the validation task failed with
     */
    private Throwable validateExpectingFailure(ColumnFamilyStore cfs, boolean cursor) throws Throwable
    {
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        Range<Token> fullRange = new Range<>(minimumToken, minimumToken);
        List<Range<Token>> ranges = Collections.singletonList(fullRange);
        TimeUUID parentId = nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(parentId,
                                                                    FBUtilities.getBroadcastAddressAndPort(),
                                                                    Lists.newArrayList(cfs),
                                                                    Sets.newHashSet(fullRange),
                                                                    false,
                                                                    ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                    true,
                                                                    PreviewKind.NONE);
        RepairJobDesc desc = new RepairJobDesc(parentId, nextTimeUUID(), cfs.getKeyspaceName(), cfs.getTableName(), ranges);
        ValidationState state = new ValidationState(Clock.Global.clock(), desc, FBUtilities.getBroadcastAddressAndPort());
        CompletableFuture<ValidationResponse> response = new CompletableFuture<>();
        Validator validator = new Validator(SharedContext.Global.instance, state, FBUtilities.nowInSeconds(), false, false, PreviewKind.NONE, false)
        {
            @Override
            void respond(ValidationResponse r)
            {
                response.complete(r);
            }
        };

        long runsBefore = CassandraTableRepairManager.cursorValidationsRun.sum();
        long fallbacksBefore = CassandraTableRepairManager.cursorValidationFallbacks.sum();
        Throwable failure = null;
        try
        {
            ValidationManager.instance.submitValidation(cfs, validator).get(60, TimeUnit.SECONDS);
            fail("validation should fail");
        }
        catch (ExecutionException e)
        {
            failure = e.getCause();
        }
        failReadsOfDataFile = null;

        assertTrue("validation must fail with CorruptSSTableException, got " + failure, failure instanceof CorruptSSTableException);
        assertEquals("cursor validations run", cursor ? 1 : 0, CassandraTableRepairManager.cursorValidationsRun.sum() - runsBefore);
        assertEquals("cursor validation fallbacks", 0, CassandraTableRepairManager.cursorValidationFallbacks.sum() - fallbacksBefore);
        assertEquals("the read failed in the cursor reader exactly when cursor repair is on", cursor, readByCursorReader(failure));

        ValidationResponse r = response.get(60, TimeUnit.SECONDS);
        assertFalse("the validator must respond with a failure", r.success());
        assertNotNull("validation state records the failure", state.getFailureCause());

        Throwable inspected = diskErrors.stream().filter(t -> t instanceof CorruptSSTableException).findFirst().orElse(null);
        assertNotNull("the disk failure handler must see the CorruptSSTableException, saw " + diskErrors, inspected);
        return failure;
    }

    private static boolean readByCursorReader(Throwable failure)
    {
        for (Throwable t = failure; t != null; t = t.getCause())
            for (StackTraceElement frame : t.getStackTrace())
                if (frame.getClassName().endsWith("CursorCompactor") || frame.getClassName().endsWith("CursorValidationIterator"))
                    return true;
        return false;
    }

    private static Throwable rootCause(Throwable t)
    {
        while (t.getCause() != null)
            t = t.getCause();
        return t;
    }

    private static String randomText(Random random, int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++)
            sb.append((char) ('a' + random.nextInt(26)));
        return sb.toString();
    }

    /** Records every throwable the disk failure handling inspects. */
    private static final class RecordingDiskErrorsHandler implements DiskErrorsHandler
    {
        private final Queue<Throwable> seen;

        RecordingDiskErrorsHandler(Queue<Throwable> seen)
        {
            this.seen = seen;
        }

        @Override
        public void init() {}

        @Override
        public void close() {}

        @Override
        public void handleCorruptSSTable(CorruptSSTableException e) {}

        @Override
        public void handleFSError(FSError e) {}

        @Override
        public boolean handleCommitError(String message, Throwable t)
        {
            return true;
        }

        @Override
        public void handleStartupFSError(Throwable t) {}

        @Override
        public void inspectDiskError(Throwable t)
        {
            seen.add(t);
        }

        @Override
        public void inspectCommitLogError(Throwable t) {}
    }
}
