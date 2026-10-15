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

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.service.DiskErrorsHandler;
import org.apache.cassandra.service.DiskErrorsHandlerService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A disk error during a CQL read must reach the disk failure policy on the cursor path at least
 * as the iterator path does.  The read runs through {@link QueryProcessor} at ONE, on the read
 * stage, whose executor hands an uncaught error to {@link JVMStabilityInspector}, the default
 * uncaught exception handler the daemon installs.  It runs both as a coordinator's local read and
 * as a replica's response.  Each case checks, cursor reads off and on: the client sees a failure,
 * the errors handed to the disk errors handler, and whether the {@code die} policy kills the JVM.
 * The stack trace of the error shows which path read.  Three real faults:
 * <ul>
 *   <li>bytes overwritten in an uncompressed data file: both paths turn the IOException into a
 *       {@link CorruptSSTableException} and mark the sstable suspect, so the suspect flag must
 *       match;</li>
 *   <li>a compressed chunk that fails its checksum ({@link CorruptSSTableException});</li>
 *   <li>a data file channel closed under the reader, so the read gets a real {@code IOException}
 *       from the file system ({@link FSReadError}).</li>
 * </ul>
 * For the last two the iterator path marks the sstable suspect for some reads and not others, so
 * the suspect flag is not compared, except that a chunk checksum failure met while a cursor read
 * reads a cell value must mark the sstable suspect, as the iterator path does for a row it reads.
 */
public class CursorReadDiskFailurePolicyTest extends CQLTester
{
    private enum Fault { CORRUPT_BYTES, CHUNK_CHECKSUM, CLOSED_CHANNEL }

    private SSTableFormat<?, ?> originalFormat;
    private DiskFailurePolicy originalPolicy;
    private DiskErrorsHandler originalHandler;
    private JVMStabilityInspector.Killer originalKiller;
    private Thread.UncaughtExceptionHandler originalUncaughtHandler;
    private final RecordingHandler handler = new RecordingHandler();
    private final KillerForTests killer = new KillerForTests();
    private int valueReadFailures;

    @Before
    public void save()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        originalPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        originalHandler = DiskErrorsHandlerService.get();
        DiskErrorsHandlerService.set(handler);
        originalKiller = JVMStabilityInspector.replaceKiller(killer);
        // the handler the daemon installs; the test server installs one that only logs
        originalUncaughtHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(JVMStabilityInspector::uncaughtException);
        DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.die);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setDiskFailurePolicy(originalPolicy);
        DatabaseDescriptor.setCursorReadsEnabled(false);
        JVMStabilityInspector.replaceKiller(originalKiller);
        Thread.setDefaultUncaughtExceptionHandler(originalUncaughtHandler);
        DiskErrorsHandlerService.set(originalHandler);
    }

    @Test
    public void corruptDataFile() throws Throwable
    {
        run(Fault.CORRUPT_BYTES, CorruptSSTableException.class);
    }

    @Test
    public void chunkChecksumFailure() throws Throwable
    {
        run(Fault.CHUNK_CHECKSUM, CorruptSSTableException.class);
    }

    @Test
    public void fileSystemReadError() throws Throwable
    {
        run(Fault.CLOSED_CHANNEL, FSReadError.class);
    }

    private void run(Fault fault, Class<? extends Throwable> expected) throws Throwable
    {
        valueReadFailures = 0;
        for (String format : new String[]{ "big", "bti" })
        {
            // a small partition stays on the chunk cache reader; a large one moves the cursor
            // read to the read-ahead reader
            for (int rows : fault == Fault.CORRUPT_BYTES ? new int[]{ 200 } : new int[]{ 50, 3000 })
            {
                for (boolean merged : new boolean[]{ false, true })
                {
                    for (boolean reversed : new boolean[]{ false, true })
                    {
                        String label = fault + " " + format + " rows=" + rows + (merged ? " merged" : "") + (reversed ? " reversed" : "");
                        DatabaseDescriptor.setSelectedSSTableFormat(format);
                        String compression = fault == Fault.CHUNK_CHECKSUM
                                             ? "{'class': 'LZ4Compressor', 'chunk_length_in_kb': 4}"
                                             : "{'enabled': false}";
                        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                                    "WITH compression = " + compression + " AND crc_check_chance = 1.0");
                        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                        cfs.disableAutoCompaction();
                        // short values, so overwritten bytes land on row structure, not inside a value
                        String value = fault == Fault.CORRUPT_BYTES ? "v" : "x".repeat(1000);
                        for (long ck = 0; ck < rows; ck++)
                            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?) USING TIMESTAMP 10", ck, value + ck);
                        flush();
                        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
                        if (merged)
                            execute("INSERT INTO %s (pk, ck, v) VALUES (1, " + rows + ", 'memtable') USING TIMESTAMP 20");
                        if (fault == Fault.CHUNK_CHECKSUM)
                            corruptLastChunks(sstable);
                        else if (fault == Fault.CORRUPT_BYTES)
                            corruptMiddle(sstable);
                        else
                            closeDataChannel(sstable);

                        String query = String.format("SELECT * FROM %s.%s WHERE pk = 1%s", KEYSPACE, currentTable(),
                                                     reversed ? " ORDER BY ck DESC" : "");
                        for (boolean replica : new boolean[]{ false, true })
                            check(label + (replica ? " replica response" : ""), fault, expected, sstable, query, replica);
                    }
                }
            }
        }
        if (fault == Fault.CHUNK_CHECKSUM)
            assertTrue("some cursor read fails while it reads a cell value", valueReadFailures > 0);
    }

    private void check(String label, Fault fault, Class<? extends Throwable> expected, SSTableReader sstable, String query, boolean replica) throws Exception
    {
        // a replica serves a remote read through ReadCommand.createResponseLocally
        StorageProxy.localReadsAsReplicaResponses = replica;
        Outcome iterator;
        Outcome cursor;
        try
        {
            iterator = read(sstable, query, false);
            cursor = read(sstable, query, true);
        }
        finally
        {
            StorageProxy.localReadsAsReplicaResponses = false;
        }

        assertFalse(label + ": the iterator read never reaches the cursor reader", iterator.cursorRead);
        assertTrue(label + ": the cursor read reads through the cursor reader", cursor.cursorRead);
        assertTrue(label + ": the cursor read hands " + expected.getSimpleName() + " to the policy, got " + cursor,
                   cursor.diskErrors.contains(expected.getSimpleName()));
        assertTrue(label + ": the die policy kills the JVM on the cursor path", cursor.killed);
        // The iterator path sometimes hides the error behind a NullPointerException
        // (AbstractSSTableIterator's constructor), so it can reach the policy less
        // often, never more.
        assertTrue(label + ": the cursor read hands the policy every error the iterator read does, iterator "
                   + iterator + ", cursor " + cursor,
                   cursor.diskErrors.containsAll(iterator.diskErrors));
        // Both paths turn an IOException met while reading into a CorruptSSTableException
        // and mark the sstable suspect.
        if (fault == Fault.CORRUPT_BYTES)
            assertEquals(label + ": suspect", iterator.suspect, cursor.suspect);
        // The iterator path marks the sstable suspect when a chunk fails its checksum while it
        // reads a row (UnfilteredSerializer.deserializeRowBody turns it into an IOException).
        // A cursor read of a cell value straight into its destination must do the same.
        if (fault == Fault.CHUNK_CHECKSUM && cursor.valueRead)
        {
            valueReadFailures++;
            assertTrue(label + ": a failed cell value read marks the sstable suspect", cursor.suspect);
        }
    }

    /** Overwrites the middle of the data file with bytes no row starts with. */
    private static void corruptMiddle(SSTableReader sstable) throws Exception
    {
        try (RandomAccessFile file = new RandomAccessFile(sstable.descriptor.fileFor(SSTableFormat.Components.DATA).toJavaIOFile(), "rw"))
        {
            file.seek(file.length() / 2);
            for (int i = 0; i < 64; i++)
                file.write(0xFF);
        }
    }

    /** Overwrites the end of the data file, so every chunk read past the middle fails its checksum. */
    private static void corruptLastChunks(SSTableReader sstable) throws Exception
    {
        try (RandomAccessFile file = new RandomAccessFile(sstable.descriptor.fileFor(SSTableFormat.Components.DATA).toJavaIOFile(), "rw"))
        {
            long length = file.length();
            for (long position = length / 2; position < length; position += 512)
            {
                file.seek(position);
                file.write(0x5A);
            }
        }
    }

    /** Closes the file channel under the sstable's data file, so every read of it throws an IOException. */
    private static void closeDataChannel(SSTableReader sstable) throws Exception
    {
        Field field = ChannelProxy.class.getDeclaredField("channel");
        field.setAccessible(true);
        ((FileChannel) field.get(sstable.getDataChannel())).close();
    }

    private Outcome read(SSTableReader sstable, String query, boolean cursorReads) throws Exception
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursorReads);
        sstable.unmarkSuspect();
        if (ChunkCache.instance != null)
            ChunkCache.instance.clear();
        handler.errors.clear();
        killer.reset();
        boolean failed = false;
        try
        {
            QueryProcessor.process(query, ConsistencyLevel.ONE);
        }
        catch (Exception e)
        {
            failed = true;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
        if (!failed)
            fail("the read should fail: " + query + ", cursor reads " + cursorReads);
        // the read stage hands the error to the policy after it tells the client
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (handler.errors.isEmpty() && System.nanoTime() < deadline)
            Thread.sleep(10);
        Thread.sleep(50);

        Set<String> diskErrors = new TreeSet<>();
        boolean cursorRead = false;
        boolean valueRead = false;
        for (Throwable t : handler.errors)
        {
            if (t instanceof CorruptSSTableException || t instanceof FSError)
                diskErrors.add(t.getClass().getSimpleName());
            cursorRead |= readThroughCursorReader(t);
            valueRead |= failedReadingValue(t);
        }
        return new Outcome(diskErrors, sstable.isMarkedSuspect(), killer.wasKilled(), cursorRead, valueRead);
    }

    /** Whether the error came from reading a cell value straight into its destination. */
    private static boolean failedReadingValue(Throwable t)
    {
        for (StackTraceElement frame : t.getStackTrace())
        {
            if (!frame.getMethodName().equals("readValue"))
                continue;
            try
            {
                if (SSTableCursorReader.DirectValueTarget.class.isAssignableFrom(Class.forName(frame.getClassName())))
                    return true;
            }
            catch (ClassNotFoundException e)
            {
                throw new AssertionError(e);
            }
        }
        return false;
    }

    private static boolean readThroughCursorReader(Throwable t)
    {
        return Arrays.stream(t.getStackTrace()).anyMatch(frame -> frame.getClassName().startsWith(SSTableCursorReader.class.getName())
                                                                  || frame.getClassName().startsWith(CursorReads.class.getName()));
    }

    private static final class Outcome
    {
        final Set<String> diskErrors;
        final boolean suspect;
        final boolean killed;
        final boolean cursorRead;
        final boolean valueRead;

        Outcome(Set<String> diskErrors, boolean suspect, boolean killed, boolean cursorRead, boolean valueRead)
        {
            this.diskErrors = diskErrors;
            this.suspect = suspect;
            this.killed = killed;
            this.cursorRead = cursorRead;
            this.valueRead = valueRead;
        }

        @Override
        public String toString()
        {
            // which path read is checked on its own
            return "disk errors=" + diskErrors + ", suspect=" + suspect + ", killed=" + killed;
        }
    }

    /** Records every error handed to the disk failure policy. */
    private static final class RecordingHandler implements DiskErrorsHandler
    {
        final List<Throwable> errors = new CopyOnWriteArrayList<>();

        @Override
        public void init() {}

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
            errors.add(t);
        }

        @Override
        public void inspectCommitLogError(Throwable t) {}

        @Override
        public void close() {}
    }
}
