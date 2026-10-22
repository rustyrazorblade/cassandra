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
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Reads of a corrupt data file: the cursor path must fail as the iterator path does, with the
 * same exception class, the sstable marked suspect, the same errors handed to the disk failure
 * policy ({@link JVMStabilityInspector} walks the causes and hands each {@link CorruptSSTableException}
 * and {@link FSError} to it), and the same failure reason a coordinator reports.  The iterator path
 * turns an {@code IOException} met while reading a row into a {@code CorruptSSTableException} and
 * marks the sstable suspect ({@code AbstractSSTableIterator}).  On an nb sstable the iterator path
 * can miss the corruption: it returns part of the partition with no error, or a garbage row that
 * only an assertion catches.  There the cursor path must still fail, and the two are not compared.
 */
public class CursorReadCorruptionTest extends CQLTester
{
    private static final String NO_FAILURE = "no failure";
    private static final String CORRUPT = "CorruptSSTableException, suspect=true, disk errors=[CorruptSSTableException]";

    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void save()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        DatabaseDescriptor.setCursorReadsEnabled(false);
    }

    @Test
    public void readOfACorruptDataFile() throws Throwable
    {
        for (String format : new String[]{ "big", "bti" })
        {
            for (boolean merged : new boolean[]{ false, true })
            {
                for (boolean reversed : new boolean[]{ false, true })
                {
                    DatabaseDescriptor.setSelectedSSTableFormat(format);
                    createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) WITH compression = {'enabled': false}");
                    ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                    cfs.disableAutoCompaction();
                    for (long ck = 0; ck < 200; ck++)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?) USING TIMESTAMP 10", ck, "v" + ck);
                    flush();
                    SSTableReader corrupt = cfs.getLiveSSTables().iterator().next();
                    if (merged)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 300, 'memtable') USING TIMESTAMP 20");
                    corruptMiddle(corrupt);

                    String label = format + (merged ? " merged" : "") + (reversed ? " reversed" : "");
                    LongFunction<SinglePartitionReadCommand> read = now -> {
                        var b = Util.cmd(cfs, 1L).withNowInSeconds(now);
                        return (SinglePartitionReadCommand) (reversed ? b.reverse() : b).build();
                    };
                    for (boolean replica : new boolean[]{ false, true })
                    {
                        String iterator = failure(corrupt, read, false, replica);
                        String cursor = failure(corrupt, read, true, replica);
                        String message = label + (replica ? " replica response" : " executeLocally");
                        if (!iterator.startsWith(CORRUPT) && !corrupt.descriptor.version.hasUIntDeletionTime())
                            assertTrue(message + ": " + cursor, cursor.startsWith(CORRUPT));
                        else
                            assertEquals(message, iterator, cursor);
                    }
                }
            }
        }
    }

    /** Overwrites the middle of the data file with bytes no row starts with. */
    private static void corruptMiddle(SSTableReader sstable) throws Exception
    {
        try (RandomAccessFile file = new RandomAccessFile(sstable.descriptor.fileFor(SSTableFormat.Components.DATA).toJavaIOFile(), "rw"))
        {
            long length = file.length();
            file.seek(length / 2);
            for (int i = 0; i < 64; i++)
                file.write(0xFF);
        }
        if (ChunkCache.instance != null)
            ChunkCache.instance.close();
    }

    /**
     * Reads on one path and describes the failure: the exception class, whether the sstable was
     * marked suspect, the errors the disk failure policy would be handed, and the failure reason.
     */
    private static String failure(SSTableReader sstable, LongFunction<SinglePartitionReadCommand> read, boolean cursor, boolean replica)
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
        sstable.unmarkSuspect();
        if (ChunkCache.instance != null)
            ChunkCache.instance.close();
        SinglePartitionReadCommand command = read.apply(System.currentTimeMillis() / 1000);
        Throwable thrown = null;
        try
        {
            if (replica)
            {
                ReadCommandVerbHandler.instance.doRead(command, false);
            }
            else
            {
                try (ReadExecutionController controller = command.executionController();
                     UnfilteredPartitionIterator partitions = command.executeLocally(controller))
                {
                    while (partitions.hasNext())
                    {
                        try (UnfilteredRowIterator partition = partitions.next())
                        {
                            while (partition.hasNext())
                                partition.next();
                        }
                    }
                }
            }
        }
        catch (Throwable t)
        {
            thrown = t;
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
        if (thrown == null)
            return NO_FAILURE;
        List<String> diskErrors = new ArrayList<>();
        JVMStabilityInspector.inspectThrowable(thrown, t -> {
            if (t instanceof CorruptSSTableException || t instanceof FSError)
                diskErrors.add(t.getClass().getSimpleName());
        }, false);
        return thrown.getClass().getSimpleName() + ", suspect=" + sstable.isMarkedSuspect() + ", disk errors=" + diskErrors
               + ", reason=" + RequestFailureReason.forException(thrown);
    }
}
