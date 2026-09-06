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

package org.apache.cassandra.test.microbench.sstable;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * CPU and allocation benchmark for the cursor read path (CASSANDRA-20428).
 *
 * The branch's standing instruments are all {@code ThreadMXBean} allocation gates in
 * test/unit/org/apache/cassandra/db/cursorreads/, which measure bytes allocated and nothing else.
 * This is the first CPU-time measurement of the path. Run with {@code -gc true} and one run
 * reports ns/op alongside {@code gc.alloc.rate.norm} (bytes per operation), so a change can be
 * judged on both at once.
 *
 * TWO SHAPES, because the read path forks into two very different halves and a change usually
 * touches only one of them:
 * <ul>
 *   <li>{@link #materializingRead} — {@code executeLocally}, which never reaches the transcode
 *       response path, so the merge materializes real {@code Row}/{@code Cell} objects through
 *       {@code CursorReads.MaterializingMergeSink}. This is the half that serves ordinary paged
 *       and limited CQL traffic.</li>
 *   <li>{@link #transcodeResponse} — {@code createResponseLocally}, which routes through
 *       {@code SinglePartitionReadCommand.queryStorageToResponseBytes} and writes
 *       {@code ReadResponse} wire bytes directly via {@code CursorReads.TranscodeMergeSink},
 *       building no Row objects at all.</li>
 * </ul>
 *
 * WORKLOAD: one fully-overlapping partition across {@value #DEFAULT_SSTABLES} sstables — every
 * round rewrites every (pk, ck) with a later timestamp, so the merge does real reconciliation and
 * discards S-1 shadowed copies, the shape the cursor merge exists for. Columns alternate
 * {@code bigint} and {@code text} so both value arms run: fixed-length values land straight in the
 * final array, variable-length values take {@code SSTableCursorReader.copyCellContents}' chunked
 * path.
 *
 * GATE VERIFICATION: {@link #setup} proves the transcode path really serves
 * {@link #transcodeResponse} before any measurement runs. Every gate in
 * {@code queryStorageToResponseBytes} declines silently by returning null, so a benchmark that
 * quietly measured the default path instead would look perfectly healthy and mean nothing — the
 * same silent-fallback discipline the differential harness applies with
 * {@link CursorReads#transcodeResponsesServed()}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CursorReadBench extends CQLTester
{
    private static final int DEFAULT_SSTABLES = 3;
    private static final long PK = 1L;

    @Param("3")
    int sstableCount = DEFAULT_SSTABLES;

    /** rows in the single queried partition, per sstable (every sstable holds all of them) */
    @Param("1024")
    int rowCount = 1024;

    /** regular columns, alternating bigint and text */
    @Param("8")
    int colCount = 8;

    @Param("true")
    boolean isCursor = true;

    @Param("bti")
    String format = "bti";

    private static String keyspace;
    private String table;
    private ColumnFamilyStore cfs;
    private long nowInSec;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        DatabaseDescriptor.setCursorReadsEnabled(isCursor);
        nowInSec = FBUtilities.nowInSeconds();
        createSStables();
        verifyRouting();
    }

    private void createSStables() throws Throwable
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', " +
                                  "'replication_factor' : 1 } and durable_writes = false");

        StringBuilder create = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        StringBuilder names = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < colCount; i++)
        {
            create.append(", v").append(i).append(i % 2 == 0 ? " bigint" : " text");
            names.append(", v").append(i);
            placeholders.append(", ?");
        }
        create.append(", PRIMARY KEY(pk, ck)) WITH compression = {'enabled': 'false'}");
        table = createTable(keyspace, create.toString());
        execute("use " + keyspace + ';');

        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));
        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        String insert = "INSERT INTO " + table + "(pk, ck" + names + ")VALUES(?, ?" + placeholders + ')';
        Object[] values = new Object[2 + colCount];
        values[0] = PK;
        for (int round = 0; round < sstableCount; round++)
        {
            for (long ck = 0; ck < rowCount; ck++)
            {
                values[1] = ck;
                for (int i = 0; i < colCount; i++)
                    values[2 + i] = i % 2 == 0 ? (Object) (ck * 31 + round)
                                               : (Object) ("value-" + round + '-' + ck + '-' + i);
                execute(insert, values);
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        // every leg must survive into the merge: a partition-level deletion or a non-overlapping
        // sstable would silently reduce the leg count and change what is being measured
        if (cfs.getLiveSSTables().size() != sstableCount)
            throw new IllegalStateException("expected " + sstableCount + " overlapping sstables, got "
                                            + cfs.getLiveSSTables().size());
    }

    /** See the class javadoc: a silently declining gate produces a healthy-looking benchmark of
     *  the wrong path. Both shapes are checked in the direction they are supposed to run. */
    private void verifyRouting()
    {
        long merges = CursorReads.cursorMergesServed();
        consume(materializingPass());
        if (isCursor && CursorReads.cursorMergesServed() == merges)
            throw new IllegalStateException("materializingRead did not reach the cursor merge");

        long transcoded = CursorReads.transcodeResponsesServed();
        transcodePass();
        if (isCursor && CursorReads.transcodeResponsesServed() == transcoded)
            throw new IllegalStateException("transcodeResponse did not reach the transcode path");
    }

    private SinglePartitionReadCommand command()
    {
        // unlimited, unfiltered, forward, single slice, >= 2 legs: the shape
        // queryStorageToResponseBytes accepts. materializingRead runs the same command through
        // executeLocally, which has no transcode path at all, so one command measures both halves.
        return (SinglePartitionReadCommand) Util.cmd(cfs, PK).withNowInSeconds(nowInSec).build();
    }

    private UnfilteredPartitionIterator materializingPass()
    {
        SinglePartitionReadCommand cmd = command();
        try (ReadExecutionController controller = cmd.executionController())
        {
            return cmd.executeLocally(controller);
        }
    }

    private Object transcodePass()
    {
        SinglePartitionReadCommand cmd = command();
        try (ReadExecutionController controller = cmd.executionController())
        {
            return cmd.createResponseLocally(controller);
        }
    }

    private static long consume(UnfilteredPartitionIterator partitions)
    {
        long count = 0;
        try (UnfilteredPartitionIterator it = partitions)
        {
            while (it.hasNext())
            {
                try (UnfilteredRowIterator partition = it.next())
                {
                    count += partition.staticRow().isEmpty() ? 0 : 1;
                    while (partition.hasNext())
                    {
                        partition.next();
                        count++;
                    }
                }
            }
        }
        return count;
    }

    @Benchmark
    public void materializingRead(Blackhole bh)
    {
        bh.consume(consume(materializingPass()));
    }

    @Benchmark
    public void transcodeResponse(Blackhole bh)
    {
        bh.consume(transcodePass());
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        ClusterMetadataService.instance().log().close();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }
}
