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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.AuxCounters;
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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CursorRangeReads;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Range reads (partition-range scans) through the cursor path and the iterator path, on the same
 * sstables.
 *
 * <p>Data sets ({@code data}, "partitions x rows"): {@code 1000000x1} (many small partitions),
 * {@code 100000x10}, {@code 1000x1000}, {@code 10x100000} and {@code 1x1000000} (wide partitions),
 * rows of about 100 bytes.  {@code layout=one} is one sstable; {@code layout=four_plus_memtable}
 * spreads the rows over four sstables, row (pk, ck) in sstable {@code (pk * 31 + ck) % 4}, so a
 * partition is in most of them, and overwrites one row in 1000 in the memtable;
 * {@code layout=memtable_only} writes every row into the memtable and flushes nothing.  The sstables are
 * written once per data set into {@code -Dcassandra.bench.range_read.cache=<dir>} (default
 * {@code build/microbench-data/RangeReadBench}) and hard-linked into each trial.
 *
 * <p>{@code deletes=mixed} writes, in every partition, one row in 20 as a row deletion, one row in 20
 * with an expired TTL (purgeable), one row in 20 with a live TTL, a range deletion over a tenth of
 * the rows of one partition in 50 (when it has 10 rows or more), and an older partition deletion
 * in one partition in 97.  The tombstone thresholds are raised so the full ring scan does not fail.
 *
 * <p>Settings: UCS (no compaction), trie memtable with {@code offheap_objects}, LZ4 with 16 KiB
 * chunks or no compression ({@code compression}), chunk cache and key cache off.
 *
 * <p>Shapes:
 * <ul>
 *   <li>{@code full_ring}: one read of the whole table, no limit;</li>
 *   <li>{@code paged100}, {@code paged5000}, {@code paged20000}: the whole table in pages of that many
 *       rows, as the range pager asks for them;</li>
 *   <li>{@code token_splits}: the ring cut into 64 token ranges, each read in pages of 5000 rows, as
 *       the easy-stress RangeScan workload with splits does;</li>
 *   <li>{@code filtered}: {@code WHERE v1 = 3 ALLOW FILTERING} (one row in 10 matches) in pages of
 *       5000 rows;</li>
 *   <li>{@code limit10}: 100 reads of 10 rows from seeded random tokens;</li>
 *   <li>{@code distinct}: {@code SELECT DISTINCT pk} over the whole table.</li>
 * </ul>
 *
 * <p>Entries: {@code replica} is {@code ReadCommandVerbHandler.doRead} ({@code createResponseLocally},
 * the response bytes a replica sends); {@code local} is {@code executeLocally}, every cell consumed.
 *
 * <p>{@link #interleaved} runs both paths in one trial in alternating blocks and counts each
 * path's thread CPU time, wall time, rows, response bytes and allocated bytes; CPU ns/row is
 * {@code cursorCpuNanos / cursorRows}.  It is the comparison to use on a shared machine.
 * {@link #read} runs one path ({@code cursor}), for {@code -prof gc} and profilers.
 * <pre>
 *   ant microbench -Dbenchmark.name='RangeReadBench.interleaved' \
 *       -Djmh.args="-p data=100000x10 -p layout=one -p shape=full_ring -p entry=replica"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xms4G", "-Xmx4G", "-XX:+AlwaysPreTouch",
                                   "-Dcassandra.config.allow_system_properties=true",
                                   "-Dcassandra.settings.file_cache_enabled=false",
                                   "-Dcassandra.settings.key_cache_size=0MiB",
                                   "-Dcassandra.settings.row_cache_size=0MiB",
                                   "-Dcassandra.settings.memtable_allocation_type=offheap_objects" })
@Threads(1)
@State(Scope.Benchmark)
public class RangeReadBench extends CQLTester
{
    private static final int SPLITS = 64;
    private static final int SPLIT_PAGE_SIZE = 5000;
    private static final int FILTERED_PAGE_SIZE = 5000;
    private static final int LIMIT = 10;
    private static final int LIMIT_READS = 100;
    private static final long BASE_TIMESTAMP = 1_000_000L;
    private static final int MEMTABLE_EVERY = 1000;
    private static final int PAYLOAD_BYTES = 64;
    private static final int MULTI_SSTABLES = 4;
    /** The write time of the TTL cells: the short TTL has expired long ago, the long one has not. */
    private static final long TTL_WRITE_TIME = 1_700_000_000L;
    private static final int SHORT_TTL = 100;
    private static final int LONG_TTL = 630_720_000;
    /** The local deletion time of the row, range and partition deletions: not purgeable. */
    private static final long DELETION_LOCAL_TIME = 2_000_000_000L;
    /** Bumped whenever the generated data changes, so a stale cache is not reused. */
    private static final int GENERATOR_VERSION = 2;
    private static final long SEED = 20261005L;

    @Param({ "100000x10", "1000x1000" })
    public String data;

    @Param({ "one", "four_plus_memtable" })
    public String layout;

    @Param({ "lz4" })
    public String compression;

    @Param({ "none" })
    public String deletes;

    @Param({ "full_ring", "paged5000", "limit10", "distinct" })
    public String shape;

    @Param({ "replica" })
    public String entry;

    @Param({ "bti" })
    public String format;

    /** The path {@link #read} uses; {@link #interleaved} ignores it. */
    @Param({ "true" })
    public boolean cursor;

    private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

    private ColumnFamilyStore cfs;
    private int partitions;
    private int rowsPerPartition;
    private int sstables;
    private boolean mixedDeletes;
    private long nowInSec;
    /** Rows one op of each shape reads, counted on the iterator path in setup. */
    private long rowsPerOp;
    private String selectedFormatWas;
    private int tombstoneWarnWas;
    private int tombstoneFailWas;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        String[] parts = data.split("x");
        partitions = Integer.parseInt(parts[0]);
        rowsPerPartition = Integer.parseInt(parts[1]);
        switch (layout)
        {
            case "one": sstables = 1; break;
            case "four_plus_memtable": sstables = MULTI_SSTABLES; break;
            case "memtable_only": sstables = 0; break;
            default: throw new IllegalArgumentException(layout);
        }
        switch (deletes)
        {
            case "none": mixedDeletes = false; break;
            case "mixed": mixedDeletes = true; break;
            default: throw new IllegalArgumentException(deletes);
        }

        CQLTester.setUpClass();
        beforeTest(); // JMH does not run JUnit @Before
        selectedFormatWas = DatabaseDescriptor.getSelectedSSTableFormat().name();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        tombstoneWarnWas = DatabaseDescriptor.getTombstoneWarnThreshold();
        tombstoneFailWas = DatabaseDescriptor.getTombstoneFailureThreshold();
        DatabaseDescriptor.setTombstoneWarnThreshold(Integer.MAX_VALUE);
        DatabaseDescriptor.setTombstoneFailureThreshold(Integer.MAX_VALUE);
        setPath(false);

        String compressionOption;
        switch (compression)
        {
            case "lz4": compressionOption = "{'class': 'LZ4Compressor', 'chunk_length_in_kb': 16}"; break;
            case "none": compressionOption = "{'enabled': 'false'}"; break;
            default: throw new IllegalArgumentException(compression);
        }
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 int, payload blob, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = " + compressionOption + " AND memtable = 'trie' " +
                    "AND compaction = {'class': 'UnifiedCompactionStrategy'} AND caching = {'keys': 'NONE', 'rows_per_partition': 'NONE'} " +
                    "AND gc_grace_seconds = 864000");
        cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        nowInSec = FBUtilities.nowInSeconds();

        if (sstables == 0)
            writeAllToMemtable();
        else
            linkCachedSSTables();
        if ("four_plus_memtable".equals(layout))
            writeMemtableOverwrites();
        long onDisk = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            onDisk += sstable.onDiskLength();

        rowsPerOp = checkPathsAgree();
        System.out.println(String.format("RangeReadBench: format=%s data=%s layout=%s sstables=%d compression=%s deletes=%s shape=%s entry=%s " +
                                         "onDiskBytes=%d rowsPerOp=%d",
                                         format, data, layout, cfs.getLiveSSTables().size(), compression, deletes, shape, entry,
                                         onDisk, rowsPerOp));
        setPath(cursor);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Throwable
    {
        setPath(false);
        DatabaseDescriptor.setSelectedSSTableFormat(selectedFormatWas);
        DatabaseDescriptor.setTombstoneWarnThreshold(tombstoneWarnWas);
        DatabaseDescriptor.setTombstoneFailureThreshold(tombstoneFailWas);
        CQLTester.tearDownClass();
    }

    /** Selects the cursor range read path or the iterator path. */
    private static void setPath(boolean onCursorPath)
    {
        DatabaseDescriptor.setCursorRangeReadsEnabled(onCursorPath);
        DatabaseDescriptor.setCursorReadsEnabled(onCursorPath);
    }

    // ---------------------------------------------------------------- benchmarks

    /** The commands of one op, one set per thread. */
    @State(Scope.Thread)
    public static class Reads
    {
        List<PartitionRangeReadCommand> commands;

        @Setup(Level.Trial)
        public void setup(RangeReadBench bench)
        {
            commands = bench.commands();
        }
    }

    /** The totals of {@link #interleaved}, per path. */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class PathCounters
    {
        public long cursorRows;
        public long cursorCpuNanos;
        public long cursorWallNanos;
        public long cursorAllocatedBytes;
        public long cursorResponseBytes;
        public long iteratorRows;
        public long iteratorCpuNanos;
        public long iteratorWallNanos;
        public long iteratorAllocatedBytes;
        public long iteratorResponseBytes;

        @Setup(Level.Iteration)
        public void reset()
        {
            cursorRows = cursorCpuNanos = cursorWallNanos = cursorAllocatedBytes = cursorResponseBytes = 0;
            iteratorRows = iteratorCpuNanos = iteratorWallNanos = iteratorAllocatedBytes = iteratorResponseBytes = 0;
        }
    }

    /** Rows read per path block of {@link #interleaved}, about. */
    private static final long INTERLEAVE_BLOCK_ROWS = 200_000;
    /** Most ops per path block of {@link #interleaved}, for shapes that read few rows per op. */
    private static final long INTERLEAVE_BLOCK_MAX_OPS = 1000;

    /**
     * Both paths in one trial, alternating blocks of ops, each block timed on the thread's CPU
     * clock and the wall clock, with its allocation.
     */
    @Benchmark
    public void interleaved(Reads reads, PathCounters counters, Blackhole bh)
    {
        int block = (int) Math.max(1, Math.min(INTERLEAVE_BLOCK_MAX_OPS, INTERLEAVE_BLOCK_ROWS / Math.max(1, rowsPerOp)));
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) THREADS;
        long thread = Thread.currentThread().getId();
        for (boolean onCursorPath : new boolean[]{ true, false })
        {
            setPath(onCursorPath);
            long wall = System.nanoTime();
            long cpu = THREADS.getCurrentThreadCpuTime();
            long allocated = threads.getThreadAllocatedBytes(thread);
            long bytes = onCursorPath ? cursorBlock(reads, block, bh) : iteratorBlock(reads, block, bh);
            cpu = THREADS.getCurrentThreadCpuTime() - cpu;
            wall = System.nanoTime() - wall;
            allocated = threads.getThreadAllocatedBytes(thread) - allocated;
            if (onCursorPath)
            {
                counters.cursorRows += rowsPerOp * block;
                counters.cursorCpuNanos += cpu;
                counters.cursorWallNanos += wall;
                counters.cursorAllocatedBytes += allocated;
                counters.cursorResponseBytes += bytes;
            }
            else
            {
                counters.iteratorRows += rowsPerOp * block;
                counters.iteratorCpuNanos += cpu;
                counters.iteratorWallNanos += wall;
                counters.iteratorAllocatedBytes += allocated;
                counters.iteratorResponseBytes += bytes;
            }
        }
    }

    /** The cursor path's ops of {@link #interleaved}, in their own frame so a profile can tell them apart. */
    private long cursorBlock(Reads reads, int block, Blackhole bh)
    {
        long bytes = 0;
        for (int i = 0; i < block; i++)
            for (PartitionRangeReadCommand command : reads.commands)
                bytes += readOnce(command, bh);
        return bytes;
    }

    /** The iterator path's ops of {@link #interleaved}, in their own frame. */
    private long iteratorBlock(Reads reads, int block, Blackhole bh)
    {
        long bytes = 0;
        for (int i = 0; i < block; i++)
            for (PartitionRangeReadCommand command : reads.commands)
                bytes += readOnce(command, bh);
        return bytes;
    }

    /** One op on the {@code cursor} path. */
    @Benchmark
    public void read(Reads reads, Blackhole bh)
    {
        for (PartitionRangeReadCommand command : reads.commands)
            bh.consume(readOnce(command, bh));
    }

    /** One read; returns the serialized response size ({@code replica}) or the cells consumed ({@code local}). */
    private long readOnce(PartitionRangeReadCommand command, Blackhole bh)
    {
        switch (entry)
        {
            case "replica":
            {
                ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
                return ReadResponse.serializer.serializedSize(response, MessagingService.current_version);
            }
            case "local":
                try (ReadExecutionController controller = command.executionController();
                     UnfilteredPartitionIterator partitions = command.executeLocally(controller))
                {
                    return drain(partitions, bh);
                }
            default:
                throw new IllegalArgumentException(entry);
        }
    }

    private static long drain(UnfilteredPartitionIterator partitions, Blackhole bh)
    {
        long cells = 0;
        while (partitions.hasNext())
        {
            try (UnfilteredRowIterator partition = partitions.next())
            {
                bh.consume(partition.staticRow());
                while (partition.hasNext())
                {
                    Unfiltered unfiltered = partition.next();
                    if (!unfiltered.isRow())
                        continue;
                    for (Cell<?> cell : ((Row) unfiltered).cells())
                    {
                        bh.consume(cell.valueSize());
                        cells++;
                    }
                }
            }
        }
        return cells;
    }

    // ---------------------------------------------------------------- shapes

    /** The commands of one op of {@link #shape}. */
    List<PartitionRangeReadCommand> commands()
    {
        switch (shape)
        {
            case "full_ring":
                return Collections.singletonList(rangeCommand("SELECT * FROM %s"));
            case "distinct":
                return Collections.singletonList(rangeCommand("SELECT DISTINCT pk FROM %s"));
            case "limit10":
            {
                Random random = new Random(SEED);
                List<PartitionRangeReadCommand> commands = new ArrayList<>(LIMIT_READS);
                for (int i = 0; i < LIMIT_READS; i++)
                    commands.add(rangeCommand("SELECT * FROM %s WHERE token(pk) > ? LIMIT " + LIMIT, random.nextLong()));
                return commands;
            }
            case "paged100":
                return pages(rangeCommand("SELECT * FROM %s"), 100);
            case "paged5000":
                return pages(rangeCommand("SELECT * FROM %s"), 5000);
            case "paged20000":
                return pages(rangeCommand("SELECT * FROM %s"), 20000);
            case "token_splits":
            {
                List<PartitionRangeReadCommand> commands = new ArrayList<>();
                long step = -(Long.MIN_VALUE / SPLITS) * 2;
                for (int i = 0; i < SPLITS; i++)
                {
                    long start = Long.MIN_VALUE + i * step;
                    long end = i == SPLITS - 1 ? Long.MAX_VALUE : start + step;
                    commands.addAll(pages(rangeCommand("SELECT * FROM %s WHERE token(pk) > ? AND token(pk) <= ?", start, end), SPLIT_PAGE_SIZE));
                }
                return commands;
            }
            case "filtered":
                return pages(rangeCommand("SELECT * FROM %s WHERE v1 = 3 ALLOW FILTERING"), FILTERED_PAGE_SIZE);
            default:
                throw new IllegalArgumentException(shape);
        }
    }

    private PartitionRangeReadCommand rangeCommand(String query, Object... values)
    {
        SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(formatQuery(query), ClientState.forInternalCalls());
        List<ByteBuffer> bound = new ArrayList<>(values.length);
        for (Object value : values)
            bound.add(LongType.instance.decompose((Long) value));
        QueryOptions options = QueryOptions.create(ConsistencyLevel.ONE, bound, false, -1, null, ConsistencyLevel.SERIAL,
                                                   ProtocolVersion.V5, KEYSPACE, Long.MIN_VALUE, nowInSec);
        return (PartitionRangeReadCommand) select.getQuery(options, nowInSec);
    }

    /**
     * The page commands the range pager sends for {@code base}, {@code pageSize} rows each: every page
     * after the first resumes after the last row of the one before, inside its partition, as
     * {@code PartitionRangeQueryPager.nextPageReadQuery} builds it.  Built by reading the pages on
     * the iterator path.
     */
    private List<PartitionRangeReadCommand> pages(PartitionRangeReadCommand base, int pageSize)
    {
        List<PartitionRangeReadCommand> pages = new ArrayList<>();
        PartitionRangeReadCommand page = base.withUpdatedLimit(base.limits().forPaging(pageSize));
        TableMetadata metadata = cfs.metadata();
        while (true)
        {
            pages.add(page);
            DecoratedKey lastKey = null;
            Clustering<?> lastClustering = null;
            int rowsInPage = 0;
            int rowsInLastPartition = 0;
            try (ReadExecutionController controller = page.executionController();
                 UnfilteredPartitionIterator partitions = page.executeLocally(controller))
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        rowsInLastPartition = 0;
                        while (partition.hasNext())
                        {
                            Unfiltered unfiltered = partition.next();
                            if (!unfiltered.isRow())
                                continue;
                            lastKey = partition.partitionKey();
                            lastClustering = ((Row) unfiltered).clustering();
                            rowsInPage++;
                            rowsInLastPartition++;
                        }
                    }
                }
            }
            if (rowsInPage < pageSize)
                return pages;
            AbstractBounds<PartitionPosition> bounds = new Bounds<>(lastKey, base.dataRange().keyRange().right);
            DataRange range = base.dataRange().forPaging(bounds, metadata.comparator, lastClustering, false);
            DataLimits limits = base.limits().forPaging(pageSize, lastKey.getKey(), Integer.MAX_VALUE - rowsInLastPartition);
            page = base.withUpdatedLimitsAndDataRange(limits, range);
        }
    }

    /**
     * Fails the trial unless both paths give the same replica responses and the cursor path served
     * every read.  Returns the rows one op reads.
     */
    private long checkPathsAgree()
    {
        List<PartitionRangeReadCommand> commands = commands();
        setPath(false);
        List<byte[]> iterator = responses(commands);
        long rows = countRows(commands);
        setPath(true);
        long servedBefore = CursorRangeReads.rangeReadsServed();
        long writtenBefore = CursorRangeReads.rangeResponsesWritten();
        long notServedBefore = CursorRangeReads.rangeReadsNotServed();
        List<byte[]> cursorResponses = responses(commands);
        long served = CursorRangeReads.rangeReadsServed() - servedBefore;
        long written = CursorRangeReads.rangeResponsesWritten() - writtenBefore;
        long notServed = CursorRangeReads.rangeReadsNotServed() - notServedBefore;
        setPath(false);
        long responseBytes = 0;
        for (int i = 0; i < commands.size(); i++)
        {
            if (!java.util.Arrays.equals(iterator.get(i), cursorResponses.get(i)))
                throw new IllegalStateException(shape + ": replica response " + i + " differs between the paths");
            responseBytes += iterator.get(i).length;
        }
        if (served < commands.size() || notServed > 0)
            throw new IllegalStateException(shape + ": the cursor path served " + served + " of " + commands.size() + " reads");
        System.out.println(String.format("RangeReadBench: reads=%d servedByCursor=%d responsesWrittenByCursor=%d responseBytesPerOp=%d",
                                         commands.size(), served, written, responseBytes));
        return rows;
    }

    private static List<byte[]> responses(List<PartitionRangeReadCommand> commands)
    {
        List<byte[]> responses = new ArrayList<>(commands.size());
        for (PartitionRangeReadCommand command : commands)
        {
            ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
            try (org.apache.cassandra.io.util.DataOutputBuffer out = new org.apache.cassandra.io.util.DataOutputBuffer())
            {
                ReadResponse.serializer.serialize(response, out, MessagingService.current_version);
                responses.add(out.toByteArray());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        return responses;
    }

    private static long countRows(List<PartitionRangeReadCommand> commands)
    {
        long rows = 0;
        for (PartitionRangeReadCommand command : commands)
        {
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller))
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        if (!partition.staticRow().isEmpty() || !partition.hasNext())
                            rows++; // a DISTINCT partition, read as its key
                        while (partition.hasNext())
                        {
                            if (partition.next().isRow())
                                rows++;
                        }
                    }
                }
            }
        }
        return rows;
    }

    // ---------------------------------------------------------------- data

    /** Hard-links the data set's sstables into the table, writing them into the cache first if absent. */
    private void linkCachedSSTables() throws IOException
    {
        String cacheRoot = System.getProperty("cassandra.bench.range_read.cache", "build/microbench-data/RangeReadBench"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        Path cache = Paths.get(cacheRoot, String.format("v%d-%s-%s-%d-%s-%s", GENERATOR_VERSION, format, data, sstables, compression, deletes)).toAbsolutePath();
        Path done = cache.resolve("complete");
        if (!Files.exists(done))
        {
            if (Files.exists(cache))
                try (Stream<Path> stale = Files.list(cache))
                {
                    for (Path p : (Iterable<Path>) stale::iterator)
                        Files.delete(p);
                }
            Files.createDirectories(cache);
            List<DecoratedKey> keys = sortedKeys();
            for (int s = 0; s < sstables; s++)
                writeSSTable(new File(cache), keys, s);
            Files.createFile(done);
        }

        List<Path> dataFiles = new ArrayList<>();
        try (Stream<Path> files = Files.list(cache))
        {
            files.filter(p -> p.getFileName().toString().endsWith("-Data.db")).forEach(dataFiles::add);
        }
        File target = cfs.getDirectories().getDirectoryForNewSSTables();
        List<SSTableReader> readers = new ArrayList<>();
        for (Path dataFile : dataFiles)
        {
            Descriptor cached = Descriptor.fromFileWithComponent(new File(dataFile), cfs.getKeyspaceName(), cfs.getTableName()).left;
            Descriptor linked = cfs.newSSTableDescriptor(target, cached.version);
            for (Component component : cached.discoverComponents())
                Files.createLink(linked.fileFor(component).toPath(), cached.fileFor(component).toPath());
            readers.add(SSTableReader.open(cfs, linked));
        }
        cfs.addSSTables(readers);
    }

    private List<DecoratedKey> sortedKeys()
    {
        List<DecoratedKey> keys = new ArrayList<>(partitions);
        for (long pk = 0; pk < partitions; pk++)
            keys.add(cfs.metadata().partitioner.decorateKey(LongType.instance.decompose(pk)));
        Collections.sort(keys);
        return keys;
    }

    /** Writes sstable {@code index}: the rows (pk, ck) with {@code (pk * 31 + ck) % sstables == index}. */
    private void writeSSTable(File directory, List<DecoratedKey> keys, int index)
    {
        TableMetadata metadata = cfs.metadata();
        Descriptor descriptor = cfs.newSSTableDescriptor(directory, DatabaseDescriptor.getSelectedSSTableFormat());
        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, descriptor, keys.size(), 0, null, false, header))
        {
            for (DecoratedKey key : keys)
            {
                long pk = LongType.instance.compose(key.getKey());
                GeneratedRows rows = new GeneratedRows(metadata, key, pk, index, BASE_TIMESTAMP + index);
                if (rows.isEmptyPartition())
                    continue;
                writer.append(rows);
            }
            writer.finish(false);
        }
    }

    /** One row in MEMTABLE_EVERY, overwritten in the memtable. */
    /** Every row of the data set, into the memtable, with the deletes of {@code deletes=mixed} left out. */
    private void writeAllToMemtable() throws Throwable
    {
        Random random = new Random(SEED);
        for (long pk = 0; pk < partitions; pk++)
            for (long ck = 0; ck < rowsPerPartition; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2, payload) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP " + BASE_TIMESTAMP,
                        pk, ck, (pk + ck) % 10, (int) ck, payload(random, pk, ck));
    }

    private void writeMemtableOverwrites() throws Throwable
    {
        Random random = new Random(SEED);
        long total = (long) partitions * rowsPerPartition;
        for (long n = MEMTABLE_EVERY / 2; n < total; n += MEMTABLE_EVERY)
        {
            long pk = n / rowsPerPartition;
            long ck = n % rowsPerPartition;
            execute("INSERT INTO %s (pk, ck, v1, v2, payload) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP " + (BASE_TIMESTAMP + 100),
                    pk, ck, (pk + ck) % 10, (int) ck, payload(random, pk, ck));
        }
    }

    private static final byte[] TEXT = new byte[8192];
    static
    {
        Random random = new Random(42);
        for (int i = 0; i < TEXT.length; i++)
            TEXT[i] = (byte) ('a' + random.nextInt(26));
    }

    /** Half random bytes and half text repeated across rows, so LZ4 compresses it some. */
    private static ByteBuffer payload(Random random, long pk, long ck)
    {
        byte[] bytes = new byte[PAYLOAD_BYTES];
        int randomPart = PAYLOAD_BYTES / 2;
        for (int i = 0; i < randomPart; i++)
            bytes[i] = (byte) random.nextInt();
        int offset = (int) ((pk * 1009 + ck * 7) % (TEXT.length - PAYLOAD_BYTES));
        System.arraycopy(TEXT, offset, bytes, randomPart, PAYLOAD_BYTES - randomPart);
        return ByteBuffer.wrap(bytes);
    }

    /** The rows of one partition in one sstable, built as the writer pulls them. */
    private final class GeneratedRows extends AbstractUnfilteredRowIterator
    {
        private final ColumnMetadata v1;
        private final ColumnMetadata v2;
        private final ColumnMetadata payload;
        private final long pk;
        private final long timestamp;
        private final Random random;
        private final int index;
        /** The range deletion of this partition in this sstable, {@code [rangeStart, rangeEnd)}; empty when start == end. */
        private final long rangeStart;
        private final long rangeEnd;
        private final DeletionTime rangeDeletion;
        private boolean rangeOpen;
        private boolean rangeClosed;
        private long ck;

        GeneratedRows(TableMetadata metadata, DecoratedKey key, long pk, int index, long timestamp)
        {
            super(metadata, key, partitionDeletion(pk, index), metadata.regularAndStaticColumns(), Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS);
            this.v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
            this.v2 = metadata.getColumn(ByteBufferUtil.bytes("v2"));
            this.payload = metadata.getColumn(ByteBufferUtil.bytes("payload"));
            this.pk = pk;
            this.index = index;
            this.timestamp = timestamp;
            this.random = new Random(SEED ^ (pk * 0x9E3779B97F4A7C15L) ^ index);
            boolean ranged = mixedDeletes && index == 0 && pk % 50 == 0 && rowsPerPartition >= 10;
            this.rangeStart = ranged ? rowsPerPartition / 10 : 0;
            this.rangeEnd = ranged ? rowsPerPartition / 5 : 0;
            this.rangeDeletion = DeletionTime.build(BASE_TIMESTAMP + 1000, DELETION_LOCAL_TIME);
            this.ck = nextCk(0);
        }

        private long nextCk(long from)
        {
            for (long c = from; c < rowsPerPartition; c++)
            {
                if ((pk * 31 + c) % sstables == index)
                    return c;
            }
            return rowsPerPartition;
        }

        boolean isEmptyPartition()
        {
            return ck >= rowsPerPartition && partitionLevelDeletion().isLive() && rangeStart == rangeEnd;
        }

        @Override
        protected Unfiltered computeNext()
        {
            if (rangeStart < rangeEnd)
            {
                if (!rangeOpen && ck >= rangeStart)
                {
                    rangeOpen = true;
                    return RangeTombstoneBoundMarker.inclusiveOpen(false, Clustering.make(LongType.instance.decompose(rangeStart)), rangeDeletion);
                }
                if (rangeOpen && !rangeClosed && ck >= rangeEnd)
                {
                    rangeClosed = true;
                    return RangeTombstoneBoundMarker.exclusiveClose(false, Clustering.make(LongType.instance.decompose(rangeEnd)), rangeDeletion);
                }
            }
            if (ck >= rowsPerPartition)
                return endOfData();
            Row row = buildRow(ck);
            ck = nextCk(ck + 1);
            return row;
        }

        private Row buildRow(long c)
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.make(LongType.instance.decompose(c)));
            int kind = mixedDeletes ? (int) ((pk + c) % 20) : -1;
            if (kind == 3)
            {
                builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(timestamp, DELETION_LOCAL_TIME)));
                return builder.build();
            }
            int ttl = kind == 7 ? SHORT_TTL : kind == 11 ? LONG_TTL : 0;
            ByteBuffer payloadValue = RangeReadBench.payload(random, pk, c);
            ByteBuffer v1Value = LongType.instance.decompose((pk + c) % 10);
            ByteBuffer v2Value = Int32Type.instance.decompose((int) c);
            // a sorted builder takes the cells in column order
            if (ttl == 0)
            {
                builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp));
                builder.addCell(BufferCell.live(payload, timestamp, payloadValue));
                builder.addCell(BufferCell.live(v1, timestamp, v1Value));
                builder.addCell(BufferCell.live(v2, timestamp, v2Value));
            }
            else
            {
                builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, ttl, TTL_WRITE_TIME));
                builder.addCell(BufferCell.expiring(payload, timestamp, ttl, TTL_WRITE_TIME, payloadValue));
                builder.addCell(BufferCell.expiring(v1, timestamp, ttl, TTL_WRITE_TIME, v1Value));
                builder.addCell(BufferCell.expiring(v2, timestamp, ttl, TTL_WRITE_TIME, v2Value));
            }
            return builder.build();
        }
    }

    /** The older partition deletion of one partition in 97, in sstable 0, with {@code deletes=mixed}. */
    private DeletionTime partitionDeletion(long pk, int index)
    {
        if (mixedDeletes && index == 0 && pk % 97 == 0)
            return DeletionTime.build(BASE_TIMESTAMP - 1, DELETION_LOCAL_TIME);
        return DeletionTime.LIVE;
    }
}
