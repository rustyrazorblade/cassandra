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
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Reads one partition of a table with many columns through the cursor read path and the iterator
 * path, on the same sstables, to measure the cost per row and per cell of a full scan.
 *
 * <p>Schemas ({@code columns}):
 * <ul>
 * <li>{@code 17}: the flight-bench {@code fact} table: three primary key columns, one static
 *     text column, and thirteen regular columns of int, bigint, text, decimal, uuid, a frozen set
 *     and a non-frozen map.</li>
 * <li>{@code 1000}: two primary key columns and 998 regular columns; one in ten is text, the
 *     rest int.  Every cell is written.</li>
 * </ul>
 *
 * <p>Layouts: {@code one} sstable; {@code four} sstables with the rows interleaved
 * (row {@code r} goes to sstable {@code r % 4}); {@code four_plus_memtable}, the same with every
 * thousandth row written again in the memtable.  {@code memtable} picks the memtable allocation
 * type for the last layout.
 *
 * <p>Shapes: {@code wildcard} is {@code SELECT *}; {@code all_listed} names every column, as a bulk
 * reader that lists its projection does, so every regular column is fetched and queried;
 * {@code project3} names three regular columns, so the others are fetched but not queried.  Each
 * read is the whole partition, in pages of {@code pageSize} rows, through the replica response
 * ({@code ReadCommandVerbHandler.doRead}).
 *
 * <p>The {@code interleaved} benchmark runs both paths in one trial in alternating blocks and
 * counts each path's thread CPU time and allocation, so both see the same machine state.  CPU
 * ns/row is {@code cursorCpuNanos / cursorRows}; B/row is {@code cursorAllocatedBytes / cursorRows}.
 * Sstables are written once per data set into {@code -Dcassandra.bench.cursor_schema.cache=<dir>}
 * (default {@code build/microbench-data/CursorWideSchemaReadBench}) and hard-linked into each trial.
 *
 * <pre>
 *   ant microbench -Dbenchmark.name=CursorWideSchemaReadBench.interleaved \
 *       -Djmh.args="-p columns=17 -p rows=100000 -p layout=one -p shape=all_listed"
 *   ant microbench -Dbenchmark.name=CursorWideSchemaReadBench.interleaved \
 *       -Djmh.args="-p columns=1000 -p rows=10000 -p pageSize=1000 -jvmArgsAppend -Xmx4G"
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 4, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 4, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CursorWideSchemaReadBench extends CQLTester
{
    private static final long PK = 1L;
    private static final long BASE_TIMESTAMP = 1_000_000L;
    /** One row in this many is written again in the memtable of the {@code four_plus_memtable} layout. */
    private static final int MEMTABLE_EVERY = 1000;
    /** In the 1000-column schema, one column in this many is text. */
    private static final int TEXT_EVERY = 10;

    @Param({ "17", "1000" })
    public int columns;

    @Param({ "100000" })
    public int rows;

    @Param({ "5000" })
    public int pageSize;

    @Param({ "one", "four", "four_plus_memtable" })
    public String layout;

    @Param({ "heap_buffers" })
    public String memtable;

    @Param({ "all_listed", "project3", "wildcard" })
    public String shape;

    private ColumnFamilyStore cfs;
    private long nowInSec;
    private boolean cursorReadsWas;
    private String selectedFormatWas;
    private String table;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        ServerTestUtils.daemonInitialization();
        Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
        confField.setAccessible(true);
        ((Config) confField.get(null)).memtable_allocation_type = Config.MemtableAllocationType.valueOf(memtable);
        prePrepareServer();
        prepareServer();
        beforeTest(); // JMH does not run JUnit @Before
        cursorReadsWas = DatabaseDescriptor.cursorReadsEnabled();
        selectedFormatWas = DatabaseDescriptor.getSelectedSSTableFormat().name();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");

        createTable("CREATE TABLE %s (" + schema() + ") WITH compression = {'class': 'LZ4Compressor'} AND memtable = 'trie' " +
                    "AND compaction = {'class': 'UnifiedCompactionStrategy'} AND gc_grace_seconds = 864000");
        table = formatQuery("%s");
        cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        nowInSec = FBUtilities.nowInSeconds();

        int sstables = "one".equals(layout) ? 1 : 4;
        loadSSTables(sstables);
        if ("four_plus_memtable".equals(layout))
        {
            for (int r = MEMTABLE_EVERY / 2; r < rows; r += MEMTABLE_EVERY)
                insert(r, BASE_TIMESTAMP + 100);
        }

        checkPathsAgree();
        long cells = cellsPerRow();
        System.out.println(String.format("CursorWideSchemaReadBench: columns=%d rows=%d layout=%s memtable=%s shape=%s sstables=%d " +
                                         "pages=%d cellsPerRow=%d",
                                         columns, rows, layout, memtable, shape, cfs.getLiveSSTables().size(),
                                         commands().size(), cells));
    }

    @TearDown(Level.Trial)
    public void teardown() throws Throwable
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursorReadsWas);
        DatabaseDescriptor.setSelectedSSTableFormat(selectedFormatWas);
        CQLTester.tearDownClass();
    }

    // ---------------------------------------------------------------- benchmark

    /** The totals of {@link #interleaved}, per path. */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class PathCounters
    {
        public long cursorRows;
        public long cursorCpuNanos;
        public long cursorAllocatedBytes;
        public long iteratorRows;
        public long iteratorCpuNanos;
        public long iteratorAllocatedBytes;

        @Setup(Level.Iteration)
        public void reset()
        {
            cursorRows = cursorCpuNanos = cursorAllocatedBytes = 0;
            iteratorRows = iteratorCpuNanos = iteratorAllocatedBytes = 0;
        }
    }

    /** The commands of one read, one set per thread. */
    @State(Scope.Thread)
    public static class Reads
    {
        List<SinglePartitionReadCommand> commands;

        @Setup(Level.Trial)
        public void setup(CursorWideSchemaReadBench bench)
        {
            commands = bench.commands();
        }
    }

    /** Both paths in one trial, one whole-partition read each, alternately, timed on the thread's CPU clock. */
    @Benchmark
    public void interleaved(Reads reads, PathCounters counters, Blackhole bh)
    {
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long thread = Thread.currentThread().getId();
        for (boolean onCursorPath : new boolean[]{ true, false })
        {
            DatabaseDescriptor.setCursorReadsEnabled(onCursorPath);
            long cpu = threads.getCurrentThreadCpuTime();
            long allocated = threads.getThreadAllocatedBytes(thread);
            if (onCursorPath)
                cursorRead(reads, bh);
            else
                iteratorRead(reads, bh);
            cpu = threads.getCurrentThreadCpuTime() - cpu;
            allocated = threads.getThreadAllocatedBytes(thread) - allocated;
            if (onCursorPath)
            {
                counters.cursorRows += rows;
                counters.cursorCpuNanos += cpu;
                counters.cursorAllocatedBytes += allocated;
            }
            else
            {
                counters.iteratorRows += rows;
                counters.iteratorCpuNanos += cpu;
                counters.iteratorAllocatedBytes += allocated;
            }
        }
    }

    /** The cursor path's read of {@link #interleaved}, in its own frame so a profile can tell them apart. */
    private void cursorRead(Reads reads, Blackhole bh)
    {
        for (SinglePartitionReadCommand command : reads.commands)
            bh.consume(replicaRead(command));
    }

    /** The iterator path's read of {@link #interleaved}, in its own frame. */
    private void iteratorRead(Reads reads, Blackhole bh)
    {
        for (SinglePartitionReadCommand command : reads.commands)
            bh.consume(replicaRead(command));
    }

    private static long replicaRead(SinglePartitionReadCommand command)
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
        return ReadResponse.serializer.serializedSize(response, MessagingService.current_version);
    }

    // ---------------------------------------------------------------- schema and shapes

    private String schema()
    {
        if (columns == 17)
            return "pk bigint, event_time timestamp, row_id bigint, region text static, i32 int, i64 bigint, bucket int, " +
                   "name text, amount decimal, uid uuid, ref_id bigint, g_low int, g_mid int, g_high bigint, dim2_id int, " +
                   "tags frozen<set<int>>, attrs map<text, int>, PRIMARY KEY ((pk), event_time, row_id)";
        if (columns == 1000)
        {
            StringBuilder s = new StringBuilder("pk bigint, ck bigint");
            for (int c = 0; c < 998; c++)
                s.append(", ").append(wideColumn(c)).append(c % TEXT_EVERY == 0 ? " text" : " int");
            return s.append(", PRIMARY KEY ((pk), ck)").toString();
        }
        throw new IllegalArgumentException("columns=" + columns);
    }

    private static String wideColumn(int c)
    {
        return String.format("c%04d", c);
    }

    private List<String> projection()
    {
        if (columns == 17)
            return Arrays.asList("i32", "name", "amount");
        return Arrays.asList(wideColumn(1), wideColumn(500), wideColumn(997));
    }

    private ColumnFilter columnFilter(TableMetadata metadata)
    {
        switch (shape)
        {
            case "wildcard":
                return ColumnFilter.all(metadata);
            case "all_listed":
            {
                ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(metadata, false);
                builder.addAll(metadata.regularAndStaticColumns());
                return builder.build();
            }
            case "project3":
            {
                ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(metadata, false);
                for (String name : projection())
                    builder.add(metadata.getColumn(ByteBufferUtil.bytes(name)));
                return builder.build();
            }
            default:
                throw new IllegalArgumentException(shape);
        }
    }

    /** The commands of one read of the whole partition, one per page. */
    List<SinglePartitionReadCommand> commands()
    {
        TableMetadata metadata = cfs.metadata();
        DecoratedKey key = metadata.partitioner.decorateKey(LongType.instance.decompose(PK));
        DataLimits limits = DataLimits.cqlLimits(Integer.MAX_VALUE).forPaging(pageSize);
        SinglePartitionReadCommand first = SinglePartitionReadCommand.create(metadata, nowInSec, columnFilter(metadata), RowFilter.none(),
                                                                             limits, key, new ClusteringIndexSliceFilter(Slices.ALL, false));
        List<SinglePartitionReadCommand> commands = new ArrayList<>();
        commands.add(first);
        for (int page = 1; (long) page * pageSize < rows; page++)
            commands.add(first.forPaging(clustering(page * pageSize - 1), limits));
        return commands;
    }

    private Clustering<?> clustering(int r)
    {
        if (columns == 17)
            return Clustering.make(ByteBufferUtil.bytes(eventTime(r)), LongType.instance.decompose((long) r));
        return Clustering.make(LongType.instance.decompose((long) r));
    }

    private static long eventTime(int r)
    {
        return 1_700_000_000_000L + r * 1000L;
    }

    // ---------------------------------------------------------------- data

    private void insert(int r, long timestamp) throws Throwable
    {
        if (columns == 17)
        {
            Set<Integer> tags = new LinkedHashSet<>(Arrays.asList(r % 5, 5 + r % 7));
            Map<String, Integer> attrs = new LinkedHashMap<>();
            attrs.put("a", r);
            attrs.put("b", r % 3);
            executeFormattedQuery("INSERT INTO " + table + " (pk, event_time, row_id, region, i32, i64, bucket, name, amount, uid, ref_id, " +
                                  "g_low, g_mid, g_high, dim2_id, tags, attrs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                  "USING TIMESTAMP " + timestamp,
                                  PK, new Date(eventTime(r)), (long) r, "us-west-2", r, r * 7L, r % 16, "name-" + (r % 1000),
                                  BigDecimal.valueOf(r, 2), new UUID(r, r * 31L), (long) (r % 10000), r % 10, r % 10000, r * 13L,
                                  r % 100, tags, attrs);
            return;
        }
        Object[] values = new Object[1000];
        values[0] = PK;
        values[1] = (long) r;
        for (int c = 0; c < 998; c++)
            values[c + 2] = c % TEXT_EVERY == 0 ? "t" + (r * 31 + c) % 100000 : r + c;
        executeFormattedQuery(wideInsert(timestamp), values);
    }

    private String wideInsertQuery;

    private String wideInsert(long timestamp)
    {
        if (wideInsertQuery == null)
        {
            StringBuilder names = new StringBuilder("pk, ck");
            StringBuilder marks = new StringBuilder("?, ?");
            for (int c = 0; c < 998; c++)
            {
                names.append(", ").append(wideColumn(c));
                marks.append(", ?");
            }
            wideInsertQuery = "INSERT INTO " + table + " (" + names + ") VALUES (" + marks + ") USING TIMESTAMP ";
        }
        return wideInsertQuery + timestamp;
    }

    /** Links the data set's sstables into the table, writing them into the cache first if absent. */
    private void loadSSTables(int sstables) throws Throwable
    {
        String cacheRoot = System.getProperty("cassandra.bench.cursor_schema.cache", "build/microbench-data/CursorWideSchemaReadBench"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        Path cache = Paths.get(cacheRoot, String.format("bti-%d-%d-%d", columns, rows, sstables)).toAbsolutePath();
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
            for (int s = 0; s < sstables; s++)
            {
                // the memtable may flush by size part way, so the rows of one sstable are
                // compacted into one, kept in the cache, and taken out of the table
                for (int r = s; r < rows; r += sstables)
                    insert(r, BASE_TIMESTAMP + s);
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                if (cfs.getLiveSSTables().size() > 1)
                    cfs.forceMajorCompaction();
                if (cfs.getLiveSSTables().size() != 1)
                    throw new IllegalStateException("expected one sstable, found " + cfs.getLiveSSTables().size());
                for (SSTableReader sstable : cfs.getLiveSSTables())
                    for (Component component : sstable.descriptor.discoverComponents())
                        Files.copy(sstable.descriptor.fileFor(component).toPath(), cache.resolve(sstable.descriptor.fileFor(component).name()));
                cfs.clearUnsafe();
            }
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

    // ---------------------------------------------------------------- checks

    /** Fails the trial unless both paths return the same rows and the cursor path served every leg. */
    private void checkPathsAgree() throws IOException
    {
        long[] iterator = rowHashes(false);
        long fellBackBefore = CursorReads.sstableLegsFellBackToIterator();
        long servedBefore = CursorReads.sstableLegsServed();
        long[] cursorRows = rowHashes(true);
        if (iterator.length != rows || cursorRows.length != rows)
            throw new IllegalStateException(String.format("%s: expected %d rows, iterator read %d, cursor read %d",
                                                          shape, rows, iterator.length, cursorRows.length));
        for (int i = 0; i < rows; i++)
        {
            if (iterator[i] != cursorRows[i])
                throw new IllegalStateException(String.format("%s: row %d differs", shape, i));
        }
        if (CursorReads.sstableLegsServed() == servedBefore || CursorReads.sstableLegsFellBackToIterator() != fellBackBefore)
            throw new IllegalStateException(shape + ": the cursor path did not serve every leg");
        // the replica response bytes must be the same too
        for (SinglePartitionReadCommand command : commands())
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
            ReadResponse expected = ReadCommandVerbHandler.instance.doRead(command, false);
            DatabaseDescriptor.setCursorReadsEnabled(true);
            ReadResponse actual = ReadCommandVerbHandler.instance.doRead(command, false);
            if (!serialized(expected).equals(serialized(actual)))
                throw new IllegalStateException(shape + ": replica response bytes differ");
        }
    }

    private static ByteBuffer serialized(ReadResponse response) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, out, MessagingService.current_version);
            return out.asNewBuffer();
        }
    }

    private long cellsPerRow()
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        SinglePartitionReadCommand command = commands().get(0);
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller);
             UnfilteredRowIterator partition = partitions.next())
        {
            Unfiltered unfiltered = partition.next();
            int cells = 0;
            for (Cell<?> ignored : ((Row) unfiltered).cells())
                cells++;
            return cells;
        }
    }

    /** A hash of each row the read returns: clustering, liveness, and each cell's column, path, timestamp and value. */
    private long[] rowHashes(boolean onCursorPath)
    {
        DatabaseDescriptor.setCursorReadsEnabled(onCursorPath);
        long[] hashes = new long[rows];
        int count = 0;
        for (SinglePartitionReadCommand command : commands())
        {
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller))
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        while (partition.hasNext())
                        {
                            Unfiltered unfiltered = partition.next();
                            if (!unfiltered.isRow())
                                continue;
                            Row row = (Row) unfiltered;
                            long h = row.clustering().hashCode();
                            h = h * 31 + row.primaryKeyLivenessInfo().timestamp();
                            for (Cell<?> cell : row.cells())
                            {
                                h = (h * 31 + cell.column().name.hashCode()) * 31 + cell.timestamp();
                                h = h * 31 + cell.buffer().hashCode();
                                if (cell.path() != null)
                                    h = h * 31 + cell.path().get(0).hashCode();
                            }
                            if (count == hashes.length)
                                hashes = Arrays.copyOf(hashes, hashes.length * 2 + 1);
                            hashes[count++] = h;
                        }
                    }
                }
            }
        }
        return Arrays.copyOf(hashes, count);
    }
}
