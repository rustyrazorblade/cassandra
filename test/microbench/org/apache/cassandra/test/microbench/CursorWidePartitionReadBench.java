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
import java.util.Arrays;
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
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTreeSet;

/**
 * Reads one wide partition through the cursor read path and the iterator path
 * ({@code cursor_reads_enabled} on and off), on the same sstables.
 *
 * <p>Each data set is one partition of {@code rows} rows of about {@code rowSize} bytes.  The
 * {@code one} layout is a single sstable.  The {@code four_plus_memtable} layout spreads the rows
 * over four sstables (row {@code ck} goes to sstable {@code ck % 4}, so every row merges across
 * four sstables) and overwrites one row in a thousand in the memtable.  Sstables are written once
 * per data set into {@code -Dcassandra.bench.cursor_wide.cache=<dir>} (default
 * {@code build/microbench-data/CursorWidePartitionReadBench}) and hard-linked into each trial.
 *
 * <p>Entries:
 * <ul>
 * <li>{@code local}: {@code executeLocally}, every cell consumed.</li>
 * <li>{@code replica}: {@code ReadCommandVerbHandler.doRead}, the replica response path
 *     ({@code createResponseLocally}, the cursor path writes the response bytes directly).</li>
 * <li>{@code coordinator}: {@code executeLocally} plus the in-memory object response the
 *     coordinator builds for its own local read (CASSANDRA-21354).</li>
 * </ul>
 *
 * <p>The counters (totals over the measured iterations): {@code cpuNanos} is the reading thread's
 * CPU time, steadier than wall time on a shared machine; {@code rows} is the rows of the partition the read covers (for {@code filtered},
 * the whole partition, though a tenth matches), {@code diskBytes} is the share of the sstables'
 * on-disk bytes those rows take.  CPU ns/row is {@code cpuNanos / rows}; B/row is
 * {@code gc.alloc.rate.norm} divided by rows per op.
 *
 * <p>Subsets (one trial per param combination; keep each run small):
 * <pre>
 *   # the main comparison, BTI, 100K rows of 1 KiB, every shape, both paths, replica entry
 *   ant microbench -Dbenchmark.name=CursorWidePartitionReadBench \
 *       -Djmh.args="-p rows=100000 -p rowSize=1024 -p layout=one -p compression=lz4 -p entry=replica -prof gc"
 *   # the same through executeLocally / the coordinator-local response
 *   ... -p entry=local ...    ... -p entry=coordinator ...
 *   # the merge: four sstables plus memtable
 *   ... -p layout=four_plus_memtable ...
 *   # one shape, thread scaling
 *   ... -p shape=full_paged -p cursor=true -t 4 ...
 *   # BIG, to validate the setup only
 *   ... -p format=big -p shape=full_paged ...
 *   # 1M rows of 8 KiB is about 8 GB per data set; add -jvmArgsAppend -Xmx4G for the iterator path
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CursorWidePartitionReadBench extends CQLTester
{
    private static final long PK = 1L;
    private static final int PAGE_SIZE = 5000;
    private static final int SLICE_ROWS = 1000;
    private static final int MULTI_SLICE_ROWS = 100;
    private static final int LIMIT = 10;
    private static final long BASE_TIMESTAMP = 1_000_000L;
    /** One row in this many is overwritten in the memtable of the {@code four_plus_memtable} layout. */
    private static final int MEMTABLE_EVERY = 1000;
    /** v1 = ck % FILTER_MODULUS; the filtered shape keeps v1 = 0. */
    private static final int FILTER_MODULUS = 10;

    @Param({ "100000" })
    public int rows;

    @Param({ "1024" })
    public int rowSize;

    @Param({ "one", "four_plus_memtable" })
    public String layout;

    @Param({ "lz4" })
    public String compression;

    @Param({ "bti" })
    public String format;

    @Param({ "limit10", "limit10_reversed", "slice1000", "full_paged", "names2", "multi_slice3", "filtered" })
    public String shape;

    @Param({ "true", "false" })
    public boolean cursor;

    @Param({ "replica" })
    public String entry;

    private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

    private ColumnFamilyStore cfs;
    private long nowInSec;
    private long onDiskBytes;
    private boolean cursorReadsWas;
    private String selectedFormatWas;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        beforeTest(); // JMH does not run JUnit @Before
        cursorReadsWas = DatabaseDescriptor.cursorReadsEnabled();
        selectedFormatWas = DatabaseDescriptor.getSelectedSSTableFormat().name();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        DatabaseDescriptor.setCursorReadsEnabled(cursor);

        String compressionOption = "off".equals(compression) ? "{'enabled': 'false'}" : "{'class': 'LZ4Compressor'}";
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 int, payload blob, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = " + compressionOption + " AND memtable = 'trie' " +
                    "AND compaction = {'class': 'UnifiedCompactionStrategy'} AND gc_grace_seconds = 864000");
        cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        nowInSec = FBUtilities.nowInSeconds();

        int sstables = "one".equals(layout) ? 1 : 4;
        linkCachedSSTables(sstables);
        if (!"one".equals(layout))
            writeMemtableOverwrites();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            onDiskBytes += sstable.onDiskLength();

        checkPathsAgree();
        DatabaseDescriptor.setCursorReadsEnabled(cursor);
        System.out.println(String.format("CursorWidePartitionReadBench: format=%s rows=%d rowSize=%d layout=%s compression=%s sstables=%d " +
                                         "onDiskBytes=%d rowsPerOp=%d returnedPerOp=%d",
                                         format, rows, rowSize, layout, compression, cfs.getLiveSSTables().size(), onDiskBytes,
                                         rowsCovered(), rowsReturned()));
    }

    @TearDown(Level.Trial)
    public void teardown() throws Throwable
    {
        DatabaseDescriptor.setCursorReadsEnabled(cursorReadsWas);
        DatabaseDescriptor.setSelectedSSTableFormat(selectedFormatWas);
        CQLTester.tearDownClass();
    }

    // ---------------------------------------------------------------- benchmark

    /** Rows and on-disk bytes covered, as rates. */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class Counters
    {
        public long rows;
        public long diskBytes;
        /** CPU time of the reading thread; steadier than wall time on a shared machine.  Read
         *  once per iteration, since reading it costs about a microsecond on macOS. */
        public long cpuNanos;
        private long cpuAtStart;

        @Setup(Level.Iteration)
        public void reset()
        {
            rows = 0;
            diskBytes = 0;
            cpuNanos = 0;
            cpuAtStart = THREADS.getCurrentThreadCpuTime();
        }

        @TearDown(Level.Iteration)
        public void recordCpu()
        {
            cpuNanos = THREADS.getCurrentThreadCpuTime() - cpuAtStart;
        }
    }

    /** The commands of one read, one set per thread. */
    @State(Scope.Thread)
    public static class Reads
    {
        List<SinglePartitionReadCommand> commands;

        @Setup(Level.Trial)
        public void setup(CursorWidePartitionReadBench bench)
        {
            commands = bench.commands();
        }
    }

    @Benchmark
    public void read(Reads reads, Counters counters, Blackhole bh)
    {
        for (SinglePartitionReadCommand command : reads.commands)
            bh.consume(readOnce(command, bh));
        long covered = rowsCovered();
        counters.rows += covered;
        counters.diskBytes += onDiskBytes * covered / rows;
    }

    private long readOnce(SinglePartitionReadCommand command, Blackhole bh)
    {
        switch (entry)
        {
            case "local":
                try (ReadExecutionController controller = command.executionController();
                     UnfilteredPartitionIterator partitions = command.executeLocally(controller))
                {
                    return drain(partitions, bh);
                }
            case "replica":
            {
                ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
                return ReadResponse.serializer.serializedSize(response, MessagingService.current_version);
            }
            case "coordinator":
                try (ReadExecutionController controller = command.executionController();
                     UnfilteredPartitionIterator partitions = command.executeLocally(controller))
                {
                    ReadResponse response = command.createLocalObjectResponse(partitions, controller.getRepairedDataInfo(), true);
                    bh.consume(response);
                    return 1;
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

    /** The rows of the partition one read covers. */
    private long rowsCovered()
    {
        switch (shape)
        {
            case "limit10":
            case "limit10_reversed":
                return LIMIT;
            case "slice1000":
                return SLICE_ROWS;
            case "names2":
                return 2;
            case "multi_slice3":
                return 3L * MULTI_SLICE_ROWS;
            case "full_paged":
            case "filtered":
                return rows;
            default:
                throw new IllegalArgumentException(shape);
        }
    }

    private long rowsReturned()
    {
        return "filtered".equals(shape) ? rows / FILTER_MODULUS : rowsCovered();
    }

    /** The commands of one read: one, or one per page for the paged shapes. */
    List<SinglePartitionReadCommand> commands()
    {
        TableMetadata metadata = cfs.metadata();
        DecoratedKey key = metadata.partitioner.decorateKey(LongType.instance.decompose(PK));
        ColumnFilter columns = ColumnFilter.all(metadata);
        List<SinglePartitionReadCommand> commands = new ArrayList<>();
        switch (shape)
        {
            case "limit10":
            case "limit10_reversed":
                commands.add(SinglePartitionReadCommand.create(metadata, nowInSec, columns, RowFilter.none(), DataLimits.cqlLimits(LIMIT), key,
                                                               new ClusteringIndexSliceFilter(Slices.ALL, "limit10_reversed".equals(shape))));
                break;
            case "slice1000":
            {
                long start = rows / 2;
                commands.add(SinglePartitionReadCommand.create(metadata, nowInSec, columns, RowFilter.none(), DataLimits.NONE, key,
                                                               sliceFilter(metadata, new long[]{ start }, SLICE_ROWS)));
                break;
            }
            case "multi_slice3":
                commands.add(SinglePartitionReadCommand.create(metadata, nowInSec, columns, RowFilter.none(), DataLimits.NONE, key,
                                                               sliceFilter(metadata, new long[]{ rows / 10, rows / 2, rows * 9L / 10 }, MULTI_SLICE_ROWS)));
                break;
            case "names2":
            {
                BTreeSet.Builder<Clustering<?>> names = BTreeSet.builder(metadata.comparator);
                names.add(Clustering.make(LongType.instance.decompose((long) rows / 10)));
                names.add(Clustering.make(LongType.instance.decompose(rows * 9L / 10)));
                commands.add(SinglePartitionReadCommand.create(metadata, nowInSec, columns, RowFilter.none(), DataLimits.NONE, key,
                                                               new ClusteringIndexNamesFilter(names.build(), false)));
                break;
            }
            case "full_paged":
                pages(commands, metadata, key, columns, RowFilter.none(), 1);
                break;
            case "filtered":
            {
                RowFilter filter = RowFilter.create(true);
                ColumnMetadata v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
                filter.add(v1, Operator.EQ, LongType.instance.decompose(0L));
                pages(commands, metadata, key, columns, filter, FILTER_MODULUS);
                break;
            }
            default:
                throw new IllegalArgumentException(shape);
        }
        return commands;
    }

    /**
     * A paged read of the whole partition, PAGE_SIZE returned rows per page; one row in
     * {@code stride} matches, so page k resumes after clustering {@code (k * PAGE_SIZE - 1) * stride}.
     */
    private void pages(List<SinglePartitionReadCommand> commands, TableMetadata metadata, DecoratedKey key,
                       ColumnFilter columns, RowFilter filter, int stride)
    {
        DataLimits limits = DataLimits.cqlLimits(Integer.MAX_VALUE).forPaging(PAGE_SIZE);
        SinglePartitionReadCommand first = SinglePartitionReadCommand.create(metadata, nowInSec, columns, filter, limits, key,
                                                                             new ClusteringIndexSliceFilter(Slices.ALL, false));
        commands.add(first);
        long matching = rows / stride;
        for (long page = 1; page * PAGE_SIZE < matching; page++)
        {
            long lastReturned = (page * PAGE_SIZE - 1) * stride;
            commands.add(first.forPaging(Clustering.make(LongType.instance.decompose(lastReturned)), limits));
        }
    }

    private static ClusteringIndexSliceFilter sliceFilter(TableMetadata metadata, long[] starts, int width)
    {
        Slices.Builder slices = new Slices.Builder(metadata.comparator);
        for (long start : starts)
            slices.add(Slice.make(ClusteringBound.create(metadata.comparator, true, true, start),
                                  ClusteringBound.create(metadata.comparator, false, false, start + width)));
        return new ClusteringIndexSliceFilter(slices.build(), false);
    }

    /**
     * Fails the trial unless both paths return the same rows, and the cursor path served the read
     * without a leg falling back to the iterator.
     */
    private void checkPathsAgree()
    {
        long[] iterator = rowHashes(false);
        long servedBefore = CursorReads.sstableLegsServed();
        long fellBackBefore = CursorReads.sstableLegsFellBackToIterator();
        long transcodedBefore = CursorReads.transcodeResponsesServed();
        long[] cursorRows = rowHashes(true);
        if (iterator.length != cursorRows.length)
            throw new IllegalStateException(String.format("%s: iterator rows=%d, cursor rows=%d", shape, iterator.length, cursorRows.length));
        for (int i = 0; i < iterator.length; i++)
        {
            if (iterator[i] != cursorRows[i])
                throw new IllegalStateException(String.format("%s: row %d differs", shape, i));
        }
        if (iterator.length != rowsReturned())
            throw new IllegalStateException(shape + ": expected " + rowsReturned() + " rows, read " + iterator.length);
        if (CursorReads.sstableLegsServed() == servedBefore || CursorReads.sstableLegsFellBackToIterator() != fellBackBefore)
            throw new IllegalStateException(shape + ": the cursor path did not serve every leg");
        // run the replica path once on the cursor path to report whether it wrote the bytes directly
        DatabaseDescriptor.setCursorReadsEnabled(true);
        for (SinglePartitionReadCommand command : commands())
            ReadCommandVerbHandler.instance.doRead(command, false);
        System.out.println(String.format("CursorWidePartitionReadBench: shape=%s replica responses written by the cursor path: %d of %d",
                                         shape, CursorReads.transcodeResponsesServed() - transcodedBefore, commands().size()));
    }

    /** A hash of each row the read returns: clustering, liveness, and each cell's column, timestamp and value. */
    private long[] rowHashes(boolean onCursorPath)
    {
        DatabaseDescriptor.setCursorReadsEnabled(onCursorPath);
        long[] hashes = new long[(int) rowsReturned()];
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
                            long h = LongType.instance.compose(row.clustering().bufferAt(0));
                            h = h * 31 + row.primaryKeyLivenessInfo().timestamp();
                            for (Cell<?> cell : row.cells())
                                h = (h * 31 + cell.column().name.hashCode()) * 31 * 31 + cell.timestamp() * 31 + cell.buffer().hashCode();
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

    // ---------------------------------------------------------------- data

    /** Hard-links the data set's sstables into the table, writing them into the cache first if absent. */
    private void linkCachedSSTables(int sstables) throws IOException
    {
        String cacheRoot = System.getProperty("cassandra.bench.cursor_wide.cache", "build/microbench-data/CursorWidePartitionReadBench"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        Path cache = Paths.get(cacheRoot, String.format("%s-%d-%d-%s-%s", format, rows, rowSize, layout, compression)).toAbsolutePath();
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
                writeSSTable(new File(cache), s, sstables);
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

    /** Writes sstable {@code index} of {@code count}: the rows with {@code ck % count == index}. */
    private void writeSSTable(File directory, int index, int count)
    {
        TableMetadata metadata = cfs.metadata();
        Descriptor descriptor = cfs.newSSTableDescriptor(directory, DatabaseDescriptor.getSelectedSSTableFormat());
        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
        DecoratedKey key = metadata.partitioner.decorateKey(LongType.instance.decompose(PK));
        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, descriptor, 1, 0, null, false, header))
        {
            writer.append(new GeneratedRows(metadata, key, index, count, BASE_TIMESTAMP + index));
            writer.finish(false);
        }
    }

    /** The memtable part of the {@code four_plus_memtable} layout: one row in MEMTABLE_EVERY, overwritten. */
    private void writeMemtableOverwrites() throws Throwable
    {
        Random random = new Random(rows);
        for (long ck = MEMTABLE_EVERY / 2; ck < rows; ck += MEMTABLE_EVERY)
            execute("INSERT INTO %s (pk, ck, v1, v2, payload) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP " + (BASE_TIMESTAMP + 100),
                    PK, ck, ck % FILTER_MODULUS, (int) ck, payload(random, ck));
    }

    private static final byte[] TEXT = new byte[8192];
    static
    {
        Random random = new Random(42);
        for (int i = 0; i < TEXT.length; i++)
            TEXT[i] = (byte) ('a' + random.nextInt(26));
    }

    /** About half random bytes and half text repeated across rows, so LZ4 compresses it some. */
    private ByteBuffer payload(Random random, long ck)
    {
        int size = Math.max(8, rowSize - 28);
        byte[] bytes = new byte[size];
        int randomPart = size / 2;
        for (int i = 0; i < randomPart; i++)
            bytes[i] = (byte) random.nextInt();
        int offset = (int) (ck * 7 % (TEXT.length - size));
        System.arraycopy(TEXT, offset, bytes, randomPart, size - randomPart);
        return ByteBuffer.wrap(bytes);
    }

    /** The rows of one sstable, built as the writer pulls them. */
    private final class GeneratedRows extends AbstractUnfilteredRowIterator
    {
        private final ColumnMetadata v1;
        private final ColumnMetadata v2;
        private final ColumnMetadata payload;
        private final long timestamp;
        private final int count;
        private final Random random;
        private long ck;

        GeneratedRows(TableMetadata metadata, DecoratedKey key, int index, int count, long timestamp)
        {
            super(metadata, key, DeletionTime.LIVE, metadata.regularAndStaticColumns(), Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS);
            this.v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
            this.v2 = metadata.getColumn(ByteBufferUtil.bytes("v2"));
            this.payload = metadata.getColumn(ByteBufferUtil.bytes("payload"));
            this.timestamp = timestamp;
            this.count = count;
            this.random = new Random(rows * 31L + index);
            this.ck = index;
        }

        @Override
        protected Unfiltered computeNext()
        {
            if (ck >= rows)
                return endOfData();
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.make(LongType.instance.decompose(ck)));
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp));
            // a sorted builder takes the cells in column order
            builder.addCell(BufferCell.live(payload, timestamp, CursorWidePartitionReadBench.this.payload(random, ck)));
            builder.addCell(BufferCell.live(v1, timestamp, LongType.instance.decompose(ck % FILTER_MODULUS)));
            builder.addCell(BufferCell.live(v2, timestamp, Int32Type.instance.decompose((int) ck)));
            ck += count;
            return builder.build();
        }
    }
}
