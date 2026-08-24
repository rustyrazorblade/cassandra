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

package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.sun.management.ThreadMXBean;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.ActiveCompactions;
import org.apache.cassandra.db.compaction.CompactionTasks;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

/**
 * Benchmark for the cost of writing a single large partition through {@link BigFormatPartitionWriter}.
 *
 * <p>{@link BigFormatPartitionWriter#addIndexBlock()} used to grow its on-heap {@code indexOffsets} array by a fixed
 * increment of 10 elements, so a partition with n index blocks performed n/10 reallocations copying O(n^2) bytes in
 * total. A 20GiB partition at the default 64KiB granularity produces roughly 327,680 blocks, which was tens of GiB of
 * memcpy and on-heap garbage for a single partition. The array is now presized from the partition's known or
 * estimated size into a pooled off-heap buffer and doubled when an estimate undershoots. This test records
 * allocation, wall-clock and GC cost in that regime, so a regression back towards the quadratic growth shows up as a
 * change in the recorded numbers.
 *
 * <p>Two deliberate choices:
 *
 * <ul>
 * <li>The measured operation is <em>compaction</em>, not flush. Flush runs on the flush executor, so
 * {@link ThreadMXBean#getThreadAllocatedBytes(long)} on the test thread would not see it; a user-defined compaction
 * task executes synchronously on the calling thread. Compaction is also the shape of the repair/anticompaction
 * scenario this work targets.
 * <li>{@code column_index_size} is lowered to 1KiB so the block-count regime that triggers the quadratic behaviour is
 * reached with tens of MiB of data rather than tens of GiB. Block count, not byte count, is what drives the cost.
 * </ul>
 *
 * <p>The allocation-instrumenter agent that {@code test-memory} attaches is intentionally not used here: its sampler
 * fires on every allocation and would make the wall-clock and GC numbers meaningless.
 */
public class LargePartitionWriteOverheadTest
{
    private static final Logger logger = LoggerFactory.getLogger(LargePartitionWriteOverheadTest.class);
    private static final ThreadMXBean threadMX = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private static final Path RESULTS_FILE = Paths.get("benchmark-results", "large-partition-write.md");

    private static final String TABLE_CQL = "CREATE TABLE tbl (k text, c int, v1 text, v2 text, v3 text, v4 text, PRIMARY KEY (k, c))";
    private static final String PARTITION_KEY = "large";

    private static final int COLUMN_INDEX_SIZE_KIB = 1;
    /** Four values of this width give a row slightly wider than one index block, so blocks track rows one to one. */
    private static final int VALUE_WIDTH = 256;

    private static final int WARMUP_ROWS = 2048;
    private static final int DEFAULT_ROWS = 32768;
    private static final int STRESS_ROWS = 327_680;
    private static final int GIBIBYTE_ROWS = 1_048_576;

    /** Same ASF header the repository's other markdown files carry; {@code ant rat-check} requires it. */
    private static final String LICENSE_HEADER =
        "<!--\n" +
        "# Licensed to the Apache Software Foundation (ASF) under one\n" +
        "# or more contributor license agreements.  See the NOTICE file\n" +
        "# distributed with this work for additional information\n" +
        "# regarding copyright ownership.  The ASF licenses this file\n" +
        "# to you under the Apache License, Version 2.0 (the\n" +
        "# \"License\"); you may not use this file except in compliance\n" +
        "# with the License.  You may obtain a copy of the License at\n" +
        "#\n" +
        "#     http://www.apache.org/licenses/LICENSE-2.0\n" +
        "#\n" +
        "# Unless required by applicable law or agreed to in writing, software\n" +
        "# distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
        "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
        "# See the License for the specific language governing permissions and\n" +
        "# limitations under the License.\n" +
        "-->\n\n";

    private static final List<Result> results = new ArrayList<>();

    private static int originalColumnIndexSizeInKiB;

    @BeforeClass
    public static void setupClass() throws Throwable
    {
        SchemaLoader.prepareServer();

        Assert.assertEquals("This benchmark only measures the BIG format partition writer",
                            BigFormat.NAME, DatabaseDescriptor.getSelectedSSTableFormat().name());

        originalColumnIndexSizeInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(COLUMN_INDEX_SIZE_KIB);

        // let the write path JIT before anything is recorded
        measure("warmup", WARMUP_ROWS, 2);
        results.clear();
    }

    @AfterClass
    public static void afterClass() throws IOException
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSizeInKiB);

        if (results.isEmpty())
            return;

        StringBuilder sb = new StringBuilder();
        sb.append(LICENSE_HEADER);
        sb.append("# Large partition write overhead\n\n");
        sb.append("Cost of compacting a single large partition through `BigFormatPartitionWriter`, measured by\n");
        sb.append("`test/memory/org/apache/cassandra/io/sstable/format/big/LargePartitionWriteOverheadTest.java`.\n\n");
        sb.append("Configuration:\n\n");
        sb.append("- sstable format: `").append(BigFormat.NAME).append("`\n");
        sb.append("- `column_index_size`: ").append(COLUMN_INDEX_SIZE_KIB).append(" KiB\n");
        sb.append("- row width: 4 x ").append(VALUE_WIDTH).append(" bytes\n");
        sb.append("- input sstables per run: 2, compacted into 1\n");
        sb.append("- allocation measured with `ThreadMXBean.getThreadAllocatedBytes` on the compacting thread\n");
        sb.append("- a full GC is requested immediately before each measured window, so the GC columns count only\n");
        sb.append("  collections the operation itself provoked; at the default scale the burst still fits in young gen\n\n");
        sb.append("`indexOffsets` is presized from the partition's known or estimated size into a pooled off-heap\n");
        sb.append("buffer, reused across partitions within a writer and doubled rather than grown by a constant when\n");
        sb.append("an estimate undershoots, so the allocated total below is compaction proper rather than offset\n");
        sb.append("copying.\n\n");
        sb.append("| scenario | rows | index blocks | partition MiB | allocated MiB | bytes/block | wall clock ms | GC count | GC ms |\n");
        sb.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (Result result : results)
            sb.append(result.toTableRow()).append('\n');

        Files.createDirectories(RESULTS_FILE.getParent());
        Files.write(RESULTS_FILE, sb.toString().getBytes(StandardCharsets.UTF_8));
        logger.info("Wrote {}", RESULTS_FILE.toAbsolutePath());
    }

    @Test
    public void singleLargePartition()
    {
        Result result = measure("singleLargePartition", DEFAULT_ROWS, 2);
        Assert.assertTrue("no allocation recorded", result.allocatedBytes > 0);
        Assert.assertTrue("expected tens of thousands of index blocks, got " + result.blockCount,
                          result.blockCount > DEFAULT_ROWS / 2);
    }

    /**
     * Approximates the ~20GiB / 327K block partition this work is about. Needs a larger heap than the default
     * test-memory fork and takes minutes; not for routine CI. Run with
     * {@code ant test-memory -Dtest.name=LargePartitionWriteOverheadTest} after removing {@code @Ignore}.
     */
    @Test
    @Ignore("stress scale, run manually with a larger heap")
    public void singleHugePartition()
    {
        measure("singleHugePartition", STRESS_ROWS, 8);
    }

    /**
     * 2^20 rows of four 256 byte values is ~1GiB of value bytes in one partition, and at the 1KiB granularity this
     * test configures that is ~1,048,576 index blocks - three times the block count of a 20GiB partition at the
     * default 64KiB granularity. Eight input sstables keep each memtable near the ~134MiB of the stress scenario.
     *
     * <p>At that block count the pre-fix {@code +10} growth of {@code indexOffsets} copies roughly 205GiB, so this
     * scenario needs a heap far larger than the {@code test-memory} default. Ant appends its own {@code -Xmx1024m}
     * after every {@code jvmarg}, so the heap has to be raised through the environment rather than
     * {@code test.jvm.args}. Run with {@code _JAVA_OPTIONS=-Xmx16g ant test-memory
     * -Dtest.name=LargePartitionWriteOverheadTest} after removing {@code @Ignore}.
     */
    @Test
    @Ignore("1GiB scale, run manually with a large heap and a raised timeout")
    public void singleGibibytePartition()
    {
        measure("singleGibibytePartition", GIBIBYTE_ROWS, 8);
    }

    private static Result measure(String name, int rowCount, int sstableCount)
    {
        String ksname = "ks_" + toLowerCaseLocalized(name);
        SchemaLoader.createKeyspace(ksname, KeyspaceParams.simple(1),
                                    CreateTableStatement.parse(TABLE_CQL, ksname).build());

        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata(ksname, "tbl").id);
        Assert.assertNotNull(cfs);
        cfs.disableAutoCompaction();

        populate(cfs, ksname, rowCount, sstableCount);

        long partitionBytes = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            partitionBytes += sstable.uncompressedLength();

        System.gc();
        long[] gcBefore = gcSnapshot();
        long allocatedBefore = threadMX.getThreadAllocatedBytes(Thread.currentThread().getId());
        long startNanos = System.nanoTime();

        compact(cfs);

        long elapsedNanos = System.nanoTime() - startNanos;
        long allocatedBytes = threadMX.getThreadAllocatedBytes(Thread.currentThread().getId()) - allocatedBefore;
        long[] gcAfter = gcSnapshot();

        Result result = new Result(name,
                                   rowCount,
                                   blockCount(cfs),
                                   partitionBytes,
                                   allocatedBytes,
                                   TimeUnit.NANOSECONDS.toMillis(elapsedNanos),
                                   gcAfter[0] - gcBefore[0],
                                   gcAfter[1] - gcBefore[1]);
        results.add(result);
        logger.info("*** {}", result);

        cfs.truncateBlocking();
        return result;
    }

    private static void populate(ColumnFamilyStore cfs, String ksname, int rowCount, int sstableCount)
    {
        // fixed seed so the same bytes are written on every run, making before/after numbers comparable
        Random random = new Random(0);
        String insert = String.format("INSERT INTO %s.tbl (k, c, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?, ?)", ksname);
        int rowsPerSSTable = rowCount / sstableCount;
        for (int f = 0; f < sstableCount; f++)
        {
            for (int r = 0; r < rowsPerSSTable; r++)
            {
                QueryProcessor.executeInternal(insert, PARTITION_KEY, (f * rowsPerSSTable) + r,
                                               randomValue(random), randomValue(random),
                                               randomValue(random), randomValue(random));
            }
            Util.flush(cfs);
        }
        Assert.assertEquals(sstableCount, cfs.getLiveSSTables().size());
    }

    private static String randomValue(Random random)
    {
        char[] chars = new char[VALUE_WIDTH];
        for (int i = 0; i < chars.length; i++)
            chars[i] = (char) ('a' + random.nextInt('z' - 'a' + 1));
        return new String(chars);
    }

    private static void compact(ColumnFamilyStore cfs)
    {
        ActiveCompactions active = new ActiveCompactions();
        CompactionTasks tasks = cfs.getCompactionStrategyManager()
                                   .getUserDefinedTasks(cfs.getLiveSSTables(), FBUtilities.nowInSeconds());
        Assert.assertFalse(tasks.isEmpty());
        for (AbstractCompactionTask task : tasks)
            task.execute(active);

        Assert.assertEquals(1, cfs.getLiveSSTables().size());
    }

    private static int blockCount(ColumnFamilyStore cfs)
    {
        BigTableReader sstable = (BigTableReader) Iterables.getOnlyElement(cfs.getLiveSSTables());
        RowIndexEntry entry = sstable.getRowIndexEntry(sstable.getFirst(), SSTableReader.Operator.EQ);
        Assert.assertNotNull(entry);
        return entry.blockCount();
    }

    /** @return {@code [collection count, collection time millis]} summed across all collectors. */
    private static long[] gcSnapshot()
    {
        long count = 0;
        long millis = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans())
        {
            // both accessors return -1 when the collector does not expose the statistic
            count += Math.max(0, bean.getCollectionCount());
            millis += Math.max(0, bean.getCollectionTime());
        }
        return new long[]{ count, millis };
    }

    private static class Result
    {
        final String name;
        final int rowCount;
        final int blockCount;
        final long partitionBytes;
        final long allocatedBytes;
        final long wallClockMillis;
        final long gcCount;
        final long gcMillis;

        Result(String name, int rowCount, int blockCount, long partitionBytes,
               long allocatedBytes, long wallClockMillis, long gcCount, long gcMillis)
        {
            this.name = name;
            this.rowCount = rowCount;
            this.blockCount = blockCount;
            this.partitionBytes = partitionBytes;
            this.allocatedBytes = allocatedBytes;
            this.wallClockMillis = wallClockMillis;
            this.gcCount = gcCount;
            this.gcMillis = gcMillis;
        }

        String toTableRow()
        {
            return String.format("| %s | %d | %d | %.1f | %.1f | %d | %d | %d | %d |",
                                 name, rowCount, blockCount,
                                 partitionBytes / (double) (1 << 20),
                                 allocatedBytes / (double) (1 << 20),
                                 allocatedBytes / blockCount,
                                 wallClockMillis, gcCount, gcMillis);
        }

        @Override
        public String toString()
        {
            return String.format("%s: %d rows, %d index blocks, %s partition, %s allocated (%d bytes/block), %dms, %d GC pauses totalling %dms",
                                 name, rowCount, blockCount,
                                 FBUtilities.prettyPrintMemory(partitionBytes),
                                 FBUtilities.prettyPrintMemory(allocatedBytes),
                                 allocatedBytes / blockCount,
                                 wallClockMillis, gcCount, gcMillis);
        }
    }
}
