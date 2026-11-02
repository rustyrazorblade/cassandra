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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.compaction.unified.ShardedCompactionWriter;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.compaction.writers.SplittingSizeTieredCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Compacts the same input with async_compaction_writer_enabled on and off and compares the
 * outputs. The row count is set by -Dcassandra.test.async_compaction_writer.burn_rows (default
 * 100000).
 *
 * The input has static rows, row deletions, range tombstones, cell tombstones and TTLs, all within
 * gc grace so the compaction keeps them.
 *
 * Writers that switch output on token boundaries or never switch must give byte-identical
 * Data.db, CompressionInfo.db and Digest files. Writers that switch on the size estimate need not:
 * with the option on, the estimate counts chunks not yet written at the ratio seen so far, so a
 * switch can fall on another partition. For those, the two runs must hold the same content in the
 * same order, read through the scanners, and differ only in where they split; see
 * {@link #assertSameContentSplitDifferently}.
 *
 * Every row is read back after each run. The cursor cases use CursorCompactor, which writes BIG
 * only, so they are skipped under another format.
 */
public class AsyncCompactionWriterBurnTest extends CQLTester
{
    private static final int ROWS = CassandraRelevantProperties.TEST_ASYNC_COMPACTION_WRITER_BURN_ROWS.getInt();
    private static final int ROWS_PER_PARTITION = 10;
    private static final int INPUT_SSTABLES = 4;
    private static final int VALUE_SIZE = 200;
    private static final int CHUNK_KB = 16;
    private static final int TTL_SECONDS = 86400;
    /** On-disk size of one partition at most: ten rows of VALUE_SIZE with their overhead. */
    private static final long PARTITION_BYTES = ROWS_PER_PARTITION * (VALUE_SIZE + 64L);
    private static final String WRITER_THREAD_PREFIX = "CompactionWriter-";

    private boolean savedAsync;
    private boolean savedCursor;

    private interface WriterFactory
    {
        CompactionAwareWriter create(ColumnFamilyStore cfs, Directories directories, ILifecycleTransaction txn, Set<SSTableReader> inputs);
    }

    /** About ten outputs, whatever the row count. */
    private static long maxOutputBytes()
    {
        return Math.max(64L * 1024, (long) ROWS * VALUE_SIZE / 2 / 10);
    }

    @Before
    public void saveOptions()
    {
        savedAsync = DatabaseDescriptor.asyncCompactionWriterEnabled();
        savedCursor = DatabaseDescriptor.cursorCompactionEnabled();
    }

    @After
    public void restoreOptions()
    {
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(savedAsync);
        DatabaseDescriptor.setCursorCompactionEnabled(savedCursor);
    }

    @Test
    public void iteratorDefaultWriter() throws Throwable
    {
        assertAsyncMatchesSync(false, (c, d, txn, in) -> new DefaultCompactionWriter(c, d, txn, in), 1);
    }

    @Test
    public void cursorDefaultWriter() throws Throwable
    {
        assumeTrue("cursor compaction writes BIG only", BigFormat.isSelected());
        assertAsyncMatchesSync(true, (c, d, txn, in) -> new DefaultCompactionWriter(c, d, txn, in), 1);
    }

    @Test
    public void cursorMaxSSTableSizeWriterSameContentSplitDifferently() throws Throwable
    {
        assumeTrue("cursor compaction writes BIG only", BigFormat.isSelected());
        assertSameContentSplitDifferently(true, (c, d, txn, in) -> new MaxSSTableSizeWriter(c, d, txn, in, maxOutputBytes(), 0), maxOutputBytes());
    }

    @Test
    public void maxSSTableSizeWriterSameContentSplitDifferently() throws Throwable
    {
        assertSameContentSplitDifferently(false, (c, d, txn, in) -> new MaxSSTableSizeWriter(c, d, txn, in, maxOutputBytes(), 0), maxOutputBytes());
    }

    @Test
    public void splittingSizeTieredCompactionWriterSameContentSplitDifferently() throws Throwable
    {
        // Each output targets half the size of the one before, so no fixed cap applies.
        assertSameContentSplitDifferently(false, (c, d, txn, in) -> new SplittingSizeTieredCompactionWriter(c, d, txn, in, 1024), Long.MAX_VALUE);
    }

    @Test
    public void majorLeveledCompactionWriterSameContentSplitDifferently() throws Throwable
    {
        assertSameContentSplitDifferently(false, (c, d, txn, in) -> new MajorLeveledCompactionWriter(c, d, txn, in, maxOutputBytes()), maxOutputBytes());
    }

    @Test
    public void shardedCompactionWriter() throws Throwable
    {
        assertAsyncMatchesSync(false, (c, d, txn, in) -> new ShardedCompactionWriter(c, d, txn, in, false, true,
                                                                                     ShardManager.create(c).boundaries(16)), 2);
    }

    /**
     * A compaction stopped part way must leave no writer thread, no direct memory and no output files.
     */
    @Test
    public void stoppedCompactionReleasesEverything() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        Set<Descriptor> inputs = descriptors(cfs.getLiveSSTables());
        Set<String> filesBefore = dataDirectoryFiles(cfs);
        long directBefore = directMemoryUsed();
        long directWriteBuffersBefore = StorageMetrics.directWriteBufferBytes.getCount();

        DatabaseDescriptor.setCursorCompactionEnabled(false);
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(true);

        // Stop once more than one output exists, so a switched-out writer and an open one are both aborted.
        AtomicBoolean stopped = new AtomicBoolean();
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        CompactionTask task = new CompactionTask(cfs, txn, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()))
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return new MaxSSTableSizeWriter(cfs, directories, transaction, nonExpiredSSTables, maxOutputBytes(), 0)
                {
                    private int switches;

                    @Override
                    protected boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key)
                    {
                        boolean switching = super.shouldSwitchWriterInCurrentLocation(key);
                        if (switching && ++switches == 2)
                        {
                            stopped.set(true);
                            throw new CompactionInterruptedException(null);
                        }
                        return switching;
                    }
                };
            }
        };
        try
        {
            task.execute(ActiveCompactionsTracker.NOOP);
            fail("the compaction was not stopped");
        }
        catch (CompactionInterruptedException expected)
        {
            // expected
        }
        LifecycleTransaction.waitForDeletions();

        assertTrue("the stop point was never reached", stopped.get());
        assertEquals("writer threads left after the stopped compaction", 0, writerThreads());
        assertEquals("the inputs must still be live", inputs, descriptors(cfs.getLiveSSTables()));
        assertEquals("output files left after the stopped compaction", filesBefore, dataDirectoryFiles(cfs));
        // The O_DIRECT write buffers have an exact gauge. The JVM direct pool also counts unrelated
        // allocations, so allow less than one chunk there; a leaked slot is a whole chunk.
        assertEquals("O_DIRECT write buffers left after the stopped compaction",
                     directWriteBuffersBefore, StorageMetrics.directWriteBufferBytes.getCount());
        long grown = directMemoryUsed() - directBefore;
        assertTrue("direct memory grew by " + grown + " bytes", grown < CHUNK_KB * 1024);
        assertAllRows();
    }

    private void assertAsyncMatchesSync(boolean cursor, WriterFactory factory, int minOutputs) throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        Path scratch = Files.createTempDirectory("async-burn");
        try
        {
            Set<Descriptor> inputs = descriptors(cfs.getLiveSSTables());
            Path inputCopy = scratch.resolve("inputs");
            copyComponents(inputs, inputCopy);

            long start = System.nanoTime();
            List<Path> sync = compactAndCapture(cfs, cursor, false, factory, scratch.resolve("sync"));
            assertAllRows();
            restoreInputs(cfs, inputs, inputCopy);
            List<Path> async = compactAndCapture(cfs, cursor, true, factory, scratch.resolve("async"));
            logger.info("{} rows, {} outputs, both runs in {} ms", ROWS, sync.size(), (System.nanoTime() - start) / 1_000_000);

            assertTrue("expected at least " + minOutputs + " outputs, got " + sync.size(), sync.size() >= minOutputs);
            assertEquals("output count differs", sync.size(), async.size());
            for (int i = 0; i < sync.size(); i++)
                for (Component component : new Component[]{ Components.DATA, Components.COMPRESSION_INFO, Components.DIGEST })
                    assertArrayEquals("output " + i + ' ' + component.name + " differs",
                                      Files.readAllBytes(sync.get(i).resolve(component.name)),
                                      Files.readAllBytes(async.get(i).resolve(component.name)));

            assertAllRows();
            assertEquals("writer threads left after the compaction", 0, writerThreads());
        }
        finally
        {
            org.apache.commons.io.FileUtils.deleteDirectory(scratch.toFile());
        }
    }

    /**
     * For writers that switch on the size estimate. The sync and async runs must:
     * <ul>
     * <li>hold the same content: every output of a run, in token order, read through the scanners,
     *     gives the same ordered list of partitions, deletions, static rows, rows, cells and range
     *     tombstone markers as the other run;</li>
     * <li>split it the same way or nearly: the output counts differ by at most one;</li>
     * <li>each have outputs with ordered, non-overlapping key ranges that hold every input partition
     *     exactly once;</li>
     * <li>each have outputs that pass extended verification and stay within {@code maxBytes} plus one
     *     partition and two chunks, the same bound for both runs.</li>
     * </ul>
     */
    private void assertSameContentSplitDifferently(boolean cursor, WriterFactory factory, long maxBytes) throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        Path scratch = Files.createTempDirectory("async-burn");
        try
        {
            Set<Descriptor> inputs = descriptors(cfs.getLiveSSTables());
            Path inputCopy = scratch.resolve("inputs");
            copyComponents(inputs, inputCopy);
            List<DecoratedKey> inputKeys = partitionKeys(cfs.getLiveSSTables());

            byte[][] digests = new byte[2][];
            int[] outputCounts = new int[2];
            for (boolean async : new boolean[]{ false, true })
            {
                long start = System.nanoTime();
                compactAndCapture(cfs, cursor, async, factory, scratch.resolve(async ? "async" : "sync"));
                List<SSTableReader> outputs = inTokenOrder(cfs.getLiveSSTables());
                logger.info("{} rows, async {}, {} outputs in {} ms", ROWS, async, outputs.size(), (System.nanoTime() - start) / 1_000_000);

                assertTrue("expected the size cap to split the output, got " + outputs.size(), outputs.size() >= 2);
                assertDisjointAndComplete(outputs, inputKeys);
                for (SSTableReader output : outputs)
                {
                    verify(cfs, output);
                    // The switch is checked once per partition, so an output may pass the cap by one
                    // partition and the chunk still being filled; one more chunk covers the error of the
                    // estimate for chunks in flight. An estimate that left those chunks out would pass
                    // the cap by as much as is in flight, and fails here when the writer thread falls
                    // behind. AsyncCompactionWriterSSTableSizeTest holds the writer thread to make that
                    // certain.
                    long bound = maxBytes == Long.MAX_VALUE
                                 ? Long.MAX_VALUE
                                 : maxBytes + PARTITION_BYTES + 2 * CHUNK_KB * 1024L;
                    assertTrue("output of " + output.onDiskLength() + " bytes is over the " + bound + " byte bound",
                               output.onDiskLength() <= bound);
                }
                digests[async ? 1 : 0] = contentDigest(outputs);
                outputCounts[async ? 1 : 0] = outputs.size();
                assertAllRows();
                assertEquals("writer threads left after the compaction", 0, writerThreads());
                if (!async)
                    restoreInputs(cfs, inputs, inputCopy);
            }

            assertArrayEquals("the async run holds different content from the sync run", digests[0], digests[1]);
            assertTrue("output counts differ by more than one: sync " + outputCounts[0] + ", async " + outputCounts[1],
                       Math.abs(outputCounts[0] - outputCounts[1]) <= 1);
        }
        finally
        {
            org.apache.commons.io.FileUtils.deleteDirectory(scratch.toFile());
        }
    }

    private static List<SSTableReader> inTokenOrder(Set<SSTableReader> sstables)
    {
        List<SSTableReader> sorted = new ArrayList<>(sstables);
        sorted.sort((a, b) -> a.getFirst().compareTo(b.getFirst()));
        return sorted;
    }

    /** Every partition key of the sstables, in token order, with duplicates across sstables merged. */
    private static List<DecoratedKey> partitionKeys(Set<SSTableReader> sstables)
    {
        java.util.TreeSet<DecoratedKey> keys = new java.util.TreeSet<>();
        for (SSTableReader sstable : sstables)
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        keys.add(partition.partitionKey());
                    }
            }
        return new ArrayList<>(keys);
    }

    /** Outputs in token order must not overlap, and together hold each input partition exactly once. */
    private static void assertDisjointAndComplete(List<SSTableReader> outputs, List<DecoratedKey> inputKeys)
    {
        List<DecoratedKey> written = new ArrayList<>();
        DecoratedKey previousLast = null;
        for (SSTableReader output : outputs)
        {
            if (previousLast != null)
                assertTrue("output " + output.descriptor + " overlaps the one before", previousLast.compareTo(output.getFirst()) < 0);
            previousLast = output.getLast();
            try (ISSTableScanner scanner = output.getScanner())
            {
                while (scanner.hasNext())
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        written.add(partition.partitionKey());
                    }
            }
        }
        assertEquals("partitions written are not the input partitions, each once", inputKeys, written);
    }

    /**
     * A digest of everything the outputs hold, in order: each partition key and partition deletion,
     * the static row, and every row and range tombstone marker with clustering, liveness, deletion
     * and each cell's path, value, timestamp, TTL and local deletion time.
     */
    private static byte[] contentDigest(List<SSTableReader> outputs) throws Exception
    {
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        for (SSTableReader output : outputs)
            try (ISSTableScanner scanner = output.getScanner())
            {
                while (scanner.hasNext())
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        update(digest, "partition", partition.partitionKey().getKey());
                        update(digest, partition.partitionLevelDeletion());
                        update(digest, partition.staticRow());
                        while (partition.hasNext())
                        {
                            Unfiltered unfiltered = partition.next();
                            if (unfiltered.isRow())
                            {
                                update(digest, (Row) unfiltered);
                            }
                            else
                            {
                                RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
                                updateClustering(digest, marker.clustering());
                                if (marker.isBoundary())
                                {
                                    update(digest, ((RangeTombstoneBoundaryMarker) marker).endDeletionTime());
                                    update(digest, ((RangeTombstoneBoundaryMarker) marker).startDeletionTime());
                                }
                                else
                                {
                                    update(digest, ((RangeTombstoneBoundMarker) marker).deletionTime());
                                }
                            }
                        }
                    }
            }
        return digest.digest();
    }

    private static void update(java.security.MessageDigest digest, Row row)
    {
        update(digest, "row", ByteBuffer.allocate(0));
        updateClustering(digest, row.clustering());
        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        updateLong(digest, liveness.timestamp());
        updateLong(digest, liveness.ttl());
        updateLong(digest, liveness.isExpiring() ? liveness.localExpirationTime() : -1);
        update(digest, row.deletion().time());
        updateLong(digest, row.deletion().isShadowable() ? 1 : 0);
        for (ColumnData data : row)
        {
            update(digest, "column", data.column().name.bytes);
            if (data instanceof ComplexColumnData)
                update(digest, ((ComplexColumnData) data).complexDeletion());
            for (Cell<?> cell : data instanceof ComplexColumnData ? (ComplexColumnData) data : List.of((Cell<?>) data))
            {
                if (cell.path() != null)
                    for (int i = 0; i < cell.path().size(); i++)
                        update(digest, "path", cell.path().get(i));
                update(digest, "value", cell.buffer());
                updateLong(digest, cell.timestamp());
                updateLong(digest, cell.ttl());
                updateLong(digest, cell.localDeletionTime());
            }
        }
    }

    private static void updateClustering(java.security.MessageDigest digest, ClusteringPrefix<?> clustering)
    {
        updateLong(digest, clustering.kind().ordinal());
        for (int i = 0; i < clustering.size(); i++)
            update(digest, "clustering", clustering.bufferAt(i));
    }

    private static void update(java.security.MessageDigest digest, DeletionTime deletion)
    {
        updateLong(digest, deletion.markedForDeleteAt());
        updateLong(digest, deletion.localDeletionTime());
    }

    private static void update(java.security.MessageDigest digest, String tag, ByteBuffer bytes)
    {
        digest.update(tag.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        updateLong(digest, bytes.remaining());
        digest.update(bytes.duplicate());
    }

    private static void updateLong(java.security.MessageDigest digest, long value)
    {
        for (int shift = 56; shift >= 0; shift -= 8)
            digest.update((byte) (value >>> shift));
    }

    /**
     * Extended verification reads every partition and checks the Digest against the Data file. Its
     * output is dropped: it logs a line per index entry, which at large row counts blocks the test
     * log appender. A failure still throws.
     */
    private static void verify(ColumnFamilyStore cfs, SSTableReader sstable)
    {
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.NullOutput(), false,
                                                      IVerifier.options().invokeDiskFailurePolicy(true)
                                                                         .extendedVerification(true).build()))
        {
            verifier.verify();
        }
    }

    /** Compacts every live sstable and copies the outputs' components into scratch, one directory per output. */
    private List<Path> compactAndCapture(ColumnFamilyStore cfs, boolean cursor, boolean async, WriterFactory factory, Path scratch) throws Throwable
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(async);

        Set<SSTableReader> inputs = new HashSet<>(cfs.getLiveSSTables());
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        // The table's own gc grace, so both runs keep every tombstone.
        new CompactionTask(cfs, txn, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()))
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return factory.create(cfs, directories, transaction, nonExpiredSSTables);
            }
        }.execute(ActiveCompactionsTracker.NOOP);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);
        LifecycleTransaction.waitForDeletions();

        List<SSTableReader> outputs = new ArrayList<>(cfs.getLiveSSTables());
        outputs.sort((a, b) -> a.getFirst().compareTo(b.getFirst()));
        List<Path> captured = new ArrayList<>();
        for (SSTableReader output : outputs)
        {
            Path dir = scratch.resolve("sstable-" + captured.size());
            copyComponents(Set.of(output.descriptor), dir);
            captured.add(dir.resolve(output.descriptor.baseFile().name()));
        }
        return captured;
    }

    private static void copyComponents(Set<Descriptor> descriptors, Path dir) throws Exception
    {
        for (Descriptor descriptor : descriptors)
        {
            Path target = dir.resolve(descriptor.baseFile().name());
            Files.createDirectories(target);
            for (Component component : descriptor.discoverComponents())
                Files.copy(descriptor.fileFor(component).toPath(), target.resolve(component.name));
        }
    }

    /** Replaces the live outputs with the copied inputs, so the next run compacts the same bytes. */
    private static void restoreInputs(ColumnFamilyStore cfs, Set<Descriptor> inputs, Path inputCopy) throws Exception
    {
        Set<SSTableReader> outputs = new HashSet<>(cfs.getLiveSSTables());
        cfs.getTracker().removeUnsafe(outputs);
        for (SSTableReader output : outputs)
        {
            for (Component component : output.descriptor.discoverComponents())
                output.descriptor.fileFor(component).tryDelete();
            output.selfRef().release();
        }

        List<SSTableReader> reopened = new ArrayList<>();
        for (Descriptor descriptor : inputs)
        {
            Path copy = inputCopy.resolve(descriptor.baseFile().name());
            try (java.util.stream.Stream<Path> files = Files.list(copy))
            {
                for (Path file : (Iterable<Path>) files::iterator)
                    Files.copy(file, descriptor.fileFor(Component.parse(file.getFileName().toString(), descriptor.version.format)).toPath());
            }
            reopened.add(SSTableReader.open(cfs, descriptor));
        }
        cfs.getTracker().addInitialSSTables(reopened);
    }

    private ColumnFamilyStore populate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, v blob, PRIMARY KEY (pk, ck)) WITH compression = " +
                    "{'class': 'LZ4Compressor', 'chunk_length_in_kb': " + CHUNK_KB + '}');
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int perSSTable = ROWS / INPUT_SSTABLES;
        for (int s = 0; s < INPUT_SSTABLES; s++)
        {
            for (int i = s * perSSTable; i < (s + 1) * perSSTable; i++)
            {
                int pk = i / ROWS_PER_PARTITION;
                int ck = i % ROWS_PER_PARTITION;
                if (i % 7 == 0)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TTL " + TTL_SECONDS, pk, ck, value(i));
                else
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, value(i));
                if (ck == 0)
                    execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, i);
            }
            // Deletions of rows written above, in the same sstable, so each run keeps the same tombstones.
            for (int pk = s * perSSTable / ROWS_PER_PARTITION; pk < (s + 1) * perSSTable / ROWS_PER_PARTITION; pk++)
            {
                if (pk % 11 == 0)
                    execute("DELETE FROM %s WHERE pk = ? AND ck = 1", pk);
                if (pk % 13 == 0)
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= 3 AND ck < 5", pk);
                if (pk % 17 == 0)
                    execute("DELETE v FROM %s WHERE pk = ? AND ck = 6", pk);
            }
            flush();
        }
        // A large row count can fill a memtable before the explicit flush, which adds input sstables.
        assertTrue("expected at least " + INPUT_SSTABLES + " input sstables", cfs.getLiveSSTables().size() >= INPUT_SSTABLES);
        return cfs;
    }

    /** True if populate deleted the row or its value. */
    private static boolean deleted(int pk, int ck)
    {
        return (pk % 11 == 0 && ck == 1) || (pk % 13 == 0 && (ck == 3 || ck == 4));
    }

    /** Half repeated bytes and half random, so LZ4 compresses about 2:1. */
    private static ByteBuffer value(int row)
    {
        byte[] bytes = new byte[VALUE_SIZE];
        new Random(row).nextBytes(bytes);
        Arrays.fill(bytes, 0, VALUE_SIZE / 2, (byte) row);
        return ByteBuffer.wrap(bytes);
    }

    /** Reads every partition by key, which keeps each read under the read size guardrails. */
    private void assertAllRows() throws Throwable
    {
        int rows = ROWS / INPUT_SSTABLES * INPUT_SSTABLES;
        int expected = 0;
        int seen = 0;
        for (int pk = 0; pk * ROWS_PER_PARTITION < rows; pk++)
        {
            for (int ck = 0; ck < ROWS_PER_PARTITION && pk * ROWS_PER_PARTITION + ck < rows; ck++)
                if (!deleted(pk, ck))
                    expected++;
            for (UntypedResultSet.Row row : execute("SELECT ck, s, v FROM %s WHERE pk = ?", pk))
            {
                int ck = row.getInt("ck");
                int i = pk * ROWS_PER_PARTITION + ck;
                assertFalse("deleted row " + i + " came back", deleted(pk, ck));
                assertEquals("static value of partition " + pk, pk * ROWS_PER_PARTITION, row.getInt("s"));
                if (pk % 17 == 0 && ck == 6)
                    assertFalse("deleted cell of row " + i + " came back", row.has("v"));
                else
                    assertEquals("row " + i + " has the wrong value", value(i), row.getBytes("v"));
                seen++;
            }
        }
        assertEquals("rows read back", expected, seen);
    }

    private static Set<Descriptor> descriptors(Iterable<SSTableReader> readers)
    {
        Set<Descriptor> descriptors = new HashSet<>();
        for (SSTableReader reader : readers)
            descriptors.add(reader.descriptor);
        return descriptors;
    }

    private static Set<String> dataDirectoryFiles(ColumnFamilyStore cfs) throws Exception
    {
        Set<String> names = new HashSet<>();
        for (org.apache.cassandra.io.util.File dir : cfs.getDirectories().getCFDirectories())
            try (java.util.stream.Stream<Path> files = Files.list(dir.toPath()))
            {
                files.filter(Files::isRegularFile).forEach(p -> names.add(p.getFileName().toString()));
            }
        return names;
    }

    private static int writerThreads()
    {
        int count = 0;
        for (Thread t : Thread.getAllStackTraces().keySet())
            if (t.isAlive() && t.getName().startsWith(WRITER_THREAD_PREFIX))
                count++;
        return count;
    }

    private static long directMemoryUsed()
    {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class))
            if (pool.getName().equals("direct"))
                return pool.getMemoryUsed();
        throw new AssertionError("no direct buffer pool");
    }
}
