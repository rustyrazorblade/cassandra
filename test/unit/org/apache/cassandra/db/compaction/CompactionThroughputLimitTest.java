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
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.ThroughputLimiter;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.StorageService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compaction charges the compaction limiter with the bytes it reads from data files and writes to data and index
 * files.  Reads do not charge it, and flushes charge it only with memtable_flush_throttle_enabled.
 */
@RunWith(Parameterized.class)
public class CompactionThroughputLimitTest extends CQLTester
{
    /** The components written through the limited writers; the rest are small and written once. */
    private static final Set<String> LIMITED_COMPONENTS = ImmutableSet.of("Data.db", "Index.db", "Partitions.db", "Rows.db");

    @Parameterized.Parameter(0)
    public String format;

    @Parameterized.Parameter(1)
    public boolean compressed;

    @Parameterized.Parameter(2)
    public boolean cursor;

    @Parameterized.Parameters(name = "{0} compressed={1} cursor={2}")
    public static Collection<Object[]> parameters()
    {
        List<Object[]> parameters = new ArrayList<>();
        for (String format : new String[]{ "big", "bti" })
            for (boolean compressed : new boolean[]{ true, false })
                for (boolean cursor : new boolean[]{ true, false })
                    if (!cursor || format.equals("big")) // cursor compaction writes only BIG on 6.0
                        parameters.add(new Object[]{ format, compressed, cursor });
        return parameters;
    }

    private String formatBefore;
    private boolean cursorBefore;
    private int readAheadBefore;
    private DiskAccessMode readModeBefore;
    private DiskAccessMode writeModeBefore;

    @Before
    public void configure()
    {
        formatBefore = DatabaseDescriptor.getSelectedSSTableFormat().name();
        cursorBefore = DatabaseDescriptor.cursorCompactionEnabled();
        readAheadBefore = DatabaseDescriptor.getCompressedReadAheadBufferSize() / 1024;
        readModeBefore = DatabaseDescriptor.getCompactionReadDiskAccessMode();
        writeModeBefore = DatabaseDescriptor.getBackgroundWriteDiskAccessMode();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        for (Keyspace keyspace : Keyspace.all())
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                cfs.disableAutoCompaction();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(formatBefore);
        DatabaseDescriptor.setCursorCompactionEnabled(cursorBefore);
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(readAheadBefore);
        DatabaseDescriptor.setCompactionReadDiskAccessMode(readModeBefore);
        DatabaseDescriptor.setBackgroundWriteDiskAccessMode(writeModeBefore);
        DatabaseDescriptor.setCompactionThroughputMebibytesPerSec(0);
        DatabaseDescriptor.setMemtableFlushThrottleEnabled(false);
        CompactionManager.instance.getRateLimiter();
    }

    @Test
    public void scansChargeEveryByteOfTheDataFile() throws Throwable
    {
        ColumnFamilyStore cfs = writeSSTables(1);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        // mmap is covered by ThrottledIoTest: an sstable opened without it cannot be scanned with it.
        List<DiskAccessMode> modes = new ArrayList<>(List.of(DiskAccessMode.standard));
        if (compressed)
            modes.add(DiskAccessMode.direct);
        for (int readAheadKiB : new int[]{ 0, 256 })
        {
            DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(readAheadKiB);
            for (DiskAccessMode mode : modes)
            {
                ThroughputLimiter limiter = new ThroughputLimiter(0);
                try (ISSTableScanner scanner = sstable.getScanner(mode, limiter))
                {
                    drain(scanner);
                }
                assertThat(limiter.bytesCharged()).as("mode %s, read-ahead %d KiB", mode, readAheadKiB)
                                                  .isEqualTo(sstable.onDiskLength());
            }
        }
    }

    /** The scanners that compaction and repair validation open charge the compaction limiter. */
    @Test
    public void strategyScannersChargeTheCompactionLimiter() throws Throwable
    {
        ColumnFamilyStore cfs = writeSSTables(3);
        long charged = charged();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(cfs.getLiveSSTables()))
        {
            for (ISSTableScanner scanner : scanners.scanners)
                drain(scanner);
        }
        assertThat(charged() - charged).isEqualTo(onDiskLength(cfs.getLiveSSTables()));
    }

    @Test
    public void compactionChargesItsReadsAndWrites() throws Throwable
    {
        ColumnFamilyStore cfs = writeSSTables(3);
        Set<SSTableReader> inputs = ImmutableSet.copyOf(cfs.getLiveSSTables());
        long charged = charged();
        cfs.forceMajorCompaction();
        Set<SSTableReader> outputs = ImmutableSet.copyOf(cfs.getLiveSSTables());
        assertThat(outputs).doesNotContainAnyElementsOf(inputs);
        assertThat(charged() - charged).isEqualTo(onDiskLength(inputs) + limitedComponentsLength(outputs));
    }

    @Test
    public void directWritesChargeTheirPadding() throws Throwable
    {
        Assume.assumeTrue("direct writes are only for compressed sstables", compressed);
        DatabaseDescriptor.setBackgroundWriteDiskAccessMode(DiskAccessMode.direct);
        ColumnFamilyStore cfs = writeSSTables(3);
        Set<SSTableReader> inputs = ImmutableSet.copyOf(cfs.getLiveSSTables());
        long charged = charged();
        cfs.forceMajorCompaction();
        long expected = onDiskLength(inputs) + limitedComponentsLength(cfs.getLiveSSTables());
        // A direct write covers whole blocks.  The writer pads the last block and charges it, then truncates the file.
        assertThat(charged() - charged).isBetween(expected, expected + 64 * 1024);
    }

    @Test
    public void readsAndFlushesAreNotCharged() throws Throwable
    {
        long charged = charged();
        writeSSTables(2);
        execute("SELECT * FROM %s");
        execute("SELECT * FROM %s WHERE pk = ?", 7);
        execute("SELECT * FROM %s WHERE pk = ?", BIG_PARTITION);
        assertThat(charged() - charged).isZero();
    }

    /** The option, set over JMX, makes the next flush charge the limiter with every byte of its limited components. */
    @Test
    public void flushesChargeWhenTheOptionIsOn() throws Throwable
    {
        StorageService.instance.setMemtableFlushThrottleEnabled(true);
        assertThat(DatabaseDescriptor.isMemtableFlushThrottleEnabled()).isTrue();
        long charged = charged();
        long priorityCharged = priorityCharged();
        ColumnFamilyStore cfs = writeSSTables(2);
        long expected = limitedComponentsLength(cfs.getLiveSSTables());
        assertThat(charged() - charged).isEqualTo(expected);
        assertThat(priorityCharged() - priorityCharged).isEqualTo(expected);
    }

    /**
     * A flush keeps to the limit while a compaction runs, and the compaction does not hold it back.  Sharing the limit
     * with one compaction thread would take about twice as long.
     */
    @Test
    public void flushIsNotHeldBackByCompaction() throws Throwable
    {
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(0);
        ColumnFamilyStore compacted = writeSSTables(3);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text) WITH compression = {'enabled': false}");
        ColumnFamilyStore flushed = getCurrentColumnFamilyStore();
        flushed.disableAutoCompaction();
        Random random = new Random(7);
        for (int pk = 0; pk < 4_500; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, value(random, 1_000));

        int bytesPerSecond = 2 << 20;
        DatabaseDescriptor.setCompactionThroughputBytesPerSec(bytesPerSecond);
        DatabaseDescriptor.setMemtableFlushThrottleEnabled(true);
        Future<?> compaction = CompactionManager.instance.submitMaximal(compacted, false, 1).get(0);
        Util.spinAssertEquals(true, () -> !CompactionManager.instance.active.getCompactions().isEmpty(), 10);
        long waited = CompactionManager.instance.getRateLimiter().priorityWaitNanos();
        long start = System.nanoTime();
        flushed.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        double seconds = (System.nanoTime() - start) / (double) TimeUnit.SECONDS.toNanos(1);
        long bytes = limitedComponentsLength(flushed.getLiveSSTables());
        assertThat(compaction.isDone()).as("the compaction ran during the whole flush").isFalse();
        assertThat(bytes).as("enough bytes to pace").isGreaterThan(4L << 20);
        assertThat(CompactionManager.instance.getRateLimiter().priorityWaitNanos()).as("the flush waited for the limit").isGreaterThan(waited);
        assertThat(seconds).as("%d bytes", bytes).isLessThan(bytes / (double) bytesPerSecond + 1);
        compaction.get();
    }

    /** Compaction charges the bytes of the storage-attached index files it writes, as well as the sstable's. */
    @Test
    public void compactionChargesItsIndexWrites() throws Throwable
    {
        ColumnFamilyStore cfs = writeSSTables(3);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        Set<SSTableReader> inputs = ImmutableSet.copyOf(cfs.getLiveSSTables());
        long charged = charged();
        cfs.forceMajorCompaction();
        Set<SSTableReader> outputs = ImmutableSet.copyOf(cfs.getLiveSSTables());
        assertThat(indexLength(outputs)).isPositive();
        assertThat(charged() - charged).isEqualTo(onDiskLength(inputs) + limitedComponentsLength(outputs) + indexLength(outputs));
    }

    @Test
    public void flushChargesItsIndexWritesWhenTheOptionIsOn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Random random = new Random(5);
        for (int pk = 0; pk < 1_000; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0, value(random));
        DatabaseDescriptor.setMemtableFlushThrottleEnabled(true);
        long charged = priorityCharged();
        flush();
        Set<SSTableReader> flushed = cfs.getLiveSSTables();
        assertThat(indexLength(flushed)).isPositive();
        assertThat(priorityCharged() - charged).isEqualTo(limitedComponentsLength(flushed) + indexLength(flushed));
    }

    /** The flushes of commit log replay do not charge the limiter: at startup the node serves nothing yet. */
    @Test
    public void replayFlushesAreNotCharged() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Random random = new Random(11);
        for (int pk = 0; pk < 1_000; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0, value(random));
        cfs.clearUnsafe();
        DatabaseDescriptor.setMemtableFlushThrottleEnabled(true);
        long charged = priorityCharged();
        CommitLog.instance.resetUnsafe(false);
        assertThat(cfs.getLiveSSTables()).as("replay flushed the table").isNotEmpty();
        assertThat(priorityCharged() - charged).isZero();
    }

    /**
     * With a low limit, compaction takes as long as the limit says, so a big partition cannot be read at full
     * speed and paid for later.
     */
    @Test
    public void compactionKeepsToTheLimit() throws Throwable
    {
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(0);
        ColumnFamilyStore cfs = writeSSTables(2);
        int bytesPerSecond = 512 << 10;
        DatabaseDescriptor.setCompactionThroughputBytesPerSec(bytesPerSecond);
        long charged = charged();
        long start = System.nanoTime();
        cfs.forceMajorCompaction();
        double seconds = (System.nanoTime() - start) / (double) TimeUnit.SECONDS.toNanos(1);
        long bytes = charged() - charged;
        // The compaction thread can be one I/O ahead of the limit, at most a 64 KiB write buffer.
        double floor = (bytes - (64 << 10)) / (double) bytesPerSecond;
        assertThat(floor).as("enough bytes to measure").isGreaterThan(1);
        assertThat(seconds).as("%d bytes", bytes).isGreaterThanOrEqualTo(floor);
        // Nor does it run much slower than the limit allows.
        assertThat(seconds).as("%d bytes", bytes).isLessThan(2 * bytes / (double) bytesPerSecond + 2);
    }

    @Test
    public void scrubChargesItsReadsAndWrites() throws Throwable
    {
        ColumnFamilyStore cfs = writeSSTables(2);
        Set<SSTableReader> inputs = ImmutableSet.copyOf(cfs.getLiveSSTables());
        long charged = charged();
        CompactionManager.instance.performScrub(cfs, IScrubber.options().checkData().build(), 1);
        assertThat(charged() - charged).isEqualTo(onDiskLength(inputs) + limitedComponentsLength(cfs.getLiveSSTables()));
    }

    @Test
    public void verifyChargesItsReads() throws Throwable
    {
        ColumnFamilyStore cfs = writeSSTables(2);
        long charged = charged();
        CompactionManager.instance.performVerify(cfs, IVerifier.options().extendedVerification(true).build());
        // Verify seeks within the data file, and a scan reads a chunk again from disk after a seek back.  BIG reads
        // every partition whole; BTI skips the partition contents, as its writers already checked them.
        long length = onDiskLength(cfs.getLiveSSTables());
        assertThat(charged() - charged).isBetween(format.equals("big") ? length : 1, 2 * length);
    }

    @Test
    public void upgradeChargesItsReadsAndWrites() throws Throwable
    {
        ColumnFamilyStore cfs = writeSSTables(2);
        Set<SSTableReader> inputs = ImmutableSet.copyOf(cfs.getLiveSSTables());
        long charged = charged();
        CompactionManager.instance.performSSTableRewrite(cfs, false, Long.MAX_VALUE, false, 1);
        assertThat(cfs.getLiveSSTables()).doesNotContainAnyElementsOf(inputs);
        assertThat(charged() - charged).isEqualTo(onDiskLength(inputs) + limitedComponentsLength(cfs.getLiveSSTables()));
    }

    @Test
    public void garbageCollectChargesItsReadsAndWrites() throws Throwable
    {
        ColumnFamilyStore cfs = writeSSTables(2);
        Set<SSTableReader> inputs = ImmutableSet.copyOf(cfs.getLiveSSTables());
        long charged = charged();
        CompactionManager.instance.performGarbageCollection(cfs, CompactionParams.TombstoneOption.ROW, 1);
        assertThat(cfs.getLiveSSTables()).doesNotContainAnyElementsOf(inputs);
        assertThat(charged() - charged).isEqualTo(onDiskLength(inputs) + limitedComponentsLength(cfs.getLiveSSTables()));
    }

    private static final int BIG_PARTITION = 1_000_000;

    /** Small partitions, and one partition far larger than a read-ahead block, in each sstable. */
    private ColumnFamilyStore writeSSTables(int count) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH compression = " +
                    (compressed ? "{'class': 'LZ4Compressor', 'chunk_length_in_kb': 16}" : "{'enabled': false}"));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Random random = new Random(count);
        for (int s = 0; s < count; s++)
        {
            for (int pk = 0; pk < 200; pk++)
                for (int ck = 0; ck < 5; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck + s * 5, value(random));
            for (int ck = 0; ck < 2_000; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", BIG_PARTITION, ck + s * 2_000, value(random));
            flush();
        }
        assertThat(cfs.getLiveSSTables()).hasSize(count);
        return cfs;
    }

    /** 100 random letters, so the data does not compress away. */
    private static String value(Random random)
    {
        return value(random, 100);
    }

    private static String value(Random random, int length)
    {
        char[] value = new char[length];
        for (int i = 0; i < value.length; i++)
            value[i] = (char) ('a' + random.nextInt(26));
        return new String(value);
    }

    private static long charged()
    {
        return CompactionManager.instance.getRateLimiter().bytesCharged();
    }

    private static long priorityCharged()
    {
        return CompactionManager.instance.getRateLimiter().priorityBytesCharged();
    }

    private static void drain(ISSTableScanner scanner)
    {
        while (scanner.hasNext())
        {
            try (UnfilteredRowIterator partition = scanner.next())
            {
                while (partition.hasNext())
                    partition.next();
            }
        }
    }

    private static long onDiskLength(Collection<SSTableReader> sstables)
    {
        return sstables.stream().mapToLong(SSTableReader::onDiskLength).sum();
    }

    /** The length of the storage-attached index files of {@code sstables}. */
    private static long indexLength(Collection<SSTableReader> sstables)
    {
        long length = 0;
        for (SSTableReader sstable : sstables)
        {
            String prefix = sstable.descriptor.baseFile().name() + '-' + Version.SAI_DESCRIPTOR + Version.SAI_SEPARATOR;
            for (File file : sstable.descriptor.directory.tryList(file -> file.name().startsWith(prefix)))
                length += file.length();
        }
        return length;
    }

    private static long limitedComponentsLength(Collection<SSTableReader> sstables)
    {
        long length = 0;
        for (SSTableReader sstable : sstables)
            for (Component component : sstable.getComponents())
                if (LIMITED_COMPONENTS.contains(component.name))
                    length += sstable.descriptor.fileFor(component).length();
        return length;
    }
}
