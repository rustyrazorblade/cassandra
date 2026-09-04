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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.test.microbench.CompactionBench;

/**
 * Compaction benchmark sized for the write path rather than the merge path.
 *
 * Each row carries a blob payload, so a run moves gigabytes through
 * {@code CompressedSequentialWriter} instead of the few megabytes the bigint-only
 * {@link CompactionBench} produces. The payload is deliberately half repeated bytes and half
 * random, which lands LZ4 near a 2:1 ratio: fully random data makes the compressor bail out
 * early and understates compression cost, while uniform data compresses away to nothing and
 * overstates it.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CompactionWritePathBench extends CompactionBench
{
    @Param("1024")
    int payloadSize = 1024;

    /** Fraction of each payload that repeats. The remainder is random. */
    @Param("50")
    int repeatPercent = 50;

    /** Runs compression and the write off the compaction thread. */
    @Param("false")
    boolean asyncWriter = false;

    /** Bytes the async writer keeps in flight. The slot count is derived from this. */
    @Param("4MiB")
    String writerBuffer = "4MiB";

    /**
     * Table compression chunk length. This is also the write buffer size and the slot size:
     * CompressedSequentialWriter derives both from CompressionParams.chunkLength(), so raising it
     * raises the pool's memory by the same factor.
     */
    @Param("16")
    int chunkKb = 16;

    protected void createSStables()
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, ck bigint, payload blob, PRIMARY KEY(pk, ck)) " +
                                      "WITH compression = { 'class':'LZ4Compressor', 'chunk_length_in_kb':" + chunkKb + " }");
        execute("use " + keyspace + ";");
        writeStatement = "INSERT INTO " + table + "(pk,ck,payload)VALUES(?,?,?)";

        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // 0 means no limit. The base class sets a very high ceiling; make it an actual no-op so the
        // measurement is of the compaction path and not of the rate limiter.
        DatabaseDescriptor.setCompactionThroughputMebibytesPerSec(0);
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(asyncWriter);
        DatabaseDescriptor.setAsyncCompactionWriterBuffer(writerBuffer);

        // Generating the dataset costs about two minutes, so it is done per trial by default.
        // Set cassandra.bench.dataset to reuse one set of SSTables across runs instead.
        String cachePath = System.getProperty("cassandra.bench.dataset");
        if (cachePath == null)
        {
            generateRows();
            return;
        }

        File cache = new File(cachePath);
        cache.tryCreateDirectories();
        if (dataFilesIn(cache).isEmpty())
        {
            generateRows();
            saveTo(cache);
        }
        else
        {
            restoreFrom(cache);
        }
    }

    private void generateRows()
    {
        Random r = new Random(42L);
        for (int j = 0; j < sstableCount; j++)
        {
            long pPrefix = overlap.startsWith("PK") ? 0 : (long) j * rowCount;
            long rPrefix = overlap.startsWith("PK.ROW") ? 0 : (long) j * rowCount;
            for (long i = 0; i < rowCount; i++)
                execute(writeStatement, pPrefix + i, rPrefix + i, nextPayload(r));

            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }
    }

    private static List<File> dataFilesIn(File dir)
    {
        List<File> files = new ArrayList<>();
        File[] listed = dir.tryList();
        if (listed != null)
            for (File f : listed)
                if (!f.isDirectory())
                    files.add(f);
        return files;
    }

    private File liveDirectory()
    {
        return cfs.getDirectories().getCFDirectories().get(0);
    }

    /**
     * Caches the live SSTables by enumerating each reader's own components rather than listing the
     * directory. A directory listing also picks up in-flight {@code *_txn_*.log} transaction logs, and
     * restoring one of those makes {@code loadNewSSTables} treat the whole set as an unfinished
     * transaction and reject it as corrupt.
     */
    private void saveTo(File cache)
    {
        int files = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            for (Component component : sstable.getComponents())
            {
                File src = sstable.descriptor.fileFor(component);
                if (src.exists())
                {
                    FileUtils.createHardLink(src, new File(cache, src.name()));
                    files++;
                }
            }
        }
        System.out.println("Cached " + cfs.getLiveSSTables().size() + " sstables (" + files
                           + " files) in " + cache.absolutePath());
    }

    private void restoreFrom(File cache)
    {
        List<File> cached = dataFilesIn(cache);
        File live = liveDirectory();
        for (File f : cached)
            FileUtils.createHardLink(f, new File(live, f.name()));
        cfs.loadNewSSTables();
        System.out.println("Reused " + cached.size() + " cached dataset files; "
                           + cfs.getLiveSSTables().size() + " sstables live");
    }

    private ByteBuffer nextPayload(Random r)
    {
        byte[] blob = new byte[payloadSize];
        int repeated = payloadSize * repeatPercent / 100;
        r.nextBytes(blob);
        // Overwrite the leading section with a short repeating motif so LZ4 finds real matches.
        byte motif = (byte) r.nextInt();
        for (int i = 0; i < repeated; i++)
            blob[i] = (byte) (motif + (i % 8));
        return ByteBuffer.wrap(blob);
    }
}
