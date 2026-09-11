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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Compacts a mix of a large indexed partition, many tiny partitions, and a partition-level tombstone under
 * various {@code column_index_size} / {@code column_index_cache_size} configurations, and checks the output reads
 * back correctly. This is a pre-CASSANDRA-21555 baseline: none of the optimisation's presizing APIs exist yet, so
 * every configuration exercised here must compact and read back correctly on the current tree.
 */
public class CompactionColumnIndexSizeTest extends CQLTester
{
    private static final int BIG_ROW_VALUE_BYTES = 1024;
    private static final int TINY_PARTITION_COUNT = 200;

    private int savedIndexSizeInKiB;
    private int savedCacheSizeInKiB;

    @Before
    public void saveConfig()
    {
        savedIndexSizeInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedCacheSizeInKiB = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
    }

    @After
    public void restoreConfig()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedIndexSizeInKiB);
        DatabaseDescriptor.setColumnIndexCacheSize(savedCacheSizeInKiB);
    }

    @Test
    public void compactionWithOneKiBIndexUncompressed() throws Throwable
    {
        writeCompactAndVerify(1, 0, false, 2000, true);
    }

    @Test
    public void compactionWithOneKiBIndexCompressed() throws Throwable
    {
        writeCompactAndVerify(1, 0, true, 2000, true);
    }

    @Test
    public void compactionWithOneKiBIndexLargeCache() throws Throwable
    {
        // 1024 KiB (1MiB) cache threshold keeps the index samples in array mode even at this scale.
        writeCompactAndVerify(1, 1024, false, 2000, true);
    }

    @Test
    public void compactionWithLargeIndexBlocks() throws Throwable
    {
        // 100 rows * ~1KiB stays comfortably under the 256KiB block threshold, so the big partition is single-block.
        writeCompactAndVerify(256, 0, false, 100, false);
    }

    @Test
    public void compactionSucceedsWithZeroColumnIndexSize() throws Throwable
    {
        // On the current (pre-CASSANDRA-21555) tree, column_index_size=0 just means "index every row" -- expensive,
        // but correct. The optimisation is expected to make this configuration FAIL; keeping it as its own test
        // method (rather than folded into another) is deliberate, so that expected failure reads cleanly.
        writeCompactAndVerify(0, null, false, 200, false);
    }

    @Test
    public void compactionSucceedsWithZeroColumnIndexSizeCompressed() throws Throwable
    {
        // See compactionSucceedsWithZeroColumnIndexSize -- expected to start failing once CASSANDRA-21555 lands.
        writeCompactAndVerify(0, null, true, 200, false);
    }

    @Test
    public void presizeOvershootManySmallPartitionsAfterHugeOne() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        createTable("CREATE TABLE %s (pk text, ck text, val text, PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        disableCompaction();

        String bigKeyStr = "huge";
        int bigRows = 2000;
        Map<String, String> expectedBigRows = new LinkedHashMap<>();
        for (int i = 0; i < bigRows; i++)
        {
            String ck = String.format("c%05d", i);
            String val = CompactionAllocationTest.makeRandomString(BIG_ROW_VALUE_BYTES);
            expectedBigRows.put(ck, val);
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", bigKeyStr, ck, val);
        }
        flush();

        int tinyCount = 500;
        Map<String, String> tinyValues = new LinkedHashMap<>();
        for (int i = 0; i < tinyCount; i++)
        {
            String pk = String.format("tiny%04d", i);
            String val = CompactionAllocationTest.makeRandomString(32);
            tinyValues.put(pk, val);
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", pk, "row", val);
        }
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(2, cfs.getLiveSSTables().size());

        compact();
        assertEquals(1, cfs.getLiveSSTables().size());

        UntypedResultSet full = execute("SELECT ck, val FROM %s WHERE pk = ?", bigKeyStr);
        assertEquals(bigRows, full.size());
        List<String> orderedCks = new ArrayList<>(expectedBigRows.keySet());
        int idx = 0;
        for (UntypedResultSet.Row row : full)
        {
            assertEquals(orderedCks.get(idx), row.getString("ck"));
            assertEquals(expectedBigRows.get(orderedCks.get(idx)), row.getString("val"));
            idx++;
        }

        for (Map.Entry<String, String> tiny : tinyValues.entrySet())
        {
            UntypedResultSet tinyResult = execute("SELECT val FROM %s WHERE pk = ?", tiny.getKey());
            assertEquals(1, tinyResult.size());
            assertEquals(tiny.getValue(), tinyResult.one().getString("val"));
        }
    }

    @Test
    public void topPartitionsHintDoesNotAffectCorrectness() throws Throwable
    {
        int bigRows = 300;
        long approxBigPartitionBytes = (long) bigRows * BIG_ROW_VALUE_BYTES;

        // exact, lying-small, and lying-huge hints must all be harmless: at this commit nothing consumes the
        // TopPartitionTracker size estimate for presizing, so the compacted output must read back identically
        // regardless of what the hint claims.
        for (long hint : new long[]{ approxBigPartitionBytes, 16L, 1L << 40 })
        {
            writeCompactAndVerify(1, 0, false, bigRows, false, cfs -> {
                Assume.assumeTrue("topPartitions is only tracked for non-system tables", cfs.topPartitions != null);
                DecoratedKey bigKey = cfs.decorateKey(ByteBufferUtil.bytes("big"));
                // a range whose start equals its end covers the whole ring, see TopPartitionTrackerTest
                Collection<Range<Token>> fullRange =
                    Collections.singleton(new Range<>(new Murmur3Partitioner.LongToken(0), new Murmur3Partitioner.LongToken(0)));
                TopPartitionTracker.Collector collector = new TopPartitionTracker.Collector(fullRange);
                collector.trackPartitionSize(bigKey, hint);
                cfs.topPartitions.merge(collector);
            });
        }
    }

    private void writeCompactAndVerify(int indexKiB, Integer cacheSizeKiB, boolean compressed, int bigRows,
                                        boolean expectManyBlocks) throws Throwable
    {
        writeCompactAndVerify(indexKiB, cacheSizeKiB, compressed, bigRows, expectManyBlocks, cfs -> { });
    }

    /**
     * Writes a big indexed partition (split across two flushes, so no single input sstable holds the whole thing),
     * {@link #TINY_PARTITION_COUNT} one-row partitions, and a partition-level tombstone for one of those tiny
     * partitions (in its own flush, over a key already written earlier) -- then compacts and verifies every angle
     * the plan calls for: full/spot/sliced reads of the big partition, tiny-partition read-back, the tombstone
     * reading empty, extended verification of the single output, and (for the 1KiB cases) its index block count.
     */
    private void writeCompactAndVerify(int indexKiB, Integer cacheSizeKiB, boolean compressed, int bigRows,
                                        boolean expectManyBlocks, Consumer<ColumnFamilyStore> beforeCompact) throws Throwable
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(indexKiB);
        if (cacheSizeKiB != null)
            DatabaseDescriptor.setColumnIndexCacheSize(cacheSizeKiB);

        String compression = compressed ? "{'class': 'LZ4Compressor'}" : "{'enabled': 'false'}";
        createTable("CREATE TABLE %s (pk text, ck text, val text, PRIMARY KEY (pk, ck)) WITH compression = " + compression);
        disableCompaction();

        String bigKeyStr = "big";
        Map<String, String> expectedBigRows = new LinkedHashMap<>();
        int half = bigRows / 2;
        for (int part = 0; part < 2; part++)
        {
            int from = part == 0 ? 0 : half;
            int to = part == 0 ? half : bigRows;
            for (int i = from; i < to; i++)
            {
                String ck = String.format("c%05d", i);
                String val = CompactionAllocationTest.makeRandomString(BIG_ROW_VALUE_BYTES);
                expectedBigRows.put(ck, val);
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", bigKeyStr, ck, val);
            }
            flush();
        }

        Map<String, String> tinyValues = new LinkedHashMap<>();
        for (int i = 0; i < TINY_PARTITION_COUNT; i++)
        {
            String pk = String.format("tiny%03d", i);
            String val = CompactionAllocationTest.makeRandomString(32);
            tinyValues.put(pk, val);
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", pk, "row", val);
        }
        flush();

        String deletedKey = "tiny000";
        execute("DELETE FROM %s WHERE pk = ?", deletedKey);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        beforeCompact.accept(cfs);

        compact();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader output = cfs.getLiveSSTables().iterator().next();

        // (a) full read + spot checks at 0/25/50/75/100%
        List<String> orderedCks = new ArrayList<>(expectedBigRows.keySet());
        UntypedResultSet full = execute("SELECT ck, val FROM %s WHERE pk = ?", bigKeyStr);
        assertEquals(bigRows, full.size());
        int idx = 0;
        for (UntypedResultSet.Row row : full)
        {
            String ck = orderedCks.get(idx);
            assertEquals(ck, row.getString("ck"));
            assertEquals(expectedBigRows.get(ck), row.getString("val"));
            idx++;
        }
        for (int spot : new int[]{ 0, bigRows / 4, bigRows / 2, (3 * bigRows) / 4, bigRows - 1 })
        {
            String ck = orderedCks.get(spot);
            UntypedResultSet spotResult = execute("SELECT val FROM %s WHERE pk = ? AND ck = ?", bigKeyStr, ck);
            assertEquals(1, spotResult.size());
            assertEquals(expectedBigRows.get(ck), spotResult.one().getString("val"));
        }

        // (b) forward and reversed slice reads over an interior range
        int sliceFromIdx = bigRows / 4;
        int sliceToIdx = Math.min(bigRows - 1, sliceFromIdx + 100);
        String sliceFrom = orderedCks.get(sliceFromIdx);
        String sliceTo = orderedCks.get(sliceToIdx);

        List<String> forwardCks = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT ck FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", bigKeyStr, sliceFrom, sliceTo))
            forwardCks.add(row.getString("ck"));
        assertEquals(orderedCks.subList(sliceFromIdx, sliceToIdx + 1), forwardCks);

        List<String> reversedCks = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT ck FROM %s WHERE pk = ? AND ck >= ? AND ck <= ? ORDER BY ck DESC",
                                                bigKeyStr, sliceFrom, sliceTo))
            reversedCks.add(row.getString("ck"));
        List<String> expectedReversed = new ArrayList<>(forwardCks);
        Collections.reverse(expectedReversed);
        assertEquals(expectedReversed, reversedCks);

        // (c) every tiny partition except the deleted one reads back
        for (Map.Entry<String, String> tiny : tinyValues.entrySet())
        {
            if (tiny.getKey().equals(deletedKey))
                continue;
            UntypedResultSet tinyResult = execute("SELECT val FROM %s WHERE pk = ?", tiny.getKey());
            assertEquals(1, tinyResult.size());
            assertEquals(tiny.getValue(), tinyResult.one().getString("val"));
        }

        // (d) the deleted partition reads empty
        assertEquals(0, execute("SELECT * FROM %s WHERE pk = ?", deletedKey).size());

        // (e) extended verification of the single output
        try (IVerifier verifier = output.getVerifier(cfs, new OutputHandler.LogOutput(), false,
                                                      IVerifier.options().invokeDiskFailurePolicy(true)
                                                                         .extendedVerification(true).build()))
        {
            verifier.verify();
        }

        // (f) the 1KiB cases must have produced a deeply-indexed row index entry
        if (expectManyBlocks)
        {
            Assume.assumeTrue(BigFormat.isSelected());
            DecoratedKey bigKey = cfs.decorateKey(ByteBufferUtil.bytes(bigKeyStr));
            RowIndexEntry rie = ((BigTableReader) output).getRowIndexEntry(bigKey, SSTableReader.Operator.EQ);
            assertNotNull(rie);
            assertTrue("expected >1000 index blocks, got " + rie.blockCount(), rie.blockCount() > 1000);
        }
    }
}
