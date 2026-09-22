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

import java.util.List;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Differential scenarios for a MULTI-slice {@code ClusteringIndexSliceFilter} single-partition read
 * served on the cursor path (CASSANDRA-20428, gap #8). A multi-slice slice filter comes from a
 * compound-clustering restriction that expands to several clustering ranges (for example
 * {@code WHERE pk = ? AND c1 IN (1, 2)} on a table with more than one clustering column). Before this
 * gap was closed {@code CursorReads.isReadSupported} declined {@code slices.size() > 1} for a slice
 * filter, so the read served on the legacy iterator path.
 *
 * <p>A multi-slice slice read reuses the same machinery the names fix (gap #9) built: the merge core
 * scans the covering span (the first slice's start to the last slice's end) once, validates per slice
 * through {@code isInSlice}, and the emission slicer re-filters to the exact requested slices while
 * tracking the open range-tombstone marker across the gaps between slices. A range slice differs from
 * a names point slice only in that its start and end bound distinct clusterings and it may contain
 * range-tombstone markers internally, both of which the generic slicer already handles.
 *
 * <p>Every scenario asserts the result is byte-identical to the iterator path through the base harness
 * ({@link CursorReadDifferentialTester}). The base harness's silent-fallback guard proves the cursor
 * path actually engaged.
 *
 * <p>The base class runs BIG; {@link BtiMultiSliceCursorReadDifferentialTest} pins BTI, the priority
 * format for this work.
 */
public class MultiSliceCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void pinSelectedFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(formatName());
    }

    @After
    public void restoreSelectedFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /** The sstable format this class pins. The BTI subclass overrides it. */
    protected String formatName()
    {
        return "big";
    }

    // ---------------------------------------------------------------- command builders

    /** The decorated key for the bigint partition key {@code pk}. */
    private static DecoratedKey key(ColumnFamilyStore cfs, long pk)
    {
        return cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(pk));
    }

    /** An inclusive-inclusive range slice over a single bigint clustering column. */
    private static Slice inclSlice(TableMetadata metadata, long startIncl, long endIncl)
    {
        ClusteringBound<?> start = ClusteringBound.create(metadata.comparator, true, true, startIncl);
        ClusteringBound<?> end = ClusteringBound.create(metadata.comparator, false, true, endIncl);
        return Slice.make(start, end);
    }

    /** A forward multi-slice slice-filter read of the given inclusive ranges, optional CQL limit. */
    private static Supplier<SinglePartitionReadCommand> multiSliceRead(ColumnFamilyStore cfs, long pk,
                                                                       long now, int limit, long[]... ranges)
    {
        return () -> {
            TableMetadata metadata = cfs.metadata();
            Slices.Builder builder = new Slices.Builder(metadata.comparator);
            for (long[] range : ranges)
                builder.add(inclSlice(metadata, range[0], range[1]));
            Slices slices = builder.build();
            DataLimits limits = limit < 0 ? DataLimits.NONE : DataLimits.cqlLimits(limit);
            return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(),
                                                     limits, key(cfs, pk),
                                                     new ClusteringIndexSliceFilter(slices, false));
        };
    }

    /** Differential comparison plus the merge-effectiveness guards: the cursor merge ran, and the
     *  expected sstable leg count joined it, across the harness's two cursor runs. */
    private void assertMergedMultiSliceMatches(ColumnFamilyStore cfs,
                                               Supplier<SinglePartitionReadCommand> command,
                                               int expectedMergedLegsPerRun)
    {
        long mergesBefore = CursorReads.cursorMergesServed();
        long legsBefore = CursorReads.sstableLegsCursorMerged();

        assertCursorReadMatchesIterator(cfs, command);

        assertEquals("cursor-level merges served across the harness's two cursor runs",
                     2L, CursorReads.cursorMergesServed() - mergesBefore);
        assertEquals("sstable legs cursor-merged across the harness's two cursor runs",
                     2L * expectedMergedLegsPerRun, CursorReads.sstableLegsCursorMerged() - legsBefore);
    }

    // ---------------------------------------------------------------- single-leg

    /** Two disjoint slices against one sstable leg: rows fall inside slice one, in the gap, and
     *  inside slice two. The slicer must emit only the two requested ranges. */
    @Test
    public void twoDisjointSlicesSingleLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, multiSliceRead(cfs, 1L, now, -1,
                                                            new long[]{ 2, 5 }, new long[]{ 12, 16 }));
    }

    /** Two adjacent (contiguous, no row gap) slices against one sstable leg. */
    @Test
    public void adjacentSlicesSingleLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        // [3..7] then [8..11]: contiguous coverage, still two distinct slices in the filter
        assertCursorReadMatchesIterator(cfs, multiSliceRead(cfs, 1L, now, -1,
                                                            new long[]{ 3, 7 }, new long[]{ 8, 11 }));
    }

    /** A range tombstone opens inside the first slice and closes inside the second, so the deletion
     *  is open across the gap between the two slices. The slicer must carry the open marker across the
     *  gap and synthesize the correct artificial close/open bounds at each slice edge. */
    @Test
    public void sliceGapCrossingRangeTombstoneSingleLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        // a range tombstone spanning ck 5..14, deleting rows the slices below straddle
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 5L, 14L);
        flush();

        long now = FBUtilities.nowInSeconds();
        // slice one [3..7] holds the tombstone open (at 5); the gap 8..10 stays deleted; slice two
        // [11..17] holds the tombstone close (at 14)
        assertCursorReadMatchesIterator(cfs, multiSliceRead(cfs, 1L, now, -1,
                                                            new long[]{ 3, 7 }, new long[]{ 11, 17 }));
    }

    // ---------------------------------------------------------------- multi-leg

    /** Multi-slice read across two overlapping sstable legs: newer values on some rows, a cross-leg
     *  range tombstone, both slices spanning both legs. */
    @Test
    public void multiSliceMultiLeg() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "first-" + ck);
        flush();
        // second leg: newer values on even rows, plus a range tombstone covering ck 9..12
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 9L, 12L);
        for (long ck = 0; ck < 20; ck += 2)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "second-" + ck, 1L, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertMergedMultiSliceMatches(cfs, multiSliceRead(cfs, 1L, now, -1,
                                                          new long[]{ 2, 6 }, new long[]{ 10, 15 }), 2);
    }

    /** A range tombstone opened in one leg and closed in another, straddling a slice gap: exercises
     *  cross-leg open-marker reconciliation under multi-slice emission. */
    @Test
    public void multiSliceCrossLegRangeTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", 1L, 6L, 14L);
        flush();

        long now = FBUtilities.nowInSeconds();
        assertMergedMultiSliceMatches(cfs, multiSliceRead(cfs, 1L, now, -1,
                                                          new long[]{ 3, 8 }, new long[]{ 12, 18 }), 2);
    }

    // ---------------------------------------------------------------- limit

    /** Multi-slice read with a CQL limit. {@code limitBoundFor} declines multi-slice, so production
     *  stays unbounded and the top-of-stack limit counter stays authoritative; the result must still
     *  be byte-identical to the iterator path. */
    @Test
    public void multiSliceWithLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        // three slices, limit smaller than the total matching rows so the limit actually bites
        assertCursorReadMatchesIterator(cfs, multiSliceRead(cfs, 1L, now, 4,
                                                            new long[]{ 1, 3 }, new long[]{ 7, 9 },
                                                            new long[]{ 14, 18 }));
    }

    // ---------------------------------------------------------------- memtable

    /** A multi-slice read that merges a memtable leg with an sstable leg. */
    @Test
    public void memtablePlusSstableMultiSlice() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "sstable-" + ck);
        flush();
        for (long ck = 0; ck < 20; ck += 3)
            execute("UPDATE %s SET v2 = ? WHERE pk = ? AND ck = ?", "memtable-" + ck, 1L, ck);

        long memtableLegsBefore = CursorReads.memtableLegsCursorMerged();
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, multiSliceRead(cfs, 1L, now, -1,
                                                            new long[]{ 2, 6 }, new long[]{ 11, 17 }));
        assertTrue("the memtable leg must join the cursor merge",
                   CursorReads.memtableLegsCursorMerged() - memtableLegsBefore > 0);
    }

    /** A memtable-only multi-slice read (no sstable). Modeled on {@code NamesFilter}'s
     *  {@code memtableOnlyNames}: the memtable takes the object path and no sstable leg is counted,
     *  yet the result must stay byte-identical to the iterator oracle. */
    @Test
    public void memtableOnlyMultiSlice() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, ck);
        // no flush: the read is served from the memtable only

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = multiSliceRead(cfs, 1L, now, -1,
                                                                  new long[]{ 1, 4 }, new long[]{ 10, 15 });

        DatabaseDescriptor.setCursorReadsEnabled(false);
        List<String> oracleRecords = canonicalRecords(cmd.get());
        byte[] oracleBytes = responseBytes(cmd.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            assertTrue("memtable-only multi-slice read must pass the cursor read gate",
                       CursorReads.isReadSupported(cmd.get(), cfs, liveSSTablesFor(cfs, cmd.get())));

            List<String> cursorRecords = canonicalRecords(cmd.get());
            byte[] cursorBytes = responseBytes(cmd.get());

            compareRecords(oracleRecords, cursorRecords);
            assertResponseBytesEqual(oracleBytes, cursorBytes);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    // ---------------------------------------------------------------- transcode routing

    /** The full intra-node response bytes for a command, through the production replica entry point. */
    private static byte[] fullResponseBytes(SinglePartitionReadCommand command) throws Exception
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, buffer, MessagingService.current_version);
            return buffer.toByteArray();
        }
    }

    /**
     * A multi-slice slice read must DECLINE the single-slice transcode fast path
     * ({@code queryStorageToResponseBytes}) and serve on the plain cursor merge path instead. The
     * transcode path streams one contiguous slice and would mis-read several ranges. This asserts the
     * transcode counter does not advance while the gate-on response stays byte-identical to the
     * gate-off (iterator) oracle, so the decline routes to the cursor path, not to a wrong result.
     */
    @Test
    public void multiSliceDeclinesTranscodePath() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();
        for (long ck = 20; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, ck, "v-" + ck);
        flush();

        long now = FBUtilities.nowInSeconds();
        Supplier<SinglePartitionReadCommand> cmd = multiSliceRead(cfs, 1L, now, -1,
                                                                  new long[]{ 2, 8 }, new long[]{ 25, 33 });

        DatabaseDescriptor.setCursorReadsEnabled(false);
        byte[] off = fullResponseBytes(cmd.get());

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long before = CursorReads.transcodeResponsesServed();
            byte[] on = fullResponseBytes(cmd.get());
            long after = CursorReads.transcodeResponsesServed();

            assertResponseBytesEqual(off, on);
            assertEquals("a multi-slice slice read must not engage the single-slice transcode fast path",
                         before, after);
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }
}
