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
package org.apache.cassandra.db.compaction.differential;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.DigestingCursorMergeSink;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.PrecomputedDigestPartition;
import org.apache.cassandra.db.repair.ValidationCompactionController;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Digest-parity coverage for {@link DigestingCursorMergeSink} (CASSANDRA-21452 Phase 6), mirroring
 * {@link ValidationDigestParityTest}'s scenario matrix (static row, complex/collection column,
 * counter, range tombstone/partition delete) but comparing PER-PARTITION digest bytes - matching
 * how {@code Validator.rowHash()} actually uses digests in production (one fresh
 * {@code Digest.forValidator()} per partition, not one continuous hash across a whole table) -
 * rather than one combined running hash across every partition.
 */
public class DigestingCursorMergeSinkParityTest extends CQLTester
{
    @Test
    public void staticRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, sk bigint static, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 10; pk++)
            {
                execute("UPDATE %s SET sk = ? WHERE pk = ?", pk + round, pk);
                for (long ck = 0; ck < 5; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck + "-" + round);
            }
            flush();
        }
        assertPerPartitionDigestsMatch(cfs);
    }

    @Test
    public void complexColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<text>, l list<text>, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 10; pk++)
                for (long ck = 0; ck < 5; ck++)
                {
                    execute("UPDATE %s SET m = m + ?, s = s + ?, l = l + ? WHERE pk = ? AND ck = ?",
                            java.util.Map.of("k" + round, ck + round), java.util.Set.of("e" + round), java.util.List.of("x" + round), pk, ck);
                    if (round == 1 && ck % 2 == 0)
                        execute("DELETE m['k0'] FROM %s WHERE pk = ? AND ck = ?", pk, ck);
                }
            flush();
        }
        assertPerPartitionDigestsMatch(cfs);
    }

    @Test
    public void counterColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 4; round++)
        {
            for (long pk = 0; pk < 10; pk++)
                for (long ck = 0; ck < 5; ck++)
                {
                    execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", ck + round, pk, ck);
                    if (ck % 3 == 0)
                        execute("UPDATE %s SET c2 = c2 + ? WHERE pk = ? AND ck = ?", -1L - round, pk, ck);
                }
            flush();
        }
        assertPerPartitionDigestsMatch(cfs);
    }

    @Test
    public void counterTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 5; ck++)
                execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = ? AND ck = ?", ck + 1, ck + 2, pk, ck);
        flush();

        // Delete counter column c2 on some cells - these are counter-column tombstones that must
        // digest as regular (non-counter) tombstone cells, matching the legacy path. Kept in a
        // separate sstable so the tombstone actually flows through the counter merge (7346 supremacy).
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 5; ck++)
                if (ck % 2 == 0)
                    execute("DELETE c2 FROM %s WHERE pk = ? AND ck = ?", pk, ck);
        flush();

        assertPerPartitionDigestsMatch(cfs);
    }

    @Test
    public void rangeTombstoneAndPartitionDelete() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        flush();

        for (long pk = 0; pk < 10; pk++)
            execute("DELETE FROM %s WHERE pk = ? AND ck > 2 AND ck < 7", pk);
        execute("DELETE FROM %s WHERE pk = ?", 3L);
        flush();

        assertPerPartitionDigestsMatch(cfs);
    }

    // A nowInSec far enough ahead that gc_grace_seconds=0 tombstones and short TTLs are past their
    // local deletion / expiration times, exercising the purge / ttl-to-tombstone paths.
    private static long farFutureNowInSec()
    {
        return FBUtilities.nowInSeconds() + TimeUnit.DAYS.toSeconds(30);
    }

    // G2: purge parity. gc_grace_seconds=0 + a far-future nowInSec makes every tombstone purgeable,
    // exercising shouldPurge for cell/row/range/partition tombstones and fully-purged-partition
    // suppression - the legacy PurgeFunction drop and the cursor skip must agree (same partition
    // count, same digests).
    @Test
    public void purgeParity() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, w text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 8; ck++)
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?)", pk, ck, "v" + ck, "w" + ck);
        flush();

        for (long pk = 0; pk < 10; pk++)
        {
            execute("DELETE v FROM %s WHERE pk = ? AND ck = ?", pk, 0L);        // purged simple-cell tombstone
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, 1L);          // purged row deletion
            execute("DELETE FROM %s WHERE pk = ? AND ck > 2 AND ck < 6", pk);   // purged range tombstone
        }
        execute("DELETE FROM %s WHERE pk = ?", 3L);                             // purged partition deletion -> fully purged partition
        for (long ck = 0; ck < 8; ck++)                                         // fully purge partition 7 row-by-row
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 7L, ck);
        flush();

        assertPerPartitionDigestsMatch(cfs, farFutureNowInSec());
    }

    // G3: boundary markers. Two overlapping range tombstones with different timestamps merge to a
    // RangeTombstoneBoundaryMarker where they meet, pinning writeRangeTombstone's end-then-start
    // deletion digest order.
    @Test
    public void rangeTombstoneBoundaryMarkers() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        flush();
        for (long pk = 0; pk < 10; pk++)
            execute("DELETE FROM %s WHERE pk = ? AND ck >= 2 AND ck <= 10", pk);
        flush();
        for (long pk = 0; pk < 10; pk++)
            execute("DELETE FROM %s WHERE pk = ? AND ck >= 6 AND ck <= 14", pk);
        flush();
        assertPerPartitionDigestsMatch(cfs);
    }

    // G4: row deletions and the late-start resurrect path (a newer cell surviving an older row
    // deletion, exercising CursorCompactor.lateStartRow).
    @Test
    public void rowDeletionsAndResurrect() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", pk, ck, "v" + ck);
        flush();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck = ?", pk, ck);   // row deletions
        flush();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 6; ck += 2)
                execute("UPDATE %s USING TIMESTAMP 3000 SET v = ? WHERE pk = ? AND ck = ?", "resurrect" + ck, pk, ck);  // newer cell resurrects
        flush();
        assertPerPartitionDigestsMatch(cfs);
    }

    // G5: simple (non-collection) column cell tombstones (DELETE of a specific regular column).
    @Test
    public void simpleColumnCellTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, a text, b text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", pk, ck, "a" + ck, "b" + ck);
        flush();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 6; ck++)
                if (ck % 2 == 0)
                    execute("DELETE a FROM %s WHERE pk = ? AND ck = ?", pk, ck);
        flush();
        assertPerPartitionDigestsMatch(cfs);
    }

    // G6: live TTL cells (ExpiringLivenessInfo digest) and TTL'd primary-key liveness.
    @Test
    public void ttlLiveCellsAndPkLiveness() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 5; ck++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TTL 1000000", pk, ck, "v" + ck);   // TTL'd PK liveness + cell
                execute("UPDATE %s USING TTL 1000000 SET v = ? WHERE pk = ? AND ck = ?", "u" + ck, pk, ck);
            }
        flush();
        assertPerPartitionDigestsMatch(cfs);   // normal now: cells are still live -> expiring liveness digested
    }

    // G6: cells expired at merge time -> ttl-to-tombstone conversion (validated at a far-future
    // nowInSec so the short TTL has expired, but gc_grace keeps them from being purged outright).
    @Test
    public void ttlExpiredToTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, w text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 5; ck++)
            {
                execute("INSERT INTO %s (pk, ck, w) VALUES (?, ?, ?)", pk, ck, "w" + ck);
                execute("UPDATE %s USING TTL 60 SET v = ? WHERE pk = ? AND ck = ?", "v" + ck, pk, ck);   // short TTL, expires
            }
        flush();
        assertPerPartitionDigestsMatch(cfs, farFutureNowInSec());
    }

    // G6: expiration straddling the boundary. A cell with a fixed TTL has a known localExpirationTime
    // (write-second + ttl). Validating with nowInSec just before that boundary (cell still live, an
    // expiring cell) and just after (cell expired -> ttl-to-tombstone, gc_grace keeps it from being
    // purged) must hold digest parity on BOTH sides, and the two sides must actually differ - proving
    // the boundary is genuinely crossed rather than landing on the same side twice.
    @Test
    public void ttlExpirationStraddlingBoundary() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, w text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        int ttl = 1000;
        // Capture a lower bound on the cell write-second: the actual localExpirationTime is
        // (actualWriteSecond + ttl) with actualWriteSecond >= writeNowLowerBound, so the chosen
        // before/after nowInSec values below keep a wide margin around the real boundary.
        long writeNowLowerBound = FBUtilities.nowInSeconds();
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 5; ck++)
            {
                execute("INSERT INTO %s (pk, ck, w) VALUES (?, ?, ?)", pk, ck, "w" + ck);   // untouched, keeps the row alive across the boundary
                execute("UPDATE %s USING TTL " + ttl + " SET v = ? WHERE pk = ? AND ck = ?", "v" + ck, pk, ck);
            }
        flush();

        long nowBeforeExpiry = writeNowLowerBound;             // strictly < localExpirationTime -> cell live
        long nowAfterExpiry = writeNowLowerBound + ttl + 100;  // strictly > localExpirationTime -> cell expired to tombstone

        // Parity must hold on both sides of the expiration boundary.
        assertPerPartitionDigestsMatch(cfs, nowBeforeExpiry);
        assertPerPartitionDigestsMatch(cfs, nowAfterExpiry);

        // Confirm the boundary is actually straddled: a live expiring cell and a ttl-to-tombstone
        // cell digest differently, so the two nowInSec runs must produce different per-partition
        // digests (otherwise the test is vacuously landing on the same side both times).
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        List<byte[]> liveDigests = perPartitionDigestsCursor(cfs, sstables, cfs.getDefaultGcBefore(nowBeforeExpiry), nowBeforeExpiry);
        List<byte[]> expiredDigests = perPartitionDigestsCursor(cfs, sstables, cfs.getDefaultGcBefore(nowAfterExpiry), nowAfterExpiry);
        assertEquals(liveDigests.size(), expiredDigests.size());
        boolean anyDiffer = false;
        for (int i = 0; i < liveDigests.size(); i++)
            anyDiffer |= !java.util.Arrays.equals(liveDigests.get(i), expiredDigests.get(i));
        assertTrue("test must straddle the expiration boundary: per-partition digests should differ before vs after expiry",
                   anyDiffer);
    }

    // G9: fixed-size primitives beyond bigint, including a fixed-size clustering column (ck int).
    @Test
    public void fixedSizePrimitives() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, i int, b boolean, f float, d double, ts timestamp, " +
                    "si smallint, ti tinyint, u uuid, PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int pk = 0; pk < 8; pk++)
            for (int ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, i, b, f, d, ts, si, ti, u) VALUES " +
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, 123e4567-e89b-12d3-a456-426614174000)",
                        pk, ck, ck * 7, (ck % 2 == 0), (float) (ck + 0.5), (double) (pk + 0.25),
                        new java.util.Date(1_700_000_000_000L + ck), (short) (ck + 1), (byte) ck);
        flush();
        assertPerPartitionDigestsMatch(cfs);
    }

    // G10: variable-size primitives beyond text, plus an empty (zero-length) blob value - the
    // vint-0 prefix-strip boundary in writeCellValue.
    @Test
    public void variableSizePrimitivesAndEmptyValue() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck text, bl blob, vi varint, dec decimal, n inet, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, bl, vi, dec, n) VALUES (0, 'a', 0x0a0b0c, 123456789012345678901234567890, 3.14159265358979, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, bl, vi, dec, n) VALUES (0, 'b', 0xff, -98765432109876543210, -0.0001, '::1')");
        execute("INSERT INTO %s (pk, ck, bl) VALUES (0, 'empty', 0x)");   // empty blob: zero-length value with hasValue set
        execute("INSERT INTO %s (pk, ck) VALUES (0, 'ckonly')");           // empty clustering-only row
        flush();
        assertPerPartitionDigestsMatch(cfs);
    }

    // G11: multi-component clustering (mixed types) with mixed reversed order, plus a range
    // tombstone spanning the multi-component key.
    @Test
    public void multiComponentAndReversedClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text, ck2 bigint, ck3 int, v text, PRIMARY KEY (pk, ck1, ck2, ck3)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000 " +
                    "AND CLUSTERING ORDER BY (ck1 DESC, ck2 ASC, ck3 DESC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int pk = 0; pk < 6; pk++)
            for (int c1 = 0; c1 < 3; c1++)
                for (long c2 = 0; c2 < 3; c2++)
                    for (int c3 = 0; c3 < 3; c3++)
                        execute("INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (?, ?, ?, ?, ?)", pk, "c" + c1, c2, c3, "v" + c1 + c2 + c3);
        flush();
        for (int pk = 0; pk < 6; pk++)
            execute("DELETE FROM %s WHERE pk = ? AND ck1 = ?", pk, "c1");   // range tombstone over a clustering prefix
        flush();
        assertPerPartitionDigestsMatch(cfs);
    }

    // G7: UDTs - frozen (single cell) and non-frozen (complex), plus a nested UDT.
    @Test
    public void userDefinedTypes() throws Throwable
    {
        String inner = createType("CREATE TYPE %s (a int, b text)");
        String outer = createType("CREATE TYPE %s (x int, y frozen<" + inner + ">)");
        createTable("CREATE TABLE %s (pk int, ck int, fu frozen<" + outer + ">, nu " + inner + ", " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int pk = 0; pk < 6; pk++)
            for (int ck = 0; ck < 4; ck++)
            {
                execute("INSERT INTO %s (pk, ck, fu) VALUES (?, ?, {x: ?, y: {a: ?, b: ?}})", pk, ck, ck, ck * 2, "n" + ck);
                execute("UPDATE %s SET nu = {a: ?, b: ?} WHERE pk = ? AND ck = ?", ck + 1, "m" + ck, pk, ck);
            }
        flush();
        for (int pk = 0; pk < 6; pk++)
            execute("UPDATE %s SET nu = {a: ?, b: ?} WHERE pk = ? AND ck = ?", 99, "updated", pk, 0);   // second sstable, complex-cell overwrite
        flush();
        assertPerPartitionDigestsMatch(cfs);
    }

    // G8: tuples and frozen collections (single-cell blobs).
    @Test
    public void tuplesAndFrozenCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t tuple<int, text>, fm frozen<map<text, int>>, " +
                    "fs frozen<set<int>>, fl frozen<list<text>>, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int pk = 0; pk < 6; pk++)
            for (int ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, t, fm, fs, fl) VALUES (?, ?, (?, ?), ?, ?, ?)",
                        pk, ck, ck, "t" + ck,
                        java.util.Map.of("k" + ck, ck), java.util.Set.of(ck, ck + 1), java.util.List.of("x" + ck, "y" + ck));
        flush();
        assertPerPartitionDigestsMatch(cfs);
    }

    // G12: static complex columns, static-row-only partitions, and static counter columns.
    @Test
    public void staticComplexAndStaticOnlyPartitions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, sm map<text, int> static, sl list<text> static, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int pk = 0; pk < 8; pk++)
        {
            execute("UPDATE %s SET sm = sm + ?, sl = sl + ? WHERE pk = ?", java.util.Map.of("k" + pk, pk), java.util.List.of("s" + pk), pk);
            if (pk % 2 == 0)   // half the partitions also get regular rows; the rest are static-row-only
                for (int ck = 0; ck < 3; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        }
        flush();
        for (int pk = 0; pk < 8; pk++)   // second sstable: mutate statics so they merge
            execute("UPDATE %s SET sm = sm + ? WHERE pk = ?", java.util.Map.of("k2-" + pk, pk + 100), pk);
        flush();
        assertPerPartitionDigestsMatch(cfs);
    }

    @Test
    public void staticCounterColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, sc counter static, c counter, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 3; round++)
        {
            for (int pk = 0; pk < 6; pk++)
            {
                execute("UPDATE %s SET sc = sc + ? WHERE pk = ?", (long) (pk + round + 1), pk);
                for (int ck = 0; ck < 3; ck++)
                    execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", (long) (ck + round + 1), pk, ck);
            }
            flush();
        }
        assertPerPartitionDigestsMatch(cfs);
    }

    private void assertPerPartitionDigestsMatch(ColumnFamilyStore cfs) throws Exception
    {
        assertPerPartitionDigestsMatch(cfs, FBUtilities.nowInSeconds());
    }

    /**
     * Same as {@link #assertPerPartitionDigestsMatch(ColumnFamilyStore)} but with an explicit
     * {@code nowInSec} both paths share - purge/expiry parity tests pass a far-future value so that
     * {@code gc_grace_seconds=0} tombstones and expired TTL cells become purgeable deterministically
     * (rather than depending on wall-clock crossing a second boundary during the test).
     */
    private void assertPerPartitionDigestsMatch(ColumnFamilyStore cfs, long nowInSec) throws Exception
    {
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        long gcBefore = cfs.getDefaultGcBefore(nowInSec);

        List<byte[]> legacyDigests = perPartitionDigestsLegacy(cfs, sstables, gcBefore, nowInSec);
        List<byte[]> cursorDigests = perPartitionDigestsCursor(cfs, sstables, gcBefore, nowInSec);

        assertEquals("legacy and cursor paths must see the same number of partitions",
                     legacyDigests.size(), cursorDigests.size());
        for (int i = 0; i < legacyDigests.size(); i++)
            assertArrayEquals("per-partition digest mismatch at partition " + i, legacyDigests.get(i), cursorDigests.get(i));
    }

    private List<byte[]> perPartitionDigestsLegacy(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, long gcBefore, long nowInSec) throws Exception
    {
        List<byte[]> digests = new ArrayList<>();
        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
            try
            {
                try (CompactionIterator ci = new CompactionIterator(OperationType.VALIDATION, scanners.scanners, controller,
                                                                    nowInSec, nextTimeUUID()))
                {
                    while (ci.hasNext())
                    {
                        try (UnfilteredRowIterator partition = ci.next())
                        {
                            Digest digest = Digest.forValidator();
                            UnfilteredRowIterators.digest(partition, digest, MessagingService.current_version);
                            digests.add(digest.digest());
                        }
                    }
                }
            }
            finally
            {
                scanners.close();
            }
        }
        return digests;
    }

    private List<byte[]> perPartitionDigestsCursor(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, long gcBefore, long nowInSec) throws Exception
    {
        List<byte[]> digests = new ArrayList<>();
        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore))
        {
            assertTrue("cursor validation must actually be supported for this scenario, or the parity check is vacuous",
                       CursorCompactor.isValidationSupported(sstables, controller));

            Map<SSTableReader, List<PartitionPositionBounds>> boundsBySSTable = new HashMap<>();
            for (SSTableReader sstable : sstables)
                boundsBySSTable.put(sstable, java.util.Collections.singletonList(sstable.getPositionsForFullRange()));

            DigestingCursorMergeSink sink = new DigestingCursorMergeSink(cfs.metadata());
            CursorCompactor compactor = new CursorCompactor(OperationType.VALIDATION, boundsBySSTable, controller,
                                                            nowInSec, nextTimeUUID());
            try
            {
                while (compactor.mergeNextPartition(sink))
                {
                    PrecomputedDigestPartition partition = sink.takePartitionDigest();
                    digests.add(partition.digestBytes());
                }
            }
            finally
            {
                compactor.close();
            }
        }
        return digests;
    }
}
