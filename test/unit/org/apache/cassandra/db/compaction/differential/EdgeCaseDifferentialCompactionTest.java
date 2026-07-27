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

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Edge-case corpus for the differential cursor-vs-iterator compaction harness, covering the
 * CURRENTLY SUPPORTED cursor compaction surface (see CursorCompactor.isSupported). Every
 * scenario here must run the cursor path for real — the harness fails loudly on silent
 * fallback. Scenarios for unsupported shapes belong in CursorSupportMatrixTest instead.
 *
 * Every scenario runs the differential at TWO generations (see
 * assertCursorMatchesIteratorAcrossGenerations): gen 2 re-compacts genuinely cursor-produced
 * outputs, so write-side corruption that only the NEXT merge can see fails here.
 */
public class EdgeCaseDifferentialCompactionTest extends DifferentialCompactionTester
{
    private static final Set<String> ALLOWLIST = Set.of();

    /**
     * Static-column table where some partitions have NO static values: an empty static row is
     * written for those partitions but must not be counted in stats (totalRows/totalColumnsSet).
     * Found by the randomized soak; the original staticRows scenario gave every
     * partition static data and so never produced an empty static row.
     */
    @Test
    public void emptyStaticRows() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s1 text static, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 8; pk++)
            {
                // only even partitions ever get a static value
                if (pk % 2 == 0)
                    execute("INSERT INTO %s (pk, s1, ck, v) VALUES (?, ?, ?, ?)", pk, "static" + pk, (long) round, "v" + round);
                else
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, (long) round, "v" + round);
            }
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Reversed clustering order changes on-disk ordering and bound comparisons. */
    @Test
    public void descendingClustering() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 5; pk++)
                for (long ck = 0; ck < 30; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r" + round + "v" + ck);
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", (long) round, 5L, 15L);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Multi-component clusterings: mixed types, shared prefixes, per-component bounds. */
    @Test
    public void compositeClustering() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck1 text, ck2 int, ck3 bigint, v text, " +
                    "PRIMARY KEY (pk, ck1, ck2, ck3))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String[] names = { "alpha", "beta", "gamma", "" /* empty string component */ };
        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 4; pk++)
                for (String ck1 : names)
                    for (int ck2 = 0; ck2 < 5; ck2++)
                        execute("INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (?, ?, ?, ?, ?)",
                                pk, ck1, ck2, (long) round, "v" + round);
            // prefix range delete: full ck1, partial (ck1, ck2) prefix
            execute("DELETE FROM %s WHERE pk = ? AND ck1 = ?", (long) round, "beta");
            execute("DELETE FROM %s WHERE pk = ? AND ck1 = ? AND ck2 >= ? AND ck2 < ?",
                    (long) round, "gamma", 1, 4);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Wide partition crossing column-index block boundaries (indexed RowIndexEntry path). */
    @Test
    public void widePartitionCrossingIndexBlocks() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        // two sstables, each with the same single wide partition (~4000 rows * ~200B >> 64KiB index block)
        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 4000; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, padding + "-" + round + "-" + ck);
            // plus range tombstones inside the wide partition
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, round * 500L, round * 500L + 250L);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /**
     * Partition that crosses the column-index block threshold exactly once: the index has one
     * cut block plus a tail. Iterator promotes the index (2 entries); exercises the cursor's
     * promotion decision boundary (rowIndexEntriesOffsets.size() <= 1 check happens before the
     * tail block is added).
     */
    @Test
    public void partitionCrossingOneIndexBlock() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        // ~6KB partition: crosses the test config's column_index_size (4KiB) exactly once,
        // producing one cut block plus a tail — the index promotion boundary
        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 30; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, padding + "-" + round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Overlapping range tombstones across sstables: boundary markers must merge identically. */
    @Test
    public void overlappingRangeTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 3; pk++)
            for (long ck = 0; ck < 100; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        flush();

        // sstable 2: ranges [10, 50), [60, 70]
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 10 AND ck < 50");
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 60 AND ck <= 70");
        flush();

        // sstable 3: ranges overlapping/adjacent to sstable 2's: [30, 65), (70, 80]
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 30 AND ck < 65");
        execute("DELETE FROM %s WHERE pk = 0 AND ck > 70 AND ck <= 80");
        // and exact adjacency in another partition: [10,20) then [20,30)
        execute("DELETE FROM %s WHERE pk = 1 AND ck >= 10 AND ck < 20");
        flush();

        execute("DELETE FROM %s WHERE pk = 1 AND ck >= 20 AND ck < 30");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Frozen collections and tuples are single cells and inside the supported surface. */
    @Test
    public void frozenCollections() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, " +
                    "m frozen<map<text, bigint>>, l frozen<list<text>>, s frozen<set<int>>, " +
                    "t frozen<tuple<int, text>>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 5; pk++)
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, m, l, s, t) VALUES (?, ?, ?, ?, ?, (?, ?))",
                            pk, ck,
                            map("k" + round, ck, "x", (long) round),
                            list("a" + round, "b" + ck),
                            set((int) ck, round, 42),
                            round, "tup" + ck);
            execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", 0L, (long) round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** TTLs: live expiring cells and already-expired cells (expiry far from run boundaries). */
    @Test
    public void expiringCells() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // long TTLs: alive during both runs
        for (long pk = 0; pk < 5; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL 86400", pk, ck, "a" + ck, "b" + ck);
        flush();

        // short TTLs: expired well before either run
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TTL 1", 1L, ck, "expired" + ck);
        // mixed: row with one TTL'd and one permanent cell
        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s USING TTL 86400 SET v1 = ? WHERE pk = ? AND ck = ?", "ttl" + ck, 2L, ck);
        flush();

        Thread.sleep(2000); // let the short TTLs expire well before the first run

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Same-timestamp conflicting writes: reconciliation must tie-break identically. */
    @Test
    public void timestampTies() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "aaa" + ck);
        flush();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "zzz" + ck);
        flush();

        // tombstone vs write at the same timestamp: delete wins
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck = 5");
        for (long ck = 4; ck < 7; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1L, ck, "tie" + ck);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Newer partition deletion shadowing older data across several sstables. */
    @Test
    public void shadowedPartitions() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 6; pk++)
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r" + round);
            flush();
        }
        execute("DELETE FROM %s WHERE pk = 2");
        execute("DELETE FROM %s WHERE pk = 3");
        // resurrection after the partition delete
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 3L, 0L, "alive-again");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Single-input compaction: pure rewrite, no merge. */
    @Test
    public void singleInputSSTable() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 2 AND ck < 6");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Many inputs: 8-way merge exercises the merge heap harder than the usual 2-4. */
    @Test
    public void eightWayMerge() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 8; round++)
        {
            // partial, interleaved coverage: each sstable covers a sliding window
            for (long pk = round; pk < round + 6; pk++)
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r" + round + "c" + ck);
            if (round % 2 == 0)
                execute("DELETE FROM %s WHERE pk = ? AND ck = ?", (long) round, 3L);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Disjoint inputs: no overlapping partitions, pure concatenation. */
    @Test
    public void disjointInputs() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = round * 100; pk < round * 100 + 10; pk++)
                for (long ck = 0; ck < 5; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Empty (zero-length) values are valid and distinct from null; both must survive merge. */
    @Test
    public void emptyAndNullValues() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 blob, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, "value" + ck, ByteBufferUtil.bytes("cafe"));
        flush();

        // empty-string / empty-blob overwrites
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, "", ByteBufferUtil.EMPTY_BYTE_BUFFER);
        // null overwrites (cell tombstones)
        for (long ck = 5; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, null, null)", 1L, ck);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /**
     * Empty clustering values on a DESC (reversed) clustering column, found by the randomized
     * soak (seed 99303954147053). Every base type sorts empty before
     * values, so a reversed column sorts empty AFTER values (ReversedType swaps operands
     * around the base comparison). The cursor path's raw clustering comparison decided
     * empty-vs-valued purely from the serialized flag bits, ignoring reversal:
     *  - same-partition variant: rows with empty and valued clusterings for the SAME pk in
     *    different sstables merge in the wrong order (Data.db divergence — corruption class);
     *  - cross-partition variant: the global covered-clustering max picks the wrong row
     *    (Statistics.db divergence — what the soak caught).
     */
    @Test
    public void emptyClusteringValuesDescending() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sstable 1: valued clusterings for pk 1 (same-partition variant) and pk 2 (the
        // lexically-largest valued rows, cross-partition variant)
        for (long ck = 1; ck <= 5; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "p1v" + ck);
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, ck * 1000, "p2v" + ck);
        }
        flush();

        // sstable 2: EMPTY clustering values — same partition as pk 1's valued rows (the
        // merge must order empty AFTER values under DESC), plus an empty-only partition
        // (the global max clustering must be the empty value, not pk 2's large bigints)
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ByteBufferUtil.EMPTY_BYTE_BUFFER, "p1empty");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 3L, ByteBufferUtil.EMPTY_BYTE_BUFFER, "p3empty");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 3L, "p1v3-overwrite");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** ASC counterpart of emptyClusteringValuesDescending: empty sorts BEFORE values on a
     *  non-reversed column; pins the unflipped flag ordering. */
    @Test
    public void emptyClusteringValuesAscending() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 1; ck <= 5; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "p1v" + ck);
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, -ck * 1000, "p2v" + ck);
        }
        flush();

        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ByteBufferUtil.EMPTY_BYTE_BUFFER, "p1empty");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 3L, ByteBufferUtil.EMPTY_BYTE_BUFFER, "p3empty");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 3L, "p1v3-overwrite");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /**
     * Row liveness shapes: UPDATE-built rows carry NO primary-key liveness (different row
     * flags than INSERT-built rows), primary-key-only INSERTs carry liveness and ZERO cells,
     * and merges must reconcile liveness presence/absence across sstables exactly.
     */
    @Test
    public void rowLivenessShapes() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // UPDATE-built rows (no liveness) and liveness-only rows in sstable 1
        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s SET v1 = ?, v2 = ? WHERE pk = ? AND ck = ?", "u" + ck, "w" + ck, 1L, ck);
        for (long ck = 10; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1L, ck);
        flush();

        // sstable 2: INSERT onto UPDATE-rows (liveness arrives later), cell tombstones onto
        // liveness-only rows (row must survive on liveness alone), cell delete that empties
        // an UPDATE-row entirely (no liveness + no cells = row vanishes)
        for (long ck = 0; ck < 4; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, "i" + ck);
        for (long ck = 10; ck < 13; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, null, null)", 1L, ck);
        execute("DELETE v1, v2 FROM %s WHERE pk = ? AND ck = ?", 1L, 5L);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /**
     * Row-level TTL (INSERT USING TTL sets liveness TTL + cell TTLs) merged against
     * cell-level TTL (UPDATE USING TTL sets only cell TTLs) and against plain writes;
     * includes same-timestamp expiring writes whose TTLs differ (rules (c)/(d) of the
     * resolveRegular decision table run off localExpirationTime/ttl, not just timestamps).
     */
    @Test
    public void rowAndCellTtlMix() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 12; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL 86400", 1L, ck, "a" + ck, "b" + ck);
        flush();

        // cell-level TTL different from the row TTL; plain overwrites clearing TTLs;
        // expiring-vs-expiring same-timestamp ties with different TTLs
        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TTL 172800 SET v1 = ? WHERE pk = ? AND ck = ?", "c" + ck, 1L, ck);
        for (long ck = 6; ck < 9; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, "plain" + ck);
        for (long ck = 9; ck < 12; ck++)
            execute("UPDATE %s USING TTL 100000 AND TIMESTAMP 5000 SET v2 = ? WHERE pk = ? AND ck = ?", "t1" + ck, 1L, ck);
        flush();

        for (long ck = 9; ck < 12; ck++)
            execute("UPDATE %s USING TTL 50000 AND TIMESTAMP 5000 SET v2 = ? WHERE pk = ? AND ck = ?", "t2" + ck, 1L, ck);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /**
     * Expiring-vs-live cells at the SAME timestamp, both directions across sstables: the
     * CASSANDRA-14592 rule — an expiring (or deleted) cell beats a live one on timestamp
     * tie regardless of value. Implemented in resolveRegular rule (a); this pins it at the
     * differential level (timestampTies only covered live-vs-live and delete-vs-live).
     */
    @Test
    public void expiringVsLiveTies() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // direction 1: live first, expiring second
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "zzz-live" + ck);
        // direction 2 partition: expiring first
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000 AND TTL 86400", 2L, ck, "aaa-ttl" + ck);
        flush();

        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000 AND TTL 86400", 1L, ck, "aaa-ttl" + ck);
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 2L, ck, "zzz-live" + ck);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /** Vector and duration columns: fixed-dimension float vectors and the
     *  variable-length duration encoding as ordinary single cells, overwritten and
     *  null-overwritten (cell tombstone) across sstables. */
    @Test
    public void vectorAndDuration() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, vec vector<float, 3>, dur duration, v text, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, vec, dur, v) VALUES (?, ?, [1.5, 2.5, " + ck + ".0], 2h30m, ?)",
                    1L, ck, "v" + ck);
        flush();

        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, vec, dur, v) VALUES (?, ?, [9.0, 8.0, 7.0], 45s500ms, ?)",
                    1L, ck, "w" + ck);
        // null overwrites: cell tombstones for vector and duration cells
        execute("INSERT INTO %s (pk, ck, vec, dur) VALUES (?, ?, null, null)", 1L, 7L);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /**
     * More than 64 regular columns: rows lacking columns switch from the 64-bit-mask
     * column-subset encoding to the structurally different large-subset wire format —
     * byte-compared here for the first time; previously only the old simple-suite
     * exercised it without comparing the paths against each other.
     */
    @Test
    public void over64Columns() throws Exception
    {
        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        for (int i = 0; i < 70; i++)
            ddl.append(", c").append(i).append(" int");
        ddl.append(", PRIMARY KEY (pk, ck))");
        createTable(ddl.toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sparse rows: each ck sets a sliding 10-column window (subset encoding for >64 columns)
        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 14; ck++)
            {
                StringBuilder stmt = new StringBuilder("INSERT INTO %s (pk, ck");
                int base = (int) ck * 5 + round * 3;
                for (int i = 0; i < 10; i++)
                    stmt.append(", c").append((base + i) % 70);
                stmt.append(") VALUES (?, ?");
                for (int i = 0; i < 10; i++)
                    stmt.append(", ").append(base + i);
                stmt.append(')');
                execute(stmt.toString(), 1L, ck);
            }
            // one full row per round: the HAS_ALL_COLUMNS path next to large subsets
            StringBuilder full = new StringBuilder("INSERT INTO %s (pk, ck");
            for (int i = 0; i < 70; i++)
                full.append(", c").append(i);
            full.append(") VALUES (?, ?");
            for (int i = 0; i < 70; i++)
                full.append(", ").append(i);
            full.append(')');
            execute(full.toString(), 1L, 99L);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }

    /**
     * ODD superset size at the subset-encoding mode boundary: with 71 columns, the encoder
     * and decoder must agree on present-index vs missing-index
     * mode at exactly presentCount == 35 (the integer-division boundary of supersetCount/2).
     * Rows at 34/35/36 present columns straddle the boundary from both sides.
     */
    @Test
    public void over64ColumnsOddSupersetBoundary() throws Exception
    {
        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        for (int i = 0; i < 71; i++)
            ddl.append(", c").append(i).append(" int");
        ddl.append(", PRIMARY KEY (pk, ck))");
        createTable(ddl.toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 2; round++)
        {
            long ck = 0;
            for (int present : new int[]{ 34, 35, 36, 70 })
            {
                StringBuilder stmt = new StringBuilder("INSERT INTO %s (pk, ck");
                for (int i = 0; i < present; i++)
                    stmt.append(", c").append((i + round) % 71); // shift per round so the merge unions
                stmt.append(") VALUES (?, ?");
                for (int i = 0; i < present; i++)
                    stmt.append(", ").append(i);
                stmt.append(')');
                execute(stmt.toString(), 1L, ck++);
            }
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs, ALLOWLIST);
    }
}
