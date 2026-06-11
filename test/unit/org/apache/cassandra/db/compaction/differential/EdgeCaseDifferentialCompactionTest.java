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

    /**
     * Static-column table where some partitions have NO static values: an empty static row is
     * written for those partitions but must not be counted in stats (totalRows/totalColumnsSet).
     * Found by the randomized soak (finding #7); the original staticRows scenario gave every
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Multi-cell collections merged across sstables: element updates, full-collection
     *  overwrites (complex deletion + cells), deletion-only columns, UDT field merges. */
    @Test
    public void multiCellColumnsAcrossSSTables() throws Exception
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, u " + udt + ", v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 8; ck++)
                execute("INSERT INTO %s (pk, ck, m, s, u, v) VALUES (?, ?, ?, ?, {a: ?, b: ?}, ?)",
                        pk, ck, map("k" + ck, ck, "shared", pk), set((int) ck, 7), (int) ck, "b" + ck, "v" + ck);
        flush();

        // sstable 2: element updates (merge new paths into existing columns), UDT field update
        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 8; ck += 2)
            {
                execute("UPDATE %s SET m[?] = ?, s = s + ? WHERE pk = ? AND ck = ?", "added" + ck, ck * 10, set(99), pk, ck);
                execute("UPDATE %s SET u.b = ? WHERE pk = ? AND ck = ?", "upd" + ck, pk, ck);
            }
        flush();

        // sstable 3: full-collection overwrites (complex deletion + fresh cells), deletion-only,
        // and same-path overwrites (path-equal merge with newer timestamps)
        execute("UPDATE %s SET m = ? WHERE pk = ? AND ck = ?", map("fresh", 1L), 0L, 0L);
        execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", 1L, 2L);
        execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "shared", 555L, 2L, 4L);
        execute("DELETE s FROM %s WHERE pk = ? AND ck = ?", 3L, 6L);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** The headline interaction: complex deletions interleaved with range tombstones —
     *  range deletes shadow whole rows including complex columns, complex deletions shadow
     *  cells within a column, both merging across sstables. */
    @Test
    public void complexDeletionsWithRangeTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 3; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, m, v) VALUES (?, ?, ?, ?)", pk, ck, map("a" + ck, ck, "b", pk), "v" + ck);
        flush();

        // range tombstones over rows with complex data + complex deletions inside surviving rows
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 5 AND ck < 12");
        execute("UPDATE %s SET m = ? WHERE pk = 0 AND ck = ?", map("replaced", 1L), 2L);
        execute("DELETE m FROM %s WHERE pk = 1 AND ck = ?", 15L);
        flush();

        // newer writes into ranges + paths shadowed by the earlier complex deletion
        execute("INSERT INTO %s (pk, ck, m, v) VALUES (?, ?, ?, ?)", 0L, 7L, map("resurrect", 7L), "back");
        execute("UPDATE %s SET m[?] = ? WHERE pk = 1 AND ck = ?", "post", 999L, 15L);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Empty clustering values on a DESC (reversed) clustering column — finding #10, found by
     * the widened randomized soak (seed 99303954147053). Every base type sorts empty before
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Element-level tombstones inside multi-cell columns: a tombstone cell that CARRIES a
     * CellPath (DELETE m['k'], set-element removal, list-index delete with its TimeUUID path)
     * is a distinct wire shape from both whole-column complex deletions and pathless cell
     * tombstones, and it flows through the path-ordered merge. Includes resurrection of a
     * deleted path by a newer write and a tombstone for a path that never existed.
     */
    @Test
    public void cellPathTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, l list<text>, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 3; pk++)
            for (long ck = 0; ck < 8; ck++)
                execute("INSERT INTO %s (pk, ck, m, s, l) VALUES (?, ?, ?, ?, ?)",
                        pk, ck, map("k0", 0L, "k1", 1L, "shared", ck), set(1, 2, 3), list("a", "b", "c"));
        flush();

        // element-level tombstones, all path-carrying
        for (long pk = 0; pk < 3; pk++)
            for (long ck = 0; ck < 8; ck += 2)
            {
                execute("DELETE m[?] FROM %s WHERE pk = ? AND ck = ?", "k0", pk, ck);
                execute("UPDATE %s SET s = s - ? WHERE pk = ? AND ck = ?", set(2), pk, ck);
                execute("DELETE l[1] FROM %s WHERE pk = ? AND ck = ?", pk, ck);
            }
        flush();

        // resurrect deleted paths with newer writes; tombstone a path that never existed
        execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "k0", 100L, 0L, 0L);
        execute("UPDATE %s SET s = s + ? WHERE pk = ? AND ck = ?", set(2), 0L, 0L);
        execute("DELETE m[?] FROM %s WHERE pk = ? AND ck = ?", "ghost", 1L, 1L);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Equal-timestamp conflicts INSIDE complex columns: path-level value ties (the finding-#4
     * tie-break class, but on the increment-2 path-merge code) and complex-deletion-vs-cell
     * ties (a complex deletion shadows cells with timestamp <= its own).
     */
    @Test
    public void complexTimestampTies() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET m[?] = ?, m[?] = ? WHERE pk = ? AND ck = ?",
                    "k", 111L, "other", 1L, 1L, ck);
        flush();

        // same path, same timestamp, different value: greater value must win in both paths
        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET m[?] = ? WHERE pk = ? AND ck = ?",
                    "k", 222L, 1L, ck);
        flush();

        // complex deletion at ts 1500 vs cells at 1500 (shadowed: <= deletion) and 1501 (survives)
        for (long ck = 0; ck < 5; ck++)
        {
            execute("DELETE m FROM %s USING TIMESTAMP 1500 WHERE pk = ? AND ck = ?", 1L, ck);
            execute("UPDATE %s USING TIMESTAMP 1500 SET m[?] = ? WHERE pk = ? AND ck = ?", "atDel", 5L, 1L, ck);
            execute("UPDATE %s USING TIMESTAMP 1501 SET m[?] = ? WHERE pk = ? AND ck = ?", "afterDel", 6L, 1L, ck);
        }
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * TTLs on collection cells: live expiring cells inside multi-cell columns, expired
     * cells converted to (path-carrying) tombstones, and a complex deletion over TTL'd
     * cells — expiry far from the run boundaries per the harness clock limitation.
     */
    @Test
    public void collectionCellTtls() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, m, s) VALUES (?, ?, ?, ?)",
                    1L, ck, map("perm", ck), set(1, 2));
        flush();

        // live TTL'd elements alongside permanent ones; expired elements (TTL 1)
        for (long ck = 0; ck < 8; ck++)
            execute("UPDATE %s USING TTL 86400 SET m[?] = ?, s = s + ? WHERE pk = ? AND ck = ?",
                    "ttl", ck * 10, set(3), 1L, ck);
        for (long ck = 0; ck < 4; ck++)
            execute("UPDATE %s USING TTL 1 SET m[?] = ? WHERE pk = ? AND ck = ?", "gone", 9L, 1L, ck);
        flush();

        // complex deletion over a column whose surviving cells are TTL'd
        execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", 1L, 6L);
        execute("UPDATE %s USING TTL 86400 SET m[?] = ? WHERE pk = ? AND ck = ?", "after", 1L, 1L, 6L);
        flush();

        Thread.sleep(2000); // let the TTL-1 elements expire well before the first run

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Complex columns in partitions crossing index blocks (test config column_index_size is
     * 4KiB): the promoted-index machinery (findings #5/#6) and the BTI row trie have only
     * ever seen simple-column rows at block boundaries. Range tombstones and complex
     * deletions land mid-block so block-boundary state (open marker tracking) carries
     * multi-cell content.
     */
    @Test
    public void complexColumnsCrossingIndexBlocks() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000 AND compression = {'enabled': false}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String pad = "x".repeat(120);
        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 40; ck++)
                execute("INSERT INTO %s (pk, ck, m) VALUES (?, ?, ?)",
                        1L, ck, map("a" + round, pad + ck, "b" + round, pad, "c" + round, pad));
            // range tombstone and complex deletions landing mid-partition
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 10L + round, 14L + round);
            execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", 1L, 20L + round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * STATIC complex columns in the differential corpus (previously only covered at the
     * reader level): element updates and complex deletions on static collections across
     * sstables, static-only partitions, and partitions with no static values at all
     * (empty static row + complex machinery in one row).
     */
    @Test
    public void staticComplexColumns() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, sm map<text, bigint> static, ss set<int> static, " +
                    "ck bigint, v text, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 6; pk++)
        {
            // pk 0/1: static + rows; pk 2/3: rows only (empty static row); pk 4: static only
            if (pk < 2 || pk == 4)
                execute("UPDATE %s SET sm[?] = ?, ss = ss + ? WHERE pk = ?", "s" + pk, pk, set((int) pk), pk);
            if (pk != 4)
                for (long ck = 0; ck < 4; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        }
        flush();

        // static element updates, static element tombstone, static complex deletion
        execute("UPDATE %s SET sm[?] = ? WHERE pk = ?", "added", 100L, 0L);
        execute("DELETE sm[?] FROM %s WHERE pk = ?", "s1", 1L);
        execute("DELETE ss FROM %s WHERE pk = ?", 0L);
        execute("UPDATE %s SET sm[?] = ? WHERE pk = ?", "late", 5L, 2L); // statics arrive for a rows-only partition
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Nested types (task-15 M5): multi-cell collections whose VALUES are frozen collections,
     * non-frozen UDTs containing frozen-collection fields, UDT-in-UDT, and tuples — the
     * increment-2 machinery over nested single-cell payloads, merged and deleted across
     * sstables.
     */
    @Test
    public void nestedTypes() throws Exception
    {
        String inner = createType("CREATE TYPE %s (xs frozen<list<int>>, name text)");
        String outer = createType("CREATE TYPE %s (i frozen<" + inner + ">, tag text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, frozen<list<int>>>, " +
                    "u " + inner + ", o " + outer + ", t tuple<int, text>, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 3; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, m, u, o, t) VALUES (?, ?, ?, " +
                        "{xs: [1, 2], name: ?}, {i: {xs: [3], name: ?}, tag: ?}, (?, ?))",
                        pk, ck, map("a", list(1, 2), "b", list((int) ck)),
                        "n" + ck, "deep" + ck, "g" + ck, (int) ck, "t" + ck);
        flush();

        // element updates with fresh frozen-list values, UDT field updates (incl. the
        // frozen-collection field as a single cell), element tombstone, complex deletion,
        // tuple overwrite
        for (long pk = 0; pk < 3; pk++)
        {
            execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "a", list(9, 9, 9), pk, 0L);
            execute("UPDATE %s SET u.name = ?, u.xs = ? WHERE pk = ? AND ck = ?", "upd", list(7), pk, 1L);
            execute("UPDATE %s SET o.tag = ? WHERE pk = ? AND ck = ?", "retag", pk, 2L);
            execute("DELETE m[?] FROM %s WHERE pk = ? AND ck = ?", "b", pk, 3L);
            execute("DELETE u FROM %s WHERE pk = ? AND ck = ?", pk, 4L);
            execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, (?, ?))", pk, 5L, 42, "new");
        }
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Vector and duration columns (task-15 M5): fixed-dimension float vectors and the
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * More than 64 regular columns (task-15 M5): rows lacking columns switch from the
     * 64-bit-mask column-subset encoding to the structurally different large-subset wire
     * format (the documented allocating fallback from finding #9) — byte-compared here for
     * the first time; previously only the old simple-suite exercised it without comparing
     * the paths against each other.
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * OPEN-ENDED (single-sided) range tombstones: DELETE with only a lower or upper
     * clustering bound produces markers whose other side is the unbounded partition edge —
     * zero-component TOP/BOTTOM bounds, the same empty-prefix region finding #10 lived in
     * (bound-kind comparisons, covered-clustering stats). Open RTs nest with each other,
     * overlap bounded RTs and rows across sstables, and one partition is open-RT-only.
     * Previously covered only probabilistically by the widened soak.
     */
    @Test
    public void openEndedRangeTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 1; pk <= 2; pk++)
            for (long ck = 0; ck < 30; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 10L, 20L); // bounded, for interleave
        flush();

        // open-ended deletes: up to TOP, down from BOTTOM, nested opens, and an RT-only partition
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 1L, 25L);
        execute("DELETE FROM %s WHERE pk = ? AND ck > ?", 1L, 27L);  // nests inside the >= 25 open range
        execute("DELETE FROM %s WHERE pk = ? AND ck <= ?", 2L, 4L);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 3L, 0L);  // partition with ONLY an open RT
        flush();

        // resurrection inside open-deleted ranges with newer timestamps
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 26L, "resurrected");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, 2L, "resurrected");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** DESC counterpart: single-sided bounds invert in on-disk clustering order, so the
     *  open edge swaps between TOP and BOTTOM relative to the CQL bound direction. */
    @Test
    public void openEndedRangeTombstonesDescending() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC) AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "v" + ck);
        flush();

        execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 1L, 25L);
        execute("DELETE FROM %s WHERE pk = ? AND ck <= ?", 1L, 4L);
        flush();

        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 27L, "resurrected");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * ODD superset size at the subset-encoding mode boundary (finding #12, second bug):
     * with 71 columns, the encoder and decoder must agree on present-index vs missing-index
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

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }
}
