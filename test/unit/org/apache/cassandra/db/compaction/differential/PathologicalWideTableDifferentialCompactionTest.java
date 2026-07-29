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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Pathological wide-schema differential scenario (task-15 follow-up): ~2,000 columns in one
 * table — 1,800 regulars plus 200 statics on a 20-type palette (primitives, blobs, uuids,
 * inet, duration, frozen collections, tuples, vectors, and 20% MULTI-CELL columns: maps,
 * sets, lists, UDTs) — exercising at scale what the 70-column scenarios exercise at the
 * threshold:
 *
 *  - the large-column-subset wire format with thousands of indices, in BOTH encoding modes
 *    and at the exact present==supersetCount/2 mode boundary (finding #12's machinery);
 *  - hundreds of complex-column markers in a single row's assembly (marker array growth);
 *  - full rows (HAS_ALL_COLUMNS), sparse windows including wrap-around (trailing-present
 *    shapes), single-column rows, UPDATE-only rows, liveness-only rows;
 *  - cell tombstones by the hundreds, null-overwrite tombstones, complex deletions across
 *    scattered multi-cell columns, element updates and element tombstones;
 *  - per-cell TTLs (live and expired-to-tombstone), wide static blocks with their own
 *    subset encoding, range/row/partition deletes layered over all of it.
 *
 * Its own suite (not EdgeCase): the DDL and prepared statements are large and setup
 * dominates runtime. BTI variant via subclass; two generations as everywhere.
 *
 * Width is property-configurable (defaults reproduce the standard 1800+200 run); properties
 * must reach the forked test JVM via -Dtest.jvm.args:
 *
 *   ant testsome -Dtest.name=...PathologicalWideTableDifferentialCompactionTest \
 *       -Dtest.jvm.args="-Dcassandra.test.differential.wide.regulars=5000
 *                        -Dcassandra.test.differential.wide.statics=500"
 *
 * Keep regulars >= 128 so the >64-column subset encodings and the present==half mode
 * boundary stay exercised; everything else (boundary rows, windows, delete sets) derives
 * from the configured width.
 */
public class PathologicalWideTableDifferentialCompactionTest extends DifferentialCompactionTester
{
    private static final int REGULARS = Integer.getInteger("cassandra.test.differential.wide.regulars", 1800);
    private static final int STATICS = Integer.getInteger("cassandra.test.differential.wide.statics", 200);
    private static final int PALETTE = 20;

    private String udt;

    private String typeFor(int i)
    {
        switch (i % PALETTE)
        {
            case 0: return "bigint";
            case 1: return "text";
            case 2: return "int";
            case 3: return "double";
            case 4: return "blob";
            case 5: return "uuid";
            case 6: return "boolean";
            case 7: return "decimal";
            case 8: return "varint";
            case 9: return "inet";
            case 10: return "duration";
            case 11: return "frozen<list<int>>";
            case 12: return "frozen<map<text, int>>";
            case 13: return "tuple<int, text>";
            case 14: return "vector<float, 3>";
            case 15: return "map<text, bigint>";   // multi-cell
            case 16: return "set<int>";            // multi-cell
            case 17: return "list<text>";          // multi-cell
            case 18: return udt;                   // multi-cell
            case 19: return "timestamp";
            default: throw new AssertionError();
        }
    }

    private Object valueFor(int i, int salt) throws Exception
    {
        long v = i * 31L + salt;
        switch (i % PALETTE)
        {
            case 0: return v;
            case 1: return "t" + v;
            case 2: return (int) v;
            case 3: return v / 7.0;
            case 4: return ByteBuffer.wrap(new byte[]{ (byte) v, (byte) (v >> 8), (byte) salt });
            case 5: return new UUID(v, ~v);
            case 6: return (v & 1) == 0;
            case 7: return BigDecimal.valueOf(v, 3);
            case 8: return BigInteger.valueOf(v).pow(3);
            case 9: return InetAddress.getByAddress(new byte[]{ 10, (byte) salt, (byte) (i >> 8), (byte) i });
            case 10: return Duration.newInstance(0, (int) (v % 28) + 1, (v % 1000) * 1_000_000L);
            case 11: return list((int) v, (int) v + 1);
            case 12: return map("m" + (v % 5), (int) v);
            case 13: return tuple((int) v, "tu" + v);
            case 14: return vector((float) (v % 100), salt + 0.5f, 3.25f);
            case 15: return map("k" + (v % 3), v, "shared", (long) salt);
            case 16: return set((int) (v % 50), 7);
            case 17: return list("l" + (v % 4), "x" + salt);
            case 18: return userType("a", (int) v, "b", "u" + (v % 6));
            case 19: return new Date(1_700_000_000_000L + v);
            default: throw new AssertionError();
        }
    }

    private static boolean isMultiCell(int i)
    {
        int m = i % PALETTE;
        return m >= 15 && m <= 18;
    }

    /** INSERT setting columns [start, start+count) (mod REGULARS) plus the primary key. */
    private void insertWindow(long pk, long ck, int start, int count, int salt, String using) throws Throwable
    {
        StringBuilder stmt = new StringBuilder("INSERT INTO %s (pk, ck");
        Object[] params = new Object[count + 2];
        params[0] = pk;
        params[1] = ck;
        for (int i = 0; i < count; i++)
        {
            int col = (start + i) % REGULARS;
            stmt.append(", r").append(col);
            params[i + 2] = valueFor(col, salt);
        }
        stmt.append(") VALUES (?, ?");
        stmt.append(", ?".repeat(count));
        stmt.append(')').append(using);
        execute(stmt.toString(), params);
    }

    @Test
    public void thousandsOfColumns() throws Throwable
    {
        logger.info("pathological-wide parameters: regulars={} statics={}", REGULARS, STATICS);
        udt = createType("CREATE TYPE %s (a int, b text)");

        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        for (int i = 0; i < REGULARS; i++)
            ddl.append(", r").append(i).append(' ').append(typeFor(i));
        for (int i = 0; i < STATICS; i++)
            ddl.append(", s").append(i).append(' ').append(typeFor(i)).append(" static");
        ddl.append(", PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        createTable(ddl.toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 2; round++)
        {
            int salt = round * 1000;

            // pk 0: full rows — HAS_ALL_COLUMNS with ~360 complex markers per row
            for (long ck = 0; ck < 2; ck++)
                insertWindow(0, ck, 0, REGULARS, salt + (int) ck, "");

            // pk 1: sparse windows, offsets chosen to wrap (trailing-present subset shapes)
            for (long ck = 0; ck < 10; ck++)
                insertWindow(1, ck, (int) ck * 173 + round * 61, 60, salt, "");

            // pk 2: the present == supersetCount/2 encoding-mode boundary, both sides
            insertWindow(2, 0, 0, REGULARS / 2 - 1, salt, "");
            insertWindow(2, 1, 0, REGULARS / 2, salt, "");
            insertWindow(2, 2, 0, REGULARS / 2 + 1, salt, "");

            // pk 3: single-column row, UPDATE-only row (no liveness), liveness-only row
            insertWindow(3, 0, 37, 1, salt, "");
            execute("UPDATE %s SET r0 = ?, r1 = ? WHERE pk = ? AND ck = ?",
                    valueFor(0, salt), valueFor(1, salt), 3L, 1L);
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 3L, 2L);

            // pk 4: TTL'd full row; an expired-TTL window (round 0 only, expires before runs)
            insertWindow(4, 0, 0, REGULARS, salt, " USING TTL 86400");
            if (round == 0)
                insertWindow(4, 1, 100, 40, salt, " USING TTL 1");

            // pk 6: the wide static block (its own subset encoding) plus regular rows
            {
                StringBuilder stmt = new StringBuilder("UPDATE %s SET ");
                Object[] params = new Object[STATICS + 1];
                for (int i = 0; i < STATICS; i++)
                {
                    if (i > 0) stmt.append(", ");
                    stmt.append('s').append(i).append(" = ?");
                    params[i] = valueFor(i, salt + 7);
                }
                stmt.append(" WHERE pk = ?");
                params[STATICS] = 6L;
                execute(stmt.toString(), params);
            }
            for (long ck = 0; ck < 3; ck++)
                insertWindow(6, ck, (int) ck * 200, 30, salt, "");

            // pk 7: partition that exists in round 0 and is deleted in the tombstone layer
            if (round == 0)
                insertWindow(7, 0, 0, 25, salt, "");

            flush();
        }

        // tombstone layer: a third sstable of deletes over everything above
        {
            // hundreds of named-column cell tombstones on a full row
            StringBuilder del = new StringBuilder("DELETE ");
            int n = 0;
            for (int i = 0; i < REGULARS && n < 300; i += 6, n++)
            {
                if (n > 0) del.append(", ");
                del.append('r').append(i);
            }
            del.append(" FROM %s WHERE pk = ? AND ck = ?");
            execute(del.toString(), 0L, 0L);

            // complex deletions across scattered multi-cell columns of the other full row
            StringBuilder cdel = new StringBuilder("DELETE ");
            n = 0;
            for (int i = 15; i < REGULARS && n < 40; i += PALETTE, n++)
            {
                if (n > 0) cdel.append(", ");
                cdel.append('r').append(i);
            }
            cdel.append(" FROM %s WHERE pk = ? AND ck = ?");
            execute(cdel.toString(), 0L, 1L);

            // element updates and element tombstones on multi-cell columns
            execute("UPDATE %s SET r15[?] = ?, r16 = r16 + ? WHERE pk = ? AND ck = ?",
                    "fresh", 1234L, set(99), 0L, 1L);
            execute("DELETE r35[?] FROM %s WHERE pk = ? AND ck = ?", "k0", 0L, 1L);

            // null-overwrite tombstones over a sparse window
            StringBuilder nul = new StringBuilder("INSERT INTO %s (pk, ck");
            for (int i = 0; i < 60; i++)
                nul.append(", r").append((173 + i) % REGULARS); // overlaps pk1 ck1's window
            nul.append(") VALUES (?, ?").append(", null".repeat(60)).append(')');
            execute(nul.toString(), 1L, 1L);

            // static cell + static complex deletes
            execute("DELETE s0, s1, s15, s16 FROM %s WHERE pk = ?", 6L);

            // range, row, and partition deletes
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 1L, 7L); // open-ended over windows
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 2L, 2L);  // boundary row deleted
            execute("DELETE FROM %s WHERE pk = ?", 7L);                 // whole partition
            flush();
        }

        Thread.sleep(2000); // let the TTL-1 window expire well before the first run

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }
}
