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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Increment 2(a) reader-level differential: multi-cell (complex) column READING through
 * SSTableCursorReader, verified cell-by-cell against the standard iterator read of the same
 * sstable. The production cursor-compaction gate (CursorCompactor.unsupportedMetadata) stays
 * CLOSED for complex schemas until merge (2b) and write (2c) land — this test drives the
 * reader directly, which is exactly how 2(b) will consume it.
 *
 * Canonical record stream compared (same file order on both sides):
 *   "SR" / "R" / "TM"                              structural markers
 *   "CPLX <col> del=<ts>,<ldt> n=<count>"           complex column header (incl. zero-cell)
 *   "CELL <col> path=<hex|-> live=<ts>,<ttl>,<ldt> v=<hex>"  every cell
 * Cell value bytes are compared in WIRE form: the cursor copies vint-prefixed bytes for
 * variable-length types (copyCellContents), the oracle serializes the iterator cell's value
 * through the same AbstractType.writeValue — byte-exact by construction.
 */
public class ComplexColumnCursorReadTest extends CQLTester
{
    /** Oracle: canonical records from the standard iterator read. */
    private static List<String> iteratorRecords(SSTableReader sstable) throws IOException
    {
        List<String> out = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    if (!partition.staticRow().isEmpty())
                    {
                        out.add("SR");
                        rowRecords(partition.staticRow(), out);
                    }
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRow())
                        {
                            out.add("R");
                            rowRecords((Row) unfiltered, out);
                        }
                        else
                        {
                            out.add("TM");
                        }
                    }
                }
            }
        }
        return out;
    }

    private static void rowRecords(Row row, List<String> out) throws IOException
    {
        for (ColumnData cd : row)
        {
            if (cd.column().isComplex())
            {
                ComplexColumnData complex = (ComplexColumnData) cd;
                out.add(String.format("CPLX %s del=%d,%d n=%d",
                                      cd.column().name, complex.complexDeletion().markedForDeleteAt(),
                                      complex.complexDeletion().localDeletionTime(), complex.cellsCount()));
                for (Cell<?> cell : complex)
                    out.add(cellRecord(cell));
            }
            else
            {
                out.add(cellRecord((Cell<?>) cd));
            }
        }
    }

    private static String cellRecord(Cell<?> cell) throws IOException
    {
        String path = cell.path() == null ? "-" : Hex.bytesToHex(ByteBufferUtil.getArray(cell.path().get(0)));
        String value = "";
        if (cell.valueSize() > 0)
        {
            try (DataOutputBuffer dob = new DataOutputBuffer())
            {
                cell.column().type.writeValue(cell.buffer(), org.apache.cassandra.db.marshal.ByteBufferAccessor.instance, dob);
                value = Hex.bytesToHex(dob.toByteArray());
            }
        }
        return String.format("CELL %s path=%s live=%d,%d,%d v=%s",
                             cell.column().name, path, cell.timestamp(), cell.ttl(), cell.localDeletionTime(), value);
    }

    /** Cursor side: same canonical records by driving SSTableCursorReader directly. */
    private static List<String> cursorRecords(SSTableReader sstable) throws Exception
    {
        List<String> out = new ArrayList<>();
        try (SSTableCursorReader cursor = new SSTableCursorReader(sstable))
        {
            cursor.complexColumnListener((column, deletion, count) ->
                out.add(String.format("CPLX %s del=%d,%d n=%d",
                                      column.name, deletion.markedForDeleteAt(), deletion.localDeletionTime(), count)));

            PartitionDescriptor pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            UnfilteredDescriptor rHeader = new UnfilteredDescriptor(sstable.header.clusteringTypes().toArray(AbstractType[]::new));
            byte[] transfer = new byte[4096];

            int state = cursor.readPartitionHeader(pHeader);
            while (state != DONE)
            {
                while (state != PARTITION_END)
                {
                    switch (state)
                    {
                        case STATIC_ROW_START:
                            out.add("SR");
                            state = cursor.readStaticRowHeader(rHeader);
                            state = readCells(cursor, state, transfer, out);
                            break;
                        case ROW_START:
                            out.add("R");
                            state = cursor.readRowHeader(rHeader);
                            state = readCells(cursor, state, transfer, out);
                            break;
                        case TOMBSTONE_START:
                            out.add("TM");
                            state = cursor.readTombstoneMarker(rHeader);
                            break;
                        default:
                            throw new IllegalStateException("state " + state);
                    }
                    if (state == UNFILTERED_END)
                        state = cursor.continueReading();
                }
                state = cursor.continueReading();
                if (state != DONE)
                    state = cursor.readPartitionHeader(pHeader);
            }
        }
        return out;
    }

    private static int readCells(SSTableCursorReader cursor, int state, byte[] transfer, List<String> out) throws IOException
    {
        SSTableCursorReader.CellCursor cc = cursor.cellCursor();
        while (true)
        {
            if (state == UNFILTERED_END)
                return state;
            if (state == CELL_END)
            {
                state = cursor.continueReading();
                continue;
            }
            if (state != CELL_HEADER_START)
                throw new IllegalStateException("unexpected state " + state);

            state = cursor.readCellHeader(); // CPLX records emitted via listener as headers are consumed
            if (!cc.producedCell)
                continue; // trailing deletion-only complex column(s); state is CELL_END with flags preloaded

            String path = cc.cellPathLength < 0 ? "-" : Hex.bytesToHex(java.util.Arrays.copyOf(cc.cellPathBuffer, cc.cellPathLength));
            String value = "";
            if (state == CELL_VALUE_START)
            {
                try (DataOutputBuffer dob = new DataOutputBuffer())
                {
                    state = cursor.copyCellValue(dob, transfer);
                    value = Hex.bytesToHex(dob.toByteArray());
                }
            }
            out.add(String.format("CELL %s path=%s live=%d,%d,%d v=%s",
                                  cc.cellColumn.name, path,
                                  cc.cellLiveness.timestamp(), cc.cellLiveness.ttl(), cc.cellLiveness.localExpirationTime(),
                                  value));
        }
    }

    private void assertCursorReadsMatch() throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        flush();
        assertEquals("expected exactly one sstable", 1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        List<String> expected = iteratorRecords(sstable);
        List<String> actual = cursorRecords(sstable);

        // non-vacuousness: the scenario must actually have produced complex columns and
        // path-carrying cells, or this test compares nothing it exists to compare
        assertTrue("scenario produced no complex column records",
                   expected.stream().anyMatch(r -> r.startsWith("CPLX")));
        assertTrue("scenario produced no path-carrying cells",
                   expected.stream().anyMatch(r -> r.startsWith("CELL") && !r.contains("path=-")));

        int max = Math.max(expected.size(), actual.size());
        for (int i = 0; i < max; i++)
        {
            String e = i < expected.size() ? expected.get(i) : "<missing>";
            String a = i < actual.size() ? actual.get(i) : "<missing>";
            if (!e.equals(a))
                fail(String.format("record %d differs:%n  iterator: %s%n  cursor:   %s%n  context: %s",
                                   i, e, a, context(expected, actual, i)));
        }
    }

    private static String context(List<String> expected, List<String> actual, int i)
    {
        StringBuilder sb = new StringBuilder();
        for (int j = Math.max(0, i - 3); j < Math.min(expected.size(), i + 3); j++)
            sb.append(String.format("%n    it[%d]=%s", j, expected.get(j)));
        for (int j = Math.max(0, i - 3); j < Math.min(actual.size(), i + 3); j++)
            sb.append(String.format("%n    cu[%d]=%s", j, actual.get(j)));
        return sb.toString();
    }

    /**
     * Garbage-free property for complex-column READING: allocated bytes while cursor-walking
     * a multi-cell sstable must not scale with row count. Values are copied through one
     * reused buffer; the cell path scratch grows once (amortized). The ceiling absorbs the
     * known test-env volume residuals (chunk cache, Ref debug tracking — see the compaction
     * allocation gate) measured at these sizes; first run logged the baseline.
     */
    @Test
    public void complexReadAllocationDoesNotScale() throws Exception
    {
        java.lang.management.ThreadMXBean raw = java.lang.management.ManagementFactory.getThreadMXBean();
        org.junit.Assume.assumeTrue(raw instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) raw;
        if (!bean.isThreadAllocatedMemoryEnabled())
            bean.setThreadAllocatedMemoryEnabled(true);

        long small = measureWalk(bean, 6);
        long big = measureWalk(bean, 60);
        long delta = big - small;
        logger.info("complex cursor read allocation: small={}B big={}B delta={}B", small, big, delta);
        assertTrue(String.format("complex read allocation scales with rows: %,dB -> %,dB (delta %,dB)",
                                 small, big, delta),
                   delta <= 64 * 1024); // measured 4,736B at these sizes; trips at ~+2.7B/cell
    }

    private long measureWalk(com.sun.management.ThreadMXBean bean, int partitions) throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < partitions; pk++)
            for (long ck = 0; ck < 100; ck++)
                execute("INSERT INTO %s (pk, ck, m, v) VALUES (?, ?, ?, ?)",
                        pk, ck, map("k1" + ck, ck, "k2" + ck, pk), "v" + ck);
        flush();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        long best = Long.MAX_VALUE;
        long tid = Thread.currentThread().getId();
        for (int i = 0; i < 5; i++)
        {
            long before = bean.getThreadAllocatedBytes(tid);
            leanWalk(sstable);
            long allocated = bean.getThreadAllocatedBytes(tid) - before;
            if (i >= 2)
                best = Math.min(best, allocated);
        }
        return best;
    }

    /** Pure consumption walk: every cell header + path + value, one reused value buffer. */
    private static void leanWalk(SSTableReader sstable) throws Exception
    {
        try (SSTableCursorReader cursor = new SSTableCursorReader(sstable);
             DataOutputBuffer valueSink = new DataOutputBuffer())
        {
            PartitionDescriptor pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            UnfilteredDescriptor rHeader = new UnfilteredDescriptor(sstable.header.clusteringTypes().toArray(AbstractType[]::new));
            byte[] transfer = new byte[4096];

            int state = cursor.readPartitionHeader(pHeader);
            while (state != DONE)
            {
                while (state != PARTITION_END)
                {
                    switch (state)
                    {
                        case STATIC_ROW_START: state = cursor.readStaticRowHeader(rHeader); break;
                        case ROW_START: state = cursor.readRowHeader(rHeader); break;
                        case TOMBSTONE_START: state = cursor.readTombstoneMarker(rHeader); break;
                        default: throw new IllegalStateException("state " + state);
                    }
                    // the active cell cursor is selected by the row-header read above
                    SSTableCursorReader.CellCursor cc = cursor.cellCursor();
                    while (state != UNFILTERED_END && state != PARTITION_END)
                    {
                        if (state == CELL_END) { state = cursor.continueReading(); continue; }
                        if (state != CELL_HEADER_START) break;
                        state = cursor.readCellHeader();
                        if (cc.producedCell && state == CELL_VALUE_START)
                        {
                            valueSink.clear();
                            state = cursor.copyCellValue(valueSink, transfer);
                        }
                    }
                    if (state == UNFILTERED_END)
                        state = cursor.continueReading();
                }
                state = cursor.continueReading();
                if (state != DONE)
                    state = cursor.readPartitionHeader(pHeader);
            }
        }
    }

    @Test
    public void multiCellCollections() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, l list<text>, v text, " +
                    "PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 6; ck++)
            {
                execute("INSERT INTO %s (pk, ck, m, s, l, v) VALUES (?, ?, ?, ?, ?, ?)",
                        pk, ck, map("k" + ck, ck, "x", pk), set((int) ck, 42), list("a" + ck, "b"), "v" + ck);
                // element-level updates: extra cells, no complex deletion
                execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "extra" + ck, ck * 10, pk, ck);
            }
        // full-collection overwrite: complex deletion + fresh cells, merged in the memtable
        execute("UPDATE %s SET m = ? WHERE pk = ? AND ck = ?", map("only", 1L), 1L, 1L);

        assertCursorReadsMatch();
    }

    @Test
    public void deletionOnlyComplexColumns() throws Exception
    {
        // 'zz' sorts after 'a'/'b': a deletion-only zz exercises the trailing no-cell path (-1)
        createTable("CREATE TABLE %s (pk bigint, ck bigint, a text, b bigint, zz map<text, bigint>, " +
                    "PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (long ck = 0; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, a, b, zz) VALUES (?, ?, ?, ?, ?)", 1L, ck, "a" + ck, ck, map("m" + ck, ck));
        // deletion-only zz on some rows (delete the collection, keep the row)
        for (long ck = 0; ck < 8; ck += 2)
            execute("DELETE zz FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        // a row that has ONLY the deletion-only complex column besides the key
        execute("DELETE zz FROM %s WHERE pk = ? AND ck = ?", 1L, 100L);

        assertCursorReadsMatch();
    }

    @Test
    public void multiCellUdtAndStatics() throws Exception
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk bigint, sm map<text, bigint> static, ck bigint, u " + udt + ", v text, " +
                    "PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (long pk = 0; pk < 3; pk++)
        {
            execute("UPDATE %s SET sm[?] = ? WHERE pk = ?", "static" + pk, pk, pk);
            for (long ck = 0; ck < 5; ck++)
            {
                execute("INSERT INTO %s (pk, ck, u, v) VALUES (?, ?, {a: ?, b: ?}, ?)", pk, ck, (int) ck, "f" + ck, "v" + ck);
                execute("UPDATE %s SET u.b = ? WHERE pk = ? AND ck = ?", "updated" + ck, pk, ck); // field-level cell
            }
        }

        assertCursorReadsMatch();
    }

    @Test
    public void sparseRowsWithComplex() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, a text, m map<text, bigint>, z text, " +
                    "PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (long ck = 0; ck < 12; ck++)
        {
            switch ((int) (ck % 4))
            {
                case 0: execute("INSERT INTO %s (pk, ck, a, m, z) VALUES (?, ?, ?, ?, ?)", 1L, ck, "a", map("k", ck), "z"); break;
                case 1: execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "only", ck, 1L, ck); break; // complex only
                case 2: execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1L, ck, "a" + ck); break;      // simple only
                case 3: execute("UPDATE %s SET z = ?, m[?] = ? WHERE pk = ? AND ck = ?", "z" + ck, "k2", ck, 1L, ck); break;
            }
        }

        assertCursorReadsMatch();
    }
}
