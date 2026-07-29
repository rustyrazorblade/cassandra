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
import java.util.Set;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Pins the cursor compaction support matrix at the metadata level.
 *
 * Each increment of the cursor completion work flips
 * its row here from unsupported to supported. A change in `unsupportedMetadata` semantics
 * that silently widens or narrows the fallback becomes a test failure instead of a silently
 * different code path in production.
 */
public class CursorSupportMatrixTest extends CQLTester
{
    private TableMetadata metadataFor(String createTable)
    {
        createTable(createTable);
        return getCurrentColumnFamilyStore().metadata();
    }

    private void assertSupported(String createTable)
    {
        TableMetadata metadata = metadataFor(createTable);
        assertFalse("expected cursor-supported metadata: " + metadata,
                    CursorCompactor.unsupportedMetadata(metadata));
    }

    private void assertUnsupported(String createTable)
    {
        TableMetadata metadata = metadataFor(createTable);
        assertTrue("expected cursor-UNsupported metadata: " + metadata,
                   CursorCompactor.unsupportedMetadata(metadata));
    }

    @Test
    public void simpleTableSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void staticColumnsSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, s text static, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void noClusteringSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint PRIMARY KEY, v text)");
    }

    /** Frozen collections/tuples/UDTs are single cells: inside the supported surface. */
    @Test
    public void frozenCollectionsSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, " +
                        "m frozen<map<text, bigint>>, l frozen<list<text>>, s frozen<set<int>>, " +
                        "t frozen<tuple<int, text>>, PRIMARY KEY (pk, ck))");
    }

    /** Frozen UDT as the CLUSTERING key: still a single cell, not a regular column. */
    @Test
    public void frozenUdtInClusteringKeySupported()
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertSupported("CREATE TABLE %s (pk bigint, ck frozen<" + udt + ">, v text, PRIMARY KEY (pk, ck))");
    }

    /** Frozen UDT as (part of) the PARTITION key. */
    @Test
    public void frozenUdtInPartitionKeySupported()
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertSupported("CREATE TABLE %s (pk frozen<" + udt + ">, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    /** Frozen collections as (part of) the PRIMARY key, not just as regular columns. */
    @Test
    public void frozenCollectionInPrimaryKeySupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck frozen<list<int>>, v text, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk frozen<set<text>>, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    /** Supported since increment 2 (complex read/merge/write). */
    @Test
    public void multiCellCollectionsSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, l list<text>, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, s set<int>, PRIMARY KEY (pk, ck))");
    }

    /** Supported since increment 2 (complex read/merge/write). */
    @Test
    public void multiCellUdtSupported()
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, u " + udt + ", PRIMARY KEY (pk, ck))");
    }

    /** Vector and duration are inside the supported surface (single-cell types). */
    @Test
    public void vectorAndDurationSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, vec vector<float, 3>, dur duration, " +
                        "PRIMARY KEY (pk, ck))");
    }

    /** Supported since increment 3 (format-specific cursor index writers). */
    @Test
    public void btiFormatSupported() throws Throwable
    {
        org.apache.cassandra.io.sstable.format.SSTableFormat<?, ?> original =
            org.apache.cassandra.config.DatabaseDescriptor.getSelectedSSTableFormat();
        org.apache.cassandra.config.DatabaseDescriptor.setSelectedSSTableFormat("bti");
        try
        {
            assertSupported("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, v text, PRIMARY KEY (pk, ck))");
        }
        finally
        {
            org.apache.cassandra.config.DatabaseDescriptor.setSelectedSSTableFormat(original);
        }
    }

    /** Nested types (collections of frozen collections, UDTs holding frozen collections,
     *  UDT-in-UDT) are inside the supported surface. */
    @Test
    public void nestedTypesSupported()
    {
        String inner = createType("CREATE TYPE %s (xs frozen<list<int>>, name text)");
        String outer = createType("CREATE TYPE %s (i frozen<" + inner + ">, tag text)");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, " +
                        "m map<text, frozen<list<int>>>, u " + inner + ", o " + outer + ", " +
                        "PRIMARY KEY (pk, ck))");
    }

    /** Flipped by increment 5: counter tables compact through the cursor. */
    @Test
    public void countersSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, c counter, s counter static, PRIMARY KEY (pk, ck))");
    }

    /** Out of scope for the current plan: indexes keep the iterator path. */
    @Test
    public void secondaryIndexUnsupported()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s (v)");
        assertTrue("expected index to disqualify cursor compaction",
                   CursorCompactor.unsupportedMetadata(getCurrentColumnFamilyStore().metadata()));
    }

    /**
     * A DROPPED non-frozen collection is gone from the schema but still present in the header of
     * every sstable written before the drop, still multi-cell. The metadata-level check cannot see
     * it, so the gate has to screen the input headers too — otherwise the cell cursor, which has no
     * complex framing, misparses those rows.
     */
    @Test
    public void droppedCollectionUnsupportedFromHeaders() throws Exception
    {
        assertDroppedCollectionUnsupported("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, " +
                                           "v text, PRIMARY KEY (pk, ck))",
                                           "INSERT INTO %s (pk, ck, m, v) VALUES (1, 1, {'a':'b'}, 'x')",
                                           false);
    }

    /**
     * Same hole via a dropped STATIC collection, which lands in header.columns(true) only — so a
     * check that inspected regular columns alone would pass this test's regular-column sibling and
     * still admit the misparse.
     */
    @Test
    public void droppedStaticCollectionUnsupportedFromHeaders() throws Exception
    {
        assertDroppedCollectionUnsupported("CREATE TABLE %s (pk bigint, ck bigint, " +
                                           "m map<text, text> static, v text, PRIMARY KEY (pk, ck))",
                                           "INSERT INTO %s (pk, ck, m, v) VALUES (1, 1, {'a':'b'}, 'x')",
                                           true);
    }

    private void assertDroppedCollectionUnsupported(String ddl, String insert, boolean isStatic) throws Exception
    {
        // cursor compaction only supports BIG output, so under a non-BIG format isSupported is
        // false for every table and the assertion below could not tell the header check apart
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        createTable(ddl);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute(insert);
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();

        execute("ALTER TABLE %s DROP m");

        // the schema-level check is satisfied: the collection is no longer a current column
        assertFalse("dropped collection should leave the metadata check satisfied, which is exactly " +
                    "why the header check is needed",
                    CursorCompactor.unsupportedMetadata(cfs.metadata()));

        // ... but the pre-drop sstable still carries it, multi-cell, in its own header
        boolean anyHeaderStillHasIt = false;
        for (SSTableReader reader : cfs.getLiveSSTables())
            for (ColumnMetadata column : reader.header.columns(isStatic))
                anyHeaderStillHasIt |= column.isComplex();
        assertTrue("expected a pre-drop sstable header to still list the collection as multi-cell",
                   anyHeaderStillHasIt);

        assertFalse("cursor compaction must refuse a table whose input headers still carry a " +
                    "multi-cell column; the cursor cannot parse complex framing",
                    isSupportedNow(cfs));

        // Positive control on a separate table: isSupportedNow returns true for an equivalent table
        // that never had a collection, so the rejection above is attributable to the dropped column
        // and not to the harness, the format or any other gate. (The table under test cannot serve
        // as its own pre-drop control: while the collection is still live the SCHEMA check rejects
        // it, which is the very check the drop defeats.)
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore plain = getCurrentColumnFamilyStore();
        plain.disableAutoCompaction();
        // same shape as the table under test after its drop — same surviving columns, same two
        // inserts and two flushes — so the only difference left is the dropped collection itself
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();
        assertTrue("expected a plain table with no dropped collection to be cursor-supported",
                   isSupportedNow(plain));
    }

    private boolean isSupportedNow(ColumnFamilyStore cfs) throws Exception
    {
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        try (CompactionController controller = new CompactionController(cfs, inputs, FBUtilities.nowInSeconds());
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            return CursorCompactor.isSupported(scanners, controller);
        }
    }
}
