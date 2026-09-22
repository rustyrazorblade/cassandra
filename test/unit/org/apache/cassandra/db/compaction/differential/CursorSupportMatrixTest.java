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
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.repair.ValidationCompactionController;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Pins the cursor compaction support matrix: which schemas, headers and compactions the gate
 *  accepts or refuses. */
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

    @Test
    public void frozenUdtInPartitionKeySupported()
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertSupported("CREATE TABLE %s (pk frozen<" + udt + ">, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void frozenCollectionInPrimaryKeySupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck frozen<list<int>>, v text, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk frozen<set<text>>, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    /** The cursor path can read, merge and write a multi-cell collection. */
    @Test
    public void multiCellCollectionsSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, l list<text>, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, s set<int>, PRIMARY KEY (pk, ck))");
    }

    /** The cursor path can read, merge and write a multi-cell UDT. */
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

    /** BTI output is inside the supported surface, asserted through the gate's production calls. */
    @Test
    public void btiFormatSupported() throws Exception
    {
        SSTableFormat<?, ?> original = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(BtiFormat.NAME);
        try
        {
            assertTrue("the BTI format must report cursor compaction support",
                       DatabaseDescriptor.getSelectedSSTableFormat().supportsCursorCompaction());

            ColumnFamilyStore cfs =
                twoSSTableTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, v text, " +
                                "PRIMARY KEY (pk, ck))",
                                "INSERT INTO %s (pk, ck, m, v) VALUES (1, 1, {'a': 1}, 'x')",
                                "INSERT INTO %s (pk, ck, m, v) VALUES (1, 2, {'b': 2}, 'y')");

            // the inputs must be in the format under test
            for (SSTableReader reader : cfs.getLiveSSTables())
                assertTrue("expected BTI input sstables, got " + reader.descriptor.version.format.name(),
                           BtiFormat.is(reader.descriptor.version.format));

            assertTrue("cursor compaction must accept a BTI table", isSupportedNow(cfs));
        }
        finally
        {
            DatabaseDescriptor.setSelectedSSTableFormat(original);
        }
    }

    /** A selected format that does not support cursor compaction is refused. */
    @Test
    public void formatWithoutCursorSupportUnsupported() throws Exception
    {
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        ColumnFamilyStore cfs =
            twoSSTableTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))",
                            "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')",
                            "INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");

        // control: the same table is supported under the real format
        assertTrue("expected a plain two-sstable table to be cursor-supported", isSupportedNow(cfs));

        SSTableFormat<?, ?> original = DatabaseDescriptor.getSelectedSSTableFormat();
        SSTableFormat<?, ?> noCursorSupport = Mockito.mock(SSTableFormat.class, Mockito.CALLS_REAL_METHODS);
        assertFalse("the stand-in must report no cursor compaction support",
                    noCursorSupport.supportsCursorCompaction());

        DatabaseDescriptor.setSelectedSSTableFormat(noCursorSupport);
        try
        {
            assertFalse("cursor compaction must refuse a table whose selected output format does " +
                        "not support it",
                        isSupportedNow(cfs));
        }
        finally
        {
            DatabaseDescriptor.setSelectedSSTableFormat(original);
        }

        // the table is supported again once the real format is back
        assertTrue("expected the table to be cursor-supported again under the real format",
                   isSupportedNow(cfs));
    }

    /** Creates {@code ddl} with auto-compaction off and flushes each insert into its own sstable. */
    private ColumnFamilyStore twoSSTableTable(String ddl, String firstInsert, String secondInsert)
    {
        createTable(ddl);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute(firstInsert);
        flush();
        execute(secondInsert);
        flush();
        assertEquals("expected one sstable per flush", 2, cfs.getLiveSSTables().size());
        return cfs;
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

    /** Counter tables compact through the cursor. */
    @Test
    public void countersSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, c counter, s counter static, PRIMARY KEY (pk, ck))");
    }

    /** An indexed table keeps the iterator path. */
    @Test
    public void secondaryIndexUnsupported()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s (v)");
        assertTrue("expected index to disqualify cursor compaction",
                   CursorCompactor.unsupportedMetadata(getCurrentColumnFamilyStore().metadata()));
    }

    /** Cursor compaction is refused while any key ignores gc grace. */
    @Test
    public void ignoreGcGraceForAnyKeyUnsupported() throws Throwable
    {
        // BIG required: under another format isSupported is false for every table
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // pk 1's row deletion is what the force compaction must purge; pk 2 keeps the output non-empty
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 'y')");
        flush();
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 1");
        flush();

        // control: the same table with nothing ignoring gc grace is supported
        assertEquals("expected one sstable per flush", 2, cfs.getLiveSSTables().size());
        assertFalse("no key should ignore gc grace outside a force compaction",
                    cfs.shouldIgnoreGcGraceForAnyKey());
        assertTrue("expected a plain two-sstable table to be cursor-supported",
                   isSupportedNow(cfs));

        AtomicReference<Boolean> ignoredGcGraceInside = new AtomicReference<>();
        AtomicReference<Boolean> supportedInside = new AtomicReference<>();
        AtomicReference<Throwable> observerFailure = new AtomicReference<>();
        INotificationConsumer observer = (notification, sender) ->
        {
            if (!(notification instanceof SSTableListChangedNotification))
                return;
            // record the first sstable swap only, and the key set before anything else
            if (!ignoredGcGraceInside.compareAndSet(null, cfs.shouldIgnoreGcGraceForAnyKey()))
                return;
            try
            {
                // on the commit path the live set is the compaction's output, so scanners over it are safe
                supportedInside.set(isSupportedNow(cfs));
            }
            catch (Throwable t)
            {
                // carry the throw out and rethrow it on the calling thread
                observerFailure.set(t);
            }
        };

        cfs.getTracker().subscribe(observer);
        try
        {
            cfs.forceCompactionKeysIgnoringGcGrace("1");
        }
        finally
        {
            cfs.getTracker().unsubscribe(observer);
        }

        if (observerFailure.get() != null)
            throw observerFailure.get();

        // the force compaction must have run with the key set populated
        assertNotNull("expected the force compaction to change the sstable list while the observer was subscribed",
                      ignoredGcGraceInside.get());
        assertTrue("expected the ignore-gc-grace key set to be populated during the force compaction",
                   ignoredGcGraceInside.get());

        assertNotNull("expected the observer to have evaluated the gate", supportedInside.get());
        assertFalse("cursor compaction must refuse a table while any key ignores gc grace: the iterator " +
                    "suppresses row-level purging wholesale there and a streaming cursor cannot",
                    supportedInside.get());

        // the table is supported again once the force compaction has cleared the set
        assertFalse("expected the key set to be cleared when the force compaction returned",
                    cfs.shouldIgnoreGcGraceForAnyKey());
        assertTrue("expected the table to be cursor-supported again after the force compaction",
                   isSupportedNow(cfs));
    }

    /** A dropped non-frozen collection stays gated: the header check must screen it though the
     *  schema no longer lists it. */
    @Test
    public void droppedCollectionUnsupportedFromHeaders() throws Exception
    {
        assertDroppedCollectionUnsupported("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, " +
                                           "v text, PRIMARY KEY (pk, ck))",
                                           "INSERT INTO %s (pk, ck, m, v) VALUES (1, 1, {'a':'b'}, 'x')",
                                           false);
    }

    /** The same, through a dropped STATIC collection, which lands in the static header columns only. */
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
        // BIG required: under another format isSupported is false for every table
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        createTable(ddl);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute(insert);
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();

        execute("ALTER TABLE %s DROP m");

        assertFalse("a dropped collection leaves the metadata check satisfied, so the header check is needed",
                    CursorCompactor.unsupportedMetadata(cfs.metadata()));

        boolean anyHeaderStillHasIt = false;
        for (SSTableReader reader : cfs.getLiveSSTables())
            for (ColumnMetadata column : reader.header.columns(isStatic))
                anyHeaderStillHasIt |= column.isComplex();
        assertTrue("expected a pre-drop sstable header to still list the collection as multi-cell",
                   anyHeaderStillHasIt);

        assertFalse("cursor compaction must refuse a table whose input headers still carry a " +
                    "dropped multi-cell column",
                    isSupportedNow(cfs));

        // positive control on a separate table: an equivalent table that never had a collection is
        // supported, so the rejection above is attributable to the dropped column alone
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore plain = getCurrentColumnFamilyStore();
        plain.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();
        assertTrue("expected a plain table with no dropped collection to be cursor-supported",
                   isSupportedNow(plain));
    }

    /** A dropped counter column stays gated: the cursor path has no counter merge. */
    @Test
    public void droppedCounterUnsupportedFromHeaders() throws Exception
    {
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        // one counter only, so the drop leaves no counter in the schema for unsupportedSchema to catch
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("UPDATE %s SET c = c + 1 WHERE pk = 1 AND ck = 1");
        flush();
        execute("UPDATE %s SET c = c + 1 WHERE pk = 1 AND ck = 2");
        flush();

        execute("ALTER TABLE %s DROP c");

        assertFalse("a dropped counter leaves the metadata check satisfied, so the header check is needed",
                    CursorCompactor.unsupportedMetadata(cfs.metadata()));

        boolean anyHeaderStillHasIt = false;
        for (SSTableReader reader : cfs.getLiveSSTables())
            for (ColumnMetadata column : reader.header.columns(false))
                anyHeaderStillHasIt |= cfs.metadata().getColumn(column.name) == null;
        assertTrue("expected a pre-drop sstable header to still list the dropped counter",
                   anyHeaderStillHasIt);

        assertFalse("cursor compaction must refuse a table whose input headers still carry a " +
                    "counter column; the cursor has no counter merge",
                    isSupportedNow(cfs));
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

    /**
     * The validation gate {@code isValidationSupported} must refuse the same dropped-collection
     * input the write-path gate does; see {@link #droppedCollectionUnsupportedFromHeaders}.  The
     * cursor reader cannot parse a dropped complex column's header framing on either path.
     */
    @Test
    public void droppedCollectionUnsupportedFromHeadersForValidation() throws Exception
    {
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, ck, m, v) VALUES (1, 1, {'a':'b'}, 'x')");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();

        execute("ALTER TABLE %s DROP m");

        assertFalse("cursor validation must refuse a table whose headers still carry a multi-cell column",
                    isValidationSupportedNow(cfs));

        // Positive control: an equivalent table that never had the collection, so the rejection
        // above is attributable to the dropped column alone.
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore plain = getCurrentColumnFamilyStore();
        plain.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();
        assertTrue("expected a plain table with no dropped collection to be cursor-validation-supported",
                   isValidationSupportedNow(plain));
    }

    private boolean isValidationSupportedNow(ColumnFamilyStore cfs) throws Exception
    {
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        try (ValidationCompactionController controller = new ValidationCompactionController(cfs, FBUtilities.nowInSeconds()))
        {
            return CursorCompactor.isValidationSupported(inputs, controller);
        }
    }

    /**
     * Materialized views pass this metadata-level gate, so regular cursor compaction accepts them:
     * modern view maintenance no longer produces a shadowable row deletion (since CASSANDRA-13409,
     * {@code Row.Deletion.shadowable(...)} has no remaining caller).  Cursor validation is more
     * conservative about the same risk; that check lives in
     * {@link CursorCompactor#isValidationSupported}, not in this shared metadata-only method.
     */
    @Test
    public void materializedViewSupported()
    {
        requireNetwork();
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1 FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        TableMetadata viewMetadata = getColumnFamilyStore(KEYSPACE, view).metadata();
        assertFalse("expected materialized view to be cursor-compaction-supported: " + viewMetadata,
                    CursorCompactor.unsupportedMetadata(viewMetadata));
    }
}
