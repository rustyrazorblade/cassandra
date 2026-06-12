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

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.schema.TableMetadata;

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

    /** Task-15 hardening pins: vector and duration are inside the supported surface. */
    @Test
    public void vectorAndDurationSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, vec vector<float, 3>, dur duration, " +
                        "PRIMARY KEY (pk, ck))");
    }

    /** Task-15 hardening pins: nested types (collections of frozen collections, UDTs holding
     *  frozen collections, UDT-in-UDT) are inside the supported surface. */
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
}
