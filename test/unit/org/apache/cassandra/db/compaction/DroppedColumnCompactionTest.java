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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

/**
 * Covers a dropped column whose cells carry a timestamp above the recorded drop time.  Such a cell used to survive the
 * read.  For a multi-cell column it then broke the merge with a {@code NullPointerException}, which stopped every later
 * compaction of the table.  See CASSANDRA-21607.
 *
 * <p>The read now skips a dropped column whole, so the data is gone and a re-add does not bring it back.
 */
public class DroppedColumnCompactionTest extends CQLTester
{
    /** The year 2100, in microseconds.  Any timestamp above the drop time has the same effect. */
    private static final long ABOVE_DROP_TIME = 4102444800000000L;

    @Test
    public void droppedCollectionCompacts() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text,bigint>, PRIMARY KEY (pk, ck))");
        disableCompaction();

        execute("UPDATE %s USING TIMESTAMP ? SET m = m + {'a':1} WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME);
        flush();
        execute("UPDATE %s USING TIMESTAMP ? SET m = m + {'a':2} WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME + 1);
        flush();

        alterTable("ALTER TABLE %s DROP m");
        compact();

        assertEmpty(execute("SELECT * FROM %s"));
    }

    @Test
    public void droppedCollectionKeepsTheRestOfTheRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text,bigint>, PRIMARY KEY (pk, ck))");
        disableCompaction();

        execute("UPDATE %s USING TIMESTAMP ? SET v1 = 7, m = m + {'a':1} WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME);
        flush();
        execute("UPDATE %s USING TIMESTAMP ? SET m = m + {'a':2} WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME + 1);
        flush();

        alterTable("ALTER TABLE %s DROP m");
        compact();

        assertRows(execute("SELECT pk, ck, v1 FROM %s"), row(0L, 0L, 7L));
    }

    @Test
    public void droppedStaticCollectionCompacts() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, m map<text,bigint> static, PRIMARY KEY (pk, ck))");
        disableCompaction();

        execute("UPDATE %s USING TIMESTAMP ? SET s1 = 7, m = m + {'a':1} WHERE pk = 0", ABOVE_DROP_TIME);
        flush();
        execute("UPDATE %s USING TIMESTAMP ? SET m = m + {'a':2} WHERE pk = 0", ABOVE_DROP_TIME + 1);
        flush();

        alterTable("ALTER TABLE %s DROP m");
        compact();

        assertRows(execute("SELECT pk, s1 FROM %s"), row(0L, 7L));
    }

    @Test
    public void droppedSimpleColumnCompacts() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 bigint, PRIMARY KEY (pk, ck))");
        disableCompaction();

        execute("UPDATE %s USING TIMESTAMP ? SET v1 = 7, v2 = 1 WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME);
        flush();
        execute("UPDATE %s USING TIMESTAMP ? SET v2 = 2 WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME + 1);
        flush();

        alterTable("ALTER TABLE %s DROP v2");
        compact();

        assertRows(execute("SELECT pk, ck, v1 FROM %s"), row(0L, 0L, 7L));
    }

    /**
     * A dropped collection with a surviving complex deletion, and no cell at all, reaches the merge the same way.
     */
    @Test
    public void droppedCollectionWithComplexDeletionCompacts() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text,bigint>, PRIMARY KEY (pk, ck))");
        disableCompaction();

        execute("UPDATE %s USING TIMESTAMP ? SET v1 = 7 WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME);
        execute("UPDATE %s USING TIMESTAMP ? SET m = {'a':1} WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME);
        flush();
        execute("UPDATE %s USING TIMESTAMP ? SET m = {'b':2} WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME + 1);
        flush();

        alterTable("ALTER TABLE %s DROP m");
        compact();

        assertRows(execute("SELECT pk, ck, v1 FROM %s"), row(0L, 0L, 7L));
    }

    /**
     * The read discards the dropped column, so the data is gone before any re-add.  A write made after the re-add still
     * survives.
     */
    @Test
    public void reAddedCollectionDoesNotResurrectDroppedData() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text,bigint>, PRIMARY KEY (pk, ck))");
        disableCompaction();

        execute("UPDATE %s USING TIMESTAMP ? SET v1 = 7, m = m + {'a':1} WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME);
        flush();
        execute("UPDATE %s USING TIMESTAMP ? SET m = m + {'a':2} WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME + 1);
        flush();

        alterTable("ALTER TABLE %s DROP m");
        compact();
        alterTable("ALTER TABLE %s ADD m map<text,bigint>");

        assertRows(execute("SELECT pk, ck, v1, m FROM %s"), row(0L, 0L, 7L, null));

        execute("UPDATE %s USING TIMESTAMP ? SET m = m + {'b':3} WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME + 2);
        assertRows(execute("SELECT pk, ck, m FROM %s"), row(0L, 0L, map("b", 3L)));
    }

    @Test
    public void reAddedSimpleColumnDoesNotResurrectDroppedData() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 bigint, PRIMARY KEY (pk, ck))");
        disableCompaction();

        execute("UPDATE %s USING TIMESTAMP ? SET v1 = 7, v2 = 1 WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME);
        flush();
        execute("UPDATE %s USING TIMESTAMP ? SET v2 = 2 WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME + 1);
        flush();

        alterTable("ALTER TABLE %s DROP v2");
        compact();
        alterTable("ALTER TABLE %s ADD v2 bigint");

        assertRows(execute("SELECT pk, ck, v1, v2 FROM %s"), row(0L, 0L, 7L, null));

        execute("UPDATE %s USING TIMESTAMP ? SET v2 = 3 WHERE pk = 0 AND ck = 0", ABOVE_DROP_TIME + 2);
        assertRows(execute("SELECT pk, ck, v2 FROM %s"), row(0L, 0L, 3L));
    }
}
