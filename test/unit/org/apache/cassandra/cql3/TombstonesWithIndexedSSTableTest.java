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
package org.apache.cassandra.cql3;

import java.util.Random;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.bti.BtiFormat;

public class TombstonesWithIndexedSSTableTest extends CQLTester
{
    @Test
    public void testActiveTombstoneInIndexCached() throws Throwable
    {
        Assume.assumeFalse("BTI format does not use key cache", BtiFormat.isSelected());
        testActiveTombstoneInIndex("ALL");
    }

    @Test
    public void testActiveTombstoneInIndexNotCached() throws Throwable
    {
        testActiveTombstoneInIndex("NONE");
    }

    public void testActiveTombstoneInIndex(String cacheKeys) throws Throwable
    {
        int ROWS = 1000;
        int VALUE_LENGTH = 100;

        createTable("CREATE TABLE %s (k int, t int, v1 text, v2 text, v3 text, v4 text, PRIMARY KEY (k, t)) WITH caching = { 'keys' : '" + cacheKeys + "' }");
        String text = makeRandomString(VALUE_LENGTH);

        // Write a large-enough partition to be indexed.
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s(k, t, v1) VALUES (?, ?, ?) USING TIMESTAMP 1", 0, i, text);
        // Add v2 that should survive part of the deletion we later insert
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s(k, t, v2) VALUES (?, ?, ?) USING TIMESTAMP 3", 0, i, text);
        flush();

        // Now delete parts of this partition, but add enough new data to make sure the deletion spans index blocks
        int minDeleted1 = ROWS/10;
        int maxDeleted1 = 5 * ROWS/10;
        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE k = 0 AND t >= ? AND t < ?", minDeleted1, maxDeleted1);

        // Delete again to make a boundary
        int minDeleted2 = 4 * ROWS/10;
        int maxDeleted2 = 9 * ROWS/10;
        execute("DELETE FROM %s USING TIMESTAMP 4 WHERE k = 0 AND t >= ? AND t < ?", minDeleted2, maxDeleted2);

        // Add v3 surviving that deletion too and also ensuring the two deletions span index blocks
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s(k, t, v3) VALUES (?, ?, ?) USING TIMESTAMP 5", 0, i, text);
        flush();

        // test deletions worked
        verifyExpectedActiveTombstoneRows(ROWS, text, minDeleted1, minDeleted2, maxDeleted2);

        // Test again compacted. This is much easier to pass and doesn't actually test active tombstones in index
        compact();
        verifyExpectedActiveTombstoneRows(ROWS, text, minDeleted1, minDeleted2, maxDeleted2);
    }

    private void verifyExpectedActiveTombstoneRows(int ROWS, String text, int minDeleted1, int minDeleted2, int maxDeleted2) throws Throwable
    {
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v1 = ? ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted1));
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v1 = ? ORDER BY t DESC ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted1));
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v2 = ? ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted2));
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v2 = ? ORDER BY t DESC ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted2));
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v3 = ? ALLOW FILTERING", 0, text), ROWS);
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v3 = ? ORDER BY t DESC ALLOW FILTERING", 0, text), ROWS);
        // test index yields the correct active deletions
        for (int i = 0; i < ROWS; ++i)
        {
            final String v1Expected = i < minDeleted1 || i >= maxDeleted2 ? text : null;
            final String v2Expected = i < minDeleted2 || i >= maxDeleted2 ? text : null;
            assertRows(execute("SELECT v1,v2,v3 FROM %s WHERE k = ? AND t >= ? LIMIT 1", 0, i),
                       row(v1Expected, v2Expected, text));
            assertRows(execute("SELECT v1,v2,v3 FROM %s WHERE k = ? AND t <= ? ORDER BY t DESC LIMIT 1", 0, i),
                       row(v1Expected, v2Expected, text));
        }
    }

    public static String makeRandomString(int length)
    {
        Random random = new Random();
        char[] chars = new char[length];
        for (int i = 0; i < length; ++i)
            chars[i++] = (char) ('a' + random.nextInt('z' - 'a' + 1));
        return new String(chars);
    }
}
