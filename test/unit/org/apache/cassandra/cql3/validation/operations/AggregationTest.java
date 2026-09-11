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
package org.apache.cassandra.cql3.validation.operations;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationTest extends CQLTester
{
    @Test
    public void testFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c double, d decimal, e smallint, f tinyint, primary key (a, b))");

        // Test with empty table
        assertColumnNames(execute("SELECT COUNT(*) FROM %s"), "count");
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(0L));
        assertColumnNames(execute("SELECT max(b), min(b), sum(b), avg(b)," +
                                  "max(c), sum(c), avg(c)," +
                                  "sum(d), avg(d)," +
                                  "max(e), min(e), sum(e), avg(e)," +
                                  "max(f), min(f), sum(f), avg(f) FROM %s"),
                          "system.max(b)", "system.min(b)", "system.sum(b)", "system.avg(b)",
                          "system.max(c)", "system.sum(c)", "system.avg(c)",
                          "system.sum(d)", "system.avg(d)",
                          "system.max(e)", "system.min(e)", "system.sum(e)", "system.avg(e)",
                          "system.max(f)", "system.min(f)", "system.sum(f)", "system.avg(f)");
        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b)," +
                           "max(c), sum(c), avg(c)," +
                           "sum(d), avg(d)," +
                           "max(e), min(e), sum(e), avg(e)," +
                           "max(f), min(f), sum(f), avg(f) FROM %s"),
                   row(null, null, 0, 0, null, 0.0, 0.0, new BigDecimal("0"), new BigDecimal("0"),
                       null, null, (short)0, (short)0,
                       null, null, (byte)0, (byte)0));

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 11.5, 11.5, 1, 1)");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 2, 9.5, 1.5, 2, 2)");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 3, 9.0, 2.0, 3, 3)");

        assertRows(execute("SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c), sum(d), avg(d)," +
                           "max(e), min(e), sum(e), avg(e)," +
                           "max(f), min(f), sum(f), avg(f)" +
                           " FROM %s"),
                   row(3, 1, 6, 2, 11.5, 30.0, 10.0, new BigDecimal("15.0"), new BigDecimal("5.0"),
                       (short)3, (short)1, (short)6, (short)2,
                       (byte)3, (byte)1, (byte)6, (byte)2));

        execute("INSERT INTO %s (a, b, d) VALUES (1, 5, 1.0)");
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(4L));
        assertRows(execute("SELECT COUNT(1) FROM %s"), row(4L));
        assertRows(execute("SELECT COUNT(b), count(c), count(e), count(f) FROM %s"), row(4L, 3L, 3L, 3L));
        // Makes sure that LIMIT does not affect the result of aggregates
        assertRows(execute("SELECT COUNT(b), count(c), count(e), count(f) FROM %s LIMIT 2"), row(4L, 3L, 3L, 3L));
        assertRows(execute("SELECT COUNT(b), count(c), count(e), count(f) FROM %s WHERE a = 1 LIMIT 2"),
                   row(4L, 3L, 3L, 3L));
        assertRows(execute("SELECT AVG(CAST(b AS double)) FROM %s"), row(11.0/4));
    }

    @Test
    public void testCountStarFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c double, primary key (a, b))");

        // Test with empty table
        assertColumnNames(execute("SELECT COUNT(*) FROM %s"), "count");
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(0L));
        assertColumnNames(execute("SELECT COUNT(1) FROM %s"), "count");
        assertRows(execute("SELECT COUNT(1) FROM %s"), row(0L));
        assertColumnNames(execute("SELECT COUNT(*), COUNT(*) FROM %s"), "count", "count");
        assertRows(execute("SELECT COUNT(*), COUNT(*) FROM %s"), row(0L, 0L));

        // Test with alias
        assertColumnNames(execute("SELECT COUNT(*) as myCount FROM %s"), "mycount");
        assertRows(execute("SELECT COUNT(*) as myCount FROM %s"), row(0L));
        assertColumnNames(execute("SELECT COUNT(1) as myCount FROM %s"), "mycount");
        assertRows(execute("SELECT COUNT(1) as myCount FROM %s"), row(0L));

        // Test with other aggregates
        assertColumnNames(execute("SELECT COUNT(*), max(b), b FROM %s"), "count", "system.max(b)", "b");
        assertRows(execute("SELECT COUNT(*), max(b), b  FROM %s"), row(0L, null, null));
        assertColumnNames(execute("SELECT COUNT(1), max(b), b FROM %s"), "count", "system.max(b)", "b");
        assertRows(execute("SELECT COUNT(1), max(b), b  FROM %s"), row(0L, null, null));

        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 11.5)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 9.5)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 9.0)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 5, 1.0)");

        assertRows(execute("SELECT COUNT(*) FROM %s"), row(4L));
        assertRows(execute("SELECT COUNT(1) FROM %s"), row(4L));
        assertRows(execute("SELECT max(b), b, COUNT(*) FROM %s"), row(5, 1, 4L));
        assertRows(execute("SELECT max(b), COUNT(1), b FROM %s"), row(5, 4L, 1));
        // Makes sure that LIMIT does not affect the result of aggregates
        assertRows(execute("SELECT max(b), COUNT(1), b FROM %s LIMIT 2"), row(5, 4L, 1));
        assertRows(execute("SELECT max(b), COUNT(1), b FROM %s WHERE a = 1 LIMIT 2"), row(5, 4L, 1));
    }

    @Test
    public void testMaxAggregationDescending()
    {
        createTable("CREATE TABLE %s (a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)");

        execute("INSERT INTO %s (a, b) VALUES (1, 1000)");
        execute("INSERT INTO %s (a, b) VALUES (1, 100)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");

        assertRows(execute("SELECT count(b), max(b) as max FROM %s WHERE a = 1"),
                   row(3L, 1000));

        execute("INSERT INTO %s (a, b) VALUES (2, 4000)");
        execute("INSERT INTO %s (a, b) VALUES (3, 100)");
        execute("INSERT INTO %s (a, b) VALUES (4, 0)");

        assertRows(execute("SELECT count(b), max(b) as max FROM %s"),
                   row(6L, 4000));
    }

    @Test
    public void testMinAggregationDescending()
    {
        createTable("CREATE TABLE %s (a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)");

        execute("INSERT INTO %s (a, b) VALUES (1, 1000)");
        execute("INSERT INTO %s (a, b) VALUES (1, 100)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");

        assertRows(execute("SELECT count(b), min(b) as min FROM %s WHERE a = 1"),
                   row(3L, 1));

        execute("INSERT INTO %s (a, b) VALUES (2, 4000)");
        execute("INSERT INTO %s (a, b) VALUES (3, 100)");
        execute("INSERT INTO %s (a, b) VALUES (4, 0)");

        assertRows(execute("SELECT count(b), min(b) as min FROM %s"),
                   row(6L, 0));
    }

    @Test
    public void testMaxAggregationAscending()
    {
        createTable("CREATE TABLE %s (a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b ASC)");

        execute("INSERT INTO %s (a, b) VALUES (1, 1000)");
        execute("INSERT INTO %s (a, b) VALUES (1, 100)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");

        assertRows(execute("SELECT count(b), max(b) as max FROM %s WHERE a = 1"),
                   row(3L, 1000));

        execute("INSERT INTO %s (a, b) VALUES (2, 4000)");
        execute("INSERT INTO %s (a, b) VALUES (3, 100)");
        execute("INSERT INTO %s (a, b) VALUES (4, 5)");

        assertRows(execute("SELECT count(b), max(b) as max FROM %s"),
                   row(6L, 4000));
    }

    @Test
    public void testMinAggregationAscending()
    {
        createTable("CREATE TABLE %s (a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b ASC)");

        execute("INSERT INTO %s (a, b) VALUES (1, 1000)");
        execute("INSERT INTO %s (a, b) VALUES (1, 100)");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");

        assertRows(execute("SELECT count(b), min(b) as min FROM %s WHERE a = 1"),
                   row(3L, 1));

        execute("INSERT INTO %s (a, b) VALUES (2, 4000)");
        execute("INSERT INTO %s (a, b) VALUES (3, 100)");
        execute("INSERT INTO %s (a, b) VALUES (4, 0)");

        assertRows(execute("SELECT count(b), min(b) as min FROM %s"),
                   row(6L, 0));
    }

    @Test
    public void testAggregateWithColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, primary key (a, b))");

        // Test with empty table
        assertColumnNames(execute("SELECT count(b), max(b) as max, b, c as first FROM %s"),
                          "system.count(b)", "max", "b", "first");
        assertRows(execute("SELECT count(b), max(b) as max, b, c as first FROM %s"),
                           row(0L, null, null, null));

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, null)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 4, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 8, 12)");

        assertRows(execute("SELECT count(b), max(b) as max, b, c as first FROM %s"),
                   row(3L, 8, 2, null));
    }

    @Test
    public void testAggregateOnCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b counter, primary key (a))");

        // Test with empty table
        assertColumnNames(execute("SELECT count(b), max(b) as max, b FROM %s"),
                          "system.count(b)", "max", "b");
        assertRows(execute("SELECT count(b), max(b) as max, b FROM %s"),
                   row(0L, null, null));

        execute("UPDATE %s SET b = b + 1 WHERE a = 1");
        execute("UPDATE %s SET b = b + 1 WHERE a = 1");

        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 2L, 2L, 2L, 2L));
        flush();
        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 2L, 2L, 2L, 2L));

        execute("UPDATE %s SET b = b + 2 WHERE a = 1");

        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 4L, 4L, 4L, 4L));

        execute("UPDATE %s SET b = b - 2 WHERE a = 1");

        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 2L, 2L, 2L, 2L));
        flush();
        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1L, 2L, 2L, 2L, 2L));

        execute("UPDATE %s SET b = b + 1 WHERE a = 2");
        execute("UPDATE %s SET b = b + 1 WHERE a = 2");
        execute("UPDATE %s SET b = b + 2 WHERE a = 2");

        assertRows(execute("SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(2L, 4L, 2L, 3L, 6L));
    }

    @Test
    public void testAggregateWithSets() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<int>, fs frozen<set<int>>)");

        // Test with empty table
        String select = "SELECT count(s), count(fs), min(s), min(fs), max(s), max(fs) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs,
                          "system.count(s)", "system.count(fs)",
                          "system.min(s)", "system.min(fs)",
                          "system.max(s)", "system.max(fs)");
        assertRows(rs, row(0L, 0L, null, null, null, null));

        // Test with not-empty table
        execute("INSERT INTO %s (k, s, fs) VALUES (1, {1, 2}, {1, 2})");
        execute("INSERT INTO %s (k, s, fs) VALUES (2, {1, 2, 3}, {1, 2, 3})");
        execute("INSERT INTO %s (k, s, fs) VALUES (3, {2, 1}, {2, 1})");
        assertRows(execute(select), row(3L, 3L, set(1, 2), set(1, 2), set(1, 2, 3), set(1, 2, 3)));
    }

    @Test
    public void testAggregateWithLists() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>, fl frozen<list<int>>)");

        // Test with empty table
        String select = "SELECT count(l), count(fl), min(l), min(fl), max(l), max(fl) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs,
                          "system.count(l)", "system.count(fl)",
                          "system.min(l)", "system.min(fl)",
                          "system.max(l)", "system.max(fl)");
        assertRows(rs, row(0L, 0L, null, null, null, null));

        // Test with not-empty table
        execute("INSERT INTO %s (k, l, fl) VALUES (1, [1, 2], [1, 2])");
        execute("INSERT INTO %s (k, l, fl) VALUES (2, [1, 2, 3], [1, 2, 3])");
        execute("INSERT INTO %s (k, l, fl) VALUES (3, [2, 1], [2, 1])");
        assertRows(execute(select),
                   row(3L, 3L, list(1, 2), list(1, 2), list(2, 1), list(2, 1)));
    }

    @Test
    public void testAggregateWithMaps() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<int, int>, fm frozen<map<int, int>>)");

        // Test with empty table
        String select = "SELECT count(m), count(fm), min(m), min(fm), max(m), max(fm) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs,
                          "system.count(m)", "system.count(fm)",
                          "system.min(m)", "system.min(fm)",
                          "system.max(m)", "system.max(fm)");
        assertRows(rs, row(0L, 0L, null, null, null, null));

        // Test with not-empty table
        execute("INSERT INTO %s (k, m, fm) VALUES (1, {1:10, 2:20}, {1:10, 2:20})");
        execute("INSERT INTO %s (k, m, fm) VALUES (2, {1:10, 2:20, 3:30}, {1:10, 2:20, 3:30})");
        execute("INSERT INTO %s (k, m, fm) VALUES (3, {2:20, 1:10}, {2:20, 1:10})");
        assertRows(execute(select),
                   row(3L, 3L,
                       map(1, 10, 2, 20), map(1, 10, 2, 20),
                       map(1, 10, 2, 20, 3, 30), map(1, 10, 2, 20, 3, 30)));
    }

    @Test
    public void testAggregateWithTuples() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t tuple<int, text, boolean>)");

        // Test with empty table
        String select = "SELECT count(t), min(t), max(t) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs, "system.count(t)", "system.min(t)", "system.max(t)");
        assertRows(rs, row(0L, null, null));

        // Test with not-empty table
        execute("INSERT INTO %s (k, t) VALUES (1, (1, 'a', false))");
        execute("INSERT INTO %s (k, t) VALUES (2, (2, 'b', true))");
        execute("INSERT INTO %s (k, t) VALUES (3, (3, null, true))");
        assertRows(execute(select), row(3L, tuple(1, "a", false), tuple(3, null, true)));
    }

    @Test
    public void testAggregateWithUDTs() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (x int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, u frozen<" + udt + ">, fu frozen<" + udt + ">)");

        // Test with empty table
        String select = "SELECT count(u), count(fu), min(u), min(fu), max(u), max(fu) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs,
                          "system.count(u)", "system.count(fu)",
                          "system.min(u)", "system.min(fu)",
                          "system.max(u)", "system.max(fu)");
        assertRows(rs, row(0L, 0L, null, null, null, null));

        // Test with not-empty table
        execute("INSERT INTO %s (k, u, fu) VALUES (1, {x: 2}, null)");
        execute("INSERT INTO %s (k, u, fu) VALUES (2, {x: 4}, {x: 6})");
        execute("INSERT INTO %s (k, u, fu) VALUES (3, null, {x: 8})");
        assertRows(execute(select),
                   row(2L, 2L, userType("x", 2), userType("x", 6), userType("x", 4), userType("x", 8)));
    }

    @Test
    public void testAggregateWithUdtFields() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (x int)");
        createTable("CREATE TABLE %s (a int primary key, b frozen<" + myType + ">, c frozen<" + myType + ">)");

        // Test with empty table
        assertColumnNames(execute("SELECT count(b.x), max(b.x) as max, b.x, c.x as first FROM %s"),
                          "system.count(b.x)", "max", "b.x", "first");
        assertRows(execute("SELECT count(b.x), max(b.x) as max, b.x, c.x as first FROM %s"),
                           row(0L, null, null, null));

        execute("INSERT INTO %s (a, b, c) VALUES (1, {x:2}, null)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, {x:4}, {x:6})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, {x:8}, {x:12})");

        assertRows(execute("SELECT count(b.x), max(b.x) as max, b.x, c.x as first FROM %s"),
                   row(3L, 8, 2, null));

        assertRows(execute("SELECT count(b), min(b).x, max(b).x, count(c), min(c).x, max(c).x FROM %s"),
                   row(3L, 2, 8, 2L, 6, 12));
    }

    @Test
    public void testAggregateWithWriteTimeOrTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int, c int)");

        // Test with empty table
        assertColumnNames(execute("SELECT count(writetime(b)), min(ttl(b)) as min, writetime(b), ttl(c) as first FROM %s"),
                          "system.count(writetime(b))", "min", "writetime(b)", "first");
        assertRows(execute("SELECT count(writetime(b)), min(ttl(b)) as min, writetime(b), ttl(c) as first FROM %s"),
                           row(0L, null, null, null));

        long today = System.currentTimeMillis() * 1000;
        long yesterday = today - (DateUtils.MILLIS_PER_DAY * 1000);

        final int secondsPerMinute = 60;
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, null) USING TTL " + (20 * secondsPerMinute));
        execute("INSERT INTO %s (a, b, c) VALUES (2, 4, 6) USING TTL " + (10 * secondsPerMinute));
        execute("INSERT INTO %s (a, b, c) VALUES (4, 8, 12) USING TIMESTAMP " + yesterday );

        assertRows(execute("SELECT count(writetime(b)), count(ttl(b)) FROM %s"),
                   row(3L, 2L));

        UntypedResultSet resultSet = execute("SELECT min(ttl(b)), ttl(b) FROM %s");
        assertEquals(1, resultSet.size());
        Row row = resultSet.one();
        assertTrue(row.getInt("ttl(b)") > (10 * secondsPerMinute));
        assertTrue(row.getInt("system.min(ttl(b))") <= (10 * secondsPerMinute));

        resultSet = execute("SELECT min(writetime(b)), writetime(b) FROM %s");
        assertEquals(1, resultSet.size());
        row = resultSet.one();

        assertTrue(row.getLong("writetime(b)") >= today);
        assertTrue(row.getLong("system.min(writetime(b))") == yesterday);
    }

    @Test
    public void testInvalidCalls() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, primary key (a, b))");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 10)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 9)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 8)");

        assertInvalidSyntax("SELECT max(b), max(c) FROM %s WHERE max(a) = 1");
        assertInvalidMessage("aggregate functions cannot be used as arguments of aggregate functions", "SELECT max(sum(c)) FROM %s");
    }

    @Test
    public void testReversedType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 10)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 9)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 8)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, 7)");

        assertRows(execute("SELECT max(c), min(c), avg(c) FROM %s WHERE a = 1 AND b > 1"), row(9, 7, 8));
    }

    @Test
    public void testNestedFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b timeuuid, c double, d double)");

        assertColumnNames(execute("SELECT max(a), max(to_unix_timestamp(b)) FROM %s"), "system.max(a)", "system.max(system.to_unix_timestamp(b))");
        assertRows(execute("SELECT max(a), max(to_unix_timestamp(b)) FROM %s"), row(null, null));
        assertColumnNames(execute("SELECT max(a), to_unix_timestamp(max(b)) FROM %s"), "system.max(a)", "system.to_unix_timestamp(system.max(b))");
        assertRows(execute("SELECT max(a), to_unix_timestamp(max(b)) FROM %s"), row(null, null));

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, max_timeuuid('2011-02-03 04:05:00+0000'), -1.2, 2.1)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, max_timeuuid('2011-02-03 04:06:00+0000'), 1.3, -3.4)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (3, max_timeuuid('2011-02-03 04:10:00+0000'), 1.4, 1.2)");

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = format.parse("2011-02-03 04:10:00");
        date = DateUtils.truncate(date, Calendar.MILLISECOND);

        assertRows(execute("SELECT max(a), max(to_unix_timestamp(b)) FROM %s"), row(3, date.getTime()));
        assertRows(execute("SELECT max(a), to_unix_timestamp(max(b)) FROM %s"), row(3, date.getTime()));
    }

    @Test
    public void testArithmeticCorrectness() throws Throwable
    {
        createTable("create table %s (bucket int primary key, val decimal)");
        execute("insert into %s (bucket, val) values (1, 0.25)");
        execute("insert into %s (bucket, val) values (2, 0.25)");
        execute("insert into %s (bucket, val) values (3, 0.5);");

        BigDecimal a = new BigDecimal("0.25");
        a = a.add(new BigDecimal("0.25"));
        a = a.add(new BigDecimal("0.5"));
        a = a.divide(new BigDecimal(3), RoundingMode.HALF_EVEN);

        assertRows(execute("select avg(val) from %s where bucket in (1, 2, 3);"),
                   row(a));
    }

    @Test
    public void testAggregatesWithoutOverflow() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 tinyint, v2 smallint, v3 int, v4 bigint, v5 varint)");
        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i,
                    (byte) ((Byte.MAX_VALUE / 3) + i), (short) ((Short.MAX_VALUE / 3) + i), (Integer.MAX_VALUE / 3) + i, (Long.MAX_VALUE / 3) + i,
                    BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(i)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (1, 2, 3);"),
                   row((byte) ((Byte.MAX_VALUE / 3) + 2), (short) ((Short.MAX_VALUE / 3) + 2), (Integer.MAX_VALUE / 3) + 2, (Long.MAX_VALUE / 3) + 2,
                       BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(2))));

        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i + 3,
                    (byte) (100 + i), (short) (100 + i), 100 + i, 100L + i, BigInteger.valueOf(100 + i));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (4, 5, 6);"),
                   row((byte) 102, (short) 102, 102, 102L, BigInteger.valueOf(102)));
    }

    @Test
    public void testAggregateOverflow() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 tinyint, v2 smallint, v3 int, v4 bigint, v5 varint)");
        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i,
                    Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (1, 2, 3);"),
                   row(Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2))));

        execute("truncate %s");

        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i,
                    Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE, BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (1, 2, 3);"),
                   row(Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE, BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2))));

    }

    @Test
    public void testDoubleAggregatesPrecision() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double, v3 decimal)");

        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", i,
                    Float.MAX_VALUE, Double.MAX_VALUE, BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(2)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3) from %s where bucket in (1, 2, 3);"),
                   row(Float.MAX_VALUE, Double.MAX_VALUE, BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(2))));

        execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", 4, (float) 100.10, 100.10, BigDecimal.valueOf(100.10));
        execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", 5, (float) 110.11, 110.11, BigDecimal.valueOf(110.11));
        execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", 6, (float) 120.12, 120.12, BigDecimal.valueOf(120.12));

        assertRows(execute("select avg(v1), avg(v2), avg(v3) from %s where bucket in (4, 5, 6);"),
                   row((float) 110.11, 110.11, BigDecimal.valueOf(110.11)));
    }

    @Test
    public void testNan() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double)");

        for (int i = 1; i <= 10; i++)
            if (i != 5)
                execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", i, (float) i, (double) i);

        execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", 5, Float.NaN, Double.NaN);

        assertRows(execute("select avg(v1), avg(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                   row(Float.NaN, Double.NaN));
        assertRows(execute("select sum(v1), sum(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                   row(Float.NaN, Double.NaN));
    }

    @Test
    public void testInfinity() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double)");
        for (boolean positive: new boolean[] { true, false})
        {
            final float FLOAT_INFINITY = positive ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
            final double DOUBLE_INFINITY = positive ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;

            for (int i = 1; i <= 10; i++)
                if (i != 5)
                    execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", i, (float) i, (double) i);

            execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", 5, FLOAT_INFINITY, DOUBLE_INFINITY);

            assertRows(execute("select avg(v1), avg(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                       row(FLOAT_INFINITY, DOUBLE_INFINITY));
            assertRows(execute("select sum(v1), avg(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                       row(FLOAT_INFINITY, DOUBLE_INFINITY));

            execute("truncate %s");
        }
    }

    @Test
    public void testSumPrecision() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double, v3 decimal)");

        for (int i = 1; i <= 17; i++)
            execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", i, (float) (i / 10.0), i / 10.0, BigDecimal.valueOf(i / 10.0));

        assertRows(execute("select sum(v1), sum(v2), sum(v3) from %s;"),
                   row((float) 15.3, 15.3, BigDecimal.valueOf(15.3)));
    }
}
