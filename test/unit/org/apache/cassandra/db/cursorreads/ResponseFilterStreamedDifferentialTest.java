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

package org.apache.cassandra.db.cursorreads;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The row filter of the replica response ({@code ResponseSink}) evaluated on the streamed cells,
 * for every expression the read path filters with: simple columns, frozen and non-frozen
 * collections (CONTAINS, NOT CONTAINS, CONTAINS KEY, NOT CONTAINS KEY, the map element form),
 * counter columns, and clustering columns.  Each read is compared with the iterator path on every
 * surface of {@link CursorReadDifferentialTester}, and the replica response must build no row
 * ({@link CursorReads#mergeSinkRowsBuilt()} does not move).
 *
 * <p>The data is random per trial: inserts, collection appends and element deletes, collection
 * overwrites, cell, row, range and partition deletes, and TTLs that expire between the two read
 * times.  Each read picks a random filter (one to three expressions), with or without
 * reconciliation (replica filtering protection), a read time before or after the short TTLs
 * expire, and a random slice, direction and limit.  {@code gc_grace_seconds} is 0 or 10 days, so
 * the purge of the response both drops and keeps tombstones.
 *
 * <p>Collections of {@code decimal} hold equal values with different bytes (1, 1.0, 1.00): a set
 * element or map key is found with the type's comparator, not by bytes.
 *
 * <p>The trial count comes from {@code -Dcassandra.test.response_filter_trials=N} (default
 * {@value #DEFAULT_TRIALS}).
 */
public class ResponseFilterStreamedDifferentialTest extends SSTableFormatParameterizedTester
{
    private static final Logger logger = LoggerFactory.getLogger(ResponseFilterStreamedDifferentialTest.class);

    private static final int DEFAULT_TRIALS = 12;
    private static final int TRIALS = Integer.getInteger("cassandra.test.response_filter_trials", DEFAULT_TRIALS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int READS_PER_TRIAL = 12;
    private static final int SHORT_TTL = 100;
    /** The second read time: every short TTL has expired. */
    private static final int AFTER_EXPIRY = 3 * SHORT_TTL;
    private static final int LONG_TTL = 1_000_000;
    private static final long BASE_TS = 1_000_000L;

    /**
     * Seeds of trials that failed once, replayed before fresh ones.  Add an entry when a genuine
     * bug is found and fixed; never remove one.
     */
    private static final long[] KNOWN_REGRESSION_SEEDS = {};

    private static final String TABLE = "CREATE TABLE %s (pk int, ck int, s int static, v int, " +
                                        "l list<int>, st set<int>, m map<int, int>, " +
                                        "ld list<decimal>, sd set<decimal>, md map<decimal, decimal>, " +
                                        "fl frozen<list<int>>, fs frozen<set<int>>, fm frozen<map<int, int>>, " +
                                        "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = ";

    private static final String COUNTER_TABLE = "CREATE TABLE %s (pk int, ck int, c counter, c2 counter, " +
                                                "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = ";

    /** Equal decimals with different bytes. */
    private static final BigDecimal[] DECIMALS = { new BigDecimal("1"), new BigDecimal("1.0"), new BigDecimal("1.00"),
                                                   new BigDecimal("2"), new BigDecimal("2.0"), new BigDecimal("3") };

    // ---------------------------------------------------------------- random trials

    @Test
    public void randomRegularTable() throws Throwable
    {
        long seed = System.nanoTime();
        logger.info("ResponseFilterStreamedDifferentialTest.randomRegularTable seed = {}", seed);
        for (long regressionSeed : KNOWN_REGRESSION_SEEDS)
            runTrial(regressionSeed, false);
        Random seeds = new Random(seed);
        for (int i = 0; i < TRIALS; i++)
            runTrial(seeds.nextLong(), false);
    }

    @Test
    public void randomCounterTable() throws Throwable
    {
        long seed = System.nanoTime();
        logger.info("ResponseFilterStreamedDifferentialTest.randomCounterTable seed = {}", seed);
        Random seeds = new Random(seed);
        for (int i = 0; i < TRIALS; i++)
            runTrial(seeds.nextLong(), true);
    }

    private void runTrial(long trialSeed, boolean counters) throws Throwable
    {
        try
        {
            if (counters)
                counterTrial(trialSeed);
            else
                regularTrial(trialSeed);
        }
        catch (Throwable t)
        {
            throw new AssertionError(String.format("trial failed; replay with trialSeed=%dL (%s table, format %s)",
                                                   trialSeed, counters ? "counter" : "regular", formatName()), t);
        }
    }

    private void regularTrial(long trialSeed) throws Throwable
    {
        Random r = new Random(trialSeed);
        createTable(TABLE + (r.nextBoolean() ? 0 : 864000));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int maxCk = 4 + r.nextInt(20);
        int rounds = 1 + r.nextInt(3);
        long ts = BASE_TS;
        // baseline rows at both ends so every sstable spans the clustering domain
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 0) USING TIMESTAMP " + (ts++));
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, 0) USING TIMESTAMP " + (ts++), maxCk);
        for (int round = 0; round < rounds; round++)
        {
            int ops = 5 + r.nextInt(40);
            for (int i = 0; i < ops; i++)
                regularOp(r, ts++, maxCk, true);
            flush();
        }
        boolean memtableOverlay = r.nextBoolean();
        if (memtableOverlay)
        {
            int ops = 1 + r.nextInt(6);
            for (int i = 0; i < ops; i++)
                regularOp(r, ts++, maxCk, false);
        }

        long now = FBUtilities.nowInSeconds();
        for (int read = 0; read < READS_PER_TRIAL; read++)
        {
            long readSeed = r.nextLong();
            compare(cfs, regularRead(cfs, now, maxCk, readSeed), "read seed " + readSeed);
        }
    }

    private void regularOp(Random r, long ts, int maxCk, boolean allowPartitionDelete)
    {
        int ck = r.nextInt(maxCk + 1);
        String using = " USING TIMESTAMP " + ts + ttl(r);
        switch (r.nextInt(allowPartitionDelete ? 10 : 9))
        {
            case 0:
            case 1:
                // a full row: every collection is overwritten, with a complex deletion
                execute("INSERT INTO %s (pk, ck, v, l, st, m, ld, sd, md, fl, fs, fm) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" + using,
                        ck, r.nextInt(6), intList(r), intSet(r), intMap(r), decimalList(r), decimalSet(r), decimalMap(r),
                        intList(r), intSet(r), intMap(r));
                break;
            case 2:
                // collection appends
                execute("UPDATE %s" + using + " SET l = l + ?, st = st + ?, m = m + ?, ld = ld + ?, sd = sd + ?, md = md + ? WHERE pk = 1 AND ck = ?",
                        intList(r), intSet(r), intMap(r), decimalList(r), decimalSet(r), decimalMap(r), ck);
                break;
            case 3:
                // element deletes
                execute("UPDATE %s USING TIMESTAMP " + ts + " SET st = st - ?, m = m - ?, sd = sd - ?, md = md - ? WHERE pk = 1 AND ck = ?",
                        intSet(r), intSet(r), decimalSet(r), decimalSet(r), ck);
                break;
            case 4:
                // a simple and frozen columns only
                execute("UPDATE %s" + using + " SET v = ?, fl = ?, fs = ?, fm = ? WHERE pk = 1 AND ck = ?",
                        r.nextInt(6), intList(r), intSet(r), intMap(r), ck);
                break;
            case 5:
            {
                // whole-column deletes
                String[] columns = { "v", "l", "st", "m", "sd", "md", "fm" };
                execute("DELETE " + columns[r.nextInt(columns.length)] + " FROM %s USING TIMESTAMP " + ts + " WHERE pk = 1 AND ck = ?", ck);
                break;
            }
            case 6:
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = 1 AND ck = ?", ck);
                break;
            case 7:
            {
                int a = r.nextInt(maxCk + 1), b = r.nextInt(maxCk + 1);
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = 1 AND ck >= ? AND ck <= ?", Math.min(a, b), Math.max(a, b));
                break;
            }
            case 8:
                execute("UPDATE %s" + using + " SET s = ? WHERE pk = 1", r.nextInt(6));
                break;
            case 9:
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = 1");
                break;
            default:
                throw new AssertionError();
        }
    }

    private static String ttl(Random r)
    {
        switch (r.nextInt(4))
        {
            case 0: return " AND TTL " + SHORT_TTL;
            case 1: return " AND TTL " + LONG_TTL;
            default: return "";
        }
    }

    private Supplier<SinglePartitionReadCommand> regularRead(ColumnFamilyStore cfs, long now, int maxCk, long readSeed)
    {
        return () -> {
            Random r = new Random(readSeed);
            RowFilter rowFilter = RowFilter.create(r.nextBoolean());
            int expressions = 1 + r.nextInt(3);
            for (int i = 0; i < expressions; i++)
                addRegularExpression(cfs, rowFilter, r, maxCk);
            return command(cfs, now, maxCk, r, rowFilter);
        };
    }

    private static void addRegularExpression(ColumnFamilyStore cfs, RowFilter filter, Random r, int maxCk)
    {
        ListType<Integer> intList = ListType.getInstance(Int32Type.instance, false);
        switch (r.nextInt(12))
        {
            case 0:
            {
                // a simple column, every value operator
                Operator[] ops = { Operator.EQ, Operator.NEQ, Operator.LT, Operator.LTE, Operator.GT, Operator.GTE,
                                   Operator.IN, Operator.NOT_IN, Operator.BETWEEN };
                Operator op = ops[r.nextInt(ops.length)];
                ByteBuffer value;
                if (op == Operator.IN || op == Operator.NOT_IN)
                    value = intList.decompose(Arrays.asList(r.nextInt(6), r.nextInt(6)));
                else if (op == Operator.BETWEEN)
                {
                    int a = r.nextInt(6), b = r.nextInt(6);
                    value = intList.decompose(Arrays.asList(Math.min(a, b), Math.max(a, b)));
                }
                else
                    value = Int32Type.instance.decompose(r.nextInt(6));
                filter.add(column(cfs, "v"), op, value);
                break;
            }
            case 1:
            {
                Operator[] ops = { Operator.EQ, Operator.LT, Operator.GT, Operator.LTE, Operator.GTE };
                filter.add(column(cfs, "ck"), ops[r.nextInt(ops.length)], Int32Type.instance.decompose(r.nextInt(maxCk + 1)));
                break;
            }
            case 2:
                filter.add(column(cfs, "s"), r.nextBoolean() ? Operator.EQ : Operator.GT, Int32Type.instance.decompose(r.nextInt(6)));
                break;
            case 3:
            {
                // CONTAINS / NOT CONTAINS on a non-frozen int collection
                String[] columns = { "l", "st", "m" };
                filter.add(column(cfs, columns[r.nextInt(columns.length)]), r.nextBoolean() ? Operator.CONTAINS : Operator.NOT_CONTAINS,
                           Int32Type.instance.decompose(r.nextInt(6)));
                break;
            }
            case 4:
                filter.add(column(cfs, "m"), r.nextBoolean() ? Operator.CONTAINS_KEY : Operator.NOT_CONTAINS_KEY,
                           Int32Type.instance.decompose(r.nextInt(6)));
                break;
            case 5:
                filter.addMapEquality(column(cfs, "m"), Int32Type.instance.decompose(r.nextInt(6)),
                                      r.nextBoolean() ? Operator.EQ : Operator.NEQ, Int32Type.instance.decompose(r.nextInt(6)));
                break;
            case 6:
            {
                // decimal collections: equal values with different bytes
                String[] columns = { "ld", "sd", "md" };
                filter.add(column(cfs, columns[r.nextInt(columns.length)]), r.nextBoolean() ? Operator.CONTAINS : Operator.NOT_CONTAINS,
                           DecimalType.instance.decompose(DECIMALS[r.nextInt(DECIMALS.length)]));
                break;
            }
            case 7:
                if (r.nextBoolean())
                    filter.add(column(cfs, "md"), r.nextBoolean() ? Operator.CONTAINS_KEY : Operator.NOT_CONTAINS_KEY,
                               DecimalType.instance.decompose(DECIMALS[r.nextInt(DECIMALS.length)]));
                else
                    filter.addMapEquality(column(cfs, "md"), DecimalType.instance.decompose(DECIMALS[r.nextInt(DECIMALS.length)]),
                                          r.nextBoolean() ? Operator.EQ : Operator.NEQ,
                                          DecimalType.instance.decompose(DECIMALS[r.nextInt(DECIMALS.length)]));
                break;
            case 8:
            {
                // a frozen collection compared as a whole value
                String[] columns = { "fl", "fs", "fm" };
                ColumnMetadata column = column(cfs, columns[r.nextInt(columns.length)]);
                Operator[] ops = { Operator.EQ, Operator.NEQ, Operator.LT, Operator.GT };
                filter.add(column, ops[r.nextInt(ops.length)], frozenValue(column, r));
                break;
            }
            case 9:
            {
                String[] columns = { "fl", "fs", "fm" };
                filter.add(column(cfs, columns[r.nextInt(columns.length)]), r.nextBoolean() ? Operator.CONTAINS : Operator.NOT_CONTAINS,
                           Int32Type.instance.decompose(r.nextInt(6)));
                break;
            }
            case 10:
                filter.add(column(cfs, "fm"), r.nextBoolean() ? Operator.CONTAINS_KEY : Operator.NOT_CONTAINS_KEY,
                           Int32Type.instance.decompose(r.nextInt(6)));
                break;
            case 11:
                filter.addMapEquality(column(cfs, "fm"), Int32Type.instance.decompose(r.nextInt(6)),
                                      r.nextBoolean() ? Operator.EQ : Operator.NEQ, Int32Type.instance.decompose(r.nextInt(6)));
                break;
            default:
                throw new AssertionError();
        }
    }

    @SuppressWarnings("unchecked")
    private static ByteBuffer frozenValue(ColumnMetadata column, Random r)
    {
        CollectionType<?> type = (CollectionType<?>) column.type;
        switch (type.kind)
        {
            case LIST: return ((AbstractType<Object>) (AbstractType<?>) type).decompose(intList(r));
            case SET: return ((AbstractType<Object>) (AbstractType<?>) type).decompose(intSet(r));
            default: return ((AbstractType<Object>) (AbstractType<?>) type).decompose(intMap(r));
        }
    }

    private void counterTrial(long trialSeed) throws Throwable
    {
        Random r = new Random(trialSeed);
        createTable(COUNTER_TABLE + (r.nextBoolean() ? 0 : 864000));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int maxCk = 4 + r.nextInt(12);
        int rounds = 1 + r.nextInt(3);
        execute("UPDATE %s SET c = c + 1, c2 = c2 + 1 WHERE pk = 1 AND ck = 0");
        execute("UPDATE %s SET c = c + 1, c2 = c2 + 1 WHERE pk = 1 AND ck = ?", maxCk);
        for (int round = 0; round < rounds; round++)
        {
            int ops = 5 + r.nextInt(30);
            for (int i = 0; i < ops; i++)
                counterOp(r, maxCk);
            flush();
        }
        if (r.nextBoolean())
            counterOp(r, maxCk);

        long now = FBUtilities.nowInSeconds();
        for (int read = 0; read < READS_PER_TRIAL; read++)
        {
            long readSeed = r.nextLong();
            compare(cfs, counterRead(cfs, now, maxCk, readSeed), "read seed " + readSeed);
        }
    }

    private void counterOp(Random r, int maxCk)
    {
        int ck = r.nextInt(maxCk + 1);
        switch (r.nextInt(6))
        {
            case 0:
                // a deleted counter stays deleted
                execute("DELETE c FROM %s WHERE pk = 1 AND ck = ?", ck);
                break;
            case 1:
                execute("UPDATE %s SET c2 = c2 + ? WHERE pk = 1 AND ck = ?", (long) (r.nextInt(5) - 2), ck);
                break;
            default:
                execute("UPDATE %s SET c = c + ?, c2 = c2 + ? WHERE pk = 1 AND ck = ?", (long) (r.nextInt(5) - 2), (long) r.nextInt(3), ck);
                break;
        }
    }

    private Supplier<SinglePartitionReadCommand> counterRead(ColumnFamilyStore cfs, long now, int maxCk, long readSeed)
    {
        return () -> {
            Random r = new Random(readSeed);
            RowFilter rowFilter = RowFilter.create(r.nextBoolean());
            int expressions = 1 + r.nextInt(2);
            ListType<Long> longList = ListType.getInstance(LongType.instance, false);
            for (int i = 0; i < expressions; i++)
            {
                if (r.nextInt(5) == 0)
                {
                    rowFilter.add(column(cfs, "ck"), Operator.GT, Int32Type.instance.decompose(r.nextInt(maxCk + 1)));
                    continue;
                }
                Operator[] ops = { Operator.EQ, Operator.NEQ, Operator.LT, Operator.LTE, Operator.GT, Operator.GTE, Operator.IN };
                Operator op = ops[r.nextInt(ops.length)];
                ByteBuffer value = op == Operator.IN ? longList.decompose(Arrays.asList((long) r.nextInt(8) - 2, (long) r.nextInt(8) - 2))
                                                     : LongType.instance.decompose((long) r.nextInt(10) - 2);
                rowFilter.add(column(cfs, r.nextBoolean() ? "c" : "c2"), op, value);
            }
            return command(cfs, now, maxCk, r, rowFilter);
        };
    }

    // ---------------------------------------------------------------- targeted cases

    /** A set element is found with the element type's comparator: 1.00 finds 1.0. */
    @Test
    public void setContainsComparesElementsByType() throws Throwable
    {
        createTable(TABLE + 864000);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        execute("INSERT INTO %s (pk, ck, v, sd, md) VALUES (1, 1, 1, ?, ?) USING TIMESTAMP 10",
                set(new BigDecimal("1.0")), map(new BigDecimal("2.0"), new BigDecimal("3.0")));
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 1) USING TIMESTAMP 11");
        flush();
        long now = FBUtilities.nowInSeconds();
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "sd"), Operator.CONTAINS, DecimalType.instance.decompose(new BigDecimal("1.00")))), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "md"), Operator.CONTAINS_KEY, DecimalType.instance.decompose(new BigDecimal("2")))), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "md"), Operator.CONTAINS, DecimalType.instance.decompose(new BigDecimal("3")))), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.addMapEquality(column(cfs, "md"), DecimalType.instance.decompose(new BigDecimal("2.00")),
                                                                                          Operator.EQ, DecimalType.instance.decompose(new BigDecimal("3")))), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "sd"), Operator.NOT_CONTAINS, DecimalType.instance.decompose(new BigDecimal("1")))), 0);
    }

    /**
     * With reconciliation the row is not purged before the filter, so a deleted map element is
     * still found by the map element form, with an empty value.  Without it the element is gone.
     */
    @Test
    public void mapElementFindsDeletedElementWhenReconciling() throws Throwable
    {
        createTable(TABLE + 864000);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        execute("INSERT INTO %s (pk, ck, v, m, st) VALUES (1, 1, 1, {1: 1}, {4}) USING TIMESTAMP 10");
        flush();
        execute("DELETE m[1] FROM %s USING TIMESTAMP 11 WHERE pk = 1 AND ck = 1");
        execute("UPDATE %s USING TIMESTAMP 11 SET st = st - {4} WHERE pk = 1 AND ck = 1");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 1) USING TIMESTAMP 12");
        flush();
        long now = FBUtilities.nowInSeconds();
        ByteBuffer one = Int32Type.instance.decompose(1);
        ByteBuffer four = Int32Type.instance.decompose(4);
        // the element tombstone's empty value is not 5
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, true, f -> f.addMapEquality(column(cfs, "m"), one, Operator.NEQ, Int32Type.instance.decompose(5))), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.addMapEquality(column(cfs, "m"), one, Operator.NEQ, Int32Type.instance.decompose(5))), 0);
        // a deleted set element and map key are still found when reconciling
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, true, f -> f.add(column(cfs, "st"), Operator.CONTAINS, four)), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "st"), Operator.CONTAINS, four)), 0);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, true, f -> f.add(column(cfs, "m"), Operator.CONTAINS_KEY, one)), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "m"), Operator.CONTAINS_KEY, one)), 0);
    }

    /**
     * NOT CONTAINS and NOT CONTAINS KEY fail for a row without the column, like CONTAINS.  With
     * reconciliation a column that holds only a deleted element is still in the row.
     */
    @Test
    public void notContainsNeedsTheColumn() throws Throwable
    {
        createTable(TABLE + 864000);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 10");
        execute("INSERT INTO %s (pk, ck, v, l, m) VALUES (1, 2, 1, [3], {3: 3}) USING TIMESTAMP 10");
        execute("INSERT INTO %s (pk, ck, v, l, m) VALUES (1, 4, 1, [5], {5: 5}) USING TIMESTAMP 10");
        execute("UPDATE %s USING TIMESTAMP 10 SET v = 1, st = st + {7} WHERE pk = 1 AND ck = 5");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 1) USING TIMESTAMP 11");
        execute("UPDATE %s USING TIMESTAMP 11 SET st = st - {7} WHERE pk = 1 AND ck = 5");
        flush();
        long now = FBUtilities.nowInSeconds();
        ByteBuffer three = Int32Type.instance.decompose(3);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "l"), Operator.NOT_CONTAINS, three)), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "m"), Operator.NOT_CONTAINS_KEY, three)), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "l"), Operator.CONTAINS, three)), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, true, f -> f.add(column(cfs, "st"), Operator.NOT_CONTAINS, three)), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "st"), Operator.NOT_CONTAINS, three)), 0);
    }

    /** A collection element whose TTL has expired is not found once it expires. */
    @Test
    public void expiredElementIsNotFound() throws Throwable
    {
        createTable(TABLE + 864000);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 10");
        execute("UPDATE %s USING TIMESTAMP 11 AND TTL " + SHORT_TTL + " SET st = st + {2}, m[2] = 2 WHERE pk = 1 AND ck = 1");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 1) USING TIMESTAMP 12");
        flush();
        long now = FBUtilities.nowInSeconds();
        ByteBuffer two = Int32Type.instance.decompose(2);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "st"), Operator.CONTAINS, two)), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now + AFTER_EXPIRY, false, f -> f.add(column(cfs, "st"), Operator.CONTAINS, two)), 0);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now + AFTER_EXPIRY, false, f -> f.addMapEquality(column(cfs, "m"), two, Operator.EQ, two)), 0);
        // when reconciling the expired element is a tombstone and is still found
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now + AFTER_EXPIRY, true, f -> f.add(column(cfs, "st"), Operator.CONTAINS, two)), 1);
    }

    /** A counter is compared by its total over every sstable. */
    @Test
    public void counterTotalOverSeveralSSTables() throws Throwable
    {
        createTable(COUNTER_TABLE + 864000);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (int i = 0; i < 3; i++)
        {
            execute("UPDATE %s SET c = c + 2 WHERE pk = 1 AND ck = 1");
            execute("UPDATE %s SET c = c + 1 WHERE pk = 1 AND ck = 2");
            flush();
        }
        long now = FBUtilities.nowInSeconds();
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "c"), Operator.EQ, LongType.instance.decompose(6L))), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, false, f -> f.add(column(cfs, "c"), Operator.LT, LongType.instance.decompose(6L))), 1);
        assertMatchesAndReturnsRows(cfs, filtered(cfs, now, true, f -> f.add(column(cfs, "c"), Operator.GTE, LongType.instance.decompose(3L))), 2);
    }

    // ---------------------------------------------------------------- helpers

    private Supplier<SinglePartitionReadCommand> filtered(ColumnFamilyStore cfs, long now, boolean needsReconciliation,
                                                          java.util.function.Consumer<RowFilter> expressions)
    {
        return () -> {
            RowFilter rowFilter = RowFilter.create(needsReconciliation);
            expressions.accept(rowFilter);
            SinglePartitionReadCommand base = (SinglePartitionReadCommand) Util.cmd(cfs, 1).withNowInSeconds(now).build();
            return SinglePartitionReadCommand.create(cfs.metadata(), now, ColumnFilter.all(cfs.metadata()), rowFilter,
                                                     DataLimits.NONE, base.partitionKey(), base.clusteringIndexFilter());
        };
    }

    /** A read of partition 1 with {@code rowFilter}: a random read time, slice, direction and limit. */
    private static SinglePartitionReadCommand command(ColumnFamilyStore cfs, long now, int maxCk, Random r, RowFilter rowFilter)
    {
        long nowInSec = r.nextBoolean() ? now : now + AFTER_EXPIRY;
        AbstractReadCommandBuilder b = Util.cmd(cfs, 1).withNowInSeconds(nowInSec);
        if (r.nextInt(3) == 0)
        {
            int a = r.nextInt(maxCk + 1), c = r.nextInt(maxCk + 1);
            b = b.fromIncl(Math.min(a, c)).toIncl(Math.max(a, c));
        }
        if (r.nextInt(3) == 0)
            b = b.reverse();
        SinglePartitionReadCommand base = (SinglePartitionReadCommand) b.build();
        DataLimits limits = r.nextInt(3) == 0 ? DataLimits.cqlLimits(1 + r.nextInt(4)) : DataLimits.NONE;
        return SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, ColumnFilter.all(cfs.metadata()), rowFilter,
                                                 limits, base.partitionKey(), base.clusteringIndexFilter());
    }

    /** The differential, then the replica response built again with no row built for the filter. */
    private void compare(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command, String label)
    {
        try
        {
            assertCursorReadMatchesIterator(cfs, command);
            assertResponseBuildsNoRow(cfs, command);
        }
        catch (Throwable t)
        {
            throw new AssertionError(label + ": " + command.get().toCQLString(), t);
        }
    }

    private static void assertResponseBuildsNoRow(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    {
        SinglePartitionReadCommand probe = command.get();
        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            long servedBefore = CursorReads.transcodeResponsesServed();
            long builtBefore = CursorReads.mergeSinkRowsBuilt();
            replicaResponseBytes(command.get());
            if (CursorReadOracle.transcodeEligible(probe, cfs))
            {
                assertEquals("the replica response was not streamed", 1, CursorReads.transcodeResponsesServed() - servedBefore);
                assertEquals("rows built for the replica response", 0, CursorReads.mergeSinkRowsBuilt() - builtBefore);
            }
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    private void assertMatchesAndReturnsRows(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command, int expectedRows)
    {
        long rows = canonicalRecords(command.get()).stream().filter(s -> s.startsWith("ROW")).count();
        assertEquals("rows the iterator path returns: " + command.get().toCQLString(), expectedRows, rows);
        compare(cfs, command, "targeted");
    }

    private static ColumnMetadata column(ColumnFamilyStore cfs, String name)
    {
        ColumnMetadata column = cfs.metadata().getColumn(ByteBufferUtil.bytes(name));
        assertTrue("no column " + name, column != null);
        return column;
    }

    private static List<Integer> intList(Random r)
    {
        List<Integer> list = new ArrayList<>();
        int n = r.nextInt(4);
        for (int i = 0; i < n; i++)
            list.add(r.nextInt(6));
        return list;
    }

    private static Set<Integer> intSet(Random r)
    {
        return new LinkedHashSet<>(intList(r));
    }

    private static Map<Integer, Integer> intMap(Random r)
    {
        Map<Integer, Integer> map = new LinkedHashMap<>();
        int n = r.nextInt(4);
        for (int i = 0; i < n; i++)
            map.put(r.nextInt(6), r.nextInt(6));
        return map;
    }

    private static List<BigDecimal> decimalList(Random r)
    {
        List<BigDecimal> list = new ArrayList<>();
        int n = r.nextInt(3);
        for (int i = 0; i < n; i++)
            list.add(DECIMALS[r.nextInt(DECIMALS.length)]);
        return list;
    }

    private static Set<BigDecimal> decimalSet(Random r)
    {
        return new LinkedHashSet<>(decimalList(r));
    }

    private static Map<BigDecimal, BigDecimal> decimalMap(Random r)
    {
        Map<BigDecimal, BigDecimal> map = new LinkedHashMap<>();
        int n = r.nextInt(3);
        for (int i = 0; i < n; i++)
            map.put(DECIMALS[r.nextInt(DECIMALS.length)], DECIMALS[r.nextInt(DECIMALS.length)]);
        return map;
    }
}
