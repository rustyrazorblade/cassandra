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
package org.apache.cassandra.cql3.selection.arena;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CQL_ARENA_AGGREGATION_MAX_BYTES;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CQL_ARENA_AGGREGATION_MAX_QUERY_BYTES;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CQL_ARENA_AGGREGATION_MAX_ROWS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Adversarial tests for the arena aggregation path.
 *
 * <p>Every test here ACTIVATES the arena.  Activation needs three things together: the flag on, a
 * single-partition query (EQ on the full partition key), and an ORDER BY on a NON-clustering column
 * that is also in the SELECT list.  A test that omits any of these silently runs the on-heap path,
 * so its assertions prove nothing about the arena.  The prior suite made exactly that mistake.
 *
 * <p>The pool ({@link ArenaScratchPool}) is a program-lifetime singleton: it reads
 * {@code cassandra.cql.arena_aggregation.max_bytes} ONCE, at first use, and never resizes.  These
 * tests therefore never set max_bytes; that would fix the pool size for the whole JVM run and
 * corrupt later tests.  Only the per-query knobs (max_query_bytes, max_rows), read fresh on every
 * query, are lowered here.
 */
public class ArenaAggregationTest extends CQLTester
{
    @After
    public void teardown()
    {
        // Restore every arena property to its default.  Never touch max_bytes here: it is read once
        // at pool init and cannot be reset for the running JVM.
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.reset();
        CASSANDRA_CQL_ARENA_AGGREGATION_MAX_ROWS.reset();
        CASSANDRA_CQL_ARENA_AGGREGATION_MAX_BYTES.reset();
        CASSANDRA_CQL_ARENA_AGGREGATION_MAX_QUERY_BYTES.reset();
    }

    // ------------------------------------------------------------------------------------------------
    // Gate errors: HAVING and non-clustering ORDER BY with the flag off.
    // ------------------------------------------------------------------------------------------------

    /**
     * HAVING with the flag off throws the pinned InvalidRequestException that points at the flag.
     */
    @Test
    public void testHavingFlagOffError() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(false);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        assertThatThrownBy(() -> execute("SELECT pk, SUM(v) FROM %s WHERE pk = 1 GROUP BY pk HAVING SUM(v) > 10"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("HAVING clause is not supported when arena aggregation is disabled")
            .hasMessageContaining("cassandra.cql.arena_aggregation.enabled");
    }

    /**
     * HAVING with the flag ON is honestly rejected as unfinished, not silently run unfiltered.
     */
    @Test
    public void testHavingFlagOnNotYetSupported() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        assertThatThrownBy(() -> execute("SELECT pk, SUM(v) FROM %s WHERE pk = 1 GROUP BY pk HAVING SUM(v) > 10"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("HAVING is not yet supported");
    }

    /**
     * A non-clustering ORDER BY with the flag off still fails with the pinned clustering-only error.
     */
    @Test
    public void testNonClusteringOrderByFlagOffError() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(false);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        assertThatThrownBy(() -> execute("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("Order by is currently only supported on the clustered columns of the PRIMARY KEY, got v");
    }

    // ------------------------------------------------------------------------------------------------
    // Differential sort tests: arena order must match an on-heap sort using the SAME type comparator.
    // ------------------------------------------------------------------------------------------------

    /**
     * Ascending ORDER BY on a non-clustering int column through the arena.
     */
    @Test
    public void testArenaOrderByIntAscending() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        List<Object> values = Arrays.asList(5, 1, 9, 3, 7, 1, 8, 2);
        insertValues(values);

        List<Object> actual = orderByColumn("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v", 1);
        List<Object> expected = referenceSort(Int32Type.instance, values, false);

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Descending ORDER BY exercises the reversed compare path.
     */
    @Test
    public void testArenaOrderByIntDescending() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        List<Object> values = Arrays.asList(5, 1, 9, 3, 7, 1, 8, 2);
        insertValues(values);

        List<Object> actual = orderByColumn("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v DESC", 1);
        List<Object> expected = referenceSort(Int32Type.instance, values, true);

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * A column with NULLs.  NULL ordering must match the reference (null-first regardless of direction).
     */
    @Test
    public void testArenaOrderByWithNulls() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        List<Object> values = Arrays.asList(4, null, 1, null, 3, 2);
        insertValues(values);

        List<Object> actualAsc = orderByColumn("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v", 1);
        assertThat(actualAsc).isEqualTo(referenceSort(Int32Type.instance, values, false));

        List<Object> actualDesc = orderByColumn("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v DESC", 1);
        assertThat(actualDesc).isEqualTo(referenceSort(Int32Type.instance, values, true));
    }

    /**
     * Varint values that span the 127/128 byte-length boundary.  A naive unsigned-byte compare would
     * misorder these; the arena must use IntegerType.compare.
     */
    @Test
    public void testArenaOrderByVarintTypeAware() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);

        createTable("CREATE TABLE %s (pk int, ck int, v varint, PRIMARY KEY (pk, ck))");

        List<Object> values = Arrays.asList(BigInteger.valueOf(127),
                                            BigInteger.valueOf(128),
                                            BigInteger.valueOf(255),
                                            BigInteger.valueOf(256),
                                            BigInteger.valueOf(-1),
                                            BigInteger.valueOf(-129),
                                            BigInteger.valueOf(1));
        insertValues(values);

        List<Object> actual = orderByColumn("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v", 1);
        List<Object> expected = referenceSort(IntegerType.instance, values, false);

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Decimal values with differing scales.  Byte order does not match value order, so the arena
     * must use DecimalType.compare.
     */
    @Test
    public void testArenaOrderByDecimalTypeAware() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);

        createTable("CREATE TABLE %s (pk int, ck int, v decimal, PRIMARY KEY (pk, ck))");

        List<Object> values = Arrays.asList(new BigDecimal("1.5"),
                                            new BigDecimal("2.25"),
                                            new BigDecimal("0.5"),
                                            new BigDecimal("10.125"),
                                            new BigDecimal("2.5"),
                                            new BigDecimal("0.05"));
        insertValues(values);

        List<Object> actual = orderByColumn("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v DESC", 1);
        List<Object> expected = referenceSort(DecimalType.instance, values, true);

        assertThat(actual).isEqualTo(expected);
    }

    // ------------------------------------------------------------------------------------------------
    // Capacity breaches: each must surface an InvalidRequestException naming the exact knob to raise.
    // ------------------------------------------------------------------------------------------------

    /**
     * Test I (row cap): more buffered rows than max_rows breaches; the error names max_rows.
     */
    @Test
    public void testCapacityBreachRowLimitNamesKnob() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);
        CASSANDRA_CQL_ARENA_AGGREGATION_MAX_ROWS.setInt(10);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        for (int i = 0; i < 50; i++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", i, i);

        assertThatThrownBy(() -> execute("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("cassandra.cql.arena_aggregation.max_rows");
    }

    /**
     * Test I (byte cap): projected payload larger than the per-query lease breaches; the error names
     * max_query_bytes.  max_query_bytes rounds up to a whole 1 MB block, so ~2 MB of text overflows
     * a 1-byte (one-block) lease while staying well under the default row cap.
     */
    @Test
    public void testCapacityBreachByteLimitNamesKnob() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);
        CASSANDRA_CQL_ARENA_AGGREGATION_MAX_QUERY_BYTES.setLong(1L); // rounds up to one 1 MB block

        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

        String wide = repeat('x', 2000);
        for (int i = 0; i < 1000; i++) // ~2 MB payload, exceeds the 1 MB lease
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", i, wide);

        assertThatThrownBy(() -> execute("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("cassandra.cql.arena_aggregation.max_query_bytes");
    }

    /**
     * Test J (release-on-exception): after a breach, the pool returns to fully free.  A leaked lease
     * would leave leasedBytes above zero and freeBytes below the total.
     */
    @Test
    public void testReleaseOnCapacityBreach() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);
        CASSANDRA_CQL_ARENA_AGGREGATION_MAX_ROWS.setInt(10);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        for (int i = 0; i < 50; i++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", i, i);

        assertThatThrownBy(() -> execute("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v"))
            .isInstanceOf(InvalidRequestException.class);

        ArenaScratchPool.PoolStats stats = ArenaScratchPool.getInstance().getStats();
        assertThat(stats.leasedBytes).isEqualTo(0L); // no leaked lease
        assertThat(stats.freeBytes).isEqualTo(stats.totalBytes); // run returned
    }

    // ------------------------------------------------------------------------------------------------
    // Test K: large payload (> 64 KB) crosses the payload/directory boundary; order must still match.
    // ------------------------------------------------------------------------------------------------

    /**
     * A single-partition ORDER BY whose projected payload far exceeds 64 KB: 400 rows of ~258-byte
     * text.  The payload region (growing up) and the directory region (growing down) must stay
     * disjoint, so the sort order must still match a UTF8Type reference sort exactly.
     */
    @Test
    public void testArenaOrderByLargePayloadTextTypeAware() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);

        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

        Random rnd = new Random(42);
        List<Object> values = new ArrayList<>(400);
        for (int i = 0; i < 400; i++)
        {
            StringBuilder sb = new StringBuilder(258);
            // A pseudo-random 8-digit prefix so the sorted order differs from insertion order.
            sb.append(String.format("%08d", rnd.nextInt(100000000)));
            for (int j = 0; j < 250; j++)
                sb.append((char) ('a' + rnd.nextInt(26)));
            values.add(sb.toString());
        }
        insertValues(values);

        List<Object> actual = orderByColumn("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v", 1);
        List<Object> expected = referenceSort(UTF8Type.instance, values, false);

        assertThat(actual).isEqualTo(expected);
    }

    // ------------------------------------------------------------------------------------------------
    // Test L: concurrent multi-query isolation across the shared pool.
    // ------------------------------------------------------------------------------------------------

    /**
     * Many arena ORDER BY queries run in parallel against the shared pool.  A small per-query cap
     * makes every lease one block, so all of them coexist and the free list is exercised hard.  Each
     * partition holds a disjoint value range, so a leaked or cross-contaminated row from another
     * lease shows up as a wrong value or wrong count, not a coincidental match.
     */
    @Test
    public void testConcurrentQueryIsolation() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);
        CASSANDRA_CQL_ARENA_AGGREGATION_MAX_QUERY_BYTES.setLong(1048576L); // 1 MB per query -> one block each

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        final int partitions = 16;
        final int rowsPerPartition = 120;

        for (int p = 0; p < partitions; p++)
        {
            List<Integer> vals = new ArrayList<>(rowsPerPartition);
            for (int i = 0; i < rowsPerPartition; i++)
                vals.add(p * 100000 + i); // disjoint range per partition
            Collections.shuffle(vals, new Random(p));
            for (int i = 0; i < rowsPerPartition; i++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", p, i, vals.get(i));
        }

        final String table = KEYSPACE + '.' + currentTable();
        final int iterations = 40;
        final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

        for (int iter = 0; iter < iterations && failures.isEmpty(); iter++)
        {
            final CountDownLatch start = new CountDownLatch(1);
            List<Thread> workers = new ArrayList<>(partitions);
            for (int p = 0; p < partitions; p++)
            {
                final int partition = p;
                Thread t = new Thread(() -> {
                    try
                    {
                        start.await();
                        String q = "SELECT ck, v FROM " + table + " WHERE pk = " + partition + " ORDER BY v";
                        Object[][] rows = getRows(execute(q));
                        List<Object> actual = new ArrayList<>(rows.length);
                        for (Object[] row : rows)
                            actual.add(row[1]);

                        List<Object> expected = new ArrayList<>(rowsPerPartition);
                        for (int i = 0; i < rowsPerPartition; i++)
                            expected.add(partition * 100000 + i);

                        if (!actual.equals(expected))
                            failures.add(new AssertionError("partition " + partition
                                                            + " cross-contaminated: expected " + expected
                                                            + " got " + actual));
                    }
                    catch (Throwable th)
                    {
                        failures.add(th);
                    }
                });
                workers.add(t);
                t.start();
            }

            start.countDown(); // release every worker at once for real contention
            for (Thread t : workers)
                t.join();
        }

        assertThat(failures).isEmpty();

        // Every lease must have been returned once the queries finish.
        ArenaScratchPool.PoolStats stats = ArenaScratchPool.getInstance().getStats();
        assertThat(stats.leasedBytes).isEqualTo(0L);
    }

    // ------------------------------------------------------------------------------------------------
    // Test M: large pre-sorted and reverse-sorted input; introsort must not degrade or overflow.
    // ------------------------------------------------------------------------------------------------

    /**
     * Tens of thousands of rows inserted in already-sorted and, separately, reverse-sorted key order:
     * the worst case for a naive fixed-pivot quicksort.  The introsort (median-of-three pivot,
     * heapsort fallback) must complete without a StackOverflowError and return the correct order.
     */
    @Test
    public void testLargePreSortedInput() throws Throwable
    {
        CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.setBoolean(true);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        final int n = 20000;

        for (int i = 0; i < n; i++) // pk=1: ascending insertion order
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", i, i);
        for (int i = 0; i < n; i++) // pk=2: reverse insertion order
            execute("INSERT INTO %s (pk, ck, v) VALUES (2, ?, ?)", i, n - 1 - i);

        List<Object> ascExpected = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
            ascExpected.add(i);

        assertThat(orderByColumn("SELECT ck, v FROM %s WHERE pk = 1 ORDER BY v", 1)).isEqualTo(ascExpected);
        assertThat(orderByColumn("SELECT ck, v FROM %s WHERE pk = 2 ORDER BY v", 1)).isEqualTo(ascExpected);
    }

    // ------------------------------------------------------------------------------------------------
    // Helpers.
    // ------------------------------------------------------------------------------------------------

    /**
     * Insert one row per value into partition pk=1, one clustering key per row so nothing is
     * overwritten.  A null value leaves the column unset.
     */
    private void insertValues(List<Object> values) throws Throwable
    {
        for (int i = 0; i < values.size(); i++)
        {
            Object v = values.get(i);
            if (v == null)
                execute("INSERT INTO %s (pk, ck) VALUES (1, ?)", i);
            else
                execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", i, v);
        }
    }

    /**
     * Run a query and return the values of the given result column, in result order.
     */
    private List<Object> orderByColumn(String query, int columnIndex) throws Throwable
    {
        Object[][] rows = getRows(execute(query));
        List<Object> out = new ArrayList<>(rows.length);
        for (Object[] row : rows)
            out.add(row[columnIndex]);
        return out;
    }

    /**
     * On-heap reference sort that mirrors ArenaRowComparator exactly: null-first (never flipped by
     * direction), then the type comparator with reversal applied by argument swap.
     */
    @SuppressWarnings("unchecked")
    private static List<Object> referenceSort(AbstractType<?> type, List<Object> values, boolean reversed)
    {
        AbstractType<Object> t = (AbstractType<Object>) type;
        List<ByteBuffer> buffers = new ArrayList<>(values.size());
        for (Object v : values)
            buffers.add(v == null ? null : t.decompose(v));

        buffers.sort((a, b) -> {
            if (a == null && b == null)
                return 0;
            if (a == null)
                return -1;
            if (b == null)
                return 1;
            return reversed ? t.compare(b, a) : t.compare(a, b);
        });

        List<Object> out = new ArrayList<>(buffers.size());
        for (ByteBuffer bb : buffers)
            out.add(bb == null ? null : t.compose(bb));
        return out;
    }

    /**
     * Build a string of {@code count} copies of {@code c}.  Avoids String.repeat so the value is
     * obvious at the call site.
     */
    private static String repeat(char c, int count)
    {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }
}
