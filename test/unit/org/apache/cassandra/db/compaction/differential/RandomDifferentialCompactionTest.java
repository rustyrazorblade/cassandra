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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableList;

import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.JavaRandom;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.AbstractTypeGenerators.ValueDomain;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.Generators;

import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;
import static org.junit.Assert.assertTrue;

/**
 * Randomized soak for the cursor-vs-iterator differential harness: random schemas restricted to
 * the currently supported cursor compaction surface (the same unsupportedMetadata filter
 * production uses), random multi-round workloads with overwrites and deletes flushed into
 * overlapping sstables, then byte+logical differential comparison of both compaction paths.
 *
 * Workload space: INSERT / UPDATE (no row liveness) / primary-key-only INSERT (liveness, no
 * cells) / INSERT USING TTL (long-lived, far from the expiry boundary) / explicit USING
 * TIMESTAMP collisions from a small pool (same-timestamp tie-breaks); null values on
 * non-key columns (cell tombstones on simple columns; mapped to empty buffers on clustering
 * columns); row deletes, partition deletes, single-sided and prefix range deletes,
 * multi-column cell deletes, static cell deletes. Composite partition keys (1-2 components).
 *
 * Example count is property-gated: -Dcassandra.test.differential.examples=N (default
 * {@value #DEFAULT_EXAMPLES}; the pre-JIRA validation run uses thousands).
 *
 * Reproducing a failure: every failure message is wrapped in a seed; rerun with
 * -Dcassandra.test.differential.seed=N (the failing seed becomes example 0), or plug it
 * into {@code withFixedSeed} below.
 *
 * Known coverage gaps (deliberate, covered by the deterministic corpus): EMPTY_BYTES value
 * domain (invalid CQL for some generated multi-cell shapes), collection-element deletes
 * (DELETE m['k'] needs element values of the right type), expired TTLs (timing-dependent).
 */
public class RandomDifferentialCompactionTest extends DifferentialCompactionTester
{
    static
    {
        // make sure generated blobs are deterministic per seed
        CassandraRelevantProperties.TEST_BLOB_SHARED_SEED.setInt(42);
    }

    private static final int DEFAULT_EXAMPLES = 10;
    private static final int EXAMPLES = CassandraRelevantProperties.TEST_DIFFERENTIAL_EXAMPLES.getInt(DEFAULT_EXAMPLES);

    /** Long enough that expiry can never fall between the two differential runs. */
    private static final int SOAK_TTL_SECONDS = 30 * 24 * 60 * 60;

    @Test
    public void randomizedDifferential() throws Throwable
    {
        // a zero or negative example count would make SeedRunner.run iterate zero times and the
        // test pass having compared nothing
        assertTrue("cassandra.test.differential.examples must be > 0, got " + EXAMPLES, EXAMPLES > 0);
        new SeedRunner(EXAMPLES).run(this::runOneExample);
    }

    private void runOneExample(long seed) throws Throwable
    {
        JavaRandom qtRandom = new JavaRandom(seed);
        Random workload = new Random(seed);

        // ~1 in 5 examples: a COUNTER table. Counters cannot mix with regular columns
        // (CQL rejects the combination), so instead of widening the type generator they
        // get a dedicated generation mode with counter-update syntax (increment 5).
        if (workload.nextInt(5) == 0)
        {
            runCounterExample(seed, workload);
            return;
        }

        Gen<String> udtName = Generators.unique(IDENTIFIER_GEN);
        TypeGenBuilder safePrimary = AbstractTypeGenerators.withoutUnsafeEquality().withUDTNames(udtName);
        TableMetadata metadata;
        do
        {
            metadata = new TableMetadataBuilder()
                       .withKeyspaceName(KEYSPACE)
                       .withTableKinds(TableMetadata.Kind.REGULAR)
                       .withKnownMemtables()
                       .withDefaultTypeGen(AbstractTypeGenerators.builder()
                                                                 .withoutEmpty()
                                                                 .withMaxDepth(2)
                                                                 .withDefaultSetKey(safePrimary)
                                                                 .withoutTypeKinds(AbstractTypeGenerators.TypeKind.COUNTER)
                                                                 .withUDTNames(udtName))
                       .withPartitionColumnsBetween(1, 2)
                       .withPrimaryColumnTypeGen(new TypeGenBuilder(safePrimary).withMaxDepth(1))
                       .withClusteringColumnsBetween(0, 3)
                       .withRegularColumnsBetween(1, 5)
                       .withStaticColumnsBetween(0, 2)
                       .build(qtRandom);
        }
        // Same filter production uses to route to the cursor pipeline.
        // Also rejects invalid CQL the generator can produce: static columns require
        // clustering columns.
        while (CursorCompactor.unsupportedMetadata(metadata)
               || (metadata.clusteringColumns().isEmpty() && !metadata.staticColumns().isEmpty()));

        maybeCreateUDTs(metadata);
        String createTableCql = metadata.toCqlString(true, false, false)
                                        .replaceAll("org.apache.cassandra.db.marshal.", "");
        logger.info("randomizedDifferential seed={} schema:\n{}", seed, createTableCql);
        createTable(KEYSPACE, createTableCql);
        // the CQL embeds the generator's table name; createTable's returned name is not it
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, metadata.name);
        cfs.disableAutoCompaction();

        // ~12% of non-key values are null: cell tombstones on simple columns. The data
        // generator maps null to an empty buffer on clustering columns (null clustering is
        // invalid; empty is legal and exercises the empty-vs-valued clustering comparison)
        // and never applies the domain to partition keys.
        Gen<ValueDomain> valueDomains = SourceDSL.integers().between(0, 99)
                                                 .map(i -> i < 12 ? ValueDomain.NULL : ValueDomain.NORMAL);
        Gen<ByteBuffer[]> dataGen = CassandraGenerators.data(metadata, valueDomains);

        int partitionColumnCount = metadata.partitionKeyColumns().size();
        int clusteringColumnCount = metadata.clusteringColumns().size();
        int primaryColumnCount = partitionColumnCount + clusteringColumnCount;
        String insertStmt = insertStmt(metadata);
        String deleteRowStmt = deleteStmt(metadata, primaryColumnCount);
        String deletePartitionStmt = deleteStmt(metadata, partitionColumnCount);

        // select-order index of every column, for UPDATE/cell-delete binding
        Map<String, Integer> selectOrderIndex = new HashMap<>();
        {
            Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder();
            for (int i = 0; it.hasNext(); i++)
                selectOrderIndex.put(it.next().name.toString(), i);
        }
        List<ColumnMetadata> regularColumns = ImmutableList.copyOf(metadata.regularColumns());
        List<ColumnMetadata> staticColumns = ImmutableList.copyOf(metadata.staticColumns());

        // small explicit-timestamp pool: cross-sstable same-timestamp conflicts on
        // overwritten primary keys exercise the reconciliation tie-break rules
        long tiePoolBase = 1_000_000;

        List<ByteBuffer[]> rows = new ArrayList<>();
        int rounds = 2 + workload.nextInt(3); // 2-4 sstables
        for (int round = 0; round < rounds; round++)
        {
            int inserts = 15 + workload.nextInt(26); // 15-40 rows
            for (int i = 0; i < inserts; i++)
            {
                ByteBuffer[] row = dataGen.generate(qtRandom);
                boolean overwrite = !rows.isEmpty() && workload.nextInt(100) < 30;
                if (overwrite)
                {
                    // overwrite: keep a previously used primary key, fresh non-key values —
                    // this is what makes the merge actually reconcile rather than concatenate
                    ByteBuffer[] prev = rows.get(workload.nextInt(rows.size()));
                    System.arraycopy(prev, 0, row, 0, primaryColumnCount);
                }

                int mode = workload.nextInt(100);
                if (overwrite && workload.nextInt(100) < 40)
                {
                    // explicit-timestamp collision candidate: two writes to the same primary
                    // key with the same timestamp force the same-ts tie-break path
                    long ts = tiePoolBase + workload.nextInt(3);
                    execute(insertStmt + " USING TIMESTAMP " + ts, (Object[]) row);
                }
                else if (mode < 15 && !regularColumns.isEmpty())
                {
                    // UPDATE: writes cells without primary-key liveness (different row flags)
                    execute(updateStmt(metadata, regularColumns),
                            updateParams(row, regularColumns, selectOrderIndex, primaryColumnCount));
                }
                else if (mode < 22)
                {
                    // primary-key-only INSERT: row liveness with zero cells
                    execute(pkOnlyInsertStmt(metadata), (Object[]) Arrays.copyOf(row, primaryColumnCount));
                }
                else if (mode < 30)
                {
                    // long TTL: liveness info with ttl + expiration far from the runs
                    execute(insertStmt + " USING TTL " + SOAK_TTL_SECONDS, (Object[]) row);
                }
                else
                {
                    execute(insertStmt, (Object[]) row);
                }
                rows.add(row);
            }

            // row deletes against known keys
            for (int i = 0; i < 3 && !rows.isEmpty(); i++)
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                execute(deleteRowStmt, (Object[]) Arrays.copyOf(victim, primaryColumnCount));
            }

            // range deletes (clustering tables only): single-sided slices and clustering-prefix
            // deletes against known keys; single-sided bounds cannot produce inverted ranges
            for (int i = 0; i < 2 && clusteringColumnCount > 0 && !rows.isEmpty(); i++)
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                if (clusteringColumnCount >= 2 && workload.nextBoolean())
                {
                    // prefix delete: equality on a strict prefix of the clustering columns
                    int depth = 1 + workload.nextInt(clusteringColumnCount - 1);
                    execute(deleteStmt(metadata, partitionColumnCount + depth),
                            (Object[]) Arrays.copyOf(victim, partitionColumnCount + depth));
                }
                else
                {
                    int eqDepth = workload.nextInt(clusteringColumnCount);
                    String op = new String[]{ ">=", ">", "<=", "<" }[workload.nextInt(4)];
                    execute(rangeDeleteStmt(metadata, eqDepth, op),
                            (Object[]) Arrays.copyOf(victim, partitionColumnCount + eqDepth + 1));
                }
            }

            // cell deletes: random subset of regular columns at a known row; occasionally a
            // static cell delete instead
            for (int i = 0; i < 2 && !rows.isEmpty(); i++)
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                if (!staticColumns.isEmpty() && workload.nextInt(100) < 30)
                {
                    ColumnMetadata col = staticColumns.get(workload.nextInt(staticColumns.size()));
                    execute(cellDeleteStmt(metadata, List.of(col), partitionColumnCount),
                            (Object[]) Arrays.copyOf(victim, partitionColumnCount));
                }
                else
                {
                    List<ColumnMetadata> subset = randomSubset(regularColumns, workload);
                    execute(cellDeleteStmt(metadata, subset, primaryColumnCount),
                            (Object[]) Arrays.copyOf(victim, primaryColumnCount));
                }
            }

            // occasional partition delete
            if (workload.nextInt(100) < 40 && !rows.isEmpty())
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                execute(deletePartitionStmt, (Object[]) Arrays.copyOf(victim, partitionColumnCount));
            }
            flush(KEYSPACE, metadata.name);
        }

        assertCursorMatchesIterator(cfs);
    }

    /**
     * Counter-table example: random clustering depth (0-2), 1-3 counter columns, optional
     * static counter; rounds of increments with random deltas plus cell, static-cell, row,
     * and partition deletes — the CASSANDRA-7346 tombstone-vs-increment interleavings arise
     * naturally from delete-then-increment round ordering.
     */
    private void runCounterExample(long seed, Random workload) throws Throwable
    {
        int clusterings = workload.nextInt(3);
        int counters = 1 + workload.nextInt(3);
        boolean staticCounter = clusterings > 0 && workload.nextBoolean();

        StringBuilder schema = new StringBuilder("CREATE TABLE %s (pk bigint");
        for (int i = 0; i < clusterings; i++)
            schema.append(", ck").append(i).append(" bigint");
        for (int i = 0; i < counters; i++)
            schema.append(", c").append(i).append(" counter");
        if (staticCounter)
            schema.append(", cs counter static");
        schema.append(", PRIMARY KEY (pk");
        for (int i = 0; i < clusterings; i++)
            schema.append(", ck").append(i);
        schema.append(")) WITH gc_grace_seconds = 864000");
        createTable(schema.toString());
        logger.info("randomizedDifferential seed={} counter schema:\n{}", seed, schema);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        StringBuilder where = new StringBuilder(" WHERE pk = ?");
        for (int i = 0; i < clusterings; i++)
            where.append(" AND ck").append(i).append(" = ?");

        int rounds = 2 + workload.nextInt(3);
        for (int round = 0; round < rounds; round++)
        {
            int writes = 15 + workload.nextInt(26);
            for (int w = 0; w < writes; w++)
            {
                Object[] key = new Object[1 + clusterings];
                key[0] = (long) workload.nextInt(4);
                for (int i = 0; i < clusterings; i++)
                    key[1 + i] = (long) workload.nextInt(6);

                int op = workload.nextInt(100);
                if (op < 70)
                {
                    int col = workload.nextInt(counters);
                    Object[] args = new Object[1 + key.length];
                    args[0] = (long) (workload.nextInt(200) - 100);
                    System.arraycopy(key, 0, args, 1, key.length);
                    execute("UPDATE %s SET c" + col + " = c" + col + " + ?" + where, args);
                }
                else if (op < 80 && staticCounter)
                {
                    execute("UPDATE %s SET cs = cs + ? WHERE pk = ?", (long) (workload.nextInt(50) - 25), key[0]);
                }
                else if (op < 90)
                {
                    int col = workload.nextInt(counters);
                    execute("DELETE c" + col + " FROM %s" + where, key);
                }
                else if (op < 96)
                {
                    execute("DELETE FROM %s" + where, key);
                }
                else
                {
                    execute("DELETE FROM %s WHERE pk = ?", key[0]);
                }
            }
            flush();
        }

        assertCursorMatchesIterator(cfs);
    }

    /** Appends {@code columns[i].name}, joined by separator. */
    private static void appendNames(StringBuilder sb, List<ColumnMetadata> columns, String separator)
    {
        for (int i = 0; i < columns.size(); i++)
        {
            if (i > 0) sb.append(separator);
            sb.append(columns.get(i).name.toCQLString());
        }
    }

    /** Appends "name = ?" equality predicates over columns, joined by separator. */
    private static void appendEqPredicates(StringBuilder sb, List<ColumnMetadata> columns, String separator)
    {
        for (int i = 0; i < columns.size(); i++)
        {
            if (i > 0) sb.append(separator);
            sb.append(columns.get(i).name.toCQLString()).append(" = ?");
        }
    }

    /** Appends {@code count} "?" placeholders, joined by ", ". */
    private static void appendPlaceholders(StringBuilder sb, int count)
    {
        for (int i = 0; i < count; i++)
        {
            if (i > 0) sb.append(", ");
            sb.append('?');
        }
    }

    private static String insertStmt(TableMetadata metadata)
    {
        List<ColumnMetadata> cols = ImmutableList.copyOf(metadata.allColumnsInSelectOrder());
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(metadata).append(" (");
        appendNames(sb, cols, ", ");
        sb.append(") VALUES (");
        appendPlaceholders(sb, cols.size());
        return sb.append(')').toString();
    }

    /** INSERT binding only the primary key columns: row liveness without any cells. */
    private static String pkOnlyInsertStmt(TableMetadata metadata)
    {
        List<ColumnMetadata> keys = primaryKeyColumns(metadata);
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(metadata).append(" (");
        appendNames(sb, keys, ", ");
        sb.append(") VALUES (");
        appendPlaceholders(sb, keys.size());
        return sb.append(')').toString();
    }

    /** UPDATE setting every regular column: cells without primary-key liveness. */
    private static String updateStmt(TableMetadata metadata, List<ColumnMetadata> regularColumns)
    {
        StringBuilder sb = new StringBuilder("UPDATE ").append(metadata).append(" SET ");
        appendEqPredicates(sb, regularColumns, ", ");
        sb.append(" WHERE ");
        appendEqPredicates(sb, primaryKeyColumns(metadata), " AND ");
        return sb.toString();
    }

    private static Object[] updateParams(ByteBuffer[] row, List<ColumnMetadata> regularColumns,
                                         Map<String, Integer> selectOrderIndex, int primaryColumnCount)
    {
        Object[] params = new Object[regularColumns.size() + primaryColumnCount];
        for (int i = 0; i < regularColumns.size(); i++)
            params[i] = row[selectOrderIndex.get(regularColumns.get(i).name.toString())];
        for (int i = 0; i < primaryColumnCount; i++)
            params[regularColumns.size() + i] = row[i];
        return params;
    }

    /** DELETE col1, col2 FROM t WHERE first {@code keyColumnCount} primary key columns bound. */
    private static String cellDeleteStmt(TableMetadata metadata, List<ColumnMetadata> columns, int keyColumnCount)
    {
        StringBuilder sb = new StringBuilder("DELETE ");
        appendNames(sb, columns, ", ");
        sb.append(" FROM ").append(metadata).append(" WHERE ");
        appendEqPredicates(sb, primaryKeyColumns(metadata).subList(0, keyColumnCount), " AND ");
        return sb.toString();
    }

    /**
     * DELETE with equality on the partition key plus the first {@code eqDepth} clustering
     * columns, and a single-sided {@code op} bound on clustering column {@code eqDepth}.
     */
    private static String rangeDeleteStmt(TableMetadata metadata, int eqDepth, String op)
    {
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(metadata).append(" WHERE ");
        List<ColumnMetadata> keys = primaryKeyColumns(metadata);
        int partitionColumnCount = metadata.partitionKeyColumns().size();
        int bound = partitionColumnCount + eqDepth;
        appendEqPredicates(sb, keys.subList(0, bound), " AND ");
        sb.append(" AND ").append(keys.get(bound).name.toCQLString()).append(' ').append(op).append(" ?");
        return sb.toString();
    }

    private static List<ColumnMetadata> primaryKeyColumns(TableMetadata metadata)
    {
        return ImmutableList.<ColumnMetadata>builder()
                            .addAll(metadata.partitionKeyColumns())
                            .addAll(metadata.clusteringColumns())
                            .build();
    }

    private static List<ColumnMetadata> randomSubset(List<ColumnMetadata> columns, Random workload)
    {
        List<ColumnMetadata> shuffled = new ArrayList<>(columns);
        java.util.Collections.shuffle(shuffled, workload);
        return shuffled.subList(0, 1 + workload.nextInt(shuffled.size()));
    }

    /** DELETE with the first {@code keyColumnCount} primary key columns bound (partition or full row). */
    private static String deleteStmt(TableMetadata metadata, int keyColumnCount)
    {
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(metadata).append(" WHERE ");
        appendEqPredicates(sb, primaryKeyColumns(metadata).subList(0, keyColumnCount), " AND ");
        return sb.toString();
    }

    /** Seed chaining copied from RandomSchemaTest so failures reproduce the same way. */
    private static final class SeedRunner
    {
        private static final long multiplier = 0x5DEECE66DL;
        private static final long addend = 0xBL;
        private static final long mask = (1L << 48) - 1;

        private long seed = CassandraRelevantProperties.TEST_DIFFERENTIAL_SEED.getLong(System.currentTimeMillis());
        private final int examples;

        SeedRunner(int examples)
        {
            this.examples = examples;
        }

        /** Dead code on purpose: plug a failing seed in here to reproduce. */
        @SuppressWarnings("unused")
        SeedRunner withFixedSeed(long seed)
        {
            this.seed = seed;
            return this;
        }

        interface SeededTest
        {
            void run(long seed) throws Throwable;
        }

        void run(SeededTest test) throws Throwable
        {
            for (int i = 0; i < examples; i++)
            {
                if (i > 0)
                    seed = (seed * multiplier + addend) & mask;
                try
                {
                    test.run(seed);
                }
                catch (AssumptionViolatedException a)
                {
                    // an Assume skip has to stay a skip: JUnit decides skip-vs-fail on the type
                    // thrown, not on its cause, so wrapping this below would turn the soak red
                    throw a;
                }
                catch (Throwable t)
                {
                    // keep the cause's detail in the message: junit XML only preserves the
                    // top-level message reliably
                    throw new AssertionError("Failure for seed " + seed + " (example " + i + "): " + t.getMessage(), t);
                }
            }
        }
    }
}
