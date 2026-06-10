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
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.quicktheories.core.Gen;
import org.quicktheories.impl.JavaRandom;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.Generators;

import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;

/**
 * Randomized soak for the cursor-vs-iterator differential harness: random schemas restricted to
 * the currently supported cursor compaction surface (the same unsupportedMetadata filter
 * production uses), random multi-round workloads with overwrites and deletes flushed into
 * overlapping sstables, then byte+logical differential comparison of both compaction paths.
 *
 * Reproducing a failure: every failure message is wrapped in a seed; plug it into
 * {@code withFixedSeed} below.
 *
 * Known coverage gaps (deliberate, journaled in doc/cursor-compaction-plan.md): null/empty
 * value domains and range deletes are exercised by the deterministic corpus only.
 */
public class RandomDifferentialCompactionTest extends DifferentialCompactionTester
{
    static
    {
        // make sure generated blobs are deterministic per seed
        CassandraRelevantProperties.TEST_BLOB_SHARED_SEED.setInt(42);
    }

    private static final Set<String> ALLOWLIST = Set.of();
    private static final int EXAMPLES = 10;

    @Test
    public void randomizedDifferential() throws Throwable
    {
        new SeedRunner(EXAMPLES).run(this::runOneExample);
    }

    private void runOneExample(long seed) throws Throwable
    {
        JavaRandom qtRandom = new JavaRandom(seed);
        Random workload = new Random(seed);

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
                       .withPartitionColumnsCount(1)
                       .withPrimaryColumnTypeGen(new TypeGenBuilder(safePrimary).withMaxDepth(1))
                       .withClusteringColumnsBetween(0, 3)
                       .withRegularColumnsBetween(1, 5)
                       .withStaticColumnsBetween(0, 2)
                       .build(qtRandom);
        }
        // Same filter production uses to route to the cursor pipeline. When increment 2 lands
        // (multi-cell columns), this loop disappears and the generator space widens.
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

        Gen<ByteBuffer[]> dataGen = CassandraGenerators.data(metadata, ignore -> AbstractTypeGenerators.ValueDomain.NORMAL);
        int partitionColumnCount = metadata.partitionKeyColumns().size();
        int primaryColumnCount = partitionColumnCount + metadata.clusteringColumns().size();
        String insertStmt = insertStmt(metadata);
        String deleteRowStmt = deleteStmt(metadata, primaryColumnCount);
        String deletePartitionStmt = deleteStmt(metadata, partitionColumnCount);

        List<ByteBuffer[]> rows = new ArrayList<>();
        int rounds = 2 + workload.nextInt(3); // 2-4 sstables
        for (int round = 0; round < rounds; round++)
        {
            int inserts = 15 + workload.nextInt(26); // 15-40 rows
            for (int i = 0; i < inserts; i++)
            {
                ByteBuffer[] row = dataGen.generate(qtRandom);
                if (!rows.isEmpty() && workload.nextInt(100) < 30)
                {
                    // overwrite: keep a previously used primary key, fresh non-key values —
                    // this is what makes the merge actually reconcile rather than concatenate
                    ByteBuffer[] prev = rows.get(workload.nextInt(rows.size()));
                    System.arraycopy(prev, 0, row, 0, primaryColumnCount);
                }
                execute(insertStmt, (Object[]) row);
                rows.add(row);
            }

            // row deletes against known keys
            for (int i = 0; i < 3 && !rows.isEmpty(); i++)
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                execute(deleteRowStmt, (Object[]) Arrays.copyOf(victim, primaryColumnCount));
            }
            // occasional partition delete
            if (workload.nextInt(100) < 40 && !rows.isEmpty())
            {
                ByteBuffer[] victim = rows.get(workload.nextInt(rows.size()));
                execute(deletePartitionStmt, (Object[]) Arrays.copyOf(victim, partitionColumnCount));
            }
            flush(KEYSPACE, metadata.name);
        }

        assertCursorMatchesIterator(cfs, ALLOWLIST);
    }

    private static String insertStmt(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(metadata).append(" (");
        Iterator<ColumnMetadata> cols = metadata.allColumnsInSelectOrder();
        while (cols.hasNext())
        {
            sb.append(cols.next().name.toCQLString());
            if (cols.hasNext()) sb.append(", ");
        }
        sb.append(") VALUES (");
        for (int i = 0; i < metadata.columns().size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append('?');
        }
        return sb.append(')').toString();
    }

    /** DELETE with the first {@code keyColumnCount} primary key columns bound (partition or full row). */
    private static String deleteStmt(TableMetadata metadata, int keyColumnCount)
    {
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(metadata).append(" WHERE ");
        List<ColumnMetadata> keys = ImmutableList.<ColumnMetadata>builder()
                                                 .addAll(metadata.partitionKeyColumns())
                                                 .addAll(metadata.clusteringColumns())
                                                 .build();
        for (int i = 0; i < keyColumnCount; i++)
        {
            if (i > 0) sb.append(" AND ");
            sb.append(keys.get(i).name.toCQLString()).append(" = ?");
        }
        return sb.toString();
    }

    /** Seed chaining copied from RandomSchemaTest so failures reproduce the same way. */
    private static final class SeedRunner
    {
        private static final long multiplier = 0x5DEECE66DL;
        private static final long addend = 0xBL;
        private static final long mask = (1L << 48) - 1;

        private long seed = System.currentTimeMillis();
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
