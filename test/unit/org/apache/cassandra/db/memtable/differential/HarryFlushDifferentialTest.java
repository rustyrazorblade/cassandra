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

package org.apache.cassandra.db.memtable.differential;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.MemtableCursorFlusher;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.execution.CQLTesterVisitExecutor;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.junit.Assert.assertTrue;

/**
 * True property-based coverage for cursor flush: unlike every other test in this package, BOTH
 * the schema (column count/types/reversed-clustering) and the operation sequence are randomly
 * generated per trial, using Harry's own generators - the same schema-generation infrastructure
 * {@code HarryDifferentialCompactionTest} uses for compaction. {@link RandomFlushDifferentialTest}
 * randomizes operations against one fixed schema; {@link MemtableFlushSupportMatrixTest}
 * exercises every type but via hand-picked, fixed scenarios. This is the one test where neither
 * the schema nor the workload is chosen by a human.
 * <p>
 * Unlike compaction's Harry test - which replays ONE history once, flushes it into several
 * sstables, and compacts those same sstables both ways - a live memtable can only be flushed
 * once (see {@link MemtableFlushDifferentialTester}'s own class doc), so this instead builds a
 * fresh pair of tables sharing an IDENTICAL schema (same column objects, same seed - only the
 * table name differs, which {@link SchemaSpec#valueGenerators} never depends on) per attempt, and
 * replays the SAME abstract operation sequence through two independent {@link CQLVisitExecutor}s,
 * one per table, before flushing each via its own path.
 * <p>
 * Every trial is fully determined by one {@code long trialSeed} ({@code withRandom(trialSeed, ...)}
 * derives the schema and the entire operation sequence from it) - so, exactly like
 * {@link RandomFlushDifferentialTest}, a single logged seed is always enough to replay one trial
 * in isolation, which both {@link #KNOWN_REGRESSION_SEEDS} and {@link #shrink} depend on.
 * <p>
 * Schema uses simple types only (the currently supported cursor surface; Harry cannot generate
 * multi-cell columns yet). Complex-column (collection/UDT) variety is covered by
 * {@code MemtableFlushSupportMatrixTest} instead. No Harry read-validation visits are issued -
 * the differential comparison against the iterator path is the oracle.
 */
public class HarryFlushDifferentialTest extends MemtableFlushDifferentialTester
{
    private static final Logger logger = LoggerFactory.getLogger(HarryFlushDifferentialTest.class);
    private static final AtomicInteger idGen = new AtomicInteger(0);

    private static final int TRIALS = 8;
    private static final int PARTITIONS = 20;
    private static final int ROWS = 15;
    private static final int OPS = 200;

    /** Bounds the number of extra flush-and-compare cycles a shrink phase can spend once a trial has already failed. */
    private static final int MAX_SHRINK_ATTEMPTS = 150;

    /**
     * Seeds of trials that previously failed this test, replayed as fixed trials on every run
     * before any fresh random exploration - see {@link RandomFlushDifferentialTest}'s identical
     * mechanism. Add an entry (with a comment: date, description) whenever a genuine bug is found
     * and fixed here; never remove an entry once added.
     */
    private static final long[] KNOWN_REGRESSION_SEEDS = {
        // (none yet)
    };

    @Test
    public void harryRandomSchemaAndHistory() throws Throwable
    {
        long seed = System.currentTimeMillis();
        logger.info("HarryFlushDifferentialTest seed={}", seed);

        for (long regressionSeed : KNOWN_REGRESSION_SEEDS)
            runTrialWithSeed(regressionSeed, "known-regression replay");

        Random seedPicker = new Random(seed);
        for (int trial = 0; trial < TRIALS; trial++)
            runTrialWithSeed(seedPicker.nextLong(), "trial " + trial);
    }

    private void runTrialWithSeed(long trialSeed, String label) throws Throwable
    {
        withRandom(trialSeed, rng -> runTrial(rng, trialSeed, label));
    }

    private void runTrial(EntropySource rng, long trialSeed, String label) throws Throwable
    {
        String ks = "harry_flush_ks" + idGen.incrementAndGet();
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
                                   "{'class': 'SimpleStrategy', 'replication_factor': '1'}", ks));

        List<ColumnSpec<?>> partitionKeys = randomPartitionKeys(rng, 1 + rng.nextInt(2));
        List<ColumnSpec<?>> clusteringKeys = randomClusteringKeys(rng, 1 + rng.nextInt(3));
        List<ColumnSpec<?>> regularColumns = randomRegularColumns(rng, 3 + rng.nextInt(6));
        List<ColumnSpec<?>> staticColumns = randomStaticColumns(rng, rng.nextInt(3));
        // Same seed reused for every schema built from these columns (see reproducesWith): a
        // SchemaSpec's valueGenerators depends only on the columns + seed, never on
        // keyspace/table, so any number of schemas sharing these columns/seed behave identically.
        long schemaSeed = rng.next();
        ColumnDefs columns = new ColumnDefs(ks, schemaSeed, partitionKeys, clusteringKeys, regularColumns, staticColumns);

        // A throwaway schema, never used to create a table, purely so HistoryBuilder has
        // valueGenerators and HistoryBuilderHelper/deleteRowRange have a clusteringKeys.size() to
        // work with while building the abstract operation sequence below.
        SchemaSpec referenceSchema = columns.schemaFor("unused_reference");
        HistoryBuilder history = new HistoryBuilder(referenceSchema.valueGenerators);
        for (int op = 0; op < OPS; op++)
        {
            int pd = rng.nextInt(PARTITIONS);
            int row = rng.nextInt(ROWS);
            int kind = rng.nextInt(100);
            if (kind < 65)
                history.insert(pd, row);
            else if (kind < 75)
                history.deleteRow(pd, row);
            else if (kind < 85)
            {
                int lower = rng.nextInt(ROWS);
                int upper = rng.nextInt(lower, 2 * ROWS);
                history.deleteRowRange(pd, lower, upper,
                                       rng.nextInt(referenceSchema.clusteringKeys.size()),
                                       rng.nextBoolean(),
                                       rng.nextBoolean());
            }
            else if (kind < 95)
                HistoryBuilderHelper.deleteRandomColumns(referenceSchema, pd, row, rng, history);
            else
                history.deletePartition(pd);
        }

        List<Visit> allVisits = new ArrayList<>();
        for (Visit visit : history)
            allVisits.add(visit);

        try
        {
            reproducesWith(columns, allVisits);
        }
        catch (Throwable t)
        {
            String shrunkDescription;
            try
            {
                List<Visit> shrunk = shrink(columns, allVisits);
                shrunkDescription = describeVisits(shrunk);
            }
            catch (Throwable shrinkFailure)
            {
                shrunkDescription = "(shrinking itself threw: " + shrinkFailure + ")";
            }
            throw new AssertionError(String.format(
                "%s failed. trialSeed=%dL, keyspace=%s%nOriginal history had %d operations across " +
                "a schema with %d partition key(s), %d clustering key(s), %d regular column(s), " +
                "%d static column(s).%n" +
                "Shrunk (minimal known reproducer found by delta-debugging, same schema): %s%n" +
                "If this is a genuine bug: once fixed, add trialSeed=%dL to KNOWN_REGRESSION_SEEDS " +
                "(with a comment) so it's replayed on every future run.",
                label, trialSeed, ks, allVisits.size(),
                partitionKeys.size(), clusteringKeys.size(), regularColumns.size(), staticColumns.size(),
                shrunkDescription, trialSeed), t);
        }
    }

    /**
     * Replays {@code visits} through a brand-new pair of tables (fresh names, same columns/seed)
     * via the iterator path then the cursor path, and asserts the flushed output matches. Used
     * both for a trial's original attempt and, via {@link #reproduces}, for every candidate
     * during {@link #shrink} - the shrink check is deliberately the exact same code path as the
     * original failure detection, not a separate approximation of it.
     */
    private void reproducesWith(ColumnDefs columns, List<Visit> visits) throws Throwable
    {
        String tableA = "tbl_iterator_" + idGen.incrementAndGet();
        String tableB = "tbl_cursor_" + idGen.incrementAndGet();
        SchemaSpec schemaA = columns.schemaFor(tableA);
        SchemaSpec schemaB = columns.schemaFor(tableB);

        createTable(schemaA.compile());
        createTable(schemaB.compile());
        ColumnFamilyStore cfsA = Keyspace.open(columns.keyspace).getColumnFamilyStore(tableA);
        ColumnFamilyStore cfsB = Keyspace.open(columns.keyspace).getColumnFamilyStore(tableB);
        cfsA.disableAutoCompaction();
        cfsB.disableAutoCompaction();

        Path scratch = Files.createTempDirectory("differential-flush-harry");
        try
        {
            DatabaseDescriptor.setCursorFlushEnabled(false);
            replay(schemaA, visits);
            flush(columns.keyspace, tableA);
            CapturedOutput iteratorOut = captureAll(cfsA, scratch.resolve("iterator"));

            DatabaseDescriptor.setCursorFlushEnabled(true);
            replay(schemaB, visits);
            assertTrue("trial's table/memtable doesn't satisfy MemtableCursorFlusher.isSupported " +
                       "- won't actually exercise the cursor path; fix the schema generation, not this assertion",
                       MemtableCursorFlusher.isSupported(cfsB.metadata(), cfsB.getCurrentMemtable()));
            flush(columns.keyspace, tableB);
            CapturedOutput cursorOut = captureAll(cfsB, scratch.resolve("cursor"));

            // Logical-only, not strict: a DELETE's local_delete_time is server-wall-clock-seconds
            // at processing time (see MemtableFlushDifferentialTester#assertFlushMatchesLogically),
            // and a several-hundred-statement Harry replay can genuinely straddle a second
            // boundary between the two sequential replay passes - observed flaking on rerun, not
            // hypothetical. Cell/row timestamps themselves ARE deterministic (Harry's own
            // WriteHelper/DeleteHelper set them explicitly), so ignoreCellTimestamps stays false.
            assertEquivalentOutputsLogically(iteratorOut, cursorOut, false);
        }
        finally
        {
            DatabaseDescriptor.setCursorFlushEnabled(false);
            FileUtils.deleteDirectory(scratch.toFile());
        }
    }

    private int shrinkAttemptsUsed;

    private boolean reproduces(ColumnDefs columns, List<Visit> visits)
    {
        if (visits.isEmpty() || shrinkAttemptsUsed >= MAX_SHRINK_ATTEMPTS)
            return false;
        shrinkAttemptsUsed++;
        try
        {
            reproducesWith(columns, visits);
            return false;
        }
        catch (Throwable t)
        {
            return true;
        }
    }

    /**
     * Delta-debugging over the operation list: repeatedly try removing contiguous chunks (halves,
     * then quarters, ... down to single operations), keeping any removal that still reproduces
     * the divergence via {@link #reproduces}, verified the exact same way the original failure
     * was detected - never a false minimization. One downward pass per chunk size (not full ddmin
     * restart-on-success), bounded by {@link #MAX_SHRINK_ATTEMPTS} total replay attempts, so a
     * large original history can't make the shrink phase run unboundedly long; if the cap is hit
     * first, whatever's been reduced so far is returned rather than continuing.
     */
    private List<Visit> shrink(ColumnDefs columns, List<Visit> failing)
    {
        shrinkAttemptsUsed = 0;
        List<Visit> current = new ArrayList<>(failing);
        for (int chunkSize = Math.max(1, current.size() / 2); chunkSize >= 1; chunkSize /= 2)
        {
            int i = 0;
            while (i < current.size() && shrinkAttemptsUsed < MAX_SHRINK_ATTEMPTS)
            {
                int end = Math.min(current.size(), i + chunkSize);
                List<Visit> candidate = new ArrayList<>(current);
                candidate.subList(i, end).clear();
                if (reproduces(columns, candidate))
                    current = candidate; // keep the reduction; re-check the same position against the shorter list
                else
                    i += chunkSize;
            }
            if (shrinkAttemptsUsed >= MAX_SHRINK_ATTEMPTS)
                break;
        }
        return current;
    }

    private static String describeVisits(List<Visit> visits)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(visits.size()).append(" operation(s):\n");
        for (Visit visit : visits)
            sb.append(visit).append('\n');
        return sb.toString();
    }

    private void replay(SchemaSpec schema, List<Visit> visits)
    {
        DataTracker tracker = new DataTracker.SequentialDataTracker();
        CQLVisitExecutor executor =
            new CQLTesterVisitExecutor(schema, tracker,
                                       new QuiescentChecker(schema.valueGenerators, tracker, new ReplayList(visits)),
                                       statement -> execute(statement.cql(), statement.bindings()));
        for (Visit visit : visits)
            executor.execute(visit);
    }

    /**
     * Adapts a plain {@code List<Visit>} (e.g. a shrunk subset) to {@link Model.Replay}, the type
     * {@link QuiescentChecker} requires - {@code HistoryBuilder} itself implements it, but a
     * post-shrink subset isn't a {@code HistoryBuilder}. Only ever consulted if a read/validation
     * {@code Visit} is replayed, which never happens here (write-only histories, no Harry
     * read-validation visits issued - see this class's own doc) - the lookups below exist only to
     * satisfy the interface, not because they're exercised.
     */
    private static final class ReplayList implements Model.Replay
    {
        private final List<Visit> visits;

        ReplayList(List<Visit> visits)
        {
            this.visits = visits;
        }

        @Override
        public Iterator<Visit> iterator()
        {
            return visits.iterator();
        }

        @Override
        public Visit replay(long lts)
        {
            for (Visit visit : visits)
                if (visit.lts == lts)
                    return visit;
            throw new IllegalStateException("No visit with lts=" + lts + " in this (possibly shrunk) history");
        }

        @Override
        public Operations.Operation replay(long lts, int opId)
        {
            return replay(lts).operations[opId];
        }
    }

    /** Everything needed to build any number of {@link SchemaSpec}s that share the same logical shape - see {@link #reproducesWith}. */
    private static final class ColumnDefs
    {
        final String keyspace;
        final long schemaSeed;
        final List<ColumnSpec<?>> partitionKeys;
        final List<ColumnSpec<?>> clusteringKeys;
        final List<ColumnSpec<?>> regularColumns;
        final List<ColumnSpec<?>> staticColumns;

        ColumnDefs(String keyspace, long schemaSeed,
                  List<ColumnSpec<?>> partitionKeys, List<ColumnSpec<?>> clusteringKeys,
                  List<ColumnSpec<?>> regularColumns, List<ColumnSpec<?>> staticColumns)
        {
            this.keyspace = keyspace;
            this.schemaSeed = schemaSeed;
            this.partitionKeys = partitionKeys;
            this.clusteringKeys = clusteringKeys;
            this.regularColumns = regularColumns;
            this.staticColumns = staticColumns;
        }

        SchemaSpec schemaFor(String table)
        {
            return new SchemaSpec(schemaSeed, 1000, keyspace, table, partitionKeys, clusteringKeys, regularColumns, staticColumns);
        }
    }

    /** Shared by the four random-column-list builders below, which differ only in which {@link ColumnSpec} factory they call per index. */
    private static List<ColumnSpec<?>> randomColumns(int count, IntFunction<ColumnSpec<?>> factory)
    {
        List<ColumnSpec<?>> columns = new ArrayList<>();
        for (int i = 0; i < count; i++)
            columns.add(factory.apply(i));
        return columns;
    }

    private static List<ColumnSpec<?>> randomPartitionKeys(EntropySource rng, int count)
    {
        return randomColumns(count, i -> ColumnSpec.pk("pk" + i, ColumnSpec.regularColumnTypeGen().generate(rng)));
    }

    private static List<ColumnSpec<?>> randomClusteringKeys(EntropySource rng, int count)
    {
        return randomColumns(count, i -> ColumnSpec.ck("ck" + i, ColumnSpec.clusteringColumnTypeGen().generate(rng), rng.nextBoolean()));
    }

    private static List<ColumnSpec<?>> randomRegularColumns(EntropySource rng, int count)
    {
        return randomColumns(count, i -> ColumnSpec.regularColumn("r" + i, ColumnSpec.regularColumnTypeGen().generate(rng)));
    }

    private static List<ColumnSpec<?>> randomStaticColumns(EntropySource rng, int count)
    {
        return randomColumns(count, i -> ColumnSpec.staticColumn("s" + i, ColumnSpec.regularColumnTypeGen().generate(rng)));
    }
}
