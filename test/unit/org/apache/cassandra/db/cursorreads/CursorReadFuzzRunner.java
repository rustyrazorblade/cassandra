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

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Column;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.FilterKind;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.FuzzCase;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Op;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.OpKind;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Query;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Rng;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Schema;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Shape;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.SliceSpec;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Type;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.splitmix64;

/**
 * Seeded, replayable fuzzing of the cursor read path over {@link CursorReadOracle}.
 * <p>
 * A case is a generated table (partition key, clustering columns in either order, statics,
 * scalar, frozen, tuple, vector and non-frozen collection columns, or counters), a history of
 * writes and every deletion kind at explicit timestamps and local deletion times, flushes to BIG,
 * BTI or both, an optional memtable on top, and schema changes; then a list of reads.  Each read
 * is compared on S1, S2 and S4-S6, and some also through the internal pager, from a resume point,
 * and as a CQL SELECT (S3).  On top of the oracle, each read checks the cursor counters against
 * what the read allows (see {@link #checkMechanisms}).
 * <p>
 * The case seed of case {@code i} is {@code splitmix64(master ^ i)}; it is logged before the case
 * runs, and {@code -Dcassandra.test.cursor_fuzz.case_seed=<seed>} replays that case alone.  A
 * failing case is shrunk (layout and history, then the read) and written as a JUnit method to
 * {@code build/test/cursor-fuzz-repros/<caseSeed>.java}.
 * <p>
 * Each read and case records the cells of a coverage table; a run fails if a required cell was
 * hit fewer than {@code min_cell_hits} times.
 */
public abstract class CursorReadFuzzRunner extends CursorReadOracle
{
    private static final Logger logger = LoggerFactory.getLogger(CursorReadFuzzRunner.class);

    /** Bump when a change makes an old case seed generate a different case. */
    static final int GENERATOR_VERSION = 1;
    static final String[] PROFILES = { "big", "bti", "mixed" };

    private static final Path REPRO_DIR = Paths.get("build", "test", "cursor-fuzz-repros");

    /** What the read in progress allows the cursor counters to do; null outside a fuzz read. */
    private ReadContext current;
    /** Null while shrinking, so replays do not count. */
    protected Coverage coverage = new Coverage();

    private SSTableFormat<?, ?> savedFormat;
    private int savedColumnIndexKiB, savedWarn, savedFail;

    @Before
    public void saveFuzzConfig()
    {
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        savedColumnIndexKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedWarn = DatabaseDescriptor.getTombstoneWarnThreshold();
        savedFail = DatabaseDescriptor.getTombstoneFailureThreshold();
    }

    @After
    public void restoreFuzzConfig()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(savedFormat);
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexKiB);
        DatabaseDescriptor.setTombstoneWarnThreshold(savedWarn);
        DatabaseDescriptor.setTombstoneFailureThreshold(savedFail);
    }

    /** The settings of one run. */
    static final class RunSettings
    {
        final long master;
        final String profile;
        final int maxCases;
        final long durationSeconds;
        final int rows;
        final int queries;
        /** Failing cases to collect before the run stops. */
        final int maxFailures;

        RunSettings(long master, String profile, int maxCases, long durationSeconds, int rows, int queries)
        {
            this(master, profile, maxCases, durationSeconds, rows, queries, 1);
        }

        RunSettings(long master, String profile, int maxCases, long durationSeconds, int rows, int queries, int maxFailures)
        {
            this.maxFailures = maxFailures;
            this.master = master;
            this.profile = profile;
            this.maxCases = maxCases;
            this.durationSeconds = durationSeconds;
            this.rows = rows;
            this.queries = queries;
        }

        String profileOf(int caseIndex)
        {
            return profile.equals("all") ? PROFILES[caseIndex % PROFILES.length] : profile;
        }

        @Override
        public String toString()
        {
            return String.format("master=%dL profile=%s max_cases=%d duration=%ds rows=%d queries=%d generator=%d",
                                 master, profile, maxCases, durationSeconds, rows, queries, GENERATOR_VERSION);
        }
    }

    static FuzzCase generate(RunSettings settings, int caseIndex)
    {
        long caseSeed = splitmix64(settings.master ^ caseIndex);
        return CursorReadFuzzGenerator.generate(caseSeed, settings.profileOf(caseIndex), settings.rows, settings.queries);
    }

    /**
     * Runs cases until {@code maxCases} or the duration runs out.  A failing case is shrunk and
     * rethrown with its seed and repro.
     *
     * @return the number of cases run
     */
    protected int fuzz(RunSettings settings)
    {
        logger.info("cursor fuzz run: {}", settings);
        long start = System.nanoTime();
        long lastProgress = start;
        int cases = 0;
        List<AssertionError> failures = new ArrayList<>();
        for (int i = 0; i < settings.maxCases; i++)
        {
            long elapsed = System.nanoTime() - start;
            if (settings.durationSeconds > 0 && elapsed >= settings.durationSeconds * 1_000_000_000L)
                break;
            FuzzCase fc = generate(settings, i);
            logger.info("cursor fuzz case {}: caseSeed={}L profile={} master={}L generator={}",
                        i, fc.caseSeed, fc.profile, settings.master, GENERATOR_VERSION);
            try
            {
                runReportingFailure(fc);
            }
            catch (AssertionError failure)
            {
                failures.add(failure);
                logger.error("cursor fuzz case {} failed", i, failure);
                if (failures.size() >= settings.maxFailures)
                    break;
            }
            cases++;
            if (System.nanoTime() - lastProgress > 60_000_000_000L)
            {
                lastProgress = System.nanoTime();
                logger.info("cursor fuzz progress: {} cases in {}s", cases, (lastProgress - start) / 1_000_000_000L);
            }
        }
        double minutes = (System.nanoTime() - start) / 60e9;
        logger.info("cursor fuzz run done: {} cases in {} min, {} cases per minute", cases, String.format("%.2f", minutes),
                    String.format("%.1f", cases / Math.max(minutes, 1e-9)));
        if (failures.size() == 1)
            throw failures.get(0);
        if (!failures.isEmpty())
        {
            AssertionError all = new AssertionError(failures.size() + " cursor fuzz cases failed:" + System.lineSeparator()
                                                    + failures.stream().map(Throwable::getMessage).collect(Collectors.joining(System.lineSeparator())));
            failures.forEach(all::addSuppressed);
            throw all;
        }
        return cases;
    }

    /** Runs one case; on failure, shrinks it and throws with the seed and the repro. */
    protected void runReportingFailure(FuzzCase fc)
    {
        try
        {
            runCase(fc);
        }
        catch (CaseFailure failure)
        {
            throw report(fc, failure);
        }
    }

    // ---------------------------------------------------------------- one case

    /** A case failed at one of its reads. */
    static final class CaseFailure extends RuntimeException
    {
        final int queryIndex;

        CaseFailure(int queryIndex, Throwable cause)
        {
            super(cause);
            this.queryIndex = queryIndex;
        }

        EnumSet<Surface> surfaces()
        {
            return getCause() instanceof Divergence ? ((Divergence) getCause()).surfaces : EnumSet.noneOf(Surface.class);
        }
    }

    protected void runCase(FuzzCase fc)
    {
        Schema s = fc.schema;
        DatabaseDescriptor.setColumnIndexSizeInKiB(s.columnIndexKiB);
        createTable(s.createTable());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        try
        {
            for (Op op : fc.ops)
            {
                try
                {
                    apply(s, cfs, op);
                }
                catch (Throwable t)
                {
                    throw new CaseFailure(-1, new AssertionError("the case could not be set up at " + op, t));
                }
            }
            if (coverage != null)
                recordCase(fc, cfs);
            for (int i = 0; i < fc.queries.size(); i++)
            {
                try
                {
                    runQuery(fc, i, cfs);
                }
                catch (Throwable t)
                {
                    throw new CaseFailure(i, t);
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setSelectedSSTableFormat(savedFormat);
            DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexKiB);
            dropTable("DROP TABLE %s");
        }
    }

    // ---------------------------------------------------------------- history

    private void apply(Schema s, ColumnFamilyStore cfs, Op op) throws Exception
    {
        switch (op.kind)
        {
            case FLUSH:
                DatabaseDescriptor.setSelectedSSTableFormat(op.format);
                flush();
                return;
            case COMPACT:
                DatabaseDescriptor.setSelectedSSTableFormat(op.format);
                cfs.forceMajorCompaction();
                return;
            case MARK_REPAIRED:
            {
                List<SSTableReader> live = new ArrayList<>(cfs.getLiveSSTables());
                if (live.isEmpty())
                    return;
                live.sort(Comparator.comparing(r -> r.descriptor.id.toString()));
                SSTableReader sstable = live.get((int) Long.remainderUnsigned(op.valueSeed, live.size()));
                sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, FBUtilities.nowInSeconds(), null, false);
                sstable.reloadSSTableMetadata();
                return;
            }
            case DROP_COLUMN:
            {
                Column c = s.valueColumns().get(op.column);
                if (cfs.metadata().getColumn(ByteBufferUtil.bytes(c.name)) != null)
                    alterTable("ALTER TABLE %s DROP " + c.name + " USING TIMESTAMP " + op.timestamp);
                return;
            }
            case ADD_COLUMN:
            {
                Column c = s.valueColumns().get(op.column);
                if (cfs.metadata().getColumn(ByteBufferUtil.bytes(c.name)) == null)
                    alterTable("ALTER TABLE %s ADD " + c.name + ' ' + c.type.cql);
                return;
            }
            default:
        }
        if (s.counters)
        {
            applyCounter(s, op);
            return;
        }

        TableMetadata m = cfs.metadata();
        PartitionUpdate.Builder update = new PartitionUpdate.Builder(m, key(s, m, op.partition), m.regularAndStaticColumns(), 1);
        Rng r = new Rng(op.valueSeed);
        List<Column> values = s.valueColumns();
        Column single = op.column >= 0 ? values.get(op.column) : null;
        boolean staticTarget = single != null && op.column < s.statics.size();
        switch (op.kind)
        {
            case INSERT:
            case UPDATE:
            {
                Row.Builder row = rowBuilder(m, staticTarget || s.clustering.isEmpty() ? null : op.clustering, s);
                if (op.kind == OpKind.INSERT)
                    row.addPrimaryKeyLivenessInfo(LivenessInfo.create(op.timestamp, op.ttl, op.writeNow));
                boolean any = false;
                for (Column c : s.regulars)
                {
                    if (r.chance(op.kind == OpKind.INSERT ? 0.7 : 0.5) || (!any && c == s.regulars.get(s.regulars.size() - 1) && op.kind == OpKind.UPDATE))
                        any |= addValue(m, row, c, op, r, s.padding, true);
                }
                update.add(row.build());
                break;
            }
            case STATIC_WRITE:
            {
                Row.Builder row = rowBuilder(m, null, s);
                for (Column c : s.statics)
                    if (r.chance(0.7) || c == s.statics.get(0))
                        addValue(m, row, c, op, r, s.padding, true);
                update.add(row.build());
                break;
            }
            case CELL_DELETE:
            {
                ColumnMetadata column = m.getColumn(ByteBufferUtil.bytes(single.name));
                if (column == null)
                    return;
                Row.Builder row = rowBuilder(m, staticTarget ? null : op.clustering, s);
                row.addCell(BufferCell.tombstone(column, op.timestamp, op.writeNow));
                update.add(row.build());
                break;
            }
            case ROW_DELETE:
            {
                Row.Builder row = rowBuilder(m, op.clustering, s);
                row.addRowDeletion(Row.Deletion.regular(DeletionTime.build(op.timestamp, op.writeNow)));
                update.add(row.build());
                break;
            }
            case RANGE_DELETE:
            {
                Slice slice = slice(m.comparator, s, op.clustering, op.end, op.startInclusive, op.endInclusive);
                if (slice == null)
                    return;
                update.add(new RangeTombstone(slice, DeletionTime.build(op.timestamp, op.writeNow)));
                break;
            }
            case PARTITION_DELETE:
                update.addPartitionDeletion(DeletionTime.build(op.timestamp, op.writeNow));
                break;
            case COLLECTION_APPEND:
            case COLLECTION_REPLACE:
            case ELEMENT_DELETE:
            case COLLECTION_DELETE:
            {
                ColumnMetadata column = m.getColumn(ByteBufferUtil.bytes(single.name));
                if (column == null)
                    return;
                Row.Builder row = rowBuilder(m, staticTarget ? null : op.clustering, s);
                if (op.kind == OpKind.COLLECTION_DELETE)
                    row.addComplexDeletion(column, DeletionTime.build(op.timestamp, op.writeNow));
                else if (op.kind == OpKind.ELEMENT_DELETE)
                    row.addCell(BufferCell.tombstone(column, op.timestamp, op.writeNow, elementPath(single.type, r.pickIndex(20))));
                else
                    addValue(m, row, single, op, r, s.padding, op.kind == OpKind.COLLECTION_REPLACE);
                update.add(row.build());
                break;
            }
            default:
                throw new AssertionError(op.kind);
        }
        new Mutation(update.build()).applyUnsafe();
    }

    private void applyCounter(Schema s, Op op)
    {
        Rng r = new Rng(op.valueSeed);
        List<Object> keyValues = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE ");
        for (int i = 0; i < s.partitionKey.size(); i++)
        {
            where.append(i == 0 ? "" : " AND ").append(s.partitionKey.get(i).name).append(" = ?");
            keyValues.add(partitionKeyValue(s.partitionKey.get(i), op.partition));
        }
        boolean withClustering = op.kind != OpKind.PARTITION_DELETE;
        if (withClustering)
        {
            for (int i = 0; i < s.clustering.size(); i++)
            {
                where.append(" AND ").append(s.clustering.get(i).name).append(" = ?");
                keyValues.add(clusteringValue(s.clustering.get(i), op.clustering[i]));
            }
        }
        switch (op.kind)
        {
            case COUNTER_ADD:
            {
                List<Object> bound = new ArrayList<>();
                String set = s.regulars.stream().map(c -> c.name + " = " + c.name + " + ?").collect(Collectors.joining(", "));
                for (Column ignored : s.regulars)
                    bound.add((long) r.between(-5, 10));
                bound.addAll(keyValues);
                execute("UPDATE %s SET " + set + where, bound.toArray());
                return;
            }
            case CELL_DELETE:
                execute("DELETE " + s.regulars.get(op.column).name + " FROM %s" + where, keyValues.toArray());
                return;
            case ROW_DELETE:
            case PARTITION_DELETE:
                execute("DELETE FROM %s" + where, keyValues.toArray());
                return;
            default:
                throw new AssertionError(op.kind);
        }
    }

    private static Row.Builder rowBuilder(TableMetadata m, int[] clustering, Schema s)
    {
        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(clustering == null ? (s.clustering.isEmpty() ? Clustering.EMPTY : Clustering.STATIC_CLUSTERING)
                                      : m.comparator.make(clusteringValues(s, clustering)));
        return row;
    }

    /** Adds a value for {@code c}; a multi-cell column gets one to three elements.  False if the column is dropped. */
    private static boolean addValue(TableMetadata m, Row.Builder row, Column c, Op op, Rng r, int padding, boolean replace)
    {
        ColumnMetadata column = m.getColumn(ByteBufferUtil.bytes(c.name));
        if (column == null)
            return false;
        if (!c.type.multiCell())
        {
            row.addCell(cell(column, op, scalarValue(column, c.type, r, padding), null));
            return true;
        }
        if (replace)
            row.addComplexDeletion(column, DeletionTime.build(op.timestamp - 1, op.writeNow));
        int elements = r.between(1, 3);
        for (int i = 0; i < elements; i++)
        {
            int element = r.pickIndex(20);
            ByteBuffer value = c.type == Type.SET ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                                                  : UTF8Type.instance.decompose((c.type == Type.MAP ? "m" : "l") + r.pickIndex(40));
            row.addCell(cell(column, op, value, elementPath(c.type, element)));
        }
        return true;
    }

    private static BufferCell cell(ColumnMetadata column, Op op, ByteBuffer value, CellPath path)
    {
        return op.ttl > 0 ? BufferCell.expiring(column, op.timestamp, op.ttl, op.writeNow, value, path)
                          : BufferCell.live(column, op.timestamp, value, path);
    }

    /** The cell path of element {@code element} of a set, map or list; elements come from a small domain so deletes hit. */
    private static CellPath elementPath(Type type, int element)
    {
        if (type == Type.LIST)
            return CellPath.create(TimeUUID.atUnixMicrosWithLsb(1_000_000L + element, 0x8000000000000000L | element).toBytes());
        return CellPath.create(Int32Type.instance.decompose(element));
    }

    /**
     * A value from a small domain, so row filters match some rows and timestamp ties compare
     * equal values.  The same seed gives the same value.
     */
    @SuppressWarnings("unchecked")
    static ByteBuffer scalarValue(ColumnMetadata column, Type type, Rng r, int padding)
    {
        int x = r.pickIndex(40);
        AbstractType<Object> t = (AbstractType<Object>) column.type;
        switch (type)
        {
            case INT: return t.decompose(x);
            case BIGINT: return t.decompose(1000L * x);
            case TEXT: return t.decompose("t" + x + "x".repeat(padding));
            case BLOB:
            {
                ByteBuffer b = ByteBuffer.allocate(4 + padding);
                b.putInt(x);
                return (ByteBuffer) b.rewind();
            }
            case DOUBLE: return t.decompose(x / 4.0);
            case BOOLEAN: return t.decompose(x % 2 == 0);
            case FROZEN_LIST:
            {
                List<Integer> list = new ArrayList<>();
                for (int i = x % 4; i > 0; i--)
                    list.add(r.pickIndex(10));
                return t.decompose(list);
            }
            case FROZEN_MAP:
            {
                Map<Integer, String> map = new TreeMap<>();
                for (int i = x % 4; i > 0; i--)
                    map.put(r.pickIndex(10), "f" + r.pickIndex(10));
                return t.decompose(map);
            }
            case TUPLE: return ((TupleType) column.type).pack(Int32Type.instance.decompose(x), UTF8Type.instance.decompose("u" + r.pickIndex(10)));
            case VECTOR: return t.decompose(Arrays.asList((float) x, (float) r.pickIndex(10)));
            default: throw new AssertionError(type);
        }
    }

    static Object partitionKeyValue(Column c, int partition)
    {
        switch (c.type)
        {
            case INT: return partition * 7;
            case BIGINT: return partition * 7L;
            case TEXT: return "p" + partition;
            default: throw new AssertionError(c.type);
        }
    }

    static Object clusteringValue(Column c, int index)
    {
        switch (c.type)
        {
            case INT: return index;
            case BIGINT: return (long) index;
            case TEXT: return String.format("c%06d", index + 10);
            default: throw new AssertionError(c.type);
        }
    }

    private static Object[] clusteringValues(Schema s, int[] indexes)
    {
        Object[] values = new Object[indexes.length];
        for (int i = 0; i < indexes.length; i++)
            values[i] = clusteringValue(s.clustering.get(i), indexes[i]);
        return values;
    }

    static DecoratedKey key(Schema s, TableMetadata m, int partition)
    {
        Object[] values = new Object[s.partitionKey.size()];
        for (int i = 0; i < values.length; i++)
            values[i] = partitionKeyValue(s.partitionKey.get(i), partition);
        return m.partitioner.decorateKey(m.partitionKeyAsClusteringComparator().make(values).serializeAsPartitionKey());
    }

    /** A slice of two prefixes (null is open), with the bounds swapped if they are out of order; null if empty. */
    private static Slice slice(ClusteringComparator comparator, Schema s, int[] start, int[] end, boolean startInclusive, boolean endInclusive)
    {
        ClusteringBound<?> from = bound(comparator, s, start, true, startInclusive);
        ClusteringBound<?> to = bound(comparator, s, end, false, endInclusive);
        if (comparator.compare(from, to) > 0)
        {
            from = bound(comparator, s, end, true, endInclusive);
            to = bound(comparator, s, start, false, startInclusive);
        }
        Slice slice = Slice.make(from, to);
        return slice.isEmpty(comparator) ? null : slice;
    }

    private static ClusteringBound<?> bound(ClusteringComparator comparator, Schema s, int[] prefix, boolean isStart, boolean inclusive)
    {
        if (prefix == null)
            return isStart ? ClusteringBound.BOTTOM : ClusteringBound.TOP;
        return ClusteringBound.create(comparator, isStart, inclusive, clusteringValues(s, prefix));
    }

    // ---------------------------------------------------------------- reads

    static SinglePartitionReadCommand command(Schema s, TableMetadata m, Query q, long nowInSec)
    {
        ClusteringIndexFilter filter;
        switch (q.shape)
        {
            case ALL:
                filter = new ClusteringIndexSliceFilter(Slices.ALL, q.reversed);
                break;
            case SLICE:
            case MULTI_SLICE:
            {
                Slices.Builder builder = new Slices.Builder(m.comparator);
                for (SliceSpec spec : q.slices)
                {
                    Slice slice = slice(m.comparator, s, spec.start, spec.end, spec.startInclusive, spec.endInclusive);
                    if (slice != null)
                        builder.add(slice);
                }
                Slices slices = builder.build();
                filter = new ClusteringIndexSliceFilter(slices.isEmpty() ? Slices.ALL : slices, q.reversed);
                break;
            }
            case NAMES:
            {
                NavigableSet<Clustering<?>> names = new TreeSet<>(m.comparator);
                if (s.clustering.isEmpty())
                    names.add(Clustering.EMPTY);
                for (int[] name : q.names)
                    names.add(m.comparator.make(clusteringValues(s, name)));
                filter = new ClusteringIndexNamesFilter(names, q.reversed);
                break;
            }
            case STATIC_ONLY:
                filter = new ClusteringIndexNamesFilter(new TreeSet<>(m.comparator), q.reversed);
                break;
            default:
                throw new AssertionError(q.shape);
        }

        RowFilter rowFilter = rowFilter(s, m, q);
        ColumnFilter columns = columnFilter(s, m, q, rowFilter);
        DataLimits limits = q.perPartitionLimit > 0 ? DataLimits.cqlLimits(q.limit > 0 ? q.limit : DataLimits.NO_LIMIT, q.perPartitionLimit)
                          : q.limit > 0 ? DataLimits.cqlLimits(q.limit) : DataLimits.NONE;
        return SinglePartitionReadCommand.create(m, nowInSec, columns, rowFilter, limits, key(s, m, q.partition), filter);
    }

    private static List<ColumnMetadata> presentColumns(Schema s, TableMetadata m)
    {
        List<ColumnMetadata> present = new ArrayList<>();
        for (Column c : s.valueColumns())
        {
            ColumnMetadata column = m.getColumn(ByteBufferUtil.bytes(c.name));
            if (column != null)
                present.add(column);
        }
        return present;
    }

    private static ColumnFilter columnFilter(Schema s, TableMetadata m, Query q, RowFilter rowFilter)
    {
        List<ColumnMetadata> present = presentColumns(s, m);
        if (q.columns == CursorReadFuzzModel.Columns.ALL || present.isEmpty())
            return ColumnFilter.all(m);
        Rng r = new Rng(q.columnsSeed);
        List<ColumnMetadata> chosen = new ArrayList<>();
        for (ColumnMetadata c : present)
            if (r.chance(0.5))
                chosen.add(c);
        if (chosen.isEmpty())
            chosen.add(r.pick(present));
        // the filtered column must be fetched
        for (RowFilter.Expression e : rowFilter.getExpressions())
            if (!e.column().isPrimaryKeyColumn() && !chosen.contains(e.column()))
                chosen.add(e.column());
        switch (q.columns)
        {
            case SELECTION:
                return ColumnFilter.selectionBuilder().addAll(chosen).build();
            case FETCH_ALL_QUERY_SOME:
                return ColumnFilter.allRegularColumnsBuilder(m, r.chance(0.5)).addAll(chosen).build();
            case ELEMENT_SELECTION:
            {
                ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(m, false).addAll(chosen);
                for (Column c : s.valueColumns())
                {
                    ColumnMetadata column = m.getColumn(ByteBufferUtil.bytes(c.name));
                    if (column == null || (c.type != Type.SET && c.type != Type.MAP))
                        continue;
                    int a = r.pickIndex(20), b = r.pickIndex(20);
                    if (r.chance(0.5))
                        builder.select(column, elementPath(c.type, a));
                    else
                        builder.slice(column, elementPath(c.type, Math.min(a, b)), elementPath(c.type, Math.max(a, b)));
                }
                return builder.build();
            }
            default:
                throw new AssertionError(q.columns);
        }
    }

    private static RowFilter rowFilter(Schema s, TableMetadata m, Query q)
    {
        if (q.filter == null)
            return RowFilter.none();
        Rng r = new Rng(q.filterSeed);
        RowFilter filter = RowFilter.create(q.filter == FilterKind.NEEDS_RECONCILIATION);
        switch (q.filter)
        {
            case PARTITION_KEY_EQ:
            {
                Column c = s.partitionKey.get(0);
                Object value = partitionKeyValue(c, r.chance(0.7) ? q.partition : r.pickIndex(s.partitions + 1));
                filter.add(m.getColumn(ByteBufferUtil.bytes(c.name)), Operator.EQ, decompose(m, c.name, value));
                return filter;
            }
            case CLUSTERING_EQ:
            case CLUSTERING_GT:
            {
                Column c = s.clustering.get(0);
                Object value = clusteringValue(c, r.between(-1, 2 * s.domain[0]));
                filter.add(m.getColumn(ByteBufferUtil.bytes(c.name)), q.filter == FilterKind.CLUSTERING_EQ ? Operator.EQ : Operator.GT,
                           decompose(m, c.name, value));
                return filter;
            }
            case COUNTER_EQ:
            {
                ColumnMetadata column = m.getColumn(ByteBufferUtil.bytes(s.regulars.get(0).name));
                filter.add(column, Operator.EQ, ByteBufferUtil.bytes((long) r.between(-5, 20)));
                return filter;
            }
            case STATIC_EQ:
            case REGULAR_EQ:
            case NEEDS_RECONCILIATION:
            case REGULAR_RANGE:
            {
                List<Column> candidates = new ArrayList<>();
                for (Column c : q.filter == FilterKind.STATIC_EQ ? s.statics : s.regulars)
                    if ((q.filter == FilterKind.REGULAR_RANGE ? c.type.numeric() : c.type.scalar()) && m.getColumn(ByteBufferUtil.bytes(c.name)) != null)
                        candidates.add(c);
                if (candidates.isEmpty())
                    return RowFilter.none();
                Column c = r.pick(candidates);
                ColumnMetadata column = m.getColumn(ByteBufferUtil.bytes(c.name));
                Operator op = q.filter == FilterKind.REGULAR_RANGE ? r.pick(Operator.GT, Operator.LTE) : Operator.EQ;
                filter.add(column, op, scalarValue(column, c.type, r, s.padding));
                return filter;
            }
            case SET_CONTAINS:
            case MAP_ELEMENT:
            {
                Type type = q.filter == FilterKind.SET_CONTAINS ? Type.SET : Type.MAP;
                List<Column> candidates = new ArrayList<>();
                for (Column c : s.valueColumns())
                    if (c.type == type && m.getColumn(ByteBufferUtil.bytes(c.name)) != null)
                        candidates.add(c);
                if (candidates.isEmpty())
                    return RowFilter.none();
                ColumnMetadata column = m.getColumn(ByteBufferUtil.bytes(r.pick(candidates).name));
                if (type == Type.SET)
                    filter.add(column, Operator.CONTAINS, Int32Type.instance.decompose(r.pickIndex(20)));
                else
                    filter.addMapEquality(column, Int32Type.instance.decompose(r.pickIndex(20)), Operator.EQ,
                                          UTF8Type.instance.decompose("m" + r.pickIndex(40)));
                return filter;
            }
            default:
                throw new AssertionError(q.filter);
        }
    }

    @SuppressWarnings("unchecked")
    private static ByteBuffer decompose(TableMetadata m, String column, Object value)
    {
        return ((AbstractType<Object>) m.getColumn(ByteBufferUtil.bytes(column)).type).decompose(value);
    }

    private static String literal(Column c, Object value)
    {
        return c.type == Type.TEXT ? '\'' + (String) value + '\'' : String.valueOf(value);
    }

    /** A CQL SELECT of the read's partition, built from the read's seed and the current schema. */
    static String cql(Schema s, TableMetadata m, Query q)
    {
        Rng r = new Rng(q.cqlSeed);
        StringBuilder where = new StringBuilder();
        for (int i = 0; i < s.partitionKey.size(); i++)
        {
            Column c = s.partitionKey.get(i);
            where.append(i == 0 ? " WHERE " : " AND ").append(c.name).append(" = ").append(literal(c, partitionKeyValue(c, q.partition)));
        }
        List<String> present = presentColumns(s, m).stream().map(c -> c.name.toString()).collect(Collectors.toList());
        List<String> scalarRegulars = s.regulars.stream().filter(c -> c.type.scalar() && present.contains(c.name))
                                                 .map(c -> c.name).collect(Collectors.toList());
        String selection;
        boolean count = false;
        switch (r.weighted(60, 10, 20, s.counters || scalarRegulars.isEmpty() ? 0 : 10))
        {
            case 1: selection = "COUNT(*)"; count = true; break;
            case 2:
            {
                List<String> chosen = new ArrayList<>();
                if (!s.clustering.isEmpty() && r.chance(0.5))
                    chosen.add(s.clustering.get(0).name);
                for (String name : present)
                    if (r.chance(0.5))
                        chosen.add(name);
                // a selection of statics only may not restrict the clustering
                if (!chosen.isEmpty() && !s.clustering.isEmpty() && s.statics.stream().map(c -> c.name).collect(Collectors.toList()).containsAll(chosen))
                    chosen.add(0, s.clustering.get(0).name);
                selection = chosen.isEmpty() ? "*" : String.join(", ", chosen);
                break;
            }
            case 3:
            {
                String v = r.pick(scalarRegulars);
                selection = s.partitionKey.get(0).name + ", " + v + ", WRITETIME(" + v + "), TTL(" + v + ')';
                break;
            }
            default: selection = "*";
        }
        if (!s.clustering.isEmpty())
        {
            Column ck = s.clustering.get(0);
            int domain = 2 * s.domain[0];
            switch (r.weighted(40, 30, 15, 15))
            {
                case 1:
                    where.append(" AND ").append(ck.name).append(r.pick(" >= ", " > ")).append(literal(ck, clusteringValue(ck, r.between(-1, domain))));
                    if (r.chance(0.7))
                        where.append(" AND ").append(ck.name).append(r.pick(" <= ", " < ")).append(literal(ck, clusteringValue(ck, r.between(-1, domain))));
                    break;
                case 2:
                {
                    TreeSet<Integer> in = new TreeSet<>();
                    for (int i = r.between(1, 4); i > 0; i--)
                        in.add(r.between(-1, domain));
                    where.append(" AND ").append(ck.name).append(" IN (")
                         .append(in.stream().map(x -> literal(ck, clusteringValue(ck, x))).collect(Collectors.joining(", "))).append(')');
                    break;
                }
                case 3:
                    where.append(" AND ").append(ck.name).append(" = ").append(literal(ck, clusteringValue(ck, r.between(-1, domain))));
                    break;
                default:
            }
        }
        StringBuilder sb = new StringBuilder("SELECT ").append(selection).append(" FROM %s").append(where);
        if (q.reversed && !s.clustering.isEmpty() && !count)
            sb.append(" ORDER BY ").append(s.clustering.stream().map(c -> c.name + (c.desc ? " ASC" : " DESC")).collect(Collectors.joining(", ")));
        if (q.perPartitionLimit > 0 && !count)
            sb.append(" PER PARTITION LIMIT ").append(q.perPartitionLimit);
        if (q.limit > 0)
            sb.append(" LIMIT ").append(q.limit);
        return sb.toString();
    }

    /** What one read lets the cursor counters do. */
    private static final class ReadContext
    {
        final Query query;
        final boolean allLegsBig;
        boolean baseRead;

        ReadContext(Query query, boolean allLegsBig)
        {
            this.query = query;
            this.allLegsBig = allLegsBig;
        }
    }

    private void runQuery(FuzzCase fc, int index, ColumnFamilyStore cfs)
    {
        Schema s = fc.schema;
        Query q = fc.queries.get(index);
        if (q.tombstoneWarn > 0)
        {
            DatabaseDescriptor.setTombstoneWarnThreshold(q.tombstoneWarn);
            DatabaseDescriptor.setTombstoneFailureThreshold(q.tombstoneFail);
        }
        try
        {
            LongFunction<SinglePartitionReadCommand> command = now -> command(s, cfs.metadata(), q, now);
            SinglePartitionReadCommand probe = command.apply(q.nowInSec);
            List<SSTableReader> candidates = liveSSTablesFor(cfs, probe);
            boolean rejected = droppedMultiCellColumnInHeader(cfs.metadata(), candidates);
            current = new ReadContext(q, !candidates.isEmpty() && candidates.stream().allMatch(r -> r instanceof BigTableReader));

            ReadCase c = ReadCase.of(String.format("fuzz case %dL read %d (%s%s)", fc.caseSeed, index, q.shape.label,
                                                   q.reversed ? " reversed" : ""), q.seed).at(q.nowInSec);
            if (q.cold)
                c = c.cold();
            if (rejected)
                c = c.rejectedBecause(UnsupportedReason.DROPPED_COLLECTION_OR_COUNTER_IN_HEADER);
            if (coverage != null)
                recordRead(fc, q, cfs, probe, candidates, rejected);

            current.baseRead = true;
            assertAllReadSurfacesMatch(c, cfs, command);
            current.baseRead = false;

            if (q.pageSize > 0)
                assertPagedMatches(c, cfs, command, q.pageSize);
            if (q.resumeAfter != null)
            {
                Clustering<?> last = cfs.metadata().comparator.make(clusteringValues(s, q.resumeAfter));
                LongFunction<SinglePartitionReadCommand> resumed = now -> {
                    SinglePartitionReadCommand base = command.apply(now);
                    return base.forPaging(last, base.limits().forPaging(q.resumeLimit));
                };
                assertAllReadSurfacesMatch(c.named(c.label + " resumed after " + Arrays.toString(q.resumeAfter)), cfs, resumed);
            }
            if (q.cqlSeed != 0)
            {
                String query = cql(s, cfs.metadata(), q);
                if (q.cqlAsReplica)
                    assertCqlReplicaResponsesMatch(c, cfs, query, q.cqlPageSize);
                else
                    assertCqlMatches(c, cfs, query, q.cqlPageSize);
            }
        }
        finally
        {
            current = null;
            DatabaseDescriptor.setTombstoneWarnThreshold(savedWarn);
            DatabaseDescriptor.setTombstoneFailureThreshold(savedFail);
        }
    }

    /** The support gate's one deliberate rejection: a dropped multi-cell column still in a candidate sstable's header. */
    private static boolean droppedMultiCellColumnInHeader(TableMetadata m, List<SSTableReader> candidates)
    {
        for (SSTableReader sstable : candidates)
            for (ColumnMetadata column : sstable.header.columns())
                if (m.getColumn(column.name) == null && (column.isComplex() || sstable.header.getType(column).isMultiCell()))
                    return true;
        return false;
    }

    // ---------------------------------------------------------------- invariants

    /**
     * Checks the cursor counters against what the read allows: a filter the cursor path can never
     * push down never engages the pushdown; a read with no limit and no filter never stops a merge early;
     * a partition the filter skips materializes nothing; a read whose legs are all BIG never seeks.
     */
    @Override
    protected String checkMechanisms(ReadCase c, EnumSet<Surface> surfaces, Observation iterator, Observation cursor)
    {
        ReadContext context = current;
        if (context == null)
            return null;
        if (coverage != null)
            cursor.counters.forEach((name, moved) -> {
                if (moved > 0)
                    coverage.hit("cursor counter moved: " + name);
            });
        if (coverage != null && cursor.failure != null && cursor.failure.contains("TombstoneOverwhelmingException"))
            coverage.hit("tombstone failure");
        if (c.unsupported != null)
            return null;
        Query q = context.query;
        List<String> problems = new ArrayList<>();
        if (q.filter != null && q.filter.neverPushedDown() && cursor.counters.get("filterPushdownEngaged") != 0)
            problems.add("filter pushdown engaged for a " + q.filter + " filter, which can never be pushed down");
        // a row filter that drops the partition also closes the merge early, so only reads with no filter count;
        // so does a read aborted above the merge (the tombstone guard), when both paths failed the same way
        boolean bothFailedAlike = cursor.failure != null && cursor.failure.equals(iterator.failure);
        if (context.baseRead && q.limit < 0 && q.perPartitionLimit < 0 && q.filter == null && !bothFailedAlike
            && cursor.counters.get("mergesStoppedByLimit") != 0)
            problems.add("a merge stopped at a limit on a read with no limit and no row filter");
        if (context.baseRead && surfaces.contains(Surface.S1) && cursor.counters.get("partitionsSkippedByFilter") != 0
            && cursor.counters.get("unfilteredsMaterialized") != 0)
            problems.add("a partition the filter skipped still materialized " + cursor.counters.get("unfilteredsMaterialized") + " unfiltereds");
        if (context.allLegsBig && cursor.counters.get("sstableLegRowIndexSeeks") != 0)
            problems.add("BIG legs sought " + cursor.counters.get("sstableLegRowIndexSeeks") + " times; only BTI legs seek");
        return problems.isEmpty() ? null : String.join(System.lineSeparator(), problems);
    }

    // ---------------------------------------------------------------- coverage

    /** Counts how often each cell of the coverage table was hit. */
    static final class Coverage
    {
        final Map<String, Long> hits = new TreeMap<>();

        void hit(String cell)
        {
            hits.merge(cell, 1L, Long::sum);
        }

        long hits(String cell)
        {
            return hits.getOrDefault(cell, 0L);
        }

        /** The cells below {@code minimum} hits. */
        List<String> missing(List<String> required, long minimum)
        {
            return required.stream().filter(cell -> hits(cell) < minimum).collect(Collectors.toList());
        }

        String table(List<String> required)
        {
            StringBuilder sb = new StringBuilder(String.format("%-70s %10s%n", "cell", "hits"));
            TreeSet<String> all = new TreeSet<>(hits.keySet());
            all.addAll(required);
            for (String cell : all)
                sb.append(String.format("%-70s %10d%s%n", cell, hits(cell), required.contains(cell) ? "" : "  (not required)"));
            return sb.toString();
        }
    }

    /** Every cell a run must hit. */
    static List<String> requiredCells()
    {
        List<String> cells = new ArrayList<>();
        for (String profile : PROFILES)
            for (String direction : new String[]{ "forward", "reversed" })
                for (Shape shape : Shape.values())
                    cells.add("read: " + profile + ' ' + direction + ' ' + shape.label);
        for (String cell : new String[]{
            "layout: memtable only", "layout: sstables only", "layout: sstables and memtable",
            "layout: 1 sstable", "layout: 2-5 sstables", "layout: 6+ sstables", "layout: partition with a row index",
            "layout: repaired sstable", "layout: compaction",
            "schema: counters", "schema: no clustering", "schema: 2+ clustering columns", "schema: desc clustering",
            "schema: 2-column partition key", "schema: statics", "schema: non-frozen collection", "schema: frozen collection",
            "schema: tuple", "schema: vector", "schema: dropped and re-added column", "schema: dropped column not re-added",
            "compression: none", "compression: LZ4Compressor", "compression: ZstdCompressor", "compression: DeflateCompressor",
            "compression: SnappyCompressor", "memtable: default", "memtable: skiplist", "memtable: trie", "memtable: skiplist_sharded",
            "column index: 1 KiB", "column index: 4 KiB", "column index: 64 KiB",
            "read: limit 1", "read: limit 2-10", "read: limit above rows", "read: per-partition limit",
            "read: column selection", "read: fetch all, query some", "read: collection element selection", "read: cold caches",
            "read: absent key, no sstable covers it", "read: absent key, 1 sstable covers it", "read: absent key, 2+ sstables cover it",
            "read: support gate rejects", "read: at expiry - 1", "read: at expiry", "read: at expiry + 1",
            "read: at purge time - 1", "read: at purge time", "read: at purge time + 1", "read: tombstone thresholds set",
            "paging: page size 1", "paging: page size 2-9", "paging: page size 10+",
            "paging: resume at a written clustering", "paging: resume between written clusterings",
            "paging: resume before the first clustering", "paging: resume after the last clustering",
            "cql: unpaged", "cql: paged", "cql: as replica responses", "tombstone failure",
            "history: range deletion closing at TOP", "history: range deletion opening at BOTTOM",
            "read: slice from a bound to TOP", "read: slice from BOTTOM to a bound" })
            cells.add(cell);
        for (OpKind kind : OpKind.values())
            if (kind != OpKind.DROP_COLUMN && kind != OpKind.ADD_COLUMN && kind != OpKind.MARK_REPAIRED && kind != OpKind.COMPACT)
                cells.add("history: " + kind);
        for (FilterKind kind : FilterKind.values())
            cells.add("filter: " + kind);
        for (String counter : new String[]{ "cursorMergesServed", "sstableLegsCursorMerged", "memtableLegsCursorMerged",
                                            "namesTimestampOrderReads", "sstableLegRowIndexSeeks",
                                            "filterPushdownEngaged", "partitionsSkippedByFilter", "rowsDroppedByFilter",
                                            "rowsDroppedByRegularColumnFilter", "mergesStoppedByLimit", "memtableRowsReused",
                                            "memtableCellsReused", "unfilteredsMaterialized" })
            cells.add("cursor counter moved: " + counter);
        return cells;
    }

    private void recordCase(FuzzCase fc, ColumnFamilyStore cfs)
    {
        Schema s = fc.schema;
        int sstables = fc.sstablesFlushed();
        boolean memtable = !cfs.getTracker().getView().getCurrentMemtable().isClean();
        coverage.hit(sstables == 0 ? "layout: memtable only" : memtable ? "layout: sstables and memtable" : "layout: sstables only");
        if (sstables > 0)
            coverage.hit(sstables == 1 ? "layout: 1 sstable" : sstables <= 5 ? "layout: 2-5 sstables" : "layout: 6+ sstables");
        if (cfs.getLiveSSTables().stream().anyMatch(SSTableReader::isRepaired))
            coverage.hit("layout: repaired sstable");
        if (fc.ops.stream().anyMatch(op -> op.kind == OpKind.COMPACT))
            coverage.hit("layout: compaction");
        if (s.counters)
            coverage.hit("schema: counters");
        if (s.clustering.isEmpty())
            coverage.hit("schema: no clustering");
        if (s.clustering.size() >= 2)
            coverage.hit("schema: 2+ clustering columns");
        if (s.clustering.stream().anyMatch(c -> c.desc))
            coverage.hit("schema: desc clustering");
        if (s.partitionKey.size() == 2)
            coverage.hit("schema: 2-column partition key");
        if (!s.statics.isEmpty())
            coverage.hit("schema: statics");
        if (s.valueColumns().stream().anyMatch(c -> c.type.multiCell()))
            coverage.hit("schema: non-frozen collection");
        if (s.valueColumns().stream().anyMatch(c -> c.type.frozenCollection()))
            coverage.hit("schema: frozen collection");
        if (s.valueColumns().stream().anyMatch(c -> c.type == Type.TUPLE))
            coverage.hit("schema: tuple");
        if (s.valueColumns().stream().anyMatch(c -> c.type == Type.VECTOR))
            coverage.hit("schema: vector");
        boolean dropped = fc.ops.stream().anyMatch(op -> op.kind == OpKind.DROP_COLUMN);
        boolean added = fc.ops.stream().anyMatch(op -> op.kind == OpKind.ADD_COLUMN);
        if (dropped)
            coverage.hit(added ? "schema: dropped and re-added column" : "schema: dropped column not re-added");
        coverage.hit("compression: " + (s.compression == null ? "none" : s.compression));
        coverage.hit("memtable: " + (s.memtable == null ? "default" : s.memtable));
        coverage.hit("column index: " + s.columnIndexKiB + " KiB");
        for (Op op : fc.ops)
        {
            coverage.hit("history: " + op.kind);
            if (op.kind == OpKind.RANGE_DELETE && op.clustering != null && op.end == null)
                coverage.hit("history: range deletion closing at TOP");
            if (op.kind == OpKind.RANGE_DELETE && op.clustering == null && op.end != null)
                coverage.hit("history: range deletion opening at BOTTOM");
        }
    }

    private void recordRead(FuzzCase fc, Query q, ColumnFamilyStore cfs, SinglePartitionReadCommand probe,
                            List<SSTableReader> candidates, boolean rejected)
    {
        Schema s = fc.schema;
        coverage.hit("read: " + fc.profile + ' ' + (q.reversed ? "reversed" : "forward") + ' ' + q.shape.label);
        for (SliceSpec spec : q.slices)
        {
            if (spec.start != null && spec.end == null)
                coverage.hit("read: slice from a bound to TOP");
            if (spec.start == null && spec.end != null)
                coverage.hit("read: slice from BOTTOM to a bound");
        }
        if (q.limit == 1)
            coverage.hit("read: limit 1");
        else if (q.limit > s.rows())
            coverage.hit("read: limit above rows");
        else if (q.limit > 1)
            coverage.hit("read: limit 2-10");
        if (q.perPartitionLimit > 0)
            coverage.hit("read: per-partition limit");
        switch (q.columns)
        {
            case SELECTION: coverage.hit("read: column selection"); break;
            case FETCH_ALL_QUERY_SOME: coverage.hit("read: fetch all, query some"); break;
            case ELEMENT_SELECTION: coverage.hit("read: collection element selection"); break;
            default:
        }
        if (q.filter != null && !probe.rowFilter().isEmpty())
            coverage.hit("filter: " + q.filter);
        if (q.cold)
            coverage.hit("read: cold caches");
        if (q.tombstoneWarn > 0)
            coverage.hit("read: tombstone thresholds set");
        if (rejected)
            coverage.hit("read: support gate rejects");
        if (q.partition >= s.partitions)
            coverage.hit(candidates.isEmpty() ? "read: absent key, no sstable covers it"
                                              : candidates.size() == 1 ? "read: absent key, 1 sstable covers it"
                                                                       : "read: absent key, 2+ sstables cover it");
        for (SSTableReader sstable : candidates)
        {
            AbstractRowIndexEntry entry = sstable.getRowIndexEntry(probe.partitionKey(), SSTableReader.Operator.EQ);
            if (entry != null && entry.isIndexed())
            {
                coverage.hit("layout: partition with a row index");
                break;
            }
        }
        for (Op op : fc.ops)
        {
            if (op.ttl > 0)
            {
                long expiry = op.writeNow + op.ttl;
                if (q.nowInSec == expiry - 1)
                    coverage.hit("read: at expiry - 1");
                else if (q.nowInSec == expiry)
                    coverage.hit("read: at expiry");
                else if (q.nowInSec == expiry + 1)
                    coverage.hit("read: at expiry + 1");
            }
            if (op.kind.deletion())
            {
                long purge = op.writeNow + s.gcGrace;
                if (q.nowInSec == purge - 1)
                    coverage.hit("read: at purge time - 1");
                else if (q.nowInSec == purge)
                    coverage.hit("read: at purge time");
                else if (q.nowInSec == purge + 1)
                    coverage.hit("read: at purge time + 1");
            }
        }
        if (q.pageSize > 0)
            coverage.hit(q.pageSize == 1 ? "paging: page size 1" : q.pageSize < 10 ? "paging: page size 2-9" : "paging: page size 10+");
        if (q.resumeAfter != null)
        {
            int last = q.resumeAfter[0];
            int domain = 2 * s.domain[0];
            coverage.hit(last < 0 ? "paging: resume before the first clustering"
                                  : last >= domain ? "paging: resume after the last clustering"
                                                   : last % 2 == 0 ? "paging: resume at a written clustering"
                                                                   : "paging: resume between written clusterings");
        }
        if (q.cqlSeed != 0)
        {
            coverage.hit(q.cqlPageSize > 0 ? "cql: paged" : "cql: unpaged");
            if (q.cqlAsReplica)
                coverage.hit("cql: as replica responses");
        }
    }

    /** Logs the coverage table and fails if a required cell has fewer than {@code minimum} hits. */
    protected void assertCoverage(long minimum)
    {
        List<String> required = requiredCells();
        logger.info("cursor fuzz coverage table:{}{}", System.lineSeparator(), coverage.table(required));
        List<String> missing = coverage.missing(required, minimum);
        if (!missing.isEmpty())
            throw new AssertionError("coverage cells hit fewer than " + minimum + " times: " + missing);
    }

    // ---------------------------------------------------------------- shrinking

    private AssertionError report(FuzzCase fc, CaseFailure failure)
    {
        Coverage saved = coverage;
        coverage = null;
        String shrunkJava;
        try
        {
            FuzzCase shrunk = shrink(fc, failure);
            shrunkJava = shrunk.toJava();
            writeRepro(fc.caseSeed, shrunkJava);
        }
        catch (Throwable t)
        {
            shrunkJava = "(shrinking failed: " + t + ')';
        }
        finally
        {
            coverage = saved;
        }
        return new AssertionError(String.format("cursor fuzz case failed at read %d: caseSeed=%dL profile=%s generator=%d%n" +
                                                "replay with -Dcassandra.test.cursor_fuzz.case_seed=%d " +
                                                "-Dcassandra.test.cursor_fuzz.profile=%s%nshrunk repro (also in %s):%n%s",
                                                failure.queryIndex, fc.caseSeed, fc.profile, GENERATOR_VERSION, fc.caseSeed,
                                                fc.profile, REPRO_DIR.resolve(Long.toUnsignedString(fc.caseSeed) + ".java"), shrunkJava),
                                  failure.getCause());
    }

    /** Whether {@code candidate} still fails the way the original failed. */
    private boolean stillFails(FuzzCase candidate, CaseFailure original)
    {
        try
        {
            runCase(candidate);
            return false;
        }
        catch (CaseFailure failure)
        {
            if (original.getCause() instanceof Divergence)
            {
                EnumSet<Surface> overlap = failure.surfaces();
                overlap.retainAll(original.surfaces());
                return !overlap.isEmpty();
            }
            return failure.getCause().getClass() == original.getCause().getClass();
        }
        catch (Throwable t)
        {
            return false;
        }
    }

    /** Shrinks the reads, then the history and layout (ddmin), then the failing read. */
    private FuzzCase shrink(FuzzCase failing, CaseFailure failure)
    {
        int budget = 400;
        FuzzCase best = failing;
        if (failure.queryIndex >= 0)
        {
            FuzzCase alone = failing.withQueries(List.of(failing.queries.get(failure.queryIndex)));
            if (stillFails(alone, failure))
                best = alone;
            else
                best = failing.withQueries(failing.queries.subList(0, failure.queryIndex + 1));
        }

        List<Op> ops = new ArrayList<>(best.ops);
        int n = 2;
        while (ops.size() >= 2 && budget > 0)
        {
            int chunk = (ops.size() + n - 1) / n;
            boolean reduced = false;
            for (int i = 0; i < ops.size() && budget > 0; i += chunk)
            {
                List<Op> complement = new ArrayList<>(ops.subList(0, i));
                complement.addAll(ops.subList(Math.min(ops.size(), i + chunk), ops.size()));
                budget--;
                if (stillFails(best.withOps(complement), failure))
                {
                    ops = complement;
                    n = Math.max(n - 1, 2);
                    reduced = true;
                    break;
                }
            }
            if (!reduced)
            {
                if (n >= ops.size())
                    break;
                n = Math.min(ops.size(), 2 * n);
            }
        }
        best = best.withOps(ops);

        if (best.queries.size() == 1)
        {
            boolean progress = true;
            while (progress && budget > 0)
            {
                progress = false;
                for (Query simpler : best.queries.get(0).simpler())
                {
                    budget--;
                    FuzzCase candidate = best.withQueries(List.of(simpler));
                    if (stillFails(candidate, failure))
                    {
                        best = candidate;
                        progress = true;
                        break;
                    }
                }
            }
        }
        return best;
    }

    private static void writeRepro(long caseSeed, String java)
    {
        try
        {
            Files.createDirectories(REPRO_DIR);
            Files.writeString(REPRO_DIR.resolve(Long.toUnsignedString(caseSeed) + ".java"), java);
        }
        catch (Exception e)
        {
            logger.warn("could not write the cursor fuzz repro", e);
        }
    }

    /** For tests of this class: the cells a run hit. */
    Map<String, Long> coverageHits()
    {
        return new LinkedHashMap<>(coverage.hits);
    }
}
