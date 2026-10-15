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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The data a {@link CursorReadFuzzRunner} case is made of: a schema, a history of operations, and
 * a list of reads.  Every field is plain data, so the shrinker can drop parts of a case and
 * {@link #toJava} can print a case as code that rebuilds it.
 */
final class CursorReadFuzzModel
{
    private CursorReadFuzzModel()
    {
    }

    /** splitmix64: the per-case seed of case {@code i} is {@code splitmix64(master ^ i)}. */
    static long splitmix64(long z)
    {
        z += 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** A splitmix64 stream. */
    static final class Rng
    {
        private long state;

        Rng(long seed)
        {
            state = seed;
        }

        long nextLong()
        {
            state += 0x9E3779B97F4A7C15L;
            long z = state;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        /** A value in {@code [0, n)}. */
        int pickIndex(int n)
        {
            if (n <= 0)
                throw new IllegalArgumentException("n must be positive: " + n);
            return (int) Long.remainderUnsigned(nextLong(), n);
        }

        /** A value in {@code [lo, hi]}, both inclusive. */
        int between(int lo, int hi)
        {
            return lo + pickIndex(hi - lo + 1);
        }

        boolean chance(double p)
        {
            return (nextLong() >>> 11) * 0x1.0p-53 < p;
        }

        @SafeVarargs
        final <T> T pick(T... values)
        {
            return values[pickIndex(values.length)];
        }

        <T> T pick(List<T> values)
        {
            return values.get(pickIndex(values.size()));
        }

        /** An index chosen by weight. */
        int weighted(int... weights)
        {
            int total = Arrays.stream(weights).sum();
            int x = pickIndex(total);
            for (int i = 0; i < weights.length; i++)
            {
                if (x < weights[i])
                    return i;
                x -= weights[i];
            }
            throw new AssertionError();
        }
    }

    enum Type
    {
        INT("int"), BIGINT("bigint"), TEXT("text"), BLOB("blob"), DOUBLE("double"), BOOLEAN("boolean"),
        FROZEN_LIST("frozen<list<int>>"), FROZEN_MAP("frozen<map<int, text>>"), TUPLE("tuple<int, text>"),
        VECTOR("vector<float, 2>"), SET("set<int>"), LIST("list<text>"), MAP("map<int, text>"), COUNTER("counter");

        final String cql;

        Type(String cql)
        {
            this.cql = cql;
        }

        boolean multiCell()
        {
            return this == SET || this == LIST || this == MAP;
        }

        boolean frozenCollection()
        {
            return this == FROZEN_LIST || this == FROZEN_MAP;
        }

        /** A plain value that a row filter can compare with EQ or a range operator. */
        boolean scalar()
        {
            return this == INT || this == BIGINT || this == TEXT || this == BLOB || this == DOUBLE || this == BOOLEAN;
        }

        boolean numeric()
        {
            return this == INT || this == BIGINT || this == DOUBLE;
        }
    }

    static final class Column
    {
        final String name;
        final Type type;
        final boolean desc;

        Column(String name, Type type, boolean desc)
        {
            this.name = name;
            this.type = type;
            this.desc = desc;
        }

        String toJava()
        {
            return String.format("new Column(\"%s\", Type.%s, %s)", name, type, desc);
        }
    }

    static final class Schema
    {
        final boolean counters;
        final List<Column> partitionKey, clustering, statics, regulars;
        /** Distinct written values of each clustering column. */
        final int[] domain;
        final int partitions;
        /** Characters added to text and blob values. */
        final int padding;
        final String compression;
        final int chunkKiB;
        final int gcGrace;
        /** Null for the configured default. */
        final String memtable;
        final int columnIndexKiB;

        Schema(boolean counters, List<Column> partitionKey, List<Column> clustering, List<Column> statics, List<Column> regulars,
               int[] domain, int partitions, int padding, String compression, int chunkKiB, int gcGrace, String memtable,
               int columnIndexKiB)
        {
            this.counters = counters;
            this.partitionKey = partitionKey;
            this.clustering = clustering;
            this.statics = statics;
            this.regulars = regulars;
            this.domain = domain;
            this.partitions = partitions;
            this.padding = padding;
            this.compression = compression;
            this.chunkKiB = chunkKiB;
            this.gcGrace = gcGrace;
            this.memtable = memtable;
            this.columnIndexKiB = columnIndexKiB;
        }

        /** Statics, then regulars: the columns an operation's column index refers to. */
        List<Column> valueColumns()
        {
            List<Column> all = new ArrayList<>(statics);
            all.addAll(regulars);
            return all;
        }

        int rows()
        {
            int rows = 1;
            for (int d : domain)
                rows *= d;
            return rows;
        }

        String createTable()
        {
            StringBuilder sb = new StringBuilder("CREATE TABLE %s (");
            for (Column c : partitionKey)
                sb.append(c.name).append(' ').append(c.type.cql).append(", ");
            for (Column c : clustering)
                sb.append(c.name).append(' ').append(c.type.cql).append(", ");
            for (Column c : statics)
                sb.append(c.name).append(' ').append(c.type.cql).append(" static, ");
            for (Column c : regulars)
                sb.append(c.name).append(' ').append(c.type.cql).append(", ");
            sb.append("PRIMARY KEY (");
            String pk = partitionKey.stream().map(c -> c.name).collect(Collectors.joining(", "));
            sb.append(partitionKey.size() > 1 ? '(' + pk + ')' : pk);
            for (Column c : clustering)
                sb.append(", ").append(c.name);
            sb.append(")) WITH gc_grace_seconds = ").append(gcGrace);
            if (!clustering.isEmpty())
                sb.append(" AND CLUSTERING ORDER BY (")
                  .append(clustering.stream().map(c -> c.name + (c.desc ? " DESC" : " ASC")).collect(Collectors.joining(", ")))
                  .append(')');
            if (compression == null)
                sb.append(" AND compression = {'enabled': 'false'}");
            else
                sb.append(" AND compression = {'class': '").append(compression).append("', 'chunk_length_in_kb': ").append(chunkKiB).append('}');
            if (memtable != null)
                sb.append(" AND memtable = '").append(memtable).append('\'');
            return sb.toString();
        }

        String toJava()
        {
            return String.format("new Schema(%s,%n    %s,%n    %s,%n    %s,%n    %s,%n    new int[]{ %s }, %d, %d, %s, %d, %d, %s, %d)",
                                 counters, columnsToJava(partitionKey), columnsToJava(clustering), columnsToJava(statics),
                                 columnsToJava(regulars), ints(domain), partitions, padding, quoted(compression), chunkKiB,
                                 gcGrace, quoted(memtable), columnIndexKiB);
        }
    }

    enum OpKind
    {
        INSERT, UPDATE, STATIC_WRITE, CELL_DELETE, ROW_DELETE, RANGE_DELETE, PARTITION_DELETE,
        COLLECTION_APPEND, COLLECTION_REPLACE, ELEMENT_DELETE, COLLECTION_DELETE, COUNTER_ADD,
        FLUSH, COMPACT, MARK_REPAIRED, DROP_COLUMN, ADD_COLUMN;

        boolean deletion()
        {
            return this == CELL_DELETE || this == ROW_DELETE || this == RANGE_DELETE || this == PARTITION_DELETE
                   || this == ELEMENT_DELETE || this == COLLECTION_DELETE;
        }
    }

    /** One step of a case's history. */
    static final class Op
    {
        final OpKind kind;
        final int partition;
        /** The row's clustering, or the range start (a prefix, or null for an open start), as value indexes. */
        final int[] clustering;
        /** The range end: a prefix, or null for an open end. */
        final int[] end;
        final boolean startInclusive, endInclusive;
        final long timestamp;
        /** The nowInSec the write is made at: the local deletion time of its tombstones and the base of its TTLs. */
        final long writeNow;
        final int ttl;
        /** Index into {@link Schema#valueColumns} for single-column operations; -1 otherwise. */
        final int column;
        /** Seed of the values and of the column subset written. */
        final long valueSeed;
        /** The sstable format of a flush or compaction. */
        final String format;

        Op(OpKind kind, int partition, int[] clustering, int[] end, boolean startInclusive, boolean endInclusive,
           long timestamp, long writeNow, int ttl, int column, long valueSeed, String format)
        {
            this.kind = kind;
            this.partition = partition;
            this.clustering = clustering;
            this.end = end;
            this.startInclusive = startInclusive;
            this.endInclusive = endInclusive;
            this.timestamp = timestamp;
            this.writeNow = writeNow;
            this.ttl = ttl;
            this.column = column;
            this.valueSeed = valueSeed;
            this.format = format;
        }

        static Op layout(OpKind kind, String format, long seed)
        {
            return new Op(kind, -1, null, null, false, false, 0, 0, 0, -1, seed, format);
        }

        String toJava()
        {
            return String.format("new Op(OpKind.%s, %d, %s, %s, %s, %s, %dL, %dL, %d, %d, %dL, %s)", kind, partition,
                                 intsOrNull(clustering), intsOrNull(end), startInclusive, endInclusive, timestamp,
                                 writeNow, ttl, column, valueSeed, quoted(format));
        }

        @Override
        public String toString()
        {
            return toJava();
        }
    }

    enum Shape
    {
        ALL("all"), SLICE("slice"), MULTI_SLICE("multi-slice"), NAMES("names"), STATIC_ONLY("static-only");

        final String label;

        Shape(String label)
        {
            this.label = label;
        }
    }

    enum FilterKind
    {
        STATIC_EQ, PARTITION_KEY_EQ, CLUSTERING_EQ, CLUSTERING_GT, REGULAR_EQ, REGULAR_RANGE,
        SET_CONTAINS, MAP_ELEMENT, NEEDS_RECONCILIATION, COUNTER_EQ;

        /** Filters the cursor path can never push down. */
        boolean neverPushedDown()
        {
            return this == SET_CONTAINS || this == MAP_ELEMENT || this == NEEDS_RECONCILIATION || this == COUNTER_EQ;
        }
    }

    /** One slice of a slice read, as value indexes; a null bound is open. */
    static final class SliceSpec
    {
        final int[] start, end;
        final boolean startInclusive, endInclusive;

        SliceSpec(int[] start, int[] end, boolean startInclusive, boolean endInclusive)
        {
            this.start = start;
            this.end = end;
            this.startInclusive = startInclusive;
            this.endInclusive = endInclusive;
        }

        String toJava()
        {
            return String.format("new SliceSpec(%s, %s, %s, %s)", intsOrNull(start), intsOrNull(end), startInclusive, endInclusive);
        }
    }

    enum Columns { ALL, SELECTION, FETCH_ALL_QUERY_SOME, ELEMENT_SELECTION }

    /** One read of a case, and the extra surfaces it is compared on. */
    static final class Query
    {
        final long seed;
        final int partition;
        final Shape shape;
        final boolean reversed;
        final List<SliceSpec> slices;
        final List<int[]> names;
        final Columns columns;
        final long columnsSeed;
        /** -1 for no limit. */
        final int limit, perPartitionLimit;
        /** Null for no row filter. */
        final FilterKind filter;
        final long filterSeed;
        final long nowInSec;
        final boolean cold;
        /** Internal paging page size; 0 for none. */
        final int pageSize;
        /** A clustering to resume after with {@code forPaging}, or null. */
        final int[] resumeAfter;
        final int resumeLimit;
        /** Seed of a CQL SELECT also compared, or 0 for none. */
        final long cqlSeed;
        final int cqlPageSize;
        final boolean cqlAsReplica;
        /** Tombstone thresholds for this read; -1 keeps the configured one. */
        final int tombstoneWarn, tombstoneFail;

        Query(long seed, int partition, Shape shape, boolean reversed, List<SliceSpec> slices, List<int[]> names,
              Columns columns, long columnsSeed, int limit, int perPartitionLimit, FilterKind filter, long filterSeed,
              long nowInSec, boolean cold, int pageSize, int[] resumeAfter, int resumeLimit, long cqlSeed,
              int cqlPageSize, boolean cqlAsReplica, int tombstoneWarn, int tombstoneFail)
        {
            this.seed = seed;
            this.partition = partition;
            this.shape = shape;
            this.reversed = reversed;
            this.slices = slices;
            this.names = names;
            this.columns = columns;
            this.columnsSeed = columnsSeed;
            this.limit = limit;
            this.perPartitionLimit = perPartitionLimit;
            this.filter = filter;
            this.filterSeed = filterSeed;
            this.nowInSec = nowInSec;
            this.cold = cold;
            this.pageSize = pageSize;
            this.resumeAfter = resumeAfter;
            this.resumeLimit = resumeLimit;
            this.cqlSeed = cqlSeed;
            this.cqlPageSize = cqlPageSize;
            this.cqlAsReplica = cqlAsReplica;
            this.tombstoneWarn = tombstoneWarn;
            this.tombstoneFail = tombstoneFail;
        }

        Query with(Shape newShape, List<SliceSpec> newSlices, List<int[]> newNames)
        {
            return new Query(seed, partition, newShape, reversed, newSlices, newNames, columns, columnsSeed, limit,
                             perPartitionLimit, filter, filterSeed, nowInSec, cold, pageSize, resumeAfter, resumeLimit,
                             cqlSeed, cqlPageSize, cqlAsReplica, tombstoneWarn, tombstoneFail);
        }

        /** The same read with one extra surface or option removed, for the shrinker; null if there is nothing left. */
        List<Query> simpler()
        {
            List<Query> out = new ArrayList<>();
            if (cqlSeed != 0)
                out.add(new Query(seed, partition, shape, reversed, slices, names, columns, columnsSeed, limit, perPartitionLimit,
                                  filter, filterSeed, nowInSec, cold, pageSize, resumeAfter, resumeLimit, 0, 0, false, tombstoneWarn, tombstoneFail));
            if (pageSize != 0)
                out.add(new Query(seed, partition, shape, reversed, slices, names, columns, columnsSeed, limit, perPartitionLimit,
                                  filter, filterSeed, nowInSec, cold, 0, resumeAfter, resumeLimit, cqlSeed, cqlPageSize, cqlAsReplica, tombstoneWarn, tombstoneFail));
            if (resumeAfter != null)
                out.add(new Query(seed, partition, shape, reversed, slices, names, columns, columnsSeed, limit, perPartitionLimit,
                                  filter, filterSeed, nowInSec, cold, pageSize, null, 0, cqlSeed, cqlPageSize, cqlAsReplica, tombstoneWarn, tombstoneFail));
            if (filter != null)
                out.add(new Query(seed, partition, shape, reversed, slices, names, columns, columnsSeed, limit, perPartitionLimit,
                                  null, 0, nowInSec, cold, pageSize, resumeAfter, resumeLimit, cqlSeed, cqlPageSize, cqlAsReplica, tombstoneWarn, tombstoneFail));
            if (columns != Columns.ALL)
                out.add(new Query(seed, partition, shape, reversed, slices, names, Columns.ALL, 0, limit, perPartitionLimit,
                                  filter, filterSeed, nowInSec, cold, pageSize, resumeAfter, resumeLimit, cqlSeed, cqlPageSize, cqlAsReplica, tombstoneWarn, tombstoneFail));
            if (limit != -1 || perPartitionLimit != -1)
                out.add(new Query(seed, partition, shape, reversed, slices, names, columns, columnsSeed, -1, -1,
                                  filter, filterSeed, nowInSec, cold, pageSize, resumeAfter, resumeLimit, cqlSeed, cqlPageSize, cqlAsReplica, tombstoneWarn, tombstoneFail));
            if (tombstoneWarn != -1 || tombstoneFail != -1)
                out.add(new Query(seed, partition, shape, reversed, slices, names, columns, columnsSeed, limit, perPartitionLimit,
                                  filter, filterSeed, nowInSec, cold, pageSize, resumeAfter, resumeLimit, cqlSeed, cqlPageSize, cqlAsReplica, -1, -1));
            if (cold)
                out.add(new Query(seed, partition, shape, reversed, slices, names, columns, columnsSeed, limit, perPartitionLimit,
                                  filter, filterSeed, nowInSec, false, pageSize, resumeAfter, resumeLimit, cqlSeed, cqlPageSize, cqlAsReplica, tombstoneWarn, tombstoneFail));
            if (shape != Shape.ALL && shape != Shape.STATIC_ONLY)
                out.add(with(Shape.ALL, List.of(), List.of()));
            return out;
        }

        String toJava()
        {
            String sliceList = "List.of(" + slices.stream().map(SliceSpec::toJava).collect(Collectors.joining(", ")) + ')';
            String nameList = "List.of(" + names.stream().map(CursorReadFuzzModel::ints).map(s -> "new int[]{ " + s + " }")
                                               .collect(Collectors.joining(", ")) + ')';
            return String.format("new Query(%dL, %d, Shape.%s, %s, %s, %s,%n    Columns.%s, %dL, %d, %d, %s, %dL, %dL, %s, %d, %s, %d, %dL, %d, %s, %d, %d)",
                                 seed, partition, shape, reversed, sliceList, nameList, columns, columnsSeed, limit,
                                 perPartitionLimit, filter == null ? "null" : "FilterKind." + filter, filterSeed, nowInSec,
                                 cold, pageSize, intsOrNull(resumeAfter), resumeLimit, cqlSeed, cqlPageSize, cqlAsReplica,
                                 tombstoneWarn, tombstoneFail);
        }
    }

    static final class FuzzCase
    {
        final long caseSeed;
        final String profile;
        final Schema schema;
        final List<Op> ops;
        final List<Query> queries;

        FuzzCase(long caseSeed, String profile, Schema schema, List<Op> ops, List<Query> queries)
        {
            this.caseSeed = caseSeed;
            this.profile = profile;
            this.schema = schema;
            this.ops = ops;
            this.queries = queries;
        }

        FuzzCase withOps(List<Op> newOps)
        {
            return new FuzzCase(caseSeed, profile, schema, newOps, queries);
        }

        FuzzCase withQueries(List<Query> newQueries)
        {
            return new FuzzCase(caseSeed, profile, schema, ops, newQueries);
        }

        int sstablesFlushed()
        {
            return (int) ops.stream().filter(op -> op.kind == OpKind.FLUSH).count();
        }

        /** A JUnit test method that rebuilds and runs this case. */
        String toJava()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("    // generator version ").append(CursorReadFuzzRunner.GENERATOR_VERSION).append(", case seed ").append(caseSeed).append("L\n");
            sb.append("    @Test\n    public void case").append(Long.toUnsignedString(caseSeed)).append("() throws Throwable\n    {\n");
            sb.append("        Schema schema = ").append(schema.toJava().replace("\n", "\n        ")).append(";\n");
            sb.append("        List<Op> ops = List.of(\n");
            sb.append(ops.stream().map(op -> "            " + op.toJava()).collect(Collectors.joining(",\n")));
            sb.append(");\n        List<Query> queries = List.of(\n");
            sb.append(queries.stream().map(q -> "            " + q.toJava().replace("\n", "\n            ")).collect(Collectors.joining(",\n")));
            sb.append(");\n        runCase(new FuzzCase(").append(caseSeed).append("L, \"").append(profile).append("\", schema, ops, queries));\n    }\n");
            return sb.toString();
        }
    }

    static String ints(int[] values)
    {
        return Arrays.stream(values).mapToObj(String::valueOf).collect(Collectors.joining(", "));
    }

    static String intsOrNull(int[] values)
    {
        return values == null ? "null" : "new int[]{ " + ints(values) + " }";
    }

    private static String quoted(String s)
    {
        return s == null ? "null" : '"' + s + '"';
    }

    private static String columnsToJava(List<Column> columns)
    {
        return "List.of(" + columns.stream().map(Column::toJava).collect(Collectors.joining(", ")) + ')';
    }
}
