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
import java.util.List;
import java.util.TreeSet;

import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Column;
import org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.Columns;
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

import static org.apache.cassandra.db.cursorreads.CursorReadFuzzModel.splitmix64;

/**
 * Builds a {@link FuzzCase} from a case seed.  The schema, the history, the layout and the reads
 * each draw from their own stream, split from the case seed.
 * <p>
 * Clustering values are written at even value indexes only, so an odd index is a clustering
 * that sits between two written ones; -1 sorts before every written value and {@code 2 * domain}
 * after every one.
 */
final class CursorReadFuzzGenerator
{
    /** The pinned nowInSec reads are made around. */
    static final long NOW = 1_700_000_000L;
    private static final long BASE_TS = 1_000_000L;
    private static final int[] TTLS = { 1, 60, 1_000_000 };

    private final long caseSeed;
    private final String profile;
    private final int maxRows;
    private final int queries;

    private Schema schema;
    private final List<Long> timestamps = new ArrayList<>();
    private final TreeSet<Long> readTimes = new TreeSet<>();
    private final List<int[]> rangeBounds = new ArrayList<>();
    private long ts = BASE_TS;

    private CursorReadFuzzGenerator(long caseSeed, String profile, int maxRows, int queries)
    {
        this.caseSeed = caseSeed;
        this.profile = profile;
        this.maxRows = maxRows;
        this.queries = queries;
    }

    static FuzzCase generate(long caseSeed, String profile, int maxRows, int queries)
    {
        return new CursorReadFuzzGenerator(caseSeed, profile, maxRows, queries).generate();
    }

    private FuzzCase generate()
    {
        schema = schema(new Rng(splitmix64(caseSeed ^ 0x5C4E3AL)));
        List<Op> data = history(new Rng(splitmix64(caseSeed ^ 0x415702L)));
        List<Op> ops = layout(new Rng(splitmix64(caseSeed ^ 0x1A7007L)), data);
        Rng q = new Rng(splitmix64(caseSeed ^ 0x9E3779L));
        List<Query> reads = new ArrayList<>();
        for (int i = 0; i < queries; i++)
            reads.add(query(q));
        return new FuzzCase(caseSeed, profile, schema, ops, reads);
    }

    // ---------------------------------------------------------------- schema

    private Schema schema(Rng r)
    {
        boolean counters = r.chance(0.08);
        List<Column> pk = new ArrayList<>();
        int pkCount = r.chance(0.25) ? 2 : 1;
        for (int i = 0; i < pkCount; i++)
            pk.add(new Column("pk" + i, r.pick(Type.INT, Type.BIGINT, Type.TEXT), false));

        int ckCount = r.weighted(10, 55, 25, 10);
        List<Column> ck = new ArrayList<>();
        for (int i = 0; i < ckCount; i++)
            ck.add(new Column("ck" + i, r.pick(Type.INT, Type.BIGINT, Type.TEXT), r.chance(0.3)));

        List<Column> statics = new ArrayList<>();
        if (ckCount > 0 && !counters)
        {
            int n = r.weighted(40, 40, 20);
            for (int i = 0; i < n; i++)
                statics.add(new Column("s" + i, i == 0 ? r.pick(Type.BIGINT, Type.TEXT) : r.pick(Type.BIGINT, Type.TEXT, Type.SET), false));
        }

        List<Column> regulars = new ArrayList<>();
        if (counters)
        {
            int n = r.between(1, 2);
            for (int i = 0; i < n; i++)
                regulars.add(new Column("v" + i, Type.COUNTER, false));
        }
        else
        {
            int n = r.between(1, 5);
            for (int i = 0; i < n; i++)
            {
                Type t = i == 0 ? r.pick(Type.INT, Type.BIGINT, Type.TEXT, Type.BLOB, Type.DOUBLE, Type.BOOLEAN)
                                : r.pick(Type.values());
                if (t == Type.COUNTER)
                    t = Type.TEXT;
                regulars.add(new Column("v" + i, t, false));
            }
        }

        int rows = r.chance(0.3) ? r.between(1, Math.min(20, maxRows)) : r.between(1, maxRows);
        int padding;
        switch (r.weighted(60, 25, 12, 3))
        {
            case 0: padding = r.between(1, 16); break;
            case 1: padding = r.between(100, 200); break;
            case 2: padding = r.between(400, 600); break;
            default: padding = r.between(4000, 8000); rows = Math.min(rows, 40);
        }
        int[] domain = domain(r, ckCount, rows);

        String compression = r.pick(null, "LZ4Compressor", "LZ4Compressor", "ZstdCompressor", "DeflateCompressor", "SnappyCompressor");
        int chunkKiB = r.pick(4, 16, 64);
        int gcGrace = r.pick(0, 1, 864000);
        String memtable = r.pick(null, "skiplist", "trie", "skiplist_sharded");
        int columnIndexKiB = r.pick(1, 1, 4, 4, 64);
        return new Schema(counters, pk, ck, statics, regulars, domain, r.between(1, 4), padding, compression, chunkKiB,
                          gcGrace, memtable, columnIndexKiB);
    }

    private static int[] domain(Rng r, int ckCount, int rows)
    {
        switch (ckCount)
        {
            case 0: return new int[0];
            case 1: return new int[]{ rows };
            case 2:
            {
                int first = r.between(1, 6);
                return new int[]{ first, Math.max(1, (rows + first - 1) / first) };
            }
            default:
            {
                int first = r.between(1, 3), second = r.between(1, 4);
                return new int[]{ first, second, Math.max(1, (rows + first * second - 1) / (first * second)) };
            }
        }
    }

    // ---------------------------------------------------------------- history

    private long nextTimestamp(Rng r)
    {
        long t;
        if (!timestamps.isEmpty() && r.chance(0.08))
            t = r.pick(timestamps); // a tie with an earlier write
        else
            t = ts += r.between(1, 3);
        timestamps.add(t);
        return t;
    }

    private long writeNow(Rng r)
    {
        return NOW - r.between(0, 200);
    }

    private int ttl(Rng r, double p)
    {
        return r.chance(p) ? TTLS[r.pickIndex(TTLS.length)] : 0;
    }

    private int partition(Rng r)
    {
        return r.chance(0.8) ? 0 : r.pickIndex(schema.partitions);
    }

    /** A full clustering at written (even) value indexes. */
    private int[] writtenClustering(Rng r)
    {
        int[] c = new int[schema.domain.length];
        for (int i = 0; i < c.length; i++)
            c[i] = 2 * r.pickIndex(schema.domain[i]);
        return c;
    }

    /** A prefix of any value indexes, written or not. */
    private int[] boundPrefix(Rng r)
    {
        int length = r.chance(0.7) ? schema.domain.length : r.between(1, schema.domain.length);
        int[] prefix = new int[length];
        for (int i = 0; i < length; i++)
            prefix[i] = r.between(-1, 2 * schema.domain[i]);
        return prefix;
    }

    private List<Op> history(Rng r)
    {
        List<Op> ops = new ArrayList<>();
        boolean hasClustering = schema.domain.length > 0;
        List<Column> values = schema.valueColumns();
        int staticCount = schema.statics.size();

        // most rows written once first, so later operations shadow something
        for (int[] c : allClusterings())
            if (r.chance(0.85))
                ops.add(write(r, schema.counters ? OpKind.COUNTER_ADD : OpKind.INSERT, partition(r), c, -1));

        int count = Math.max(5, Math.min(800, r.between(schema.rows() / 4, schema.rows() * 2)));
        for (int i = 0; i < count; i++)
        {
            int p = partition(r);
            if (schema.counters)
            {
                switch (r.weighted(80, 8, 8, 4))
                {
                    case 0: ops.add(write(r, OpKind.COUNTER_ADD, p, writtenClustering(r), -1)); break;
                    case 1: ops.add(write(r, OpKind.CELL_DELETE, p, writtenClustering(r), r.pickIndex(values.size()))); break;
                    case 2: ops.add(hasClustering ? write(r, OpKind.ROW_DELETE, p, writtenClustering(r), -1)
                                                  : write(r, OpKind.COUNTER_ADD, p, writtenClustering(r), -1)); break;
                    default: ops.add(write(r, OpKind.PARTITION_DELETE, p, null, -1));
                }
                continue;
            }
            switch (r.weighted(30, 15, staticCount > 0 ? 6 : 0, 5, hasClustering ? 6 : 0, hasClustering ? 6 : 0, 2, 5, 3, 3, 2))
            {
                case 0: ops.add(write(r, OpKind.INSERT, p, writtenClustering(r), -1)); break;
                case 1: ops.add(write(r, OpKind.UPDATE, p, writtenClustering(r), -1)); break;
                case 2: ops.add(write(r, OpKind.STATIC_WRITE, p, null, -1)); break;
                case 3:
                {
                    int column = simpleColumn(r, values);
                    if (column >= 0)
                        ops.add(write(r, OpKind.CELL_DELETE, p, writtenClustering(r), column));
                    break;
                }
                case 4: ops.add(write(r, OpKind.ROW_DELETE, p, writtenClustering(r), -1)); break;
                case 5: ops.add(rangeDelete(r, p)); break;
                case 6: ops.add(write(r, OpKind.PARTITION_DELETE, p, null, -1)); break;
                default:
                {
                    int column = multiCellColumn(r, values);
                    if (column < 0)
                        break;
                    OpKind kind = r.pick(OpKind.COLLECTION_APPEND, OpKind.COLLECTION_APPEND, OpKind.COLLECTION_REPLACE,
                                         OpKind.ELEMENT_DELETE, OpKind.COLLECTION_DELETE);
                    ops.add(write(r, kind, p, column < staticCount ? null : writtenClustering(r), column));
                }
            }
        }
        return ops;
    }

    private List<int[]> allClusterings()
    {
        List<int[]> all = new ArrayList<>();
        int[] c = new int[schema.domain.length];
        int rows = schema.rows();
        for (int n = 0; n < rows; n++)
        {
            int rest = n;
            for (int i = c.length - 1; i >= 0; i--)
            {
                c[i] = 2 * (rest % schema.domain[i]);
                rest /= schema.domain[i];
            }
            all.add(c.clone());
        }
        return all;
    }

    private int simpleColumn(Rng r, List<Column> values)
    {
        List<Integer> simple = new ArrayList<>();
        for (int i = 0; i < values.size(); i++)
            if (!values.get(i).type.multiCell())
                simple.add(i);
        return simple.isEmpty() ? -1 : r.pick(simple);
    }

    private int multiCellColumn(Rng r, List<Column> values)
    {
        List<Integer> multi = new ArrayList<>();
        for (int i = 0; i < values.size(); i++)
            if (values.get(i).type.multiCell())
                multi.add(i);
        return multi.isEmpty() ? -1 : r.pick(multi);
    }

    private Op write(Rng r, OpKind kind, int partition, int[] clustering, int column)
    {
        long t = nextTimestamp(r);
        long now = writeNow(r);
        int ttl = kind == OpKind.INSERT || kind == OpKind.UPDATE || kind == OpKind.STATIC_WRITE || kind == OpKind.COLLECTION_APPEND
                  ? ttl(r, 0.2) : 0;
        if (ttl > 0)
        {
            addReadTimes(now + ttl);
            addReadTimes(now + ttl + schema.gcGrace);
        }
        if (kind.deletion())
            addReadTimes(now + schema.gcGrace);
        return new Op(kind, partition, clustering, null, false, false, t, now, ttl, column, r.nextLong(), null);
    }

    private Op rangeDelete(Rng r, int partition)
    {
        // about 15% are open at one end: closing at TOP or opening at BOTTOM
        int[] start = null, end = null;
        switch (r.weighted(84, 7, 7, 2))
        {
            case 0: start = boundPrefix(r); end = boundPrefix(r); break;
            case 1: start = boundPrefix(r); break;
            case 2: end = boundPrefix(r); break;
            default:
        }
        if (start != null)
            rangeBounds.add(start);
        if (end != null)
            rangeBounds.add(end);
        long now = writeNow(r);
        addReadTimes(now + schema.gcGrace);
        return new Op(OpKind.RANGE_DELETE, partition, start, end, r.chance(0.5), r.chance(0.5), nextTimestamp(r), now, 0, -1,
                      r.nextLong(), null);
    }

    /** Reads land on both sides of an expiry or purge time. */
    private void addReadTimes(long t)
    {
        readTimes.add(t - 1);
        readTimes.add(t);
        readTimes.add(t + 1);
    }

    // ---------------------------------------------------------------- layout

    private String format(Rng r)
    {
        return profile.equals("mixed") ? r.pick("big", "bti") : profile;
    }

    private List<Op> layout(Rng r, List<Op> data)
    {
        int sstables = r.pick(0, 1, 1, 2, 2, 3, 3, 4, 5, 8);
        boolean overlay = sstables == 0 || r.chance(0.6);
        TreeSet<Integer> cuts = new TreeSet<>();
        int segments = sstables + (overlay ? 1 : 0);
        while (cuts.size() < segments - 1 && cuts.size() < data.size() - 1)
            cuts.add(r.between(1, data.size() - 1));

        // a mixed layout holds both formats once there are two sstables
        List<String> formats = new ArrayList<>();
        for (int i = 0; i < sstables; i++)
            formats.add(format(r));
        if (profile.equals("mixed") && sstables >= 2 && !formats.contains("big"))
            formats.set(0, "big");
        if (profile.equals("mixed") && sstables >= 2 && !formats.contains("bti"))
            formats.set(1, "bti");

        int dropAt = -1, addAt = -1;
        boolean schemaChange = !schema.counters && schema.regulars.size() >= 2 && sstables >= 1 && r.chance(0.12);
        int dropColumn = schemaChange ? schema.statics.size() + r.between(1, schema.regulars.size() - 1) : -1;

        List<Op> ops = new ArrayList<>();
        int flushed = 0;
        for (int i = 0; i < data.size(); i++)
        {
            if (cuts.contains(i) && flushed < sstables)
            {
                ops.add(Op.layout(OpKind.FLUSH, formats.get(flushed++), 0));
                if (r.chance(0.05))
                    ops.add(Op.layout(OpKind.COMPACT, format(r), 0));
                if (schemaChange && dropAt < 0)
                {
                    dropAt = ops.size();
                    ops.add(new Op(OpKind.DROP_COLUMN, -1, null, null, false, false, nextTimestamp(r), 0, 0, dropColumn, 0, null));
                }
                else if (schemaChange && addAt < 0 && r.chance(0.7))
                {
                    addAt = ops.size();
                    ops.add(new Op(OpKind.ADD_COLUMN, -1, null, null, false, false, nextTimestamp(r), 0, 0, dropColumn, 0, null));
                }
            }
            ops.add(data.get(i));
        }
        while (flushed < sstables)
            ops.add(Op.layout(OpKind.FLUSH, formats.get(flushed++), 0));
        if (sstables > 0 && r.chance(0.15))
            ops.add(Op.layout(OpKind.MARK_REPAIRED, null, r.nextLong()));
        return ops;
    }

    // ---------------------------------------------------------------- reads

    /** Value indexes a bound or name lands on: written, between written, before the first, after the last, and range bounds. */
    private int valueIndex(Rng r, int column)
    {
        int domain = schema.domain[column];
        switch (r.weighted(55, 20, 5, 5, 15))
        {
            case 0: return 2 * r.pickIndex(domain);
            case 1: return 2 * r.pickIndex(domain) + 1;
            case 2: return -1;
            case 3: return 2 * domain;
            default:
            {
                List<Integer> fromRanges = new ArrayList<>();
                for (int[] bound : rangeBounds)
                    if (bound.length > column)
                        fromRanges.add(bound[column]);
                return fromRanges.isEmpty() ? 2 * r.pickIndex(domain) : r.pick(fromRanges);
            }
        }
    }

    private int[] clusteringOrPrefix(Rng r, boolean full)
    {
        int length = full || r.chance(0.7) ? schema.domain.length : r.between(1, schema.domain.length);
        int[] c = new int[length];
        for (int i = 0; i < length; i++)
            c[i] = valueIndex(r, i);
        return c;
    }

    private Query query(Rng r)
    {
        long seed = r.nextLong();
        boolean hasClustering = schema.domain.length > 0;
        int partition = r.chance(0.85) ? r.pickIndex(schema.partitions) : schema.partitions + r.pickIndex(3);

        Shape shape;
        if (!hasClustering)
            shape = r.chance(0.5) ? Shape.ALL : Shape.NAMES;
        else
            shape = Shape.values()[r.weighted(15, 30, 15, 30, schema.statics.isEmpty() ? 0 : 5)];
        boolean reversed = r.chance(0.4);

        List<SliceSpec> slices = new ArrayList<>();
        List<int[]> names = new ArrayList<>();
        if (shape == Shape.SLICE || shape == Shape.MULTI_SLICE)
        {
            int n = shape == Shape.SLICE ? 1 : r.between(2, 6);
            for (int i = 0; i < n; i++)
            {
                // some slices start at a bound and end at TOP, or start at BOTTOM
                int open = r.weighted(76, 14, 10);
                int[] start = open == 2 ? null : clusteringOrPrefix(r, false);
                int[] end = open == 1 ? null : clusteringOrPrefix(r, false);
                slices.add(new SliceSpec(start, end, r.chance(0.6), r.chance(0.6)));
            }
        }
        else if (shape == Shape.NAMES && hasClustering)
        {
            int n = r.chance(0.8) ? r.between(1, 6) : r.between(7, 50);
            for (int i = 0; i < n; i++)
                names.add(clusteringOrPrefix(r, true));
        }

        Columns columns = Columns.values()[r.weighted(60, 15, 15, hasMultiCell() ? 10 : 0)];
        int limit = -1, perPartitionLimit = -1;
        switch (r.weighted(50, 15, 20, 5, 10))
        {
            case 1: limit = 1; break;
            case 2: limit = r.between(2, 10); break;
            case 3: limit = schema.rows() + r.between(1, 10); break;
            case 4: perPartitionLimit = r.between(1, 5); limit = r.chance(0.5) ? r.between(1, 20) : -1; break;
            default:
        }

        FilterKind filter = null;
        if (r.chance(0.25))
        {
            List<FilterKind> kinds = filterKinds();
            if (!kinds.isEmpty())
                filter = r.pick(kinds);
        }

        long nowInSec = readTimes.isEmpty() || r.chance(0.5) ? NOW : r.pick(new ArrayList<>(readTimes));
        boolean cold = r.chance(0.2);

        int pageSize = 0;
        if (r.chance(0.3))
            pageSize = r.weighted(20, 50, 30) == 0 ? 1 : r.chance(0.6) ? r.between(2, 9) : r.between(10, 40);
        int[] resumeAfter = hasClustering && r.chance(0.25) ? clusteringOrPrefix(r, true) : null;
        int resumeLimit = r.pick(1, 5);

        long cqlSeed = r.chance(0.3) ? r.nextLong() | 1 : 0;
        int cqlPageSize = r.pick(0, 1, 3, 10, 100);
        boolean cqlAsReplica = r.chance(0.5);

        int warn = -1, fail = -1;
        if (r.chance(0.1))
        {
            warn = r.pick(1, 10, 100);
            fail = r.pick(5, 50, 1000);
        }
        return new Query(seed, partition, shape, reversed, slices, names, columns, r.nextLong(), limit, perPartitionLimit,
                         filter, r.nextLong(), nowInSec, cold, pageSize, resumeAfter, resumeLimit, cqlSeed, cqlPageSize,
                         cqlAsReplica, warn, fail);
    }

    private boolean hasMultiCell()
    {
        return schema.valueColumns().stream().anyMatch(c -> c.type == Type.SET || c.type == Type.MAP);
    }

    private List<FilterKind> filterKinds()
    {
        List<FilterKind> kinds = new ArrayList<>();
        kinds.add(FilterKind.PARTITION_KEY_EQ);
        if (!schema.clustering.isEmpty())
        {
            kinds.add(FilterKind.CLUSTERING_EQ);
            kinds.add(FilterKind.CLUSTERING_GT);
        }
        if (schema.counters)
        {
            kinds.add(FilterKind.COUNTER_EQ);
            return kinds;
        }
        if (schema.statics.stream().anyMatch(c -> c.type.scalar()))
            kinds.add(FilterKind.STATIC_EQ);
        if (schema.regulars.stream().anyMatch(c -> c.type.scalar()))
        {
            kinds.add(FilterKind.REGULAR_EQ);
            kinds.add(FilterKind.REGULAR_EQ);
            kinds.add(FilterKind.NEEDS_RECONCILIATION);
        }
        if (schema.regulars.stream().anyMatch(c -> c.type.numeric()))
            kinds.add(FilterKind.REGULAR_RANGE);
        if (schema.valueColumns().stream().anyMatch(c -> c.type == Type.SET))
            kinds.add(FilterKind.SET_CONTAINS);
        if (schema.valueColumns().stream().anyMatch(c -> c.type == Type.MAP))
            kinds.add(FilterKind.MAP_ELEMENT);
        return kinds;
    }
}
