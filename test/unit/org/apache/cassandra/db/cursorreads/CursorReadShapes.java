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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongFunction;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Named single-partition read shapes over a table whose only clustering column is a {@code bigint}
 * {@code ck}: slices with every bound inclusivity, names (present, absent, deleted), multi-slice
 * (disjoint and adjacent), reversed, limits, and column subsets.
 */
final class CursorReadShapes
{
    private CursorReadShapes()
    {
    }

    /**
     * @param interesting clustering values the slices and names land on (rows, deleted rows, range
     *                    tombstone bounds, absent values); at least four
     * @param columns     column subsets to read, each a list of column names (null means all)
     */
    static Map<String, LongFunction<SinglePartitionReadCommand>> forTable(ColumnFamilyStore cfs, long pk, long[] interesting,
                                                                          String[][] columns)
    {
        Map<String, LongFunction<SinglePartitionReadCommand>> shapes = new LinkedHashMap<>();
        long a = interesting[0], b = interesting[1], c = interesting[2], d = interesting[3];

        shapes.put("full", now -> builder(cfs, pk, now).build());
        shapes.put("full reversed", now -> builder(cfs, pk, now).reverse().build());
        shapes.put("slice [a,c]", now -> builder(cfs, pk, now).fromIncl(a).toIncl(c).build());
        shapes.put("slice [a,c)", now -> builder(cfs, pk, now).fromIncl(a).toExcl(c).build());
        shapes.put("slice (a,c]", now -> builder(cfs, pk, now).fromExcl(a).toIncl(c).build());
        shapes.put("slice (a,c)", now -> builder(cfs, pk, now).fromExcl(a).toExcl(c).build());
        shapes.put("slice from b", now -> builder(cfs, pk, now).fromIncl(b).build());
        shapes.put("slice to b", now -> builder(cfs, pk, now).toIncl(b).build());
        shapes.put("slice (b,d] reversed", now -> builder(cfs, pk, now).fromExcl(b).toIncl(d).reverse().build());
        shapes.put("slice [a,a]", now -> builder(cfs, pk, now).fromIncl(a).toIncl(a).build());
        for (int i = 0; i < interesting.length; i++)
        {
            long from = interesting[i];
            shapes.put("slice from " + from + " limit 3", now -> builder(cfs, pk, now).fromIncl(from).withLimit(3).build());
        }
        shapes.put("names a b c d", now -> builder(cfs, pk, now).includeRow(a).includeRow(b).includeRow(c).includeRow(d).build());
        shapes.put("names a d reversed", now -> builder(cfs, pk, now).includeRow(a).includeRow(d).reverse().build());
        shapes.put("names all interesting", now -> {
            AbstractReadCommandBuilder r = Util.cmd(cfs, pk).withNowInSeconds(now);
            for (long value : interesting)
                r = r.includeRow(value);
            return (SinglePartitionReadCommand) r.build();
        });
        shapes.put("multi-slice [a,b] [c,d]", now -> multiSlice(cfs, pk, now, -1, false, a, b, c, d));
        shapes.put("multi-slice adjacent [a,b] [b+1,c]", now -> multiSlice(cfs, pk, now, -1, false, a, b, b + 1, c));
        shapes.put("multi-slice [a,b] [c,d] reversed", now -> multiSlice(cfs, pk, now, -1, true, a, b, c, d));
        shapes.put("multi-slice [a,b] [c,d] limit 2", now -> multiSlice(cfs, pk, now, 2, false, a, b, c, d));
        shapes.put("limit 1", now -> builder(cfs, pk, now).withLimit(1).build());
        shapes.put("limit 5", now -> builder(cfs, pk, now).withLimit(5).build());
        shapes.put("limit 5 reversed", now -> builder(cfs, pk, now).withLimit(5).reverse().build());
        shapes.put("limit 100000", now -> builder(cfs, pk, now).withLimit(100_000).build());
        for (String[] subset : columns)
        {
            String name = String.join(",", subset);
            shapes.put("columns " + name, now -> builder(cfs, pk, now).columns(subset).build());
            shapes.put("columns " + name + " slice [b,d]", now -> builder(cfs, pk, now).columns(subset).fromIncl(b).toIncl(d).build());
            shapes.put("columns " + name + " names a c", now -> builder(cfs, pk, now).columns(subset).includeRow(a).includeRow(c).build());
        }
        return shapes;
    }

    private static Builder builder(ColumnFamilyStore cfs, long pk, long now)
    {
        return new Builder(Util.cmd(cfs, pk).withNowInSeconds(now));
    }

    /** Typed wrapper so each shape's lambda returns a {@link SinglePartitionReadCommand}. */
    private static final class Builder
    {
        private AbstractReadCommandBuilder b;

        Builder(AbstractReadCommandBuilder b)
        {
            this.b = b;
        }

        Builder fromIncl(long v)
        {
            b = b.fromIncl(v);
            return this;
        }

        Builder fromExcl(long v)
        {
            b = b.fromExcl(v);
            return this;
        }

        Builder toIncl(long v)
        {
            b = b.toIncl(v);
            return this;
        }

        Builder toExcl(long v)
        {
            b = b.toExcl(v);
            return this;
        }

        Builder includeRow(long v)
        {
            b = b.includeRow(v);
            return this;
        }

        Builder reverse()
        {
            b = b.reverse();
            return this;
        }

        Builder withLimit(int limit)
        {
            b = b.withLimit(limit);
            return this;
        }

        Builder columns(String... columns)
        {
            b = b.columns(columns);
            return this;
        }

        SinglePartitionReadCommand build()
        {
            return (SinglePartitionReadCommand) b.build();
        }
    }

    /** A slice-filter read of inclusive ranges given as start, end pairs. */
    static SinglePartitionReadCommand multiSlice(ColumnFamilyStore cfs, long pk, long now, int limit, boolean reversed, long... bounds)
    {
        TableMetadata metadata = cfs.metadata();
        Slices.Builder builder = new Slices.Builder(metadata.comparator);
        for (int i = 0; i < bounds.length; i += 2)
            builder.add(Slice.make(ClusteringBound.create(metadata.comparator, true, true, bounds[i]),
                                   ClusteringBound.create(metadata.comparator, false, true, bounds[i + 1])));
        DataLimits limits = limit < 0 ? DataLimits.NONE : DataLimits.cqlLimits(limit);
        return SinglePartitionReadCommand.create(metadata, now, ColumnFilter.all(metadata), RowFilter.none(), limits,
                                                 metadata.partitioner.decorateKey(ByteBufferUtil.bytes(pk)),
                                                 new ClusteringIndexSliceFilter(builder.build(), reversed));
    }
}
