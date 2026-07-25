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

package org.apache.cassandra.arrow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.cassandra.arrow.CompiledAggregation.AccumulatorKind;
import org.apache.cassandra.arrow.CompiledAggregation.CompiledAggregate;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Server-side {@code GROUP BY} accumulator: one {@link Accumulator} per (group, aggregate) pair,
 * fed a row at a time via {@link #accumulate(RowValues)} (called from {@link ArrowRowAssembler} in
 * place of writing the row to the output batch - see that class's javadoc), and flushed to exactly
 * one final Arrow batch via {@link #emit} once the scan completes.
 * <p>
 * <b>No {@code GROUP BY} (global aggregate) semantics:</b> a single implicit group is seeded eagerly
 * (in the constructor) when {@code groupBy} is empty, so a global aggregate over zero matching rows
 * still emits exactly one row (e.g. {@code COUNT(*) = 0}, {@code SUM(...) = NULL}) - matching SQL,
 * not "zero rows in, zero rows out" (which is instead the correct, and this class's natural,
 * behavior when {@code groupBy} is non-empty and no row matches any group).
 * <p>
 * <b>Chunking (documented judgment call):</b> {@link #emit} always produces a single Arrow batch,
 * however many distinct groups were seen - acceptable for v1 per the task spec; a very high-
 * cardinality {@code GROUP BY} would need this split into multiple batches (bounded by row count or
 * allocator pressure, mirroring {@link ArrowRowAssembler#maybeFlush}), which is straightforward to
 * add later without a wire-format change.
 */
final class RowAggregator
{
    private final CompiledAggregation compiled;
    private final Map<GroupKey, Accumulator[]> groups = new LinkedHashMap<>();

    RowAggregator(CompiledAggregation compiled)
    {
        this.compiled = compiled;
        if (compiled.groupBy.isEmpty())
            groups.put(new GroupKey(new Object[0]), newAccumulators());
    }

    void accumulate(RowValues row)
    {
        Object[] keyValues = new Object[compiled.groupBy.size()];
        for (int i = 0; i < keyValues.length; i++)
            keyValues[i] = row.get(compiled.groupBy.get(i));
        Accumulator[] accumulators = groups.computeIfAbsent(new GroupKey(keyValues), k -> newAccumulators());
        for (Accumulator accumulator : accumulators)
            accumulator.accumulate(row);
    }

    private Accumulator[] newAccumulators()
    {
        Accumulator[] accumulators = new Accumulator[compiled.aggregates.size()];
        for (int i = 0; i < accumulators.length; i++)
            accumulators[i] = newAccumulator(compiled.aggregates.get(i));
        return accumulators;
    }

    private static Accumulator newAccumulator(CompiledAggregate spec)
    {
        switch (spec.accumulatorKind)
        {
            case COUNT:
                return new CountAccumulator(spec.column);
            case SUM_INTEGRAL:
                return new SumIntegralAccumulator(spec.column);
            case SUM_DECIMAL:
                return new SumDecimalAccumulator(spec.column);
            case SUM_FLOATING:
                return new SumFloatingAccumulator(spec.column);
            case MIN:
                return new MinMaxAccumulator(spec.column, true);
            case MAX:
                return new MinMaxAccumulator(spec.column, false);
            case AVG:
                return new AvgAccumulator(spec.column);
            default:
                throw new IllegalStateException("unhandled accumulator kind " + spec.accumulatorKind);
        }
    }

    /** @return true if a batch was produced (false when {@code groupBy} is non-empty and no row ever matched). */
    boolean emit(BufferAllocator allocator, Consumer<VectorSchemaRoot> onBatch)
    {
        if (groups.isEmpty())
            return false;

        VectorSchemaRoot root = VectorSchemaRoot.create(compiled.outputSchema, allocator);
        root.allocateNew();
        int rowIndex = 0;
        for (Map.Entry<GroupKey, Accumulator[]> entry : groups.entrySet())
        {
            for (int i = 0; i < compiled.groupBy.size(); i++)
            {
                ColumnMetadata column = compiled.groupBy.get(i);
                FieldVector vector = (FieldVector) root.getVector(column.name.toString());
                ArrowRowAssembler.writeDecomposedValue(vector, unwrap(column.type), rowIndex, entry.getKey().values[i]);
            }
            Accumulator[] accumulators = entry.getValue();
            for (int i = 0; i < accumulators.length; i++)
            {
                FieldVector vector = (FieldVector) root.getVector(compiled.aggregates.get(i).outputName);
                accumulators[i].writeTo(vector, rowIndex);
            }
            rowIndex++;
        }
        root.setRowCount(rowIndex);
        onBatch.accept(root);
        return true;
    }

    private static AbstractType<?> unwrap(AbstractType<?> type)
    {
        return type.isReversed() ? ((ReversedType<?>) type).baseType : type;
    }

    // ================= group key =================

    private static final class GroupKey
    {
        final Object[] values;

        GroupKey(Object[] values)
        {
            this.values = values;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof GroupKey))
                return false;
            Object[] other = ((GroupKey) o).values;
            if (values.length != other.length)
                return false;
            for (int i = 0; i < values.length; i++)
                if (!deepEquals(values[i], other[i]))
                    return false;
            return true;
        }

        @Override
        public int hashCode()
        {
            int h = 1;
            for (Object v : values)
                h = 31 * h + deepHash(v);
            return h;
        }

        private static boolean deepEquals(Object a, Object b)
        {
            if (a == null || b == null)
                return a == b;
            if (a instanceof byte[])
                return b instanceof byte[] && Arrays.equals((byte[]) a, (byte[]) b);
            return a.equals(b);
        }

        private static int deepHash(Object a)
        {
            if (a == null)
                return 0;
            return a instanceof byte[] ? Arrays.hashCode((byte[]) a) : a.hashCode();
        }
    }

    // ================= per-(group, aggregate) accumulators =================

    private abstract static class Accumulator
    {
        abstract void accumulate(RowValues row);
        abstract void writeTo(FieldVector vector, int rowIndex);
    }

    private static final class CountAccumulator extends Accumulator
    {
        private final ColumnMetadata column; // null => COUNT(*)
        private long count;

        CountAccumulator(ColumnMetadata column)
        {
            this.column = column;
        }

        @Override
        void accumulate(RowValues row)
        {
            if (column == null || row.get(column) != null)
                count++;
        }

        @Override
        void writeTo(FieldVector vector, int rowIndex)
        {
            ((BigIntVector) vector).setSafe(rowIndex, count);
        }
    }

    private static final class SumIntegralAccumulator extends Accumulator
    {
        private final ColumnMetadata column;
        private BigInteger sum = BigInteger.ZERO;
        private boolean hasValue;

        SumIntegralAccumulator(ColumnMetadata column)
        {
            this.column = column;
        }

        @Override
        void accumulate(RowValues row)
        {
            Object value = row.get(column);
            if (value == null)
                return;
            hasValue = true;
            sum = sum.add(toBigInteger(value));
        }

        @Override
        void writeTo(FieldVector vector, int rowIndex)
        {
            if (!hasValue)
                vector.setNull(rowIndex);
            else
                ((Decimal256Vector) vector).setSafe(rowIndex, CassandraArrowTypeMapping.toArrowDecimal(sum));
        }
    }

    private static final class SumDecimalAccumulator extends Accumulator
    {
        private final ColumnMetadata column;
        private BigDecimal sum = BigDecimal.ZERO.setScale(CassandraArrowTypeMapping.DECIMAL_SCALE);
        private boolean hasValue;

        SumDecimalAccumulator(ColumnMetadata column)
        {
            this.column = column;
        }

        @Override
        void accumulate(RowValues row)
        {
            Object value = row.get(column);
            if (value == null)
                return;
            hasValue = true;
            sum = sum.add((BigDecimal) value);
        }

        @Override
        void writeTo(FieldVector vector, int rowIndex)
        {
            if (!hasValue)
                vector.setNull(rowIndex);
            else
                ((Decimal256Vector) vector).setSafe(rowIndex, sum);
        }
    }

    private static final class SumFloatingAccumulator extends Accumulator
    {
        private final ColumnMetadata column;
        private double sum;
        private boolean hasValue;

        SumFloatingAccumulator(ColumnMetadata column)
        {
            this.column = column;
        }

        @Override
        void accumulate(RowValues row)
        {
            Object value = row.get(column);
            if (value == null)
                return;
            hasValue = true;
            sum += toDouble(value);
        }

        @Override
        void writeTo(FieldVector vector, int rowIndex)
        {
            if (!hasValue)
                vector.setNull(rowIndex);
            else
                ((Float8Vector) vector).setSafe(rowIndex, sum);
        }
    }

    private static final class MinMaxAccumulator extends Accumulator
    {
        private final ColumnMetadata column;
        private final boolean isMin;
        private Object current;

        MinMaxAccumulator(ColumnMetadata column, boolean isMin)
        {
            this.column = column;
            this.isMin = isMin;
        }

        @Override
        void accumulate(RowValues row)
        {
            Object value = row.get(column);
            if (value == null)
                return;
            if (current == null)
            {
                current = value;
                return;
            }
            int comparison = FilterValueOps.compare(column.type, value, current);
            if ((isMin && comparison < 0) || (!isMin && comparison > 0))
                current = value;
        }

        @Override
        void writeTo(FieldVector vector, int rowIndex)
        {
            ArrowRowAssembler.writeDecomposedValue(vector, unwrap(column.type), rowIndex, current);
        }
    }

    private static final class AvgAccumulator extends Accumulator
    {
        private final ColumnMetadata column;
        private double sum;
        private long count;

        AvgAccumulator(ColumnMetadata column)
        {
            this.column = column;
        }

        @Override
        void accumulate(RowValues row)
        {
            Object value = row.get(column);
            if (value == null)
                return;
            sum += toDouble(value);
            count++;
        }

        @Override
        void writeTo(FieldVector vector, int rowIndex)
        {
            if (count == 0)
                vector.setNull(rowIndex);
            else
                ((Float8Vector) vector).setSafe(rowIndex, sum / count);
        }
    }

    // ================= numeric widening (input types are ArrowRowAssembler#decompose's normalized shapes) =================

    private static BigInteger toBigInteger(Object value)
    {
        if (value instanceof BigDecimal)
            return ((BigDecimal) value).toBigIntegerExact();
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long)
            return BigInteger.valueOf(((Number) value).longValue());
        throw new IllegalArgumentException("unexpected value for integral SUM: " + value.getClass());
    }

    private static double toDouble(Object value)
    {
        if (value instanceof BigDecimal)
            return ((BigDecimal) value).doubleValue();
        if (value instanceof Number)
            return ((Number) value).doubleValue();
        throw new IllegalArgumentException("unexpected value for numeric aggregate: " + value.getClass());
    }
}
