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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.cassandra.arrow.AggregationSpec.AggregateFunction;
import org.apache.cassandra.arrow.AggregationSpec.AggregateSpec;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@link AggregationSpec} resolved against a specific table: {@code groupBy}/aggregate column
 * names turned into {@link ColumnMetadata}, plus the Arrow output {@link Schema} both
 * {@code getFlightInfo} (schema discovery) and {@link RowAggregator} (final batch construction)
 * need. See {@code ARROW-FLIGHT.md} for the wire format.
 * <p>
 * <b>Output type judgment calls (documented, see task spec):</b>
 * <ul>
 *   <li>{@code COUNT} (star or column) -&gt; {@code Int64}.</li>
 *   <li>{@code SUM} over an integer-family column (byte/short/int/long/varint/counter) -&gt;
 *       {@code Decimal256} at scale 0 - avoids the classic "sum of many longs overflows a long"
 *       surprise; matches this mapping's existing varint/decimal-as-Decimal256 convention
 *       ({@link CassandraArrowTypeMapping}).</li>
 *   <li>{@code SUM} over a {@code decimal} column -&gt; {@code Decimal256} at
 *       {@link CassandraArrowTypeMapping#DECIMAL_SCALE} (same fixed scale the plain decimal mapping
 *       already uses).</li>
 *   <li>{@code SUM} over a {@code float}/{@code double} column -&gt; {@code Float8}; floating-point
 *       accumulation error is an accepted, well-known cost of summing floats at all, not something
 *       this mapping can paper over by changing output type.</li>
 *   <li>{@code MIN}/{@code MAX} -&gt; the same Arrow type as the input column.</li>
 *   <li>{@code AVG} -&gt; always {@code Float8} (computed internally as {@code SUM}/{@code COUNT} in
 *       {@code double} arithmetic), regardless of input type - simplest single behavior to document
 *       and test, at the cost of decimal exactness for {@code AVG} over {@code decimal}/varint
 *       columns; revisit (e.g. a {@code Decimal256} AVG mode) if a real workload needs it.</li>
 * </ul>
 * {@code SUM}/{@code MIN}/{@code MAX}/{@code AVG} over a collection/tuple/UDT/vector/duration column
 * is rejected at compile time (matches {@link FilterValueOps}'s equivalent restriction for filters -
 * none of these have a well-defined sum/order in CQL either).
 */
public final class CompiledAggregation
{
    public final List<ColumnMetadata> groupBy;
    public final List<CompiledAggregate> aggregates;
    public final Schema outputSchema;

    private CompiledAggregation(List<ColumnMetadata> groupBy, List<CompiledAggregate> aggregates, Schema outputSchema)
    {
        this.groupBy = groupBy;
        this.aggregates = aggregates;
        this.outputSchema = outputSchema;
    }

    public enum AccumulatorKind
    {
        COUNT, SUM_INTEGRAL, SUM_DECIMAL, SUM_FLOATING, MIN, MAX, AVG
    }

    public static final class CompiledAggregate
    {
        public final AggregateFunction function;
        public final ColumnMetadata column; // nullable, only for COUNT(*)
        public final String outputName;
        public final AccumulatorKind accumulatorKind;

        CompiledAggregate(AggregateFunction function, ColumnMetadata column, String outputName, AccumulatorKind accumulatorKind)
        {
            this.function = function;
            this.column = column;
            this.outputName = outputName;
            this.accumulatorKind = accumulatorKind;
        }
    }

    public static CompiledAggregation compile(AggregationSpec spec, TableMetadata table)
    {
        List<ColumnMetadata> groupByColumns = new ArrayList<>(spec.groupBy.size());
        for (String name : spec.groupBy)
            groupByColumns.add(resolveColumn(table, name));

        List<CompiledAggregate> aggregates = new ArrayList<>(spec.aggregates.size());
        List<Field> fields = new ArrayList<>(groupByColumns.size() + spec.aggregates.size());
        for (ColumnMetadata column : groupByColumns)
            fields.add(CassandraArrowTypeMapping.toArrowField(column));

        for (AggregateSpec aggregateSpec : spec.aggregates)
        {
            ColumnMetadata column = aggregateSpec.column == null ? null : resolveColumn(table, aggregateSpec.column);
            AccumulatorKind kind = accumulatorKind(aggregateSpec.function, column);
            aggregates.add(new CompiledAggregate(aggregateSpec.function, column, aggregateSpec.outputName, kind));
            fields.add(outputField(aggregateSpec.outputName, aggregateSpec.function, column, kind));
        }

        return new CompiledAggregation(groupByColumns, aggregates, new Schema(fields));
    }

    private static ColumnMetadata resolveColumn(TableMetadata table, String name)
    {
        ColumnMetadata column = table.getColumn(ColumnIdentifier.getInterned(name, true));
        if (column == null)
            throw CallStatus.INVALID_ARGUMENT.withDescription("no such column: " + name).toRuntimeException();
        return column;
    }

    private static AccumulatorKind accumulatorKind(AggregateFunction function, ColumnMetadata column)
    {
        switch (function)
        {
            case COUNT:
                return AccumulatorKind.COUNT;
            case AVG:
                requireNumeric(column, "AVG");
                return AccumulatorKind.AVG;
            case MIN:
                requireOrderable(column, "MIN");
                return AccumulatorKind.MIN;
            case MAX:
                requireOrderable(column, "MAX");
                return AccumulatorKind.MAX;
            case SUM:
                return sumKind(column);
            default:
                throw new IllegalStateException("unhandled aggregate function " + function);
        }
    }

    private static AccumulatorKind sumKind(ColumnMetadata column)
    {
        AbstractType<?> type = unwrap(column.type);
        if (isIntegerFamily(type))
            return AccumulatorKind.SUM_INTEGRAL;
        if (type instanceof DecimalType)
            return AccumulatorKind.SUM_DECIMAL;
        if (type instanceof FloatType || type instanceof DoubleType)
            return AccumulatorKind.SUM_FLOATING;
        throw CallStatus.INVALID_ARGUMENT.withDescription("SUM is not supported for column " + column.name + " of type " + type.asCQL3Type()).toRuntimeException();
    }

    private static void requireNumeric(ColumnMetadata column, String function)
    {
        AbstractType<?> type = unwrap(column.type);
        if (!isIntegerFamily(type) && !(type instanceof DecimalType) && !(type instanceof FloatType) && !(type instanceof DoubleType))
            throw CallStatus.INVALID_ARGUMENT.withDescription(function + " is not supported for column " + column.name + " of type " + type.asCQL3Type()).toRuntimeException();
    }

    private static void requireOrderable(ColumnMetadata column, String function)
    {
        AbstractType<?> type = unwrap(column.type);
        if (type instanceof CollectionType || type instanceof TupleType || type instanceof VectorType || type instanceof DurationType)
            throw CallStatus.INVALID_ARGUMENT.withDescription(function + " is not supported for column " + column.name + " of type " + type.asCQL3Type()).toRuntimeException();
    }

    private static boolean isIntegerFamily(AbstractType<?> type)
    {
        return type instanceof ByteType || type instanceof ShortType || type instanceof Int32Type
               || type instanceof LongType || type instanceof IntegerType || type instanceof CounterColumnType;
    }

    private static Field outputField(String outputName, AggregateFunction function, ColumnMetadata column, AccumulatorKind kind)
    {
        switch (function)
        {
            case COUNT:
                return new Field(outputName, FieldType.nullable(new ArrowType.Int(64, true)), List.of());
            case AVG:
                return new Field(outputName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), List.of());
            case MIN:
            case MAX:
                return new Field(outputName, FieldType.nullable(CassandraArrowTypeMapping.arrowType(unwrap(column.type))), List.of());
            case SUM:
                switch (kind)
                {
                    case SUM_INTEGRAL:
                        return new Field(outputName, FieldType.nullable(new ArrowType.Decimal(CassandraArrowTypeMapping.DECIMAL_PRECISION, 0, CassandraArrowTypeMapping.DECIMAL_BIT_WIDTH)), List.of());
                    case SUM_DECIMAL:
                        return new Field(outputName, FieldType.nullable(new ArrowType.Decimal(CassandraArrowTypeMapping.DECIMAL_PRECISION, CassandraArrowTypeMapping.DECIMAL_SCALE, CassandraArrowTypeMapping.DECIMAL_BIT_WIDTH)), List.of());
                    case SUM_FLOATING:
                        return new Field(outputName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), List.of());
                    default:
                        throw new IllegalStateException("unhandled SUM kind " + kind);
                }
            default:
                throw new IllegalStateException("unhandled aggregate function " + function);
        }
    }

    private static AbstractType<?> unwrap(AbstractType<?> type)
    {
        return type.isReversed() ? ((ReversedType<?>) type).baseType : type;
    }
}
