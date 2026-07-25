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
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Maps Cassandra's internal type system ({@link AbstractType}) and schema ({@link TableMetadata}/
 * {@link ColumnMetadata}) onto Apache Arrow's {@link ArrowType}/{@link Field}/{@link Schema}.
 * <p>
 * This is a mechanical, largely 1:1 mapping with a few deliberate, documented decisions:
 * <ul>
 *   <li><b>varint/decimal</b> - Cassandra's unbounded-precision numeric types do not fit Arrow's
 *       fixed precision/scale decimal representation. Both map to {@link ArrowType.Decimal} backed
 *       by a 256-bit width, at a single fixed scale ({@link #DECIMAL_SCALE}) for the whole column.
 *       Values that need more fractional digits than the fixed scale, or whose magnitude does not
 *       fit the fixed precision, throw {@link ArrowValueOverflowException} rather than silently
 *       truncating or rounding. This is a real limitation (arbitrary-precision values are, by
 *       definition, not always representable in a fixed-width column) accepted for this PoC.</li>
 *   <li><b>counters</b> - map to a plain {@code Int64}; the counter total is computed by the
 *       caller via {@link org.apache.cassandra.db.context.CounterContext#total} before reaching
 *       this mapping (this class only describes the resulting scalar type).</li>
 *   <li><b>collections</b> - {@code list}/{@code set} map to Arrow {@code List}; {@code map} maps
 *       to Arrow {@code Map}. Cassandra's {@code set} has no native Arrow equivalent.</li>
 *   <li><b>tuples/UDTs</b> - map to Arrow {@code Struct}, fields named positionally ({@code "1"},
 *       {@code "2"}, ...) for tuples, or by field name for UDTs.</li>
 *   <li><b>vectors</b> ({@code VECTOR<T, N>}) - map to Arrow {@code FixedSizeList(N)} of {@code T}.</li>
 *   <li>Column kind (partition key / clustering / static / regular) and, for key columns, their
 *       position within the key, are carried as Arrow field metadata under {@link #KIND_METADATA_KEY}
 *       and {@link #POSITION_METADATA_KEY} so a consumer can reconstruct key structure without a
 *       second schema channel.</li>
 * </ul>
 * Writetime/TTL virtual columns and filter pushdown are out of scope for this PoC (see
 * {@code ARROW-FLIGHT.md}).
 */
public final class CassandraArrowTypeMapping
{
    private CassandraArrowTypeMapping()
    {
    }

    /** Arrow field metadata key carrying the Cassandra column kind (see {@link #kindMetadataValue}). */
    public static final String KIND_METADATA_KEY = "cassandra.kind";
    /** Arrow field metadata key carrying a partition/clustering column's 0-based position in its key. */
    public static final String POSITION_METADATA_KEY = "cassandra.position";

    private static final String KIND_PARTITION_KEY = "partition_key";
    private static final String KIND_CLUSTERING = "clustering";
    private static final String KIND_STATIC = "static";
    private static final String KIND_REGULAR = "regular";

    /**
     * Fixed decimal scale used for both {@code decimal} (arbitrary per-value scale rescaled to
     * this fixed value) and {@code varint} (always scale 0, exact) columns. See the class javadoc.
     */
    public static final int DECIMAL_SCALE = 38;
    /** Max decimal digits a 256-bit Arrow decimal can hold; see {@link #DECIMAL_SCALE}. */
    public static final int DECIMAL_PRECISION = 76;
    /** Bit width backing every {@link ArrowType.Decimal} this mapping produces; see {@link #DECIMAL_PRECISION}. */
    public static final int DECIMAL_BIT_WIDTH = 256;
    // 2^255 - 1: largest magnitude that fits in a signed 256-bit two's-complement integer.
    private static final BigInteger DECIMAL_MAX_UNSCALED = BigInteger.ONE.shiftLeft(DECIMAL_BIT_WIDTH - 1).subtract(BigInteger.ONE);
    private static final BigInteger DECIMAL_MIN_UNSCALED = DECIMAL_MAX_UNSCALED.negate().subtract(BigInteger.ONE);

    /** Thrown when a varint/decimal value does not fit this mapping's fixed-width Arrow decimal. */
    public static class ArrowValueOverflowException extends RuntimeException
    {
        public ArrowValueOverflowException(String message)
        {
            super(message);
        }
    }

    /** Builds the full Arrow {@link Schema} for a table: partition key, clustering, static, then regular columns. */
    public static Schema toArrowSchema(TableMetadata table)
    {
        List<Field> fields = new ArrayList<>(table.partitionKeyColumns().size()
                                              + table.clusteringColumns().size()
                                              + table.regularAndStaticColumns().size());
        for (ColumnMetadata column : table.partitionKeyColumns())
            fields.add(toArrowField(column));
        for (ColumnMetadata column : table.clusteringColumns())
            fields.add(toArrowField(column));
        for (ColumnMetadata column : table.regularAndStaticColumns().statics)
            fields.add(toArrowField(column));
        for (ColumnMetadata column : table.regularAndStaticColumns().regulars)
            fields.add(toArrowField(column));
        return new Schema(fields);
    }

    /** Builds the Arrow {@link Field} for a single column, including its {@code cassandra.kind} metadata. */
    public static Field toArrowField(ColumnMetadata column)
    {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(KIND_METADATA_KEY, kindMetadataValue(column));
        if (column.isPartitionKey() || column.isClusteringColumn())
            metadata.put(POSITION_METADATA_KEY, Integer.toString(column.position()));

        AbstractType<?> type = column.type.isReversed() ? ((ReversedType<?>) column.type).baseType : column.type;
        return arrowField(column.name.toString(), type, true, metadata);
    }

    private static String kindMetadataValue(ColumnMetadata column)
    {
        if (column.isPartitionKey())
            return KIND_PARTITION_KEY;
        if (column.isClusteringColumn())
            return KIND_CLUSTERING;
        if (column.isStatic())
            return KIND_STATIC;
        return KIND_REGULAR;
    }

    /** Returns just the {@link ArrowType} for a Cassandra marshal type, with no field name/metadata/children. */
    public static ArrowType arrowType(AbstractType<?> type)
    {
        return arrowField("_", type, true, Map.of()).getType();
    }

    private static Field arrowField(String name, AbstractType<?> type, boolean nullable, Map<String, String> metadata)
    {
        AbstractType<?> unwrapped = type.isReversed() ? ((ReversedType<?>) type).baseType : type;
        FieldType fieldType;
        List<Field> children = List.of();

        if (unwrapped instanceof CounterColumnType)
        {
            fieldType = new FieldType(nullable, new ArrowType.Int(64, true), null, metadata);
        }
        else if (unwrapped instanceof BooleanType)
        {
            fieldType = new FieldType(nullable, ArrowType.Bool.INSTANCE, null, metadata);
        }
        else if (unwrapped instanceof ByteType)
        {
            fieldType = new FieldType(nullable, new ArrowType.Int(8, true), null, metadata);
        }
        else if (unwrapped instanceof ShortType)
        {
            fieldType = new FieldType(nullable, new ArrowType.Int(16, true), null, metadata);
        }
        else if (unwrapped instanceof Int32Type)
        {
            fieldType = new FieldType(nullable, new ArrowType.Int(32, true), null, metadata);
        }
        else if (unwrapped instanceof LongType)
        {
            fieldType = new FieldType(nullable, new ArrowType.Int(64, true), null, metadata);
        }
        else if (unwrapped instanceof FloatType)
        {
            fieldType = new FieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null, metadata);
        }
        else if (unwrapped instanceof DoubleType)
        {
            fieldType = new FieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null, metadata);
        }
        else if (unwrapped instanceof AsciiType || unwrapped instanceof UTF8Type)
        {
            fieldType = new FieldType(nullable, ArrowType.Utf8.INSTANCE, null, metadata);
        }
        else if (unwrapped instanceof BytesType)
        {
            fieldType = new FieldType(nullable, ArrowType.Binary.INSTANCE, null, metadata);
        }
        else if (unwrapped instanceof InetAddressType)
        {
            // IPv4 (4 bytes) and IPv6 (16 bytes) addresses can both appear in the same column,
            // so a fixed-width binary representation cannot be used here.
            fieldType = new FieldType(nullable, ArrowType.Binary.INSTANCE, null, metadata);
        }
        else if (unwrapped instanceof TimestampType)
        {
            fieldType = new FieldType(nullable, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null, metadata);
        }
        else if (unwrapped instanceof SimpleDateType)
        {
            fieldType = new FieldType(nullable, new ArrowType.Date(DateUnit.DAY), null, metadata);
        }
        else if (unwrapped instanceof TimeType)
        {
            fieldType = new FieldType(nullable, new ArrowType.Time(TimeUnit.NANOSECOND, 64), null, metadata);
        }
        else if (unwrapped instanceof TimeUUIDType || unwrapped instanceof UUIDType)
        {
            fieldType = new FieldType(nullable, new ArrowType.FixedSizeBinary(16), null, metadata);
        }
        else if (unwrapped instanceof DurationType)
        {
            fieldType = new FieldType(nullable, new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO), null, metadata);
        }
        else if (unwrapped instanceof IntegerType)
        {
            // varint: exact (scale 0) within DECIMAL_PRECISION digits, else ArrowValueOverflowException.
            fieldType = new FieldType(nullable, new ArrowType.Decimal(DECIMAL_PRECISION, 0, DECIMAL_BIT_WIDTH), null, metadata);
        }
        else if (unwrapped instanceof DecimalType)
        {
            fieldType = new FieldType(nullable, new ArrowType.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE, DECIMAL_BIT_WIDTH), null, metadata);
        }
        else if (unwrapped instanceof VectorType<?>)
        {
            // Accepted PoC limitation: schema discovery accepts any element type here (including a
            // non-fixed-width one, e.g. VECTOR<text, N>), but the value-writing path
            // (ArrowRowAssembler#decomposeVectorElements) rejects a non-fixed-width element type at
            // SCAN time instead - a real client would only discover the mismatch when it actually
            // tries to read such a table, not at GetSchema time. Narrowing schema discovery to
            // fixed-width-only would need this method to answer "can I even build a schema for this
            // table" earlier than it currently can (it doesn't have failure-reporting plumbing back
            // to GetSchema/GetFlightInfo callers today) - not worth adding for a PoC whose type
            // mapping doc already scopes VECTOR to fixed-width element types.
            VectorType<?> vectorType = (VectorType<?>) unwrapped;
            fieldType = new FieldType(nullable, new ArrowType.FixedSizeList(vectorType.dimension), null, metadata);
            children = List.of(arrowField("item", vectorType.elementType, true, Map.of()));
        }
        else if (unwrapped instanceof SetType<?>)
        {
            fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null, metadata);
            children = List.of(arrowField("item", ((SetType<?>) unwrapped).getElementsType(), true, Map.of()));
        }
        else if (unwrapped instanceof ListType<?>)
        {
            fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null, metadata);
            children = List.of(arrowField("item", ((ListType<?>) unwrapped).getElementsType(), true, Map.of()));
        }
        else if (unwrapped instanceof MapType<?, ?>)
        {
            MapType<?, ?> mapType = (MapType<?, ?>) unwrapped;
            fieldType = new FieldType(nullable, new ArrowType.Map(false), null, metadata);
            Field keyField = arrowField(org.apache.arrow.vector.complex.MapVector.KEY_NAME, mapType.getKeysType(), false, Map.of());
            Field valueField = arrowField(org.apache.arrow.vector.complex.MapVector.VALUE_NAME, mapType.getValuesType(), true, Map.of());
            Field entries = new Field(org.apache.arrow.vector.complex.MapVector.DATA_VECTOR_NAME,
                                       FieldType.notNullable(ArrowType.Struct.INSTANCE),
                                       List.of(keyField, valueField));
            children = List.of(entries);
        }
        else if (unwrapped instanceof UserType)
        {
            UserType userType = (UserType) unwrapped;
            List<Field> fieldChildren = new ArrayList<>(userType.size());
            for (int i = 0; i < userType.size(); i++)
                fieldChildren.add(arrowField(userType.fieldNameAsString(i), userType.fieldType(i), true, Map.of()));
            fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null, metadata);
            children = fieldChildren;
        }
        else if (unwrapped instanceof TupleType)
        {
            TupleType tupleType = (TupleType) unwrapped;
            List<Field> fieldChildren = new ArrayList<>(tupleType.size());
            for (int i = 0; i < tupleType.size(); i++)
                fieldChildren.add(arrowField(Integer.toString(i + 1), tupleType.type(i), true, Map.of()));
            fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null, metadata);
            children = fieldChildren;
        }
        else if (unwrapped instanceof CollectionType)
        {
            throw new IllegalArgumentException("Unhandled collection type: " + unwrapped);
        }
        else
        {
            throw new IllegalArgumentException("No Arrow type mapping for Cassandra type: " + unwrapped.asCQL3Type());
        }

        return new Field(name, fieldType, children);
    }

    /**
     * Rescales {@code value} to {@link #DECIMAL_SCALE} and returns its unscaled 256-bit-representable
     * value, throwing {@link ArrowValueOverflowException} if the value needs more fractional digits
     * than {@link #DECIMAL_SCALE} or does not fit {@link #DECIMAL_PRECISION} digits.
     */
    public static BigDecimal toArrowDecimal(BigDecimal value)
    {
        BigDecimal rescaled;
        try
        {
            rescaled = value.setScale(DECIMAL_SCALE, RoundingMode.UNNECESSARY);
        }
        catch (ArithmeticException e)
        {
            throw new ArrowValueOverflowException("decimal value " + value + " has more than " + DECIMAL_SCALE +
                                                   " fractional digits; cannot represent exactly as a fixed-scale Arrow decimal");
        }
        checkUnscaledFits(rescaled.unscaledValue(), value);
        return rescaled;
    }

    /** Converts a {@code varint} value to a scale-0 {@link BigDecimal}, checking it fits the 256-bit decimal. */
    public static BigDecimal toArrowDecimal(BigInteger varint)
    {
        checkUnscaledFits(varint, varint);
        return new BigDecimal(varint, 0);
    }

    private static void checkUnscaledFits(BigInteger unscaled, Object original)
    {
        if (unscaled.compareTo(DECIMAL_MAX_UNSCALED) > 0 || unscaled.compareTo(DECIMAL_MIN_UNSCALED) < 0)
        {
            throw new ArrowValueOverflowException("value " + original + " does not fit in a " + DECIMAL_PRECISION +
                                                   "-digit (256-bit) Arrow decimal");
        }
    }
}
