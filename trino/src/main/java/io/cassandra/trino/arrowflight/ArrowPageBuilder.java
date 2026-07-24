package io.cassandra.trino.arrowflight;

import java.math.BigDecimal;
import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.Text;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

/**
 * Converts one Arrow {@link VectorSchemaRoot} batch (as delivered by the Cassandra Arrow
 * Flight service) into a Trino {@link Page}, projecting to the requested columns and
 * building nested (array/map/row) blocks recursively.
 *
 * <p>This is a straightforward per-value copy, not zero-copy sharing of Arrow buffers with
 * Trino blocks - acceptable for this PoC (see {@code trino/README.md}); a zero-copy path is
 * a documented stretch goal, not implemented here.
 *
 * <p>Note: {@link ArrowTypeMapping} never actually produces a Trino {@code DECIMAL} type on the
 * current wire (the server's fixed 76-digit precision always falls back to {@code VARCHAR} -
 * see its class javadoc), so writing a real {@code DecimalType} column is intentionally not
 * implemented here; if a future narrower server-side precision changes that, this class needs a
 * matching {@code DecimalType} branch in {@link #writeValue}.
 */
public final class ArrowPageBuilder
{
    private ArrowPageBuilder()
    {
    }

    /** Converts {@code root} to a {@link Page}, with columns in the given order. */
    public static Page toPage(VectorSchemaRoot root, List<String> columnNames, List<Type> columnTypes)
    {
        int rowCount = root.getRowCount();
        Block[] blocks = new Block[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++)
        {
            FieldVector vector = root.getVector(columnNames.get(i));
            if (vector == null)
                throw new IllegalStateException(
                    "Column '" + columnNames.get(i) + "' requested by the connector is missing from the "
                    + "delivered Arrow batch; schema drift between planning and scan time");
            blocks[i] = toBlock(columnTypes.get(i), vector, rowCount);
        }
        return new Page(rowCount, blocks);
    }

    private static Block toBlock(Type type, FieldVector vector, int rowCount)
    {
        BlockBuilder builder = type.createBlockBuilder(null, rowCount);
        for (int position = 0; position < rowCount; position++)
        {
            if (vector.isNull(position))
                builder.appendNull();
            else
                writeValue(type, vector, position, builder);
        }
        return builder.build();
    }

    private static void writeValue(Type type, FieldVector vector, int position, BlockBuilder builder)
    {
        if (type instanceof BooleanType)
        {
            BooleanType.BOOLEAN.writeBoolean(builder, ((BitVector) vector).get(position) != 0);
        }
        else if (type instanceof TinyintType)
        {
            TinyintType.TINYINT.writeLong(builder, ((TinyIntVector) vector).get(position));
        }
        else if (type instanceof SmallintType)
        {
            SmallintType.SMALLINT.writeLong(builder, ((SmallIntVector) vector).get(position));
        }
        else if (type instanceof IntegerType)
        {
            IntegerType.INTEGER.writeLong(builder, ((IntVector) vector).get(position));
        }
        else if (type instanceof BigintType)
        {
            BigintType.BIGINT.writeLong(builder, ((BigIntVector) vector).get(position));
        }
        else if (type instanceof RealType)
        {
            RealType.REAL.writeLong(builder, Float.floatToRawIntBits(((Float4Vector) vector).get(position)));
        }
        else if (type instanceof DoubleType)
        {
            DoubleType.DOUBLE.writeDouble(builder, ((Float8Vector) vector).get(position));
        }
        else if (type instanceof DateType)
        {
            DateType.DATE.writeLong(builder, ((DateDayVector) vector).get(position));
        }
        else if (type instanceof TimestampType timestampType)
        {
            writeTimestamp(timestampType, vector, position, builder);
        }
        else if (type instanceof TimeType timeType)
        {
            writeTime(timeType, vector, position, builder);
        }
        else if (type instanceof UuidType)
        {
            UuidType.UUID.writeSlice(builder, Slices.wrappedBuffer(((FixedSizeBinaryVector) vector).get(position)));
        }
        else if (type instanceof VarcharType)
        {
            writeVarchar((VarcharType) type, vector, position, builder);
        }
        else if (type instanceof VarbinaryType)
        {
            VarbinaryType.VARBINARY.writeSlice(builder, Slices.wrappedBuffer(binaryBytes(vector, position)));
        }
        else if (type instanceof ArrayType arrayType)
        {
            writeArray(arrayType, vector, position, builder);
        }
        else if (type instanceof MapType mapType)
        {
            writeMap(mapType, vector, position, builder);
        }
        else if (type instanceof RowType rowType)
        {
            writeRow(rowType, vector, position, builder);
        }
        else
        {
            throw new IllegalArgumentException("Unsupported Trino type for Arrow conversion: " + type);
        }
    }

    /**
     * Arrow timestamps arrive as {@code Timestamp(MILLISECOND, "UTC")} (see
     * {@link ArrowTypeMapping} class javadoc); Trino's short {@code TIMESTAMP} representation
     * is always epoch-MICROSECONDS regardless of declared precision, so this upscales x1000.
     */
    private static void writeTimestamp(TimestampType type, FieldVector vector, int position, BlockBuilder builder)
    {
        long epochMillis = ((org.apache.arrow.vector.TimeStampVector) vector).get(position);
        type.writeLong(builder, Math.multiplyExact(epochMillis, 1_000L));
    }

    /**
     * Arrow times arrive as {@code Time(NANOSECOND, 64)}; Trino's {@code TIME} representation
     * is always picoseconds-of-day regardless of declared precision, so this upscales x1000.
     */
    private static void writeTime(TimeType type, FieldVector vector, int position, BlockBuilder builder)
    {
        long nanosOfDay = ((TimeNanoVector) vector).get(position);
        type.writeLong(builder, Math.multiplyExact(nanosOfDay, 1_000L));
    }

    private static void writeVarchar(VarcharType type, FieldVector vector, int position, BlockBuilder builder)
    {
        if (vector instanceof VarCharVector v)
        {
            type.writeSlice(builder, Slices.wrappedBuffer(v.get(position)));
        }
        else if (vector instanceof DecimalVector v)
        {
            // varint/decimal fallback to VARCHAR (see ArrowTypeMapping class javadoc): render
            // the exact BigDecimal value as its canonical plain-string form.
            type.writeSlice(builder, Slices.utf8Slice(v.getObject(position).toPlainString()));
        }
        else if (vector instanceof org.apache.arrow.vector.Decimal256Vector v)
        {
            // The server's varint/decimal columns are always 256-bit (see
            // ArrowTypeMapping class javadoc), so this is the branch actually exercised on the
            // wire; DecimalVector (128-bit) above is handled too in case that ever changes.
            type.writeSlice(builder, Slices.utf8Slice(v.getObject(position).toPlainString()));
        }
        else if (vector instanceof org.apache.arrow.vector.IntervalMonthDayNanoVector v)
        {
            // duration fallback to VARCHAR (see ArrowTypeMapping class javadoc): no clean Trino
            // interval type covers months+days+nanos together, so render Arrow's own ISO-8601
            // interval string - informational display only, not usable for interval arithmetic.
            type.writeSlice(builder, Slices.utf8Slice(v.getObject(position).toISO8601IntervalString()));
        }
        else
        {
            throw new IllegalArgumentException(
                "Cannot map Arrow vector " + vector.getClass().getSimpleName() + " to VARCHAR");
        }
    }

    private static byte[] binaryBytes(FieldVector vector, int position)
    {
        if (vector instanceof VarBinaryVector v)
            return v.get(position);
        if (vector instanceof FixedSizeBinaryVector v)
            return v.get(position);
        throw new IllegalArgumentException(
            "Cannot map Arrow vector " + vector.getClass().getSimpleName() + " to VARBINARY");
    }

    private static void writeArray(ArrayType type, FieldVector vector, int position, BlockBuilder builder)
    {
        Type elementType = type.getElementType();
        FieldVector elements;
        int start;
        int end;
        if (vector instanceof ListVector list)
        {
            elements = list.getDataVector();
            start = list.getElementStartIndex(position);
            end = list.getElementEndIndex(position);
        }
        else if (vector instanceof FixedSizeListVector list)
        {
            elements = list.getDataVector();
            start = position * list.getListSize();
            end = start + list.getListSize();
        }
        else
        {
            throw new IllegalArgumentException(
                "Cannot map Arrow vector " + vector.getClass().getSimpleName() + " to ARRAY");
        }
        ((io.trino.spi.block.ArrayBlockBuilder) builder).buildEntry(elementBuilder -> {
            for (int i = start; i < end; i++)
            {
                if (elements.isNull(i))
                    elementBuilder.appendNull();
                else
                    writeValue(elementType, elements, i, elementBuilder);
            }
        });
    }

    private static void writeMap(MapType type, FieldVector vector, int position, BlockBuilder builder)
    {
        if (!(vector instanceof MapVector mapVector))
            throw new IllegalArgumentException(
                "Cannot map Arrow vector " + vector.getClass().getSimpleName() + " to MAP");
        StructVector entries = (StructVector) mapVector.getDataVector();
        FieldVector keys = entries.getChild(MapVector.KEY_NAME, FieldVector.class);
        FieldVector values = entries.getChild(MapVector.VALUE_NAME, FieldVector.class);
        int start = mapVector.getElementStartIndex(position);
        int end = mapVector.getElementEndIndex(position);
        Type keyType = type.getKeyType();
        Type valueType = type.getValueType();
        ((io.trino.spi.block.MapBlockBuilder) builder).buildEntry((keyBuilder, valueBuilder) -> {
            for (int i = start; i < end; i++)
            {
                writeValue(keyType, keys, i, keyBuilder);
                if (values.isNull(i))
                    valueBuilder.appendNull();
                else
                    writeValue(valueType, values, i, valueBuilder);
            }
        });
    }

    private static void writeRow(RowType type, FieldVector vector, int position, BlockBuilder builder)
    {
        if (!(vector instanceof StructVector struct))
            throw new IllegalArgumentException(
                "Cannot map Arrow vector " + vector.getClass().getSimpleName() + " to ROW");
        List<RowType.Field> fields = type.getFields();
        ((io.trino.spi.block.RowBlockBuilder) builder).buildEntry(fieldBuilders -> {
            for (int i = 0; i < fields.size(); i++)
            {
                FieldVector child = struct.getChildrenFromFields().get(i);
                BlockBuilder fieldBuilder = fieldBuilders.get(i);
                if (child.isNull(position))
                    fieldBuilder.appendNull();
                else
                    writeValue(fields.get(i).getType(), child, position, fieldBuilder);
            }
        });
    }

    /** {@link Text}/UTF-8 helper retained for callers reading Arrow values outside a Block (tests). */
    public static String textOf(Object arrowObject)
    {
        return arrowObject instanceof Text text ? text.toString() : String.valueOf(arrowObject);
    }
}
