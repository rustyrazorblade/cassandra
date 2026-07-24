package io.cassandra.trino.arrowflight;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
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
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link ArrowPageBuilder}'s Arrow batch -&gt; Trino {@link Page} conversion for every
 * type family {@link ArrowTypeMapping} maps to, including null handling and nested
 * array/map/row values.
 */
class ArrowPageBuilderTest
{
    private BufferAllocator allocator;

    @BeforeEach
    void setUp()
    {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown()
    {
        allocator.close();
    }

    private static Field field(String name, ArrowType type, List<Field> children)
    {
        return new Field(name, new FieldType(true, type, null, Map.of()), children);
    }

    private static Field field(String name, ArrowType type)
    {
        return field(name, type, List.of());
    }

    private VectorSchemaRoot singleColumnRoot(Field column)
    {
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(column)), allocator);
        root.allocateNew();
        return root;
    }

    private static Page toPage(VectorSchemaRoot root, Type type)
    {
        String name = root.getSchema().getFields().get(0).getName();
        return ArrowPageBuilder.toPage(root, List.of(name), List.of(type));
    }

    @Test
    void convertsBooleanWithNull()
    {
        Field f = field("v", ArrowType.Bool.INSTANCE);
        try (VectorSchemaRoot root = singleColumnRoot(f))
        {
            BitVector v = (BitVector) root.getVector("v");
            v.setSafe(0, 1);
            v.setNull(1);
            root.setRowCount(2);

            Block block = toPage(root, BooleanType.BOOLEAN).getBlock(0);
            assertThat(BooleanType.BOOLEAN.getBoolean(block, 0)).isTrue();
            assertThat(block.isNull(1)).isTrue();
        }
    }

    @Test
    void convertsIntegerWidths()
    {
        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.Int(8, true))))
        {
            ((TinyIntVector) root.getVector("v")).setSafe(0, (byte) 42);
            root.setRowCount(1);
            assertThat(TinyintType.TINYINT.getLong(toPage(root, TinyintType.TINYINT).getBlock(0), 0)).isEqualTo(42);
        }
        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.Int(16, true))))
        {
            ((SmallIntVector) root.getVector("v")).setSafe(0, (short) 1234);
            root.setRowCount(1);
            assertThat(SmallintType.SMALLINT.getLong(toPage(root, SmallintType.SMALLINT).getBlock(0), 0)).isEqualTo(1234);
        }
        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.Int(32, true))))
        {
            ((IntVector) root.getVector("v")).setSafe(0, 123456);
            root.setRowCount(1);
            assertThat(IntegerType.INTEGER.getLong(toPage(root, IntegerType.INTEGER).getBlock(0), 0)).isEqualTo(123456);
        }
        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.Int(64, true))))
        {
            ((BigIntVector) root.getVector("v")).setSafe(0, 9_876_543_210L);
            root.setRowCount(1);
            assertThat(BigintType.BIGINT.getLong(toPage(root, BigintType.BIGINT).getBlock(0), 0)).isEqualTo(9_876_543_210L);
        }
    }

    @Test
    void convertsFloatingPoint()
    {
        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))))
        {
            ((Float4Vector) root.getVector("v")).setSafe(0, 1.5f);
            root.setRowCount(1);
            long bits = RealType.REAL.getLong(toPage(root, RealType.REAL).getBlock(0), 0);
            assertThat(Float.intBitsToFloat((int) bits)).isEqualTo(1.5f);
        }
        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))))
        {
            ((Float8Vector) root.getVector("v")).setSafe(0, 2.5);
            root.setRowCount(1);
            assertThat(DoubleType.DOUBLE.getDouble(toPage(root, DoubleType.DOUBLE).getBlock(0), 0)).isEqualTo(2.5);
        }
    }

    @Test
    void convertsUtf8AndBinary()
    {
        try (VectorSchemaRoot root = singleColumnRoot(field("v", ArrowType.Utf8.INSTANCE)))
        {
            ((VarCharVector) root.getVector("v")).setSafe(0, "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            root.setRowCount(1);
            assertThat(VarcharType.VARCHAR.getSlice(toPage(root, VarcharType.VARCHAR).getBlock(0), 0).toStringUtf8())
                .isEqualTo("hello");
        }
        try (VectorSchemaRoot root = singleColumnRoot(field("v", ArrowType.Binary.INSTANCE)))
        {
            byte[] bytes = {1, 2, 3, 4};
            ((VarBinaryVector) root.getVector("v")).setSafe(0, bytes);
            root.setRowCount(1);
            assertThat(VarbinaryType.VARBINARY.getSlice(toPage(root, VarbinaryType.VARBINARY).getBlock(0), 0).getBytes())
                .isEqualTo(bytes);
        }
    }

    @Test
    void convertsUuid()
    {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = new byte[16];
        java.nio.ByteBuffer.wrap(bytes).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());

        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.FixedSizeBinary(16))))
        {
            ((FixedSizeBinaryVector) root.getVector("v")).setSafe(0, bytes);
            root.setRowCount(1);
            io.airlift.slice.Slice slice = UuidType.UUID.getSlice(toPage(root, UuidType.UUID).getBlock(0), 0);
            assertThat(UuidType.trinoUuidToJavaUuid(slice)).isEqualTo(uuid);
        }
    }

    @Test
    void convertsDateTimeAndTimestamp()
    {
        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.Date(DateUnit.DAY))))
        {
            ((DateDayVector) root.getVector("v")).setSafe(0, 19_000);
            root.setRowCount(1);
            assertThat(DateType.DATE.getLong(toPage(root, DateType.DATE).getBlock(0), 0)).isEqualTo(19_000);
        }
        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.Time(TimeUnit.NANOSECOND, 64))))
        {
            ((TimeNanoVector) root.getVector("v")).setSafe(0, 3_600_000_000_000L); // 1 hour, in nanos
            root.setRowCount(1);
            long picosOfDay = TimeType.TIME_NANOS.getLong(toPage(root, TimeType.TIME_NANOS).getBlock(0), 0);
            assertThat(picosOfDay).isEqualTo(3_600_000_000_000L * 1_000L);
        }
        try (VectorSchemaRoot root = singleColumnRoot(field("v", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))))
        {
            ((TimeStampMilliTZVector) root.getVector("v")).setSafe(0, 1_700_000_000_000L);
            root.setRowCount(1);
            long epochMicros = io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS
                .getLong(toPage(root, io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS).getBlock(0), 0);
            assertThat(epochMicros).isEqualTo(1_700_000_000_000L * 1_000L);
        }
    }

    @Test
    void convertsWideDecimalToVarcharPreservingExactValue()
    {
        Field f = field("v", new ArrowType.Decimal(76, 38, 256));
        try (VectorSchemaRoot root = singleColumnRoot(f))
        {
            Decimal256Vector v = (Decimal256Vector) root.getVector("v");
            BigDecimal value = new BigDecimal("123456789012345678901234567890.123456789012345678")
                .setScale(38, java.math.RoundingMode.UNNECESSARY);
            v.setSafe(0, value);
            root.setRowCount(1);

            String rendered = VarcharType.VARCHAR.getSlice(toPage(root, VarcharType.VARCHAR).getBlock(0), 0).toStringUtf8();
            assertThat(new BigDecimal(rendered)).isEqualByComparingTo(value);
        }
    }

    @Test
    void convertsDurationToVarcharIso8601Fallback()
    {
        Field f = field("v", new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO));
        try (VectorSchemaRoot root = singleColumnRoot(f))
        {
            IntervalMonthDayNanoVector v = (IntervalMonthDayNanoVector) root.getVector("v");
            v.setSafe(0, 14, 2, 3_600_000_000_000L);
            root.setRowCount(1);

            String rendered = VarcharType.VARCHAR.getSlice(toPage(root, VarcharType.VARCHAR).getBlock(0), 0).toStringUtf8();
            assertThat(rendered).startsWith("P");
        }
    }

    @Test
    void convertsListOfIntegers()
    {
        Field element = field("item", new ArrowType.Int(32, true));
        Field listField = field("v", ArrowType.List.INSTANCE, List.of(element));
        try (VectorSchemaRoot root = singleColumnRoot(listField))
        {
            ListVector listVector = (ListVector) root.getVector("v");
            UnionListWriter writer = listVector.getWriter();
            writer.allocate();
            writer.setPosition(0);
            writer.startList();
            writer.integer().writeInt(10);
            writer.integer().writeInt(20);
            writer.integer().writeInt(30);
            writer.endList();
            writer.setValueCount(1);
            root.setRowCount(1);

            ArrayType arrayType = new ArrayType(IntegerType.INTEGER);
            Block arrayBlock = toPage(root, arrayType).getBlock(0);
            Block elements = arrayType.getObject(arrayBlock, 0);
            assertThat(elements.getPositionCount()).isEqualTo(3);
            assertThat(IntegerType.INTEGER.getLong(elements, 0)).isEqualTo(10);
            assertThat(IntegerType.INTEGER.getLong(elements, 1)).isEqualTo(20);
            assertThat(IntegerType.INTEGER.getLong(elements, 2)).isEqualTo(30);
        }
    }

    @Test
    void convertsMapOfVarcharToInteger()
    {
        Field key = new Field(MapVector.KEY_NAME, FieldType.notNullable(ArrowType.Utf8.INSTANCE), List.of());
        Field value = new Field(MapVector.VALUE_NAME, FieldType.nullable(new ArrowType.Int(32, true)), List.of());
        Field entries = new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(key, value));
        Field mapField = field("v", new ArrowType.Map(false), List.of(entries));

        try (VectorSchemaRoot root = singleColumnRoot(mapField))
        {
            MapVector mapVector = (MapVector) root.getVector("v");
            UnionMapWriter writer = mapVector.getWriter();
            writer.allocate();
            writer.setPosition(0);
            writer.startMap();
            writer.startEntry();
            writer.key().varChar().writeVarChar("a");
            writer.value().integer().writeInt(1);
            writer.endEntry();
            writer.startEntry();
            writer.key().varChar().writeVarChar("b");
            writer.value().integer().writeInt(2);
            writer.endEntry();
            writer.endMap();
            writer.setValueCount(1);
            root.setRowCount(1);

            MapType mapType = new MapType(VarcharType.VARCHAR, IntegerType.INTEGER, new TypeOperators());
            Block mapBlock = toPage(root, mapType).getBlock(0);
            SqlMap sqlMap = mapType.getObject(mapBlock, 0);
            assertThat(sqlMap.getSize()).isEqualTo(2);
            int offset = sqlMap.getRawOffset();
            Block keys = sqlMap.getRawKeyBlock();
            Block values = sqlMap.getRawValueBlock();
            assertThat(VarcharType.VARCHAR.getSlice(keys, offset).toStringUtf8()).isEqualTo("a");
            assertThat(IntegerType.INTEGER.getLong(values, offset)).isEqualTo(1);
            assertThat(VarcharType.VARCHAR.getSlice(keys, offset + 1).toStringUtf8()).isEqualTo("b");
            assertThat(IntegerType.INTEGER.getLong(values, offset + 1)).isEqualTo(2);
        }
    }

    @Test
    void convertsStructToRow()
    {
        Field a = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
        Field b = new Field("b", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of());
        Field structField = field("v", ArrowType.Struct.INSTANCE, List.of(a, b));

        try (VectorSchemaRoot root = singleColumnRoot(structField))
        {
            StructVector structVector = (StructVector) root.getVector("v");
            structVector.getChild("a", IntVector.class).setSafe(0, 7);
            structVector.getChild("b", VarCharVector.class).setSafe(0, "x".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            structVector.setIndexDefined(0);
            structVector.setValueCount(1);
            root.setRowCount(1);

            RowType rowType = RowType.from(List.of(RowType.field("a", IntegerType.INTEGER), RowType.field("b", VarcharType.VARCHAR)));
            Block rowBlock = toPage(root, rowType).getBlock(0);
            SqlRow row = rowType.getObject(rowBlock, 0);
            int index = row.getRawIndex();
            assertThat(IntegerType.INTEGER.getLong(row.getRawFieldBlock(0), index)).isEqualTo(7);
            assertThat(VarcharType.VARCHAR.getSlice(row.getRawFieldBlock(1), index).toStringUtf8()).isEqualTo("x");
        }
    }

    @Test
    void missingProjectedColumnFailsLoudly()
    {
        Field f = field("v", ArrowType.Utf8.INSTANCE);
        try (VectorSchemaRoot root = singleColumnRoot(f))
        {
            root.setRowCount(0);
            assertThat(org.assertj.core.api.Assertions.catchThrowable(() ->
                ArrowPageBuilder.toPage(root, List.of("does_not_exist"), List.of(VarcharType.VARCHAR))))
                .isInstanceOf(IllegalStateException.class);
        }
    }
}
