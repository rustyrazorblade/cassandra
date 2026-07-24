package io.cassandra.trino.arrowflight;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

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
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers every Arrow -&gt; Trino type-family mapping {@link ArrowTypeMapping} implements - the
 * exact inverse of {@code CassandraArrowTypeMapping} on the Cassandra side.
 */
class ArrowTypeMappingTest
{
    private static Field field(String name, ArrowType type, List<Field> children)
    {
        return new Field(name, new FieldType(true, type, null, Map.of()), children);
    }

    private static Field field(String name, ArrowType type)
    {
        return field(name, type, List.of());
    }

    @Test
    void mapsBoolean()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("b", ArrowType.Bool.INSTANCE))).isEqualTo(BooleanType.BOOLEAN);
    }

    @Test
    void mapsIntegerWidths()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Int(8, true)))).isEqualTo(TinyintType.TINYINT);
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Int(16, true)))).isEqualTo(SmallintType.SMALLINT);
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Int(32, true)))).isEqualTo(IntegerType.INTEGER);
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Int(64, true)))).isEqualTo(BigintType.BIGINT);
    }

    @Test
    void mapsFloatingPoint()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))))
            .isEqualTo(RealType.REAL);
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))))
            .isEqualTo(DoubleType.DOUBLE);
    }

    @Test
    void mapsUtf8AndBinary()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("v", ArrowType.Utf8.INSTANCE))).isEqualTo(VarcharType.VARCHAR);
        assertThat(ArrowTypeMapping.toTrinoType(field("v", ArrowType.Binary.INSTANCE))).isEqualTo(VarbinaryType.VARBINARY);
    }

    @Test
    void mapsFixedSizeBinary16ToUuid()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.FixedSizeBinary(16)))).isEqualTo(UuidType.UUID);
    }

    @Test
    void mapsOtherWidthFixedSizeBinaryToVarbinary()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.FixedSizeBinary(4)))).isEqualTo(VarbinaryType.VARBINARY);
    }

    @Test
    void mapsTimestampToTimestampMillis()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))))
            .isEqualTo(TimestampType.TIMESTAMP_MILLIS);
    }

    @Test
    void mapsDateDayToDate()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Date(DateUnit.DAY)))).isEqualTo(DateType.DATE);
    }

    @Test
    void mapsTimeNanosToTimeNanos()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Time(TimeUnit.NANOSECOND, 64))))
            .isEqualTo(TimeType.TIME_NANOS);
    }

    @Test
    void mapsDurationIntervalToVarcharFallback()
    {
        // Lossy/documented fallback: no Trino interval type covers months+days+nanos together.
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO))))
            .isEqualTo(VarcharType.VARCHAR);
    }

    @Test
    void mapsWideDecimalToVarcharFallback()
    {
        // The server's fixed 76-digit precision exceeds Trino's 38-digit DECIMAL ceiling.
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Decimal(76, 38, 256))))
            .isEqualTo(VarcharType.VARCHAR);
    }

    @Test
    void mapsNarrowDecimalToRealDecimalType()
    {
        assertThat(ArrowTypeMapping.toTrinoType(field("v", new ArrowType.Decimal(10, 2, 128))))
            .isEqualTo(io.trino.spi.type.DecimalType.createDecimalType(10, 2));
    }

    @Test
    void mapsListToArray()
    {
        Field element = field("item", new ArrowType.Int(32, true));
        Field list = field("v", ArrowType.List.INSTANCE, List.of(element));
        assertThat(ArrowTypeMapping.toTrinoType(list)).isEqualTo(new ArrayType(IntegerType.INTEGER));
    }

    @Test
    void mapsFixedSizeListToArray()
    {
        Field element = field("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        Field list = field("v", new ArrowType.FixedSizeList(3), List.of(element));
        assertThat(ArrowTypeMapping.toTrinoType(list)).isEqualTo(new ArrayType(RealType.REAL));
    }

    @Test
    void mapsMapToMapType()
    {
        Field key = field(MapVector.KEY_NAME, ArrowType.Utf8.INSTANCE);
        Field value = field(MapVector.VALUE_NAME, new ArrowType.Int(32, true));
        Field entries = new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(key, value));
        Field map = field("v", new ArrowType.Map(false), List.of(entries));

        Object mapped = ArrowTypeMapping.toTrinoType(map);
        assertThat(mapped).isInstanceOf(MapType.class);
        MapType mapType = (MapType) mapped;
        assertThat(mapType.getKeyType()).isEqualTo(VarcharType.VARCHAR);
        assertThat(mapType.getValueType()).isEqualTo(IntegerType.INTEGER);
    }

    @Test
    void mapsStructToRowByFieldName()
    {
        Field a = field("a", new ArrowType.Int(32, true));
        Field b = field("b", ArrowType.Utf8.INSTANCE);
        Field struct = field("v", ArrowType.Struct.INSTANCE, List.of(a, b));

        Object mapped = ArrowTypeMapping.toTrinoType(struct);
        assertThat(mapped).isInstanceOf(RowType.class);
        RowType rowType = (RowType) mapped;
        assertThat(rowType.getFields()).hasSize(2);
        assertThat(rowType.getFields().get(0).getName()).contains("a");
        assertThat(rowType.getFields().get(0).getType()).isEqualTo(IntegerType.INTEGER);
        assertThat(rowType.getFields().get(1).getName()).contains("b");
        assertThat(rowType.getFields().get(1).getType()).isEqualTo(VarcharType.VARCHAR);
    }

    @Test
    void mapsTupleToRowWithPositionalFieldNames()
    {
        Field first = field("1", new ArrowType.Int(64, true));
        Field second = field("2", ArrowType.Utf8.INSTANCE);
        Field tuple = field("v", ArrowType.Struct.INSTANCE, List.of(first, second));

        RowType rowType = (RowType) ArrowTypeMapping.toTrinoType(tuple);
        assertThat(rowType.getFields().get(0).getName()).contains("1");
        assertThat(rowType.getFields().get(1).getName()).contains("2");
    }

    @Test
    void kindAndPositionMetadataAreReadable()
    {
        Field field = new Field("pk", new FieldType(true, new ArrowType.Int(32, true), null,
            Map.of(ArrowTypeMapping.KIND_METADATA_KEY, "partition_key", ArrowTypeMapping.POSITION_METADATA_KEY, "0")),
            List.of());
        assertThat(ArrowTypeMapping.kindOf(field)).isEqualTo("partition_key");
        assertThat(ArrowTypeMapping.positionOf(field)).hasValue(0);
    }

    @Test
    void positionIsAbsentWhenMetadataMissing()
    {
        Field field = field("v", ArrowType.Utf8.INSTANCE);
        assertThat(ArrowTypeMapping.positionOf(field)).isEmpty();
    }

    @Test
    void throwsForUnhandledArrowType()
    {
        // Null (Arrow's absent-type marker) has no Cassandra source type and is not expected on
        // the wire; the mapping should fail loudly rather than silently guess.
        assertThatThrownBy(() -> ArrowTypeMapping.toTrinoType(field("v", ArrowType.Null.INSTANCE)))
            .isInstanceOf(ArrowTypeMapping.UnsupportedArrowTypeException.class);
    }
}
