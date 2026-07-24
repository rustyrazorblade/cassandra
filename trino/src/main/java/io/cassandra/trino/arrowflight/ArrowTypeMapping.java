package io.cassandra.trino.arrowflight;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

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
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

/**
 * Maps Apache Arrow types (as produced by
 * {@code org.apache.cassandra.arrow.CassandraArrowTypeMapping} on the server side) onto
 * Trino {@link Type}s. This is the inverse of that class; see its javadoc for the
 * Cassandra -&gt; Arrow half of the mapping.
 *
 * <p>The closed set of {@link ArrowType} shapes the server can emit is exactly what is
 * handled here: {@code Bool}, {@code Int(8/16/32/64)}, {@code FloatingPoint(SINGLE/DOUBLE)},
 * {@code Utf8}, {@code Binary}, {@code FixedSizeBinary(16)} (uuid/timeuuid),
 * {@code Timestamp(MILLISECOND, "UTC")}, {@code Date(DAY)}, {@code Time(NANOSECOND, 64)},
 * {@code Interval(MONTH_DAY_NANO)} (duration), {@code Decimal(76, scale, 256)}
 * (varint/decimal), {@code FixedSizeList} (vector), {@code List} (list/set), {@code Map},
 * and {@code Struct} (UDT/tuple).
 *
 * <p><b>Lossy/approximate mappings, documented up front (see also trino/README.md):</b>
 * <ul>
 *   <li><b>varint/decimal -&gt; VARCHAR</b>: the server maps both to a fixed-width 256-bit
 *       Arrow {@code Decimal(precision=76, scale=38)} (see {@code CassandraArrowTypeMapping}
 *       javadoc). Trino's {@code DECIMAL} type tops out at 38 total digits, so a 76-digit
 *       decimal cannot be represented as a Trino {@code DECIMAL} without truncation. Rather
     *   than silently drop precision, this connector surfaces the column as {@code VARCHAR}
     *   carrying the value's exact canonical string form (see {@link ArrowPageBuilder}). If a
     *   future Cassandra-side change narrows the fixed precision to fit within 38 digits, this
     *   mapping should switch to a real {@code DECIMAL} (kept as one seam: {@link #decimalType}).
     *   Values within (-1, 1) at scale &lt;= 38 <em>would</em> fit; this mapping always falls back
     *   to VARCHAR at the server's actual precision (76) since that is what is on the wire today.</li>
 *   <li><b>duration (Arrow {@code Interval(MONTH_DAY_NANO)}) -&gt; VARCHAR</b>: Trino has
 *       {@code INTERVAL YEAR TO MONTH} and {@code INTERVAL DAY TO SECOND}, but neither can
 *       represent a value with independent months, days, <em>and</em> nanoseconds components
 *       (Cassandra's {@code duration} type, per CQL, genuinely has all three - e.g. {@code 1mo2d3h}
 *       is not reducible to either Trino interval family without losing a component). This
 *       connector surfaces {@code duration} columns as {@code VARCHAR} in an ISO-8601-like
 *       {@code P<months>M<days>DT<nanos>N} textual form - informational display only, not
 *       usable for interval arithmetic in Trino.</li>
 *   <li><b>uuid/timeuuid -&gt; UUID</b>: both map to Arrow {@code FixedSizeBinary(16)} with no
 *       distinguishing metadata (the server does not tag it as a UUID extension type), but
 *       since {@code CassandraArrowTypeMapping} only ever uses {@code FixedSizeBinary(16)} for
 *       {@code uuid}/{@code timeuuid} columns, treating every such field as Trino's native
 *       {@link UuidType} is safe and lossless (both use the same big-endian 16-byte RFC 4122
 *       layout).</li>
 *   <li><b>timestamp -&gt; TIMESTAMP (no time zone)</b>: the server always emits
 *       {@code Timestamp(MILLISECOND, "UTC")} (Cassandra's {@code timestamp} is a UTC instant
 *       with no zone of its own). This connector reports plain {@code TIMESTAMP(3)}
     *   (interpreting the instant's UTC wall-clock fields), matching traditional CQL driver
     *   behavior, rather than {@code TIMESTAMP WITH TIME ZONE}; either is defensible; document
     *   if you change it.</li>
 * </ul>
 */
public final class ArrowTypeMapping
{
    private ArrowTypeMapping()
    {
    }

    /** Arrow field metadata key carrying the Cassandra column kind; see the server's mapping class. */
    public static final String KIND_METADATA_KEY = "cassandra.kind";
    /** Arrow field metadata key carrying a key column's 0-based position within its key. */
    public static final String POSITION_METADATA_KEY = "cassandra.position";

    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    /** Thrown when an Arrow field's type has no handling in this mapping. */
    public static class UnsupportedArrowTypeException extends RuntimeException
    {
        public UnsupportedArrowTypeException(String message)
        {
            super(message);
        }
    }

    /** Maps one Arrow {@link Field} (column or nested child) to its Trino {@link Type}. */
    public static Type toTrinoType(Field field)
    {
        ArrowType type = field.getType();

        if (type instanceof ArrowType.Bool)
            return BooleanType.BOOLEAN;

        if (type instanceof ArrowType.Int intType)
            return integerType(intType);

        if (type instanceof ArrowType.FloatingPoint fp)
            return switch (fp.getPrecision())
            {
                case SINGLE -> RealType.REAL;
                case DOUBLE -> DoubleType.DOUBLE;
                case HALF -> throw new UnsupportedArrowTypeException(
                    "half-precision float has no Cassandra source type and is not expected on the wire");
            };

        if (type instanceof ArrowType.Utf8)
            return VarcharType.VARCHAR;

        if (type instanceof ArrowType.Binary)
            return VarbinaryType.VARBINARY;

        if (type instanceof ArrowType.FixedSizeBinary fixedBinary)
            return fixedBinary.getByteWidth() == 16
                   ? UuidType.UUID
                   : VarbinaryType.VARBINARY;

        if (type instanceof ArrowType.Timestamp)
            // The server always emits millisecond precision; see class javadoc.
            return TimestampType.TIMESTAMP_MILLIS;

        if (type instanceof ArrowType.Date)
            return DateType.DATE;

        if (type instanceof ArrowType.Time)
            // The server always emits Time(NANOSECOND, 64); see class javadoc.
            return TimeType.TIME_NANOS;

        if (type instanceof ArrowType.Interval)
            // duration: no clean Trino interval covers months+days+nanos together; see class javadoc.
            return VarcharType.VARCHAR;

        if (type instanceof ArrowType.Decimal decimal)
            return decimalType(decimal);

        if (type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.List)
            return new ArrayType(toTrinoType(onlyChild(field)));

        if (type instanceof ArrowType.Map)
            return mapType(field);

        if (type instanceof ArrowType.Struct)
            return rowType(field);

        throw new UnsupportedArrowTypeException(
            "No Trino type mapping for Arrow type: " + type + " (field '" + field.getName() + "')");
    }

    private static Type integerType(ArrowType.Int intType)
    {
        return switch (intType.getBitWidth())
        {
            case 8 -> TinyintType.TINYINT;
            case 16 -> SmallintType.SMALLINT;
            case 32 -> IntegerType.INTEGER;
            case 64 -> BigintType.BIGINT;
            default -> throw new UnsupportedArrowTypeException(
                "Unsupported Arrow integer width: " + intType.getBitWidth());
        };
    }

    /**
     * varint/decimal -&gt; VARCHAR. See class javadoc: the server's fixed 76-digit precision
     * does not fit Trino's 38-digit DECIMAL ceiling, so this always falls back to VARCHAR at
     * present. The seam is kept as its own method so a future narrower server-side precision
     * can switch to {@code DecimalType.createDecimalType(precision, scale)} without touching
     * {@link #toTrinoType}'s dispatch.
     */
    private static Type decimalType(ArrowType.Decimal decimal)
    {
        if (decimal.getPrecision() <= 38)
            return io.trino.spi.type.DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
        return VarcharType.VARCHAR;
    }

    private static Type mapType(Field field)
    {
        // Arrow Map's single child is a non-nullable Struct("entries") with two children: key, value.
        Field entries = onlyChild(field);
        List<Field> keyValue = entries.getChildren();
        if (keyValue.size() != 2)
            throw new UnsupportedArrowTypeException(
                "Arrow Map field '" + field.getName() + "' has a malformed entries struct: " + keyValue);
        Type keyType = toTrinoType(keyValue.get(0));
        Type valueType = toTrinoType(keyValue.get(1));
        return new MapType(keyType, valueType, TYPE_OPERATORS);
    }

    private static Type rowType(Field field)
    {
        List<RowType.Field> rowFields = new ArrayList<>(field.getChildren().size());
        for (Field child : field.getChildren())
            rowFields.add(RowType.field(child.getName(), toTrinoType(child)));
        return RowType.from(rowFields);
    }

    private static Field onlyChild(Field field)
    {
        List<Field> children = field.getChildren();
        if (children.size() != 1)
            throw new UnsupportedArrowTypeException(
                "Expected exactly one child field on '" + field.getName() + "', found " + children.size());
        return children.get(0);
    }

    /** The Cassandra column kind carried in a field's metadata, or {@code null} if absent. */
    public static String kindOf(Field field)
    {
        return field.getMetadata().get(KIND_METADATA_KEY);
    }

    /** The key position carried in a field's metadata, or empty if the column is not a key column. */
    public static java.util.OptionalInt positionOf(Field field)
    {
        String value = field.getMetadata().get(POSITION_METADATA_KEY);
        return value == null ? java.util.OptionalInt.empty() : java.util.OptionalInt.of(Integer.parseInt(value));
    }
}
