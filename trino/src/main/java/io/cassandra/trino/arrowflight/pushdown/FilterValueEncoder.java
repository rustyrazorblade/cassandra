package io.cassandra.trino.arrowflight.pushdown;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

import io.airlift.slice.Slice;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

/**
 * Converts a Trino "native stack representation" value (see {@link io.trino.spi.predicate.Range}
 * / {@link io.trino.spi.expression.Constant} javadoc) into a JSON-literal-safe value for
 * {@link io.cassandra.trino.arrowflight.ticket.FilterExpression.Cmp}/{@code In}.
 *
 * <p>The wire contract (see {@code ARROW-FLIGHT.md}/{@code trino/README.md}) only specifies
 * {@code "value": <JSON literal>} generically - it does not pin down a per-type encoding. This
 * class makes that encoding an explicit, documented design choice (mirroring how a human would
 * write the equivalent CQL literal) rather than leaving it implicit:
 * <ul>
 *   <li>Numeric types (tinyint/smallint/integer/bigint/real/double) &rarr; a JSON number.</li>
 *   <li>{@code boolean} &rarr; a JSON boolean.</li>
 *   <li>{@code varchar} &rarr; a JSON string (UTF-8 decoded).</li>
 *   <li>{@code varbinary} &rarr; a JSON string, Base64-encoded.</li>
 *   <li>{@code uuid} &rarr; a JSON string, canonical {@code UUID.toString()} form.</li>
 *   <li>{@code date} &rarr; a JSON string, ISO-8601 {@code yyyy-MM-dd}.</li>
 *   <li>{@code timestamp(3)} &rarr; a JSON string, ISO-8601 UTC instant (millisecond precision,
 *       matching the server's always-millisecond {@code Timestamp} type - see
 *       {@code ArrowTypeMapping}).</li>
 *   <li>{@code time(9)} &rarr; a JSON string, ISO-8601 local time.</li>
 * </ul>
 * Every other type (decimal/interval/array/map/row, and any type not listed above) is
 * unsupported here and returns {@link Optional#empty()} - the caller ({@code PredicatePushdown})
 * treats that as "this column's constraint cannot be pushed down" and leaves it in the
 * {@code remainingFilter} for Trino to apply itself, per the {@code applyFilter} SPI contract.
 * decimal is deliberately excluded even though a narrow ({@code precision <= 38}) case exists in
 * {@code ArrowTypeMapping}: the current wire contract's server-side numeric type is always the
 * 76-digit wide form (see that class's javadoc), so this connector never actually produces a
 * Trino {@code DECIMAL}-typed column in practice; adding decimal pushdown now would be
 * speculative against a shape that isn't on the wire today.
 */
final class FilterValueEncoder
{
    private FilterValueEncoder()
    {
    }

    static Optional<Object> encode(Type type, Object nativeValue)
    {
        if (nativeValue == null)
            return Optional.of(null);

        if (type instanceof BooleanType)
            return Optional.of(nativeValue);

        if (type instanceof TinyintType || type instanceof SmallintType
            || type instanceof IntegerType || type instanceof BigintType)
            return Optional.of(nativeValue);

        if (type instanceof RealType)
            return Optional.of((double) Float.intBitsToFloat(((Number) nativeValue).intValue()));

        if (type instanceof DoubleType)
            return Optional.of(nativeValue);

        if (type instanceof VarcharType)
            return Optional.of(((Slice) nativeValue).toString(StandardCharsets.UTF_8));

        if (type instanceof VarbinaryType)
            return Optional.of(Base64.getEncoder().encodeToString(((Slice) nativeValue).getBytes()));

        if (type instanceof UuidType)
            return Optional.of(toUuid((Slice) nativeValue).toString());

        if (type instanceof DateType)
            return Optional.of(LocalDate.ofEpochDay(((Number) nativeValue).longValue()).toString());

        if (type instanceof TimestampType timestampType && timestampType.getPrecision() <= 6)
            return Optional.of(epochMicrosToInstant(((Number) nativeValue).longValue()).toString());

        if (type instanceof TimeType timeType && timeType.getPrecision() == 9)
            return Optional.of(LocalTime.ofNanoOfDay(((Number) nativeValue).longValue() / 1000).toString());

        // decimal/interval/array/map/row/anything else: not supported for pushdown, see class javadoc.
        return Optional.empty();
    }

    private static UUID toUuid(Slice slice)
    {
        ByteBuffer buffer = ByteBuffer.wrap(slice.getBytes()).order(ByteOrder.BIG_ENDIAN);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static Instant epochMicrosToInstant(long epochMicros)
    {
        long epochMillis = Math.floorDiv(epochMicros, 1000L);
        return Instant.ofEpochMilli(epochMillis);
    }
}
