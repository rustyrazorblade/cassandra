package io.cassandra.trino.arrowflight.pushdown;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import io.cassandra.trino.arrowflight.ArrowFlightColumnHandle;
import io.cassandra.trino.arrowflight.ticket.FilterExpression;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Translates real Trino {@link TupleDomain}/{@link Domain}/{@link ValueSet} shapes into
 * {@link FilterExpression} trees - no network, no live server.
 */
class PredicatePushdownTest
{
    private static final ArrowFlightColumnHandle AMOUNT = new ArrowFlightColumnHandle("amount", IntegerType.INTEGER);
    private static final ArrowFlightColumnHandle REGION = new ArrowFlightColumnHandle("region", VarcharType.VARCHAR);
    private static final ArrowFlightColumnHandle DELETED_AT = new ArrowFlightColumnHandle("deleted_at", TimestampType.TIMESTAMP_MILLIS);
    private static final ArrowFlightColumnHandle ACTIVE = new ArrowFlightColumnHandle("active", BooleanType.BOOLEAN);

    @Test
    void allTupleDomainPushesNothing()
    {
        PredicatePushdown.Result result = PredicatePushdown.translate(TupleDomain.all());
        assertThat(result.pushedDown()).isEmpty();
        assertThat(result.remaining().isAll()).isTrue();
    }

    @Test
    void singleValueTranslatesToEqualityComparison()
    {
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(AMOUNT, Domain.singleValue(IntegerType.INTEGER, 100L)));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Cmp("amount", FilterExpression.Op.EQ, 100L));
        assertThat(result.remaining().isAll()).isTrue();
    }

    @Test
    void discreteSetTranslatesToIn()
    {
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(AMOUNT, Domain.multipleValues(IntegerType.INTEGER, List.of(1L, 2L, 3L))));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.In("amount", List.of(1L, 2L, 3L)));
    }

    @Test
    void greaterThanTranslatesToOpenLowerBound()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(IntegerType.INTEGER, 100L)), false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(AMOUNT, domain));

        PredicatePushdown.Result result = PredicatePushdown.translate(tupleDomain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100L));
    }

    @Test
    void betweenTranslatesToAndOfTwoComparisons()
    {
        Domain domain = Domain.create(
            ValueSet.ofRanges(Range.range(IntegerType.INTEGER, 10L, true, 20L, false)), false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(AMOUNT, domain));

        PredicatePushdown.Result result = PredicatePushdown.translate(tupleDomain);

        assertThat(result.pushedDown()).contains(new FilterExpression.And(List.of(
            new FilterExpression.Cmp("amount", FilterExpression.Op.GE, 10L),
            new FilterExpression.Cmp("amount", FilterExpression.Op.LT, 20L))));
    }

    @Test
    void multipleDisjointRangesTranslateToOr()
    {
        Domain domain = Domain.create(
            ValueSet.ofRanges(
                Range.lessThan(IntegerType.INTEGER, 0L),
                Range.greaterThan(IntegerType.INTEGER, 100L)),
            false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(AMOUNT, domain));

        PredicatePushdown.Result result = PredicatePushdown.translate(tupleDomain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Or(List.of(
            new FilterExpression.Cmp("amount", FilterExpression.Op.LT, 0L),
            new FilterExpression.Cmp("amount", FilterExpression.Op.GT, 100L))));
    }

    @Test
    void onlyNullTranslatesToIsNull()
    {
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(DELETED_AT, Domain.onlyNull(TimestampType.TIMESTAMP_MILLIS)));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.IsNull("deleted_at"));
    }

    @Test
    void notNullTranslatesToIsNotNull()
    {
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(DELETED_AT, Domain.notNull(TimestampType.TIMESTAMP_MILLIS)));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.IsNotNull("deleted_at"));
    }

    @Test
    void nullableValueSetTranslatesToOrWithIsNull()
    {
        Domain domain = Domain.create(ValueSet.of(IntegerType.INTEGER, 5L), true);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(AMOUNT, domain));

        PredicatePushdown.Result result = PredicatePushdown.translate(tupleDomain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Or(List.of(
            new FilterExpression.Cmp("amount", FilterExpression.Op.EQ, 5L),
            new FilterExpression.IsNull("amount"))));
    }

    @Test
    void booleanSingleValueTranslates()
    {
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(ACTIVE, Domain.singleValue(BooleanType.BOOLEAN, true)));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Cmp("active", FilterExpression.Op.EQ, true));
    }

    @Test
    void varcharValueDecodesToPlainString()
    {
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(REGION, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("us-east"))));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Cmp("region", FilterExpression.Op.EQ, "us-east"));
    }

    @Test
    void uuidValueDecodesToCanonicalString()
    {
        ArrowFlightColumnHandle idColumn = new ArrowFlightColumnHandle("id", UuidType.UUID);
        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        io.airlift.slice.Slice slice = io.trino.spi.type.UuidType.javaUuidToTrinoUuid(uuid);

        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(idColumn, Domain.singleValue(UuidType.UUID, slice)));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Cmp("id", FilterExpression.Op.EQ, uuid.toString()));
    }

    @Test
    void dateValueDecodesToIsoDateString()
    {
        ArrowFlightColumnHandle dateColumn = new ArrowFlightColumnHandle("d", DateType.DATE);
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(dateColumn, Domain.singleValue(DateType.DATE, 19723L))); // 2024-01-01

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Cmp("d", FilterExpression.Op.EQ, "2024-01-01"));
    }

    // decimal is deliberately unsupported (see FilterValueEncoder javadoc) - a convenient stand-in
    // for "some type FilterValueEncoder declines to encode" without needing to hand-build a Block.
    private static final io.trino.spi.type.DecimalType SHORT_DECIMAL = io.trino.spi.type.DecimalType.createDecimalType(10, 2);

    @Test
    void unsupportedTypeIsLeftInRemainingDomain()
    {
        ArrowFlightColumnHandle priceColumn = new ArrowFlightColumnHandle("price", SHORT_DECIMAL);
        Domain valueBearing = Domain.singleValue(SHORT_DECIMAL, 12345L);
        TupleDomain<ColumnHandle> unsupported = TupleDomain.withColumnDomains(Map.of(priceColumn, valueBearing));

        PredicatePushdown.Result result = PredicatePushdown.translate(unsupported);

        assertThat(result.pushedDown()).isEmpty();
        assertThat(result.remaining().getDomains()).hasValueSatisfying(domains -> assertThat(domains).containsKey(priceColumn));
    }

    @Test
    void notNullOnAnUnsupportedTypeStillPushesDownSinceItNeedsNoValueEncoding()
    {
        ArrowFlightColumnHandle priceColumn = new ArrowFlightColumnHandle("price", SHORT_DECIMAL);
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(priceColumn, Domain.notNull(SHORT_DECIMAL)));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        // IS NOT NULL doesn't need to encode a value, so it's still expressible even for a type
        // FilterValueEncoder otherwise declines to handle.
        assertThat(result.pushedDown()).contains(new FilterExpression.IsNotNull("price"));
    }

    @Test
    void mixOfSupportedAndUnsupportedColumnsSplitsBetweenPushedAndRemaining()
    {
        ArrowFlightColumnHandle priceColumn = new ArrowFlightColumnHandle("price", SHORT_DECIMAL);

        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
            AMOUNT, Domain.singleValue(IntegerType.INTEGER, 42L),
            priceColumn, Domain.singleValue(SHORT_DECIMAL, 12345L)));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Cmp("amount", FilterExpression.Op.EQ, 42L));
        assertThat(result.remaining().getDomains()).hasValueSatisfying(domains -> assertThat(domains).containsOnlyKeys(priceColumn));
    }

    @Test
    void varbinaryValueDecodesToBase64()
    {
        ArrowFlightColumnHandle blob = new ArrowFlightColumnHandle("blob", VarbinaryType.VARBINARY);
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(blob, Domain.singleValue(VarbinaryType.VARBINARY, Slices.wrappedBuffer(new byte[] {1, 2, 3}))));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(
            new FilterExpression.Cmp("blob", FilterExpression.Op.EQ, java.util.Base64.getEncoder().encodeToString(new byte[] {1, 2, 3})));
    }

    @Test
    void bigintValuesPassThroughUnchanged()
    {
        ArrowFlightColumnHandle count = new ArrowFlightColumnHandle("count", BigintType.BIGINT);
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(
            Map.of(count, Domain.singleValue(BigintType.BIGINT, 9_000_000_000L)));

        PredicatePushdown.Result result = PredicatePushdown.translate(domain);

        assertThat(result.pushedDown()).contains(new FilterExpression.Cmp("count", FilterExpression.Op.EQ, 9_000_000_000L));
    }
}
