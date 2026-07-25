package io.cassandra.trino.arrowflight;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for the bug fixed in {@link ArrowFlightMetadata#applyFilter} (see its
 * javadoc): Trino's SPI contract allows - and in practice does - call {@code applyFilter} again
 * on the same table handle with a constraint that adds nothing beyond what is already enforced
 * (observed live with a Trino-rewritten {@code LIKE 'prefix%'} range predicate, though nothing
 * here depends on {@code LIKE} specifically). The connector previously treated every call as
 * incremental and wrapped the new translation around the handle's existing filter unconditionally,
 * growing the filter tree by one nesting level per redundant call until it failed to serialize.
 * {@code config}/{@code flight} are irrelevant to {@code applyFilter} (it never touches either
 * field), so {@code null} is fine here - no network, no live server.
 */
class ArrowFlightMetadataApplyFilterTest
{
    private static final ArrowFlightColumnHandle AMOUNT = new ArrowFlightColumnHandle("amount", BigintType.BIGINT);
    private static final ArrowFlightColumnHandle REGION = new ArrowFlightColumnHandle("region", VarcharType.VARCHAR);

    private final ArrowFlightMetadata metadata = new ArrowFlightMetadata(null, null);

    private static TupleDomain<ColumnHandle> greaterThan(ArrowFlightColumnHandle column, long value)
    {
        return TupleDomain.withColumnDomains(
            Map.of(column, Domain.create(ValueSet.ofRanges(Range.greaterThan(BigintType.BIGINT, value)), false)));
    }

    @Test
    void reapplyingTheSameConstraintReturnsEmpty()
    {
        ConnectorTableHandle bare = ArrowFlightTableHandle.of("ks", "tbl");
        TupleDomain<ColumnHandle> constraint = greaterThan(AMOUNT, 100);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> first =
            metadata.applyFilter(null, bare, new Constraint(constraint));
        assertThat(first).isPresent();
        ConnectorTableHandle afterFirst = first.get().getHandle();

        // Simulates Trino re-invoking applyFilter on a later optimizer pass with the same
        // already-enforced constraint - must be a no-op, not a re-wrap.
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> second =
            metadata.applyFilter(null, afterFirst, new Constraint(constraint));
        assertThat(second).isEmpty();
    }

    @Test
    void manyRedundantReapplicationsNeverGrowTheFilter()
    {
        ConnectorTableHandle handle = ArrowFlightTableHandle.of("ks", "tbl");
        TupleDomain<ColumnHandle> constraint = greaterThan(AMOUNT, 100);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> first = metadata.applyFilter(null, handle, new Constraint(constraint));
        assertThat(first).isPresent();
        handle = first.get().getHandle();
        String filterAfterFirst = ((ArrowFlightTableHandle) handle).filterJson().orElseThrow();

        // A real, unbounded plan could invoke applyFilter arbitrarily many times with the same
        // constraint (this is exactly what happened live) - simulate a generous number of them.
        for (int i = 0; i < 50; i++)
        {
            Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, new Constraint(constraint));
            assertThat(result).isEmpty();
            // handle is unchanged since applyFilter returned empty - filterJson must not have grown.
            assertThat(((ArrowFlightTableHandle) handle).filterJson()).contains(filterAfterFirst);
        }
    }

    @Test
    void genuinelyNewConstraintOnAnotherColumnStillPushesDown()
    {
        ConnectorTableHandle handle = ArrowFlightTableHandle.of("ks", "tbl");
        TupleDomain<ColumnHandle> firstConstraint = greaterThan(AMOUNT, 100);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> first = metadata.applyFilter(null, handle, new Constraint(firstConstraint));
        assertThat(first).isPresent();
        handle = first.get().getHandle();

        TupleDomain<ColumnHandle> combinedConstraint = firstConstraint.intersect(
            TupleDomain.withColumnDomains(Map.of(REGION, Domain.singleValue(VarcharType.VARCHAR, io.airlift.slice.Slices.utf8Slice("us-east")))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> second = metadata.applyFilter(null, handle, new Constraint(combinedConstraint));
        assertThat(second).isPresent();

        String filterJson = ((ArrowFlightTableHandle) second.get().getHandle()).filterJson().orElseThrow();
        assertThat(filterJson).contains("amount").contains("region");
        // A flat translation of the accumulated domain, not the old filter wrapped around itself.
        assertThat(filterJson.split("amount", -1).length - 1).isEqualTo(1);
    }
}
