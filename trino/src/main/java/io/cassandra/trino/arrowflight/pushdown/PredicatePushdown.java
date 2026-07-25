package io.cassandra.trino.arrowflight.pushdown;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import io.cassandra.trino.arrowflight.ArrowFlightColumnHandle;
import io.cassandra.trino.arrowflight.ticket.FilterExpression;

/**
 * Translates Trino's {@link TupleDomain} predicate representation (the {@code
 * Constraint#getSummary()} passed to {@code ConnectorMetadata#applyFilter}) into the Flight
 * ticket {@code filter} tree (see {@code ARROW-FLIGHT.md} &sect;7 and {@link FilterExpression}).
 *
 * <p>Per-column, per-{@link Domain} translation is all-or-nothing: a column either translates
 * completely (single value &rarr; {@code cmp EQ}, discrete set &rarr; {@code in}, range(s)
 * &rarr; {@code cmp}/{@code and}/{@code or}, {@code IS [NOT] NULL}) or not at all, in which case
 * it is left in {@link Result#remaining()} for Trino to apply itself - never partially/
 * incorrectly translated. This only handles {@code Constraint#getSummary()}; the separate
 * {@code Constraint#getExpression()} (arbitrary {@code ConnectorExpression}s - casts, functions,
 * multi-column comparisons, etc.) is not translated here and is always left for Trino, matching
 * most JDBC-style Trino connectors' scope for a v1 (see {@code ArrowFlightMetadata#applyFilter}).
 */
public final class PredicatePushdown
{
    private PredicatePushdown()
    {
    }

    /** @param pushedDown the filter tree to send to the server, if anything was pushable */
    public record Result(Optional<FilterExpression> pushedDown, TupleDomain<ColumnHandle> remaining)
    {
    }

    public static Result translate(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll() || tupleDomain.isNone())
            return new Result(Optional.empty(), tupleDomain);

        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().orElseThrow();
        List<FilterExpression> pushed = new ArrayList<>();
        Map<ColumnHandle, Domain> remaining = new LinkedHashMap<>();

        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet())
        {
            ArrowFlightColumnHandle column = (ArrowFlightColumnHandle) entry.getKey();
            Optional<FilterExpression> translated = translateDomain(column.name(), entry.getValue());
            if (translated.isPresent())
                pushed.add(translated.get());
            else
                remaining.put(entry.getKey(), entry.getValue());
        }

        Optional<FilterExpression> filter = toFilter(pushed);
        TupleDomain<ColumnHandle> remainingDomain = remaining.isEmpty() ? TupleDomain.all() : TupleDomain.withColumnDomains(remaining);
        return new Result(filter, remainingDomain);
    }

    private static Optional<FilterExpression> translateDomain(String column, Domain domain)
    {
        if (domain.isOnlyNull())
            return Optional.of(new FilterExpression.IsNull(column));

        boolean nullAllowed = domain.isNullAllowed();
        ValueSet values = domain.getValues();

        if (values.isAll())
        {
            // isAll() && !nullAllowed => Domain.notNull(); isAll() && nullAllowed => Domain.isAll()
            // (fully unconstrained - TupleDomain normally omits such columns from the map
            // entirely, but decline to push rather than emit a vacuous filter if it ever occurs).
            return nullAllowed ? Optional.empty() : Optional.of(new FilterExpression.IsNotNull(column));
        }
        if (values.isNone())
        {
            // Domain.none() without nullAllowed would make the whole TupleDomain none(), which
            // applyFilter is never called with; be defensive rather than assume it can't happen.
            return Optional.empty();
        }

        Optional<FilterExpression> valuesExpr = translateValueSet(column, domain.getType(), values);
        if (valuesExpr.isEmpty())
            return Optional.empty();

        return nullAllowed
               ? Optional.of(new FilterExpression.Or(List.of(valuesExpr.get(), new FilterExpression.IsNull(column))))
               : valuesExpr;
    }

    private static Optional<FilterExpression> translateValueSet(String column, Type type, ValueSet values)
    {
        if (values.isDiscreteSet())
        {
            List<Object> discrete = values.getDiscreteSet();
            List<Object> encoded = new ArrayList<>(discrete.size());
            for (Object value : discrete)
            {
                Optional<Object> e = FilterValueEncoder.encode(type, value);
                if (e.isEmpty())
                    return Optional.empty();
                encoded.add(e.get());
            }
            if (encoded.size() == 1)
                return Optional.of(new FilterExpression.Cmp(column, FilterExpression.Op.EQ, encoded.get(0)));
            return Optional.of(new FilterExpression.In(column, encoded));
        }

        Ranges ranges;
        try
        {
            ranges = values.getRanges();
        }
        catch (UnsupportedOperationException e)
        {
            // Non-orderable type with a non-discrete ValueSet shape this translator doesn't
            // understand - decline rather than guess.
            return Optional.empty();
        }

        List<FilterExpression> rangeExpressions = new ArrayList<>();
        for (Range range : ranges.getOrderedRanges())
        {
            Optional<FilterExpression> rangeExpression = translateRange(column, type, range);
            if (rangeExpression.isEmpty())
                return Optional.empty();
            rangeExpressions.add(rangeExpression.get());
        }
        if (rangeExpressions.isEmpty())
            return Optional.empty();
        return Optional.of(rangeExpressions.size() == 1 ? rangeExpressions.get(0) : new FilterExpression.Or(rangeExpressions));
    }

    private static Optional<FilterExpression> translateRange(String column, Type type, Range range)
    {
        if (range.isSingleValue())
        {
            return FilterValueEncoder.encode(type, range.getSingleValue())
                                      .map(value -> new FilterExpression.Cmp(column, FilterExpression.Op.EQ, value));
        }

        List<FilterExpression> bounds = new ArrayList<>(2);
        if (!range.isLowUnbounded())
        {
            Optional<Object> value = FilterValueEncoder.encode(type, range.getLowBoundedValue());
            if (value.isEmpty())
                return Optional.empty();
            bounds.add(new FilterExpression.Cmp(column, range.isLowInclusive() ? FilterExpression.Op.GE : FilterExpression.Op.GT, value.get()));
        }
        if (!range.isHighUnbounded())
        {
            Optional<Object> value = FilterValueEncoder.encode(type, range.getHighBoundedValue());
            if (value.isEmpty())
                return Optional.empty();
            bounds.add(new FilterExpression.Cmp(column, range.isHighInclusive() ? FilterExpression.Op.LE : FilterExpression.Op.LT, value.get()));
        }
        if (bounds.isEmpty())
            // A fully-unbounded Range should only ever appear as ValueSet.all(), handled earlier.
            return Optional.empty();
        return Optional.of(bounds.size() == 1 ? bounds.get(0) : new FilterExpression.And(bounds));
    }

    private static Optional<FilterExpression> toFilter(List<FilterExpression> expressions)
    {
        if (expressions.isEmpty())
            return Optional.empty();
        return Optional.of(expressions.size() == 1 ? expressions.get(0) : new FilterExpression.And(expressions));
    }
}
