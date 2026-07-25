package io.cassandra.trino.arrowflight.ticket;

import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Builds the JSON ticket bytes the Cassandra Arrow Flight service expects, both for a
 * {@code DoGet} {@code Ticket} and (via {@link org.apache.arrow.flight.FlightDescriptor#command})
 * for a {@code GetFlightInfo} schema-discovery call. See {@code ARROW-FLIGHT.md} and
 * {@code trino/README.md} for the full wire contract:
 *
 * <pre>
 * {
 *   "keyspace": "ks", "table": "tbl",
 *   "tokenRange": {"start": "...", "end": "..."},
 *   "filter": {"and": [...]},
 *   "aggregation": {"groupBy": [...], "aggregates": [...]}
 * }
 * </pre>
 *
 * <p>{@code tokenRange}/{@code filter}/{@code aggregation} are all independently optional; for a
 * {@code GetFlightInfo} call, {@code tokenRange} and {@code filter} are accepted but ignored for
 * schema-resolution purposes - only {@code aggregation} changes the returned schema.
 */
public record ArrowFlightTicket(
    String keyspace,
    String table,
    Optional<TokenRange> tokenRange,
    Optional<FilterExpression> filter,
    Optional<AggregationSpec> aggregation)
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ArrowFlightTicket
    {
        Objects.requireNonNull(keyspace, "keyspace is null");
        Objects.requireNonNull(table, "table is null");
        Objects.requireNonNull(tokenRange, "tokenRange is null");
        Objects.requireNonNull(filter, "filter is null");
        Objects.requireNonNull(aggregation, "aggregation is null");
    }

    /** A ticket for {@code keyspace.table} with no tokenRange/filter/aggregation clauses. */
    public static ArrowFlightTicket of(String keyspace, String table)
    {
        return new ArrowFlightTicket(keyspace, table, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public ArrowFlightTicket withTokenRange(TokenRange range)
    {
        return new ArrowFlightTicket(keyspace, table, Optional.of(range), filter, aggregation);
    }

    public ArrowFlightTicket withFilter(FilterExpression filterExpression)
    {
        return new ArrowFlightTicket(keyspace, table, tokenRange, Optional.of(filterExpression), aggregation);
    }

    public ArrowFlightTicket withAggregation(AggregationSpec aggregationSpec)
    {
        return new ArrowFlightTicket(keyspace, table, tokenRange, filter, Optional.of(aggregationSpec));
    }

    /** The JSON-tree representation of this ticket, for serialization or test assertions. */
    public Map<String, Object> toJsonMap()
    {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("keyspace", keyspace);
        json.put("table", table);
        tokenRange.ifPresent(range -> json.put("tokenRange", range.toJson()));
        filter.ifPresent(filterExpression -> json.put("filter", filterExpression.toJson()));
        aggregation.ifPresent(aggregationSpec -> json.put("aggregation", aggregationSpec.toJson()));
        return json;
    }

    /** UTF-8 JSON bytes, suitable for {@code Ticket}/{@code FlightDescriptor#command}. */
    public byte[] toJsonBytes()
    {
        try
        {
            return MAPPER.writeValueAsBytes(toJsonMap());
        }
        catch (JsonProcessingException e)
        {
            // Every value in the tree is a String/Number/Boolean/List/Map/null by construction
            // (see FilterExpression/AggregationSpec) - Jackson cannot fail to serialize that.
            throw new UncheckedIOException(e);
        }
    }
}
