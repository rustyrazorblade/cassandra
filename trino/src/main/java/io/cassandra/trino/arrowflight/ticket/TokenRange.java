package io.cassandra.trino.arrowflight.ticket;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The {@code tokenRange} clause of a Flight ticket/descriptor - a partition-boundary-aligned,
 * open-lower/closed-upper {@code (start, end]} token subrange (standard Cassandra convention),
 * carried as decimal strings on the wire (see {@code ARROW-FLIGHT.md} and {@code trino/README.md}).
 *
 * <p>This is exactly the shape {@code CassandraRing}/{@code TokenPartitioner}
 * (from {@code cassandra-analytics-common}) already produce as {@code Range<BigInteger>} splits -
 * see {@link #of(BigInteger, BigInteger)}.
 */
public record TokenRange(String start, String end)
{
    public static TokenRange of(BigInteger start, BigInteger end)
    {
        return new TokenRange(start.toString(), end.toString());
    }

    Map<String, Object> toJson()
    {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("start", start);
        json.put("end", end);
        return json;
    }
}
