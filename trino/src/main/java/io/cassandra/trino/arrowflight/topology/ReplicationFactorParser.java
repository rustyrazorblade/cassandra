package io.cassandra.trino.arrowflight.topology;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.spark.data.ReplicationFactor;

/**
 * Parses the {@code replication} map out of the {@code CREATE KEYSPACE ...} DDL text returned by
 * cassandra-sidecar's {@code schema(keyspace)} endpoint (see {@code SidecarClient#schema} /
 * {@code SchemaResponse#schema()}) into the {@code Map<String, String>} shape
 * {@link ReplicationFactor}'s constructor expects.
 *
 * <p>There is no structured replication-factor field on the sidecar wire response - only the raw
 * DDL text - so this is a best-effort regex parse of Cassandra's own CQL map-literal rendering,
 * e.g. {@code CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy',
 * 'replication_factor': '3'} AND durable_writes = true;}. Since {@code schema(keyspace)} is
 * itself keyspace-scoped, the first {@code replication = {...}} match in the response is always
 * the keyspace's own (there is exactly one {@code CREATE KEYSPACE} statement in a keyspace-scoped
 * schema dump).
 */
final class ReplicationFactorParser
{
    private ReplicationFactorParser()
    {
    }

    private static final Pattern REPLICATION_MAP = Pattern.compile(
        "replication\\s*=\\s*\\{([^}]*)}", Pattern.CASE_INSENSITIVE);
    private static final Pattern ENTRY = Pattern.compile("'([^']*)'\\s*:\\s*'?([^,'}]*)'?");

    static ReplicationFactor parse(String keyspace, String createKeyspaceCql)
    {
        Matcher mapMatcher = REPLICATION_MAP.matcher(createKeyspaceCql);
        if (!mapMatcher.find())
            throw new IllegalArgumentException(
                "Could not find a replication map in schema DDL for keyspace '" + keyspace + "': " + createKeyspaceCql);

        Map<String, String> options = new LinkedHashMap<>();
        Matcher entryMatcher = ENTRY.matcher(mapMatcher.group(1));
        while (entryMatcher.find())
            options.put(entryMatcher.group(1), entryMatcher.group(2).trim());

        if (!options.containsKey("class"))
            throw new IllegalArgumentException(
                "Replication map for keyspace '" + keyspace + "' has no 'class' entry: " + mapMatcher.group(1));

        return new ReplicationFactor(options);
    }
}
