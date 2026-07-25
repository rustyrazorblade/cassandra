package io.cassandra.trino.arrowflight.ticket;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The nested, arbitrary boolean-expression filter tree carried in a Flight ticket's
 * {@code filter} clause (see {@code ARROW-FLIGHT.md} &sect;7 and {@code trino/README.md}):
 * {@code and}/{@code or}/{@code not}/{@code cmp}/{@code isNull}/{@code isNotNull}/{@code in}.
 *
 * <p>Values carried by {@link Cmp}/{@link In} must already be JSON-literal-safe (a {@link String},
 * a boxed numeric primitive, a {@link Boolean}, or {@code null}) - converting a Trino/Cassandra
 * native value into that form is the pushdown translator's job (see {@code PredicatePushdown}),
 * not this class's; this class only knows how to serialize an already-built tree.
 *
 * <p><b>No Jackson polymorphism annotations here</b> - a sealed interface embedded directly as a
 * {@code ConnectorTableHandle} field cannot round-trip through Trino's internal coordinator/worker
 * JSON codec via {@code @JsonTypeInfo}/{@code @JsonSubTypes}: Trino's {@code ObjectMapperProvider}
 * disables {@code MapperFeature.AUTO_DETECT_CREATORS}/{@code AUTO_DETECT_GETTERS}/{@code
 * AUTO_DETECT_FIELDS} (relying only on Java records' built-in structural introspection, not
 * Jackson's annotation-driven mechanisms), which silently drops the type-discriminator property
 * on serialization - confirmed by decompiling {@code io.airlift.json.BaseJacksonProvider} and by a
 * live query failing with "missing type id property" despite the annotations being present in the
 * deployed bytecode. Instead, {@link ArrowFlightTableHandle} stores this tree pre-serialized as a
 * JSON string (via {@link #toJson()} + {@link #fromJson(Map)}), sidestepping Jackson's polymorphic
 * dispatch entirely - see that class's javadoc.
 */
public sealed interface FilterExpression
{
    /** Builds this node's {@code {"and": [...]}} / {@code {"cmp": {...}}} / etc. JSON representation. */
    Map<String, Object> toJson();

    /** Reconstructs a {@link FilterExpression} tree from the {@code Map} shape {@link #toJson()} produces. */
    @SuppressWarnings("unchecked")
    static FilterExpression fromJson(Map<String, Object> json)
    {
        if (json.containsKey("and"))
            return new And(childrenFromJson((List<Object>) json.get("and")));
        if (json.containsKey("or"))
            return new Or(childrenFromJson((List<Object>) json.get("or")));
        if (json.containsKey("not"))
            return new Not(fromJson((Map<String, Object>) json.get("not")));
        if (json.containsKey("cmp"))
        {
            Map<String, Object> cmp = (Map<String, Object>) json.get("cmp");
            return new Cmp((String) cmp.get("column"), Op.valueOf((String) cmp.get("op")), cmp.get("value"));
        }
        if (json.containsKey("isNull"))
            return new IsNull((String) ((Map<String, Object>) json.get("isNull")).get("column"));
        if (json.containsKey("isNotNull"))
            return new IsNotNull((String) ((Map<String, Object>) json.get("isNotNull")).get("column"));
        if (json.containsKey("in"))
        {
            Map<String, Object> in = (Map<String, Object>) json.get("in");
            return new In((String) in.get("column"), (List<Object>) in.get("values"));
        }
        throw new IllegalArgumentException("Unrecognized filter JSON shape: " + json.keySet());
    }

    @SuppressWarnings("unchecked")
    private static List<FilterExpression> childrenFromJson(List<Object> children)
    {
        List<FilterExpression> result = new ArrayList<>(children.size());
        for (Object child : children)
            result.add(fromJson((Map<String, Object>) child));
        return result;
    }

    record And(List<FilterExpression> children) implements FilterExpression
    {
        public And
        {
            if (children.isEmpty())
                throw new IllegalArgumentException("and requires at least one child");
            children = List.copyOf(children);
        }

        @Override
        public Map<String, Object> toJson()
        {
            return Map.of("and", childrenJson(children));
        }
    }

    record Or(List<FilterExpression> children) implements FilterExpression
    {
        public Or
        {
            if (children.isEmpty())
                throw new IllegalArgumentException("or requires at least one child");
            children = List.copyOf(children);
        }

        @Override
        public Map<String, Object> toJson()
        {
            return Map.of("or", childrenJson(children));
        }
    }

    record Not(FilterExpression child) implements FilterExpression
    {
        @Override
        public Map<String, Object> toJson()
        {
            return Map.of("not", child.toJson());
        }
    }

    enum Op
    {
        EQ, NE, LT, LE, GT, GE
    }

    record Cmp(String column, Op op, Object value) implements FilterExpression
    {
        @Override
        public Map<String, Object> toJson()
        {
            Map<String, Object> cmp = new LinkedHashMap<>();
            cmp.put("column", column);
            cmp.put("op", op.name());
            cmp.put("value", value);
            return Map.of("cmp", cmp);
        }
    }

    record IsNull(String column) implements FilterExpression
    {
        @Override
        public Map<String, Object> toJson()
        {
            return Map.of("isNull", Map.of("column", column));
        }
    }

    record IsNotNull(String column) implements FilterExpression
    {
        @Override
        public Map<String, Object> toJson()
        {
            return Map.of("isNotNull", Map.of("column", column));
        }
    }

    record In(String column, List<Object> values) implements FilterExpression
    {
        public In
        {
            if (values.isEmpty())
                throw new IllegalArgumentException("in requires at least one value");
            values = List.copyOf(values);
        }

        @Override
        public Map<String, Object> toJson()
        {
            Map<String, Object> in = new LinkedHashMap<>();
            in.put("column", column);
            in.put("values", values);
            return Map.of("in", in);
        }
    }

    private static List<Object> childrenJson(List<FilterExpression> children)
    {
        List<Object> json = new ArrayList<>(children.size());
        for (FilterExpression child : children)
            json.add(child.toJson());
        return json;
    }
}
