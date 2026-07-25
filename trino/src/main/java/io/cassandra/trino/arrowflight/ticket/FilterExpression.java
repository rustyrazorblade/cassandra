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
 */
public sealed interface FilterExpression
{
    /** Builds this node's {@code {"and": [...]}} / {@code {"cmp": {...}}} / etc. JSON representation. */
    Map<String, Object> toJson();

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
