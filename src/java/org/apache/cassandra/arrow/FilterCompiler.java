/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.arrow;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.arrow.flight.CallStatus;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;

/**
 * Compiles a Flight ticket's raw {@code filter} JSON tree (a tagged union - one key per node: {@code
 * and}/{@code or}/{@code not}/{@code cmp}/{@code isNull}/{@code isNotNull}/{@code in}, see
 * {@code ARROW-FLIGHT.md}) into a {@link FilterExpression}, resolving column names against {@code
 * table} and coercing each JSON literal ({@code string|number|boolean|null}) into the column's
 * normalized (decomposed) representation.
 * <p>
 * <b>Literal coercion (documented judgment call):</b> rather than hand-rolling a second, parallel
 * JSON-to-Cassandra-type coercion, a literal is rendered to text ({@link JsonNode#asText()}) and run
 * through the column's own {@link AbstractType#fromString} (the same CQL-literal parser used
 * elsewhere in this codebase, e.g. by {@code cqlsh}/SSTable loaders) to get a {@link ByteBuffer},
 * which is then normalized via {@link ArrowRowAssembler#decompose} - guaranteeing filter literals
 * land in EXACTLY the same Java representation real row values do, with no second comparator table
 * to keep in sync. Two scalar-only fast paths bypass {@code fromString} instead: {@code timestamp}/
 * {@code date} columns accept a bare JSON number as already-epoch-normalized (millis-since-epoch /
 * {@code ArrowRowAssembler}'s shifted day count) rather than requiring an ISO-8601 string, and
 * {@code counter} columns (whose normalized representation is a plain composed {@code Long} total,
 * never run through {@code decompose}) parse directly as a long.
 */
final class FilterCompiler
{
    private FilterCompiler()
    {
    }

    static FilterExpression compile(JsonNode node, TableMetadata table)
    {
        if (node == null || !node.isObject() || node.size() != 1)
            throw CallStatus.INVALID_ARGUMENT.withDescription("filter node must be a JSON object with exactly one key (and/or/not/cmp/isNull/isNotNull/in)").toRuntimeException();

        Map.Entry<String, JsonNode> entry = node.fields().next();
        String tag = entry.getKey();
        JsonNode body = entry.getValue();
        switch (tag)
        {
            case "and":
                return new FilterExpression.And(compileChildren(body, table));
            case "or":
                return new FilterExpression.Or(compileChildren(body, table));
            case "not":
                return new FilterExpression.Not(compile(body, table));
            case "cmp":
                return compileComparison(body, table);
            case "isNull":
                return new FilterExpression.IsNull(resolveColumn(body, table));
            case "isNotNull":
                return new FilterExpression.IsNotNull(resolveColumn(body, table));
            case "in":
                return compileIn(body, table);
            default:
                throw CallStatus.INVALID_ARGUMENT.withDescription("unknown filter node type: " + tag).toRuntimeException();
        }
    }

    private static List<FilterExpression> compileChildren(JsonNode body, TableMetadata table)
    {
        if (body == null || !body.isArray())
            throw CallStatus.INVALID_ARGUMENT.withDescription("and/or filter node requires an array of child nodes").toRuntimeException();
        List<FilterExpression> children = new ArrayList<>(body.size());
        for (JsonNode child : body)
            children.add(compile(child, table));
        return children;
    }

    private static FilterExpression compileComparison(JsonNode body, TableMetadata table)
    {
        ColumnMetadata column = resolveColumn(body, table);
        String opName = textField(body, "op", "cmp.op");
        FilterExpression.Op op;
        try
        {
            op = FilterExpression.Op.valueOf(opName.toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException e)
        {
            throw CallStatus.INVALID_ARGUMENT.withDescription("unknown comparison op: " + opName).toRuntimeException();
        }
        Object literal = coerceLiteral(column, body.get("value"));
        return new FilterExpression.Comparison(column, op, literal);
    }

    private static FilterExpression compileIn(JsonNode body, TableMetadata table)
    {
        ColumnMetadata column = resolveColumn(body, table);
        JsonNode valuesNode = body.get("values");
        if (valuesNode == null || !valuesNode.isArray())
            throw CallStatus.INVALID_ARGUMENT.withDescription("in.values must be an array").toRuntimeException();
        List<Object> literals = new ArrayList<>(valuesNode.size());
        for (JsonNode value : valuesNode)
            literals.add(coerceLiteral(column, value));
        return new FilterExpression.In(column, literals);
    }

    private static ColumnMetadata resolveColumn(JsonNode body, TableMetadata table)
    {
        String name = textField(body, "column", "filter.column");
        ColumnMetadata column = table.getColumn(ColumnIdentifier.getInterned(name, true));
        if (column == null)
            throw CallStatus.INVALID_ARGUMENT.withDescription("no such column: " + name).toRuntimeException();
        return column;
    }

    private static String textField(JsonNode parent, String key, String pathForError)
    {
        JsonNode field = parent == null ? null : parent.get(key);
        if (field == null || field.isNull() || !field.isTextual() || field.asText().isEmpty())
            throw CallStatus.INVALID_ARGUMENT.withDescription("filter field '" + pathForError + "' must be a non-empty string").toRuntimeException();
        return field.asText();
    }

    private static Object coerceLiteral(ColumnMetadata column, JsonNode node)
    {
        if (node == null || node.isNull())
            return null;

        AbstractType<?> type = unwrap(column.type);

        if (type instanceof CounterColumnType)
            return node.isTextual() ? Long.parseLong(node.asText()) : node.asLong();
        if (type instanceof TimestampType && node.isNumber())
            return node.asLong(); // already-normalized millis-since-epoch; see class javadoc
        if (type instanceof SimpleDateType && node.isNumber())
            return node.asInt(); // already-normalized shifted day count; see class javadoc
        if (type instanceof IntegerType)
            return CassandraArrowTypeMapping.toArrowDecimal(new BigDecimal(node.asText()).toBigIntegerExact());
        if (type instanceof DecimalType)
            return CassandraArrowTypeMapping.toArrowDecimal(new BigDecimal(node.asText()));

        ByteBuffer buffer;
        try
        {
            buffer = type.fromString(node.asText());
        }
        catch (MarshalException e)
        {
            throw CallStatus.INVALID_ARGUMENT.withDescription("cannot parse filter literal '" + node.asText() +
                                                                "' as " + type.asCQL3Type() + ": " + e.getMessage()).toRuntimeException();
        }
        return ArrowRowAssembler.decompose(type, buffer);
    }

    private static AbstractType<?> unwrap(AbstractType<?> type)
    {
        return type.isReversed() ? ((ReversedType<?>) type).baseType : type;
    }
}
