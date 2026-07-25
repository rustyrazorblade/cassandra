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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.arrow.flight.CallStatus;

import org.apache.cassandra.utils.JsonUtils;

/**
 * The Arrow Flight wire ticket / command payload - see {@code ARROW-FLIGHT.md} for the full JSON
 * shape. {@code keyspace}/{@code table} are required; {@code tokenRange}/{@code filter}/
 * {@code aggregation} are all optional and independently omittable. Omitting all three reproduces
 * the original PoC's exact behavior (full unfiltered scan, raw rows).
 * <p>
 * {@link #filter} is kept as a raw {@link JsonNode} tree (not resolved against a table) since this
 * class is shared by both {@code getFlightInfo} (which never needs to resolve/evaluate the filter -
 * a filter never changes output schema) and {@code getStream} (which does, via
 * {@link FilterCompiler}). {@link #aggregation}, by contrast, DOES change output schema, so it is
 * parsed into a structured (but still table-independent) {@link AggregationSpec} eagerly.
 */
public final class FlightTicket
{
    public final String keyspace;
    public final String table;
    public final TokenRangeSpec tokenRange; // nullable
    public final JsonNode filter; // nullable; see FilterCompiler
    public final AggregationSpec aggregation; // nullable

    public FlightTicket(String keyspace, String table, TokenRangeSpec tokenRange, JsonNode filter, AggregationSpec aggregation)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tokenRange = tokenRange;
        this.filter = filter;
        this.aggregation = aggregation;
    }

    /** (start, end] token bounds as the decimal strings carried on the wire - see {@code ARROW-FLIGHT.md}. */
    public static final class TokenRangeSpec
    {
        public final String start;
        public final String end;

        public TokenRangeSpec(String start, String end)
        {
            this.start = start;
            this.end = end;
        }
    }

    public static FlightTicket parse(byte[] json)
    {
        JsonNode root;
        try
        {
            root = JsonUtils.JSON_OBJECT_MAPPER.readTree(json);
        }
        catch (IOException e)
        {
            throw CallStatus.INVALID_ARGUMENT.withDescription("malformed ticket JSON: " + e.getMessage()).toRuntimeException();
        }
        if (root == null || !root.isObject())
            throw CallStatus.INVALID_ARGUMENT.withDescription("ticket must be a JSON object").toRuntimeException();

        String keyspace = requireField(root, "keyspace", "keyspace");
        String table = requireField(root, "table", "table");

        TokenRangeSpec tokenRange = null;
        JsonNode tokenRangeNode = root.get("tokenRange");
        if (tokenRangeNode != null && !tokenRangeNode.isNull())
            tokenRange = new TokenRangeSpec(requireField(tokenRangeNode, "start", "tokenRange.start"),
                                             requireField(tokenRangeNode, "end", "tokenRange.end"));

        JsonNode filterNode = root.get("filter");
        if (filterNode != null && filterNode.isNull())
            filterNode = null;

        AggregationSpec aggregation = null;
        JsonNode aggregationNode = root.get("aggregation");
        if (aggregationNode != null && !aggregationNode.isNull())
            aggregation = parseAggregation(aggregationNode);

        return new FlightTicket(keyspace, table, tokenRange, filterNode, aggregation);
    }

    private static AggregationSpec parseAggregation(JsonNode node)
    {
        List<String> groupBy = new ArrayList<>();
        JsonNode groupByNode = node.get("groupBy");
        if (groupByNode != null && !groupByNode.isNull())
        {
            if (!groupByNode.isArray())
                throw CallStatus.INVALID_ARGUMENT.withDescription("aggregation.groupBy must be an array").toRuntimeException();
            for (JsonNode column : groupByNode)
                groupBy.add(requireTextNode(column, "aggregation.groupBy[]"));
        }

        JsonNode aggregatesNode = node.get("aggregates");
        if (aggregatesNode == null || !aggregatesNode.isArray() || aggregatesNode.isEmpty())
            throw CallStatus.INVALID_ARGUMENT.withDescription("aggregation.aggregates must be a non-empty array").toRuntimeException();

        List<AggregationSpec.AggregateSpec> aggregates = new ArrayList<>();
        for (JsonNode aggregateNode : aggregatesNode)
        {
            String functionName = requireField(aggregateNode, "function", "aggregation.aggregates[].function");
            AggregationSpec.AggregateFunction function;
            try
            {
                function = AggregationSpec.AggregateFunction.valueOf(functionName.toUpperCase(Locale.ROOT));
            }
            catch (IllegalArgumentException e)
            {
                throw CallStatus.INVALID_ARGUMENT.withDescription("unknown aggregate function: " + functionName).toRuntimeException();
            }
            JsonNode columnNode = aggregateNode.get("column");
            String column = (columnNode == null || columnNode.isNull()) ? null : columnNode.asText();
            if (column == null && function != AggregationSpec.AggregateFunction.COUNT)
                throw CallStatus.INVALID_ARGUMENT.withDescription(function + " requires a column").toRuntimeException();
            String outputName = requireField(aggregateNode, "outputName", "aggregation.aggregates[].outputName");
            aggregates.add(new AggregationSpec.AggregateSpec(function, column, outputName));
        }

        return new AggregationSpec(groupBy, aggregates);
    }

    private static String requireField(JsonNode parent, String key, String pathForError)
    {
        JsonNode field = parent.get(key);
        return requireTextNode(field, pathForError);
    }

    private static String requireTextNode(JsonNode field, String pathForError)
    {
        if (field == null || field.isNull() || !field.isTextual() || field.asText().isEmpty())
            throw CallStatus.INVALID_ARGUMENT.withDescription("ticket field '" + pathForError + "' must be a non-empty string").toRuntimeException();
        return field.asText();
    }

    /** Canonical minimal ticket - {@code keyspace}/{@code table} only, matching the PoC's original behavior. */
    public static byte[] serializeMinimal(String keyspace, String table)
    {
        ObjectNode node = JsonUtils.JSON_OBJECT_MAPPER.createObjectNode();
        node.put("keyspace", keyspace);
        node.put("table", table);
        try
        {
            return JsonUtils.JSON_OBJECT_MAPPER.writeValueAsBytes(node);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
