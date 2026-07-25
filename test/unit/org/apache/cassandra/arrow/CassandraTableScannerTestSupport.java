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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Shared Arrow-vector-to-plain-Java-value helpers for arrow package tests (originally written for
 * {@link CassandraTableScannerTest}, reused by the filter/aggregation/token-range tests added
 * alongside them) - converts Arrow's {@code Text} wrapper to {@link String} and Arrow
 * {@code MapVector}'s "list of {key, value} structs" representation to a plain {@link Map}, for
 * easier assertion.
 */
final class CassandraTableScannerTestSupport
{
    private CassandraTableScannerTestSupport()
    {
    }

    static Object valueOf(VectorSchemaRoot root, String column, int index)
    {
        org.apache.arrow.vector.ValueVector vector = root.getVector(column);
        if (vector == null || vector.isNull(index))
            return null;
        return normalize(vector.getObject(index));
    }

    static Object normalize(Object value)
    {
        if (value instanceof org.apache.arrow.vector.util.Text)
            return value.toString();
        if (value instanceof List)
        {
            List<?> list = (List<?>) value;
            if (!list.isEmpty() && list.stream().allMatch(CassandraTableScannerTestSupport::isMapEntry))
            {
                Map<Object, Object> result = new LinkedHashMap<>();
                for (Object element : list)
                {
                    Map<?, ?> entry = (Map<?, ?>) element;
                    result.put(normalize(entry.get("key")), normalize(entry.get("value")));
                }
                return result;
            }
            List<Object> result = new ArrayList<>();
            for (Object element : list)
                result.add(normalize(element));
            return result;
        }
        return value;
    }

    private static boolean isMapEntry(Object o)
    {
        return o instanceof Map && ((Map<?, ?>) o).size() == 2
               && ((Map<?, ?>) o).containsKey("key") && ((Map<?, ?>) o).containsKey("value");
    }
}
