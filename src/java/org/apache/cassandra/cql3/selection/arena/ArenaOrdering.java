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
package org.apache.cassandra.cql3.selection.arena;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Resolved ORDER BY key for a single-partition sort on a non-clustering column that the arena owns.
 * The planner builds this at prepare time.  It holds the result-set index, the type, and the
 * effective reversal for each ordering column.
 */
public final class ArenaOrdering
{
    private final List<Integer> keyColumnIndices;
    private final List<AbstractType<?>> keyColumnTypes;
    private final List<Boolean> keyColumnReversed;

    private ArenaOrdering(List<Integer> keyColumnIndices,
                          List<AbstractType<?>> keyColumnTypes,
                          List<Boolean> keyColumnReversed)
    {
        this.keyColumnIndices = keyColumnIndices;
        this.keyColumnTypes = keyColumnTypes;
        this.keyColumnReversed = keyColumnReversed;
    }

    /**
     * Returns true if every ordering column can be stored and compared in the arena.
     *
     * @param orderingColumns the ORDER BY columns
     * @return true if all columns are encodable
     */
    public static boolean canEncode(Map<ColumnMetadata, Ordering> orderingColumns)
    {
        for (ColumnMetadata column : orderingColumns.keySet())
        {
            if (!ArenaEncoding.isEncodable(column.type))
                return false;
        }
        return true;
    }

    /**
     * Build the resolved ordering key from the prepared selection.
     * The effective reversal is (direction is DESC) XOR (the type is reversed), so the arena
     * comparator matches Cassandra's per-column order semantics.
     *
     * @param selection the prepared selection
     * @param orderingColumns the ORDER BY columns in order
     * @return the resolved ordering key
     */
    public static ArenaOrdering build(Selection selection, Map<ColumnMetadata, Ordering> orderingColumns)
    {
        List<Integer> indices = new ArrayList<>(orderingColumns.size());
        List<AbstractType<?>> types = new ArrayList<>(orderingColumns.size());
        List<Boolean> reversed = new ArrayList<>(orderingColumns.size());

        for (Map.Entry<ColumnMetadata, Ordering> entry : orderingColumns.entrySet())
        {
            ColumnMetadata column = entry.getKey();
            boolean desc = entry.getValue().direction == Ordering.Direction.DESC;

            indices.add(selection.getOrderingIndex(column));
            types.add(column.type);
            reversed.add(desc ^ column.isReversedType());
        }

        return new ArenaOrdering(indices, types, reversed);
    }

    List<Integer> keyColumnIndices()
    {
        return keyColumnIndices;
    }

    List<AbstractType<?>> keyColumnTypes()
    {
        return keyColumnTypes;
    }

    List<Boolean> keyColumnReversed()
    {
        return keyColumnReversed;
    }
}
