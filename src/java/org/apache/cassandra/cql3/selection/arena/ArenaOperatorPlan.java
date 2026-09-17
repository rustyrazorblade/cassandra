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

import java.util.List;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Static planner for arena aggregation.
 * The arena owns exactly one shape today: an ORDER BY on a non-clustering column in a
 * single-partition query, with the flag on.  Everything else falls back to the existing path.
 * DISTINCT and HAVING are not yet implemented; the prepare path rejects HAVING before it reaches
 * the planner, so the planner never has to reason about it.
 */
public final class ArenaOperatorPlan
{
    private final boolean arenaOwned;
    private final boolean needsSort;
    private final List<Integer> keyColumnIndices;
    private final List<AbstractType<?>> keyColumnTypes;
    private final List<Boolean> keyColumnReversed;

    private ArenaOperatorPlan(boolean arenaOwned,
                              boolean needsSort,
                              List<Integer> keyColumnIndices,
                              List<AbstractType<?>> keyColumnTypes,
                              List<Boolean> keyColumnReversed)
    {
        this.arenaOwned = arenaOwned;
        this.needsSort = needsSort;
        this.keyColumnIndices = keyColumnIndices;
        this.keyColumnTypes = keyColumnTypes;
        this.keyColumnReversed = keyColumnReversed;
    }

    /**
     * Plan whether to use arena aggregation for a prepared SelectStatement.
     * Arena ONLY for an ORDER BY on a non-clustering column in a single-partition query.
     *
     * @param arenaOrdering the resolved non-clustering ORDER BY key, or null if the query has none
     */
    public static ArenaOperatorPlan plan(SelectStatement stmt,
                                         Selection selection,
                                         StatementRestrictions restrictions,
                                         TableMetadata table,
                                         ArenaOrdering arenaOrdering)
    {
        // Check feature flag.  A window query (ROW_NUMBER) always routes to the arena, because the
        // arena is the only path that computes the rank; it engages regardless of the arena
        // aggregation flag.  Prepare has already rejected every window shape the arena cannot own.
        if (!CassandraRelevantProperties.CASSANDRA_CQL_ARENA_AGGREGATION_ENABLED.getBoolean() && !stmt.hasWindow())
        {
            return fallback();
        }

        // Check single partition requirement - must be exact partition key match
        if (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing() || restrictions.keyIsInRelation())
        {
            return fallback();
        }

        // Check for unsupported features
        if (restrictions.isTopK())
        {
            return fallback();
        }

        // ORDER BY on a non-clustering column: the arena buffers the projected rows, sorts them by
        // the resolved key, then applies the LIMIT after the sort.  This shape is prepared upstream;
        // arenaOrdering is non-null only when the flag is on and the query is single-partition.
        if (arenaOrdering != null)
        {
            return new ArenaOperatorPlan(true,
                                         true,  // needsSort
                                         arenaOrdering.keyColumnIndices(),
                                         arenaOrdering.keyColumnTypes(),
                                         arenaOrdering.keyColumnReversed());
        }

        // Plain aggregates, clustering-column ORDER BY, ANN/index ordering, and multi-partition
        // queries all stay on the existing path.
        return fallback();
    }

    private static ArenaOperatorPlan fallback()
    {
        return new ArenaOperatorPlan(false, false, List.of(), List.of(), List.of());
    }

    public boolean isArenaOwned()
    {
        return arenaOwned;
    }

    public boolean needsSort()
    {
        return needsSort;
    }

    public List<Integer> getKeyColumnIndices()
    {
        return keyColumnIndices;
    }

    public List<AbstractType<?>> getKeyColumnTypes()
    {
        return keyColumnTypes;
    }

    public List<Boolean> getKeyColumnReversed()
    {
        return keyColumnReversed;
    }
}
