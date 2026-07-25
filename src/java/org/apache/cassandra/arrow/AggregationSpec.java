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

import java.util.List;

/**
 * Raw (table-independent) parse of a Flight ticket's {@code aggregation} field - see
 * {@code ARROW-FLIGHT.md}. Resolved against a specific {@link org.apache.cassandra.schema.TableMetadata}
 * by {@link CompiledAggregation#compile}.
 */
public final class AggregationSpec
{
    public final List<String> groupBy;
    public final List<AggregateSpec> aggregates;

    public AggregationSpec(List<String> groupBy, List<AggregateSpec> aggregates)
    {
        this.groupBy = groupBy;
        this.aggregates = aggregates;
    }

    /**
     * {@code AVG} is implemented internally as {@code SUM}/{@code COUNT} (see
     * {@link CompiledAggregation}'s class javadoc for the output-type judgment call this implies).
     */
    public enum AggregateFunction
    {
        COUNT, SUM, MIN, MAX, AVG
    }

    /** {@code column} is {@code null} only for {@code COUNT(*)}; see {@code FlightTicket#parseAggregation}. */
    public static final class AggregateSpec
    {
        public final AggregateFunction function;
        public final String column; // nullable
        public final String outputName;

        public AggregateSpec(AggregateFunction function, String column, String outputName)
        {
            this.function = function;
            this.column = column;
            this.outputName = outputName;
        }
    }
}
