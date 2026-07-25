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

import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Read-only view over one fully-assembled row's column values, in {@link ArrowRowAssembler}'s
 * normalized (decomposed) representation - the same Java shapes {@code decompose}/
 * {@code writeDecomposedValue} use, e.g. {@code byte[]} for text/blob/uuid, {@code BigDecimal} for
 * varint/decimal, boxed primitives for scalars. Used by {@link FilterExpression#evaluate} and
 * {@link RowAggregator} so both operate on exactly the values that would have been written to the
 * output batch, without re-reading anything back out of an Arrow vector.
 */
public interface RowValues
{
    /** @return the current row's decomposed value for {@code column}, or {@code null} if absent. */
    Object get(ColumnMetadata column);
}
