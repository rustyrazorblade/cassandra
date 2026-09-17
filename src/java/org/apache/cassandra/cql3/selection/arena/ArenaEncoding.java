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

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Maps AbstractType to encoding information for off-heap storage.
 * Single source of truth for which column types are encodable in the arena.
 */
final class ArenaEncoding
{
    /**
     * Determine if a column type can be encoded for off-heap comparison.
     * All standard CQL types except collections, UDTs, and tuples are encodable.
     *
     * @param type the column type to check
     * @return true if the type can be stored and compared in the arena
     */
    static boolean isEncodable(AbstractType<?> type)
    {
        // For research POC, accept all non-collection types.
        // Collections, UDTs, and tuples would need special handling.
        return !type.isCollection() && !type.isUDT() && !type.isTuple();
    }

    private ArenaEncoding()
    {
        // utility class
    }
}
