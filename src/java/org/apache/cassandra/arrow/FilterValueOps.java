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
import java.util.Arrays;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;

/**
 * Equality/ordering over {@link ArrowRowAssembler}'s normalized (decomposed) value representation -
 * the same objects {@link FilterExpression} and {@link RowAggregator} operate on. Deliberately
 * type-directed (not generic {@link Comparable} dispatch): every decomposed value's concrete Java
 * class is fully determined by its Cassandra type (see {@code ArrowRowAssembler#decompose}), so a
 * left/right pair compared under the same {@link AbstractType} is always the same concrete class.
 * <p>
 * <b>Ordering scope (documented judgment call):</b> relational comparisons ({@code LT}/{@code LE}/
 * {@code GT}/{@code GE}) are supported for every scalar numeric/text/temporal type, plus UUID/
 * TimeUUID (unsigned byte-lexicographic - this does NOT match {@code TimeUUIDType}'s own
 * time-first clustering order; a filter needing chronological ordering on a TimeUUID column should
 * use the {@code timeuuid}'s embedded timestamp via a different predicate shape - out of scope for
 * v1). Collections, tuples/UDTs, vectors and durations have no relational order in CQL either, so
 * {@code LT}/{@code LE}/{@code GT}/{@code GE} on them throws {@link UnsupportedOperationException};
 * equality ({@code EQ}/{@code NE}/{@code IN}/{@code isNull}) is supported for every type.
 */
final class FilterValueOps
{
    private FilterValueOps()
    {
    }

    static boolean valuesEqual(AbstractType<?> type, Object left, Object right)
    {
        if (left == null || right == null)
            return left == right;
        if (left instanceof byte[])
            return Arrays.equals((byte[]) left, (byte[]) right);
        if (left instanceof Object[])
            return Arrays.deepEquals((Object[]) left, (Object[]) right);
        return left.equals(right);
    }

    @SuppressWarnings("unchecked")
    static int compare(AbstractType<?> rawType, Object left, Object right)
    {
        AbstractType<?> type = unwrap(rawType);
        if (type instanceof CollectionType || type instanceof TupleType || type instanceof VectorType || type instanceof DurationType)
            throw new UnsupportedOperationException("relational comparison (<, <=, >, >=) is not supported for type " + type.asCQL3Type());
        if (type instanceof CounterColumnType)
            return Long.compare((Long) left, (Long) right);
        if (type instanceof IntegerType || type instanceof DecimalType)
            return ((BigDecimal) left).compareTo((BigDecimal) right);
        if (type instanceof UTF8Type || type instanceof AsciiType || type instanceof BytesType || type instanceof InetAddressType)
            return Arrays.compareUnsigned((byte[]) left, (byte[]) right);
        if (left instanceof byte[]) // UUID/TimeUUID - see class javadoc
            return Arrays.compareUnsigned((byte[]) left, (byte[]) right);
        if (left instanceof Comparable)
            return ((Comparable<Object>) left).compareTo(right);
        throw new UnsupportedOperationException("relational comparison (<, <=, >, >=) is not supported for type " + type.asCQL3Type());
    }

    private static AbstractType<?> unwrap(AbstractType<?> type)
    {
        return type.isReversed() ? ((ReversedType<?>) type).baseType : type;
    }
}
