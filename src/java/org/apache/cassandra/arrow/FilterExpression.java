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

import org.apache.cassandra.schema.ColumnMetadata;

/**
 * A compiled (column-resolved, literal-coerced) post-merge filter predicate tree - see
 * {@code ARROW-FLIGHT.md} / the Flight ticket's {@code filter} field for the wire (JSON) shape and
 * {@link FilterCompiler} for how a raw ticket filter is turned into one of these. Evaluated once per
 * fully-assembled row (see {@link ArrowRowAssembler}), after static-column replication and
 * partition/row-liveness reconciliation, via {@link #evaluate(RowValues)}.
 */
public abstract class FilterExpression
{
    public abstract boolean evaluate(RowValues row);

    public static final class And extends FilterExpression
    {
        final List<FilterExpression> children;

        And(List<FilterExpression> children)
        {
            this.children = children;
        }

        @Override
        public boolean evaluate(RowValues row)
        {
            for (FilterExpression child : children)
                if (!child.evaluate(row))
                    return false;
            return true;
        }
    }

    public static final class Or extends FilterExpression
    {
        final List<FilterExpression> children;

        Or(List<FilterExpression> children)
        {
            this.children = children;
        }

        @Override
        public boolean evaluate(RowValues row)
        {
            for (FilterExpression child : children)
                if (child.evaluate(row))
                    return true;
            return false;
        }
    }

    public static final class Not extends FilterExpression
    {
        final FilterExpression child;

        Not(FilterExpression child)
        {
            this.child = child;
        }

        @Override
        public boolean evaluate(RowValues row)
        {
            return !child.evaluate(row);
        }
    }

    public enum Op
    {
        EQ, NE, LT, LE, GT, GE
    }

    public static final class Comparison extends FilterExpression
    {
        final ColumnMetadata column;
        final Op op;
        final Object literal; // already coerced to the column's normalized (decomposed) representation

        Comparison(ColumnMetadata column, Op op, Object literal)
        {
            this.column = column;
            this.op = op;
            this.literal = literal;
        }

        @Override
        public boolean evaluate(RowValues row)
        {
            Object value = row.get(column);
            // SQL/CQL three-valued-logic simplification for this PoC: a NULL operand makes any
            // comparison (including != and IS DISTINCT FROM-shaped checks) false rather than
            // UNKNOWN-propagating - matches CQL's own WHERE-clause behavior, where a null column
            // never satisfies any relational predicate. Use isNull/isNotNull to test for null.
            if (value == null || literal == null)
                return false;
            switch (op)
            {
                case EQ:
                    return FilterValueOps.valuesEqual(column.type, value, literal);
                case NE:
                    return !FilterValueOps.valuesEqual(column.type, value, literal);
                case LT:
                    return FilterValueOps.compare(column.type, value, literal) < 0;
                case LE:
                    return FilterValueOps.compare(column.type, value, literal) <= 0;
                case GT:
                    return FilterValueOps.compare(column.type, value, literal) > 0;
                case GE:
                    return FilterValueOps.compare(column.type, value, literal) >= 0;
                default:
                    throw new IllegalStateException("unhandled op " + op);
            }
        }
    }

    public static final class IsNull extends FilterExpression
    {
        final ColumnMetadata column;

        IsNull(ColumnMetadata column)
        {
            this.column = column;
        }

        @Override
        public boolean evaluate(RowValues row)
        {
            return row.get(column) == null;
        }
    }

    public static final class IsNotNull extends FilterExpression
    {
        final ColumnMetadata column;

        IsNotNull(ColumnMetadata column)
        {
            this.column = column;
        }

        @Override
        public boolean evaluate(RowValues row)
        {
            return row.get(column) != null;
        }
    }

    public static final class In extends FilterExpression
    {
        final ColumnMetadata column;
        final List<Object> literals; // already coerced, see Comparison

        In(ColumnMetadata column, List<Object> literals)
        {
            this.column = column;
            this.literals = literals;
        }

        @Override
        public boolean evaluate(RowValues row)
        {
            Object value = row.get(column);
            if (value == null)
                return false;
            for (Object literal : literals)
                if (literal != null && FilterValueOps.valuesEqual(column.type, value, literal))
                    return true;
            return false;
        }
    }
}
