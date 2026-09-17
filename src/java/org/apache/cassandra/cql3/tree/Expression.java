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
package org.apache.cassandra.cql3.tree;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;

/**
 * Abstract base for expression nodes. All expressions are immutable.
 */
public abstract class Expression implements AstNode
{
    private final SourceSpan span;
    
    /**
     * Parser-built domain object carried for lowering.
     * POC optimization: AstLowering returns this for unmodified subtrees,
     * avoiding factory reconstruction. Null when not available.
     */
    @Nullable
    private final Object carriedRaw;

    protected Expression(SourceSpan span)
    {
        this(span, null);
    }
    
    protected Expression(SourceSpan span, @Nullable Object carriedRaw)
    {
        this.span = span;
        this.carriedRaw = carriedRaw;
    }
    
    @Nullable
    public Object getCarriedRaw()
    {
        return carriedRaw;
    }

    @Override
    public SourceSpan getSourceSpan()
    {
        return span;
    }

    public abstract <R> R accept(ExpressionVisitor<R> visitor);

    public static final class ColumnRef extends Expression
    {
        public final QualifiedName name;

        public ColumnRef(SourceSpan span, QualifiedName name)
        {
            super(span);
            this.name = name;
        }
        
        public ColumnRef(SourceSpan span, QualifiedName name, Object carriedRaw)
        {
            super(span, carriedRaw);
            this.name = name;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitColumnRef(this);
        }
    }

    public static final class Literal extends Expression
    {
        public final Object value;

        public Literal(SourceSpan span, Object value)
        {
            super(span);
            this.value = value;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitLiteral(this);
        }
    }

    public static final class NullLiteral extends Expression
    {
        public NullLiteral(SourceSpan span)
        {
            super(span);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitNullLiteral(this);
        }
    }

    public static final class BindMarker extends Expression
    {
        public final int ordinal;
        @Nullable
        public final ColumnIdentifier name; // null for positional ?

        public BindMarker(SourceSpan span, int ordinal, @Nullable ColumnIdentifier name)
        {
            super(span);
            this.ordinal = ordinal;
            this.name = name;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitBindMarker(this);
        }
    }

    public static final class FunctionCall extends Expression
    {
        public final QualifiedName functionName;
        public final List<Expression> arguments;
        public final boolean isCountStar;

        public FunctionCall(SourceSpan span, QualifiedName functionName, List<Expression> arguments, boolean isCountStar)
        {
            super(span);
            this.functionName = functionName;
            this.arguments = ImmutableList.copyOf(arguments);
            this.isCountStar = isCountStar;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitFunctionCall(this);
        }
    }

    public static final class Arithmetic extends Expression
    {
        public enum Operator { ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO }

        public final Operator operator;
        public final Expression left;
        public final Expression right;

        public Arithmetic(SourceSpan span, Operator operator, Expression left, Expression right)
        {
            super(span);
            this.operator = operator;
            this.left = left;
            this.right = right;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitArithmetic(this);
        }
    }

    public static final class Negation extends Expression
    {
        public final Expression operand;

        public Negation(SourceSpan span, Expression operand)
        {
            super(span);
            this.operand = operand;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitNegation(this);
        }
    }

    public static final class FieldAccess extends Expression
    {
        public final Expression base;
        public final ColumnIdentifier field;

        public FieldAccess(SourceSpan span, Expression base, ColumnIdentifier field)
        {
            super(span);
            this.base = base;
            this.field = field;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitFieldAccess(this);
        }
    }

    public static final class ElementAccess extends Expression
    {
        public final Expression base;
        public final Expression index;

        public ElementAccess(SourceSpan span, Expression base, Expression index)
        {
            super(span);
            this.base = base;
            this.index = index;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitElementAccess(this);
        }
    }

    public static final class SliceAccess extends Expression
    {
        public final Expression base;
        @Nullable
        public final Expression start;
        @Nullable
        public final Expression end;

        public SliceAccess(SourceSpan span, Expression base, @Nullable Expression start, @Nullable Expression end)
        {
            super(span);
            this.base = base;
            this.start = start;
            this.end = end;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitSliceAccess(this);
        }
    }

    public static final class Cast extends Expression
    {
        public final Expression operand;
        public final TypeNode targetType;

        public Cast(SourceSpan span, Expression operand, TypeNode targetType)
        {
            super(span);
            this.operand = operand;
            this.targetType = targetType;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitCast(this);
        }
    }

    public static final class TypeHint extends Expression
    {
        public final TypeNode type;
        public final Expression operand;

        public TypeHint(SourceSpan span, TypeNode type, Expression operand)
        {
            super(span);
            this.type = type;
            this.operand = operand;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitTypeHint(this);
        }
    }

    public static final class ListExpr extends Expression
    {
        public final List<Expression> elements;

        public ListExpr(SourceSpan span, List<Expression> elements)
        {
            super(span);
            this.elements = ImmutableList.copyOf(elements);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitListExpr(this);
        }
    }

    public static final class SetExpr extends Expression
    {
        public final List<Expression> elements;

        public SetExpr(SourceSpan span, List<Expression> elements)
        {
            super(span);
            this.elements = ImmutableList.copyOf(elements);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitSetExpr(this);
        }
    }

    public static final class MapExpr extends Expression
    {
        public final Map<Expression, Expression> entries;

        public MapExpr(SourceSpan span, Map<Expression, Expression> entries)
        {
            super(span);
            this.entries = ImmutableMap.copyOf(entries);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitMapExpr(this);
        }
    }

    public static final class TupleExpr extends Expression
    {
        public final List<Expression> elements;

        public TupleExpr(SourceSpan span, List<Expression> elements)
        {
            super(span);
            this.elements = ImmutableList.copyOf(elements);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitTupleExpr(this);
        }
    }

    public static final class UdtExpr extends Expression
    {
        public final Map<ColumnIdentifier, Expression> fields;

        public UdtExpr(SourceSpan span, Map<ColumnIdentifier, Expression> fields)
        {
            super(span);
            this.fields = ImmutableMap.copyOf(fields);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitUdtExpr(this);
        }
    }

    // Predicate nodes for WHERE clause
    public static final class Comparison extends Expression
    {
        public enum Operator { EQ, NEQ, LT, LTE, GT, GTE }

        public final Expression left;
        public final Operator operator;
        public final Expression right;

        public Comparison(SourceSpan span, Expression left, Operator operator, Expression right)
        {
            super(span);
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitComparison(this);
        }
    }

    public static final class InExpr extends Expression
    {
        public final Expression left;
        public final List<Expression> values;

        public InExpr(SourceSpan span, Expression left, List<Expression> values)
        {
            super(span);
            this.left = left;
            this.values = ImmutableList.copyOf(values);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitInExpr(this);
        }
    }

    public static final class ContainsExpr extends Expression
    {
        public final Expression collection;
        public final Expression element;
        public final boolean isKey;

        public ContainsExpr(SourceSpan span, Expression collection, Expression element, boolean isKey)
        {
            super(span);
            this.collection = collection;
            this.element = element;
            this.isKey = isKey;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitContainsExpr(this);
        }
    }

    public static final class TokenExpr extends Expression
    {
        public final List<Expression> partitionKeys;

        public TokenExpr(SourceSpan span, List<Expression> partitionKeys)
        {
            super(span);
            this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitTokenExpr(this);
        }
    }

    public static final class CustomIndexExpr extends Expression
    {
        public final String indexName;
        public final Expression value;

        public CustomIndexExpr(SourceSpan span, String indexName, Expression value)
        {
            super(span);
            this.indexName = indexName;
            this.value = value;
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            return visitor.visitCustomIndexExpr(this);
        }
    }

    // Extension points for future phases
    public static abstract class SubqueryExpr extends Expression
    {
        protected SubqueryExpr(SourceSpan span)
        {
            super(span);
            throw new UnsupportedOperationException("Subqueries not yet implemented");
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            throw new UnsupportedOperationException("Subqueries not yet implemented");
        }
    }

    public static abstract class WindowExpr extends Expression
    {
        protected WindowExpr(SourceSpan span)
        {
            super(span);
            throw new UnsupportedOperationException("Window functions not yet implemented");
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor)
        {
            throw new UnsupportedOperationException("Window functions not yet implemented");
        }
    }
}
