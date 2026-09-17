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
import org.apache.cassandra.cql3.statements.SelectStatement;

/**
 * AST node for a SELECT statement. Immutable.
 *
 * POC optimization: carries the parser-built SelectStatement.RawStatement for lowering.
 * AstLowering returns this for unmodified trees, avoiding factory reconstruction.
 * Full bidirectional AST<->domain construction deferred to CEP.
 */
public final class SelectAst extends Statement
{
    public final boolean isDistinct;
    public final boolean isJson;
    public final List<SelectItemAst> selectItems;
    public final FromRelation from;
    @Nullable
    public final WhereAst where;
    @Nullable
    public final GroupByAst groupBy;
    @Nullable
    public final OrderByAst orderBy;
    @Nullable
    public final Expression perPartitionLimit;
    @Nullable
    public final Expression limit;
    public final boolean allowFiltering;
    public final Map<String, String> options;

    /**
     * Parser-built RawStatement carried for POC lowering.
     * Null only if not available from parser context.
     */
    @Nullable
    public final SelectStatement.RawStatement carriedRawStatement;

    public SelectAst(SourceSpan span,
                     boolean isDistinct,
                     boolean isJson,
                     List<SelectItemAst> selectItems,
                     FromRelation from,
                     @Nullable WhereAst where,
                     @Nullable GroupByAst groupBy,
                     @Nullable OrderByAst orderBy,
                     @Nullable Expression perPartitionLimit,
                     @Nullable Expression limit,
                     boolean allowFiltering,
                     Map<String, String> options,
                     @Nullable SelectStatement.RawStatement carriedRawStatement)
    {
        super(span);
        this.isDistinct = isDistinct;
        this.isJson = isJson;
        this.selectItems = ImmutableList.copyOf(selectItems);
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.orderBy = orderBy;
        this.perPartitionLimit = perPartitionLimit;
        this.limit = limit;
        this.allowFiltering = allowFiltering;
        this.options = ImmutableMap.copyOf(options);
        this.carriedRawStatement = carriedRawStatement;
    }

    @Override
    public <R> R accept(AstVisitor<R> visitor)
    {
        return visitor.visitSelectAst(this);
    }

    public static final class SelectItemAst implements AstNode
    {
        public final Expression expression;
        @Nullable
        public final ColumnIdentifier alias;

        public SelectItemAst(Expression expression, @Nullable ColumnIdentifier alias)
        {
            this.expression = expression;
            this.alias = alias;
        }

        @Override
        public SourceSpan getSourceSpan()
        {
            return expression.getSourceSpan();
        }
    }

    public static final class WhereAst implements AstNode
    {
        public final List<Expression> predicates;

        public WhereAst(List<Expression> predicates)
        {
            this.predicates = ImmutableList.copyOf(predicates);
        }

        @Override
        public SourceSpan getSourceSpan()
        {
            return predicates.isEmpty() ? null : predicates.get(0).getSourceSpan();
        }
    }

    public static final class GroupByAst implements AstNode
    {
        public final List<Expression> groupingKeys;

        public GroupByAst(List<Expression> groupingKeys)
        {
            this.groupingKeys = ImmutableList.copyOf(groupingKeys);
        }

        @Override
        public SourceSpan getSourceSpan()
        {
            return groupingKeys.isEmpty() ? null : groupingKeys.get(0).getSourceSpan();
        }
    }

    public static final class OrderByAst implements AstNode
    {
        public final List<OrderingItem> orderings;

        public OrderByAst(List<OrderingItem> orderings)
        {
            this.orderings = ImmutableList.copyOf(orderings);
        }

        @Override
        public SourceSpan getSourceSpan()
        {
            return orderings.isEmpty() ? null : orderings.get(0).getSourceSpan();
        }
    }

    public static final class OrderingItem implements AstNode
    {
        public enum Direction { ASC, DESC }

        public final Expression expression;
        public final Direction direction;

        public OrderingItem(Expression expression, Direction direction)
        {
            this.expression = expression;
            this.direction = direction;
        }

        @Override
        public SourceSpan getSourceSpan()
        {
            return expression.getSourceSpan();
        }
    }
}
