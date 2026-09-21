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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for rewriting passes that transform an immutable AST into a new AST.
 * The default implementation returns an unchanged tree. Subclasses override specific
 * visit methods to transform nodes.
 */
public abstract class AstRewriter implements ExpressionVisitor<Expression>
{
    public Statement rewrite(Statement stmt)
    {
        if (stmt instanceof SelectAst)
            return rewriteSelectAst((SelectAst) stmt);
        return stmt;
    }

    protected SelectAst rewriteSelectAst(SelectAst node)
    {
        List<SelectAst.SelectItemAst> newItems = node.selectItems.stream()
            .map(item -> new SelectAst.SelectItemAst(item.expression.accept(this), item.alias))
            .collect(Collectors.toList());

        SelectAst.WhereAst newWhere = node.where == null ? null : rewriteWhereAst(node.where);
        SelectAst.OrderByAst newOrderBy = node.orderBy == null ? null : rewriteOrderByAst(node.orderBy);
        Expression newPerPartitionLimit = node.perPartitionLimit == null ? null : node.perPartitionLimit.accept(this);
        Expression newLimit = node.limit == null ? null : node.limit.accept(this);

        return new SelectAst(node.getSourceSpan(), node.isDistinct, node.isJson, newItems,
                             node.from, newWhere, node.groupBy, newOrderBy,
                             newPerPartitionLimit, newLimit, node.allowFiltering, node.options, null);
    }

    protected SelectAst.WhereAst rewriteWhereAst(SelectAst.WhereAst where)
    {
        List<Expression> newPredicates = where.predicates.stream()
            .map(p -> p.accept(this))
            .collect(Collectors.toList());
        return new SelectAst.WhereAst(newPredicates);
    }

    protected SelectAst.OrderByAst rewriteOrderByAst(SelectAst.OrderByAst orderBy)
    {
        List<SelectAst.OrderingItem> newOrderings = orderBy.orderings.stream()
            .map(o -> new SelectAst.OrderingItem(o.expression.accept(this), o.direction))
            .collect(Collectors.toList());
        return new SelectAst.OrderByAst(newOrderings);
    }

    // Default implementations return unchanged nodes
    @Override
    public Expression visitColumnRef(Expression.ColumnRef node)
    {
        return node;
    }

    @Override
    public Expression visitLiteral(Expression.Literal node)
    {
        return node;
    }

    @Override
    public Expression visitNullLiteral(Expression.NullLiteral node)
    {
        return node;
    }

    @Override
    public Expression visitBindMarker(Expression.BindMarker node)
    {
        return node;
    }

    @Override
    public Expression visitFunctionCall(Expression.FunctionCall node)
    {
        List<Expression> newArgs = node.arguments.stream()
            .map(arg -> arg.accept(this))
            .collect(Collectors.toList());
        return new Expression.FunctionCall(node.getSourceSpan(), node.functionName, newArgs, node.isCountStar);
    }

    @Override
    public Expression visitArithmetic(Expression.Arithmetic node)
    {
        Expression newLeft = node.left.accept(this);
        Expression newRight = node.right.accept(this);
        return new Expression.Arithmetic(node.getSourceSpan(), node.operator, newLeft, newRight);
    }

    @Override
    public Expression visitNegation(Expression.Negation node)
    {
        Expression newOperand = node.operand.accept(this);
        return new Expression.Negation(node.getSourceSpan(), newOperand);
    }

    @Override
    public Expression visitFieldAccess(Expression.FieldAccess node)
    {
        Expression newBase = node.base.accept(this);
        return new Expression.FieldAccess(node.getSourceSpan(), newBase, node.field);
    }

    @Override
    public Expression visitElementAccess(Expression.ElementAccess node)
    {
        Expression newBase = node.base.accept(this);
        Expression newIndex = node.index.accept(this);
        return new Expression.ElementAccess(node.getSourceSpan(), newBase, newIndex);
    }

    @Override
    public Expression visitSliceAccess(Expression.SliceAccess node)
    {
        Expression newBase = node.base.accept(this);
        Expression newStart = node.start == null ? null : node.start.accept(this);
        Expression newEnd = node.end == null ? null : node.end.accept(this);
        return new Expression.SliceAccess(node.getSourceSpan(), newBase, newStart, newEnd);
    }

    @Override
    public Expression visitCast(Expression.Cast node)
    {
        Expression newOperand = node.operand.accept(this);
        return new Expression.Cast(node.getSourceSpan(), newOperand, node.targetType);
    }

    @Override
    public Expression visitTypeHint(Expression.TypeHint node)
    {
        Expression newOperand = node.operand.accept(this);
        return new Expression.TypeHint(node.getSourceSpan(), node.type, newOperand);
    }

    @Override
    public Expression visitListExpr(Expression.ListExpr node)
    {
        List<Expression> newElements = node.elements.stream()
            .map(e -> e.accept(this))
            .collect(Collectors.toList());
        return new Expression.ListExpr(node.getSourceSpan(), newElements);
    }

    @Override
    public Expression visitSetExpr(Expression.SetExpr node)
    {
        List<Expression> newElements = node.elements.stream()
            .map(e -> e.accept(this))
            .collect(Collectors.toList());
        return new Expression.SetExpr(node.getSourceSpan(), newElements);
    }

    @Override
    public Expression visitMapExpr(Expression.MapExpr node)
    {
        Map<Expression, Expression> newEntries = new HashMap<>();
        for (Map.Entry<Expression, Expression> e : node.entries.entrySet())
            newEntries.put(e.getKey().accept(this), e.getValue().accept(this));
        return new Expression.MapExpr(node.getSourceSpan(), newEntries);
    }

    @Override
    public Expression visitTupleExpr(Expression.TupleExpr node)
    {
        List<Expression> newElements = node.elements.stream()
            .map(e -> e.accept(this))
            .collect(Collectors.toList());
        return new Expression.TupleExpr(node.getSourceSpan(), newElements);
    }

    @Override
    public Expression visitUdtExpr(Expression.UdtExpr node)
    {
        Map<org.apache.cassandra.cql3.ColumnIdentifier, Expression> newFields = new HashMap<>();
        for (Map.Entry<org.apache.cassandra.cql3.ColumnIdentifier, Expression> e : node.fields.entrySet())
            newFields.put(e.getKey(), e.getValue().accept(this));
        return new Expression.UdtExpr(node.getSourceSpan(), newFields);
    }

    @Override
    public Expression visitComparison(Expression.Comparison node)
    {
        Expression newLeft = node.left.accept(this);
        Expression newRight = node.right.accept(this);
        return new Expression.Comparison(node.getSourceSpan(), newLeft, node.operator, newRight);
    }

    @Override
    public Expression visitInExpr(Expression.InExpr node)
    {
        Expression newLeft = node.left.accept(this);
        List<Expression> newValues = node.values.stream()
            .map(v -> v.accept(this))
            .collect(Collectors.toList());
        return new Expression.InExpr(node.getSourceSpan(), newLeft, newValues);
    }

    @Override
    public Expression visitContainsExpr(Expression.ContainsExpr node)
    {
        Expression newCollection = node.collection.accept(this);
        Expression newElement = node.element.accept(this);
        return new Expression.ContainsExpr(node.getSourceSpan(), newCollection, newElement, node.isKey);
    }

    @Override
    public Expression visitTokenExpr(Expression.TokenExpr node)
    {
        List<Expression> newKeys = node.partitionKeys.stream()
            .map(k -> k.accept(this))
            .collect(Collectors.toList());
        return new Expression.TokenExpr(node.getSourceSpan(), newKeys);
    }

    @Override
    public Expression visitCustomIndexExpr(Expression.CustomIndexExpr node)
    {
        Expression newValue = node.value.accept(this);
        return new Expression.CustomIndexExpr(node.getSourceSpan(), node.indexName, newValue);
    }
}
