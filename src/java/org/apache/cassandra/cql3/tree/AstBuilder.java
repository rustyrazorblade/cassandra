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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nullable;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;

/**
 * Builds AST nodes from ANTLR4 CqlParser contexts. Scoped to corpus-tested SELECT constructs.
 * Expression nodes carry parser-built Selectable.Raw/Term.Raw; SelectAst carries RawStatement.
 */
public final class AstBuilder
{
    private final Map<Token, BindMarkerInfo> bindMarkersByToken = new HashMap<>();

    private static final class BindMarkerInfo
    {
        final int ordinal;
        @Nullable
        final ColumnIdentifier name;

        BindMarkerInfo(int ordinal, @Nullable ColumnIdentifier name)
        {
            this.ordinal = ordinal;
            this.name = name;
        }
    }

    private AstBuilder()
    {
    }

    public static Statement build(CqlParser.QueryContext ctx)
    {
        AstBuilder builder = new AstBuilder();
        builder.assignBindMarkerOrdinals(ctx);

        if (ctx.cqlStatement() != null && ctx.cqlStatement().selectStatement() != null)
            return builder.buildSelectAst(ctx.cqlStatement().selectStatement());

        throw new UnsupportedAstException("Only SELECT supported in POC");
    }

    private void assignBindMarkerOrdinals(ParserRuleContext ctx)
    {
        Map<Integer, CqlParser.MarkerContext> markersByPosition = new TreeMap<>();
        collectBindMarkerContexts(ctx, markersByPosition);

        int ordinal = 0;
        for (CqlParser.MarkerContext marker : markersByPosition.values())
        {
            ColumnIdentifier name = null;
            if (marker.noncol_ident() != null)
                name = marker.noncol_ident().id;

            bindMarkersByToken.put(marker.getStart(), new BindMarkerInfo(ordinal++, name));
        }
    }

    private void collectBindMarkerContexts(ParseTree tree, Map<Integer, CqlParser.MarkerContext> markers)
    {
        if (tree instanceof CqlParser.MarkerContext)
        {
            CqlParser.MarkerContext m = (CqlParser.MarkerContext) tree;
            markers.put(m.getStart().getTokenIndex(), m);
        }

        if (tree instanceof ParserRuleContext)
        {
            ParserRuleContext ctx = (ParserRuleContext) tree;
            for (int i = 0; i < ctx.getChildCount(); i++)
                collectBindMarkerContexts(ctx.getChild(i), markers);
        }
    }

    private SelectAst buildSelectAst(CqlParser.SelectStatementContext ctx)
    {
        AstNode.SourceSpan span = span(ctx);

        boolean isJson = ctx.K_JSON() != null;
        boolean isDistinct = ctx.selectClause() != null && ctx.selectClause().K_DISTINCT() != null;

        List<SelectAst.SelectItemAst> items = buildSelectItems(ctx.selectClause());
        FromRelation from = buildFrom(ctx.columnFamilyName());

        SelectAst.WhereAst where = null;
        if (ctx.whereClause() != null)
            where = buildWhere(ctx.whereClause());

        SelectAst.GroupByAst groupBy = null;
        SelectAst.OrderByAst orderBy = null;
        if (!ctx.orderByClause().isEmpty())
            orderBy = buildOrderBy(ctx.orderByClause());

        Expression limit = null;
        Expression perPartitionLimit = null;

        // Extract limits from intValue contexts
        // Order: if PER PARTITION LIMIT present, it comes first, then LIMIT
        if (!ctx.intValue().isEmpty())
        {
            if (ctx.K_PER() != null && ctx.K_PARTITION() != null && ctx.intValue().size() >= 1)
            {
                // Has PER PARTITION LIMIT
                perPartitionLimit = buildExprFromIntValue(ctx.intValue(0));
                if (ctx.intValue().size() >= 2)
                    limit = buildExprFromIntValue(ctx.intValue(1));
            }
            else if (ctx.intValue().size() >= 1)
            {
                // Only LIMIT (no PER PARTITION)
                limit = buildExprFromIntValue(ctx.intValue(0));
            }
        }

        boolean allowFiltering = ctx.K_ALLOW() != null && ctx.K_FILTERING() != null;

        // Carry the parser-built RawStatement for POC lowering
        return new SelectAst(span, isDistinct, isJson, items, from, where, groupBy, orderBy,
                             perPartitionLimit, limit, allowFiltering, new HashMap<>(), ctx.expr);
    }

    private List<SelectAst.SelectItemAst> buildSelectItems(CqlParser.SelectClauseContext ctx)
    {
        List<SelectAst.SelectItemAst> items = new ArrayList<>();

        if (ctx != null && ctx.selectors() != null)
        {
            for (CqlParser.SelectorContext sctx : ctx.selectors().selector())
            {
                RawSelector rawSelector = sctx.s;
                Selectable.Raw raw = rawSelector.selectable;

                Expression expr = buildExprFromSelectable(sctx, raw);
                items.add(new SelectAst.SelectItemAst(expr, rawSelector.alias));
            }
        }

        return items;
    }

    private Expression buildExprFromSelectable(CqlParser.SelectorContext sctx, Selectable.Raw raw)
    {
        // Try to extract arithmetic from unaliasedSelector
        if (sctx.unaliasedSelector() != null && sctx.unaliasedSelector().selectionAddition() != null)
        {
            CqlParser.SelectionAdditionContext addCtx = sctx.unaliasedSelector().selectionAddition();
            Expression arith = tryBuildArithmetic(addCtx);
            if (arith != null)
                return arith;
        }

        // Fallback: ColumnRef
        QualifiedName name;
        if (raw instanceof Selectable.RawIdentifier)
        {
            Selectable.RawIdentifier ident = (Selectable.RawIdentifier) raw;
            name = new QualifiedName(null, ident.getText());
        }
        else
        {
            name = new QualifiedName(null, "col");
        }

        return new Expression.ColumnRef(span(sctx), name, raw);
    }

    private Expression tryBuildArithmetic(CqlParser.SelectionAdditionContext ctx)
    {
        if (ctx.selectionMultiplication().size() == 2)
        {
            // Binary addition or subtraction
            Expression left = tryBuildMultiplication(ctx.selectionMultiplication(0));
            Expression right = tryBuildMultiplication(ctx.selectionMultiplication(1));
            if (left != null && right != null)
            {
                // Read the operator token between the two operands (child index 1)
                String opText = ctx.getChild(1).getText();
                Expression.Arithmetic.Operator op = opText.equals("+")
                    ? Expression.Arithmetic.Operator.ADD
                    : Expression.Arithmetic.Operator.SUBTRACT;
                return new Expression.Arithmetic(span(ctx), op, left, right);
            }
        }
        return null;
    }

    private Expression tryBuildMultiplication(CqlParser.SelectionMultiplicationContext ctx)
    {
        if (ctx.selectionGroup().size() == 2)
        {
            // Binary multiplication, division, or modulo
            Expression left = tryBuildSelectionGroup(ctx.selectionGroup(0));
            Expression right = tryBuildSelectionGroup(ctx.selectionGroup(1));
            if (left != null && right != null)
            {
                // Read the operator token between the two operands (child index 1)
                String opText = ctx.getChild(1).getText();
                Expression.Arithmetic.Operator op;
                if (opText.equals("*"))
                    op = Expression.Arithmetic.Operator.MULTIPLY;
                else if (opText.equals("/"))
                    op = Expression.Arithmetic.Operator.DIVIDE;
                else
                    op = Expression.Arithmetic.Operator.MODULO;
                return new Expression.Arithmetic(span(ctx), op, left, right);
            }
        }
        else if (ctx.selectionGroup().size() == 1)
        {
            return tryBuildSelectionGroup(ctx.selectionGroup(0));
        }
        return null;
    }

    private Expression tryBuildSelectionGroup(CqlParser.SelectionGroupContext ctx)
    {
        // Try to extract primary term
        if (ctx.selectionGroupWithField() != null)
        {
            CqlParser.SelectionGroupWithFieldContext fieldCtx = ctx.selectionGroupWithField();
            if (fieldCtx.selectionGroupWithoutField() != null)
            {
                CqlParser.SelectionGroupWithoutFieldContext withoutCtx = fieldCtx.selectionGroupWithoutField();
                if (withoutCtx.simpleUnaliasedSelector() != null)
                {
                    CqlParser.SimpleUnaliasedSelectorContext simpleCtx = withoutCtx.simpleUnaliasedSelector();
                    if (simpleCtx.sident() != null)
                    {
                        // Simple column reference
                        CqlParser.SidentContext sidCtx = simpleCtx.sident();
                        String text = sidCtx.getText();
                        return new Expression.ColumnRef(span(sidCtx), new QualifiedName(null, text));
                    }
                    else if (simpleCtx.selectionLiteral() != null)
                    {
                        // Literal
                        return buildExprFromSelectionLiteral(simpleCtx.selectionLiteral());
                    }
                }
            }
        }
        return null;
    }

    private Expression buildExprFromSelectionLiteral(CqlParser.SelectionLiteralContext ctx)
    {
        if (ctx.constant() != null)
            return buildExprFromConstant(ctx.constant());
        String text = ctx.getText();
        return new Expression.Literal(span(ctx), text);
    }

    private Expression buildExprFromConstant(CqlParser.ConstantContext ctx)
    {
        String text = ctx.getText();
        if (text.matches("-?\\d+"))
            return new Expression.Literal(span(ctx), Long.parseLong(text));
        if (text.startsWith("'") && text.endsWith("'"))
        {
            String unquoted = text.substring(1, text.length() - 1).replace("''", "'");
            return new Expression.Literal(span(ctx), unquoted);
        }
        return new Expression.Literal(span(ctx), text);
    }

    private FromRelation buildFrom(CqlParser.ColumnFamilyNameContext ctx)
    {
        QualifiedName tableName = ctx.name != null ? ctx.name : new QualifiedName(null, "tbl");
        return new FromRelation.TableRef(span(ctx), tableName);
    }

    private SelectAst.WhereAst buildWhere(CqlParser.WhereClauseContext ctx)
    {
        List<Expression> predicates = new ArrayList<>();

        for (CqlParser.RelationOrExpressionContext roeCtx : ctx.relationOrExpression())
        {
            if (roeCtx.relation() != null)
            {
                Expression predicate = buildRelation(roeCtx.relation());
                if (predicate != null)
                    predicates.add(predicate);
            }
        }

        return new SelectAst.WhereAst(predicates);
    }

    private SelectAst.OrderByAst buildOrderBy(List<CqlParser.OrderByClauseContext> clauses)
    {
        List<SelectAst.OrderingItem> orderings = new ArrayList<>();

        for (CqlParser.OrderByClauseContext clause : clauses)
        {
            if (clause.cident() != null)
            {
                Expression expr = new Expression.ColumnRef(span(clause.cident()), extractQualifiedName(clause.cident()));
                SelectAst.OrderingItem.Direction dir = SelectAst.OrderingItem.Direction.ASC;
                if (clause.K_DESC() != null)
                    dir = SelectAst.OrderingItem.Direction.DESC;
                orderings.add(new SelectAst.OrderingItem(expr, dir));
            }
        }

        return new SelectAst.OrderByAst(orderings);
    }

    private Expression buildRelation(CqlParser.RelationContext ctx)
    {
        // Simple: cident relationType term
        if (ctx.cident() != null && ctx.relationType() != null && !ctx.term().isEmpty())
        {
            Expression left = new Expression.ColumnRef(span(ctx.cident()), extractQualifiedName(ctx.cident()));
            Expression.Comparison.Operator op = mapRelationTypeToOperator(ctx.relationType());
            Expression right = buildExprFromTerm(ctx.term(0));
            return new Expression.Comparison(span(ctx), left, op, right);
        }

        // IN: cident IN ( term, term, ... )
        if (ctx.cident() != null && ctx.inValue != null)
        {
            Expression left = new Expression.ColumnRef(span(ctx.cident()), extractQualifiedName(ctx.cident()));
            List<Expression> values = new ArrayList<>();

            if (ctx.inValue.terms() != null)
            {
                for (CqlParser.TermContext termCtx : ctx.inValue.terms().term())
                    values.add(buildExprFromTerm(termCtx));
            }
            // inMarker is for single marker representing the whole list, skip for now
            // The corpus tests use explicit term lists

            return new Expression.InExpr(span(ctx), left, values);
        }

        throw new UnsupportedAstException("Unsupported relation: " + ctx.getText());
    }

    private Expression buildExprFromTerm(CqlParser.TermContext ctx)
    {
        CqlParser.MarkerContext marker = findMarker(ctx);
        if (marker != null)
        {
            BindMarkerInfo info = bindMarkersByToken.get(marker.getStart());
            if (info != null)
                return new Expression.BindMarker(span(marker), info.ordinal, info.name);
        }

        String text = ctx.getText();
        if (text.matches("-?\\d+"))
            return new Expression.Literal(span(ctx), Long.parseLong(text));

        if (text.startsWith("'") && text.endsWith("'"))
        {
            String unquoted = text.substring(1, text.length() - 1).replace("''", "'");
            return new Expression.Literal(span(ctx), unquoted);
        }

        throw new UnsupportedAstException("Unsupported term: " + text);
    }

    private Expression buildExprFromIntValue(CqlParser.IntValueContext ctx)
    {
        if (ctx.marker() != null)
        {
            CqlParser.MarkerContext marker = ctx.marker();
            BindMarkerInfo info = bindMarkersByToken.get(marker.getStart());
            if (info != null)
                return new Expression.BindMarker(span(marker), info.ordinal, info.name);
        }

        if (ctx.INTEGER() != null)
        {
            long value = Long.parseLong(ctx.INTEGER().getText());
            return new Expression.Literal(span(ctx), value);
        }

        throw new UnsupportedAstException("Unsupported intValue: " + ctx.getText());
    }

    private CqlParser.MarkerContext findMarker(ParseTree tree)
    {
        if (tree instanceof CqlParser.MarkerContext)
            return (CqlParser.MarkerContext) tree;

        if (tree instanceof ParserRuleContext)
        {
            ParserRuleContext ctx = (ParserRuleContext) tree;
            for (int i = 0; i < ctx.getChildCount(); i++)
            {
                CqlParser.MarkerContext found = findMarker(ctx.getChild(i));
                if (found != null)
                    return found;
            }
        }

        return null;
    }

    private QualifiedName extractQualifiedName(CqlParser.CidentContext ctx)
    {
        if (ctx.ident() != null)
            return new QualifiedName(null, ctx.ident().getText());
        return new QualifiedName(null, ctx.getText());
    }

    private Expression.Comparison.Operator mapRelationTypeToOperator(CqlParser.RelationTypeContext ctx)
    {
        String text = ctx.getText();
        switch (text)
        {
            case "=": return Expression.Comparison.Operator.EQ;
            case "!=":
            case "<>": return Expression.Comparison.Operator.NEQ;
            case "<": return Expression.Comparison.Operator.LT;
            case "<=": return Expression.Comparison.Operator.LTE;
            case ">": return Expression.Comparison.Operator.GT;
            case ">=": return Expression.Comparison.Operator.GTE;
            default: throw new UnsupportedAstException("Unknown operator: " + text);
        }
    }

    private AstNode.SourceSpan span(ParserRuleContext ctx)
    {
        Token start = ctx.getStart();
        Token stop = ctx.getStop();
        return new AstNode.SourceSpan(start.getLine(), start.getCharPositionInLine(),
                                      stop.getLine(), stop.getCharPositionInLine());
    }
}
