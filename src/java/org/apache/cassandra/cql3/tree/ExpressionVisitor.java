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

/**
 * Visitor for expression nodes.
 */
public interface ExpressionVisitor<R>
{
    R visitColumnRef(Expression.ColumnRef node);
    R visitLiteral(Expression.Literal node);
    R visitNullLiteral(Expression.NullLiteral node);
    R visitBindMarker(Expression.BindMarker node);
    R visitFunctionCall(Expression.FunctionCall node);
    R visitArithmetic(Expression.Arithmetic node);
    R visitNegation(Expression.Negation node);
    R visitFieldAccess(Expression.FieldAccess node);
    R visitElementAccess(Expression.ElementAccess node);
    R visitSliceAccess(Expression.SliceAccess node);
    R visitCast(Expression.Cast node);
    R visitTypeHint(Expression.TypeHint node);
    R visitListExpr(Expression.ListExpr node);
    R visitSetExpr(Expression.SetExpr node);
    R visitMapExpr(Expression.MapExpr node);
    R visitTupleExpr(Expression.TupleExpr node);
    R visitUdtExpr(Expression.UdtExpr node);
    R visitComparison(Expression.Comparison node);
    R visitInExpr(Expression.InExpr node);
    R visitContainsExpr(Expression.ContainsExpr node);
    R visitTokenExpr(Expression.TokenExpr node);
    R visitCustomIndexExpr(Expression.CustomIndexExpr node);
    R visitCase(Expression.Case node);
    R visitSubqueryExpr(Expression.SubqueryExpr node);
}
