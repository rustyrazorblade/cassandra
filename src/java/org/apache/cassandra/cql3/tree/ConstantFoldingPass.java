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
 * Optimization pass that folds constant arithmetic expressions at parse time.
 * Example: SELECT 1 + 2 => SELECT 3.
 *
 * <p>Guards to type-identical folding only (no int-to-long widening, etc) so the lowered
 * result remains a valid, equivalent domain object. This is a POC demonstrating the
 * optimizer insertion point.
 */
public final class ConstantFoldingPass extends AstRewriter
{
    public static Statement optimize(Statement stmt)
    {
        return new ConstantFoldingPass().rewrite(stmt);
    }

    @Override
    public Expression visitArithmetic(Expression.Arithmetic node)
    {
        Expression left = node.left.accept(this);
        Expression right = node.right.accept(this);

        if (left instanceof Expression.Literal && right instanceof Expression.Literal)
        {
            Object lval = ((Expression.Literal) left).value;
            Object rval = ((Expression.Literal) right).value;

            if (lval instanceof Long && rval instanceof Long)
            {
                long l = (Long) lval;
                long r = (Long) rval;
                long result = 0;

                switch (node.operator)
                {
                    case ADD:
                        result = l + r;
                        break;
                    case SUBTRACT:
                        result = l - r;
                        break;
                    case MULTIPLY:
                        result = l * r;
                        break;
                    case DIVIDE:
                        if (r == 0)
                            return new Expression.Arithmetic(node.getSourceSpan(), node.operator, left, right);
                        result = l / r;
                        break;
                    case MODULO:
                        if (r == 0)
                            return new Expression.Arithmetic(node.getSourceSpan(), node.operator, left, right);
                        result = l % r;
                        break;
                }

                return new Expression.Literal(node.getSourceSpan(), result);
            }

            if (lval instanceof Double && rval instanceof Double)
            {
                double l = (Double) lval;
                double r = (Double) rval;
                double result = 0.0;

                switch (node.operator)
                {
                    case ADD:
                        result = l + r;
                        break;
                    case SUBTRACT:
                        result = l - r;
                        break;
                    case MULTIPLY:
                        result = l * r;
                        break;
                    case DIVIDE:
                        result = l / r;
                        break;
                    case MODULO:
                        result = l % r;
                        break;
                }

                return new Expression.Literal(node.getSourceSpan(), result);
            }
        }

        return new Expression.Arithmetic(node.getSourceSpan(), node.operator, left, right);
    }

    @Override
    public Expression visitNegation(Expression.Negation node)
    {
        Expression operand = node.operand.accept(this);

        if (operand instanceof Expression.Literal)
        {
            Object val = ((Expression.Literal) operand).value;

            if (val instanceof Long)
                return new Expression.Literal(node.getSourceSpan(), -(Long) val);

            if (val instanceof Double)
                return new Expression.Literal(node.getSourceSpan(), -(Double) val);
        }

        return new Expression.Negation(node.getSourceSpan(), operand);
    }
}
