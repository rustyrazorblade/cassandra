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
package org.apache.cassandra.cql3;

import org.junit.Test;

import org.apache.cassandra.cql3.tree.AstBuilder;
import org.apache.cassandra.cql3.tree.Expression;
import org.apache.cassandra.cql3.tree.SelectAst;
import org.apache.cassandra.cql3.tree.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Happy-path unit tests for the AST builder. Asserts the AST node structure directly
 * (not via lowering). Catches structural regressions the equivalence test might miss.
 */
public class AstShapeTest
{
    @Test
    public void simpleSelect()
    {
        Statement stmt = parseSelect("SELECT a, b FROM ks.tbl");
        assertTrue(stmt instanceof SelectAst);

        SelectAst select = (SelectAst) stmt;
        assertFalse(select.isDistinct);
        assertFalse(select.isJson);
        assertEquals(2, select.selectItems.size());

        assertEquals("a", getColumnRefName(select.selectItems.get(0).expression));
        assertEquals("b", getColumnRefName(select.selectItems.get(1).expression));
    }

    @Test
    public void selectWithWhere()
    {
        Statement stmt = parseSelect("SELECT * FROM tbl WHERE a = 1 AND b = 2");
        SelectAst select = (SelectAst) stmt;

        assertNotNull(select.where);
        assertEquals(2, select.where.predicates.size());

        assertTrue(select.where.predicates.get(0) instanceof Expression.Comparison);
        assertTrue(select.where.predicates.get(1) instanceof Expression.Comparison);
    }

    @Test
    public void selectWithBindMarkers()
    {
        Statement stmt = parseSelect("SELECT * FROM tbl WHERE x = ? AND y = ?");
        SelectAst select = (SelectAst) stmt;

        assertNotNull(select.where);
        assertEquals(2, select.where.predicates.size());

        Expression.Comparison comp0 = (Expression.Comparison) select.where.predicates.get(0);
        Expression.Comparison comp1 = (Expression.Comparison) select.where.predicates.get(1);

        assertTrue(comp0.right instanceof Expression.BindMarker);
        assertTrue(comp1.right instanceof Expression.BindMarker);

        Expression.BindMarker marker0 = (Expression.BindMarker) comp0.right;
        Expression.BindMarker marker1 = (Expression.BindMarker) comp1.right;

        assertEquals(0, marker0.ordinal);
        assertEquals(1, marker1.ordinal);
    }

    @Test
    public void selectWithNamedBindMarkers()
    {
        Statement stmt = parseSelect("SELECT * FROM tbl WHERE x = :named AND y = :other");
        SelectAst select = (SelectAst) stmt;

        assertNotNull(select.where);
        assertEquals(2, select.where.predicates.size());

        Expression.Comparison comp0 = (Expression.Comparison) select.where.predicates.get(0);
        Expression.Comparison comp1 = (Expression.Comparison) select.where.predicates.get(1);

        Expression.BindMarker marker0 = (Expression.BindMarker) comp0.right;
        Expression.BindMarker marker1 = (Expression.BindMarker) comp1.right;

        assertEquals(0, marker0.ordinal);
        assertEquals(1, marker1.ordinal);
        assertNotNull(marker0.name);
        assertNotNull(marker1.name);
        assertEquals("named", marker0.name.toString());
        assertEquals("other", marker1.name.toString());
    }

    @Test
    public void selectWithLimits()
    {
        Statement stmt = parseSelect("SELECT * FROM tbl WHERE x = ? PER PARTITION LIMIT ? LIMIT ?");
        SelectAst select = (SelectAst) stmt;

        assertNotNull(select.perPartitionLimit);
        assertNotNull(select.limit);

        assertTrue(select.perPartitionLimit instanceof Expression.BindMarker);
        assertTrue(select.limit instanceof Expression.BindMarker);

        Expression.BindMarker ppl = (Expression.BindMarker) select.perPartitionLimit;
        Expression.BindMarker lim = (Expression.BindMarker) select.limit;

        assertEquals(1, ppl.ordinal);
        assertEquals(2, lim.ordinal);
    }

    @Test
    public void selectWithIn()
    {
        Statement stmt = parseSelect("SELECT * FROM tbl WHERE id IN (?, ?, ?)");
        SelectAst select = (SelectAst) stmt;

        assertNotNull(select.where);
        assertEquals(1, select.where.predicates.size());

        assertTrue(select.where.predicates.get(0) instanceof Expression.InExpr);
        Expression.InExpr inExpr = (Expression.InExpr) select.where.predicates.get(0);

        assertEquals(3, inExpr.values.size());
        for (int i = 0; i < 3; i++)
        {
            assertTrue(inExpr.values.get(i) instanceof Expression.BindMarker);
            assertEquals(i, ((Expression.BindMarker) inExpr.values.get(i)).ordinal);
        }
    }

    @Test
    public void selectWithOrderBy()
    {
        Statement stmt = parseSelect("SELECT * FROM tbl ORDER BY a ASC, b DESC");
        SelectAst select = (SelectAst) stmt;

        assertNotNull(select.orderBy);
        assertEquals(2, select.orderBy.orderings.size());

        assertEquals(SelectAst.OrderingItem.Direction.ASC, select.orderBy.orderings.get(0).direction);
        assertEquals(SelectAst.OrderingItem.Direction.DESC, select.orderBy.orderings.get(1).direction);
    }

    @Test
    public void selectWithArithmetic()
    {
        Statement stmt = parseSelect("SELECT a + 1 FROM tbl");
        SelectAst select = (SelectAst) stmt;

        assertEquals(1, select.selectItems.size());
        assertTrue(select.selectItems.get(0).expression instanceof Expression.Arithmetic);

        Expression.Arithmetic arith = (Expression.Arithmetic) select.selectItems.get(0).expression;
        assertEquals(Expression.Arithmetic.Operator.ADD, arith.operator);
    }

    @Test
    public void selectDistinctJson()
    {
        Statement stmt = parseSelect("SELECT JSON DISTINCT a FROM tbl");
        SelectAst select = (SelectAst) stmt;

        assertTrue(select.isJson);
        assertTrue(select.isDistinct);
    }

    @Test
    public void selectWithMixedNamedPositionalBindMarkers()
    {
        Statement stmt = parseSelect("SELECT * FROM ks.tbl WHERE x = :named AND y = ? AND z = :other");
        SelectAst select = (SelectAst) stmt;

        assertNotNull(select.where);
        assertEquals(3, select.where.predicates.size());

        Expression.Comparison comp0 = (Expression.Comparison) select.where.predicates.get(0);
        Expression.Comparison comp1 = (Expression.Comparison) select.where.predicates.get(1);
        Expression.Comparison comp2 = (Expression.Comparison) select.where.predicates.get(2);

        Expression.BindMarker marker0 = (Expression.BindMarker) comp0.right;
        Expression.BindMarker marker1 = (Expression.BindMarker) comp1.right;
        Expression.BindMarker marker2 = (Expression.BindMarker) comp2.right;

        assertEquals(0, marker0.ordinal);
        assertEquals(1, marker1.ordinal);
        assertEquals(2, marker2.ordinal);

        assertNotNull(marker0.name);
        assertEquals("named", marker0.name.toString());
        assertNull(marker1.name);
        assertNotNull(marker2.name);
        assertEquals("other", marker2.name.toString());
    }

    @Test
    public void selectWithInAndLimitBindMarkers()
    {
        Statement stmt = parseSelect("SELECT * FROM ks.tbl WHERE id IN (?, ?) LIMIT ?");
        SelectAst select = (SelectAst) stmt;

        assertNotNull(select.where);
        assertEquals(1, select.where.predicates.size());

        Expression.InExpr inExpr = (Expression.InExpr) select.where.predicates.get(0);
        assertEquals(2, inExpr.values.size());

        Expression.BindMarker inMarker0 = (Expression.BindMarker) inExpr.values.get(0);
        Expression.BindMarker inMarker1 = (Expression.BindMarker) inExpr.values.get(1);

        assertEquals(0, inMarker0.ordinal);
        assertEquals(1, inMarker1.ordinal);

        assertNotNull(select.limit);
        assertTrue(select.limit instanceof Expression.BindMarker);
        Expression.BindMarker limitMarker = (Expression.BindMarker) select.limit;
        assertEquals(2, limitMarker.ordinal);
    }

    private Statement parseSelect(String cql)
    {
        try
        {
            CqlParser.QueryContext ctx = CQLFragmentParser.parseAnyUnhandled(p -> p.query(), cql);
            return AstBuilder.build(ctx);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Parse failed for: " + cql, e);
        }
    }

    private String getColumnRefName(Expression expr)
    {
        if (expr instanceof Expression.ColumnRef)
            return ((Expression.ColumnRef) expr).name.getName();
        throw new AssertionError("Not a ColumnRef: " + expr.getClass());
    }
}
