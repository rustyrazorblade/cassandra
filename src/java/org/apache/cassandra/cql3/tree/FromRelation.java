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

import org.apache.cassandra.cql3.QualifiedName;

/**
 * Abstract base for FROM clause relations. TableRef is the only implementation for now.
 * JoinRel is declared but not implemented (phase 9).
 */
public abstract class FromRelation implements AstNode
{
    private final SourceSpan span;

    protected FromRelation(SourceSpan span)
    {
        this.span = span;
    }

    @Override
    public SourceSpan getSourceSpan()
    {
        return span;
    }

    public static final class TableRef extends FromRelation
    {
        public final QualifiedName tableName;

        public TableRef(SourceSpan span, QualifiedName tableName)
        {
            super(span);
            this.tableName = tableName;
        }
    }

    /**
     * Extension point for phase 9 (joins). Not implemented yet.
     */
    public static abstract class JoinRel extends FromRelation
    {
        protected JoinRel(SourceSpan span)
        {
            super(span);
            // The phase-9 broadcast hash join is NOT driven from this AST tree.  It is driven from
            // SelectStatement.JoinSpec, resolved in SelectStatement.RawStatement.prepare and executed
            // at the coordinator.  This stub stays unimplemented on purpose.
            throw new UnsupportedOperationException("Joins not yet implemented");
        }
    }
}
