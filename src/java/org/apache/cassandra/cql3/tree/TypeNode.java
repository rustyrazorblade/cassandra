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

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.QualifiedName;

/**
 * Represents a type reference in the AST (for casts, type hints, etc).
 */
public abstract class TypeNode implements AstNode
{
    private final SourceSpan span;

    protected TypeNode(SourceSpan span)
    {
        this.span = span;
    }

    @Override
    public SourceSpan getSourceSpan()
    {
        return span;
    }

    public static final class SimpleType extends TypeNode
    {
        public final QualifiedName name;

        public SimpleType(SourceSpan span, QualifiedName name)
        {
            super(span);
            this.name = name;
        }
    }

    public static final class CollectionType extends TypeNode
    {
        public enum Kind { LIST, SET, MAP }

        public final Kind kind;
        public final List<TypeNode> typeArgs;
        public final boolean frozen;

        public CollectionType(SourceSpan span, Kind kind, List<TypeNode> typeArgs, boolean frozen)
        {
            super(span);
            this.kind = kind;
            this.typeArgs = ImmutableList.copyOf(typeArgs);
            this.frozen = frozen;
        }
    }

    public static final class TupleType extends TypeNode
    {
        public final List<TypeNode> elementTypes;

        public TupleType(SourceSpan span, List<TypeNode> elementTypes)
        {
            super(span);
            this.elementTypes = ImmutableList.copyOf(elementTypes);
        }
    }
}
