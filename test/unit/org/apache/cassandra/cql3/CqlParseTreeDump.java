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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Dependency-free, deterministic serializer for a parsed CQL domain-object graph.
 *
 * <p>The ANTLR 3 -> 4 migration must preserve the objects the parser builds.  This class turns a
 * parsed object (a {@code *.Raw} statement, a {@code Term.Raw}, a {@code CQL3Type.Raw}, ...) into a
 * canonical string by walking its fields with reflection.  The same source file is compiled against
 * the old ANTLR 3 parser to record golden fixtures and against the new ANTLR 4 parser in the
 * differential test, so any behavior change shows up as a text diff.</p>
 *
 * <p>The walk is deterministic: object fields are sorted by name, {@link Map} entries and non-list
 * {@link Collection}s are sorted by their dumped element, and lists keep their order.  Only the
 * parser output shape matters, so the dump uses simple class names and never calls user
 * {@code toString()} on domain objects.</p>
 */
public final class CqlParseTreeDump
{
    private CqlParseTreeDump()
    {
    }

    public static String dump(Object root)
    {
        StringBuilder sb = new StringBuilder();
        dump(root, sb, Collections.newSetFromMap(new IdentityHashMap<>()));
        return sb.toString();
    }

    private static void dump(Object o, StringBuilder sb, Set<Object> path)
    {
        if (o == null)
        {
            sb.append("null");
            return;
        }

        Class<?> c = o.getClass();

        if (o instanceof CharSequence)
        {
            sb.append('"').append(o).append('"');
            return;
        }
        if (o instanceof Number || o instanceof Boolean || o instanceof Character)
        {
            sb.append(o);
            return;
        }
        if (o instanceof Enum)
        {
            // getDeclaringClass() is stable even for enum constants with bodies (whose getClass()
            // is a synthetic anonymous subclass).
            Enum<?> e = (Enum<?>) o;
            sb.append(e.getDeclaringClass().getSimpleName()).append('.').append(e.name());
            return;
        }
        // A marshal type is machinery the parser attaches, not raw parser output.  Do not reflect its
        // internal comparator graph (it explodes and carries per-JVM state).  Render the type name plus
        // its type parameters, recursively, and the frozen/multicell flag for collections and UDTs, so
        // list<int> and set<int>, and frozen<set<int>> and set<int>, dump differently.
        if (o instanceof AbstractType)
        {
            dumpMarshalType((AbstractType<?>) o, sb);
            return;
        }
        // Terms.Raw is an abstract class with anonymous implementations (empty simple name), so the
        // lambda guard below would collapse every WHERE/IN/UPDATE/DELETE term to "<lambda>" and make
        // id = 1 and id = 999 indistinguishable.  Render the underlying Term.Raw list when the
        // implementation exposes it; otherwise fall back to the stable getText() form.
        if (o instanceof Terms.Raw)
        {
            Terms.Raw terms = (Terms.Raw) o;
            List<? extends Term.Raw> list = null;
            try
            {
                list = terms.asList();
            }
            catch (UnsupportedOperationException e)
            {
                // asList() is not supported (for example a whole-list bind marker); use getText().
            }
            if (list != null)
            {
                sb.append("Terms.Raw");
                dumpCollection(list, true, sb, path);
            }
            else
            {
                sb.append("Terms.Raw{text=\"").append(terms.getText()).append("\"}");
            }
            return;
        }
        // Lambdas and other synthetic/anonymous classes carry per-JVM identity; do not reflect them.
        if (c.isSynthetic() || c.getSimpleName().isEmpty() || c.getName().contains("$$Lambda"))
        {
            sb.append("<lambda>");
            return;
        }
        if (o instanceof ByteBuffer)
        {
            ByteBuffer b = ((ByteBuffer) o).duplicate();
            StringBuilder hex = new StringBuilder("0x");
            while (b.hasRemaining())
                hex.append(String.format("%02x", b.get()));
            sb.append(hex);
            return;
        }
        if (c.isArray())
        {
            dumpCollection(asList(o), false, sb, path);
            return;
        }
        if (o instanceof Map)
        {
            List<String> entries = new ArrayList<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet())
            {
                StringBuilder es = new StringBuilder();
                dump(e.getKey(), es, path);
                es.append('=');
                dump(e.getValue(), es, path);
                entries.add(es.toString());
            }
            Collections.sort(entries);
            sb.append('{').append(String.join(", ", entries)).append('}');
            return;
        }
        if (o instanceof Collection)
        {
            boolean ordered = o instanceof List;
            dumpCollection((Collection<?>) o, ordered, sb, path);
            return;
        }

        // A plain domain object: guard against cycles, then dump its fields by name.
        if (!path.add(o))
        {
            sb.append('<').append(c.getSimpleName()).append(" cycle>");
            return;
        }
        try
        {
            List<Field> fields = new ArrayList<>();
            for (Class<?> k = c; k != null && k != Object.class; k = k.getSuperclass())
                for (Field f : k.getDeclaredFields())
                    if (!Modifier.isStatic(f.getModifiers()) && !f.isSynthetic())
                        fields.add(f);
            fields.sort((a, b) -> a.getName().compareTo(b.getName()));

            sb.append(c.getSimpleName()).append('{');
            boolean first = true;
            for (Field f : fields)
            {
                if (!first)
                    sb.append(", ");
                first = false;
                sb.append(f.getName()).append('=');
                try
                {
                    f.setAccessible(true);
                    Object v = f.get(o);
                    // A memoized hashCode cache (an int field equal to the enclosing object's own
                    // hashCode) is not parser output.  It is derived from fields already in the dump,
                    // and when it folds an enum's identity hashCode (for example
                    // RoleResource.hash = Objects.hashCode(level, name)) it differs between JVMs.
                    // Render a stable marker so it never spuriously fails the differential.
                    if (f.getType() == int.class && isMemoizedHash(o, ((Integer) v).intValue()))
                        sb.append("<hashCode>");
                    else
                        dump(v, sb, path);
                }
                catch (Throwable t)
                {
                    sb.append("<unreadable:").append(t.getClass().getSimpleName()).append('>');
                }
            }
            sb.append('}');
        }
        finally
        {
            path.remove(o);
        }
    }

    private static void dumpCollection(Collection<?> coll, boolean ordered, StringBuilder sb, Set<Object> path)
    {
        List<String> elems = new ArrayList<>();
        for (Object e : coll)
        {
            StringBuilder es = new StringBuilder();
            dump(e, es, path);
            elems.add(es.toString());
        }
        if (!ordered)
            Collections.sort(elems);
        sb.append('[').append(String.join(", ", elems)).append(']');
    }

    private static void dumpMarshalType(AbstractType<?> t, StringBuilder sb)
    {
        sb.append("<type:").append(t.getClass().getSimpleName());
        // The frozen/multicell distinction is a real parser-visible difference for collections and
        // user-defined types; ReversedType, TupleType and VectorType already differ by class name.
        if (t.isCollection() || t.isUDT())
            sb.append(t.isMultiCell() ? "{multicell}" : "{frozen}");
        List<AbstractType<?>> sub = t.subTypes();
        if (sub != null && !sub.isEmpty())
        {
            sb.append('[');
            for (int i = 0; i < sub.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                dumpMarshalType(sub.get(i), sb);
            }
            sb.append(']');
        }
        sb.append('>');
    }

    private static boolean isMemoizedHash(Object owner, int value)
    {
        // A domain object with no hashCode override returns its identity hash, which no meaningful
        // parser int field ever equals; an object that memoizes hashCode returns exactly that field.
        try
        {
            return owner.hashCode() == value;
        }
        catch (Throwable t)
        {
            return false;
        }
    }

    private static List<Object> asList(Object array)
    {
        int n = java.lang.reflect.Array.getLength(array);
        List<Object> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
            out.add(java.lang.reflect.Array.get(array, i));
        return out;
    }
}
