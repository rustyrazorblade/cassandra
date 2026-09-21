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

import org.apache.cassandra.cql3.selection.Selectable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Guards the collection-slice grammar action against the null-bound regression.  A slice can leave
 * one bound absent: {@code data[x..]} has no upper bound and {@code data[..y]} has no lower bound.
 * The grammar action must read the bound context only when it is present; otherwise it hits a null
 * context and throws.  The absent bound must resolve to a null {@link org.apache.cassandra.cql3.Term.Raw},
 * which the prepare step later turns into UNSET.
 *
 * The parse must produce a {@link Selectable.WithSliceSelection.Raw}.  Its {@code toString} renders
 * an absent bound as an empty segment, so {@code data[x..]} ends with {@code [x..]} and
 * {@code data[..y]} ends with {@code [..y]}.  The bracket suffix is the public signal that the
 * absent bound is null, not a stray zero or the other bound.
 */
public class CollectionSliceSelectionTest
{
    private static Selectable.Raw parseSelector(String cql)
    {
        return CQLFragmentParser.parseAny(p -> p.unaliasedSelector().s, cql, "selector");
    }

    @Test
    public void sliceWithAbsentUpperBoundParses()
    {
        Selectable.Raw raw = parseSelector("data[1..]");
        assertTrue("expected a WithSliceSelection.Raw, got " + raw.getClass().getName(),
                   raw instanceof Selectable.WithSliceSelection.Raw);
        // Empty segment after ".." means the upper bound is null (UNSET at prepare time).
        assertTrue("expected absent upper bound, got " + raw, raw.toString().endsWith("[1..]"));
    }

    @Test
    public void sliceWithAbsentLowerBoundParses()
    {
        Selectable.Raw raw = parseSelector("data[..3]");
        assertTrue("expected a WithSliceSelection.Raw, got " + raw.getClass().getName(),
                   raw instanceof Selectable.WithSliceSelection.Raw);
        // Empty segment before ".." means the lower bound is null (UNSET at prepare time).
        assertTrue("expected absent lower bound, got " + raw, raw.toString().endsWith("[..3]"));
    }

    @Test
    public void sliceWithBothBoundsParses()
    {
        Selectable.Raw raw = parseSelector("data[1..3]");
        assertTrue("expected a WithSliceSelection.Raw, got " + raw.getClass().getName(),
                   raw instanceof Selectable.WithSliceSelection.Raw);
        assertTrue("expected both bounds present, got " + raw, raw.toString().endsWith("[1..3]"));
    }

    @Test
    public void singleElementIsNotASlice()
    {
        // A single subscript is element selection, not a slice; guards against mislabelling.
        Selectable.Raw raw = parseSelector("data[1]");
        assertEquals(Selectable.WithElementSelection.Raw.class, raw.getClass());
    }
}
