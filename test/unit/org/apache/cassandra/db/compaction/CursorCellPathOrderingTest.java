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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * The cursor's complex-cell path order must match the reference
 * {@link ColumnMetadata#cellPathComparator()} for every complex type: that comparator
 * dictates both the on-disk cell order written by flush and the iterator's merge grouping,
 * so any divergence mis-groups same-path cells across sources during a cursor merge.
 *
 * The trap pinned here: UDT cell paths are 2-byte field indexes compared by
 * {@link UserType#nameComparator()} == ShortType, whose first-byte comparison is SIGNED —
 * raw unsigned bytewise order diverges exactly at field index 32768 (0x8000). Unreachable
 * below 32769-field UDTs, but the orders must match by construction, not by schema-size
 * luck.
 */
public class CursorCellPathOrderingTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static void assertOrderMatchesReference(ColumnMetadata column, ByteBuffer p1, ByteBuffer p2)
    {
        int reference = Integer.signum(column.cellPathComparator().compare(CellPath.create(p1), CellPath.create(p2)));
        int cursor = Integer.signum(CursorCompactor.comparePaths(column, p1.duplicate(), p2.duplicate()));
        assertEquals("cursor path order diverges from ColumnMetadata.cellPathComparator for " + column.type +
                     " on paths " + ByteBufferUtil.bytesToHex(p1) + " / " + ByteBufferUtil.bytesToHex(p2),
                     reference, cursor);
        // antisymmetry, both directions
        int referenceRev = Integer.signum(column.cellPathComparator().compare(CellPath.create(p2), CellPath.create(p1)));
        int cursorRev = Integer.signum(CursorCompactor.comparePaths(column, p2.duplicate(), p1.duplicate()));
        assertEquals(referenceRev, cursorRev);
    }

    @Test
    public void udtFieldIndexBoundary()
    {
        UserType udt = new UserType("ks", ByteBufferUtil.bytes("t"),
                                    Arrays.asList(new FieldIdentifier(ByteBufferUtil.bytes("f1")),
                                                  new FieldIdentifier(ByteBufferUtil.bytes("f2"))),
                                    Arrays.asList(UTF8Type.instance, UTF8Type.instance),
                                    true);
        ColumnMetadata column = ColumnMetadata.regularColumn("ks", "cf", "u", udt, 0);

        // the signed/unsigned boundary: 0x7FFF (32767) vs 0x8000 (32768 as unsigned,
        // -32768 as ShortType) — reference sorts 0x8000 FIRST
        assertOrderMatchesReference(column, ByteBufferUtil.bytes((short) 0x7FFF), ByteBufferUtil.bytes((short) 0x8000));
        // ordinary low indexes, both orders
        assertOrderMatchesReference(column, ByteBufferUtil.bytes((short) 0), ByteBufferUtil.bytes((short) 1));
        // equal paths
        assertOrderMatchesReference(column, ByteBufferUtil.bytes((short) 7), ByteBufferUtil.bytes((short) 7));
    }

    @Test
    public void collectionPathsStayTypeRouted()
    {
        AbstractType<?> mapType = MapType.getInstance(Int32Type.instance, UTF8Type.instance, true);
        ColumnMetadata column = ColumnMetadata.regularColumn("ks", "cf", "m", mapType, 0);

        // Int32 keys: signed compare, bytewise diverges for negative vs positive
        assertOrderMatchesReference(column, Int32Type.instance.decompose(-3), Int32Type.instance.decompose(5));
        assertOrderMatchesReference(column, Int32Type.instance.decompose(1), Int32Type.instance.decompose(2));
    }
}
