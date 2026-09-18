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

package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertArrayEquals;

public class ComplexColumnDataTest
{
    private static final String KEYSPACE = "complex_column_data_test";
    private static final String TABLE = "kcvm";
    private static final TableMetadata metadata;
    private static final ColumnMetadata m;

    static
    {
        DatabaseDescriptor.daemonInitialization();
        metadata =
            TableMetadata.builder(KEYSPACE, TABLE)
                         .addPartitionKeyColumn("k", IntegerType.instance)
                         .addClusteringColumn("c", IntegerType.instance)
                         .addRegularColumn("m", MapType.getInstance(IntegerType.instance, IntegerType.instance, true))
                         .build();

        m = metadata.getColumn(new ColumnIdentifier("m", false));
    }

    private static final ByteBuffer BB1 = ByteBufferUtil.bytes(1);
    private static final ByteBuffer BB2 = ByteBufferUtil.bytes(2);
    private static final ByteBuffer BB3 = ByteBufferUtil.bytes(3);

    /**
     * The production digest walks the cells with BTree.apply.  The reference walk uses the cell
     * iterator directly.  Both must produce the same bytes, or a rolling upgrade would see spurious
     * digest and merkle mismatches.
     */
    @Test
    public void digestMatchesPerCellWalk()
    {
        DeletionTime complexDeletion = DeletionTime.build(1000L, 2000);
        ComplexColumnData.Builder builder = ComplexColumnData.builder();
        builder.newColumn(m);
        builder.addComplexDeletion(complexDeletion);
        builder.addCell(BufferCell.live(m, 100L, BB1, CellPath.create(BB1)));
        builder.addCell(BufferCell.live(m, 100L, BB2, CellPath.create(BB2)));
        builder.addCell(BufferCell.live(m, 100L, BB3, CellPath.create(BB3)));
        ComplexColumnData data = builder.build();

        assertDigestsMatch(data);
    }

    /**
     * The leaf path is a single cell (no complex deletion).  The two walks must still match.
     */
    @Test
    public void digestMatchesPerCellWalkSingleCell()
    {
        ComplexColumnData.Builder builder = ComplexColumnData.builder();
        builder.newColumn(m);
        builder.addCell(BufferCell.live(m, 100L, BB1, CellPath.create(BB1)));
        ComplexColumnData data = builder.build();

        assertDigestsMatch(data);
    }

    /**
     * Digest the data with the production path and with a manual reference walk, then compare.
     */
    private static void assertDigestsMatch(ComplexColumnData data)
    {
        Digest production = Digest.forReadResponse();
        data.digest(production);

        Digest reference = Digest.forReadResponse();
        if (!data.complexDeletion().isLive())
            data.complexDeletion().digest(reference);
        for (Cell<?> cell : data)
            cell.digest(reference);

        assertArrayEquals(reference.digest(), production.digest());
    }
}
