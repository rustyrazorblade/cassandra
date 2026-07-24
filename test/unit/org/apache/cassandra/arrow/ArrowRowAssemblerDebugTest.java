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

package org.apache.cassandra.arrow;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

import org.junit.Test;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrowRowAssemblerDebugTest
{
    @Test
    public void listColumnSerializesCleanlyOverIpc() throws Exception
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("pk", Int32Type.instance)
                                            .addClusteringColumn("ck", Int32Type.instance)
                                            .addRegularColumn("v", UTF8Type.instance)
                                            .addRegularColumn("l", ListType.getInstance(Int32Type.instance, true))
                                            .offline()
                                            .build();

        try (RootAllocator allocator = new RootAllocator())
        {
            VectorSchemaRoot[] captured = new VectorSchemaRoot[1];
            try (ArrowRowAssembler assembler = new ArrowRowAssembler(table, allocator, 16L * 1024 * 1024, root -> captured[0] = root))
            {
                assembler.startPartition(Int32Type.instance.decompose(1));
                assembler.startRow(false, org.apache.cassandra.db.Clustering.make(Int32Type.instance.decompose(1)));
                assembler.putSimpleCell(table.getColumn(org.apache.cassandra.cql3.ColumnIdentifier.getInterned("v", false)), ByteBufferUtil.bytes("row-1"));
                org.apache.cassandra.schema.ColumnMetadata lColumn = table.getColumn(org.apache.cassandra.cql3.ColumnIdentifier.getInterned("l", false));
                assembler.beginComplexColumn(lColumn);
                assembler.putComplexCell(lColumn, null, Int32Type.instance.decompose(1));
                assembler.putComplexCell(lColumn, null, Int32Type.instance.decompose(2));
                assembler.putComplexCell(lColumn, null, Int32Type.instance.decompose(3));
                assembler.endComplexColumn(lColumn);
                assembler.endRow();
                assembler.endPartition();
            }

            try (VectorSchemaRoot root = captured[0])
            {
                assertThat(root).isNotNull();
                assertThat(root.getRowCount()).isEqualTo(1);

                // Exercise the exact serialization path Arrow Flight uses (ArrowMessage/IPC
                // encoding), without any gRPC/networking involved, to isolate row-assembly bugs
                // from Flight-layer ones.
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out)))
                {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                assertThat(out.size()).isGreaterThan(0);
            }
        }
    }
}
