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

package org.apache.cassandra.db.cursorreads;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/**
 * A read of a virtual table that reaches the replica response path, as a read of a virtual table
 * arriving over internode messaging does (VirtualTableFromInternodeTest).  A virtual table has no
 * memtables or sstables, so the cursor replica response must not serve it; the iterator path
 * answers it from the table's own data.  The cursor path once looked the table's store up and
 * failed the read with "Unknown keyspace".
 */
public class CursorReadVirtualTableTest extends CQLTester
{
    private static final String KS_NAME = "cursor_reads_vts";

    private static final class KeyValueTable extends AbstractVirtualTable
    {
        KeyValueTable()
        {
            super(TableMetadata.builder(KS_NAME, "kv")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn("pk", UTF8Type.instance)
                               .addClusteringColumn("ck", Int32Type.instance)
                               .addRegularColumn("v", UTF8Type.instance)
                               .build());
        }

        @Override
        public SimpleDataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            for (int ck = 0; ck < 3; ck++)
                result.row("a", ck).column("v", "value " + ck);
            return result;
        }
    }

    private static final KeyValueTable TABLE = new KeyValueTable();

    @BeforeClass
    public static void registerVirtualTable()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(TABLE)));
    }

    @Test
    public void replicaResponseOfAVirtualTableRead()
    {
        TableMetadata metadata = TABLE.metadata();
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(metadata, FBUtilities.nowInSeconds(), ColumnFilter.all(metadata),
                                                                               RowFilter.none(), DataLimits.NONE,
                                                                               metadata.partitioner.decorateKey(ByteBufferUtil.bytes("a")),
                                                                               new ClusteringIndexSliceFilter(Slices.ALL, false));
        boolean cursorReads = DatabaseDescriptor.cursorReadsEnabled();
        try
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
            List<String> iterator = responseRows(command);
            assertEquals(iterator.toString(), 3, iterator.size());
            DatabaseDescriptor.setCursorReadsEnabled(true);
            assertEquals(iterator, responseRows(command));
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(cursorReads);
        }
    }

    /** The rows of the replica response to {@code command}, as text, without timestamps. */
    private static List<String> responseRows(SinglePartitionReadCommand command)
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
        List<String> rows = new ArrayList<>();
        try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        // without timestamps: a virtual table gives each read its own
                        rows.add(partition.next().toString(command.metadata(), false));
                }
            }
        }
        return rows;
    }
}
