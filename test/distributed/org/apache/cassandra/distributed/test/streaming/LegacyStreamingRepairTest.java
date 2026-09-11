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
package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The legacy streaming path through a real two-node cluster, rather than through a writer and reader
 * called directly.
 *
 * A repair moves the data, and the test checks the receiving node ends up with exactly what the sender
 * holds. Compressed and uncompressed tables run separately, because each uses a different writer and reader.
 */
public class LegacyStreamingRepairTest extends TestBaseImpl
{
    private static final int ROWS = 500;
    private static final int VALUE_SIZE = 4096;

    @Test
    public void repairsCompressedTable() throws IOException
    {
        repairs("compressed", "{ 'class' : 'LZ4Compressor' }");
    }

    @Test
    public void repairsUncompressedTable() throws IOException
    {
        repairs("uncompressed", "{ 'enabled' : false }");
    }

    private void repairs(String table, String compression) throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(Feature.values())
                                                             // force the legacy path: no whole-SSTable transfers
                                                             .set("stream_entire_sstables", false))
                                           .start()))
        {
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (id int PRIMARY KEY, val blob) WITH compression = %s",
                                               KEYSPACE, table, compression));
            cluster.stream().forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE).asserts().success());

            IInvokableInstance first = cluster.get(1);
            IInvokableInstance second = cluster.get(2);

            // written on one node only, so the repair has something to stream
            Random random = new Random(0);
            byte[] value = new byte[VALUE_SIZE];
            for (int i = 0; i < ROWS; i++)
            {
                random.nextBytes(value);
                first.executeInternal(String.format("INSERT INTO %s.%s (id, val) VALUES (?, ?)", KEYSPACE, table),
                                      i, ByteBuffer.wrap(value.clone()));
            }
            first.flush(KEYSPACE);

            Object[][] before = rows(second, table);
            assertThat(before.length).describedAs("the second node should be missing the data before the repair")
                                     .isLessThan(ROWS);

            second.nodetoolResult("repair", "--full", KEYSPACE).asserts().success();

            assertThat(rows(second, table)).describedAs("after the repair the second node should hold exactly what the first does")
                                           .isEqualTo(rows(first, table));
            assertThat(rows(second, table).length).describedAs("every row should have been streamed")
                                                  .isEqualTo(ROWS);
        }
    }

    /** Every row as this node holds it locally, ordered so two nodes can be compared directly. */
    private static Object[][] rows(IInvokableInstance instance, String table)
    {
        Object[][] rows = instance.executeInternal(String.format("SELECT id, val FROM %s.%s", KEYSPACE, table));
        Arrays.sort(rows, Comparator.comparingInt(row -> (Integer) row[0]));
        return rows;
    }
}
