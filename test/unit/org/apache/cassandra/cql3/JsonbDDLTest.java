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

/**
 * DDL tests for JSONB type, verifying that it cannot be used in primary keys.
 */
public class JsonbDDLTest extends CQLTester
{
    @Test
    public void testJsonbPartitionKeyRejection()
    {
        // JSONB cannot be used as a partition key
        assertInvalidMessage("jsonb type is not supported for PRIMARY KEY column",
                           String.format("CREATE TABLE %s.%s (pk 'org.apache.cassandra.db.marshal.JsonbType', v int, PRIMARY KEY (pk))",
                                       keyspace(), createTableName()));
    }

    @Test
    public void testJsonbClusteringKeyRejection()
    {
        // JSONB cannot be used as a clustering key
        assertInvalidMessage("jsonb type is not supported for PRIMARY KEY column",
                           String.format("CREATE TABLE %s.%s (pk int, ck 'org.apache.cassandra.db.marshal.JsonbType', v int, PRIMARY KEY (pk, ck))",
                                       keyspace(), createTableName()));
    }

    @Test
    public void testJsonbCompoundPartitionKeyRejection()
    {
        // JSONB cannot be used in a compound partition key
        assertInvalidMessage("jsonb type is not supported for PRIMARY KEY column",
                           String.format("CREATE TABLE %s.%s (pk1 int, pk2 'org.apache.cassandra.db.marshal.JsonbType', v int, PRIMARY KEY ((pk1, pk2)))",
                                       keyspace(), createTableName()));
    }

    @Test
    public void testJsonbRegularColumnAllowed()
    {
        // JSONB CAN be used as a regular column
        String tableName = createTableName();
        execute(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, data 'org.apache.cassandra.db.marshal.JsonbType')",
                            keyspace(), tableName));
        // If we got here without exception, the test passed
    }
}
