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
package org.apache.cassandra.auth;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.CassandraDaemon;

import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_SUBQUERY_ENABLED;

/**
 * Proves the inner-statement authorization loop for IN-subqueries.  An ordinary user with SELECT on
 * the outer table, but NOT on the inner table, must be rejected: the subquery reads the inner table,
 * so the inner statement is authorized as well as the outer.  Research POC.
 */
public class SelectSubqueryAuthTest extends CQLTester
{
    private static final String user = "subquery_user";
    private static final String pass = "12345";

    @BeforeClass
    public static void setUpAuth()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setRolesValidity(0);
        // A valid placement is needed so the distributed metadata tables are queryable under auth.
        ClusterMetadataTestHelper.reconfigureCms(ReplicationParams.ntsMeta(Collections.singletonMap(DatabaseDescriptor.getLocalDataCenter(), 1)));
        ServerTestUtils.markCMS();
        requireAuthentication();
        requireNetwork();
        CassandraDaemon.getInstanceForTesting().setupVirtualKeyspaces();
    }

    @Before
    public void enableSubqueries()
    {
        CQL_SUBQUERY_ENABLED.setBoolean(true);
    }

    @After
    public void tearDown() throws Throwable
    {
        CQL_SUBQUERY_ENABLED.reset();
        useSuperUser();
        executeNet("DROP ROLE IF EXISTS " + user);
    }

    @Test
    public void testSubqueryRejectedWithoutInnerSelect() throws Throwable
    {
        useSuperUser();

        String inner = KEYSPACE_PER_TEST + '.' + createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (k int PRIMARY KEY)");
        String outer = KEYSPACE_PER_TEST + '.' + createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int PRIMARY KEY, v int)");

        executeNet(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));
        // Grant SELECT on the OUTER table only.  The inner table is deliberately not granted.
        executeNet("GRANT SELECT ON TABLE " + outer + " TO " + user);

        useUser(user, pass);

        // The subquery must be rejected because the user cannot SELECT the inner table.
        assertUnauthorizedQuery("User " + user + " has no SELECT permission on <table " + inner + "> or any of its parents",
                                "SELECT pk, v FROM " + outer + " WHERE pk IN (SELECT k FROM " + inner + ")");
    }
}
