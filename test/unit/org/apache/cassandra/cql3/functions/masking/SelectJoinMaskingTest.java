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

package org.apache.cassandra.cql3.functions.masking;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static java.lang.String.format;

import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_JOIN_ENABLED;

/**
 * Proves the broadcast hash JOIN honours dynamic data masking on the build side.
 *
 * <p>The build side of a JOIN is read at the coordinator with the same {@code SELECT *} the probe uses.
 * The security concern was that this read hardcoded unmask, so a caller without the UNMASK permission
 * could read the build table's masked columns in cleartext through a JOIN.  This test drives a role that
 * has SELECT but not UNMASK on the build table: its JOIN must return the build table's masked NON-key
 * column MASKED.  The same role, once granted UNMASK, must get the column in cleartext.</p>
 *
 * <p>The DEFAULT mask on an int returns 0, so a masked value of 42 reads back as 0.</p>
 */
public class SelectJoinMaskingTest extends CQLTester
{
    private static final String USER = "join_ddm_user";
    private static final String PASSWORD = "join_ddm_password";

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setRolesValidity(0);
        requireAuthentication();
        requireNetwork();
    }

    @Before
    public void before() throws Throwable
    {
        CQL_JOIN_ENABLED.setBoolean(true);
        useSuperUser();
        executeNet(format("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s'", USER, PASSWORD));
        executeNet(format("GRANT SELECT ON ALL KEYSPACES TO %s", USER));
    }

    @After
    public void after() throws Throwable
    {
        useSuperUser();
        executeNet("DROP USER IF EXISTS " + USER);
        CQL_JOIN_ENABLED.reset();
    }

    /**
     * A masked NON-key column on the build table is returned masked to a SELECT-only role, and in
     * cleartext once the role is granted UNMASK on the build table.  The join key itself is not masked.
     */
    @Test
    public void testBuildSideMaskedNonKeyColumnRespectsUnmask() throws Throwable
    {
        // Probe t1 (id, ref); build t2 (bid, secret, label) with secret MASKED.  Join on t1.ref = t2.bid.
        String t2 = createTable("CREATE TABLE %s (bid int PRIMARY KEY, secret int, label text)");
        execute("ALTER TABLE " + fqn(t2) + " ALTER secret MASKED WITH DEFAULT");
        String t1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, ref int)");

        execute("INSERT INTO " + fqn(t1) + " (id, ref) VALUES (1, 10)");
        execute("INSERT INTO " + fqn(t2) + " (bid, secret, label) VALUES (10, 42, 'x')");

        String join = "SELECT id, ref FROM " + fqn(t1) + " JOIN " + fqn(t2) + " ON " + t1 + ".ref = " + t2 + ".bid";

        // Output columns are id, ref (probe) then the build's SELECT * columns: bid, then the regular
        // columns in alphabetical order (label, secret).
        // SELECT but no UNMASK: the masked non-key build column reads back as the DEFAULT mask (0).
        useUser(USER, PASSWORD);
        assertRowsNet(executeNet(join), row(1, 10, 10, "x", 0));

        // Grant UNMASK on the build table: the same role now sees the real value (42).
        useSuperUser();
        executeNet(format("GRANT UNMASK ON ALL KEYSPACES TO %s", USER));

        useUser(USER, PASSWORD);
        assertRowsNet(executeNet(join), row(1, 10, 10, "x", 42));
    }

    private String fqn(String table)
    {
        return keyspace() + '.' + table;
    }
}
