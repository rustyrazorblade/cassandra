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
package org.apache.cassandra.cql3.validation.operations;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.ThresholdTester;

import static org.apache.cassandra.config.CassandraRelevantProperties.CQL_SUBQUERY_ENABLED;

/**
 * Proves H1: an oversized IN-subquery result fails loudly through
 * {@link Guardrails#partitionKeysInSelect}, capped at {@code failThreshold + 1}.
 *
 * <p>This test extends {@link ThresholdTester} because guardrails are excluded for internal and
 * superuser queries; the subquery must run as an ordinary user for the guardrail to apply.  The
 * lighter, flag-and-shape tests live in {@link SelectSubqueryTest}.</p>
 */
public class SelectSubqueryGuardrailTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 3;
    private static final int FAIL_THRESHOLD = 5;

    public SelectSubqueryGuardrailTest()
    {
        super(WARN_THRESHOLD,
              FAIL_THRESHOLD,
              Guardrails.partitionKeysInSelect,
              Guardrails::setPartitionKeysInSelectThreshold,
              Guardrails::getPartitionKeysInSelectWarnThreshold,
              Guardrails::getPartitionKeysInSelectFailThreshold);
    }

    @Before
    public void enableSubqueries()
    {
        CQL_SUBQUERY_ENABLED.setBoolean(true);
    }

    @After
    public void resetSubqueries()
    {
        CQL_SUBQUERY_ENABLED.reset();
    }

    @Test
    public void testOversizedInnerFailsGuardrail() throws Throwable
    {
        String innerName = createTable("CREATE TABLE %s (k int PRIMARY KEY)");
        String inner = keyspace() + '.' + innerName;
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO " + inner + " (k) VALUES (?)", i);

        // The outer table becomes the %s target for assertFails.
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, i);

        // The inner read is capped at failThreshold + 1 = 6 keys, so the guardrail aborts with a
        // deterministic count.  The message names the inner table, which is where the keys come from.
        assertFails("SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + ")",
                    String.format("Aborting query with partition keys in IN clause on table %s, " +
                                  "number of partition keys 6 exceeds fail threshold of 5.", innerName));
    }

    @Test
    public void testDuplicateInnerValuesDedupedUnderThreshold() throws Throwable
    {
        // A clustering key lets one partition value repeat across many rows.  The RAW inner rows (6)
        // exceed the fail threshold (5), but the DISTINCT partition keys (3) stay under it.  Dedup
        // happens before the guardrail, so the query must SUCCEED.  If the guardrail counted raw rows
        // instead of distinct keys, this would fail.
        String inner = keyspace() + '.' + createTable("CREATE TABLE %s (k int, c int, PRIMARY KEY (k, c))");
        for (int k = 1; k <= 3; k++)
            for (int c = 1; c <= 2; c++)
                execute("INSERT INTO " + inner + " (k, c) VALUES (?, ?)", k, c);

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        for (int i = 1; i <= 3; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, i);

        assertValid("SELECT pk, v FROM %s WHERE pk IN (SELECT k FROM " + inner + ")");
    }
}
