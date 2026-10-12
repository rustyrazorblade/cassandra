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

package org.apache.cassandra.repair;

import java.util.Objects;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class RepairJobDescTest extends AbstractRepairTest
{
    /**
     * hashCode caches its result on first use, so it must equal the plain field hash and stay the same
     * across repeated calls. RepairJobDesc is used as a map key, so a wrong or unstable value breaks lookups.
     */
    @Test
    public void hashCodeMatchesFieldsAndIsStable()
    {
        TimeUUID parentSessionId = nextTimeUUID();
        TimeUUID sessionId = nextTimeUUID();
        RepairJobDesc desc = new RepairJobDesc(parentSessionId, sessionId, "ks", "cf",
                                               Sets.newHashSet(RANGE1, RANGE2));

        int expected = Objects.hash(parentSessionId, sessionId, "ks", "cf", Sets.newHashSet(RANGE1, RANGE2));
        Assert.assertEquals(expected, desc.hashCode());
        Assert.assertEquals(desc.hashCode(), desc.hashCode());
    }
}
