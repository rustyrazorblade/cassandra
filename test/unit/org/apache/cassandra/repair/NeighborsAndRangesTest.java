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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.repair.RepairCoordinator.NeighborsAndRanges;
import static org.apache.cassandra.repair.RepairCoordinator.addRangeToNeighbors;

public class NeighborsAndRangesTest extends AbstractRepairTest
{
    /**
     * For non-forced repairs, common ranges should be passed through as-is
     */
    @Test
    public void filterCommonIncrementalRangesNotForced()
    {
        CommonRange cr = new CommonRange(PARTICIPANTS, Collections.emptySet(), ALL_RANGES);
        NeighborsAndRanges nr = new NeighborsAndRanges(false, false, PARTICIPANTS, Collections.singletonList(cr));
        List<CommonRange> expected = Lists.newArrayList(cr);
        List<CommonRange> actual = nr.filterCommonRanges(null, null).commonRanges;

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void forceFilterCommonIncrementalRanges()
    {
        CommonRange cr1 = new CommonRange(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2), Collections.emptySet(), Sets.newHashSet(RANGE1));
        CommonRange cr2 = new CommonRange(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3), Collections.emptySet(), Sets.newHashSet(RANGE3));
        CommonRange cr3 = new CommonRange(Sets.newHashSet(PARTICIPANT2, PARTICIPANT3), Collections.emptySet(), Sets.newHashSet(RANGE2));
        Set<InetAddressAndPort> liveEndpoints = Sets.newHashSet(PARTICIPANT2, PARTICIPANT3); // PARTICIPANT1 is excluded
        List<CommonRange> initial = Lists.newArrayList(cr1, cr2, cr3);
        List<CommonRange> expected = Lists.newArrayList(new CommonRange(Sets.newHashSet(PARTICIPANT2), Collections.emptySet(), Sets.newHashSet(RANGE1), true),
                                                        new CommonRange(Sets.newHashSet(PARTICIPANT2, PARTICIPANT3), Collections.emptySet(), Sets.newHashSet(RANGE3), true),
                                                        new CommonRange(Sets.newHashSet(PARTICIPANT2, PARTICIPANT3), Collections.emptySet(), Sets.newHashSet(RANGE2), false));

        NeighborsAndRanges nr = new NeighborsAndRanges(true, false, liveEndpoints, initial);
        List<CommonRange> actual = nr.filterCommonRanges(null, null).commonRanges;

        Assert.assertEquals(expected, actual);
    }

    /**
     * addRangeToNeighbors groups ranges by their endpoint sets. Ranges with the same full and transient
     * endpoints join one CommonRange; a different transient set starts a new group. The groups keep
     * first-seen order, and each group keeps its ranges in the order they were added.
     */
    @Test
    public void addRangeToNeighborsGroupsByEndpointSets()
    {
        Map<Pair<Set<InetAddressAndPort>, Set<InetAddressAndPort>>, CommonRange> groups = new LinkedHashMap<>();

        addRangeToNeighbors(groups, RANGE1, allFull(RANGE1, PARTICIPANT1, PARTICIPANT2));
        addRangeToNeighbors(groups, RANGE2, allFull(RANGE2, PARTICIPANT1, PARTICIPANT2));
        addRangeToNeighbors(groups, RANGE3, fullPlusTransient(RANGE3, PARTICIPANT1, PARTICIPANT2));

        List<CommonRange> commonRanges = new ArrayList<>(groups.values());
        Assert.assertEquals(2, commonRanges.size());

        CommonRange full = commonRanges.get(0);
        Assert.assertEquals(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2), full.endpoints);
        Assert.assertEquals(Collections.emptySet(), full.transEndpoints);
        Assert.assertEquals(Lists.newArrayList(RANGE1, RANGE2), new ArrayList<>(full.ranges));

        CommonRange transient2 = commonRanges.get(1);
        Assert.assertEquals(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2), transient2.endpoints);
        Assert.assertEquals(Sets.newHashSet(PARTICIPANT2), transient2.transEndpoints);
        Assert.assertEquals(Lists.newArrayList(RANGE3), new ArrayList<>(transient2.ranges));
    }

    private static EndpointsForRange allFull(Range<Token> range, InetAddressAndPort... endpoints)
    {
        EndpointsForRange.Builder builder = EndpointsForRange.builder(range, endpoints.length);
        for (InetAddressAndPort endpoint : endpoints)
            builder.add(Replica.fullReplica(endpoint, range));
        return builder.build();
    }

    // The last endpoint is transient; the rest are full.
    private static EndpointsForRange fullPlusTransient(Range<Token> range, InetAddressAndPort... endpoints)
    {
        EndpointsForRange.Builder builder = EndpointsForRange.builder(range, endpoints.length);
        for (int i = 0; i < endpoints.length; i++)
        {
            boolean last = i == endpoints.length - 1;
            builder.add(last ? Replica.transientReplica(endpoints[i], range) : Replica.fullReplica(endpoints[i], range));
        }
        return builder.build();
    }
}
