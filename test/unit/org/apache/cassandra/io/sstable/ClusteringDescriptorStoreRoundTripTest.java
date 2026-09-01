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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.BufferClusteringBoundary;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link UnfilteredDescriptor#storeRowClustering} and {@link UnfilteredDescriptor#storeMarker}
 * are the memtable-source (write) counterparts of {@link ClusteringDescriptor#loadClustering} —
 * they must serialize into a byte layout that {@link ClusteringDescriptor#toClusteringPrefix}
 * (the same decode path {@link SSTableCursorReader} relies on) reads back losslessly. Pinned
 * here in isolation, before any of it is exercised through a full memtable flush.
 */
public class ClusteringDescriptorStoreRoundTripTest
{
    private static final AbstractType<?>[] TYPES = { Int32Type.instance, UTF8Type.instance };
    private static final List<AbstractType<?>> TYPES_LIST = Arrays.asList(TYPES);

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static void assertPrefixEquals(ClusteringPrefix<?> expected, ClusteringPrefix<?> actual)
    {
        assertEquals(expected.kind(), actual.kind());
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.getBufferArray(), actual.getBufferArray());
    }

    @Test
    public void rowClusteringRoundTripsMultiColumn()
    {
        UnfilteredDescriptor descriptor = new UnfilteredDescriptor(TYPES);
        Clustering<ByteBuffer> clustering = Clustering.make(ByteBufferUtil.bytes(42), ByteBufferUtil.bytes("hello"));

        descriptor.storeRowClustering(clustering);

        assertEquals((byte) ClusteringPrefix.Kind.CLUSTERING.ordinal(), descriptor.clusteringKindEncoded());
        assertPrefixEquals(clustering, descriptor.toClusteringPrefix(TYPES_LIST));
    }

    @Test
    public void rowClusteringRoundTripsEmpty()
    {
        UnfilteredDescriptor descriptor = new UnfilteredDescriptor(new AbstractType<?>[0]);
        Clustering<ByteBuffer> clustering = Clustering.EMPTY;

        descriptor.storeRowClustering(clustering);

        assertEquals(0, descriptor.clusteringLength());
    }

    @Test
    public void inclStartBoundRoundTrips()
    {
        UnfilteredDescriptor descriptor = new UnfilteredDescriptor(TYPES);
        BufferClusteringBound bound = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_START_BOUND,
                                                                 new ByteBuffer[]{ ByteBufferUtil.bytes(7) });
        DeletionTime deletion = DeletionTime.build(1000L, 100);

        descriptor.storeMarker(bound.kind(), bound, deletion, DeletionTime.LIVE);

        assertEquals((byte) ClusteringPrefix.Kind.INCL_START_BOUND.ordinal(), descriptor.clusteringKindEncoded());
        assertEquals(1, descriptor.clusteringColumnsBound());
        assertPrefixEquals(bound, descriptor.toClusteringPrefix(TYPES_LIST));
        assertEquals(deletion.markedForDeleteAt(), descriptor.deletionTime().markedForDeleteAt());
        assertEquals(deletion.localDeletionTime(), descriptor.deletionTime().localDeletionTime());
    }

    @Test
    public void exclEndBoundRoundTripsZeroValues()
    {
        UnfilteredDescriptor descriptor = new UnfilteredDescriptor(TYPES);
        BufferClusteringBound bound = new BufferClusteringBound(ClusteringPrefix.Kind.EXCL_END_BOUND, new ByteBuffer[0]);
        DeletionTime deletion = DeletionTime.build(2000L, 200);

        descriptor.storeMarker(bound.kind(), bound, deletion, DeletionTime.LIVE);

        assertEquals((byte) ClusteringPrefix.Kind.EXCL_END_BOUND.ordinal(), descriptor.clusteringKindEncoded());
        assertEquals(0, descriptor.clusteringColumnsBound());
        assertEquals(0, descriptor.clusteringLength());
    }

    @Test
    public void boundaryRoundTripsBothDeletionTimes()
    {
        UnfilteredDescriptor descriptor = new UnfilteredDescriptor(TYPES);
        BufferClusteringBoundary boundary = new BufferClusteringBoundary(ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY,
                                                                          new ByteBuffer[]{ ByteBufferUtil.bytes(5), ByteBufferUtil.bytes("mid") });
        DeletionTime close = DeletionTime.build(10L, 10);
        DeletionTime open = DeletionTime.build(20L, 20);

        descriptor.storeMarker(boundary.kind(), boundary, close, open);

        assertTrue(ClusteringPrefix.Kind.fromOrdinal(descriptor.clusteringKindEncoded()).isBoundary());
        assertPrefixEquals(boundary, descriptor.toClusteringPrefix(TYPES_LIST));
        assertEquals(close.markedForDeleteAt(), descriptor.deletionTime().markedForDeleteAt());
        assertEquals(close.localDeletionTime(), descriptor.deletionTime().localDeletionTime());
        assertEquals(open.markedForDeleteAt(), descriptor.deletionTime2().markedForDeleteAt());
        assertEquals(open.localDeletionTime(), descriptor.deletionTime2().localDeletionTime());
    }

    @Test
    public void nonBoundaryLeavesDeletionTime2Live()
    {
        UnfilteredDescriptor descriptor = new UnfilteredDescriptor(TYPES);
        BufferClusteringBound bound = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_END_BOUND,
                                                                 new ByteBuffer[]{ ByteBufferUtil.bytes(1) });

        descriptor.storeMarker(bound.kind(), bound, DeletionTime.build(1L, 1), DeletionTime.LIVE);

        assertTrue(descriptor.deletionTime2().isLive());
    }

    @Test
    public void kindCanBeOverriddenIndependentlyOfValuesSource()
    {
        // The RangeTombstoneList-driven marker adapter reuses one side's already-live
        // ClusteringBound as the values source for a merged boundary, passing the boundary
        // kind separately (mirroring ClusteringBoundary.create) rather than allocating a new
        // ClusteringBoundary wrapper. storeMarker must honor the passed-in kind, not
        // valuesSource.kind().
        UnfilteredDescriptor descriptor = new UnfilteredDescriptor(TYPES);
        BufferClusteringBound endBound = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_END_BOUND,
                                                                    new ByteBuffer[]{ ByteBufferUtil.bytes(9), ByteBufferUtil.bytes("z") });

        descriptor.storeMarker(ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY, endBound,
                               DeletionTime.build(5L, 5), DeletionTime.build(6L, 6));

        assertEquals((byte) ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY.ordinal(), descriptor.clusteringKindEncoded());
        ClusteringPrefix<?> decoded = descriptor.toClusteringPrefix(TYPES_LIST);
        assertEquals(endBound.size(), decoded.size());
        assertArrayEquals(endBound.getBufferArray(), decoded.getBufferArray());
        assertTrue(descriptor.deletionTime2().equals(DeletionTime.build(6L, 6)));
    }

    @Test
    public void repeatedStoresReuseScratchCorrectly()
    {
        // storeRowClustering/storeMarker reuse a grow-only scratch buffer; a longer write
        // followed by a shorter one must not leak trailing bytes from the previous call.
        UnfilteredDescriptor descriptor = new UnfilteredDescriptor(TYPES);
        Clustering<ByteBuffer> longer = Clustering.make(ByteBufferUtil.bytes(1), ByteBufferUtil.bytes("a much longer string value"));
        Clustering<ByteBuffer> shorter = Clustering.make(ByteBufferUtil.bytes(2), ByteBufferUtil.bytes("x"));

        descriptor.storeRowClustering(longer);
        descriptor.storeRowClustering(shorter);

        assertPrefixEquals(shorter, descriptor.toClusteringPrefix(TYPES_LIST));
    }
}
