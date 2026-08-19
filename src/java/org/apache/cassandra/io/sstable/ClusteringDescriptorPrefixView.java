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

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * A reusable, allocation-free {@link ClusteringPrefix} view over a {@link ClusteringDescriptor}'s
 * raw serialized clustering bytes. Component boundaries are parsed on {@link #reset} from the
 * descriptor's buffer (same wire format the cursor reader stores: per-32-component block
 * header vints — bit 2i = empty, bit 2i+1 = null — then fixed-width raw bytes or vint-length
 * prefixed bytes per present component).
 *
 * ONLY the surface used by {@link org.apache.cassandra.db.ClusteringComparator#asByteComparable}
 * is supported: kind/size/get/accessor. get(i) returns a single reusable window re-positioned
 * per call, so components must be consumed one at a time (as the byte-comparable encoder does);
 * use two distinct views to compare two prefixes.
 */
public class ClusteringDescriptorPrefixView implements ClusteringPrefix<ByteBuffer>
{
    private final AbstractType<?>[] types;
    private int size;
    private ClusteringPrefix.Kind kind;
    private int[] offsets = new int[8];
    private int[] lengths = new int[8]; // -1 = null component, 0 = empty
    private byte[] backing;
    private ByteBuffer window;

    public ClusteringDescriptorPrefixView(AbstractType<?>[] types)
    {
        this.types = types;
    }

    /**
     * Snapshot factory for retaining consumers: the returned view owns a COPY of the
     * descriptor's bytes, so it stays valid after the descriptor is reused. The BTI row
     * trie retains prefixes across add() calls (prevMax/prevSep are lazy over the prefix),
     * so block-boundary prefixes must be snapshots, not reusable views.
     */
    public static ClusteringDescriptorPrefixView snapshotOf(ClusteringDescriptor descriptor, AbstractType<?>[] types)
    {
        ClusteringDescriptorPrefixView view = new ClusteringDescriptorPrefixView(types);
        byte[] copy = java.util.Arrays.copyOf(descriptor.clusteringBytes(), descriptor.clusteringLength());
        view.kind = descriptor.clusteringKind();
        view.size = descriptor.clusteringColumnsBound();
        view.backing = copy;
        view.window = ByteBuffer.wrap(copy);
        view.parse(copy.length);
        return view;
    }

    public ClusteringDescriptorPrefixView reset(ClusteringDescriptor descriptor)
    {
        this.kind = descriptor.clusteringKind();
        this.size = descriptor.clusteringColumnsBound();
        byte[] bytes = descriptor.clusteringBytes();
        int limit = descriptor.clusteringLength();
        if (backing != bytes || window == null)
        {
            backing = bytes;
            window = ByteBuffer.wrap(bytes);
        }
        parse(limit);
        return this;
    }

    private void parse(int limit)
    {
        if (offsets.length < size)
        {
            offsets = new int[size];
            lengths = new int[size];
        }

        int pos = 0;
        long header = 0;
        for (int i = 0; i < size; i++)
        {
            if (i % 32 == 0)
            {
                window.limit(limit).position(pos);
                header = VIntCoding.readUnsignedVInt(window);
                pos = window.position();
            }
            long flags = (header >>> ((i % 32) * 2)) & 0b11;
            if (flags == 0)
            {
                AbstractType<?> type = types[i];
                int len;
                if (type.isValueLengthFixed())
                {
                    len = type.valueLengthIfFixed();
                }
                else
                {
                    window.limit(limit).position(pos);
                    len = (int) VIntCoding.readUnsignedVInt(window);
                    pos = window.position();
                }
                offsets[i] = pos;
                lengths[i] = len;
                pos += len;
            }
            else if ((flags & 0b10) != 0) // null bit (2i+1)
            {
                offsets[i] = pos;
                lengths[i] = -1;
            }
            else // empty bit (2i)
            {
                offsets[i] = pos;
                lengths[i] = 0;
            }
        }
    }

    @Override
    public Kind kind()
    {
        return kind;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public ByteBuffer get(int i)
    {
        if (lengths[i] < 0)
            return null;
        window.limit(offsets[i] + lengths[i]).position(offsets[i]);
        return window;
    }

    @Override
    public ValueAccessor<ByteBuffer> accessor()
    {
        return ByteBufferAccessor.instance;
    }

    @Override
    public String toString(TableMetadata metadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusteringBound<ByteBuffer> asStartBound()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusteringBound<ByteBuffer> asEndBound()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] getRawValues()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] getBufferArray()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusteringPrefix<?> retainable()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long unsharedHeapSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusteringPrefix<ByteBuffer> clustering()
    {
        return this;
    }
}
