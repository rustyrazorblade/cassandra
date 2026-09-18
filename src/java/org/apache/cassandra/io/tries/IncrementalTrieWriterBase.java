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
package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.LightweightRecycler;
import org.apache.cassandra.utils.concurrent.ThreadLocals;

/**
 * Helper base class for incremental trie builders.
 */
public abstract class IncrementalTrieWriterBase<VALUE, DEST, NODE extends IncrementalTrieWriterBase.BaseNode<VALUE, NODE>>
implements IncrementalTrieWriter<VALUE>
{
    protected final Deque<NODE> stack = new ArrayDeque<>();
    protected final TrieSerializer<VALUE, ? super DEST> serializer;
    protected final DEST dest;
    protected ByteComparable prev = null;
    // Reused buffers holding the byte-comparable form of the previous and next keys.  add() materializes each key once
    // into curBytes instead of decoding both prev and next on every call.
    private static final int INITIAL_KEY_BUFFER_SIZE = 32;
    private byte[] prevBytes = new byte[INITIAL_KEY_BUFFER_SIZE];
    private int prevLen = 0;
    private byte[] curBytes = new byte[INITIAL_KEY_BUFFER_SIZE];
    long count = 0;

    protected IncrementalTrieWriterBase(TrieSerializer<VALUE, ? super DEST> serializer, DEST dest, NODE root)
    {
        this.serializer = serializer;
        this.dest = dest;
        this.stack.addLast(root);
    }

    protected void reset(NODE root)
    {
        this.prev = null;
        this.prevLen = 0;
        this.count = 0;
        this.stack.clear();
        this.stack.addLast(root);
    }


    @Override
    public void close()
    {
        this.prev = null;
        this.prevLen = 0;
        this.count = 0;
        this.stack.clear();
    }

    @Override
    public void add(ByteComparable next, VALUE value) throws IOException
    {
        ++count;

        // Materialize the byte-comparable form of the key once into a reused buffer.  The old code decoded both the
        // previous and the next key on every call and walked them in lockstep; here we keep the previous key's bytes
        // around and only decode the next key.
        ByteSource sn = next.asComparableBytes(Walker.BYTE_COMPARABLE_VERSION);
        int curLen = 0;
        for (int b = sn.next(); b != ByteSource.END_OF_STREAM; b = sn.next())
        {
            if (curLen == curBytes.length)
                curBytes = Arrays.copyOf(curBytes, curLen * 2);
            curBytes[curLen++] = (byte) b;
        }

        int stackpos = 0;
        if (prev != null)
        {
            // The shared prefix length is the first index where the two keys differ.  Arrays.mismatch returns -1 only
            // when the two ranges are identical, which means duplicate keys.
            int mismatch = Arrays.mismatch(prevBytes, 0, prevLen, curBytes, 0, curLen);
            assert mismatch != -1 : String.format("Incremental trie requires unique sorted keys, got equal %s(%s) after %s(%s).",
                                                  next,
                                                  next.byteComparableAsString(Walker.BYTE_COMPARABLE_VERSION),
                                                  prev,
                                                  prev.byteComparableAsString(Walker.BYTE_COMPARABLE_VERSION));

            // The next key must sort after the previous key.  If the mismatch is at the end of one key, the shorter key
            // is a prefix of the other; otherwise compare the first differing byte as unsigned.
            boolean sorted;
            if (mismatch == prevLen)          // previous key is a strict prefix of the next key
                sorted = true;
            else if (mismatch == curLen)      // next key is a strict prefix of the previous key
                sorted = false;
            else
                sorted = (curBytes[mismatch] & 0xFF) > (prevBytes[mismatch] & 0xFF);
            assert sorted : String.format("Incremental trie requires sorted keys, got %s(%s) after %s(%s).",
                                          next,
                                          next.byteComparableAsString(Walker.BYTE_COMPARABLE_VERSION),
                                          prev,
                                          prev.byteComparableAsString(Walker.BYTE_COMPARABLE_VERSION));
            stackpos = mismatch;
        }

        while (stack.size() > stackpos + 1)
            completeLast();

        NODE node = stack.getLast();
        for (int i = stackpos; i < curLen; ++i)
        {
            node = node.addChild(curBytes[i]);
            stack.addLast(node);
        }

        VALUE existingPayload = node.setPayload(value);
        assert existingPayload == null;

        // The next key becomes the previous key.  Swap buffers so the old previous buffer is reused as scratch.
        byte[] tmp = prevBytes;
        prevBytes = curBytes;
        curBytes = tmp;
        prevLen = curLen;
        prev = next;
    }

    public long complete() throws IOException
    {
        NODE root = stack.getFirst();
        if (root.filePos != -1)
            return root.filePos;

        return performCompletion().filePos;
    }

    NODE performCompletion() throws IOException
    {
        NODE root = null;
        while (!stack.isEmpty())
            root = completeLast();
        stack.addLast(root);
        return root;
    }

    public long count()
    {
        return count;
    }

    protected NODE completeLast() throws IOException
    {
        NODE node = stack.removeLast();
        complete(node);
        return node;
    }

    abstract void complete(NODE value) throws IOException;
    abstract public PartialTail makePartialRoot() throws IOException;

    static class PTail implements PartialTail
    {
        long root;
        long cutoff;
        long count;
        ByteBuffer tail;

        @Override
        public long root()
        {
            return root;
        }

        @Override
        public long cutoff()
        {
            return cutoff;
        }

        @Override
        public ByteBuffer tail()
        {
            return tail;
        }

        @Override
        public long count()
        {
            return count;
        }
    }

    static abstract class BaseNode<VALUE, NODE extends BaseNode<VALUE, NODE>> implements SerializationNode<VALUE>
    {
        private static final int CHILDREN_LIST_RECYCLER_LIMIT = 1024;
        @SuppressWarnings("rawtypes")
        private static final LightweightRecycler<ArrayList> CHILDREN_LIST_RECYCLER = ThreadLocals.createLightweightRecycler(CHILDREN_LIST_RECYCLER_LIMIT);
        @SuppressWarnings("rawtypes")
        private static final ArrayList EMPTY_LIST = new ArrayList<>(0);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static <NODE> ArrayList<NODE> allocateChildrenList()
        {
            return CHILDREN_LIST_RECYCLER.reuseOrAllocate(() -> new ArrayList(4));
        }

        private static <NODE> void recycleChildrenList(ArrayList<NODE> children)
        {
            CHILDREN_LIST_RECYCLER.tryRecycle(children);
        }

        VALUE payload;
        ArrayList<NODE> children;
        final int transition;
        long filePos = -1;

        @SuppressWarnings("unchecked")
        BaseNode(int transition)
        {
            children = EMPTY_LIST;
            this.transition = transition;
        }

        public VALUE payload()
        {
            return payload;
        }

        public VALUE setPayload(VALUE newPayload)
        {
            VALUE p = payload;
            payload = newPayload;
            return p;
        }

        public NODE addChild(byte b)
        {
            assert children.isEmpty() || (children.get(children.size() - 1).transition & 0xFF) < (b & 0xFF);
            NODE node = newNode(b);
            if (children == EMPTY_LIST)
                children = allocateChildrenList();

            children.add(node);
            return node;
        }

        public int childCount()
        {
            return children.size();
        }

        void finalizeWithPosition(long position)
        {
            this.filePos = position;

            // Make sure we are not holding on to pointers to data we no longer need
            // (otherwise we keep the whole trie in memory).
            if (children != EMPTY_LIST)
                // the recycler will also clear the collection before adding it to the pool
                recycleChildrenList(children);

            children = null;
            payload = null;
        }

        public int transition(int i)
        {
            return children.get(i).transition;
        }

        @Override
        public String toString()
        {
            return String.format("%02x", transition);
        }

        abstract NODE newNode(byte transition);
    }
}
