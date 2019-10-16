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

package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.ExpirationDateOverflowHandling;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public abstract class ArrayCell extends AbstractCell
{
    private final long timestamp;

    public ArrayCell(ColumnMetadata column, long timestamp)
    {
        super(column);
        this.timestamp = timestamp;
    }

    private static class Live extends ArrayCell
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new ArrayCell.Live(ColumnMetadata.regularColumn("", "", "", ByteType.instance), 0L, ByteBufferUtil.EMPTY_BYTE_ARRAY));

        private final byte[] value;

        public Live(ColumnMetadata column, long timestamp, byte[] value)
        {
            super(column, timestamp);
            this.value = value;
        }

        public byte[] array()
        {
            return value;
        }

        public int ttl()
        {
            return NO_TTL;
        }

        public int localDeletionTime()
        {
            return NO_DELETION_TIME;
        }

        public CellPath path()
        {
            return null;
        }

        protected long emptySize()
        {
            return EMPTY_SIZE;
        }
    }

    private static class Tombstone extends ArrayCell
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new ArrayCell.Tombstone(ColumnMetadata.regularColumn("", "", "", ByteType.instance), 0L, 0));

        private final int localDeletionTime;

        public Tombstone(ColumnMetadata column, long timestamp, int localDeletionTime)
        {
            super(column, timestamp);
            this.localDeletionTime = localDeletionTime;
        }

        public ByteBuffer value()
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        public boolean hasArray()
        {
            return true;
        }

        public byte[] array()
        {
            return ByteBufferUtil.EMPTY_BYTE_ARRAY;
        }

        public int ttl()
        {
            return NO_TTL;
        }

        public int localDeletionTime()
        {
            return localDeletionTime;
        }

        public CellPath path()
        {
            return null;
        }

        protected long emptySize()
        {
            return EMPTY_SIZE;
        }
    }

    private static class Simple extends ArrayCell
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new ArrayCell.Simple(ColumnMetadata.regularColumn("", "", "", ByteType.instance), 0L, 0, 0, ByteBufferUtil.EMPTY_BYTE_ARRAY));
        private final int ttl;
        private final int localDeletionTime;

        private final byte[] value;

        public Simple(ColumnMetadata column, long timestamp, int ttl, int localDeletionTime, byte[] value)
        {
            super(column, timestamp);
            this.ttl = ttl;
            this.localDeletionTime = localDeletionTime;
            this.value = value;
        }

        public byte[] array()
        {
            return value;
        }

        public int ttl()
        {
            return ttl;
        }

        public int localDeletionTime()
        {
            return localDeletionTime;
        }

        public CellPath path()
        {
            return null;
        }

        protected long emptySize()
        {
            return EMPTY_SIZE;
        }
    }

    // public for BTree row builder
    public static class Complex extends ArrayCell
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Complex(ColumnMetadata.regularColumn("", "", "", ByteType.instance), 0L, 0, 0, ByteBufferUtil.EMPTY_BYTE_ARRAY, null));
        private final int ttl;
        private final int localDeletionTime;

        private final byte[] value;
        private final CellPath path;

        public Complex(ColumnMetadata column, long timestamp, int ttl, int localDeletionTime, byte[] value, CellPath path)
        {
            super(column, timestamp);
            this.ttl = ttl;
            this.localDeletionTime = localDeletionTime;
            this.value = value;
            this.path = path;
        }

        public byte[] array()
        {
            return value;
        }

        public int ttl()
        {
            return ttl;
        }

        public int localDeletionTime()
        {
            return localDeletionTime;
        }

        public CellPath path()
        {
            return path;
        }

        protected long emptySize()
        {
            return EMPTY_SIZE;
        }
    }

    public static ArrayCell create(ColumnMetadata column, long timestamp, int ttl, int localDeletionTime, byte[] value, CellPath path)
    {
        assert column.isComplex() == (path != null);
        if (path != null)
            return new Complex(column, timestamp, ttl, localDeletionTime, value, path);

        if (value.length == 0 && ttl == NO_TTL)
            return new Tombstone(column, timestamp, localDeletionTime);

        if (ttl == NO_TTL && localDeletionTime == NO_DELETION_TIME)
            return new Live(column, timestamp, value);

        return new Simple(column, timestamp, ttl, localDeletionTime, value);
    }

    public static ArrayCell live(ColumnMetadata column, long timestamp, byte[] value)
    {
        return live(column, timestamp, value, null);
    }

    public static ArrayCell live(ColumnMetadata column, long timestamp, byte[] value, CellPath path)
    {
        return create(column, timestamp, NO_TTL, NO_DELETION_TIME, value, path);
    }

    public static ArrayCell expiring(ColumnMetadata column, long timestamp, int ttl, int nowInSec, byte[] value)
    {
        return expiring(column, timestamp, ttl, nowInSec, value, null);
    }

    public static ArrayCell expiring(ColumnMetadata column, long timestamp, int ttl, int nowInSec, byte[] value, CellPath path)
    {
        assert ttl != NO_TTL;
        return create(column, timestamp, ttl, ExpirationDateOverflowHandling.computeLocalExpirationTime(nowInSec, ttl), value, path);
    }

    public static ArrayCell tombstone(ColumnMetadata column, long timestamp, int nowInSec)
    {
        return tombstone(column, timestamp, nowInSec, null);
    }

    public static ArrayCell tombstone(ColumnMetadata column, long timestamp, int nowInSec, CellPath path)
    {
        return create(column, timestamp, NO_TTL, nowInSec, ByteBufferUtil.EMPTY_BYTE_ARRAY, path);
    }

    public long timestamp()
    {
        return timestamp;
    }

    public Cell withUpdatedColumn(ColumnMetadata newColumn)
    {
        return create(newColumn, timestamp, ttl(), localDeletionTime(), array(), path());
    }

    public Cell withUpdatedValue(ByteBuffer newValue)
    {
        throw new UnsupportedOperationException();
    }

    public Cell withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime)
    {
        return create(column, newTimestamp, ttl(), newLocalDeletionTime, array(), path());
    }

    public Cell copy(AbstractAllocator allocator)
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasBuffer()
    {
        return false;
    }

    public ByteBuffer value()
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasArray()
    {
        return true;
    }

    protected abstract long emptySize();

    public long unsharedHeapSizeExcludingData()
    {
        return emptySize() + ObjectSizes.sizeOnHeapExcludingData(value()) + (path() == null ? 0 : path().unsharedHeapSizeExcludingData());
    }
}
