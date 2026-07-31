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
package org.apache.cassandra.db.compaction;

import java.util.NoSuchElementException;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A partition marker produced by {@link DigestingCursorMergeSink}: carries a per-partition digest
 * already computed directly from cursor primitives (see that class), rather than a real row/cell
 * object graph a caller could re-digest itself. Exists only so {@link CursorCompactor#mergeNextPartition}
 * can keep returning something through the existing {@code CursorMergeSink}-driven partition loop
 * without changing that loop's shape.
 * <p>
 * The only code that ever needs to know this type exists is {@code CursorValidationIterator}
 * itself, which produces these and overrides {@code ValidationPartitionIterator#feed} to read
 * {@link #digestBytes()}/{@link #digestInputBytes()} directly instead of iterating rows or calling
 * {@code UnfilteredRowIterators.digest()} - the whole point of this type existing is that no real
 * row/cell content was ever materialized to digest in the first place. The driving loop in
 * {@code ValidationManager#doValidation} calls {@code feed} polymorphically and never inspects the
 * partition's runtime type. Every row-content method here throws rather than silently returning
 * empty/wrong data, as a defense-in-depth backstop should any other code ever end up holding one
 * of these and try to treat it as a genuine iterator.
 */
public class PrecomputedDigestPartition implements UnfilteredRowIterator
{
    private final TableMetadata metadata;
    private final DecoratedKey partitionKey;
    private final RegularAndStaticColumns columns;
    private final byte[] digestBytes;
    private final long digestInputBytes;

    public PrecomputedDigestPartition(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns,
                                       byte[] digestBytes, long digestInputBytes)
    {
        this.metadata = metadata;
        this.partitionKey = partitionKey;
        this.columns = columns;
        this.digestBytes = digestBytes;
        this.digestInputBytes = digestInputBytes;
    }

    public byte[] digestBytes()
    {
        return digestBytes;
    }

    public long digestInputBytes()
    {
        return digestInputBytes;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public boolean isReverseOrder()
    {
        return false;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        return columns;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        throw new UnsupportedOperationException("PrecomputedDigestPartition carries only a precomputed digest, not real row content");
    }

    @Override
    public Row staticRow()
    {
        throw new UnsupportedOperationException("PrecomputedDigestPartition carries only a precomputed digest, not real row content");
    }

    @Override
    public boolean isEmpty()
    {
        throw new UnsupportedOperationException("PrecomputedDigestPartition carries only a precomputed digest, not real row content");
    }

    @Override
    public EncodingStats stats()
    {
        throw new UnsupportedOperationException("PrecomputedDigestPartition carries only a precomputed digest, not real row content");
    }

    @Override
    public boolean hasNext()
    {
        return false;
    }

    @Override
    public Unfiltered next()
    {
        throw new NoSuchElementException("PrecomputedDigestPartition carries only a precomputed digest, not real row content");
    }

    @Override
    public void close()
    {
    }
}
