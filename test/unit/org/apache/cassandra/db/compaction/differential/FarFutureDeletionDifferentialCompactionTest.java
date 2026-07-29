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

package org.apache.cassandra.db.compaction.differential;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Deletion times past Jan 2038 (localDeletionTime >= 2^31 seconds) are legal up to
 * Cell.MAX_DELETION_TIME (~year 2106, unsigned-int encoded on the wire): the header
 * delta encoding casts the delta to int (SerializationHeader.writeLocalDeletionTime),
 * so a delta in [2^31, 2^32) sign-extends to a 9-byte vint on write and decodes back
 * NEGATIVE through readLocalDeletionTime.
 *
 * Two distinct hazards for the cursor path, both pinned here:
 * - SIZE: the cursor writer PREDICTS complex-deletion sizes from the long-domain delta
 *   (5 bytes) while the write emits the sign-extended form (9 bytes) — the row-size vint
 *   understates the body and the output sstable is structurally corrupt. The iterator
 *   measures its row bodies and cannot mismatch.
 * - DECODE parity: the negative round-trip lands on the INVALID classification identically
 *   on both paths (DeletionTime.build and ReusableDeletionTime.reset normalize negatives the
 *   same way; the unsigned fixup in UnfilteredSerializer.readComplexColumn is unreachable for
 *   5.0-format inputs because build normalizes first) — pinned for complex AND row deletions
 *   so neither path can drift.
 *
 * Raw-applied because CQL derives deletion times from the server clock.
 */
public class FarFutureDeletionDifferentialCompactionTest extends DifferentialCompactionTester
{
    /** ~year 2096: valid (< MAX_DELETION_TIME) but >= 2^31 above any current-epoch minimum. */
    private static final long FAR_FUTURE_LDT = 4_000_000_000L;

    /**
     * The round-trip of a far-future LDT through the header delta encoding lands on the
     * INVALID classification on read (both paths, identically); under the test default
     * corrupted_tombstone_strategy=exception the ITERATOR leg refuses such inputs outright,
     * so this suite runs with the production default (disabled) — the byte-parity contract
     * is what is under test, not the guardrail.
     */
    @BeforeClass
    public static void disableCorruptedTombstoneStrategy()
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.disabled);
    }

    @Test
    public void farFutureComplexDeletion() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text, bigint>, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();

        // ordinary recent content, including a recent tombstone pinning the sstable's
        // minLocalDeletionTime to the current epoch. ck=2 deliberately gets NO map content
        // via CQL: INSERT of a collection writes its own complex deletion at the statement
        // timestamp, which would supersede the raw-applied far-future one in the memtable.
        for (long ck = 0; ck < 5; ck++)
        {
            if (ck == 2)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
            else
                execute("INSERT INTO %s (pk, ck, v1, m) VALUES (0, ?, ?, {'a': 1, 'b': 2})", ck, ck);
        }
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 100");

        // far-future complex deletion: output delta = FAR_FUTURE_LDT - minLDT lands in
        // [2^31, 2^32), the sign-extension domain
        applyComplexDeletion(metadata, 0L, 2L, "m", DeletionTime.build(2000, FAR_FUTURE_LDT));
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /** Same wire shape on a ROW deletion: no unsigned fixup on either path — parity pin. */
    @Test
    public void farFutureRowDeletion() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();

        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 100");

        applyRowDeletion(metadata, 0L, 2L, DeletionTime.build(2000, FAR_FUTURE_LDT));
        flush();

        assertCursorMatchesIterator(cfs);
    }

    private static void applyComplexDeletion(TableMetadata metadata, long pk, long ck, String column, DeletionTime deletion)
    {
        ColumnMetadata cm = metadata.getColumn(ByteBufferUtil.bytes(column));
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(new BufferClustering(ByteBufferUtil.bytes(ck)));
        builder.addComplexDeletion(cm, deletion);
        apply(metadata, pk, builder.build());
    }

    private static void applyRowDeletion(TableMetadata metadata, long pk, long ck, DeletionTime deletion)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(new BufferClustering(ByteBufferUtil.bytes(ck)));
        builder.addRowDeletion(new Row.Deletion(deletion, false));
        apply(metadata, pk, builder.build());
    }

    private static void apply(TableMetadata metadata, long pk, Row row)
    {
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(
            metadata, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(pk)), row);
        new Mutation(update).apply();
    }
}
