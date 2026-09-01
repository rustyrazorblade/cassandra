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

package org.apache.cassandra.db.cursorreads;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CursorReads;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Gap C of the Phase 1 charter (journal 2026-08-07): {@code UnfilteredValidation}'s
 * corrupted-tombstone-strategy checks on cursor-served legs. Every scenario writes an sstable
 * carrying an on-disk deletion that classifies INVALID on read, then asserts the cursor path
 * behaves exactly like the iterator path under each {@code corrupted_tombstone_strategy}:
 * <ul>
 *   <li>{@code exception}: BOTH paths abort the read with a {@link CorruptSSTableException} whose
 *       {@link MarshalException} diagnosis is IDENTICAL (same key, same offending content), and
 *       both mark the sstable suspect;</li>
 *   <li>{@code warn}/{@code disabled}: both paths serve the partition, byte-identically (the
 *       standard differential contract) — the invalid deletion flows through unfixed.</li>
 * </ul>
 *
 * Fixture techniques, cribbed from the canonical upstream corrupted-tombstone tests:
 * <ul>
 *   <li>row deletion with a NEGATIVE localDeletionTime, raw-applied
 *       ({@code RowUpdateBuilder.deleteRowAt(..., -1, ...)} — {@code CompactionsCQLTest}'s
 *       CASSANDRA-14227 fixture). The memtable holds an {@code InvalidDeletionTime} whose on-disk
 *       form re-classifies INVALID on both read paths;</li>
 *   <li>complex (collection) deletion, same negative-ldt technique via a raw
 *       {@code Row.Builder} ({@code FarFutureDeletionDifferentialCompactionTest}'s shape);</li>
 *   <li>an EXPIRING cell whose localDeletionTime is smaller than its ttl —
 *       {@code Cell.decodeLocalDeletionTime} classifies that {@code INVALID_DELETION_TIME} on
 *       read (it cannot occur without corruption), on both paths identically.</li>
 * </ul>
 *
 * Also pins the deliberate upstream ASYMMETRY this gap's investigation surfaced: on the latest
 * (uint-ldt) sstable format a far-future (post-2038) CELL localDeletionTime is LEGAL — the
 * unsigned fixup in {@code Cell.decodeLocalDeletionTime} recovers the true value from the
 * sign-extended wire form — while the same value on a row/complex DELETION classifies INVALID
 * ({@code DeletionTime.build} normalizes before any fixup could run). The far-future-cell
 * scenario proves the cursor path reproduces the fixup byte-identically and does NOT spuriously
 * trip validation under {@code exception}.
 *
 * Silent-fallback discipline (base-class contract): the throwing scenarios assert the gate's own
 * verdict, that {@link CursorReads#sstableLegsServed()} advanced on the cursor leg, and that the
 * cursor leg re-marked the sstable suspect after the iterator leg's mark was cleared — a cursor
 * run that silently fell back would fail all three.
 */
public class CorruptedTombstoneCursorReadDifferentialTest extends CursorReadDifferentialTester
{
    /**
     * Legal in the long domain (&lt; Cell.MAX_DELETION_TIME, ~year 2103) but ≥ 2^31 above any
     * current-epoch minimum, so the header delta encoding sign-extends on write.
     * FarFutureDeletionDifferentialCompactionTest uses 4.0e9; 4.2e9 keeps the delta ≥ 2^31
     * until ~2035 instead of ~2028.
     */
    private static final long FAR_FUTURE_LDT = 4_200_000_000L;

    private Config.CorruptedTombstoneStrategy savedStrategy;

    @Before
    public void saveCorruptedTombstoneStrategy()
    {
        savedStrategy = DatabaseDescriptor.getCorruptedTombstoneStrategy();
    }

    @After
    public void restoreCorruptedTombstoneStrategy()
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(savedStrategy);
    }

    // ---------------------------------------------------------------- scenarios

    @Test
    public void invalidRowDeletionExceptionStrategyFailsBothPathsIdentically() throws Throwable
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        ColumnFamilyStore cfs = prepareInvalidRowDeletion();
        long now = FBUtilities.nowInSeconds();
        assertBothPathsFailIdentically(cfs, fullPartition(cfs, now));
    }

    @Test
    public void invalidRowDeletionWarnStrategyMatchesByteIdentically() throws Throwable
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.warn);
        ColumnFamilyStore cfs = prepareInvalidRowDeletion();
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now));
    }

    @Test
    public void invalidRowDeletionDisabledStrategyMatchesByteIdentically() throws Throwable
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.disabled);
        ColumnFamilyStore cfs = prepareInvalidRowDeletion();
        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now));
    }

    @Test
    public void invalidComplexDeletionExceptionStrategyFailsBothPathsIdentically() throws Throwable
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();
        for (long ck = 0; ck < 5; ck++)
        {
            // ck=2 gets NO map content via CQL: an INSERT of a collection writes its own complex
            // deletion at the statement timestamp, which would supersede the raw-applied invalid one
            if (ck == 2)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
            else
                execute("INSERT INTO %s (pk, ck, v1, m) VALUES (0, ?, ?, {'a': 1, 'b': 2})", ck, ck);
        }
        applyComplexDeletion(metadata, 0L, 2L, "m", DeletionTime.build(2000, -1)); // InvalidDeletionTime
        flush();

        long now = FBUtilities.nowInSeconds();
        assertBothPathsFailIdentically(cfs, fullPartition(cfs, now));
    }

    @Test
    public void invalidExpiringCellExceptionStrategyFailsBothPathsIdentically() throws Throwable
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
        // an expiring cell whose localDeletionTime (50) precedes its ttl (100):
        // Cell.decodeLocalDeletionTime classifies this INVALID_DELETION_TIME on read — "can't occur
        // without corruption". ck=7 has no CQL-written v1 that could supersede the raw cell.
        applyCell(metadata, 0L, 7L, "v1", 1000, 100, 50L, ByteBufferUtil.bytes(7L));
        flush();

        long now = FBUtilities.nowInSeconds();
        assertBothPathsFailIdentically(cfs, fullPartition(cfs, now));
    }

    /** The asymmetry pin: a far-future CELL ldt is LEGAL on the uint format — no throw, byte parity. */
    @Test
    public void farFutureCellIsValidOnBothPaths() throws Throwable
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
        // recent tombstone pins the sstable's minLocalDeletionTime to the current epoch so the
        // far-future ldt's write-side delta lands in the sign-extension domain [2^31, 2^32)
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 100");
        applyCell(metadata, 0L, 7L, "v1", 1000, 100, FAR_FUTURE_LDT, ByteBufferUtil.bytes(7L));
        flush();

        long now = FBUtilities.nowInSeconds();
        assertCursorReadMatchesIterator(cfs, fullPartition(cfs, now));
    }

    // ---------------------------------------------------------------- machinery

    private Supplier<SinglePartitionReadCommand> fullPartition(ColumnFamilyStore cfs, long now)
    {
        return () -> (SinglePartitionReadCommand) Util.cmd(cfs, 0L).withNowInSeconds(now).build();
    }

    private ColumnFamilyStore prepareInvalidRowDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck, "v" + ck);
        // CASSANDRA-14227 fixture: row deletion with negative ldt (LDTs are never user-supplied and
        // should never be negative) — classifies InvalidDeletionTime on both write and read
        RowUpdateBuilder.deleteRowAt(cfs.metadata(), 1000, -1, 0L, 2L).apply();
        flush();
        return cfs;
    }

    /**
     * The corrupted-input counterpart of the base class's differential contract: under
     * {@code strategy=exception} both paths must REFUSE the read, with the same exception type,
     * the SAME MarshalException diagnosis (key + offending content — materialization parity makes
     * them equal), and the same mark-suspect side effect. Includes the full silent-fallback guard
     * for the cursor leg.
     */
    private void assertBothPathsFailIdentically(ColumnFamilyStore cfs, Supplier<SinglePartitionReadCommand> command)
    {
        DatabaseDescriptor.setCursorReadsEnabled(false);
        MarshalException iteratorDiagnosis = expectCorruptRead(command);
        assertSuspectAndReset(cfs);

        DatabaseDescriptor.setCursorReadsEnabled(true);
        try
        {
            SinglePartitionReadCommand probe = command.get();
            assertTrue("scenario is not supported by the cursor read gate; this run would silently " +
                       "compare iterator vs iterator",
                       CursorReads.isReadSupported(probe, cfs, liveSSTablesFor(cfs, probe)));

            long servedBefore = CursorReads.sstableLegsServed();
            MarshalException cursorDiagnosis = expectCorruptRead(command);
            assertTrue("cursor path did not actually serve the sstable leg whose validation is under " +
                       "test (silent fallback?)",
                       CursorReads.sstableLegsServed() > servedBefore);
            assertSuspectAndReset(cfs); // the CURSOR leg's handleInvalid must re-mark it

            assertEquals("iterator and cursor paths diagnosed DIFFERENT corruption",
                         iteratorDiagnosis.getMessage(), cursorDiagnosis.getMessage());
        }
        finally
        {
            DatabaseDescriptor.setCursorReadsEnabled(false);
        }
    }

    /** Consumes the read and demands it fail with CorruptSSTableException caused by MarshalException. */
    private MarshalException expectCorruptRead(Supplier<SinglePartitionReadCommand> command)
    {
        try
        {
            canonicalRecords(command.get());
        }
        catch (Throwable t)
        {
            boolean corrupt = false;
            for (Throwable c = t; c != null; c = c.getCause())
            {
                if (c instanceof CorruptSSTableException)
                    corrupt = true;
                if (c instanceof MarshalException)
                {
                    assertTrue("MarshalException without enclosing CorruptSSTableException", corrupt);
                    return (MarshalException) c;
                }
            }
            throw new AssertionError("read of invalid deletion failed, but not with " +
                                     "CorruptSSTableException(MarshalException)", t);
        }
        fail("read of an sstable with an invalid deletion SUCCEEDED under corrupted_tombstone_strategy=" +
             DatabaseDescriptor.getCorruptedTombstoneStrategy() +
             " (cursor_reads_enabled=" + DatabaseDescriptor.cursorReadsEnabled() + ')');
        throw new AssertionError("unreachable");
    }

    private static void assertSuspectAndReset(ColumnFamilyStore cfs)
    {
        boolean any = false;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.isMarkedSuspect())
            {
                any = true;
                sstable.unmarkSuspect();
            }
        }
        assertTrue("no sstable was marked suspect by the failed read", any);
    }

    private static void applyComplexDeletion(TableMetadata metadata, long pk, long ck, String column, DeletionTime deletion)
    {
        ColumnMetadata cm = metadata.getColumn(ByteBufferUtil.bytes(column));
        assertNotNull(cm);
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(new BufferClustering(ByteBufferUtil.bytes(ck)));
        builder.addComplexDeletion(cm, deletion);
        apply(metadata, pk, builder.build());
    }

    private static void applyCell(TableMetadata metadata, long pk, long ck, String column,
                                  long timestamp, int ttl, long localDeletionTime, ByteBuffer value)
    {
        ColumnMetadata cm = metadata.getColumn(ByteBufferUtil.bytes(column));
        assertNotNull(cm);
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(new BufferClustering(ByteBufferUtil.bytes(ck)));
        builder.addCell(new BufferCell(cm, timestamp, ttl, localDeletionTime, value, null));
        apply(metadata, pk, builder.build());
    }

    private static void apply(TableMetadata metadata, long pk, Row row)
    {
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(
            metadata, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(pk)), row);
        new Mutation(update).apply();
    }
}
