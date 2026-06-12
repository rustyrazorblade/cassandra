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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;

/**
 * Differential coverage for COUNTER tables (increment 5, H9 corpus). Counter merge differs
 * from regular reconciliation in two load-bearing ways the scenarios target:
 *
 *  - live + live counter cells MERGE their contexts (CounterContext semantics) instead of
 *    one side winning; the resulting timestamp is the max of the contributors;
 *  - a counter TOMBSTONE beats a live counter cell REGARDLESS of timestamps
 *    (CASSANDRA-7346), so shadowed inputs must be excluded before any context merge.
 *
 * Single-JVM CQL produces single-CounterId global-shard contexts only; the multi-shard,
 * local/remote, and marked-to-clear shapes are pinned at the unit level by
 * CursorCounterContextMergeTest against the upstream CounterContext implementation.
 */
public class CounterDifferentialCompactionTest extends DifferentialCompactionTester
{
    /** Same counter cells incremented across many sstables: pure context-merge folds. */
    @Test
    public void shardMergesAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 4; round++)
        {
            for (long pk = 0; pk < 4; pk++)
                for (long ck = 0; ck < 10; ck++)
                {
                    execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", ck + round, pk, ck);
                    if (ck % 3 == 0)
                        execute("UPDATE %s SET c2 = c2 + ? WHERE pk = ? AND ck = ?", -1L - round, pk, ck);
                }
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Counter tombstones (DELETE c / row deletes) interleaved with increments across
     * sstables — the CASSANDRA-7346 rule: the tombstone wins against live counter cells
     * regardless of timestamp order, so increments AFTER the delete (higher ts) still lose.
     */
    @Test
    public void counterTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 12; ck++)
        {
            execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = ? AND ck = ?", ck, 100 + ck, 1L, ck);
            execute("UPDATE %s SET c1 = c1 + ? WHERE pk = ? AND ck = ?", ck, 2L, ck);
        }
        flush();

        // cell deletes: tombstone newer than the increments above
        for (long ck = 0; ck < 6; ck++)
            execute("DELETE c1 FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        // row deletes over counter rows
        execute("DELETE FROM %s WHERE pk = 2 AND ck = 3");
        execute("DELETE FROM %s WHERE pk = 2 AND ck = 4");
        flush();

        // increments NEWER than the tombstones: 7346 — the tombstone still wins for c1;
        // c2 (never deleted) keeps merging
        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = ? AND ck = ?", 1000 + ck, ck, 1L, ck);
        execute("UPDATE %s SET c1 = c1 + 99 WHERE pk = 2 AND ck = 3");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Static counters next to clustered counters, incl. deletes of each. */
    @Test
    public void staticCounters() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, s counter static, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 5; pk++)
            {
                execute("UPDATE %s SET s = s + ? WHERE pk = ?", pk + round + 1, pk);
                for (long ck = 0; ck < 6; ck++)
                    execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", ck + round, pk, ck);
            }
            flush();
        }
        execute("DELETE s FROM %s WHERE pk = 3");
        execute("DELETE c FROM %s WHERE pk = 4 AND ck = 2");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Counter columns in partitions wide enough to cross index blocks (4KiB test config). */
    @Test
    public void countersAcrossIndexBlocks() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, c3 counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 400; ck++)
                execute("UPDATE %s SET c1 = c1 + ?, c2 = c2 + ?, c3 = c3 + ? WHERE pk = ? AND ck = ?",
                        ck, ck * 31, -ck, 1L, ck);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Counter cell shapes CQL cannot produce but replication/streaming legitimately writes,
     * applied as RAW mutations (the streaming/repair path, bypassing CounterMutation):
     *
     *  - MULTI-SHARD contexts (global + remote shards from several CounterIds) — single-JVM
     *    CQL only ever makes one-shard global contexts, so this is the only differential
     *    coverage of real shard-level merge folds;
     *  - MARKED local-shard contexts (markLocalToBeCleared, the streamed-sstable shape) —
     *    pins the deserialization-time clear transform end-to-end, both paths;
     *  - a counter TOMBSTONE carrying a value: serializers preserve the value faithfully.
     *
     * NOT covered, with proof of unreachability: EMPTY-VALUE live counter cells (the
     * #10657/#11726 read-path artifact) cannot exist in an sstable — memtable flush itself
     * dies in Cells.collectStats -> CounterContext.hasLegacyShards (IndexOutOfBounds on the
     * 0-length context) before one can be written, so they can never be compaction inputs.
     */
    @Test
    public void exoticCounterCellShapes() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, c2 counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000 AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();
        ColumnMetadata c1 = metadata.getColumn(ByteBufferUtil.bytes("c1"));
        ColumnMetadata c2 = metadata.getColumn(ByteBufferUtil.bytes("c2"));

        // sstable 1
        applyCounterCell(metadata, 1L, 0L, BufferCell.live(c1, 1000, context(global(1, 5, 100), remote(2, 3, 7))));
        applyCounterCell(metadata, 1L, 1L, BufferCell.live(c1, 1000, marked(context(local(3, 2, 11), global(4, 1, 5)))));
        applyCounterCell(metadata, 1L, 2L, BufferCell.live(c1, 1000, context(remote(8, 1, 4))));
        // a RETAINED tombstone (recent ldt — an epoch-old one purges in both paths and the
        // write path is never exercised: vacuous-green) that carries a VALUE
        applyCounterCell(metadata, 1L, 3L, new BufferCell(c1, 1000, Cell.NO_TTL,
                                                          org.apache.cassandra.utils.FBUtilities.nowInSeconds() - 60,
                                                          context(global(9, 1, 1)), null));
        applyCounterCell(metadata, 1L, 4L, BufferCell.live(c2, 1000, context(remote(5, -2, 42))));
        flush();

        // sstable 2: overlapping shapes that force every reconciliation rule — same-id
        // global clock races, disjoint-id real merges, marked-context clears, 7346
        applyCounterCell(metadata, 1L, 0L, BufferCell.live(c1, 2000, context(global(1, 7, 200), remote(6, 1, 1))));
        applyCounterCell(metadata, 1L, 1L, BufferCell.live(c1, 1500, context(remote(3, 4, 13))));
        applyCounterCell(metadata, 1L, 2L, BufferCell.live(c1, 3000, context(global(7, 2, 9)))); // disjoint ids: true merge
        applyCounterCell(metadata, 1L, 3L, BufferCell.live(c1, 4000, context(global(9, 2, 2)))); // loses to the tombstone (7346)
        applyCounterCell(metadata, 1L, 4L, BufferCell.live(c2, 900, context(remote(5, -1, 50)))); // higher clock wins
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Counter TOMBSTONES tied on BOTH timestamp and localDeletionTime resolve by their raw
     * value bytes: a counter tombstone is not a counter cell (AbstractCell.isCounterCell),
     * so the iterator routes the pair through Cells.resolveRegular, whose final tie-break is
     * compareValues(left, right) >= 0 ? left : right — unsigned lexicographic over the RAW
     * value bytes, greater wins. Value-carrying tombstones (the shape exoticCounterCellShapes
     * makes legal) with differing bytes therefore pick a winner independent of source order.
     * Pins both encounter orders plus a valued-vs-valued tie.
     */
    @Test
    public void counterTombstoneValueTieBreak() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c1 counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000 AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();
        ColumnMetadata c1 = metadata.getColumn(ByteBufferUtil.bytes("c1"));

        // recent ldt so the tied tombstones are retained (an epoch-old pair purges in both
        // paths and the tie-break is never exercised), identical across both sstables
        long ldt = org.apache.cassandra.utils.FBUtilities.nowInSeconds() - 60;
        long ts = 1000;

        // sstable 1: ck=0 empty-value first, ck=1 valued first, ck=2 valued (smaller bytes)
        applyCounterCell(metadata, 1L, 0L, new BufferCell(c1, ts, Cell.NO_TTL, ldt,
                                                          ByteBufferUtil.EMPTY_BYTE_BUFFER, null));
        applyCounterCell(metadata, 1L, 1L, new BufferCell(c1, ts, Cell.NO_TTL, ldt,
                                                          context(global(9, 1, 1)), null));
        applyCounterCell(metadata, 1L, 2L, new BufferCell(c1, ts, Cell.NO_TTL, ldt,
                                                          context(global(5, 1, 1)), null));
        flush();

        // sstable 2: the opposite shapes at identical (ts, ldt)
        applyCounterCell(metadata, 1L, 0L, new BufferCell(c1, ts, Cell.NO_TTL, ldt,
                                                          context(global(9, 1, 1)), null));
        applyCounterCell(metadata, 1L, 1L, new BufferCell(c1, ts, Cell.NO_TTL, ldt,
                                                          ByteBufferUtil.EMPTY_BYTE_BUFFER, null));
        applyCounterCell(metadata, 1L, 2L, new BufferCell(c1, ts, Cell.NO_TTL, ldt,
                                                          context(global(7, 2, 2)), null));
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    private static void applyCounterCell(TableMetadata metadata, long pk, long ck, Cell<?> cell)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(new BufferClustering(ByteBufferUtil.bytes(ck)));
        builder.addCell(cell);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(
            metadata, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(pk)), builder.build());
        new Mutation(update).apply();
    }

    private static ByteBuffer context(ShardSpec... shards)
    {
        int globals = 0, locals = 0, remotes = 0;
        for (ShardSpec s : shards)
        {
            if (s.kind == 0) globals++;
            else if (s.kind == 1) locals++;
            else remotes++;
        }
        CounterContext.ContextState state = CounterContext.ContextState.allocate(globals, locals, remotes);
        for (ShardSpec s : shards)
        {
            if (s.kind == 0) state.writeGlobal(CounterId.fromInt(s.id), s.clock, s.count);
            else if (s.kind == 1) state.writeLocal(CounterId.fromInt(s.id), s.clock, s.count);
            else state.writeRemote(CounterId.fromInt(s.id), s.clock, s.count);
        }
        return state.context;
    }

    private static ByteBuffer marked(ByteBuffer context)
    {
        return CounterContext.instance().markLocalToBeCleared(context);
    }

    private static ShardSpec global(int id, long clock, long count) { return new ShardSpec(0, id, clock, count); }
    private static ShardSpec local(int id, long clock, long count)  { return new ShardSpec(1, id, clock, count); }
    private static ShardSpec remote(int id, long clock, long count) { return new ShardSpec(2, id, clock, count); }

    private static final class ShardSpec
    {
        final int kind; final int id; final long clock; final long count;
        ShardSpec(int kind, int id, long clock, long count)
        {
            this.kind = kind; this.id = id; this.clock = clock; this.count = count;
        }
    }

    /** Purge boundary for counter tombstones: explicit gcBefore at and past the deletion second. */
    @Test
    public void counterTombstonePurge() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 8; ck++)
            execute("UPDATE %s SET c = c + ? WHERE pk = ? AND ck = ?", ck + 1, 1L, ck);
        flush();
        for (long ck = 0; ck < 4; ck++)
            execute("DELETE c FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        flush();

        long maxLdt = Long.MIN_VALUE;
        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : cfs.getLiveSSTables())
        {
            long ldt = sstable.getSSTableMetadata().maxLocalDeletionTime;
            if (ldt != Long.MAX_VALUE)
                maxLdt = Math.max(maxLdt, ldt);
        }
        org.junit.Assert.assertTrue("scenario produced no deletion times", maxLdt > 0 && maxLdt < Long.MAX_VALUE);

        // retained at the boundary, purged one past — note 7346: the purged tombstone's
        // shadowed counter cells must STAY dead (they were excluded before merge)
        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt);
        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt + 1);
    }
}
