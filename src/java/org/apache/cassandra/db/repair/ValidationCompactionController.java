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

package org.apache.cassandra.db.repair;

import java.util.function.LongPredicate;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionController;

/**
 * Controller for validation compaction that always purges.
 * Note that we should not call cfs.getOverlappingSSTables on the provided
 * sstables because those sstables are not guaranteed to be active sstables
 * (since we can run repair on a snapshot).
 * <p>
 * Extracted to a top-level, public class (originally a private nested class of
 * {@link CassandraValidationIterator}) so both the legacy and cursor-backed validation paths can
 * share exactly one construction site.
 * <p>
 * Constructed with {@code compacting = null} via the {@link CompactionController#CompactionController(ColumnFamilyStore, long)}
 * 2-arg constructor, {@link CompactionController#shadowSources} always returns null/empty
 * regardless of {@code tombstoneOption} - see {@code ValidationCompactionControllerTest} (three
 * cases, one per {@code tombstoneOption} value) for the regression tests pinning this invariant,
 * which {@link #guaranteesNoShadowSources} exposes so
 * {@code CursorCompactor#isValidationSupported} can rely on it instead of checking
 * {@code tombstoneOption} directly or checking for this concrete controller type.
 */
public class ValidationCompactionController extends CompactionController
{
    public ValidationCompactionController(ColumnFamilyStore cfs, long gcBefore)
    {
        super(cfs, gcBefore);
    }

    @Override
    public boolean guaranteesNoShadowSources()
    {
        return true;
    }

    @Override
    public LongPredicate getPurgeEvaluator(DecoratedKey key)
    {
        /*
         * The main reason we always purge is that including gcable tombstone would mean that the
         * repair digest will depends on the scheduling of compaction on the different nodes. This
         * is still not perfect because gcbefore is currently dependend on the current time at which
         * the validation compaction start, which while not too bad for normal repair is broken for
         * repair on snapshots. A better solution would be to agree on a gcbefore that all node would
         * use, and we'll do that with CASSANDRA-4932.
         * Note validation compaction includes all sstables, so we don't have the problem of purging
         * a tombstone that could shadow a column in another sstable, but this is doubly not a concern
         * since validation compaction is read-only.
         */
        return time -> true;
    }
}
