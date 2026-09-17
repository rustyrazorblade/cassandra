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

/**
 * The {@link MergeSeekSliceCursorReadDifferentialTest} corpus under the BTI format, the
 * configuration the merged-leg seek was built for. With {@link #seekCapableFormat()} true, every
 * scenario enforces the merged-mode seek-effectiveness guard on top of the differential
 * comparison: exact per-leg seek counts via {@code CursorReads.sstableLegRowIndexSeeks()} and
 * block-granular bounds on merged materialization via {@code CursorReads.unfilteredsMaterialized()}.
 * The wrong-seed mutation test, skipped under BIG where no seed exists to corrupt, runs here. A
 * merged cursor path that reverted to the eager per-leg walk would still pass the differential
 * comparison; this class is what fails it.
 */
public class BtiMergeSeekSliceCursorReadDifferentialTest extends MergeSeekSliceCursorReadDifferentialTest
{
    @Override
    protected String formatName()
    {
        return "bti";
    }

    @Override
    protected boolean seekCapableFormat()
    {
        return true;
    }
}
