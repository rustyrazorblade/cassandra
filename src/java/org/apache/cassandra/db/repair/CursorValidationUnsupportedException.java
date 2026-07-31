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

import org.apache.cassandra.db.compaction.CursorCompactor;

/**
 * Thrown by {@link CursorValidationIterator}'s constructor when
 * {@link CursorCompactor#isValidationSupported} rejects the ACTUAL sstable set/controller this
 * iterator would validate against (checked as one of the first things the constructor does,
 * before any further side effects). Deliberately a distinct, narrow exception type - not a bare
 * {@code IllegalStateException} - so {@link CassandraTableRepairManager} can catch exactly this
 * case to fall back to {@link CassandraValidationIterator}, without silently swallowing an
 * unrelated bug that happens to also throw {@code IllegalStateException} deeper in construction.
 */
public class CursorValidationUnsupportedException extends RuntimeException
{
    public CursorValidationUnsupportedException(String message)
    {
        super(message);
    }
}
