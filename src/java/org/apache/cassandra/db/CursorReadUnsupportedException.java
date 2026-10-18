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

package org.apache.cassandra.db;

/**
 * A read only the iterator path can serve, asked of a reader that runs only cursor code
 * ({@link CursorRangeScan}), which never falls back to the iterator path.
 */
public final class CursorReadUnsupportedException extends RuntimeException
{
    /** The range read gate's reason, or null when the reason is the command's own ({@link #getMessage}). */
    public final CursorRangeReads.Unsupported gateReason;

    CursorReadUnsupportedException(CursorRangeReads.Unsupported gateReason, String message)
    {
        super(message);
        this.gateReason = gateReason;
    }

    CursorReadUnsupportedException(String message)
    {
        this(null, message);
    }
}
