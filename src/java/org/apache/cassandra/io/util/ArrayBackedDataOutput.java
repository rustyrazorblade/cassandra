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

package org.apache.cassandra.io.util;

import java.io.IOException;

/**
 * An output that can take bytes straight off a {@link DataInputPlus} into its own heap array,
 * so a copy loop through a caller's transfer buffer is not needed.
 *
 * A producer that would otherwise read into scratch and then write the scratch out — see
 * {@code SSTableCursorReader.copyCellContents} — can ask for this instead and make one copy where
 * it would have made two.
 */
public interface ArrayBackedDataOutput
{
    /** Whether this output currently holds a heap array {@link #readFully} can land bytes in. */
    boolean hasArray();

    /**
     * Appends exactly {@code length} bytes read from {@code in} to this output, in one copy.
     * Only valid when {@link #hasArray()} is true.
     */
    void readFully(DataInputPlus in, int length) throws IOException;
}
