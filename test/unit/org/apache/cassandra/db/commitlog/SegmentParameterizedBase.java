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

package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.security.EncryptionContext;

import static org.junit.Assert.assertEquals;

/**
 * One parameterization from {@link CommitLogPropertyFixture#segmentParameterizations}, held for a
 * JUnit Parameterized test to apply and to check.
 *
 * A subclass takes the four values through its constructor, calls {@link #applySegmentConfiguration}
 * before it writes anything, and calls {@link #assertSegmentType} afterwards. The check is what stops a
 * configuration that fell back to another segment implementation from reporting as coverage of the one
 * it names.
 */
abstract class SegmentParameterizedBase
{
    private final ParameterizedClass commitLogCompression;
    private final EncryptionContext encryptionContext;
    private final Config.DiskAccessMode diskAccessMode;
    private final Class<? extends CommitLogSegment> expectedSegmentType;

    SegmentParameterizedBase(ParameterizedClass commitLogCompression,
                             EncryptionContext encryptionContext,
                             Config.DiskAccessMode diskAccessMode,
                             Class<? extends CommitLogSegment> expectedSegmentType)
    {
        this.commitLogCompression = commitLogCompression;
        this.encryptionContext = encryptionContext;
        this.diskAccessMode = diskAccessMode;
        this.expectedSegmentType = expectedSegmentType;
    }

    /** Skips the run when this parameterization needs direct IO and the file system cannot do it. */
    void applySegmentConfiguration()
    {
        CommitLogPropertyFixture.applySegmentConfiguration(commitLogCompression, encryptionContext, diskAccessMode);
    }

    void assertSegmentType()
    {
        assertEquals("this parameterization did not run the segment type it names",
                     expectedSegmentType, CommitLog.instance.segmentManager.allocatingFrom().getClass());
    }
}
