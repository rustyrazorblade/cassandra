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
package org.apache.cassandra.io.sstable.format;

import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.Config.FlushCompression;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * async_compaction_writer_enabled applies to the compaction family only. Every OperationType is
 * classified, and the writer DataComponent builds must follow that classification.
 */
public class DataComponentAsyncWriterSelectionTest
{
    private static final String KS = "ks_async_selection";
    private static final String CF = "cf_async_selection";

    private static final Set<OperationType> COMPACTION_FAMILY = EnumSet.of(OperationType.COMPACTION,
                                                                           OperationType.MAJOR_COMPACTION,
                                                                           OperationType.ANTICOMPACTION,
                                                                           OperationType.CLEANUP,
                                                                           OperationType.UPGRADE_SSTABLES,
                                                                           OperationType.GARBAGE_COLLECT,
                                                                           OperationType.TOMBSTONE_COMPACTION,
                                                                           OperationType.RELOCATE);

    private File tmpDir;
    private int nextId;
    private boolean savedEnabled;
    private DiskAccessMode savedMode;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws Exception
    {
        tmpDir = new File(Files.createTempDirectory("async_selection"));
        nextId = 0;
        savedEnabled = DatabaseDescriptor.asyncCompactionWriterEnabled();
        savedMode = DatabaseDescriptor.getBackgroundWriteDiskAccessMode();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(savedEnabled);
        DatabaseDescriptor.setBackgroundWriteDiskAccessMode(savedMode);
        if (tmpDir != null)
            FileUtils.deleteRecursive(tmpDir);
    }

    // Pinned here rather than read from the classification, so a wrong entry there fails this test.
    @Test
    public void onlyTheCompactionFamilyIsClassifiedForTheAsyncWriter()
    {
        for (OperationType op : OperationType.values())
            assertEquals("async writer classification for " + op,
                         COMPACTION_FAMILY.contains(op), DataComponent.isAsyncWriterSupported(op));
    }

    @Test
    public void writersFollowTheClassificationWhenEnabled()
    {
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(true);
        for (DiskAccessMode mode : new DiskAccessMode[]{ DiskAccessMode.standard, DiskAccessMode.direct })
        {
            DatabaseDescriptor.setBackgroundWriteDiskAccessMode(mode);
            for (OperationType op : OperationType.values())
                assertEquals(mode + ", " + op, COMPACTION_FAMILY.contains(op), usesPipeline(op));
        }
    }

    @Test
    public void scrubFlushStreamAndWriteNeverGetAPipeline()
    {
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(true);
        for (OperationType op : new OperationType[]{ OperationType.SCRUB, OperationType.FLUSH, OperationType.STREAM,
                                                     OperationType.WRITE, OperationType.UNKNOWN })
            assertFalse(op + " must write inline", usesPipeline(op));
        assertTrue(usesPipeline(OperationType.COMPACTION));
    }

    @Test
    public void noWriterGetsAPipelineWhenDisabled()
    {
        DatabaseDescriptor.setAsyncCompactionWriterEnabled(false);
        for (OperationType op : OperationType.values())
            assertFalse(op + " must write inline with the option off", usesPipeline(op));
    }

    private boolean usesPipeline(OperationType op)
    {
        Descriptor descriptor = new Descriptor(tmpDir, KS, CF, new SequenceBasedSSTableId(nextId++),
                                               DatabaseDescriptor.getSelectedSSTableFormat());
        MetadataCollector collector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        SequentialWriterOption options = SequentialWriterOption.newBuilder().finishOnClose(false).build();
        try (SequentialWriter w = DataComponent.buildWriter(descriptor, CompressionParams.lz4(), options, collector,
                                                            op, FlushCompression.table, null))
        {
            return ((CompressedSequentialWriter) w).usesAsyncPipeline();
        }
    }
}
