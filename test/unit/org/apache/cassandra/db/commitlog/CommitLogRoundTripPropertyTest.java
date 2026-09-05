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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.crypto.Cipher;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.security.CipherFactory;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.quicktheories.impl.JavaRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Everything written to the commit log comes back out of it, unchanged and in order.
 *
 * The oracle is the list of mutations the test wrote. There is no reference implementation and no model,
 * so the property survives any change to how the bytes get onto disk as long as replay still returns
 * what was written. That is what makes it a regression net: it constrains the observable behaviour of
 * the write path rather than its structure.
 *
 * Mutations are compared by their serialized bytes rather than by object equality, because a replayed
 * mutation is a fresh object graph and Mutation has no value equality.
 *
 * Each parameterization asserts which segment implementation actually ran. A configuration that quietly
 * fell back to the memory-mapped path would otherwise report six passes for one code path.
 *
 * What this test cannot see:
 *
 * - User-defined types. Registering a generated UDT needs the type in the keyspace before the table, and
 *   the round trip has nothing to say about UDTs that the serializer property does not already cover.
 * - Counters, and multi-cell collections. RowUpdateBuilder wants a Java collection rather than the
 *   generator's raw buffer for multi-cell values.
 * - Sync modes other than the configured default. Sync mode governs when add returns, not what the
 *   segment holds.
 */
@RunWith(Parameterized.class)
public class CommitLogRoundTripPropertyTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogRoundTripPropertyTest.class);

    private static final String KEYSPACE = "commitlog_roundtrip_property";
    private static final int EXAMPLES = CassandraRelevantProperties.TEST_COMMITLOG_EXAMPLES.getInt();
    private static final int MUTATIONS = CassandraRelevantProperties.TEST_COMMITLOG_MUTATIONS_PER_EXAMPLE.getInt();

    /** Generated once so the keyspace is built once; an example picks one of them. */
    private static final int TABLES = 8;
    private static final List<TableMetadata> TABLES_GENERATED = new ArrayList<>(TABLES);

    private final Class<? extends CommitLogSegment> expectedSegmentType;

    public CommitLogRoundTripPropertyTest(ParameterizedClass commitLogCompression,
                                          EncryptionContext encryptionContext,
                                          Class<? extends CommitLogSegment> expectedSegmentType)
    {
        this.expectedSegmentType = expectedSegmentType;
        DatabaseDescriptor.setCommitLogCompression(commitLogCompression);
        DatabaseDescriptor.setEncryptionContext(encryptionContext);
        DatabaseDescriptor.initializeCommitLogDiskAccessMode();
    }

    @Parameters(name = "{2}")
    public static Collection<Object[]> generateData() throws Exception
    {
        return Arrays.asList(new Object[][]
                             {
                             { null, EncryptionContextGenerator.createDisabledContext(), MemoryMappedSegment.class },
                             { null, newEncryptionContext(), EncryptedSegment.class },
                             { new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap()),
                               EncryptionContextGenerator.createDisabledContext(), CompressedSegment.class },
                             { new ParameterizedClass(DeflateCompressor.class.getName(), Collections.emptyMap()),
                               EncryptionContextGenerator.createDisabledContext(), CompressedSegment.class },
                             { new ParameterizedClass(ZstdCompressor.class.getName(), Collections.emptyMap()),
                               EncryptionContextGenerator.createDisabledContext(), CompressedSegment.class }
                             });
    }

    private static EncryptionContext newEncryptionContext() throws Exception
    {
        EncryptionContext context = EncryptionContextGenerator.createContext(true);
        CipherFactory cipherFactory = new CipherFactory(context.getTransparentDataEncryptionOptions());
        Cipher cipher = cipherFactory.getEncryptor(context.getTransparentDataEncryptionOptions().cipher,
                                                   context.getTransparentDataEncryptionOptions().key_alias);
        return EncryptionContextGenerator.createContext(cipher.getIV(), true);
    }

    @BeforeClass
    public static void beforeClass()
    {
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        SchemaLoader.prepareServer();

        long schemaSeed = CassandraRelevantProperties.TEST_COMMITLOG_SEED.getLong(System.currentTimeMillis());
        logger.info("schema seed={}, examples={}, mutations per example={}", schemaSeed, EXAMPLES, MUTATIONS);
        JavaRandom random = new JavaRandom(schemaSeed);
        for (int i = 0; i < TABLES; i++)
            TABLES_GENERATED.add(CommitLogPropertyFixture.generateTable(KEYSPACE, random, i));

        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    TABLES_GENERATED.toArray(new TableMetadata[0]));
    }

    @Before
    public void before() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @Test
    public void everythingWrittenReplays() throws Throwable
    {
        new CommitLogSeedRunner(EXAMPLES).run(seed -> {
            CommitLog.instance.resetUnsafe(true);

            JavaRandom random = new JavaRandom(seed);
            Random workload = new Random(seed);
            TableMetadata metadata = TABLES_GENERATED.get(workload.nextInt(TABLES));

            List<Mutation> written = new ArrayList<>(MUTATIONS);
            for (int i = 0; i < MUTATIONS; i++)
            {
                Mutation mutation = CommitLogPropertyFixture.generateMutation(metadata, random);
                CommitLog.instance.add(mutation);
                written.add(mutation);
            }
            CommitLog.instance.sync(true);

            // configuration hides coverage: prove the parameterization selected the segment it names
            CommitLogSegment segment = CommitLog.instance.segmentManager.allocatingFrom();
            assertEquals("this parameterization did not run the segment type it names",
                         expectedSegmentType, segment.getClass());

            List<ByteBuffer> replayed = CommitLogPropertyFixture.replay(metadata, CommitLogPosition.NONE);
            List<ByteBuffer> expected = new ArrayList<>(written.size());
            for (Mutation m : written)
                expected.add(CommitLogPropertyFixture.bytes(m));
            assertEquals("what replayed differs from what was written, schema:\n"
                         + metadata.toCqlString(true, false, false),
                         expected, replayed);
        });
    }

}
