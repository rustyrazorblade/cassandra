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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.quicktheories.impl.Constraint;
import org.quicktheories.impl.JavaRandom;


/**
 * Schema generation, mutation generation and replay, shared by the commit log property and model tests.
 *
 * The generated schemas are deliberately narrower than the type generator can produce:
 *
 * - Single-cell types only. RowUpdateBuilder takes a Java collection for a multi-cell value, not the raw
 *   buffer the generator produces.
 * - No user-defined types. Registering a generated UDT means putting the type in the keyspace before the
 *   table, and the round trip has nothing to say about UDTs that the serializer size property does not.
 * - No counters. Their serialized form comes from the local counter context, not the generated value.
 * - One partition key column, because a composite key would have to be handed to RowUpdateBuilder as
 *   components rather than as a composed buffer.
 *
 * Frozen collections, tuples, vectors and every primitive still reach the serializer through these.
 *
 * Closing those four gaps means building the mutations by executing CQL INSERT statements rather than
 * assembling them here, which is how RandomDifferentialCompactionTest generates its workload. CQL does
 * the type plumbing, so all four come at once. It replaces this class's mutation building, which every
 * property test depends on, so it is a rewrite of that half rather than a patch.
 */
final class CommitLogPropertyFixture
{
    /** Every generated table carries this, so a workload can make a mutation arbitrarily wide. */
    static final String PAD_COLUMN = "pad_blob";

    private CommitLogPropertyFixture()
    {
    }

    static TableMetadata generateTable(String keyspace, JavaRandom random, int index)
    {
        TypeGenBuilder primary = AbstractTypeGenerators.withoutUnsafeEquality()
                                                       .withMaxDepth(1)
                                                       .withMultiCell(false);
        TableMetadata generated =
            new TableMetadataBuilder()
            .withKeyspaceName(keyspace)
            .withTableName("t" + index)
            .withTableKinds(TableMetadata.Kind.REGULAR)
            .withSimpleColumnNames()
            .withDefaultTypeGen(AbstractTypeGenerators.builder()
                                                      .withoutEmpty()
                                                      .withMaxDepth(2)
                                                      .withMultiCell(false)
                                                      .withDefaultSetKey(primary)
                                                      .withoutTypeKinds(AbstractTypeGenerators.TypeKind.COUNTER,
                                                                        AbstractTypeGenerators.TypeKind.UDT))
            .withPrimaryColumnTypeGen(primary)
            .withPartitionColumnsCount(1)
            .withClusteringColumnsBetween(0, 3)
            .withRegularColumnsBetween(1, 4)
            .withStaticColumnsBetween(0, 0)
            .build(random);

        return generated.unbuild().addRegularColumn(PAD_COLUMN, BytesType.instance).build();
    }

    static Mutation generateMutation(TableMetadata metadata, JavaRandom random)
    {
        return buildMutation(metadata, generateRow(metadata, random), null);
    }

    static ByteBuffer[] generateRow(TableMetadata metadata, JavaRandom random)
    {
        return CassandraGenerators.data(metadata, null).generate(random);
    }

    /** {@code pad} overrides the generated value of the pad column when it is not null. */
    static Mutation buildMutation(TableMetadata metadata, ByteBuffer[] row, ByteBuffer pad)
    {
        int clusteringColumns = metadata.clusteringColumns().size();

        RowUpdateBuilder builder = new RowUpdateBuilder(metadata, 1L, row[0]);
        if (clusteringColumns > 0)
        {
            Object[] clustering = new Object[clusteringColumns];
            System.arraycopy(row, 1, clustering, 0, clusteringColumns);
            builder = builder.clustering(clustering);
        }

        // the generator produced a value for every column, so the cursor advances for all of them;
        // skipping one shifts every value after it into the wrong column and the type rejects it
        int valueIndex = 1 + clusteringColumns;
        for (ColumnMetadata column : metadata.regularColumns())
        {
            ByteBuffer generated = row[valueIndex++];
            ByteBuffer value = pad != null && column.name.toString().equals(PAD_COLUMN) ? pad : generated;
            if (value != null)
                builder = builder.add(column, value);
        }
        return builder.build();
    }

    /**
     * A mutation whose serialized size is exactly {@code target} bytes, by widening the pad column and
     * correcting for the vint that encodes its length.
     *
     * The row is generated once and only the pad varies. Regenerating it per attempt moves the base size
     * under the correction and the loop never converges.
     */
    static Mutation mutationOfExactSize(TableMetadata metadata, JavaRandom random, int target, int version)
    {
        ByteBuffer[] row = generateRow(metadata, random);
        byte[] padding = new byte[Math.max(0, target)];
        new Random(random.next(Constraint.between(Long.MIN_VALUE, Long.MAX_VALUE))).nextBytes(padding);

        int pad = Math.max(0, target - 512);
        for (int attempt = 0; attempt < 12; attempt++)
        {
            Mutation mutation = buildMutation(metadata, row, ByteBuffer.wrap(padding, 0, pad));
            int actual = mutation.serializedSize(version);
            if (actual == target)
                return mutation;
            pad += target - actual;
            if (pad < 0 || pad > padding.length)
                throw new IllegalArgumentException("this schema cannot reach a serialized size of " + target
                                                   + "; its overhead alone puts it outside the range");
        }
        throw new IllegalStateException("could not hit an exact serialized size of " + target);
    }

    /**
     * Replays the active segments and returns the serialized form of every mutation for the given table,
     * in file order. Serialized bytes rather than objects, because a replayed mutation is a fresh object
     * graph and Mutation has no value equality.
     */
    static List<ByteBuffer> replay(TableMetadata metadata, CommitLogPosition from) throws IOException
    {
        return replay(metadata, from, activeSegmentFiles());
    }

    /**
     * Replays the given files. Damage tests hand in copies: the active segments are memory-mapped by the
     * running commit log, and truncating one under its own mapping raises SIGBUS in whoever touches it
     * next, which takes the JVM with it. A stopped node's files are what replay reads in production
     * anyway.
     */
    static List<ByteBuffer> replay(TableMetadata metadata, CommitLogPosition from, File[] files) throws IOException
    {
        if (files.length == 0)
            return Collections.emptyList();
        Arrays.sort(files, new CommitLogSegment.CommitLogSegmentFileComparator());

        CollectingReplayer replayer = new CollectingReplayer(metadata, from);
        replayer.replayFiles(files);
        return replayer.collected;
    }

    /** Copies the active segments into {@code destination} and returns the copies. */
    static File[] copyActiveSegments(File destination) throws IOException
    {
        destination.tryCreateDirectory();
        File[] source = activeSegmentFiles();
        File[] copies = new File[source.length];
        for (int i = 0; i < source.length; i++)
        {
            copies[i] = new File(destination, source[i].name());
            Files.copy(source[i].toPath(), copies[i].toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        return copies;
    }

    private static File[] activeSegmentFiles()
    {
        // no segments is not an error: a commit log that has been written to and then reset holds
        // nothing, and replaying it must produce nothing rather than fail. A caller that wrote
        // mutations first will see the empty result fail its own comparison, which is the honest place
        // for that to be caught.
        List<String> activeSegments = CommitLog.instance.getActiveSegmentNames();
        if (activeSegments.isEmpty())
            return new File[0];
        File[] files = new File(CommitLog.instance.segmentManager.storageDirectory)
                       .tryList((dir, name) -> activeSegments.contains(name));
        return files == null ? new File[0] : files;
    }

    static ByteBuffer bytes(Mutation mutation)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Mutation.serializer.serialize(mutation, out, MessagingService.current_version);
            return ByteBuffer.wrap(out.toByteArray());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static final class CollectingReplayer extends CommitLogReplayer
    {
        final List<ByteBuffer> collected = new ArrayList<>();
        private final TableMetadata metadata;
        private final CommitLogPosition from;

        CollectingReplayer(TableMetadata metadata, CommitLogPosition from)
        {
            super(CommitLog.instance, from, Collections.emptyMap(), ReplayFilter.create());
            this.metadata = metadata;
            this.from = from;
        }

        @Override
        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        {
            // the reader deserializes entries before the requested position too, so the filter has to be
            // applied here as well; SimpleCountingReplayer in CommitLogTest does the same
            if (entryLocation <= from.position)
                return;
            // the system keyspaces write to the same log and would flake every comparison
            if (m.getPartitionUpdates().stream().anyMatch(u -> u.metadata().id.equals(metadata.id)))
                collected.add(bytes(m));
        }
    }
}
