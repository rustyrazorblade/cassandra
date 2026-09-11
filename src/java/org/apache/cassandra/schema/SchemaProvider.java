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

package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.tcm.ClusterMetadata;

public interface SchemaProvider
{
    Set<String> getKeyspaces();
    int getNumberOfTables();

    ClusterMetadata submit(SchemaTransformation transformation);

    default UUID getVersion()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null)
            return null;
        return metadata.schema.getVersion();
    }

    Keyspaces localKeyspaces();
    Keyspaces distributedKeyspaces();
    Keyspaces distributedAndLocalKeyspaces();
    Keyspaces getUserKeyspaces();

    void registerListener(SchemaChangeListener listener);
    void unregisterListener(SchemaChangeListener listener);

    SchemaChangeNotifier schemaChangeNotifier();

    Optional<TableMetadata> getIndexMetadata(String keyspace, String index);

    default TableMetadata getTableMetadata(Descriptor descriptor)
    {
        return getTableMetadata(descriptor.ksname, descriptor.cfname);
    }

    default TableMetadataRef getTableMetadataRef(Descriptor descriptor)
    {
        return getTableMetadata(descriptor.ksname, descriptor.cfname).ref;
    }

    default ViewMetadata getView(String keyspaceName, String viewName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = distributedKeyspaces().getNullable(keyspaceName);
        return (ksm == null) ? null : ksm.views.getNullable(viewName);
    }

    default Keyspaces getNonLocalStrategyKeyspaces()
    {
        return distributedKeyspaces().filter(keyspace -> keyspace.params.replication.klass != LocalStrategy.class);
    }

    default TableMetadata validateTable(String keyspaceName, String tableName)
    {
        if (tableName.isEmpty())
            throw new InvalidRequestException("non-empty table is required");

        KeyspaceMetadata keyspace = getKeyspaceMetadata(keyspaceName);
        if (keyspace == null)
            throw new KeyspaceNotDefinedException(String.format("keyspace %s does not exist", keyspaceName));

        TableMetadata metadata = keyspace.getTableOrViewNullable(tableName);
        if (metadata == null)
            throw new InvalidRequestException(String.format("table %s does not exist", tableName));

        return metadata;
    }

    default ColumnFamilyStore getColumnFamilyStoreInstance(TableId id)
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata == null)
            return null;

        Keyspace instance = getKeyspaceInstance(metadata.keyspace);
        if (instance == null)
            return null;

        return instance.hasColumnFamilyStore(metadata.id)
               ? instance.getColumnFamilyStore(metadata.id)
               : null;
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace
     * @return metadata about ColumnFamilies the belong to the given keyspace
     */
    Iterable<TableMetadata> getTablesAndViews(String keyspaceName);

    @Nullable
    Keyspace getKeyspaceInstance(String keyspaceName);

    @Nullable
    KeyspaceMetadata getKeyspaceMetadata(String keyspaceName);

    @Nullable
    TableMetadata getTableMetadata(TableId id);

    @Nullable
    default IPartitioner getTablePartitioner(TableId id)
    {
        TableMetadata metadata = getTableMetadata(id);
        return metadata == null ? null : metadata.partitioner;
    }

    default IPartitioner getExistingTablePartitioner(TableId id) throws UnknownTableException
    {
        return getExistingTableMetadata(id).partitioner;
    }

    @Nullable
    default TableMetadataRef getTableMetadataRef(TableId id)
    {
        return getTableMetadata(id).ref;
    }

    @Nullable
    TableMetadata getTableMetadata(String keyspace, String table);

    default TableMetadataRef getTableMetadataRef(String keyspace, String table)
    {
        return getTableMetadata(keyspace, table).ref;
    }

    @Nullable
    default ColumnMetadata getColumnMetadata(String keyspace, String table, ColumnIdentifier name)
    {
        TableMetadata metadata = getTableMetadata(keyspace, table);
        if (metadata == null) return null;
        return metadata.getColumn(name);
    }

    @Nullable
    default ColumnMetadata getColumnMetadata(String keyspace, String table, ByteBuffer name)
    {
        TableMetadata metadata = getTableMetadata(keyspace, table);
        if (metadata == null) return null;
        return metadata.getColumn(name);
    }

    default TableMetadata getExistingTableMetadata(TableId id) throws UnknownTableException
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata != null)
            return metadata;

        String message = "Couldn't find table with id " + id + ". If a table was just created, this is likely due to the schema "
                          + "not being fully propagated.  Please wait for schema agreement on table creation.";
        throw new UnknownTableException(message, id);
    }

    /**
     * Compute the largest gc grace seconds amongst all the tables
     * @return the largest gcgs.
     */
    default int largestGcgs()
    {
        return distributedAndLocalKeyspaces().stream()
                                        .flatMap(ksm -> ksm.tables.stream())
                                        .mapToInt(tm -> tm.params.gcGraceSeconds)
                                        .max()
                                        .orElse(Integer.MIN_VALUE);
    }

    // TODO: remove?
    public abstract void saveSystemKeyspace();
}
