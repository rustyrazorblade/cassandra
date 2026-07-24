package io.cassandra.trino.arrowflight;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;

/**
 * Schema/table discovery over the Cassandra Arrow Flight service's {@code ListFlights}/
 * {@code GetFlightInfo} descriptors (see {@code CassandraFlightProducer}), and Arrow-schema
 * &rarr; Trino column mapping via {@link ArrowTypeMapping}.
 *
 * <p><b>PoC scope</b>: no filter/limit/aggregation pushdown (matches the server, which always
 * streams the full unfiltered table - see {@code trino/README.md}).
 */
public class ArrowFlightMetadata implements ConnectorMetadata
{
    private final ArrowFlightConfig config;
    private final ArrowFlightClient flight;

    public ArrowFlightMetadata(ArrowFlightConfig config, ArrowFlightClient flight)
    {
        this.config = config;
        this.flight = flight;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        List<String> keyspaces = new ArrayList<>();
        for (SchemaTableName table : flight.listTables(config.host(), config.port()))
            if (!keyspaces.contains(table.getSchemaName()))
                keyspaces.add(table.getSchemaName());
        return keyspaces;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> tables = flight.listTables(config.host(), config.port());
        if (schemaName.isEmpty())
            return tables;
        List<SchemaTableName> filtered = new ArrayList<>();
        for (SchemaTableName table : tables)
            if (table.getSchemaName().equals(schemaName.get()))
                filtered.add(table);
        return filtered;
    }

    @Override
    public ConnectorTableHandle getTableHandle(
        ConnectorSession session,
        SchemaTableName tableName,
        Optional<ConnectorTableVersion> startVersion,
        Optional<ConnectorTableVersion> endVersion)
    {
        try
        {
            // GetFlightInfo returns NOT_FOUND (surfaced as a FlightRuntimeException) for an
            // unknown keyspace/table; a null return tells Trino the table does not exist.
            flight.getFlightInfo(config.host(), config.port(), tableName.getSchemaName(), tableName.getTableName());
        }
        catch (org.apache.arrow.flight.FlightRuntimeException e)
        {
            if (e.status().code() == org.apache.arrow.flight.FlightStatusCode.NOT_FOUND)
                return null;
            throw e;
        }
        return new ArrowFlightTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (Field field : arrowSchema((ArrowFlightTableHandle) table).getFields())
            handles.put(field.getName(), new ArrowFlightColumnHandle(field.getName(), ArrowTypeMapping.toTrinoType(field)));
        return handles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table, ColumnHandle columnHandle)
    {
        ArrowFlightColumnHandle column = (ArrowFlightColumnHandle) columnHandle;
        return new ColumnMetadata(column.name(), column.type());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        List<ColumnMetadata> columns = new ArrayList<>();
        for (Field field : arrowSchema(handle).getFields())
            columns.add(new ColumnMetadata(field.getName(), ArrowTypeMapping.toTrinoType(field)));
        return new ConnectorTableMetadata(handle.schemaTableName(), columns);
    }

    private Schema arrowSchema(ArrowFlightTableHandle handle)
    {
        FlightInfo info = flight.getFlightInfo(config.host(), config.port(), handle.keyspace(), handle.table());
        return info.getSchemaOptional()
                   .orElseThrow(() -> new IllegalStateException(
                       "Cassandra Arrow Flight service returned no schema for "
                       + handle.keyspace() + "." + handle.table()));
    }
}
