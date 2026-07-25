package io.cassandra.trino.arrowflight;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import io.cassandra.trino.arrowflight.pushdown.AggregationPushdown;
import io.cassandra.trino.arrowflight.pushdown.PredicatePushdown;
import io.cassandra.trino.arrowflight.ticket.AggregationSpec;
import io.cassandra.trino.arrowflight.ticket.ArrowFlightTicket;
import io.cassandra.trino.arrowflight.ticket.FilterExpression;

/**
 * Schema/table discovery over the Cassandra Arrow Flight service's {@code ListFlights}/
 * {@code GetFlightInfo} descriptors (see {@code CassandraFlightProducer}), and Arrow-schema
 * &rarr; Trino column mapping via {@link ArrowTypeMapping}; plus predicate ({@link #applyFilter})
 * and aggregation ({@link #applyAggregation}) pushdown into the Flight ticket's {@code filter}/
 * {@code aggregation} clauses (see {@code ARROW-FLIGHT.md} and the {@code pushdown} package).
 *
 * <p>{@code arrow-flight.host}/{@code arrow-flight.port} remain the bootstrap contact point for
 * schema discovery only (the schema is uniform cluster-wide); actual scan routing goes through
 * per-split replica addresses computed by {@link io.cassandra.trino.arrowflight.topology.ArrowFlightTopologyService}
 * (see {@link ArrowFlightSplitManager}), not this class.
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
            flight.getFlightInfo(config.host(), config.port(), ArrowFlightTicket.of(tableName.getSchemaName(), tableName.getTableName()));
        }
        catch (org.apache.arrow.flight.FlightRuntimeException e)
        {
            if (e.status().code() == org.apache.arrow.flight.FlightStatusCode.NOT_FOUND)
                return null;
            throw e;
        }
        return ArrowFlightTableHandle.of(tableName.getSchemaName(), tableName.getTableName());
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

    /**
     * Translates {@code constraint.getSummary()} (a {@code TupleDomain<ColumnHandle>}) into the
     * ticket's {@code filter} clause (see {@link PredicatePushdown}). Only the summary domain is
     * handled - {@code constraint.getExpression()} (arbitrary {@code ConnectorExpression}s) is
     * left untouched for Trino to evaluate itself, matching most JDBC-style connectors' v1 scope.
     * Per-column translation is all-or-nothing (see {@link PredicatePushdown}), so this can push
     * down part of the constraint and correctly leave the rest in the returned
     * {@code remainingFilter} for Trino to still apply.
     *
     * <p><b>Idempotency (bug fixed here):</b> the Trino SPI contract allows - and in practice
     * does - invoke {@code applyFilter} again on the same table handle with a constraint that
     * includes what was already pushed down (e.g. across successive optimizer passes); a
     * connector is required to recognize that and return {@link Optional#empty()} once nothing
     * new is being added, rather than treating every call as incremental. This used to
     * unconditionally wrap {@code handle.filter()} (the previous call's translated tree) around a
     * fresh translation of whatever constraint arrived this time - for a plan requiring several
     * {@code applyFilter} passes over an unchanged constraint (observed with a Trino-rewritten
     * {@code LIKE 'prefix%'} range, though nothing here is specific to {@code LIKE}), that grew
     * the filter tree by one nesting level per call until it failed to serialize. Fixed by
     * tracking the raw, already-enforced {@link TupleDomain} (see
     * {@link ArrowFlightTableHandle#enforcedConstraint()}), short-circuiting when intersecting it
     * with the incoming constraint changes nothing, and otherwise re-translating the FULL
     * accumulated domain from scratch each time (replacing, not wrapping, the previous filter) -
     * both correct regardless of how many times Trino calls this for the same effective
     * constraint, and simpler than combining two already-translated trees.
     */
    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
        ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        TupleDomain<ColumnHandle> newConstraint = handle.enforcedConstraint().intersect(constraint.getSummary());

        if (newConstraint.equals(handle.enforcedConstraint()))
            // Nothing new beyond what's already enforced - must not re-translate/re-wrap, or a
            // plan that revisits this table handle several times would grow the filter tree by
            // one nesting level per visit (see javadoc above).
            return Optional.empty();

        PredicatePushdown.Result result = PredicatePushdown.translate(newConstraint);
        if (result.pushedDown().isEmpty())
            return Optional.empty();

        ArrowFlightTableHandle newHandle = handle.withFilter(newConstraint, result.pushedDown().get());
        return Optional.of(new ConstraintApplicationResult<>(newHandle, result.remaining(), constraint.getExpression(), false));
    }

    /**
     * Translates the requested aggregation into the ticket's {@code aggregation} clause (see
     * {@link AggregationPushdown}) - all-or-nothing per the {@code applyAggregation} SPI
     * contract: either every aggregate/the single grouping set is supported and the whole
     * aggregation is pushed down, or {@link Optional#empty()} and Trino computes it itself.
     */
    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
        ConnectorSession session,
        ConnectorTableHandle table,
        List<AggregateFunction> aggregates,
        Map<String, ColumnHandle> assignments,
        List<List<ColumnHandle>> groupingSets)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        if (handle.aggregation().isPresent())
            // Already aggregated by an earlier call - Trino shouldn't stack aggregations on an
            // aggregated handle, but decline rather than silently misbuild the ticket if it does.
            return Optional.empty();

        Optional<AggregationPushdown.Result> translated = AggregationPushdown.translate(aggregates, groupingSets);
        if (translated.isEmpty())
            return Optional.empty();

        AggregationSpec spec = translated.get().spec();
        ArrowFlightTableHandle newHandle = handle.withAggregation(spec, translated.get().mergePlan());

        return Optional.of(new AggregationApplicationResult<>(
            newHandle,
            translated.get().projections(),
            translated.get().assignments(),
            Map.of(),
            false));
    }

    private Schema arrowSchema(ArrowFlightTableHandle handle)
    {
        ArrowFlightTicket ticket = ArrowFlightTicket.of(handle.keyspace(), handle.table());
        if (handle.aggregation().isPresent())
            ticket = ticket.withAggregation(handle.aggregation().get());

        FlightInfo info = flight.getFlightInfo(config.host(), config.port(), ticket);
        return info.getSchemaOptional()
                   .orElseThrow(() -> new IllegalStateException(
                       "Cassandra Arrow Flight service returned no schema for "
                       + handle.keyspace() + "." + handle.table()));
    }
}
