package io.cassandra.trino.arrowflight;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

/**
 * One projected column: its Arrow field name (also the Trino column name) and its mapped
 * Trino {@link Type} (see {@link ArrowTypeMapping}).
 */
public record ArrowFlightColumnHandle(String name, Type type) implements ColumnHandle
{
}
