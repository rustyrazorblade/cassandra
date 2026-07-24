package io.cassandra.trino.arrowflight;

import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * This connector is read-only with no cross-call state, so a single shared, stateless
 * transaction handle is sufficient (matches the Cassandra-side service's read-only PoC scope).
 */
public enum ArrowFlightTransactionHandle implements ConnectorTransactionHandle
{
    INSTANCE
}
