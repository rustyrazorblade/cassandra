#!/usr/bin/env bash
#
# Entrypoint for the docker-compose "cassandra" service (see ../docker-compose.yml).
#
# Runs the Cassandra checkout bind-mounted at $CASSANDRA_HOME (built on the HOST beforehand via
# `.build/sh/ai-build` - see trino/README.md) with a container-specific cassandra.yaml. The
# mounted repo's own conf/ directory is never modified in place - since it's a live bind mount of
# the host checkout, `sed -i` against it directly would permanently edit the user's real conf
# files. Everything needed is copied to a container-local directory first, and only the copy is
# patched.
set -euo pipefail

CASSANDRA_HOME="${CASSANDRA_HOME:-/cassandra}"
CONF_DIR=/etc/cassandra-docker

rm -rf "$CONF_DIR"
cp -r "$CASSANDRA_HOME/conf" "$CONF_DIR"

YAML="$CONF_DIR/cassandra.yaml"

# Bind to the compose service's own hostname (resolvable by other containers on the compose
# network) instead of localhost, listen for client traffic on all interfaces, and turn on the
# Arrow Flight service this branch adds. arrow_flight_port is left at its cassandra.yaml default
# (9143); override CASSANDRA_LISTEN_ADDRESS/CASSANDRA_SEEDS if this compose file's service name
# ever changes from "cassandra".
sed -i \
    -e "s/^listen_address: .*/listen_address: ${CASSANDRA_LISTEN_ADDRESS:-cassandra}/" \
    -e "s/^rpc_address: .*/rpc_address: 0.0.0.0/" \
    -e "s/^# broadcast_rpc_address: .*/broadcast_rpc_address: ${CASSANDRA_LISTEN_ADDRESS:-cassandra}/" \
    -e "s/- seeds: .*/- seeds: \"${CASSANDRA_SEEDS:-cassandra:7000}\"/" \
    -e "s/^start_arrow_flight: .*/start_arrow_flight: true/" \
    "$YAML"

# Point data/commitlog/hints/saved_caches at a dedicated path (backed by the "cassandra-data"
# docker volume - see docker-compose.yml) instead of cassandra.yaml's commented-out default of
# $CASSANDRA_HOME/data, which would otherwise write runtime data into the bind-mounted source
# checkout itself.
sed -i \
    -e "s|^# data_file_directories:|data_file_directories:|" \
    -e "s|^#     - /var/lib/cassandra/data|    - /var/lib/cassandra/data|" \
    -e "s|^# commitlog_directory: /var/lib/cassandra/commitlog|commitlog_directory: /var/lib/cassandra/commitlog|" \
    -e "s|^# saved_caches_directory: /var/lib/cassandra/saved_caches|saved_caches_directory: /var/lib/cassandra/saved_caches|" \
    -e "s|^# hints_directory: /var/lib/cassandra/hints|hints_directory: /var/lib/cassandra/hints|" \
    "$YAML"

mkdir -p /var/lib/cassandra/data /var/lib/cassandra/commitlog /var/lib/cassandra/saved_caches /var/lib/cassandra/hints

export CASSANDRA_CONF="$CONF_DIR"
# -R: this container runs as root (no dedicated non-root user is configured for this disposable
# local test stack), which bin/cassandra otherwise refuses to start under by default.
exec "$CASSANDRA_HOME/bin/cassandra" -f -R
