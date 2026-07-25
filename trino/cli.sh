#!/usr/bin/env bash
#
# Convenience wrapper for running queries against the Trino container from ../docker-compose.yml
# (see README.md's "Loading test data and querying end-to-end" section). Uses the CLI already
# bundled in the trino image inside the running container, rather than requiring a separate local
# Trino CLI install.
#
# Usage:
#   ./cli.sh                              # interactive trino> shell, preset to the arrow_flight
#                                          # catalog / cassandra_easy_stress schema
#   ./cli.sh --execute "SELECT ..."        # one-shot query, passed straight through to `trino`
#   ./cli.sh <any other trino CLI args>    # forwarded as-is (e.g. --catalog/--schema overrides)
set -euo pipefail

CONTAINER="${TRINO_CONTAINER:-arrow-flight-trino}"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "error: container '$CONTAINER' is not running - start the stack first:" >&2
    echo "    docker compose up -d cassandra sidecar trino" >&2
    exit 1
fi

if [[ $# -eq 0 ]]; then
    exec docker exec -it "$CONTAINER" trino --catalog arrow_flight --schema cassandra_easy_stress
else
    exec docker exec -i "$CONTAINER" trino --catalog arrow_flight --schema cassandra_easy_stress "$@"
fi
