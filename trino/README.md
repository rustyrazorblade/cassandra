# Cassandra Arrow Flight Trino Connector

A minimal Trino connector plugin that reads Cassandra table data through the embedded Arrow
Flight service added on this branch (`org.apache.cassandra.arrow.*`, see
`../ARROW-FLIGHT.md` and `src/java/org/apache/cassandra/arrow/` at the repo root), instead of
Trino's existing CQL-based Cassandra connector.

**This directory is a separate, self-contained Gradle project.** It does not touch, depend on,
or get built by Cassandra's own Ant build; nothing outside `trino/` was changed to produce it.

## Scope (PoC, matches the server)

The Cassandra-side service this connector talks to is an explicit proof-of-concept:

- **No authentication.** Anyone who can open a TCP connection to the Flight port can read every
  row of every user table. Do not point this at a Cassandra node reachable from an untrusted
  network.
- **No filter/limit/aggregation pushdown.** Every scan reads the whole table; Trino applies
  `WHERE`/`LIMIT`/aggregation client-side, which is correct, just not as fast as server-side
  pushdown would be.
- **No token-range splitting.** `GetFlightInfo` always returns exactly one Flight endpoint
  covering a table's entire local range, so this connector emits exactly one split per table -
  no read parallelism yet. When the server gains real splitting, `ArrowFlightSplitManager` is
  the seam to extend.
- **Full-table scan only.** No point-read API.

This connector is a faithful, equally-scoped PoC client for that service - it does not
implement anything the server doesn't yet support.

## Versions targeted

- **Trino 481** - the current stable release at the time this connector was written. Trino 479+
  requires **JDK 25** to build and run; verify you're still on a current release before relying
  on this (`trino.io/docs/current/release/release-481.html`); bump `trinoVersion` in
  `build.gradle.kts` if a newer stable release has since shipped, and re-check the SPI method
  signatures used in `ArrowFlightMetadata`/`ArrowFlightPageSourceProvider`/etc. against the new
  version - Trino's connector SPI evolves across releases.
- **Apache Arrow (Java) 19.0.0** - pinned to match the version already vendored on the
  Cassandra side (`lib/arrow-vector-19.0.0.jar`, `.build/cassandra-deps-maven-pom.xml`) so the
  client and server negotiate the same Arrow IPC format.
- **Netty 4.1.130.Final**, explicitly pinned (see the comment block in `build.gradle.kts`).
  Arrow-java 19's off-heap allocator is incompatible with Netty 4.2.x (a `NettyAllocationManager`
  static-init failure on the very first `RootAllocator`); `flight-core:19.0.0`'s transitive Netty
  requests land on 4.1.130.Final under Maven's nearest-wins, but Gradle's module-metadata
  resolution can float several Netty modules up to 4.2.x, so this is pinned explicitly rather
  than left to chance.

The build auto-provisions a JDK 25 toolchain via the `foojay-resolver-convention` Gradle plugin
(`settings.gradle.kts`), so `./gradlew` works even if your shell's default JDK is older (Cassandra
itself commonly runs on JDK 11/17/21).

## Building

```bash
cd trino
./gradlew build          # compiles, runs unit tests, produces build/libs/*.jar
./gradlew installPlugin  # additionally assembles build/plugin/cassandra_arrow_flight/
                          # (this connector's jar + every runtime dependency, flattened -
                          # exactly the directory shape Trino's plugin loader expects)
```

`./gradlew test` runs just the unit tests (see [Tests](#tests) below).

## Installing into a Trino server

Trino loads each plugin from its own directory of jars under `plugin/<name>/`, and each catalog
from a `.properties` file under `etc/catalog/`. The two are wired together via
`connector.name` in the properties file (must match `ArrowFlightConnectorFactory.NAME`,
`cassandra_arrow_flight`).

### Option A: Docker (fastest way to verify manually)

```bash
cd trino
./gradlew installPlugin

docker run -d --name trino-arrow-flight \
  -p 8080:8080 \
  -v "$(pwd)/build/plugin/cassandra_arrow_flight:/usr/lib/trino/plugin/cassandra_arrow_flight" \
  -v "$(pwd)/catalog/arrow_flight.properties:/etc/trino/catalog/arrow_flight.properties" \
  trinodb/trino:481

# wait for it to come up, then:
docker exec -it trino-arrow-flight trino
```

If your Cassandra node isn't reachable at `127.0.0.1` from inside the container (it usually
isn't - the container has its own network namespace), edit
`catalog/arrow_flight.properties` first: set `arrow-flight.host` to an address the container can
reach (e.g. the host's LAN IP, or `host.docker.internal` on Docker Desktop), or run the container
with `--network host` on Linux.

### Option B: a manual Trino server install

1. Download and unpack a Trino server tarball for the same version:
   `https://repo1.maven.org/maven2/io/trino/trino-server/481/trino-server-481.tar.gz`
   (see `trino.io/docs/current/installation/deployment.html` for full server setup - JVM config,
   `node.properties`, `config.properties`, etc.; that setup is unrelated to this connector).
2. `./gradlew installPlugin`, then copy the whole
   `build/plugin/cassandra_arrow_flight/` directory to
   `<TRINO_HOME>/plugin/cassandra_arrow_flight/`.
3. Copy `catalog/arrow_flight.properties` to `<TRINO_HOME>/etc/catalog/arrow_flight.properties`,
   editing `arrow-flight.host`/`arrow-flight.port` to point at your Cassandra node.
4. Start the server (`bin/launcher start` or `run`), then connect with the Trino CLI or any
   JDBC/ODBC client.

## Cassandra-side setup

On the Cassandra node(s) you want to query, enable the Flight service in `cassandra.yaml`:

```yaml
start_arrow_flight: true
arrow_flight_port: 9143   # default; change if you also change arrow-flight.port above
```

**This is a development/PoC-only service with no authentication** (see
`src/java/org/apache/cassandra/arrow/ArrowFlightService.java`) - do not enable it on a node
reachable from an untrusted network.

## Example query

```sql
SHOW SCHEMAS FROM arrow_flight;
SHOW TABLES FROM arrow_flight.my_keyspace;
SELECT * FROM arrow_flight.my_keyspace.my_table LIMIT 10;
```

(`arrow_flight` here is the catalog name - the properties file's base name; `my_keyspace`/
`my_table` map directly to the Cassandra keyspace/table the Flight service exposes.)

## Type mapping

Inverse of `CassandraArrowTypeMapping` on the Cassandra side; implemented in
`ArrowTypeMapping.java` (see its class javadoc for the full rationale behind every entry marked
lossy below).

| Arrow type (as emitted by the server) | Trino type | Notes |
|---|---|---|
| `Bool` | `BOOLEAN` | |
| `Int(8)` | `TINYINT` | |
| `Int(16)` | `SMALLINT` | |
| `Int(32)` | `INTEGER` | |
| `Int(64)` | `BIGINT` | also covers counters (server pre-composes the counter total to a plain `Int64`) |
| `FloatingPoint(SINGLE)` | `REAL` | |
| `FloatingPoint(DOUBLE)` | `DOUBLE` | |
| `Utf8` | `VARCHAR` | ascii/text |
| `Binary` | `VARBINARY` | blob/inet |
| `FixedSizeBinary(16)` | `UUID` | uuid/timeuuid; lossless - both use the same big-endian RFC 4122 16-byte layout |
| `Timestamp(MILLISECOND, "UTC")` | `TIMESTAMP(3)` | plain `TIMESTAMP`, not `TIMESTAMP WITH TIME ZONE` - see javadoc |
| `Date(DAY)` | `DATE` | |
| `Time(NANOSECOND, 64)` | `TIME(9)` | |
| `Decimal(76, scale, 256)` | `VARCHAR` | **lossy (by necessity)**: varint/decimal; the server's fixed 76-digit precision exceeds Trino's 38-digit `DECIMAL` ceiling, so the exact value is rendered as its canonical plain-string form instead of truncating |
| `Interval(MONTH_DAY_NANO)` | `VARCHAR` | **lossy (by necessity)**: duration; no Trino interval type covers months+days+nanos together, so this is Arrow's own ISO-8601 interval string - informational display only, not usable for interval arithmetic |
| `FixedSizeList(N)` | `ARRAY` | vector\<T, N\> |
| `List` | `ARRAY` | list/set |
| `Map` | `MAP` | |
| `Struct` (named children) | `ROW` | UDT, fields named by column name |
| `Struct` (positional children `"1"`, `"2"`, ...) | `ROW` | tuple, fields named positionally |

Every Arrow field also carries `cassandra.kind` (`partition_key`/`clustering`/`static`/`regular`)
and, for key columns, `cassandra.position` metadata (see `ArrowTypeMapping.kindOf`/`positionOf`).
This connector reads it but doesn't yet act on it (no pushdown in this PoC) - it's there for a
future filter/split-pushdown extension to use without a second schema round-trip.

## Tests

Unit tests only (`./gradlew test`), run via plain JUnit 5 - this is a separate Gradle build from
Cassandra's own Ant-based test tooling:

- `ArrowTypeMappingTest` - every Arrow type family listed above, both directions of the
  narrow-vs-wide decimal fallback, nested array/map/row (including UDT vs. tuple field naming),
  the `cassandra.kind`/`cassandra.position` metadata readers, and an unhandled-type failure case.
- `ArrowPageBuilderTest` - builds real Arrow vectors (via `VectorSchemaRoot`/`RootAllocator`) for
  every type family and asserts the resulting Trino `Page`/`Block` values, including null
  handling, nested array/map/row value extraction, and the missing-projected-column failure path.

Both suites are pure unit tests with no network, no Cassandra, and no Trino server involved - they
exercise `ArrowTypeMapping`/`ArrowPageBuilder` directly against hand-built Arrow data.

### What is *not* covered by an automated test here

An end-to-end test (real `ArrowFlightService` + real Trino `Connector`/`ConnectorMetadata`/
`ConnectorPageSourceProvider` wired together via `io.trino:trino-testing`) was scoped as a
stretch goal and was **not** built, to keep this PoC's scope bounded - per the task's own
guidance, clear manual verification steps are an acceptable substitute here. Concretely, the
following still need a real end-to-end run once a Cassandra node is available with
`start_arrow_flight: true`:

- `listSchemaNames`/`listTables`/`getTableHandle` actually round-tripping through a live
  `ListFlights`/`GetFlightInfo` call (unit-tested only via the type-mapping/page-building logic
  they call into, not the gRPC/Flight wire calls themselves).
- The single-split-per-table path (`ArrowFlightSplitManager`/`ArrowFlightPageSourceProvider`)
  actually opening a `DoGet` stream against a running service and paging through real batches.
- Behavior against Cassandra's actual per-batch flush/memtable-freshness semantics (see
  `ARROW-FLIGHT.md` §3) - the unit tests here build synthetic Arrow batches directly and never
  touch Cassandra's read path.
- Any data-correctness bug on the Cassandra side that the parallel bug-fixing pass on
  `src/java/org/apache/cassandra/arrow/` is actively addressing - this connector was written
  against the documented wire contract (ticket format, descriptor format, schema field metadata),
  which that pass is not changing, but has not been run against a live, fixed server.

To perform that manual verification once a Cassandra node is up: follow
[Installing into a Trino server](#installing-into-a-trino-server) above, then run the
[example query](#example-query) against a real keyspace/table.
