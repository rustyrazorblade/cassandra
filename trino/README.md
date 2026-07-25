# Cassandra Arrow Flight Trino Connector

A minimal Trino connector plugin that reads Cassandra table data through the embedded Arrow
Flight service added on this branch (`org.apache.cassandra.arrow.*`, see
`../ARROW-FLIGHT.md` and `src/java/org/apache/cassandra/arrow/` at the repo root), instead of
Trino's existing CQL-based Cassandra connector.

**This directory is a separate, self-contained Gradle project.** It does not touch, depend on,
or get built by Cassandra's own Ant build; nothing outside `trino/` was changed to produce it.

## Scope

The Cassandra-side service this connector talks to is still a proof-of-concept in some respects
(no authentication, see below), but cluster-aware, distributed reads and predicate/aggregation
pushdown are implemented - see `../ARROW-FLIGHT.md`:

- **No authentication yet.** Anyone who can open a TCP connection to the Flight port can read
  every row of every user table. Do not point this at a Cassandra node reachable from an
  untrusted network until this lands.
- **Cluster-aware, token-range-split reads**: ring topology discovery via `cassandra-sidecar`'s
  client, split planning via `cassandra-analytics-common`'s `CassandraRing`/`TokenPartitioner`,
  and per-split routing directly to the owning replica(s)' Arrow Flight port - see
  [Cluster-aware reads](#cluster-aware-reads) below for the full design, including the
  replica-selection simplification versus the Spark analytics connector.
- **Filter and aggregation pushdown**: `WHERE` predicates and `GROUP BY`/aggregate functions are
  translated into the Flight ticket's `filter`/`aggregation` clauses and pushed to the server,
  evaluated after the cursor-merge reconciles each row (correctness requires this - a predicate
  can only be evaluated once shadowing/reconciliation has resolved a column to its true value).
  See [Filter and aggregation pushdown](#filter-and-aggregation-pushdown) below for exact
  coverage and fallback behavior.
- **Full-table scan only** (split by token range). No point-read API yet.
- The Cassandra-side Arrow Flight service's support for `tokenRange`/`filter`/`aggregation` in
  the ticket protocol was being implemented in parallel with this connector and had not landed
  yet as of this writing - this connector is written against the documented wire contract (see
  `../ARROW-FLIGHT.md`) but the full pushdown path has not been run end-to-end against a live,
  finished server. Ring discovery and split planning *have* been verified against a real running
  `cassandra`+`sidecar` stack - see [Tests](#tests).

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
  than left to chance. `sidecar-vertx-client`'s Vert.x 4.5.23 also requests plain Netty 4.1.130.Final
  directly, so it doesn't disturb this pin.
- **`org.apache.cassandra:sidecar-client:0.4.0`** + **`sidecar-vertx-client:0.4.0`** - the
  sidecar REST client used for ring/topology discovery. `sidecar-client` ships only the
  transport-agnostic `HttpClient` interface (no concrete implementation, and no shaded fat jar is
  published for plain consumption) - `sidecar-vertx-client` supplies the actual Vert.x-based
  `HttpClient`/`RequestExecutor` implementation; both are required together (see
  `topology/SidecarClients.java`'s javadoc for how this was discovered/verified, since neither
  artifact publishes a sources/javadoc jar).
- **`org.apache.cassandra:cassandra-analytics-common:0.4.0`** - `CassandraRing`/
  `TokenPartitioner`/`RangeUtils` ring/split math, confirmed to have zero Spark dependencies.
  Two undeclared runtime requirements were discovered by testing against it (see the comment
  block above `cassandra-analytics-common` in `build.gradle.kts`) and pinned explicitly rather
  than left to accident:
  - **Guava** (`33.5.0-jre`) - `CassandraRing`/`TokenPartitioner`/`RangeUtils` use Guava
    `Range`/`RangeMap` at the API level, but the artifact declares no Guava dependency of its own;
    it only compiles/links today because Guava rides in transitively from `flight-core`'s
    `grpc-netty` dependency.
  - **Kryo** (`com.esotericsoftware:kryo:5.6.2`) - `CassandraRing`/`TokenPartitioner`/
    `CassandraInstance`/`ReplicationFactor` each declare a `SERIALIZER` static field whose type
    extends `com.esotericsoftware.kryo.Serializer`, so merely *loading* any of those classes
    (not using Kryo serialization at all) fails with `NoClassDefFoundError` without Kryo on the
    classpath - not declared as a dependency of `cassandra-analytics-common`'s own POM (Spark's
    bulk-reader environment normally supplies its own Kryo).
- **`com.fasterxml.jackson.core:jackson-databind:2.21.0`** - builds/serializes the Flight ticket
  JSON (`tokenRange`/`filter`/`aggregation`); already present transitively (`arrow-vector` and
  `sidecar-client` both pull it in), declared explicitly rather than left to whichever version
  those happen to request.

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
`catalog/arrow_flight.properties` first: set `arrow-flight.host` (schema-discovery bootstrap) and
`sidecar.contact-points` (ring/topology discovery) to an address the container can reach (e.g.
the host's LAN IP, or `host.docker.internal` on Docker Desktop), or run the container with
`--network host` on Linux. Every replica address returned by ring discovery must also be reachable
from wherever Trino workers run - not just the bootstrap/contact-point hosts - since scans connect
directly to each split's owning replica(s).

### Option B: a manual Trino server install

1. Download and unpack a Trino server tarball for the same version:
   `https://repo1.maven.org/maven2/io/trino/trino-server/481/trino-server-481.tar.gz`
   (see `trino.io/docs/current/installation/deployment.html` for full server setup - JVM config,
   `node.properties`, `config.properties`, etc.; that setup is unrelated to this connector).
2. `./gradlew installPlugin`, then copy the whole
   `build/plugin/cassandra_arrow_flight/` directory to
   `<TRINO_HOME>/plugin/cassandra_arrow_flight/`.
3. Copy `catalog/arrow_flight.properties` to `<TRINO_HOME>/etc/catalog/arrow_flight.properties`,
   editing `arrow-flight.host`/`arrow-flight.port` (schema-discovery bootstrap) and
   `sidecar.contact-points` (ring/topology discovery, `host:port`, comma-separated) to point at
   your cluster. See [Cluster-aware reads](#cluster-aware-reads) for what each property does.
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

## Loading test data and querying end-to-end

The full local stack (`../docker-compose.yml`, run from `trino/`) wires together this branch's
Cassandra build with the Flight service enabled, `cassandra-sidecar` for topology discovery, and
Trino with this connector installed. This section documents a verified round trip: bring the
stack up, load real data, and query it through Trino - not just each piece in isolation.

```bash
cd trino
docker compose up -d cassandra sidecar trino   # build + start; see docker-compose.yml's header
                                                 # comment for the on-host Cassandra build prerequisite
docker compose --profile stress run --rm easy-stress   # loads 10M rows via RandomPartitionAccess
```

Verified against a real run of this exact stack: 10,000,000 write operations via
`RandomPartitionAccess` (single thread, `-i 10000000`, `--readrate 0 --deleterate 0` - see the
`easy-stress` service definition in `../docker-compose.yml` for why iteration count is per-thread,
not a total) landed 9,875,527 distinct rows in `cassandra_easy_stress.random_access` (the random
partition/row-id generator produces a small number of overwritten duplicates at this fill ratio -
expected, not a bug). Querying it via Trino afterward:

```bash
docker exec -it arrow-flight-trino trino
```

```sql
SELECT count(*) FROM arrow_flight.cassandra_easy_stress.random_access;
SELECT * FROM arrow_flight.cassandra_easy_stress.random_access LIMIT 5;
```

returned the same row count and real data, confirming the full path: cursor-merge scan ->
`ArrowRowAssembler` -> Arrow Flight `DoGet` -> this connector's `ArrowFlightPageSource` -> Trino.
This specifically verifies the **plain-scan path** (no `WHERE`/`GROUP BY` pushdown exercised) -
see [Tests](#tests) below for what filter/aggregation pushdown still lacks live verification.

For monitoring query history/cluster state, Trino's own Web UI is at
`http://localhost:${TRINO_PORT:-8080}` (`http://localhost:18080` if you've overridden
`TRINO_PORT`, as in a setup running alongside another local Trino/Cassandra) - it is not a SQL
editor; use the `trino` CLI above (or any JDBC/ODBC client against the same port) to actually run
queries.

**Operational gotcha confirmed by this run**: `sidecar` shares `cassandra`'s network namespace
(`network_mode: "service:cassandra"`) to reach its JMX/CQL ports directly. Restarting only the
`cassandra` service (e.g. `docker compose restart cassandra`, needed after any config or jar
change) leaves `sidecar` holding a dead JMX RMI connection - it stays reporting healthy (its own
HTTP health check doesn't probe JMX) but every ring/topology-dependent Trino query then fails
with `RetriesExhaustedException` on `/api/v1/cassandra/settings`. Always restart `sidecar`
immediately after restarting `cassandra`:

```bash
docker compose restart cassandra
docker compose restart sidecar
```

## Cluster-aware reads

Ring topology is discovered per query via `cassandra-sidecar`'s async Java client
(`org.apache.cassandra:sidecar-client`/`sidecar-vertx-client`, wired up in
`topology/SidecarClients.java`): `ring(keyspace)`, `nodeSettings()` (for the cluster's
partitioner), and `schema(keyspace)` (whose `CREATE KEYSPACE` DDL text embeds the replication
factor - parsed by `topology/ReplicationFactorParser.java`, since sidecar has no structured RF
field). That feeds `cassandra-analytics-common`'s `CassandraRing`/`TokenPartitioner` (the same
ring-math library the Spark bulk-reader/writer use) to compute a token-range split plan
(`topology/SplitPlanner.java`), targeting `arrow-flight.splits-per-node` splits per node.

Each `ArrowFlightSplit` carries its resolved `(start, end]` token range and an ordered list of
candidate replica Arrow Flight addresses directly - unlike the Spark analytics connector's
bare-int `InputPartition`, which resolves a partition ID back into a token range/replica set via
broadcast state shared across executors. Trino has no equivalent shared-state mechanism (splits
serialize independently to separate workers), so this connector's splits are self-contained.

**Replica-selection simplification**: this connector does not reproduce
`cassandra-analytics`'s `PartitionedDataLayer`/`AvailabilityHint`/consistency-level machinery.
Trino's SPI has no consistency-level concept at all, so the contract here is deliberately
simpler: a split's replicas are tried in order, and the first one that accepts the `DoGet`
request wins (see `ArrowFlightPageSource`). This is closer to a CQL driver at consistency level
`ONE` with a fixed replica preference order than to any stronger guarantee - there is no retry
budget, no speculative execution, and no cross-replica reconciliation. It is a reasonable v1
given the existing CQL-based Cassandra connector for Trino makes essentially the same trust
assumption at `ONE`/`LOCAL_ONE`.

Each replica's Arrow Flight port is *not* discoverable via sidecar (`arrow_flight_port` is a
custom addition on this branch, not a stock Cassandra/sidecar concept) - it is assumed uniform
across the cluster and taken from `arrow-flight.port`, combined with each replica's address from
the ring response.

## Filter and aggregation pushdown

`ConnectorMetadata#applyFilter`/`#applyAggregation` translate Trino's predicate/aggregation
pushdown representations into the Flight ticket's `filter`/`aggregation` clauses (see
`../ARROW-FLIGHT.md` §7 and the `pushdown` package); `ArrowFlightPageSourceProvider` embeds the
result, plus the split's own token range, directly into each split's `DoGet` ticket.

**Predicate pushdown** (`pushdown/PredicatePushdown.java`) translates `Constraint#getSummary()`
(a `TupleDomain<ColumnHandle>`) - single values to `cmp EQ`, discrete sets to `in`, ranges to
`cmp`/`and`/`or` trees, nullability to `isNull`/`isNotNull`, with `OR ... IS NULL` for nullable
domains. Translation is all-or-nothing *per column*: a column either translates completely or is
left untouched in the returned `remainingFilter` for Trino to still apply - so a query mixing
pushable and non-pushable predicates still gets partial pushdown. Value encoding
(`pushdown/FilterValueEncoder.java`) is a documented, explicit design choice for every type this
connector supports (numbers as JSON numbers, timestamps/dates/times as ISO-8601 strings, UUIDs as
canonical strings, `varbinary` as base64) since the wire contract only specifies `"value": <JSON
literal>` generically, not a per-type encoding. **Not translated**: `Constraint#getExpression()`
(arbitrary `ConnectorExpression`s - casts, multi-column comparisons, function calls) is always
left for Trino, matching most JDBC-style Trino connectors' v1 scope; `decimal`-typed columns are
never pushed down (the server's fixed 76-digit precision means this connector never actually
produces a Trino `DECIMAL`-typed column today - see `ArrowTypeMapping`); `array`/`map`/`row`
columns are never pushed down.

**Aggregation pushdown** (`pushdown/AggregationPushdown.java`) supports `COUNT(*)`, `COUNT(col)`,
`SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)` over a single grouping set (plain `GROUP BY` or a
global aggregation) with a bare column argument - no `DISTINCT`, no `FILTER (WHERE ...)`, no
`ORDER BY` within the aggregate, no `GROUPING SETS`/`CUBE`/`ROLLUP`, no expression arguments. Per
the `applyAggregation` SPI contract there is **no partial pushdown**: if any single aggregate in
the query is unsupported, the *entire* aggregation is left for Trino to compute itself - this
connector never silently drops or mishandles part of a pushed-down aggregation.

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
This connector reads it but the current predicate/aggregation pushdown (see
[Filter and aggregation pushdown](#filter-and-aggregation-pushdown)) doesn't yet key any decision
off column kind (e.g. preferring partition-key equality predicates) - it's there for a future,
more targeted pushdown strategy to use without a second schema round-trip.

## Tests

Unit tests only (`./gradlew test`), run via plain JUnit 5 - this is a separate Gradle build from
Cassandra's own Ant-based test tooling:

- `ArrowTypeMappingTest` - every Arrow type family listed above, both directions of the
  narrow-vs-wide decimal fallback, nested array/map/row (including UDT vs. tuple field naming),
  the `cassandra.kind`/`cassandra.position` metadata readers, and an unhandled-type failure case.
- `ArrowPageBuilderTest` - builds real Arrow vectors (via `VectorSchemaRoot`/`RootAllocator`) for
  every type family and asserts the resulting Trino `Page`/`Block` values, including null
  handling, nested array/map/row value extraction, and the missing-projected-column failure path.
- `ticket.ArrowFlightTicketTest` - pure JSON serialization of the ticket/filter/aggregation
  model: every filter comparison operator, `and`/`or`/`not` nesting (including deeply nested
  trees), `in`/`isNull`/`isNotNull`, every aggregate function, `COUNT(*)`'s null column, and the
  full documented example ticket shape from `../ARROW-FLIGHT.md`.
- `topology.SplitPlannerTest` - ring-topology &rarr; split-plan computation against synthetic
  `RingResponse`/`NodeSettings`/`SchemaResponse` data (real `cassandra-analytics-common` classes,
  no live sidecar): single- and multi-node rings, `SimpleStrategy` vs `NetworkTopologyStrategy`
  (including a fully-qualified `org.apache.cassandra.locator.*` class name, as real schema dumps
  use), RF 1 vs RF 2 replica-set sizing, no-gaps/no-overlaps coverage of the full token space,
  split count scaling with `splitsPerNode`, and error handling for an empty ring/malformed schema.
- `pushdown.PredicatePushdownTest` - real Trino `TupleDomain`/`Domain`/`ValueSet`/`Range` shapes:
  single values, discrete sets, open/closed/unbounded ranges, disjoint multi-range `OR`,
  nullability (`onlyNull`/`notNull`/nullable value sets), every supported type's value encoding
  (including `uuid`/`varbinary`/`date`), and a mixed supported/unsupported-column case verifying
  partial pushdown (one column pushed, the other left in `remainingFilter`).
- `pushdown.AggregationPushdownTest` - real Trino `AggregateFunction`/grouping-set shapes: every
  supported function, `COUNT(*)` vs `COUNT(col)`, `GROUP BY` column propagation, projection/
  assignment ordering and typing, and every unsupported shape (`DISTINCT`, `FILTER`, multiple
  grouping sets, non-`Variable` arguments, unknown function names, one bad aggregate among
  several) correctly falling back to `Optional.empty()`.

All of the above are pure unit tests with no network, no Cassandra, and no Trino server involved.

### Live-verified against a real sidecar

`ArrowFlightTopologyService`/`SidecarClients` (the actual Vert.x-based `SidecarClient` wiring -
see [Versions targeted](#versions-targeted) for why two separate artifacts are needed) was run
against the real, running `cassandra` + `sidecar` services from `../docker-compose.yml`
(`docker compose up -d cassandra sidecar` from `trino/`) - not just the synthetic-data unit
tests above. This confirmed, against a live server: real HTTP connectivity and error handling
(a genuine `403 Forbidden` for a sidecar-restricted system keyspace was correctly surfaced through
`RetriesExhaustedException`), and a full successful ring-discovery-to-split-plan round trip
against a real user keyspace (`ring()`/`nodeSettings()`/`schema()` calls, `ReplicationFactor`
parsing of real DDL text using a fully-qualified strategy class name, ring vnode-token coverage,
and correct application of the configured Arrow Flight port) - 17 splits computed correctly
covering the full token ring for a real single-node/16-vnode/RF=1 keyspace.

### Now live-verified: the plain-scan path end-to-end

Update: the gap below describing an unverified `DoGet` round trip is now partly closed. Following
[Loading test data and querying end-to-end](#loading-test-data-and-querying-end-to-end), a real
`docker compose` stack (Cassandra with the Flight service enabled, `sidecar`, this Trino
connector) served a real 10M-row table (`cassandra_easy_stress.random_access`, loaded via
`cassandra-easy-stress`'s `RandomPartitionAccess`) through `SELECT count(*)` and `SELECT * ...
LIMIT 5` via Trino, with results matching a direct `pyarrow.flight` client against the same
ticket. This confirms, for real: `listSchemaNames`/`listTables`/`getTableHandle` resolving through
a live `ListFlights`/`GetFlightInfo` call, and `ArrowFlightPageSource` opening a real `DoGet` and
receiving correctly-typed Arrow batches back for a plain (no `tokenRange`/`filter`/`aggregation`)
ticket.

**Still not covered** (this run did not exercise them): a `WHERE`/`GROUP BY` query that actually
exercises `filter`/`aggregation` pushdown end-to-end - the ticket JSON this connector produces for
those is unit-tested against the documented wire contract (`ticket.ArrowFlightTicketTest`) and the
translation logic is unit-tested against real Trino SPI types (`pushdown.*Test`), but the two
halves plus the server's actual filter/aggregation evaluation have not been connected end-to-end
against a live server. Multi-split (`tokenRange`-bounded) scans were also not exercised by this
single-node run - see [Cluster-aware reads](#cluster-aware-reads) for that separately-verified
ring/split-planning logic. Confirm with a query like `SELECT count(*) FROM ... WHERE value >
'M'` or `SELECT partition_id, count(*) FROM ... GROUP BY partition_id` against the same table to
close this remaining gap.

### What is *not* covered by an automated or live test here

- `listSchemaNames`/`listTables`/`getTableHandle`/schema resolution actually round-tripping
  through a live `ListFlights`/`GetFlightInfo` call (unit-tested only via the type-mapping/
  page-building logic they call into, not the gRPC/Flight wire calls themselves) - **now live-
  verified for the plain-scan path**, see above.
- A full `ConnectorMetadata`/`ConnectorSplitManager`/`ConnectorPageSourceProvider` wired
  together via `io.trino:trino-testing` against a real Trino engine - out of scope, as before.
- Behavior against Cassandra's actual per-batch flush/memtable-freshness semantics (see
  `ARROW-FLIGHT.md` §3).
- Any data-correctness bug on the Cassandra side that a parallel bug-fixing pass on
  `src/java/org/apache/cassandra/arrow/` may be addressing - this connector was written against
  the documented wire contract, which that pass is not changing, but has not been run against a
  live, finished server.

To perform manual verification once a Cassandra node with a finished Arrow Flight service is up:
follow [Installing into a Trino server](#installing-into-a-trino-server) above, then run the
[example query](#example-query) against a real keyspace/table, including `WHERE`/`GROUP BY`
queries to exercise pushdown.
