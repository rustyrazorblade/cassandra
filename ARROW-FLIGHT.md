# CEP-DRAFT: Arrow Flight Storage-Engine Query Service

## Status

| | |
|---|---|
| **Current state** | Draft — internal design doc, not yet submitted to dev@cassandra.apache.org |
| **Discussion** | none yet (see [Mailing list / Slack channels](#mailing-list--slack-channels)) |
| **JIRA** | none yet |
| **Depends on** | `cursor-compaction-completion` (unmerged) |
| **Prep work** | Landed on `cursor-compaction-completion-arrow-prep` — see [Proposed Changes §1](#1-cursor-merge-integration-landed) |
| **Sponsor** | Jon Haddad |

Discussion of this proposal should happen wherever the author directs it (dev list thread, JIRA, or team channel) once opened — not as inline comments on this file.

---

## Scope

### In Scope

- A library that taps directly into the storage engine (not CQL) to read table data.
- A new gRPC service, embedded in the Cassandra process, that streams query results as Apache Arrow record batches via Arrow Flight.
- Two read modes:
  - **Point reads** of a table (single partition / clustering-bounded lookups).
  - **Full-table scans** driven by the cursor compaction merge path, split by token range for parallelism.
- A nested, arbitrary boolean-expression filter language — richer than CQL's `WHERE` — evaluated on the server after rows have been merged/reconciled, so it can reference any column regardless of indexing.
- Both read modes reflect current memtable contents, not just flushed sstables.
- A Trino connector that consumes this service as an alternative to Trino's existing CQL-based Cassandra connector, for OLAP-style bulk reads.

### Out of Scope

- Writes of any kind. This is a read-only service.
- Replacing CQL or the native protocol. Arrow Flight is an additional interface, not a migration path.
- Multi-replica read reconciliation / quorum reads. See [Proposed Changes §6](#6-consistency-model).
- A generic query planner, SQL surface, or Flight SQL implementation (see [Rejected Alternatives](#rejected-alternatives)).
- Changes to on-disk formats, CQL semantics, or the existing native-transport/JMX services beyond adding a new sibling service.
- Enabling subrange compaction (`nodetool compact -st/-et`) on the cursor path. The token-range positioning primitive this proposal depends on is read-only; wiring it into live compaction is explicitly not part of this work.

## Goals

### What this must allow that we can't (cleanly) do today

- Bulk, OLAP-shaped reads of an entire table's data at close to disk-read speed, without CQL's per-partition/per-row driver overhead or the allocation cost of the traditional iterator-based read path.
- Filter predicates that are richer than a CQL `WHERE` clause (arbitrary nested boolean trees over any column) evaluated server-side, so Trino doesn't have to pull unfiltered data across the wire to do reconciliation itself.
- A columnar wire format (Arrow) that downstream OLAP engines can consume natively, instead of paying a row-to-column transposition cost after the fact.
- A read path that is honest about combining memtable and sstable state, rather than requiring a flush before every analytical query.

### Non-Goals

- Sub-second point-lookup latency competitive with the native protocol — point reads exist for completeness and for Trino's dynamic filtering / index-join use cases, not to replace CQL for OLTP traffic.
- Cross-replica consistency stronger than what a single replica can locally provide (see [§6](#6-consistency-model)).
- Support for every Cassandra type/table shape on day one — accord-enabled tables, SAI-indexed tables on old sstable versions, and non-`Murmur3Partitioner` clusters are explicitly allowed to fall back to a slower, correct path (or be temporarily unsupported) rather than blocking the whole feature (see [§4](#4-full-table-scan-api-and-split-model)).

## Motivation

Trino (and similar engines) currently query Cassandra exclusively through the CQL native protocol. For OLAP-shaped workloads — full or large partial table scans, ad hoc analytical filters, joins against other Trino catalogs — this has three compounding costs:

1. **Row-at-a-time driver overhead.** The CQL connector issues token-range-split CQL queries and reconstructs each row from the native protocol wire format, which was designed for OLTP-shaped point/range queries, not high-throughput columnar consumption.
2. **No expressive server-side filtering.** CQL's `WHERE` clause pushdown is limited to indexed/partition-key predicates; anything else has to be filtered client-side after the full row set crosses the wire.
3. **A row-to-column transpose tax.** Every OLAP consumer wants columnar batches; today that transposition happens in Trino after CQL has already paid the cost of row serialization.

Separately, the `cursor-compaction-completion` branch built a garbage-free, byte-parity-verified merge path for compaction that reads N sstable cursors and reconciles them into merged output with very low allocation overhead. That merge machinery — partition/row/cell reconciliation, tombstone and TTL handling, counter shard composition — is exactly the reconciliation Trino would otherwise have to approximate client-side, and it already exists, tested, in this codebase. This proposal is about exposing that machinery (plus the equivalent point-read path, plus memtable state) through a wire format built for analytical consumption, rather than teaching Trino to reimplement Cassandra's read reconciliation.

## Audience

- Operators running Cassandra alongside Trino (or another Arrow-Flight-capable engine) for analytical workloads, who want to query Cassandra data without a separate ETL/CDC pipeline into an OLAP store.
- Cassandra contributors working on the storage engine, read path, or cursor compaction, who need to know this service's dependency on those internals when changing them.
- Future authors of the companion Trino connector.

## Proposed Changes

### 1. Cursor merge integration (landed)

The cursor compaction merge loop (`CursorCompactor`) originally pushed its output directly into a concrete `SSTableCursorWriter`. Before any Arrow/gRPC code is written, we landed a set of small, behavior-preserving refactors on `cursor-compaction-completion-arrow-prep` (branched off the still-unmerged `cursor-compaction-completion`) so this service has a clean seam to build on instead of a risky retrofit later:

- **`CursorMergeConsumer`** (`src/java/org/apache/cassandra/io/sstable/CursorMergeConsumer.java`) — an interface covering every merge-semantic event (`startPartition`, `endPartition`, `startRow`/`endRow`, cell/complex-column events, range tombstones, stats hooks). `SSTableCursorWriter` implements it; `CursorCompactor`'s output field is now interface-typed, with a separate concrete field retained only for writer-rollover bookkeeping. A future Arrow row assembler implements the same interface and receives the identical event stream compaction does, with no changes to the merge/reconciliation logic itself.
- **Read/write gate split** — `CursorCompactor.isSupported` split into `isCursorReadSupported` (sstable version, partitioner support, full-range check) and `isCursorWriteSupported` (output format, tombstone option, table has no indexes). This service depends only on the read gate, so SAI-indexed tables — previously excluded only because the *compaction writer* can't build index components — are readable via cursors even though they still correctly can't be cursor-*compacted*.
- **Scan-shaped construction** — a `CursorCompactor` constructor/`writeNextPartition` overload that takes a `Collection<SSTableReader>` and a `CursorMergeConsumer` directly, with no fabricated `ISSTableScanner`/`CompactionAwareWriter`, and no writer-rollover code in the call path.
- **Partial-range positioning primitive** — `StatefulCursor.positionAt(PartitionPosition)` and an optional `endBound`, letting a cursor be seeded and stopped at partition-boundary-aligned token bounds (used by `SSTableReader.getPosition`/`seekPartition` under the hood). Partition-boundary-aligned only; wraparound ranges are the caller's responsibility. This is what makes token-range split scans (§4) possible without waiting on a larger subrange-compaction feature.
- **Reader/writer decoupling** — `SSTableCursorReader`'s constructor no longer requires an `SSTableReader`; it takes its five actual inputs (data source, serialization header, table metadata, version, dropped columns) directly. This is what will let a future memtable adapter (§3) feed serialized rows through the same reader machinery instead of a bespoke merge path.

All of the above preserve compaction's byte-identical output, verified against the existing differential/golden-master test suite (`test/unit/org/apache/cassandra/db/compaction/differential/`) plus the allocation-gate tests. No Arrow/gRPC code exists yet; this section is infrastructure only.

### 2. Deployment model

The service runs **in-process**, inside the Cassandra JVM, as a sibling to `NativeTransportService` — a new field on `CassandraDaemon`, constructed in `initializeClientTransports()`, started/stopped behind a config flag, with `nodetool enablearrowflight`/`disablearrowflight`-style controls mirroring the existing native-transport toggles.

This is not a stylistic choice: the requirement that reads reflect current memtable state rules out a separate sidecar process, which cannot see memtable contents without forcing a flush (turning every scan into a flush and racing the very data it's trying to read). A resource jail — a dedicated bounded executor, a capped Arrow allocator, and throttled scan I/O (mirroring compaction's existing rate limiting) — is required so that analytical scans cannot starve the database's own read/write/compaction threads.

The wire stack uses `grpc-netty-shaded` rather than bare `grpc-netty`, because Cassandra's `netty-all` dependency explicitly excludes the HTTP/2 codecs gRPC requires; the shaded artifact avoids that classpath conflict entirely and is what Arrow Flight ships against by default.

### 3. Memtable strategy

Point reads reuse `SinglePartitionReadCommand.queryMemtableAndDisk`, which already merges memtable and sstable state correctly — no new work needed there.

For full scans, v1 ships **flush-then-scan**: `ColumnFamilyStore.forceBlockingFlush` before pinning the sstable set for a scan, giving a clean "snapshot as of scan start" semantic with zero new merge code. Frequent concurrent scans against the same table will need flush coalescing (one flush serves all in-flight scan requests against it) to avoid a scan storm becoming a flush storm; this is a v1 implementation detail, not an API concept.

The API is designed so a live, no-flush merge can replace this later without a breaking change. The long-term direction (not built now) is **not** a bespoke two-way (memtable-object × cursor-buffer) merge — that would re-implement reconciliation logic the cursor merge already has verified. Instead, a memtable partition would be serialized on the fly (via the existing `UnfilteredSerializer`) into an in-memory buffer and read back through a real `SSTableCursorReader` — which §1's reader/writer decoupling already makes possible — so it becomes just another cursor source to the existing N-way merge. This is flagged as future work, not a v1 deliverable.

### 4. Full-table scan API and split model

`GetFlightInfo` returns one Flight endpoint per token subrange (not one per node, not one per sstable) — real Trino parallelism, retryable splits, and each range served by exactly one replica. This is built on §1's partial-range positioning primitive. Splits are partition-boundary-aligned; the server, not the client, is responsible for producing non-wrapping subranges.

Tables that fail `isCursorReadSupported` (old sstable version not yet rewritten, non-`Murmur3Partitioner`/`LocalPartitioner`, accord-enabled) fall back to the existing iterator-based read path (`PartitionRangeReadCommand`-style). The fallback must produce identical output shape (types, nulls, filter behavior) to the cursor path — the filter/Arrow-assembly stages are shared between both producers; only the producer differs.

### 5. Point-read API

A thin adapter over `SinglePartitionReadCommand` — construct the command programmatically from the request's key/clustering bounds, drive it through `queryMemtableAndDisk`, and feed the resulting `UnfilteredRowIterator` into the same Arrow-assembly stage full scans use. No reimplementation of read-path semantics (timestamp-order optimization, multicell/counter handling, tombstone shadowing).

### 6. Consistency model

The service promises **node-local, `CL.ONE`-equivalent** semantics: a scan or point read sees this replica's data, full stop. Multi-replica reconciliation is out of scope — a Trino connector routes each token range to one replica, the same trust model the existing CQL connector uses at `ONE`/`LOCAL_ONE`. Within a single scan, each partition is internally consistent as of the moment it was merged; the scan as a whole is not a single point-in-time snapshot unless flush-then-scan's "as of flush time" framing is used (§3).

### 7. Filter expression model

Filters are a bespoke nested expression tree (`And` / `Or` / `Not` / `Comparison` / `IsNull` / `In` / ...), carried in the Flight ticket, evaluated **after** merge — correctness requires this, since a predicate can only be evaluated once shadowing/reconciliation across sources has happened. v1 evaluates filters as a vectorized pass over completed Arrow batches (simplest correct implementation); a pre-Arrow fast path for cheap conjunctive predicates (partition-key range, single-column comparisons on fixed-width types) is a valid future optimization that doesn't require an API change.

Trino's pushdown today is realistically conjunctive (`TupleDomain`), so the tree's nesting will initially be under-used by its first consumer — it's still the right wire format, since partial `ConnectorExpression` pushdown or other future callers can use the full tree, and a flat format would have to be replaced later.

### 8. Type mapping

Cassandra types map to Arrow largely mechanically (`Int32Type→Int32`, `UTF8Type→Utf8`, `TimestampType→Timestamp`, `UUIDType→FixedSizeBinary(16)`, collections/UDTs → `List`/`Map`/`Struct`, `VectorType→FixedSizeList`, etc.), with explicit decisions required on:

- **Varint/decimal** — Cassandra's unbounded-precision numeric types don't fit Arrow's fixed precision/scale decimals. Default: map to `Decimal256` with an explicit overflow error rather than silent truncation; revisit if real workloads need wider values.
- **Counters** — compose the on-disk shard context to a final `Int64` total via `CounterContext.total()`; counter tables' lack of row liveness is a no-op for Arrow output.
- **Tombstones/TTL/writetime** — expired and shadowed cells become null/absent, never stale values, in the default mapping. Writetime/TTL are not exposed by default; opt-in `_writetime_<col>`/`_ttl_<col>` virtual columns are a candidate future addition, not v1.
- **Static columns** — replicated onto every row of the partition (matches Trino's relational expectations) rather than exposed as a separate stream.
- **Key metadata** — partition/clustering/static/regular column kind is carried as Arrow field metadata, so the Trino connector can derive split and pushdown eligibility without a second schema channel.

### 9. Auth and schema discovery

Auth: Flight's handshake plus per-call bearer middleware, backed by Cassandra's existing `IAuthenticator` for authentication and `IAuthorizer` (`SELECT` permission check) before opening any stream — reusing the database's own role/permission model rather than inventing a parallel one. mTLS-only is acceptable as a coarser-grained v1 option but bearer-token-over-roles is the target.

Schema discovery: custom Flight descriptors (`ListFlights` enumerates `keyspace.table`; `GetSchema` returns the Arrow schema with the key-kind metadata from §8) consumed by a purpose-built Trino connector, rather than implementing Arrow Flight SQL. Trino has no first-party Flight SQL connector, so adopting Flight SQL would still require writing a custom connector — custom descriptors are the smaller total scope for the stated goal (Trino specifically). Broader tool interop (DuckDB, ADBC, etc.) via Flight SQL metadata endpoints is a plausible future addition, not a v1 requirement.

## New or Changed Public Interfaces

- New public API surface: `CursorMergeConsumer` (`io.sstable`), new `CursorCompactor` constructor/method overloads, new `StatefulCursor` methods (`positionAt`, `setEndBound`) — all additive, already landed on `cursor-compaction-completion-arrow-prep`.
- New network-facing interface: a gRPC/Arrow Flight service on a new, separately configured port — new `cassandra.yaml` options (enable flag, port, resource-jail limits), new `nodetool` verbs (`enablearrowflight`/`disablearrowflight`/status equivalent).
- New dependencies: Apache Arrow (Java), `grpc-netty-shaded`, gRPC/Flight core libraries — none of which exist in this codebase today (verified: no Arrow, gRPC, or protobuf dependencies present as of this proposal).
- New external consumer artifact: a Trino connector (separate project/repo), not part of this codebase.
- No changes to CQL, the native protocol, SSTable on-disk formats, or existing JMX/nodetool surfaces beyond the additions above.

## Compatibility, Migration Plan, and Deprecation

- Purely additive; the service is opt-in (disabled by default) and CQL/native-protocol behavior is unaffected whether it's enabled or not.
- No deprecation of any existing interface. No data migration. No upgrade-path hazard beyond the existing `cursor-compaction-completion` constraint that cursor reads require sstables at the latest on-disk version — nodes with un-rewritten old-format sstables transparently fall back to the iterator-based read path (§4) rather than failing.
- Because this depends on `cursor-compaction-completion`, it cannot ship ahead of (or independently from) that branch merging.

## Operational Implications

- A new port to open in firewalls/security groups if the service is enabled.
- New capacity-planning inputs: scan concurrency limits, Arrow allocator memory cap, scan I/O throttle — these need sane defaults and documentation, since an unbounded analytical scan is a classic way to destabilize a database node.
- Held `Ref`s on sstables for the duration of a scan delay file deletion after compaction on frequently-compacted tables; token-range-sized splits (§4) bound this window, whole-table scans would not.
- Flush-then-scan (§3) means analytical query volume translates into flush volume; this needs monitoring/alerting guidance so operators don't mistake scan-driven flushes for write-driven ones.

## Test Plan

- Unit tests for `CursorMergeConsumer`/`StatefulCursor` changes: already covered by the existing differential/parity suite plus new tests added with the prep work (`ScanShapedCursorCompactorTest`, `BoundedScanStatefulCursorTest`), verified against the byte-identical compaction oracle.
- New tests required for the service itself (not yet written): Arrow schema/type-mapping correctness per Cassandra type (including the sharp edges in §8), filter-tree evaluation correctness (including nested/negated predicates CQL can't express), token-range split correctness (no gaps/overlaps, no duplicate/missing rows versus a full unsplit scan), memtable+sstable freshness under concurrent writes during a scan, fallback-path parity (cursor vs iterator producer must yield identical Arrow output for the same data), and auth/authorization enforcement.
- A differential harness comparing this service's full-table-scan output against a CQL `SELECT *` (reconciled client-side) over the same data is the recommended top-level correctness oracle, analogous to how `DifferentialCompactionTester` anchors the cursor compaction work.

## Rejected Alternatives

- **Sidecar process reading sstables directly** — rejected because it cannot see memtable contents without forcing a flush per scan, which the feature's own freshness requirement rules out.
- **A standalone merge engine reusing only low-level cursor primitives** (rather than extracting `CursorMergeConsumer` from the existing writer) — rejected because it would duplicate recently-debugged, subtle merge/reconciliation logic (complex-deletion shadowing, counter shard supremacy, range-tombstone algebra) with permanent drift risk.
- **Compact-to-scratch** (run real compaction to a temp sstable, then linearly read it) — rejected as the general-purpose architecture; it doubles write I/O, needs scratch disk, and still doesn't address memtable freshness. Worth revisiting only as a narrow single-sstable fast-path optimization later.
- **Reusing `RowFilter` for the filter language** — rejected; it's a flat AND-list on this branch with no nesting, and would drag in CQL-specific semantics not needed here.
- **Substrait for filter expressions** — rejected for v1; heavier tooling than the needed subset warrants, and Trino doesn't speak it natively, so a bespoke encoder/decoder would be needed either way.
- **Arrow Flight SQL** — rejected for v1; Trino has no first-party Flight SQL connector, so it wouldn't reduce the amount of connector work required, and it implies a SQL surface (statement parsing, catalog semantics) this service doesn't need. Revisit only if broader cross-tool access becomes a goal.
- **A cursor-*source* interface for memtables, built now alongside `CursorMergeConsumer`** — rejected as premature: it would mirror a ~20-method byte-format state machine with no second implementation to validate the shape against. The serialize-to-buffer-and-reread approach in §3 is the better long-term path and doesn't get harder if deferred.
- **Enabling subrange compaction on the cursor path now that the positioning primitive exists** — rejected as out of scope for this proposal; it changes live compaction behavior and needs its own differential coverage, decoupled from this read-only feature's timeline.

## Timeline

No committed timeline. Sequencing depends on `cursor-compaction-completion` merging to trunk; the prep work in §1 is designed to be mergeable as part of that branch regardless of when (or whether) the rest of this proposal proceeds.

## Mailing list / Slack channels

Not yet opened for discussion. To be filled in if/when this moves from internal design doc to a proposal on dev@cassandra.apache.org.
