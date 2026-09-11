# Removals

This document records features that this branch removes.  Each subsection covers one removal.

## Triggers

### Purpose

This change removes the triggers feature.  Triggers let a user attach custom Java code to a table.  The code ran on the coordinator before each write.  The feature saw little use.  It added cost to the write path and to the schema.  This branch removes bad old technology, so the removal is a goal.

### What changes

CQL no longer accepts `CREATE TRIGGER` or `DROP TRIGGER`.  The grammar drops both statements and the `TRIGGER` keyword.

The write path no longer runs trigger code.  `StorageProxy.mutateWithTriggers(...)` becomes `StorageProxy.mutate(...)`.  The Paxos path drops its trigger call.

The schema no longer stores triggers.  `TableMetadata` drops its `triggers` field.  The `system_schema.triggers` table is gone.

The `nodetool reloadtriggers` command is gone.  The `reloadTriggerClasses` JMX operation on `StorageProxyMBean` is gone.

The `triggers_policy` option is gone from `cassandra.yaml` and `cassandra_latest.yaml`.  The `cassandra.triggers_dir` property is gone.

cqlsh drops its trigger completion, help topics, and syntax rules.

### What is removed

- The `org.apache.cassandra.triggers` package: `CustomClassLoader`, `ITrigger`, `TriggerDisabledException`, and `TriggerExecutor`.
- The CQL statements: `CreateTriggerStatement` and `DropTriggerStatement`.
- The schema classes: `TriggerMetadata` and `Triggers`.
- The `triggers` field, builder method, and serializer sites in `TableMetadata`.
- The `system_schema.triggers` table in `SchemaKeyspace` and `SchemaKeyspaceTables`, and the trigger fetch, diff, and mutation helpers.
- The trigger call sites in `StorageProxy`, `Paxos`, `ModificationStatement`, and `BatchStatement`.
- The `TriggersPolicy` enum and `triggers_policy` option in `Config`, plus the `DatabaseDescriptor` getters and setters.
- The `TRIGGERS_DIR` property, `FBUtilities.cassandraTriggerDir()`, and the `DEFAULT_TRIGGER_DIR` constant.
- The `nodetool reloadtriggers` command and `NodeProbe.reloadTriggers()`.
- The `reloadTriggerClasses` JMX operation on `StorageProxyMBean`.
- The `TRIGGER` keyword and both trigger rules in the ANTLR grammar.
- The trigger blocks in `conf/cassandra.yaml`, `conf/cassandra_latest.yaml`, `conf/jvm-server.options`, `debian/cassandra.install`, and `debian/nodetool-completion`.
- The `conf/triggers` directory.
- The trigger support in cqlsh: `cql3handling.py`, `cqlshmain.py`, and `helptopics.py`.
- All trigger tests and the trigger cases in shared tests.

### Tests

Run each command from the worktree root with JDK 21.

- `ant build`: BUILD SUCCESSFUL.
- `ant test -Dtest.name=CreateTest`: 21 tests, 0 failures, 0 errors.
- `ant test -Dtest.name=VirtualTableTest`: 15 tests, 0 failures, 0 errors.
- `ant test -Dtest.name=CreateLikeTest`: 38 tests, 0 failures, 0 errors.
- `ant test -Dtest.name=AlterSchemaStatementNoOpTest`: 7 tests, 0 failures, 0 errors.
- `ant test -Dtest.name=SchemaChangeDuringRangeMovementTest`: 2 tests, 0 failures, 0 errors.
- `ant test -Dtest.name=DatabaseDescriptorRefTest`: 0 failures, 0 errors.
- `ant test -Dtest.name=JMXCompatibilityTest`: 4 tests, 0 failures, 0 errors.

`JMXCompatibilityTest` excludes the trigger MBeans from the old baselines.  The excludes cover the `system_schema.triggers` table MBeans in the `metrics` and `db` domains, and the `reloadTriggerClasses` operation.

### Risks

This change breaks the schema serialization format.  `TableMetadata.serializer` no longer reads or writes the trigger section.  A node on this branch cannot read a schema payload from a node that still has the trigger section.  This break is accepted.  There is no upgrade or migration path.

Any user that depends on triggers must move that logic into the application.

## Thrift dead code

### Purpose

This change removes dead Thrift remnants.  The heavy Thrift infrastructure is already gone from this branch.  These remnants stayed behind after that removal.  No live code path uses them for same-version operation.  This branch removes bad old technology, so the removal is a goal.

### What changes

`DataLimits` drops two unused limit kinds.  The `Kind` enum no longer holds `THRIFT_LIMIT` or `SUPER_COLUMN_COUNTING_LIMIT`.  No live code constructs either kind.  The serializer never had a case for them.

`ReadCommand` drops the Thrift wire flag.  The serializer never sets the flag.  The deserializer no longer reads it and no longer throws on it.

### What is removed

- The `THRIFT_LIMIT` and `SUPER_COLUMN_COUNTING_LIMIT` values in `DataLimits.Kind`, plus their deprecation javadoc.
- The `IS_FOR_THRIFT` flag constant in `ReadCommand.Serializer`.
- The `isForThrift(int)` helper and its comment.
- The Thrift guard branch in `ReadCommand.Serializer.deserialize(...)` that threw on the flag.
- The `isForThrift(flags)` term in the Accord guard in `deserializeForAccord(...)`, and the word "thrift" in that error message.

### Tests

Run each command from the worktree root with JDK 21.

- `ant build`: BUILD SUCCESSFUL.
- `ant testsome -Dtest.name=org.apache.cassandra.db.ReadCommandTest`: BUILD SUCCESSFUL, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.db.ReadResponseTest`: 8 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.db.SinglePartitionSliceCommandTest`: 0 failures, 0 errors.
- `ant test -Dtest.name=QueryPagerTest`: 8 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.service.pager.PagingStateTest`: 8 tests, 0 failures, 0 errors.

### Risks

This change breaks the read-path wire format.  `DataLimits` uses ordinal-based serialization.  The removal of the two `Kind` values shifts the ordinals of `CQL_GROUP_BY_LIMIT` and `CQL_GROUP_BY_PAGING_LIMIT` from 4 and 5 to 2 and 3.  A node on this branch cannot exchange a group-by `DataLimits` with a node that still has the old ordinals.  This break is accepted.  There is no upgrade or migration path.

The `IS_FOR_THRIFT` bit at `0x02` is now unused.  The other flag bits keep their explicit values, so their wire positions do not change.

This change does not touch the legacy `isForThrift` placeholder boolean in the `UnfilteredPartitionIterators` serializer.  That byte is a live wire element; the writer and the reader both agree on it, and `CursorReads` and the response test oracle mirror it.  Its removal is a separate, deeper wire change and is out of scope here.

## UDF and UDA

### Purpose

This change removes user defined functions (UDF) and user defined aggregates (UDA).  A UDF let a user run custom Java code inside the database.  A UDA built an aggregate from user functions.  The feature added a large attack surface, a sandbox, a compiler dependency, and cost to the schema.  This branch removes bad old technology, so the removal is a goal.

The native function framework stays.  Built-in functions still work.  These include cast functions, time functions, token functions, collection functions, and the data masking functions.  Only user defined functions and aggregates go away.

### What changes

A client can no longer create a UDF or a UDA.  The grammar drops `CREATE FUNCTION`, `DROP FUNCTION`, `CREATE AGGREGATE`, and `DROP AGGREGATE`.  The parser rejects these statements.

The schema no longer stores or loads user functions.  `KeyspaceMetadata` drops its `UserFunctions` member.  The schema read path is native-only; `FunctionResolver` resolves built-in functions only.

The `system_schema.functions` and `system_schema.aggregates` tables stay.  They are empty, read-only stub tables.  The database never writes a row to them.  They stay so that native-protocol clients and drivers can read the schema without an error.

### What is removed

- The `CREATE FUNCTION`, `DROP FUNCTION`, `CREATE AGGREGATE`, and `DROP AGGREGATE` grammar in `Parser.g` and the generated parser.
- The statement classes for these four statements.
- The `UserFunction`, `UDFunction`, `UDAggregate`, `JavaBasedUDFunction`, `UDFByteCodeVerifier`, and `UDFContext` classes and the UDF sandbox and security manager hooks.
- The `UserFunctions` schema container and the `KeyspaceMetadata` `userFunctions` member.
- The fetch, create, and store logic for user functions and aggregates in `SchemaKeyspace`.
- The `UDTAndFunctionsAwareMetadataSerializer`.
- The UDF configuration in `Config`, `DatabaseDescriptor`, and both `cassandra.yaml` files: `user_defined_functions_enabled`, the threads setting, the two insecure settings, the two timeouts, and the timeout policy.
- The Eclipse compiler (ECJ) dependency, which only the UDF compiler used.
- The cqlsh grammar and fixtures for the four statements.

The `system_schema.functions` and `system_schema.aggregates` tables are NOT removed.  They stay as empty, read-only stub tables for client and driver schema-read compatibility.

### Tests

Run each command from the worktree root with JDK 21.

- `ant build`: BUILD SUCCESSFUL.
- `ant build-test`: BUILD SUCCESSFUL.
- `ant testsome -Dtest.name=org.apache.cassandra.cql3.statements.DescribeStatementTest`: 24 tests, 0 failures, 0 errors.  This test drives the DataStax driver over the native protocol; it confirms the driver connects and reads the schema.
- `ant testsome -Dtest.name=org.apache.cassandra.schema.SchemaKeyspaceTest`: 7 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.schema.SchemaMetadataSerializationTest`: 11 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.schema.TableMetadataSerDeTest`: 3 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.cql3.functions.NativeFunctionsTest`: 2 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.cql3.functions.FunctionFactoryTest`: 9 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.cql3.functions.CastFctsTest`: 13 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.cql3.functions.masking.ColumnMaskTest`: 17 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.auth.FunctionResourceTest`: 8 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.cql3.validation.operations.AggregationTest`: 25 tests, 0 failures, 0 errors.
- `ant testsome -Dtest.name=org.apache.cassandra.cql3.validation.operations.SelectTest`: 83 tests, 0 failures, 0 errors.

### Risks

This change breaks the TCM `KeyspaceMetadata` metadata format.  The metadata no longer carries a `UserFunctions` section, and there is no `Version` gate.  A node on this branch cannot exchange keyspace metadata with a node that still has the old format.  This break is accepted.

The `system_schema.functions` and `system_schema.aggregates` tables stay as empty, read-only stub tables.  They exist only for live driver connectivity.  The DataStax java-driver control connection reads these two tables during a schema refresh; a missing table breaks all native-protocol clients.  The stub tables keep that path working.  They do not hold data and they are not for old data.

## Key cache

### Purpose

This change removes the key cache.  The key cache mapped a partition key to a position in an SSTable data file.  Only the BIG SSTable format used it.  The BTI format never used it; BTI holds its own partition index in memory.  The BIG read path works without the cache; it falls through to the partition index summary and the on-disk index.  This branch removes bad old technology, so the removal is a goal.

The row cache and the counter cache stay.  This change removes only the key cache.

### What changes

The BIG read path no longer consults or fills a key cache.  A read seeks through the partition index summary and the on-disk index, as it already does on a cache miss.

The `CacheService` no longer creates a `keyCache`.  The `CacheType` enum drops its `KEY_CACHE` value.  The `SSTableFormat` interface drops the `KeyCacheValueSerializer` contract; a reader no longer supplies a key cache.

The `nodetool setcachecapacity` command takes two arguments instead of three.  The order is now `<row-cache-capacity> <counter-cache-capacity>`.  The `nodetool setcachekeystosave` command takes two arguments instead of three.  The order is now `<row-cache-keys-to-save> <counter-cache-keys-to-save>`.  The `nodetool invalidatekeycache` command is gone.

The `CacheServiceMBean` drops its key cache attributes and operations.

The `key_cache_*` options are gone from `cassandra.yaml` and `cassandra_latest.yaml`.

### What is removed

- The `org.apache.cassandra.io.sstable.keycache` package: `KeyCache`, `KeyCacheSupport`, and `KeyCacheMetrics`.
- The `KeyCacheKey` cache key class.
- The `keyCache` field, the `initKeyCache()` method, the `KeyCacheSerializer`, and the key cache JMX methods in `CacheService`: `getKeyCacheSavePeriodInSeconds`, `setKeyCacheSavePeriodInSeconds`, `getKeyCacheKeysToSave`, `setKeyCacheKeysToSave`, `invalidateKeyCache`, `invalidateKeyCacheForCf`, and `setKeyCacheCapacityInMB`.
- The matching key cache attributes and operations on `CacheServiceMBean`.
- The `KEY_CACHE` value in the `CacheService.CacheType` enum.
- The `KeyCacheValueSerializer` interface and `getKeyCacheValueSerializer()` in `SSTableFormat`, and the key cache wiring in `BigFormat`, `BigTableReader`, `BigTableWriter`, and `BigSSTableReaderLoadingBuilder`.
- The `KEY_CACHE_SAVE` value in `OperationType`.
- The `KEY_CACHE_HIT` value in `SSTableReadsListener.SelectionReason`.
- The key cache config in `Config`, `DatabaseDescriptor`, and both `cassandra.yaml` files: `key_cache_size`, `key_cache_size_in_mb`, `key_cache_keys_to_save`, `key_cache_save_period`, `key_cache_migrate_during_compaction`, and `key_cache_invalidate_after_sstable_deletion`.
- The `nodetool invalidatekeycache` command and `NodeProbe.invalidateKeyCache()`.
- The third argument of `nodetool setcachecapacity` and `nodetool setcachekeystosave`, and the matching key cache parameters on `StorageServiceMBean`.
- The key cache lines in `Info`, `StatusLogger`, and the `CachesTable` virtual table.
- The `KeyCacheTest`, `AutoSavingCacheTest`, `KeyCacheCqlTest`, `CursorKeyCacheMigrationTest`, and `CacheLoaderBench`, plus the key cache cases in shared tests.

### Tests

Run each command from the worktree root with JDK 21.

- `ant build`: BUILD SUCCESSFUL.
- `ant build-test`: BUILD SUCCESSFUL.
- `ant test -Dtest.name=RowCacheTest`: 11 tests, 0 failures, 0 errors.
- `ant test -Dtest.name=CounterCacheTest`: 5 tests, 0 failures, 0 errors.
- `ant test -Dtest.name=CacheProviderTest`: 2 tests, 0 failures, 0 errors.
- `ant test -Dtest.name=CacheMetricsTest`: 1 test, 0 failures, 0 errors.
- `ant test -Dtest.name=SetCacheKeysToSaveMockTest`: 1 test, 0 failures, 0 errors.
- `ant test -Dtest.name=SSTableReaderTest`: 23 tests, 0 failures, 0 errors.  This test covers the read path for the default format.
- `ant test -Dtest.name=LegacySSTableTest`: 13 tests, 0 failures, 0 errors, 1 skipped.  This test covers the BIG legacy read path.
- `ant test -Dtest.name=PartitionIndexTest`: 28 tests, 0 failures, 0 errors.  This test covers the BTI partition index read path.
- `ant test -Dtest.name=JMXCompatibilityTest`: 4 tests, 0 failures, 0 errors.

`JMXCompatibilityTest` excludes the key cache MBeans from the old baselines.  The excludes cover the `KeyCache` cache metrics, the `KeyCacheHitRate` table metric, the key cache attributes on `CacheServiceMBean`, and the `invalidateKeyCache` operation.

`ConfigCompatibilityTest` adds the removed key cache options to its allow list.  The list covers `key_cache_size`, `key_cache_size_in_mb`, `key_cache_keys_to_save`, `key_cache_save_period`, and `key_cache_migrate_during_compaction`.

### Risks

This change breaks two command line contracts.  The `nodetool setcachecapacity` and `nodetool setcachekeystosave` commands drop their key cache argument.  Each now takes two arguments, not three.  A script that passes three arguments fails.  This break is accepted.

This change breaks a JMX contract.  The `CacheServiceMBean` drops its key cache attributes and operations: `KeyCacheCapacityInMB`, `KeyCacheKeysToSave`, `KeyCacheSavePeriodInSeconds`, `MigrateKeycacheOnCompaction`, and `invalidateKeyCache`.  A client that calls these fails.  This break is accepted.
