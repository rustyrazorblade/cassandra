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
