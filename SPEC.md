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
