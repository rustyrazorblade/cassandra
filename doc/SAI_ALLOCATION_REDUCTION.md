# SAI allocation/CPU reduction (branch `sai-allocation-reduction`)

This branch implements the "cheap Tier 1" findings from a review of allocation and CPU
overhead on SAI's (Storage Attached Index) hot query path, on top of
`cassandra-6.0-rustyrazorblade`. It exists to record what was done, how it was measured,
and - more importantly - what the measurements actually showed, including a negative
result that turned out to be the most useful finding.

## Commits, in order

1. `1b25792eda` - Add JMX allocation-measurement baseline for SAI query execution.
   Adds `QueryContext.totalQueryAllocatedBytes()` (via `ThreadMXBean.getThreadAllocatedBytes`)
   and exposes it as a new `AllocatedBytes` histogram on `TableQueryMetrics.PerQueryMetrics`,
   alongside the existing `QueryLatency` timer. This commit intentionally lands *before* any
   optimization, so both metrics can be read before/after each subsequent commit.
2. `42a6f7a3c3` - Avoid re-hashing partition key bytes when the token is already known
   (`SkinnyPrimaryKeyMap`/`WidePrimaryKeyMap`/`PrimaryKey.Factory`).
3. `abd1573483` - Cache the per-block `DirectReader` in `AbstractBlockPackedReader.get()`.
4. `94b29af2a6` - Hoist a reusable `BytesRefBuilder` for `KeyLookup.Cursor#clusteredSeekToKey`.
5. `28c3e06546` - De-box and de-duplicate per-row work in SAI post-filtering
   (`Operation.BooleanOperator`, `FilterTree`'s `nowInSeconds()` call, `Expression`'s
   IN-list unpacking and `Value.encoded`).
6. `c56e97af5e` - Reuse `SeekingRandomAccessInput` across bbtree leaves in
   `BlockBalancedTreeReader.FilteringIntersection` (scoped down from the original review's
   suggestion - see "The FixedBitSet finding" below).

Each commit's own message has the full design rationale, the independent opus-model
review summary, and that commit's specific before/after numbers. This document is the
cross-commit summary.

## Methodology

For each commit, the same procedure was used to get a before/after comparison:

1. A temporary "scratch benchmark" `@Test` method was added to
   `test/unit/org/apache/cassandra/index/sai/metrics/QueryMetricsTest.java` (never
   committed - added, measured, removed before landing each real commit). It:
   - creates a table and SAI index sized to exercise the code path the commit touches
     (20,000 rows; a wide/clustered table with two indexed columns for the commit that
     needed a multi-predicate intersection),
   - runs 20 warm-up queries (to get past JIT tiering, class loading, and other
     one-time costs before measuring),
   - snapshots `AllocatedBytes`/`QueryLatency`'s JMX `Count`/`Mean` at that point,
   - runs 40 more queries,
   - takes the delta of `Count * Mean` (i.e. the sum) between the two snapshots, divided
     by the delta in `Count`, to get the post-warmup steady-state mean - isolating the
     "hot" average from the warm-up phase's one-time costs, which would otherwise
     dominate a naive read of the final `Mean` (a Dropwizard histogram's `Mean`, not a
     true arithmetic mean over a small `Count`).
   - fails with the computed numbers in the assertion message (the established way to get
     values out of this test harness's JVM, since ad hoc `System.out`/logger output isn't
     reliably captured).
2. `-Dcassandra.test.random.seed=<fixed value>` was passed as a JVM arg
   (`-Dtest.jvm.args="-Dcassandra.test.random.seed=42"`) to pin `CQLTester.Fuzzed`'s
   per-JVM randomized storage-engine config (memtable class, sstable format, disk access
   mode). Without this, the "before" and "after" runs can silently pick *different*
   storage engines, which is itself a large confound - this was caught and fixed partway
   through this work (see "Mistakes made along the way" below).
3. **Rebuilt with `ant build-test` (not `ant build` alone) before every run**, including
   when switching between the "before" (`git stash`) and "after" (`git stash pop`) states.
   This was the single biggest methodology bug found and fixed during this work: the test
   runtime classpath (`cassandra.classpath.test`) loads classes from the packaged jar
   (`build/apache-cassandra-*.jar`), not the raw `build/classes/main` directory that plain
   `ant build` compiles to. Only `ant build-test`'s `_main-jar` dependency repackages that
   jar. Using `ant build` alone between states silently measures the same (stale) code
   twice - see below.
4. Isolated one commit's change at a time via `git stash push -- <the commit's files>`,
   never a broad `git stash`, so only the change under test moved between the two runs.

## Results

| Commit | AllocatedBytes (before → after) | QueryLatency (before → after) |
|---|---|---|
| Fix 1a (token reuse) | 24,110,697.8 → 24,110,697.8 bytes/query (identical) | 6,188–6,729ns → 6,886–6,966ns/query |
| Fix 2 (DirectReader cache) | 24,110,697.8 → 24,110,697.8 bytes/query (identical) | 7,202ns → 7,673ns/query |
| Fix 3 (BytesRefBuilder reuse) | 28,935,954.4 → 28,935,954.4 bytes/query (identical, wide-table benchmark) | 13,009ns → 12,817ns/query |
| Fix 4 (de-boxing/de-dup) | 24,110,697.8 → 24,110,697.8 bytes/query (identical) | 6,586ns → 7,016ns/query |
| Fix 6 (SeekingRandomAccessInput reuse) | 24,110,697.8 → 24,110,697.8 bytes/query (identical) | 6,486ns → 7,257ns/query |

**AllocatedBytes was bit-identical before and after every single one of the five fixes.**
QueryLatency moved by roughly ±200-770ns (about 1.5-12%) each time, with no consistent
direction (Fix 3 went *down*; the other four went *up*) - i.e. noise, not signal. Repeated
runs of *identical* code (no change at all) showed 500-700ns (~8-11%) swings on their own,
which is comparable to or larger than every one of the above deltas.

## Why: the JIT already eliminates these allocations once hot

Every one of these five fixes removes a small, short-lived object that was constructed
inside a method and never stored anywhere beyond that method call (a `long[2]` hash-output
array, a `DirectReader`/`LongValues` wrapper, a `BytesRefBuilder`, autoboxed `Boolean`s
[JVM-cached singletons anyway], a `SeekingRandomAccessInput` wrapper). Objects like this
are exactly what JIT escape analysis is designed to eliminate via scalar replacement once a
method is hot enough to be C2-compiled - which the 20-iteration warm-up in this benchmark
reliably achieves. So in the *specific scenario this benchmark measures* - a single query,
already JIT-warmed, no other threads or GC pressure competing for allocation - the
"before" code was, in practice, not actually putting these objects on the heap either.

This does not mean the fixes are pointless. It means this particular benchmark - clean,
single-threaded, steady-state JIT-warmed throughput measured via JMX histograms - is the
wrong instrument to see their effect. They likely matter more:
- under real allocation/GC pressure at production concurrency, where escape analysis's
  scalar-replacement decisions can be different (or where the removed CPU work, not
  allocation, is what shows up as latency under load);
- during cold start / interpreted execution, before a query pattern is hot enough to be
  C2-compiled;
- for the CPU work itself in cases where escape analysis doesn't apply (e.g. a real
  Murmur3 hash computation avoided in Fix 1a is real CPU cycles regardless of whether the
  scratch array it uses is heap-allocated).

Measuring any of that would need a different tool - a profiler (async-profiler/JFR) under
sustained concurrent load, or a proper JMH microbenchmark isolating just the changed method
- not this coarse, single-query, end-to-end JMX approach. That's out of scope for this
pass; if this branch's changes are ever questioned for their actual production impact,
that's where to start.

## The FixedBitSet finding (Fix 6)

The original review's "Fix 6" proposed reusing *both* a `FixedBitSet` and a
`SeekingRandomAccessInput` across bbtree leaves in
`BlockBalancedTreeReader.FilteringIntersection`. While implementing it, tracing the actual
lifetime of the `FixedBitSet` surfaced a real correctness bug in that suggestion:

- `filterLeaf()` builds a `FixedBitSet` per leaf and wraps it in a `FilteringPostingList`,
  which stores the bitset **by reference** and reads it **lazily** during iteration
  (`nextPosting()`/`advance()`), not eagerly at construction.
- That `FilteringPostingList` is added to a `PriorityQueue` accumulated across the *entire*
  recursive tree traversal (potentially many leaves), and only merged (via
  `MergePostingList`, which interleaves by polling whichever list currently has the lowest
  row ID - not draining one list before touching the next) after the whole traversal
  completes.
- So every surviving leaf's `FixedBitSet` must stay simultaneously alive, distinct, and
  unmutated for the entire merge phase. Reusing/clearing a single mutable `FixedBitSet`
  across leaves would silently corrupt results (a later leaf's bits read back by an
  earlier leaf's `FilteringPostingList`) - not a crash, just wrong query results.

This was independently verified by an opus-model review pass specifically asked to
re-derive (not just trust) the conclusion, which confirmed it by reading all four files
involved and additionally assessed whether some safe variant (pooling, one-leaf-at-a-time
draining) could still capture the win - concluding no, because the leaves' lifetimes
genuinely overlap by construction, which is the entire point of the priority-queue merge.

Only the `SeekingRandomAccessInput` half of the original suggestion was implemented
(commit `c56e97af5e`); the `FixedBitSet` stays a fresh per-leaf allocation.

## Mistakes made along the way (recorded so they aren't repeated)

- **`ant build` vs `ant build-test`**: an early round of "before/after" measurements for
  Fix 1a were invalidated by using plain `ant build` (which compiles to
  `build/classes/main` but does not repackage `build/apache-cassandra-*.jar`) between the
  `git stash`/`git stash pop` states, while the test runner (`ant testsome
  -Dno-build-test=true`) loads classes from that jar. Both "before" and "after" runs ended
  up measuring the same (whichever was jar'd most recently) code. Caught by writing a
  focused regression test (`PrimaryKeyMapTest`), deliberately corrupting the fix under
  test, and finding the "corrupted" test run still passed - which shouldn't have been
  possible and led directly to finding the stale-jar bug. Fixed by always using
  `ant build-test` (never `-Dno-build-test=true` on the build step) when switching
  between source states being compared.
- **Unpinned random seed**: before pinning `-Dcassandra.test.random.seed`, "before" and
  "after" runs could pick different `CQLTester.Fuzzed` storage-engine configs (e.g. BIG vs
  BTI sstable format, SkipList vs Trie memtable), which swamps any real code-level signal.
- **IN-clause + `ALLOW FILTERING` benchmark**: an attempt to specifically exercise Fix 4's
  IN-list-caching change via `WHERE v1 IN (...) ALLOW FILTERING` resulted in zero recorded
  `AllocatedBytes`/`QueryLatency` samples - that query shape appears to route around the
  indexed per-query metrics path entirely. Not pursued further (IN-list correctness is
  covered by `ExpressionTest`/`UnindexedExpressionsTest` instead); Fix 4 was measured with
  a plain range query, which still exercises the other three sub-changes in that commit.
