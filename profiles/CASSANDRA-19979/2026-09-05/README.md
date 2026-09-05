# CASSANDRA-19979: sendfile for compressed SSTable streaming

Does replacing the compressed writer's pread-into-a-pooled-buffer with a `SharedDefaultFileRegion`
reduce CPU and allocation, and by how much.

## What was compared

| | commit | path |
|---|---|---|
| before | `8a261ea9c4` Add a burn test for compressed SSTable streaming | buffered: pread into a pooled direct buffer, then a socket write |
| after | `eab773e2e1` Send compressed SSTable sections with sendfile | `SharedDefaultFileRegion` per batch, no user-space buffer |

Branch `CASSANDRA-19979`, worktree `/Users/jhaddad/dev/cassandra/streaming-slowpath`.
The two commits differ only in the writer; the burn test is byte-identical in both.

## Workload

`CompressedStreamWriterBurnTest` streams a 521.922 MiB compressed SSTable through
`CassandraCompressedStreamWriter` to a real TCP server on loopback, over and over. Stream
throughput throttling is disabled in the test, `stream_chunk_size` is the 128 KiB default, and the
data file is hot in the page cache after the first iteration.

Counters, 200 iterations, no agent attached:

```
ant burn-testsome -Dno-build-accord=true \
  -Dtest.name=org.apache.cassandra.db.streaming.CompressedStreamWriterBurnTest \
  -Dtest.jvm.args="-Dcassandra.test.streaming_burn_mib=512 -Dcassandra.test.streaming_burn_iterations=200"
```

Profiles, 2500 iterations, roughly 145 s before and 120 s of streaming after, which clears the
two-minute steady-state floor:

```
ant burn-testsome -Dno-build-accord=true \
  -Dtest.name=org.apache.cassandra.db.streaming.CompressedStreamWriterBurnTest \
  -Dtest.burn.timeout=3600000 \
  -Dtest.jvm.args="-Dcassandra.test.streaming_burn_mib=512 -Dcassandra.test.streaming_burn_iterations=2500 \
-agentpath:/opt/homebrew/lib/libasyncProfiler.dylib=start,event=<cpu|alloc>,interval=1ms,jfr,\
file=<dir>/<before|after>-<event>-%p.jfr,\
exclude=*junit*,exclude=org.junit.*,exclude=*JUnitCore*,exclude=*RunListener*"
```

The counters come from agent-free runs on purpose: a sampling agent allocates, and it would be
measuring itself in exactly the number under study.

## Results

Per 521.922 MiB streamed, best of 200 for time, minimum of 200 for allocation:

| | before | after |
|---|---|---|
| wall | 58 ms | 48 ms |
| throughput | 8998.7 MiB/s | 10873.4 MiB/s |
| process CPU | 152 ms | 136 ms |
| heap allocated | 44.821 MiB | 1.317 MiB |
| networking pool acquisitions | 4177 | 1 |

Wall time over three earlier JVMs per arm was 63/65/68 ms before and 49/50/51 ms after; the ranges
do not overlap.

4177 pool acquisitions is one per 128 KiB chunk of 521.922 MiB, which is what the buffered path is:
one pooled buffer per chunk. The single remaining acquisition is the stream header.

### CPU, from `diff-cpu.svg`

Total samples 412943 before, 279135 after, a factor of 1.48. Self samples on the frames that moved:

| frame | before | after |
|---|---|---|
| `sendfile` | 0 | 118391 |
| `pread` | 78100 | 109 |
| `write` | 87900 | 1318 |
| `StackTraceElement` allocation | 9012 | 94 |

166000 samples of `pread` plus `write` become 118391 of `sendfile`, about 29% less work for the same
bytes. The receiving side's `read` is unchanged in absolute terms; the differential shows its share
rising only because the total fell.

### Allocation, from `diff-alloc.svg`

Total samples 239035 before, 20536 after, a factor of 11.6. By class:

| class | before | after |
|---|---|---|
| `StackTraceElement[]` | 142127 | 1032 |
| `GlobalBufferPoolAllocator$Wrapped` | 1749 | 0 |
| `SharedDefaultFileRegion` | 0 | 1371 |
| `ProgressInfo` | 1258 | 1214 |
| `AsyncChannelPromise` | 946 | 973 |
| `GenericFutureListenerList` | 637 | 632 |

The per-batch bookkeeping is unchanged, as it should be: both paths submit one batch, one promise and
one progress event per chunk. What goes away is the pooled buffer itself and the reference-counting
that rides on it. `SharedDefaultFileRegion` is the new per-batch object and it is cheaper than the
`Wrapped` buffer it replaces.

## What these numbers cannot be used for

- **The heap figures are inflated and are a comparison, not a production number.** The ant test JVM
  sets `cassandra.debugrefcount`, so every reference-counted buffer captures a stack trace.
  `StackTraceElement[]` is 59% of the before total on its own. Discounting it, the reduction is
  closer to 5x than to the 11.6x the raw totals show. The direction is real; the magnitude is not
  transferable.
- **Loopback flatters the buffered path.** The copy sendfile removes is a copy into memory that is
  already hot, and there is no NIC. On a real link the gap should be wider, not narrower.
- **The page cache is hot throughout.** This measures the copy, not the read.
- **Single machine, single stream.** Nothing here says anything about many concurrent sessions, or
  about a repair or bootstrap in a real cluster.

## Machine

Apple M4 Max, macOS 26.6.2, OpenJDK 21.0.10, async-profiler 4.5.

## Files

- `before-cpu.html`, `after-cpu.html`, `diff-cpu.svg`
- `before-alloc.html`, `after-alloc.html`, `diff-alloc.svg`
- `*.jfr` and `*.collapsed` are the recordings the graphs were rendered from.
