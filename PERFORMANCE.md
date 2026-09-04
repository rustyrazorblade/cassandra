# async-compaction-writer: performance record

Branch based on `21463-cursor-collections`. Every figure here was measured on this machine,
not modelled. Modelled estimates are marked as such.

## Harness

`CompactionWritePathBench` (`test/microbench/.../sstable/`), a `CompactionBench` subclass whose
rows carry a blob payload so a run moves gigabytes through `CompressedSequentialWriter` rather
than the few megabytes the bigint-only base class produces.

```
ant microbench -Dno-build-accord=true -Dbenchmark.name=CompactionWritePathBench \
  -Djmh.args="-p isCursor=true,false -p payloadSize=4096 -p rowCount=140000 \
              -p sstableCount=16 -f 1 -wi 2 -i 15 -jvmArgsAppend -Xmx24G"
```

- Dataset: 4543 MiB across 16 SSTables (memtable pressure splits these further, ~128 in practice).
- Payload: 4 KiB, half repeating motif and half random, which lands LZ4 near 2:1. Fully random
  data makes LZ4 bail out early and understates compression cost; uniform data compresses away
  and overstates it.
- Compaction throttling disabled: `setCompactionThroughputMebibytesPerSec(0)`. The shipped
  default of 64 MiB/s is roughly a quarter of what the serial path sustains, so a run at the
  default measures the rate limiter and reports no gain.
- Machine: 16 cores, 128 GiB RAM, JDK 21.0.10. The dataset fits in page cache, so this measures
  the CPU-side write path, not the device.
- 15 iterations after 2 warmups. Intervals are JMH's 99.9%.

Anything comparing runs must hold every parameter above fixed.

## Results

### Baseline, before any change

| path | ms/op | MiB/s |
|---|---|---|
| cursor | 15809 +/- 565 | 287 |
| iterator | 18476 +/- 600 | 246 |

### Phase 1: chunk and CRC trailer in one gathering write (`69bf76735b`)

| path | ms/op | MiB/s | change |
|---|---|---|---|
| cursor | 14781 +/- 411 | 307 | -6.5% |
| iterator | 17606 +/- 332 | 258 | -4.7% |

The cursor intervals do not overlap ([15245, 16374] vs [14370, 15192]). The iterator intervals
overlap by about 60 ms, so that arm sits at the significance threshold rather than past it.

An earlier run at 3 iterations gave cursor 15275 +/- 1827 and iterator 17910 +/- 1632. The
+/-12% intervals there could not resolve a change this size, which is why the record above uses 15.

### Phase 2: chunk-buffer slot pool, work still inline

Selected with `-p asyncWriter=true`, which sets `async_compaction_writer_enabled`. Compared
against phase 1, which is the same build with the flag off.

| path | ms/op | MiB/s | vs phase 1 |
|---|---|---|---|
| cursor | 14931 +/- 612 | 304 | +1.0% |
| iterator | 17702 +/- 568 | 257 | +0.5% |

Neutral, which is the result this phase needed. Both shifts are well inside the intervals. The
work still runs inline on the calling thread; all that changed is that the producer takes a fresh
slot instead of clearing the one it just filled, so a regression here would have meant the swap
itself costs something unaccounted for.

### Phase 3: compress, write and checksum on a writer thread

Same flag, `-p asyncWriter=true`. Compare against phase 2, which is the same build with the work
still inline, and against phase 1, which is the synchronous writer.

| path | ms/op | MiB/s | vs phase 2 | vs baseline |
|---|---|---|---|---|
| cursor | 11672 +/- 359 | 389 | -21.8% | -26.2% |
| iterator | 14663 +/- 606 | 310 | -17.2% | -20.6% |

Neither arm's interval overlaps phase 2's: cursor [11313, 12031] against [14319, 15543], iterator
[14057, 15269] against [17134, 18270]. Process CPU went from 75% to 103%, so the second thread is
doing real work rather than the producer simply waiting differently.

Against the synchronous writer in the same build this is 1.27x on the cursor path, short of the
1.6x the profile allows. The gap is the drain points: `syncInternal` runs at every writer switch,
`sstable_preemptive_open_interval` defaults to 50 MiB so a 4.5 GiB compaction hits about ninety of
them, and the trickle fsync serialises the writer thread every 10 MiB. Closing that gap is
follow-on work, not a defect in the split.

### Phase 4: pool depth and a periodic force

Two changes, measured separately on the cursor path.

| variant | ms/op | MiB/s | improvement over baseline |
|---|---|---|---|
| baseline | 15809 +/- 565 | 287 | -- |
| phase 3, writer thread, 16 slots, byte-triggered fsync | 11672 +/- 359 | 389 | +35.4% |
| fsync on its own thread, 16 slots | 11380 +/- 376 | 399 | +38.9% |
| fsync on its own thread, 256 slots | 9813 +/- 463 | 463 | +61.1% |
| periodic 1s force, 256 slots | 6093 +/- 128 | 746 | **+159.4%** |

Two findings, both of which contradict what the design predicted.

**Pool depth mattered more than taking the fsync off the writer thread.** Moving the fsync to its
own thread while leaving 16 slots was worth 2.5%; deepening the pool to 256 on top of that was
worth another 16%. Sixteen 16 KiB slots is 256 KiB, 0.6 ms of runway at this rate, so the producer
felt every writer-side hiccup regardless of which thread the fsync ran on.

**Forcing on a byte interval was itself most of the cost.** trickle_fsync's 10 MiB interval fires
about 450 times over a 4.4 GiB compaction, and fdatasync forces the whole file, not a range, so
that is hundreds of full-file forces against a file growing to 4.4 GiB. Replacing it with a 1 s
periodic force, about six over a compaction, took another 38% off. The byte interval exists to cap
how long the *writing* thread stalls in one force; nothing writes on that thread here, so the cap
bought nothing and cost a great deal.

Corroborated by Cassandra's own compaction log, which is independent of the JMH harness and shows
identical input and output every run:

| run | compacted | logged time | logged read throughput |
|---|---|---|---|
| phase 3 | 4.395 -> 4.398 GiB | 15,335 ms | 293 MiB/s |
| fsync thread, 256 slots | 4.395 -> 4.398 GiB | 10,377 ms | 434 MiB/s |
| periodic force, 256 slots | 4.396 -> 4.398 GiB | 6,179 ms | 728 MiB/s |

`async_compaction_writer_slots` now defaults to 256. Each slot is `chunk_length_in_kb`, so the
memory is that times the slot count, per open data file: 4 MiB at the default 16 KiB chunk, 16 MiB
if the chunk length is raised to 64 KiB.

### Pool size: 4 MiB is enough

Both arms in one invocation, cursor path, byte-budget config.

| variant | ms/op | MiB/s | improvement over baseline |
|---|---|---|---|
| baseline | 15809 +/- 565 | 287 | -- |
| 4 MiB pool | 6146 +/- 102 | 739 | +157.2% |
| 16 MiB pool | 6184 +/- 80 | 735 | +155.6% |

Indistinguishable, so 4 MiB already covers the longest writer-side stall and more buys nothing.
This also re-measures the byte-budget refactor at the same setting the slot-count form used
(4 MiB / 16 KiB is 256 slots): 6146 against 6093, inside the intervals.

### After the correctness review

The review (REVIEW-async-writer.md) found one critical and three major defects. Fixing them
replaced the dedicated writer thread with a shared pool, which costs a little indirection.

| variant | ms/op | MiB/s | improvement over baseline |
|---|---|---|---|
| baseline | 15809 +/- 565 | 287 | -- |
| before the fixes | 6146 +/- 102 | 739 | +157.2% |
| after the fixes | 6267 +/- 161 | 725 | +152.2% |

Intervals overlap ([6044, 6248] against [6106, 6428]), so the correctness work is close to free.

### Back to one thread per writer

A shared pool caps total compression bandwidth at its thread count. The path it replaces has no
such cap: the synchronous writer compresses on the producer's own thread, so every writer gets a
core's worth. DataComponent.buildWriter also routes memtable flushes, streams, scrub and index
builds through here, so a pool sized to concurrent_compactors would queue a flush behind
compactions, which never used to happen.

| variant | ms/op | MiB/s | improvement over baseline |
|---|---|---|---|
| baseline | 15809 +/- 565 | 287 | -- |
| shared pool | 6267 +/- 161 | 725 | +152.2% |
| one thread per writer | 6171 +/- 236 | 736 | +156.1% |

Intervals overlap, so on a single compaction the two are equivalent; the pool's cost would show
only under concurrency, which this harness does not exercise.

### Summary across phases, cursor path

| | ms/op | MiB/s | improvement over baseline |
|---|---|---|---|
| baseline | 15809 | 287 | -- |
| phase 1, gathering write | 14781 | 307 | +7.0% |
| phase 2, slot pool inline | 14931 | 304 | +5.9% |
| phase 3, writer thread | 11672 | 389 | +35.4% |
| phase 4, deep pool + periodic force | 6093 | 746 | +159.4% |
| after review fixes, one thread per writer | 6171 | 736 | +156.1% |

The 1.6x ceiling the profile predicted assumed the write side was 37.2% of a fixed total. It was
not fixed: several hundred full-file fsyncs were part of that total, and removing them shrank the
work rather than merely overlapping it.

## CPU profile

async-profiler through `ant microbench-with-profiler`, cursor path, phase 1 applied,
1.3 GiB dataset (`rowCount=40000`), 749 samples.

| | share of compaction | share of write side |
|---|---:|---:|
| `SequentialWriter.doFlush`, the whole write side | 37.2% | 100% |
| `LZ4Compressor.compress` | 21.1% | 56.6% |
| CRC32 update | 7.6% | 20.4% |

Compression plus CRC is 77% of the write side; the remainder is syscalls and bookkeeping.

A naive per-title sum of the flamegraph double-counts nested syscall frames (`writev` inside
`writev0`) and produced a write figure above 100% of its own parent. Only containment-checked
figures are recorded here.

**This sets the ceiling for the threading work.** If a writer thread absorbs the whole 37.2%,
the merge side at 62.8% becomes the slowest pipeline stage, so the ceiling is 1/0.628, about
**1.6x**, or a 59% throughput gain. One writer thread has to keep up to reach it.

## Settled facts

- **The CRC trailer cost one syscall, not four.** `DataOutputStream.writeInt` fills an internal
  `writeBuffer` and issues a single `write(byte[], 0, 4)`; a counting `WritableByteChannel`
  driven through the exact construction at `CompressedSequentialWriter.java:167` recorded one
  four-byte write per chunk. The design estimate of four syscalls, worth 14% of the write side,
  was wrong; the real figure was about 4%. Phase 1 still returned 6.5%, because removing the
  `DataOutputStream` layer and the `rewind()` costs more than the syscall alone.
- **The write buffers are off heap.** `CompressedSequentialWriter` allocates through
  `compressor.preferredBufferType()`; LZ4, Zstd and Snappy return `OFF_HEAP`. Deflate and Noop
  return `ON_HEAP`.
- **`mark()` and `resetAndTruncate()` never run during compaction.** Only
  `SortedTableScrubber.tryAppend` reaches them. The cursor writer and `CursorCompactor` contain
  no call to either.

## Known noise sources

- Dataset generation runs once per JMH fork, about two minutes. An attempt to cache SSTables
  across forks failed: `loadNewSSTables` renumbers generations on import and collided
  (`pa-103` renamed onto `pa-130`, itself being imported). The cache path survives behind
  `-Dcassandra.bench.dataset` but is off by default and is not used for any figure here.
- `ant microbench` re-runs the Accord gradle build, which does `clean build` every time and
  has hung on this machine. Every run above passed `-Dno-build-accord=true`.
