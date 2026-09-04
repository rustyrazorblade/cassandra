# Correctness review: async-compaction-writer

Branch: `async-compaction-writer` (6 commits on `21463-cursor-collections`).  Files below are relative to the worktree root.  Line numbers are from the current tree.

Abbreviations: ACSW = `src/java/org/apache/cassandra/io/compress/AsyncCompressedSequentialWriter.java`, CSW = `src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java`, SW = `src/java/org/apache/cassandra/io/util/SequentialWriter.java`.

## CRITICAL

### C1. `mark()` is not overridden.  It captures stale `chunkOffset` and `chunkCount`; `resetAndTruncate` then rebuilds the buffer from the wrong chunk and truncates live data.

Sites:

- CSW:314 `mark()` reads `chunkOffset` and `chunkCount` on the producer.  Both are written only on the writer thread (CSW:266, CSW:277).  No drain precedes the read.
- ACSW:284 overrides `resetAndTruncate` but not `mark`.
- Reached from `SortedTableWriter.mark()` (`src/java/org/apache/cassandra/io/sstable/format/SortedTableWriter.java:328`) via `SSTableRewriter.tryAppend` (`src/java/org/apache/cassandra/io/sstable/SSTableRewriter.java:147`), which `SortedTableScrubber.tryAppend` (`.../format/SortedTableScrubber.java:302`) calls for every partition on the BIG and BTI scrub paths.
- `DataComponent.buildWriter` (`src/java/org/apache/cassandra/io/sstable/format/DataComponent.java:122`) has no operation-type gate on the async branch, unlike the direct branch at :111-112 which lists SCRUB as `UNSUPPORTED_CORRECTNESS` for exactly this reason (:72).  A scrub with `async_compaction_writer_enabled: true` gets this writer.

Sequence (chunk length CL, `off_n` = file offset of chunk n):

1. Producer has flushed chunks 0..4 (`submitted == 5`).  Writer has completed 0..2, so `chunkOffset == off_3`, `chunkCount == 3`.
2. Scrubber calls `mark()`.  Mark = `{chunkOffset: off_3, uncDataOffset: 5*CL + p, validBufferBytes: p, nextChunkIndex: 4}`.  Correct values would be `off_5` and `6`.
3. `append` throws on a corrupt partition.  `resetAndTruncate(mark)`: `drain()` runs; writer finishes 3 and 4; `chunkOffset == off_5`.
4. CSW:327 `realMark.chunkOffset (off_3) != chunkOffset (off_5)`.  The in-buffer branch is skipped although the mark is inside the current buffer.
5. CSW:335 `syncInternal()` flushes the partial buffer as chunk 5.  CSW:337 sets `chunkOffset = off_3`.  CSW:340 computes `chunkSize` from chunk 3.  CSW:352-362 reads chunk 3 and decompresses it into `buffer`.
6. CSW:393 `buffer.position(p)`.  CSW:395 `bufferOffset = 5*CL`.  CSW:396 `chunkCount = 3`.  CSW:399 truncates the file to `off_3` and the offsets to 3 chunks.

State after step 6: the buffer holds chunk 3's bytes, labelled as uncompressed offset `5*CL`.  Chunks 3 and 4 are gone from disk and from the offsets file.  The next flush writes chunk 3's old bytes plus new data at `off_3` as chunk index 3.  Every index entry the scrubber already recorded for partitions in `[3*CL, 5*CL + p)` now points at different bytes.  The scrub output is a corrupt sstable that passes its own CRC.

Also affected once truncated: `durableOffset` (ACSW:118) is only ever assigned on the writer thread, so after the truncate it still reports the pre-truncate offset to the post-flush listener until the next chunk lands.  `SortedTableScrubber.java:189` constructs its rewriter with early opening enabled, so an early-open reader can be handed a data length past the truncation point.

## MAJOR

### M1. Every writer-thread `RuntimeException` becomes an `FSWriteError` and reaches the disk failure policy.

ACSW:365-367.  A `RuntimeException` that is not `FSWriteError` is wrapped in `FSWriteError(new IOException(t), getPath())`.  CSW:237 throws `RuntimeException("Compression exception")` for any compressor `IOException`; `LZ4Compressor.compress` (`.../LZ4Compressor.java:132`) converts `LZ4Exception` to that.  A `BufferOverflowException` from an undersized `compressed` buffer takes the same path.

On the synchronous writer these propagate as plain `RuntimeException`: the compaction aborts, `JVMStabilityInspector.inspectThrowable` (`src/java/org/apache/cassandra/utils/JVMStabilityInspector.java:161-163`) does not treat them as `FSError`, and the node keeps serving.  On this branch the same fault is an `FSWriteError`: with `disk_failure_policy: die` (:161-163) the JVM is killed; with the default `stop` (`src/java/org/apache/cassandra/service/DefaultDiskErrorsHandler.java:85-90`) transports are stopped; with `best_effort` (:106) the data directory is marked unwritable.  A compressor bug on one table now takes the node down.

The same wrapping applies to `FSReadError` from `seekToChunkStart` (CSW:435): read error reported as write error.  Same policy, wrong class.

### M2. Switched-away writers keep their thread, their slot pool and their scheduled force until the whole compaction finishes.

`SSTableRewriter.switchWriter` (`src/java/org/apache/cassandra/io/sstable/SSTableRewriter.java:279`) calls only `openFinalEarly()` on the outgoing writer, which does `dataWriter.sync()` (`BigTableWriter.java:218`, `BtiTableWriter.java:140`).  `prepareToCommit`, and so `AsyncTransactionalProxy.doPreCleanup` (ACSW:404) and `stopWriter` (ACSW:370), run only in `SSTableRewriter.doPrepare` (:340) at the end.  `ShardedMultiWriter` (`.../unified/ShardedMultiWriter.java:143`) keeps every shard writer open until `finish`.

Per idle writer that is: one parked thread, `slotCount` direct buffers (4 MiB at the default budget, ACSW:145-146), and with `trickle_fsync: true` a periodic task on `ScheduledTasks` (ACSW:152-154).  The synchronous writer holds one chunk buffer plus `compressed` in the same state.  A major compaction that emits 1000 sstables through `MaxSSTableSizeWriter` holds 1000 threads and 4 GiB of direct memory it will not touch again until commit.  `MemoryUtil.clean` (`.../MemoryUtil.java:330`) is a no-op for heap buffers, so the figure is off-heap only for compressors that prefer `OFF_HEAP` (LZ4: `LZ4Compressor.java:244`), which is the default.

### M3. The background force runs blocking I/O on the shared single-thread `ScheduledTasks` executor.

ACSW:153 schedules `backgroundForce` on `ScheduledExecutors.scheduledTasks`.  That executor is a `ScheduledThreadPoolExecutorPlus` with `super(1, threadFactory)` (`.../ScheduledThreadPoolExecutorPlus.java:79`).  `backgroundForce` (ACSW:222) calls `SyncUtil.force` inside `fsyncLock`, which on a loaded device takes tens to hundreds of milliseconds per file.  One task per open async writer, including the idle ones from M2, so with `concurrent_compactors` compactions each on several sharded writers the single scheduler thread spends most of every second inside `fsync`.

Other users of the same thread include `MessagingService`, `HintsService`, `DynamicEndpointSnitch`, `LoadBroadcaster`, `DiskUsageMonitor`, `MonitoringTask`, `AbstractAllocatorMemtable` and `PaxosUncommittedTracker` (`rg -l ScheduledExecutors.scheduledTasks src/java`).  All of them are delayed by the sum of the forces queued ahead of them.  Only active with `trickle_fsync: true`.

## MINOR

### m1. `stopWriter` gives up silently after 30 s and frees the writer's memory anyway.

ACSW:387 `writer.join(WRITER_JOIN_MILLIS)`.  The return value of `isAlive()` is not checked and nothing is logged.  Afterwards `doPreCleanup` (ACSW:415) closes the channel and cleans every buffer it can find.

I could not build a JVM crash out of this.  After the channel close unblocks a stuck `fchannel.write` with `AsynchronousCloseException`, the writer thread touches only Java-side fields (`slot.clear()`, `free.offer`, `completed++`) before it takes the poison pill.  `FileChannelImpl.implCloseChannel` also waits for in-flight I/O on other threads, so the producer blocks in `channel.close()` rather than racing.  What does happen: if the writer's `free.offer(slot)` (ACSW:203) lands after the producer's `free.drainTo` (ACSW:418), that slot is never cleaned.  One chunk of direct memory leaks per occurrence.

### m2. A writer thread that exits on `InterruptedException` leaves `failure` unset; the producer then hangs without bound.

ACSW:180-183 returns from `writerLoop` without latching a failure.  From then on `completed` is frozen.  `takeFreeSlot` (ACSW:302-316) polls forever once the pool is exhausted, `drain()` (ACSW:267) spins forever in `doPrepare`, and nothing in `stopWriter` ever runs because the producer never reaches `close()`.

I found no code path that interrupts this thread: it is a raw `Thread`, `CompactionManager.forceShutdown` (`CompactionManager.java:335-338`) stops compactions through `Holder.stop()`, not `Thread.interrupt()`.  So the hang needs an interrupter that does not exist today.  It is listed because the exit path is unguarded, not because I have a trigger.

### m3. A producer-thread interrupt is reported as `FSWriteError`.

ACSW:311-314 and ACSW:344-347 convert `InterruptedException` to `FSWriteError` and so into the disk failure policy (see M1 for consequences).  The synchronous writer never blocks, so an interrupt there surfaces from wherever the compaction next checks it.  Same caveat as m2: I found nothing that interrupts a compaction thread, so no trigger today.

### m4. `getEstimatedOnDiskBytesWritten()` returns a stale `chunkOffset`.

CSW:199 reads `chunkOffset` on the producer with no drain.  Callers: `MaxSSTableSizeWriter.java:78`, `SplittingSizeTieredCompactionWriter.java:84`, `MajorLeveledCompactionWriter.java:75`, `ShardedMultiWriter.java:204`.  The value lags by up to `slotCount` chunks of compressed bytes, so sstable-size switches happen up to one pool late.  Not torn on 64-bit HotSpot, though the JLS does not promise that for a plain `long`.

### m5. Constructor failure after `writer.start()` leaks the thread, the channel and the pool.

ACSW:150 starts the thread before ACSW:153 schedules the force.  If `scheduleAtFixedRate` throws (executor shut down), the constructor propagates, no poison pill is ever offered, and the thread stays parked on `filled.take()` holding `slotCount` buffers and the open `FileChannel` from the superclass.  Shutdown-time only.

## Cleared

Numbered to match the brief.

1. Memory visibility.  `uncompressedSize`, `compressedSize`, `chunkCount`, `chunkOffset`, `lastFlushOffset`, `crcMetadata`'s full checksum and `metadataWriter` are read by the producer only in `doPrepare` (CSW:492-495), `open` (ACSW:296), `resetAndTruncate` (ACSW:289) and `writeDigestFile` (CSW:493), all after `drain()`.  The writer's stores precede its volatile store of `completed` (ACSW:204); the producer's volatile load in `drain()` establishes the edge.  `bufferOffset` is producer-only on this class: the async `doFlush` (ACSW:246) writes it and the writer never does.  The two unsynchronised reads are C1 and m4.  `BtiTableWriter.java:103` reads `getLastFlushOffset()` on the producer, but only after a listener call that read `durableOffset` (volatile, ACSW:258), whose store follows the `lastFlushOffset` store (ACSW:191), so the value is at least the one the listener reported and any newer value reflects a completed `write`.
2. Lifecycle.  `stopWriter` takes `fsyncLock` before setting `shutdown` (ACSW:376-379) and `backgroundForce` checks `shutdown` under the same lock (ACSW:213), so no force starts after the channel closes, and one already running finishes first.  `cancel(false)` is right for that reason.  Second `doPreCleanup` on abort-after-prepare is idempotent: `shutdown` short-circuits, `buffer`, `compressed` and the trailer are null, the queues are empty.  Poison-pill after a dead writer: `filled.offer` succeeds (see 4), `join` returns at once on a dead thread, the pill is skipped at ACSW:422.  The 30 s tail is m1.
3. Drain.  `submitted` is producer-only, incremented before `put` (ACSW:252).  `put` cannot throw between the increment and the enqueue except on interrupt (m3), and that path throws to the caller, so the mismatch never reaches `drain`.  The writer's `finally` (ACSW:200-205) always increments `completed`, including when `flushData` throws, so a latched failure never stalls the count; `drain` sees the failure at ACSW:269.  The only way `completed` stops is m2.
4. Back-pressure.  Total buffers = 1 installed + `slotCount - 1` pooled = `slotCount`.  At `put` time the producer holds two of them, so `filled` holds at most `slotCount - 2` before and `slotCount - 1` after; capacity is `slotCount`.  `filled.put` never blocks and the poison pill always fits.  `free.offer` from the writer likewise never fails.  The only blocking wait is `takeFreeSlot`, which is bounded while the writer lives.
5. Post-flush listener.  The value handed to the listener is `lastFlushOffset` after a completed chunk write, the same quantity and the same durability level (channel, not fsync) as the superclass at SW:279 and CSW:279.  Monotonic except through `resetAndTruncate` (C1).  Firing before the just-submitted chunk lands only lowers the value, which `IndexSummaryBuilder.markDataSynced` (:164) and `PartitionIndexBuilder.markDataSynced` (:94) tolerate: both are floor lookups.  `runPostFlush` stays null; the only readers are SW:278 and CSW:278, both guarded.  The final offset is never reported after the last drain, but neither `openFinalEarly` nor `openFinal` consumes the listener, and `BtiTableWriter.openFinalEarly` (:135) explicitly completes the index to drop any pending early open.
6. Gathering write.  Compressed branch: `compressed` is flipped to `[0, compressedLength)`, `appendDirect` works on a duplicate, the gathering write drains both buffers.  Incompressible branch without padding: `src` is `[0, uncompressedLength)` after `flip`.  With padding: `limit = maxCompressedLength`, `writeZeroes` fills to it, `flip` gives `[0, maxCompressedLength)`, CRC covers the same range the old code covered.  `src.position(uncompressedLength)` at CSW:274 restores the count `bytesWritten` (ACSW:193) and the sync path's `current()` need; `slot.clear()` (ACSW:202) and `resetBuffer` (SW:362) restore the limit.  The loop at CSW:293 ends because a blocking `FileChannel.write(ByteBuffer[])` returns a positive count while bytes remain.  The trailer buffer is filled and written within one `writeChunk` call on one thread; nothing else reads it.  `DirectCompressedSequentialWriter` overrides both `createChecksumWriter` (:147) and `writeChunk` (:225), so `inlineTrailer == null` there is never dereferenced, and the `doPreCleanup` guard at CSW:512 handles it.
7. `flushData(ByteBuffer src)`.  Every former `buffer` use in the method body became `src`.  `this.buffer` is read on the writer thread nowhere: `seekToChunkStart`, `writeChunk` and the `InlineTrailerChecksumWriter` do not touch it.  The no-arg `flushData()` (CSW:209) is dead on the async class because `doFlush` is overridden.
8. `resetAndTruncate`.  The drain makes the writer idle before the six fields and `compressed` are touched, and the `super.resetAndTruncate` inner `syncInternal` goes through the async override, so the second drain is real.  `compressed` is otherwise touched only by the writer (`flushData`) and by `doPreCleanup` after `stopWriter`.  The reallocation is published to the writer through the next `filled.put`/`take`.  The bug in this area is the un-overridden `mark()`, C1.
9. Slot accounting.  Allocated: 1 (CSW:130 via `allocateBuffer`) + `slotCount - 1` (ACSW:145) = `slotCount`.  Freed on both commit and abort: `buffer` at SW:87, then the union of `free` and `filled` at ACSW:418-427.  With the writer stopped every non-installed slot is in one of the two queues, so the count is `slotCount` exactly.  The shortfall cases are m1 and m5.  `padToPageBoundary` (CSW:449), which mutates `lastFlushOffset` on the producer, is called only by `IncrementalTrieWriterPageAware` on the uncompressed index writers, never on the data file.
