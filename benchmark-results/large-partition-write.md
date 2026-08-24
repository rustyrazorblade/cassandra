<!--
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->

# Large partition write overhead

Cost of compacting a single large partition through `BigFormatPartitionWriter`, measured by
`test/memory/org/apache/cassandra/io/sstable/format/big/LargePartitionWriteOverheadTest.java`.

Configuration:

- sstable format: `big`
- `column_index_size`: 1 KiB
- row width: 4 x 256 bytes
- input sstables per run: 2, compacted into 1
- allocation measured with `ThreadMXBean.getThreadAllocatedBytes` on the compacting thread
- a full GC is requested immediately before each measured window, so the GC columns count only
  collections the operation itself provoked; at the default scale the burst still fits in young gen

`indexOffsets` is presized from the partition's known or estimated size into a pooled off-heap
buffer, reused across partitions within a writer and doubled rather than grown by a constant when
an estimate undershoots, so the allocated total below is compaction proper rather than offset
copying.

| scenario | rows | index blocks | partition MiB | allocated MiB | bytes/block | wall clock ms | GC count | GC ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| singleLargePartition | 32768 | 32768 | 32.8 | 85.2 | 2725 | 209 | 0 | 0 |
