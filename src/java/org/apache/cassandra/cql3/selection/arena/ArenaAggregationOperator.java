/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.selection.arena;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Arena-based single-partition sort/dedup operator.
 * Leases scratch space from ArenaScratchPool, performs real projection via Selectors.
 */
public final class ArenaAggregationOperator implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(ArenaAggregationOperator.class);

    // Ranges at or below this width sort faster with insertion sort than with recursive partitioning.
    private static final int INSERTION_SORT_THRESHOLD = 16;

    private final ArenaScratchPool.Lease lease;
    private final ArenaOperatorPlan plan;
    private final ArenaRowBuffer buffer;
    private final ArenaRowComparator comparator;
    private final int maxRows;
    private final int columnCount;

    private ArenaAggregationOperator(ArenaOperatorPlan plan, int columnCount) throws ArenaCapacityException
    {
        this.plan = plan;
        this.columnCount = columnCount;
        this.maxRows = CassandraRelevantProperties.CASSANDRA_CQL_ARENA_AGGREGATION_MAX_ROWS.getInt();

        long maxQueryBytes = CassandraRelevantProperties.CASSANDRA_CQL_ARENA_AGGREGATION_MAX_QUERY_BYTES.getLong();

        ArenaScratchPool pool = ArenaScratchPool.getInstance();
        this.lease = pool.lease(maxQueryBytes);
        this.buffer = new ArenaRowBuffer(lease, columnCount, maxRows);
        this.comparator = new ArenaRowComparator(buffer,
                                                  plan.getKeyColumnIndices(),
                                                  plan.getKeyColumnTypes(),
                                                  plan.getKeyColumnReversed());
    }

    /**
     * Execute arena aggregation over a single partition.
     */
    public static ResultSet execute(RowIterator partition,
                                    QueryOptions options,
                                    Selection selection,
                                    ArenaOperatorPlan plan,
                                    int userLimit,
                                    ResultSet.ResultMetadata resultMetadata,
                                    TableMetadata table,
                                    long nowInSec) throws InvalidRequestException
    {
        int columnCount = selection.getResultMetadata().names.size();

        try (ArenaAggregationOperator operator = new ArenaAggregationOperator(plan, columnCount))
        {
            operator.bufferPartition(partition, selection, options, table, nowInSec);
            operator.sortRows();
            ResultSet result = operator.materialize(resultMetadata, userLimit);
            if (logger.isDebugEnabled())
                logger.debug("Arena aggregation path taken: {} rows buffered, {} bytes leased, key columns {}",
                             operator.buffer.getRowCount(), operator.lease.capacity(), plan.getKeyColumnIndices());
            return result;
        }
        catch (ArenaCapacityException e)
        {
            // A capacity breach is a user-facing limit, not a defect: surface it as an invalid request
            // naming the knob to raise.
            logger.warn("Arena capacity exceeded: {}", e.getMessage());
            throw new InvalidRequestException(e.getMessage());
        }
        catch (RuntimeException e)
        {
            // An internal defect (NPE, buffer fault) is not the user's fault: log it and let it surface
            // as an internal error rather than masking it as an invalid request.
            logger.error("Arena aggregation internal error", e);
            throw e;
        }
    }

    private void bufferPartition(RowIterator partition,
                                 Selection selection,
                                 QueryOptions options,
                                 TableMetadata table,
                                 long nowInSec) throws ArenaCapacityException
    {
        Selection.Selectors selectors = selection.newSelectors(options);
        selectors.prepare(options);

        ProtocolVersion protocolVersion = options.getProtocolVersion();
        byte[][] keyComponents = getPartitionKeyComponentsAsBytes(table, partition.partitionKey());

        Selector.InputRow inputRow = null;

        while (partition.hasNext())
        {
            Row row = partition.next();

            // Create or reuse InputRow
            if (inputRow == null)
            {
                inputRow = new Selector.InputRow(protocolVersion,
                                                 selection.getColumns(),
                                                 false, // unmask
                                                 selectors.collectWritetimes(),
                                                 selectors.collectTTLs());
            }

            // Populate InputRow with column values (mirrors processPartition logic)
            for (ColumnMetadata def : selection.getColumns())
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        inputRow.add(keyComponents[def.position()]);
                        break;
                    case CLUSTERING:
                        inputRow.add(row.clustering().arrayAt(def.position()));
                        break;
                    case REGULAR:
                        inputRow.add(row.getColumnData(def), nowInSec);
                        break;
                    case STATIC:
                        inputRow.add(partition.staticRow().getColumnData(def), nowInSec);
                        break;
                }
            }

            // Project through selectors
            selectors.addInputRow(inputRow);
            List<byte[]> projectedRow = selectors.getOutputRow();

            // Build null flags
            boolean[] nullFlags = new boolean[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                nullFlags[i] = (projectedRow.get(i) == null);
            }

            // Calculate group hash
            int groupHash = Arrays.hashCode(projectedRow.toArray());

            // Append to buffer
            buffer.appendRow(projectedRow, nullFlags, groupHash);

            // Reset for next row
            inputRow.reset(!selectors.hasProcessing());
            selectors.reset();
        }
    }

    private static byte[][] getPartitionKeyComponentsAsBytes(TableMetadata metadata, DecoratedKey dk)
    {
        ByteBuffer key = dk.getKey();
        if (metadata.partitionKeyColumns().size() != 1 && metadata.partitionKeyType instanceof CompositeType)
        {
            ByteBuffer[] components = ((CompositeType) metadata.partitionKeyType).split(key);
            byte[][] result = new byte[components.length][];
            for (int i = 0; i < components.length; i++)
                result[i] = ByteBufferUtil.getArrayUnsafeNullable(components[i]);
            return result;
        }
        return new byte[][]{ ByteBufferUtil.getArrayUnsafeNullable(key) };
    }

    /**
     * Sort the buffered rows in place by the plan's key columns, ascending.
     * Uses introsort: quicksort with a median-of-three pivot, falling back to heapsort once the
     * recursion depth passes {@code 2*floor(log2(n))}, and to insertion sort on small ranges.  This
     * gives O(n log n) worst case and bounded stack depth, so a pre-sorted or adversarial partition
     * cannot degrade to O(n^2) or overflow the stack.
     */
    private void sortRows()
    {
        if (!plan.needsSort())
            return;

        int count = buffer.getRowCount();
        if (count < 2)
            return;

        int depthLimit = 2 * (31 - Integer.numberOfLeadingZeros(count)); // 2 * floor(log2(count))
        introSort(0, count - 1, depthLimit);
    }

    private void introSort(int low, int high, int depthLimit)
    {
        while (high - low >= INSERTION_SORT_THRESHOLD)
        {
            if (depthLimit == 0)
            {
                heapSort(low, high);
                return;
            }
            depthLimit--;

            int pivotIndex = partition(low, high);

            // Recurse into the smaller side and loop on the larger side.  This keeps the recursion
            // depth at O(log n) regardless of pivot quality.
            if (pivotIndex - low < high - pivotIndex)
            {
                introSort(low, pivotIndex - 1, depthLimit);
                low = pivotIndex + 1;
            }
            else
            {
                introSort(pivotIndex + 1, high, depthLimit);
                high = pivotIndex - 1;
            }
        }
        insertionSort(low, high);
    }

    private void insertionSort(int low, int high)
    {
        for (int i = low + 1; i <= high; i++)
        {
            for (int j = i; j > low && comparator.compare(j, j - 1) < 0; j--)
            {
                buffer.swapSlots(j, j - 1);
            }
        }
    }

    /**
     * Lomuto partition around a median-of-three pivot.
     * The median of the low, middle, and high slots is chosen as the pivot, which avoids the
     * O(n^2) behaviour a fixed pivot shows on already-sorted input.
     */
    private int partition(int low, int high)
    {
        int mid = low + ((high - low) >>> 1);

        // Order low <= mid <= high, so the median lands at mid.
        if (comparator.compare(mid, low) < 0)
            buffer.swapSlots(mid, low);
        if (comparator.compare(high, low) < 0)
            buffer.swapSlots(high, low);
        if (comparator.compare(high, mid) < 0)
            buffer.swapSlots(high, mid);

        // Move the median to the high position to use as the Lomuto pivot.
        buffer.swapSlots(mid, high);
        int pivot = high;
        int i = low - 1;

        for (int j = low; j < high; j++)
        {
            if (comparator.compare(j, pivot) <= 0)
            {
                i++;
                buffer.swapSlots(i, j);
            }
        }

        buffer.swapSlots(i + 1, high);
        return i + 1;
    }

    private void heapSort(int low, int high)
    {
        int n = high - low + 1;

        // Build a max-heap over [low, high].
        for (int start = (n >>> 1) - 1; start >= 0; start--)
            siftDown(low, start, n);

        // Repeatedly move the max to the end and shrink the heap.
        for (int end = n - 1; end > 0; end--)
        {
            buffer.swapSlots(low, low + end);
            siftDown(low, 0, end);
        }
    }

    private void siftDown(int low, int start, int size)
    {
        int root = start;
        while (true)
        {
            int child = 2 * root + 1;
            if (child >= size)
                break;
            if (child + 1 < size && comparator.compare(low + child, low + child + 1) < 0)
                child++;
            if (comparator.compare(low + root, low + child) < 0)
            {
                buffer.swapSlots(low + root, low + child);
                root = child;
            }
            else
            {
                break;
            }
        }
    }

    private ResultSet materialize(ResultSet.ResultMetadata resultMetadata, int userLimit)
    {
        int count = buffer.getRowCount();
        int limit = (userLimit > 0 && userLimit < count) ? userLimit : count;

        List<List<byte[]>> rows = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++)
        {
            rows.add(buffer.extractRow(i));
        }

        return new ResultSet(resultMetadata, rows);
    }

    @Override
    public void close()
    {
        if (lease != null)
        {
            try
            {
                lease.close();
            }
            catch (Exception e)
            {
                logger.warn("Failed to release arena lease", e);
            }
        }
    }
}
