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

package org.apache.cassandra.arrow;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.sstable.CursorMergeConsumer;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Assembles rows for one table into Arrow {@link VectorSchemaRoot} batches.
 * <p>
 * This is the single "assembly stage" shared by both scan producers (see {@code ARROW-FLIGHT.md}
 * / task #11): the cursor-merge path drives it through {@link CursorMergeConsumer} (this class
 * implements that interface directly, decoding the merge's raw on-disk cell bytes itself); the
 * iterator fallback path drives it through the plain, already-decoded methods below directly
 * ({@link #startPartition(ByteBuffer)}, {@link #startRow}, {@link #putSimpleCell}, etc.), since an
 * {@link org.apache.cassandra.db.rows.Cell} already carries a decoded {@link ByteBuffer} value with
 * no on-disk framing to strip. Both producers therefore always go through the same null/static/
 * batching logic below, so output is identical regardless of which one ran.
 * <p>
 * Absence is the only representation of "this column has no value for this row": a column simply
 * never receives a {@code putSimpleCell}/{@code putComplexCell} call (and its vector position is
 * never written), so it defaults to null. Tombstoned and TTL-expired cells are treated exactly like
 * absent cells by both producers (each checks liveness - {@code cell.isLive(nowInSec)} for the
 * iterator path, {@code cellLiveness.isLive(nowInSec)} for the cursor path - before ever calling
 * into this class) - this class never sees a "this cell used to have value X" signal to
 * accidentally carry forward.
 * <p>
 * Static columns are cached once per partition (as already-decoded Java values) and replayed onto
 * every subsequent row of that partition, matching Trino's relational (denormalized) expectation
 * that a static column reads the same on every row of its partition.
 * <p>
 * <b>How filter/aggregation compose with static replication and dead-row discard:</b> a filter
 * (see {@link FilterExpression}) or {@code GROUP BY}/aggregate (see {@link RowAggregator}) is
 * evaluated in {@link #endRow()}, which is reached ONLY for a row {@link #endRow(UnfilteredDescriptor,
 * boolean)}/the iterator path already determined has live data - so a tombstone/expired-liveness
 * row (handled by {@link #discardDeadRow()} directly, bypassing {@link #endRow()} entirely) is never
 * filtered or aggregated; it was never a real row to begin with. By the time {@link #endRow()} runs,
 * static-column replication has already happened (a partition's static row - if any - is always
 * read before any of its real rows), so a filter/{@code GROUP BY} referencing a static column sees
 * the same replicated value real CQL would. A row that fails the filter, or that the aggregator has
 * consumed, is rolled back exactly like a dead row (see {@link #discardDeadRow()}) - {@code rowIndex}
 * is never advanced, so it is invisible in any output batch and the next row silently overwrites its
 * slot.
 */
public class ArrowRowAssembler implements CursorMergeConsumer, AutoCloseable, RowValues
{
    private final TableMetadata table;
    private final BufferAllocator allocator;
    private final long targetBatchBytes;
    private final Consumer<VectorSchemaRoot> onBatch;
    private final long nowInSec;
    private final FilterExpression filter;
    private final RowAggregator aggregator;
    // Whether anything will ever read the RowValues view (see #get) this row - i.e. whether a
    // filter or aggregator is active. currentRowValues (unlike staticValues/partitionKeyValues,
    // which are also needed for static/partition-key replication onto every row regardless) exists
    // ONLY to serve that view - see the field's own javadoc. Skipping its upkeep entirely for a
    // plain, unfiltered/unaggregated scan (the common case for a join-side full-table scan feeding
    // Trino, which pushes no predicate at all onto the table it isn't filtering) removes one
    // HashMap.Node allocation per regular/clustering column per row for free - real allocation
    // measured via CassandraFlightProducer's ThreadMXBean instrumentation, not a hypothetical
    // optimization (see ARROW-FLIGHT.md's garbage-free-reads investigation).
    private final boolean needsRowValues;

    private final Schema schema;
    private final List<ColumnMetadata> partitionKeyColumns;
    private final List<ColumnMetadata> clusteringColumns;
    private final AbstractType<?> partitionKeyType;
    private final boolean hasStaticColumns;

    private VectorSchemaRoot root;
    private Map<ColumnMetadata, FieldVector> vectors;
    private int rowIndex;

    // current-partition static-column replication cache: decoded value (possibly null) per static column
    private final Map<ColumnMetadata, Object> staticValues = new HashMap<>();
    // current-partition key replication cache: a partition key is written once per partition on
    // disk, not once per row, so (like static columns) it must be replayed onto every row
    private final Map<ColumnMetadata, Object> partitionKeyValues = new HashMap<>();
    // current-ROW (clustering + regular) decoded value cache, cleared at the start of every real
    // row - see startRow(boolean, ClusteringPrefix)/emitStaticOnlyRow. Backs the RowValues view
    // (see #get) that FilterExpression/RowAggregator evaluate against; kept separate from
    // staticValues/partitionKeyValues (which are partition-scoped, not row-scoped).
    private final Map<ColumnMetadata, Object> currentRowValues = new HashMap<>();
    // number of real (non-static) output rows emitted for the current partition so far; used to
    // detect a static-only partition (see endPartition/emitStaticOnlyRow)
    private int rowsInCurrentPartition;

    // CursorMergeConsumer adapter state (see the writeCellHeader/writeCellPath/writeCellValue trio)
    private boolean currentRowIsStatic;
    // Whether the CURRENT (non-static) row's own primary-key liveness info is live, and whether
    // at least one live cell has been dispatched for it so far - together these mirror
    // Row#hasLiveData(nowInSec, enforceStrictLiveness) (see endRow(UnfilteredDescriptor, boolean)).
    // CursorCompactor's own merge opens (calls startRow/lateStartRow for) a row whenever there is
    // EITHER live data OR a deletion/expired-liveness marker worth preserving for compaction
    // output correctness - a strictly larger set than "has live data"; a real CQL SELECT would
    // never return a row that has neither.
    private boolean pendingRowPkLive;
    private boolean pendingRowSawLiveCell;
    private ColumnMetadata pendingColumn;
    private boolean pendingColumnLive;
    private boolean pendingColumnHasValue;
    private ByteBuffer pendingPath;
    private final DataOutputBuffer valueCapture = new DataOutputBuffer(128);

    // in-progress complex (multi-cell) column state
    private ColumnMetadata openComplexColumn;
    private int openComplexCellCount;

    public ArrowRowAssembler(TableMetadata table, BufferAllocator allocator, long targetBatchBytes, Consumer<VectorSchemaRoot> onBatch)
    {
        this(table, allocator, targetBatchBytes, onBatch, null, null);
    }

    /**
     * @param filter      post-merge filter to apply to every fully-assembled row, or {@code null} for
     *                    no filtering - see the class javadoc and {@link FilterExpression}.
     * @param aggregation server-side {@code GROUP BY}/aggregate spec, or {@code null} for plain
     *                    (unaggregated) row output - see {@link RowAggregator}. When non-null, no raw
     *                    row is ever written to an output batch; {@link #close()} emits exactly one
     *                    aggregated batch instead (or none, if {@code aggregation} has a non-empty
     *                    {@code groupBy} and no row ever matched - see {@link RowAggregator}).
     */
    public ArrowRowAssembler(TableMetadata table, BufferAllocator allocator, long targetBatchBytes, Consumer<VectorSchemaRoot> onBatch,
                              FilterExpression filter, CompiledAggregation aggregation)
    {
        this.table = table;
        this.allocator = allocator;
        this.targetBatchBytes = targetBatchBytes;
        this.onBatch = onBatch;
        this.nowInSec = FBUtilities.nowInSeconds();
        this.filter = filter;
        this.aggregator = aggregation == null ? null : new RowAggregator(aggregation);
        this.needsRowValues = filter != null || this.aggregator != null;
        this.schema = CassandraArrowTypeMapping.toArrowSchema(table);
        this.partitionKeyColumns = table.partitionKeyColumns();
        this.clusteringColumns = table.clusteringColumns();
        this.partitionKeyType = table.partitionKeyType;
        this.hasStaticColumns = !table.regularAndStaticColumns().statics.isEmpty();
        newBatch();
    }

    /**
     * {@link RowValues} view over the row currently being finished (see {@link #endRow()}), used by
     * {@link #filter}/{@link #aggregator}: partition-key and static values come from their
     * partition-scoped caches (already fully populated for any real row - static rows are always
     * read before any real row of the same partition), everything else from {@link #currentRowValues}.
     */
    @Override
    public Object get(ColumnMetadata column)
    {
        if (column.isPartitionKey())
            return partitionKeyValues.get(column);
        if (column.isStatic())
            return staticValues.get(column);
        return currentRowValues.get(column);
    }

    public long nowInSec()
    {
        return nowInSec;
    }

    /**
     * Allocates a fresh {@link VectorSchemaRoot} (and its per-column vector lookup) for the next
     * batch. A new root - rather than clearing/reusing one root's buffers in place - is used for
     * every batch deliberately: {@code onBatch} (e.g. the Flight producer's {@code getStream} ->
     * {@code ServerStreamListener.putNext()}) may serialize/send this root's buffers
     * asynchronously, so this class must not mutate or free them after handing a root off.
     * {@code onBatch} is responsible for releasing each root it is given once it is truly done
     * with it (e.g. after the corresponding {@code putNext()} call, per Arrow Flight's own
     * buffer-ownership contract).
     */
    private void newBatch()
    {
        root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        vectors = new HashMap<>();
        List<ColumnMetadata> allColumns = new ArrayList<>(partitionKeyColumns.size() + clusteringColumns.size()
                                                           + table.regularAndStaticColumns().size());
        allColumns.addAll(partitionKeyColumns);
        allColumns.addAll(clusteringColumns);
        table.regularAndStaticColumns().statics.forEach(allColumns::add);
        table.regularAndStaticColumns().regulars.forEach(allColumns::add);
        for (ColumnMetadata column : allColumns)
            vectors.put(column, root.getVector(column.name.toString()));
        rowIndex = 0;
    }

    private void maybeFlush()
    {
        // Accepted PoC limitation: this measures the WHOLE shared allocator's allocated memory,
        // not just this batch's own bytes, so batch sizing is skewed by any other concurrent
        // activity against the same allocator (e.g. other in-flight scans). Getting this batch's
        // own byte count cheaply would need summing every vector's buffer size on every row; not
        // worth the per-row cost for a PoC where batch size is only a soft target.
        if (allocator.getAllocatedMemory() >= targetBatchBytes)
            flush();
    }

    /**
     * Emits whatever has been assembled so far (even a partial/undersized batch). Idempotent when
     * empty.
     * <p>
     * Transitions to a fresh batch state (via {@link #newBatch()}) BEFORE invoking {@code onBatch},
     * not after: if {@code onBatch} throws (e.g. a cancelled Flight stream), this assembler must not
     * be left pointing at the same already-handed-off batch, or a subsequent {@code flush()} (e.g.
     * from {@link #close()}) would re-invoke {@code onBatch} with the same stale root - causing
     * either a duplicate delivery or a permanent leak of that batch's buffers.
     */
    public void flush()
    {
        if (rowIndex == 0)
            return;
        VectorSchemaRoot completed = root;
        completed.setRowCount(rowIndex);
        newBatch();
        onBatch.accept(completed);
    }

    @Override
    public void close()
    {
        try
        {
            // When aggregation is active, no row is ever committed (see #endRow), so this is
            // always a no-op (rowIndex stays 0) - the aggregated batch below is the only output.
            flush();
            if (aggregator != null)
                aggregator.emit(allocator, onBatch);
        }
        finally
        {
            root.close();
            valueCapture.close();
        }
    }

    // ================= plain, already-decoded API (shared by both scan producers) =================

    public void startPartition(ByteBuffer partitionKey)
    {
        staticValues.clear();
        partitionKeyValues.clear();
        rowsInCurrentPartition = 0;
        decomposeKeyComponents(partitionKeyColumns, partitionKeyType, partitionKey, partitionKeyValues);
    }

    /**
     * A partition consisting only of a static row (no clustering/regular rows) still returns exactly
     * one row from CQL's own perspective (partition-key + static values populated, everything else
     * null) - see e.g. {@code SELECT * FROM t WHERE pk = ?} after only {@code UPDATE t SET s = ...}
     * was ever applied to that partition, with no clustering value ever written. {@link #endRow()} is
     * otherwise only reached for real (non-static) rows, so such a partition would otherwise
     * contribute zero output rows; synthesize the one CQL would produce here, before the per-partition
     * caches this depends on (partition key / static values) are cleared.
     */
    public void endPartition()
    {
        if (rowsInCurrentPartition == 0 && !staticValues.isEmpty())
            emitStaticOnlyRow();
        staticValues.clear();
        partitionKeyValues.clear();
    }

    private void emitStaticOnlyRow()
    {
        currentRowValues.clear();
        for (Map.Entry<ColumnMetadata, Object> entry : partitionKeyValues.entrySet())
            writeDecomposedValue(vectors.get(entry.getKey()), unwrap(entry.getKey().type), rowIndex, entry.getValue());
        for (Map.Entry<ColumnMetadata, Object> entry : staticValues.entrySet())
        {
            if (entry.getValue() != null)
                writeDecomposedValue(vectors.get(entry.getKey()), unwrap(entry.getKey().type), rowIndex, entry.getValue());
        }
        endRow();
    }

    public void startRow(boolean isStatic)
    {
        // no-op placeholder for symmetry/clarity at call sites; row index is established by endRow()
    }

    /**
     * Overload for real (non-static) rows: also writes clustering column values. Generic over the
     * clustering's value representation ({@code byte[]} from the cursor path's
     * {@link UnfilteredDescriptor#toClusteringPrefix}, {@code ByteBuffer} from a real {@link Row}
     * on the iterator path) since both producers share this method.
     */
    public <V> void startRow(boolean isStatic, ClusteringPrefix<V> clustering)
    {
        if (!isStatic)
        {
            if (needsRowValues)
                currentRowValues.clear();

            // Partition key values are decoded once per partition (startPartition) and, like
            // static columns, replayed onto every row of it - a partition key does not have its
            // own per-row storage on disk (it is written once per partition, not once per row).
            for (Map.Entry<ColumnMetadata, Object> entry : partitionKeyValues.entrySet())
                writeDecomposedValue(vectors.get(entry.getKey()), unwrap(entry.getKey().type), rowIndex, entry.getValue());

            for (int i = 0; i < clusteringColumns.size(); i++)
            {
                ColumnMetadata column = clusteringColumns.get(i);
                V value = i < clustering.size() ? clustering.get(i) : null;
                if (value != null)
                {
                    ByteBuffer buffer = clustering.accessor().toBuffer(value);
                    Object decomposed = decompose(unwrap(column.type), buffer);
                    if (needsRowValues)
                        currentRowValues.put(column, decomposed);
                    writeDecomposedValue(vectors.get(column), unwrap(column.type), rowIndex, decomposed);
                }
            }
        }
        // static-column replication: every row (static or regular) gets the partition's cached static values
        for (Map.Entry<ColumnMetadata, Object> entry : staticValues.entrySet())
        {
            if (entry.getValue() != null)
                writeDecomposedValue(vectors.get(entry.getKey()), unwrap(entry.getKey().type), rowIndex, entry.getValue());
        }
    }

    /**
     * Finishes the row currently being assembled: evaluates {@link #filter} (if any) and, depending
     * on the outcome and whether {@link #aggregator} is active, either commits the row to the output
     * batch, feeds it to the aggregator, or discards it - see the class javadoc's "Where filter/
     * aggregation compose" note and {@link #discardDeadRow()}. Called for every row CursorCompactor
     * (or the iterator path) determined has live data - see
     * {@link #endRow(UnfilteredDescriptor, boolean)} - so a tombstone/expired-liveness-only row
     * never reaches here (and is correctly never filtered/aggregated - it was never a real row).
     */
    public void endRow()
    {
        // A real (non-static) row existed for this partition regardless of what
        // filter/aggregation do with it below - increment unconditionally so endPartition's
        // static-only-row synthesis (emitStaticOnlyRow) correctly sees "at least one real row was
        // seen" and does NOT fire just because every real row happened to be filtered out or fed
        // to the aggregator instead of committed (a static-only-row synthesis storm, one phantom
        // null-valued row per partition, was exactly the bug this comment now prevents - caught by
        // RowAggregationTest#sumMinMaxAvgGroupedByStaticColumn).
        rowsInCurrentPartition++;

        boolean passesFilter = filter == null || filter.evaluate(this);
        if (aggregator != null)
        {
            if (passesFilter)
                aggregator.accumulate(this);
            discardDeadRow();
            return;
        }
        if (!passesFilter)
        {
            discardDeadRow();
            return;
        }
        commitRow();
    }

    private void commitRow()
    {
        rowIndex++;
        maybeFlush();
    }

    /** {@code value == null} leaves the column null for this row (tombstoned/expired/absent). */
    public void putSimpleCell(ColumnMetadata column, ByteBuffer value)
    {
        if (value == null)
            return;
        AbstractType<?> type = unwrap(column.type);
        Object decomposed = decompose(type, value);
        if (column.isStatic())
            staticValues.put(column, decomposed);
        else
        {
            if (needsRowValues)
                currentRowValues.put(column, decomposed);
            writeDecomposedValue(vectors.get(column), type, rowIndex, decomposed);
        }
    }

    /** Counter columns: {@code total} is the pre-composed {@link CounterContext#total} value. */
    public void putCounterCell(ColumnMetadata column, long total)
    {
        if (column.isStatic())
        {
            staticValues.put(column, total);
        }
        else
        {
            if (needsRowValues)
                currentRowValues.put(column, total);
            ((BigIntVector) vectors.get(column)).setSafe(rowIndex, total);
        }
    }

    public void beginComplexColumn(ColumnMetadata column)
    {
        // Close a still-open DIFFERENT complex column before opening this one. This is the only
        // place two adjacent complex columns in the same row can be told apart on the
        // CursorMergeConsumer path: CursorCompactor always calls startComplexColumn (-> here)
        // before writeCellHeader runs for the new column's first cell, so by the time
        // writeCellHeader's own (complex -> plain column) transition check runs, openComplexColumn
        // has already been overwritten below - that check can never fire for a
        // complex-column-to-complex-column transition (see ARROW-FLIGHT bug tracker task #2).
        if (openComplexColumn != null && !openComplexColumn.equals(column))
            endComplexColumn(openComplexColumn);

        openComplexColumn = column;
        openComplexCellCount = 0;
        AbstractType<?> type = unwrap(column.type);
        if (!column.isStatic())
        {
            FieldVector vector = vectors.get(column);
            if (type instanceof ListType || type instanceof SetType)
                ((ListVector) vector).startNewValue(rowIndex);
            else if (type instanceof MapType)
                ((MapVector) vector).startNewValue(rowIndex);
            // UserType (multi-cell UDT): struct children are written directly by field index; no start needed.
        }
    }

    /** One cell of a multi-cell (list/set/map/non-frozen-UDT) column. */
    public void putComplexCell(ColumnMetadata column, ByteBuffer path, ByteBuffer value)
    {
        AbstractType<?> type = unwrap(column.type);
        openComplexCellCount++;
        // Accumulate a plain decoded Java value alongside the vector write below (regular columns)
        // or INSTEAD of one (static columns, which are cached, not written immediately) - both are
        // needed for the RowValues view (see #get) that FilterExpression/RowAggregator read, so a
        // filter/GROUP BY can reference any complex column, static or regular. A static column's
        // accumulation into staticValues is NOT skippable even when !needsRowValues - unlike
        // currentRowValues, staticValues also backs partition-wide replication (see startRow/
        // emitStaticOnlyRow), so it is this cell's only chance to be captured for later writing.
        if (column.isStatic())
            accumulateComplexCell(staticValues, column, type, path, value);
        else if (needsRowValues)
            accumulateComplexCell(currentRowValues, column, type, path, value);
        if (column.isStatic())
            return;
        FieldVector vector = vectors.get(column);
        if (type instanceof ListType)
        {
            AbstractType<?> elementType = ((ListType<?>) type).getElementsType();
            appendToDataVector((ListVector) vector, elementType, decompose(elementType, value));
        }
        else if (type instanceof SetType)
        {
            AbstractType<?> elementType = ((SetType<?>) type).getElementsType();
            appendToDataVector((ListVector) vector, elementType, decompose(elementType, path));
        }
        else if (type instanceof MapType)
        {
            MapType<?, ?> mapType = (MapType<?, ?>) type;
            appendToMapEntry((MapVector) vector, mapType, decompose(mapType.getKeysType(), path), decompose(mapType.getValuesType(), value));
        }
        else if (type instanceof UserType)
        {
            UserType userType = (UserType) type;
            int fieldIndex = ByteBufferUtil.toShort(path);
            AbstractType<?> fieldType = userType.fieldType(fieldIndex);
            StructVector structVector = (StructVector) vector;
            structVector.setIndexDefined(rowIndex);
            FieldVector fieldVector = (FieldVector) structVector.getChildrenFromFields().get(fieldIndex);
            writeDecomposedValue(fieldVector, unwrap(fieldType), rowIndex, decompose(unwrap(fieldType), value));
        }
        else
        {
            throw new IllegalArgumentException("Unsupported multi-cell column type: " + type);
        }
    }

    /**
     * Accumulates one complex-column cell's decoded value into {@code target} ({@link #staticValues}
     * for a static column, {@link #currentRowValues} for a regular one - see {@link #putComplexCell}),
     * independent of (and in addition to, for regular columns) the row-indexed Arrow vector write.
     */
    @SuppressWarnings("unchecked")
    private void accumulateComplexCell(Map<ColumnMetadata, Object> target, ColumnMetadata column, AbstractType<?> type, ByteBuffer path, ByteBuffer value)
    {
        if (type instanceof ListType)
        {
            AbstractType<?> elementType = ((ListType<?>) type).getElementsType();
            ((List<Object>) target.computeIfAbsent(column, c -> new ArrayList<>())).add(decompose(elementType, value));
        }
        else if (type instanceof SetType)
        {
            AbstractType<?> elementType = ((SetType<?>) type).getElementsType();
            ((List<Object>) target.computeIfAbsent(column, c -> new ArrayList<>())).add(decompose(elementType, path));
        }
        else if (type instanceof MapType)
        {
            MapType<?, ?> mapType = (MapType<?, ?>) type;
            ((Map<Object, Object>) target.computeIfAbsent(column, c -> new LinkedHashMap<>()))
                .put(decompose(mapType.getKeysType(), path), decompose(mapType.getValuesType(), value));
        }
        else if (type instanceof UserType)
        {
            UserType userType = (UserType) type;
            Object[] fields = (Object[]) target.computeIfAbsent(column, c -> new Object[userType.size()]);
            int fieldIndex = ByteBufferUtil.toShort(path);
            fields[fieldIndex] = decompose(unwrap(userType.fieldType(fieldIndex)), value);
        }
    }

    public void endComplexColumn(ColumnMetadata column)
    {
        AbstractType<?> type = unwrap(column.type);
        if (!column.isStatic())
        {
            FieldVector vector = vectors.get(column);
            if (openComplexCellCount == 0)
            {
                vector.setNull(rowIndex);
            }
            else if (type instanceof ListType || type instanceof SetType)
            {
                ((ListVector) vector).endValue(rowIndex, openComplexCellCount);
            }
            else if (type instanceof MapType)
            {
                ((MapVector) vector).endValue(rowIndex, openComplexCellCount);
            }
            if (openComplexCellCount == 0)
                currentRowValues.remove(column);
        }
        else if (openComplexCellCount == 0)
        {
            staticValues.remove(column);
        }
        openComplexColumn = null;
        openComplexCellCount = 0;
    }

    // ================= key decoding =================

    private static void decomposeKeyComponents(List<ColumnMetadata> columns, AbstractType<?> keyType, ByteBuffer key,
                                                Map<ColumnMetadata, Object> destination)
    {
        if (columns.size() == 1)
        {
            ColumnMetadata column = columns.get(0);
            destination.put(column, decompose(unwrap(column.type), key));
            return;
        }
        // Accepted PoC limitation: a shorter-than-expected `components` array (a malformed/
        // corrupt partition key) is silently truncated (later columns simply never get a
        // destination entry, reading as null) rather than raising an error - a real corruption
        // would be a pre-existing, unrelated on-disk integrity problem, not something this PoC
        // needs to detect specially.
        ByteBuffer[] components = ((CompositeType) keyType).split(key);
        for (int i = 0; i < columns.size() && i < components.length; i++)
        {
            ColumnMetadata column = columns.get(i);
            destination.put(column, decompose(unwrap(column.type), components[i]));
        }
    }

    private static AbstractType<?> unwrap(AbstractType<?> type)
    {
        return type.isReversed() ? ((ReversedType<?>) type).baseType : type;
    }

    // ================= value decoding: raw on-disk bytes -> natural Java representation =================

    /**
     * Package-visible (not {@code private}) so {@link FilterCompiler} can normalize a filter literal
     * into EXACTLY this same representation - see that class's javadoc.
     */
    static Object decompose(AbstractType<?> type, ByteBuffer value)
    {
        if (type instanceof IntegerType)
            return CassandraArrowTypeMapping.toArrowDecimal((BigInteger) type.compose(value));
        if (type instanceof DecimalType)
            return CassandraArrowTypeMapping.toArrowDecimal((BigDecimal) type.compose(value));
        if (type instanceof SimpleDateType)
            return ((Integer) type.compose(value)) + Integer.MIN_VALUE;
        if (type instanceof org.apache.cassandra.db.marshal.TimestampType)
            return ((java.util.Date) type.compose(value)).getTime();
        if (type instanceof org.apache.cassandra.db.marshal.TimeType)
            return (Long) type.compose(value);
        if (type instanceof org.apache.cassandra.db.marshal.UUIDType || type instanceof org.apache.cassandra.db.marshal.TimeUUIDType)
            return ByteBufferUtil.getArray(value); // on-disk bytes already are the 16-byte UUID representation
        if (type instanceof org.apache.cassandra.db.marshal.InetAddressType)
            return ByteBufferUtil.getArray(value); // on-disk bytes already are the raw 4/16-byte address
        if (type instanceof org.apache.cassandra.db.marshal.BytesType)
            return ByteBufferUtil.getArray(value);
        if (type instanceof org.apache.cassandra.db.marshal.AsciiType || type instanceof org.apache.cassandra.db.marshal.UTF8Type)
            return ByteBufferUtil.getArray(value); // utf8/ascii bytes on disk are already the string's bytes
        if (type instanceof DurationType)
            return type.compose(value); // org.apache.cassandra.cql3.Duration
        if (type instanceof TupleType) // also covers frozen UserType, which extends TupleType
            return decomposeTupleOrUdt((TupleType) type, value);
        if (type instanceof ListType)
            return decomposeElements(unwrap(((ListType<?>) type).getElementsType()), unpackCollection(type, value));
        if (type instanceof SetType)
            return decomposeElements(unwrap(((SetType<?>) type).getElementsType()), unpackCollection(type, value));
        if (type instanceof MapType)
            return decomposeMapEntries((MapType<?, ?>) type, unpackCollection(type, value));
        if (type instanceof VectorType)
            return decomposeVectorElements((VectorType<?>) type, value);
        // remaining scalars: boolean/byte/short/int/long/float/double
        return type.compose(value);
    }

    /**
     * Decomposes a frozen (single-cell) tuple or UDT value into its field values, using this
     * class's own normalized {@link #decompose} for each field (not the raw {@code AbstractType}
     * composition, which would use a different, non-Arrow-normalized representation for e.g.
     * dates/decimals/UUIDs). Nested (non-scalar) fields are rejected - see {@link #requireScalar}.
     */
    private static Object[] decomposeTupleOrUdt(TupleType type, ByteBuffer value)
    {
        List<ByteBuffer> fields = type.unpack(value, ByteBufferAccessor.instance);
        Object[] result = new Object[type.size()];
        for (int i = 0; i < fields.size(); i++)
        {
            ByteBuffer field = fields.get(i);
            if (field != null)
            {
                AbstractType<?> fieldType = unwrap(type.type(i));
                result[i] = decompose(fieldType, field);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<ByteBuffer> unpackCollection(AbstractType<?> type, ByteBuffer value)
    {
        return ((CollectionSerializer<?>) type.getSerializer()).unpack(value);
    }

    private static List<Object> decomposeElements(AbstractType<?> elementType, List<ByteBuffer> raw)
    {
        requireScalar(elementType, "collection element");
        List<Object> result = new ArrayList<>(raw.size());
        for (ByteBuffer element : raw)
            result.add(decompose(elementType, element));
        return result;
    }

    private static Map<Object, Object> decomposeMapEntries(MapType<?, ?> type, List<ByteBuffer> flatKeysAndValues)
    {
        AbstractType<?> keyType = unwrap(type.getKeysType());
        AbstractType<?> valueType = unwrap(type.getValuesType());
        requireScalar(keyType, "map key");
        requireScalar(valueType, "map value");
        Map<Object, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < flatKeysAndValues.size(); i += 2)
            result.put(decompose(keyType, flatKeysAndValues.get(i)), decompose(valueType, flatKeysAndValues.get(i + 1)));
        return result;
    }

    private static List<Object> decomposeVectorElements(VectorType<?> type, ByteBuffer value)
    {
        AbstractType<?> elementType = unwrap(type.elementType);
        requireScalar(elementType, "vector element");
        if (!elementType.isValueLengthFixed())
            throw new UnsupportedOperationException("Arrow Flight PoC only supports fixed-width vector element types; got " + elementType.asCQL3Type());
        int elementSize = elementType.valueLengthIfFixed();
        List<Object> result = new ArrayList<>(type.dimension);
        int base = value.position();
        for (int i = 0; i < type.dimension; i++)
        {
            ByteBuffer element = value.duplicate();
            element.position(base + i * elementSize).limit(base + (i + 1) * elementSize);
            result.add(decompose(elementType, element));
        }
        return result;
    }

    /**
     * PoC limitation: a collection/vector/tuple/UDT whose element/field type is itself a
     * collection, tuple, UDT, or vector ("nested" complex types, e.g. {@code list<frozen<list<int>>>})
     * is rejected rather than silently mishandled - see {@code ARROW-FLIGHT.md} for the type
     * mapping's documented scope. Every type this mapping does support is a plain scalar at that
     * point, so this check is deliberately narrow (allow-list, not a deny-list of complex types).
     */
    private static void requireScalar(AbstractType<?> type, String what)
    {
        if (type instanceof CollectionType || type instanceof TupleType || type instanceof VectorType)
            throw new UnsupportedOperationException("Arrow Flight PoC does not support nested complex types (" + what +
                                                      " is " + type.asCQL3Type() + "); only scalar collection/vector elements and tuple/UDT fields are supported");
    }

    // ================= value writing: natural Java representation -> Arrow vector =================

    /**
     * Package-visible and {@code static} (not {@code private}/instance) - this method touches no
     * instance state (every input is a parameter; recursion is self-contained), so
     * {@link RowAggregator} reuses it as-is to write {@code GROUP BY}/{@code MIN}/{@code MAX} output
     * values into its own, separately-schemad final batch, rather than duplicating this ~150-line
     * type dispatch.
     */
    @SuppressWarnings("unchecked")
    static void writeDecomposedValue(FieldVector vector, AbstractType<?> type, int index, Object value)
    {
        if (value == null)
        {
            vector.setNull(index);
            return;
        }
        if (type instanceof CounterColumnType)
        {
            ((BigIntVector) vector).setSafe(index, (Long) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.BooleanType)
        {
            ((BitVector) vector).setSafe(index, ((Boolean) value) ? 1 : 0);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.ByteType)
        {
            ((TinyIntVector) vector).setSafe(index, (Byte) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.ShortType)
        {
            ((SmallIntVector) vector).setSafe(index, (Short) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.Int32Type)
        {
            ((IntVector) vector).setSafe(index, (Integer) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.LongType)
        {
            ((BigIntVector) vector).setSafe(index, (Long) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.FloatType)
        {
            ((Float4Vector) vector).setSafe(index, (Float) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.DoubleType)
        {
            ((Float8Vector) vector).setSafe(index, (Double) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.AsciiType || type instanceof org.apache.cassandra.db.marshal.UTF8Type)
        {
            ((VarCharVector) vector).setSafe(index, (byte[]) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.BytesType || type instanceof org.apache.cassandra.db.marshal.InetAddressType)
        {
            ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.TimestampType)
        {
            ((TimeStampMilliVector) vector).setSafe(index, (Long) value);
        }
        else if (type instanceof SimpleDateType)
        {
            ((DateDayVector) vector).setSafe(index, (Integer) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.TimeType)
        {
            ((TimeNanoVector) vector).setSafe(index, (Long) value);
        }
        else if (type instanceof org.apache.cassandra.db.marshal.UUIDType || type instanceof org.apache.cassandra.db.marshal.TimeUUIDType)
        {
            ((FixedSizeBinaryVector) vector).setSafe(index, (byte[]) value);
        }
        else if (type instanceof DurationType)
        {
            org.apache.cassandra.cql3.Duration duration = (org.apache.cassandra.cql3.Duration) value;
            ((IntervalMonthDayNanoVector) vector).setSafe(index, duration.getMonths(), duration.getDays(), duration.getNanoseconds());
        }
        else if (type instanceof IntegerType || type instanceof DecimalType)
        {
            ((Decimal256Vector) vector).setSafe(index, (BigDecimal) value);
        }
        else if (type instanceof VectorType)
        {
            VectorType<?> vectorType = (VectorType<?>) type;
            List<?> elements = (List<?>) value;
            FixedSizeListVector listVector = (FixedSizeListVector) vector;
            int start = listVector.startNewValue(index);
            FieldVector dataVector = (FieldVector) listVector.getDataVector();
            for (int i = 0; i < elements.size(); i++)
                writeDecomposedValue(dataVector, unwrap(vectorType.elementType), start + i, elements.get(i));
        }
        else if (type instanceof ListType)
        {
            AbstractType<?> elementType = unwrap(((ListType<?>) type).getElementsType());
            ListVector listVector = (ListVector) vector;
            int start = listVector.startNewValue(index);
            List<?> elements = (List<?>) value;
            FieldVector dataVector = (FieldVector) listVector.getDataVector();
            for (int i = 0; i < elements.size(); i++)
                writeDecomposedValue(dataVector, elementType, start + i, elements.get(i));
            listVector.endValue(index, elements.size());
        }
        else if (type instanceof SetType)
        {
            AbstractType<?> elementType = unwrap(((SetType<?>) type).getElementsType());
            ListVector listVector = (ListVector) vector;
            int start = listVector.startNewValue(index);
            Set<?> elements = (Set<?>) value;
            FieldVector dataVector = (FieldVector) listVector.getDataVector();
            int i = 0;
            for (Object element : elements)
                writeDecomposedValue(dataVector, elementType, start + (i++), element);
            listVector.endValue(index, elements.size());
        }
        else if (type instanceof MapType)
        {
            MapType<?, ?> mapType = (MapType<?, ?>) type;
            MapVector mapVector = (MapVector) vector;
            int start = mapVector.startNewValue(index);
            Map<?, ?> map = (Map<?, ?>) value;
            StructVector entries = (StructVector) mapVector.getDataVector();
            FieldVector keyVector = (FieldVector) entries.getChildrenFromFields().get(0);
            FieldVector valueVector = (FieldVector) entries.getChildrenFromFields().get(1);
            int i = 0;
            for (Map.Entry<?, ?> e : map.entrySet())
            {
                entries.setIndexDefined(start + i);
                writeDecomposedValue(keyVector, unwrap(mapType.getKeysType()), start + i, e.getKey());
                writeDecomposedValue(valueVector, unwrap(mapType.getValuesType()), start + i, e.getValue());
                i++;
            }
            mapVector.endValue(index, map.size());
        }
        else if (type instanceof UserType || type instanceof TupleType)
        {
            TupleType tupleType = (TupleType) type;
            StructVector structVector = (StructVector) vector;
            structVector.setIndexDefined(index);
            Object[] fields = (Object[]) value;
            for (int i = 0; i < tupleType.size() && i < fields.length; i++)
            {
                if (fields[i] != null)
                {
                    AbstractType<?> fieldType = unwrap(tupleType.type(i));
                    FieldVector fieldVector = (FieldVector) structVector.getChildrenFromFields().get(i);
                    writeDecomposedValue(fieldVector, fieldType, index, fields[i]);
                }
            }
        }
        else
        {
            throw new IllegalArgumentException("No Arrow vector writer for Cassandra type: " + type);
        }
    }

    private void appendToDataVector(ListVector listVector, AbstractType<?> elementType, Object value)
    {
        FieldVector dataVector = (FieldVector) listVector.getDataVector();
        int position = dataVector.getValueCount();
        writeDecomposedValue(dataVector, unwrap(elementType), position, value);
        dataVector.setValueCount(position + 1);
    }

    private void appendToMapEntry(MapVector mapVector, MapType<?, ?> mapType, Object key, Object value)
    {
        StructVector entries = (StructVector) mapVector.getDataVector();
        int position = entries.getValueCount();
        entries.setIndexDefined(position);
        FieldVector keyVector = (FieldVector) entries.getChildrenFromFields().get(0);
        FieldVector valueVector = (FieldVector) entries.getChildrenFromFields().get(1);
        writeDecomposedValue(keyVector, unwrap(mapType.getKeysType()), position, key);
        writeDecomposedValue(valueVector, unwrap(mapType.getValuesType()), position, value);
        entries.setValueCount(position + 1);
    }

    // ================= CursorMergeConsumer adapter: decodes raw on-disk cell bytes, then defers to the plain API =================

    @Override
    public void startPartition(byte[] partitionKey, int keyLength, DeletionTime partitionDeletion) throws IOException
    {
        startPartition(ByteBuffer.wrap(partitionKey, 0, keyLength));
    }

    @Override
    public void endPartition(DecoratedKey key, byte[] partitionKey, int keyLength, DeletionTime partitionDeletion) throws IOException
    {
        endPartition();
    }

    @Override
    public boolean writeEmptyStaticRow() throws IOException
    {
        return hasStaticColumns;
    }

    @Override
    public void startRow(UnfilteredDescriptor clustering, LivenessInfo liveness, DeletionTime deletion, boolean isStatic) throws IOException
    {
        currentRowIsStatic = isStatic;
        if (isStatic)
        {
            startRow(true);
        }
        else
        {
            pendingRowPkLive = liveness.isLive(nowInSec);
            pendingRowSawLiveCell = false;
            startRow(false, clustering.toClusteringPrefix(table.comparator.subtypes()));
        }
    }

    @Override
    public void endRow(UnfilteredDescriptor row, boolean isFirstUnfiltered) throws IOException
    {
        // CursorMergeConsumer has no explicit "end of complex column" callback (see
        // startComplexColumn/writeCellHeader below): a complex column open at end-of-row is the
        // row's last column, so it closes here.
        if (openComplexColumn != null)
            endComplexColumn(openComplexColumn);

        // The static row is not itself an output row - see startPartition/putSimpleCell javadoc:
        // its values are cached into staticValues and replayed onto every real row instead.
        if (!currentRowIsStatic)
        {
            // CursorCompactor opens (calls startRow for) a row whenever there is EITHER live data
            // to write OR a deletion/expired-liveness marker worth preserving for compaction
            // output correctness (tombstone propagation, anti-entropy) - a strictly larger set
            // than "has live data". Mirror Row#hasLiveData(nowInSec, enforceStrictLiveness) here
            // (pendingRowPkLive is the PK-liveness branch; pendingRowSawLiveCell, set by
            // dispatchPendingCell/the counter path, is the "at least one live cell" fallback) so a
            // row that is only a tombstone/expired marker - which a real CQL SELECT would never
            // return - isn't counted as an output row.
            if (pendingRowPkLive || (pendingRowSawLiveCell && !table.enforceStrictLiveness()))
                endRow();
            else
                discardDeadRow();
        }
    }

    /**
     * Undoes any partial vector writes made for a row CursorCompactor opened (to preserve a
     * tombstone/expired-liveness marker for compaction correctness) but which turned out to have
     * no live data - see {@link #endRow(UnfilteredDescriptor, boolean)}. {@code rowIndex} is left
     * untouched (not incremented) so the next real row simply overwrites this slot; only REGULAR
     * columns need explicit resetting here, since partition-key/static columns are unconditionally
     * rewritten at this same index by every subsequent row's {@code startRow} anyway.
     */
    private void discardDeadRow()
    {
        for (Map.Entry<ColumnMetadata, FieldVector> entry : vectors.entrySet())
        {
            if (entry.getKey().isRegular())
                entry.getValue().setNull(rowIndex);
        }
    }

    @Override
    public void startComplexColumn(ColumnMetadata column, DeletionTime mergedDeletion) throws IOException
    {
        beginComplexColumn(column);
    }

    @Override
    public void writeCellHeader(int cellFlags, ReusableLivenessInfo cellLiveness, ColumnMetadata column) throws IOException
    {
        // CursorMergeConsumer signals the START of a complex column via startComplexColumn (which
        // itself now closes a still-open DIFFERENT complex column - see beginComplexColumn), but
        // never its end explicitly for a COMPLEX -> PLAIN transition: a still-open complex column
        // closes the moment a cell for a different, non-complex column arrives (cells of one
        // complex column are always contiguous on the wire - see
        // SSTableCursorReader.CellCursor#readCellHeader). column.isComplex() is checked (rather
        // than always closing here) so this doesn't race with beginComplexColumn's own closing of
        // the SAME transition for complex -> complex.
        if (!column.isComplex() && openComplexColumn != null && !openComplexColumn.equals(column))
            endComplexColumn(openComplexColumn);

        pendingColumn = column;
        pendingColumnLive = cellLiveness.isLive(nowInSec);
        pendingColumnHasValue = Cell.Serializer.hasValue(cellFlags);
        pendingPath = null;

        // Cassandra's wire format cannot distinguish "no value" from "a genuinely empty value" -
        // both serialize with HAS_EMPTY_VALUE_MASK set and no value bytes at all (see
        // Cell.Serializer#serialize / #hasValue: hasValue = valueSize > 0), so CursorCompactor
        // never calls writeCellValue for either case (Cell.Serializer.hasValue(cellFlags) gates
        // it). For a genuinely live cell with an empty value (e.g. `v text` set to '', or any Set
        // element - Cassandra always writes a Set cell's value empty, see cql3/terms/Sets.java),
        // this cell must still be dispatched with an empty value, not silently treated as absent.
        // A PLAIN (non-complex) cell has no writeCellPath call coming (see mergeCells: writeCellPath
        // is only invoked `if (column.isComplex())`), so it must dispatch right here; a complex
        // cell's dispatch (which needs the path too, e.g. a Set's element or a Map's key) happens
        // in writeCellPath below instead.
        if (pendingColumnLive && !pendingColumnHasValue && !column.isComplex())
            dispatchPendingCell(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Override
    public void writeCellPath(byte[] pathBuffer, int pathLength) throws IOException
    {
        pendingPath = ByteBuffer.wrap(Arrays.copyOf(pathBuffer, pathLength));

        // See writeCellHeader: a complex cell with no value (Set elements always; a live List/Map/
        // UDT cell with a genuinely empty value) never gets a writeCellValue call, so it must be
        // dispatched here instead, now that its path is known.
        if (pendingColumnLive && !pendingColumnHasValue)
            dispatchPendingCell(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Override
    public int writeCellValue(SSTableCursorReader source, byte[] copyBuffer) throws IOException
    {
        valueCapture.clear();
        int written = source.copyCellValue(valueCapture, copyBuffer);
        if (pendingColumnLive)
            dispatchPendingCell(rawCellValue(unwrap(pendingColumn.type)));
        return written;
    }

    @Override
    public void writeCellValue(DataOutputBuffer staged) throws IOException
    {
        if (pendingColumnLive)
        {
            AbstractType<?> type = unwrap(pendingColumn.type);
            ByteBuffer raw = staged.buffer(true);
            dispatchPendingCell(stripLengthPrefixIfNeeded(type, raw));
        }
    }

    @Override
    public void writeCellValue(byte[] value, int offset, int length) throws IOException
    {
        // The only writeCellValue overload used for counters (see CursorCompactor): `value` is the
        // already fully-merged counter context (shards), not a plain scalar - fold it to a total.
        if (pendingColumnLive)
        {
            byte[] copy = Arrays.copyOfRange(value, offset, offset + length);
            long total = CounterContext.instance().total(ByteBuffer.wrap(copy), ByteBufferAccessor.instance);
            if (pendingColumn.isStatic())
                staticValues.put(pendingColumn, total);
            else if (openComplexColumn == null)
            {
                pendingRowSawLiveCell = true;
                putCounterCell(pendingColumn, total);
            }
        }
    }

    private ByteBuffer rawCellValue(AbstractType<?> type)
    {
        ByteBuffer raw = valueCapture.buffer(true);
        return stripLengthPrefixIfNeeded(type, raw);
    }

    private static ByteBuffer stripLengthPrefixIfNeeded(AbstractType<?> type, ByteBuffer raw)
    {
        if (type.isValueLengthFixed())
            return raw;
        ByteBuffer dup = raw.duplicate();
        int length = VIntCoding.readUnsignedVInt32(dup);
        dup.limit(dup.position() + length);
        return dup;
    }

    private void dispatchPendingCell(ByteBuffer value)
    {
        if (!pendingColumn.isStatic())
            pendingRowSawLiveCell = true;
        if (openComplexColumn != null)
            putComplexCell(pendingColumn, pendingPath, value);
        else
            putSimpleCell(pendingColumn, value);
    }

    @Override
    public void writeRangeTombstone(UnfilteredDescriptor marker, boolean isFirstUnfiltered) throws IOException
    {
        // Row/range-tombstone shadowing is fully resolved upstream by the merge before these
        // events reach the consumer (see CursorMergeConsumer javadoc); no action needed here.
    }
}
