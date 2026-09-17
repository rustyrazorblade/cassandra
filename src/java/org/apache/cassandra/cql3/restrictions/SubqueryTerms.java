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

package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.FunctionContext;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.transport.Dispatcher;

/**
 * A {@link Terms} implementation whose values are the result of an uncorrelated IN-subquery
 * ({@code pk IN (SELECT one_col FROM t2 [WHERE ...])}).  Research POC, gated by
 * {@code CQL_SUBQUERY_ENABLED}.
 *
 * <p>The instance is part of a cached, shared prepared statement, so it holds ONLY immutable
 * data: a global slot id, the prepared inner statement, and the inner table name.  The resolved
 * value list for a single request rides on the per-request {@link QueryOptions} (see
 * {@link QueryOptions#withSubqueryResults}) keyed by the slot id.  {@link #bindAndGet} reads it
 * back from {@code context.options()}.  A field here would be a data race across requests.</p>
 *
 * <p>The inner statement is resolved once, at the coordinator, before any outer {@code ReadQuery}
 * is built; see {@link #resolve}.  Nothing about a subquery ever reaches a serialized command.</p>
 */
public final class SubqueryTerms implements Terms
{
    private static final AtomicLong SLOT_IDS = new AtomicLong();

    private final long slotId;
    private final SelectStatement inner;

    public SubqueryTerms(SelectStatement inner)
    {
        this.slotId = SLOT_IDS.getAndIncrement();
        this.inner = inner;
    }

    public long slotId()
    {
        return slotId;
    }

    public SelectStatement innerStatement()
    {
        return inner;
    }

    /**
     * Executes the inner statement once, fully materialized and unpaged, at the coordinator, using
     * the outer consistency level.  Caps the read at {@code failThreshold + 1} keys and then applies
     * {@link Guardrails#partitionKeysInSelect} so an oversized inner fails loudly.  Dedupes the keys
     * (order preserved), skips null values, and isolates inner client warnings from the outer client.
     *
     * @return the deduped list of partition-key values selected by the inner query.
     */
    public List<ByteBuffer> resolve(ClientState clientState,
                                    ConsistencyLevel consistency,
                                    long nowInSec,
                                    Dispatcher.RequestTime requestTime,
                                    boolean internal)
    {
        int failThreshold = DatabaseDescriptor.getGuardrailsConfig().getPartitionKeysInSelectFailThreshold();
        // failThreshold <= 0 means the guardrail is disabled; do not impose an artificial cap.
        int cap = failThreshold > 0 ? failThreshold + 1 : DataLimits.NO_LIMIT;

        QueryOptions innerOptions = QueryOptions.forInternalCalls(consistency, Collections.emptyList());
        Selection.Selectors selectors = inner.getSelection().newSelectors(innerOptions);
        ColumnFilter columnFilter = selectors.getColumnFilter();

        // Read at most 'cap' rows, unpaged.  The inner is never driver-paged with the client page size.
        ReadQuery query = inner.getQuery(innerOptions,
                                                                 clientState,
                                                                 columnFilter,
                                                                 nowInSec,
                                                                 cap,
                                                                 DataLimits.NO_LIMIT,
                                                                 cap,
                                                                 null,
                                                                 PotentialTxnConflicts.DISALLOW);

        LinkedHashSet<ByteBuffer> keys = new LinkedHashSet<>();

        // Isolate the inner read's client warnings so they do not leak to the outer client, then
        // restore the outer warning state.  This only bypasses the CQL client-request metrics; the
        // lower-level coordinator and table read metrics still count the inner read.
        ClientWarn.State savedWarnState = ClientWarn.instance.get();
        ClientWarn.instance.captureWarnings();
        try
        {
            if (internal)
            {
                // Internal path: read locally, matching the outer executeInternal() context.
                try (ReadExecutionController controller = query.executionController();
                     PartitionIterator data = query.executeInternal(controller))
                {
                    collectKeys(data, nowInSec, clientState, keys);
                }
            }
            else
            {
                // Distributed path: a proper coordinator read at the outer consistency level.
                try (PartitionIterator data = query.execute(consistency, clientState, requestTime))
                {
                    collectKeys(data, nowInSec, clientState, keys);
                }
            }
        }
        finally
        {
            ClientWarn.instance.set(savedWarnState);
        }

        // H1: cap the inner result loudly, naming the threshold.  Do not rely only on the read timeout.
        Guardrails.partitionKeysInSelect.guard(keys.size(), inner.table(), false, clientState);

        return new ArrayList<>(keys);
    }

    private void collectKeys(PartitionIterator data, long nowInSec, ClientState clientState, LinkedHashSet<ByteBuffer> keys)
    {
        ResultSet rs = inner.process(data, nowInSec, true, clientState);
        for (List<byte[]> row : rs.rows)
        {
            // ResultSet holds already-serialized column values as byte[]; wrap them for the IN list.
            // SEMANTICS: skip null inner values so they do not become spurious IN entries.
            byte[] value = row.get(0);
            if (value != null)
                keys.add(ByteBuffer.wrap(value));
        }
    }

    @Override
    public List<ByteBuffer> bindAndGet(FunctionContext context)
    {
        return context.options().getSubqueryResult(slotId);
    }

    @Override
    public Terminals bind(FunctionContext context)
    {
        List<ByteBuffer> buffers = bindAndGet(context);
        List<Term.Terminal> terminals = new ArrayList<>(buffers.size());
        for (ByteBuffer buffer : buffers)
            terminals.add(new Constants.Value(buffer));
        return Terminals.of(terminals);
    }

    @Override
    public List<List<ByteBuffer>> bindAndGetElements(FunctionContext context)
    {
        List<ByteBuffer> buffers = bindAndGet(context);
        List<List<ByteBuffer>> elements = new ArrayList<>(buffers.size());
        for (ByteBuffer buffer : buffers)
            elements.add(Collections.singletonList(buffer));
        return elements;
    }

    @Override
    public boolean isSingleTerm(FunctionContext context)
    {
        // The count is unknown until the inner resolves; treat it as a multi-valued IN list.
        return false;
    }

    @Override
    public boolean containsSingleTerm()
    {
        // Unknown at prepare time; returning false prevents an IN -> EQ collapse in Relation.
        return false;
    }

    @Override
    public void collectMarkerSpecification(VariableSpecifications boundNames, Object owner)
    {
        // Uncorrelated: the inner carries no outer bind markers.
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        // The inner statement's functions are authorized and validated through the inner statement
        // itself, not through the outer restriction, so nothing is added here.
    }

    @Override
    public String toString()
    {
        return "(" + inner + ")";
    }
}
