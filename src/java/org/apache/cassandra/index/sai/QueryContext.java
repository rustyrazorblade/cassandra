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

package org.apache.cassandra.index.sai;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import com.sun.management.ThreadMXBean;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.plan.FilterTree;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_TEST_DISABLE_TIMEOUT;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 * <p>
 * Fields here are non-volatile, as they are accessed from a single thread.
 */
@NotThreadSafe
public class QueryContext
{
    private static final boolean DISABLE_TIMEOUT = SAI_TEST_DISABLE_TIMEOUT.getBoolean();

    // Thread-allocated-bytes tracking for TableQueryMetrics.PerQueryMetrics#allocatedBytes - a
    // baseline measurement point added deliberately BEFORE any SAI allocation-reduction changes,
    // so each subsequent commit's effect can be read off this same JMX histogram without needing
    // a profiler. Valid only because QueryContext is itself documented single-thread-per-query
    // (constructed and read back in #record on the same thread - see class doc); if that ever
    // stops holding, this would silently read a different thread's allocation counter.
    //
    // Note: threadAllocatedMemoryBean() below may call setThreadAllocatedMemoryEnabled(true), a
    // PROCESS-WIDE JMX setting, as a side effect of this class loading - harmless (HotSpot enables
    // it by default already, so this rarely actually flips anything) but worth knowing if this
    // static ever needs to be touched again.
    private static final ThreadMXBean THREAD_MX_BEAN = threadAllocatedMemoryBean();

    private static ThreadMXBean threadAllocatedMemoryBean()
    {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof ThreadMXBean))
            return null;
        ThreadMXBean sunBean = (ThreadMXBean) bean;
        if (!sunBean.isThreadAllocatedMemorySupported())
            return null;
        if (!sunBean.isThreadAllocatedMemoryEnabled())
            sunBean.setThreadAllocatedMemoryEnabled(true);
        return sunBean;
    }

    private final ReadCommand readCommand;
    private final long queryStartTimeNanos;
    private final long queryStartThreadId;
    private final long queryStartAllocatedBytes;

    public final long executionQuotaNano;

    public long sstablesHit = 0;
    public long segmentsHit = 0;
    public long partitionsRead = 0;
    public long rowsFiltered = 0;

    public long trieSegmentsHit = 0;
    public long triePostingsSkips = 0;
    public long triePostingsDecodes = 0;

    public long balancedTreePostingListsHit = 0;
    public long balancedTreeSegmentsHit = 0;
    public long balancedTreePostingsSkips = 0;
    public long balancedTreePostingsDecodes = 0;

    public boolean queryTimedOut = false;

    /**
     * {@code true} if the local query for this context has matches from Memtable-attached indexes or indexes on
     * unrepaired SSTables, and {@code false} otherwise. When this is {@code false}, {@link FilterTree} can ignore the
     * coordinator suggestion to downgrade to non-strict filtering, potentially reducing the number of false positives.
     *
     * @see QueryController#getIndexQueryResults(Collection)
     * */
    public boolean hasUnrepairedMatches = false;

    public QueryContext(ReadCommand readCommand, long executionQuotaMs)
    {
        this.readCommand = readCommand;
        executionQuotaNano = TimeUnit.MILLISECONDS.toNanos(executionQuotaMs);
        queryStartTimeNanos = Clock.Global.nanoTime();
        queryStartThreadId = Thread.currentThread().threadId();
        queryStartAllocatedBytes = THREAD_MX_BEAN != null ? THREAD_MX_BEAN.getCurrentThreadAllocatedBytes() : 0;
    }

    public long totalQueryTimeNs()
    {
        return Clock.Global.nanoTime() - queryStartTimeNanos;
    }

    /**
     * Bytes allocated between this {@link QueryContext} being constructed and this call, measured
     * via {@link ThreadMXBean} around the same single thread it's constructed and read back on (see
     * class doc). This is constructed early in {@code ReadCommand#executeLocally} (before the SAI
     * searcher's result iterator is even assembled) and read back from {@code TableQueryMetrics}
     * when that iterator is closed (after the caller has fully drained and post-filtered it) - so
     * this measures allocation across the WHOLE local read on the SAI path (iterator wrapping,
     * post-filtering, row-limit tracking, etc. included), not SAI-internal work exclusively. That's
     * still a valid yardstick for diffing one commit's SAI-internal change against the next, as long
     * as the non-SAI portion of that pipeline is unchanged between the two commits being compared -
     * just don't read an absolute value from this as "how much SAI itself allocated." Not recorded
     * at all for the vector/ANN path ({@code ScoreOrderedResultRetriever} doesn't call back into
     * {@code TableQueryMetrics#record}, matching that path's existing metrics gaps for
     * sstablesHit/segmentsHit/etc. - this histogram inherits the same blind spot, not a new one).
     * <p>
     * Returns 0 if this JVM's thread allocation tracking isn't available, or if this query's
     * execution ever crossed threads between construction and this call (the thread that's now
     * calling this may not be the one whose allocations were actually being tracked, and even if it
     * happens to be, work from something else on that thread in between would be counted too) -
     * either way, 0 rather than a misleading number. Also floored at 0 in case allocation tracking
     * was disabled by some external agent between construction and this call, which would otherwise
     * surface as {@link ThreadMXBean#getThreadAllocatedBytes} returning {@code -1}.
     */
    public long totalQueryAllocatedBytes()
    {
        if (THREAD_MX_BEAN == null || Thread.currentThread().threadId() != queryStartThreadId)
            return 0;
        return Math.max(0, THREAD_MX_BEAN.getThreadAllocatedBytes(queryStartThreadId) - queryStartAllocatedBytes);
    }

    public void checkpoint()
    {
        if (totalQueryTimeNs() >= executionQuotaNano && !DISABLE_TIMEOUT)
        {
            queryTimedOut = true;
            throw new QueryCancelledException(readCommand);
        }
    }

    public int limit()
    {
        return readCommand.limits().count();
    }

    /**
     * The query's canonical "now" (in seconds), frozen at read-command construction time - the same
     * value the rest of the read path already uses for liveness/TTL evaluation (see
     * {@link ReadCommand#nowInSec()}). Used by {@link org.apache.cassandra.index.sai.plan.FilterTree}
     * instead of a fresh {@link org.apache.cassandra.utils.FBUtilities#nowInSeconds()} clock read per
     * row, since this value already exists and second-granularity TTL checks don't benefit from
     * re-reading the clock on every row of a single query.
     */
    public long nowInSec()
    {
        return readCommand.nowInSec();
    }
}
