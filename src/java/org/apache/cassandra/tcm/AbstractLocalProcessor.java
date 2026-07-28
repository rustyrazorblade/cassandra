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

package org.apache.cassandra.tcm;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.exceptions.ExceptionCode.SERVER_ERROR;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AbstractLocalProcessor implements Processor
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosBackedProcessor.class);

    protected final LocalLog log;

    /**
     * Serialises the read-execute-propose cycle for commits originating on this node, i.e.
     * {@link LocalLog#waitForHighestConsecutive}, execution of the transformation, the {@link #tryCommitOne} proposal
     * and, when that proposal wins its epoch, the subsequent {@link LocalLog#append}.
     *
     * Invariants:
     * <ul>
     *     <li>acquisition waits at most the caller's remaining {@link Retry} deadline, so being queued consumes only
     *         the waiter's own budget and can never push it past its own deadline;</li>
     *     <li>every successful acquisition is matched by exactly one unlock;</li>
     *     <li>it is held across one whole cycle, including the blocking and remote work that cycle needs, and is
     *         released before everything which is not part of deriving and proposing an epoch: local enactment via
     *         {@link LocalLog#awaitAtLeast}, the uninterruptible retry backoff, and any log fetch performed after a
     *         rejection or a lost epoch;</li>
     *     <li>admission is fair, hence FIFO, and does not prioritise by {@link Transformation.Kind}: a topology
     *         change can queue behind schema DDL which arrived first.</li>
     * </ul>
     *
     * This does not, and cannot, address races between separate CMS members; those remain the distributed log's job.
     */
    private final ReentrantLock proposalLock = new ReentrantLock(true);

    /**
     * What a single read-execute-propose cycle concluded, so that the cycle can hand off to the code which acts on it
     * after {@link #proposalLock} has been released.
     */
    private enum Outcome
    {
        /** The proposal won its epoch and has been appended to the local log. */
        APPLIED,
        /** Execution against the latest local metadata was rejected; nothing was proposed. */
        REJECTED,
        /** The proposal did not win its epoch, or its outcome is unknown. */
        NOT_APPLIED,
        /** The proposal threw; the throwable is reported and inspected once the lock has been released. */
        FAILED
    }

    public AbstractLocalProcessor(LocalLog log)
    {
        this.log = log;
    }


    /**
     * Epoch returned by processor in the Result is _not_ guaranteed to be visible by the Follower by
     * the time when this method returns.
     */
    @Override
    public final Commit.Result commit(Entry.Id entryId, Transformation transform, final Epoch lastKnown, Retry retryPolicy)
    {
        String transformStr = transform.toString(); // convert once as idempotent and used in multiple logs
        logger.debug("Starting local commit of {} with policy {}", transformStr, retryPolicy);
        long commitStart = nanoTime();
        // History, not per-iteration state: set immediately before the first tryCommitOne call and never cleared, so
        // that the failure message below can only claim nothing was proposed while it is still false.
        boolean proposalAttempted = false;
        while (!retryPolicy.hasExpired())
        {
            // Serialise the read-execute-propose cycle for locally originating commits. Acquisition consumes this
            // caller's own deadline, so a request which cannot be admitted in time gives up here rather than proposing.
            if (!acquireProposalLock(retryPolicy))
                return notAdmitted(transformStr, commitStart, retryPolicy, proposalAttempted);

            ClusterMetadata previous;
            Transformation.Result result;
            Outcome outcome;
            Throwable failure = null;
            // The one critical section: read the highest consecutive metadata, execute against it, propose the derived
            // epoch and, if the proposal won, append it locally. Nothing else belongs here, and the lock is dropped in
            // the single finally below before any of the outcomes are acted on.
            try
            {
                previous = log.waitForHighestConsecutive();
                if (!acceptCommit(previous))
                {
                    String msg = String.format("Node %s is not a CMS member in epoch %s; members=%s",
                                               FBUtilities.getBroadcastAddressAndPort(),
                                               previous.epoch.getEpoch(),
                                               previous.fullCMSMembers());
                    logger.warn(msg);
                    throw new NotCMSException(msg);
                }

                if (!transform.eligibleToCommit(previous))
                {
                    result = new Transformation.Rejected(INVALID, "Transformation rejected, can't commit " + transformStr +
                                                                  " it not supported with cluster common serialization version " + previous.directory.commonSerializationVersion +
                                                                  " and min/max serialization versions " + previous.directory.clusterMinVersion + "/" + previous.directory.clusterMaxVersion);
                }
                else
                {
                    result = executeStrictly(previous, transform);
                }

                if (result.isRejected())
                {
                    // Nothing to propose; the catch-up which decides whether this rejection is final happens after the
                    // lock is released, as it may involve remote peers.
                    outcome = Outcome.REJECTED;
                }
                else
                {
                    try
                    {
                        Epoch nextEpoch = result.success().metadata.epoch;
                        // If metadata applies, try committing it to the log
                        long casStart = nanoTime();
                        proposalAttempted = true;
                        boolean applied = tryCommitOne(entryId, transform, previous.epoch, nextEpoch);
                        long casElapsedUs = NANOSECONDS.toMicros(nanoTime() - casStart);
                        logger.debug("tryCommitOne for {} epoch {}->{}: applied={}, took {}us",
                                     transform.kind(), previous.epoch, nextEpoch, applied, casElapsedUs);

                        // Application here semantially means "succeeded in committing to the distributed log".
                        if (applied)
                        {
                            logger.info("Committed {}. New epoch is {}. Took {} attempts in {}us total.",
                                        transformStr, nextEpoch, retryPolicy.attempts(),
                                        NANOSECONDS.toMicros(nanoTime() - commitStart));
                            log.append(new Entry(entryId, nextEpoch, new Transformation.Executed(transform, result)));
                            outcome = Outcome.APPLIED;
                        }
                        else
                        {
                            outcome = Outcome.NOT_APPLIED;
                        }
                    }
                    catch (Throwable e)
                    {
                        // Reported and inspected outside the lock, so that neither is done while holding it.
                        failure = e;
                        outcome = Outcome.FAILED;
                    }
                }
            }
            finally
            {
                proposalLock.unlock();
            }

            // Everything from here on runs without the lock: it is either remote, uninterruptible, or waits on the
            // local log being caught up, and none of it is part of deriving and proposing an epoch.
            if (outcome == Outcome.REJECTED)
            {
                // If we got a rejection, it could be that _we_ are not aware of the highest epoch.
                // Just try to catch up to the latest distributed state.
                // Use a dedicated retry policy here as the one for the commit itself may not be appropriate.
                // It uses the wrong metric and for STARTUP transformations will retry indefinitely, which is not
                // what we want here.
                Retry fetchLogRetry = Retry.until(retryPolicy.deadlineNanos, TCMMetrics.instance.fetchLogRetries);
                ClusterMetadata replayed = fetchLogAndWait(null, fetchLogRetry);

                // Retry if replay has changed the epoch, return rejection otherwise.
                if (!replayed.epoch.isAfter(previous.epoch))
                {
                    logger.info("No epoch change after fetched latest log entries, returning rejection response");
                    return maybeFailure(entryId,
                                        lastKnown,
                                        () -> Commit.Result.rejected(result.rejected().code, result.rejected().reason, toLogState(lastKnown)));
                }

                logger.info("Fetched latest log entries after transformation rejection, re-entering " +
                            "commit retry loop with {}ms remaining until deadline",
                            NANOSECONDS.toMillis(retryPolicy.remainingNanos()));
                continue;
            }

            if (outcome == Outcome.FAILED)
            {
                logger.error("Caught error while trying to perform a local commit", failure);
                JVMStabilityInspector.inspectThrowable(failure);
                if (!retryPolicy.maybeSleep())
                    break;
                continue;
            }

            try
            {
                if (outcome == Outcome.APPLIED)
                {
                    // The proposal is durable in the distributed log and appended locally, so the next proposer can
                    // already derive its epoch from it. Holding the lock across this wait would serialise the local
                    // enactment latency of this commit onto unrelated ones.
                    Epoch nextEpoch = result.success().metadata.epoch;
                    log.awaitAtLeast(nextEpoch);

                    return new Commit.Result.Success(nextEpoch,
                                                     toLogState(result.success(), entryId, lastKnown, transform));
                }

                // Lost the epoch, or the outcome is unknown. Back off, then refetch: the sleep is uninterruptible and
                // the fetch may be remote, neither of which should block another local proposer.
                if (!retryPolicy.maybeSleep())
                    break;

                logger.info("Backed off after failure to commit to log, fetching latest log entries before retry");
                // TODO: could also add epoch from mis-application from [applied].
                // Use a dedicated retry policy here as the one for the commit itself may not be appropriate.
                // It uses the wrong metric and for STARTUP transformations will retry indefinitely, which is not
                // what we want here.
                Retry fetchLogRetry = Retry.until(retryPolicy.deadlineNanos, TCMMetrics.instance.fetchLogRetries);
                fetchLogAndWait(null, fetchLogRetry);
                logger.info("Fetched latest log entries, re-entering commit retry loop with {}ms remaining until deadline",
                            NANOSECONDS.toMillis(retryPolicy.remainingNanos()));
            }
            catch (Throwable e)
            {
                logger.error("Caught error while trying to perform a local commit", e);
                JVMStabilityInspector.inspectThrowable(e);
                if (!retryPolicy.maybeSleep())
                    break;
            }
        }
        String failureMsg = String.format("Could not perform commit after %d attempts. Time remaining: %dms",
                                          retryPolicy.attempts(), NANOSECONDS.toMillis(retryPolicy.remainingNanos()));
        return failed(transformStr, commitStart, failureMsg);
    }

    /**
     * Builds the failure for a commit which could not be admitted to {@link #proposalLock} within its deadline. Either
     * shutdown or a cancelled request, or simply too many local commits queued ahead of this one.
     *
     * @param proposalAttempted whether any earlier iteration already reached {@link #tryCommitOne}. If it did, its
     *        outcome may be genuinely unknown, as losing the epoch and an ambiguous Paxos result are indistinguishable
     *        to us, so the message states only what happened and promises nothing about whether the transformation
     *        applied. If it did not, retrying is unambiguously safe, and reporting an attempt count would imply CAS
     *        activity which did not happen.
     */
    private Commit.Result notAdmitted(String transformStr, long commitStart, Retry retryPolicy, boolean proposalAttempted)
    {
        String cause = Thread.currentThread().isInterrupted()
                       ? "Interrupted while waiting to submit commit"
                       : String.format("Timed out after %dms waiting to submit commit",
                                       NANOSECONDS.toMillis(nanoTime() - commitStart));
        long remainingMillis = NANOSECONDS.toMillis(retryPolicy.remainingNanos());
        String failureMsg = proposalAttempted
                            ? String.format("%s. A proposal was made during one of the preceding %d attempts, so " +
                                            "whether the transformation was committed to the log is unknown. " +
                                            "Time remaining: %dms",
                                            cause, retryPolicy.attempts(), remainingMillis)
                            : String.format("%s. No proposal was made. Time remaining: %dms", cause, remainingMillis);
        return failed(transformStr, commitStart, failureMsg);
    }

    private Commit.Result failed(String transformStr, long commitStart, String failureMsg)
    {
        logger.debug("Commit {} failed in {}us total. {}",
                     transformStr, NANOSECONDS.toMicros(nanoTime() - commitStart), failureMsg);
        return Commit.Result.failed(SERVER_ERROR, failureMsg);
    }

    /**
     * Acquires {@link #proposalLock}, waiting no longer than the caller's remaining deadline. Every successful
     * acquisition is matched by exactly one unlock in the {@code finally} which closes the critical section, so
     * hold-count accounting is safe on a thread which is already mid-cycle: a nested acquisition only raises the
     * count, and releasing it cannot free the lock out from under the outer cycle. That is an accounting invariant
     * only; it does not assert that a nested commit is semantically safe, as a synchronous pre-commit listener could
     * recursively encounter the same pending entry. No current production listener does this.
     *
     * @return true if the lock is held by the calling thread on return, false if the deadline expired first or the
     *         thread was interrupted while waiting, in which case this attempt proposed nothing.
     */
    private boolean acquireProposalLock(Retry retryPolicy)
    {
        long remainingNanos = retryPolicy.remainingNanos();
        if (remainingNanos <= 0)
            return false;

        try
        {
            return proposalLock.tryLock(remainingNanos, NANOSECONDS);
        }
        catch (InterruptedException e)
        {
            // Preserve the interrupt for the caller (e.g. so shutdown is not swallowed) and give up on this attempt.
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public Commit.Result maybeFailure(Entry.Id entryId, Epoch lastKnown, Supplier<Commit.Result.Failure> orElse)
    {
        LogState logState = toLogState(lastKnown);
        Epoch commitedAt = null;
        for (Entry entry : logState.entries)
        {
            if (entry.id.equals(entryId))
                commitedAt = entry.epoch;
        }

        // Succeeded after retry
        if (commitedAt != null)
            return new Commit.Result.Success(commitedAt, logState);
        else
            return orElse.get();
    }

    /**
     * Calls {@link Transformation#execute(ClusterMetadata)}, but catches any
     * {@link Transformation.RejectedTransformationException}, which is used by implementations to indicate that a known
     * and ultimately fatal error has been encountered. Throwing such an error implies that the transformation has not
     * succeeded and will not succeed if executed again, so no retries should be attempted. These scenarios are rare and
     * using an unchecked exception for this purpose enables us to propagate fatal errors without polluting the
     * {@link Transformation}interface . Here, those exceptions are caught and a {@link Transformation.Rejected}
     * response returned, shortcircuiting any retry logic intended to mitigate more transient failures.
     *
     * @param metadata the starting state
     * @param transformation to be applied to the starting state
     * @return result of the application
     */
    private Transformation.Result executeStrictly(ClusterMetadata metadata, Transformation transformation)
    {
        try
        {
            return transformation.execute(metadata);
        }
        catch (Transformation.RejectedTransformationException e)
        {
            return new Transformation.Rejected(INVALID, e.getMessage());
        }
    }

    private LogState toLogState(Transformation.Success success, Entry.Id entryId, Epoch lastKnown, Transformation transform)
    {
        if (lastKnown == null || lastKnown.isDirectlyBefore(success.metadata.epoch))
            return LogState.of(new Entry(entryId, success.metadata.epoch, transform));
        else
            return toLogState(lastKnown);
    }

    private LogState toLogState(Epoch lastKnown)
    {
        LogState logState;
        if (lastKnown == null)
            logState = LogState.EMPTY;
        else
        {
            // We can use local log here since we always call this method only if local log is up-to-date:
            // in case of a successful commit, we apply against latest metadata locally before committing,
            // and in case of a rejection, we fetch latest entries to verify linearizability.
            logState = log.getLocalEntries(lastKnown);
        }

        return logState;
    }

    public abstract ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy);
    protected abstract boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch);
    protected abstract boolean acceptCommit(ClusterMetadata metadata);
}