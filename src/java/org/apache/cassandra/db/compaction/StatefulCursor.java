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

package org.apache.cassandra.db.compaction;

import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.UnfilteredValidation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;

import static org.apache.cassandra.db.rows.Cell.INVALID_DELETION_TIME;
import static org.apache.cassandra.db.rows.Cell.NO_DELETION_TIME;
import static org.apache.cassandra.db.rows.Cell.NO_TTL;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

class StatefulCursor extends SSTableCursorReader
{
    private final Config.CorruptedTombstoneStrategy corruptedTombstoneStrategy = DatabaseDescriptor.getCorruptedTombstoneStrategy();
    private final boolean corruptedTombstoneValidationEnabled = corruptedTombstoneStrategy != Config.CorruptedTombstoneStrategy.disabled;

    private PartitionDescriptor currPartition;
    /**
     * Holds the previous partition's header. The read side compares it for key order. The write
     * side takes it as the last written partition.
     */
    private PartitionDescriptor prevPartition;

    /** @see #partitionSwaps() */
    private long partitionSwaps = 0;

    /** Non-final: it is exchanged by {@link #detachUnfiltered}. */
    private UnfilteredDescriptor unfiltered;

    private boolean resetAfterDone = false;
    private long bytesReadPositionSnapshot = 0;

    private boolean isOpenRangeTombstonePresent = false;

    // Multi-segment partial-range bound support (see positionAt()). Null means unbounded (the
    // default, existing full-range behavior) - every check below short-circuits in that case.
    private Iterator<PartitionPositionBounds> remainingBounds;
    private long currentSegmentEndPosition;
    // True when DONE was reached by exhausting those bounds inside readPartitionHeader(), rather
    // than by hitting end of file from continueReading(). The two leave opposite curr/prev
    // arrangements - see resetAfterDone(), the only reader of this.
    private boolean doneByBoundExhaustion = false;

    public StatefulCursor(SSTableReader reader, DiskAccessMode diskAccessMode)
    {
        super(reader, diskAccessMode);
        // A deletion-only complex column must reach the merge as a position of its own, so that
        // its column-level deletion reaches the output.
        pauseAtEmptyComplexColumns(true);
        currPartition = new PartitionDescriptor(reader.getPartitioner().createReusableKey(0));
        prevPartition = new PartitionDescriptor(reader.getPartitioner().createReusableKey(0));
        unfiltered = new UnfilteredDescriptor(reader.header.clusteringTypes().toArray(AbstractType[]::new));
    }

    /**
     * Installs a set of partition-boundary-aligned byte ranges (as produced by
     * {@link SSTableReader#getPositionsForRanges}, the same type {@link org.apache.cassandra.io.sstable.format.SSTableSimpleScanner}
     * consumes) this cursor is restricted to, and seeks to the first one. Ranges must be
     * non-overlapping and in ascending order, exactly like {@code SSTableSimpleScanner}'s
     * contract - each range's bounds must fall on a partition boundary (verified by
     * {@link #seekPartition}).
     * <p>
     * Must be called before the first {@link #readPartitionHeader()}. Exhausting the assigned
     * ranges surfaces as the ordinary {@code DONE} state to callers - indistinguishable from
     * true end-of-file - see {@link #isFileEOF()} for the byte-accounting distinction.
     *
     * @return the resulting cursor state (mirrors {@link #seekPartition})
     */
    public int positionAt(List<PartitionPositionBounds> bounds)
    {
        assert bounds != null && !bounds.isEmpty();
        remainingBounds = bounds.iterator();
        PartitionPositionBounds first = remainingBounds.next();
        currentSegmentEndPosition = first.upperPosition;
        int state = seekPartition(first.lowerPosition);
        // Without this, bytesReadSinceSnapshot()'s first call after positioning would diff
        // against the stale default of 0, over-reporting by first.lowerPosition worth of bytes
        // that were skipped via seek, never actually read.
        bytesReadPositionSnapshot = first.lowerPosition;
        return state;
    }

    public int readPartitionHeader()
    {
        // Re-entry guard, matching the one readPartitionHeader(PartitionDescriptor) already applies
        // for unbounded cursors. A bounded cursor reaches DONE via forceDone() below without ever
        // entering that method, so without this check a repeat call would fall through to the swap
        // and rotate prevPartition onto stale data - silently, where the unbounded path throws.
        // Both DONE paths now reject re-entry identically, and checking before the swap leaves a
        // rejected call's descriptors untouched.
        if (state() != PARTITION_START)
            throw new IllegalStateException("readPartitionHeader() requires PARTITION_START, was: " + this);

        // Swap BEFORE the bounds check below, not just before the read: every path out of this
        // method must leave prevPartition holding the last partition actually read, because
        // CursorCompactor reads it back as the last key written to the output sstable
        // (writerRollover -> setLast). A cursor stopped early by its bounds returns DONE from
        // inside the loop below, and without this swap it would leave prevPartition one partition
        // behind - writing a `last` key that disagrees with the index. Unbounded cursors reach
        // DONE from readPartitionHeader(currPartition) after the swap has already happened, which
        // is the arrangement this reproduces.
        swapCurrAndPrevPartition();

        // Multi-segment bound support: once the underlying reader reaches the end of the
        // currently-active segment, advance to the next assigned segment (skipping any
        // degenerate empty ones) or report DONE if none remain - never read a partition that
        // falls outside the assigned ranges.
        while (remainingBounds != null && position() >= currentSegmentEndPosition)
        {
            if (!remainingBounds.hasNext())
            {
                doneByBoundExhaustion = true;
                return forceDone();
            }

            PartitionPositionBounds next = remainingBounds.next();
            currentSegmentEndPosition = next.upperPosition;
            long posBeforeSeek = position();
            int seekState = seekPartition(next.lowerPosition);
            // The seek jumps over the gap between the previous segment's end and this segment's
            // start; those bytes are skipped, never read, so advance the snapshot past them - the
            // same correction positionAt() applies for the leading skip. Without this the gap is
            // counted as bytes read on the next bytesReadSinceSnapshot(), pushing getBytesRead()
            // beyond getEstimatedBytes() (>100% progress) for a validation over disjoint ranges.
            bytesReadPositionSnapshot += next.lowerPosition - posBeforeSeek;
            if (seekState == DONE)
            {
                // Also post-swap, so it leaves the same arrangement as the forceDone above.
                doneByBoundExhaustion = true;
                return seekState;
            }
            // loop again: the newly-entered segment may itself already be at its own end
        }

        int state = readPartitionHeader(currPartition);

        if (prevPartition.keyLength() != 0 && prevPartition.key().compareTo(currPartition.key()) >= 0)
            corruptSSTableKeysOOO();
        if (corruptedTombstoneValidationEnabled)
            validateInvalidPartitionDeletion();
        return state;
    }

    /**
     * Hands the write side the instance holding the previous partition's header and takes
     * {@code floater} in its place. This exchange is what these comments call a steal: a cursor
     * overwrites its two descriptors as it reads, so the write side takes the object rather than
     * its contents and hands back a spare, the floater.
     *
     * It detaches {@code prev} rather than {@code curr} because {@code curr} is the next read's
     * out-of-order comparison source.
     */
    PartitionDescriptor detachPrevPartition(PartitionDescriptor floater)
    {
        assert floater != prevPartition && floater != currPartition
             : "the floater is already one of this cursor's slots; a steal was not handed back";
        PartitionDescriptor detached = prevPartition;
        prevPartition = floater;
        return detached;
    }

    /**
     * Counts the exchanges of the curr and prev partition slots, so the write side can assert that
     * the slot it recorded still holds its partition. It counts swaps rather than reads because
     * {@link #resetAfterDone} advances the slots too.
     */
    long partitionSwaps()
    {
        return partitionSwaps;
    }

    /**
     * Hands the write side the instance holding the unfiltered it has just written out, and takes
     * {@code floater} in its place. See {@link #detachPrevPartition}.
     *
     * The steal cannot wait a round, and need not: the next touch of the slot is another load. A
     * cursor stolen from in any state but {@code UNFILTERED_END} mis-orders the merge silently.
     */
    UnfilteredDescriptor detachUnfiltered(UnfilteredDescriptor floater)
    {
        assert floater != unfiltered
             : "the floater is already this cursor's slot; a steal was not handed back";
        assert state() == UNFILTERED_END
             : "an unfiltered was stolen from a cursor that has not finished it: " + this;
        UnfilteredDescriptor detached = unfiltered;
        unfiltered = floater;
        return detached;
    }

    private int corruptSSTableKeysOOO()
    {
        return corruptSSTable("Keys out of order. Current key: " + keyToString(currentKey()) + " <= "  + keyToString(prevKey()));
    }

    private void swapCurrAndPrevPartition()
    {
        partitionSwaps++;
        PartitionDescriptor temp = currPartition;
        currPartition = prevPartition;
        prevPartition = temp;
    }

    public int skipUnfiltered()
    {
        if (isState(state(), CELL_HEADER_START | CELL_VALUE_START | CELL_END))
            return super.skipRowCells(unfiltered().dataStart(), unfiltered().size(), false);

        return super.skipUnfiltered(false);
    }

    public int skipStaticRow()
    {
        if (isState(state(), CELL_HEADER_START | CELL_VALUE_START | CELL_END))
            return super.skipRowCells(unfiltered().dataStart(), unfiltered().size(), false);

        return super.skipStaticRow(false);
    }

    /** @see SSTableCursorReader#rewindRowCells(UnfilteredDescriptor, boolean) */
    public int rewindRowCells(boolean isStatic)
    {
        return super.rewindRowCells(unfiltered, isStatic);
    }

    @Override
    public String toString()
    {
        return "StatefulCursor{" +
               "pHeader=" + currPartition() +
               ", rHeader=" + unfiltered() +
               ", state=" + state() +
               '}';
    }

    /**
     * Establishes the same post-DONE arrangement however this cursor reached DONE: {@link #prevKey()}
     * holds the last partition actually read - {@code CursorCompactor} consults it as the last key
     * written to an output sstable ({@code writerRollover -> setLast}) - and {@code currPartition} is
     * cleared, so partition-key sorting sees no leftover key for a cursor with nothing left to offer.
     * <p>
     * The two routes to DONE arrive with OPPOSITE arrangements, which is why the swap is conditional:
     * <ul>
     *   <li>End of file: reached from {@code continueReading()} after the last partition's
     *       {@code PARTITION_END}, WITHOUT re-entering {@link #readPartitionHeader()} - so
     *       {@code currPartition} still holds the last partition read and has to be swapped into
     *       {@code prevPartition} before being cleared.</li>
     *   <li>Bound exhaustion: reached from inside {@link #readPartitionHeader()}, which already
     *       swapped on the way in - so {@code prevPartition} ALREADY holds the last partition read
     *       and {@code currPartition} holds stale content. Swapping again would clear the last
     *       partition read and leave {@code prevKey()} pointing two partitions back.</li>
     * </ul>
     *
     * @return true if reset, false if already been reset
     */
    public boolean resetAfterDone()
    {
        if (resetAfterDone)
            return false;
        resetAfterDone = true;
        if (!doneByBoundExhaustion)
        {
            // Exactly ONE route reaches DONE without setting that flag - true end of file - and the
            // swap below is correct only for that one. A future third route (CASSANDRA-21544 also
            // does multi-cursor partial-range writing) that reaches DONE by some other means would
            // land here and swap the wrong way: prevKey() would point two partitions back and the
            // output sstable would get a `last` key disagreeing with its index, silently, with no
            // error anywhere. Fail loudly instead of corrupting quietly.
            assert isFileEOF() : "resetAfterDone() reached DONE via neither bound exhaustion nor end of " +
                                 "file, so the curr/prev arrangement here is unknown: " + this;
            swapCurrAndPrevPartition();
        }
        // only current is reset, prev is still needed.
        currPartition().resetPartition();
        unfiltered().resetUnfiltered();
        return true;
    }

    DecoratedKey currentKey()
    {
        return currPartition.key();
    }

    DecoratedKey prevKey()
    {
        return prevPartition.key();
    }

    public PartitionDescriptor currPartition()
    {
        return currPartition;
    }

    public UnfilteredDescriptor unfiltered()
    {
        return unfiltered;
    }

    public long bytesReadSinceSnapshot()
    {
        // isFileEOF(), NOT isEOF(): a cursor stopped early by positionAt()'s bounds reports DONE
        // via isEOF() well before the file's actual end - using uncompressedLength() there would
        // over-report this cursor's remaining/total bytes. isFileEOF() distinguishes true
        // end-of-file from that logical, bound-triggered DONE. For an unbounded cursor the two
        // are always equivalent (state only ever reaches DONE when dataReader.isEOF() does), so
        // this is behavior-preserving for existing full-range callers.
        long latestByteReadPosition = isFileEOF() ? uncompressedLength() : position();
        long cursorBytesRead = latestByteReadPosition - bytesReadPositionSnapshot;
        bytesReadPositionSnapshot = latestByteReadPosition;
        return cursorBytesRead;
    }

    private String keyToString(DecoratedKey key)
    {
        String keyString;
        try
        {
            keyString = ssTableReader().metadata().partitionKeyType.getString(key.getKey());
        }
        catch (Throwable t)
        {
            keyString = "[corrupt token="+key.getToken()+"]";
        }
        return keyString;
    }

    public void readRowHeader()
    {
        super.readRowHeader(unfiltered);
        if (corruptedTombstoneValidationEnabled)
            validateInvalidRowDeletion();
    }

    public void readTombstoneMarker()
    {
        super.readTombstoneMarker(unfiltered);

        if (corruptedTombstoneValidationEnabled)
            validateInvalidTombstoneDeletion();

        boolean isStartBound = unfiltered.isStartBound();
        if (isOpenRangeTombstonePresent && isStartBound)
            corruptSSTable("Encountered an open range tombstone marker before the prev was closed: " + unfiltered);
        if (!isOpenRangeTombstonePresent && !isStartBound)
            corruptSSTable("Encountered an close/boundary range tombstone marker before an open one: " + unfiltered);
        isOpenRangeTombstonePresent = isStartBound || unfiltered.isBoundary();
        // TODO: can also add verification of open/close timestamp match
    }

    public void readStaticRowHeader()
    {
        super.readStaticRowHeader(unfiltered);
        if (corruptedTombstoneValidationEnabled)
            validateInvalidRowDeletion();
    }

    @Override
    public int readCellHeader()
    {
        int state = super.readCellHeader();
        // cellLiveness is valid only where a cell was produced; elsewhere it still holds an
        // earlier cell's values, possibly from an earlier row. The state test excludes
        // UNFILTERED_END, which the dropped-column filter can reach with no cell. producedCell
        // excludes a deletion-only complex column, which returns CELL_END with no cell fields.
        if (corruptedTombstoneValidationEnabled && isState(state, CELL_VALUE_START | CELL_END) && cellCursor().producedCell)
            validateInvalidCellDeletion();
        return state;
    }

    private void validateInvalidTombstoneDeletion()
    {
        if (!unfiltered.deletionTime().validate()) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "rowDeletion="+currPartition.deletionTime().toString());
        }
        if (unfiltered.isBoundary() && !unfiltered.deletionTime2().validate()) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "rowDeletion2="+currPartition.deletionTime().toString());
        }
    }

    private void validateInvalidCellDeletion()
    {
        ReusableCellLivenessInfo cellLiveness = cellCursor().cellLiveness;
        if (hasInvalidCellDeletion(cellLiveness.ttl(), cellLiveness.localDeletionTime())) {
            UnfilteredValidation.handleInvalid(
            ssTableReader().metadata(),
            currPartition.key(),
            ssTableReader(),
            "cellLiveness="+cellLiveness);
        }
    }

    /**
     * Mirrors {@link org.apache.cassandra.db.rows.AbstractCell#hasInvalidDeletions()}, where
     * {@code ttl != NO_TTL} is the reference's {@code isExpiring()}.
     */
    @VisibleForTesting
    static boolean hasInvalidCellDeletion(int ttl, long localExpirationTime)
    {
        return ttl < 0
               || localExpirationTime == INVALID_DELETION_TIME
               || localExpirationTime < 0
               || (ttl != NO_TTL && localExpirationTime == NO_DELETION_TIME);
    }

    /** Mirrors the primary-key liveness clause of {@link org.apache.cassandra.db.rows.AbstractRow#hasInvalidDeletions()}. */
    @VisibleForTesting
    static boolean hasInvalidRowLiveness(int ttl, long localExpirationTime)
    {
        return ttl != NO_TTL && (ttl < 0 || localExpirationTime < 0);
    }

    private void validateInvalidRowDeletion()
    {
        if (!unfiltered.deletionTime().validate()) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "rowDeletion="+currPartition.deletionTime().toString());
        }
        ReusableLivenessInfo livenessInfo = unfiltered.livenessInfo();
        if (hasInvalidRowLiveness(livenessInfo.ttl(), livenessInfo.localExpirationTime())) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "rowLiveness="+livenessInfo.toString());
        }

    }

    private void validateInvalidPartitionDeletion()
    {
        if (!currPartition.deletionTime().validate()) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "partitionLevelDeletion="+currPartition.deletionTime().toString());
        }
    }
}
