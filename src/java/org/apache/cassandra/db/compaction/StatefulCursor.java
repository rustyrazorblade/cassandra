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
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

// Cursor state
class StatefulCursor extends SSTableCursorReader
{
    private final Config.CorruptedTombstoneStrategy corruptedTombstoneStrategy = DatabaseDescriptor.getCorruptedTombstoneStrategy();
    private final boolean corruptedTombstoneValidationEnabled = corruptedTombstoneStrategy != Config.CorruptedTombstoneStrategy.disabled;

    private PartitionDescriptor currPartition;
    /**
     * used for tracking reader and writer order, as well as last pk
     */
    private PartitionDescriptor prevPartition;

    private final UnfilteredDescriptor unfiltered;

    private boolean resetAfterDone = false;
    private long bytesReadPositionSnapshot = 0;

    private boolean isOpenRangeTombstonePresent = false;

    // Multi-segment partial-range bound support (see positionAt()). Null means unbounded (the
    // default, existing full-range behavior) - every check below short-circuits in that case.
    private Iterator<PartitionPositionBounds> remainingBounds;
    private long currentSegmentEndPosition;

    public StatefulCursor(SSTableReader reader, DiskAccessMode diskAccessMode)
    {
        super(reader, diskAccessMode);
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
        // Multi-segment bound support: once the underlying reader reaches the end of the
        // currently-active segment, advance to the next assigned segment (skipping any
        // degenerate empty ones) or report DONE if none remain - never read a partition that
        // falls outside the assigned ranges.
        while (remainingBounds != null && position() >= currentSegmentEndPosition)
        {
            if (!remainingBounds.hasNext())
                return forceDone();

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
                return seekState;
            // loop again: the newly-entered segment may itself already be at its own end
        }

        swapCurrAndPrevPartition();
        int state = readPartitionHeader(currPartition);

        if (prevPartition.keyLength() != 0 && prevPartition.key().compareTo(currPartition.key()) >= 0)
            corruptSSTableKeysOOO();
        if (corruptedTombstoneValidationEnabled)
            validateInvalidPartitionDeletion();
        return state;
    }

    private int corruptSSTableKeysOOO()
    {
        return corruptSSTable("Keys out of order. Current key: " + keyToString(currentKey()) + " <= "  + keyToString(prevKey()));
    }

    private void swapCurrAndPrevPartition()
    {
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
     * @return true if reset, false if already been reset
     */
    public boolean resetAfterDone()
    {
        if (resetAfterDone)
            return false;
        resetAfterDone = true;
        swapCurrAndPrevPartition();
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
        // Validate only where a cell was actually surfaced. Every path that surfaces one returns
        // CELL_VALUE_START or CELL_END, including a valueless final cell; UNFILTERED_END means the
        // dropped-column filter discarded every remaining column, and cellLiveness then still
        // describes the last DISCARDED cell rather than the current position.
        if (corruptedTombstoneValidationEnabled && isState(state, CELL_VALUE_START | CELL_END))
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
        ReusableLivenessInfo cellLiveness = cellCursor().cellLiveness;
        if (hasInvalidCellDeletion(cellLiveness.ttl(), cellLiveness.localExpirationTime())) {
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
