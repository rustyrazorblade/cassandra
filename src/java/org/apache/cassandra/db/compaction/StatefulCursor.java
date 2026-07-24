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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.UnfilteredValidation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.db.rows.Cell.INVALID_DELETION_TIME;
import static org.apache.cassandra.db.rows.Cell.NO_DELETION_TIME;
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

    // Partition-boundary-aligned upper bound (inclusive): once a partition header is read whose
    // key compares greater than this, the cursor reports DONE without reading any further into
    // that partition. Null (the default) means unbounded. See positionAt/setEndBound.
    private PartitionPosition endBound;

    public StatefulCursor(SSTableReader reader, DiskAccessMode diskAccessMode)
    {
        super(reader, diskAccessMode);
        currPartition = new PartitionDescriptor(reader.getPartitioner().createReusableKey(0));
        prevPartition = new PartitionDescriptor(reader.getPartitioner().createReusableKey(0));
        unfiltered = new UnfilteredDescriptor(reader.header.clusteringTypes().toArray(AbstractType[]::new));
    }

    public int readPartitionHeader()
    {
        swapCurrAndPrevPartition();
        int state = readPartitionHeader(currPartition);

        if (prevPartition.keyLength() != 0 && prevPartition.key().compareTo(currPartition.key()) >= 0)
            corruptSSTableKeysOOO();
        if (corruptedTombstoneValidationEnabled)
            validateInvalidPartitionDeletion();
        // logical end-of-range: this partition is past the caller-supplied bound (see
        // positionAt/setEndBound) — report DONE without reading any further into it. The header
        // itself has already been consumed (its key is what we're comparing), but no row/marker
        // of this out-of-bound partition is read.
        if (endBound != null && state != DONE && currentKey().compareTo(endBound) > 0)
            return forceDone();
        return state;
    }

    /**
     * Resolves {@code startBound} to a file position via the sstable's index (the same lookup
     * the iterator path uses for a partial-range scanner) and seeks there. If there is no
     * partition at or after the bound (it is past the end of the file), the cursor ends up in
     * its DONE state, exactly as at true end-of-file.
     * <p>
     * Partition-boundary-aligned only: this does not support splitting mid-partition (range
     * tombstone state can't be carried across such a split, and nothing needs it to). Wraparound
     * token ranges are the caller's responsibility — split into non-wrapping sub-ranges before
     * calling.
     */
    public int positionAt(PartitionPosition startBound)
    {
        long position = ssTableReader().getPosition(startBound, SSTableReader.Operator.GE);
        if (position < 0)
            return forceDone();
        return seekPartition(position);
    }

    /**
     * Sets a partition-boundary-aligned, INCLUSIVE upper bound: once a partition header is read
     * whose key compares greater than {@code endBound}, {@link #readPartitionHeader()} reports
     * DONE without reading further into it. Null (the default) means unbounded.
     */
    public void setEndBound(PartitionPosition endBound)
    {
        this.endBound = endBound;
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
        // isFileEOF(), NOT isEOF(): a cursor stopped early by an endBound (positionAt) reports
        // DONE via isEOF() well before the file's actual end — using uncompressedLength() there
        // would over-report this cursor's remaining/total bytes. isFileEOF() distinguishes true
        // end-of-file from that logical, bound-triggered DONE.
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
        if (corruptedTombstoneValidationEnabled)
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
        long ldt = cellLiveness.localExpirationTime();
        if (cellLiveness.ttl() < 0 || ldt == INVALID_DELETION_TIME || ldt < 0 || (cellLiveness.isExpiring() && ldt == NO_DELETION_TIME)) {
            UnfilteredValidation.handleInvalid(
            ssTableReader().metadata(),
            currPartition.key(),
            ssTableReader(),
            "cellLiveness="+cellLiveness);
        }
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
        if (livenessInfo.isExpiring() && (livenessInfo.ttl() < 0 || livenessInfo.localExpirationTime() < 0)) {
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
