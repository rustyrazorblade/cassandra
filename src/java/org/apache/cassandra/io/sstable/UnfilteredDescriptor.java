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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.util.RandomAccessReader;

public class UnfilteredDescriptor extends ClusteringDescriptor
{
    private final ReusableLivenessInfo rowLivenessInfo = new ReusableLivenessInfo();
    private final ReusableDeletionTime deletionTime = ReusableDeletionTime.live();
    private final ReusableDeletionTime deletionTime2 = ReusableDeletionTime.live();

    private long position;
    private int flags;
    private int extendedFlags;

    private long unfilteredSize;
    private long unfilteredDataStart;
    private long prevUnfilteredSize;
    Columns rowColumns;
    // Bit i set means column i of rowColumns, in iteration order, is absent from this row.
    // Only a subset row with fewer than 64 superset columns sets a bit.
    // A superset of 64 columns or more uses presentColumnsWords instead.
    private long missingColumnsMask;
    // Bit i of word i/64 set means superset column i is present in this row.
    // A row with a superset of 64 columns or more decodes into it, and useColumnsWords then
    // marks it valid. The next row overwrites the array, so CellCursor reads it within
    // the same row.
    private long[] presentColumnsWords;
    private boolean useColumnsWords;

    public UnfilteredDescriptor(AbstractType<?>[] clusteringTypes)
    {
        super(clusteringTypes);
    }

    void loadTombstone(RandomAccessReader dataReader,
                       SerializationHeader serializationHeader,
                       int flags) throws IOException
    {
        this.flags = flags;
        this.extendedFlags = 0;
        rowColumns = null;
        missingColumnsMask = 0;
        useColumnsWords = false;
        byte clusteringKind = dataReader.readByte();
        if (clusteringKind == STATIC_CLUSTERING_KIND || clusteringKind == ROW_CLUSTERING_KIND) {
            // A row or static clustering kind carries no deletion time.
            throw new IllegalStateException();
        }

        int columnsBound = dataReader.readUnsignedShort();
        loadClustering(dataReader, clusteringKind, columnsBound);
        unfilteredSize = dataReader.readUnsignedVInt();
        prevUnfilteredSize = dataReader.readUnsignedVInt(); // debug only, unused otherwise
        if (clusteringKind == EXCL_END_INCL_START_BOUNDARY_CLUSTERING_KIND ||
            clusteringKind == INCL_END_EXCL_START_BOUNDARY_CLUSTERING_KIND)
        {
            // CLOSE
            serializationHeader.readDeletionTime(dataReader, deletionTime);
            // OPEN
            serializationHeader.readDeletionTime(dataReader, deletionTime2);
        }
        else
        {
            // CLOSE|OPEN
            serializationHeader.readDeletionTime(dataReader, deletionTime);
        }
    }

    void loadRow(RandomAccessReader dataReader,
                 SerializationHeader serializationHeader,
                 DeserializationHelper deserializationHelper,
                 int flags,
                 int extendedFlags) throws IOException {
        // The unfiltered starts at its flags byte, which the reader has already consumed.
        // A non-static row adds an extended-flags byte only for a shadowable deletion.
        position = dataReader.getPosition() - (UnfilteredSerializer.isExtended(flags) ? 2 : 1);
        this.flags = flags;
        this.extendedFlags = extendedFlags;

        loadClustering(dataReader, ROW_CLUSTERING_KIND, this.clusteringTypes.length);

        rowColumns = serializationHeader.columns(false);

        loadCommonRowFields(dataReader, serializationHeader, deserializationHelper, flags);
    }

    void loadStaticRow(RandomAccessReader dataReader,
                       SerializationHeader serializationHeader,
                       DeserializationHelper deserializationHelper,
                       int flags,
                       int extendedFlags) throws IOException {
        // The unfiltered starts at its flags byte, and a static row always adds an
        // extended-flags byte.
        position = dataReader.getPosition() - 2;
        this.flags = flags;
        this.extendedFlags = extendedFlags;
        loadClustering(dataReader, STATIC_CLUSTERING_KIND, 0);
        rowColumns = serializationHeader.columns(true);

        loadCommonRowFields(dataReader, serializationHeader, deserializationHelper, flags);
    }

    private void loadCommonRowFields(RandomAccessReader dataReader,
                                     SerializationHeader serializationHeader,
                                     DeserializationHelper deserializationHelper,
                                     int flags) throws IOException
    {
        unfilteredSize = dataReader.readUnsignedVInt();
        // size covers the body from this point on, prev_row_size included.
        unfilteredDataStart = dataReader.getPosition();
        prevUnfilteredSize = dataReader.readUnsignedVInt(); // debug only, unused otherwise

        SSTableCursorReader.readLivenessInfo(dataReader,
                                             serializationHeader,
                                             deserializationHelper,
                                             flags,
                                             rowLivenessInfo);

        if (UnfilteredSerializer.hasDeletion(flags))
        {
            serializationHeader.readDeletionTime(dataReader, deletionTime);
        }
        else
        {
            deletionTime.resetLive();
        }
        useColumnsWords = false;
        if (!UnfilteredSerializer.hasAllColumns(flags))
        {
            if (rowColumns.size() < 64)
            {
                // Columns.Serializer.deserializeSubset would build a Columns per row, so decode
                // its wire format here: an unsigned vint bitmask of the missing superset columns.
                // rowColumns stays the superset, and consumers filter with missingColumnsMask().
                long encoded = dataReader.readUnsignedVInt();
                // Mirrors the corruption check in Columns.Serializer.deserializeSubset.
                if ((encoded >>> rowColumns.size()) != 0)
                    throw new IOException("Invalid Columns subset bytes; too many bits set: " + Long.toBinaryString(encoded));
                missingColumnsMask = encoded;
            }
            else
            {
                // Wire format per Columns.Serializer.serializeLargeSubset: an unsigned vint delta
                // of supersetCount - presentCount, then one unsigned vint superset index per
                // column. The indices name the present columns when presentCount is under half
                // the superset, and the missing columns otherwise. Decoding into reusable mask
                // words leaves rowColumns as the superset, so CellCursor never rebuilds its
                // per-superset arrays.
                long encoded = dataReader.readUnsignedVInt();
                int supersetCount = rowColumns.size();
                if (encoded > supersetCount)
                    throw new IOException("Invalid large Columns subset: missing count " + encoded + " of " + supersetCount);
                int delta = (int) encoded;
                int columnCount = supersetCount - delta;
                int nWords = (supersetCount + 63) >>> 6;
                if (presentColumnsWords == null || presentColumnsWords.length < nWords)
                    presentColumnsWords = new long[nWords];
                if (columnCount < supersetCount / 2)
                {
                    // Present-index mode.
                    java.util.Arrays.fill(presentColumnsWords, 0, nWords, 0L);
                    for (int i = 0; i < columnCount; i++)
                    {
                        int idx = dataReader.readUnsignedVInt32();
                        if (idx < 0 || idx >= supersetCount)
                            throw new IOException("Invalid large Columns subset: present index " + idx + " of " + supersetCount);
                        presentColumnsWords[idx >>> 6] |= 1L << (idx & 63);
                    }
                }
                else
                {
                    // Missing-index mode: the last word starts trimmed to the column range.
                    // A delta of 0 clears nothing and leaves every column present.
                    java.util.Arrays.fill(presentColumnsWords, 0, nWords, -1L);
                    if ((supersetCount & 63) != 0)
                        presentColumnsWords[nWords - 1] = -1L >>> (64 - (supersetCount & 63));
                    for (int i = 0; i < delta; i++)
                    {
                        int idx = dataReader.readUnsignedVInt32();
                        if (idx < 0 || idx >= supersetCount)
                            throw new IOException("Invalid large Columns subset: missing index " + idx + " of " + supersetCount);
                        presentColumnsWords[idx >>> 6] &= ~(1L << (idx & 63));
                    }
                }
                useColumnsWords = true;
                missingColumnsMask = 0;
            }
        }
        else
        {
            missingColumnsMask = 0;
        }
    }

    /**
     * Memtable-source (write) path: populates just the clustering-key bytes for a regular row
     * from a live {@link Clustering}. {@link SSTableCursorWriter#writeRowEnd} only reads the
     * clustering fields off this descriptor for a non-static row (liveness/deletion are passed
     * to {@code writeRowStart} directly, and column-subset bookkeeping is tracked by the writer
     * itself as cells are written) — so no other field needs to be populated here.
     */
    public void storeRowClustering(Clustering<?> clustering)
    {
        storeClustering(ROW_CLUSTERING_KIND, clusteringTypes.length, clustering);
    }

    /**
     * Memtable-source (write) path: populates a range-tombstone bound/boundary marker —
     * clustering bytes plus the deletion time(s) {@link SSTableCursorWriter#writeRangeTombstone}
     * reads off this descriptor. {@code open} is only consulted for boundary kinds; pass
     * {@link DeletionTime#LIVE} for plain bounds.
     * <p>
     * {@code kind} is taken separately from {@code valuesSource} (rather than reading
     * {@code valuesSource.kind()}) so a boundary can reuse one side's already-live
     * {@link org.apache.cassandra.db.ClusteringBound} as the values source under the merged
     * boundary kind, the same way {@link org.apache.cassandra.db.ClusteringBoundary#create}
     * does for the iterator path — {@link ClusteringPrefix.Serializer#serializeValuesWithoutSize}
     * only reads {@code valuesSource}'s size/values, never its own kind.
     */
    public void storeMarker(ClusteringPrefix.Kind kind, ClusteringPrefix<?> valuesSource, DeletionTime close, DeletionTime open)
    {
        storeClustering((byte) kind.ordinal(), valuesSource.size(), valuesSource);
        deletionTime.reset(close);
        if (kind.isBoundary())
            deletionTime2.reset(open);
        else
            deletionTime2.resetLive();
    }

    public void resetUnfiltered()
    {
        resetClustering();
        position = 0;
        flags = 0;
        extendedFlags = 0;
        unfilteredSize = 0;
        unfilteredDataStart = 0;
        prevUnfilteredSize = 0;
        rowColumns = null;
        missingColumnsMask = 0;
        useColumnsWords = false;
    }

    public long position()
    {
        return position;
    }

    public ReusableLivenessInfo livenessInfo()
    {
        return rowLivenessInfo;
    }

    public ReusableDeletionTime deletionTime()
    {
        return deletionTime;
    }

    public ReusableDeletionTime openDeletionTime()
    {
        return isBoundary() ? deletionTime2 : isEndBound() ? null : deletionTime;
    }

    public ReusableDeletionTime deletionTime2()
    {
        return deletionTime2;
    }

    // Row.Deletion.isShadowable(): deprecated since 4.0 (CASSANDRA-11500), reachable only on
    // old Materialized View data nothing still produces. A live deletion is never shadowable
    // (Row.Deletion's own constructor asserts this), so this is meaningful only alongside a
    // non-live deletionTime().
    public boolean isShadowableDeletion()
    {
        return UnfilteredSerializer.deletionIsShadowable(extendedFlags);
    }

    /** True when this row stores a deletion for every complex column, live deletions included. */
    public boolean hasComplexDeletion()
    {
        return UnfilteredSerializer.hasComplexDeletion(flags);
    }

    public int flags()
    {
        return flags;
    }

    public long size()
    {
        return unfilteredSize;
    }

    public long dataStart()
    {
        return unfilteredDataStart;
    }

    public Columns rowColumns()
    {
        return rowColumns;
    }

    /** See {@link #missingColumnsMask} */
    public long missingColumnsMask()
    {
        return missingColumnsMask;
    }

    /**
     * Present-column mask words for this row, or null when the row holds every column or its
     * superset has fewer than 64 columns. See {@link #presentColumnsWords} for the bit layout.
     * The next unfiltered load overwrites the array, so read it before then.
     */
    public long[] presentColumnsWords()
    {
        return useColumnsWords ? presentColumnsWords : null;
    }

    @Override
    public String toString()
    {
        return "UnfilteredDescriptor{" +
               "rowLivenessInfo=" + rowLivenessInfo +
               ", deletionTime=" + deletionTime +
               ", position=" + position +
               ", flags=" + flags +
               ", extFlags=" + extendedFlags +
               ", unfilteredSize=" + unfilteredSize +
               ", prevUnfilteredSize=" + prevUnfilteredSize +
               ", unfilteredDataStart=" + unfilteredDataStart +
               ", rowColumns=" + rowColumns +
               ", clusteringTypes=" + Arrays.toString(clusteringTypes()) +
               '}';
    }
}
