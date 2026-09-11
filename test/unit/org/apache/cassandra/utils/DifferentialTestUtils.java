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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Byte/JSON-diff helpers shared by this branch's differential test harnesses -
 * {@code DifferentialCompactionTester} (compaction: iterator vs. cursor) and
 * {@code MemtableFlushDifferentialTester} (flush: iterator vs. cursor). Both harnesses
 * independently carried byte-for-byte identical copies of these methods; this is that one
 * shared copy. Thread-allocation measurement (formerly also shared from here) now goes through
 * the production {@code org.apache.cassandra.utils.ThreadStats} instead - see that class.
 */
public final class DifferentialTestUtils
{
    private DifferentialTestUtils()
    {
    }

    /** The byte offset of the first difference between two files, or -1 if they're identical. */
    public static long firstFileDifference(Path a, Path b)
    {
        try (java.io.InputStream ia = new java.io.BufferedInputStream(Files.newInputStream(a), 1 << 16);
             java.io.InputStream ib = new java.io.BufferedInputStream(Files.newInputStream(b), 1 << 16))
        {
            byte[] bufA = new byte[1 << 16];
            byte[] bufB = new byte[1 << 16];
            long offset = 0;
            while (true)
            {
                int readA = ia.readNBytes(bufA, 0, bufA.length);
                int readB = ib.readNBytes(bufB, 0, bufB.length);
                int common = Math.min(readA, readB);
                int mismatch = Arrays.mismatch(bufA, 0, common, bufB, 0, common);
                if (mismatch >= 0)
                    return offset + mismatch;
                if (readA != readB)
                    return offset + common; // same prefix, different length
                if (readA == 0)
                    return -1;
                offset += readA;
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /** A human-readable description of a byte divergence, with hex context from both files. */
    public static String describeFileDiff(String component, Path a, Path b, long firstDiff)
    {
        try
        {
            return String.format("  %s: lengths %d vs %d, first divergence at offset %d%n    iterator: %s%n    cursor:   %s",
                                 component, Files.size(a), Files.size(b), firstDiff,
                                 hexContext(a, firstDiff), hexContext(b, firstDiff));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /** A hex dump of the bytes around {@code offset} in {@code file}, with the byte at {@code offset} bracketed. */
    public static String hexContext(Path file, long offset) throws IOException
    {
        long size = Files.size(file);
        long from = Math.max(0, offset - 8);
        int len = (int) Math.min(size - from, 32);
        byte[] window = new byte[Math.max(len, 0)];
        try (java.io.InputStream in = Files.newInputStream(file))
        {
            in.skipNBytes(from);
            in.readNBytes(window, 0, window.length);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < window.length; i++)
        {
            long abs = from + i;
            if (abs == offset)
                sb.append('[');
            sb.append(String.format("%02x", window[i]));
            if (abs == offset)
                sb.append(']');
            sb.append(' ');
        }
        if (from + window.length < size)
            sb.append("...");
        return sb.toString();
    }

    /** The first differing line between two multi-line strings, with a few lines of context. */
    public static String firstJsonDiff(String a, String b)
    {
        String[] linesA = a.split("\n", -1);
        String[] linesB = b.split("\n", -1);
        int max = Math.max(linesA.length, linesB.length);
        for (int i = 0; i < max; i++)
        {
            String la = i < linesA.length ? linesA[i] : "<missing>";
            String lb = i < linesB.length ? linesB[i] : "<missing>";
            if (!la.equals(lb))
            {
                StringBuilder sb = new StringBuilder();
                sb.append("first differing line ").append(i + 1).append(" of ").append(max).append(":\n");
                for (int j = Math.max(0, i - 2); j < Math.min(max, i + 3); j++)
                {
                    String ja = j < linesA.length ? linesA[j] : "<missing>";
                    String jb = j < linesB.length ? linesB[j] : "<missing>";
                    sb.append(j == i ? ">>" : "  ").append(" iterator: ").append(ja).append('\n');
                    sb.append(j == i ? ">>" : "  ").append(" cursor:   ").append(jb).append('\n');
                }
                return sb.toString();
            }
        }
        return "(no line diff found despite string inequality — check line endings)";
    }
}
