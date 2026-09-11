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
package org.apache.cassandra.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Random;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The direct source has to return the same bytes as the buffered one, from any offset and for any length.
 *
 * O_DIRECT reads whole blocks at block boundaries only. A section is aligned to neither, so each read takes
 * the aligned span covering the request and copies the wanted bytes out of it. The cases here cover aligned
 * and unaligned offsets and lengths.
 *
 * These tests skip themselves where the volume has no O_DIRECT, and they only mean something on Linux. On
 * macOS the JDK maps the option to F_NOCACHE, which does not enforce alignment, so they pass there whether
 * the arithmetic is right or not.
 */
public class StreamingFileSourceTest
{
    private static final int FILE_SIZE = 1 << 20;

    private static File file;
    private static byte[] contents;
    private static int blockSize;

    @BeforeClass
    public static void writeFile() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();

        file = new File(Files.createTempFile("streaming-file-source", ".db"));
        file.deleteOnExit();

        contents = new byte[FILE_SIZE];
        new Random(0).nextBytes(contents);
        Files.write(file.toPath(), contents);

        Assume.assumeTrue("this volume does not support direct IO", FileUtils.isDirectIOSupported(file));
        blockSize = FileUtils.getFileBlockSize(file);
        assertTrue("a usable block size is what makes the alignment arithmetic possible", blockSize > 0);
    }

    @Test
    public void readsAlignedToABlock() throws IOException
    {
        assertSameAsBuffered(blockSize, blockSize);
    }

    @Test
    public void readsFromAnUnalignedOffset() throws IOException
    {
        assertSameAsBuffered(blockSize + 1, blockSize);
    }

    @Test
    public void readsAnUnalignedLength() throws IOException
    {
        assertSameAsBuffered(blockSize, blockSize - 1);
    }

    @Test
    public void readsWithNeitherEndAligned() throws IOException
    {
        assertSameAsBuffered(blockSize + 37, (3 * blockSize) - 11);
    }

    @Test
    public void readsFromTheStartOfTheFile() throws IOException
    {
        assertSameAsBuffered(0, 17);
    }

    @Test
    public void readsShorterThanABlock() throws IOException
    {
        assertSameAsBuffered(blockSize + 5, 3);
    }

    @Test
    public void readsUpToTheEndOfTheFile() throws IOException
    {
        // the aligned span covering this runs past the end of the file
        assertSameAsBuffered(FILE_SIZE - 100, 100);
    }

    @Test
    public void readsSeveralRangesFromOneSource() throws IOException
    {
        try (StreamingFileSource source = StreamingFileSource.open(file, DiskAccessMode.direct))
        {
            assertReads(source, 0, 64);
            assertReads(source, blockSize + 1, 129);
            assertReads(source, FILE_SIZE - 64, 64);
        }
    }

    private void assertSameAsBuffered(int position, int length) throws IOException
    {
        byte[] buffered;
        try (StreamingFileSource source = StreamingFileSource.open(file, DiskAccessMode.standard))
        {
            buffered = read(source, position, length);
        }

        byte[] direct;
        try (StreamingFileSource source = StreamingFileSource.open(file, DiskAccessMode.direct))
        {
            direct = read(source, position, length);
        }

        assertArrayEquals("the file's own bytes at [" + position + ", " + (position + length) + ')',
                          expected(position, length), buffered);
        assertArrayEquals("direct and buffered reads must agree at [" + position + ", " + (position + length) + ')',
                          buffered, direct);
    }

    private void assertReads(StreamingFileSource source, int position, int length) throws IOException
    {
        assertArrayEquals("read at [" + position + ", " + (position + length) + ')',
                          expected(position, length), read(source, position, length));
    }

    private static byte[] read(StreamingFileSource source, int position, int length) throws IOException
    {
        ByteBuffer into = ByteBuffer.allocate(length);
        source.read(into, position);

        assertEquals("the source should fill the buffer it was given", 0, into.remaining());

        return into.array();
    }

    private static byte[] expected(int position, int length)
    {
        byte[] expected = new byte[length];
        System.arraycopy(contents, position, expected, 0, length);
        return expected;
    }
}
