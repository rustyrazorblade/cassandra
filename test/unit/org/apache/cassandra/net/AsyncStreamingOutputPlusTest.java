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

package org.apache.cassandra.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.utils.FBUtilities;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsyncStreamingOutputPlusTest
{

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSuccess() throws IOException
    {
        EmbeddedChannel channel = new TestChannel(4);
        ByteBuf read;
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            out.writeInt(1);
            assertEquals(0, out.flushed());
            assertEquals(0, out.flushedToNetwork());
            assertEquals(4, out.position());

            out.doFlush(0);
            assertEquals(4, out.flushed());
            assertEquals(4, out.flushedToNetwork());

            out.writeInt(2);
            assertEquals(8, out.position());
            assertEquals(4, out.flushed());
            assertEquals(4, out.flushedToNetwork());

            out.doFlush(0);
            assertEquals(8, out.position());
            assertEquals(8, out.flushed());
            assertEquals(4, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(4, read.readableBytes());
            assertEquals(1, read.getInt(0));
            assertEquals(8, out.flushed());
            assertEquals(8, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(4, read.readableBytes());
            assertEquals(2, read.getInt(0));

            out.write(new byte[16]);
            assertEquals(24, out.position());
            assertEquals(8, out.flushed());
            assertEquals(8, out.flushedToNetwork());

            out.doFlush(0);
            assertEquals(24, out.position());
            assertEquals(24, out.flushed());
            assertEquals(24, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(16, read.readableBytes());
            assertEquals(0, read.getLong(0));
            assertEquals(0, read.getLong(8));
            assertEquals(24, out.position());
            assertEquals(24, out.flushed());
            assertEquals(24, out.flushedToNetwork());

            out.writeToChannel(alloc -> {
                ByteBuffer buffer = alloc.get(16);
                buffer.putLong(1);
                buffer.putLong(2);
                buffer.flip();
            }, StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort()));

            assertEquals(40, out.position());
            assertEquals(40, out.flushed());
            assertEquals(40, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(16, read.readableBytes());
            assertEquals(1, read.getLong(0));
            assertEquals(2, read.getLong(8));
        }
    }

    @Test
    public void testLegacyStreamingUsesConfiguredSendWindow() throws IOException
    {
        // A TestChannel uses Netty's default WriteBufferWaterMark (64 KiB high), which historically
        // capped the legacy streaming send window and made throughput latency-bound. The legacy path
        // must instead honor the configurable stream_send_window, while never dropping below the
        // channel's own high water mark.
        int originalWindow = DatabaseDescriptor.getStreamSendWindowInBytes();
        try
        {
            int window = 2 << 20; // 2 MiB
            DatabaseDescriptor.setStreamSendWindowInBytes(window);

            EmbeddedChannel channel = new TestChannel(4);
            try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
            {
                assertEquals(window, out.streamingSendWindowHighWaterMark);
                assertEquals(window / 2, out.streamingSendWindowLowWaterMark);
                // proves we are no longer bounded by the small Netty channel default
                assertTrue("send window should exceed the channel default high water mark",
                           out.streamingSendWindowHighWaterMark > out.defaultHighWaterMark);
            }
        }
        finally
        {
            DatabaseDescriptor.setStreamSendWindowInBytes(originalWindow);
        }
    }

    @Test
    public void testSendWindowNeverBelowChannelDefault() throws IOException
    {
        // If an operator configures a tiny window, we must not regress below the channel's own
        // high water mark, otherwise a single chunk could exceed the window.
        int originalWindow = DatabaseDescriptor.getStreamSendWindowInBytes();
        try
        {
            DatabaseDescriptor.setStreamSendWindowInBytes(1024); // 1 KiB, smaller than the 64 KiB default

            EmbeddedChannel channel = new TestChannel(4);
            try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
            {
                assertEquals(out.defaultHighWaterMark, out.streamingSendWindowHighWaterMark);
            }
        }
        finally
        {
            DatabaseDescriptor.setStreamSendWindowInBytes(originalWindow);
        }
    }

    /**
     * The window has to be used, not merely computed. A TestChannel that is never drained completes the first
     * write and stalls the rest, so the writer keeps submitting until the bytes in flight reach the window and
     * then parks. How many writes it manages before parking is therefore a direct reading of the window it is
     * actually applying, and it changes if the configured window is not the one in force.
     */
    @Test
    public void testConfiguredSendWindowGovernsHowMuchIsInFlight() throws Exception
    {
        int originalWindow = DatabaseDescriptor.getStreamSendWindowInBytes();
        try
        {
            int chunk = 64 << 10;

            // The writer parks before a write whose in-flight total would pass max(low, high - chunk).
            // The first chunk always flushes, so the count is that threshold in chunks, plus one.
            //
            // No window of our own: the channel's 64 KiB high and 32 KiB low stand, the threshold is
            // max(32 KiB, 0) = 32 KiB, and one chunk of 64 KiB already passes it.
            assertEquals("with no window of our own the channel's marks should govern",
                         2, writesBeforeParking(0, chunk));

            // A 256 KiB window gives max(128 KiB, 192 KiB) = 192 KiB, which is three more chunks.
            assertEquals("the configured window should govern how much the writer keeps in flight",
                         5, writesBeforeParking(4 * chunk, chunk));
        }
        finally
        {
            DatabaseDescriptor.setStreamSendWindowInBytes(originalWindow);
        }
    }

    /** Submit fixed size writes to a channel that is never drained, and report how many land before it parks. */
    private int writesBeforeParking(int window, int chunk) throws Exception
    {
        DatabaseDescriptor.setStreamSendWindowInBytes(window);

        TestChannel channel = new TestChannel(4);
        AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel);
        AtomicInteger completed = new AtomicInteger();

        Thread writer = new Thread(() -> {
            try
            {
                StreamManager.StreamRateLimiter limiter = StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort());
                for (int i = 0; i < 16; i++)
                {
                    out.writeToChannel(supplier -> {
                        ByteBuffer buffer = supplier.get(chunk);
                        buffer.position(buffer.limit());
                        buffer.flip();
                    }, limiter);
                    completed.incrementAndGet();
                }
            }
            catch (Throwable ignored)
            {
                // the test drains the channel and interrupts; whatever falls out here is not the subject
            }
        });
        writer.setDaemon(true);
        writer.start();

        try
        {
            // let it get as far as it can, then confirm it is parked rather than merely slow
            long deadline = nanoTime() + TimeUnit.SECONDS.toNanos(30);
            int stable = -1;
            while (nanoTime() < deadline)
            {
                Thread.State state = writer.getState();
                int done = completed.get();
                if (done == stable && (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING))
                    return done;
                stable = done;
                Thread.sleep(50);
            }
            throw new AssertionError("the writer never parked; it completed " + completed.get() + " writes");
        }
        finally
        {
            while (channel.readOutbound() != null)
            {
                // drain, so the parked writer can finish and the thread can exit
            }
            writer.interrupt();
            writer.join(TimeUnit.SECONDS.toMillis(10));
            out.discard();
        }
    }

    /**
     * The window is derived from stream_send_window and the channel's own water marks, and the interesting
     * values are the ones near where those two meet.
     */
    @Test
    public void testSendWindowBoundaries() throws IOException
    {
        int originalWindow = DatabaseDescriptor.getStreamSendWindowInBytes();
        try
        {
            EmbeddedChannel probe = new TestChannel(4);
            int channelHigh;
            int channelLow;
            try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(probe))
            {
                channelHigh = out.defaultHighWaterMark;
                channelLow = out.defaultLowWaterMark;
            }

            // zero: the channel's own marks stand
            assertWindow(0, channelHigh, channelLow);
            // exactly the channel's high mark: same marks again, and no regression below them
            assertWindow(channelHigh, channelHigh, channelLow);
            // between the low and high marks: still clamped up to the channel's
            assertWindow(channelHigh / 2, channelHigh, channelLow);
            // above: the configured window, with the low mark at half of it
            assertWindow(4 * channelHigh, 4 * channelHigh, 2 * channelHigh);
        }
        finally
        {
            DatabaseDescriptor.setStreamSendWindowInBytes(originalWindow);
        }
    }

    private void assertWindow(int configured, int expectedHigh, int expectedLow) throws IOException
    {
        DatabaseDescriptor.setStreamSendWindowInBytes(configured);
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(new TestChannel(4)))
        {
            assertEquals("high water mark for a window of " + configured, expectedHigh, out.streamingSendWindowHighWaterMark);
            assertEquals("low water mark for a window of " + configured, expectedLow, out.streamingSendWindowLowWaterMark);
            assertTrue("the window must never drop below the channel's own high water mark",
                       out.streamingSendWindowHighWaterMark >= out.defaultHighWaterMark);
            assertTrue("the low mark must never drop below the channel's own",
                       out.streamingSendWindowLowWaterMark >= out.defaultLowWaterMark);
        }
    }

    @Test
    public void testWriteFileToChannelEntireSSTableNoThrottling() throws IOException
    {
        // Disable throttling by setting entire SSTable throughput and entire SSTable inter-DC throughput to 0
        DatabaseDescriptor.setEntireSSTableStreamThroughputOutboundMebibytesPerSec(0);
        DatabaseDescriptor.setEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec(0);
        StreamManager.StreamRateLimiter.updateEntireSSTableThroughput();
        StreamManager.StreamRateLimiter.updateEntireSSTableInterDCThroughput();

        testWriteFileToChannel(true);
    }

    @Test
    public void testWriteFileToChannelEntireSSTable() throws IOException
    {
        // Enable entire SSTable throttling by setting it to 200 Mbps
        DatabaseDescriptor.setEntireSSTableStreamThroughputOutboundMebibytesPerSec(200);
        DatabaseDescriptor.setEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec(200);
        StreamManager.StreamRateLimiter.updateEntireSSTableThroughput();
        StreamManager.StreamRateLimiter.updateEntireSSTableInterDCThroughput();

        testWriteFileToChannel(true);
    }

    @Test
    public void testWriteFileToChannelSSL() throws IOException
    {
        testWriteFileToChannel(false);
    }

    private void testWriteFileToChannel(boolean zeroCopy) throws IOException
    {
        File file = populateTempData("zero_copy_" + zeroCopy);
        int length = (int) file.length();

        EmbeddedChannel channel = new TestChannel(4);
        StreamManager.StreamRateLimiter limiter = zeroCopy ? StreamManager.getEntireSSTableRateLimiter(FBUtilities.getBroadcastAddressAndPort())
                                                           : StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort());

        try (FileChannel fileChannel = file.newReadChannel();
             AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            assertTrue(fileChannel.isOpen());

            if (zeroCopy)
                out.writeFileToChannelZeroCopy(fileChannel, limiter, length, length, length * 2);
            else
                out.writeFileToChannel(fileChannel, limiter, length);

            assertEquals(length, out.flushed());
            assertEquals(length, out.flushedToNetwork());
            assertEquals(length, out.position());

            assertFalse(fileChannel.isOpen());
        }
    }

    private File populateTempData(String name) throws IOException
    {
        File file = new File(Files.createTempFile(name, ".txt"));
        file.deleteOnExit();

        Random r = new Random();
        byte [] content = new byte[16];
        r.nextBytes(content);
        Files.write(file.toPath(), content);

        return file;
    }
}
