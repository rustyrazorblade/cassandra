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
package org.apache.cassandra.io.compress;

import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.junit.Test;

import org.apache.cassandra.concurrent.NamedThreadFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class DeflateCompressorTest
{
    /**
     * Each thread that compresses gets a native zlib stream. A short-lived thread, such as an async
     * compaction writer thread, must free it when the thread exits rather than leave it to the GC.
     */
    @Test
    public void threadExitEndsTheThreadsStreams() throws Exception
    {
        AtomicReference<Deflater> deflater = new AtomicReference<>();
        AtomicReference<Inflater> inflater = new AtomicReference<>();
        Thread thread = NamedThreadFactory.createThread(() -> {
            deflater.set(DeflateCompressor.instance.threadDeflater());
            inflater.set(DeflateCompressor.instance.threadInflater());
        }, "deflate-test");
        thread.start();
        thread.join();

        assertNotNull(deflater.get());
        assertNotNull(inflater.get());
        assertEnded("Deflater", () -> deflater.get().getBytesRead());
        assertEnded("Inflater", () -> inflater.get().getBytesRead());
    }

    /**
     * Checks that the stream was ended. Every JDK fails a call on an ended stream with the
     * message "Deflater has been closed" (or "Inflater ..."). JDK 25 changed the exception from
     * NullPointerException to IllegalStateException when Deflater and Inflater became AutoCloseable.
     */
    private static void assertEnded(String name, Runnable call)
    {
        Class<? extends RuntimeException> expected = Runtime.version().feature() >= 25
                                                     ? IllegalStateException.class
                                                     : NullPointerException.class;
        try
        {
            call.run();
        }
        catch (RuntimeException e)
        {
            assertEquals(expected, e.getClass());
            assertEquals(name + " has been closed", e.getMessage());
            return;
        }
        fail("the thread's " + name + " was not ended when the thread exited");
    }
}
