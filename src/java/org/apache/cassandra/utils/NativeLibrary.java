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

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.jna.LastErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;

import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORE_MISSING_NATIVE_FILE_HINTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.OS_ARCH;
import static org.apache.cassandra.config.CassandraRelevantProperties.OS_NAME;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;
import static org.apache.cassandra.utils.NativeLibrary.OSType.AIX;
import static org.apache.cassandra.utils.NativeLibrary.OSType.LINUX;
import static org.apache.cassandra.utils.NativeLibrary.OSType.MAC;

public final class NativeLibrary
{
    private static final Logger logger = LoggerFactory.getLogger(NativeLibrary.class);
    private static final boolean REQUIRE = !IGNORE_MISSING_NATIVE_FILE_HINTS.getBoolean();

    public enum OSType
    {
        LINUX,
        MAC,
        AIX,
        OTHER;
    }

    public static final OSType osType;

    private static final int MCL_CURRENT;
    private static final int MCL_FUTURE;

    private static final int ENOMEM = 12;

    private static final int F_GETFL   = 3;  /* get file status flags */
    private static final int F_SETFL   = 4;  /* set file status flags */
    private static final int F_NOCACHE = 48; /* Mac OS X specific flag, turns cache on/off */
    private static final int O_DIRECT  = 040000; /* fcntl.h */
    private static final int O_RDONLY  = 00000000; /* fcntl.h */

    private static final int POSIX_FADV_NORMAL     = 0; /* fadvise.h */
    private static final int POSIX_FADV_RANDOM     = 1; /* fadvise.h */
    private static final int POSIX_FADV_SEQUENTIAL = 2; /* fadvise.h */
    private static final int POSIX_FADV_WILLNEED   = 3; /* fadvise.h */
    private static final int POSIX_FADV_DONTNEED   = 4; /* fadvise.h */
    private static final int POSIX_FADV_NOREUSE    = 5; /* fadvise.h */

    private static final int SYNC_FILE_RANGE_WRITE = 2; /* fs.h */

    private static final NativeLibraryWrapper wrappedLibrary;
    private static boolean jnaLockable = false;

    // Logs once if the sync_file_range writeback hint is unavailable, so the feature going silently off is visible.
    private static final AtomicBoolean syncFileRangeUnavailableLogged = new AtomicBoolean(false);

    private static final Field FILE_DESCRIPTOR_FD_FIELD;
    private static final Field FILE_CHANNEL_FD_FIELD;

    static
    {
        FILE_DESCRIPTOR_FD_FIELD = FBUtilities.getProtectedField(FileDescriptor.class, "fd");
        try
        {
            FILE_CHANNEL_FD_FIELD = FBUtilities.getProtectedField(Class.forName("sun.nio.ch.FileChannelImpl"), "fd");
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        // detect the OS type the JVM is running on and then set the CLibraryWrapper
        // instance to a compatable implementation of CLibraryWrapper for that OS type
        osType = getOsType();
        switch (osType)
        {
            case MAC: wrappedLibrary = new NativeLibraryDarwin(); break;
            case LINUX:
            case AIX:
            case OTHER:
            default: wrappedLibrary = new NativeLibraryLinux();
        }

        if (toLowerCaseLocalized(OS_ARCH.getString()).contains("ppc"))
        {
            if (osType == LINUX)
            {
               MCL_CURRENT = 0x2000;
               MCL_FUTURE = 0x4000;
            }
            else if (osType == AIX)
            {
                MCL_CURRENT = 0x100;
                MCL_FUTURE = 0x200;
            }
            else
            {
                MCL_CURRENT = 1;
                MCL_FUTURE = 2;
            }
        }
        else
        {
            MCL_CURRENT = 1;
            MCL_FUTURE = 2;
        }
    }

    private NativeLibrary() {}

    /**
     * @return the detected OSType of the Operating System running the JVM using crude string matching
     */
    private static OSType getOsType()
    {
        String osName = toLowerCaseLocalized(OS_NAME.getString());
        if  (osName.contains("linux"))
            return LINUX;
        else if (osName.contains("mac"))
            return MAC;

        logger.warn("the current operating system, {}, is unsupported by Cassandra", osName);
        if (osName.contains("aix"))
            return AIX;
        else
            // fall back to the Linux impl for all unknown OS types until otherwise implicitly supported as needed
            return LINUX;
    }

    private static int errno(RuntimeException e)
    {
        assert e instanceof LastErrorException;
        try
        {
            return ((LastErrorException) e).getErrorCode();
        }
        catch (NoSuchMethodError x)
        {
            if (REQUIRE)
                logger.warn("Obsolete version of JNA present; unable to read errno. Upgrade to JNA 3.2.7 or later");
            return 0;
        }
    }

    /**
     * Checks if the library has been successfully linked.
     * @return {@code true} if the library has been successfully linked, {@code false} otherwise.
     */
    public static boolean isAvailable()
    {
        return wrappedLibrary.isAvailable();
    }

    public static boolean jnaMemoryLockable()
    {
        return jnaLockable;
    }

    public static void tryMlockall()
    {
        try
        {
            wrappedLibrary.callMlockall(MCL_CURRENT);
            jnaLockable = true;
            logger.info("JNA mlockall successful");
        }
        catch (UnsatisfiedLinkError e)
        {
            // this will have already been logged by CLibrary, no need to repeat it
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            if (errno(e) == ENOMEM && osType == LINUX)
            {
                logger.warn("Unable to lock JVM memory (ENOMEM)."
                        + " This can result in part of the JVM being swapped out, especially with mmapped I/O enabled."
                        + " Increase RLIMIT_MEMLOCK.");
            }
            else if (osType != MAC)
            {
                // OS X allows mlockall to be called, but always returns an error
                logger.warn("Unknown mlockall error {}", errno(e));
            }
        }
    }

    public static void trySkipCache(String path, long offset, long len)
    {
        File f = new File(path);
        if (!f.exists())
            return;

        try (FileInputStreamPlus fis = new FileInputStreamPlus(f))
        {
            trySkipCache(getfd(fis.getChannel()), offset, len, path);
        }
        catch (IOException e)
        {
            logger.warn("Could not skip cache", e);
        }
    }

    public static void trySkipCache(int fd, long offset, long len, String path)
    {
        if (len == 0)
            trySkipCache(fd, 0, 0, path);

        while (len > 0)
        {
            int sublen = (int) Math.min(Integer.MAX_VALUE, len);
            trySkipCache(fd, offset, sublen, path);
            len -= sublen;
            offset -= sublen;
        }
    }

    public static void trySkipCache(int fd, long offset, int len, String path)
    {
        if (fd < 0)
            return;

        try
        {
            if (osType == LINUX)
            {
                int result = wrappedLibrary.callPosixFadvise(fd, offset, len, POSIX_FADV_DONTNEED);
                if (result != 0)
                    NoSpamLogger.log(
                            logger,
                            NoSpamLogger.Level.WARN,
                            10,
                            TimeUnit.MINUTES,
                            "Failed trySkipCache on file: {} Error: " + wrappedLibrary.callStrerror(result).getString(0),
                            path);
            }
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping Direct I/O
            // instance of this class will act like normal RandomAccessFile
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn("posix_fadvise({}, {}) failed, errno ({}).", fd, offset, errno(e));
        }
    }

    /**
     * Starts kernel writeback of a byte range without the durability or metadata cost of fsync.
     *
     * Uses {@code SYNC_FILE_RANGE_WRITE} alone: it queues the range's dirty pages for writeout and
     * returns. It does not wait for that writeout, and it makes no durability guarantee; a later fsync
     * still owns durability. Its purpose is to keep the dirty-page backlog bounded so that final fsync
     * is cheap. Callers pass strictly increasing, non-overlapping ranges, so the wait-before flag would
     * have nothing in flight to wait on and is left off.
     *
     * Linux only. On any other OS the call is a no-op, because the syscall does not exist there.
     */
    public static void trySyncFileRange(int fd, long offset, long nbytes)
    {
        if (fd < 0 || osType != LINUX)
            return;

        try
        {
            wrappedLibrary.callSyncFileRange(fd, offset, nbytes, SYNC_FILE_RANGE_WRITE);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA unavailable, or an OS without the syscall: the write path still works without the hint.
            // Log once so the feature going silently off is visible without spamming the hot path.
            if (syncFileRangeUnavailableLogged.compareAndSet(false, true))
                logger.info("sync_file_range is unavailable ({}); writeback hints are off and the commit fsync owns durability",
                            e.getMessage());
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;
            // A failed hint is not a durability fault; the final fsync still runs. But a hint that fails
            // every interval (a kernel or filesystem without sync_file_range, or a disk writeback error)
            // makes this feature silently a no-op, so warn -- rate-limited, since this is a hot path.
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 10, TimeUnit.MINUTES,
                             "sync_file_range writeback hint failed (fd={}, errno={}); the commit fsync still owns durability",
                             fd, errno(e));
        }
    }

    public static int tryFcntl(int fd, int command, int flags)
    {
        // fcntl return value may or may not be useful, depending on the command
        int result = -1;

        try
        {
            result = wrappedLibrary.callFcntl(fd, command, flags);
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            if (REQUIRE)
                logger.warn("fcntl({}, {}, {}) failed, errno ({}).", fd, command, flags, errno(e));
        }

        return result;
    }

    public static int tryOpenDirectory(String path)
    {
        int fd = -1;

        try
        {
            return wrappedLibrary.callOpen(path, O_RDONLY);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            if (REQUIRE)
                logger.warn("open({}, O_RDONLY) failed, errno ({}).", path, errno(e));
        }

        return fd;
    }

    public static void trySync(int fd)
    {
        if (fd == -1)
            return;

        try
        {
            wrappedLibrary.callFsync(fd);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            if (REQUIRE)
            {
                String errMsg = String.format("fsync(%s) failed, errno (%s) %s", fd, errno(e), e.getMessage());
                logger.warn(errMsg);
                throw new FSWriteError(e, errMsg);
            }
        }
    }

    public static void tryCloseFD(int fd)
    {
        if (fd == -1)
            return;

        try
        {
            wrappedLibrary.callClose(fd);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            if (REQUIRE)
            {
                String errMsg = String.format("close(%d) failed, errno (%d).", fd, errno(e));
                logger.warn(errMsg);
                throw new FSWriteError(e, errMsg);
            }
        }
    }

    public static int getfd(FileChannel channel)
    {
        try
        {
            return getfd((FileDescriptor)FILE_CHANNEL_FD_FIELD.get(channel));
        }
        catch (IllegalArgumentException|IllegalAccessException e)
        {
            if (REQUIRE)
                logger.warn("Unable to read fd field from FileChannel", e);
        }
        return -1;
    }

    /**
     * Get system file descriptor from FileDescriptor object.
     * @param descriptor - FileDescriptor objec to get fd from
     * @return file descriptor, -1 or error
     */
    public static int getfd(FileDescriptor descriptor)
    {
        try
        {
            return FILE_DESCRIPTOR_FD_FIELD.getInt(descriptor);
        }
        catch (Exception e)
        {
            if (REQUIRE)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.warn("Unable to read fd field from FileDescriptor", e);
            }
        }

        return -1;
    }

    /**
     * @return the PID of the JVM or -1 if we failed to get the PID
     */
    public static long getProcessID()
    {
        try
        {
            return wrappedLibrary.callGetpid();
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping
        }
        catch (Exception e)
        {
            if (REQUIRE)
                logger.info("Failed to get PID from JNA", e);
        }

        return -1;
    }

    public static boolean isEnabled()
    {
        return REQUIRE;
    }
}
