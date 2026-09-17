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
package org.apache.cassandra.db.marshal.jsonb;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.serializers.MarshalException;

/**
 * FFM binding to the native JSONB library. Loads the library lazily on first use.
 */
public class JsonbNative
{
    private static final Logger logger = LoggerFactory.getLogger(JsonbNative.class);

    // Status codes from Rust
    private static final int STATUS_OK = 0;
    private static final int STATUS_INVALID_INPUT = -1;
    private static final int STATUS_INVALID_ARG = -6;
    private static final int STATUS_PANIC = -5;
    private static final int STATUS_NOT_FOUND = -3;

    // Input limits
    private static final int MAX_INPUT_BYTES = CassandraRelevantProperties.CASSANDRA_JSONB_MAX_INPUT_BYTES.getInt();
    private static final int MAX_NESTING_DEPTH = CassandraRelevantProperties.CASSANDRA_JSONB_MAX_NESTING_DEPTH.getInt();

    // Lazy initialization holder
    private static class Holder
    {
        static final JsonbNative INSTANCE;

        static
        {
            try
            {
                INSTANCE = new JsonbNative();
            }
            catch (Throwable t)
            {
                logger.error("Failed to initialize JSONB native library", t);
                throw new ExceptionInInitializerError(t);
            }
        }
    }

    private final MethodHandle fromTextHandle;
    private final MethodHandle toTextHandle;
    private final MethodHandle validateHandle;
    private final MethodHandle freeHandle;
    private final MethodHandle getByKeyHandle;
    private final MethodHandle getByIndexHandle;
    private final MethodHandle containsHandle;
    private final MethodHandle existsKeyHandle;
    private final MethodHandle typeOfHandle;
    private final MethodHandle arrayLengthHandle;
    private final MethodHandle objectKeysHandle;

    private JsonbNative() throws Exception
    {
        String libPath = CassandraRelevantProperties.CASSANDRA_JSONB_LIBRARY.getString();
        if (libPath == null || libPath.isEmpty())
        {
            // Default to cargo output in the current working directory
            File defaultFile = new File("native/jsonb/target/release/libcassandra_jsonb.dylib");
            if (!defaultFile.exists())
            {
                // Try .so for Linux
                defaultFile = new File("native/jsonb/target/release/libcassandra_jsonb.so");
            }
            libPath = defaultFile.absolutePath();
        }

        logger.info("Loading JSONB native library from: {}", libPath);
        File libFile = new File(libPath);
        if (!libFile.exists())
        {
            String error = "JSONB native library not found at: " + libPath;
            logger.error(error);
            throw new UnsatisfiedLinkError(error);
        }

        try
        {
            System.load(libPath);
        }
        catch (Throwable t)
        {
            logger.error("Failed to load JSONB native library from: {}", libPath, t);
            throw t;
        }

        Path path = libFile.toPath();

        Linker linker = Linker.nativeLinker();
        SymbolLookup lookup = SymbolLookup.libraryLookup(path, Arena.global());

        // Helper to find symbols with clear error messages
        java.util.function.Function<String, MemorySegment> findSymbol = name ->
            lookup.find(name).orElseThrow(() -> new UnsatisfiedLinkError("Symbol not found in JSONB library: " + name));

        // jsonb_from_text(in_ptr: *const u8, in_len: usize, out_ptr: *mut *mut u8, out_len: *mut usize) -> i32
        fromTextHandle = linker.downcallHandle(
            findSymbol.apply("jsonb_from_text"),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.ADDRESS)
        );

        // jsonb_to_text(in_ptr: *const u8, in_len: usize, out_ptr: *mut *mut u8, out_len: *mut usize) -> i32
        toTextHandle = linker.downcallHandle(
            lookup.find("jsonb_to_text").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.ADDRESS)
        );

        // jsonb_validate(in_ptr: *const u8, in_len: usize) -> i32
        validateHandle = linker.downcallHandle(
            lookup.find("jsonb_validate").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG)
        );

        // jsonb_free(ptr: *mut u8, len: usize)
        freeHandle = linker.downcallHandle(
            lookup.find("jsonb_free").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // jsonb_get_by_key(in_ptr, in_len, key_ptr, key_len, out_ptr, out_len) -> i32
        getByKeyHandle = linker.downcallHandle(
            lookup.find("jsonb_get_by_key").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.ADDRESS)
        );

        // jsonb_get_by_index(in_ptr, in_len, index, out_ptr, out_len) -> i32
        getByIndexHandle = linker.downcallHandle(
            lookup.find("jsonb_get_by_index").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.ADDRESS)
        );

        // jsonb_contains(a_ptr, a_len, b_ptr, b_len, out_result) -> i32
        containsHandle = linker.downcallHandle(
            lookup.find("jsonb_contains").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS)
        );

        // jsonb_exists_key(in_ptr, in_len, key_ptr, key_len, out_result) -> i32
        existsKeyHandle = linker.downcallHandle(
            lookup.find("jsonb_exists_key").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS)
        );

        // jsonb_type_of(in_ptr, in_len, out_ptr, out_len) -> i32
        typeOfHandle = linker.downcallHandle(
            lookup.find("jsonb_type_of").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.ADDRESS)
        );

        // jsonb_array_length(in_ptr, in_len, out_length) -> i32
        arrayLengthHandle = linker.downcallHandle(
            lookup.find("jsonb_array_length").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS)
        );

        // jsonb_object_keys(in_ptr, in_len, out_ptr, out_len) -> i32
        objectKeysHandle = linker.downcallHandle(
            lookup.find("jsonb_object_keys").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.JAVA_LONG,
                                 ValueLayout.ADDRESS,
                                 ValueLayout.ADDRESS)
        );

        logger.info("JSONB native library loaded successfully");
    }

    /**
     * Validate input length before calling native code.
     */
    private static void validateInputLength(int length) throws MarshalException
    {
        if (length > MAX_INPUT_BYTES)
        {
            throw new MarshalException(String.format("JSONB input exceeds maximum length of %d bytes (got %d)",
                                                     MAX_INPUT_BYTES, length));
        }
    }

    /**
     * Get the singleton instance. Triggers lazy library loading on first call.
     */
    public static JsonbNative getInstance()
    {
        return Holder.INSTANCE;
    }

    /**
     * Parse JSON text to binary JSONB.
     */
    public ByteBuffer fromText(String text) throws MarshalException
    {
        byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);
        validateInputLength(textBytes.length);

        MemorySegment resultPtr = null;
        long resultLength = 0;

        try (Arena arena = Arena.ofConfined())
        {
            // Copy input into arena
            MemorySegment in = arena.allocate(textBytes.length);
            MemorySegment.copy(textBytes, 0, in, ValueLayout.JAVA_BYTE, 0, textBytes.length);

            // Allocate out-parameters
            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);

            // Call native function
            int status = (int) fromTextHandle.invokeExact(in, (long) textBytes.length, outPtr, outLen);

            if (status != STATUS_OK)
            {
                if (status == STATUS_PANIC)
                {
                    logger.warn("JSONB native panic in fromText operation");
                }
                throw new MarshalException("Failed to parse JSON text: " + getStatusMessage(status));
            }

            // Extract result and capture for cleanup
            resultPtr = outPtr.get(ValueLayout.ADDRESS, 0);
            resultLength = outLen.get(ValueLayout.JAVA_LONG, 0);

            if (resultPtr.address() == 0 || resultLength == 0)
            {
                resultPtr = null;  // Clear so finally doesn't try to free
                throw new MarshalException("Native function returned null result");
            }

            // Reinterpret with correct length
            resultPtr = resultPtr.reinterpret(resultLength);

            // Copy to heap
            byte[] bytes = resultPtr.toArray(ValueLayout.JAVA_BYTE);

            return ByteBuffer.wrap(bytes);
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
        finally
        {
            if (resultPtr != null && resultLength > 0)
            {
                try
                {
                    freeHandle.invokeExact(resultPtr, resultLength);
                }
                catch (Throwable t)
                {
                    logger.warn("Failed to free native buffer in fromText", t);
                }
            }
        }
    }

    /**
     * Render binary JSONB to JSON text.
     */
    public String toText(ByteBuffer buffer) throws MarshalException
    {
        // Extract bytes from buffer (handle direct, sliced, and heap buffers)
        byte[] bytes;
        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.remaining() == buffer.array().length)
        {
            bytes = buffer.array();
        }
        else
        {
            bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
        }

        MemorySegment resultPtr = null;
        long resultLength = 0;

        try (Arena arena = Arena.ofConfined())
        {
            // Copy input into arena
            MemorySegment in = arena.allocate(bytes.length);
            MemorySegment.copy(bytes, 0, in, ValueLayout.JAVA_BYTE, 0, bytes.length);

            // Allocate out-parameters
            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);

            // Call native function
            int status = (int) toTextHandle.invokeExact(in, (long) bytes.length, outPtr, outLen);

            if (status != STATUS_OK)
            {
                throw new MarshalException("Failed to render JSONB to text: " + getStatusMessage(status));
            }

            // Extract result and capture for cleanup
            resultPtr = outPtr.get(ValueLayout.ADDRESS, 0);
            resultLength = outLen.get(ValueLayout.JAVA_LONG, 0);

            if (resultPtr.address() == 0 || resultLength == 0)
            {
                resultPtr = null;  // Clear so finally doesn't try to free
                throw new MarshalException("Native function returned null result");
            }

            // Reinterpret with correct length
            resultPtr = resultPtr.reinterpret(resultLength);

            // Copy to heap
            byte[] textBytes = resultPtr.toArray(ValueLayout.JAVA_BYTE);

            return new String(textBytes, StandardCharsets.UTF_8);
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
        finally
        {
            if (resultPtr != null && resultLength > 0)
            {
                try
                {
                    freeHandle.invokeExact(resultPtr, resultLength);
                }
                catch (Throwable t)
                {
                    logger.warn("Failed to free native buffer in toText", t);
                }
            }
        }
    }

    /**
     * Validate binary JSONB.
     */
    public void validate(ByteBuffer buffer) throws MarshalException
    {
        // Extract bytes from buffer
        byte[] bytes;
        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.remaining() == buffer.array().length)
        {
            bytes = buffer.array();
        }
        else
        {
            bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
        }

        try (Arena arena = Arena.ofConfined())
        {
            // Copy input into arena
            MemorySegment in = arena.allocate(bytes.length);
            MemorySegment.copy(bytes, 0, in, ValueLayout.JAVA_BYTE, 0, bytes.length);

            // Call native function
            int status = (int) validateHandle.invokeExact(in, (long) bytes.length);

            if (status != STATUS_OK)
            {
                throw new MarshalException("Invalid JSONB binary: " + getStatusMessage(status));
            }
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
    }

    /**
     * Get a JSONB value by key from an object.
     */
    public ByteBuffer getByKey(ByteBuffer buffer, String key) throws MarshalException
    {
        byte[] bytes = extractBytes(buffer);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        MemorySegment resultPtr = null;
        long resultLength = 0;

        try (Arena arena = Arena.ofConfined())
        {
            MemorySegment in = arena.allocate(bytes.length);
            MemorySegment.copy(bytes, 0, in, ValueLayout.JAVA_BYTE, 0, bytes.length);

            MemorySegment keyIn = arena.allocate(keyBytes.length);
            MemorySegment.copy(keyBytes, 0, keyIn, ValueLayout.JAVA_BYTE, 0, keyBytes.length);

            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);

            int status = (int) getByKeyHandle.invokeExact(in, (long) bytes.length, keyIn, (long) keyBytes.length, outPtr, outLen);

            if (status == STATUS_NOT_FOUND)
            {
                return null;
            }

            if (status != STATUS_OK)
            {
                throw new MarshalException("Failed to get key: " + getStatusMessage(status));
            }

            resultPtr = outPtr.get(ValueLayout.ADDRESS, 0);
            resultLength = outLen.get(ValueLayout.JAVA_LONG, 0);

            if (resultPtr.address() == 0 || resultLength == 0)
            {
                resultPtr = null;
                throw new MarshalException("Native function returned null result");
            }

            resultPtr = resultPtr.reinterpret(resultLength);
            byte[] resultBytes = resultPtr.toArray(ValueLayout.JAVA_BYTE);

            return ByteBuffer.wrap(resultBytes);
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
        finally
        {
            if (resultPtr != null && resultLength > 0)
            {
                try
                {
                    freeHandle.invokeExact(resultPtr, resultLength);
                }
                catch (Throwable t)
                {
                    logger.warn("Failed to free native buffer in getByKey", t);
                }
            }
        }
    }

    /**
     * Get element from array by index.
     */
    public ByteBuffer getByIndex(ByteBuffer buffer, int index) throws MarshalException
    {
        byte[] bytes = extractBytes(buffer);

        MemorySegment resultPtr = null;
        long resultLength = 0;

        try (Arena arena = Arena.ofConfined())
        {
            MemorySegment in = arena.allocate(bytes.length);
            MemorySegment.copy(bytes, 0, in, ValueLayout.JAVA_BYTE, 0, bytes.length);

            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);

            int status = (int) getByIndexHandle.invokeExact(in, (long) bytes.length, index, outPtr, outLen);

            if (status == STATUS_NOT_FOUND)
            {
                return null;
            }

            if (status != STATUS_OK)
            {
                throw new MarshalException("Failed to get index: " + getStatusMessage(status));
            }

            resultPtr = outPtr.get(ValueLayout.ADDRESS, 0);
            resultLength = outLen.get(ValueLayout.JAVA_LONG, 0);

            if (resultPtr.address() == 0 || resultLength == 0)
            {
                resultPtr = null;
                throw new MarshalException("Native function returned null result");
            }

            resultPtr = resultPtr.reinterpret(resultLength);
            byte[] resultBytes = resultPtr.toArray(ValueLayout.JAVA_BYTE);

            return ByteBuffer.wrap(resultBytes);
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
        finally
        {
            if (resultPtr != null && resultLength > 0)
            {
                try
                {
                    freeHandle.invokeExact(resultPtr, resultLength);
                }
                catch (Throwable t)
                {
                    logger.warn("Failed to free native buffer in getByIndex", t);
                }
            }
        }
    }

    /**
     * Check if a contains b.
     */
    public boolean contains(ByteBuffer a, ByteBuffer b) throws MarshalException
    {
        byte[] aBytes = extractBytes(a);
        byte[] bBytes = extractBytes(b);

        try (Arena arena = Arena.ofConfined())
        {
            MemorySegment aIn = arena.allocate(aBytes.length);
            MemorySegment.copy(aBytes, 0, aIn, ValueLayout.JAVA_BYTE, 0, aBytes.length);

            MemorySegment bIn = arena.allocate(bBytes.length);
            MemorySegment.copy(bBytes, 0, bIn, ValueLayout.JAVA_BYTE, 0, bBytes.length);

            MemorySegment outResult = arena.allocate(ValueLayout.JAVA_BYTE);

            int status = (int) containsHandle.invokeExact(aIn, (long) aBytes.length, bIn, (long) bBytes.length, outResult);

            if (status != STATUS_OK)
            {
                throw new MarshalException("Failed to check contains: " + getStatusMessage(status));
            }

            return outResult.get(ValueLayout.JAVA_BYTE, 0) != 0;
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
    }

    /**
     * Check if key exists in object.
     */
    public boolean existsKey(ByteBuffer buffer, String key) throws MarshalException
    {
        byte[] bytes = extractBytes(buffer);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        try (Arena arena = Arena.ofConfined())
        {
            MemorySegment in = arena.allocate(bytes.length);
            MemorySegment.copy(bytes, 0, in, ValueLayout.JAVA_BYTE, 0, bytes.length);

            MemorySegment keyIn = arena.allocate(keyBytes.length);
            MemorySegment.copy(keyBytes, 0, keyIn, ValueLayout.JAVA_BYTE, 0, keyBytes.length);

            MemorySegment outResult = arena.allocate(ValueLayout.JAVA_BYTE);

            int status = (int) existsKeyHandle.invokeExact(in, (long) bytes.length, keyIn, (long) keyBytes.length, outResult);

            if (status != STATUS_OK)
            {
                throw new MarshalException("Failed to check key existence: " + getStatusMessage(status));
            }

            return outResult.get(ValueLayout.JAVA_BYTE, 0) != 0;
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
    }

    /**
     * Get JSONB type name.
     */
    public String typeOf(ByteBuffer buffer) throws MarshalException
    {
        byte[] bytes = extractBytes(buffer);

        MemorySegment resultPtr = null;
        long resultLength = 0;

        try (Arena arena = Arena.ofConfined())
        {
            MemorySegment in = arena.allocate(bytes.length);
            MemorySegment.copy(bytes, 0, in, ValueLayout.JAVA_BYTE, 0, bytes.length);

            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);

            int status = (int) typeOfHandle.invokeExact(in, (long) bytes.length, outPtr, outLen);

            if (status != STATUS_OK)
            {
                throw new MarshalException("Failed to get type: " + getStatusMessage(status));
            }

            resultPtr = outPtr.get(ValueLayout.ADDRESS, 0);
            resultLength = outLen.get(ValueLayout.JAVA_LONG, 0);

            if (resultPtr.address() == 0 || resultLength == 0)
            {
                resultPtr = null;
                throw new MarshalException("Native function returned null result");
            }

            resultPtr = resultPtr.reinterpret(resultLength);
            byte[] textBytes = resultPtr.toArray(ValueLayout.JAVA_BYTE);

            return new String(textBytes, StandardCharsets.UTF_8);
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
        finally
        {
            if (resultPtr != null && resultLength > 0)
            {
                try
                {
                    freeHandle.invokeExact(resultPtr, resultLength);
                }
                catch (Throwable t)
                {
                    logger.warn("Failed to free native buffer in typeOf", t);
                }
            }
        }
    }

    /**
     * Get array length.
     */
    public long arrayLength(ByteBuffer buffer) throws MarshalException
    {
        byte[] bytes = extractBytes(buffer);

        try (Arena arena = Arena.ofConfined())
        {
            MemorySegment in = arena.allocate(bytes.length);
            MemorySegment.copy(bytes, 0, in, ValueLayout.JAVA_BYTE, 0, bytes.length);

            MemorySegment outLength = arena.allocate(ValueLayout.JAVA_LONG);

            int status = (int) arrayLengthHandle.invokeExact(in, (long) bytes.length, outLength);

            if (status != STATUS_OK)
            {
                throw new MarshalException("Failed to get array length: " + getStatusMessage(status));
            }

            return outLength.get(ValueLayout.JAVA_LONG, 0);
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
    }

    /**
     * Get object keys as JSONB array.
     */
    public ByteBuffer objectKeys(ByteBuffer buffer) throws MarshalException
    {
        byte[] bytes = extractBytes(buffer);

        MemorySegment resultPtr = null;
        long resultLength = 0;

        try (Arena arena = Arena.ofConfined())
        {
            MemorySegment in = arena.allocate(bytes.length);
            MemorySegment.copy(bytes, 0, in, ValueLayout.JAVA_BYTE, 0, bytes.length);

            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);

            int status = (int) objectKeysHandle.invokeExact(in, (long) bytes.length, outPtr, outLen);

            if (status != STATUS_OK)
            {
                throw new MarshalException("Failed to get object keys: " + getStatusMessage(status));
            }

            resultPtr = outPtr.get(ValueLayout.ADDRESS, 0);
            resultLength = outLen.get(ValueLayout.JAVA_LONG, 0);

            if (resultPtr.address() == 0 || resultLength == 0)
            {
                resultPtr = null;
                throw new MarshalException("Native function returned null result");
            }

            resultPtr = resultPtr.reinterpret(resultLength);
            byte[] resultBytes = resultPtr.toArray(ValueLayout.JAVA_BYTE);

            return ByteBuffer.wrap(resultBytes);
        }
        catch (MarshalException e)
        {
            throw e;
        }
        catch (Error e)
        {
            // Rethrow JVM errors (OOM, StackOverflow) rather than wrapping
            logger.error("JVM error in JSONB native operation", e);
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error in JSONB native operation", t);
            throw new MarshalException("Native call failed: " + t.getMessage(), t);
        }
        finally
        {
            if (resultPtr != null && resultLength > 0)
            {
                try
                {
                    freeHandle.invokeExact(resultPtr, resultLength);
                }
                catch (Throwable t)
                {
                    logger.warn("Failed to free native buffer in objectKeys", t);
                }
            }
        }
    }

    /**
     * Helper to extract bytes from a ByteBuffer (handles direct, sliced, and heap buffers).
     */
    private byte[] extractBytes(ByteBuffer buffer) throws MarshalException
    {
        validateInputLength(buffer.remaining());

        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.remaining() == buffer.array().length)
        {
            return buffer.array();
        }
        else
        {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        }
    }

    private static String getStatusMessage(int status)
    {
        return switch (status)
        {
            case STATUS_INVALID_INPUT -> "malformed JSON or JSONB";
            case STATUS_INVALID_ARG -> "invalid argument (null pointer or bad length)";
            case STATUS_PANIC -> "native panic caught";
            case STATUS_NOT_FOUND -> "key or path not found";
            default -> "unknown error code " + status;
        };
    }
}
