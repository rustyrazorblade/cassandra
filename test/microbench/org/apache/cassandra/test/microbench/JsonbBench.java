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

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.marshal.jsonb.JsonbNative;

/**
 * Benchmark comparing native JSONB (Rust + Panama FFM) vs Jackson (pure JVM).
 *
 * This benchmark tests the performance difference between:
 * - Native JSONB: Rust-based JSON processing via Panama Foreign Function & Memory API
 * - Jackson: Pure Java JSON library (com.fasterxml.jackson.databind)
 *
 * Operations tested:
 * - Parse: JSON text to binary representation
 * - Serialize: Binary to JSON text
 * - Field access: Extract a field from an object
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@State(Scope.Benchmark)
public class JsonbBench
{
    // Small JSON document (~100 bytes)
    private static final String SMALL_JSON =
        "{\"id\":123,\"name\":\"Alice\",\"active\":true,\"score\":95.5}";

    // Medium JSON document (~500 bytes)
    private static final String MEDIUM_JSON =
        "{\"userId\":\"user_12345\"," +
        "\"profile\":{\"name\":\"Bob Smith\",\"email\":\"bob@example.com\",\"age\":30}," +
        "\"preferences\":{\"theme\":\"dark\",\"language\":\"en\",\"notifications\":true}," +
        "\"activity\":{\"lastLogin\":\"2024-09-16T10:30:00Z\",\"loginCount\":42,\"posts\":128}," +
        "\"tags\":[\"premium\",\"verified\",\"developer\"]}";

    @Param({"small", "medium"})
    String documentSize;

    private String jsonText;
    private ByteBuffer nativeJsonb;
    private JsonNode jacksonNode;
    private ObjectMapper mapper;
    private JsonbNative jsonbNative;

    @Setup(Level.Trial)
    public void setup() throws Exception
    {
        jsonText = documentSize.equals("small") ? SMALL_JSON : MEDIUM_JSON;
        mapper = new ObjectMapper();

        // Initialize native library (triggers lazy loading)
        jsonbNative = JsonbNative.getInstance();

        // Pre-parse for serialize and field access benchmarks
        nativeJsonb = jsonbNative.fromText(jsonText);
        jacksonNode = mapper.readTree(jsonText);
    }

    // ========== Parse Benchmarks ==========

    @Benchmark
    public ByteBuffer nativeParse() throws Exception
    {
        return jsonbNative.fromText(jsonText);
    }

    @Benchmark
    public JsonNode jacksonParse() throws Exception
    {
        return mapper.readTree(jsonText);
    }

    // ========== Serialize Benchmarks ==========

    @Benchmark
    public String nativeSerialize() throws Exception
    {
        return jsonbNative.toText(nativeJsonb);
    }

    @Benchmark
    public String jacksonSerialize() throws Exception
    {
        return mapper.writeValueAsString(jacksonNode);
    }

    // ========== Field Access Benchmarks ==========

    @Benchmark
    public ByteBuffer nativeFieldAccess() throws Exception
    {
        // Access a top-level field that exists in both documents
        return jsonbNative.getByKey(nativeJsonb, documentSize.equals("small") ? "name" : "userId");
    }

    @Benchmark
    public JsonNode jacksonFieldAccess() throws Exception
    {
        // Access a top-level field that exists in both documents
        return documentSize.equals("small") ? jacksonNode.get("name") : jacksonNode.get("userId");
    }
}
