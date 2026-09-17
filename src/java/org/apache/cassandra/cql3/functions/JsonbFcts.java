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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.FunctionContext;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.JsonbType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.jsonb.JsonbNative;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * JSONB functions that operate on binary JSONB values.
 */
public class JsonbFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(jsonbFromText);
        functions.add(jsonbToText);
        functions.add(jsonbGet_text);
        functions.add(jsonbGet_int);
        functions.add(jsonbGetText_text);
        functions.add(jsonbGetText_int);
        functions.add(jsonbGetKey);
        functions.add(jsonbGetIndex);
        functions.add(jsonbContains);
        functions.add(jsonbContained);
        functions.add(jsonbExists);
        functions.add(jsonbExistsAny);
        functions.add(jsonbExistsAll);
        functions.add(jsonbType);
        functions.add(jsonbArrayLength);
        functions.add(jsonbObjectKeys);
    }

    /**
     * Parse JSON text to binary JSONB.
     */
    public static final NativeScalarFunction jsonbFromText = new NativeScalarFunction("jsonb_from_text", JsonbType.instance, UTF8Type.instance)
    {
        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            String text = arguments.get(0);
            if (text == null)
                return null;

            try
            {
                return JsonbNative.getInstance().fromText(text);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to parse JSON: " + e.getMessage());
            }
        }
    };

    /**
     * Render binary JSONB to JSON text.
     */
    public static final NativeScalarFunction jsonbToText = new NativeScalarFunction("jsonb_to_text", UTF8Type.instance, JsonbType.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 1);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            if (buffer == null)
                return null;

            try
            {
                String text = JsonbNative.getInstance().toText(buffer);
                return UTF8Type.instance.decompose(text);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to render JSONB: " + e.getMessage());
            }
        }
    };

    /**
     * Polymorphic accessor: get a value by key from a JSONB object (text overload).
     */
    public static final NativeScalarFunction jsonbGet_text = new NativeScalarFunction("jsonb_get", JsonbType.instance, JsonbType.instance, UTF8Type.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 2);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            ByteBuffer keyBuffer = arguments.get(1);

            if (buffer == null || keyBuffer == null)
                return null;

            String key = UTF8Type.instance.compose(keyBuffer);

            try
            {
                return JsonbNative.getInstance().getByKey(buffer, key);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to get key: " + e.getMessage());
            }
        }
    };

    /**
     * Polymorphic accessor: get an element by index from a JSONB array (int overload).
     */
    public static final NativeScalarFunction jsonbGet_int = new NativeScalarFunction("jsonb_get", JsonbType.instance, JsonbType.instance, Int32Type.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 2);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            ByteBuffer indexBuffer = arguments.get(1);

            if (buffer == null || indexBuffer == null)
                return null;

            int index = Int32Type.instance.compose(indexBuffer);

            try
            {
                return JsonbNative.getInstance().getByIndex(buffer, index);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to get index: " + e.getMessage());
            }
        }
    };

    /**
     * Polymorphic accessor returning text: get a value by key from a JSONB object as text (text overload).
     */
    public static final NativeScalarFunction jsonbGetText_text = new NativeScalarFunction("jsonb_get_text", UTF8Type.instance, JsonbType.instance, UTF8Type.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 2);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            ByteBuffer keyBuffer = arguments.get(1);

            if (buffer == null || keyBuffer == null)
                return null;

            String key = UTF8Type.instance.compose(keyBuffer);

            try
            {
                ByteBuffer result = JsonbNative.getInstance().getByKey(buffer, key);
                if (result == null)
                    return null;

                String text = JsonbNative.getInstance().toText(result);
                return UTF8Type.instance.decompose(text);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to get key as text: " + e.getMessage());
            }
        }
    };

    /**
     * Polymorphic accessor returning text: get an element by index from a JSONB array as text (int overload).
     */
    public static final NativeScalarFunction jsonbGetText_int = new NativeScalarFunction("jsonb_get_text", UTF8Type.instance, JsonbType.instance, Int32Type.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 2);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            ByteBuffer indexBuffer = arguments.get(1);

            if (buffer == null || indexBuffer == null)
                return null;

            int index = Int32Type.instance.compose(indexBuffer);

            try
            {
                ByteBuffer result = JsonbNative.getInstance().getByIndex(buffer, index);
                if (result == null)
                    return null;

                String text = JsonbNative.getInstance().toText(result);
                return UTF8Type.instance.decompose(text);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to get index as text: " + e.getMessage());
            }
        }
    };

    /**
     * Get a value by key from a JSONB object.
     */
    public static final NativeScalarFunction jsonbGetKey = new NativeScalarFunction("jsonb_get_key", JsonbType.instance, JsonbType.instance, UTF8Type.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 2);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            ByteBuffer keyBuffer = arguments.get(1);

            if (buffer == null || keyBuffer == null)
                return null;

            String key = UTF8Type.instance.compose(keyBuffer);

            try
            {
                return JsonbNative.getInstance().getByKey(buffer, key);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to get key: " + e.getMessage());
            }
        }
    };

    /**
     * Get an element by index from a JSONB array.
     */
    public static final NativeScalarFunction jsonbGetIndex = new NativeScalarFunction("jsonb_get_index", JsonbType.instance, JsonbType.instance, Int32Type.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 2);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            ByteBuffer indexBuffer = arguments.get(1);

            if (buffer == null || indexBuffer == null)
                return null;

            int index = Int32Type.instance.compose(indexBuffer);

            try
            {
                return JsonbNative.getInstance().getByIndex(buffer, index);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to get index: " + e.getMessage());
            }
        }
    };

    /**
     * Check if JSONB value a contains b.
     */
    public static final NativeScalarFunction jsonbContains = new NativeScalarFunction("jsonb_contains", BooleanType.instance, JsonbType.instance, JsonbType.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 2);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer a = arguments.get(0);
            ByteBuffer b = arguments.get(1);

            if (a == null || b == null)
                return null;

            try
            {
                boolean result = JsonbNative.getInstance().contains(a, b);
                return BooleanType.instance.decompose(result);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to check containment: " + e.getMessage());
            }
        }
    };

    /**
     * Check if JSONB value a is contained by b (reverse of contains).
     */
    public static final NativeScalarFunction jsonbContained = new NativeScalarFunction("jsonb_contained", BooleanType.instance, JsonbType.instance, JsonbType.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 2);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer a = arguments.get(0);
            ByteBuffer b = arguments.get(1);

            if (a == null || b == null)
                return null;

            try
            {
                // Reverse: a is contained by b means b contains a
                boolean result = JsonbNative.getInstance().contains(b, a);
                return BooleanType.instance.decompose(result);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to check containment: " + e.getMessage());
            }
        }
    };

    /**
     * Check if a key exists in a JSONB object.
     */
    public static final NativeScalarFunction jsonbExists = new NativeScalarFunction("jsonb_exists", BooleanType.instance, JsonbType.instance, UTF8Type.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 2);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            ByteBuffer keyBuffer = arguments.get(1);

            if (buffer == null || keyBuffer == null)
                return null;

            String key = UTF8Type.instance.compose(keyBuffer);

            try
            {
                boolean result = JsonbNative.getInstance().existsKey(buffer, key);
                return BooleanType.instance.decompose(result);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to check key existence: " + e.getMessage());
            }
        }
    };

    /**
     * Check if ANY of the listed keys exist at the top level of a JSONB object.
     */
    public static final NativeScalarFunction jsonbExistsAny = new NativeScalarFunction("jsonb_exists_any", BooleanType.instance, JsonbType.instance, ListType.getInstance(UTF8Type.instance, false))
    {
        private static final int MAX_KEY_LIST_LENGTH = CassandraRelevantProperties.CASSANDRA_JSONB_MAX_KEY_LIST_LENGTH.getInt();

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            List<String> keys = arguments.get(1);

            if (buffer == null || keys == null)
                return null;

            if (keys.isEmpty())
                return BooleanType.instance.decompose(false);

            if (keys.size() > MAX_KEY_LIST_LENGTH)
                throw new InvalidRequestException(String.format("Key list exceeds maximum length of %d (got %d)",
                                                               MAX_KEY_LIST_LENGTH, keys.size()));

            try
            {
                for (String key : keys)
                {
                    if (key == null)
                        throw new InvalidRequestException("Key list contains null element");

                    if (JsonbNative.getInstance().existsKey(buffer, key))
                    {
                        return BooleanType.instance.decompose(true);
                    }
                }
                return BooleanType.instance.decompose(false);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to check key existence: " + e.getMessage());
            }
        }
    };

    /**
     * Check if ALL of the listed keys exist at the top level of a JSONB object.
     */
    public static final NativeScalarFunction jsonbExistsAll = new NativeScalarFunction("jsonb_exists_all", BooleanType.instance, JsonbType.instance, ListType.getInstance(UTF8Type.instance, false))
    {
        private static final int MAX_KEY_LIST_LENGTH = CassandraRelevantProperties.CASSANDRA_JSONB_MAX_KEY_LIST_LENGTH.getInt();

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);
            List<String> keys = arguments.get(1);

            if (buffer == null || keys == null)
                return null;

            if (keys.isEmpty())
                return BooleanType.instance.decompose(true);

            if (keys.size() > MAX_KEY_LIST_LENGTH)
                throw new InvalidRequestException(String.format("Key list exceeds maximum length of %d (got %d)",
                                                               MAX_KEY_LIST_LENGTH, keys.size()));

            try
            {
                for (String key : keys)
                {
                    if (key == null)
                        throw new InvalidRequestException("Key list contains null element");

                    if (!JsonbNative.getInstance().existsKey(buffer, key))
                    {
                        return BooleanType.instance.decompose(false);
                    }
                }
                return BooleanType.instance.decompose(true);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to check key existence: " + e.getMessage());
            }
        }
    };

    /**
     * Get the JSON type name of a JSONB value.
     */
    public static final NativeScalarFunction jsonbType = new NativeScalarFunction("jsonb_type", UTF8Type.instance, JsonbType.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 1);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);

            if (buffer == null)
                return null;

            try
            {
                String typeName = JsonbNative.getInstance().typeOf(buffer);
                return UTF8Type.instance.decompose(typeName);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to get type: " + e.getMessage());
            }
        }
    };

    /**
     * Get the length of a JSONB array.
     */
    public static final NativeScalarFunction jsonbArrayLength = new NativeScalarFunction("jsonb_array_length", Int32Type.instance, JsonbType.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 1);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);

            if (buffer == null)
                return null;

            try
            {
                long length = JsonbNative.getInstance().arrayLength(buffer);
                // Cast to int (safe for practical array sizes)
                return ByteBufferUtil.bytes((int) length);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to get array length: " + e.getMessage());
            }
        }
    };

    /**
     * Get the keys of a JSONB object as a JSONB array.
     */
    public static final NativeScalarFunction jsonbObjectKeys = new NativeScalarFunction("jsonb_object_keys", JsonbType.instance, JsonbType.instance)
    {
        @Override
        public Arguments newArguments(FunctionContext context)
        {
            return FunctionArguments.newNoopInstance(context, 1);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            ByteBuffer buffer = arguments.get(0);

            if (buffer == null)
                return null;

            try
            {
                return JsonbNative.getInstance().objectKeys(buffer);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Failed to get object keys: " + e.getMessage());
            }
        }
    };
}
