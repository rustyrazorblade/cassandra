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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.JsonbType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonbFctsTest
{
    private final JsonbType jsonbType = JsonbType.instance;

    private ByteBuffer parseJson(String json)
    {
        return jsonbType.fromString(json);
    }

    @Test
    public void testJsonbFromText()
    {
        Arguments args = makeArguments(JsonbFcts.jsonbFromText, "{\"key\":\"value\"}");
        ByteBuffer result = JsonbFcts.jsonbFromText.execute(args);
        assertNotNull(result);

        // Convert back to text to verify
        String text = jsonbType.getSerializer().toString(result);
        assertTrue(text.contains("key"));
        assertTrue(text.contains("value"));
    }

    @Test
    public void testJsonbFromTextNull()
    {
        Arguments args = makeArguments(JsonbFcts.jsonbFromText, (String) null);
        ByteBuffer result = JsonbFcts.jsonbFromText.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbFromTextMalformed()
    {
        try
        {
            Arguments args = makeArguments(JsonbFcts.jsonbFromText, "{invalid}");
            JsonbFcts.jsonbFromText.execute(args);
            fail("Should reject malformed JSON");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("parse"));
        }
    }

    @Test
    public void testJsonbToText()
    {
        ByteBuffer jsonb = parseJson("{\"test\":123}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbToText, jsonb);
        ByteBuffer result = JsonbFcts.jsonbToText.execute(args);
        assertNotNull(result);

        String text = UTF8Type.instance.compose(result);
        assertTrue(text.contains("test"));
        assertTrue(text.contains("123"));
    }

    @Test
    public void testJsonbToTextNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbToText, (ByteBuffer) null);
        ByteBuffer result = JsonbFcts.jsonbToText.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbGetKey()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\",\"age\":30}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetKey, jsonb, "name");
        ByteBuffer result = JsonbFcts.jsonbGetKey.execute(args);
        assertNotNull(result);

        String value = jsonbType.getSerializer().toString(result);
        assertEquals("\"Alice\"", value);
    }

    @Test
    public void testJsonbGetKeyNotFound()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\"}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetKey, jsonb, "missing");
        ByteBuffer result = JsonbFcts.jsonbGetKey.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbGetKeyNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetKey, null, "key");
        ByteBuffer result = JsonbFcts.jsonbGetKey.execute(args);
        assertNull(result);

        ByteBuffer jsonb = parseJson("{}");
        args = makeNoopArguments(JsonbFcts.jsonbGetKey, jsonb, (String) null);
        result = JsonbFcts.jsonbGetKey.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbGetIndex()
    {
        ByteBuffer jsonb = parseJson("[10,20,30]");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetIndex, jsonb, 1);
        ByteBuffer result = JsonbFcts.jsonbGetIndex.execute(args);
        assertNotNull(result);

        String value = jsonbType.getSerializer().toString(result);
        assertTrue(value.equals("20") || value.equals("20.0"));
    }

    @Test
    public void testJsonbGetIndexOutOfBounds()
    {
        ByteBuffer jsonb = parseJson("[10,20,30]");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetIndex, jsonb, 10);
        ByteBuffer result = JsonbFcts.jsonbGetIndex.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbGetIndexNegative()
    {
        // Negative array index should return null
        ByteBuffer jsonb = parseJson("[10,20,30]");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetIndex, jsonb, -1);
        ByteBuffer result = JsonbFcts.jsonbGetIndex.execute(args);
        assertNull("Negative array index should return null", result);

        // Test with other negative indices
        args = makeNoopArguments(JsonbFcts.jsonbGetIndex, jsonb, -5);
        result = JsonbFcts.jsonbGetIndex.execute(args);
        assertNull("Negative array index -5 should return null", result);
    }

    @Test
    public void testJsonbGetIndexNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetIndex, null, 0);
        ByteBuffer result = JsonbFcts.jsonbGetIndex.execute(args);
        assertNull(result);

        ByteBuffer jsonb = parseJson("[]");
        args = makeNoopArguments(JsonbFcts.jsonbGetIndex, jsonb, (Integer) null);
        result = JsonbFcts.jsonbGetIndex.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbContains()
    {
        ByteBuffer a = parseJson("{\"a\":1,\"b\":2,\"c\":3}");
        ByteBuffer b = parseJson("{\"a\":1,\"b\":2}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbContains, a, b);
        ByteBuffer result = JsonbFcts.jsonbContains.execute(args);
        assertNotNull(result);

        boolean contains = BooleanType.instance.compose(result);
        assertTrue(contains);
    }

    @Test
    public void testJsonbContainsFalse()
    {
        ByteBuffer a = parseJson("{\"a\":1}");
        ByteBuffer b = parseJson("{\"b\":2}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbContains, a, b);
        ByteBuffer result = JsonbFcts.jsonbContains.execute(args);
        assertNotNull(result);

        boolean contains = BooleanType.instance.compose(result);
        assertFalse(contains);
    }

    @Test
    public void testJsonbContainsNull()
    {
        ByteBuffer jsonb = parseJson("{}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbContains, null, jsonb);
        ByteBuffer result = JsonbFcts.jsonbContains.execute(args);
        assertNull(result);

        args = makeNoopArguments(JsonbFcts.jsonbContains, jsonb, null);
        result = JsonbFcts.jsonbContains.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbContained()
    {
        ByteBuffer a = parseJson("{\"a\":1,\"b\":2}");
        ByteBuffer b = parseJson("{\"a\":1,\"b\":2,\"c\":3}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbContained, a, b);
        ByteBuffer result = JsonbFcts.jsonbContained.execute(args);
        assertNotNull(result);

        boolean contained = BooleanType.instance.compose(result);
        assertTrue(contained);
    }

    @Test
    public void testJsonbContainedNull()
    {
        ByteBuffer jsonb = parseJson("{}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbContained, null, jsonb);
        ByteBuffer result = JsonbFcts.jsonbContained.execute(args);
        assertNull(result);

        args = makeNoopArguments(JsonbFcts.jsonbContained, jsonb, null);
        result = JsonbFcts.jsonbContained.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbExists()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\",\"age\":30}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbExists, jsonb, "name");
        ByteBuffer result = JsonbFcts.jsonbExists.execute(args);
        assertNotNull(result);

        boolean exists = BooleanType.instance.compose(result);
        assertTrue(exists);
    }

    @Test
    public void testJsonbExistsFalse()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\"}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbExists, jsonb, "missing");
        ByteBuffer result = JsonbFcts.jsonbExists.execute(args);
        assertNotNull(result);

        boolean exists = BooleanType.instance.compose(result);
        assertFalse(exists);
    }

    @Test
    public void testJsonbExistsNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbExists, null, "key");
        ByteBuffer result = JsonbFcts.jsonbExists.execute(args);
        assertNull(result);

        ByteBuffer jsonb = parseJson("{}");
        args = makeNoopArguments(JsonbFcts.jsonbExists, jsonb, (String) null);
        result = JsonbFcts.jsonbExists.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbType()
    {
        ByteBuffer jsonb = parseJson("{\"key\":\"value\"}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbType, jsonb);
        ByteBuffer result = JsonbFcts.jsonbType.execute(args);
        assertNotNull(result);

        String typeName = UTF8Type.instance.compose(result);
        assertEquals("object", typeName);

        jsonb = parseJson("[1,2,3]");
        args = makeNoopArguments(JsonbFcts.jsonbType, jsonb);
        result = JsonbFcts.jsonbType.execute(args);
        typeName = UTF8Type.instance.compose(result);
        assertEquals("array", typeName);

        jsonb = parseJson("\"text\"");
        args = makeNoopArguments(JsonbFcts.jsonbType, jsonb);
        result = JsonbFcts.jsonbType.execute(args);
        typeName = UTF8Type.instance.compose(result);
        assertEquals("string", typeName);

        jsonb = parseJson("42");
        args = makeNoopArguments(JsonbFcts.jsonbType, jsonb);
        result = JsonbFcts.jsonbType.execute(args);
        typeName = UTF8Type.instance.compose(result);
        assertEquals("number", typeName);

        jsonb = parseJson("true");
        args = makeNoopArguments(JsonbFcts.jsonbType, jsonb);
        result = JsonbFcts.jsonbType.execute(args);
        typeName = UTF8Type.instance.compose(result);
        assertEquals("boolean", typeName);

        jsonb = parseJson("null");
        args = makeNoopArguments(JsonbFcts.jsonbType, jsonb);
        result = JsonbFcts.jsonbType.execute(args);
        typeName = UTF8Type.instance.compose(result);
        assertEquals("null", typeName);
    }

    @Test
    public void testJsonbTypeNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbType, (ByteBuffer) null);
        ByteBuffer result = JsonbFcts.jsonbType.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbArrayLength()
    {
        ByteBuffer jsonb = parseJson("[1,2,3,4,5]");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbArrayLength, jsonb);
        ByteBuffer result = JsonbFcts.jsonbArrayLength.execute(args);
        assertNotNull(result);

        int length = Int32Type.instance.compose(result);
        assertEquals(5, length);
    }

    @Test
    public void testJsonbArrayLengthEmpty()
    {
        ByteBuffer jsonb = parseJson("[]");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbArrayLength, jsonb);
        ByteBuffer result = JsonbFcts.jsonbArrayLength.execute(args);
        assertNotNull(result);

        int length = Int32Type.instance.compose(result);
        assertEquals(0, length);
    }

    @Test
    public void testJsonbArrayLengthNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbArrayLength, (ByteBuffer) null);
        ByteBuffer result = JsonbFcts.jsonbArrayLength.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbArrayLengthNotArray()
    {
        try
        {
            ByteBuffer jsonb = parseJson("{\"not\":\"array\"}");
            Arguments args = makeNoopArguments(JsonbFcts.jsonbArrayLength, jsonb);
            JsonbFcts.jsonbArrayLength.execute(args);
            fail("Should reject non-array input");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("array length"));
        }
    }

    @Test
    public void testJsonbObjectKeys()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\",\"age\":30,\"city\":\"NYC\"}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbObjectKeys, jsonb);
        ByteBuffer result = JsonbFcts.jsonbObjectKeys.execute(args);
        assertNotNull(result);

        String keysJson = jsonbType.getSerializer().toString(result);
        assertTrue(keysJson.contains("name"));
        assertTrue(keysJson.contains("age"));
        assertTrue(keysJson.contains("city"));
    }

    @Test
    public void testJsonbObjectKeysEmpty()
    {
        ByteBuffer jsonb = parseJson("{}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbObjectKeys, jsonb);
        ByteBuffer result = JsonbFcts.jsonbObjectKeys.execute(args);
        assertNotNull(result);

        String keysJson = jsonbType.getSerializer().toString(result);
        assertTrue(keysJson.equals("[]"));
    }

    @Test
    public void testJsonbObjectKeysNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbObjectKeys, (ByteBuffer) null);
        ByteBuffer result = JsonbFcts.jsonbObjectKeys.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbObjectKeysNotObject()
    {
        try
        {
            ByteBuffer jsonb = parseJson("[1,2,3]");
            Arguments args = makeNoopArguments(JsonbFcts.jsonbObjectKeys, jsonb);
            JsonbFcts.jsonbObjectKeys.execute(args);
            fail("Should reject non-object input");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("object keys"));
        }
    }

    @Test
    public void testJsonbGet_text()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\",\"age\":30}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGet_text, jsonb, "name");
        ByteBuffer result = JsonbFcts.jsonbGet_text.execute(args);
        assertNotNull(result);

        String value = jsonbType.getSerializer().toString(result);
        assertEquals("\"Alice\"", value);
    }

    @Test
    public void testJsonbGet_textNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGet_text, null, "key");
        ByteBuffer result = JsonbFcts.jsonbGet_text.execute(args);
        assertNull(result);

        ByteBuffer jsonb = parseJson("{}");
        args = makeNoopArguments(JsonbFcts.jsonbGet_text, jsonb, (String) null);
        result = JsonbFcts.jsonbGet_text.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbGet_int()
    {
        ByteBuffer jsonb = parseJson("[10,20,30]");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGet_int, jsonb, 1);
        ByteBuffer result = JsonbFcts.jsonbGet_int.execute(args);
        assertNotNull(result);

        String value = jsonbType.getSerializer().toString(result);
        assertTrue(value.equals("20") || value.equals("20.0"));
    }

    @Test
    public void testJsonbGet_intNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGet_int, null, 0);
        ByteBuffer result = JsonbFcts.jsonbGet_int.execute(args);
        assertNull(result);

        ByteBuffer jsonb = parseJson("[]");
        args = makeNoopArguments(JsonbFcts.jsonbGet_int, jsonb, (Integer) null);
        result = JsonbFcts.jsonbGet_int.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbGetText_text()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\",\"age\":30}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetText_text, jsonb, "name");
        ByteBuffer result = JsonbFcts.jsonbGetText_text.execute(args);
        assertNotNull(result);

        String text = UTF8Type.instance.compose(result);
        assertEquals("\"Alice\"", text);
    }

    @Test
    public void testJsonbGetText_textNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetText_text, null, "key");
        ByteBuffer result = JsonbFcts.jsonbGetText_text.execute(args);
        assertNull(result);

        ByteBuffer jsonb = parseJson("{}");
        args = makeNoopArguments(JsonbFcts.jsonbGetText_text, jsonb, (String) null);
        result = JsonbFcts.jsonbGetText_text.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbGetText_textNotFound()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\"}");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetText_text, jsonb, "missing");
        ByteBuffer result = JsonbFcts.jsonbGetText_text.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbGetText_int()
    {
        ByteBuffer jsonb = parseJson("[10,20,30]");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetText_int, jsonb, 1);
        ByteBuffer result = JsonbFcts.jsonbGetText_int.execute(args);
        assertNotNull(result);

        String text = UTF8Type.instance.compose(result);
        assertTrue(text.equals("20") || text.equals("20.0"));
    }

    @Test
    public void testJsonbGetText_intNull()
    {
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetText_int, null, 0);
        ByteBuffer result = JsonbFcts.jsonbGetText_int.execute(args);
        assertNull(result);

        ByteBuffer jsonb = parseJson("[]");
        args = makeNoopArguments(JsonbFcts.jsonbGetText_int, jsonb, (Integer) null);
        result = JsonbFcts.jsonbGetText_int.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbGetText_intOutOfBounds()
    {
        ByteBuffer jsonb = parseJson("[10,20,30]");
        Arguments args = makeNoopArguments(JsonbFcts.jsonbGetText_int, jsonb, 10);
        ByteBuffer result = JsonbFcts.jsonbGetText_int.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbExistsAny()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\",\"age\":30}");
        List<String> keys = Arrays.asList("name", "city");
        Arguments args = makeArguments(JsonbFcts.jsonbExistsAny, jsonb, keys);
        ByteBuffer result = JsonbFcts.jsonbExistsAny.execute(args);
        assertNotNull(result);

        boolean exists = BooleanType.instance.compose(result);
        assertTrue(exists);
    }

    @Test
    public void testJsonbExistsAnyNoMatch()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\",\"age\":30}");
        List<String> keys = Arrays.asList("city", "country");
        Arguments args = makeArguments(JsonbFcts.jsonbExistsAny, jsonb, keys);
        ByteBuffer result = JsonbFcts.jsonbExistsAny.execute(args);
        assertNotNull(result);

        boolean exists = BooleanType.instance.compose(result);
        assertFalse(exists);
    }

    @Test
    public void testJsonbExistsAnyEmptyList()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\"}");
        List<String> keys = Collections.emptyList();
        Arguments args = makeArguments(JsonbFcts.jsonbExistsAny, jsonb, keys);
        ByteBuffer result = JsonbFcts.jsonbExistsAny.execute(args);
        assertNotNull(result);

        boolean exists = BooleanType.instance.compose(result);
        assertFalse(exists);
    }

    @Test
    public void testJsonbExistsAnyNull()
    {
        Arguments args = makeArguments(JsonbFcts.jsonbExistsAny, null, Arrays.asList("key"));
        ByteBuffer result = JsonbFcts.jsonbExistsAny.execute(args);
        assertNull(result);

        ByteBuffer jsonb = parseJson("{}");
        args = makeArguments(JsonbFcts.jsonbExistsAny, jsonb, null);
        result = JsonbFcts.jsonbExistsAny.execute(args);
        assertNull(result);
    }

    @Test
    public void testJsonbExistsAll()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\",\"age\":30}");
        List<String> keys = Arrays.asList("name", "age");
        Arguments args = makeArguments(JsonbFcts.jsonbExistsAll, jsonb, keys);
        ByteBuffer result = JsonbFcts.jsonbExistsAll.execute(args);
        assertNotNull(result);

        boolean exists = BooleanType.instance.compose(result);
        assertTrue(exists);
    }

    @Test
    public void testJsonbExistsAllNoMatch()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\",\"age\":30}");
        List<String> keys = Arrays.asList("name", "city");
        Arguments args = makeArguments(JsonbFcts.jsonbExistsAll, jsonb, keys);
        ByteBuffer result = JsonbFcts.jsonbExistsAll.execute(args);
        assertNotNull(result);

        boolean exists = BooleanType.instance.compose(result);
        assertFalse(exists);
    }

    @Test
    public void testJsonbExistsAllEmptyList()
    {
        ByteBuffer jsonb = parseJson("{\"name\":\"Alice\"}");
        List<String> keys = Collections.emptyList();
        Arguments args = makeArguments(JsonbFcts.jsonbExistsAll, jsonb, keys);
        ByteBuffer result = JsonbFcts.jsonbExistsAll.execute(args);
        assertNotNull(result);

        boolean exists = BooleanType.instance.compose(result);
        assertTrue(exists);
    }

    @Test
    public void testJsonbExistsAllNull()
    {
        Arguments args = makeArguments(JsonbFcts.jsonbExistsAll, null, Arrays.asList("key"));
        ByteBuffer result = JsonbFcts.jsonbExistsAll.execute(args);
        assertNull(result);

        ByteBuffer jsonb = parseJson("{}");
        args = makeArguments(JsonbFcts.jsonbExistsAll, jsonb, null);
        result = JsonbFcts.jsonbExistsAll.execute(args);
        assertNull(result);
    }

    /**
     * Helper to create deserialized arguments for a function.
     */
    private Arguments makeArguments(NativeFunction function, Object... values)
    {
        Arguments args = function.newArguments(ProtocolVersion.CURRENT);

        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            if (value == null)
            {
                args.set(i, null);
            }
            else if (value instanceof String)
            {
                args.set(i, UTF8Type.instance.decompose((String) value));
            }
            else if (value instanceof Integer)
            {
                args.set(i, Int32Type.instance.decompose((Integer) value));
            }
            else if (value instanceof ByteBuffer)
            {
                args.set(i, (ByteBuffer) value);
            }
            else if (value instanceof List)
            {
                // For List<String> arguments in deserializing mode, serialize to ByteBuffer
                ListType<String> listType = ListType.getInstance(UTF8Type.instance, false);
                args.set(i, listType.decompose((List<String>) value));
            }
            else
            {
                throw new IllegalArgumentException("Unsupported type: " + value.getClass());
            }
        }

        return args;
    }

    /**
     * Helper to create no-op (non-deserialized) arguments for a function.
     */
    private Arguments makeNoopArguments(NativeFunction function, Object... values)
    {
        Arguments args = FunctionArguments.newNoopInstance(ProtocolVersion.CURRENT, values.length);

        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            if (value == null)
            {
                args.set(i, null);
            }
            else if (value instanceof String)
            {
                args.set(i, UTF8Type.instance.decompose((String) value));
            }
            else if (value instanceof Integer)
            {
                args.set(i, Int32Type.instance.decompose((Integer) value));
            }
            else if (value instanceof ByteBuffer)
            {
                args.set(i, (ByteBuffer) value);
            }
            else if (value instanceof List)
            {
                // For List<String> arguments in noop mode, serialize the list
                ListType<String> listType = ListType.getInstance(UTF8Type.instance, false);
                args.set(i, listType.decompose((List<String>) value));
            }
            else
            {
                throw new IllegalArgumentException("Unsupported type: " + value.getClass());
            }
        }

        return args;
    }
}
