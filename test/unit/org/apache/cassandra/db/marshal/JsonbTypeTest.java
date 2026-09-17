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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType.ComparisonType;
import org.apache.cassandra.serializers.MarshalException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonbTypeTest
{
    private final JsonbType type = JsonbType.instance;

    @Test
    public void testRoundTripStability()
    {
        String json1 = "{\"key\":\"value\",\"num\":42}";

        // First round trip
        ByteBuffer binary1 = type.fromString(json1);
        String text1 = type.getSerializer().toString(binary1);

        // Second round trip
        ByteBuffer binary2 = type.fromString(text1);
        String text2 = type.getSerializer().toString(binary2);

        // Text should stabilize after first round trip
        assertEquals("Round trip should produce stable text", text1, text2);

        // Binary should be identical after second parse
        assertEquals("Binary should be stable after second parse", binary1, binary2);
    }

    @Test
    public void testCanonicality()
    {
        // Test 1: Object keys should be sorted
        String unsorted = "{\"z\":1,\"a\":2,\"m\":3}";
        String sorted = "{\"a\":2,\"m\":3,\"z\":1}";

        ByteBuffer binary1 = type.fromString(unsorted);
        ByteBuffer binary2 = type.fromString(sorted);

        // Binary representations should be identical if canonical
        assertTrue("Keys should be sorted for canonical representation",
                   Arrays.equals(
                       binary1.array(), binary1.arrayOffset(), binary1.arrayOffset() + binary1.remaining(),
                       binary2.array(), binary2.arrayOffset(), binary2.arrayOffset() + binary2.remaining()
                   ));

        // Test 2: Duplicate keys should be deduplicated
        String withDups = "{\"a\":1,\"a\":2,\"a\":3}";
        ByteBuffer binaryDups = type.fromString(withDups);
        String resultText = type.getSerializer().toString(binaryDups);

        // Count occurrences of "\"a\":" in the result
        int count = (resultText.length() - resultText.replace("\"a\":", "").length()) / 4;
        assertEquals("Duplicate keys should be deduplicated to one occurrence", 1, count);

        // The library keeps the last occurrence of a duplicate key.
        // Parse two objects with same duplicate keys in different orders - the last value should win.
        String firstKeyWins = "{\"a\":99,\"a\":1}";
        String secondKeyWins = "{\"a\":1,\"a\":99}";
        ByteBuffer binaryFirst = type.fromString(firstKeyWins);
        ByteBuffer binarySecond = type.fromString(secondKeyWins);

        String textFirst = type.getSerializer().toString(binaryFirst);
        String textSecond = type.getSerializer().toString(binarySecond);

        // Both should contain the value 99 since it's the last occurrence in both
        assertTrue("First should keep last value 1", textFirst.contains("1"));
        assertTrue("Second should keep last value 99", textSecond.contains("99"));

        // Test 3: Whitespace normalization
        String withSpace = "{ \"a\" : 1 , \"b\" : 2 }";
        String withoutSpace = "{\"a\":1,\"b\":2}";

        ByteBuffer binarySpace1 = type.fromString(withSpace);
        ByteBuffer binarySpace2 = type.fromString(withoutSpace);

        assertTrue("Whitespace should be normalized for canonical representation",
                   Arrays.equals(
                       binarySpace1.array(), binarySpace1.arrayOffset(), binarySpace1.arrayOffset() + binarySpace1.remaining(),
                       binarySpace2.array(), binarySpace2.arrayOffset(), binarySpace2.arrayOffset() + binarySpace2.remaining()
                   ));
    }

    @Test
    public void testNumberFormCaveat()
    {
        // Numbers with different forms (42 vs 42.0) should NOT be byte-equal in Databend jsonb 0.4.1
        ByteBuffer binary42 = type.fromString("42");
        ByteBuffer binary42dot0 = type.fromString("42.0");

        assertFalse("Number forms 42 and 42.0 should NOT be byte-equal (Databend jsonb 0.4.1 behavior)",
                    Arrays.equals(
                        binary42.array(), binary42.arrayOffset(), binary42.arrayOffset() + binary42.remaining(),
                        binary42dot0.array(), binary42dot0.arrayOffset(), binary42dot0.arrayOffset() + binary42dot0.remaining()
                    ));

        // Verify the text representation differs as well
        String text42 = type.getSerializer().toString(binary42);
        String text42dot0 = type.getSerializer().toString(binary42dot0);
        assertFalse("Text representation should preserve number form difference", text42.equals(text42dot0));
    }

    @Test
    public void testCorruptBinaryRead()
    {
        // Test malformed binary data
        ByteBuffer malformed = ByteBuffer.wrap(new byte[]{0x01, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        try
        {
            type.getSerializer().validate(malformed);
            fail("Malformed binary should fail validation");
        }
        catch (MarshalException e)
        {
            // Expected
            assertTrue("Error message should indicate invalid JSONB", e.getMessage().contains("Invalid JSONB"));
        }

        // Test wrong format version byte (not 0x01)
        ByteBuffer wrongVersion = ByteBuffer.wrap(new byte[]{0x02, 0x00, 0x00});
        try
        {
            type.getSerializer().validate(wrongVersion);
            fail("Wrong version byte should fail validation");
        }
        catch (MarshalException e)
        {
            // Expected
            assertTrue("Error message should indicate invalid JSONB", e.getMessage().contains("Invalid JSONB"));
        }

        // Test toText with wrong version byte
        // JsonbSerializer.toString catches exceptions and returns an error string
        String result = type.getSerializer().toString(wrongVersion);
        assertTrue("Wrong version byte should produce error string in toText",
                   result.startsWith("<invalid JSONB:"));
    }

    @Test
    public void testFormatVersionTag()
    {
        // Verify that stored values have a leading 0x01 byte
        ByteBuffer binary = type.fromString("{\"test\":123}");
        assertTrue("Stored value should have remaining bytes", binary.hasRemaining());

        byte firstByte = binary.get(binary.position());
        assertEquals("Stored value should have format version byte 0x01", 0x01, firstByte);

        // Test that a different version byte is rejected
        byte[] wrongVersionBytes = new byte[binary.remaining()];
        binary.duplicate().get(wrongVersionBytes);
        wrongVersionBytes[0] = (byte) 0x99;  // Wrong version

        ByteBuffer wrongVersionBuffer = ByteBuffer.wrap(wrongVersionBytes);
        try
        {
            type.getSerializer().validate(wrongVersionBuffer);
            fail("Wrong version byte should be rejected");
        }
        catch (MarshalException e)
        {
            // Expected
            assertTrue("Error should indicate invalid JSONB", e.getMessage().contains("Invalid JSONB"));
        }
    }

    @Test
    public void testCaseSensitivity()
    {
        // Keys are case-sensitive (Rust as_object().get())
        ByteBuffer jsonb = type.fromString("{\"a\":1,\"A\":2}");

        // Use JsonbNative directly for exists check
        org.apache.cassandra.db.marshal.jsonb.JsonbNative nativeLib =
            org.apache.cassandra.db.marshal.jsonb.JsonbNative.getInstance();

        // Check lowercase 'a' exists
        assertTrue("Lowercase 'a' should exist", nativeLib.existsKey(jsonb, "a"));

        // Check uppercase 'A' exists
        assertTrue("Uppercase 'A' should exist", nativeLib.existsKey(jsonb, "A"));

        // Check wrong case doesn't exist
        assertFalse("Wrong case 'B' should not exist for key 'A'", nativeLib.existsKey(jsonb, "B"));

        // Verify getByKey returns exact-case match
        ByteBuffer valueA = nativeLib.getByKey(jsonb, "a");
        assertNotNull(valueA);
        String textA = type.getSerializer().toString(valueA);
        assertTrue("Value for 'a' should be 1", textA.equals("1") || textA.equals("1.0"));

        ByteBuffer valueCapitalA = nativeLib.getByKey(jsonb, "A");
        assertNotNull(valueCapitalA);
        String textCapitalA = type.getSerializer().toString(valueCapitalA);
        assertTrue("Value for 'A' should be 2", textCapitalA.equals("2") || textCapitalA.equals("2.0"));

        // Non-matching case returns null
        ByteBuffer valueB = nativeLib.getByKey(jsonb, "B");
        assertNull(valueB);
    }

    @Test
    public void testNestedRoundTrip()
    {
        // Multi-level nested object + array document
        String deepNested = "{\"outer\":{\"middle\":{\"inner\":[1,2,{\"deep\":\"value\"}]},\"array\":[[1,2],[3,4]]}}";

        // First round trip
        ByteBuffer binary1 = type.fromString(deepNested);
        String text1 = type.getSerializer().toString(binary1);

        // Second round trip
        ByteBuffer binary2 = type.fromString(text1);
        String text2 = type.getSerializer().toString(binary2);

        // Text should stabilize
        assertEquals("Nested structure should round-trip with stable text", text1, text2);

        // Binary should be identical
        assertEquals("Binary should be stable after round-trip", binary1, binary2);

        // Verify structure is preserved
        assertTrue("Should contain 'outer' key", text2.contains("outer"));
        assertTrue("Should contain 'middle' key", text2.contains("middle"));
        assertTrue("Should contain 'inner' key", text2.contains("inner"));
        assertTrue("Should contain 'deep' key", text2.contains("deep"));
        assertTrue("Should contain 'value'", text2.contains("value"));
    }

    @Test
    public void testValidation()
    {
        // Valid JSON should pass
        ByteBuffer valid = type.fromString("{\"valid\":true}");
        type.getSerializer().validate(valid);

        // Empty buffer should fail
        try
        {
            type.getSerializer().validate(ByteBuffer.allocate(0));
            fail("Empty buffer should fail validation");
        }
        catch (MarshalException e)
        {
            assertTrue(e.getMessage().contains("empty"));
        }
    }

    @Test
    public void testPartitionKeyRejection()
    {
        try
        {
            type.validateForKey(true, false);
            fail("JSONB should not be allowed in partition keys");
        }
        catch (MarshalException e)
        {
            assertTrue("Error message should mention partition keys",
                      e.getMessage().toLowerCase().contains("partition"));
        }
    }

    @Test
    public void testClusteringKeyRejection()
    {
        try
        {
            type.validateForKey(false, true);
            fail("JSONB should not be allowed in clustering keys");
        }
        catch (MarshalException e)
        {
            assertTrue("Error message should mention clustering keys",
                      e.getMessage().toLowerCase().contains("clustering"));
        }
    }

    @Test
    public void testBasicTypes()
    {
        // Null
        ByteBuffer nullBuf = type.fromString("null");
        assertEquals("null", type.getSerializer().toString(nullBuf));

        // Boolean
        ByteBuffer trueBuf = type.fromString("true");
        assertEquals("true", type.getSerializer().toString(trueBuf));

        // Number
        ByteBuffer numBuf = type.fromString("123");
        String numResult = type.getSerializer().toString(numBuf);
        assertTrue(numResult.equals("123") || numResult.equals("123.0"));

        // String
        ByteBuffer strBuf = type.fromString("\"hello\"");
        assertEquals("\"hello\"", type.getSerializer().toString(strBuf));

        // Array
        ByteBuffer arrBuf = type.fromString("[1,2,3]");
        String arrResult = type.getSerializer().toString(arrBuf);
        assertTrue(arrResult.contains("1") && arrResult.contains("2") && arrResult.contains("3"));

        // Object
        ByteBuffer objBuf = type.fromString("{\"a\":1}");
        String objResult = type.getSerializer().toString(objBuf);
        assertTrue(objResult.contains("\"a\"") && objResult.contains("1"));
    }

    @Test
    public void testMalformedJSON()
    {
        String[] malformed = {
            "{invalid}",
            "[1,2,",
            "{\"key\":}",
            "undefined",
            ""
        };

        for (String bad : malformed)
        {
            try
            {
                type.fromString(bad);
                fail("Should reject malformed JSON: " + bad);
            }
            catch (MarshalException e)
            {
                // Expected
            }
        }
    }

    @Test
    public void testCompatibility()
    {
        // Only compatible with itself
        assertTrue(type.isCompatibleWith(JsonbType.instance));
        assertFalse(type.isCompatibleWith(BytesType.instance));
        assertFalse(type.isCompatibleWith(UTF8Type.instance));

        assertTrue(type.isValueCompatibleWithInternal(JsonbType.instance));
        assertFalse(type.isValueCompatibleWithInternal(BytesType.instance));
        assertFalse(type.isValueCompatibleWithInternal(UTF8Type.instance));
    }

    @Test
    public void testByteOrderComparison()
    {
        // Verify the type uses byte-order comparison
        assertEquals(ComparisonType.BYTE_ORDER, type.comparisonType);
        assertTrue(type.isByteOrderComparable);
    }

    @Test
    public void testEmptyNotAllowed()
    {
        assertFalse("JSONB should not allow empty values", type.allowsEmpty());
        assertFalse("Empty should not be meaningless", type.isEmptyValueMeaningless());
    }
}
