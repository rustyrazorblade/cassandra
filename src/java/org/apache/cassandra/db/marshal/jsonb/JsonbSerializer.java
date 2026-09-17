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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;

/**
 * Serializer for JSONB binary values. The value is stored as-is with no additional transformation.
 */
public class JsonbSerializer extends TypeSerializer<ByteBuffer>
{
    public static final JsonbSerializer instance = new JsonbSerializer();

    private JsonbSerializer() {}

    @Override
    public ByteBuffer serialize(ByteBuffer value)
    {
        return value;
    }

    @Override
    public <V> ByteBuffer deserialize(V value, ValueAccessor<V> accessor)
    {
        return accessor.toBuffer(value);
    }

    @Override
    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (accessor.isEmpty(value))
        {
            throw new MarshalException("JSONB value cannot be empty");
        }

        // Convert to ByteBuffer for native validation
        ByteBuffer buffer = accessor.toBuffer(value);

        // Validate through native library
        JsonbNative.getInstance().validate(buffer);
    }

    @Override
    public String toString(ByteBuffer value)
    {
        if (value == null || !value.hasRemaining())
        {
            return "null";
        }

        try
        {
            return JsonbNative.getInstance().toText(value);
        }
        catch (MarshalException e)
        {
            return "<invalid JSONB: " + e.getMessage() + ">";
        }
    }

    @Override
    public Class<ByteBuffer> getType()
    {
        return ByteBuffer.class;
    }
}
