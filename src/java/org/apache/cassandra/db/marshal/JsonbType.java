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

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.jsonb.JsonbNative;
import org.apache.cassandra.db.marshal.jsonb.JsonbSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JsonUtils;

/**
 * Binary JSONB type using the Databend JSONB format.
 * This type uses byte-order comparison and is value-only (cannot be used in primary or clustering keys).
 */
public class JsonbType extends AbstractType<ByteBuffer>
{
    public static final JsonbType instance = new JsonbType();

    // Masked value is lazily computed on first access
    private volatile ByteBuffer maskedValue;

    JsonbType()
    {
        super(ComparisonType.BYTE_ORDER);
    }

    @Override
    public boolean allowsEmpty()
    {
        return false;
    }

    @Override
    public boolean isEmptyValueMeaningless()
    {
        return false;
    }

    /**
     * JSONB must not be used in partition or clustering keys.
     * The type uses opaque byte-order comparison with no semantic ordering.
     */
    public void validateForKey(boolean isPartitionKey, boolean isClusteringKey) throws MarshalException
    {
        if (isPartitionKey)
        {
            throw new MarshalException("JSONB cannot be used in partition keys (opaque byte-order comparison only)");
        }
        if (isClusteringKey)
        {
            throw new MarshalException("JSONB cannot be used in clustering keys (opaque byte-order comparison only)");
        }
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        if (source == null || source.isEmpty())
        {
            throw new MarshalException("JSONB value cannot be empty");
        }

        return JsonbNative.getInstance().fromText(source);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        // Re-serialize through Jackson then parse to JSONB
        String text = JsonUtils.writeAsJsonString(parsed);
        ByteBuffer binary = JsonbNative.getInstance().fromText(text);
        return new Constants.Value(binary);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        if (buffer == null || !buffer.hasRemaining())
        {
            return "null";
        }

        // Return JSON text as-is (not quoted)
        return JsonbNative.getInstance().toText(buffer);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        // Custom type registration (no grammar change)
        return new CQL3Type.Custom(this);
    }

    @Override
    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return JsonbSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ArgumentDeserializer.NOOP_DESERIALIZER;
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        // Only compatible with itself
        return this == previous;
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        // Only compatible with itself
        return this == otherType;
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        if (maskedValue == null)
        {
            synchronized (this)
            {
                if (maskedValue == null)
                {
                    // Lazy initialization: create JSONB null value
                    try
                    {
                        maskedValue = JsonbNative.getInstance().fromText("null");
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException("Failed to create masked JSONB value", e);
                    }
                }
            }
        }
        return maskedValue;
    }

    @Override
    public String toString()
    {
        return getClass().getName();
    }
}
