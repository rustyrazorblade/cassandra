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

package org.apache.cassandra.arrow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Test;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CassandraArrowTypeMappingTest
{
    @Test
    public void scalarTypesMapMechanically()
    {
        assertThat(CassandraArrowTypeMapping.arrowType(BooleanType.instance)).isEqualTo(new ArrowType.Bool());
        assertThat(CassandraArrowTypeMapping.arrowType(Int32Type.instance)).isEqualTo(new ArrowType.Int(32, true));
        assertThat(CassandraArrowTypeMapping.arrowType(LongType.instance)).isEqualTo(new ArrowType.Int(64, true));
        assertThat(CassandraArrowTypeMapping.arrowType(FloatType.instance))
            .isEqualTo(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        assertThat(CassandraArrowTypeMapping.arrowType(DoubleType.instance))
            .isEqualTo(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        assertThat(CassandraArrowTypeMapping.arrowType(UTF8Type.instance)).isEqualTo(ArrowType.Utf8.INSTANCE);
        assertThat(CassandraArrowTypeMapping.arrowType(AsciiType.instance)).isEqualTo(ArrowType.Utf8.INSTANCE);
        assertThat(CassandraArrowTypeMapping.arrowType(BytesType.instance)).isEqualTo(ArrowType.Binary.INSTANCE);
        assertThat(CassandraArrowTypeMapping.arrowType(InetAddressType.instance)).isEqualTo(ArrowType.Binary.INSTANCE);
        assertThat(CassandraArrowTypeMapping.arrowType(TimestampType.instance))
            .isEqualTo(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
        assertThat(CassandraArrowTypeMapping.arrowType(SimpleDateType.instance)).isEqualTo(new ArrowType.Date(DateUnit.DAY));
        assertThat(CassandraArrowTypeMapping.arrowType(TimeType.instance)).isEqualTo(new ArrowType.Time(TimeUnit.NANOSECOND, 64));
        assertThat(CassandraArrowTypeMapping.arrowType(UUIDType.instance)).isEqualTo(new ArrowType.FixedSizeBinary(16));
        assertThat(CassandraArrowTypeMapping.arrowType(TimeUUIDType.instance)).isEqualTo(new ArrowType.FixedSizeBinary(16));
        assertThat(CassandraArrowTypeMapping.arrowType(DurationType.instance))
            .isEqualTo(new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO));
    }

    @Test
    public void counterMapsToInt64()
    {
        assertThat(CassandraArrowTypeMapping.arrowType(CounterColumnType.instance)).isEqualTo(new ArrowType.Int(64, true));
    }

    @Test
    public void varintAndDecimalMapToFixed256Decimal()
    {
        ArrowType.Decimal varint = (ArrowType.Decimal) CassandraArrowTypeMapping.arrowType(IntegerType.instance);
        assertThat(varint.getScale()).isEqualTo(0);
        assertThat(varint.getBitWidth()).isEqualTo(256);

        ArrowType.Decimal decimal = (ArrowType.Decimal) CassandraArrowTypeMapping.arrowType(DecimalType.instance);
        assertThat(decimal.getScale()).isEqualTo(CassandraArrowTypeMapping.DECIMAL_SCALE);
        assertThat(decimal.getBitWidth()).isEqualTo(256);
    }

    @Test
    public void decimalRescalingRoundTripsExactValues()
    {
        BigDecimal value = new BigDecimal("123.45");
        BigDecimal rescaled = CassandraArrowTypeMapping.toArrowDecimal(value);
        assertThat(rescaled.scale()).isEqualTo(CassandraArrowTypeMapping.DECIMAL_SCALE);
        assertThat(value).isEqualByComparingTo(rescaled);
    }

    @Test
    public void decimalWithTooManyFractionalDigitsOverflows()
    {
        BigDecimal tooPrecise = BigDecimal.ONE.movePointLeft(CassandraArrowTypeMapping.DECIMAL_SCALE + 5);
        assertThatThrownBy(() -> CassandraArrowTypeMapping.toArrowDecimal(tooPrecise))
            .isInstanceOf(CassandraArrowTypeMapping.ArrowValueOverflowException.class);
    }

    @Test
    public void varintTooLargeOverflows()
    {
        BigInteger huge = BigInteger.TWO.pow(300);
        assertThatThrownBy(() -> CassandraArrowTypeMapping.toArrowDecimal(huge))
            .isInstanceOf(CassandraArrowTypeMapping.ArrowValueOverflowException.class);
    }

    @Test
    public void varintWithinRangeConvertsExactly()
    {
        BigInteger value = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(3));
        BigDecimal converted = CassandraArrowTypeMapping.toArrowDecimal(value);
        assertThat(converted.scale()).isEqualTo(0);
        assertThat(new BigDecimal(value)).isEqualByComparingTo(converted);
    }

    @Test
    public void listMapsToArrowList()
    {
        ListType<?> list = ListType.getInstance(UTF8Type.instance, true);
        Field field = CassandraArrowTypeMapping.toArrowField(ColumnMetadata.regularColumn("ks", "tbl", "l", list, 0));
        assertThat(field.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(field.getChildren()).hasSize(1);
        assertThat(field.getChildren().get(0).getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
    }

    @Test
    public void setMapsToArrowListOfElementType()
    {
        SetType<?> set = SetType.getInstance(Int32Type.instance, true);
        Field field = CassandraArrowTypeMapping.toArrowField(ColumnMetadata.regularColumn("ks", "tbl", "s", set, 0));
        assertThat(field.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(field.getChildren().get(0).getType()).isEqualTo(new ArrowType.Int(32, true));
    }

    @Test
    public void mapMapsToArrowMapWithKeyValueEntries()
    {
        MapType<?, ?> map = MapType.getInstance(UTF8Type.instance, Int32Type.instance, true);
        Field field = CassandraArrowTypeMapping.toArrowField(ColumnMetadata.regularColumn("ks", "tbl", "m", map, 0));
        assertThat(field.getType()).isInstanceOf(ArrowType.Map.class);
        Field entries = field.getChildren().get(0);
        assertThat(entries.getChildren()).hasSize(2);
        assertThat(entries.getChildren().get(0).getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
        assertThat(entries.getChildren().get(1).getType()).isEqualTo(new ArrowType.Int(32, true));
    }

    @Test
    public void vectorMapsToFixedSizeList()
    {
        VectorType<?> vector = VectorType.getInstance(FloatType.instance, 4);
        Field field = CassandraArrowTypeMapping.toArrowField(ColumnMetadata.regularColumn("ks", "tbl", "v", vector, 0));
        assertThat(field.getType()).isInstanceOf(ArrowType.FixedSizeList.class);
        assertThat(((ArrowType.FixedSizeList) field.getType()).getListSize()).isEqualTo(4);
        assertThat(field.getChildren().get(0).getType()).isEqualTo(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
    }

    @Test
    public void tupleMapsToStructWithPositionalFieldNames()
    {
        TupleType tuple = new TupleType(Arrays.asList(Int32Type.instance, UTF8Type.instance));
        Field field = CassandraArrowTypeMapping.toArrowField(ColumnMetadata.regularColumn("ks", "tbl", "t", tuple, 0));
        assertThat(field.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(field.getChildren()).extracting(Field::getName).containsExactly("1", "2");
    }

    @Test
    public void udtMapsToStructWithFieldNames()
    {
        UserType udt = new UserType("ks",
                                     ByteBufferUtil.bytes("my_type"),
                                     Arrays.asList(FieldIdentifier.forUnquoted("a"), FieldIdentifier.forUnquoted("b")),
                                     Arrays.asList(Int32Type.instance, UTF8Type.instance),
                                     true);
        Field field = CassandraArrowTypeMapping.toArrowField(ColumnMetadata.regularColumn("ks", "tbl", "u", udt, 0));
        assertThat(field.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(field.getChildren()).extracting(Field::getName).containsExactly("a", "b");
    }

    @Test
    public void columnKindAndPositionAreCarriedAsMetadata()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("pk1", UTF8Type.instance)
                                            .addPartitionKeyColumn("pk2", Int32Type.instance)
                                            .addClusteringColumn("ck1", Int32Type.instance)
                                            .addStaticColumn("s1", UTF8Type.instance)
                                            .addRegularColumn("r1", UTF8Type.instance)
                                            .offline()
                                            .build();
        Schema schema = CassandraArrowTypeMapping.toArrowSchema(table);
        assertThat(schema.getFields()).hasSize(5);

        Field pk1 = findField(schema, "pk1");
        assertThat(pk1.getMetadata()).containsEntry(CassandraArrowTypeMapping.KIND_METADATA_KEY, "partition_key")
                                      .containsEntry(CassandraArrowTypeMapping.POSITION_METADATA_KEY, "0");

        Field pk2 = findField(schema, "pk2");
        assertThat(pk2.getMetadata()).containsEntry(CassandraArrowTypeMapping.KIND_METADATA_KEY, "partition_key")
                                      .containsEntry(CassandraArrowTypeMapping.POSITION_METADATA_KEY, "1");

        Field ck1 = findField(schema, "ck1");
        assertThat(ck1.getMetadata()).containsEntry(CassandraArrowTypeMapping.KIND_METADATA_KEY, "clustering")
                                      .containsEntry(CassandraArrowTypeMapping.POSITION_METADATA_KEY, "0");

        Field s1 = findField(schema, "s1");
        assertThat(s1.getMetadata()).containsEntry(CassandraArrowTypeMapping.KIND_METADATA_KEY, "static");

        Field r1 = findField(schema, "r1");
        assertThat(r1.getMetadata()).containsEntry(CassandraArrowTypeMapping.KIND_METADATA_KEY, "regular");

        // key columns must be non-null in CQL semantics, but every field is still marked nullable
        // at the Arrow level for simplicity (a missing partition key cannot occur in practice,
        // since it is the source of row identity).
        assertThat(pk1.isNullable()).isTrue();
    }

    private static Field findField(Schema schema, String name)
    {
        return schema.getFields().stream().filter(f -> f.getName().equals(name)).findFirst()
                     .orElseThrow(() -> new AssertionError("no field named " + name));
    }
}
