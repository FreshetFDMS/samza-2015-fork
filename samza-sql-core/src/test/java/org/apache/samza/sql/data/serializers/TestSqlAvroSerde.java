/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.sql.data.serializers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.Serde;
import org.apache.samza.sql.Utils;
import org.apache.samza.sql.data.avro.AvroData;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestSqlAvroSerde {

  public static final String ORDERS_AVRO_SCHEMA_URL = "resource:orders.avsc";
  private static Serde serde = new SqlAvroSerdeFactory().getSerde("sqlAvro", sqlAvroSerdeTestConfig());

  @Test
  public void testSqlAvroSerdeDeserialization() throws IOException {
    Schema orderSchema = Utils.loadAvroSchemaFromClassPath(ORDERS_AVRO_SCHEMA_URL);
    AvroData decodedDatum = (AvroData)serde.fromBytes(encodeMessage(sampleOrderRecord(), orderSchema));

    Assert.assertTrue(decodedDatum.schema().getType() == org.apache.samza.sql.api.data.Schema.Type.STRUCT);
    Assert.assertTrue(decodedDatum.getFieldData("id").schema().getType() == org.apache.samza.sql.api.data.Schema.Type.INTEGER);
    Assert.assertTrue(decodedDatum.getFieldData("units").schema().getType() == org.apache.samza.sql.api.data.Schema.Type.INTEGER);
    Assert.assertTrue(decodedDatum.getFieldData("productId").schema().getType() == org.apache.samza.sql.api.data.Schema.Type.STRING);
  }

  @Test
  public void testSqlAvroSerialization() throws IOException {
    Schema orderSchema = Utils.loadAvroSchemaFromClassPath(ORDERS_AVRO_SCHEMA_URL);
    AvroData decodedDatumOriginal = (AvroData)serde.fromBytes(encodeMessage(sampleOrderRecord(), orderSchema));
    byte[] encodedDatum = serde.toBytes(decodedDatumOriginal);

    AvroData decodedDatum = (AvroData)serde.fromBytes(encodedDatum);

    Assert.assertTrue(decodedDatum.schema().getType() == org.apache.samza.sql.api.data.Schema.Type.STRUCT);
    Assert.assertTrue(decodedDatum.getFieldData("id").schema().getType() == org.apache.samza.sql.api.data.Schema.Type.INTEGER);
    Assert.assertTrue(decodedDatum.getFieldData("units").schema().getType() == org.apache.samza.sql.api.data.Schema.Type.INTEGER);
    Assert.assertTrue(decodedDatum.getFieldData("productId").schema().getType() == org.apache.samza.sql.api.data.Schema.Type.STRING);
  }

  private static Config sqlAvroSerdeTestConfig(){
    Map<String, String> config = new HashMap<String, String>();
    config.put("serializers.sqlAvro.schema", ORDERS_AVRO_SCHEMA_URL);

    return new MapConfig(config);
  }

  private static byte[] encodeMessage(GenericRecord datum, Schema avroSchema) throws IOException {
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroSchema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    writer.write(datum, encoder);
    encoder.flush();

    return  output.toByteArray();
  }

  private static GenericRecord sampleOrderRecord() throws IOException {
    Schema orderSchema = Utils.loadAvroSchemaFromClassPath(ORDERS_AVRO_SCHEMA_URL);
    GenericData.Record datum = new GenericData.Record(orderSchema);
    datum.put("id", 1);
    datum.put("productId", "paint");
    datum.put("units", 3);

    return datum;
  }
}
