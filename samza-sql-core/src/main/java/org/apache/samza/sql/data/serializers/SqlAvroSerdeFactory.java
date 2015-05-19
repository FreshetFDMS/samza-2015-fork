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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.sql.Utils;
import org.apache.samza.sql.data.avro.AvroData;

import java.io.IOException;

public class SqlAvroSerdeFactory implements SerdeFactory<AvroData> {
  public static final String PROP_AVRO_SCHEMA = "serializers.%s.schema";

  @Override
  public Serde<AvroData> getSerde(String name, Config config) {
    String avroSchemaLocation = config.get(String.format(PROP_AVRO_SCHEMA, name));
    if (avroSchemaLocation == null || avroSchemaLocation.isEmpty()) {
      throw new SamzaException("Cannot find avro schema for SerdeFactory '" + name + "'.");
    }

    if (avroSchemaLocation.startsWith("resource:")) {
      try {
        return new SqlAvroSerde(Utils.loadAvroSchemaFromClassPath(avroSchemaLocation));
      } catch (IOException e) {
        throw new SamzaException("Cannot create SqlAvroSerde", e);
      }
    } else {
      throw new SamzaException("Unsupported schema location type.");
    }
  }
}