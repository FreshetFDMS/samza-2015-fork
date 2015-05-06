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
package org.apache.samza.sql.calcite.schema;

import com.google.common.collect.Maps;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Map;

/**
 * Table based on a stream.
 *
 * <p>During the query planning phase, streams are considered as a special type of tables. </p>
 */
public abstract class Stream extends AbstractTable implements StreamableTable, TranslatableTable {

  private String parent;

  /**
   * Name of the corresponding stream (e. g. In case of Kafka, name of the stream is Kafka topic).
   */
  private String name;

  /**
   * Additional properties corresponding to the stream. For example, 'partition key' can be a additional
   * property of the corresponding stream.
   */
  private Map<String, Object> properties = Maps.newHashMap();

  private SamzaStreamType type;

  public Stream(String parent, String name, SamzaStreamType type) {
    this.name = name;
    this.parent = parent;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.STREAM;
  }

  public String getParent() {
    return parent;
  }

  public void addProperty(String name, Object value){
    properties.put(name, value);
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public org.apache.samza.sql.api.data.Schema getType(RelDataTypeFactory typeFactory) {
    return type.apply(typeFactory);
  }
}

