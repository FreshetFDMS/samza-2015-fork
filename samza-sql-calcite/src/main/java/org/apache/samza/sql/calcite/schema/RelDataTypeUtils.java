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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines utility methods to handle different schema types.
 */
public class RelDataTypeUtils {

  /**
   * Convert a row type to a mapping from field/column index to field/column name.
   *
   * @param type row type
   * @return mapping from index to column/field name
   */
  public static Map<Integer, String> relDataTypeToIndexNameMap(RelDataType type) {
    Map<Integer, String> columnIndexToNameMap = new HashMap<Integer, String>();
    for (Ord<RelDataTypeField> field : Ord.zip(type.getFieldList())) {
      String fieldName = field.e.getName();
      if (fieldName == null) {
        fieldName = "field#" + field.i;
      }
      columnIndexToNameMap.put(field.i, fieldName);
    }

    return columnIndexToNameMap;
  }
}
