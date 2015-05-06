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
package org.apache.samza.sql.data;

import org.apache.samza.SamzaException;
import org.apache.samza.sql.api.data.Data;
import org.apache.samza.sql.api.data.Field;
import org.apache.samza.sql.api.data.Schema;

import java.util.List;
import java.util.Map;

/**
 * Defines utility methods which interact with Samza SQL {@link org.apache.samza.sql.api.data.Data} objects.
 */
public class DataUtils {

  /**
   * Convert Samza {@link org.apache.samza.sql.api.data.Data} object to array for passing into a row expression.
   *
   * <p>This assumes that your {@link org.apache.samza.sql.api.data.Data} objects are of type Struct.</p>
   *
   * @param data data object
   * @param type data object schema
   * @return field values as an array
   */
  public static Object[] dataToObjectArray(Data data, Schema type){
    if(type.getType() != Schema.Type.STRUCT){
      throw new SamzaException(
          String.format("Unsupported type %s. Only data of type STRUCT is supported at this sytage.", type.getType()));
    }

    List<Field> fields = type.getFieldList();
    Object[] indexedFieldValues = new Object[fields.size()];

    for(Field f : fields){
      indexedFieldValues[f.getIndex()] = data.getFieldData(f.getName()).value();
    }

    return indexedFieldValues;
  }

  /**
   * Convert Samza {@link org.apache.samza.sql.api.data.Data} object to array for passing into a row expression.
   *
   * <p>This assumes that your {@link org.apache.samza.sql.api.data.Data} objects are of type Struct.</p>
   *
   * @param data                 Samza data object
   * @param columnCount          Number of columns/fields in the row represented by the above data object
   * @param indexToColumnNameMap Column index to struct field name mapping
   * @return                     array of struct field values
   */
  public static Object[] samzaStructDataToObjectArray(Data data, int columnCount, Map<Integer, String> indexToColumnNameMap){
    Object[] values = new Object[columnCount];

    for(int i = 0; i < columnCount; i++){
      values[i] = data.getFieldData(indexToColumnNameMap.get(i)).value();
    }
    return values;
  }

  /**
   * Is type of this data object a struct
   * @param data  data object
   * @return      true if data object is a struct, false otherwise
   */
  public static boolean isStruct(Data data){
    return data.schema().getType() == org.apache.samza.sql.api.data.Schema.Type.STRUCT;
  }
}
