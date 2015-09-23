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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.api.data.Data;
import org.apache.samza.sql.data.avro.AvroData;
import org.apache.samza.sql.data.avro.AvroSchema;
import org.apache.samza.sql.schema.AvroSchemaUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Converts instances of {@link org.apache.samza.sql.api.data.Data} to
 * {@link Object} array to use with expression generated by Calcite.
 * <p/>
 * TODO: Implement support for map, array, multiset and nested struct. Also need proper type mapping mechanism.
 * TODO: Also for converting floats, decimals and double we can use scale and precision values fromData type.
 */
public class TupleConverter {

    /**
     * Converts a {@link Data} instance to an Object array.
     *
     * @param tuple   {@code Data} object instance
     * @param sqlType row type to convert to
     * @return object array corresponding to specified type
     */
    public static Object[] samzaDataToObjectArray(Data tuple, RelDataType sqlType) {

        if (sqlType.isStruct()) {
            Object[] out = new Object[sqlType.getFieldCount()];

            for (RelDataTypeField field : sqlType.getFieldList()) {
                RelDataType fieldType = field.getType();

                // TODO: Implement support for maps, arrays and sets.
                if (isMap(sqlType) || isCollection(sqlType)) {
                    throw new SamzaException(String.format("Unsupported SQL type %s", sqlType.toString()));
                }

                out[field.getIndex()] = convertPrimitiveToSqlType(tuple.getFieldData(field.getName()), fieldType);
            }

            return out;
        } else if (!isMap(sqlType) && !isCollection(sqlType)) {
            return new Object[]{convertPrimitiveToSqlType(tuple, sqlType)};
        }


        throw new SamzaException(String.format("Unsupported SQL type %s", sqlType.toString()));
    }


    private static Object convertPrimitiveToSqlType(Data data, RelDataType type) {
    /* I hope that following this method instead of calling value.value() is better
    because we can catch any type mismatches. */
        switch (type.getSqlTypeName()) {
            case BOOLEAN:
                return data.booleanValue();
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return data.intValue();
            case BIGINT:
                return data.longValue();
            case REAL:
                return data.floatValue();
            case FLOAT:
            case DOUBLE:
                return data.doubleValue();
            case DATE:
                return new Date(data.longValue());
            case TIME:
                return new Time(data.longValue());
            case TIMESTAMP:
                return data.longValue();
            case CHAR:
            case VARCHAR:
                return data.strValue();
            case BINARY:
            case VARBINARY:
                return data.bytesValue();
            case ANY:
                return data.value();
            case SYMBOL:
                return data.value();
            default:
                throw new SamzaException(String.format("Unsupported type %s", type.getSqlTypeName()));
        }
    }

    private static Object[] convertArray(Data data, RelDataType type) {
        return null;
    }

    private static boolean isMap(RelDataType type) {
        return type.getKeyType() != null && type.getValueType() != null;
    }

    private static boolean isCollection(RelDataType type) {
        return type.getComponentType() != null;
    }

    public static Data objectArrayToSamzaData(Object[] tuple, RelDataType sqlType) {
        Schema avroSchema = AvroSchemaUtils.relDataTypeToAvroSchema(sqlType);
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);

        if (sqlType.isStruct()) {
            for (RelDataTypeField field : sqlType.getFieldList()) {
                recordBuilder.set(field.getName(), convertPrimitiveToAvroType(tuple[field.getIndex()], field.getType()));
            }

            GenericRecord record = recordBuilder.build();
            return AvroData.getStruct(AvroSchema.getSchema(avroSchema), record);
        }

        throw new SamzaException(String.format("SQL type %s is not supported at this level.", sqlType.toString()));
    }

    private static Object convertPrimitiveToAvroType(Object object, RelDataType type) {
        switch (type.getSqlTypeName()) {
            case BOOLEAN:
                return (Boolean)object;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return (Integer) object;
            case BIGINT:
                return (Long) object;
            case REAL:
                return (Float) object;
            case FLOAT:
            case DOUBLE:
                return (Double) object;
            case DATE:
                return ((Date) object).getTime();
            case TIME:
                return ((Time) object).getTime();
            case TIMESTAMP:
                return (Long)object;
            case CHAR:
            case VARCHAR:
                return (String) object;
            case BINARY:
            case VARBINARY:
                return (byte[])object;
            case ANY:
                return object;
            case SYMBOL:
                return object.toString();   // TODO: Verify this.
            default:
                throw new SamzaException(String.format("Unsupported type %s", type.getSqlTypeName()));
        }
    }
}
