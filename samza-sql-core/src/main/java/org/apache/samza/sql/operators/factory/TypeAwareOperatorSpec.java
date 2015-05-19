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
package org.apache.samza.sql.operators.factory;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Schema;

import java.util.ArrayList;
import java.util.List;

public class TypeAwareOperatorSpec extends SimpleOperatorSpec {

  /**
   * Schemas of input messages (Certain operators  such as join can have multiple inputs).
   */
  private final List<Schema> inputTypes = new ArrayList<Schema>();

  /**
   * Schema of the output message
   */
  private final List<Schema> outputTypes = new ArrayList<Schema>();

  /**
   * Ctor of the {@code TypeAwareOperatorSpec} for simple {@link org.apache.samza.sql.api.operators.SimpleOperator}s w/
   * one input and one output (so one input type and one output type).
   *
   * @param id         Unique identifier of the {@link org.apache.samza.sql.api.operators.SimpleOperator} object
   * @param input      The only input entity
   * @param output     The only output entity
   * @param inputType  Type of the input entity
   * @param outputType Type of the output entity
   */
  public TypeAwareOperatorSpec(String id, EntityName input, EntityName output, Schema inputType, Schema outputType) {
    super(id, input, output);
    this.inputTypes.add(inputType);
    this.outputTypes.add(outputType);
  }

  /**
   * Ctor of the {@code TypeAwareOperatorSpec} for simple {@link org.apache.samza.sql.api.operators.SimpleOperator}s w/
   * multiple inputs and one output (so one input type and one output type).
   *
   * @param id         Unique identifier of the {@link org.apache.samza.sql.api.operators.SimpleOperator} object
   * @param inputs     The list of input entities
   * @param output     The only output entity
   * @param inputTypes The list of type of input entities
   * @param outputType Type of the output entity
   */
  public TypeAwareOperatorSpec(String id, List<EntityName> inputs, EntityName output, List<Schema> inputTypes, Schema outputType) {
    super(id, inputs, output);
    this.inputTypes.addAll(inputTypes);
    this.outputTypes.add(outputType);
  }

  public List<Schema> getInputTypes() {
    return inputTypes;
  }

  public List<Schema> getOutputTypes() {
    return outputTypes;
  }

  /**
   * Method to get the type of first input entity
   *
   * @return The first input entity type
   */
  public Schema getInputType() {
    return inputTypes.get(0);
  }

  /**
   * Method to get the type of first output entity
   *
   * @return The first input entity type
   */
  public Schema getOutputType() {
    return outputTypes.get(0);
  }
}
