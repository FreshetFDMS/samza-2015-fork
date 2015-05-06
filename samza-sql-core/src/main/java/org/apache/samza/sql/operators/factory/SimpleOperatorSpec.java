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
import org.apache.samza.sql.api.operators.spec.OperatorSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * An abstract class that encapsulate the basic information and methods that all specification of operators should implement.
 */
public abstract class SimpleOperatorSpec implements OperatorSpec {
  /**
   * The identifier of the corresponding operator
   */
  private final String id;

  /**
   * The list of input entity names of the corresponding operator
   */
  private final List<EntityName> inputs = new ArrayList<EntityName>();

  /**
   * The list of output entity names of the corresponding operator
   */
  private final List<EntityName> outputs = new ArrayList<EntityName>();

  /**
   * type of the corresponding operator
   */
  private final Type type;

  /**
   * Ctor of the <code>SimpleOperatorSpec</code> for simple <code>Operator</code>s w/ one input and one output
   *
   * @param id     Unique identifier of the <code>Operator</code> object
   * @param input  The only input entity
   * @param output The only output entity
   */
  public SimpleOperatorSpec(String id, EntityName input, EntityName output) {
    this.id = id;
    this.inputs.add(input);
    this.outputs.add(output);
    this.type = Type.TUPLE;
  }

  public SimpleOperatorSpec(String id, Type type, EntityName input, EntityName output) {
    this.id = id;
    this.type = type;
    this.inputs.add(input);
    this.outputs.add(output);
  }

  /**
   * Ctor of <code>SimpleOperatorSpec</code> with general format: m inputs and n outputs
   *
   * @param id     Unique identifier of the <code>Operator</code> object
   * @param inputs The list of input entities
   * @param output The list of output entities
   */
  public SimpleOperatorSpec(String id, List<EntityName> inputs, EntityName output) {
    this.id = id;
    this.inputs.addAll(inputs);
    this.outputs.add(output);
    this.type = Type.TUPLE;
  }

  public SimpleOperatorSpec(String id, Type type, List<EntityName> inputs, EntityName output) {
    this.id = id;
    this.inputs.addAll(inputs);
    this.outputs.add(output);
    this.type = type;
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public List<EntityName> getInputNames() {
    return this.inputs;
  }

  @Override
  public List<EntityName> getOutputNames() {
    return this.outputs;
  }

  /**
   * Method to get the first output entity
   *
   * @return The first output entity name
   */
  public EntityName getOutputName() {
    return this.outputs.get(0);
  }

  /**
   * Method to get the first input entity
   *
   * @return The first input entity name
   */
  public EntityName getInputName() {
    return this.inputs.get(0);
  }

  @Override
  public Type getType() {
    return type;
  }

  protected static String genId(String operatorType) {
    /**
     * Note (Yi's comment from rb https://reviews.apache.org/r/33142/)
     * ---------------------------------------------------------------
     * For operators that keep states that should survive across restarts, UUID may not work. For example,
     * getId() is used in WindowOperator to identify the state store name, which should be the same across re-starts.
     */
    return String.format("%s:%s", operatorType, UUID.randomUUID().toString());
  }
}
