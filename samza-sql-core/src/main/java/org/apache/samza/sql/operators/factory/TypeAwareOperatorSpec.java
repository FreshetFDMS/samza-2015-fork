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
import org.apache.samza.sql.api.operators.spec.OperatorSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeAwareOperatorSpec extends SimpleOperatorSpec implements OperatorSpec {

  /**
   * Schema of the input message
   */
  private final Schema inputType;

  /**
   * Schema of the output message
   */
  private final Schema outputType;

  public TypeAwareOperatorSpec(String id, EntityName input, EntityName output, Schema inputType, Schema outputType) {
    super(id, input, output);
    this.inputType = inputType;
    this.outputType = outputType;
  }

  public TypeAwareOperatorSpec(String id, List<EntityName> inputs, EntityName output, Schema inputType, Schema outputType) {
    super(id, inputs, output);
    this.inputType = inputType;
    this.outputType = outputType;
  }

  public Schema getInputType() {
    return inputType;
  }

  public Schema getOutputType() {
    return outputType;
  }
}
