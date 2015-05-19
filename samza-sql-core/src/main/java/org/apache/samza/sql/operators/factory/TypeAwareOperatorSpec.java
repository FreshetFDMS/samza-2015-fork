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

import java.util.List;

public class TypeAwareOperatorSpec extends SimpleOperatorSpec {

  /**
   * Schemas of input messages (Certain operators  such as join can have multiple inputs).
   */
  private final List<Schema> inputTypes;

  /**
   * Schema of the output message
   */
  private final Schema outputType;

  public TypeAwareOperatorSpec(String id, EntityName input, EntityName output, List<Schema> inputTypes, Schema outputType) {
    super(id, input, output);
    this.inputTypes = inputTypes;
    this.outputType = outputType;
  }

  public TypeAwareOperatorSpec(String id, List<EntityName> inputs, EntityName output, List<Schema> inputTypes, Schema outputType) {
    super(id, inputs, output);
    this.inputTypes = inputTypes;
    this.outputType = outputType;
  }

  public List<Schema> getInputTypes() {
    return inputTypes;
  }

  public Schema getOutputType() {
    return outputType;
  }
}
