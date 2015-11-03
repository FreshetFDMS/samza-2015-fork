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

package org.apache.samza.sql.physical.window.codegen;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.rel.core.Window;

/**
 * Generates the code for a window operator based on {@link org.apache.samza.sql.planner.logical.SamzaWindowRel}.
 *
 * Logic
 * -----
 *
 * Idea is to use a template (abstract class)
 */
public class WindowOperatorGenerator {

  private final JavaTypeFactory typeFactory;
  private final BlockBuilder blockBuilder;

  public WindowOperatorGenerator(JavaTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
    this.blockBuilder = new BlockBuilder();
  }

  public void generate(Window windowRel) {

  }
}
