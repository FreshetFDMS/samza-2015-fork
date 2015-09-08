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
package org.apache.samza.sql.planner.common;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.samza.sql.planner.logical.SamzaStream;

import java.util.List;

public class SamzaStreamInsertRelBase extends TableModify implements SamzaRelNode {

  protected final SamzaStream samzaStream;

  protected SamzaStreamInsertRelBase(RelOptCluster cluster, RelTraitSet traits, RelOptTable table,
                                     Prepare.CatalogReader catalogReader, RelNode child,
                                     Operation operation, List<String> updateColumnList,
                                     boolean flattened) {
    super(cluster, traits, table, catalogReader, child, operation, updateColumnList, flattened);
    this.samzaStream = table.unwrap(SamzaStream.class);
  }
}
