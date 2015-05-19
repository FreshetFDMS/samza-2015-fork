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
package org.apache.samza.sql.calcite.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;

public class ProjectableFilterableStreamScan extends StreamScan {
  public final ImmutableList<RexNode> filters;
  public final ImmutableIntList projects;


  ProjectableFilterableStreamScan(RelOptCluster cluster, RelOptTable table,
                                  ImmutableList<RexNode> filters,
                                  ImmutableIntList projects) {
    super(cluster, table);
    this.filters = filters;
    this.projects = projects;
    Preconditions.checkArgument(canHandle(table));
  }

  public static ProjectableFilterableStreamScan create(RelOptCluster cluster,
                                                       RelOptTable relOptTable, List<RexNode> filters,
                                                       List<Integer> projects) {
    // TODO: Verify whether this should be similar to BindableScan
    return new ProjectableFilterableStreamScan(cluster, relOptTable,
        ImmutableList.copyOf(filters), ImmutableIntList.copyOf(projects));
  }

  @Override
  public RelDataType deriveRowType() {
    final RelDataTypeFactory.FieldInfoBuilder builder =
        getCluster().getTypeFactory().builder();
    final List<RelDataTypeField> fieldList =
        table.getRowType().getFieldList();
    for (int project : projects) {
      builder.add(fieldList.get(project));
    }
    return builder.build();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("filters", filters, !filters.isEmpty())
        .itemIf("projects", projects, !projects.equals(identity()));
  }

  public ImmutableList<RexNode> getFilters() {
    return filters;
  }

  public ImmutableIntList getProjects() {
    return projects;
  }
}
