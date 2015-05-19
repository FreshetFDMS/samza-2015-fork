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
package org.apache.samza.sql.calcite.planner.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.samza.sql.calcite.rel.ProjectableFilterableStreamScan;
import org.apache.samza.sql.calcite.rel.StreamScan;
import org.apache.samza.sql.calcite.schema.Stream;

import java.util.List;

public abstract class ProjectableStreamScanRule extends RelOptRule {

  public static final ProjectableStreamScanRule INSTANCE =
      new ProjectableStreamScanRule(
          operand(Project.class,
              operand(StreamScan.class, none())),
          "ProjectStreamScanRule") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          final StreamScan scan = call.rel(1);
          apply(call, project, scan);
        }
      };

  private ProjectableStreamScanRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  protected void apply(RelOptRuleCall call, Project project, StreamScan scan) {
    final RelOptTable table = scan.getTable();
    assert table.unwrap(Stream.class) != null;

    final Mappings.TargetMapping mapping = project.getMapping();
    if (mapping == null
        || Mappings.isIdentity(mapping)) {
      return;
    }

    final ImmutableIntList projects;
    final ImmutableList<RexNode> filters;
    if (scan instanceof ProjectableFilterableStreamScan) {
      final ProjectableFilterableStreamScan pfStreamScan =
          (ProjectableFilterableStreamScan) scan;
      filters = pfStreamScan.filters;
      projects = pfStreamScan.projects;
    } else {
      filters = ImmutableList.of();
      projects = scan.identity();
    }

    final List<Integer> projects2 =
        Mappings.apply((Mapping) mapping, projects);
    call.transformTo(
        ProjectableFilterableStreamScan.create(scan.getCluster(), scan.getTable(),
            filters, projects2));
  }
}
