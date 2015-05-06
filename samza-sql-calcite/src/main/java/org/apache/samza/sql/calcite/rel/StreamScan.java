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
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.samza.sql.calcite.schema.Stream;

import java.util.List;

/**
 * Relational operator that reads from a stream.
 */
public class StreamScan extends TableScan {

  public StreamScan(RelOptCluster cluster,
                       RelOptTable stream) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), stream);
    Preconditions.checkArgument(canHandle(stream));
  }

  @Override
  public RelOptTable getTable() {
    return super.getTable();
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new StreamScan(getCluster(), table);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw);
  }

  @Override public RelDataType deriveRowType() {
    final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
    final RelDataTypeFactory.FieldInfoBuilder builder =
        getCluster().getTypeFactory().builder();
    for (RelDataTypeField field : fieldList) {
      builder.add(field);
    }
    return builder.build();
  }

  protected boolean canHandle(RelOptTable table){
    return table.unwrap(Stream.class) != null;
  }
}
