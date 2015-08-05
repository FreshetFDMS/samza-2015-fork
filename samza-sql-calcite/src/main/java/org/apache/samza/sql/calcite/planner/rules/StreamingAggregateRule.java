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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.util.Util;
import org.apache.samza.sql.calcite.rel.StreamingAggregate;

/**
 * Defines a rule convert LogicalAggregate to a StreamingAggregate if it is a input to a LogicalDelta.
 *
 * <p>We have to do this to handle streaming aggregates and normal aggregates differently. Calcite will make
 * sure streaming aggregate is possible and valid in the context of the query.</p>
 */
public abstract class StreamingAggregateRule extends RelOptRule {

  public static final StreamingAggregateRule DELTA = new StreamingAggregateRule(
      operand(LogicalDelta.class, operand(LogicalAggregate.class, any())), "StreamingAggregateRule") {
    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalDelta delta = call.rel(0);
      final LogicalAggregate aggregate = call.rel(1);
      apply(call, delta, aggregate);
    }
  };

  private StreamingAggregateRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  protected void apply(RelOptRuleCall call, LogicalDelta delta, LogicalAggregate aggregate) {
    final StreamingAggregate newAggregate = StreamingAggregate.create(
        aggregate.getInput(), aggregate.indicator, aggregate.getGroupSet(), aggregate.groupSets, aggregate.getAggCallList());
    Util.discard(delta);
    LogicalDelta newDelta = LogicalDelta.create(newAggregate);
    call.transformTo(newDelta);
  }
}
