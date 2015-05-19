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
package org.apache.samza.sql.operators.scan;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.expressions.Expression;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.data.DataUtils;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.operators.factory.SimpleOperatorImpl;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

public class FilterableStreamScanOp extends SimpleOperatorImpl {

  private FilterableStreamScanSpec spec;

  /**
   * Ctor of <code>ProjectableFilterableScan</code> class
   *
   * @param spec The specification of this operator
   */
  public FilterableStreamScanOp(FilterableStreamScanSpec spec) {
    super(spec);
    this.spec = spec;
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {

  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    throw new UnsupportedOperationException("FilterableStreamScanOp operator doesn't support processing relations.");
  }

  @Override
  protected void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if(!DataUtils.isStruct(ituple.getMessage())){
      // Log to error topic.
      throw new SamzaException("Only messages of type struct is supported. Message type: " + ituple.getMessage().schema().getType());
    }

    Object[] inputValues = DataUtils.dataToObjectArray(ituple.getMessage(), spec.getInputTypes().get(0));
    Expression filter = spec.getFilterExpression();
    if (filter != null && (Boolean)filter.execute(inputValues)){
      // TODO: Handle project after filter
      collector.send(IntermediateMessageTuple.fromTuple(ituple, false, spec.getOutputName()));
    }
  }

  @Override
  public OperatorSpec getSpec() {
    return spec;
  }
}
