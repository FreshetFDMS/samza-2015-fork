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
package org.apache.samza.sql.operators.project;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.expressions.Expression;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.data.DataUtils;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.operators.factory.SimpleOperator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SqlMessageCollector;

public class ProjectOp extends SimpleOperator implements TupleOperator {
  private ProjectSpec spec;

  /**
   * Ctor of <code>SimpleOperator</code> class
   *
   * @param spec The specification of this operator
   */
  public ProjectOp(ProjectSpec spec) {
    super(spec);
    this.spec = spec;
  }

  @Override
  public void process(Tuple tuple, SqlMessageCollector collector) throws Exception {
    if(!DataUtils.isStruct(tuple.getMessage())){
      // Log to error topic.
      throw new SamzaException("Only messages of type struct is supported. Message type: " + tuple.getMessage().schema().getType());
    }

    Object[] inputValues = DataUtils.dataToObjectArray(tuple.getMessage(), spec.getInputType());
    Object[] results = new Object[spec.getOutputType().getFieldList().size()];
    Expression project = spec.getProjectExpression();
    project.execute(inputValues, results);

    collector.send(IntermediateMessageTuple.fromProcessedMessage(results, false, spec.getOutputName(), tuple.getKey()));
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }
}
