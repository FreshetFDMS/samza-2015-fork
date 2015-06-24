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
package org.apache.samza.sql.operators.aggregate;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.operators.aggregate.api.Grouping;
import org.apache.samza.sql.operators.factory.SimpleOperatorImpl;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

public class AggregateOp extends SimpleOperatorImpl {

  private final AggregateSpec spec;

  /**
   * Ctor of <code>AggregateOp</code> class
   *
   * @param spec The specification of this operator
   */
  public AggregateOp(AggregateSpec spec) {
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

  }

  @Override
  protected void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    for (Grouping grouping : spec.getGroupings()) {
      grouping.send(ituple, collector);
    }
  }
}
