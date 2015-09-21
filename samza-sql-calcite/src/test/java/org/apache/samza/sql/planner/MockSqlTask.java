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
package org.apache.samza.sql.planner;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.planner.logical.SamzaRel;
import org.apache.samza.sql.schema.CalciteModelProcessor;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

/**
 * Simple streaming SQL task for testing purposes.
 */
public class MockSqlTask implements StreamTask {

  private final OperatorRouter operatorRouter;

  public MockSqlTask(String schema, String query) throws Exception {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    QueryContext queryContext = new MockQueryContext(
        new CalciteModelProcessor("inline:" + schema, rootSchema).getDefaultSchema());
    QueryPlanner queryPlanner = new QueryPlanner(queryContext);
    this.operatorRouter = queryPlanner.getPhysicalPlan((SamzaRel) queryPlanner.getPlan(query));
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    operatorRouter.process(new IncomingMessageTuple(envelope), collector, coordinator);
  }
}
