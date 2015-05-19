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
package org.apache.samza.sql.calcite.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schemas;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.calcite.planner.ExecutionPlanner;
import org.apache.samza.sql.calcite.planner.QueryPlanner;
import org.apache.samza.sql.calcite.planner.SamzaCalciteConnection;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.apache.samza.sql.calcite.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class StreamSqlTask implements StreamTask, InitableTask, WindowableTask {
  private static final Logger log = LoggerFactory.getLogger(StreamSqlTask.class);

  public static final String PROP_STREAM_SQL_QUERY = "stream.sql.query";
  public static final String PROP_STREAM_SQL_MODEL = "stream.sql.model";
  public static final String PROP_MAX_MASSAGES = "task.max.messages";

  private OperatorRouter operatorRouter;

  /**
   * How many messages the all tasks in a single container have processed.
   */
  private static int messagesProcessed = 0;

  /**
   * How many messages to process before shutting down.
   */
  private int maxMessages;

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    String streamingQuery = config.get(PROP_STREAM_SQL_QUERY);
    if (streamingQuery == null) {
      throw new SamzaException("Missing configuration '" + PROP_STREAM_SQL_QUERY + "'.");
    }

    String calciteModelLocation = config.get(PROP_STREAM_SQL_MODEL);
    if (calciteModelLocation == null) {
      throw new SamzaException("Missing configuration '" + PROP_STREAM_SQL_MODEL + "'.");
    }

    if (calciteModelLocation.startsWith("resource:")) {
      try {
        String model = Utils.loadCalciteModelToString(calciteModelLocation);
        operatorRouter = getExecutionPlan(model, streamingQuery);
      } catch (IOException e){
        log.error("Operation involving IO failed", e);
        throw new SamzaException("Cannot read required resources or error while performing io operation.", e);
      }
    } else {
      throw new SamzaException("Unsupported model location  " + calciteModelLocation);
    }

    maxMessages = config.getInt(PROP_MAX_MASSAGES, 8);
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    messagesProcessed += 1;

    IncomingMessageTuple tuple = new IncomingMessageTuple(envelope);
    operatorRouter.process(tuple, collector, coordinator);

    if (messagesProcessed >= maxMessages) {
      coordinator.shutdown(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
    }
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  /**
   * Get the execution plan for streaming query.
   *
   * @param model calcite model string
   * @param query streaming query
   * @return execution plan
   * @throws SQLException
   * @throws IOException
   */
  private static OperatorRouter getExecutionPlan(String model, String query) throws SQLException, IOException {
    SamzaCalciteConnection connection = new SamzaCalciteConnection(model);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultQueryPlannerConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode logicalPlan = planner.getPlan(query, context, true);

    ExecutionPlanner executionPlanner = new ExecutionPlanner(logicalPlan);

    return executionPlanner.getExecutionPlan();
  }
}
