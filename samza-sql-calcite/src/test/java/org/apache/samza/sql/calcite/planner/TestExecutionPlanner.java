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
package org.apache.samza.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schemas;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.router.OperatorRouter;
import org.apache.samza.sql.calcite.test.Constants;
import org.apache.samza.sql.calcite.test.OrderStreamTableFactory;
import org.apache.samza.sql.calcite.test.Utils;
import org.apache.samza.sql.operators.project.ProjectOp;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class TestExecutionPlanner {

  @Test
  public void testBasicExecutionPlanning() throws SQLException, IOException {
    SamzaCalciteConnection connection =
        new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.INSERT_INTO, context, true);

    ExecutionPlanner executionPlanner = new ExecutionPlanner(relNode);
    OperatorRouter router = executionPlanner.getExecutionPlan();

    Assert.assertNotNull(router);

    List<TupleOperator> tupleOperators = router.getTupleOperators(EntityName.getStreamName("streams:orders"));

    Assert.assertNotNull(tupleOperators);
    Assert.assertEquals(1, tupleOperators.size());
  }

  @Test
  public void testExecutionPlanWithProject() throws SQLException, IOException {
    SamzaCalciteConnection connection =
        new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE_AND_PROJECT, context, true);

    ExecutionPlanner executionPlanner = new ExecutionPlanner(relNode);
    OperatorRouter router = executionPlanner.getExecutionPlan();

    Assert.assertNotNull(router);

    List<TupleOperator> tupleOperators = router.getTupleOperators(EntityName.getStreamName("streams:orders"));

    Assert.assertNotNull(tupleOperators);
    Assert.assertEquals(1, tupleOperators.size());

    EntityName scanOutputStream = tupleOperators.get(0).getSpec().getOutputNames().get(0);

    tupleOperators = router.getTupleOperators(scanOutputStream);

    Assert.assertNotNull(tupleOperators);
    Assert.assertEquals(1, tupleOperators.size());
    Assert.assertTrue(tupleOperators.get(0) instanceof ProjectOp);
  }
}
