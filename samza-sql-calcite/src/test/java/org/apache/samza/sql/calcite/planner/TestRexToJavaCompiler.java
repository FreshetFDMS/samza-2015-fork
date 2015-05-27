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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.schema.Schemas;
import org.apache.samza.sql.api.expressions.Expression;
import org.apache.samza.sql.calcite.rel.FilterableStreamScan;
import org.apache.samza.sql.calcite.test.Constants;
import org.apache.samza.sql.calcite.test.Utils;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestRexToJavaCompiler {
  @Test
  public void testConditions() throws IOException, SQLException {
    SamzaCalciteConnection connection = new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE, context, true);
    FilterableStreamScan streamScan = (FilterableStreamScan) relNode.getInput(0);
    RexToJavaCompiler expressionCompiler = new RexToJavaCompiler(relNode.getCluster().getRexBuilder());

    List<RelNode> inputs = new ArrayList<RelNode>();
    inputs.add(streamScan);

    Expression expr = expressionCompiler.compile(inputs, streamScan.getFilters());
    Assert.assertNotNull(expr);

    Assert.assertFalse((Boolean) expr.execute(Constants.SAMPLE_ORDER_1));
    Assert.assertTrue((Boolean) expr.execute(Constants.SAMPLE_ORDER_2));
  }

  @Test
  public void testProjections() throws IOException, SQLException {
    SamzaCalciteConnection connection = new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE_AND_RENAME, context, true);

    Project project = (Project) relNode.getInput(0);
    FilterableStreamScan streamScan = (FilterableStreamScan) project.getInput(0);

    RexToJavaCompiler expressionCompiler = new RexToJavaCompiler(relNode.getCluster().getRexBuilder());

    List<RelNode> inputs = new ArrayList<RelNode>();
    inputs.add(streamScan);

    Expression expr = expressionCompiler.compile(inputs, project.getProjects());
    Assert.assertNotNull(expr);

    Object[] results = new Object[2];
    expr.execute(Constants.SAMPLE_ORDER_1, results);

    Assert.assertEquals("paint", results[0]);
    Assert.assertEquals(5, results[1]);
  }
}
