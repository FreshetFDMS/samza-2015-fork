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
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.util.Util;
import org.apache.samza.sql.calcite.test.Constants;
import org.apache.samza.sql.calcite.test.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

public class TestQueryPlanner {


  @Test
  public void testUnoptimizedSelectAllWithWhere() throws IOException, SQLException {

    SamzaCalciteConnection connection = new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE, context, false);
    Assert.assertNotNull(relNode);
    String s = Util.toLinux(RelOptUtil.toString(relNode));
    Assert.assertTrue(s.contains(Constants.SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE_PLAN_EXPECTED));
  }

  @Test
  public void testOptimizedSelectAllWithWhere() throws IOException, SQLException {

    SamzaCalciteConnection connection = new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE, context, true);
    Assert.assertNotNull(relNode);
    String s = Util.toLinux(RelOptUtil.toString(relNode));
    Assert.assertTrue(s.contains(Constants.SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE_OPTIMIZED_PLAN_EXPECTED));
  }

  @Test
  public void testInsertInto() throws IOException, SQLException {
    SamzaCalciteConnection connection = new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.INSERT_INTO, context, true);
    Assert.assertNotNull(relNode);
    String s = Util.toLinux(RelOptUtil.toString(relNode));
    Assert.assertTrue(s.contains(Constants.INSERT_INTO_OPTIMIZED_PLAN_EXPECTED));
  }

  @Test
  public void testTumblingWindows() throws IOException, SQLException {

    SamzaCalciteConnection connection = new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.TUMBLING_WINDOW_AGGREGATE, context, true);
    Assert.assertNotNull(relNode);

    System.out.println(RelOptUtil.toString(relNode));
  }

  @Test
  public void testSlidingWindowsIncludingWith() throws IOException, SQLException {
    SamzaCalciteConnection connection = new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.EXPLICIT_WINDOW_DEFS, context, true);
    Assert.assertNotNull(relNode);

    System.out.println(RelOptUtil.toString(relNode));
  }

  @Test
  public void testHoppingWindow() throws IOException, SQLException {
    SamzaCalciteConnection connection = new SamzaCalciteConnection(Constants.STREAM_MODEL);
    CalcitePrepare.Context context = Schemas.makeContext(connection,
        connection.getCalciteRootSchema(),
        ImmutableList.of(connection.getSchema()),
        ImmutableMap.copyOf(Utils.defaultConfiguration()));

    QueryPlanner planner = new QueryPlanner();
    RelNode relNode = planner.getPlan(Constants.SLIDING_WINDOW, context, true);
    Assert.assertNotNull(relNode);

    System.out.println(RelOptUtil.toString(relNode));
  }
}
