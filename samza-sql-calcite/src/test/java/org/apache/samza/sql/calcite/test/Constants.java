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
package org.apache.samza.sql.calcite.test;


public class Constants {
  public static final String STREAM_SCHEMA = "     {\n"
      + "       name: 'STREAMS',\n"
      + "       tables: [ {\n"
      + "         type: 'custom',\n"
      + "         name: 'ORDERS',\n"
      + "         stream: {\n"
      + "           stream: true\n"
      + "         },\n"
      + "         factory: '" + OrderStreamTableFactory.class.getName() + "'\n"
      + "       }, {"
      + "         type: 'custom',\n"
      + "         name: 'FILTEREDORDERS',\n"
      + "         stream: {\n"
      + "            stream: true\n"
      + "         },\n"
      + "         factory: '" + OrderStreamTableFactory.class.getName() + "'\n"
      + "       }]\n"
      + "     }\n";

  public static final String STREAM_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'STREAMS',\n"
      + "   schemas: [\n"
      + STREAM_SCHEMA
      + "   ]\n"
      + "}";

  public static final String SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE_PLAN_EXPECTED =
      "LogicalDelta\n" +
          "  LogicalProject(id=[$0], productId=[$1], units=[$2], rowtime=[$3])\n" +
          "    LogicalFilter(condition=[>($2, 5)])\n" +
          "      StreamScan(table=[[STREAMS, ORDERS]])";
  public static final String SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE =
      "select stream * from orders where units > 5";

  public static final String SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE_AND_RENAME =
      "select stream productId, (units + 1) as quantity from orders where units > 5";

  public static final String INSERT_INTO =
      "insert into filteredorders select stream * from orders where units > 5";

  public static final String EXPLICIT_WINDOW_DEFS = "WITH HourlyTotals (rowtime, productId, c, su) AS (\n" +
      "  SELECT FLOOR(rowtime TO HOUR),\n" +
      "    productId,\n" +
      "    COUNT(*),\n" +
      "    SUM(units)\n" +
      "  FROM Orders\n" +
      "  GROUP BY FLOOR(rowtime TO HOUR), productId) \n" +
      "SELECT STREAM rowtime,\n" +
      "  productId,\n" +
      "  SUM(su) OVER w AS su,\n" +
      "  SUM(c) OVER w AS c\n" +
      "FROM HourlyTotals\n" +
      "WINDOW w AS (\n" +
      "  ORDER BY rowtime\n" +
      "  RANGE INTERVAL '2' HOUR PRECEDING)";

  public static final String SELECT_ALL_FROM_ORDERS_WHERE_QUANTITY_GREATER_THAN_FIVE_OPTIMIZED_PLAN_EXPECTED =
      "LogicalDelta\n" +
          "  ProjectableFilterableStreamScan(table=[[STREAMS, ORDERS]], filters=[[>($2, 5)]])";

  public static final String INSERT_INTO_OPTIMIZED_PLAN_EXPECTED =
      "LogicalTableModify(table=[[STREAMS, FILTEREDORDERS]], operation=[INSERT], updateColumnList=[[]], flattened=[false])\n" +
          "  ProjectableFilterableStreamScan(table=[[STREAMS, ORDERS]], filters=[[>($2, 5)]])";

  public static final Object[] SAMPLE_ORDER_1 = {1, "paint", 4, System.currentTimeMillis()};
  public static final Object[] SAMPLE_ORDER_2 = {2, "salt", 7, System.currentTimeMillis()};

}
