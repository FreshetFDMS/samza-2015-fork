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
package org.apache.samza.sql.calcite;

import org.apache.avro.Schema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionProperty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

public class Utils {

  static {
    URL.setURLStreamHandlerFactory(new org.apache.samza.sql.Utils.ResourceStreamHandlerFactory());
  }

  /**
   * Read Calcite model from a URL to string.
   *
   * @param modelUrl calcite model url
   * @return model as a string
   * @throws IOException
   */
  public static String loadCalciteModelToString(String modelUrl) throws IOException {
    URL calciteModel = new URL(modelUrl);
    URLConnection connection = calciteModel.openConnection();
    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

    StringBuilder model = new StringBuilder();
    String inputLine;

    while ((inputLine = in.readLine()) != null) {
      model.append(inputLine);
    }

    in.close();

    return model.toString();
  }

  /**
   * Default Calcite query planner configuration.
   *
   * @return query planner configuration
   */
  public static Map<CalciteConnectionProperty, String> defaultQueryPlannerConfiguration() {
    Map<CalciteConnectionProperty, String> map = new HashMap<CalciteConnectionProperty, String>();

    map.put(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    map.put(CalciteConnectionProperty.QUOTED_CASING, Casing.UNCHANGED.name());
    map.put(CalciteConnectionProperty.UNQUOTED_CASING, Casing.UNCHANGED.name());
    map.put(CalciteConnectionProperty.QUOTING, Quoting.BACK_TICK.name());

    return map;
  }


}
