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

import com.google.common.collect.ImmutableList;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.operators.aggregate.api.Grouping;
import org.apache.samza.sql.operators.factory.SimpleOperatorSpec;

import java.util.List;

public class AggregateSpec extends SimpleOperatorSpec{

  /*
   * For {@code GROUP BY} groupings contains one element.
   * If  {@code GROUP BY} is not specified or if {@code GROUP BY ()} is specified
   * groupings will contain empty grouping.
   *
   *
   */
  private final ImmutableList<Grouping> groupings;

  public AggregateSpec(String id, EntityName input, EntityName output, List<Grouping> groupings) {
    super(id, input, output);
    this.groupings = ImmutableList.copyOf(groupings);
  }

  public ImmutableList<Grouping> getGroupings() {
    return groupings;
  }

}
